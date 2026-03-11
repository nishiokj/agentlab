
impl RunSink for BufferedRunSink {
    fn write_run_manifest(&mut self, _run: &RunManifestRecord) -> Result<()> {
        Ok(())
    }

    fn append_trial_record(&mut self, row: &TrialRecord) -> Result<()> {
        self.trial_records.push(row.clone());
        Ok(())
    }

    fn append_metric_rows(&mut self, rows: &[MetricRow]) -> Result<()> {
        self.metric_rows.extend(rows.iter().cloned());
        Ok(())
    }

    fn append_event_rows(&mut self, rows: &[EventRow]) -> Result<()> {
        self.event_rows.extend(rows.iter().cloned());
        Ok(())
    }

    fn append_variant_snapshot(&mut self, rows: &[VariantSnapshotRow]) -> Result<()> {
        self.variant_snapshot_rows.extend(rows.iter().cloned());
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn load_jsonl_value_rows(path: &Path) -> Result<Vec<Value>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        rows.push(serde_json::from_str::<Value>(trimmed)?);
    }
    Ok(rows)
}

fn read_optional_json_value(path: &Path) -> Result<Option<Value>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path)?;
    if raw.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str::<Value>(&raw)?))
}

fn load_optional_json_record_with_schema(schema_name: &str, path: &Path) -> Result<Option<Value>> {
    let Some(value) = read_optional_json_value(path)? else {
        return Ok(None);
    };
    let schema = compile_schema(schema_name)?;
    if let Err(errors) = schema.validate(&value) {
        let msgs = errors.map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
        return Err(anyhow!(
            "schema validation failed ({}) {}: {}",
            schema_name,
            path.display(),
            msgs
        ));
    }
    Ok(Some(value))
}

fn trial_index_from_trial_id(trial_id: &str) -> Option<usize> {
    trial_id
        .strip_prefix("trial_")
        .and_then(|suffix| suffix.parse::<usize>().ok())
        .filter(|idx| *idx > 0)
}

struct TrialExecutor;

impl TrialExecutor {
    #[allow(clippy::too_many_arguments)]
    fn execute_slot(
        mode: ScheduleEngineMode,
        run_dir: &Path,
        run_id: &str,
        workload_type: &str,
        project_root: &Path,
        _dataset_path: &Path,
        variants: &[Variant],
        tasks: &[Value],
        schedule_idx: usize,
        slot: &TrialSlot,
        policy_config: &PolicyConfig,
        benchmark_config: &BenchmarkConfig,
        variant_runtime_profiles: &[VariantRuntimeProfile],
        behavior: &RunBehavior,
        materialize_mode: MaterializationMode,
        task_boundary_policy: &TaskBoundaryPolicy,
        trials_dir: &Path,
        _evidence_dir: &Path,
        evidence_records_path: &Path,
        task_chain_states_path: &Path,
        artifact_store: &ArtifactStore,
        trial_index: &mut usize,
        chain_states: &mut BTreeMap<String, ChainRuntimeState>,
        baseline_id: &str,
        run_sink: &mut dyn RunSink,
    ) -> Result<TrialExecutionResult> {
        let variant = &variants[slot.variant_idx];
        let variant_runtime = &variant_runtime_profiles[slot.variant_idx];
        let agent_runtime = &variant_runtime.agent_runtime;
        let agent_runtime_env = &variant_runtime.agent_runtime_env;
        let trial_experiment = &variant_runtime.experiment;
        let invocation_source = variant_runtime.invocation_source.clone();
        let configured_network_mode = variant_runtime.configured_network_mode.as_str();
        let effective_network_mode = variant_runtime.effective_network_mode.as_str();
        let task_idx = slot.task_idx;
        let task = &tasks[task_idx];
        let task_boundary = parse_task_boundary_from_dataset_task(task)?;
        let _ = task_boundary_policy;
        validate_task_boundary_workspace_materialization(&task_boundary)?;
        let repl = slot.repl_idx;
        let task_id = task_boundary
            .task_payload
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("task_{}", task_idx));
        let benchmark_grading_enabled = benchmark_config.grader.is_some()
            && task_boundary
                .task_payload
                .pointer("/grading/enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
        let effective_policy = resolve_effective_task_policy(
            policy_config,
            &benchmark_config.policy,
            &task_boundary.task_payload,
        );
        let chain_label = resolve_chain_label(
            &task_boundary.task_payload,
            &task_id,
            effective_policy.state_policy,
        );
        let chain_key = format!("{}::{}", variant.id, chain_label);
        let chain_step_index = chain_states
            .get(&chain_key)
            .map(|state| state.step_index + 1)
            .unwrap_or(0);
        let has_chain_snapshot = chain_states.contains_key(&chain_key);

        *trial_index += 1;
        let trial_id = format!("trial_{}", *trial_index);
        let trial_dir = trials_dir.join(&trial_id);
        ensure_dir(&trial_dir)?;
        write_trial_state(&trial_dir, &trial_id, "running", None, None, None)?;
        let mut trial_guard = TrialStateGuard::new(&trial_dir, &trial_id);

        let trial_paths = TrialPaths::new(&trial_dir, project_root)?;
        trial_paths.prepare(false)?;
        materialize_task_dependencies_for_trial(&task_boundary, &trial_paths)?;
        if matches!(effective_policy.state_policy, StatePolicy::IsolatePerTrial)
            || !has_chain_snapshot
        {
            materialize_workspace_base(project_root, &trial_paths, &task_boundary.workspace.base)?;
        }
        if !matches!(effective_policy.state_policy, StatePolicy::IsolatePerTrial) {
            if let Some(chain_state) = chain_states.get(&chain_key) {
                if let Some(workspace_ref) = chain_state.latest_workspace_ref.as_deref() {
                    restore_workspace_from_object_ref(
                        artifact_store,
                        workspace_ref,
                        &trial_paths.workspace,
                    )?;
                }
            }
        }

        materialize_workspace_overlays(&trial_paths, &task_boundary.workspace.overlays)?;
        let dynamic_mounts =
            resolve_workspace_aux_mounts(project_root, &task_boundary.workspace.aux_mounts)?;

        let input = build_agent_task(
            trial_experiment,
            run_id,
            &trial_id,
            variant,
            task_idx,
            repl,
            &task_boundary,
        );
        let input_bytes = serde_json::to_vec_pretty(&input)?;
        let trial_input_ref = artifact_store.put_bytes(&input_bytes)?;
        let mut bootstrap_store = BackingSqliteStore::open(run_dir)?;
        bootstrap_store.upsert_attempt_object(
            run_id,
            &trial_id,
            schedule_idx,
            0,
            "trial_input",
            &trial_input_ref,
            None,
        )?;
        let variant_digest = variant_digest(variant)?;

        let trial_metadata = json!({
            "schema_version": "trial_metadata_v1",
            "variant_digest": variant_digest,
            "ids": {
                "run_id": run_id,
                "trial_id": trial_id.as_str(),
                "variant_id": variant.id.as_str(),
                "task_id": task_id.as_str(),
                "repl_idx": repl,
                "task_index": task_idx
            },
            "runtime": {
                "integration_level": agent_runtime.integration_level.as_str(),
                "network_mode_requested": configured_network_mode,
                "network_mode_effective": effective_network_mode,
                "agent_runtime": {
                    "image": agent_runtime.image.clone(),
                    "workspace": AGENTLAB_CONTRACT_WORKSPACE_DIR,
                },
                "task_sandbox": {
                    "executor": "docker",
                    "image": task_boundary.task_image.clone(),
                    "workspace": AGENTLAB_CONTRACT_WORKSPACE_DIR
                }
            },
            "policy_merge": {
                "global_defaults": {
                    "state_policy": "isolate_per_trial",
                    "task_model": "independent",
                    "scoring_lifecycle": "predict_then_score",
                    "required_evidence_classes": []
                },
                "experiment_type_policy": {
                    "state_policy": match policy_config.state {
                        StatePolicy::IsolatePerTrial => "isolate_per_trial",
                        StatePolicy::PersistPerTask => "persist_per_task",
                        StatePolicy::Accumulate => "accumulate",
                    }
                },
                "benchmark_type_policy": {
                    "task_model": benchmark_config.policy.task_model.as_str(),
                    "scoring_lifecycle": benchmark_config.policy.scoring_lifecycle.as_str(),
                    "required_evidence_classes": benchmark_config.policy.required_evidence_classes.clone()
                },
                "task_override": task_boundary.task_payload.get("policy_override").cloned(),
                "effective": {
                    "state_policy": match effective_policy.state_policy {
                        StatePolicy::IsolatePerTrial => "isolate_per_trial",
                        StatePolicy::PersistPerTask => "persist_per_task",
                        StatePolicy::Accumulate => "accumulate",
                    },
                    "task_model": effective_policy.task_model.as_str(),
                    "scoring_lifecycle": effective_policy.scoring_lifecycle.as_str(),
                    "required_evidence_classes": effective_policy.required_evidence_classes.clone(),
                    "chain_failure_policy": effective_policy.chain_failure_policy.as_str(),
                }
            },
            "chain": {
                "chain_id": chain_key.as_str(),
                "step_index": chain_step_index
            }
        });
        atomic_write_json_pretty(&trial_dir.join("trial_metadata.json"), &trial_metadata)?;

        let io_paths = prepare_io_paths(&trial_paths, &input_bytes)?;
        stage_benchmark_trial_preflight(
            benchmark_config,
            &trial_dir,
            run_id,
            &trial_id,
            schedule_idx,
            &variant.id,
            &task_boundary.task_payload,
            Some(task_boundary.task_image.as_str()),
            &io_paths.input_host,
        )?;
        let runtime_env = build_runtime_contract_env(
            run_id,
            &input,
            &io_paths,
            resolve_trial_timeout_ms(&input),
        );
        let benchmark_prediction_path = trial_paths.out.join(BENCHMARK_PREDICTION_FILENAME);
        let benchmark_score_path = trial_paths.out.join(BENCHMARK_SCORE_FILENAME);
        let benchmark_grade_error_path = trial_paths.out.join(BENCHMARK_GRADE_ERROR_FILENAME);
        let adapter = adapter_registry_entry(&agent_runtime.adapter_ref)?;
        let trial_evidence_dir = trial_dir.join("evidence");
        ensure_dir(&trial_evidence_dir)?;

        let pre_snapshot_manifest = collect_workspace_snapshot_manifest(&trial_paths.workspace)?;
        let pre_snapshot_path = trial_evidence_dir.join("workspace_pre_snapshot.json");
        atomic_write_json_pretty(&pre_snapshot_path, &pre_snapshot_manifest)?;
        let pre_snapshot_ref = artifact_store.put_file(&pre_snapshot_path)?;

        let (chain_root_snapshot_ref, chain_root_snapshot_manifest) =
            if let Some(existing) = chain_states.get(&chain_key) {
                (
                    existing.chain_root_snapshot_ref.clone(),
                    existing.chain_root_snapshot_manifest.clone(),
                )
            } else {
                (pre_snapshot_ref.clone(), pre_snapshot_manifest.clone())
            };

        let mut status = String::new();
        let mut trial_output =
            trial_output_error_payload("result_missing", "agent did not write a result payload");
        let mut result_parse_error: Option<String> = None;
        let trial_started_at = Instant::now();
        for attempt in 0..policy_config.retry_max_attempts {
            let _ = fs::remove_file(&benchmark_prediction_path);
            let _ = fs::remove_file(&benchmark_score_path);
            let _ = fs::remove_file(&benchmark_grade_error_path);

            let run_request = AdapterRunRequest {
                runtime_experiment: trial_experiment,
                runtime: agent_runtime,
                variant_args: &variant_runtime.variant_args,
                runtime_env: &runtime_env,
                runtime_overrides_env: agent_runtime_env,
                trial_paths: &trial_paths,
                dynamic_mounts: &dynamic_mounts,
                io_paths: &io_paths,
                network_mode: effective_network_mode,
                benchmark_grader: benchmark_config.grader.as_ref(),
                benchmark_grading_enabled,
                run_id,
                task_image: Some(task_boundary.task_image.as_str()),
                agent_artifact: Some(agent_runtime.agent_artifact.as_path()),
            };
            let proc_result = adapter.run_trial(&run_request)?;
            status = proc_result.status;

            let (loaded_output, parse_error) = load_trial_output_resilient(&io_paths.output_host)?;
            trial_output = loaded_output;
            result_parse_error = parse_error;

            let outcome = trial_output
                .get("outcome")
                .and_then(|v| v.as_str())
                .unwrap_or("error");
            let is_last_attempt = attempt + 1 >= policy_config.retry_max_attempts;
            if !is_last_attempt && should_retry_outcome(outcome, &status, &policy_config.retry_on) {
                continue;
            }
            break;
        }

        let mut deferred_benchmark_prediction_records = Vec::new();
        let mut deferred_benchmark_score_records = Vec::new();
        let mut grade_error_reason: Option<String> = None;
        let mut missing_score_reason: Option<String> = None;
        if benchmark_grading_enabled {
            match load_optional_json_record_with_schema(
                "benchmark_prediction_record_v1.jsonschema",
                &benchmark_prediction_path,
            ) {
                Ok(Some(row)) => deferred_benchmark_prediction_records.push(row),
                Ok(None) => {}
                Err(err) => {
                    grade_error_reason = Some(format!("prediction_record_invalid: {}", err));
                }
            }
            match load_optional_json_record_with_schema(
                "benchmark_score_record_v1.jsonschema",
                &benchmark_score_path,
            ) {
                Ok(Some(row)) => deferred_benchmark_score_records.push(row),
                Ok(None) => {
                    missing_score_reason = Some(format!(
                        "score_record_missing: {}",
                        benchmark_score_path.display()
                    ));
                }
                Err(err) => {
                    grade_error_reason = Some(format!("score_record_invalid: {}", err));
                }
            }
            if grade_error_reason.is_none() && benchmark_grade_error_path.exists() {
                let marker_reason = fs::read_to_string(&benchmark_grade_error_path)
                    .unwrap_or_else(|_| "grade_error".to_string());
                grade_error_reason = Some(marker_reason.trim().to_string());
            }
            if grade_error_reason.is_none() {
                if let Some(reason) = missing_score_reason.take() {
                    grade_error_reason = Some(reason);
                } else if status == BENCHMARK_GRADING_POLICY_EXIT_CODE.to_string() {
                    grade_error_reason = Some("grading_policy_exit".to_string());
                }
            }
        }

        let post_snapshot_manifest = collect_workspace_snapshot_manifest(&trial_paths.workspace)?;
        let post_snapshot_path = trial_evidence_dir.join("workspace_post_snapshot.json");
        atomic_write_json_pretty(&post_snapshot_path, &post_snapshot_manifest)?;
        let post_snapshot_ref = artifact_store.put_file(&post_snapshot_path)?;

        let diff_incremental =
            diff_workspace_snapshots(&pre_snapshot_manifest, &post_snapshot_manifest);
        let diff_cumulative =
            diff_workspace_snapshots(&chain_root_snapshot_manifest, &post_snapshot_manifest);
        let patch_incremental = derive_patch_from_diff(&diff_incremental);
        let patch_cumulative = derive_patch_from_diff(&diff_cumulative);

        let diff_incremental_path = trial_evidence_dir.join("workspace_diff_incremental.json");
        let diff_cumulative_path = trial_evidence_dir.join("workspace_diff_cumulative.json");
        let patch_incremental_path = trial_evidence_dir.join("workspace_patch_incremental.json");
        let patch_cumulative_path = trial_evidence_dir.join("workspace_patch_cumulative.json");
        atomic_write_json_pretty(&diff_incremental_path, &diff_incremental)?;
        atomic_write_json_pretty(&diff_cumulative_path, &diff_cumulative)?;
        atomic_write_json_pretty(&patch_incremental_path, &patch_incremental)?;
        atomic_write_json_pretty(&patch_cumulative_path, &patch_cumulative)?;

        let diff_incremental_ref = artifact_store.put_file(&diff_incremental_path)?;
        let diff_cumulative_ref = artifact_store.put_file(&diff_cumulative_path)?;
        let patch_incremental_ref = artifact_store.put_file(&patch_incremental_path)?;
        let patch_cumulative_ref = artifact_store.put_file(&patch_cumulative_path)?;
        let workspace_bundle_ref = if workspace_diff_is_empty(&diff_incremental) {
            chain_states
                .get(&chain_key)
                .and_then(|state| state.latest_workspace_ref.clone())
        } else {
            Some(capture_workspace_object_ref(
                artifact_store,
                &trial_paths.workspace,
            )?)
        };

        if !matches!(effective_policy.state_policy, StatePolicy::IsolatePerTrial) {
            chain_states.insert(
                chain_key.clone(),
                ChainRuntimeState {
                    chain_root_snapshot_ref: chain_root_snapshot_ref.clone(),
                    chain_root_snapshot_manifest: chain_root_snapshot_manifest.clone(),
                    latest_snapshot_ref: post_snapshot_ref.clone(),
                    latest_workspace_ref: workspace_bundle_ref.clone(),
                    step_index: chain_step_index,
                },
            );
        }

        let trial_output_ref =
            artifact_store.put_bytes(&serde_json::to_vec_pretty(&trial_output)?)?;

        let stdout_path = trial_dir.join("harness_stdout.log");
        let stderr_path = trial_dir.join("harness_stderr.log");
        let stdout_ref = if stdout_path.exists() {
            Some(artifact_store.put_file(&stdout_path)?)
        } else {
            None
        };
        let stderr_ref = if stderr_path.exists() {
            Some(artifact_store.put_file(&stderr_path)?)
        } else {
            None
        };

        let hook_events_path = if io_paths.events_host.exists() {
            Some(io_paths.events_host.clone())
        } else {
            None
        };
        let hook_events_ref = if let Some(path) = hook_events_path.as_ref() {
            Some(artifact_store.put_file(path)?)
        } else {
            None
        };

        let trial_duration_ms = trial_started_at.elapsed().as_secs_f64() * 1000.0;
        let mut evidence_record = json!({
            "schema_version": "evidence_record_v1",
            "ts": Utc::now().to_rfc3339(),
            "ids": {
                "run_id": run_id,
                "trial_id": trial_id.as_str(),
                "variant_id": variant.id.as_str(),
                "task_id": task_id.as_str(),
                "repl_idx": repl
            },
            "policy": {
                "state_policy": match effective_policy.state_policy {
                    StatePolicy::IsolatePerTrial => "isolate_per_trial",
                    StatePolicy::PersistPerTask => "persist_per_task",
                    StatePolicy::Accumulate => "accumulate",
                },
                "task_model": effective_policy.task_model.as_str(),
                "chain_id": chain_key.as_str(),
                "chain_step_index": chain_step_index
            },
            "runtime": {
                "executor": "docker",
                "exit_status": status.as_str(),
                "duration_ms": trial_duration_ms
            },
            "evidence": {
                "trial_input_ref": trial_input_ref.clone(),
                "trial_output_ref": trial_output_ref.clone(),
                "stdout_ref": stdout_ref.clone(),
                "stderr_ref": stderr_ref.clone(),
                "hook_events_ref": hook_events_ref.clone(),
                "harness_request_ref": trial_input_ref.clone(),
                "harness_response_ref": trial_output_ref.clone(),
                "workspace_pre_ref": pre_snapshot_ref.clone(),
                "workspace_post_ref": post_snapshot_ref.clone(),
                "diff_incremental_ref": diff_incremental_ref.clone(),
                "diff_cumulative_ref": diff_cumulative_ref.clone(),
                "patch_incremental_ref": patch_incremental_ref.clone(),
                "patch_cumulative_ref": patch_cumulative_ref.clone(),
                "workspace_bundle_ref": workspace_bundle_ref.clone()
            }
        });

        if let Some(evidence) = evidence_record
            .get_mut("evidence")
            .and_then(Value::as_object_mut)
        {
            if stdout_ref.is_none() {
                evidence.remove("stdout_ref");
            }
            if stderr_ref.is_none() {
                evidence.remove("stderr_ref");
            }
            if hook_events_ref.is_none() {
                evidence.remove("hook_events_ref");
            }
        }
        validate_required_evidence_classes(
            &evidence_record,
            &effective_policy.required_evidence_classes,
        )?;
        append_jsonl(evidence_records_path, &evidence_record)?;

        let checkpoint_labels = trial_output
            .get("checkpoints")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        row.get("logical_name")
                            .and_then(Value::as_str)
                            .or_else(|| row.get("path").and_then(Value::as_str))
                    })
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let chain_state_record = json!({
            "schema_version": "task_chain_state_v1",
            "ts": Utc::now().to_rfc3339(),
            "run_id": run_id,
            "chain_id": chain_key.as_str(),
            "task_model": effective_policy.task_model.as_str(),
            "step_index": chain_step_index,
            "ids": {
                "trial_id": trial_id.as_str(),
                "variant_id": variant.id.as_str(),
                "task_id": task_id.as_str(),
                "repl_idx": repl
            },
            "snapshots": {
                "chain_root_ref": chain_root_snapshot_ref,
                "prev_ref": pre_snapshot_ref,
                "post_ref": post_snapshot_ref
            },
            "diffs": {
                "incremental_ref": diff_incremental_ref,
                "cumulative_ref": diff_cumulative_ref,
                "patch_incremental_ref": patch_incremental_ref,
                "patch_cumulative_ref": patch_cumulative_ref
            },
            "checkpoint_labels": checkpoint_labels,
            "ext": {
                "latest_snapshot_ref": chain_states
                    .get(&chain_key)
                    .map(|state| state.latest_snapshot_ref.clone()),
                "latest_workspace_ref": chain_states
                    .get(&chain_key)
                    .and_then(|state| state.latest_workspace_ref.clone())
            }
        });
        append_jsonl(task_chain_states_path, &chain_state_record)?;

        write_state_inventory(
            &trial_dir,
            trial_experiment,
            agent_runtime,
            &trial_paths,
            &resolve_exec_digest(&agent_runtime.command_raw, project_root)?,
            effective_network_mode,
            invocation_source.as_str(),
            Some(task_boundary.task_image.as_str()),
        )?;

        let manifest_path = resolve_agent_runtime_manifest_path(&trial_paths)?;
        if manifest_path.exists() && io_paths.events_host.exists() {
            let manifest = load_manifest(&manifest_path)?;
            let schema = compile_schema("hook_events_v1.jsonschema")?;
            let _ = validate_hooks(&manifest, &io_paths.events_host, &schema);
        }

        let benchmark_score_row = deferred_benchmark_score_records.first();
        let mut outcome = trial_output
            .get("outcome")
            .and_then(|v| v.as_str())
            .unwrap_or("error")
            .to_string();
        if benchmark_grading_enabled && grade_error_reason.is_none() {
            if let Some(mapped_outcome) = benchmark_score_row
                .and_then(|row| row.pointer("/verdict"))
                .and_then(Value::as_str)
                .and_then(benchmark_verdict_to_trial_outcome)
            {
                outcome = mapped_outcome.to_string();
            }
        }
        let mut metrics = trial_output.get("metrics").cloned().unwrap_or(json!({}));
        if let Some(obj) = metrics.as_object_mut() {
            obj.insert("status_code".to_string(), json!(status.clone()));
            if let Some(verdict) = benchmark_score_row
                .and_then(|row| row.pointer("/verdict"))
                .and_then(Value::as_str)
            {
                obj.insert("benchmark_verdict".to_string(), json!(verdict));
            }
            if let Some(reason) = grade_error_reason.as_ref() {
                obj.insert("grade_error".to_string(), json!(true));
                obj.insert("grade_error_reason".to_string(), json!(reason));
            }
        }
        let benchmark_primary = benchmark_score_row.and_then(|row| {
            let name = row
                .pointer("/primary_metric_name")
                .and_then(Value::as_str)
                .map(str::to_string)?;
            let value = row
                .pointer("/primary_metric_value")
                .cloned()
                .unwrap_or(json!(null));
            Some((name, value))
        });
        let (primary_metric_name, primary_metric_value) = if benchmark_grading_enabled
            && grade_error_reason.is_none()
        {
            if let Some((name, value)) = benchmark_primary {
                (name, value)
            } else if let Some(obj) = trial_output.get("objective").and_then(|v| v.as_object()) {
                let name = obj
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("primary_metric")
                    .to_string();
                let value = obj.get("value").cloned().unwrap_or(json!(null));
                (name, value)
            } else {
                let fallback = if outcome == "success" { 1.0 } else { 0.0 };
                ("success".to_string(), json!(fallback))
            }
        } else if let Some(obj) = trial_output.get("objective").and_then(|v| v.as_object()) {
            let name = obj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("primary_metric")
                .to_string();
            let value = obj.get("value").cloned().unwrap_or(json!(null));
            (name, value)
        } else {
            let fallback = if outcome == "success" { 1.0 } else { 0.0 };
            ("success".to_string(), json!(fallback))
        };
        let bindings = variant_bindings_for_summary(variant);
        let event_rows = if io_paths.events_host.exists() {
            load_event_rows(
                &io_paths.events_host,
                run_id,
                &trial_id,
                schedule_idx,
                &variant.id,
                &task_id,
                repl,
            )?
        } else {
            Vec::new()
        };
        let metric_rows = build_metric_rows(
            run_id,
            &trial_id,
            schedule_idx,
            &variant.id,
            &task_id,
            repl,
            &outcome,
            &metrics,
            &primary_metric_name,
            &primary_metric_value,
        );
        let variant_snapshot_rows = build_variant_snapshot_rows(
            run_id,
            &trial_id,
            schedule_idx,
            &variant.id,
            baseline_id,
            &task_id,
            repl,
            &bindings,
        );
        run_sink.append_trial_record(&TrialRecord {
            run_id: run_id.to_string(),
            trial_id: trial_id.clone(),
            schedule_idx,
            slot_commit_id: String::new(),
            attempt: 0,
            row_seq: 0,
            baseline_id: baseline_id.to_string(),
            workload_type: workload_type.to_string(),
            variant_id: variant.id.clone(),
            task_index: task_idx,
            task_id: task_id.clone(),
            repl_idx: repl,
            outcome: outcome.clone(),
            success: outcome == "success" && grade_error_reason.is_none(),
            status_code: status.clone(),
            integration_level: agent_runtime.integration_level.clone(),
            network_mode_requested: configured_network_mode.to_string(),
            network_mode_effective: effective_network_mode.to_string(),
            primary_metric_name: primary_metric_name.clone(),
            primary_metric_value: primary_metric_value.clone(),
            metrics: metrics.clone(),
            bindings: bindings.clone(),
            hook_events_total: event_rows.len(),
            has_hook_events: !event_rows.is_empty(),
        })?;
        run_sink.append_metric_rows(&metric_rows)?;
        run_sink.append_event_rows(&event_rows)?;
        run_sink.append_variant_snapshot(&variant_snapshot_rows)?;

        let failure_classification = if grade_error_reason.is_some() {
            trial_guard.complete("failed", Some("grade_error"))?;
            Some("grade_error".to_string())
        } else if status != "0" {
            trial_guard.complete("failed", Some("agent_exit_nonzero"))?;
            Some("agent_exit_nonzero".to_string())
        } else if result_parse_error.is_some() {
            trial_guard.complete("failed", Some("result_parse_error"))?;
            Some("result_parse_error".to_string())
        } else if status == "0" && outcome != "error" {
            trial_guard.complete("completed", None)?;
            None
        } else {
            trial_guard.complete("failed", Some("result_error"))?;
            Some("result_error".to_string())
        };

        materialize_trial_runtime_layout(&trial_dir, &trial_paths, materialize_mode)?;
        trial_paths.cleanup_scratch()?;

        let slot_status = if grade_error_reason.is_none() && status == "0" && outcome != "error" {
            "completed"
        } else {
            "failed"
        };
        let mut result =
            TrialExecutionResult::minimal(trial_id, slot_status, Some(slot.variant_idx));
        result.deferred_benchmark_prediction_records = deferred_benchmark_prediction_records;
        result.deferred_benchmark_score_records = deferred_benchmark_score_records;
        result.failure_classification = failure_classification;
        Ok(result)
    }
}

struct RunCoordinator;

fn slot_commit_payload_digest_for_result(
    schedule_idx: usize,
    trial_result: &TrialExecutionResult,
) -> Result<String> {
    let payload = json!({
        "schedule_idx": schedule_idx,
        "trial_id": trial_result.trial_id.clone(),
        "slot_status": trial_result.slot_status.clone(),
        "trial_rows": trial_result.deferred_trial_records.clone(),
        "metric_rows": trial_result.deferred_metric_rows.clone(),
        "event_rows": trial_result.deferred_event_rows.clone(),
        "variant_snapshot_rows": trial_result.deferred_variant_snapshot_rows.clone(),
        "evidence_rows": trial_result.deferred_evidence_records.clone(),
        "chain_state_rows": trial_result.deferred_chain_state_records.clone(),
        "benchmark_prediction_rows": trial_result.deferred_benchmark_prediction_records.clone(),
        "benchmark_score_rows": trial_result.deferred_benchmark_score_records.clone(),
    });
    Ok(canonical_json_digest(&payload))
}

fn annotate_row_identity(
    value: &mut Value,
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
    row_seq: usize,
) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    obj.insert("schedule_idx".to_string(), json!(schedule_idx));
    obj.insert("slot_commit_id".to_string(), json!(slot_commit_id));
    obj.insert("attempt".to_string(), json!(attempt));
    obj.insert("row_seq".to_string(), json!(row_seq));
}

fn annotate_value_rows(
    rows: &[Value],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<Value> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            annotate_row_identity(&mut next, schedule_idx, slot_commit_id, attempt, row_seq);
            next
        })
        .collect()
}

fn annotate_trial_rows(
    rows: &[TrialRecord],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<TrialRecord> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            next.schedule_idx = schedule_idx;
            next.slot_commit_id = slot_commit_id.to_string();
            next.attempt = attempt;
            next.row_seq = row_seq;
            next
        })
        .collect()
}

fn annotate_metric_rows(
    rows: &[MetricRow],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<MetricRow> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            next.schedule_idx = schedule_idx;
            next.slot_commit_id = slot_commit_id.to_string();
            next.attempt = attempt;
            next.row_seq = row_seq;
            next
        })
        .collect()
}

fn annotate_event_rows(
    rows: &[EventRow],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<EventRow> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            next.schedule_idx = schedule_idx;
            next.slot_commit_id = slot_commit_id.to_string();
            next.attempt = attempt;
            next.row_seq = row_seq;
            next
        })
        .collect()
}

fn annotate_variant_snapshot_rows(
    rows: &[VariantSnapshotRow],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<VariantSnapshotRow> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            next.schedule_idx = schedule_idx;
            next.slot_commit_id = slot_commit_id.to_string();
            next.attempt = attempt;
            next.row_seq = row_seq;
            next
        })
        .collect()
}

impl RunCoordinator {
    fn commit_skipped_pruned_slot(
        run_dir: &Path,
        schedule_progress: &mut ScheduleProgress,
        schedule_idx: usize,
        run_sink: &mut dyn RunSink,
        slot_attempts: &mut HashMap<usize, usize>,
    ) -> Result<()> {
        let attempt = slot_attempts.get(&schedule_idx).copied().unwrap_or(0) + 1;
        let payload_digest = canonical_json_digest(&json!({
            "schedule_idx": schedule_idx,
            "status": "skipped_pruned"
        }));
        let slot_commit_id = make_slot_commit_id(
            &schedule_progress.run_id,
            schedule_idx,
            attempt,
            &payload_digest,
        );
        let empty_counts = SlotCommitRowCounts {
            trials: 0,
            metrics: 0,
            events: 0,
            variant_snapshots: 0,
            evidence: 0,
            chain_states: 0,
            predictions: 0,
            scores: 0,
        };
        append_slot_commit_record(
            run_dir,
            &SlotCommitRecord {
                schema_version: "slot_commit_record_v1".to_string(),
                record_type: "intent".to_string(),
                run_id: schedule_progress.run_id.clone(),
                schedule_idx,
                slot_commit_id: slot_commit_id.clone(),
                trial_id: String::new(),
                slot_status: "skipped_pruned".to_string(),
                attempt,
                recorded_at: Utc::now().to_rfc3339(),
                expected_rows: Some(empty_counts.clone()),
                payload_digest: Some(payload_digest),
                written_rows: None,
                facts_fsync_completed: None,
                runtime_fsync_completed: None,
            },
        )?;
        run_sink.flush()?;
        append_slot_commit_record(
            run_dir,
            &SlotCommitRecord {
                schema_version: "slot_commit_record_v1".to_string(),
                record_type: "commit".to_string(),
                run_id: schedule_progress.run_id.clone(),
                schedule_idx,
                slot_commit_id: slot_commit_id.clone(),
                trial_id: String::new(),
                slot_status: "skipped_pruned".to_string(),
                attempt,
                recorded_at: Utc::now().to_rfc3339(),
                expected_rows: None,
                payload_digest: None,
                written_rows: Some(empty_counts),
                facts_fsync_completed: Some(true),
                runtime_fsync_completed: Some(true),
            },
        )?;

        let mut next_progress = schedule_progress.clone();
        next_progress.completed_slots.push(SlotCompletion {
            schedule_index: schedule_idx,
            trial_id: String::new(),
            status: "skipped_pruned".to_string(),
            slot_commit_id,
            attempt,
        });
        next_progress.next_schedule_index = schedule_idx + 1;
        next_progress.updated_at = Utc::now().to_rfc3339();
        write_schedule_progress(run_dir, &next_progress)?;
        *schedule_progress = next_progress;
        emit_slot_commit_progress(
            &schedule_progress.run_id,
            schedule_progress.next_schedule_index,
            schedule_progress.total_slots,
            schedule_idx,
            "-",
            "skipped_pruned",
        );
        slot_attempts.insert(schedule_idx, attempt);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_trial_slot(
        run_dir: &Path,
        policy_config: &PolicyConfig,
        evidence_records_path: &Path,
        task_chain_states_path: &Path,
        benchmark_predictions_path: &Path,
        benchmark_scores_path: &Path,
        schedule_progress: &mut ScheduleProgress,
        schedule_idx: usize,
        trial_index: usize,
        pruned_variants: &mut HashSet<usize>,
        consecutive_failures: &mut BTreeMap<usize, usize>,
        trial_result: &TrialExecutionResult,
        run_sink: &mut dyn RunSink,
        slot_attempts: &mut HashMap<usize, usize>,
    ) -> Result<()> {
        let attempt = slot_attempts.get(&schedule_idx).copied().unwrap_or(0) + 1;
        let payload_digest = slot_commit_payload_digest_for_result(schedule_idx, trial_result)?;
        let slot_commit_id = make_slot_commit_id(
            &schedule_progress.run_id,
            schedule_idx,
            attempt,
            &payload_digest,
        );
        let expected_rows = SlotCommitRowCounts {
            trials: trial_result.deferred_trial_records.len(),
            metrics: trial_result.deferred_metric_rows.len(),
            events: trial_result.deferred_event_rows.len(),
            variant_snapshots: trial_result.deferred_variant_snapshot_rows.len(),
            evidence: trial_result.deferred_evidence_records.len(),
            chain_states: trial_result.deferred_chain_state_records.len(),
            predictions: trial_result.deferred_benchmark_prediction_records.len(),
            scores: trial_result.deferred_benchmark_score_records.len(),
        };
        append_slot_commit_record(
            run_dir,
            &SlotCommitRecord {
                schema_version: "slot_commit_record_v1".to_string(),
                record_type: "intent".to_string(),
                run_id: schedule_progress.run_id.clone(),
                schedule_idx,
                slot_commit_id: slot_commit_id.clone(),
                trial_id: trial_result.trial_id.clone(),
                slot_status: trial_result.slot_status.clone(),
                attempt,
                recorded_at: Utc::now().to_rfc3339(),
                expected_rows: Some(expected_rows.clone()),
                payload_digest: Some(payload_digest),
                written_rows: None,
                facts_fsync_completed: None,
                runtime_fsync_completed: None,
            },
        )?;

        let evidence_rows = annotate_value_rows(
            &trial_result.deferred_evidence_records,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for record in &evidence_rows {
            append_jsonl(evidence_records_path, record)?;
        }
        let chain_rows = annotate_value_rows(
            &trial_result.deferred_chain_state_records,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for record in &chain_rows {
            append_jsonl(task_chain_states_path, record)?;
        }
        let prediction_rows = annotate_value_rows(
            &trial_result.deferred_benchmark_prediction_records,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for row in &prediction_rows {
            append_jsonl(benchmark_predictions_path, row)?;
        }
        let score_rows = annotate_value_rows(
            &trial_result.deferred_benchmark_score_records,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for row in &score_rows {
            append_jsonl(benchmark_scores_path, row)?;
        }
        let trial_rows = annotate_trial_rows(
            &trial_result.deferred_trial_records,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for row in &trial_rows {
            run_sink.append_trial_record(row)?;
        }
        let metric_rows = annotate_metric_rows(
            &trial_result.deferred_metric_rows,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        let event_rows = annotate_event_rows(
            &trial_result.deferred_event_rows,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        let snapshot_rows = annotate_variant_snapshot_rows(
            &trial_result.deferred_variant_snapshot_rows,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        run_sink.append_metric_rows(&metric_rows)?;
        run_sink.append_event_rows(&event_rows)?;
        run_sink.append_variant_snapshot(&snapshot_rows)?;
        run_sink.flush()?;
        append_slot_commit_record(
            run_dir,
            &SlotCommitRecord {
                schema_version: "slot_commit_record_v1".to_string(),
                record_type: "commit".to_string(),
                run_id: schedule_progress.run_id.clone(),
                schedule_idx,
                slot_commit_id: slot_commit_id.clone(),
                trial_id: trial_result.trial_id.clone(),
                slot_status: trial_result.slot_status.clone(),
                attempt,
                recorded_at: Utc::now().to_rfc3339(),
                expected_rows: None,
                payload_digest: None,
                written_rows: Some(expected_rows),
                facts_fsync_completed: Some(true),
                runtime_fsync_completed: Some(true),
            },
        )?;

        let mut next_consecutive_failures = consecutive_failures.clone();
        let mut next_pruned_variants = pruned_variants.clone();
        if let Some(variant_idx) = trial_result.variant_idx {
            if trial_result.slot_status == "completed" {
                next_consecutive_failures.insert(variant_idx, 0);
            } else {
                *next_consecutive_failures.entry(variant_idx).or_default() += 1;
            }
            if let Some(max_failures) = policy_config.pruning_max_consecutive_failures {
                let count = next_consecutive_failures
                    .get(&variant_idx)
                    .copied()
                    .unwrap_or(0);
                if count >= max_failures {
                    next_pruned_variants.insert(variant_idx);
                }
            }
        }

        let mut next_progress = schedule_progress.clone();
        next_progress.completed_slots.push(SlotCompletion {
            schedule_index: schedule_idx,
            trial_id: trial_result.trial_id.clone(),
            status: trial_result.slot_status.clone(),
            slot_commit_id,
            attempt,
        });
        next_progress.next_schedule_index = schedule_idx + 1;
        next_progress.next_trial_index = trial_index;
        next_progress.pruned_variants = next_pruned_variants.iter().copied().collect();
        next_progress.consecutive_failures = next_consecutive_failures.clone();
        next_progress.updated_at = Utc::now().to_rfc3339();
        write_schedule_progress(run_dir, &next_progress)?;

        *schedule_progress = next_progress;
        emit_slot_commit_progress(
            &schedule_progress.run_id,
            schedule_progress.next_schedule_index,
            schedule_progress.total_slots,
            schedule_idx,
            &trial_result.trial_id,
            &trial_result.slot_status,
        );
        *consecutive_failures = next_consecutive_failures;
        *pruned_variants = next_pruned_variants;
        slot_attempts.insert(schedule_idx, attempt);
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum PendingSlotCommit {
    SkippedPruned,
    Trial(Box<TrialExecutionResult>),
}

struct DeterministicCommitter {
    next_commit_idx: usize,
    committed_keys: HashSet<String>,
    pending_by_schedule: BTreeMap<usize, PendingSlotCommit>,
    slot_attempts: HashMap<usize, usize>,
}

impl DeterministicCommitter {
    fn from_progress(progress: &ScheduleProgress, journal_records: &[SlotCommitRecord]) -> Self {
        let mut committed_keys = HashSet::new();
        let mut slot_attempts = highest_attempt_by_schedule(journal_records);
        for slot in &progress.completed_slots {
            committed_keys.insert(Self::commit_key_for_slot_completion(slot));
            let entry = slot_attempts.entry(slot.schedule_index).or_insert(0);
            if slot.attempt > *entry {
                *entry = slot.attempt;
            }
        }
        Self {
            next_commit_idx: progress.next_schedule_index,
            committed_keys,
            pending_by_schedule: BTreeMap::new(),
            slot_attempts,
        }
    }

    fn commit_key_for_slot_completion(slot: &SlotCompletion) -> String {
        format!("{}:{}:{}", slot.schedule_index, slot.trial_id, slot.status)
    }

    fn commit_key_for_pending(schedule_idx: usize, pending: &PendingSlotCommit) -> String {
        match pending {
            PendingSlotCommit::SkippedPruned => {
                format!("{}::skipped_pruned", schedule_idx)
            }
            PendingSlotCommit::Trial(result) => {
                format!(
                    "{}:{}:{}",
                    schedule_idx, result.trial_id, result.slot_status
                )
            }
        }
    }

    fn enqueue_skipped(&mut self, schedule_idx: usize) -> Result<bool> {
        self.enqueue(schedule_idx, PendingSlotCommit::SkippedPruned)
    }

    fn enqueue_trial(&mut self, schedule_idx: usize, result: TrialExecutionResult) -> Result<bool> {
        self.enqueue(schedule_idx, PendingSlotCommit::Trial(Box::new(result)))
    }

    fn enqueue(&mut self, schedule_idx: usize, pending: PendingSlotCommit) -> Result<bool> {
        let pending_key = Self::commit_key_for_pending(schedule_idx, &pending);
        if self.committed_keys.contains(&pending_key) {
            return Ok(false);
        }
        if schedule_idx < self.next_commit_idx {
            return Err(anyhow!(
                "deterministic committer protocol fault: stale completion schedule_idx {} already committed through {}",
                schedule_idx,
                self.next_commit_idx.saturating_sub(1)
            ));
        }
        if let Some(existing) = self.pending_by_schedule.get(&schedule_idx) {
            let existing_key = Self::commit_key_for_pending(schedule_idx, existing);
            if existing_key == pending_key {
                return Ok(false);
            }
            return Err(anyhow!(
                "deterministic committer protocol fault: conflicting pending completion for schedule_idx {}",
                schedule_idx
            ));
        }
        self.pending_by_schedule.insert(schedule_idx, pending);
        Ok(true)
    }

    fn pending_trial_completion_records(&self) -> Vec<PendingTrialCompletionRecord> {
        let mut out = Vec::new();
        for (schedule_idx, pending) in &self.pending_by_schedule {
            if let PendingSlotCommit::Trial(result) = pending {
                out.push(PendingTrialCompletionRecord {
                    schema_version: "pending_trial_completion_v1".to_string(),
                    schedule_idx: *schedule_idx,
                    trial_result: (**result).clone(),
                });
            }
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn drain_ready(
        &mut self,
        run_dir: &Path,
        policy_config: &PolicyConfig,
        evidence_records_path: &Path,
        task_chain_states_path: &Path,
        benchmark_predictions_path: &Path,
        benchmark_scores_path: &Path,
        schedule_progress: &mut ScheduleProgress,
        trial_index: usize,
        pruned_variants: &mut HashSet<usize>,
        consecutive_failures: &mut BTreeMap<usize, usize>,
        run_sink: &mut dyn RunSink,
    ) -> Result<usize> {
        let mut committed = 0_usize;
        while let Some(pending) = self.pending_by_schedule.remove(&self.next_commit_idx) {
            let schedule_idx = self.next_commit_idx;
            let commit_key = Self::commit_key_for_pending(schedule_idx, &pending);
            match pending {
                PendingSlotCommit::SkippedPruned => {
                    RunCoordinator::commit_skipped_pruned_slot(
                        run_dir,
                        schedule_progress,
                        schedule_idx,
                        run_sink,
                        &mut self.slot_attempts,
                    )?;
                }
                PendingSlotCommit::Trial(result) => {
                    RunCoordinator::commit_trial_slot(
                        run_dir,
                        policy_config,
                        evidence_records_path,
                        task_chain_states_path,
                        benchmark_predictions_path,
                        benchmark_scores_path,
                        schedule_progress,
                        schedule_idx,
                        trial_index,
                        pruned_variants,
                        consecutive_failures,
                        &result,
                        run_sink,
                        &mut self.slot_attempts,
                    )?;
                }
            }
            self.committed_keys.insert(commit_key);
            self.next_commit_idx = schedule_progress.next_schedule_index;
            committed += 1;
        }
        Ok(committed)
    }
}

#[derive(Clone)]
struct ParallelWorkerExecutionContext {
    mode: ScheduleEngineMode,
    run_dir: PathBuf,
    run_id: String,
    workload_type: String,
    project_root: PathBuf,
    dataset_path: PathBuf,
    variants: Vec<Variant>,
    tasks: Vec<Value>,
    policy_config: PolicyConfig,
    benchmark_config: BenchmarkConfig,
    variant_runtime_profiles: Vec<VariantRuntimeProfile>,
    behavior: RunBehavior,
    materialize_mode: MaterializationMode,
    task_boundary_policy: TaskBoundaryPolicy,
    trials_dir: PathBuf,
    evidence_dir: PathBuf,
    baseline_id: String,
}

#[derive(Debug, Clone)]
struct InFlightDispatch {
    schedule_idx: usize,
    trial_id: String,
    variant_idx: usize,
    variant_id: String,
    worker_id: String,
    started_at: String,
}

fn in_flight_active_trials(
    in_flight: &HashMap<String, InFlightDispatch>,
) -> Vec<RunControlActiveTrial> {
    let mut active: Vec<RunControlActiveTrial> = in_flight
        .values()
        .map(|item| RunControlActiveTrial {
            trial_id: item.trial_id.clone(),
            worker_id: item.worker_id.clone(),
            schedule_idx: Some(item.schedule_idx),
            variant_id: Some(item.variant_id.clone()),
            started_at: Some(item.started_at.clone()),
            control: None,
        })
        .collect();
    active.sort_by_key(|entry| entry.schedule_idx.unwrap_or(usize::MAX));
    active
}

fn remove_in_flight_tickets(
    in_flight: &mut HashMap<String, InFlightDispatch>,
    in_flight_by_variant: &mut BTreeMap<usize, usize>,
    ticket_ids: &HashSet<String>,
) {
    for ticket_id in ticket_ids {
        if let Some(removed) = in_flight.remove(ticket_id.as_str()) {
            if let Some(count) = in_flight_by_variant.get_mut(&removed.variant_idx) {
                if *count > 0 {
                    *count -= 1;
                }
                if *count == 0 {
                    in_flight_by_variant.remove(&removed.variant_idx);
                }
            }
        }
    }
}

fn process_parallel_worker_control_request(
    run_dir: &Path,
    run_id: &str,
    backend: &dyn WorkerBackend,
    in_flight: &mut HashMap<String, InFlightDispatch>,
    in_flight_by_variant: &mut BTreeMap<usize, usize>,
) -> Result<Option<ScheduleEngineOutcome>> {
    let Some(request) = load_pending_parallel_worker_control_request(run_dir)? else {
        return Ok(None);
    };

    let mut target_trial_ids = if request.target_trial_ids.is_empty() {
        in_flight
            .values()
            .map(|entry| entry.trial_id.clone())
            .collect::<Vec<_>>()
    } else {
        request.target_trial_ids.clone()
    };
    target_trial_ids.sort();
    target_trial_ids.dedup();

    let mut processed_trial_ids: Vec<String> = Vec::new();
    let mut failed_trials: Vec<String> = Vec::new();
    let mut removed_ticket_ids: HashSet<String> = HashSet::new();

    match request.action {
        ParallelWorkerControlAction::Pause => {
            let pause_label = request.label.as_deref().unwrap_or("pause");
            let mut paused_active_trials: Vec<RunControlActiveTrial> = Vec::new();
            let mut checkpoint_acked_all = true;
            let mut stop_acked_all = true;
            if target_trial_ids.is_empty() {
                failed_trials.push("pause_no_active_trial".to_string());
            }

            for trial_id in &target_trial_ids {
                let maybe_dispatch = in_flight.iter().find_map(|(ticket_id, dispatch)| {
                    if dispatch.trial_id == *trial_id {
                        Some((ticket_id.clone(), dispatch.clone()))
                    } else {
                        None
                    }
                });
                let Some((ticket_id, dispatch)) = maybe_dispatch else {
                    failed_trials.push(format!("{}: pause_target_not_active", trial_id));
                    continue;
                };

                let pause_ack = match backend.request_pause(&dispatch.worker_id, pause_label) {
                    Ok(ack) => ack,
                    Err(err) => {
                        failed_trials.push(format!("{}: pause request failed ({})", trial_id, err));
                        continue;
                    }
                };
                checkpoint_acked_all &= pause_ack.accepted;
                if let Err(err) = backend.request_stop(
                    &dispatch.worker_id,
                    format!("pause:{}", pause_label).as_str(),
                ) {
                    failed_trials
                        .push(format!("{}: pause stop request failed ({})", trial_id, err));
                    stop_acked_all = false;
                    continue;
                }

                let trial_dir = run_dir.join("trials").join(trial_id);
                if let Err(err) = write_trial_state(
                    &trial_dir,
                    trial_id,
                    "paused",
                    Some(pause_label),
                    Some(pause_label),
                    Some("paused_by_user"),
                ) {
                    failed_trials.push(format!(
                        "{}: failed to write trial_state ({})",
                        trial_id, err
                    ));
                    stop_acked_all = false;
                    continue;
                }

                paused_active_trials.push(RunControlActiveTrial {
                    trial_id: dispatch.trial_id.clone(),
                    worker_id: dispatch.worker_id.clone(),
                    schedule_idx: Some(dispatch.schedule_idx),
                    variant_id: Some(dispatch.variant_id.clone()),
                    started_at: Some(dispatch.started_at.clone()),
                    control: None,
                });
                removed_ticket_ids.insert(ticket_id);
                processed_trial_ids.push(trial_id.clone());
            }

            remove_in_flight_tickets(in_flight, in_flight_by_variant, &removed_ticket_ids);
            let pause_meta = RunControlPauseMetadata {
                label: pause_label.to_string(),
                requested_at: Utc::now().to_rfc3339(),
                requested_by: Some("user".to_string()),
            };
            if failed_trials.is_empty() {
                write_run_control_v2(
                    run_dir,
                    run_id,
                    "paused",
                    &paused_active_trials,
                    Some(&pause_meta),
                )?;
                write_parallel_worker_control_response(
                    run_dir,
                    ParallelWorkerControlResponse {
                        request_id: request.request_id,
                        action: ParallelWorkerControlAction::Pause,
                        status: PARALLEL_WORKER_CONTROL_RESPONSE_COMPLETED.to_string(),
                        processed_at: Utc::now().to_rfc3339(),
                        processed_trial_ids,
                        failed_trials: Vec::new(),
                        checkpoint_acked: Some(checkpoint_acked_all),
                        stop_acked: Some(stop_acked_all),
                        message: None,
                    },
                )?;
                return Ok(Some(ScheduleEngineOutcome::Paused));
            }

            let survivors = in_flight_active_trials(in_flight);
            write_run_control_v2(
                run_dir,
                run_id,
                "interrupted",
                &survivors,
                Some(&pause_meta),
            )?;
            let message = format!(
                "pause request failed for {} of {} targeted trial(s): {}",
                failed_trials.len(),
                target_trial_ids.len(),
                failed_trials.join(" | ")
            );
            write_parallel_worker_control_response(
                run_dir,
                ParallelWorkerControlResponse {
                    request_id: request.request_id,
                    action: ParallelWorkerControlAction::Pause,
                    status: PARALLEL_WORKER_CONTROL_RESPONSE_FAILED.to_string(),
                    processed_at: Utc::now().to_rfc3339(),
                    processed_trial_ids,
                    failed_trials,
                    checkpoint_acked: Some(checkpoint_acked_all),
                    stop_acked: Some(stop_acked_all),
                    message: Some(message),
                },
            )?;
            Ok(Some(ScheduleEngineOutcome::Interrupted))
        }
        ParallelWorkerControlAction::Stop => {
            let stop_reason = request.reason.as_deref().unwrap_or("killed_by_user");

            for trial_id in &target_trial_ids {
                let maybe_dispatch = in_flight.iter().find_map(|(ticket_id, dispatch)| {
                    if dispatch.trial_id == *trial_id {
                        Some((ticket_id.clone(), dispatch.clone()))
                    } else {
                        None
                    }
                });
                let Some((ticket_id, dispatch)) = maybe_dispatch else {
                    failed_trials.push(format!("{}: kill_target_not_active", trial_id));
                    continue;
                };

                if let Err(err) = backend.request_stop(&dispatch.worker_id, stop_reason) {
                    failed_trials.push(format!("{}: stop request failed ({})", trial_id, err));
                    continue;
                }

                let trial_dir = run_dir.join("trials").join(trial_id);
                if let Err(err) = write_trial_state(
                    &trial_dir,
                    trial_id,
                    "killed",
                    None,
                    None,
                    Some("killed_by_user"),
                ) {
                    failed_trials.push(format!(
                        "{}: failed to write trial_state ({})",
                        trial_id, err
                    ));
                    continue;
                }
                removed_ticket_ids.insert(ticket_id);
                processed_trial_ids.push(trial_id.clone());
            }

            remove_in_flight_tickets(in_flight, in_flight_by_variant, &removed_ticket_ids);
            if failed_trials.is_empty() {
                write_run_control_v2(run_dir, run_id, "killed", &[], None)?;
                write_parallel_worker_control_response(
                    run_dir,
                    ParallelWorkerControlResponse {
                        request_id: request.request_id,
                        action: ParallelWorkerControlAction::Stop,
                        status: PARALLEL_WORKER_CONTROL_RESPONSE_COMPLETED.to_string(),
                        processed_at: Utc::now().to_rfc3339(),
                        processed_trial_ids,
                        failed_trials: Vec::new(),
                        checkpoint_acked: None,
                        stop_acked: Some(true),
                        message: None,
                    },
                )?;
                return Ok(Some(ScheduleEngineOutcome::Killed));
            }

            let survivors = in_flight_active_trials(in_flight);
            write_run_control_v2(run_dir, run_id, "interrupted", &survivors, None)?;
            let message = format!(
                "stop request failed for {} of {} targeted trial(s): {}",
                failed_trials.len(),
                target_trial_ids.len(),
                failed_trials.join(" | ")
            );
            write_parallel_worker_control_response(
                run_dir,
                ParallelWorkerControlResponse {
                    request_id: request.request_id,
                    action: ParallelWorkerControlAction::Stop,
                    status: PARALLEL_WORKER_CONTROL_RESPONSE_FAILED.to_string(),
                    processed_at: Utc::now().to_rfc3339(),
                    processed_trial_ids,
                    failed_trials,
                    checkpoint_acked: None,
                    stop_acked: Some(false),
                    message: Some(message),
                },
            )?;
            Ok(Some(ScheduleEngineOutcome::Interrupted))
        }
    }
}

fn decode_parallel_completion_result(
    completion: &TrialCompletion,
    in_flight: &InFlightDispatch,
) -> Result<TrialExecutionResult> {
    if completion.classification == "trial_execution_result" {
        let mut result: TrialExecutionResult = serde_json::from_value(completion.artifacts.clone())
            .map_err(|err| {
                anyhow!(
                    "parallel worker completion decode failed for ticket {}: {}",
                    completion.ticket.ticket_id,
                    err
                )
            })?;
        if result.trial_id != in_flight.trial_id {
            return Err(anyhow!(
                "parallel worker completion trial_id mismatch: expected {}, got {}",
                in_flight.trial_id,
                result.trial_id
            ));
        }
        if result.variant_idx.is_none() {
            result.variant_idx = Some(in_flight.variant_idx);
        }
        return Ok(result);
    }
    if completion.classification == "local_worker_error" {
        let detail = completion
            .artifacts
            .pointer("/error")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("unknown local worker error");
        return Err(anyhow!(
            "local worker trial execution failed (trial_id={}, schedule_idx={}): {}",
            in_flight.trial_id,
            in_flight.schedule_idx,
            detail
        ));
    }
    Ok(TrialExecutionResult::worker_lost(
        in_flight.trial_id.clone(),
        Some(in_flight.variant_idx),
        Some(completion.classification.clone()),
    ))
}

fn is_worker_backend_capacity_error(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.starts_with(LOCAL_WORKER_CAPACITY_ERROR_PREFIX) || message.contains("at capacity")
}

fn submit_dispatch_with_backpressure(
    backend: &dyn WorkerBackend,
    dispatch: TrialDispatch,
) -> Result<Option<WorkerTicket>> {
    match backend.submit(dispatch) {
        Ok(ticket) => Ok(Some(ticket)),
        Err(err) if is_worker_backend_capacity_error(&err) => Ok(None),
        Err(err) => Err(err),
    }
}

fn execute_parallel_worker_trial(
    context: &ParallelWorkerExecutionContext,
    dispatch: TrialDispatch,
) -> Result<TrialCompletion> {
    let payload_dir = context
        .run_dir
        .join("runtime")
        .join("worker_payload")
        .join(&dispatch.trial_id);
    if payload_dir.exists() {
        fs::remove_dir_all(&payload_dir)?;
    }
    ensure_dir(&payload_dir)?;
    let payload_evidence = payload_dir.join("evidence_records.jsonl");
    let payload_chain = payload_dir.join("task_chain_states.jsonl");

    let mut local_trial_index = trial_index_from_trial_id(&dispatch.trial_id)
        .unwrap_or(dispatch.schedule_idx + 1)
        .saturating_sub(1);
    let mut local_chain_states: BTreeMap<String, ChainRuntimeState> = BTreeMap::new();
    let mut buffered_sink = BufferedRunSink::default();
    let artifact_store = ArtifactStore::new(context.run_dir.join("artifacts"));
    let execution = (|| -> Result<TrialCompletion> {
        let mut trial_result = TrialExecutor::execute_slot(
            context.mode,
            &context.run_dir,
            &context.run_id,
            &context.workload_type,
            &context.project_root,
            &context.dataset_path,
            &context.variants,
            &context.tasks,
            dispatch.schedule_idx,
            &dispatch.slot,
            &context.policy_config,
            &context.benchmark_config,
            &context.variant_runtime_profiles,
            &context.behavior,
            context.materialize_mode,
            &context.task_boundary_policy,
            &context.trials_dir,
            &context.evidence_dir,
            &payload_evidence,
            &payload_chain,
            &artifact_store,
            &mut local_trial_index,
            &mut local_chain_states,
            &context.baseline_id,
            &mut buffered_sink,
        )?;
        trial_result.variant_idx = Some(dispatch.slot.variant_idx);
        trial_result.deferred_trial_records = buffered_sink.trial_records;
        trial_result.deferred_metric_rows = buffered_sink.metric_rows;
        trial_result.deferred_event_rows = buffered_sink.event_rows;
        trial_result.deferred_variant_snapshot_rows = buffered_sink.variant_snapshot_rows;
        trial_result.deferred_evidence_records = load_jsonl_value_rows(&payload_evidence)?;
        trial_result.deferred_chain_state_records = load_jsonl_value_rows(&payload_chain)?;

        Ok(TrialCompletion {
            ticket: WorkerTicket {
                worker_id: String::new(),
                ticket_id: String::new(),
                trial_id: dispatch.trial_id.clone(),
            },
            schedule_idx: dispatch.schedule_idx,
            completion_seq: None,
            terminal_status: trial_result.slot_status.clone(),
            classification: "trial_execution_result".to_string(),
            artifacts: serde_json::to_value(trial_result)?,
            metrics: json!({}),
            runtime_summary: json!({}),
        })
    })();

    // Always attempt to cleanup worker payload materialization, including error paths.
    let _ = fs::remove_dir_all(&payload_dir);
    execution
}

#[allow(clippy::too_many_arguments)]
fn execute_schedule_engine_parallel(
    mode: ScheduleEngineMode,
    run_dir: &Path,
    run_id: &str,
    workload_type: &str,
    project_root: &Path,
    dataset_path: &Path,
    variants: &[Variant],
    tasks: &[Value],
    schedule: &[TrialSlot],
    policy_config: &PolicyConfig,
    benchmark_config: &BenchmarkConfig,
    variant_runtime_profiles: &[VariantRuntimeProfile],
    behavior: &RunBehavior,
    materialize_mode: MaterializationMode,
    task_boundary_policy: &TaskBoundaryPolicy,
    trials_dir: &Path,
    evidence_dir: &Path,
    evidence_records_path: &Path,
    task_chain_states_path: &Path,
    schedule_progress: &mut ScheduleProgress,
    trial_index: &mut usize,
    consecutive_failures: &mut BTreeMap<usize, usize>,
    pruned_variants: &mut HashSet<usize>,
    recovered_active_trials: &[RunControlActiveTrial],
    baseline_id: &str,
    run_sink: &mut dyn RunSink,
    max_concurrency: usize,
) -> Result<ScheduleEngineOutcome> {
    let benchmark_dir = run_dir.join("benchmark");
    let benchmark_predictions_path = benchmark_dir.join("predictions.jsonl");
    let benchmark_scores_path = benchmark_dir.join("scores.jsonl");

    let requested_dispatch_capacity = max_concurrency.max(1);
    let worker_context = Arc::new(ParallelWorkerExecutionContext {
        mode,
        run_dir: run_dir.to_path_buf(),
        run_id: run_id.to_string(),
        workload_type: workload_type.to_string(),
        project_root: project_root.to_path_buf(),
        dataset_path: dataset_path.to_path_buf(),
        variants: variants.to_vec(),
        tasks: tasks.to_vec(),
        policy_config: policy_config.clone(),
        benchmark_config: benchmark_config.clone(),
        variant_runtime_profiles: variant_runtime_profiles.to_vec(),
        behavior: behavior.clone(),
        materialize_mode,
        task_boundary_policy: task_boundary_policy.clone(),
        trials_dir: trials_dir.to_path_buf(),
        evidence_dir: evidence_dir.to_path_buf(),
        baseline_id: baseline_id.to_string(),
    });
    let executor_context = worker_context.clone();
    let executor: Arc<LocalTrialExecutor> = Arc::new(move |dispatch: TrialDispatch| {
        execute_parallel_worker_trial(executor_context.as_ref(), dispatch)
    });
    let local_backend = LocalThreadWorkerBackend::new(requested_dispatch_capacity, executor)?;
    if let Some(warning) = local_backend.capacity_warning() {
        eprintln!("{}", warning);
    }
    let dispatch_capacity = local_backend.effective_max_in_flight();
    let backend: Box<dyn WorkerBackend> = Box::new(local_backend);
    let min_free_bytes = resolve_min_free_bytes()?;
    let max_run_bytes = parse_max_run_bytes_from_env()?;
    let disk_check_interval = Duration::from_secs(RUNTIME_DISK_HEADROOM_CHECK_INTERVAL_SECONDS);
    let run_size_check_interval = Duration::from_secs(RUNTIME_RUN_SIZE_CHECK_INTERVAL_SECONDS);
    let mut last_disk_check = Instant::now() - disk_check_interval;
    let mut last_run_size_check = Instant::now() - run_size_check_interval;

    let journal_records = load_slot_commit_records(run_dir)?;
    let mut committer = DeterministicCommitter::from_progress(schedule_progress, &journal_records);
    let persisted_pending = load_pending_trial_completion_records(run_dir)?;
    for (schedule_idx, result) in &persisted_pending {
        if *schedule_idx < schedule_progress.next_schedule_index || *schedule_idx >= schedule.len()
        {
            continue;
        }
        committer.enqueue_trial(*schedule_idx, result.clone())?;
    }
    if !recovered_active_trials.is_empty() {
        let mut variant_idx_by_id: HashMap<String, usize> = HashMap::new();
        for (idx, variant) in variants.iter().enumerate() {
            variant_idx_by_id.insert(variant.id.clone(), idx);
        }
        for recovered in recovered_active_trials {
            let Some(schedule_idx) = recovered.schedule_idx else {
                continue;
            };
            if schedule_idx < schedule_progress.next_schedule_index
                || schedule_idx >= schedule.len()
            {
                continue;
            }
            // If we already persisted a completion for this slot, prefer it over recovered worker_lost.
            if persisted_pending.contains_key(&schedule_idx) {
                continue;
            }
            let variant_idx = recovered
                .variant_id
                .as_ref()
                .and_then(|id| variant_idx_by_id.get(id).copied());
            let result = TrialExecutionResult::worker_lost(
                recovered.trial_id.clone(),
                variant_idx,
                Some("worker_lost".to_string()),
            );
            committer.enqueue_trial(schedule_idx, result)?;
        }
    }
    persist_pending_trial_completions(run_dir, &committer)?;

    let mut next_dispatch_idx = schedule_progress.next_schedule_index;
    let mut in_flight: HashMap<String, InFlightDispatch> = HashMap::new();
    let mut in_flight_by_variant: BTreeMap<usize, usize> = BTreeMap::new();

    committer.drain_ready(
        run_dir,
        policy_config,
        evidence_records_path,
        task_chain_states_path,
        &benchmark_predictions_path,
        &benchmark_scores_path,
        schedule_progress,
        *trial_index,
        pruned_variants,
        consecutive_failures,
        run_sink,
    )?;
    persist_pending_trial_completions(run_dir, &committer)?;
    write_run_control_v2(
        run_dir,
        run_id,
        "running",
        &in_flight_active_trials(&in_flight),
        None,
    )?;

    while committer.next_commit_idx < schedule.len() || !in_flight.is_empty() {
        if last_disk_check.elapsed() >= disk_check_interval {
            enforce_runtime_disk_headroom(run_dir, min_free_bytes)?;
            last_disk_check = Instant::now();
        }
        if let Some(max_bytes) = max_run_bytes {
            if last_run_size_check.elapsed() >= run_size_check_interval {
                enforce_runtime_run_size_budget(run_dir, max_bytes)?;
                last_run_size_check = Instant::now();
            }
        }

        if let Some(outcome) = process_parallel_worker_control_request(
            run_dir,
            run_id,
            backend.as_ref(),
            &mut in_flight,
            &mut in_flight_by_variant,
        )? {
            return Ok(outcome);
        }

        let mut made_progress = false;
        let mut dispatch_backpressured = false;

        while next_dispatch_idx < schedule.len() && in_flight.len() < dispatch_capacity {
            let slot = &schedule[next_dispatch_idx];
            if pruned_variants.contains(&slot.variant_idx) {
                committer.enqueue_skipped(next_dispatch_idx)?;
                next_dispatch_idx += 1;
                made_progress = true;
                continue;
            }
            if let Some(limit) = policy_config.concurrency.max_in_flight_per_variant {
                let variant_in_flight = in_flight_by_variant
                    .get(&slot.variant_idx)
                    .copied()
                    .unwrap_or(0);
                if variant_in_flight >= limit {
                    break;
                }
            }

            let proposed_trial_index = trial_index.saturating_add(1);
            let trial_id = format!("trial_{}", proposed_trial_index);
            let variant = &variants[slot.variant_idx];
            let task_boundary = parse_task_boundary_from_dataset_task(&tasks[slot.task_idx])?;
            let task_id = task_boundary
                .task_payload
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("task_{}", slot.task_idx));
            let dispatch = TrialDispatch {
                run_id: run_id.to_string(),
                trial_id: trial_id.clone(),
                schedule_idx: next_dispatch_idx,
                slot: slot.clone(),
                variant_id: variant.id.clone(),
                task_id,
                repl_idx: slot.repl_idx,
                runtime_profile: json!({}),
                task_payload: task_boundary.task_payload,
                effective_policy: json!({}),
            };
            let Some(ticket) = submit_dispatch_with_backpressure(backend.as_ref(), dispatch)?
            else {
                dispatch_backpressured = true;
                break;
            };
            *trial_index = proposed_trial_index;
            let started_at = Utc::now().to_rfc3339();
            in_flight.insert(
                ticket.ticket_id.clone(),
                InFlightDispatch {
                    schedule_idx: next_dispatch_idx,
                    trial_id: trial_id.clone(),
                    variant_idx: slot.variant_idx,
                    variant_id: variant.id.clone(),
                    worker_id: ticket.worker_id.clone(),
                    started_at,
                },
            );
            *in_flight_by_variant.entry(slot.variant_idx).or_default() += 1;
            next_dispatch_idx += 1;
            made_progress = true;
            write_run_control_v2(
                run_dir,
                run_id,
                "running",
                &in_flight_active_trials(&in_flight),
                None,
            )?;
        }

        if dispatch_backpressured && in_flight.is_empty() {
            return Err(anyhow!(
                "parallel coordinator protocol fault: backend reported capacity with no active tickets"
            ));
        }

        let committed = committer.drain_ready(
            run_dir,
            policy_config,
            evidence_records_path,
            task_chain_states_path,
            &benchmark_predictions_path,
            &benchmark_scores_path,
            schedule_progress,
            *trial_index,
            pruned_variants,
            consecutive_failures,
            run_sink,
        )?;
        persist_pending_trial_completions(run_dir, &committer)?;
        if committed > 0 {
            made_progress = true;
        }

        if committer.next_commit_idx >= schedule.len() && in_flight.is_empty() {
            break;
        }

        let poll_timeout = if made_progress {
            Duration::from_millis(0)
        } else {
            Duration::from_millis(50)
        };
        let completions = backend.poll_completions(poll_timeout)?;
        if completions.is_empty() {
            continue;
        }

        for completion in completions {
            let in_flight_entry = in_flight
                .remove(completion.ticket.ticket_id.as_str())
                .ok_or_else(|| {
                    anyhow!(
                        "parallel coordinator protocol fault: completion for unknown ticket {}",
                        completion.ticket.ticket_id
                    )
                })?;
            if completion.schedule_idx != in_flight_entry.schedule_idx {
                return Err(anyhow!(
                    "parallel coordinator protocol fault: completion schedule_idx {} did not match dispatched schedule_idx {}",
                    completion.schedule_idx,
                    in_flight_entry.schedule_idx
                ));
            }
            if let Some(count) = in_flight_by_variant.get_mut(&in_flight_entry.variant_idx) {
                if *count > 0 {
                    *count -= 1;
                }
                if *count == 0 {
                    in_flight_by_variant.remove(&in_flight_entry.variant_idx);
                }
            }
            let trial_result = decode_parallel_completion_result(&completion, &in_flight_entry)?;
            committer.enqueue_trial(in_flight_entry.schedule_idx, trial_result)?;
        }
        persist_pending_trial_completions(run_dir, &committer)?;

        write_run_control_v2(
            run_dir,
            run_id,
            "running",
            &in_flight_active_trials(&in_flight),
            None,
        )?;
        committer.drain_ready(
            run_dir,
            policy_config,
            evidence_records_path,
            task_chain_states_path,
            &benchmark_predictions_path,
            &benchmark_scores_path,
            schedule_progress,
            *trial_index,
            pruned_variants,
            consecutive_failures,
            run_sink,
        )?;
        persist_pending_trial_completions(run_dir, &committer)?;
    }

    committer.drain_ready(
        run_dir,
        policy_config,
        evidence_records_path,
        task_chain_states_path,
        &benchmark_predictions_path,
        &benchmark_scores_path,
        schedule_progress,
        *trial_index,
        pruned_variants,
        consecutive_failures,
        run_sink,
    )?;
    persist_pending_trial_completions(run_dir, &committer)?;
    write_run_control_v2(
        run_dir,
        run_id,
        "running",
        &in_flight_active_trials(&in_flight),
        None,
    )?;
    Ok(ScheduleEngineOutcome::Completed)
}

#[allow(clippy::too_many_arguments)]
fn execute_schedule_engine(
    mode: ScheduleEngineMode,
    run_dir: &Path,
    run_id: &str,
    workload_type: &str,
    project_root: &Path,
    dataset_path: &Path,
    variants: &[Variant],
    tasks: &[Value],
    schedule: &[TrialSlot],
    policy_config: &PolicyConfig,
    benchmark_config: &BenchmarkConfig,
    variant_runtime_profiles: &[VariantRuntimeProfile],
    behavior: &RunBehavior,
    materialize_mode: MaterializationMode,
    task_boundary_policy: &TaskBoundaryPolicy,
    trials_dir: &Path,
    evidence_dir: &Path,
    evidence_records_path: &Path,
    task_chain_states_path: &Path,
    schedule_progress: &mut ScheduleProgress,
    trial_index: &mut usize,
    consecutive_failures: &mut BTreeMap<usize, usize>,
    pruned_variants: &mut HashSet<usize>,
    recovered_active_trials: &[RunControlActiveTrial],
    baseline_id: &str,
    run_sink: &mut dyn RunSink,
    max_concurrency: usize,
) -> Result<ScheduleEngineOutcome> {
    if !matches!(policy_config.state, StatePolicy::IsolatePerTrial) {
        return Err(anyhow!(
            "parallel worker hard cutover supports only isolate_per_trial state policy; got {:?}",
            policy_config.state
        ));
    }
    execute_schedule_engine_parallel(
        mode,
        run_dir,
        run_id,
        workload_type,
        project_root,
        dataset_path,
        variants,
        tasks,
        schedule,
        policy_config,
        benchmark_config,
        variant_runtime_profiles,
        behavior,
        materialize_mode,
        task_boundary_policy,
        trials_dir,
        evidence_dir,
        evidence_records_path,
        task_chain_states_path,
        schedule_progress,
        trial_index,
        consecutive_failures,
        pruned_variants,
        recovered_active_trials,
        baseline_id,
        run_sink,
        max_concurrency,
    )
}
fn load_authoring_input_for_build(
    path: &Path,
    overrides_path: Option<&Path>,
) -> Result<LoadedExperimentInput> {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if canonical.is_dir() {
        return Err(anyhow!(
            "build_input_invalid_kind: expected authoring spec file, got directory '{}'",
            canonical.display()
        ));
    }

    if canonical
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "manifest.json")
    {
        return Err(anyhow!(
            "build_input_invalid_kind: expected authoring spec file, got sealed package manifest"
        ));
    }

    let exp_dir = canonical
        .parent()
        .unwrap_or(Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));
    let project_root = find_project_root(&exp_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&exp_dir));
    let raw_yaml = fs::read_to_string(&canonical)?;
    let yaml_value: serde_yaml::Value = serde_yaml::from_str(&raw_yaml)?;
    let mut json_value: Value = serde_json::to_value(yaml_value)?;
    if let Some(overrides_path) = overrides_path {
        json_value = apply_experiment_overrides(json_value, overrides_path, &project_root)?;
    }
    json_value = normalize_experiment_authoring(json_value, &exp_dir, &project_root)?;
    Ok(LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root,
    })
}

fn as_portable_rel(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn copy_path_into_package(source: &Path, destination: &Path) -> Result<()> {
    if source.is_dir() {
        ensure_dir(destination)?;
        return copy_dir_filtered(source, destination, &[]);
    }
    if source.is_file() {
        if let Some(parent) = destination.parent() {
            ensure_dir(parent)?;
        }
        fs::copy(source, destination)?;
        return Ok(());
    }
    Err(anyhow!(
        "package build expected file or directory source, got: {}",
        source.display()
    ))
}

fn stage_source_into_package(
    raw_source: &str,
    exp_dir: &Path,
    package_dir: &Path,
    subdir: &str,
    prefix: &str,
    copies: &mut BTreeMap<String, String>,
    counter: &mut usize,
) -> Result<String> {
    let raw_path = PathBuf::from(raw_source);
    let resolved = if raw_path.is_absolute() {
        normalize_path(&raw_path)
    } else {
        normalize_path(&exp_dir.join(raw_path))
    };
    let key = resolved.to_string_lossy().to_string();
    if let Some(existing) = copies.get(&key) {
        return Ok(existing.clone());
    }
    if !resolved.exists() {
        return Err(anyhow!(
            "package build could not resolve source path: {}",
            resolved.display()
        ));
    }
    let name = resolved
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}_{}", prefix, counter));
    let rel_path = PathBuf::from(subdir).join(format!("{:03}_{}", *counter, name));
    let destination = package_dir.join(&rel_path);
    copy_path_into_package(&resolved, &destination)?;
    *counter += 1;
    let rel_portable = as_portable_rel(&rel_path);
    copies.insert(key, rel_portable.clone());
    Ok(rel_portable)
}

fn rewrite_runtime_paths_for_package(
    runtime_root: &mut Value,
    exp_dir: &Path,
    package_dir: &Path,
    artifact_copies: &mut BTreeMap<String, String>,
    file_copies: &mut BTreeMap<String, String>,
    artifact_counter: &mut usize,
    file_counter: &mut usize,
) -> Result<()> {
    let _ = file_copies;
    let _ = file_counter;
    if let Some(raw) = runtime_root
        .pointer("/agent_runtime/artifact")
        .and_then(Value::as_str)
    {
        let rel = stage_source_into_package(
            raw,
            exp_dir,
            package_dir,
            "agent_builds",
            "build",
            artifact_copies,
            artifact_counter,
        )?;
        set_json_pointer_value(runtime_root, "/agent_runtime/artifact", json!(rel.clone()))?;
        set_json_pointer_value(
            runtime_root,
            "/agent_runtime/artifact_resolved_path",
            json!(rel),
        )?;
    }
    Ok(())
}

fn sanitize_name_for_path(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "experiment".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn build_experiment_package(
    path: &Path,
    overrides_path: Option<&Path>,
    out_dir: Option<&Path>,
) -> Result<BuildResult> {
    let loaded = load_authoring_input_for_build(path, overrides_path)?;
    let mut json_value = loaded.json_value.clone();
    validate_required_fields(&json_value)?;

    let experiment_id = json_value
        .pointer("/experiment/id")
        .and_then(Value::as_str)
        .unwrap_or("experiment");
    let package_dir = if let Some(out_dir) = out_dir {
        out_dir.to_path_buf()
    } else {
        let ts = Utc::now().format("%Y%m%d_%H%M%S_%6f");
        loaded
            .project_root
            .join(".lab")
            .join("builds")
            .join(format!("{}_{}", sanitize_name_for_path(experiment_id), ts))
    };
    if package_dir.exists() {
        if !package_dir.is_dir() {
            return Err(anyhow!(
                "build output path exists and is not a directory: {}",
                package_dir.display()
            ));
        }
        let mut entries = fs::read_dir(&package_dir)?;
        if entries.next().is_some() {
            return Err(anyhow!(
                "build output directory must be empty: {}",
                package_dir.display()
            ));
        }
    } else {
        ensure_dir(&package_dir)?;
    }

    ensure_dir(&package_dir.join("agent_builds"))?;
    ensure_dir(&package_dir.join("tasks"))?;
    ensure_dir(&package_dir.join("files"))?;

    let dataset_path = resolve_dataset_path(&json_value, &loaded.exp_dir)?;
    let dataset_target = package_dir.join("tasks").join("tasks.jsonl");
    copy_path_into_package(&dataset_path, &dataset_target)?;
    let dataset_rel = PathBuf::from("tasks").join("tasks.jsonl");
    set_json_pointer_value(
        &mut json_value,
        "/dataset/path",
        json!(as_portable_rel(&dataset_rel)),
    )?;

    let mut artifact_copies: BTreeMap<String, String> = BTreeMap::new();
    let mut file_copies: BTreeMap<String, String> = BTreeMap::new();
    let mut artifact_counter = 0usize;
    let mut file_counter = 0usize;

    if let Some(runtime) = json_value.pointer_mut("/runtime") {
        rewrite_runtime_paths_for_package(
            runtime,
            &loaded.exp_dir,
            &package_dir,
            &mut artifact_copies,
            &mut file_copies,
            &mut artifact_counter,
            &mut file_counter,
        )?;
    }
    if let Some(runtime_overrides) = json_value.pointer_mut("/baseline/runtime_overrides") {
        rewrite_runtime_paths_for_package(
            runtime_overrides,
            &loaded.exp_dir,
            &package_dir,
            &mut artifact_copies,
            &mut file_copies,
            &mut artifact_counter,
            &mut file_counter,
        )?;
    }
    if let Some(variant_plan) = json_value
        .pointer_mut("/variant_plan")
        .and_then(Value::as_array_mut)
    {
        for variant in variant_plan.iter_mut() {
            if let Some(runtime_overrides) = variant.get_mut("runtime_overrides") {
                rewrite_runtime_paths_for_package(
                    runtime_overrides,
                    &loaded.exp_dir,
                    &package_dir,
                    &mut artifact_copies,
                    &mut file_copies,
                    &mut artifact_counter,
                    &mut file_counter,
                )?;
            }
        }
    }
    if let Some(variants) = json_value
        .pointer_mut("/variants")
        .and_then(Value::as_array_mut)
    {
        for variant in variants.iter_mut() {
            if let Some(runtime_overrides) = variant.get_mut("runtime_overrides") {
                rewrite_runtime_paths_for_package(
                    runtime_overrides,
                    &loaded.exp_dir,
                    &package_dir,
                    &mut artifact_copies,
                    &mut file_copies,
                    &mut artifact_counter,
                    &mut file_counter,
                )?;
            }
        }
    }

    validate_packaged_runtime_artifacts(&package_dir, &json_value)?;

    let resolved_for_manifest = json_value.clone();
    atomic_write_json_pretty(
        &package_dir.join("resolved_experiment.json"),
        &resolved_for_manifest,
    )?;

    let manifest_path = package_dir.join("manifest.json");
    let checksums_path = package_dir.join("checksums.json");
    let lock_path = package_dir.join("package.lock");
    let mut checksums: BTreeMap<String, String> = BTreeMap::new();
    for entry in walkdir::WalkDir::new(&package_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
    {
        let path = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }
        if path == checksums_path || path == manifest_path || path == lock_path {
            continue;
        }
        let rel = path
            .strip_prefix(&package_dir)
            .map(as_portable_rel)
            .unwrap_or_else(|_| path.display().to_string());
        checksums.insert(rel, sha256_file(path)?);
    }
    let checksums_value = json!({
        "schema_version": "sealed_package_checksums_v2",
        "files": checksums,
    });
    atomic_write_json_pretty(&checksums_path, &checksums_value)?;
    let package_digest = canonical_json_digest(
        checksums_value
            .pointer("/files")
            .ok_or_else(|| anyhow!("build failed to materialize checksums files map"))?,
    );
    atomic_write_json_pretty(
        &lock_path,
        &json!({
            "schema_version": "sealed_package_lock_v1",
            "package_digest": package_digest.clone(),
        }),
    )?;
    let package_manifest = json!({
        "schema_version": "sealed_run_package_v2",
        "created_at": Utc::now().to_rfc3339(),
        "resolved_experiment": resolved_for_manifest,
        "checksums_ref": "checksums.json",
        "package_digest": package_digest,
    });
    atomic_write_json_pretty(&manifest_path, &package_manifest)?;

    Ok(BuildResult {
        package_dir,
        manifest_path,
        checksums_path,
    })
}

fn run_experiment_with_behavior(
    path: &Path,
    behavior: RunBehavior,
    execution: RunExecutionOptions,
) -> Result<RunResult> {
    let LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root,
    } = load_sealed_package_for_run(path)?;
    validate_required_fields(&json_value)?;
    let workload_type = experiment_workload_type(&json_value)?;

    let execution = normalize_execution_options(&execution);
    let materialize_mode = execution.materialize.unwrap_or(MaterializationMode::Full);

    let (run_id, run_dir) = create_unique_run_dir(&project_root)?;
    emit_run_log(
        &run_id,
        format!("created run directory {}", run_dir.display()),
    );
    write_run_control_v2(&run_dir, &run_id, "running", &[], None)?;
    write_run_session_state(&run_dir, &run_id, &behavior, &execution)?;
    let _engine_lease_guard = start_engine_lease_heartbeat(&run_dir, &run_id)?;
    let mut run_guard = RunControlGuard::new(&run_dir, &run_id);

    for subdir in ["tasks", "files", "agent_builds"] {
        let source = exp_dir.join(subdir);
        if source.exists() {
            copy_path_into_package(&source, &run_dir.join(subdir))?;
        }
    }

    let resolved_path = run_dir.join("resolved_experiment.json");
    atomic_write_json_pretty(&resolved_path, &json_value)?;
    let resolved_digest = canonical_json_digest(&json_value);
    atomic_write_bytes(
        &run_dir.join("resolved_experiment.digest"),
        resolved_digest.as_bytes(),
    )?;

    let manifest = json!({
        "schema_version": "manifest_v1",
        "run_id": run_id,
        "runner_version": "rust-0.3.0",
        "created_at": Utc::now().to_rfc3339(),
    });
    atomic_write_json_pretty(&run_dir.join("manifest.json"), &manifest)?;

    let dataset_path = resolve_dataset_path_in_package(&json_value, &run_dir)?;
    let tasks = load_tasks(&dataset_path, &json_value)?;

    let (variants, baseline_id) = resolve_variant_plan(&json_value)?;
    write_resolved_variants(&run_dir, &json_value, &baseline_id, &variants)?;
    let replications = json_value
        .pointer("/design/replications")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing /design/replications"))? as usize;
    emit_run_log(
        &run_id,
        format!(
            "resolved experiment: tasks={} variants={} replications={} total_trials={}",
            tasks.len(),
            variants.len(),
            replications,
            tasks.len() * variants.len() * replications
        ),
    );

    let trials_dir = run_dir.join("trials");
    ensure_dir(&trials_dir)?;

    let evidence_dir = run_dir.join("runtime").join("sqlite_ingest");
    let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
    let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
    let benchmark_config = parse_benchmark_config(&json_value);
    let mut variant_runtime_profiles = Vec::with_capacity(variants.len());
    for variant in &variants {
        let profile = resolve_variant_runtime_profile(
            &json_value,
            variant,
            &run_dir,
            &behavior,
            &execution,
        )?;
        ensure_required_runtime_env_present(&profile.agent_runtime, &profile.agent_runtime_env)?;
        variant_runtime_profiles.push(profile);
    }
    let run_integration_level = variant_runtime_profiles
        .first()
        .map(|profile| profile.agent_runtime.integration_level.clone())
        .unwrap_or_else(|| "cli_basic".to_string());
    let isolation_grade = resolve_run_isolation_grade(&variant_runtime_profiles, &behavior);

    // Preflight checks — abort before trial execution if anything is fatally wrong
    {
        emit_run_log(
            &run_id,
            "starting preflight checks (Docker probes can take a while for per-task images)",
        );
        let preflight_started = Instant::now();
        let checks = collect_preflight_checks(
            &json_value,
            &run_dir,
            &run_dir,
            &project_root,
            &tasks,
            &benchmark_config,
            &variants,
            &variant_runtime_profiles,
        );

        let preflight = PreflightReport {
            passed: checks
                .iter()
                .all(|c| c.passed || matches!(c.severity, PreflightSeverity::Warning)),
            checks,
        };

        let mut passed_count = 0usize;
        let mut warning_count = 0usize;
        let mut failed_count = 0usize;
        for check in &preflight.checks {
            let status = if check.passed {
                passed_count += 1;
                "PASS"
            } else {
                match check.severity {
                    PreflightSeverity::Error => {
                        failed_count += 1;
                        "FAIL"
                    }
                    PreflightSeverity::Warning => {
                        warning_count += 1;
                        "WARN"
                    }
                }
            };
            emit_preflight_log(format!("[{}] {}: {}", status, check.name, check.message));
        }
        emit_run_log(
            &run_id,
            format!(
                "preflight finished in {:.1}s (passed={}, warnings={}, failed={})",
                preflight_started.elapsed().as_secs_f32(),
                passed_count,
                warning_count,
                failed_count
            ),
        );

        if !preflight.passed {
            // Clean up the partial run directory before aborting
            run_guard.complete("preflight_failed")?;
            return Err(anyhow!("preflight failed:\n{}", preflight));
        }
    }

    let mut run_sink = SqliteRunStore::new(&run_dir)?;
    run_sink.write_run_manifest(&RunManifestRecord {
        schema_version: "run_manifest_v1".to_string(),
        run_id: run_id.clone(),
        created_at: Utc::now().to_rfc3339(),
        workload_type: workload_type.clone(),
        baseline_id: baseline_id.clone(),
        variant_ids: variants.iter().map(|variant| variant.id.clone()).collect(),
    })?;

    let policy_config = parse_policies(&json_value);
    let max_concurrency = experiment_max_concurrency(&json_value);
    let random_seed = experiment_random_seed(&json_value);
    let schedule = build_trial_schedule(
        variants.len(),
        tasks.len(),
        replications,
        policy_config.scheduling,
        random_seed,
    );
    write_resolved_schedule(&run_dir, &schedule)?;
    emit_run_log(
        &run_id,
        format!(
            "starting schedule execution: slots={} max_concurrency={}",
            schedule.len(),
            max_concurrency.max(1)
        ),
    );

    // Per-variant consecutive failure tracking (for pruning)
    let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
    let mut pruned_variants: HashSet<usize> = HashSet::new();

    let mut schedule_progress = ScheduleProgress {
        schema_version: "schedule_progress_v2".to_string(),
        run_id: run_id.clone(),
        total_slots: schedule.len(),
        next_schedule_index: 0,
        next_trial_index: 0,
        schedule: schedule.clone(),
        completed_slots: Vec::new(),
        pruned_variants: Vec::new(),
        consecutive_failures: BTreeMap::new(),
        updated_at: Utc::now().to_rfc3339(),
    };
    write_schedule_progress(&run_dir, &schedule_progress)?;

    let mut trial_index: usize = 0;
    let schedule_outcome = execute_schedule_engine(
        ScheduleEngineMode::FreshRun,
        &run_dir,
        &run_id,
        &workload_type,
        &project_root,
        &dataset_path,
        &variants,
        &tasks,
        &schedule,
        &policy_config,
        &benchmark_config,
        &variant_runtime_profiles,
        &behavior,
        materialize_mode,
        &policy_config.task_boundary,
        &trials_dir,
        &evidence_dir,
        &evidence_records_path,
        &task_chain_states_path,
        &mut schedule_progress,
        &mut trial_index,
        &mut consecutive_failures,
        &mut pruned_variants,
        &[],
        &baseline_id,
        &mut run_sink,
        max_concurrency,
    )?;
    run_sink.flush()?;
    if schedule_outcome != ScheduleEngineOutcome::Completed {
        emit_run_log(
            &run_id,
            format!("schedule execution halted with {:?}", schedule_outcome),
        );
        run_guard.disarm();
        return Ok(RunResult { run_dir, run_id });
    }
    let _ = (
        &project_root,
        &benchmark_config,
        &evidence_records_path,
        &task_chain_states_path,
    );

    if isolation_grade != "hermetic" {
        run_guard.complete("invalid_isolation")?;
        return Err(anyhow!(
            "scientific run completed without hermetic isolation (got {})",
            isolation_grade
        ));
    }

    let grades = json!({
        "schema_version": "grades_v1",
        "integration_level": run_integration_level,
        "replay_grade": "best_effort",
        "isolation_grade": isolation_grade,
        "comparability_grade": "unknown",
        "provenance_grade": "recorded",
        "privacy_grade": "unknown"
    });

    let att = default_attestation(
        &resolved_digest,
        None,
        grades.clone(),
        vec![],
        json!({"name": "unknown"}),
        "hooks",
    );
    write_attestation(&run_dir, att)?;
    run_guard.complete("completed")?;
    emit_run_log(&run_id, "run completed");

    Ok(RunResult { run_dir, run_id })
}

pub fn describe_experiment(path: &Path) -> Result<ExperimentSummary> {
    let LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root: _,
    } = load_sealed_package_for_run(path)?;
    validate_required_fields(&json_value)?;

    let dataset_path = resolve_dataset_path_in_package(&json_value, &exp_dir)?;
    let task_count = count_tasks(&dataset_path, &json_value)?;
    let (variants, _) = resolve_variant_plan(&json_value)?;
    let replications = json_value
        .pointer("/design/replications")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing /design/replications"))? as usize;
    let variant_count = variants.len();
    let total_trials = task_count * replications * variant_count;

    let baseline_variant = variants
        .first()
        .ok_or_else(|| anyhow!("no variants available in experiment"))?;
    let runtime_profile = resolve_variant_runtime_profile(
        &json_value,
        baseline_variant,
        &exp_dir,
        &RunBehavior::default(),
        &RunExecutionOptions::default(),
    )?;
    let preflight_runtime_profiles = vec![runtime_profile.clone()];
    let VariantRuntimeProfile {
        agent_runtime: runtime_agent,
        configured_network_mode: network_mode,
        ..
    } = runtime_profile;
    let image = Some(runtime_agent.image.clone());

    let exp_id = json_value
        .pointer("/experiment/id")
        .and_then(|v| v.as_str())
        .unwrap_or("exp")
        .to_string();
    let workload_type = experiment_workload_type(&json_value)?;

    let policy_config = parse_policies(&json_value);
    let comparison = json_value
        .pointer("/design/comparison")
        .and_then(|v| v.as_str())
        .unwrap_or("paired")
        .to_string();

    // Collect preflight warnings (lightweight: dataset + config checks only, no Docker)
    let benchmark_config = parse_benchmark_config(&json_value);
    let tasks_for_preflight = load_tasks(&dataset_path, &json_value).unwrap_or_default();
    let mut preflight_warnings = Vec::new();
    for check in check_dataset_task_ids(
        &tasks_for_preflight,
        &benchmark_config,
        &preflight_runtime_profiles,
    ) {
        if matches!(check.severity, PreflightSeverity::Warning) || !check.passed {
            preflight_warnings.push(format!("[{}] {}", check.name, check.message));
        }
    }
    {
        let grader_check = check_benchmark_grader_reachable(
            &benchmark_config,
            &resolve_variant_runtime_profile(
                &json_value,
                baseline_variant,
                &exp_dir,
                &RunBehavior::default(),
                &RunExecutionOptions::default(),
            )?,
            baseline_variant,
            &tasks_for_preflight,
            &exp_dir,
        );
        if matches!(grader_check.severity, PreflightSeverity::Warning)
            && !grader_check.message.contains("no benchmark")
        {
            preflight_warnings.push(format!("[{}] {}", grader_check.name, grader_check.message));
        }
    }
    for check in check_dependency_files_exist(&json_value, &exp_dir) {
        if !check.passed {
            preflight_warnings.push(format!("[{}] {}", check.name, check.message));
        }
    }

    Ok(ExperimentSummary {
        exp_id,
        workload_type,
        dataset_path,
        task_count,
        replications,
        variant_count,
        total_trials,
        agent_runtime_command: runtime_agent.command_raw,
        image,
        network_mode,
        trajectory_path: runtime_agent.trajectory_path,
        causal_extraction: runtime_agent.causal_extraction,
        scheduling: match policy_config.scheduling {
            SchedulingPolicy::PairedInterleaved => "paired_interleaved".to_string(),
            SchedulingPolicy::VariantSequential => "variant_sequential".to_string(),
            SchedulingPolicy::Randomized => "randomized".to_string(),
        },
        state_policy: match policy_config.state {
            StatePolicy::IsolatePerTrial => "isolate_per_trial".to_string(),
            StatePolicy::PersistPerTask => "persist_per_task".to_string(),
            StatePolicy::Accumulate => "accumulate".to_string(),
        },
        comparison,
        retry_max_attempts: policy_config.retry_max_attempts,
        preflight_warnings,
    })
}
