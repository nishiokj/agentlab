use crate::*;

pub fn continue_run_with_options(
    run_dir: &Path,
    options: RunExecutionOptions,
) -> Result<RunResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Continue)?;
    let run_dir = run_dir
        .canonicalize()
        .unwrap_or_else(|_| run_dir.to_path_buf());

    // 1. Validate run status is terminal and continuable.
    let control_path = run_control_path(&run_dir);
    let control: Value = load_json_file(&control_path)?;
    let run_status = control
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let recovered_active_trials = run_control_active_trials(&control);
    match run_status {
        "failed" | "paused" | "interrupted" => {}
        "completed" => return Err(anyhow!("run already completed — nothing to continue")),
        "running" => {
            return Err(anyhow!(
                "run is currently active — cannot continue a running experiment; run `lab recover --run-dir {}` first",
                run_dir.display()
            ))
        }
        other => return Err(anyhow!("unexpected run status: {}", other)),
    }

    let run_id = control
        .get("run_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing run_id in run_control.json"))?
        .to_string();
    let _engine_lease_guard = start_engine_lease_heartbeat(&run_dir, &run_id)?;
    let run_session = load_run_session_state(&run_dir)?;
    if run_session.run_id != run_id {
        return Err(anyhow!(
            "run session state mismatch: run_control has {}, run_session_state has {}",
            run_id,
            run_session.run_id
        ));
    }
    let behavior = run_session.behavior;
    let persisted_execution = run_session.execution;
    let execution = normalize_execution_options(&RunExecutionOptions {
        #[cfg(test)]
        executor: persisted_execution.executor,
        materialize: persisted_execution.materialize,
        runtime_env: options.runtime_env,
        runtime_env_files: options.runtime_env_files,
    });

    // 2. Load schedule progress
    let progress = load_schedule_progress(&run_dir)?;
    if progress.next_schedule_index >= progress.total_slots {
        return Err(anyhow!(
            "all {} schedule slots were already processed — nothing to continue",
            progress.total_slots
        ));
    }

    // 3. Load resolved experiment
    let resolved_path = run_dir.join("resolved_experiment.json");
    let json_value: Value = serde_json::from_slice(&fs::read(&resolved_path)?)?;
    let policy_config = parse_policies(&json_value);
    let max_concurrency = experiment_max_concurrency(&json_value);
    let project_root = find_project_root_from_run_dir(&run_dir)?;
    let project_root = project_root
        .canonicalize()
        .unwrap_or_else(|_| project_root.clone());

    let workload_type = experiment_workload_type(&json_value)?;

    // 4. Reject non-IsolatePerTrial state policies
    if !matches!(policy_config.state, StatePolicy::IsolatePerTrial) {
        return Err(anyhow!(
            "continue_run only supports IsolatePerTrial state policy; \
             this run uses {:?} — chain state recovery is not yet implemented",
            policy_config.state
        ));
    }

    // 5. Reconstruct schedule and verify it matches
    let (variants, baseline_id) = load_run_variants(&run_dir, &json_value)?;
    write_resolved_variants(&run_dir, &json_value, &baseline_id, &variants)?;
    let exp_dir = resolved_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let dataset_path = resolve_dataset_path_in_package(&json_value, &exp_dir)?;
    let tasks = load_tasks(&dataset_path, &json_value)?;
    let replications = json_value
        .pointer("/design/replications")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing /design/replications"))? as usize;
    let random_seed = experiment_random_seed(&json_value);

    let reconstructed_schedule = build_trial_schedule(
        variants.len(),
        tasks.len(),
        replications,
        policy_config.scheduling,
        random_seed,
    );

    if reconstructed_schedule != progress.schedule {
        return Err(anyhow!(
            "schedule mismatch — the experiment configuration has changed since this run was \
             created; cannot safely continue (reconstructed {} slots vs stored {})",
            reconstructed_schedule.len(),
            progress.schedule.len()
        ));
    }

    let schedule = reconstructed_schedule;
    write_resolved_schedule(&run_dir, &schedule)?;
    let materialize_mode = execution.materialize.unwrap_or(MaterializationMode::Full);

    // 6. Mark run as running again
    write_run_control_v2(&run_dir, &run_id, "running", &[], None)?;
    let mut run_guard = RunControlGuard::new(&run_dir, &run_id);

    // 7. Reconstruct variant runtime profiles
    let mut variant_runtime_profiles = Vec::with_capacity(variants.len());
    for variant in &variants {
        let profile =
            resolve_variant_runtime_profile(&json_value, variant, &exp_dir, &behavior, &execution)?;
        ensure_required_runtime_env_present(&profile.agent_runtime, &profile.agent_runtime_env)?;
        variant_runtime_profiles.push(profile);
    }
    let run_integration_level = variant_runtime_profiles
        .first()
        .map(|profile| profile.agent_runtime.integration_level.clone())
        .unwrap_or_else(|| "cli_basic".to_string());
    let isolation_grade = resolve_run_isolation_grade(&variant_runtime_profiles, &behavior);

    let benchmark_config = parse_benchmark_config(&json_value);

    // 8. Restore scheduler state from progress
    let mut consecutive_failures: BTreeMap<usize, usize> = progress.consecutive_failures.clone();
    let mut pruned_variants: HashSet<usize> = progress.pruned_variants.iter().copied().collect();

    let trials_dir = run_dir.join("trials");
    ensure_dir(&trials_dir)?;
    let evidence_dir = run_dir.join("runtime").join("sqlite_ingest");
    let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
    let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
    let mut run_sink = SqliteRunStore::new(&run_dir)?;
    run_sink.write_run_manifest(&RunManifestRecord {
        schema_version: "run_manifest_v1".to_string(),
        run_id: run_id.clone(),
        created_at: Utc::now().to_rfc3339(),
        workload_type: workload_type.clone(),
        baseline_id: baseline_id.clone(),
        variant_ids: variants.iter().map(|variant| variant.id.clone()).collect(),
    })?;

    let mut schedule_progress = progress.clone();
    let recovered_max_trial_index = recovered_active_trials
        .iter()
        .filter_map(|active| trial_index_from_trial_id(&active.trial_id))
        .max()
        .unwrap_or(0);
    let mut trial_index: usize = schedule_progress
        .next_trial_index
        .max(recovered_max_trial_index);

    let schedule_outcome = execute_schedule_engine(
        ScheduleEngineMode::ContinueRun,
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
        &recovered_active_trials,
        &baseline_id,
        &mut run_sink,
        max_concurrency,
    )?;
    run_sink.flush()?;
    if schedule_outcome != ScheduleEngineOutcome::Completed {
        match schedule_outcome {
            ScheduleEngineOutcome::Interrupted => {
                run_guard.complete("interrupted")?;
            }
            _ => {
                // Paused/Killed: handler already wrote correct status
                run_guard.disarm();
            }
        }
        return Ok(RunResult {
            run_dir: run_dir.to_path_buf(),
            run_id,
        });
    }

    let _ = (
        &project_root,
        &benchmark_config,
        &evidence_records_path,
        &task_chain_states_path,
    );

    let resolved_digest = canonical_json_digest(&json_value);
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

    Ok(RunResult {
        run_dir: run_dir.to_path_buf(),
        run_id,
    })
}

pub(crate) fn recover_reconciled_status(previous: &str) -> &'static str {
    match previous {
        "completed" => "completed",
        "killed" => "killed",
        _ => "interrupted",
    }
}

fn reconcile_runtime_trials_for_recovery(
    run_dir: &Path,
    committed_by_schedule: &BTreeMap<usize, SlotCommitRecord>,
) -> Result<(usize, HashSet<String>)> {
    let trials_dir = run_dir.join("trials");
    if !trials_dir.exists() {
        return Ok((0, HashSet::new()));
    }

    let mut trial_dirs = fs::read_dir(&trials_dir)?
        .filter_map(|entry| entry.ok().map(|item| item.path()))
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    trial_dirs.sort();

    let mut released = 0usize;
    let mut runtime_state_trial_ids = HashSet::new();
    for trial_dir in trial_dirs {
        if !crate::trial::state::trial_attempt_state_exists(&trial_dir) {
            continue;
        }
        let Some(trial_id) = trial_dir
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string)
        else {
            continue;
        };
        runtime_state_trial_ids.insert(trial_id.clone());

        let persisted = crate::trial::state::load_trial_attempt_state(&trial_dir)?;
        let schedule_idx = persisted.state.slot.schedule_idx as usize;
        if committed_by_schedule
            .get(&schedule_idx)
            .is_some_and(|committed| committed.trial_id == trial_id)
        {
            let _ = crate::trial::state::reconcile_trial_attempt_as_committed(&trial_dir);
            continue;
        }
        if !crate::trial::state::trial_phase_requires_recovery_release(&persisted.state.phase) {
            continue;
        }
        let _ = write_trial_state(
            &trial_dir,
            &trial_id,
            "failed",
            None,
            None,
            Some("worker_lost_recovered"),
        );
        let _ = crate::trial::state::reconcile_trial_attempt_as_abandoned(&trial_dir);
        released += 1;
    }

    Ok((released, runtime_state_trial_ids))
}

pub fn recover_run(run_dir: &Path, force: bool) -> Result<RecoverResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Recover)?;
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;

    let control_path = run_control_path(&run_dir);
    let control = load_json_file(&control_path)?;
    let previous_status = control
        .pointer("/status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let run_id = control
        .pointer("/run_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing run_id in run_control.json"))?
        .to_string();
    let run_session = load_run_session_state(&run_dir)?;
    if run_session.run_id != run_id {
        return Err(anyhow!(
            "run session state mismatch: run_control has {}, run_session_state has {}",
            run_id,
            run_session.run_id
        ));
    }

    let mut progress = load_schedule_progress(&run_dir)?;
    let journal_records = load_slot_commit_records(&run_dir)?;
    adopt_engine_lease_for_recovery(&run_dir, &run_id, force)?;
    let committed_by_schedule = commit_record_by_schedule(&journal_records);

    let mut committed_prefix_len = 0usize;
    while committed_by_schedule.contains_key(&committed_prefix_len) {
        committed_prefix_len += 1;
    }

    let mut divergence_idx: Option<usize> = None;
    let comparable = std::cmp::min(progress.completed_slots.len(), committed_prefix_len);
    for idx in 0..comparable {
        let slot = &progress.completed_slots[idx];
        let committed = committed_by_schedule
            .get(&idx)
            .ok_or_else(|| anyhow!("missing committed slot at schedule_idx {}", idx))?;
        if slot.schedule_index != idx || slot.slot_commit_id != committed.slot_commit_id {
            divergence_idx = Some(idx);
            break;
        }
    }
    if divergence_idx.is_none() && progress.completed_slots.len() > committed_prefix_len {
        divergence_idx = Some(committed_prefix_len);
    }
    let rewound_to = divergence_idx.unwrap_or(progress.next_schedule_index);
    if let Some(idx) = divergence_idx {
        progress.completed_slots.truncate(idx);
        progress.pruned_variants.clear();
        progress.consecutive_failures.clear();
    }
    if committed_prefix_len > progress.completed_slots.len() {
        for idx in progress.completed_slots.len()..committed_prefix_len {
            if let Some(committed) = committed_by_schedule.get(&idx) {
                progress.completed_slots.push(SlotCompletion {
                    schedule_index: idx,
                    trial_id: committed.trial_id.clone(),
                    status: committed.slot_status.clone(),
                    slot_commit_id: committed.slot_commit_id.clone(),
                    attempt: committed.attempt.max(1),
                });
            }
        }
    }
    progress.next_schedule_index = progress.completed_slots.len();
    progress.schema_version = "schedule_progress_v2".to_string();
    progress.updated_at = Utc::now().to_rfc3339();

    let (mut active_trials_released, runtime_state_trial_ids) =
        reconcile_runtime_trials_for_recovery(&run_dir, &committed_by_schedule)?;
    let active_trials = run_control_active_trials(&control);
    for active in active_trials {
        if runtime_state_trial_ids.contains(&active.trial_id) {
            continue;
        }
        let Some(schedule_idx) = active.schedule_idx else {
            continue;
        };
        if schedule_idx < progress.next_schedule_index
            && committed_by_schedule.contains_key(&schedule_idx)
        {
            continue;
        }
        let trial_dir = run_dir.join("trials").join(&active.trial_id);
        if trial_dir.exists() {
            let _ = write_trial_state(
                &trial_dir,
                &active.trial_id,
                "failed",
                None,
                None,
                Some("worker_lost_recovered"),
            );
            let _ = crate::trial::state::reconcile_trial_attempt_as_abandoned(&trial_dir);
        }
        active_trials_released += 1;
    }

    write_schedule_progress(&run_dir, &progress)?;
    let recovered_status = recover_reconciled_status(&previous_status).to_string();
    write_run_control_v2(&run_dir, &run_id, &recovered_status, &[], None)?;
    let notes = vec![
        format!("engine lease adopted for run {}", run_id),
        format!("committed prefix length {}", committed_prefix_len),
        "active trials reconciled and released".to_string(),
    ];
    let report = json!({
        "schema_version": "recovery_report_v1",
        "run_id": run_id.clone(),
        "previous_status": previous_status.clone(),
        "recovered_status": recovered_status.clone(),
        "rewound_to_schedule_idx": rewound_to,
        "active_trials_released": active_trials_released,
        "committed_slots_verified": committed_prefix_len,
        "notes": notes,
        "recovered_at": Utc::now().to_rfc3339(),
    });
    let recovery_report_path = run_dir.join("runtime").join("recovery_report.json");
    atomic_write_json_pretty(&recovery_report_path, &report)?;

    Ok(RecoverResult {
        run_id,
        previous_status: previous_status.clone(),
        recovered_status,
        rewound_to_schedule_idx: rewound_to,
        active_trials_released,
        committed_slots_verified: committed_prefix_len,
        notes: report
            .pointer("/notes")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    })
}

pub fn replay_trial(run_dir: &Path, trial_id: &str, strict: bool) -> Result<ReplayResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Replay)?;
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;
    let run_id = run_dir
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("run")
        .to_string();
    let project_root = find_project_root(&run_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&run_dir));

    let resolved_path = run_dir.join("resolved_experiment.json");
    if !resolved_path.exists() {
        return Err(anyhow!(
            "missing resolved_experiment.json in {}",
            run_dir.display()
        ));
    }
    let json_value: Value = serde_json::from_slice(&fs::read(&resolved_path)?)?;
    let parent_trial_dir = run_dir.join("trials").join(trial_id);
    let prepared_manifest = load_prepared_task_environment_manifest(&parent_trial_dir)?;
    let (variants, _) = load_run_variants(&run_dir, &json_value)?;
    let variant_id = prepared_manifest.variant_id.as_str();
    let variant = find_variant_by_id(&variants, variant_id)?;
    let runtime_profile = resolve_variant_runtime_profile(
        &json_value,
        variant,
        &run_dir,
        &RunBehavior::default(),
        &RunExecutionOptions::default(),
    )?;
    let variant_args = runtime_profile.variant_args.clone();
    let agent_runtime = runtime_profile.agent_runtime;
    let agent_runtime_env = runtime_profile.agent_runtime_env;
    let effective_network_mode = runtime_profile.effective_network_mode;
    let runtime_experiment = runtime_profile.experiment;

    if strict && agent_runtime.integration_level != "sdk_full" {
        return Err(anyhow!(
            "strict replay requires integration_level sdk_full (found: {})",
            agent_runtime.integration_level
        ));
    }

    let replay_id = format!("replay_{}", Utc::now().format("%Y%m%d_%H%M%S"));
    let replay_dir = run_dir.join("replays").join(&replay_id);
    ensure_dir(&replay_dir)?;

    let replay_trial_id = format!("{}_{}", trial_id, replay_id);
    let task_boundary = materialize_packaged_task_boundary(&prepared_manifest.declaration)?;
    validate_task_boundary_workspace_materialization(&task_boundary)?;

    let replay_trial_dir = replay_dir.join("trial_1");
    ensure_dir(&replay_trial_dir)?;
    write_trial_state(
        &replay_trial_dir,
        &replay_trial_id,
        "running",
        None,
        None,
        None,
    )?;
    let mut trial_guard = TrialStateGuard::new(&replay_trial_dir, &replay_trial_id);

    let mut lineage_workspace_ref: Option<String> = None;
    {
        let store = BackingSqliteStore::open(&run_dir)?;
        if let Some(version_id) = store.latest_lineage_version_id_for_trial(&run_id, trial_id)? {
            lineage_workspace_ref = store.lineage_workspace_ref_by_version(&version_id)?;
        }
    }
    let prepared = prepare_task_environment(
        &project_root,
        &replay_trial_dir,
        &run_id,
        &replay_trial_id,
        &runtime_experiment,
        variant,
        prepared_manifest.task_index,
        prepared_manifest.repl_idx,
        &task_boundary,
        &agent_runtime,
        lineage_workspace_ref.as_deref(),
    )?;
    let PreparedTaskEnvironment {
        manifest: replay_prepared_manifest,
        trial_paths,
        io_paths: _,
        dynamic_mounts,
        trial_input: mut input,
    } = prepared;

    set_json_pointer_value(
        &mut input,
        "/runtime/control_plane/path",
        json!(DEFAULT_CONTAINER_CONTROL_PATH),
    )?;
    set_json_pointer_value(&mut input, "/runtime/control_plane/mode", json!("file"))?;
    let input_bytes = serde_json::to_vec_pretty(&input)?;
    let replay_task_sandbox_image = replay_prepared_manifest.task_sandbox_image().to_string();
    let replay_task_sandbox_workdir = replay_prepared_manifest
        .task_sandbox_workdir()
        .unwrap_or(task_boundary.task_workdir.as_str())
        .to_string();

    let io_paths = prepare_io_paths(&trial_paths, &input_bytes)?;
    let runtime_env = build_runtime_contract_env(
        &run_id,
        &input,
        &io_paths,
        Some(replay_task_sandbox_image.as_str()),
        resolve_trial_timeout_ms(&input),
    );
    let run_request = AdapterRunRequest {
        runtime_experiment: &runtime_experiment,
        runtime: &agent_runtime,
        variant_args: &variant_args,
        runtime_env: &runtime_env,
        runtime_overrides_env: &agent_runtime_env,
        trial_paths: &trial_paths,
        dynamic_mounts: &dynamic_mounts,
        io_paths: &io_paths,
        network_mode: effective_network_mode.as_str(),
        benchmark_grader: None,
        benchmark_grading_enabled: false,
        run_id: &run_id,
        task_image: replay_task_sandbox_image.as_str(),
        task_workdir: replay_task_sandbox_workdir.as_str(),
        task_materialization_kind: task_boundary.materialization.kind.clone(),
        agent_artifact: Some(agent_runtime.agent_artifact.as_path()),
    };
    let runtime_outcome = crate::trial::execution::execute_trial_runtime(
        &replay_trial_dir,
        0,
        1,
        &run_request,
        &replay_prepared_manifest.task_id,
        &variant.id,
        replay_prepared_manifest.repl_idx,
        replay_prepared_manifest
            .task_sandbox_plan
            .as_ref()
            .ok_or_else(|| anyhow!("prepared replay task missing task sandbox plan"))?,
    )?;
    let status = runtime_outcome.agent_exit_status;
    let trial_output = runtime_outcome.trial_output;
    let result_parse_error = runtime_outcome.result_parse_error;

    let outcome = trial_output
        .get("outcome")
        .and_then(|v| v.as_str())
        .unwrap_or("error");
    if status == "0" && outcome != "error" {
        trial_guard.complete("completed", None)?;
    } else if status != "0" {
        trial_guard.complete("failed", Some("harness_exit_nonzero"))?;
    } else if result_parse_error.is_some() {
        trial_guard.complete("failed", Some("trial_output_parse_error"))?;
    } else {
        trial_guard.complete("failed", Some("trial_output_error"))?;
    }

    let replay_grade = replay_grade_for_integration(&agent_runtime.integration_level).to_string();
    let artifact_store = ArtifactStore::new(run_dir.join("artifacts"));
    let trial_input_ref = artifact_store.put_bytes(&input_bytes)?;
    let trial_output_ref = artifact_store.put_bytes(&serde_json::to_vec_pretty(&trial_output)?)?;
    let manifest = json!({
        "schema_version": "replay_manifest_v1",
        "operation": "replay",
        "replay_id": replay_id.clone(),
        "parent_trial_id": trial_id,
        "strict": strict,
        "integration_level": agent_runtime.integration_level.clone(),
        "replay_grade": replay_grade.clone(),
        "trial_id": replay_trial_id.clone(),
        "refs": {
            "trial_input_ref": trial_input_ref,
            "trial_output_ref": trial_output_ref,
        },
        "created_at": Utc::now().to_rfc3339(),
    });
    validate_schema_contract_value(&manifest, "replay manifest metadata")?;
    let mut store = BackingSqliteStore::open(&run_dir)?;
    store.upsert_attempt_object(
        &run_id,
        &replay_trial_id,
        0,
        1,
        "trial_input",
        &trial_input_ref,
        Some(&manifest),
    )?;
    store.upsert_attempt_object(
        &run_id,
        &replay_trial_id,
        0,
        1,
        "trial_output",
        &trial_output_ref,
        Some(&manifest),
    )?;
    store.upsert_runtime_operation(&run_id, "replay", &replay_id, &manifest)?;
    trial_paths.cleanup_scratch()?;

    Ok(ReplayResult {
        replay_dir,
        replay_id,
        parent_trial_id: trial_id.to_string(),
        strict,
        replay_grade,
        harness_status: status,
    })
}

pub(crate) fn replay_grade_for_integration(level: &str) -> &'static str {
    match level {
        "sdk_full" => "strict",
        "sdk_control" => "checkpointed",
        "cli_events" | "otel" => "best_effort",
        _ => "best_effort",
    }
}

pub fn fork_trial(
    run_dir: &Path,
    from_trial: &str,
    selector: &str,
    set_bindings: &BTreeMap<String, Value>,
    strict: bool,
) -> Result<ForkResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Fork)?;
    fork_trial_inner(run_dir, from_trial, selector, set_bindings, strict)
}

pub(crate) fn fork_trial_inner(
    run_dir: &Path,
    from_trial: &str,
    selector: &str,
    set_bindings: &BTreeMap<String, Value>,
    strict: bool,
) -> Result<ForkResult> {
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;
    let project_root = find_project_root(&run_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&run_dir));

    let resolved_path = run_dir.join("resolved_experiment.json");
    if !resolved_path.exists() {
        return Err(anyhow!(
            "missing resolved_experiment.json in {}",
            run_dir.display()
        ));
    }
    let json_value: Value = serde_json::from_slice(&fs::read(&resolved_path)?)?;
    let parsed_selector = parse_fork_selector(selector)?;

    let run_id = run_dir
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("run")
        .to_string();

    let parent_trial_dir = run_dir.join("trials").join(from_trial);
    let prepared_manifest = load_prepared_task_environment_manifest(&parent_trial_dir)?;
    let parent_output = load_trial_output_payload(&run_dir, &run_id, from_trial).ok();
    let (variants, _) = load_run_variants(&run_dir, &json_value)?;
    let variant_id = prepared_manifest.variant_id.as_str();
    let mut variant = find_variant_by_id(&variants, variant_id)?.clone();
    apply_variant_binding_overrides(&mut variant, set_bindings)?;
    let runtime_profile = resolve_variant_runtime_profile(
        &json_value,
        &variant,
        &run_dir,
        &RunBehavior::default(),
        &RunExecutionOptions::default(),
    )?;
    let variant_args = runtime_profile.variant_args.clone();
    let agent_runtime = runtime_profile.agent_runtime;
    let agent_runtime_env = runtime_profile.agent_runtime_env;
    let effective_network_mode = runtime_profile.effective_network_mode;
    let runtime_experiment = runtime_profile.experiment;

    if strict && agent_runtime.integration_level != "sdk_full" {
        return Err(anyhow!(
            "strict fork requires integration_level sdk_full (found: {})",
            agent_runtime.integration_level
        ));
    }
    let source_checkpoint = resolve_selector_checkpoint(
        &parsed_selector,
        parent_output.as_ref(),
        &run_dir.join("trials").join(from_trial),
        strict,
    )?;
    if strict && source_checkpoint.is_none() {
        return Err(anyhow!(
            "strict_source_unavailable: selector {} did not resolve to a committed checkpoint",
            selector
        ));
    }

    let fork_id = format!("fork_{}", Utc::now().format("%Y%m%d_%H%M%S"));
    let fork_dir = run_dir.join("forks").join(&fork_id);
    ensure_dir(&fork_dir)?;
    let fork_trial_id = format!("{}_{}", from_trial, fork_id);
    let task_boundary = materialize_packaged_task_boundary(&prepared_manifest.declaration)?;
    validate_task_boundary_workspace_materialization(&task_boundary)?;

    let fork_trial_dir = fork_dir.join("trial_1");
    ensure_dir(&fork_trial_dir)?;
    write_trial_state(
        &fork_trial_dir,
        &fork_trial_id,
        "running",
        None,
        source_checkpoint.as_deref(),
        None,
    )?;
    let mut trial_guard = TrialStateGuard::new(&fork_trial_dir, &fork_trial_id);

    let checkpoint_workspace_ref = if let Some(ref checkpoint_token) = source_checkpoint {
        resolve_workspace_ref_from_checkpoint_token(&run_dir, checkpoint_token)?
    } else {
        None
    };
    let prepared = prepare_task_environment(
        &project_root,
        &fork_trial_dir,
        &run_id,
        &fork_trial_id,
        &runtime_experiment,
        &variant,
        prepared_manifest.task_index,
        prepared_manifest.repl_idx,
        &task_boundary,
        &agent_runtime,
        checkpoint_workspace_ref.as_deref(),
    )?;
    let PreparedTaskEnvironment {
        manifest: fork_prepared_manifest,
        trial_paths,
        io_paths: _,
        dynamic_mounts,
        trial_input: mut input,
    } = prepared;
    set_json_pointer_value(
        &mut input,
        "/ext/fork",
        json!({
            "parent_run_id": run_id,
            "parent_trial_id": from_trial,
            "selector": selector,
            "source_checkpoint": source_checkpoint.clone(),
            "strict": strict
        }),
    )?;
    set_json_pointer_value(
        &mut input,
        "/runtime/control_plane/path",
        json!(DEFAULT_CONTAINER_CONTROL_PATH),
    )?;
    set_json_pointer_value(&mut input, "/runtime/control_plane/mode", json!("file"))?;
    let input_bytes = serde_json::to_vec_pretty(&input)?;
    let fork_task_sandbox_image = fork_prepared_manifest.task_sandbox_image().to_string();
    let fork_task_sandbox_workdir = fork_prepared_manifest
        .task_sandbox_workdir()
        .unwrap_or(task_boundary.task_workdir.as_str())
        .to_string();

    let io_paths = prepare_io_paths(&trial_paths, &input_bytes)?;
    let runtime_env = build_runtime_contract_env(
        &run_id,
        &input,
        &io_paths,
        Some(fork_task_sandbox_image.as_str()),
        resolve_trial_timeout_ms(&input),
    );
    let run_request = AdapterRunRequest {
        runtime_experiment: &runtime_experiment,
        runtime: &agent_runtime,
        variant_args: &variant_args,
        runtime_env: &runtime_env,
        runtime_overrides_env: &agent_runtime_env,
        trial_paths: &trial_paths,
        dynamic_mounts: &dynamic_mounts,
        io_paths: &io_paths,
        network_mode: effective_network_mode.as_str(),
        benchmark_grader: None,
        benchmark_grading_enabled: false,
        run_id: &run_id,
        task_image: fork_task_sandbox_image.as_str(),
        task_workdir: fork_task_sandbox_workdir.as_str(),
        task_materialization_kind: task_boundary.materialization.kind.clone(),
        agent_artifact: Some(agent_runtime.agent_artifact.as_path()),
    };
    let runtime_outcome = crate::trial::execution::execute_trial_runtime(
        &fork_trial_dir,
        0,
        1,
        &run_request,
        &fork_prepared_manifest.task_id,
        &variant.id,
        fork_prepared_manifest.repl_idx,
        fork_prepared_manifest
            .task_sandbox_plan
            .as_ref()
            .ok_or_else(|| anyhow!("prepared fork task missing task sandbox plan"))?,
    )?;
    let status = runtime_outcome.agent_exit_status;
    let trial_output = runtime_outcome.trial_output;
    let result_parse_error = runtime_outcome.result_parse_error;
    let outcome = trial_output
        .get("outcome")
        .and_then(|v| v.as_str())
        .unwrap_or("error");
    if status == "0" && outcome != "error" {
        trial_guard.complete("completed", None)?;
    } else if status != "0" {
        trial_guard.complete("failed", Some("harness_exit_nonzero"))?;
    } else if result_parse_error.is_some() {
        trial_guard.complete("failed", Some("trial_output_parse_error"))?;
    } else {
        trial_guard.complete("failed", Some("trial_output_error"))?;
    }

    let replay_grade = replay_grade_for_integration(&agent_runtime.integration_level).to_string();
    let fallback_mode = "checkpoint".to_string();
    let artifact_store = ArtifactStore::new(run_dir.join("artifacts"));
    let trial_input_ref = artifact_store.put_bytes(&input_bytes)?;
    let trial_output_ref = artifact_store.put_bytes(&serde_json::to_vec_pretty(&trial_output)?)?;
    let manifest = json!({
        "schema_version": "fork_manifest_v1",
        "operation": "fork",
        "fork_id": fork_id.clone(),
        "parent_trial_id": from_trial,
        "selector": selector,
        "source_checkpoint": source_checkpoint.clone(),
        "fallback_mode": fallback_mode.clone(),
        "strict": strict,
        "integration_level": agent_runtime.integration_level.clone(),
        "replay_grade": replay_grade.clone(),
        "trial_id": fork_trial_id.clone(),
        "refs": {
            "trial_input_ref": trial_input_ref,
            "trial_output_ref": trial_output_ref,
        },
        "created_at": Utc::now().to_rfc3339(),
    });
    validate_schema_contract_value(&manifest, "fork manifest metadata")?;
    let mut store = BackingSqliteStore::open(&run_dir)?;
    store.upsert_attempt_object(
        &run_id,
        &fork_trial_id,
        0,
        1,
        "trial_input",
        &trial_input_ref,
        Some(&manifest),
    )?;
    store.upsert_attempt_object(
        &run_id,
        &fork_trial_id,
        0,
        1,
        "trial_output",
        &trial_output_ref,
        Some(&manifest),
    )?;
    store.upsert_runtime_operation(&run_id, "fork", &fork_id, &manifest)?;
    trial_paths.cleanup_scratch()?;

    Ok(ForkResult {
        fork_dir,
        fork_id,
        parent_trial_id: from_trial.to_string(),
        selector: selector.to_string(),
        strict,
        replay_grade,
        harness_status: status,
        source_checkpoint,
        fallback_mode,
    })
}

fn resolve_kill_trial_control_mode(trial_dir: &Path, trial_id: &str) -> Result<ActiveTrialControlMode> {
    let runtime_handles = runtime_trial_container_handles(trial_dir)?;
    if !runtime_handles.is_empty() {
        return Ok(ActiveTrialControlMode::RuntimeContainers);
    }
    if crate::trial::state::trial_attempt_state_exists(trial_dir) {
        return Err(anyhow!(
            "kill_missing_runtime_container: active runtime state exists for {} but no persisted container ids were found",
            trial_id
        ));
    }
    Err(anyhow!(
        "kill_missing_runtime_container: no persisted runtime state or container ids exist for {}",
        trial_id
    ))
}

enum ActiveTrialControlMode {
    RuntimeContainers,
}

fn runtime_trial_container_handles(
    trial_dir: &Path,
) -> Result<Vec<crate::backend::docker::ContainerHandle>> {
    Ok(
        crate::trial::state::load_trial_attempt_container_ids(trial_dir)?
            .into_iter()
            .map(|container_id| crate::backend::docker::ContainerHandle { container_id })
            .collect(),
    )
}

fn resolve_active_trial_control_mode(
    trial_dir: &Path,
    active: &RunControlActiveTrial,
) -> Result<ActiveTrialControlMode> {
    let runtime_handles = runtime_trial_container_handles(trial_dir)?;
    if !runtime_handles.is_empty() {
        return Ok(ActiveTrialControlMode::RuntimeContainers);
    }
    if crate::trial::state::trial_attempt_state_exists(trial_dir) {
        return Err(anyhow!(
            "pause_missing_runtime_container: active runtime state exists for {} but no persisted container ids were found",
            active.trial_id
        ));
    }
    Err(anyhow!(
        "pause_missing_runtime_container: no persisted runtime state or container ids exist for {}",
        active.trial_id
    ))
}

fn pause_trial_runtime_containers(trial_dir: &Path) -> Result<()> {
    let handles = runtime_trial_container_handles(trial_dir)?;
    if handles.is_empty() {
        return Err(anyhow!(
            "pause_missing_runtime_container: no persisted runtime containers were recorded for {}",
            trial_dir.display()
        ));
    }
    let docker = crate::backend::docker::DockerRuntime::connect()?;
    for handle in &handles {
        docker.pause_container(handle)?;
    }
    let _ = crate::trial::state::reconcile_trial_attempt_as_paused(trial_dir)?;
    Ok(())
}

pub(crate) fn kill_trial_runtime_containers_best_effort(trial_dir: &Path) -> Result<bool> {
    let handles = runtime_trial_container_handles(trial_dir)?;
    if handles.is_empty() {
        return Ok(false);
    }
    let docker = crate::backend::docker::DockerRuntime::connect()?;
    for handle in &handles {
        if let Err(err) = docker.kill_container(&handle) {
            if !err.to_string().contains("not found") {
                return Err(err);
            }
        }
        if let Err(err) = docker.remove_container(&handle, true) {
            if !err.to_string().contains("not found") {
                return Err(err);
            }
        }
    }
    let _ = crate::trial::state::reconcile_trial_attempt_as_killed(trial_dir)?;
    Ok(true)
}

fn format_trial_phase(phase: &TrialPhase) -> String {
    serde_json::to_value(phase)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{phase:?}"))
}

fn resume_trial_runtime_containers(trial_dir: &Path) -> Result<bool> {
    if !crate::trial::state::trial_attempt_state_exists(trial_dir) {
        return Ok(false);
    }
    let persisted = crate::trial::state::load_trial_attempt_state(trial_dir)?;
    if persisted.state.phase != TrialPhase::Paused {
        return Err(anyhow!(
            "resume_trial_not_paused: runtime phase is {}",
            format_trial_phase(&persisted.state.phase)
        ));
    }
    let handles: Vec<crate::backend::docker::ContainerHandle> =
        crate::trial::state::trial_attempt_container_ids(&persisted.state)
            .into_iter()
            .map(|container_id| crate::backend::docker::ContainerHandle { container_id })
            .collect();
    if handles.is_empty() {
        return Err(anyhow!(
            "resume_missing_runtime_container: paused runtime state exists for {} but no persisted container ids were found",
            trial_dir.display()
        ));
    }
    let docker = crate::backend::docker::DockerRuntime::connect()?;
    for handle in &handles {
        docker.unpause_container(handle)?;
    }
    let _ = crate::trial::state::reconcile_trial_attempt_as_resumed(trial_dir)?;
    Ok(true)
}

pub fn pause_run(
    run_dir: &Path,
    trial_id: Option<&str>,
    label: Option<&str>,
    _timeout_seconds: u64,
) -> Result<PauseResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Pause)?;
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;
    let run_control = load_json_file(&run_control_path(&run_dir))?;
    let status = run_control
        .pointer("/status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    if status != "running" {
        return Err(anyhow!("pause_non_running: run status is {}", status));
    }

    let run_id = run_control
        .pointer("/run_id")
        .and_then(|v| v.as_str())
        .unwrap_or("run")
        .to_string();
    let active_trial_ids = run_control_active_trial_ids(&run_control);
    let target_trials: Vec<String> = if let Some(id) = trial_id {
        if !active_trial_ids.iter().any(|active| active == id) {
            let active_label = if active_trial_ids.is_empty() {
                "<none>".to_string()
            } else {
                active_trial_ids.join(",")
            };
            return Err(anyhow!(
                "pause_target_not_active: active trial(s) are {}, requested {}",
                active_label,
                id
            ));
        }
        vec![id.to_string()]
    } else {
        if active_trial_ids.is_empty() {
            return Err(anyhow!("pause_no_active_trial"));
        }
        active_trial_ids.clone()
    };

    let pause_label = label.unwrap_or("pause").to_string();
    let active_by_id: HashMap<String, RunControlActiveTrial> =
        run_control_active_trials(&run_control)
            .into_iter()
            .map(|entry| (entry.trial_id.clone(), entry))
            .collect();

    let mut paused_active_trials: Vec<RunControlActiveTrial> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    let checkpoint_acked_all = true;
    let stop_acked_all = true;

    for target_trial in &target_trials {
        let Some(active) = active_by_id.get(target_trial).cloned() else {
            failures.push(format!(
                "{}: pause_missing_active_trial_metadata",
                target_trial
            ));
            continue;
        };

        let trial_dir = run_dir.join("trials").join(target_trial);
        if !trial_dir.exists() {
            failures.push(format!("{}: pause_trial_not_found", target_trial));
            continue;
        }
        let pause_requested = match resolve_active_trial_control_mode(&trial_dir, &active) {
            Ok(ActiveTrialControlMode::RuntimeContainers) => pause_trial_runtime_containers(&trial_dir),
            Err(err) => {
                failures.push(format!("{}: pause request failed ({})", target_trial, err));
                continue;
            }
        };
        if let Err(err) = pause_requested {
            failures.push(format!("{}: pause request failed ({})", target_trial, err));
            continue;
        }
        if let Err(err) = write_trial_state(
            &trial_dir,
            target_trial,
            "paused",
            Some(&pause_label),
            Some(&pause_label),
            Some("paused_by_user"),
        ) {
            failures.push(format!(
                "{}: failed to write trial_state ({})",
                target_trial, err
            ));
            continue;
        }

        paused_active_trials.push(active);
    }

    let pause_meta = RunControlPauseMetadata {
        label: pause_label.clone(),
        requested_at: Utc::now().to_rfc3339(),
        requested_by: Some("user".to_string()),
    };
    if failures.is_empty() {
        write_run_control_v2(
            &run_dir,
            &run_id,
            "paused",
            &paused_active_trials,
            Some(&pause_meta),
        )?;
        let result_trial = if target_trials.len() == 1 {
            target_trials[0].clone()
        } else {
            "multi".to_string()
        };
        return Ok(PauseResult {
            run_id,
            trial_id: result_trial,
            label: pause_label,
            checkpoint_acked: checkpoint_acked_all,
            stop_acked: stop_acked_all,
        });
    }

    let mut survivor_active_trials = run_control_active_trials(&run_control);
    let paused_trial_ids: HashSet<String> = paused_active_trials
        .iter()
        .map(|active| active.trial_id.clone())
        .collect();
    survivor_active_trials.retain(|active| !paused_trial_ids.contains(&active.trial_id));
    write_run_control_v2(
        &run_dir,
        &run_id,
        "interrupted",
        &survivor_active_trials,
        Some(&pause_meta),
    )?;
    Err(anyhow!(
        "pause_partial_failure: paused {} of {} targeted trial(s); failures: {}",
        paused_active_trials.len(),
        target_trials.len(),
        failures.join(" | ")
    ))
}

pub fn kill_run(run_dir: &Path) -> Result<KillResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Kill)?;
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;
    let run_control = load_json_file(&run_control_path(&run_dir))?;
    let status = run_control
        .pointer("/status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    match status.as_str() {
        "completed" | "failed" | "killed" => {
            return Err(anyhow!(
                "kill_terminal_status: run is already '{}', nothing to kill",
                status
            ));
        }
        _ => {}
    }

    let run_id = run_control
        .pointer("/run_id")
        .and_then(|v| v.as_str())
        .unwrap_or("run")
        .to_string();

    let active_trial_ids = run_control_active_trial_ids(&run_control);
    let active_by_id: HashMap<String, RunControlActiveTrial> =
        run_control_active_trials(&run_control)
            .into_iter()
            .map(|entry| (entry.trial_id.clone(), entry))
            .collect();
    let mut survivor_active_trials: Vec<RunControlActiveTrial> = Vec::new();
    let mut killed_trials: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    for trial_id in &active_trial_ids {
        let trial_dir = run_dir.join("trials").join(trial_id);
        let kill_result = match resolve_kill_trial_control_mode(&trial_dir, trial_id) {
            Ok(ActiveTrialControlMode::RuntimeContainers) => {
                kill_trial_runtime_containers_best_effort(&trial_dir).and_then(|killed| {
                    if killed {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "kill_missing_runtime_container: no persisted runtime containers were recorded for {}",
                            trial_id
                        ))
                    }
                })
            }
            Err(err) => Err(err),
        };
        if let Err(err) = kill_result {
            failures.push(format!("{}: kill request failed ({})", trial_id, err));
            if let Some(active) = active_by_id.get(trial_id).cloned() {
                survivor_active_trials.push(active);
            }
            continue;
        }
        if trial_dir.exists() {
            if let Err(err) = write_trial_state(
                &trial_dir,
                trial_id,
                "killed",
                None,
                None,
                Some("killed_by_user"),
            ) {
                failures.push(format!(
                    "{}: failed to write trial_state ({})",
                    trial_id, err
                ));
                continue;
            }
        }
        killed_trials.push(trial_id.clone());
    }

    if failures.is_empty() {
        write_run_control_v2(&run_dir, &run_id, "killed", &[], None)?;
        return Ok(KillResult {
            run_id,
            run_dir: run_dir.to_path_buf(),
            previous_status: status,
            killed_trials,
        });
    }

    write_run_control_v2(
        &run_dir,
        &run_id,
        "interrupted",
        &survivor_active_trials,
        None,
    )?;
    Err(anyhow!(
        "kill_partial_failure: killed {} of {} active trial(s); failures: {}",
        killed_trials.len(),
        active_trial_ids.len(),
        failures.join(" | ")
    ))
}

pub fn resume_trial(
    run_dir: &Path,
    trial_id: Option<&str>,
    label: Option<&str>,
    set_bindings: &BTreeMap<String, Value>,
    strict: bool,
) -> Result<ResumeResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Resume)?;
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;
    let run_control = load_json_file(&run_control_path(&run_dir))?;
    let status = run_control
        .pointer("/status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    if status != "paused" {
        return Err(anyhow!("resume_non_paused: run status is {}", status));
    }

    let active_trial_ids = run_control_active_trial_ids(&run_control);
    let target_trial = if let Some(id) = trial_id {
        if !active_trial_ids.is_empty() && !active_trial_ids.iter().any(|active| active == id) {
            return Err(anyhow!(
                "resume_target_not_active: active trial(s) are {}, requested {}",
                active_trial_ids.join(","),
                id
            ));
        }
        id.to_string()
    } else {
        if active_trial_ids.is_empty() {
            return Err(anyhow!("resume_no_active_trial"));
        }
        if active_trial_ids.len() > 1 {
            return Err(anyhow!(
                "resume_multiple_active_trials: {} active trials require --trial-id",
                active_trial_ids.len()
            ));
        }
        active_trial_ids[0].clone()
    };
    let trial_dir = run_dir.join("trials").join(&target_trial);
    if !trial_dir.exists() {
        return Err(anyhow!("resume_trial_not_found: {}", target_trial));
    }
    let run_id = run_control
        .pointer("/run_id")
        .and_then(|v| v.as_str())
        .unwrap_or("run")
        .to_string();

    if crate::trial::state::trial_attempt_state_exists(&trial_dir) {
        if label.is_some() || !set_bindings.is_empty() || strict {
            return Err(anyhow!(
                "resume_runtime_unpause_unsupported: live runtime resume does not support label selection, --set overrides, or --strict"
            ));
        }
        if resume_trial_runtime_containers(&trial_dir)? {
            write_trial_state(&trial_dir, &target_trial, "running", None, None, None)?;
            let active_trials = run_control_active_trials(&run_control);
            write_run_control_v2(&run_dir, &run_id, "running", &active_trials, None)?;
            return Ok(ResumeResult {
                trial_id: target_trial,
                mode: ResumeMode::RuntimeUnpause,
                selector: None,
                fork: None,
            });
        }
    }

    let trial_state_path = trial_dir.join("trial_state.json");
    if !trial_state_path.exists() {
        return Err(anyhow!(
            "resume_missing_trial_state: {}",
            trial_state_path.display()
        ));
    }
    let trial_state = load_json_file(&trial_state_path)?;
    let trial_status = trial_state
        .pointer("/status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    if trial_status != "paused" {
        return Err(anyhow!(
            "resume_trial_not_paused: trial {} status is {}",
            target_trial,
            trial_status
        ));
    }
    let pause_label = trial_state.pointer("/pause_label").and_then(|v| v.as_str());
    let selector =
        resolve_resume_selector(&run_dir, &run_id, &target_trial, label.or(pause_label))?;

    let fork = fork_trial_inner(&run_dir, &target_trial, &selector, set_bindings, strict)?;
    Ok(ResumeResult {
        trial_id: target_trial,
        mode: ResumeMode::Fork,
        selector: Some(selector),
        fork: Some(fork),
    })
}
pub(crate) fn load_trial_payload_from_attempt_objects(
    run_dir: &Path,
    run_id: &str,
    trial_id: &str,
    role: &str,
) -> Result<Option<Value>> {
    let store = BackingSqliteStore::open(run_dir)?;
    let Some(object_ref) = store.latest_attempt_object_ref(run_id, trial_id, role)? else {
        return Ok(None);
    };
    let artifact_store = ArtifactStore::new(run_dir.join("artifacts"));
    let payload = artifact_store.read_ref(&object_ref)?;
    Ok(Some(serde_json::from_slice(&payload)?))
}

pub(crate) fn load_trial_output_payload(
    run_dir: &Path,
    run_id: &str,
    trial_id: &str,
) -> Result<Value> {
    if let Some(value) =
        load_trial_payload_from_attempt_objects(run_dir, run_id, trial_id, "trial_output")?
    {
        return Ok(value);
    }
    Err(anyhow!(
        "trial output payload not found in sqlite for trial '{}'",
        trial_id
    ))
}

pub(crate) fn resolve_workspace_ref_from_checkpoint_token(
    run_dir: &Path,
    token: &str,
) -> Result<Option<String>> {
    let Some(version_id) = token.strip_prefix("lineage:") else {
        return Ok(None);
    };
    let store = BackingSqliteStore::open(run_dir)?;
    store.lineage_workspace_ref_by_version(version_id)
}

pub(crate) fn resolve_resume_selector(
    run_dir: &Path,
    run_id: &str,
    trial_id: &str,
    preferred_label: Option<&str>,
) -> Result<String> {
    let output = load_trial_output_payload(run_dir, run_id, trial_id)?;
    let checkpoints = output
        .get("checkpoints")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if checkpoints.is_empty() {
        return Err(anyhow!(
            "resume_no_checkpoint: paused trial has no declared checkpoints"
        ));
    }

    if let Some(label) = preferred_label {
        let found = checkpoints.iter().any(|cp| {
            cp.get("logical_name").and_then(|v| v.as_str()) == Some(label)
                || cp.get("path").and_then(|v| v.as_str()) == Some(label)
        });
        if !found {
            return Err(anyhow!(
                "resume_checkpoint_not_found: label '{}' was not found in trial checkpoints",
                label
            ));
        }
        return Ok(format!("checkpoint:{}", label));
    }

    let mut best_with_step: Option<(u64, Value)> = None;
    for cp in checkpoints.iter() {
        if let Some(step) = cp.get("step").and_then(|v| v.as_u64()) {
            match best_with_step {
                Some((cur, _)) if step <= cur => {}
                _ => best_with_step = Some((step, cp.clone())),
            }
        }
    }
    let chosen = if let Some((_, cp)) = best_with_step {
        cp
    } else {
        checkpoints
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("resume_no_checkpoint"))?
    };
    if let Some(name) = chosen.get("logical_name").and_then(|v| v.as_str()) {
        return Ok(format!("checkpoint:{}", name));
    }
    if let Some(path) = chosen.get("path").and_then(|v| v.as_str()) {
        return Ok(format!("checkpoint:{}", path));
    }
    Err(anyhow!("resume_no_checkpoint_token"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContractPathRoot {
    In,
    Out,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContractPathMode {
    ContainerMount,
    RuntimeEvents,
}

#[derive(Debug, Clone)]
pub(crate) struct ContractPathHostRoots {
    pub(crate) in_dir: PathBuf,
    pub(crate) out_dir: PathBuf,
    pub(crate) workspace_dir: PathBuf,
}

impl ContractPathHostRoots {
    pub(crate) fn from_trial_paths(paths: &TrialPaths) -> Self {
        Self {
            in_dir: paths.in_dir.clone(),
            out_dir: paths.out.clone(),
            workspace_dir: paths.workspace.clone(),
        }
    }

    pub(crate) fn from_trial_dir(trial_dir: &Path) -> Self {
        Self {
            in_dir: trial_dir.join("in"),
            out_dir: trial_dir.join("out"),
            workspace_dir: trial_dir.join("workspace"),
        }
    }

    fn base_for(&self, root: ContractPathRoot) -> &Path {
        match root {
            ContractPathRoot::In => self.in_dir.as_path(),
            ContractPathRoot::Out => self.out_dir.as_path(),
        }
    }
}

pub(crate) fn strip_contract_prefix<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    if path == prefix {
        return Some("");
    }
    let rest = path.strip_prefix(prefix)?;
    if rest.starts_with('/') {
        Some(rest)
    } else {
        None
    }
}

pub(crate) fn resolve_contract_path_components(path: &str) -> Option<(ContractPathRoot, &str)> {
    if let Some(rest) = strip_contract_prefix(path, AGENTLAB_CONTRACT_IN_DIR) {
        return Some((ContractPathRoot::In, rest));
    }
    if let Some(rest) = strip_contract_prefix(path, AGENTLAB_CONTRACT_OUT_DIR) {
        return Some((ContractPathRoot::Out, rest));
    }
    None
}

pub(crate) fn strip_task_workdir_placeholder_prefix(path: &str) -> Option<&str> {
    if path == AGENTLAB_TASK_WORKDIR_PLACEHOLDER {
        return Some("");
    }
    let rest = path.strip_prefix(AGENTLAB_TASK_WORKDIR_PLACEHOLDER)?;
    if rest.starts_with('/') {
        Some(rest)
    } else {
        None
    }
}

pub(crate) fn mode_allows_root(mode: ContractPathMode, root: ContractPathRoot) -> bool {
    match mode {
        ContractPathMode::ContainerMount => {
            matches!(root, ContractPathRoot::In | ContractPathRoot::Out)
        }
        ContractPathMode::RuntimeEvents => {
            matches!(root, ContractPathRoot::In | ContractPathRoot::Out)
        }
    }
}

pub(crate) fn map_contract_path_to_host(
    path: &str,
    roots: &ContractPathHostRoots,
    mode: ContractPathMode,
) -> Result<PathBuf> {
    let raw = match mode {
        ContractPathMode::ContainerMount => path.trim(),
        ContractPathMode::RuntimeEvents => path,
    };
    if raw.is_empty() {
        return Err(match mode {
            ContractPathMode::ContainerMount => anyhow!("container path is empty"),
            ContractPathMode::RuntimeEvents => anyhow!(
                "runtime event path must be absolute when resolving trial events: {}",
                raw
            ),
        });
    }
    if matches!(mode, ContractPathMode::ContainerMount) {
        if let Some(rest) = strip_task_workdir_placeholder_prefix(raw) {
            return Ok(roots.workspace_dir.join(rest.trim_start_matches('/')));
        }
    }
    if !raw.starts_with('/') {
        return Err(match mode {
            ContractPathMode::ContainerMount => anyhow!("container path must be absolute: {}", raw),
            ContractPathMode::RuntimeEvents => anyhow!(
                "runtime event path must be absolute when resolving trial events: {}",
                raw
            ),
        });
    }

    let Some((root, rest)) = resolve_contract_path_components(raw) else {
        return Err(match mode {
            ContractPathMode::ContainerMount => {
                anyhow!("unsupported container mount path: {}", raw)
            }
            ContractPathMode::RuntimeEvents => {
                anyhow!("unsupported runtime event path for trial: {}", raw)
            }
        });
    };

    if !mode_allows_root(mode, root) {
        return Err(match mode {
            ContractPathMode::ContainerMount => {
                anyhow!("unsupported container mount path: {}", raw)
            }
            ContractPathMode::RuntimeEvents => {
                anyhow!("unsupported runtime event path for trial: {}", raw)
            }
        });
    }

    Ok(roots.base_for(root).join(rest.trim_start_matches('/')))
}

pub(crate) fn resolve_event_path_for_trial(events_path: &str, trial_dir: &Path) -> Result<PathBuf> {
    map_contract_path_to_host(
        events_path,
        &ContractPathHostRoots::from_trial_dir(trial_dir),
        ContractPathMode::RuntimeEvents,
    )
}

#[cfg(test)]
pub(crate) fn read_control_seq(control_path: &Path) -> Result<u64> {
    if !control_path.exists() {
        return Ok(0);
    }
    let value = load_json_file(control_path)?;
    Ok(value.pointer("/seq").and_then(|v| v.as_u64()).unwrap_or(0))
}

#[cfg(test)]
pub(crate) fn adapter_control_ack_received(
    events_path: &Path,
    action: &str,
    control_version: &str,
) -> Result<bool> {
    if !events_path.exists() {
        return Ok(false);
    }
    let data = fs::read_to_string(events_path)?;
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parsed: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if parsed.get("event_type").and_then(|v| v.as_str()) != Some("control_ack") {
            continue;
        }
        if parsed
            .get("action_observed")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            != action
        {
            continue;
        }
        if parsed
            .get("control_version")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            == control_version
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn parse_fork_selector(selector: &str) -> Result<ForkSelector> {
    let (kind, value) = selector
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid selector '{}': expected kind:value", selector))?;
    match kind {
        "checkpoint" => {
            if value.trim().is_empty() {
                return Err(anyhow!(
                    "invalid selector '{}': checkpoint name empty",
                    selector
                ));
            }
            Ok(ForkSelector::Checkpoint(value.to_string()))
        }
        "step" => Ok(ForkSelector::Step(value.parse::<u64>().map_err(|_| {
            anyhow!("invalid selector '{}': step must be integer", selector)
        })?)),
        "event_seq" => Ok(ForkSelector::EventSeq(value.parse::<u64>().map_err(
            |_| anyhow!("invalid selector '{}': event_seq must be integer", selector),
        )?)),
        _ => Err(anyhow!(
            "invalid selector kind '{}': expected checkpoint|step|event_seq",
            kind
        )),
    }
}

pub(crate) fn resolve_selector_checkpoint(
    selector: &ForkSelector,
    trial_output: Option<&Value>,
    trial_dir: &Path,
    strict: bool,
) -> Result<Option<String>> {
    let checkpoints = trial_output
        .and_then(|v| v.get("checkpoints"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let selected = match selector {
        ForkSelector::Checkpoint(name) => checkpoints.into_iter().find(|cp| {
            cp.get("logical_name").and_then(|v| v.as_str()) == Some(name.as_str())
                || cp.get("path").and_then(|v| v.as_str()) == Some(name.as_str())
        }),
        ForkSelector::Step(step) => checkpoints
            .into_iter()
            .filter_map(|cp| {
                let cp_step = cp.get("step").and_then(|v| v.as_u64());
                cp_step.map(|s| (s, cp))
            })
            .filter(|(s, _)| *s <= *step)
            .max_by_key(|(s, _)| *s)
            .map(|(_, cp)| cp),
        ForkSelector::EventSeq(seq) => checkpoints
            .into_iter()
            .filter_map(|cp| {
                let cp_step = cp.get("step").and_then(|v| v.as_u64());
                cp_step.map(|s| (s, cp))
            })
            .filter(|(s, _)| *s <= *seq)
            .max_by_key(|(s, _)| *s)
            .map(|(_, cp)| cp),
    };

    let Some(cp) = selected else {
        if strict {
            return Err(anyhow!(
                "strict_source_unavailable: selector checkpoint not found"
            ));
        }
        return Ok(None);
    };

    if let Some(run_dir) = infer_run_dir_from_path(trial_dir) {
        let run_id = run_dir
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("run")
            .to_string();
        let trial_id = trial_dir
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow!("unable to infer trial_id from {}", trial_dir.display()))?;
        let store = BackingSqliteStore::open(&run_dir)?;
        if let Some(version_id) = store.latest_lineage_version_id_for_trial(&run_id, trial_id)? {
            return Ok(Some(format!("lineage:{}", version_id)));
        }
        if strict {
            return Err(anyhow!(
                "strict_source_unavailable: selector resolved but lineage version is unavailable"
            ));
        }
        return Ok(None);
    }

    let raw_path = cp
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("invalid checkpoint entry: missing path"))?;
    let resolved = resolve_event_path_for_trial(raw_path, trial_dir)?;
    if strict && !resolved.exists() {
        return Err(anyhow!(
            "strict_source_unavailable: checkpoint path not found {}",
            resolved.display()
        ));
    }
    if resolved.exists() {
        return Ok(Some(resolved.to_string_lossy().to_string()));
    }

    if strict {
        return Err(anyhow!(
            "strict_source_unavailable: checkpoint path not found {}",
            trial_dir.display()
        ));
    }
    Ok(None)
}

pub(crate) fn apply_variant_binding_overrides(
    variant: &mut Variant,
    set_bindings: &BTreeMap<String, Value>,
) -> Result<()> {
    if set_bindings.is_empty() {
        return Ok(());
    }
    if !variant.bindings.is_object() {
        variant.bindings = json!({});
    }
    for (key, value) in set_bindings {
        let pointer = format!("/{}", key.split('.').collect::<Vec<_>>().join("/"));
        set_json_pointer_value(&mut variant.bindings, &pointer, value.clone())?;
    }
    Ok(())
}
pub(crate) fn is_dx_contract_authoring(json_value: &Value) -> bool {
    json_value.pointer("/agent").is_some()
        || json_value.pointer("/overrides").is_some()
        || json_value.pointer("/baseline/id").is_some()
        || matches!(json_value.pointer("/benchmark"), Some(Value::String(_)))
        || json_value.pointer("/variants").is_some()
}

pub(crate) fn resolve_default_owner() -> String {
    let owner_from_git = Command::new("git")
        .args(["config", "--get", "user.name"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty());
    owner_from_git
        .or_else(|| {
            std::env::var("USER")
                .ok()
                .map(|user| user.trim().to_string())
        })
        .filter(|owner| !owner.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

pub(crate) fn tokenize_command_string(raw: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    for ch in raw.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if in_single {
            if ch == '\'' {
                in_single = false;
            } else {
                current.push(ch);
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                in_double = false;
            } else if ch == '\\' {
                escaped = true;
            } else {
                current.push(ch);
            }
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '\'' => in_single = true,
            '"' => in_double = true,
            c if c.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if escaped || in_single || in_double {
        return Err(anyhow!("agent.command has unclosed quote/escape"));
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    if tokens.is_empty() {
        return Err(anyhow!("agent.command must not be empty"));
    }
    Ok(tokens)
}

pub(crate) fn parse_dx_command_field_named(
    value: Option<&Value>,
    field: &str,
) -> Result<Vec<String>> {
    match value {
        Some(Value::String(raw)) => tokenize_command_string(raw),
        Some(Value::Array(_)) => {
            let parts = parse_string_array_field(value, field)?;
            if parts.is_empty() {
                return Err(anyhow!("{} must not be empty", field));
            }
            Ok(parts)
        }
        Some(_) => Err(anyhow!("{} must be a string or string[]", field)),
        None => Err(anyhow!("{} is required", field)),
    }
}

pub(crate) fn resolve_dx_artifact_path(raw: &str, exp_dir: &Path, project_root: &Path) -> PathBuf {
    let trimmed = raw.trim();
    let candidate = Path::new(trimmed);
    if candidate.is_absolute() {
        return normalize_path(candidate);
    }
    if trimmed.starts_with("./") || trimmed.starts_with("../") || trimmed.contains('/') {
        return normalize_path(&exp_dir.join(candidate));
    }

    let agents_root = project_root.join(".lab").join("agents");
    let direct = agents_root.join(trimmed);
    if direct.exists() {
        return normalize_path(&direct);
    }
    for ext in [".tar.gz", ".tgz", ".tar"] {
        let with_ext = agents_root.join(format!("{}{}", trimmed, ext));
        if with_ext.exists() {
            return normalize_path(&with_ext);
        }
    }
    normalize_path(&direct)
}

pub(crate) fn compute_artifact_content_digest(path: &Path) -> Result<String> {
    if path.is_file() {
        return sha256_file(path);
    }
    if !path.is_dir() {
        return Err(anyhow!(
            "artifact path must be a file or directory: {}",
            path.display()
        ));
    }

    let mut lines = Vec::new();
    for entry in walkdir::WalkDir::new(path)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let p = entry.path();
        if p == path {
            continue;
        }
        let rel = p
            .strip_prefix(path)
            .unwrap_or(p)
            .to_string_lossy()
            .replace('\\', "/");
        let meta = fs::symlink_metadata(p)?;
        if meta.file_type().is_symlink() {
            let target = fs::read_link(p)
                .map(|v| v.to_string_lossy().to_string())
                .unwrap_or_else(|_| "<unreadable>".to_string());
            lines.push(format!("L {} -> {}", rel, target));
        } else if meta.is_dir() {
            lines.push(format!("D {}", rel));
        } else if meta.is_file() {
            lines.push(format!("F {} {}", rel, sha256_file(p)?));
        }
    }
    lines.sort();
    Ok(sha256_bytes(lines.join("\n").as_bytes()))
}

pub(crate) fn agent_artifact_archive_flag(path: &Path) -> Option<&'static str> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        Some("-xzf")
    } else if name.ends_with(".tar") {
        Some("-xf")
    } else {
        None
    }
}

pub(crate) fn is_executable_file(path: &Path) -> bool {
    let Ok(meta) = fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        path.extension()
            .and_then(|value| value.to_str())
            .map(|value| {
                let lower = value.to_ascii_lowercase();
                lower == "exe" || lower == "bat" || lower == "cmd"
            })
            .unwrap_or(false)
    }
}

pub(crate) fn read_file_head(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let mut buf = vec![0u8; max_bytes];
    let read = file.read(&mut buf)?;
    buf.truncate(read);
    Ok(buf)
}

pub(crate) fn normalize_shell_token(raw: &str) -> Option<String> {
    let trimmed = raw.trim_matches(|ch: char| {
        ch == '"'
            || ch == '\''
            || ch == '`'
            || ch == ';'
            || ch == ','
            || ch == '('
            || ch == ')'
            || ch == '['
            || ch == ']'
            || ch == '{'
            || ch == '}'
    });
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    Some(trimmed.to_string())
}

pub(crate) fn token_looks_like_script_source_path(token: &str) -> bool {
    let lower = token.to_ascii_lowercase();
    AGENT_ARTIFACT_SCRIPT_SOURCE_EXTENSIONS
        .iter()
        .any(|ext| lower.ends_with(ext))
}

pub(crate) fn validate_agent_artifact_entrypoint_script(
    entrypoint_path: &Path,
    context: &str,
) -> Result<()> {
    let head = read_file_head(entrypoint_path, AGENT_ARTIFACT_ENTRYPOINT_HEAD_BYTES)?;
    if !head.starts_with(b"#!") {
        return Ok(());
    }
    let text = String::from_utf8_lossy(&head);
    let Some(_) = text.lines().next() else {
        return Ok(());
    };
    for (line_idx, line) in text.lines().take(8).enumerate() {
        let trimmed_line = line.trim_start();
        if line_idx > 0
            && !(trimmed_line.starts_with("exec ")
                || trimmed_line == "exec"
                || trimmed_line.starts_with("exec\t"))
        {
            continue;
        }
        for raw in line.split_whitespace() {
            let Some(token) = normalize_shell_token(raw) else {
                continue;
            };
            if token.starts_with("#!") {
                let shebang_target = token.trim_start_matches("#!");
                if shebang_target == "/usr/bin/env" {
                    continue;
                }
                if shebang_target.starts_with('/') && !shebang_target.starts_with("/opt/agent/") {
                    return Err(anyhow!(
                        "{} entrypoint delegates to image-resident path '{}'; only /opt/agent paths are allowed",
                        context,
                        shebang_target
                    ));
                }
                continue;
            }
            if !token.starts_with('/') {
                continue;
            }
            if token.starts_with("/opt/agent/") {
                if token_looks_like_script_source_path(&token) {
                    return Err(anyhow!(
                        "{} entrypoint delegates to readable script path '{}'; bundle a binary entrypoint instead",
                        context,
                        token
                    ));
                }
                continue;
            }
            return Err(anyhow!(
                "{} entrypoint delegates to image-resident path '{}'; only /opt/agent paths are allowed",
                context,
                token
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct CommandArtifactTarget {
    token_index: usize,
    raw_token: String,
    resolved_path: PathBuf,
}

pub(crate) fn resolve_artifact_path_from_command_token(
    root: &Path,
    token_index: usize,
    token: &str,
    context: &str,
) -> Result<Option<CommandArtifactTarget>> {
    if token.is_empty() {
        return Ok(None);
    }
    let Some(relative) = token.strip_prefix("/opt/agent/") else {
        return Ok(None);
    };
    let resolved = normalize_path(&root.join(relative));
    let root_cmp = canonicalize_best_effort(root);
    let resolved_cmp = canonicalize_best_effort(&resolved);
    if !resolved_cmp.starts_with(&root_cmp) {
        return Err(anyhow!(
            "{} runtime.command[{}] escapes artifact root: '{}'",
            context,
            token_index,
            token
        ));
    }
    if !resolved.exists() {
        return Err(anyhow!(
            "{} runtime.command[{}] references artifact path '{}' but it does not exist in {}",
            context,
            token_index,
            token,
            root.display()
        ));
    }
    Ok(Some(CommandArtifactTarget {
        token_index,
        raw_token: token.to_string(),
        resolved_path: resolved,
    }))
}

pub(crate) fn resolve_command_artifact_targets(
    root: &Path,
    command: &[String],
    context: &str,
) -> Result<Vec<CommandArtifactTarget>> {
    if command.is_empty() {
        return Err(anyhow!("{} runtime.command must not be empty", context));
    }

    let mut targets = Vec::new();
    let mut first_bin_candidate: Option<(String, PathBuf)> = None;

    let first = command[0].trim();
    if let Some(target) = resolve_artifact_path_from_command_token(root, 0, first, context)? {
        targets.push(target);
    } else if !first.contains('/') {
        let candidate = normalize_path(&root.join("bin").join(first));
        first_bin_candidate = Some((first.to_string(), candidate.clone()));
        if candidate.exists() {
            targets.push(CommandArtifactTarget {
                token_index: 0,
                raw_token: first.to_string(),
                resolved_path: candidate,
            });
        }
    }

    for (idx, token) in command.iter().enumerate().skip(1) {
        if let Some(target) =
            resolve_artifact_path_from_command_token(root, idx, token.trim(), context)?
        {
            targets.push(target);
        }
    }

    if targets.is_empty() {
        if let Some((token, candidate)) = first_bin_candidate {
            return Err(anyhow!(
                "{} runtime.command[0] '{}' did not resolve to artifact executable {} and no explicit /opt/agent paths were referenced",
                context,
                token,
                candidate.display()
            ));
        }
        return Err(anyhow!(
            "{} runtime.command does not reference the mounted artifact; point it at /opt/agent/... or a binary under /opt/agent/bin",
            context
        ));
    }

    Ok(targets)
}

pub(crate) fn validate_agent_artifact_root(
    root: &Path,
    command: &[String],
    context: &str,
) -> Result<()> {
    if !root.is_dir() {
        return Err(anyhow!(
            "{} artifact root must be a directory: {}",
            context,
            root.display()
        ));
    }
    let targets = resolve_command_artifact_targets(root, command, context)?;
    if let Some(primary) = targets.iter().find(|target| target.token_index == 0) {
        if !is_executable_file(&primary.resolved_path) {
            return Err(anyhow!(
                "{} runtime.command[0] '{}' is not executable inside artifact: {}",
                context,
                primary.raw_token,
                primary.resolved_path.display()
            ));
        }
        validate_agent_artifact_entrypoint_script(&primary.resolved_path, context)?;
    }
    Ok(())
}

pub(crate) fn validate_agent_artifact_path(
    path: &Path,
    command: &[String],
    context: &str,
) -> Result<()> {
    if path.is_dir() {
        return validate_agent_artifact_root(path, command, context);
    }
    if !path.is_file() {
        return Err(anyhow!(
            "{} artifact path is not a file or directory: {}",
            context,
            path.display()
        ));
    }
    let Some(tar_flag) = agent_artifact_archive_flag(path) else {
        return Err(anyhow!(
            "{} artifact archive must use .tar/.tar.gz/.tgz: {}",
            context,
            path.display()
        ));
    };
    let staging_dir = env::temp_dir().join(format!(
        "agentlab_artifact_validate_{}_{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    ));
    ensure_dir(&staging_dir)?;
    let artifact_arg = path.to_string_lossy().to_string();
    let staging_arg = staging_dir.to_string_lossy().to_string();
    let unpack_out = Command::new("tar")
        .args([tar_flag, artifact_arg.as_str(), "-C", staging_arg.as_str()])
        .output()?;
    if !unpack_out.status.success() {
        let _ = fs::remove_dir_all(&staging_dir);
        return Err(anyhow!(
            "{} failed to unpack artifact archive {}: {}",
            context,
            path.display(),
            output_error_detail(&unpack_out)
        ));
    }
    let validation = validate_agent_artifact_root(&staging_dir, command, context);
    let _ = fs::remove_dir_all(&staging_dir);
    validation
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeArtifactValidationSpec {
    pointer: String,
    artifact_path: String,
    command: Vec<String>,
}

pub(crate) fn parse_optional_command_field_named(
    value: Option<&Value>,
    field: &str,
) -> Result<Option<Vec<String>>> {
    match value {
        Some(Value::String(raw)) => Ok(Some(tokenize_command_string(raw)?)),
        Some(Value::Array(_)) => {
            let parts = parse_string_array_field(value, field)?;
            if parts.is_empty() {
                return Err(anyhow!("{} must not be empty", field));
            }
            Ok(Some(parts))
        }
        Some(_) => Err(anyhow!("{} must be a string or string[]", field)),
        None => Ok(None),
    }
}

pub(crate) fn command_for_artifact_validation(
    agent: Option<&Value>,
    field_prefix: &str,
    fallback: Option<&Vec<String>>,
) -> Result<Option<Vec<String>>> {
    let local = parse_optional_command_field_named(
        agent.and_then(|value| value.get("command")),
        &format!("{}/command", field_prefix),
    )?;
    if local.is_some() {
        return Ok(local);
    }
    Ok(fallback.cloned())
}

pub(crate) fn collect_runtime_artifact_validation_specs(
    experiment: &Value,
) -> Result<Vec<RuntimeArtifactValidationSpec>> {
    let root_agent = experiment.pointer("/runtime/agent_runtime");
    let root_command = command_for_artifact_validation(root_agent, "/runtime/agent_runtime", None)?;
    let mut specs = Vec::new();

    let mut push_spec =
        |pointer: String, agent: Option<&Value>, fallback: Option<&Vec<String>>| -> Result<()> {
            let Some(path) = agent
                .and_then(|value| value.get("artifact"))
                .and_then(Value::as_str)
            else {
                return Ok(());
            };
            let command = command_for_artifact_validation(
                agent,
                pointer.trim_end_matches("/artifact"),
                fallback,
            )?
            .ok_or_else(|| anyhow!("{} requires a command to validate artifact usage", pointer))?;
            specs.push(RuntimeArtifactValidationSpec {
                pointer,
                artifact_path: path.to_string(),
                command,
            });
            Ok(())
        };

    push_spec(
        "/runtime/agent_runtime/artifact".to_string(),
        root_agent,
        None,
    )?;
    push_spec(
        "/baseline/runtime_overrides/agent_runtime/artifact".to_string(),
        experiment.pointer("/baseline/runtime_overrides/agent_runtime"),
        root_command.as_ref(),
    )?;

    if let Some(variant_plan) = experiment
        .pointer("/variant_plan")
        .and_then(Value::as_array)
    {
        for (idx, variant) in variant_plan.iter().enumerate() {
            push_spec(
                format!(
                    "/variant_plan/{}/runtime_overrides/agent_runtime/artifact",
                    idx
                ),
                variant.pointer("/runtime_overrides/agent_runtime"),
                root_command.as_ref(),
            )?;
        }
    }
    if let Some(variants) = experiment.pointer("/variants").and_then(Value::as_array) {
        for (idx, variant) in variants.iter().enumerate() {
            push_spec(
                format!("/variants/{}/runtime_overrides/agent_runtime/artifact", idx),
                variant.pointer("/runtime_overrides/agent_runtime"),
                root_command.as_ref(),
            )?;
        }
    }

    Ok(specs)
}

pub(crate) fn validate_packaged_runtime_artifacts(
    package_dir: &Path,
    experiment: &Value,
) -> Result<()> {
    let mut seen_specs = HashSet::new();
    for spec in collect_runtime_artifact_validation_specs(experiment)? {
        let trimmed = spec.artifact_path.trim();
        if trimmed.is_empty() {
            continue;
        }
        let dedupe_key = format!("{}\u{0}{}", trimmed, spec.command.join("\u{1}"));
        if !seen_specs.insert(dedupe_key) {
            continue;
        }
        let artifact_path =
            resolve_package_path_under_root(package_dir, trimmed, spec.pointer.as_str())?;
        let context = format!("runtime artifact {} ({})", trimmed, spec.pointer);
        validate_agent_artifact_path(&artifact_path, &spec.command, context.as_str())?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct DxResolvedAgentBuild {
    artifact_raw: String,
    artifact_path: PathBuf,
    artifact_digest: String,
    image: String,
    command_base: Vec<String>,
    command: Vec<String>,
    env_base: BTreeMap<String, String>,
    env: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub(crate) struct DxVariantSpec {
    id: String,
    baseline: bool,
    agent_ref: String,
    config: Value,
    env: BTreeMap<String, String>,
}

pub(crate) fn uses_new_variant_agent_model(json_value: &Value) -> bool {
    if matches!(json_value.pointer("/agent_builds"), Some(Value::Array(_))) {
        return true;
    }
    let Some(Value::Array(variants)) = json_value.pointer("/variants") else {
        return false;
    };
    variants.iter().any(|variant| {
        variant.get("agent_ref").is_some()
            || variant.get("config").is_some()
            || variant.get("baseline").is_some()
    })
}

pub(crate) fn reject_removed_dx_agent_fields(root: &Value, root_name: &str) -> Result<()> {
    let removed = [
        ("arg_map", "put public argv directly in agent.command using $binding placeholders"),
        (
            "bindings_to_args",
            "put public argv directly in agent.command using $binding placeholders",
        ),
        (
            "default_config",
            "package agent config inside the agent artifact; authored override file wiring is not supported",
        ),
        (
            "config_files",
            "package agent config inside the agent artifact; authored host-path staging is not supported",
        ),
        ("provider_env", "bind runtime values directly with $NAME in agent.command or agent.env"),
        (
            "support_files",
            "package support files inside the agent artifact; authored host-path staging is not supported",
        ),
        ("env_from_host", "bind runtime values directly with $NAME in agent.command or agent.env"),
    ];
    for (field, guidance) in removed {
        if root.get(field).is_some() {
            return Err(anyhow!(
                "{}.{} was removed in the hard cutover; {}",
                root_name,
                field,
                guidance
            ));
        }
    }
    Ok(())
}

pub(crate) fn contains_removed_runtime_template(raw: &str) -> bool {
    raw.contains("${")
}

pub(crate) fn resolve_existing_public_path_reference(
    raw: &str,
    exp_dir: &Path,
    field_name: &str,
) -> Result<Option<PathBuf>> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.starts_with('/')
        || trimmed.starts_with('-')
        || trimmed.starts_with(AGENTLAB_TASK_WORKDIR_PLACEHOLDER)
        || trimmed.contains('$')
        || trimmed.contains("://")
    {
        return Ok(None);
    }
    let rel = validate_dx_support_file_relpath(trimmed, field_name)?;
    let resolved = normalize_path(&exp_dir.join(&rel));
    match fs::metadata(&resolved) {
        Ok(_) => Ok(Some(PathBuf::from(rel))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if trimmed.starts_with("./") || trimmed.contains('/') {
                return Err(anyhow!(
                    "{} public path '{}' resolved to missing source '{}'",
                    field_name,
                    trimmed,
                    resolved.display()
                ));
            }
            Ok(None)
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "failed to read {} public path reference '{}' resolved to '{}'",
                field_name,
                trimmed,
                resolved.display()
            )
        }),
    }
}

pub(crate) fn validate_dx_command_and_env_surface(
    command: &[String],
    env: &BTreeMap<String, String>,
    root_name: &str,
    exp_dir: &Path,
) -> Result<()> {
    for (idx, token) in command.iter().enumerate() {
        let field = format!("{}.command[{}]", root_name, idx);
        if contains_removed_runtime_template(token) {
            return Err(anyhow!(
                "{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                field
            ));
        }
        if token.trim().starts_with("/agentlab/") {
            return Err(anyhow!(
                "{} leaks runner topology; remove internal /agentlab paths from public authoring",
                field
            ));
        }
        if idx > 0 {
            let _ = resolve_existing_public_path_reference(token, exp_dir, &field)?;
        }
    }
    for (key, value) in env {
        let field = format!("{}.env.{}", root_name, key);
        if contains_removed_runtime_template(value) {
            return Err(anyhow!(
                "{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                field
            ));
        }
        if value.trim().starts_with("/agentlab/") {
            return Err(anyhow!(
                "{} leaks runner topology; remove internal /agentlab paths from public authoring",
                field
            ));
        }
        let _ = resolve_existing_public_path_reference(value, exp_dir, &field)?;
    }
    Ok(())
}

pub(crate) fn validate_dx_support_file_relpath(raw: &str, field_name: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} must not be empty", field_name));
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err(anyhow!("{} must be relative", field_name));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(seg) => normalized.push(seg),
            Component::ParentDir => {
                return Err(anyhow!("{} cannot contain '..'", field_name));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(anyhow!("{} must be relative", field_name));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(anyhow!("{} cannot resolve to empty", field_name));
    }
    Ok(normalized.to_string_lossy().replace('\\', "/"))
}

pub(crate) fn dx_runtime_asset_value(build_source_path: &Path, runtime_path: &str) -> Value {
    json!({
        "build_source_path": build_source_path.to_string_lossy().to_string(),
        "runtime_path": runtime_path,
        "required": true,
        "read_only": true
    })
}

pub(crate) fn parse_dx_agent_build(
    root: &Value,
    root_name: &str,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<DxResolvedAgentBuild> {
    reject_removed_dx_agent_fields(root, root_name)?;
    let artifact_raw = root
        .get("artifact")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("{}.artifact is required", root_name))?
        .to_string();
    let artifact_path = resolve_dx_artifact_path(&artifact_raw, exp_dir, project_root);
    fs::metadata(&artifact_path).with_context(|| {
        format!(
            "failed to read {}.artifact source path '{}' (artifact value '{}')",
            root_name,
            artifact_path.display(),
            artifact_raw
        )
    })?;
    let artifact_digest = compute_artifact_content_digest(&artifact_path)?;
    let command_base =
        parse_dx_command_field_named(root.get("command"), &format!("{}.command", root_name))?;
    let command = command_base.clone();
    let image = root
        .get("image")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("{}.image is required", root_name))?
        .to_string();
    let env_base = parse_string_map_field(root.get("env"), &format!("{}.env", root_name))?;
    let env = env_base.clone();
    validate_dx_command_and_env_surface(&command_base, &env_base, root_name, exp_dir)?;
    Ok(DxResolvedAgentBuild {
        artifact_raw,
        artifact_path,
        artifact_digest,
        image,
        command_base,
        command,
        env_base,
        env,
    })
}

pub(crate) fn runtime_override_for_variant_build(
    build: &DxResolvedAgentBuild,
    variant_env: &BTreeMap<String, String>,
) -> Value {
    let mut merged_env = build.env.clone();
    for (key, value) in variant_env {
        merged_env.insert(key.clone(), value.clone());
    }
    json!({
        "agent_runtime": {
            "command": build.command.clone(),
            "artifact": build.artifact_path.to_string_lossy().to_string(),
            "artifact_digest": build.artifact_digest.clone(),
            "artifact_resolved_path": build.artifact_path.to_string_lossy().to_string(),
            "image": build.image.clone(),
            "env": merged_env
        }
    })
}

pub(crate) fn builtin_benchmark_assets_root() -> Result<PathBuf> {
    let candidate = normalize_path(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.."));
    if candidate.join("bench").exists() && candidate.join("adapters").exists() {
        return Ok(candidate);
    }
    Err(anyhow!(
        "failed to resolve built-in benchmark assets root from {}",
        candidate.display()
    ))
}

pub(crate) fn rewrite_new_variant_agent_model(
    json_value: &Value,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<Value> {
    let mut rewritten = json_value.clone();
    let mut builds_by_id: BTreeMap<String, DxResolvedAgentBuild> = BTreeMap::new();

    if let Some(agent_builds) = json_value.pointer("/agent_builds") {
        let items = agent_builds
            .as_array()
            .ok_or_else(|| anyhow!("agent_builds must be an array"))?;
        if items.is_empty() {
            return Err(anyhow!("agent_builds must include at least one build"));
        }
        for (idx, item) in items.iter().enumerate() {
            let item_obj = item
                .as_object()
                .ok_or_else(|| anyhow!("agent_builds[{}] must be an object", idx))?;
            let id = item_obj
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| anyhow!("agent_builds[{}].id is required", idx))?
                .to_string();
            if builds_by_id.contains_key(&id) {
                return Err(anyhow!("agent_builds contains duplicate id '{}'", id));
            }
            let parsed = parse_dx_agent_build(
                item,
                &format!("agent_builds[{}]", idx),
                exp_dir,
                project_root,
            )?;
            builds_by_id.insert(id, parsed);
        }
    } else {
        let legacy_agent = json_value
            .pointer("/agent")
            .ok_or_else(|| anyhow!("agent_builds is required when agent section is missing"))?;
        let parsed = parse_dx_agent_build(legacy_agent, "agent", exp_dir, project_root)?;
        builds_by_id.insert("default".to_string(), parsed);
    }

    let variants = json_value
        .pointer("/variants")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("variants must be an array"))?;
    if variants.is_empty() {
        return Err(anyhow!("variants must include at least one entry"));
    }

    let default_build_ref = if builds_by_id.len() == 1 {
        builds_by_id.keys().next().cloned()
    } else {
        None
    };

    let mut parsed_variants = Vec::with_capacity(variants.len());
    for (idx, item) in variants.iter().enumerate() {
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("variants[{}].id is required", idx))?
            .to_string();
        let baseline = item
            .get("baseline")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let config = item
            .get("config")
            .or_else(|| item.get("bindings"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !config.is_object() {
            return Err(anyhow!("variants[{}].config must be an object", idx));
        }
        let env = parse_string_map_field(item.get("env"), &format!("variants[{}].env", idx))?;
        let agent_ref = item
            .get("agent_ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string)
            .or_else(|| default_build_ref.clone())
            .ok_or_else(|| {
                anyhow!(
                    "variants[{}].agent_ref is required when multiple agent_builds are declared",
                    idx
                )
            })?;
        if !builds_by_id.contains_key(&agent_ref) {
            return Err(anyhow!(
                "variants[{}].agent_ref '{}' does not match any agent_builds[].id",
                idx,
                agent_ref
            ));
        }
        parsed_variants.push(DxVariantSpec {
            id,
            baseline,
            agent_ref,
            config,
            env,
        });
    }

    let baseline_indices = parsed_variants
        .iter()
        .enumerate()
        .filter_map(|(idx, variant)| variant.baseline.then_some(idx))
        .collect::<Vec<_>>();
    let baseline_idx = if baseline_indices.len() == 1 {
        baseline_indices[0]
    } else if baseline_indices.is_empty() && parsed_variants.len() == 1 {
        0
    } else if baseline_indices.is_empty() {
        return Err(anyhow!(
            "exactly one variants[].baseline=true is required when more than one variant is declared"
        ));
    } else {
        return Err(anyhow!(
            "exactly one variants[].baseline=true is required (found {})",
            baseline_indices.len()
        ));
    };

    let baseline_variant = parsed_variants[baseline_idx].clone();
    let baseline_build = builds_by_id
        .get(&baseline_variant.agent_ref)
        .ok_or_else(|| anyhow!("internal error: baseline agent build missing"))?;

    let mut baseline_agent_env = baseline_build.env_base.clone();
    for (key, value) in &baseline_variant.env {
        baseline_agent_env.insert(key.clone(), value.clone());
    }
    let baseline_agent = json!({
        "artifact": baseline_build.artifact_raw.clone(),
        "image": baseline_build.image.clone(),
        "command": baseline_build.command_base.clone(),
        "env": baseline_agent_env,
    });
    set_json_pointer_value(&mut rewritten, "/agent", baseline_agent)?;
    set_json_pointer_value(
        &mut rewritten,
        "/baseline",
        json!({
            "id": baseline_variant.id,
            "bindings": baseline_variant.config,
        }),
    )?;

    let mut treatment_variants = Vec::new();
    for (idx, variant) in parsed_variants.iter().enumerate() {
        if idx == baseline_idx {
            continue;
        }
        let mut entry = json!({
            "id": variant.id,
            "bindings": variant.config,
            "agent_ref": variant.agent_ref,
        });
        let variant_build = builds_by_id
            .get(&variant.agent_ref)
            .ok_or_else(|| anyhow!("internal error: missing build for variant {}", variant.id))?;
        if variant.agent_ref != baseline_variant.agent_ref || !variant.env.is_empty() {
            set_json_pointer_value(
                &mut entry,
                "/runtime_overrides",
                runtime_override_for_variant_build(variant_build, &variant.env),
            )?;
        }
        treatment_variants.push(entry);
    }
    set_json_pointer_value(
        &mut rewritten,
        "/variants",
        Value::Array(treatment_variants),
    )?;
    if rewritten.pointer("/agent_builds").is_some() {
        set_json_pointer_value(&mut rewritten, "/agent_builds", Value::Null)?;
    }
    Ok(rewritten)
}

pub(crate) fn resolve_builtin_benchmark_dataset_path(
    json_value: &Value,
    builtin_benchmark: &str,
    project_root: &Path,
) -> Result<String> {
    if let Some(dataset) = json_value.pointer("/dataset") {
        require_exact_object_keys(dataset, &["path"], "dataset")?;
        let path = dataset
            .pointer("/path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("dataset.path must be a non-empty string"))?;
        return Ok(path.to_string());
    }
    let default_name = match builtin_benchmark {
        "bench_v0" => "bench_v0.task_spec.jsonl",
        "swebench_lite_curated" => "swebench_lite_curated.task_spec.jsonl",
        _ => unreachable!(),
    };
    Ok(project_root
        .join(".lab")
        .join("experiments")
        .join("data")
        .join(default_name)
        .to_string_lossy()
        .to_string())
}

pub(crate) fn normalize_experiment_authoring(
    json_value: Value,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<Value> {
    if !is_dx_contract_authoring(&json_value) {
        return Ok(json_value);
    }
    let mut json_value = json_value;
    if uses_new_variant_agent_model(&json_value) {
        json_value = rewrite_new_variant_agent_model(&json_value, exp_dir, project_root)?;
    }

    let experiment_id = json_value
        .pointer("/experiment/id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("experiment.id is required"))?
        .to_string();
    let experiment_name = json_value
        .pointer("/experiment/name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| experiment_id.clone());
    let experiment_description = json_value
        .pointer("/experiment/description")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    let experiment_tags =
        parse_string_array_field(json_value.pointer("/experiment/tags"), "experiment.tags")?;
    let owner = json_value
        .pointer("/experiment/owner")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(resolve_default_owner);

    let benchmark_name = json_value
        .pointer("/benchmark")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("benchmark is required and must be a non-empty string"))?;
    let builtin_benchmark = match benchmark_name {
        "bench_v0" => "bench_v0",
        "swebench_lite" | "swebench-lite" | "swebench_lite_curated" | "swebench-lite-curated" => {
            "swebench_lite_curated"
        }
        other => {
            return Err(anyhow!(
                "unknown benchmark '{}': supported built-ins are 'bench_v0' and 'swebench_lite_curated' (alias: 'swebench_lite')",
                other
            ));
        }
    };

    let baseline_id = json_value
        .pointer("/baseline/id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("baseline.id is required"))?
        .to_string();
    let baseline_bindings = json_value
        .pointer("/baseline/bindings")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !baseline_bindings.is_object() {
        return Err(anyhow!("baseline.bindings must be an object"));
    }

    let mut variant_plan = Vec::new();
    if let Some(items) = json_value.pointer("/variants") {
        let arr = items
            .as_array()
            .ok_or_else(|| anyhow!("variants must be an array"))?;
        for (idx, item) in arr.iter().enumerate() {
            let id = item
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| anyhow!("variants[{}].id is required", idx))?;
            let bindings = item.get("bindings").cloned().unwrap_or_else(|| json!({}));
            if !bindings.is_object() {
                return Err(anyhow!("variants[{}].bindings must be an object", idx));
            }
            let mut variant_entry = json!({
                "variant_id": id,
                "bindings": bindings
            });
            if let Some(runtime_overrides) = item.get("runtime_overrides") {
                if !runtime_overrides.is_object() {
                    return Err(anyhow!(
                        "variants[{}].runtime_overrides must be an object",
                        idx
                    ));
                }
                set_json_pointer_value(
                    &mut variant_entry,
                    "/runtime_overrides",
                    runtime_overrides.clone(),
                )?;
            }
            variant_plan.push(variant_entry);
        }
    }

    let has_variant_plan = !variant_plan.is_empty();
    let comparison = if has_variant_plan { "paired" } else { "none" };
    let scheduling = if has_variant_plan {
        "paired_interleaved"
    } else {
        "variant_sequential"
    };
    let builtin_assets_root = builtin_benchmark_assets_root()?;
    let dataset_path =
        resolve_builtin_benchmark_dataset_path(&json_value, builtin_benchmark, project_root)?;

    let agent_root = json_value
        .pointer("/agent")
        .ok_or_else(|| anyhow!("agent section is required"))?;
    let agent_build = parse_dx_agent_build(agent_root, "agent", exp_dir, project_root)?;
    let (
        dataset_suite_id,
        dataset_split_id,
        metrics,
        benchmark_policy,
        benchmark_grader_command,
        benchmark_grader_runtime_assets,
    ) = match builtin_benchmark {
        "bench_v0" => (
            "bench_v0",
            "test",
            json!([
                { "id": "duration_ms", "source": "runner", "weight": 0, "primary": false },
                { "id": "turn_count", "source": "runner", "weight": 0, "primary": false },
                { "id": "resolved", "source": "output", "json_pointer": "/metrics/resolved", "weight": 1, "direction": "maximize", "primary": true },
                { "id": "hidden_cases_passed", "source": "output", "json_pointer": "/metrics/hidden_cases_passed", "weight": 0, "primary": false },
                { "id": "hidden_cases_total", "source": "output", "json_pointer": "/metrics/hidden_cases_total", "weight": 0, "primary": false }
            ]),
            json!({
                "task_model": "independent",
                "evaluator_mode": "custom",
                "scoring_lifecycle": "predict_then_score",
                "chain_failure_policy": "continue_with_flag"
            }),
            json!([
                "python3",
                task_workdir_support_destination_path(
                    "bench/integration/agentlab/bench_benchmark_adapter.py"
                )
            ]),
            json!([dx_runtime_asset_value(
                &builtin_assets_root.join("bench"),
                &task_workdir_support_destination_path("bench")
            )]),
        ),
        "swebench_lite_curated" => (
            "swebench_lite_curated",
            "test",
            json!([
                { "id": "duration_ms", "source": "runner", "weight": 0, "primary": false },
                { "id": "turn_count", "source": "runner", "weight": 0, "primary": false },
                { "id": "success", "source": "output", "json_pointer": "/metrics/success", "weight": 1, "direction": "maximize", "primary": true }
            ]),
            json!({
                "task_model": "independent",
                "evaluator_mode": "custom",
                "scoring_lifecycle": "integrated_score",
                "chain_failure_policy": "continue_with_flag"
            }),
            json!([
                "python3",
                task_workdir_support_destination_path("swebench/swebench_task_container_grader.py")
            ]),
            json!([dx_runtime_asset_value(
                &builtin_assets_root.join("adapters").join("swebench"),
                &task_workdir_support_destination_path("swebench")
            )]),
        ),
        _ => unreachable!(),
    };

    let timeout_ms = json_value
        .pointer("/timeout_ms")
        .or_else(|| json_value.pointer("/agent/timeout_ms"))
        .and_then(Value::as_u64)
        .unwrap_or(600_000);
    let network_mode = json_value
        .pointer("/overrides/network")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("none")
        .to_string();
    if network_mode != "none" && network_mode != "full" && network_mode != "allowlist_enforced" {
        return Err(anyhow!(
            "overrides.network must be one of: none, full, allowlist_enforced (got '{}')",
            network_mode
        ));
    }
    let limit = json_value.pointer("/limit").and_then(Value::as_u64);
    let replications = json_value
        .pointer("/replications")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .max(1);
    let random_seed = json_value
        .pointer("/random_seed")
        .and_then(Value::as_u64)
        .unwrap_or(1);
    let max_concurrency = json_value
        .pointer("/concurrency")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .max(1);

    let mut resolved = json!({
        "experiment": {
            "id": experiment_id,
            "name": experiment_name,
            "owner": owner,
            "workload_type": "agent_runtime",
            "tags": experiment_tags
        },
        "dataset": {
            "provider": "local_jsonl",
            "path": dataset_path,
            "suite_id": dataset_suite_id,
            "split_id": dataset_split_id
        },
        "design": {
            "sanitization_profile": "hermetic_functional",
            "comparison": comparison,
            "replications": replications,
            "random_seed": random_seed,
            "shuffle_tasks": true,
            "max_concurrency": max_concurrency,
            "policies": {
                "scheduling": scheduling,
                "retry": {
                    "max_attempts": 1
                }
            }
        },
        "metrics": metrics,
        "baseline": {
            "variant_id": baseline_id,
            "bindings": baseline_bindings
        },
        "benchmark": {
            "policy": benchmark_policy,
            "grader": {
                "command": benchmark_grader_command,
                "_runtime_assets": benchmark_grader_runtime_assets
            }
        },
        "runtime": {
            "agent_runtime": {
                "command": agent_build.command.clone(),
                "artifact": agent_build.artifact_path.to_string_lossy().to_string(),
                "artifact_digest": agent_build.artifact_digest.clone(),
                "artifact_resolved_path": agent_build.artifact_path.to_string_lossy().to_string(),
                "image": agent_build.image.clone(),
                "env": agent_build.env.clone(),
                "network": network_mode
            }
        },
        "policy": {
            "timeout_ms": timeout_ms,
            "task_sandbox": {
                "profile": if benchmark_name == "swebench_lite_curated" { "swebench_testbed" } else { "default" },
                "network": network_mode
            }
        },
        "validity": {
            "fail_on_state_leak": true,
            "fail_on_profile_invariant_violation": true
        }
    });
    if let Some(description) = experiment_description {
        set_json_pointer_value(&mut resolved, "/experiment/description", json!(description))?;
    }
    if let Some(limit) = limit {
        set_json_pointer_value(&mut resolved, "/dataset/limit", json!(limit))?;
    }
    if !variant_plan.is_empty() {
        set_json_pointer_value(&mut resolved, "/variant_plan", Value::Array(variant_plan))?;
    }
    Ok(resolved)
}
pub(crate) fn configured_network_mode(json_value: &Value) -> Result<String> {
    json_value
        .pointer("/policy/task_sandbox/network")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
        .ok_or_else(|| anyhow!("missing /policy/task_sandbox/network"))
}
pub(crate) fn stage_benchmark_trial_preflight(
    benchmark_config: &BenchmarkConfig,
    trial_dir: &Path,
    run_id: &str,
    trial_id: &str,
    schedule_idx: usize,
    variant_id: &str,
    task_payload: &Value,
    environment_image: Option<&str>,
    trial_input_path: &Path,
) -> Result<()> {
    if benchmark_config.grader.is_none() {
        return Ok(());
    }

    let task_id = task_payload
        .get("id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("benchmark preflight: task payload missing non-empty id"))?;
    let environment_image = environment_image
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    let grading_enabled = task_grading_enabled(task_payload);
    if !grading_enabled {
        return Err(anyhow!(
            "benchmark preflight: grading.enabled=false was removed in Milestone 4; every benchmark task must emit mapped_grader_output.json"
        ));
    }

    let frozen_dir = trial_dir
        .join("artifacts")
        .join("benchmark_frozen_agent_input");
    ensure_dir(&frozen_dir)?;
    let frozen_input_path = frozen_dir.join("trial_input.json");
    fs::copy(trial_input_path, &frozen_input_path)?;
    let frozen_input_digest = sha256_file(&frozen_input_path)?;

    let preflight = json!({
        "schema_version": "benchmark_trial_preflight_v1",
        "run_id": run_id,
        "trial_id": trial_id,
        "schedule_idx": schedule_idx,
        "variant_id": variant_id,
        "task_id": task_id,
        "environment_image": environment_image,
        "grading": {
            "enabled": grading_enabled,
        },
        "frozen_agent_artifacts": {
            "trial_input_path": frozen_input_path,
            "trial_input_digest": frozen_input_digest,
        },
        "checked_at": Utc::now().to_rfc3339(),
    });
    atomic_write_json_pretty(&trial_dir.join("benchmark_preflight.json"), &preflight)
}
