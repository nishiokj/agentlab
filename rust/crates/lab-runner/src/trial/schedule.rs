use anyhow::{anyhow, Result};
use chrono::Utc;
use lab_core::{ensure_dir, ArtifactStore};
use lab_hooks::{load_manifest, validate_hooks};
use lab_schemas::compile_schema;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::config::*;
use crate::experiment::runtime::{resolve_exec_digest, VariantRuntimeProfile};
use crate::model::*;
use crate::persistence::journal::append_jsonl;
use crate::persistence::journal::RunSink;
use crate::persistence::rows::TrialRecord;
use crate::persistence::store::SqliteRunStore as BackingSqliteStore;
use crate::trial::artifacts::trial_output_payload_view;
use crate::trial::events::{build_metric_rows, build_variant_snapshot_rows, load_event_rows};
use crate::trial::execution::AdapterRunRequest;
use crate::trial::grade::{mapped_grader_output_state, task_grading_enabled};
use crate::trial::layout::{
    materialize_trial_runtime_layout, resolve_agent_runtime_manifest_path, write_state_inventory,
};
use crate::trial::preflight::stage_benchmark_trial_preflight;
use crate::trial::prepare::{
    prepare_task_environment, prepare_task_environment_with_paths, PreparedTaskEnvironment,
    TrialPaths,
};
use crate::trial::spec::{parse_task_boundary_from_packaged_task, TaskBoundaryMaterialization};
use crate::trial::state::{write_trial_state, TrialStateGuard};

pub(crate) struct ScheduledTrialRequest<'a> {
    pub(crate) run_dir: &'a Path,
    pub(crate) run_id: &'a str,
    pub(crate) workload_type: &'a str,
    pub(crate) project_root: &'a Path,
    pub(crate) variants: &'a [Variant],
    pub(crate) tasks: &'a [Value],
    pub(crate) schedule_idx: usize,
    pub(crate) slot: &'a TrialSlot,
    pub(crate) policy_config: &'a PolicyConfig,
    pub(crate) benchmark_config: &'a BenchmarkConfig,
    pub(crate) variant_runtime_profiles: &'a [VariantRuntimeProfile],
    pub(crate) materialize_mode: MaterializationMode,
    pub(crate) precomputed_trial_paths: Option<TrialPaths>,
    pub(crate) trials_dir: &'a Path,
    pub(crate) evidence_records_path: &'a Path,
    pub(crate) task_chain_states_path: &'a Path,
    pub(crate) artifact_store: &'a ArtifactStore,
    pub(crate) trial_index: &'a mut usize,
    pub(crate) chain_states: &'a mut BTreeMap<String, ChainRuntimeState>,
    pub(crate) baseline_id: &'a str,
    pub(crate) run_sink: &'a mut dyn RunSink,
}

pub(crate) struct PreparedScheduledTrial {
    variant: Variant,
    variant_runtime: VariantRuntimeProfile,
    task_boundary: TaskBoundaryMaterialization,
    task_id: String,
    task_idx: usize,
    repl: usize,
    pub(crate) benchmark_grading_enabled: bool,
    chain_key: String,
    chain_step_index: usize,
    trial_id: String,
    trial_dir: PathBuf,
    trial_guard: TrialStateGuard,
    prepared_manifest: PreparedTaskEnvironmentManifest,
    trial_paths: TrialPaths,
    io_paths: PreparedTrialIo,
    trial_input_ref: String,
    dynamic_mounts: Vec<ResolvedMountReference>,
    task_sandbox_image: String,
    task_sandbox_workdir: String,
    configured_network_mode: String,
    effective_network_mode: String,
    invocation_source: String,
    effective_policy: EffectiveTaskPolicy,
}

fn write_scheduled_trial_metadata(
    request: &ScheduledTrialRequest<'_>,
    prepared: &PreparedScheduledTrial,
) -> Result<()> {
    let variant_digest = variant_digest(&prepared.variant)?;
    let trial_metadata = json!({
        "schema_version": "trial_metadata_v1",
        "variant_digest": variant_digest,
        "ids": {
            "run_id": request.run_id,
            "trial_id": prepared.trial_id.as_str(),
            "variant_id": prepared.variant.id.as_str(),
            "task_id": prepared.task_id.as_str(),
            "repl_idx": prepared.repl,
            "task_index": prepared.task_idx
        },
        "runtime": {
            "integration_level": prepared.variant_runtime.agent_runtime.integration_level.as_str(),
            "network_mode_requested": prepared.configured_network_mode.as_str(),
            "network_mode_effective": prepared.effective_network_mode.as_str(),
            "agent_runtime": {
                "image": prepared.variant_runtime.agent_runtime.image.clone(),
                "workdir": prepared.task_sandbox_workdir.as_str(),
            },
            "task_sandbox": {
                "executor": "docker",
                "image": prepared.task_sandbox_image.as_str(),
                "workdir": prepared.task_sandbox_workdir.as_str()
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
                "state_policy": match request.policy_config.state {
                    StatePolicy::IsolatePerTrial => "isolate_per_trial",
                    StatePolicy::PersistPerTask => "persist_per_task",
                    StatePolicy::Accumulate => "accumulate",
                }
            },
            "benchmark_type_policy": {
                "task_model": request.benchmark_config.policy.task_model.as_str(),
                "scoring_lifecycle": request.benchmark_config.policy.scoring_lifecycle.as_str(),
                "required_evidence_classes": request.benchmark_config.policy.required_evidence_classes.clone()
            },
            "task_override": prepared.task_boundary.task_payload.get("policy_override").cloned(),
            "effective": {
                "state_policy": match prepared.effective_policy.state_policy {
                    StatePolicy::IsolatePerTrial => "isolate_per_trial",
                    StatePolicy::PersistPerTask => "persist_per_task",
                    StatePolicy::Accumulate => "accumulate",
                },
                "task_model": prepared.effective_policy.task_model.as_str(),
                "scoring_lifecycle": prepared.effective_policy.scoring_lifecycle.as_str(),
                "required_evidence_classes": prepared.effective_policy.required_evidence_classes.clone(),
                "chain_failure_policy": prepared.effective_policy.chain_failure_policy.as_str(),
            }
        },
        "chain": {
            "chain_id": prepared.chain_key.as_str(),
            "step_index": prepared.chain_step_index
        }
    });
    atomic_write_json_pretty(
        &prepared.trial_dir.join("trial_metadata.json"),
        &trial_metadata,
    )
}

pub(crate) fn prepare_scheduled_trial(
    request: &mut ScheduledTrialRequest<'_>,
) -> Result<PreparedScheduledTrial> {
    let variant = request.variants[request.slot.variant_idx].clone();
    let variant_runtime = request.variant_runtime_profiles[request.slot.variant_idx].clone();
    let agent_runtime = &variant_runtime.agent_runtime;
    let trial_experiment = &variant_runtime.experiment;
    let task_idx = request.slot.task_idx;
    let task = &request.tasks[task_idx];
    let task_boundary = parse_task_boundary_from_packaged_task(task)?;
    let task_id = task_boundary
        .task_payload
        .get("id")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("task_{}", task_idx));
    if request.benchmark_config.grader.is_some()
        && !task_grading_enabled(&task_boundary.task_payload)
    {
        return Err(anyhow!(
            "benchmark task '{}' sets grading.enabled=false, but Milestone 4 requires mapped grading output for every benchmark trial",
            task_id
        ));
    }

    let repl = request.slot.repl_idx;
    let benchmark_grading_enabled = request.benchmark_config.grader.is_some();
    let effective_policy = resolve_effective_task_policy(
        request.policy_config,
        &request.benchmark_config.policy,
        &task_boundary.task_payload,
    );
    let chain_key = format!("{}::{}", variant.id, task_id);
    let chain_step_index = request
        .chain_states
        .get(&chain_key)
        .map(|state| state.step_index + 1)
        .unwrap_or(0);
    let _has_chain_snapshot = request.chain_states.contains_key(&chain_key);

    *request.trial_index += 1;
    let trial_id = format!("trial_{}", *request.trial_index);
    let trial_dir = request.trials_dir.join(&trial_id);
    ensure_dir(&trial_dir)?;
    write_trial_state(&trial_dir, &trial_id, "running", None, None, None)?;
    let trial_guard = TrialStateGuard::new(&trial_dir, &trial_id);

    let prepared = if let Some(trial_paths) = request.precomputed_trial_paths.take() {
        prepare_task_environment_with_paths(
            trial_paths,
            request.project_root,
            &trial_dir,
            request.run_id,
            &trial_id,
            trial_experiment,
            &variant,
            task_idx,
            repl,
            &task_boundary,
            agent_runtime,
        )?
    } else {
        prepare_task_environment(
            request.project_root,
            &trial_dir,
            request.run_id,
            &trial_id,
            trial_experiment,
            &variant,
            task_idx,
            repl,
            &task_boundary,
            agent_runtime,
        )?
    };

    let PreparedTaskEnvironment {
        manifest: prepared_manifest,
        trial_paths,
        io_paths,
        dynamic_mounts,
        trial_input: input,
    } = prepared;
    let task_sandbox_image = prepared_manifest.task_sandbox_image().to_string();
    let task_sandbox_workdir = prepared_manifest
        .task_sandbox_workdir()
        .unwrap_or(task_boundary.task_workdir.as_str())
        .to_string();

    let input_bytes = serde_json::to_vec_pretty(&input)?;
    let trial_input_ref = request.artifact_store.put_bytes(&input_bytes)?;
    let mut bootstrap_store = BackingSqliteStore::open(request.run_dir)?;
    bootstrap_store.upsert_attempt_object(
        request.run_id,
        &trial_id,
        request.schedule_idx,
        0,
        "trial_input",
        &trial_input_ref,
        None,
    )?;

    let prepared = PreparedScheduledTrial {
        variant,
        variant_runtime,
        task_boundary,
        task_id,
        task_idx,
        repl,
        benchmark_grading_enabled,
        chain_key,
        chain_step_index,
        trial_id,
        trial_dir,
        trial_guard,
        prepared_manifest,
        trial_paths,
        io_paths,
        trial_input_ref,
        dynamic_mounts,
        task_sandbox_image,
        task_sandbox_workdir,
        configured_network_mode: request.variant_runtime_profiles[request.slot.variant_idx]
            .configured_network_mode
            .clone(),
        effective_network_mode: request.variant_runtime_profiles[request.slot.variant_idx]
            .effective_network_mode
            .clone(),
        invocation_source: request.variant_runtime_profiles[request.slot.variant_idx]
            .invocation_source
            .clone(),
        effective_policy,
    };

    write_scheduled_trial_metadata(request, &prepared)?;
    stage_benchmark_trial_preflight(
        request.benchmark_config,
        &prepared.trial_dir,
        request.run_id,
        &prepared.trial_id,
        request.schedule_idx,
        &prepared.variant.id,
        &prepared.task_boundary.task_payload,
        Some(prepared.task_sandbox_image.as_str()),
        &prepared.io_paths.trial_input_host,
    )?;

    Ok(prepared)
}

pub(crate) fn execute_scheduled_trial_attempt(
    request: &ScheduledTrialRequest<'_>,
    prepared: &PreparedScheduledTrial,
    attempt_no: u32,
) -> Result<crate::trial::execution::TrialRuntimeOutcome> {
    let runtime_env = prepared.prepared_manifest.runtime_env.clone();
    let run_request = AdapterRunRequest {
        runtime_experiment: &prepared.variant_runtime.experiment,
        runtime: &prepared.variant_runtime.agent_runtime,
        variant_args: &prepared.variant_runtime.variant_args,
        runtime_env: &runtime_env,
        runtime_overrides_env: &prepared.variant_runtime.agent_runtime_env,
        trial_paths: &prepared.trial_paths,
        dynamic_mounts: &prepared.dynamic_mounts,
        secret_file_mounts: &prepared.variant_runtime.secret_file_mounts,
        io_paths: &prepared.io_paths,
        network_mode: prepared.effective_network_mode.as_str(),
        benchmark_grader: request.benchmark_config.grader.as_ref(),
        benchmark_grading_enabled: prepared.benchmark_grading_enabled,
        run_id: request.run_id,
        task_image: prepared.task_sandbox_image.as_str(),
        task_workdir: prepared.task_sandbox_workdir.as_str(),
        task_materialization_kind: prepared.task_boundary.materialization.kind.clone(),
        agent_artifact: Some(
            prepared
                .variant_runtime
                .agent_runtime
                .agent_artifact
                .as_path(),
        ),
    };

    for path in [
        &prepared.io_paths.result_host,
        &prepared.trial_paths.out.join(MAPPED_GRADER_OUTPUT_FILENAME),
        &prepared.trial_paths.out.join(RAW_GRADER_OUTPUT_FILENAME),
        &prepared
            .trial_paths
            .out
            .join(BENCHMARK_GRADE_ERROR_FILENAME),
    ] {
        let _ = fs::remove_file(path);
    }
    crate::trial::execution::execute_trial_runtime(
        &prepared.trial_dir,
        request.schedule_idx,
        attempt_no,
        &run_request,
        &prepared.task_id,
        &prepared.variant.id,
        prepared.repl,
        prepared
            .prepared_manifest
            .task_sandbox_plan
            .as_ref()
            .ok_or_else(|| anyhow!("prepared task environment missing task sandbox plan"))?,
    )
}

pub(crate) fn finalize_scheduled_trial(
    request: &mut ScheduledTrialRequest<'_>,
    prepared: &mut PreparedScheduledTrial,
    runtime_outcome: crate::trial::execution::TrialRuntimeOutcome,
    trial_started_at: Instant,
) -> Result<TrialExecutionResult> {
    let status = runtime_outcome.agent_exit_status;
    let trial_output = runtime_outcome.trial_output;
    let result_parse_error = runtime_outcome.result_parse_error;
    let deferred_trial_conclusion_records = runtime_outcome.deferred_trial_conclusion_records;
    let trial_conclusion_row = runtime_outcome.trial_conclusion_row;
    let grade_error_reason = runtime_outcome.grade_error_reason;

    if !matches!(
        prepared.effective_policy.state_policy,
        StatePolicy::IsolatePerTrial
    ) {
        request.chain_states.insert(
            prepared.chain_key.clone(),
            ChainRuntimeState {
                step_index: prepared.chain_step_index,
            },
        );
    }

    let trial_output_ref = request
        .artifact_store
        .put_bytes(&serde_json::to_vec_pretty(&trial_output)?)?;

    let stdout_path = prepared.trial_dir.join("harness_stdout.log");
    let stderr_path = prepared.trial_dir.join("harness_stderr.log");
    let stdout_ref = if stdout_path.exists() {
        Some(request.artifact_store.put_file(&stdout_path)?)
    } else {
        None
    };
    let stderr_ref = if stderr_path.exists() {
        Some(request.artifact_store.put_file(&stderr_path)?)
    } else {
        None
    };

    let event_sink = prepared.variant_runtime.agent_runtime.event_sinks.first();
    let persist_hook_events = event_sink.map(|sink| sink.persist).unwrap_or(true);
    let ingest_hook_events = event_sink.map(|sink| sink.ingest).unwrap_or(true);
    let hook_events_path = if persist_hook_events && prepared.io_paths.events_host.exists() {
        Some(prepared.io_paths.events_host.clone())
    } else {
        None
    };
    let hook_events_ref = if let Some(path) = hook_events_path.as_ref() {
        Some(request.artifact_store.put_file(path)?)
    } else {
        None
    };

    let trial_duration_ms = trial_started_at.elapsed().as_secs_f64() * 1000.0;
    let mut evidence_record = json!({
        "schema_version": "evidence_record_v1",
        "ts": Utc::now().to_rfc3339(),
        "ids": {
            "run_id": request.run_id,
            "trial_id": prepared.trial_id.as_str(),
            "variant_id": prepared.variant.id.as_str(),
            "task_id": prepared.task_id.as_str(),
            "repl_idx": prepared.repl
        },
        "policy": {
            "state_policy": match prepared.effective_policy.state_policy {
                StatePolicy::IsolatePerTrial => "isolate_per_trial",
                StatePolicy::PersistPerTask => "persist_per_task",
                StatePolicy::Accumulate => "accumulate",
            },
            "task_model": prepared.effective_policy.task_model.as_str(),
            "chain_id": prepared.chain_key.as_str(),
            "chain_step_index": prepared.chain_step_index
        },
        "runtime": {
            "executor": "docker",
            "exit_status": status.as_str(),
            "duration_ms": trial_duration_ms
        },
        "evidence": {
            "trial_input_ref": prepared.trial_input_ref.clone(),
            "trial_output_ref": trial_output_ref.clone(),
            "stdout_ref": stdout_ref.clone(),
            "stderr_ref": stderr_ref.clone(),
            "hook_events_ref": hook_events_ref.clone(),
            "harness_request_ref": prepared.trial_input_ref.clone(),
            "harness_response_ref": trial_output_ref.clone()
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
        &prepared.effective_policy.required_evidence_classes,
    )?;
    append_jsonl(request.evidence_records_path, &evidence_record)?;

    let checkpoint_labels = trial_output
        .get("checkpoints")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    row.get("logical_name")
                        .and_then(Value::as_str)
                        .or_else(|| row.get("path").and_then(Value::as_str))
                        .map(str::to_string)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let chain_state_record = json!({
        "schema_version": "task_chain_state_v1",
        "ts": Utc::now().to_rfc3339(),
        "run_id": request.run_id,
        "chain_id": prepared.chain_key.as_str(),
        "task_model": prepared.effective_policy.task_model.as_str(),
        "step_index": prepared.chain_step_index,
        "ids": {
            "trial_id": prepared.trial_id.as_str(),
            "variant_id": prepared.variant.id.as_str(),
            "task_id": prepared.task_id.as_str(),
            "repl_idx": prepared.repl
        },
        "checkpoint_labels": checkpoint_labels
    });
    append_jsonl(request.task_chain_states_path, &chain_state_record)?;

    write_state_inventory(
        &prepared.trial_dir,
        &prepared.variant_runtime.experiment,
        &prepared.variant_runtime.agent_runtime,
        &prepared.variant_runtime.secret_file_mounts,
        &prepared.trial_paths,
        &resolve_exec_digest(
            &prepared.variant_runtime.agent_runtime.command_raw,
            request.project_root,
        )?,
        prepared.effective_network_mode.as_str(),
        prepared.invocation_source.as_str(),
        Some(prepared.task_boundary.task_image.as_str()),
        Some(prepared.task_boundary.task_workdir.as_str()),
    )?;

    let manifest_path = resolve_agent_runtime_manifest_path(&prepared.trial_paths)?;
    if ingest_hook_events && manifest_path.exists() && prepared.io_paths.events_host.exists() {
        let manifest = load_manifest(&manifest_path)?;
        let schema = compile_schema("hook_events_v1.jsonschema")?;
        let _ = validate_hooks(&manifest, &prepared.io_paths.events_host, &schema);
    }

    let trial_conclusion_outcome = trial_conclusion_row
        .as_ref()
        .and_then(|row| row.pointer("/reported_outcome"))
        .and_then(Value::as_str);
    let mapped_trial_outcome =
        trial_conclusion_outcome.and_then(trial_conclusion_outcome_to_trial_outcome);
    let trial_output_payload = trial_output_payload_view(&trial_output);
    let agent_outcome = trial_output_payload
        .get("outcome")
        .and_then(Value::as_str)
        .unwrap_or("error")
        .to_string();
    let mut outcome = agent_outcome.clone();
    if prepared.benchmark_grading_enabled {
        outcome = if grade_error_reason.is_some() {
            "grading_failed".to_string()
        } else if let Some(mapped_outcome) = mapped_trial_outcome {
            mapped_outcome.to_string()
        } else {
            "missing".to_string()
        };
    }
    let mut metrics = trial_output_payload
        .get("metrics")
        .cloned()
        .unwrap_or(json!({}));
    if let Some(obj) = metrics.as_object_mut() {
        obj.insert("status_code".to_string(), json!(status.clone()));
        if let Some(mapped_state) =
            mapped_grader_output_state(trial_conclusion_row.as_ref(), grade_error_reason.as_deref())
        {
            obj.insert(
                "mapped_grader_output_state".to_string(),
                json!(mapped_state),
            );
        }
        if let Some(reported_outcome) = trial_conclusion_outcome {
            obj.insert(
                "trial_conclusion_reported_outcome".to_string(),
                json!(reported_outcome),
            );
        }
        if let Some(row) = trial_conclusion_row.as_ref() {
            if let Some(payload) = row.pointer("/payload") {
                obj.insert("trial_conclusion_payload".to_string(), payload.clone());
            }
            if let Some(name) = row.pointer("/grader/name").and_then(Value::as_str) {
                obj.insert("trial_conclusion_grader".to_string(), json!(name));
            }
            if let Some(strategy) = row.pointer("/grader/strategy").and_then(Value::as_str) {
                obj.insert(
                    "trial_conclusion_grader_strategy".to_string(),
                    json!(strategy),
                );
            }
        }
        if let Some(reason) = grade_error_reason.as_ref() {
            obj.insert("grade_error".to_string(), json!(true));
            obj.insert("grade_error_reason".to_string(), json!(reason));
        }
    }
    let mapped_primary = trial_conclusion_row.as_ref().and_then(|row| {
        let name = row
            .pointer("/primary_metric/name")
            .and_then(Value::as_str)
            .map(str::to_string)?;
        let value = row
            .pointer("/primary_metric/value")
            .cloned()
            .unwrap_or(json!(null));
        Some((name, value))
    });
    let (primary_metric_name, primary_metric_value) = if prepared.benchmark_grading_enabled {
        if grade_error_reason.is_some() {
            ("grading_failed".to_string(), json!(null))
        } else if let Some((name, value)) = mapped_primary {
            (name, value)
        } else if let Some(row) = trial_conclusion_row.as_ref() {
            (
                "trial_conclusion_payload".to_string(),
                row.pointer("/payload").cloned().unwrap_or(json!(null)),
            )
        } else {
            ("grading_failed".to_string(), json!(null))
        }
    } else if let Some(obj) = trial_output_payload
        .get("objective")
        .and_then(Value::as_object)
    {
        let name = obj
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("primary_metric")
            .to_string();
        let value = obj.get("value").cloned().unwrap_or(json!(null));
        (name, value)
    } else {
        let fallback = if outcome == "success" { 1.0 } else { 0.0 };
        ("success".to_string(), json!(fallback))
    };
    let bindings = variant_bindings_for_summary(&prepared.variant);
    let event_rows = if ingest_hook_events && prepared.io_paths.events_host.exists() {
        load_event_rows(
            &prepared.io_paths.events_host,
            request.run_id,
            &prepared.trial_id,
            request.schedule_idx,
            &prepared.variant.id,
            &prepared.task_id,
            prepared.repl,
        )?
    } else {
        Vec::new()
    };
    let metric_rows = build_metric_rows(
        request.run_id,
        &prepared.trial_id,
        request.schedule_idx,
        &prepared.variant.id,
        &prepared.task_id,
        prepared.repl,
        &outcome,
        &metrics,
        &primary_metric_name,
        &primary_metric_value,
    );
    let variant_snapshot_rows = build_variant_snapshot_rows(
        request.run_id,
        &prepared.trial_id,
        request.schedule_idx,
        &prepared.variant.id,
        request.baseline_id,
        &prepared.task_id,
        prepared.repl,
        &bindings,
    );
    request.run_sink.append_trial_record(&TrialRecord {
        run_id: request.run_id.to_string(),
        trial_id: prepared.trial_id.clone(),
        schedule_idx: request.schedule_idx,
        slot_commit_id: String::new(),
        attempt: 0,
        row_seq: 0,
        baseline_id: request.baseline_id.to_string(),
        workload_type: request.workload_type.to_string(),
        variant_id: prepared.variant.id.clone(),
        task_index: prepared.task_idx,
        task_id: prepared.task_id.clone(),
        repl_idx: prepared.repl,
        outcome: outcome.clone(),
        success: outcome == "success" && grade_error_reason.is_none(),
        status_code: status.clone(),
        integration_level: prepared
            .variant_runtime
            .agent_runtime
            .integration_level
            .clone(),
        network_mode_requested: prepared.configured_network_mode.clone(),
        network_mode_effective: prepared.effective_network_mode.clone(),
        primary_metric_name: primary_metric_name.clone(),
        primary_metric_value: primary_metric_value.clone(),
        metrics: metrics.clone(),
        bindings: bindings.clone(),
        hook_events_total: event_rows.len(),
        has_hook_events: !event_rows.is_empty(),
    })?;
    request.run_sink.append_metric_rows(&metric_rows)?;
    request.run_sink.append_event_rows(&event_rows)?;
    request
        .run_sink
        .append_variant_snapshot(&variant_snapshot_rows)?;

    let failure_classification = if prepared.benchmark_grading_enabled {
        if grade_error_reason.is_some() {
            prepared
                .trial_guard
                .complete("failed", Some("grade_error"))?;
            Some("grade_error".to_string())
        } else {
            prepared.trial_guard.complete("completed", None)?;
            None
        }
    } else if status != "0" {
        prepared
            .trial_guard
            .complete("failed", Some("agent_exit_nonzero"))?;
        Some("agent_exit_nonzero".to_string())
    } else if result_parse_error.is_some() {
        prepared
            .trial_guard
            .complete("failed", Some("result_parse_error"))?;
        Some("result_parse_error".to_string())
    } else if status == "0" && outcome != "error" {
        prepared.trial_guard.complete("completed", None)?;
        None
    } else {
        prepared
            .trial_guard
            .complete("failed", Some("result_error"))?;
        Some("result_error".to_string())
    };

    materialize_trial_runtime_layout(
        &prepared.trial_dir,
        &prepared.trial_paths,
        request.materialize_mode,
    )?;
    prepared.trial_paths.cleanup_scratch()?;

    let slot_status = if prepared.benchmark_grading_enabled {
        if grade_error_reason.is_none() {
            "completed"
        } else {
            "grading_failed"
        }
    } else if status == "0" && outcome != "error" {
        "completed"
    } else {
        "failed"
    };
    let mut result = TrialExecutionResult::minimal(
        prepared.trial_id.clone(),
        slot_status,
        Some(request.slot.variant_idx),
    );
    result.deferred_trial_conclusion_records = deferred_trial_conclusion_records;
    result.failure_classification = failure_classification;
    Ok(result)
}
