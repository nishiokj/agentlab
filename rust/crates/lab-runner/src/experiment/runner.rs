use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use lab_core::{
    canonical_json_digest, ensure_dir, sha256_bytes, sha256_file, ArtifactStore,
    AGENTLAB_CONTRACT_IN_DIR, AGENTLAB_CONTRACT_OUT_DIR, AGENTLAB_TASK_WORKDIR_PLACEHOLDER,
};
use lab_provenance::{default_attestation, write_attestation};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{BufRead, Read};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use crate::config::*;
use crate::experiment::commit::*;
use crate::experiment::control::*;
use crate::experiment::lease::{
    acquire_run_operation_lease, adopt_engine_lease_for_recovery, start_engine_lease_heartbeat,
    RunOperationType,
};
use crate::experiment::preflight::*;
use crate::experiment::runtime::*;
use crate::experiment::state::*;
use crate::model::*;
use crate::package::compile::copy_path_into_package;
use crate::package::sealed::*;
use crate::package::staging::*;
use crate::package::validate::*;
use crate::persistence::journal::*;
use crate::persistence::rows::*;
use crate::persistence::store::{
    load_pending_trial_completion_records, persist_pending_trial_completions,
    SqliteRunStore as BackingSqliteStore,
};
use crate::trial::execution::resolve_container_image_digest;
use crate::trial::execution::AdapterRunRequest;
use crate::trial::grade::benchmark_retry_inputs;
use crate::trial::prepare::{
    build_runtime_contract_env, load_prepared_task_environment_manifest, prepare_io_paths,
    prepare_task_environment, resolve_trial_timeout_ms, PreparedTaskEnvironment, TrialPaths,
};
use crate::trial::schedule::*;
use crate::trial::spec::{
    materialize_packaged_task_boundary, validate_task_boundary_workspace_materialization,
};
use crate::trial::state::{write_trial_state, TrialStateGuard};
use crate::util::*;
use crate::INTERRUPTED;

pub fn continue_run_with_options(
    run_dir: &Path,
    options: RunExecutionOptions,
) -> Result<RunResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Continue)?;
    let run_dir = run_dir
        .canonicalize()
        .unwrap_or_else(|_| run_dir.to_path_buf());

    // 1. Validate run status is terminal and continuable.
    let control = load_run_control(&run_dir)?;
    let run_status = run_control_status(&control);
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

    let run_id = run_control_run_id(&control)
        .ok_or_else(|| anyhow!("missing run_id in run_control.json"))?;
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
    let mut run_sink = SqliteRunJournal::new(&run_dir)?;
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

pub(crate) fn trial_index_from_trial_id(trial_id: &str) -> Option<usize> {
    trial_id
        .strip_prefix("trial_")
        .and_then(|suffix| suffix.parse::<usize>().ok())
        .filter(|idx| *idx > 0)
}

#[derive(Clone)]
pub(crate) struct ParallelWorkerExecutionContext {
    run_dir: PathBuf,
    run_id: String,
    workload_type: String,
    project_root: PathBuf,
    variants: Vec<Variant>,
    tasks: Vec<Value>,
    policy_config: PolicyConfig,
    benchmark_config: BenchmarkConfig,
    variant_runtime_profiles: Vec<VariantRuntimeProfile>,
    materialize_mode: MaterializationMode,
    trials_dir: PathBuf,
    baseline_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InFlightDispatch {
    schedule_idx: usize,
    trial_id: String,
    variant_idx: usize,
    variant_id: String,
    worker_id: String,
    started_at: String,
}

pub(crate) struct LocalTrialLaunch {
    schedule_idx: usize,
    trial_id: String,
    slot: TrialSlot,
    trial_paths: TrialPaths,
}

#[derive(Debug)]
pub(crate) struct LocalTrialCompletion {
    trial_id: String,
    schedule_idx: usize,
    result: std::result::Result<TrialExecutionResult, String>,
}

pub(crate) fn in_flight_active_trials(
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
            #[cfg(test)]
            control: None,
        })
        .collect();
    active.sort_by_key(|entry| entry.schedule_idx.unwrap_or(usize::MAX));
    active
}

pub(crate) fn execute_local_trial(
    context: &ParallelWorkerExecutionContext,
    launch: LocalTrialLaunch,
) -> Result<TrialExecutionResult> {
    let payload_dir = context
        .run_dir
        .join("runtime")
        .join("worker_payload")
        .join(&launch.trial_id);
    if payload_dir.exists() {
        fs::remove_dir_all(&payload_dir)?;
    }
    ensure_dir(&payload_dir)?;
    let payload_evidence = payload_dir.join("evidence_records.jsonl");
    let payload_chain = payload_dir.join("task_chain_states.jsonl");

    let mut local_trial_index = trial_index_from_trial_id(&launch.trial_id)
        .unwrap_or(launch.schedule_idx + 1)
        .saturating_sub(1);
    let mut local_chain_states: BTreeMap<String, ChainRuntimeState> = BTreeMap::new();
    let mut buffered_sink = BufferedRunSink::default();
    let artifact_store = ArtifactStore::new(context.run_dir.join("artifacts"));
    let execution = (|| -> Result<TrialExecutionResult> {
        let mut request = ScheduledTrialRequest {
            run_dir: &context.run_dir,
            run_id: &context.run_id,
            workload_type: &context.workload_type,
            project_root: &context.project_root,
            variants: &context.variants,
            tasks: &context.tasks,
            schedule_idx: launch.schedule_idx,
            slot: &launch.slot,
            policy_config: &context.policy_config,
            benchmark_config: &context.benchmark_config,
            variant_runtime_profiles: &context.variant_runtime_profiles,
            materialize_mode: context.materialize_mode,
            precomputed_trial_paths: Some(launch.trial_paths),
            trials_dir: &context.trials_dir,
            evidence_records_path: &payload_evidence,
            task_chain_states_path: &payload_chain,
            artifact_store: &artifact_store,
            trial_index: &mut local_trial_index,
            chain_states: &mut local_chain_states,
            baseline_id: &context.baseline_id,
            run_sink: &mut buffered_sink,
        };
        let mut prepared = prepare_scheduled_trial(&mut request)?;
        let trial_started_at = Instant::now();
        let mut runtime_outcome = None;
        for attempt in 0..context.policy_config.retry_max_attempts {
            let outcome =
                execute_scheduled_trial_attempt(&request, &prepared, (attempt + 1) as u32)?;
            let (retry_outcome, retry_exit_status) = benchmark_retry_inputs(
                prepared.benchmark_grading_enabled,
                &outcome.trial_output,
                outcome.trial_conclusion_row.as_ref(),
                outcome.grade_error_reason.as_deref(),
                &outcome.agent_exit_status,
            );
            let is_last_attempt = attempt + 1 >= context.policy_config.retry_max_attempts;
            let should_retry = !is_last_attempt
                && should_retry_outcome(
                    &retry_outcome,
                    &retry_exit_status,
                    &context.policy_config.retry_on,
                );
            runtime_outcome = Some(outcome);
            if !should_retry {
                break;
            }
        }
        let mut trial_result = finalize_scheduled_trial(
            &mut request,
            &mut prepared,
            runtime_outcome.ok_or_else(|| anyhow!("trial runtime produced no attempt outcome"))?,
            trial_started_at,
        )?;
        trial_result.variant_idx = Some(launch.slot.variant_idx);
        trial_result.deferred_trial_records = buffered_sink.trial_records;
        trial_result.deferred_metric_rows = buffered_sink.metric_rows;
        trial_result.deferred_event_rows = buffered_sink.event_rows;
        trial_result.deferred_variant_snapshot_rows = buffered_sink.variant_snapshot_rows;
        trial_result.deferred_evidence_records = load_jsonl_value_rows(&payload_evidence)?;
        trial_result.deferred_chain_state_records = load_jsonl_value_rows(&payload_chain)?;
        Ok(trial_result)
    })();

    let _ = fs::remove_dir_all(&payload_dir);
    execution
}

pub(crate) fn spawn_local_trial(
    context: Arc<ParallelWorkerExecutionContext>,
    launch: LocalTrialLaunch,
    completion_tx: mpsc::Sender<LocalTrialCompletion>,
) -> Result<()> {
    let thread_name = format!("agentlab-{}", launch.trial_id);
    let completion_trial_id = launch.trial_id.clone();
    let completion_schedule_idx = launch.schedule_idx;
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_local_trial(context.as_ref(), launch)
            })) {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(err)) => Err(err.to_string()),
                Err(_) => Err("local trial execution panicked".to_string()),
            };
            let _ = completion_tx.send(LocalTrialCompletion {
                trial_id: completion_trial_id,
                schedule_idx: completion_schedule_idx,
                result,
            });
        })
        .map(|_| ())
        .map_err(|err| anyhow!("failed to spawn local trial thread: {}", err))
}

pub(crate) fn poll_local_trial_completions(
    completion_rx: &mpsc::Receiver<LocalTrialCompletion>,
    timeout: Duration,
) -> Result<Vec<LocalTrialCompletion>> {
    let first = if timeout.is_zero() {
        match completion_rx.try_recv() {
            Ok(completion) => Some(completion),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err(anyhow!("local scheduler completion channel disconnected"));
            }
        }
    } else {
        match completion_rx.recv_timeout(timeout) {
            Ok(completion) => Some(completion),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(anyhow!("local scheduler completion channel disconnected"));
            }
        }
    };

    let Some(first) = first else {
        return Ok(Vec::new());
    };

    let mut completions = vec![first];
    loop {
        match completion_rx.try_recv() {
            Ok(completion) => completions.push(completion),
            Err(mpsc::TryRecvError::Empty) => break,
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err(anyhow!("local scheduler completion channel disconnected"));
            }
        }
    }
    Ok(completions)
}

pub(crate) fn load_external_schedule_outcome_request(
    run_dir: &Path,
) -> Result<Option<ScheduleEngineOutcome>> {
    let run_control = load_run_control(run_dir)?;
    let status = run_control_status(&run_control);
    Ok(match status {
        "paused" => Some(ScheduleEngineOutcome::Paused),
        "killed" => Some(ScheduleEngineOutcome::Killed),
        _ => None,
    })
}

pub(crate) fn schedule_engine_status(
    requested_outcome: Option<ScheduleEngineOutcome>,
) -> &'static str {
    match requested_outcome {
        Some(ScheduleEngineOutcome::Paused) => "paused",
        Some(ScheduleEngineOutcome::Killed) => "killed",
        Some(ScheduleEngineOutcome::Interrupted) => "interrupted",
        _ => "running",
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_schedule_engine_local(
    _mode: ScheduleEngineMode,
    run_dir: &Path,
    run_id: &str,
    workload_type: &str,
    project_root: &Path,
    _dataset_path: &Path,
    variants: &[Variant],
    tasks: &[Value],
    schedule: &[TrialSlot],
    policy_config: &PolicyConfig,
    benchmark_config: &BenchmarkConfig,
    variant_runtime_profiles: &[VariantRuntimeProfile],
    _behavior: &RunBehavior,
    materialize_mode: MaterializationMode,
    _task_boundary_policy: &TaskBoundaryPolicy,
    trials_dir: &Path,
    _evidence_dir: &Path,
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
    let benchmark_conclusions_path = benchmark_dir.join("conclusions.jsonl");

    let requested_dispatch_capacity = max_concurrency.max(1);
    let configured_ceiling = parse_local_worker_capacity_ceiling_from_env()?;
    let (dispatch_capacity, capacity_warning) =
        resolve_local_worker_max_in_flight(requested_dispatch_capacity, configured_ceiling);
    if let Some(warning) = capacity_warning {
        eprintln!("{}", warning);
    }

    let execution_context = Arc::new(ParallelWorkerExecutionContext {
        run_dir: run_dir.to_path_buf(),
        run_id: run_id.to_string(),
        workload_type: workload_type.to_string(),
        project_root: project_root.to_path_buf(),
        variants: variants.to_vec(),
        tasks: tasks.to_vec(),
        policy_config: policy_config.clone(),
        benchmark_config: benchmark_config.clone(),
        variant_runtime_profiles: variant_runtime_profiles.to_vec(),
        materialize_mode,
        trials_dir: trials_dir.to_path_buf(),
        baseline_id: baseline_id.to_string(),
    });
    let (completion_tx, completion_rx) = mpsc::channel::<LocalTrialCompletion>();
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
            let _ = crate::trial::state::reconcile_trial_attempt_as_abandoned(
                &run_dir.join("trials").join(&recovered.trial_id),
            );
            committer.enqueue_trial(schedule_idx, result)?;
        }
    }
    let pending_records = committer.pending_trial_completion_records();
    persist_pending_trial_completions(run_dir, &pending_records)?;

    let mut next_dispatch_idx = schedule_progress.next_schedule_index;
    let mut in_flight: HashMap<String, InFlightDispatch> = HashMap::new();
    let mut in_flight_by_variant: BTreeMap<usize, usize> = BTreeMap::new();

    committer.drain_ready(
        run_dir,
        policy_config,
        evidence_records_path,
        task_chain_states_path,
        &benchmark_conclusions_path,
        schedule_progress,
        *trial_index,
        pruned_variants,
        consecutive_failures,
        run_sink,
    )?;
    let pending_records = committer.pending_trial_completion_records();
    persist_pending_trial_completions(run_dir, &pending_records)?;
    write_run_control_v2(
        run_dir,
        run_id,
        "running",
        &in_flight_active_trials(&in_flight),
        None,
    )?;
    let mut requested_outcome: Option<ScheduleEngineOutcome> = None;

    while committer.next_commit_idx < schedule.len() || !in_flight.is_empty() {
        if INTERRUPTED.load(Ordering::SeqCst) {
            emit_run_log(
                run_id,
                "received interrupt signal, shutting down gracefully",
            );
            write_run_control_v2(
                run_dir,
                run_id,
                "interrupted",
                &in_flight_active_trials(&in_flight),
                None,
            )?;
            return Ok(ScheduleEngineOutcome::Interrupted);
        }
        if let Some(external_outcome) = load_external_schedule_outcome_request(run_dir)? {
            requested_outcome = Some(external_outcome);
        }

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

        let mut made_progress = false;

        while requested_outcome.is_none()
            && next_dispatch_idx < schedule.len()
            && in_flight.len() < dispatch_capacity
        {
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
            let trial_dir = trials_dir.join(&trial_id);
            ensure_dir(&trial_dir)?;
            let trial_paths = TrialPaths::new(&trial_dir, project_root)?;
            trial_paths.prepare(false)?;
            let launch = LocalTrialLaunch {
                schedule_idx: next_dispatch_idx,
                trial_id: trial_id.clone(),
                slot: slot.clone(),
                trial_paths,
            };
            spawn_local_trial(execution_context.clone(), launch, completion_tx.clone())?;
            *trial_index = proposed_trial_index;
            let started_at = Utc::now().to_rfc3339();
            in_flight.insert(
                trial_id.clone(),
                InFlightDispatch {
                    schedule_idx: next_dispatch_idx,
                    trial_id: trial_id.clone(),
                    variant_idx: slot.variant_idx,
                    variant_id: variant.id.clone(),
                    worker_id: RUN_CONTROL_UNKNOWN_WORKER_ID.to_string(),
                    started_at,
                },
            );
            *in_flight_by_variant.entry(slot.variant_idx).or_default() += 1;
            next_dispatch_idx += 1;
            made_progress = true;
            write_run_control_v2(
                run_dir,
                run_id,
                schedule_engine_status(requested_outcome),
                &in_flight_active_trials(&in_flight),
                None,
            )?;
        }

        let committed = committer.drain_ready(
            run_dir,
            policy_config,
            evidence_records_path,
            task_chain_states_path,
            &benchmark_conclusions_path,
            schedule_progress,
            *trial_index,
            pruned_variants,
            consecutive_failures,
            run_sink,
        )?;
        let pending_records = committer.pending_trial_completion_records();
        persist_pending_trial_completions(run_dir, &pending_records)?;
        if committed > 0 {
            made_progress = true;
        }

        if committer.next_commit_idx >= schedule.len() && in_flight.is_empty() {
            break;
        }
        if let Some(outcome) = requested_outcome {
            if in_flight.is_empty() {
                return Ok(outcome);
            }
        }

        let poll_timeout = if made_progress {
            Duration::from_millis(0)
        } else {
            Duration::from_millis(50)
        };
        let completions = poll_local_trial_completions(&completion_rx, poll_timeout)?;
        if completions.is_empty() {
            continue;
        }

        for completion in completions {
            let in_flight_entry =
                in_flight
                    .remove(completion.trial_id.as_str())
                    .ok_or_else(|| {
                        anyhow!(
                            "local scheduler protocol fault: completion for unknown trial {}",
                            completion.trial_id
                        )
                    })?;
            if completion.schedule_idx != in_flight_entry.schedule_idx {
                return Err(anyhow!(
                    "local scheduler protocol fault: completion schedule_idx {} did not match dispatched schedule_idx {}",
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
            let mut trial_result = match completion.result {
                Ok(result) => result,
                Err(detail) => {
                    return Err(anyhow!(
                        "local trial execution failed (trial_id={}, schedule_idx={}): {}",
                        in_flight_entry.trial_id,
                        in_flight_entry.schedule_idx,
                        detail
                    ));
                }
            };
            if trial_result.trial_id != in_flight_entry.trial_id {
                return Err(anyhow!(
                    "local scheduler protocol fault: completion trial_id mismatch: expected {}, got {}",
                    in_flight_entry.trial_id,
                    trial_result.trial_id
                ));
            }
            if trial_result.variant_idx.is_none() {
                trial_result.variant_idx = Some(in_flight_entry.variant_idx);
            }
            committer.enqueue_trial(in_flight_entry.schedule_idx, trial_result)?;
        }
        let pending_records = committer.pending_trial_completion_records();
        persist_pending_trial_completions(run_dir, &pending_records)?;

        write_run_control_v2(
            run_dir,
            run_id,
            schedule_engine_status(requested_outcome),
            &in_flight_active_trials(&in_flight),
            None,
        )?;
        committer.drain_ready(
            run_dir,
            policy_config,
            evidence_records_path,
            task_chain_states_path,
            &benchmark_conclusions_path,
            schedule_progress,
            *trial_index,
            pruned_variants,
            consecutive_failures,
            run_sink,
        )?;
        let pending_records = committer.pending_trial_completion_records();
        persist_pending_trial_completions(run_dir, &pending_records)?;
        if let Some(outcome) = requested_outcome {
            if in_flight.is_empty() {
                return Ok(outcome);
            }
        }
    }

    committer.drain_ready(
        run_dir,
        policy_config,
        evidence_records_path,
        task_chain_states_path,
        &benchmark_conclusions_path,
        schedule_progress,
        *trial_index,
        pruned_variants,
        consecutive_failures,
        run_sink,
    )?;
    let pending_records = committer.pending_trial_completion_records();
    persist_pending_trial_completions(run_dir, &pending_records)?;
    write_run_control_v2(
        run_dir,
        run_id,
        schedule_engine_status(requested_outcome),
        &in_flight_active_trials(&in_flight),
        None,
    )?;
    Ok(requested_outcome.unwrap_or(ScheduleEngineOutcome::Completed))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_schedule_engine(
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
            "local async docker path supports only isolate_per_trial state policy; got {:?}",
            policy_config.state
        ));
    }
    execute_schedule_engine_local(
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

pub(crate) fn run_experiment_with_behavior(
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

    for subdir in [
        "tasks",
        "files",
        "agent_builds",
        PACKAGED_RUNTIME_ASSETS_DIR,
    ] {
        let source = exp_dir.join(subdir);
        if source.exists() {
            copy_path_into_package(&source, &run_dir.join(subdir))?;
        }
    }
    let staging_manifest_source = exp_dir.join(STAGING_MANIFEST_FILE);
    if !staging_manifest_source.is_file() {
        return Err(anyhow!(
            "sealed package missing runtime staging manifest: {}",
            staging_manifest_source.display()
        ));
    }
    copy_path_into_package(
        &staging_manifest_source,
        &run_dir.join(STAGING_MANIFEST_FILE),
    )
    .with_context(|| {
        format!(
            "failed to copy runtime staging manifest from sealed package {} into run directory {}",
            staging_manifest_source.display(),
            run_dir.display()
        )
    })?;

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
        let profile =
            resolve_variant_runtime_profile(&json_value, variant, &run_dir, &behavior, &execution)?;
        ensure_required_runtime_env_present(&profile.agent_runtime, &profile.agent_runtime_env)?;
        variant_runtime_profiles.push(profile);
    }
    let run_integration_level = variant_runtime_profiles
        .first()
        .map(|profile| profile.agent_runtime.integration_level.clone())
        .unwrap_or_else(|| "cli_basic".to_string());
    let isolation_grade = resolve_run_isolation_grade(&variant_runtime_profiles, &behavior);

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
            run_guard.complete("preflight_failed")?;
            return Err(anyhow!("preflight failed:\n{}", preflight));
        }
    }

    let mut run_sink = SqliteRunJournal::new(&run_dir)?;
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

    let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
    let mut pruned_variants: HashSet<usize> = HashSet::new();

    let mut schedule_progress = new_schedule_progress(&run_id, &schedule);
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
        match schedule_outcome {
            ScheduleEngineOutcome::Interrupted => {
                run_guard.complete("interrupted")?;
            }
            _ => {
                run_guard.disarm();
            }
        }
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
    describe_experiment_with_options(path, &RunExecutionOptions::default())
}

pub fn describe_experiment_with_options(
    path: &Path,
    execution: &RunExecutionOptions,
) -> Result<ExperimentSummary> {
    let LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root: _,
    } = load_sealed_package_for_run(path)?;
    validate_required_fields(&json_value)?;
    let execution = normalize_execution_options(execution);

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
        &execution,
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
                &execution,
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

    let control = load_run_control(&run_dir)?;
    let previous_status = run_control_status(&control).to_string();
    let run_id = run_control_run_id(&control)
        .ok_or_else(|| anyhow!("missing run_id in run_control.json"))?;
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
    let _ = crate::trial::state::reconcile_trial_attempt_as_committed(&replay_trial_dir)?;
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

    let _checkpoint_workspace_ref = if let Some(ref checkpoint_token) = source_checkpoint {
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
    let _ = crate::trial::state::reconcile_trial_attempt_as_committed(&fork_trial_dir)?;
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

pub fn run_experiment(path: &Path) -> Result<RunResult> {
    run_experiment_with_behavior(path, RunBehavior::default(), RunExecutionOptions::default())
}

pub fn run_experiment_with_options(path: &Path, options: RunExecutionOptions) -> Result<RunResult> {
    run_experiment_with_behavior(path, RunBehavior::default(), options)
}

pub fn run_experiment_strict(path: &Path) -> Result<RunResult> {
    run_experiment_strict_with_options(path, RunExecutionOptions::default())
}

pub fn run_experiment_strict_with_options(
    path: &Path,
    options: RunExecutionOptions,
) -> Result<RunResult> {
    let behavior = RunBehavior {
        network_mode_override: None,
        require_network_none: true,
    };
    run_experiment_with_behavior(path, behavior, options)
}

pub fn continue_run(run_dir: &Path) -> Result<RunResult> {
    continue_run_with_options(run_dir, RunExecutionOptions::default())
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

// ---------------------------------------------------------------------------
// Functions moved from engine.rs
// ---------------------------------------------------------------------------

pub(crate) fn emit_slot_commit_progress(
    run_id: &str,
    completed_slots: usize,
    total_slots: usize,
    schedule_idx: usize,
    trial_id: &str,
    slot_status: &str,
) {
    let pct = if total_slots == 0 {
        100.0
    } else {
        (completed_slots as f64 / total_slots as f64) * 100.0
    };
    emit_run_log(
        run_id,
        format!(
            "progress {}/{} ({:.1}%) slot={} trial={} status={}",
            completed_slots, total_slots, pct, schedule_idx, trial_id, slot_status
        ),
    );
}

pub(crate) fn parse_local_worker_capacity_ceiling_from_env() -> Result<Option<usize>> {
    match env::var(AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT_ENV) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed = trimmed.parse::<usize>().map_err(|_| {
                anyhow!(
                    "{} must be a positive integer when set (got: {})",
                    AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT_ENV,
                    raw
                )
            })?;
            if parsed == 0 {
                return Err(anyhow!(
                    "{} must be > 0 when set",
                    AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT_ENV
                ));
            }
            Ok(Some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(anyhow!(
            "failed reading {}: {}",
            AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT_ENV,
            err
        )),
    }
}

pub(crate) fn parse_max_run_bytes_from_env() -> Result<Option<u64>> {
    match env::var(AGENTLAB_MAX_RUN_BYTES_ENV) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed = trimmed.parse::<u64>().map_err(|_| {
                anyhow!(
                    "{} must be a positive integer when set (got: {})",
                    AGENTLAB_MAX_RUN_BYTES_ENV,
                    raw
                )
            })?;
            if parsed == 0 {
                return Err(anyhow!(
                    "{} must be > 0 when set",
                    AGENTLAB_MAX_RUN_BYTES_ENV
                ));
            }
            Ok(Some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(anyhow!(
            "failed reading {}: {}",
            AGENTLAB_MAX_RUN_BYTES_ENV,
            err
        )),
    }
}

pub(crate) fn resolve_local_worker_max_in_flight(
    requested_max_in_flight: usize,
    configured_ceiling: Option<usize>,
) -> (usize, Option<String>) {
    let effective_max_in_flight = configured_ceiling
        .map(|ceiling| requested_max_in_flight.min(ceiling))
        .unwrap_or(requested_max_in_flight)
        .max(1);
    if effective_max_in_flight < requested_max_in_flight {
        let warning = format!(
            "local worker backend capacity ceiling applied: requested_max_in_flight={} effective_max_in_flight={} env_var={}",
            requested_max_in_flight,
            effective_max_in_flight,
            AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT_ENV
        );
        return (effective_max_in_flight, Some(warning));
    }
    (effective_max_in_flight, None)
}

pub(crate) fn create_unique_run_dir(project_root: &Path) -> Result<(String, PathBuf)> {
    let runs_dir = project_root.join(".lab").join("runs");
    ensure_dir(&runs_dir)?;
    static RUN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

    for _ in 0..RUN_DIR_CREATE_MAX_ATTEMPTS {
        let now = Utc::now();
        let seq = RUN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let run_id = format!(
            "run_{}_{:06}_{:06}",
            now.format("%Y%m%d_%H%M%S"),
            now.timestamp_subsec_micros(),
            seq % 1_000_000
        );
        let run_dir = runs_dir.join(&run_id);
        match fs::create_dir(&run_dir) {
            Ok(_) => return Ok((run_id, run_dir)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(anyhow!(
                    "failed to create run directory {}: {}",
                    run_dir.display(),
                    err
                ))
            }
        }
    }

    Err(anyhow!(
        "failed to allocate a unique run directory under {} after {} attempts",
        runs_dir.display(),
        RUN_DIR_CREATE_MAX_ATTEMPTS
    ))
}

pub(crate) fn find_project_root_from_run_dir(run_dir: &Path) -> Result<PathBuf> {
    let root = run_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .ok_or_else(|| {
            anyhow!(
                "cannot derive project root from run_dir: {}",
                run_dir.display()
            )
        })?;
    Ok(root.to_path_buf())
}

// ---------------------------------------------------------------------------
// Functions moved from runtime.rs (trial materialization / state inventory)
// ---------------------------------------------------------------------------

pub(crate) fn materialize_trial_result(trial_dir: &Path, output_path: &Path) -> Result<PathBuf> {
    let canonical_output = trial_dir.join("result.json");
    if output_path != canonical_output {
        if canonical_output.exists() {
            let _ = fs::remove_file(&canonical_output);
        }
        if output_path.exists() {
            if let Some(parent) = canonical_output.parent() {
                ensure_dir(parent)?;
            }
            fs::copy(output_path, &canonical_output)?;
        }
    }
    Ok(canonical_output)
}

pub(crate) fn materialize_trial_runtime_layout(
    trial_dir: &Path,
    paths: &TrialPaths,
    mode: MaterializationMode,
) -> Result<()> {
    match mode {
        MaterializationMode::Full => {
            copy_dir_preserve_contents(&paths.in_dir, &trial_dir.join("in"))?;
            copy_dir_preserve_contents(&paths.out, &trial_dir.join("out"))?;
            copy_dir_preserve_contents(&paths.state, &trial_dir.join("state"))?;
            copy_dir_preserve_contents(&paths.workspace, &trial_dir.join("workspace"))?;
            copy_dir_preserve_contents(&paths.tmp, &trial_dir.join("tmp"))?;
            copy_file_if_exists(
                &paths.runtime.trial_input,
                &trial_dir.join("trial_input.json"),
            )?;
            copy_file_if_exists(
                &paths.out.join("harness_manifest.json"),
                &trial_dir.join("harness_manifest.json"),
            )?;
            let _ = materialize_trial_result(trial_dir, &paths.runtime.result)?;
        }
        MaterializationMode::OutputsOnly => {
            copy_dir_preserve_contents(&paths.out, &trial_dir.join("out"))?;
            copy_file_if_exists(
                &paths.out.join("harness_manifest.json"),
                &trial_dir.join("harness_manifest.json"),
            )?;
            let _ = materialize_trial_result(trial_dir, &paths.runtime.result)?;
        }
        MaterializationMode::MetadataOnly | MaterializationMode::None => {}
    }
    apply_materialization_policy(trial_dir, mode)
}

pub(crate) fn apply_materialization_policy(
    trial_dir: &Path,
    mode: MaterializationMode,
) -> Result<()> {
    match mode {
        MaterializationMode::Full => return Ok(()),
        MaterializationMode::OutputsOnly => {
            for dir_name in ["workspace", "state", "tmp", "artifacts"] {
                remove_path_if_exists(&trial_dir.join(dir_name))?;
            }
        }
        MaterializationMode::MetadataOnly | MaterializationMode::None => {
            for dir_name in ["workspace", "state", "tmp", "artifacts", "out"] {
                remove_path_if_exists(&trial_dir.join(dir_name))?;
            }
            remove_path_if_exists(&trial_dir.join("trial_input.json"))?;
            remove_path_if_exists(&trial_dir.join("result.json"))?;
            remove_path_if_exists(&trial_dir.join("harness_manifest.json"))?;
            remove_path_if_exists(&trial_dir.join("trace_manifest.json"))?;
            if matches!(mode, MaterializationMode::None) {
                remove_path_if_exists(&trial_dir.join("state_inventory.json"))?;
            }
        }
    }
    Ok(())
}

pub(crate) fn resolve_agent_runtime_manifest_path(paths: &TrialPaths) -> Result<PathBuf> {
    map_contract_path_to_host(
        &format!("{}/harness_manifest.json", AGENTLAB_CONTRACT_OUT_DIR),
        &ContractPathHostRoots::from_trial_paths(paths),
        ContractPathMode::ContainerMount,
    )
}

pub(crate) fn write_state_inventory(
    trial_dir: &Path,
    json_value: &Value,
    agent_runtime: &AgentRuntimeConfig,
    _paths: &TrialPaths,
    exec_digest: &str,
    effective_network_mode: &str,
    invocation_source: &str,
    task_sandbox_image: Option<&str>,
    task_sandbox_workdir: Option<&str>,
) -> Result<()> {
    let sanitization_profile = json_value
        .pointer("/design/sanitization_profile")
        .and_then(|v| v.as_str())
        .unwrap_or("hermetic_functional");
    let integration_level = agent_runtime.integration_level.as_str();
    let mode_requested = json_value
        .pointer("/policy/task_sandbox/network")
        .and_then(|v| v.as_str())
        .unwrap_or("none");
    let mode_effective = effective_network_mode;
    let enforcement_effective = if mode_requested == "none" {
        "docker_none"
    } else {
        "unknown"
    };
    let workspace_path = task_sandbox_workdir.unwrap_or(DEFAULT_TASK_WORKDIR_FALLBACK);

    let mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    let mut agent_runtime_mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    agent_runtime_mounts.push(json!({
        "name": "agent_bundle",
        "path": "/opt/agent",
        "writable": false
    }));
    let mut task_sandbox_mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    let sandbox_profile = json_value
        .pointer("/policy/task_sandbox/profile")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    if sandbox_profile == "swebench_testbed" {
        task_sandbox_mounts.push(json!({
            "name": "testbed",
            "path": "/testbed",
            "writable": true
        }));
    }
    let agent_runtime_image = Some(agent_runtime.image.as_str());
    let agent_runtime_image_digest = agent_runtime_image.and_then(resolve_container_image_digest);
    let task_sandbox_image_digest = task_sandbox_image.and_then(resolve_container_image_digest);

    let state = json!({
        "schema_version": "state_inventory_v1",
        "sanitization_profile": sanitization_profile,
        "integration_level": integration_level,
        "mounts": mounts,
        "network": {
            "mode_requested": mode_requested,
            "mode_effective": mode_effective,
            "allowed_hosts": json_value
                .pointer("/policy/task_sandbox/allowed_hosts")
                .cloned()
                .unwrap_or(json!([])),
            "enforcement_effective": enforcement_effective,
            "egress_self_test": {
                "performed": false,
                "cases": []
            }
        },
        "harness_identity": {
            "name": agent_runtime.command_raw.first().cloned().unwrap_or("unknown".to_string()),
            "exec_digest": exec_digest,
            "entry_command": agent_runtime.command_raw.clone()
        },
        "planes": {
            "agent_runtime": {
                "executor": "docker",
                "image": agent_runtime_image,
                "image_digest": agent_runtime_image_digest,
                "workdir": workspace_path,
                "mounts": agent_runtime_mounts,
                "network_mode": agent_runtime.network
            },
            "task_sandbox": {
                "executor": "docker",
                "image": task_sandbox_image,
                "image_digest": task_sandbox_image_digest,
                "workdir": workspace_path,
                "mounts": task_sandbox_mounts,
                "network_mode": mode_effective
            }
        },
        "ext": {
            "agent_runtime_identity": {
                "invocation_source": invocation_source
            }
        },
        "violations": {
            "state_leak": false,
            "profile_invariant_violation": false,
            "notes": []
        }
    });
    atomic_write_json_pretty(&trial_dir.join("state_inventory.json"), &state)?;
    Ok(())
}
