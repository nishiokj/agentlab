use crate::*;

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::Utc;
use lab_core::{
    canonical_json_digest, ensure_dir, runner_runtime_host_paths, sha256_bytes, sha256_file,
    ArtifactStore, RunnerRuntimeHostPaths, AGENTLAB_CONTRACT_GRADER_AUX_DIR,
    AGENTLAB_CONTRACT_IN_DIR, AGENTLAB_CONTRACT_OUT_DIR, AGENTLAB_CONTRACT_RUNTIME_AUX_DIR,
    AGENTLAB_ENV_GRADER_INPUT_PATH, AGENTLAB_ENV_MAPPED_GRADER_OUTPUT_PATH,
    AGENTLAB_ENV_RAW_GRADER_OUTPUT_PATH, AGENTLAB_ENV_REPL_IDX, AGENTLAB_ENV_RESULT_PATH,
    AGENTLAB_ENV_RUN_ID, AGENTLAB_ENV_TASK_ID, AGENTLAB_ENV_TIMEOUT_MS,
    AGENTLAB_ENV_TRAJECTORY_PATH, AGENTLAB_ENV_TRIAL_ID, AGENTLAB_ENV_TRIAL_INPUT_PATH,
    AGENTLAB_ENV_VARIANT_ID, AGENTLAB_RUNNER_SUPPORT_REL_DIR, AGENTLAB_TASK_WORKDIR_PLACEHOLDER,
};
use lab_hooks::{load_manifest, validate_hooks};
use lab_provenance::{default_attestation, write_attestation};
use lab_schemas::compile_schema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

pub(crate) fn parse_bool_env(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

pub(crate) fn progress_logs_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        if let Ok(value) = env::var(AGENTLAB_PROGRESS_LOG_ENV) {
            if let Some(parsed) = parse_bool_env(&value) {
                return parsed;
            }
        }
        !cfg!(test)
    })
}

pub(crate) fn emit_progress_log(scope: &str, message: impl AsRef<str>) {
    if !progress_logs_enabled() {
        return;
    }
    eprintln!("[{}] {}", scope, message.as_ref());
    let _ = std::io::stderr().flush();
}

pub(crate) fn emit_preflight_log(message: impl AsRef<str>) {
    emit_progress_log("preflight", message);
}

pub(crate) fn emit_run_log(run_id: &str, message: impl AsRef<str>) {
    emit_progress_log("run", format!("{}: {}", run_id, message.as_ref()));
}

pub(crate) fn should_emit_image_probe_progress(index: usize, total: usize) -> bool {
    if total <= 5 {
        return true;
    }
    index == 1 || index == total || index % 5 == 0
}

pub(crate) fn parse_parallelism(raw: &str) -> Option<usize> {
    raw.trim().parse::<usize>().ok().and_then(|value| {
        if value == 0 {
            None
        } else {
            Some(value.min(MAX_PREFLIGHT_IMAGE_PROBE_PARALLELISM))
        }
    })
}

pub(crate) fn preflight_image_probe_parallelism() -> usize {
    match env::var(AGENTLAB_PREFLIGHT_IMAGE_PROBE_PARALLELISM_ENV) {
        Ok(raw) => parse_parallelism(&raw).unwrap_or(DEFAULT_PREFLIGHT_IMAGE_PROBE_PARALLELISM),
        Err(_) => DEFAULT_PREFLIGHT_IMAGE_PROBE_PARALLELISM,
    }
}

pub(crate) fn run_bounded_image_probes<T, F>(images: &[String], label: &str, probe: F) -> Vec<T>
where
    T: Send,
    F: Fn(usize, &str) -> T + Sync,
{
    if images.is_empty() {
        return Vec::new();
    }
    let configured = preflight_image_probe_parallelism();
    let parallelism = configured.min(images.len()).max(1);
    if parallelism <= 1 || images.len() <= 1 {
        return images
            .iter()
            .enumerate()
            .map(|(idx, image)| probe(idx, image))
            .collect();
    }
    emit_preflight_log(format!(
        "{}: bounded probe parallelism={}",
        label, parallelism
    ));
    let next_index = AtomicUsize::new(0);
    let results = Mutex::new(
        std::iter::repeat_with(|| None)
            .take(images.len())
            .collect::<Vec<Option<T>>>(),
    );
    thread::scope(|scope| {
        for _ in 0..parallelism {
            let results_ref = &results;
            let next_index_ref = &next_index;
            let probe_ref = &probe;
            scope.spawn(move || loop {
                let idx = next_index_ref.fetch_add(1, Ordering::SeqCst);
                if idx >= images.len() {
                    break;
                }
                let result = probe_ref(idx, &images[idx]);
                let mut guard = results_ref
                    .lock()
                    .expect("preflight image probe results lock poisoned");
                guard[idx] = Some(result);
            });
        }
    });
    let collected = results
        .into_inner()
        .expect("preflight image probe results lock poisoned");
    collected
        .into_iter()
        .map(|entry| entry.expect("preflight image probe result missing"))
        .collect()
}

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

#[derive(Clone)]
pub(crate) struct AdapterRunRequest<'a> {
    pub(crate) runtime_experiment: &'a Value,
    pub(crate) runtime: &'a AgentRuntimeConfig,
    pub(crate) variant_args: &'a [String],
    pub(crate) runtime_env: &'a BTreeMap<String, String>,
    pub(crate) runtime_overrides_env: &'a BTreeMap<String, String>,
    pub(crate) trial_paths: &'a TrialPaths,
    pub(crate) dynamic_mounts: &'a [ResolvedMountReference],
    pub(crate) io_paths: &'a PreparedTrialIo,
    pub(crate) network_mode: &'a str,
    pub(crate) benchmark_grader: Option<&'a BenchmarkGraderConfig>,
    pub(crate) benchmark_grading_enabled: bool,
    pub(crate) run_id: &'a str,
    pub(crate) task_image: &'a str,
    pub(crate) task_workdir: &'a str,
    pub(crate) task_materialization_kind: TaskMaterializationKind,
    pub(crate) agent_artifact: Option<&'a Path>,
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

pub(crate) fn parse_max_workspace_bundle_bytes_from_env() -> Result<u64> {
    match env::var(AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES_ENV) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(DEFAULT_MAX_WORKSPACE_BUNDLE_BYTES);
            }
            let parsed = trimmed.parse::<u64>().map_err(|_| {
                anyhow!(
                    "{} must be a positive integer when set (got: {})",
                    AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES_ENV,
                    raw
                )
            })?;
            if parsed == 0 {
                return Err(anyhow!(
                    "{} must be > 0 when set",
                    AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES_ENV
                ));
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(DEFAULT_MAX_WORKSPACE_BUNDLE_BYTES),
        Err(err) => Err(anyhow!(
            "failed reading {}: {}",
            AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES_ENV,
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

#[derive(Debug)]
pub(crate) struct RunOperationLease {
    path: PathBuf,
    operation_id: String,
}

impl Drop for RunOperationLease {
    fn drop(&mut self) {
        let should_remove = fs::read(&self.path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<OperationLeaseRecord>(&bytes).ok())
            .map(|record| record.operation_id == self.operation_id)
            .unwrap_or(false);
        if should_remove {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) struct EngineLeaseGuard {
    stop: Arc<AtomicBool>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl Drop for EngineLeaseGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) fn operation_lease_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("operation_lease.json")
}

pub(crate) fn operation_owner_host() -> String {
    env::var("HOSTNAME")
        .or_else(|_| env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

pub(crate) fn parse_rfc3339_utc(raw: &str) -> Option<chrono::DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|ts| ts.with_timezone(&Utc))
}

pub(crate) fn operation_lease_is_stale(
    record: &OperationLeaseRecord,
    now: chrono::DateTime<Utc>,
) -> bool {
    parse_rfc3339_utc(&record.expires_at)
        .map(|expires_at| now > expires_at)
        .unwrap_or(true)
}

pub(crate) fn engine_lease_is_stale(
    record: &EngineLeaseRecord,
    now: chrono::DateTime<Utc>,
) -> bool {
    parse_rfc3339_utc(&record.expires_at)
        .map(|expires_at| now > expires_at)
        .unwrap_or(true)
}

pub(crate) fn next_unique_id(prefix: &str) -> String {
    static UNIQUE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
    let nonce = UNIQUE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let raw = format!(
        "{}:{}:{}:{}:{}",
        prefix,
        ts,
        std::process::id(),
        nonce,
        operation_owner_host()
    );
    let digest = sha256_bytes(raw.as_bytes());
    format!("{}_{}", prefix, &digest[..16])
}

pub(crate) fn make_operation_lease_record(
    op_type: RunOperationType,
    stale_takeover_of: Option<String>,
) -> OperationLeaseRecord {
    let now = Utc::now();
    let expires = now + chrono::Duration::seconds(OPERATION_LEASE_TTL_SECONDS);
    OperationLeaseRecord {
        schema_version: "operation_lease_v1".to_string(),
        operation_id: next_unique_id("op"),
        op_type: op_type.as_str().to_string(),
        owner_pid: std::process::id(),
        owner_host: operation_owner_host(),
        acquired_at: now.to_rfc3339(),
        expires_at: expires.to_rfc3339(),
        stale_takeover_of,
    }
}

pub(crate) fn acquire_run_operation_lease(
    run_dir: &Path,
    op_type: RunOperationType,
) -> Result<RunOperationLease> {
    let lease_path = operation_lease_path(run_dir);
    if let Some(parent) = lease_path.parent() {
        ensure_dir(parent)?;
    }
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lease_path)
    {
        Ok(mut file) => {
            let lease = make_operation_lease_record(op_type, None);
            let bytes = serde_json::to_vec_pretty(&lease)?;
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            let _ = file.sync_all();
            if let Some(parent) = lease_path.parent() {
                if let Ok(dir) = fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
            Ok(RunOperationLease {
                path: lease_path,
                operation_id: lease.operation_id,
            })
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let now = Utc::now();
            let existing = fs::read(&lease_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<OperationLeaseRecord>(&bytes).ok());
            if let Some(existing) = existing {
                if operation_lease_is_stale(&existing, now) {
                    let replacement =
                        make_operation_lease_record(op_type, Some(existing.operation_id.clone()));
                    atomic_write_json_pretty(&lease_path, &serde_json::to_value(&replacement)?)?;
                    return Ok(RunOperationLease {
                        path: lease_path,
                        operation_id: replacement.operation_id,
                    });
                }
            }
            Err(anyhow!(
                "operation_in_progress: run is already under control operation"
            ))
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn load_engine_lease(run_dir: &Path) -> Result<Option<EngineLeaseRecord>> {
    let store = BackingSqliteStore::open(run_dir)?;
    let Some(value) = store.get_runtime_json(RUNTIME_KEY_ENGINE_LEASE)? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_value::<EngineLeaseRecord>(value)?))
}

pub(crate) fn write_engine_lease(run_dir: &Path, lease: &EngineLeaseRecord) -> Result<()> {
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_ENGINE_LEASE, &serde_json::to_value(lease)?)
}

pub(crate) fn make_engine_lease(run_id: &str, epoch: u64) -> EngineLeaseRecord {
    let now = Utc::now();
    let expires = now + chrono::Duration::seconds(ENGINE_LEASE_TTL_SECONDS);
    EngineLeaseRecord {
        schema_version: "engine_lease_v1".to_string(),
        run_id: run_id.to_string(),
        owner_id: next_unique_id("owner"),
        pid: std::process::id(),
        hostname: operation_owner_host(),
        started_at: now.to_rfc3339(),
        heartbeat_at: now.to_rfc3339(),
        expires_at: expires.to_rfc3339(),
        epoch,
    }
}

pub(crate) fn ensure_engine_lease_for_run(
    run_dir: &Path,
    run_id: &str,
) -> Result<EngineLeaseRecord> {
    if let Some(existing) = load_engine_lease(run_dir)? {
        if existing.run_id != run_id {
            return Err(anyhow!(
                "engine lease run_id mismatch: lease has {}, run expects {}",
                existing.run_id,
                run_id
            ));
        }
        return Ok(existing);
    }
    let lease = make_engine_lease(run_id, 1);
    write_engine_lease(run_dir, &lease)?;
    Ok(lease)
}

pub(crate) fn start_engine_lease_heartbeat(
    run_dir: &Path,
    run_id: &str,
) -> Result<EngineLeaseGuard> {
    let existing = ensure_engine_lease_for_run(run_dir, run_id)?;
    let mut heartbeat_lease = make_engine_lease(run_id, existing.epoch + 1);
    write_engine_lease(run_dir, &heartbeat_lease)?;
    let run_dir = run_dir.to_path_buf();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_signal = stop.clone();
    let join_handle = thread::spawn(move || {
        while !stop_signal.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_secs(ENGINE_LEASE_HEARTBEAT_SECONDS as u64));
            let now = Utc::now();
            heartbeat_lease.heartbeat_at = now.to_rfc3339();
            heartbeat_lease.expires_at =
                (now + chrono::Duration::seconds(ENGINE_LEASE_TTL_SECONDS)).to_rfc3339();
            let _ = write_engine_lease(&run_dir, &heartbeat_lease);
        }
    });
    Ok(EngineLeaseGuard {
        stop,
        join_handle: Some(join_handle),
    })
}

pub(crate) fn adopt_engine_lease_for_recovery(
    run_dir: &Path,
    run_id: &str,
    force: bool,
) -> Result<EngineLeaseRecord> {
    let now = Utc::now();
    let existing = load_engine_lease(run_dir)?;
    if let Some(ref lease) = existing {
        if lease.run_id != run_id {
            return Err(anyhow!(
                "engine lease run_id mismatch: lease has {}, run expects {}",
                lease.run_id,
                run_id
            ));
        }
        if !force && !engine_lease_is_stale(lease, now) {
            return Err(anyhow!(
                "run_owner_alive: engine owner '{}' pid={} lease expires {}",
                lease.owner_id,
                lease.pid,
                lease.expires_at
            ));
        }
    }
    let epoch = existing.map(|lease| lease.epoch + 1).unwrap_or(1);
    let adopted = make_engine_lease(run_id, epoch);
    write_engine_lease(run_dir, &adopted)?;
    Ok(adopted)
}

pub(crate) fn append_slot_commit_record(run_dir: &Path, record: &SlotCommitRecord) -> Result<()> {
    let record_json = serde_json::to_value(record)?;
    validate_schema_contract_value(&record_json, "slot commit record")?;
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.upsert_slot_commit_record(&record_json)
}

pub(crate) fn load_slot_commit_records(run_dir: &Path) -> Result<Vec<SlotCommitRecord>> {
    let store = BackingSqliteStore::open(run_dir)?;
    let run_id = store
        .get_runtime_json(RUNTIME_KEY_RUN_CONTROL)?
        .and_then(|value| {
            value
                .pointer("/run_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| store.first_run_id_with_slot_commits().ok().flatten())
        .unwrap_or_default();
    if run_id.is_empty() {
        return Ok(Vec::new());
    }
    let values = store.load_slot_commit_records(&run_id)?;
    let mut rows = Vec::with_capacity(values.len());
    for value in values {
        rows.push(serde_json::from_value::<SlotCommitRecord>(value)?);
    }
    Ok(rows)
}

pub(crate) fn load_pending_trial_completion_records(
    run_dir: &Path,
) -> Result<BTreeMap<usize, TrialExecutionResult>> {
    let store = BackingSqliteStore::open(run_dir)?;
    let run_id = store
        .get_runtime_json(RUNTIME_KEY_RUN_CONTROL)?
        .and_then(|value| {
            value
                .pointer("/run_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| store.first_run_id_with_pending_completions().ok().flatten())
        .unwrap_or_default();
    if run_id.is_empty() {
        return Ok(BTreeMap::new());
    }
    let records = store.load_pending_trial_completions(&run_id)?;
    let mut by_schedule = BTreeMap::new();
    for record_value in records {
        let record: PendingTrialCompletionRecord = serde_json::from_value(record_value)?;
        if record.schema_version != "pending_trial_completion_v1" {
            continue;
        }
        by_schedule.insert(record.schedule_idx, record.trial_result);
    }
    Ok(by_schedule)
}

pub(crate) fn persist_pending_trial_completions(
    run_dir: &Path,
    committer: &DeterministicCommitter,
) -> Result<()> {
    let records = committer.pending_trial_completion_records();
    let run_id = BackingSqliteStore::open(run_dir)?
        .get_runtime_json(RUNTIME_KEY_RUN_CONTROL)?
        .and_then(|value| {
            value
                .pointer("/run_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| {
            records
                .iter()
                .find_map(|row| row.trial_result.deferred_trial_records.first())
                .map(|row| row.run_id.clone())
        })
        .unwrap_or_default();
    if run_id.is_empty() {
        return Ok(());
    }
    let values = records
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for value in &values {
        validate_schema_contract_value(value, "pending trial completion")?;
    }
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.replace_pending_trial_completions(&run_id, &values)
}

pub(crate) fn highest_attempt_by_schedule(records: &[SlotCommitRecord]) -> HashMap<usize, usize> {
    let mut by_schedule = HashMap::new();
    for record in records {
        let entry = by_schedule.entry(record.schedule_idx).or_insert(0);
        if record.attempt > *entry {
            *entry = record.attempt;
        }
    }
    by_schedule
}

pub(crate) fn make_slot_commit_id(
    run_id: &str,
    schedule_idx: usize,
    attempt: usize,
    payload_digest: &str,
) -> String {
    let raw = format!("{}:{}:{}:{}", run_id, schedule_idx, attempt, payload_digest);
    let digest = sha256_bytes(raw.as_bytes());
    format!("slot_{}", &digest[..24])
}

pub(crate) fn commit_record_by_schedule(
    records: &[SlotCommitRecord],
) -> BTreeMap<usize, SlotCommitRecord> {
    let mut by_schedule = BTreeMap::new();
    for record in records {
        if record.record_type == "commit" {
            by_schedule.insert(record.schedule_idx, record.clone());
        }
    }
    by_schedule
}
pub(crate) fn write_trial_state(
    trial_dir: &Path,
    trial_id: &str,
    status: &str,
    pause_label: Option<&str>,
    checkpoint_selected: Option<&str>,
    exit_reason: Option<&str>,
) -> Result<()> {
    let payload = json!({
        "schema_version": "trial_state_v1",
        "trial_id": trial_id,
        "status": status,
        "pause_label": pause_label,
        "checkpoint_selected": checkpoint_selected,
        "exit_reason": exit_reason,
        "updated_at": Utc::now().to_rfc3339(),
    });
    atomic_write_json_pretty(&trial_dir.join("trial_state.json"), &payload)
}

pub(crate) struct TrialStateGuard {
    trial_dir: PathBuf,
    trial_id: String,
    done: bool,
}

impl TrialStateGuard {
    pub(crate) fn new(trial_dir: &Path, trial_id: &str) -> Self {
        Self {
            trial_dir: trial_dir.to_path_buf(),
            trial_id: trial_id.to_string(),
            done: false,
        }
    }

    pub(crate) fn complete(&mut self, status: &str, exit_reason: Option<&str>) -> Result<()> {
        write_trial_state(
            &self.trial_dir,
            &self.trial_id,
            status,
            None,
            None,
            exit_reason,
        )?;
        self.done = true;
        Ok(())
    }
}

impl Drop for TrialStateGuard {
    fn drop(&mut self) {
        if !self.done {
            let _ = write_trial_state(
                &self.trial_dir,
                &self.trial_id,
                "failed",
                None,
                None,
                Some("aborted"),
            );
        }
    }
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

/// Derive the project root from a run directory path.
/// Run dirs live at `{project_root}/.lab/runs/{run_id}/`, so we navigate up 3 levels.
pub(crate) fn find_project_root_from_run_dir(run_dir: &Path) -> Result<PathBuf> {
    // run_dir = {root}/.lab/runs/{run_id}
    let root = run_dir
        .parent() // .lab/runs
        .and_then(|p| p.parent()) // .lab
        .and_then(|p| p.parent()) // root
        .ok_or_else(|| {
            anyhow!(
                "cannot derive project root from run_dir: {}",
                run_dir.display()
            )
        })?;
    Ok(root.to_path_buf())
}

/// Continue a previously interrupted run from where it stopped.
///
/// Loads persisted run session + schedule progress from sqlite runtime state, validates the run is
/// in a continuable terminal state, reconstructs experiment parameters,
/// verifies schedule integrity, and re-enters the trial loop from the next
/// unprocessed slot.
pub fn continue_run(run_dir: &Path) -> Result<RunResult> {
    continue_run_with_options(run_dir, RunExecutionOptions::default())
}
