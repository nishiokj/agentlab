use anyhow::{anyhow, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::Utc;
use lab_core::{
    canonical_json_digest, ensure_dir, runner_runtime_host_paths, sha256_bytes, sha256_file,
    ArtifactStore, RunnerRuntimeHostPaths, AGENTLAB_CONTRACT_DEPS_DIR, AGENTLAB_CONTRACT_IN_DIR,
    AGENTLAB_CONTRACT_OUT_DIR, AGENTLAB_CONTRACT_STATE_DIR, AGENTLAB_CONTRACT_WORKSPACE_DIR,
    AGENTLAB_ENV_BINDINGS_PATH, AGENTLAB_ENV_DEPENDENCIES_PATH, AGENTLAB_ENV_POLICY_PATH,
    AGENTLAB_ENV_REPL_IDX, AGENTLAB_ENV_RESULT_PATH, AGENTLAB_ENV_RUN_ID, AGENTLAB_ENV_TASK_ID,
    AGENTLAB_ENV_TASK_PATH, AGENTLAB_ENV_TIMEOUT_MS, AGENTLAB_ENV_TRAJECTORY_PATH,
    AGENTLAB_ENV_TRIAL_ID, AGENTLAB_ENV_VARIANT_ID,
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

fn parse_bool_env(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn progress_logs_enabled() -> bool {
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

fn emit_progress_log(scope: &str, message: impl AsRef<str>) {
    if !progress_logs_enabled() {
        return;
    }
    eprintln!("[{}] {}", scope, message.as_ref());
    let _ = std::io::stderr().flush();
}

fn emit_preflight_log(message: impl AsRef<str>) {
    emit_progress_log("preflight", message);
}

fn emit_run_log(run_id: &str, message: impl AsRef<str>) {
    emit_progress_log("run", format!("{}: {}", run_id, message.as_ref()));
}

fn should_emit_image_probe_progress(index: usize, total: usize) -> bool {
    if total <= 5 {
        return true;
    }
    index == 1 || index == total || index % 5 == 0
}

fn parse_parallelism(raw: &str) -> Option<usize> {
    raw.trim().parse::<usize>().ok().and_then(|value| {
        if value == 0 {
            None
        } else {
            Some(value.min(MAX_PREFLIGHT_IMAGE_PROBE_PARALLELISM))
        }
    })
}

fn preflight_image_probe_parallelism() -> usize {
    match env::var(AGENTLAB_PREFLIGHT_IMAGE_PROBE_PARALLELISM_ENV) {
        Ok(raw) => parse_parallelism(&raw).unwrap_or(DEFAULT_PREFLIGHT_IMAGE_PROBE_PARALLELISM),
        Err(_) => DEFAULT_PREFLIGHT_IMAGE_PROBE_PARALLELISM,
    }
}

fn run_bounded_image_probes<T, F>(images: &[String], label: &str, probe: F) -> Vec<T>
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

fn emit_slot_commit_progress(
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
struct AdapterRunRequest<'a> {
    runtime_experiment: &'a Value,
    runtime: &'a AgentRuntimeConfig,
    variant_args: &'a [String],
    runtime_env: &'a BTreeMap<String, String>,
    runtime_overrides_env: &'a BTreeMap<String, String>,
    trial_paths: &'a TrialPaths,
    dynamic_mounts: &'a [ResolvedMountReference],
    io_paths: &'a PreparedTrialIo,
    network_mode: &'a str,
    benchmark_grader: Option<&'a BenchmarkGraderConfig>,
    benchmark_grading_enabled: bool,
    run_id: &'a str,
    task_image: Option<&'a str>,
    agent_artifact: Option<&'a Path>,
}

#[derive(Clone)]
struct AdapterPauseRequest<'a> {
    control: &'a ActiveAdapterControl,
    label: &'a str,
    timeout: Duration,
}

// ---------------------------------------------------------------------------
// Worker execution boundary contracts (P1: contract freeze)
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TrialDispatch {
    run_id: String,
    trial_id: String,
    schedule_idx: usize,
    slot: TrialSlot,
    variant_id: String,
    task_id: String,
    repl_idx: usize,
    runtime_profile: Value,
    task_payload: Value,
    effective_policy: Value,
}

#[allow(dead_code)]
trait WorkerBackend: Send + Sync {
    fn submit(&self, dispatch: TrialDispatch) -> Result<WorkerTicket>;
    fn poll_completions(&self, timeout: Duration) -> Result<Vec<TrialCompletion>>;
    fn request_pause(&self, worker_id: &str, label: &str) -> Result<WorkerPauseAck>;
    fn request_stop(&self, worker_id: &str, reason: &str) -> Result<()>;
}

#[allow(dead_code)]
type LocalTrialExecutor = dyn Fn(TrialDispatch) -> Result<TrialCompletion> + Send + Sync + 'static;

#[allow(dead_code)]
#[derive(Clone)]
struct LocalThreadWorkerBackend {
    inner: Arc<LocalThreadWorkerBackendInner>,
}

struct LocalThreadWorkerBackendInner {
    max_in_flight: usize,
    capacity_warning: Option<String>,
    max_completions_per_poll: usize,
    executor: Arc<LocalTrialExecutor>,
    next_ticket_seq: AtomicU64,
    next_worker_seq: AtomicU64,
    completions_tx: mpsc::Sender<TrialCompletion>,
    completions_rx: Mutex<mpsc::Receiver<TrialCompletion>>,
    state: Mutex<LocalThreadWorkerState>,
}

#[derive(Default)]
struct LocalThreadWorkerState {
    in_flight_by_ticket: HashMap<String, WorkerTicket>,
}

fn parse_local_worker_capacity_ceiling_from_env() -> Result<Option<usize>> {
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

fn parse_max_run_bytes_from_env() -> Result<Option<u64>> {
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

fn parse_max_workspace_bundle_bytes_from_env() -> Result<u64> {
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

fn resolve_local_worker_max_in_flight(
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

#[allow(dead_code)]
impl LocalThreadWorkerBackend {
    fn new(max_in_flight: usize, executor: Arc<LocalTrialExecutor>) -> Result<Self> {
        let configured_ceiling = parse_local_worker_capacity_ceiling_from_env()?;
        Self::new_with_ceiling(max_in_flight, executor, configured_ceiling)
    }

    fn new_with_ceiling(
        max_in_flight: usize,
        executor: Arc<LocalTrialExecutor>,
        configured_ceiling: Option<usize>,
    ) -> Result<Self> {
        if max_in_flight == 0 {
            return Err(anyhow!("local worker backend requires max_in_flight > 0"));
        }
        let (effective_max_in_flight, capacity_warning) =
            resolve_local_worker_max_in_flight(max_in_flight, configured_ceiling);
        let (tx, rx) = mpsc::channel();
        Ok(Self {
            inner: Arc::new(LocalThreadWorkerBackendInner {
                max_in_flight: effective_max_in_flight,
                capacity_warning,
                max_completions_per_poll: LOCAL_WORKER_MAX_COMPLETIONS_PER_POLL,
                executor,
                next_ticket_seq: AtomicU64::new(1),
                next_worker_seq: AtomicU64::new(1),
                completions_tx: tx,
                completions_rx: Mutex::new(rx),
                state: Mutex::new(LocalThreadWorkerState::default()),
            }),
        })
    }

    fn next_ticket(&self, trial_id: &str) -> WorkerTicket {
        let ticket_seq = self.inner.next_ticket_seq.fetch_add(1, Ordering::Relaxed);
        let worker_seq = self.inner.next_worker_seq.fetch_add(1, Ordering::Relaxed);
        WorkerTicket {
            worker_id: format!("local.worker.{}", worker_seq),
            ticket_id: format!("local.ticket.{}", ticket_seq),
            trial_id: trial_id.to_string(),
        }
    }

    fn normalize_completion(
        dispatch: &TrialDispatch,
        ticket: &WorkerTicket,
        mut completion: TrialCompletion,
    ) -> TrialCompletion {
        completion.ticket = ticket.clone();
        completion.schedule_idx = dispatch.schedule_idx;
        completion
    }

    fn worker_error_completion(
        dispatch: &TrialDispatch,
        ticket: &WorkerTicket,
        err: &anyhow::Error,
    ) -> TrialCompletion {
        TrialCompletion {
            ticket: ticket.clone(),
            schedule_idx: dispatch.schedule_idx,
            completion_seq: None,
            terminal_status: "failed".to_string(),
            classification: "local_worker_error".to_string(),
            artifacts: json!({
                "error": err.to_string(),
            }),
            metrics: json!({}),
            runtime_summary: json!({}),
        }
    }

    fn consume_completion(&self, completion: TrialCompletion) -> Result<TrialCompletion> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| anyhow!("local worker backend state lock poisoned"))?;
        if state
            .in_flight_by_ticket
            .remove(completion.ticket.ticket_id.as_str())
            .is_none()
        {
            return Err(anyhow!(
                "local worker backend protocol fault: completion for unknown ticket {}",
                completion.ticket.ticket_id
            ));
        }
        Ok(completion)
    }

    fn effective_max_in_flight(&self) -> usize {
        self.inner.max_in_flight
    }

    fn capacity_warning(&self) -> Option<&str> {
        self.inner.capacity_warning.as_deref()
    }
}

impl WorkerBackend for LocalThreadWorkerBackend {
    fn submit(&self, dispatch: TrialDispatch) -> Result<WorkerTicket> {
        let ticket = self.next_ticket(&dispatch.trial_id);
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| anyhow!("local worker backend state lock poisoned"))?;
            if state.in_flight_by_ticket.len() >= self.inner.max_in_flight {
                return Err(anyhow!(
                    "{} in_flight={} max_in_flight={}",
                    LOCAL_WORKER_CAPACITY_ERROR_PREFIX,
                    state.in_flight_by_ticket.len(),
                    self.inner.max_in_flight
                ));
            }
            state
                .in_flight_by_ticket
                .insert(ticket.ticket_id.clone(), ticket.clone());
        }

        let dispatch_for_worker = dispatch.clone();
        let ticket_for_worker = ticket.clone();
        let executor = self.inner.executor.clone();
        let completions_tx = self.inner.completions_tx.clone();
        let spawn_result = thread::Builder::new()
            .name(format!("agentlab-{}", ticket_for_worker.ticket_id))
            .spawn(move || {
                let completion = match executor(dispatch_for_worker.clone()) {
                    Ok(completion) => LocalThreadWorkerBackend::normalize_completion(
                        &dispatch_for_worker,
                        &ticket_for_worker,
                        completion,
                    ),
                    Err(err) => LocalThreadWorkerBackend::worker_error_completion(
                        &dispatch_for_worker,
                        &ticket_for_worker,
                        &err,
                    ),
                };
                let _ = completions_tx.send(completion);
            });
        if let Err(err) = spawn_result {
            // Best-effort rollback of ticket bookkeeping so failed spawn attempts do not
            // permanently consume local worker capacity.
            if let Ok(mut state) = self.inner.state.lock() {
                state.in_flight_by_ticket.remove(ticket.ticket_id.as_str());
            }
            return Err(anyhow!("failed to spawn local worker thread: {}", err));
        }

        Ok(ticket)
    }

    fn poll_completions(&self, timeout: Duration) -> Result<Vec<TrialCompletion>> {
        let mut raw: Vec<TrialCompletion> = Vec::new();
        let max_per_poll = self.inner.max_completions_per_poll.max(1);
        {
            let rx = self
                .inner
                .completions_rx
                .lock()
                .map_err(|_| anyhow!("local worker backend completion lock poisoned"))?;
            match rx.recv_timeout(timeout) {
                Ok(completion) => raw.push(completion),
                Err(mpsc::RecvTimeoutError::Timeout) => return Ok(Vec::new()),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(anyhow!(
                        "local worker backend completion channel disconnected"
                    ));
                }
            }
            while raw.len() < max_per_poll {
                match rx.try_recv() {
                    Ok(completion) => raw.push(completion),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            }
        }
        let mut completions = Vec::with_capacity(raw.len());
        for completion in raw {
            completions.push(self.consume_completion(completion)?);
        }
        Ok(completions)
    }

    fn request_pause(&self, worker_id: &str, label: &str) -> Result<WorkerPauseAck> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| anyhow!("local worker backend state lock poisoned"))?;
        let ticket = state
            .in_flight_by_ticket
            .values()
            .find(|ticket| ticket.worker_id == worker_id)
            .ok_or_else(|| {
                anyhow!(
                    "local worker backend pause failed: unknown active worker {}",
                    worker_id
                )
            })?;
        Err(anyhow!(
            "local worker backend does not support pause for active worker {} (trial {} label {})",
            worker_id,
            ticket.trial_id,
            label
        ))
    }

    fn request_stop(&self, worker_id: &str, reason: &str) -> Result<()> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| anyhow!("local worker backend state lock poisoned"))?;
        let _ = state
            .in_flight_by_ticket
            .values()
            .find(|ticket| ticket.worker_id == worker_id)
            .ok_or_else(|| {
                anyhow!(
                    "local worker backend stop failed: unknown active worker {} (reason: {})",
                    worker_id,
                    reason
                )
            })?;
        Err(anyhow!(
            "local worker backend does not support stop for active worker {} (reason: {})",
            worker_id,
            reason
        ))
    }
}

trait AgentAdapter {
    fn capabilities(&self) -> AgentAdapterCapabilities;

    fn run_trial(&self, request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult>;

    fn pause_trial(&self, request: &AdapterPauseRequest<'_>) -> Result<AdapterPauseAck>;
}

#[derive(Debug, Clone, Copy)]
struct BuiltinCommandAdapter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrebuiltAdapterFlavor {
    CodexCli,
    RexJesus,
}

#[derive(Debug, Clone, Copy)]
struct PrebuiltCommandAdapter {
    flavor: PrebuiltAdapterFlavor,
}

fn supported_agent_adapters() -> &'static [(&'static str, &'static str)] {
    &[
        (BUILTIN_COMMAND_ADAPTER_ID, BUILTIN_COMMAND_ADAPTER_VERSION),
        (PREBUILT_CODEX_ADAPTER_ID, PREBUILT_AGENT_ADAPTER_VERSION),
        (
            PREBUILT_REX_JESUS_ADAPTER_ID,
            PREBUILT_AGENT_ADAPTER_VERSION,
        ),
    ]
}

fn adapter_registry_entry(adapter_ref: &AgentAdapterRef) -> Result<Box<dyn AgentAdapter>> {
    match (adapter_ref.id.as_str(), adapter_ref.version.as_str()) {
        (BUILTIN_COMMAND_ADAPTER_ID, BUILTIN_COMMAND_ADAPTER_VERSION) => {
            Ok(Box::new(BuiltinCommandAdapter))
        }
        (PREBUILT_CODEX_ADAPTER_ID, PREBUILT_AGENT_ADAPTER_VERSION) => {
            Ok(Box::new(PrebuiltCommandAdapter {
                flavor: PrebuiltAdapterFlavor::CodexCli,
            }))
        }
        (PREBUILT_REX_JESUS_ADAPTER_ID, PREBUILT_AGENT_ADAPTER_VERSION) => {
            Ok(Box::new(PrebuiltCommandAdapter {
                flavor: PrebuiltAdapterFlavor::RexJesus,
            }))
        }
        _ => Err(anyhow!(
            "unsupported runtime.agent adapter '{}@{}'; supported: {}",
            adapter_ref.id,
            adapter_ref.version,
            supported_agent_adapters()
                .iter()
                .map(|(id, version)| format!("{}@{}", id, version))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

#[derive(Debug)]
struct RunOperationLease {
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

struct EngineLeaseGuard {
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

fn operation_lease_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("operation_lease.json")
}

fn operation_owner_host() -> String {
    env::var("HOSTNAME")
        .or_else(|_| env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

fn parse_rfc3339_utc(raw: &str) -> Option<chrono::DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|ts| ts.with_timezone(&Utc))
}

fn operation_lease_is_stale(record: &OperationLeaseRecord, now: chrono::DateTime<Utc>) -> bool {
    parse_rfc3339_utc(&record.expires_at)
        .map(|expires_at| now > expires_at)
        .unwrap_or(true)
}

fn engine_lease_is_stale(record: &EngineLeaseRecord, now: chrono::DateTime<Utc>) -> bool {
    parse_rfc3339_utc(&record.expires_at)
        .map(|expires_at| now > expires_at)
        .unwrap_or(true)
}

fn next_unique_id(prefix: &str) -> String {
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

fn make_operation_lease_record(
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

fn acquire_run_operation_lease(
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

fn load_engine_lease(run_dir: &Path) -> Result<Option<EngineLeaseRecord>> {
    let store = BackingSqliteStore::open(run_dir)?;
    let Some(value) = store.get_runtime_json(RUNTIME_KEY_ENGINE_LEASE)? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_value::<EngineLeaseRecord>(value)?))
}

fn write_engine_lease(run_dir: &Path, lease: &EngineLeaseRecord) -> Result<()> {
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_ENGINE_LEASE, &serde_json::to_value(lease)?)
}

fn make_engine_lease(run_id: &str, epoch: u64) -> EngineLeaseRecord {
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

fn ensure_engine_lease_for_run(run_dir: &Path, run_id: &str) -> Result<EngineLeaseRecord> {
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

fn start_engine_lease_heartbeat(run_dir: &Path, run_id: &str) -> Result<EngineLeaseGuard> {
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

fn adopt_engine_lease_for_recovery(
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

fn append_slot_commit_record(run_dir: &Path, record: &SlotCommitRecord) -> Result<()> {
    let record_json = serde_json::to_value(record)?;
    validate_schema_contract_value(&record_json, "slot commit record")?;
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.upsert_slot_commit_record(&record_json)
}

fn load_slot_commit_records(run_dir: &Path) -> Result<Vec<SlotCommitRecord>> {
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

fn load_pending_trial_completion_records(
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

fn persist_pending_trial_completions(
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

fn highest_attempt_by_schedule(records: &[SlotCommitRecord]) -> HashMap<usize, usize> {
    let mut by_schedule = HashMap::new();
    for record in records {
        let entry = by_schedule.entry(record.schedule_idx).or_insert(0);
        if record.attempt > *entry {
            *entry = record.attempt;
        }
    }
    by_schedule
}

fn make_slot_commit_id(
    run_id: &str,
    schedule_idx: usize,
    attempt: usize,
    payload_digest: &str,
) -> String {
    let raw = format!("{}:{}:{}:{}", run_id, schedule_idx, attempt, payload_digest);
    let digest = sha256_bytes(raw.as_bytes());
    format!("slot_{}", &digest[..24])
}

fn commit_record_by_schedule(records: &[SlotCommitRecord]) -> BTreeMap<usize, SlotCommitRecord> {
    let mut by_schedule = BTreeMap::new();
    for record in records {
        if record.record_type == "commit" {
            by_schedule.insert(record.schedule_idx, record.clone());
        }
    }
    by_schedule
}
fn normalize_execution_options(execution: &RunExecutionOptions) -> RunExecutionOptions {
    RunExecutionOptions {
        #[cfg(test)]
        executor: execution.executor,
        materialize: Some(execution.materialize.unwrap_or(MaterializationMode::Full)),
        runtime_env: execution.runtime_env.clone(),
        runtime_env_files: execution.runtime_env_files.clone(),
    }
}

fn execution_options_for_session_state(execution: &RunExecutionOptions) -> RunExecutionOptions {
    RunExecutionOptions {
        #[cfg(test)]
        executor: execution.executor,
        materialize: Some(execution.materialize.unwrap_or(MaterializationMode::Full)),
        runtime_env: BTreeMap::new(),
        runtime_env_files: Vec::new(),
    }
}
fn run_control_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("run_control.json")
}

fn load_parallel_worker_control_state(
    run_dir: &Path,
) -> Result<Option<ParallelWorkerControlState>> {
    let store = BackingSqliteStore::open(run_dir)?;
    let Some(value) = store.get_runtime_json(RUNTIME_KEY_PARALLEL_WORKER_CONTROL)? else {
        return Ok(None);
    };
    let state: ParallelWorkerControlState = serde_json::from_value(value)?;
    Ok(Some(state))
}

fn write_parallel_worker_control_state(
    run_dir: &Path,
    state: &ParallelWorkerControlState,
) -> Result<()> {
    let payload = serde_json::to_value(state)?;
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_PARALLEL_WORKER_CONTROL, &payload)
}

fn write_parallel_worker_control_request(
    run_dir: &Path,
    request: ParallelWorkerControlRequest,
) -> Result<()> {
    let state = ParallelWorkerControlState {
        schema_version: PARALLEL_WORKER_CONTROL_SCHEMA_V1.to_string(),
        request: Some(request),
        response: None,
        updated_at: Utc::now().to_rfc3339(),
    };
    write_parallel_worker_control_state(run_dir, &state)
}

fn write_parallel_worker_control_response(
    run_dir: &Path,
    response: ParallelWorkerControlResponse,
) -> Result<()> {
    let mut state =
        load_parallel_worker_control_state(run_dir)?.unwrap_or(ParallelWorkerControlState {
            schema_version: PARALLEL_WORKER_CONTROL_SCHEMA_V1.to_string(),
            request: None,
            response: None,
            updated_at: Utc::now().to_rfc3339(),
        });
    state.response = Some(response);
    state.updated_at = Utc::now().to_rfc3339();
    write_parallel_worker_control_state(run_dir, &state)
}

fn load_pending_parallel_worker_control_request(
    run_dir: &Path,
) -> Result<Option<ParallelWorkerControlRequest>> {
    let Some(state) = load_parallel_worker_control_state(run_dir)? else {
        return Ok(None);
    };
    let Some(request) = state.request else {
        return Ok(None);
    };
    if let Some(response) = state.response {
        if response.request_id == request.request_id {
            return Ok(None);
        }
    }
    Ok(Some(request))
}

fn wait_for_parallel_worker_control_response(
    run_dir: &Path,
    request_id: &str,
    timeout: Duration,
) -> Result<ParallelWorkerControlResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(state) = load_parallel_worker_control_state(run_dir)? {
            if let Some(response) = state.response {
                if response.request_id == request_id {
                    return Ok(response);
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "parallel worker control timeout: no response for request {} within {:?}",
                request_id,
                timeout
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn write_run_session_state(
    run_dir: &Path,
    run_id: &str,
    behavior: &RunBehavior,
    execution: &RunExecutionOptions,
) -> Result<()> {
    let state = RunSessionState {
        schema_version: "run_session_state_v1".to_string(),
        run_id: run_id.to_string(),
        behavior: behavior.clone(),
        execution: execution_options_for_session_state(execution),
    };
    let payload = serde_json::to_value(state)?;
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_RUN_SESSION_STATE, &payload)
}

fn load_run_session_state(run_dir: &Path) -> Result<RunSessionState> {
    let store = BackingSqliteStore::open(run_dir)?;
    if let Some(value) = store.get_runtime_json(RUNTIME_KEY_RUN_SESSION_STATE)? {
        return Ok(serde_json::from_value(value)?);
    }
    Err(anyhow!(
        "run_session_state_v1 not found in sqlite runtime_kv — this run predates continue behavior persistence"
    ))
}

fn active_adapter_payload_value(active_control: Option<&ActiveAdapterControl>) -> Value {
    match active_control {
        Some(control) => json!({
            "id": control.adapter_id,
            "version": control.adapter_version,
            "command_path": control.command_path,
            "events_path": control.events_path,
        }),
        None => Value::Null,
    }
}

fn run_control_active_trials_payload(
    active_trials: &[RunControlActiveTrial],
    updated_at: &str,
) -> serde_json::Map<String, Value> {
    let mut payload = serde_json::Map::new();
    for active in active_trials {
        payload.insert(
            active.trial_id.clone(),
            json!({
                "trial_id": active.trial_id,
                "worker_id": active.worker_id,
                "schedule_idx": active.schedule_idx,
                "variant_id": active.variant_id,
                "started_at": active.started_at.as_deref().unwrap_or(updated_at),
                "control": active_adapter_payload_value(active.control.as_ref()),
            }),
        );
    }
    payload
}

fn run_control_active_trial_ids(run_control: &Value) -> Vec<String> {
    run_control
        .pointer("/active_trials")
        .and_then(|v| v.as_object())
        .map(|active_trials| active_trials.keys().cloned().collect())
        .unwrap_or_default()
}

fn run_control_active_adapter_for_trial(run_control: &Value, trial_id: &str) -> Option<Value> {
    let active_trials = run_control
        .pointer("/active_trials")
        .and_then(|v| v.as_object())?;
    let entry = active_trials.get(trial_id)?;
    let control = entry.pointer("/control")?;
    if control.is_null() {
        None
    } else {
        Some(control.clone())
    }
}

fn run_control_active_trials(run_control: &Value) -> Vec<RunControlActiveTrial> {
    let mut active = Vec::new();
    if let Some(entries) = run_control
        .pointer("/active_trials")
        .and_then(|v| v.as_object())
    {
        for (trial_id, entry) in entries {
            let worker_id = entry
                .pointer("/worker_id")
                .and_then(|v| v.as_str())
                .unwrap_or(RUN_CONTROL_UNKNOWN_WORKER_ID)
                .to_string();
            let schedule_idx = entry
                .pointer("/schedule_idx")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);
            let variant_id = entry
                .pointer("/variant_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let started_at = entry
                .pointer("/started_at")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let control = entry
                .pointer("/control")
                .cloned()
                .and_then(|value| if value.is_null() { None } else { Some(value) })
                .and_then(|value| serde_json::from_value::<ActiveAdapterControl>(value).ok());
            active.push(RunControlActiveTrial {
                trial_id: trial_id.to_string(),
                worker_id,
                schedule_idx,
                variant_id,
                started_at,
                control,
            });
        }
    }
    active
}

fn write_run_control_v2(
    run_dir: &Path,
    run_id: &str,
    status: &str,
    active_trials: &[RunControlActiveTrial],
    pause: Option<&RunControlPauseMetadata>,
) -> Result<()> {
    let updated_at = Utc::now().to_rfc3339();
    let active_trials_payload = run_control_active_trials_payload(active_trials, &updated_at);
    let pause_value = pause.map_or(Value::Null, |metadata| {
        json!({
            "label": metadata.label,
            "requested_at": metadata.requested_at,
            "requested_by": metadata.requested_by,
        })
    });
    let payload = json!({
        "schema_version": "run_control_v2",
        "run_id": run_id,
        "status": status,
        "active_trials": active_trials_payload,
        "pause": pause_value,
        "updated_at": updated_at,
    });
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_RUN_CONTROL, &payload)
}

fn write_trial_state(
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

struct RunControlGuard {
    run_dir: PathBuf,
    run_id: String,
    done: bool,
}

impl RunControlGuard {
    fn new(run_dir: &Path, run_id: &str) -> Self {
        Self {
            run_dir: run_dir.to_path_buf(),
            run_id: run_id.to_string(),
            done: false,
        }
    }

    fn complete(&mut self, status: &str) -> Result<()> {
        write_run_control_v2(&self.run_dir, &self.run_id, status, &[], None)?;
        self.done = true;
        Ok(())
    }

    fn disarm(&mut self) {
        self.done = true;
    }
}

impl Drop for RunControlGuard {
    fn drop(&mut self) {
        if !self.done {
            let status = if INTERRUPTED.load(Ordering::SeqCst) {
                "interrupted"
            } else {
                "failed"
            };
            let _ = write_run_control_v2(&self.run_dir, &self.run_id, status, &[], None);
        }
    }
}

struct TrialStateGuard {
    trial_dir: PathBuf,
    trial_id: String,
    done: bool,
}

impl TrialStateGuard {
    fn new(trial_dir: &Path, trial_id: &str) -> Self {
        Self {
            trial_dir: trial_dir.to_path_buf(),
            trial_id: trial_id.to_string(),
            done: false,
        }
    }

    fn complete(&mut self, status: &str, exit_reason: Option<&str>) -> Result<()> {
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
fn create_unique_run_dir(project_root: &Path) -> Result<(String, PathBuf)> {
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
fn find_project_root_from_run_dir(run_dir: &Path) -> Result<PathBuf> {
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
