use anyhow::{anyhow, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::Utc;
use lab_core::{
    canonical_json_digest, ensure_dir, runner_runtime_host_paths, sha256_bytes, sha256_file,
    ArtifactStore, RunnerRuntimeHostPaths, AGENTLAB_AGENTLABD_START_REQUEST_PATH,
    AGENTLAB_AGENTLABD_START_RESPONSE_PATH, AGENTLAB_BINDINGS_PATH, AGENTLAB_CONTRACT_DEPS_DIR,
    AGENTLAB_CONTRACT_IN_DIR, AGENTLAB_CONTRACT_OUT_DIR, AGENTLAB_CONTRACT_STATE_DIR,
    AGENTLAB_CONTRACT_WORKSPACE_DIR, AGENTLAB_CONTROL_PATH, AGENTLAB_DEPENDENCIES_PATH,
    AGENTLAB_ENV_BINDINGS_PATH, AGENTLAB_ENV_DEPENDENCIES_PATH, AGENTLAB_ENV_POLICY_PATH,
    AGENTLAB_ENV_REPL_IDX, AGENTLAB_ENV_RESULT_PATH, AGENTLAB_ENV_RUN_ID, AGENTLAB_ENV_TASK_ID,
    AGENTLAB_ENV_TASK_PATH, AGENTLAB_ENV_TIMEOUT_MS, AGENTLAB_ENV_TRAJECTORY_PATH,
    AGENTLAB_ENV_TRIAL_ID, AGENTLAB_ENV_VARIANT_ID, AGENTLAB_POLICY_PATH, AGENTLAB_RESULT_PATH,
    AGENTLAB_TASK_PATH, AGENTLAB_TRAJECTORY_PATH, AGENTLAB_TRIAL_INPUT_PATH, HARNESS_IN_DIR,
    HARNESS_OUT_DIR, HARNESS_RESULT_PATH, HARNESS_TASK_PATH,
};
use lab_hooks::{load_manifest, validate_hooks};
use lab_provenance::{default_attestation, write_attestation};
use lab_schemas::compile_schema;
use reqwest::blocking::Client as HttpClient;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

mod sink;
use sink::{
    EventRow, JsonlRunSink, MetricRow, RunManifestRecord, RunSink, TrialRecord, VariantSnapshotRow,
};

const DEFAULT_CONTAINER_TASK_PATH: &str = AGENTLAB_TASK_PATH;
const DEFAULT_CONTAINER_BINDINGS_PATH: &str = AGENTLAB_BINDINGS_PATH;
const DEFAULT_CONTAINER_DEPENDENCIES_PATH: &str = AGENTLAB_DEPENDENCIES_PATH;
const DEFAULT_CONTAINER_POLICY_PATH: &str = AGENTLAB_POLICY_PATH;
const DEFAULT_CONTAINER_RESULT_PATH: &str = AGENTLAB_RESULT_PATH;
const DEFAULT_CONTAINER_TRAJECTORY_PATH: &str = AGENTLAB_TRAJECTORY_PATH;
const DEFAULT_CONTAINER_TRIAL_INPUT_PATH: &str = AGENTLAB_TRIAL_INPUT_PATH;
const DEFAULT_CONTAINER_CONTROL_PATH: &str = AGENTLAB_CONTROL_PATH;
const DEFAULT_CLEAN_TASK_PATH: &str = HARNESS_TASK_PATH;
const DEFAULT_CLEAN_RESULT_PATH: &str = HARNESS_RESULT_PATH;
const AGENTLAB_ENV_TASK_IMAGE: &str = "AGENTLAB_TASK_IMAGE";
const AGENTLAB_ENV_BENCHMARK_PREDICTION_PATH: &str = "AGENTLAB_BENCHMARK_PREDICTION_PATH";
const AGENTLAB_ENV_BENCHMARK_SCORE_PATH: &str = "AGENTLAB_BENCHMARK_SCORE_PATH";
const AGENTLAB_ENV_AGENT_EXIT_STATUS: &str = "AGENTLAB_AGENT_EXIT_STATUS";
const BENCHMARK_PREDICTION_FILENAME: &str = "benchmark_prediction.json";
const BENCHMARK_SCORE_FILENAME: &str = "benchmark_score.json";
const BENCHMARK_GRADE_ERROR_FILENAME: &str = "benchmark_grade_error.txt";
const BENCHMARK_GRADING_POLICY_EXIT_CODE: i32 = 125;
const AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT_ENV: &str = "AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT";
const AGENTLAB_REMOTE_PROTOCOL_RETRY_MAX_ATTEMPTS_ENV: &str =
    "AGENTLAB_REMOTE_PROTOCOL_RETRY_MAX_ATTEMPTS";
const AGENTLAB_REMOTE_PROTOCOL_RETRY_BASE_BACKOFF_MS_ENV: &str =
    "AGENTLAB_REMOTE_PROTOCOL_RETRY_BASE_BACKOFF_MS";
const AGENTLAB_REMOTE_PROTOCOL_CONNECT_TIMEOUT_MS_ENV: &str =
    "AGENTLAB_REMOTE_PROTOCOL_CONNECT_TIMEOUT_MS";
const AGENTLAB_REMOTE_PROTOCOL_SUBMIT_TIMEOUT_MS_ENV: &str =
    "AGENTLAB_REMOTE_PROTOCOL_SUBMIT_TIMEOUT_MS";
const AGENTLAB_REMOTE_PROTOCOL_POLL_TIMEOUT_GRACE_MS_ENV: &str =
    "AGENTLAB_REMOTE_PROTOCOL_POLL_TIMEOUT_GRACE_MS";
const AGENTLAB_REMOTE_PROTOCOL_PAUSE_TIMEOUT_MS_ENV: &str =
    "AGENTLAB_REMOTE_PROTOCOL_PAUSE_TIMEOUT_MS";
const AGENTLAB_REMOTE_PROTOCOL_STOP_TIMEOUT_MS_ENV: &str =
    "AGENTLAB_REMOTE_PROTOCOL_STOP_TIMEOUT_MS";
const LOCAL_WORKER_CAPACITY_ERROR_PREFIX: &str = "local worker backend at capacity:";
const LOCAL_WORKER_MAX_COMPLETIONS_PER_POLL: usize = 256;
const REMOTE_PROTOCOL_RETRY_MAX_ATTEMPTS_DEFAULT: usize = 3;
const REMOTE_PROTOCOL_RETRY_BASE_BACKOFF_MS_DEFAULT: u64 = 20;
const REMOTE_PROTOCOL_CONNECT_TIMEOUT_MS_DEFAULT: u64 = 5_000;
const REMOTE_PROTOCOL_SUBMIT_TIMEOUT_MS_DEFAULT: u64 = 30_000;
const REMOTE_PROTOCOL_POLL_TIMEOUT_GRACE_MS_DEFAULT: u64 = 1_000;
const REMOTE_PROTOCOL_PAUSE_TIMEOUT_MS_DEFAULT: u64 = 30_000;
const REMOTE_PROTOCOL_STOP_TIMEOUT_MS_DEFAULT: u64 = 30_000;
const REMOTE_BACKEND_QUARANTINED_PREFIX: &str = "remote worker backend quarantined:";
const REMOTE_COMPLETION_SEQ_FALLBACK: u64 = 0;
const CANONICAL_TRIAL_RESULT_FILENAME: &str = "result.json";
const PARALLEL_WORKER_CONTROL_SCHEMA_V1: &str = "parallel_worker_control_v1";
const PARALLEL_WORKER_CONTROL_RESPONSE_COMPLETED: &str = "completed";
const PARALLEL_WORKER_CONTROL_RESPONSE_FAILED: &str = "failed";
const KILL_RUN_WORKER_CONTROL_TIMEOUT_SECONDS: u64 = 30;
const WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES: &[&str] = &[
    "logs",
    ".haiku",
    ".graphd",
    ".watcher",
    ".agentlab_generated",
    ".claude",
    ".cockpit",
    "auth-states",
];

const BUILTIN_COMMAND_ADAPTER_ID: &str = "builtin.command_contract";
const BUILTIN_COMMAND_ADAPTER_VERSION: &str = "v1";
const PREBUILT_CODEX_ADAPTER_ID: &str = "prebuilt.codex_cli";
const PREBUILT_REX_JESUS_ADAPTER_ID: &str = "prebuilt.rex_jesus";
const PREBUILT_AGENT_ADAPTER_VERSION: &str = "v1";

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentAdapterRef {
    id: String,
    version: String,
}

impl Default for AgentAdapterRef {
    fn default() -> Self {
        Self {
            id: BUILTIN_COMMAND_ADAPTER_ID.to_string(),
            version: BUILTIN_COMMAND_ADAPTER_VERSION.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AgentAdapterCapabilities {
    pause: bool,
    control_ack: bool,
    event_stream: bool,
    strict_replay: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveAdapterControl {
    #[serde(
        rename = "id",
        alias = "adapter_id",
        default = "default_active_adapter_id"
    )]
    adapter_id: String,
    #[serde(
        rename = "version",
        alias = "adapter_version",
        default = "default_active_adapter_version"
    )]
    adapter_version: String,
    command_path: String,
    #[serde(default)]
    events_path: Option<String>,
}

fn default_active_adapter_id() -> String {
    BUILTIN_COMMAND_ADAPTER_ID.to_string()
}

fn default_active_adapter_version() -> String {
    BUILTIN_COMMAND_ADAPTER_VERSION.to_string()
}

#[derive(Clone)]
struct AdapterRunRequest<'a> {
    runtime_experiment: &'a Value,
    runtime: &'a AgentRuntimeConfig,
    variant_args: &'a [String],
    runtime_env: &'a BTreeMap<String, String>,
    runtime_overrides_env: &'a BTreeMap<String, String>,
    container_mode: bool,
    trial_paths: &'a TrialPaths,
    dynamic_mounts: &'a [ResolvedMountReference],
    io_paths: &'a PreparedTrialIo,
    network_mode: &'a str,
    setup_command: Option<&'a str>,
    benchmark_adapter: Option<&'a BenchmarkAdapterConfig>,
    benchmark_grading_enabled: bool,
    run_id: &'a str,
    task_image: Option<&'a str>,
    task_workspace: Option<&'a str>,
    agent_artifact: Option<&'a Path>,
}

#[derive(Clone)]
struct AdapterPauseRequest<'a> {
    control: &'a ActiveAdapterControl,
    label: &'a str,
    timeout: Duration,
}

#[derive(Debug, Clone)]
struct AdapterPauseAck {
    checkpoint_acked: bool,
    stop_acked: bool,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct WorkerTicket {
    worker_id: String,
    ticket_id: String,
    trial_id: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TrialCompletion {
    ticket: WorkerTicket,
    schedule_idx: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completion_seq: Option<u64>,
    terminal_status: String,
    classification: String,
    artifacts: Value,
    metrics: Value,
    runtime_summary: Value,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct WorkerPauseAck {
    worker_id: String,
    trial_id: String,
    label: String,
    accepted: bool,
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

fn parse_optional_positive_usize_env(name: &str) -> Result<Option<usize>> {
    match env::var(name) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed = trimmed.parse::<usize>().map_err(|_| {
                anyhow!(
                    "{} must be a positive integer when set (got: {})",
                    name,
                    raw
                )
            })?;
            if parsed == 0 {
                return Err(anyhow!("{} must be > 0 when set", name));
            }
            Ok(Some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(anyhow!("failed reading {}: {}", name, err)),
    }
}

fn parse_optional_positive_u64_env(name: &str) -> Result<Option<u64>> {
    match env::var(name) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed = trimmed.parse::<u64>().map_err(|_| {
                anyhow!(
                    "{} must be a positive integer when set (got: {})",
                    name,
                    raw
                )
            })?;
            if parsed == 0 {
                return Err(anyhow!("{} must be > 0 when set", name));
            }
            Ok(Some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(anyhow!("failed reading {}: {}", name, err)),
    }
}

#[derive(Debug, Clone, Copy)]
struct RemoteRetrySettings {
    max_attempts: usize,
    base_backoff_ms: u64,
}

impl Default for RemoteRetrySettings {
    fn default() -> Self {
        Self {
            max_attempts: REMOTE_PROTOCOL_RETRY_MAX_ATTEMPTS_DEFAULT,
            base_backoff_ms: REMOTE_PROTOCOL_RETRY_BASE_BACKOFF_MS_DEFAULT,
        }
    }
}

fn resolve_remote_retry_settings_from_env() -> Result<RemoteRetrySettings> {
    let mut settings = RemoteRetrySettings::default();
    if let Some(max_attempts) =
        parse_optional_positive_usize_env(AGENTLAB_REMOTE_PROTOCOL_RETRY_MAX_ATTEMPTS_ENV)?
    {
        settings.max_attempts = max_attempts;
    }
    if let Some(base_backoff_ms) =
        parse_optional_positive_u64_env(AGENTLAB_REMOTE_PROTOCOL_RETRY_BASE_BACKOFF_MS_ENV)?
    {
        settings.base_backoff_ms = base_backoff_ms;
    }
    Ok(settings)
}

#[derive(Debug, Clone, Copy)]
struct RemoteProtocolTimeoutSettings {
    connect_timeout_ms: u64,
    submit_timeout_ms: u64,
    poll_timeout_grace_ms: u64,
    pause_timeout_ms: u64,
    stop_timeout_ms: u64,
}

impl Default for RemoteProtocolTimeoutSettings {
    fn default() -> Self {
        Self {
            connect_timeout_ms: REMOTE_PROTOCOL_CONNECT_TIMEOUT_MS_DEFAULT,
            submit_timeout_ms: REMOTE_PROTOCOL_SUBMIT_TIMEOUT_MS_DEFAULT,
            poll_timeout_grace_ms: REMOTE_PROTOCOL_POLL_TIMEOUT_GRACE_MS_DEFAULT,
            pause_timeout_ms: REMOTE_PROTOCOL_PAUSE_TIMEOUT_MS_DEFAULT,
            stop_timeout_ms: REMOTE_PROTOCOL_STOP_TIMEOUT_MS_DEFAULT,
        }
    }
}

fn resolve_remote_protocol_timeout_settings_from_env() -> Result<RemoteProtocolTimeoutSettings> {
    let mut settings = RemoteProtocolTimeoutSettings::default();
    if let Some(connect_timeout_ms) =
        parse_optional_positive_u64_env(AGENTLAB_REMOTE_PROTOCOL_CONNECT_TIMEOUT_MS_ENV)?
    {
        settings.connect_timeout_ms = connect_timeout_ms;
    }
    if let Some(submit_timeout_ms) =
        parse_optional_positive_u64_env(AGENTLAB_REMOTE_PROTOCOL_SUBMIT_TIMEOUT_MS_ENV)?
    {
        settings.submit_timeout_ms = submit_timeout_ms;
    }
    if let Some(poll_timeout_grace_ms) =
        parse_optional_positive_u64_env(AGENTLAB_REMOTE_PROTOCOL_POLL_TIMEOUT_GRACE_MS_ENV)?
    {
        settings.poll_timeout_grace_ms = poll_timeout_grace_ms;
    }
    if let Some(pause_timeout_ms) =
        parse_optional_positive_u64_env(AGENTLAB_REMOTE_PROTOCOL_PAUSE_TIMEOUT_MS_ENV)?
    {
        settings.pause_timeout_ms = pause_timeout_ms;
    }
    if let Some(stop_timeout_ms) =
        parse_optional_positive_u64_env(AGENTLAB_REMOTE_PROTOCOL_STOP_TIMEOUT_MS_ENV)?
    {
        settings.stop_timeout_ms = stop_timeout_ms;
    }
    Ok(settings)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteProtocolErrorKind {
    Retryable,
    Fatal,
}

#[derive(Debug)]
struct RemoteProtocolError {
    kind: RemoteProtocolErrorKind,
    message: String,
}

impl RemoteProtocolError {
    fn retryable(message: impl Into<String>) -> Self {
        Self {
            kind: RemoteProtocolErrorKind::Retryable,
            message: message.into(),
        }
    }

    fn fatal(message: impl Into<String>) -> Self {
        Self {
            kind: RemoteProtocolErrorKind::Fatal,
            message: message.into(),
        }
    }

    fn is_retryable(&self) -> bool {
        self.kind == RemoteProtocolErrorKind::Retryable
    }
}

impl std::fmt::Display for RemoteProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RemoteProtocolError {}

fn is_retryable_remote_http_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

fn truncate_remote_error_body(raw: &str) -> String {
    const MAX_ERROR_BODY_CHARS: usize = 512;
    let normalized = raw.replace('\n', " ");
    if normalized.chars().count() <= MAX_ERROR_BODY_CHARS {
        return normalized;
    }
    normalized.chars().take(MAX_ERROR_BODY_CHARS).collect()
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
        thread::Builder::new()
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
            })
            .map_err(|e| anyhow!("failed to spawn local worker thread: {}", e))?;

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
        Ok(WorkerPauseAck {
            worker_id: worker_id.to_string(),
            trial_id: ticket.trial_id.clone(),
            label: label.to_string(),
            accepted: true,
        })
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
        Ok(())
    }
}

const REMOTE_SUBMIT_SCHEMA_V1: &str = "remote_worker_submit_v1";
const REMOTE_POLL_SCHEMA_V1: &str = "remote_worker_poll_v1";
const REMOTE_PAUSE_SCHEMA_V1: &str = "remote_worker_pause_v1";
const REMOTE_STOP_SCHEMA_V1: &str = "remote_worker_stop_v1";
const REMOTE_SUBMIT_PATH_V1: &str = "v1/worker/submit";
const REMOTE_POLL_PATH_V1: &str = "v1/worker/poll";
const REMOTE_PAUSE_PATH_V1: &str = "v1/worker/pause";
const REMOTE_STOP_PATH_V1: &str = "v1/worker/stop";

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RemoteSubmitRequest {
    schema_version: String,
    dispatch: TrialDispatch,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RemoteSubmitResponse {
    schema_version: String,
    ticket: WorkerTicket,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RemotePollRequest {
    schema_version: String,
    timeout_ms: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RemotePollResponse {
    schema_version: String,
    completions: Vec<TrialCompletion>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RemotePauseRequest {
    schema_version: String,
    worker_id: String,
    label: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RemotePauseResponse {
    schema_version: String,
    ack: WorkerPauseAck,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RemoteStopRequest {
    schema_version: String,
    worker_id: String,
    reason: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RemoteStopResponse {
    schema_version: String,
    accepted: bool,
}

#[allow(dead_code)]
trait RemoteWorkerProtocol: Send + Sync {
    fn submit(&self, request: RemoteSubmitRequest) -> Result<RemoteSubmitResponse>;
    fn poll(&self, request: RemotePollRequest) -> Result<RemotePollResponse>;
    fn pause(&self, request: RemotePauseRequest) -> Result<RemotePauseResponse>;
    fn stop(&self, request: RemoteStopRequest) -> Result<RemoteStopResponse>;
}

#[derive(Clone)]
struct HttpRemoteWorkerProtocol {
    endpoint: String,
    bearer_token: Option<String>,
    client: HttpClient,
    timeouts: RemoteProtocolTimeoutSettings,
}

impl HttpRemoteWorkerProtocol {
    fn new(endpoint: &str, bearer_token: Option<String>) -> Result<Self> {
        let endpoint = endpoint.trim().to_string();
        if endpoint.is_empty() {
            return Err(anyhow!("remote worker endpoint must not be empty"));
        }
        let timeouts = resolve_remote_protocol_timeout_settings_from_env()?;
        let client = HttpClient::builder()
            .connect_timeout(Duration::from_millis(timeouts.connect_timeout_ms))
            .build()?;
        Ok(Self {
            endpoint,
            bearer_token,
            client,
            timeouts,
        })
    }

    fn url_for_path(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.endpoint.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    fn post_json<Req, Resp>(
        &self,
        path: &str,
        request: &Req,
        timeout: Option<Duration>,
    ) -> Result<Resp>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let url = self.url_for_path(path);
        let mut builder = self.client.post(&url);
        if let Some(token) = self.bearer_token.as_ref() {
            builder = builder.bearer_auth(token);
        }
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let response = builder.json(request).send().map_err(|err| {
            let detail = format!("remote worker http POST {} transport error: {}", url, err);
            let classified = if err.is_timeout() || err.is_connect() || err.is_request() {
                RemoteProtocolError::retryable(detail)
            } else {
                RemoteProtocolError::fatal(detail)
            };
            anyhow!(classified)
        })?;
        let status = response.status();
        if !status.is_success() {
            let code = status.as_u16();
            let body = response
                .text()
                .map(|value| truncate_remote_error_body(&value))
                .unwrap_or_else(|_| "<response body unavailable>".to_string());
            let detail = format!(
                "remote worker http POST {} failed: status={} body={}",
                url, code, body
            );
            let classified = if is_retryable_remote_http_status(code) {
                RemoteProtocolError::retryable(detail)
            } else {
                RemoteProtocolError::fatal(detail)
            };
            return Err(anyhow!(classified));
        }
        response.json::<Resp>().map_err(|err| {
            let detail = format!(
                "remote worker http POST {} returned invalid JSON payload: {}",
                url, err
            );
            anyhow!(RemoteProtocolError::fatal(detail))
        })
    }
}

impl RemoteWorkerProtocol for HttpRemoteWorkerProtocol {
    fn submit(&self, request: RemoteSubmitRequest) -> Result<RemoteSubmitResponse> {
        self.post_json(
            REMOTE_SUBMIT_PATH_V1,
            &request,
            Some(Duration::from_millis(self.timeouts.submit_timeout_ms)),
        )
    }

    fn poll(&self, request: RemotePollRequest) -> Result<RemotePollResponse> {
        let timeout = Duration::from_millis(
            request
                .timeout_ms
                .saturating_add(self.timeouts.poll_timeout_grace_ms),
        );
        self.post_json(REMOTE_POLL_PATH_V1, &request, Some(timeout))
    }

    fn pause(&self, request: RemotePauseRequest) -> Result<RemotePauseResponse> {
        self.post_json(
            REMOTE_PAUSE_PATH_V1,
            &request,
            Some(Duration::from_millis(self.timeouts.pause_timeout_ms)),
        )
    }

    fn stop(&self, request: RemoteStopRequest) -> Result<RemoteStopResponse> {
        self.post_json(
            REMOTE_STOP_PATH_V1,
            &request,
            Some(Duration::from_millis(self.timeouts.stop_timeout_ms)),
        )
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct RemoteWorkerBackend {
    protocol: Arc<dyn RemoteWorkerProtocol>,
    state: Arc<Mutex<RemoteWorkerBackendState>>,
    retry_settings: RemoteRetrySettings,
}

#[derive(Default)]
struct RemoteWorkerBackendState {
    submitted_tickets: HashMap<String, RemoteSubmissionRecord>,
    active_tickets_by_worker: HashMap<String, HashSet<String>>,
    completion_key_by_ticket: HashMap<String, RemoteCompletionDedupKey>,
    quarantined_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct RemoteSubmissionRecord {
    run_id: String,
    trial_id: String,
    schedule_idx: usize,
    worker_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RemoteCompletionDedupKey {
    run_id: String,
    schedule_idx: usize,
    trial_id: String,
    worker_id: String,
    completion_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteCompletionValidation {
    Deliver,
    Duplicate,
}

#[allow(dead_code)]
impl RemoteWorkerBackend {
    fn new(protocol: Arc<dyn RemoteWorkerProtocol>) -> Result<Self> {
        Ok(Self {
            protocol,
            state: Arc::new(Mutex::new(RemoteWorkerBackendState::default())),
            retry_settings: resolve_remote_retry_settings_from_env()?,
        })
    }

    fn ensure_available(&self) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("remote worker backend state lock poisoned"))?;
        if let Some(reason) = state.quarantined_reason.as_deref() {
            return Err(anyhow!("{} {}", REMOTE_BACKEND_QUARANTINED_PREFIX, reason));
        }
        Ok(())
    }

    fn quarantined_error(reason: &str) -> anyhow::Error {
        anyhow!("{} {}", REMOTE_BACKEND_QUARANTINED_PREFIX, reason)
    }

    fn protocol_fault(&self, detail: impl AsRef<str>) -> anyhow::Error {
        let reason = format!("remote worker backend protocol fault: {}", detail.as_ref());
        match self.state.lock() {
            Ok(mut state) => {
                if state.quarantined_reason.is_none() {
                    state.quarantined_reason = Some(reason);
                }
                let quarantined_reason = state
                    .quarantined_reason
                    .as_deref()
                    .unwrap_or("protocol fault");
                Self::quarantined_error(quarantined_reason)
            }
            Err(_) => Self::quarantined_error("state lock poisoned while setting quarantine"),
        }
    }

    fn remove_active_submission_for_ticket(
        state: &mut RemoteWorkerBackendState,
        ticket_id: &str,
    ) -> Option<RemoteSubmissionRecord> {
        let submission = state.submitted_tickets.remove(ticket_id)?;
        if let Some(ticket_ids) = state
            .active_tickets_by_worker
            .get_mut(submission.worker_id.as_str())
        {
            ticket_ids.remove(ticket_id);
            if ticket_ids.is_empty() {
                state
                    .active_tickets_by_worker
                    .remove(submission.worker_id.as_str());
            }
        }
        Some(submission)
    }

    fn active_submissions_for_worker(
        &self,
        worker_id: &str,
        op_name: &str,
    ) -> Result<Vec<RemoteSubmissionRecord>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("remote worker backend state lock poisoned"))?;
        if let Some(reason) = state.quarantined_reason.as_deref() {
            return Err(Self::quarantined_error(reason));
        }

        let Some(ticket_ids) = state.active_tickets_by_worker.get(worker_id).cloned() else {
            return Err(anyhow!(
                "remote worker backend {} failed: unknown active worker {}",
                op_name,
                worker_id
            ));
        };

        let mut active_submissions = Vec::new();
        let mut stale_ticket_ids = Vec::new();
        for ticket_id in ticket_ids {
            if let Some(submission) = state.submitted_tickets.get(ticket_id.as_str()).cloned() {
                active_submissions.push(submission);
            } else {
                stale_ticket_ids.push(ticket_id);
            }
        }

        if !stale_ticket_ids.is_empty() {
            if let Some(active_tickets) = state.active_tickets_by_worker.get_mut(worker_id) {
                for stale_ticket_id in stale_ticket_ids {
                    active_tickets.remove(stale_ticket_id.as_str());
                }
                if active_tickets.is_empty() {
                    state.active_tickets_by_worker.remove(worker_id);
                }
            }
        }

        if active_submissions.is_empty() {
            return Err(anyhow!(
                "remote worker backend {} failed: unknown active worker {}",
                op_name,
                worker_id
            ));
        }

        Ok(active_submissions)
    }

    fn remember_submission(&self, dispatch: &TrialDispatch, ticket: &WorkerTicket) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("remote worker backend state lock poisoned"))?;
        if let Some(reason) = state.quarantined_reason.as_deref() {
            return Err(Self::quarantined_error(reason));
        }
        if ticket.ticket_id.trim().is_empty() {
            drop(state);
            return Err(self.protocol_fault("submit returned empty ticket_id"));
        }
        if ticket.worker_id.trim().is_empty() {
            drop(state);
            return Err(self.protocol_fault(format!(
                "submit returned empty worker_id for ticket {}",
                ticket.ticket_id
            )));
        }
        if state
            .submitted_tickets
            .contains_key(ticket.ticket_id.as_str())
            || state
                .completion_key_by_ticket
                .contains_key(ticket.ticket_id.as_str())
        {
            drop(state);
            return Err(self.protocol_fault(format!("duplicate ticket_id {}", ticket.ticket_id)));
        }
        state.submitted_tickets.insert(
            ticket.ticket_id.clone(),
            RemoteSubmissionRecord {
                run_id: dispatch.run_id.clone(),
                trial_id: dispatch.trial_id.clone(),
                schedule_idx: dispatch.schedule_idx,
                worker_id: ticket.worker_id.clone(),
            },
        );
        state
            .active_tickets_by_worker
            .entry(ticket.worker_id.clone())
            .or_default()
            .insert(ticket.ticket_id.clone());
        Ok(())
    }

    fn completion_seq_for_dedupe(completion: &TrialCompletion) -> u64 {
        completion
            .completion_seq
            .unwrap_or(REMOTE_COMPLETION_SEQ_FALLBACK)
    }

    fn completion_key(
        submission: &RemoteSubmissionRecord,
        completion_seq: u64,
    ) -> RemoteCompletionDedupKey {
        RemoteCompletionDedupKey {
            run_id: submission.run_id.clone(),
            schedule_idx: submission.schedule_idx,
            trial_id: submission.trial_id.clone(),
            worker_id: submission.worker_id.clone(),
            completion_seq,
        }
    }

    fn validate_and_consume_completion(
        &self,
        completion: &TrialCompletion,
    ) -> Result<RemoteCompletionValidation> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("remote worker backend state lock poisoned"))?;
        if let Some(reason) = state.quarantined_reason.as_deref() {
            return Err(Self::quarantined_error(reason));
        }

        let completion_seq = Self::completion_seq_for_dedupe(completion);
        let ticket_id = completion.ticket.ticket_id.as_str();

        if let Some(submission) = state.submitted_tickets.get(ticket_id).cloned() {
            if completion.ticket.worker_id != submission.worker_id {
                drop(state);
                return Err(self.protocol_fault(format!(
                    "completion worker_id {} did not match submitted worker_id {} for ticket {}",
                    completion.ticket.worker_id, submission.worker_id, completion.ticket.ticket_id
                )));
            }
            if completion.ticket.trial_id != submission.trial_id {
                drop(state);
                return Err(self.protocol_fault(format!(
                    "completion trial_id {} did not match submitted trial_id {} for ticket {}",
                    completion.ticket.trial_id, submission.trial_id, completion.ticket.ticket_id
                )));
            }
            if completion.schedule_idx != submission.schedule_idx {
                drop(state);
                return Err(self.protocol_fault(format!(
                    "completion schedule_idx {} did not match submitted schedule_idx {} for ticket {}",
                    completion.schedule_idx, submission.schedule_idx, completion.ticket.ticket_id
                )));
            }
            Self::remove_active_submission_for_ticket(&mut state, ticket_id);
            state.completion_key_by_ticket.insert(
                completion.ticket.ticket_id.clone(),
                Self::completion_key(&submission, completion_seq),
            );
            return Ok(RemoteCompletionValidation::Deliver);
        }

        if let Some(existing) = state.completion_key_by_ticket.get(ticket_id).cloned() {
            let duplicate = completion.ticket.trial_id == existing.trial_id
                && completion.ticket.worker_id == existing.worker_id
                && completion.schedule_idx == existing.schedule_idx
                && completion_seq == existing.completion_seq;
            if duplicate {
                return Ok(RemoteCompletionValidation::Duplicate);
            }
            drop(state);
            return Err(self.protocol_fault(format!(
                "conflicting duplicate completion for ticket {} (expected trial_id={}, worker_id={}, schedule_idx={}, completion_seq={}, got trial_id={}, worker_id={}, schedule_idx={}, completion_seq={})",
                completion.ticket.ticket_id,
                existing.trial_id,
                existing.worker_id,
                existing.schedule_idx,
                existing.completion_seq,
                completion.ticket.trial_id,
                completion.ticket.worker_id,
                completion.schedule_idx,
                completion_seq
            )));
        }

        drop(state);
        Err(self.protocol_fault(format!(
            "completion for unknown ticket {}",
            completion.ticket.ticket_id
        )))
    }

    fn is_retryable_protocol_error(err: &anyhow::Error) -> bool {
        for cause in err.chain() {
            if let Some(protocol_error) = cause.downcast_ref::<RemoteProtocolError>() {
                return protocol_error.is_retryable();
            }
        }
        let message = err.to_string().to_ascii_lowercase();
        if message.contains("timeout")
            || message.contains("timed out")
            || message.contains("connection reset")
            || message.contains("connection refused")
            || message.contains("connection aborted")
            || message.contains("connection closed")
            || message.contains("temporarily unavailable")
            || message.contains("broken pipe")
        {
            return true;
        }
        message.contains(" 429 ")
            || message.contains("status=429")
            || message.contains("status 429")
            || message.contains(" 408 ")
            || message.contains("status=408")
            || message.contains("status 408")
            || message.contains(" 500 ")
            || message.contains("status=500")
            || message.contains("status 500")
            || message.contains(" 502 ")
            || message.contains("status=502")
            || message.contains("status 502")
            || message.contains(" 503 ")
            || message.contains("status=503")
            || message.contains("status 503")
            || message.contains(" 504 ")
            || message.contains("status=504")
            || message.contains("status 504")
    }

    fn retry_backoff_delay(&self, attempt: usize) -> Duration {
        let shift = attempt.saturating_sub(1).min(8) as u32;
        let multiplier = 1u64 << shift;
        Duration::from_millis(
            self.retry_settings
                .base_backoff_ms
                .saturating_mul(multiplier),
        )
    }

    fn call_protocol_with_retry<T>(
        &self,
        op_name: &str,
        mut op: impl FnMut() -> Result<T>,
    ) -> Result<T> {
        let attempts = self.retry_settings.max_attempts.max(1);
        for attempt in 1..=attempts {
            match op() {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let retryable = Self::is_retryable_protocol_error(&err);
                    if retryable && attempt < attempts {
                        thread::sleep(self.retry_backoff_delay(attempt));
                        continue;
                    }
                    if retryable {
                        return Err(anyhow!(
                            "remote worker backend {} request failed after {} attempts: {}",
                            op_name,
                            attempt,
                            err
                        ));
                    }
                    return Err(err);
                }
            }
        }
        unreachable!("attempt loop always returns");
    }
}

impl WorkerBackend for RemoteWorkerBackend {
    fn submit(&self, dispatch: TrialDispatch) -> Result<WorkerTicket> {
        self.ensure_available()?;
        let request = RemoteSubmitRequest {
            schema_version: REMOTE_SUBMIT_SCHEMA_V1.to_string(),
            dispatch: dispatch.clone(),
        };
        let response =
            self.call_protocol_with_retry("submit", || self.protocol.submit(request.clone()))?;
        if response.schema_version != REMOTE_SUBMIT_SCHEMA_V1 {
            return Err(self.protocol_fault(format!(
                "submit schema_version {} did not match {}",
                response.schema_version, REMOTE_SUBMIT_SCHEMA_V1
            )));
        }
        if response.ticket.trial_id != dispatch.trial_id {
            return Err(self.protocol_fault(format!(
                "returned ticket trial_id {} did not match dispatch trial_id {}",
                response.ticket.trial_id, dispatch.trial_id
            )));
        }
        self.remember_submission(&dispatch, &response.ticket)?;
        Ok(response.ticket)
    }

    fn poll_completions(&self, timeout: Duration) -> Result<Vec<TrialCompletion>> {
        self.ensure_available()?;
        let timeout_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;
        let request = RemotePollRequest {
            schema_version: REMOTE_POLL_SCHEMA_V1.to_string(),
            timeout_ms,
        };
        let response =
            self.call_protocol_with_retry("poll", || self.protocol.poll(request.clone()))?;
        if response.schema_version != REMOTE_POLL_SCHEMA_V1 {
            return Err(self.protocol_fault(format!(
                "poll schema_version {} did not match {}",
                response.schema_version, REMOTE_POLL_SCHEMA_V1
            )));
        }
        let mut accepted = Vec::with_capacity(response.completions.len());
        for completion in response.completions {
            match self.validate_and_consume_completion(&completion)? {
                RemoteCompletionValidation::Deliver => accepted.push(completion),
                RemoteCompletionValidation::Duplicate => {}
            }
        }
        Ok(accepted)
    }

    fn request_pause(&self, worker_id: &str, label: &str) -> Result<WorkerPauseAck> {
        self.ensure_available()?;
        let active_submissions = self.active_submissions_for_worker(worker_id, "pause")?;
        let request = RemotePauseRequest {
            schema_version: REMOTE_PAUSE_SCHEMA_V1.to_string(),
            worker_id: worker_id.to_string(),
            label: label.to_string(),
        };
        let response =
            self.call_protocol_with_retry("pause", || self.protocol.pause(request.clone()))?;
        if response.schema_version != REMOTE_PAUSE_SCHEMA_V1 {
            return Err(self.protocol_fault(format!(
                "pause schema_version {} did not match {}",
                response.schema_version, REMOTE_PAUSE_SCHEMA_V1
            )));
        }
        if response.ack.worker_id != worker_id {
            return Err(self.protocol_fault(format!(
                "pause ack worker_id {} did not match request worker_id {}",
                response.ack.worker_id, worker_id
            )));
        }
        if response.ack.label != label {
            return Err(self.protocol_fault(format!(
                "pause ack label {} did not match request label {}",
                response.ack.label, label
            )));
        }
        if !response.ack.accepted {
            return Err(anyhow!(
                "remote worker backend pause rejected for worker {}",
                worker_id
            ));
        }
        let expected_trials: HashSet<String> = active_submissions
            .iter()
            .map(|entry| entry.trial_id.clone())
            .collect();
        if !expected_trials.contains(response.ack.trial_id.as_str()) {
            let mut expected_trials_sorted: Vec<String> = expected_trials.into_iter().collect();
            expected_trials_sorted.sort();
            return Err(self.protocol_fault(format!(
                "pause ack trial_id {} did not match active trial(s) [{}] for worker {}",
                response.ack.trial_id,
                expected_trials_sorted.join(","),
                worker_id
            )));
        }
        Ok(response.ack)
    }

    fn request_stop(&self, worker_id: &str, reason: &str) -> Result<()> {
        self.ensure_available()?;
        let _active_submissions = self.active_submissions_for_worker(worker_id, "stop")?;
        let request = RemoteStopRequest {
            schema_version: REMOTE_STOP_SCHEMA_V1.to_string(),
            worker_id: worker_id.to_string(),
            reason: reason.to_string(),
        };
        let response =
            self.call_protocol_with_retry("stop", || self.protocol.stop(request.clone()))?;
        if response.schema_version != REMOTE_STOP_SCHEMA_V1 {
            return Err(self.protocol_fault(format!(
                "stop schema_version {} did not match {}",
                response.schema_version, REMOTE_STOP_SCHEMA_V1
            )));
        }
        if !response.accepted {
            return Err(anyhow!(
                "remote worker backend stop rejected for worker {}",
                worker_id
            ));
        }
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Clone, Default)]
struct FakeRemoteWorkerHarness {
    state: Arc<Mutex<FakeRemoteWorkerState>>,
}

#[derive(Default)]
struct FakeRemoteWorkerState {
    ticket_seq: u64,
    worker_by_ticket: HashMap<String, String>,
    trial_by_worker: HashMap<String, String>,
    submit_requests: Vec<RemoteSubmitRequest>,
    poll_requests: Vec<RemotePollRequest>,
    pause_requests: Vec<RemotePauseRequest>,
    stop_requests: Vec<RemoteStopRequest>,
    queued_completions: VecDeque<TrialCompletion>,
}

#[allow(dead_code)]
impl FakeRemoteWorkerHarness {
    fn new() -> Self {
        Self::default()
    }

    fn enqueue_completion(&self, completion: TrialCompletion) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        state.queued_completions.push_back(completion);
        Ok(())
    }

    fn submit_requests(&self) -> Result<Vec<RemoteSubmitRequest>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        Ok(state.submit_requests.clone())
    }

    fn poll_requests(&self) -> Result<Vec<RemotePollRequest>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        Ok(state.poll_requests.clone())
    }

    fn pause_requests(&self) -> Result<Vec<RemotePauseRequest>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        Ok(state.pause_requests.clone())
    }

    fn stop_requests(&self) -> Result<Vec<RemoteStopRequest>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        Ok(state.stop_requests.clone())
    }
}

impl RemoteWorkerProtocol for FakeRemoteWorkerHarness {
    fn submit(&self, request: RemoteSubmitRequest) -> Result<RemoteSubmitResponse> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        state.ticket_seq += 1;
        state.submit_requests.push(request.clone());
        let worker_id = format!("fake.remote.worker.{}", state.ticket_seq);
        let ticket_id = format!("fake.remote.ticket.{}", state.ticket_seq);
        state
            .worker_by_ticket
            .insert(ticket_id.clone(), worker_id.clone());
        state
            .trial_by_worker
            .insert(worker_id.clone(), request.dispatch.trial_id.clone());
        Ok(RemoteSubmitResponse {
            schema_version: REMOTE_SUBMIT_SCHEMA_V1.to_string(),
            ticket: WorkerTicket {
                worker_id,
                ticket_id,
                trial_id: request.dispatch.trial_id,
            },
        })
    }

    fn poll(&self, request: RemotePollRequest) -> Result<RemotePollResponse> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        state.poll_requests.push(request);
        let mut completions = Vec::new();
        while let Some(completion) = state.queued_completions.pop_front() {
            completions.push(completion);
        }
        Ok(RemotePollResponse {
            schema_version: REMOTE_POLL_SCHEMA_V1.to_string(),
            completions,
        })
    }

    fn pause(&self, request: RemotePauseRequest) -> Result<RemotePauseResponse> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        state.pause_requests.push(request.clone());
        let trial_id = state
            .trial_by_worker
            .get(request.worker_id.as_str())
            .cloned()
            .unwrap_or_else(|| "unknown_trial".to_string());
        let accepted = state
            .trial_by_worker
            .contains_key(request.worker_id.as_str());
        Ok(RemotePauseResponse {
            schema_version: REMOTE_PAUSE_SCHEMA_V1.to_string(),
            ack: WorkerPauseAck {
                worker_id: request.worker_id,
                trial_id,
                label: request.label,
                accepted,
            },
        })
    }

    fn stop(&self, request: RemoteStopRequest) -> Result<RemoteStopResponse> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fake remote harness state lock poisoned"))?;
        state.stop_requests.push(request.clone());
        let accepted = state
            .trial_by_worker
            .remove(request.worker_id.as_str())
            .is_some();
        Ok(RemoteStopResponse {
            schema_version: REMOTE_STOP_SCHEMA_V1.to_string(),
            accepted,
        })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentLaunchMode {
    File,
    Stdio,
}

impl AgentLaunchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Stdio => "stdio",
        }
    }

    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.unwrap_or("file") {
            "file" => Ok(Self::File),
            "stdio" => Ok(Self::Stdio),
            other => Err(anyhow!("unsupported launch mode: {}", other)),
        }
    }
}

#[derive(Debug)]
pub struct RunResult {
    pub run_dir: PathBuf,
    pub run_id: String,
}

pub struct ReplayResult {
    pub replay_dir: PathBuf,
    pub replay_id: String,
    pub parent_trial_id: String,
    pub strict: bool,
    pub replay_grade: String,
    pub harness_status: String,
}

pub struct ForkResult {
    pub fork_dir: PathBuf,
    pub fork_id: String,
    pub parent_trial_id: String,
    pub selector: String,
    pub strict: bool,
    pub replay_grade: String,
    pub harness_status: String,
    pub source_checkpoint: Option<String>,
    pub fallback_mode: String,
}

pub struct PauseResult {
    pub run_id: String,
    pub trial_id: String,
    pub label: String,
    pub checkpoint_acked: bool,
    pub stop_acked: bool,
}

pub struct KillResult {
    pub run_id: String,
    pub run_dir: PathBuf,
    pub previous_status: String,
    pub killed_trials: Vec<String>,
}

pub struct ResumeResult {
    pub trial_id: String,
    pub selector: String,
    pub fork: ForkResult,
}

enum ForkSelector {
    Checkpoint(String),
    Step(u64),
    EventSeq(u64),
}

#[derive(Debug)]
struct RunOperationLock {
    path: PathBuf,
}

impl Drop for RunOperationLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_run_operation_lock(run_dir: &Path) -> Result<RunOperationLock> {
    let lock_path = run_dir.join("runtime").join("operation.lock");
    if let Some(parent) = lock_path.parent() {
        ensure_dir(parent)?;
    }
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
    {
        Ok(mut file) => {
            let payload = format!(
                "{{\"pid\":{},\"acquired_at\":\"{}\"}}\n",
                std::process::id(),
                Utc::now().to_rfc3339()
            );
            let _ = file.write_all(payload.as_bytes());
            let _ = file.sync_all();
            Ok(RunOperationLock { path: lock_path })
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(anyhow!(
            "operation_in_progress: run is already under control operation"
        )),
        Err(e) => Err(e.into()),
    }
}

#[derive(Debug, Deserialize)]
struct ExperimentOverrides {
    schema_version: String,
    #[serde(default)]
    manifest_path: Option<String>,
    #[serde(default)]
    values: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct KnobManifest {
    schema_version: String,
    knobs: Vec<KnobDef>,
}

#[derive(Debug, Deserialize)]
struct KnobDef {
    id: String,
    json_pointer: String,
    #[serde(rename = "type")]
    value_type: String,
    #[serde(default)]
    options: Option<Vec<Value>>,
    #[serde(default)]
    minimum: Option<f64>,
    #[serde(default)]
    maximum: Option<f64>,
}

pub fn validate_knob_overrides(manifest_path: &Path, overrides_path: &Path) -> Result<()> {
    let manifest = load_knob_manifest(manifest_path)?;
    let overrides = load_experiment_overrides(overrides_path)?;
    let mut by_id: BTreeMap<String, KnobDef> = BTreeMap::new();
    for knob in manifest.knobs {
        by_id.insert(knob.id.clone(), knob);
    }
    for (id, value) in overrides.values.iter() {
        let knob = by_id
            .get(id)
            .ok_or_else(|| anyhow!("override references unknown knob id: {}", id))?;
        validate_knob_value(knob, value)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunBehavior {
    pub setup_command: Option<String>,
    pub network_mode_override: Option<String>,
    pub require_network_none: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    LocalDocker,
    LocalProcess,
    Remote,
}

impl ExecutorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalDocker => "local_docker",
            Self::LocalProcess => "local_process",
            Self::Remote => "remote",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationMode {
    None,
    MetadataOnly,
    OutputsOnly,
    Full,
}

impl MaterializationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::MetadataOnly => "metadata_only",
            Self::OutputsOnly => "outputs_only",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunExecutionOptions {
    pub executor: Option<ExecutorKind>,
    pub materialize: Option<MaterializationMode>,
    pub remote_endpoint: Option<String>,
    pub remote_token_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunSessionState {
    schema_version: String,
    run_id: String,
    behavior: RunBehavior,
    execution: RunExecutionOptions,
}

fn normalize_execution_options(execution: &RunExecutionOptions) -> RunExecutionOptions {
    RunExecutionOptions {
        executor: execution.executor,
        materialize: Some(execution.materialize.unwrap_or(MaterializationMode::Full)),
        remote_endpoint: execution.remote_endpoint.clone(),
        remote_token_env: execution.remote_token_env.clone(),
    }
}

fn resolve_remote_bearer_token(token_env: Option<&str>) -> Result<Option<String>> {
    let name = token_env
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("unset"));
    let Some(name) = name else {
        return Ok(None);
    };
    let value = std::env::var(name).map_err(|_| {
        anyhow!(
            "remote executor token env var '{}' is not set in current process environment",
            name
        )
    })?;
    Ok(Some(value))
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let ts = Utc::now().timestamp_micros();
    let pid = std::process::id();
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("tmpfile");
    let tmp = path.with_file_name(format!(".{}.tmp.{}.{}", name, pid, ts));
    let mut file = fs::File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn atomic_write_json_pretty(path: &Path, value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    atomic_write_bytes(path, &bytes)
}

fn run_control_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("run_control.json")
}

fn run_session_state_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("run_session_state.json")
}

fn parallel_worker_control_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("parallel_worker_control.json")
}

fn load_parallel_worker_control_state(
    run_dir: &Path,
) -> Result<Option<ParallelWorkerControlState>> {
    let path = parallel_worker_control_path(run_dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let state: ParallelWorkerControlState = serde_json::from_slice(&bytes)?;
    Ok(Some(state))
}

fn write_parallel_worker_control_state(
    run_dir: &Path,
    state: &ParallelWorkerControlState,
) -> Result<()> {
    let payload = serde_json::to_value(state)?;
    atomic_write_json_pretty(&parallel_worker_control_path(run_dir), &payload)
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
        execution: normalize_execution_options(execution),
    };
    let payload = serde_json::to_value(state)?;
    atomic_write_json_pretty(&run_session_state_path(run_dir), &payload)
}

fn load_run_session_state(run_dir: &Path) -> Result<RunSessionState> {
    let path = run_session_state_path(run_dir);
    if !path.exists() {
        return Err(anyhow!(
            "run_session_state.json not found — this run predates continue behavior persistence"
        ));
    }
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

const RUN_CONTROL_UNKNOWN_WORKER_ID: &str = "worker.unknown";

#[derive(Debug, Clone)]
struct RunControlActiveTrial {
    trial_id: String,
    worker_id: String,
    schedule_idx: Option<usize>,
    variant_id: Option<String>,
    started_at: Option<String>,
    control: Option<ActiveAdapterControl>,
}

#[derive(Debug, Clone)]
struct RunControlPauseMetadata {
    label: String,
    requested_at: String,
    requested_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleEngineOutcome {
    Completed,
    Paused,
    Killed,
    Interrupted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ParallelWorkerControlAction {
    Pause,
    Stop,
}

impl ParallelWorkerControlAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Stop => "stop",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ParallelWorkerControlRequest {
    request_id: String,
    action: ParallelWorkerControlAction,
    requested_at: String,
    target_trial_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ParallelWorkerControlResponse {
    request_id: String,
    action: ParallelWorkerControlAction,
    status: String,
    processed_at: String,
    processed_trial_ids: Vec<String>,
    failed_trials: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_acked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stop_acked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ParallelWorkerControlState {
    schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request: Option<ParallelWorkerControlRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<ParallelWorkerControlResponse>,
    updated_at: String,
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
    let mut ids: Vec<String> = Vec::new();
    if let Some(active_trials) = run_control
        .pointer("/active_trials")
        .and_then(|v| v.as_object())
    {
        for (trial_id, entry) in active_trials {
            let candidate = entry
                .pointer("/trial_id")
                .and_then(|v| v.as_str())
                .unwrap_or(trial_id);
            if !ids.iter().any(|existing| existing == candidate) {
                ids.push(candidate.to_string());
            }
        }
    }
    ids
}

fn run_control_active_adapter_for_trial(run_control: &Value, trial_id: &str) -> Option<Value> {
    let active_trials = run_control
        .pointer("/active_trials")
        .and_then(|v| v.as_object())?;
    let entry = active_trials.iter().find_map(|(key, value)| {
        let candidate = value
            .pointer("/trial_id")
            .and_then(|v| v.as_str())
            .unwrap_or(key.as_str());
        if candidate == trial_id {
            Some(value)
        } else {
            None
        }
    })?;
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
        for (trial_id_key, entry) in entries {
            let trial_id = entry
                .pointer("/trial_id")
                .and_then(|v| v.as_str())
                .unwrap_or(trial_id_key)
                .to_string();
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
                trial_id,
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
    atomic_write_json_pretty(&run_control_path(run_dir), &payload)
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
            let _ = write_run_control_v2(&self.run_dir, &self.run_id, "failed", &[], None);
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

pub fn find_project_root(experiment_dir: &Path) -> PathBuf {
    let mut cur = Some(experiment_dir);
    while let Some(p) = cur {
        if p.file_name().and_then(|s| s.to_str()) == Some(".lab") {
            return p.parent().unwrap_or(experiment_dir).to_path_buf();
        }
        cur = p.parent();
    }
    experiment_dir.to_path_buf()
}

#[derive(Debug, Clone)]
pub struct ExperimentSummary {
    pub exp_id: String,
    pub workload_type: String,
    pub dataset_path: PathBuf,
    pub task_count: usize,
    pub replications: usize,
    pub variant_count: usize,
    pub total_trials: usize,
    pub agent_runtime_command: Vec<String>,
    pub container_mode: bool,
    pub image: Option<String>,
    pub network_mode: String,
    pub trajectory_path: Option<String>,
    pub causal_extraction: Option<String>,
    pub scheduling: String,
    pub state_policy: String,
    pub comparison: String,
    pub retry_max_attempts: usize,
}

pub fn run_experiment(path: &Path, use_container: bool) -> Result<RunResult> {
    run_experiment_with_behavior(
        path,
        use_container,
        RunBehavior::default(),
        None,
        RunExecutionOptions::default(),
    )
}

pub fn run_experiment_dev(path: &Path, setup_command: Option<String>) -> Result<RunResult> {
    run_experiment_dev_with_overrides(path, setup_command, None)
}

pub fn run_experiment_with_overrides(
    path: &Path,
    use_container: bool,
    overrides_path: Option<&Path>,
) -> Result<RunResult> {
    run_experiment_with_behavior(
        path,
        use_container,
        RunBehavior::default(),
        overrides_path,
        RunExecutionOptions::default(),
    )
}

pub fn run_experiment_with_options_and_overrides(
    path: &Path,
    use_container: bool,
    overrides_path: Option<&Path>,
    options: RunExecutionOptions,
) -> Result<RunResult> {
    run_experiment_with_behavior(
        path,
        use_container,
        RunBehavior::default(),
        overrides_path,
        options,
    )
}

pub fn run_experiment_dev_with_overrides(
    path: &Path,
    setup_command: Option<String>,
    overrides_path: Option<&Path>,
) -> Result<RunResult> {
    let behavior = RunBehavior {
        setup_command,
        network_mode_override: Some("full".to_string()),
        require_network_none: false,
    };
    run_experiment_with_behavior(
        path,
        true,
        behavior,
        overrides_path,
        RunExecutionOptions::default(),
    )
}

pub fn run_experiment_strict(path: &Path) -> Result<RunResult> {
    run_experiment_strict_with_overrides(path, None)
}

pub fn run_experiment_strict_with_overrides(
    path: &Path,
    overrides_path: Option<&Path>,
) -> Result<RunResult> {
    let behavior = RunBehavior {
        setup_command: None,
        network_mode_override: None,
        require_network_none: true,
    };
    run_experiment_with_behavior(
        path,
        true,
        behavior,
        overrides_path,
        RunExecutionOptions::default(),
    )
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
/// Loads persisted run session + `schedule_progress.json`, validates the run is
/// in a continuable terminal state, reconstructs experiment parameters,
/// verifies schedule integrity, and re-enters the trial loop from the next
/// unprocessed slot.
pub fn continue_run(run_dir: &Path) -> Result<RunResult> {
    let _op_lock = acquire_run_operation_lock(run_dir)?;
    let run_dir = run_dir
        .canonicalize()
        .unwrap_or_else(|_| run_dir.to_path_buf());

    // 1. Validate run status is terminal and continuable.
    let control_path = run_control_path(&run_dir);
    let control: Value = serde_json::from_slice(&fs::read(&control_path)?)?;
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
                "run is currently active — cannot continue a running experiment"
            ))
        }
        other => return Err(anyhow!("unexpected run status: {}", other)),
    }

    let run_id = control
        .get("run_id")
        .and_then(|v| v.as_str())
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
    let behavior = run_session.behavior;
    let execution = run_session.execution;

    // 2. Load schedule progress
    let progress_path = schedule_progress_path(&run_dir);
    if !progress_path.exists() {
        return Err(anyhow!(
            "schedule_progress.json not found — this run predates continue support"
        ));
    }
    let progress: ScheduleProgress = serde_json::from_slice(&fs::read(&progress_path)?)?;
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
    write_resolved_variants(&run_dir, &baseline_id, &variants)?;
    let exp_dir = resolved_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let dataset_path = resolve_dataset_path(&json_value, &exp_dir)?;
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
    let use_container = progress.use_container;
    let materialize_mode = execution.materialize.unwrap_or(MaterializationMode::Full);

    // 6. Mark run as running again
    write_run_control_v2(&run_dir, &run_id, "running", &[], None)?;
    let mut run_guard = RunControlGuard::new(&run_dir, &run_id);

    // 7. Reconstruct variant runtime profiles
    let mut variant_runtime_profiles = Vec::with_capacity(variants.len());
    for variant in &variants {
        variant_runtime_profiles.push(resolve_variant_runtime_profile(
            &json_value,
            variant,
            &project_root,
            use_container,
            &behavior,
            &execution,
        )?);
    }
    let run_integration_level = variant_runtime_profiles
        .first()
        .map(|profile| profile.agent_runtime.integration_level.clone())
        .unwrap_or_else(|| "cli_basic".to_string());
    let all_container_mode = variant_runtime_profiles
        .iter()
        .all(|profile| profile.container_mode);

    let benchmark_config = parse_benchmark_config(&json_value);

    // 8. Restore scheduler state from progress
    let mut consecutive_failures: BTreeMap<usize, usize> = progress.consecutive_failures.clone();
    let mut pruned_variants: HashSet<usize> = progress.pruned_variants.iter().copied().collect();

    let trials_dir = run_dir.join("trials");
    ensure_dir(&trials_dir)?;
    let evidence_dir = run_dir.join("evidence");
    ensure_dir(&evidence_dir)?;
    let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
    let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
    let mut run_sink = JsonlRunSink::new(&run_dir)?;
    let facts_run_manifest_path = run_dir.join("facts").join("run_manifest.json");
    if !facts_run_manifest_path.exists() {
        run_sink.write_run_manifest(&RunManifestRecord {
            schema_version: "run_manifest_v1".to_string(),
            run_id: run_id.clone(),
            created_at: Utc::now().to_rfc3339(),
            workload_type: workload_type.clone(),
            baseline_id: baseline_id.clone(),
            variant_ids: variants.iter().map(|variant| variant.id.clone()).collect(),
        })?;
    }

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
        execution.remote_endpoint.as_deref(),
        execution.remote_token_env.as_deref(),
    )?;
    run_sink.flush()?;
    if schedule_outcome != ScheduleEngineOutcome::Completed {
        run_guard.disarm();
        return Ok(RunResult {
            run_dir: run_dir.to_path_buf(),
            run_id,
        });
    }

    validate_jsonl_against_schema("evidence_record_v1.jsonschema", &evidence_records_path)?;
    validate_jsonl_against_schema("task_chain_state_v1.jsonschema", &task_chain_states_path)?;
    if let Some(adapter) = benchmark_config.adapter.as_ref() {
        let _scores_path = process_benchmark_outputs(
            &project_root,
            &run_dir,
            &run_id,
            adapter,
            &evidence_records_path,
            &task_chain_states_path,
        )?;
    }

    let resolved_digest = canonical_json_digest(&json_value);
    let grades = json!({
        "schema_version": "grades_v1",
        "integration_level": run_integration_level,
        "replay_grade": "best_effort",
        "isolation_grade": if all_container_mode {"bounded"} else {"leaky"},
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

pub fn replay_trial(run_dir: &Path, trial_id: &str, strict: bool) -> Result<ReplayResult> {
    let _op_lock = acquire_run_operation_lock(run_dir)?;
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
    let policy_config = parse_policies(&json_value);
    let parent_trial_dir = run_dir.join("trials").join(trial_id);
    if !parent_trial_dir.exists() {
        return Err(anyhow!("parent trial not found: {}", trial_id));
    }
    let parent_input_path = parent_trial_dir.join("trial_input.json");
    if !parent_input_path.exists() {
        return Err(anyhow!(
            "parent trial missing trial_input.json: {}",
            parent_input_path.display()
        ));
    }
    let mut input: Value = serde_json::from_slice(&fs::read(&parent_input_path)?)?;
    let (variants, _) = load_run_variants(&run_dir, &json_value)?;
    let variant_id = input
        .pointer("/ids/variant_id")
        .and_then(|v| v.as_str())
        .or_else(|| {
            json_value
                .pointer("/baseline/variant_id")
                .and_then(|v| v.as_str())
        })
        .unwrap_or("");
    let variant = find_variant_by_id(&variants, variant_id)?;
    let runtime_profile = resolve_variant_runtime_profile(
        &json_value,
        variant,
        &project_root,
        false,
        &RunBehavior::default(),
        &RunExecutionOptions::default(),
    )?;
    let variant_args = runtime_profile.variant_args.clone();
    let agent_runtime = runtime_profile.agent_runtime;
    let agent_runtime_env = runtime_profile.agent_runtime_env;
    let container_mode = runtime_profile.container_mode;
    let invocation_default_timeout_ms = runtime_profile.invocation_default_timeout_ms;
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
    set_json_pointer_value(
        &mut input,
        "/ids/trial_id",
        Value::String(replay_trial_id.clone()),
    )?;
    let task_boundary = parse_task_boundary_from_trial_input(&input)?;
    validate_task_boundary_workspace_materialization(&task_boundary, &policy_config.task_boundary)?;

    let dataset_src = first_file_in_dir(&parent_trial_dir.join("dataset"))?;
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

    let workspace_src = if parent_trial_dir.join("workspace").exists() {
        parent_trial_dir.join("workspace")
    } else {
        project_root.clone()
    };
    let trial_paths = TrialPaths::new(&replay_trial_dir, &workspace_src, &dataset_src)?;
    trial_paths.prepare(true)?;
    stage_dependencies_for_trial(&agent_runtime, &trial_paths)?;
    materialize_workspace_files(&trial_paths, &task_boundary.workspace_files)?;

    set_json_pointer_value(
        &mut input,
        "/runtime/control_plane/path",
        json!(DEFAULT_CONTAINER_CONTROL_PATH),
    )?;
    set_json_pointer_value(&mut input, "/runtime/control_plane/mode", json!("file"))?;
    let input_bytes = serde_json::to_vec_pretty(&input)?;
    let canonical_input = replay_trial_dir.join("trial_input.json");
    atomic_write_bytes(&canonical_input, &input_bytes)?;

    let io_paths = prepare_io_paths(
        &trial_paths,
        container_mode,
        &input_bytes,
        is_clean_contract_experiment(&runtime_experiment),
    )?;
    let runtime_env = build_runtime_contract_env(
        &run_id,
        &input,
        &io_paths,
        resolve_trial_timeout_ms(&input, invocation_default_timeout_ms),
        runtime_experiment
            .pointer("/version")
            .and_then(|v| v.as_str())
            == Some("1.0"),
    );
    let dynamic_mounts = resolve_task_mounts(
        &project_root,
        &task_boundary.mount_references,
        container_mode,
    )?;
    let adapter = adapter_registry_entry(&agent_runtime.adapter_ref)?;
    let run_request = AdapterRunRequest {
        runtime_experiment: &runtime_experiment,
        runtime: &agent_runtime,
        variant_args: &variant_args,
        runtime_env: &runtime_env,
        runtime_overrides_env: &agent_runtime_env,
        container_mode,
        trial_paths: &trial_paths,
        dynamic_mounts: &dynamic_mounts,
        io_paths: &io_paths,
        network_mode: effective_network_mode.as_str(),
        setup_command: None,
        benchmark_adapter: None,
        benchmark_grading_enabled: false,
        run_id: &run_id,
        task_image: task_boundary.task_image.as_deref(),
        task_workspace: task_boundary.task_workspace.as_deref(),
        agent_artifact: agent_runtime.agent_artifact.as_deref(),
    };
    let proc_result = adapter.run_trial(&run_request)?;
    let status = proc_result.status;

    materialize_trial_result(&replay_trial_dir, &io_paths.output_host)?;

    let canonical_output = replay_trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME);
    let trial_output: Value = if canonical_output.exists() {
        serde_json::from_slice(&fs::read(&canonical_output)?)?
    } else {
        json!({"schema_version":"agent_result_v1","outcome":"error"})
    };

    let outcome = trial_output
        .get("outcome")
        .and_then(|v| v.as_str())
        .unwrap_or("error");
    if status == "0" && outcome != "error" {
        trial_guard.complete("completed", None)?;
    } else if status != "0" {
        trial_guard.complete("failed", Some("harness_exit_nonzero"))?;
    } else {
        trial_guard.complete("failed", Some("trial_output_error"))?;
    }

    let replay_grade = replay_grade_for_integration(&agent_runtime.integration_level).to_string();
    let manifest = json!({
        "schema_version": "replay_manifest_v1",
        "operation": "replay",
        "replay_id": replay_id.clone(),
        "parent_trial_id": trial_id,
        "strict": strict,
        "integration_level": agent_runtime.integration_level.clone(),
        "replay_grade": replay_grade.clone(),
        "created_at": Utc::now().to_rfc3339(),
    });
    atomic_write_json_pretty(&replay_dir.join("manifest.json"), &manifest)?;

    Ok(ReplayResult {
        replay_dir,
        replay_id,
        parent_trial_id: trial_id.to_string(),
        strict,
        replay_grade,
        harness_status: status,
    })
}

fn first_file_in_dir(dir: &Path) -> Result<PathBuf> {
    if !dir.exists() {
        return Err(anyhow!("directory not found: {}", dir.display()));
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            return Ok(entry.path());
        }
    }
    Err(anyhow!("no files found in {}", dir.display()))
}

fn replay_grade_for_integration(level: &str) -> &'static str {
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
    let _op_lock = acquire_run_operation_lock(run_dir)?;
    fork_trial_inner(run_dir, from_trial, selector, set_bindings, strict)
}

fn fork_trial_inner(
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
    let policy_config = parse_policies(&json_value);
    let parent_trial_dir = run_dir.join("trials").join(from_trial);
    if !parent_trial_dir.exists() {
        return Err(anyhow!("parent trial not found: {}", from_trial));
    }
    let parent_input_path = parent_trial_dir.join("trial_input.json");
    if !parent_input_path.exists() {
        return Err(anyhow!(
            "parent trial missing trial_input.json: {}",
            parent_input_path.display()
        ));
    }
    let parent_output_path = parent_trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME);
    let parent_output = if parent_output_path.exists() {
        Some(serde_json::from_slice::<Value>(&fs::read(
            &parent_output_path,
        )?)?)
    } else {
        None
    };
    let parsed_selector = parse_fork_selector(selector)?;

    let run_id = run_dir
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("run")
        .to_string();

    let mut input: Value = serde_json::from_slice(&fs::read(&parent_input_path)?)?;
    let (variants, _) = load_run_variants(&run_dir, &json_value)?;
    let variant_id = input
        .pointer("/ids/variant_id")
        .and_then(|v| v.as_str())
        .or_else(|| {
            json_value
                .pointer("/baseline/variant_id")
                .and_then(|v| v.as_str())
        })
        .unwrap_or("");
    let variant = find_variant_by_id(&variants, variant_id)?;
    let runtime_profile = resolve_variant_runtime_profile(
        &json_value,
        variant,
        &project_root,
        false,
        &RunBehavior::default(),
        &RunExecutionOptions::default(),
    )?;
    let variant_args = runtime_profile.variant_args.clone();
    let agent_runtime = runtime_profile.agent_runtime;
    let agent_runtime_env = runtime_profile.agent_runtime_env;
    let container_mode = runtime_profile.container_mode;
    let invocation_default_timeout_ms = runtime_profile.invocation_default_timeout_ms;
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
        &parent_trial_dir,
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
    set_json_pointer_value(
        &mut input,
        "/ids/trial_id",
        Value::String(fork_trial_id.clone()),
    )?;
    apply_binding_overrides(&mut input, set_bindings)?;
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
    let task_boundary = parse_task_boundary_from_trial_input(&input)?;
    validate_task_boundary_workspace_materialization(&task_boundary, &policy_config.task_boundary)?;

    let dataset_src = first_file_in_dir(&parent_trial_dir.join("dataset"))?;
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

    let workspace_src = if let Some(ref checkpoint) = source_checkpoint {
        let p = PathBuf::from(checkpoint);
        if p.is_dir() {
            p
        } else if parent_trial_dir.join("workspace").exists() {
            parent_trial_dir.join("workspace")
        } else {
            project_root.clone()
        }
    } else if parent_trial_dir.join("workspace").exists() {
        parent_trial_dir.join("workspace")
    } else {
        project_root.clone()
    };
    let trial_paths = TrialPaths::new(&fork_trial_dir, &workspace_src, &dataset_src)?;
    trial_paths.prepare(true)?;
    stage_dependencies_for_trial(&agent_runtime, &trial_paths)?;
    materialize_workspace_files(&trial_paths, &task_boundary.workspace_files)?;

    set_json_pointer_value(
        &mut input,
        "/runtime/control_plane/path",
        json!(DEFAULT_CONTAINER_CONTROL_PATH),
    )?;
    set_json_pointer_value(&mut input, "/runtime/control_plane/mode", json!("file"))?;
    let input_bytes = serde_json::to_vec_pretty(&input)?;
    let canonical_input = fork_trial_dir.join("trial_input.json");
    atomic_write_bytes(&canonical_input, &input_bytes)?;

    let io_paths = prepare_io_paths(
        &trial_paths,
        container_mode,
        &input_bytes,
        is_clean_contract_experiment(&runtime_experiment),
    )?;
    let runtime_env = build_runtime_contract_env(
        &run_id,
        &input,
        &io_paths,
        resolve_trial_timeout_ms(&input, invocation_default_timeout_ms),
        runtime_experiment
            .pointer("/version")
            .and_then(|v| v.as_str())
            == Some("1.0"),
    );
    let dynamic_mounts = resolve_task_mounts(
        &project_root,
        &task_boundary.mount_references,
        container_mode,
    )?;
    let adapter = adapter_registry_entry(&agent_runtime.adapter_ref)?;
    let run_request = AdapterRunRequest {
        runtime_experiment: &runtime_experiment,
        runtime: &agent_runtime,
        variant_args: &variant_args,
        runtime_env: &runtime_env,
        runtime_overrides_env: &agent_runtime_env,
        container_mode,
        trial_paths: &trial_paths,
        dynamic_mounts: &dynamic_mounts,
        io_paths: &io_paths,
        network_mode: effective_network_mode.as_str(),
        setup_command: None,
        benchmark_adapter: None,
        benchmark_grading_enabled: false,
        run_id: &run_id,
        task_image: task_boundary.task_image.as_deref(),
        task_workspace: task_boundary.task_workspace.as_deref(),
        agent_artifact: agent_runtime.agent_artifact.as_deref(),
    };
    let proc_result = adapter.run_trial(&run_request)?;
    let status = proc_result.status;

    materialize_trial_result(&fork_trial_dir, &io_paths.output_host)?;

    let canonical_output = fork_trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME);
    let trial_output: Value = if canonical_output.exists() {
        serde_json::from_slice(&fs::read(&canonical_output)?)?
    } else {
        json!({"schema_version":"agent_result_v1","outcome":"error"})
    };
    let outcome = trial_output
        .get("outcome")
        .and_then(|v| v.as_str())
        .unwrap_or("error");
    if status == "0" && outcome != "error" {
        trial_guard.complete("completed", None)?;
    } else if status != "0" {
        trial_guard.complete("failed", Some("harness_exit_nonzero"))?;
    } else {
        trial_guard.complete("failed", Some("trial_output_error"))?;
    }

    let replay_grade = replay_grade_for_integration(&agent_runtime.integration_level).to_string();
    let fallback_mode = if source_checkpoint.is_some() {
        "checkpoint".to_string()
    } else {
        "input_only".to_string()
    };
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
        "created_at": Utc::now().to_rfc3339(),
    });
    atomic_write_json_pretty(&fork_dir.join("manifest.json"), &manifest)?;

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

fn active_trials_use_worker_control_plane(
    active_by_id: &HashMap<String, RunControlActiveTrial>,
    target_trials: &[String],
) -> bool {
    !target_trials.is_empty()
        && target_trials.iter().all(|trial_id| {
            active_by_id
                .get(trial_id)
                .map(|active| {
                    active.control.is_none() && active.worker_id != RUN_CONTROL_UNKNOWN_WORKER_ID
                })
                .unwrap_or(false)
        })
}

fn next_parallel_worker_control_request_id(action: ParallelWorkerControlAction) -> String {
    format!("{}_{}", action.as_str(), Utc::now().timestamp_micros())
}

fn request_parallel_worker_pause(
    run_dir: &Path,
    run_id: &str,
    target_trials: &[String],
    pause_label: &str,
    timeout: Duration,
) -> Result<PauseResult> {
    let request_id = next_parallel_worker_control_request_id(ParallelWorkerControlAction::Pause);
    write_parallel_worker_control_request(
        run_dir,
        ParallelWorkerControlRequest {
            request_id: request_id.clone(),
            action: ParallelWorkerControlAction::Pause,
            requested_at: Utc::now().to_rfc3339(),
            target_trial_ids: target_trials.to_vec(),
            label: Some(pause_label.to_string()),
            reason: None,
        },
    )?;
    let response = wait_for_parallel_worker_control_response(run_dir, &request_id, timeout)?;
    if response.status != PARALLEL_WORKER_CONTROL_RESPONSE_COMPLETED {
        let detail = response
            .message
            .unwrap_or_else(|| response.failed_trials.join(" | "));
        return Err(anyhow!("pause_partial_failure: {}", detail));
    }
    let trial_id = if target_trials.len() == 1 {
        target_trials[0].clone()
    } else {
        "multi".to_string()
    };
    Ok(PauseResult {
        run_id: run_id.to_string(),
        trial_id,
        label: pause_label.to_string(),
        checkpoint_acked: response.checkpoint_acked.unwrap_or(false),
        stop_acked: response.stop_acked.unwrap_or(false),
    })
}

fn request_parallel_worker_stop(
    run_dir: &Path,
    run_id: &str,
    previous_status: &str,
    target_trials: &[String],
    timeout: Duration,
) -> Result<KillResult> {
    let request_id = next_parallel_worker_control_request_id(ParallelWorkerControlAction::Stop);
    write_parallel_worker_control_request(
        run_dir,
        ParallelWorkerControlRequest {
            request_id: request_id.clone(),
            action: ParallelWorkerControlAction::Stop,
            requested_at: Utc::now().to_rfc3339(),
            target_trial_ids: target_trials.to_vec(),
            label: None,
            reason: Some("killed_by_user".to_string()),
        },
    )?;
    let response = wait_for_parallel_worker_control_response(run_dir, &request_id, timeout)?;
    if response.status != PARALLEL_WORKER_CONTROL_RESPONSE_COMPLETED {
        let detail = response
            .message
            .unwrap_or_else(|| response.failed_trials.join(" | "));
        return Err(anyhow!("kill_partial_failure: {}", detail));
    }
    Ok(KillResult {
        run_id: run_id.to_string(),
        run_dir: run_dir.to_path_buf(),
        previous_status: previous_status.to_string(),
        killed_trials: target_trials.to_vec(),
    })
}

pub fn pause_run(
    run_dir: &Path,
    trial_id: Option<&str>,
    label: Option<&str>,
    timeout_seconds: u64,
) -> Result<PauseResult> {
    let _op_lock = acquire_run_operation_lock(run_dir)?;
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
    let timeout = Duration::from_secs(timeout_seconds.max(1));
    let active_by_id: HashMap<String, RunControlActiveTrial> =
        run_control_active_trials(&run_control)
            .into_iter()
            .map(|entry| (entry.trial_id.clone(), entry))
            .collect();
    if active_trials_use_worker_control_plane(&active_by_id, &target_trials) {
        return request_parallel_worker_pause(
            &run_dir,
            &run_id,
            &target_trials,
            &pause_label,
            timeout,
        );
    }

    let resolved = load_json_file(&run_dir.join("resolved_experiment.json"))?;
    let project_root = find_project_root(&run_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&run_dir));
    let (variants, _) = load_run_variants(&run_dir, &resolved)?;

    let mut paused_active_trials: Vec<RunControlActiveTrial> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    let mut checkpoint_acked_all = true;
    let mut stop_acked_all = true;

    for target_trial in &target_trials {
        let active_adapter_value =
            match run_control_active_adapter_for_trial(&run_control, target_trial) {
                Some(value) => value,
                None => {
                    failures.push(format!("{}: pause_missing_active_adapter", target_trial));
                    continue;
                }
            };
        let active_control: ActiveAdapterControl =
            match serde_json::from_value(active_adapter_value) {
                Ok(control) => control,
                Err(err) => {
                    failures.push(format!(
                        "{}: invalid active adapter control ({})",
                        target_trial, err
                    ));
                    continue;
                }
            };

        let trial_dir = run_dir.join("trials").join(target_trial);
        if !trial_dir.exists() {
            failures.push(format!("{}: pause_trial_not_found", target_trial));
            continue;
        }
        let trial_input = match load_json_file(&trial_dir.join("trial_input.json")) {
            Ok(value) => value,
            Err(err) => {
                failures.push(format!(
                    "{}: failed to read trial_input.json ({})",
                    target_trial, err
                ));
                continue;
            }
        };
        let variant_id = trial_input
            .pointer("/ids/variant_id")
            .and_then(|v| v.as_str())
            .or_else(|| {
                resolved
                    .pointer("/baseline/variant_id")
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("");
        let variant = match find_variant_by_id(&variants, variant_id) {
            Ok(variant) => variant,
            Err(err) => {
                failures.push(format!("{}: {}", target_trial, err));
                continue;
            }
        };
        let runtime_profile = match resolve_variant_runtime_profile(
            &resolved,
            variant,
            &project_root,
            false,
            &RunBehavior::default(),
            &RunExecutionOptions::default(),
        ) {
            Ok(profile) => profile,
            Err(err) => {
                failures.push(format!("{}: {}", target_trial, err));
                continue;
            }
        };
        let adapter = match adapter_registry_entry(&runtime_profile.agent_runtime.adapter_ref) {
            Ok(adapter) => adapter,
            Err(err) => {
                failures.push(format!("{}: {}", target_trial, err));
                continue;
            }
        };
        let capabilities = adapter.capabilities();
        if !capabilities.pause {
            failures.push(format!(
                "{}: pause_unsupported_capability for adapter '{}@{}'",
                target_trial,
                runtime_profile.agent_runtime.adapter_ref.id,
                runtime_profile.agent_runtime.adapter_ref.version
            ));
            continue;
        }
        if runtime_profile.agent_runtime.adapter_ref.id != active_control.adapter_id
            || runtime_profile.agent_runtime.adapter_ref.version != active_control.adapter_version
        {
            failures.push(format!(
                "{}: pause_adapter_mismatch active='{}@{}' resolved='{}@{}'",
                target_trial,
                active_control.adapter_id,
                active_control.adapter_version,
                runtime_profile.agent_runtime.adapter_ref.id,
                runtime_profile.agent_runtime.adapter_ref.version
            ));
            continue;
        }

        let pause_ack = match adapter.pause_trial(&AdapterPauseRequest {
            control: &active_control,
            label: &pause_label,
            timeout,
        }) {
            Ok(ack) => ack,
            Err(err) => {
                failures.push(format!("{}: pause request failed ({})", target_trial, err));
                continue;
            }
        };
        checkpoint_acked_all &= pause_ack.checkpoint_acked;
        stop_acked_all &= pause_ack.stop_acked;
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

        if let Some(mut active) = active_by_id.get(target_trial).cloned() {
            active.control = Some(active_control.clone());
            paused_active_trials.push(active);
        } else {
            failures.push(format!(
                "{}: pause_missing_active_trial_metadata",
                target_trial
            ));
        }
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
    let _op_lock = acquire_run_operation_lock(run_dir)?;
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
    if status == "running"
        && active_trials_use_worker_control_plane(&active_by_id, &active_trial_ids)
    {
        return request_parallel_worker_stop(
            &run_dir,
            &run_id,
            &status,
            &active_trial_ids,
            Duration::from_secs(KILL_RUN_WORKER_CONTROL_TIMEOUT_SECONDS),
        );
    }

    for trial_id in &active_trial_ids {
        let trial_dir = run_dir.join("trials").join(trial_id);
        if trial_dir.exists() {
            let _ = write_trial_state(
                &trial_dir,
                trial_id,
                "killed",
                None,
                None,
                Some("killed_by_user"),
            );
        }
    }

    write_run_control_v2(&run_dir, &run_id, "killed", &[], None)?;

    Ok(KillResult {
        run_id,
        run_dir: run_dir.to_path_buf(),
        previous_status: status,
        killed_trials: active_trial_ids,
    })
}

pub fn resume_trial(
    run_dir: &Path,
    trial_id: Option<&str>,
    label: Option<&str>,
    set_bindings: &BTreeMap<String, Value>,
    strict: bool,
) -> Result<ResumeResult> {
    let _op_lock = acquire_run_operation_lock(run_dir)?;
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
    let selector = resolve_resume_selector(&trial_dir, label.or(pause_label))?;

    let fork = fork_trial_inner(&run_dir, &target_trial, &selector, set_bindings, strict)?;
    Ok(ResumeResult {
        trial_id: target_trial,
        selector,
        fork,
    })
}

fn load_json_file(path: &Path) -> Result<Value> {
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn resolve_resume_selector(trial_dir: &Path, preferred_label: Option<&str>) -> Result<String> {
    let output_path = trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME);
    if !output_path.exists() {
        return Err(anyhow!("resume_no_trial_result: {}", output_path.display()));
    }
    let output = load_json_file(&output_path)?;
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
enum ContractPathRoot {
    In,
    State,
    Out,
    Deps,
    Workspace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContractPathMode {
    ContainerMount,
    RuntimeIo,
    RuntimeEvents,
}

#[derive(Debug, Clone)]
struct ContractPathHostRoots {
    in_dir: PathBuf,
    state_dir: PathBuf,
    out_dir: PathBuf,
    deps_dir: PathBuf,
    workspace_dir: PathBuf,
}

impl ContractPathHostRoots {
    fn from_trial_paths(paths: &TrialPaths) -> Self {
        Self {
            in_dir: paths.in_dir.clone(),
            state_dir: paths.state.clone(),
            out_dir: paths.out.clone(),
            deps_dir: paths.deps.clone(),
            workspace_dir: paths.workspace.clone(),
        }
    }

    fn from_trial_dir(trial_dir: &Path) -> Self {
        Self {
            in_dir: trial_dir.join("in"),
            state_dir: trial_dir.join("state"),
            out_dir: trial_dir.join("out"),
            deps_dir: trial_dir.join("deps"),
            workspace_dir: trial_dir.join("workspace"),
        }
    }

    fn base_for(&self, root: ContractPathRoot) -> &Path {
        match root {
            ContractPathRoot::In => self.in_dir.as_path(),
            ContractPathRoot::State => self.state_dir.as_path(),
            ContractPathRoot::Out => self.out_dir.as_path(),
            ContractPathRoot::Deps => self.deps_dir.as_path(),
            ContractPathRoot::Workspace => self.workspace_dir.as_path(),
        }
    }
}

fn strip_contract_prefix<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
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

fn resolve_contract_path_components(path: &str) -> Option<(ContractPathRoot, &str)> {
    if let Some(rest) = strip_contract_prefix(path, AGENTLAB_CONTRACT_IN_DIR) {
        return Some((ContractPathRoot::In, rest));
    }
    if let Some(rest) = strip_contract_prefix(path, AGENTLAB_CONTRACT_STATE_DIR) {
        return Some((ContractPathRoot::State, rest));
    }
    if let Some(rest) = strip_contract_prefix(path, AGENTLAB_CONTRACT_OUT_DIR) {
        return Some((ContractPathRoot::Out, rest));
    }
    if let Some(rest) = strip_contract_prefix(path, AGENTLAB_CONTRACT_DEPS_DIR) {
        return Some((ContractPathRoot::Deps, rest));
    }
    if let Some(rest) = strip_contract_prefix(path, AGENTLAB_CONTRACT_WORKSPACE_DIR) {
        return Some((ContractPathRoot::Workspace, rest));
    }
    None
}

fn mode_allows_root(mode: ContractPathMode, root: ContractPathRoot) -> bool {
    match mode {
        ContractPathMode::ContainerMount | ContractPathMode::RuntimeIo => matches!(
            root,
            ContractPathRoot::In
                | ContractPathRoot::State
                | ContractPathRoot::Out
                | ContractPathRoot::Deps
                | ContractPathRoot::Workspace
        ),
        ContractPathMode::RuntimeEvents => matches!(
            root,
            ContractPathRoot::In
                | ContractPathRoot::State
                | ContractPathRoot::Out
                | ContractPathRoot::Workspace
        ),
    }
}

fn map_contract_path_to_host(
    path: &str,
    roots: &ContractPathHostRoots,
    mode: ContractPathMode,
) -> Result<PathBuf> {
    let raw = match mode {
        ContractPathMode::ContainerMount => path.trim(),
        ContractPathMode::RuntimeIo | ContractPathMode::RuntimeEvents => path,
    };
    if raw.is_empty() {
        return Err(match mode {
            ContractPathMode::ContainerMount => anyhow!("container path is empty"),
            ContractPathMode::RuntimeIo => anyhow!(
                "runtime io path must be absolute when using container mount contract: {}",
                raw
            ),
            ContractPathMode::RuntimeEvents => anyhow!(
                "runtime event path must be absolute when resolving trial events: {}",
                raw
            ),
        });
    }
    if !raw.starts_with('/') {
        return Err(match mode {
            ContractPathMode::ContainerMount => anyhow!("container path must be absolute: {}", raw),
            ContractPathMode::RuntimeIo => anyhow!(
                "runtime io path must be absolute when using container mount contract: {}",
                raw
            ),
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
            ContractPathMode::RuntimeIo => {
                anyhow!(
                    "unsupported runtime io path for non-container trials: {}",
                    raw
                )
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
            ContractPathMode::RuntimeIo => {
                anyhow!(
                    "unsupported runtime io path for non-container trials: {}",
                    raw
                )
            }
            ContractPathMode::RuntimeEvents => {
                anyhow!("unsupported runtime event path for trial: {}", raw)
            }
        });
    }

    Ok(roots.base_for(root).join(rest.trim_start_matches('/')))
}

fn resolve_event_path_for_trial(events_path: &str, trial_dir: &Path) -> Result<PathBuf> {
    map_contract_path_to_host(
        events_path,
        &ContractPathHostRoots::from_trial_dir(trial_dir),
        ContractPathMode::RuntimeEvents,
    )
}

fn read_control_seq(control_path: &Path) -> Result<u64> {
    if !control_path.exists() {
        return Ok(0);
    }
    let value = load_json_file(control_path)?;
    Ok(value.pointer("/seq").and_then(|v| v.as_u64()).unwrap_or(0))
}

fn wait_for_adapter_control_ack(
    events_path: &Path,
    action: &str,
    control_version: &str,
    deadline: Instant,
) -> Result<()> {
    loop {
        if adapter_control_ack_received(events_path, action, control_version)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "control_ack_missing: action={}, control_version={}, events_path={}",
                action,
                control_version,
                events_path.display()
            ));
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn adapter_control_ack_received(
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

fn parse_fork_selector(selector: &str) -> Result<ForkSelector> {
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

fn resolve_selector_checkpoint(
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
        Ok(Some(resolved.to_string_lossy().to_string()))
    } else {
        Ok(None)
    }
}

fn apply_binding_overrides(
    input: &mut Value,
    set_bindings: &BTreeMap<String, Value>,
) -> Result<()> {
    if set_bindings.is_empty() {
        return Ok(());
    }
    if input.pointer("/bindings").is_none() {
        set_json_pointer_value(input, "/bindings", json!({}))?;
    }
    for (key, value) in set_bindings {
        let pointer = format!("/bindings/{}", key.split('.').collect::<Vec<_>>().join("/"));
        set_json_pointer_value(input, &pointer, value.clone())?;
    }
    Ok(())
}

fn experiment_version_string(json_value: &Value) -> Option<String> {
    match json_value.pointer("/version") {
        Some(Value::String(raw)) => Some(raw.trim().to_string()),
        Some(Value::Number(raw)) => Some(raw.to_string()),
        _ => None,
    }
}

fn is_clean_contract_experiment(json_value: &Value) -> bool {
    experiment_version_string(json_value).as_deref() == Some("1.0")
}

fn experiment_workload_type(json_value: &Value) -> Result<String> {
    if let Some(value) = json_value
        .pointer("/experiment/workload_type")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Ok(value.to_string());
    }
    if is_clean_contract_experiment(json_value) {
        return Ok("agent_runtime".to_string());
    }
    Err(anyhow!("missing /experiment/workload_type"))
}

fn experiment_random_seed(json_value: &Value) -> u64 {
    if is_clean_contract_experiment(json_value) {
        return json_value
            .pointer("/design/seed")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);
    }
    json_value
        .pointer("/design/random_seed")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
}

fn experiment_max_concurrency(json_value: &Value) -> usize {
    let raw = json_value
        .pointer("/design/max_concurrency")
        .and_then(|v| v.as_u64())
        .unwrap_or(1);
    (raw.max(1)).min(usize::MAX as u64) as usize
}

fn configured_network_mode(json_value: &Value) -> Result<String> {
    if is_clean_contract_experiment(json_value) {
        return Ok(json_value
            .pointer("/runtime/network")
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string());
    }
    json_value
        .pointer("/runtime/policy/network/mode")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
        .ok_or_else(|| anyhow!("missing /runtime/policy/network/mode"))
}

fn validate_required_fields(json_value: &Value) -> Result<()> {
    if is_clean_contract_experiment(json_value) {
        let required_v1: &[&str] = &[
            "/experiment/id",
            "/experiment/name",
            "/dataset/path",
            "/design/replications",
            "/baseline/variant_id",
            "/runtime/image",
            "/runtime/command",
        ];
        let mut missing = Vec::new();
        for pointer in required_v1 {
            let value = json_value.pointer(pointer);
            let is_missing = match value {
                None => true,
                Some(Value::String(s)) => s.trim().is_empty(),
                Some(Value::Array(items)) if *pointer == "/runtime/command" => items.is_empty(),
                Some(Value::Number(n)) if *pointer == "/design/replications" => {
                    n.as_u64() == Some(0)
                }
                _ => false,
            };
            if is_missing {
                missing.push(*pointer);
            }
        }

        let has_command = match json_value.pointer("/runtime/command") {
            Some(Value::String(s)) => !s.trim().is_empty(),
            Some(Value::Array(parts)) if !parts.is_empty() => parts
                .iter()
                .all(|part| part.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false)),
            _ => false,
        };
        if !has_command {
            missing.push("/runtime/command");
        }
        if missing.is_empty() {
            return Ok(());
        }
        let lines = missing
            .iter()
            .map(|p| format!("  - {}", p))
            .collect::<Vec<_>>();
        return Err(anyhow!(
            "experiment.yaml missing required fields:\n{}",
            lines.join("\n")
        ));
    }

    let required: &[&str] = &[
        "/experiment/workload_type",
        "/design/sanitization_profile",
        "/design/replications",
        "/runtime/policy/timeout_ms",
        "/runtime/policy/network/mode",
        "/baseline/variant_id",
    ];
    let mut missing = Vec::new();
    for pointer in required {
        let value = json_value.pointer(pointer);
        let is_missing = match value {
            None => true,
            Some(Value::String(s)) => s.is_empty(),
            Some(Value::Number(n)) => {
                n.as_u64() == Some(0)
                    && (*pointer == "/design/replications"
                        || *pointer == "/runtime/policy/timeout_ms")
            }
            _ => false,
        };
        if is_missing {
            missing.push(*pointer);
        }
    }
    if json_value.pointer("/runtime/agent").is_none() {
        missing.push("/runtime/agent");
    }
    let has_command = match json_value.pointer("/runtime/agent/command") {
        Some(Value::String(s)) => !s.trim().is_empty(),
        Some(Value::Array(parts)) if !parts.is_empty() => parts
            .iter()
            .all(|part| part.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false)),
        _ => false,
    };
    if !has_command {
        missing.push("/runtime/agent/command");
    }

    let mut invalid = Vec::new();
    if json_value.pointer("/runtime/agent/mode").is_some() {
        invalid
            .push("/runtime/agent/mode (removed; use runtime.agent.command + runtime.agent.image)");
    }
    if json_value
        .pointer("/runtime/agent/known_agent_ref")
        .is_some()
    {
        invalid.push(
            "/runtime/agent/known_agent_ref (removed; ship built runtime in container image)",
        );
    }
    if json_value.pointer("/runtime/agent/custom_image").is_some() {
        invalid.push("/runtime/agent/custom_image (removed; use runtime.agent.image)");
    }
    if json_value.pointer("/runtime/agent/adapter").is_some() {
        invalid.push("/runtime/agent/adapter (removed from user-facing spec)");
    }
    if json_value.pointer("/runtime/agent/aliases").is_some() {
        invalid.push("/runtime/agent/aliases (removed from user-facing spec)");
    }
    if json_value.pointer("/runtime/agent/overrides").is_some() {
        invalid.push("/runtime/agent/overrides (removed; package runtime concerns in the image)");
    }

    let sandbox_mode = json_value
        .pointer("/runtime/policy/sandbox/mode")
        .and_then(|v| v.as_str())
        .unwrap_or("local");
    let image_source = json_value
        .pointer("/runtime/agent/image_source")
        .and_then(|v| v.as_str())
        .unwrap_or("global");
    if image_source != "global" && image_source != "per_task" {
        invalid.push("/runtime/agent/image_source (must be 'global' or 'per_task')");
    }
    if sandbox_mode == "container" {
        let runtime_agent_image = json_value
            .pointer("/runtime/agent/image")
            .and_then(|v| v.as_str())
            .map(|v| v.trim().to_string())
            .unwrap_or_default();
        let has_container_image = !runtime_agent_image.is_empty();
        if image_source != "per_task" && !has_container_image {
            missing.push("/runtime/agent/image");
        }
        if image_source == "per_task" {
            let agent_artifact = json_value
                .pointer("/runtime/agent/artifact")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .unwrap_or("");
            if agent_artifact.is_empty() {
                missing.push("/runtime/agent/artifact");
            }
        }
    }
    if json_value.pointer("/benchmark").is_some() {
        let has_adapter_command = match json_value.pointer("/benchmark/adapter/command") {
            Some(Value::Array(parts)) if !parts.is_empty() => parts
                .iter()
                .all(|part| part.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false)),
            _ => false,
        };
        if !has_adapter_command {
            missing.push("/benchmark/adapter/command");
        }
    }
    if missing.is_empty() && invalid.is_empty() {
        Ok(())
    } else {
        let mut lines = missing
            .iter()
            .map(|p| format!("  - {}", p))
            .collect::<Vec<_>>();
        lines.extend(invalid.iter().map(|p| format!("  - {}", p)));
        Err(anyhow!(
            "experiment.yaml missing required fields:\n{}",
            lines.join("\n")
        ))
    }
}

fn stage_benchmark_trial_preflight(
    benchmark_config: &BenchmarkConfig,
    trial_dir: &Path,
    run_id: &str,
    trial_id: &str,
    schedule_idx: usize,
    variant_id: &str,
    task_payload: &Value,
    trial_input_path: &Path,
) -> Result<()> {
    if benchmark_config.adapter.is_none() {
        return Ok(());
    }

    let task_id = task_payload
        .get("id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("benchmark preflight: task payload missing non-empty id"))?;
    let task_image = task_payload
        .get("image")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    if task_payload.get("image").is_some() && task_image.is_none() {
        return Err(anyhow!(
            "benchmark preflight: task image must be a non-empty string when provided"
        ));
    }
    let grading_enabled = task_payload
        .pointer("/grading/enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

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
        "task_image": task_image,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleEngineMode {
    FreshRun,
    ContinueRun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrialExecutionResult {
    trial_id: String,
    slot_status: String,
    #[serde(default)]
    variant_idx: Option<usize>,
    #[serde(default)]
    deferred_trial_records: Vec<TrialRecord>,
    #[serde(default)]
    deferred_metric_rows: Vec<MetricRow>,
    #[serde(default)]
    deferred_event_rows: Vec<EventRow>,
    #[serde(default)]
    deferred_variant_snapshot_rows: Vec<VariantSnapshotRow>,
    #[serde(default)]
    deferred_evidence_records: Vec<Value>,
    #[serde(default)]
    deferred_chain_state_records: Vec<Value>,
    #[serde(default)]
    deferred_benchmark_prediction_records: Vec<Value>,
    #[serde(default)]
    deferred_benchmark_score_records: Vec<Value>,
    #[serde(default)]
    failure_classification: Option<String>,
}

impl TrialExecutionResult {
    fn minimal(trial_id: String, slot_status: &str, variant_idx: Option<usize>) -> Self {
        Self {
            trial_id,
            slot_status: slot_status.to_string(),
            variant_idx,
            deferred_trial_records: Vec::new(),
            deferred_metric_rows: Vec::new(),
            deferred_event_rows: Vec::new(),
            deferred_variant_snapshot_rows: Vec::new(),
            deferred_evidence_records: Vec::new(),
            deferred_chain_state_records: Vec::new(),
            deferred_benchmark_prediction_records: Vec::new(),
            deferred_benchmark_score_records: Vec::new(),
            failure_classification: None,
        }
    }

    fn worker_lost(
        trial_id: String,
        variant_idx: Option<usize>,
        classification: Option<String>,
    ) -> Self {
        let mut result = Self::minimal(trial_id, "failed", variant_idx);
        result.failure_classification = classification;
        result
    }
}

#[derive(Default)]
struct BufferedRunSink {
    trial_records: Vec<TrialRecord>,
    metric_rows: Vec<MetricRow>,
    event_rows: Vec<EventRow>,
    variant_snapshot_rows: Vec<VariantSnapshotRow>,
}

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
    let raw = fs::read_to_string(path)?;
    let mut rows = Vec::new();
    for line in raw.lines() {
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
        dataset_path: &Path,
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
        evidence_dir: &Path,
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
        let invocation_default_timeout_ms = variant_runtime.invocation_default_timeout_ms;
        let executor_kind = variant_runtime.executor_kind;
        let container_mode = variant_runtime.container_mode;
        let configured_network_mode = variant_runtime.configured_network_mode.as_str();
        let effective_network_mode = variant_runtime.effective_network_mode.as_str();
        let task_idx = slot.task_idx;
        let task = &tasks[task_idx];
        let task_boundary = parse_task_boundary_from_dataset_task(task)?;
        validate_task_boundary_workspace_materialization(&task_boundary, task_boundary_policy)?;
        let repl = slot.repl_idx;
        let task_id = task_boundary
            .task_payload
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("task_{}", task_idx));
        let benchmark_grading_enabled = benchmark_config.adapter.is_some()
            && !is_clean_contract_experiment(trial_experiment)
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

        *trial_index += 1;
        let trial_id = format!("trial_{}", *trial_index);
        let chain_fs_key = sanitize_for_fs(&chain_key);
        let trial_dir = trials_dir.join(&trial_id);
        ensure_dir(&trial_dir)?;
        write_trial_state(&trial_dir, &trial_id, "running", None, None, None)?;
        let mut trial_guard = TrialStateGuard::new(&trial_dir, &trial_id);

        let trial_paths = TrialPaths::new(&trial_dir, project_root, dataset_path)?;
        trial_paths.prepare(false)?;
        stage_dependencies_for_trial(agent_runtime, &trial_paths)?;
        if !matches!(effective_policy.state_policy, StatePolicy::IsolatePerTrial) {
            if let Some(chain_state) = chain_states.get(&chain_key) {
                restore_workspace_from_snapshot(
                    &chain_state.latest_snapshot_path,
                    &trial_paths.workspace,
                )?;
            }
        }

        materialize_workspace_files(&trial_paths, &task_boundary.workspace_files)?;
        let dynamic_mounts = resolve_task_mounts(
            project_root,
            &task_boundary.mount_references,
            container_mode,
        )?;

        let input = build_agent_task(
            trial_experiment,
            run_id,
            &trial_id,
            variant,
            task_idx,
            repl,
            &task_boundary,
            agent_runtime,
        );
        let input_bytes = serde_json::to_vec_pretty(&input)?;
        let canonical_input_path = trial_dir.join("trial_input.json");
        atomic_write_bytes(&canonical_input_path, &input_bytes)?;
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
                "container_mode": container_mode,
                "integration_level": agent_runtime.integration_level.as_str(),
                "network_mode_requested": configured_network_mode,
                "network_mode_effective": effective_network_mode
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
        stage_benchmark_trial_preflight(
            benchmark_config,
            &trial_dir,
            run_id,
            &trial_id,
            schedule_idx,
            &variant.id,
            &task_boundary.task_payload,
            &canonical_input_path,
        )?;

        let io_paths = prepare_io_paths(
            &trial_paths,
            container_mode,
            &input_bytes,
            is_clean_contract_experiment(trial_experiment),
        )?;
        let runtime_env = build_runtime_contract_env(
            run_id,
            &input,
            &io_paths,
            resolve_trial_timeout_ms(&input, invocation_default_timeout_ms),
            trial_experiment
                .pointer("/version")
                .and_then(|v| v.as_str())
                == Some("1.0"),
        );
        let benchmark_prediction_path = trial_paths.out.join(BENCHMARK_PREDICTION_FILENAME);
        let benchmark_score_path = trial_paths.out.join(BENCHMARK_SCORE_FILENAME);
        let benchmark_grade_error_path = trial_paths.out.join(BENCHMARK_GRADE_ERROR_FILENAME);
        let adapter = adapter_registry_entry(&agent_runtime.adapter_ref)?;
        let trial_evidence_dir = trial_dir.join("evidence");
        ensure_dir(&trial_evidence_dir)?;
        let chains_dir = evidence_dir.join("chains").join(&chain_fs_key);
        ensure_dir(&chains_dir)?;

        let pre_snapshot_manifest = collect_workspace_snapshot_manifest(&trial_paths.workspace)?;
        let pre_snapshot_path = trial_evidence_dir.join("workspace_pre_snapshot.json");
        atomic_write_json_pretty(&pre_snapshot_path, &pre_snapshot_manifest)?;
        let pre_snapshot_ref = artifact_store.put_file(&pre_snapshot_path)?;

        let (chain_root_snapshot_ref, chain_root_snapshot_path) = if let Some(existing) =
            chain_states.get(&chain_key)
        {
            (
                existing.chain_root_snapshot_ref.clone(),
                existing.chain_root_snapshot_path.clone(),
            )
        } else {
            let root_workspace = chains_dir.join(chain_root_workspace_dir_name(trial_id.as_str()));
            if root_workspace.exists() {
                fs::remove_dir_all(&root_workspace)?;
            }
            ensure_dir(&root_workspace)?;
            copy_dir_filtered(
                &trial_paths.workspace,
                &root_workspace,
                WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES,
            )?;
            (pre_snapshot_ref.clone(), root_workspace)
        };

        let mut status = String::new();
        let mut trial_output: Value =
            json!({"schema_version": "agent_result_v1", "outcome": "error"});
        let trial_started_at = Instant::now();
        for attempt in 0..policy_config.retry_max_attempts {
            let mut otel_receiver = None;
            let mut otel_manifest = None;
            if agent_runtime.tracing_mode == Some("otlp".to_string()) {
                if container_mode
                    && trial_experiment
                        .pointer("/runtime/policy/network/mode")
                        .and_then(|v| v.as_str())
                        == Some("none")
                {
                    otel_manifest = Some(json!({
                        "schema_version": "trace_manifest_v1",
                        "mode": "none",
                        "reason": "network_none",
                    }));
                } else {
                    let receiver = lab_otel::OtlpReceiver::start(
                        4318,
                        ArtifactStore::new(trial_dir.join("artifacts")),
                    )?;
                    let endpoint = receiver.endpoint.clone();
                    otel_receiver = Some(receiver);
                    otel_manifest = Some(json!({
                        "schema_version": "trace_manifest_v1",
                        "mode": "otlp",
                        "endpoint": endpoint,
                    }));
                }
            }

            let setup_command = match mode {
                ScheduleEngineMode::FreshRun => behavior.setup_command.as_deref(),
                ScheduleEngineMode::ContinueRun => None,
            };
            let _ = fs::remove_file(&benchmark_prediction_path);
            let _ = fs::remove_file(&benchmark_score_path);
            let _ = fs::remove_file(&benchmark_grade_error_path);

            let run_request = AdapterRunRequest {
                runtime_experiment: trial_experiment,
                runtime: agent_runtime,
                variant_args: &variant_runtime.variant_args,
                runtime_env: &runtime_env,
                runtime_overrides_env: agent_runtime_env,
                container_mode: matches!(executor_kind, ExecutorKind::LocalDocker),
                trial_paths: &trial_paths,
                dynamic_mounts: &dynamic_mounts,
                io_paths: &io_paths,
                network_mode: effective_network_mode,
                setup_command,
                benchmark_adapter: benchmark_config.adapter.as_ref(),
                benchmark_grading_enabled,
                run_id,
                task_image: task_boundary.task_image.as_deref(),
                task_workspace: task_boundary.task_workspace.as_deref(),
                agent_artifact: agent_runtime.agent_artifact.as_deref(),
            };
            let proc_result = adapter.run_trial(&run_request)?;
            status = proc_result.status;
            atomic_write_bytes(
                &trial_dir.join("harness_stdout.log"),
                proc_result.stdout.as_bytes(),
            )?;
            atomic_write_bytes(
                &trial_dir.join("harness_stderr.log"),
                proc_result.stderr.as_bytes(),
            )?;

            if let Some(receiver) = otel_receiver {
                let records = receiver.records();
                receiver.stop();
                if let Some(mut manifest) = otel_manifest {
                    if let Some(obj) = manifest.as_object_mut() {
                        obj.insert("records".to_string(), serde_json::to_value(records)?);
                    }
                    let path = trial_dir.join("trace_manifest.json");
                    atomic_write_json_pretty(&path, &manifest)?;
                }
            }

            materialize_trial_result(&trial_dir, &io_paths.output_host)?;
            let canonical_output = trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME);
            trial_output = if canonical_output.exists() {
                serde_json::from_slice(&fs::read(&canonical_output)?)?
            } else {
                json!({"schema_version": "agent_result_v1", "outcome": "error"})
            };

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
                    grade_error_reason = Some(format!(
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
            if grade_error_reason.is_none()
                && status == BENCHMARK_GRADING_POLICY_EXIT_CODE.to_string()
            {
                grade_error_reason = Some("grading_policy_exit".to_string());
            }
        }

        let post_snapshot_manifest = collect_workspace_snapshot_manifest(&trial_paths.workspace)?;
        let post_snapshot_path = trial_evidence_dir.join("workspace_post_snapshot.json");
        atomic_write_json_pretty(&post_snapshot_path, &post_snapshot_manifest)?;
        let post_snapshot_ref = artifact_store.put_file(&post_snapshot_path)?;

        let chain_root_snapshot_manifest =
            collect_workspace_snapshot_manifest(&chain_root_snapshot_path)?;
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

        let post_workspace_snapshot_dir = chains_dir.join(format!(
            "step_{:06}_{}_workspace",
            chain_step_index,
            sanitize_for_fs(&trial_id)
        ));
        if post_workspace_snapshot_dir.exists() {
            fs::remove_dir_all(&post_workspace_snapshot_dir)?;
        }
        ensure_dir(&post_workspace_snapshot_dir)?;
        copy_dir_filtered(
            &trial_paths.workspace,
            &post_workspace_snapshot_dir,
            WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES,
        )?;

        if !matches!(effective_policy.state_policy, StatePolicy::IsolatePerTrial) {
            chain_states.insert(
                chain_key.clone(),
                ChainRuntimeState {
                    chain_root_snapshot_ref: chain_root_snapshot_ref.clone(),
                    chain_root_snapshot_path: chain_root_snapshot_path.clone(),
                    latest_snapshot_ref: post_snapshot_ref.clone(),
                    latest_snapshot_path: post_workspace_snapshot_dir.clone(),
                    step_index: chain_step_index,
                },
            );
        }

        let canonical_output = trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME);
        let trial_input_ref = artifact_store.put_file(&canonical_input_path)?;
        let trial_output_ref = artifact_store.put_file(&canonical_output)?;

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
                "executor": executor_kind.as_str(),
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
                "patch_cumulative_ref": patch_cumulative_ref.clone()
            },
            "paths": {
                "trial_dir": rel_to_run_dir(&trial_dir, run_dir),
                "trial_input": rel_to_run_dir(&canonical_input_path, run_dir),
                "trial_output": rel_to_run_dir(&canonical_output, run_dir),
                "stdout": rel_to_run_dir(&stdout_path, run_dir),
                "stderr": rel_to_run_dir(&stderr_path, run_dir),
                "hook_events": hook_events_path.as_ref().map(|p| rel_to_run_dir(p, run_dir)),
                "workspace_pre_snapshot": rel_to_run_dir(&pre_snapshot_path, run_dir),
                "workspace_post_snapshot": rel_to_run_dir(&post_snapshot_path, run_dir),
                "diff_incremental": rel_to_run_dir(&diff_incremental_path, run_dir),
                "diff_cumulative": rel_to_run_dir(&diff_cumulative_path, run_dir),
                "patch_incremental": rel_to_run_dir(&patch_incremental_path, run_dir),
                "patch_cumulative": rel_to_run_dir(&patch_cumulative_path, run_dir)
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
        if hook_events_path.is_none() {
            if let Some(paths_obj) = evidence_record
                .get_mut("paths")
                .and_then(Value::as_object_mut)
            {
                paths_obj.remove("hook_events");
            }
        }

        validate_required_evidence_classes(
            &evidence_record,
            &effective_policy.required_evidence_classes,
        )?;
        append_jsonl(evidence_records_path, &evidence_record)?;

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
            "ext": {
                "chain_fs_key": chain_fs_key,
                "latest_snapshot_ref": chain_states
                    .get(&chain_key)
                    .map(|state| state.latest_snapshot_ref.clone())
            }
        });
        append_jsonl(task_chain_states_path, &chain_state_record)?;

        write_state_inventory(
            &trial_dir,
            trial_experiment,
            agent_runtime,
            container_mode,
            &trial_paths,
            &resolve_exec_digest(&agent_runtime.command_raw, project_root)?,
            effective_network_mode,
            invocation_source.as_str(),
        )?;

        let manifest_path = resolve_agent_runtime_manifest_path(&trial_paths, container_mode)?;
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
            &variant.id,
            baseline_id,
            &task_id,
            repl,
            &bindings,
        );
        run_sink.append_trial_record(&TrialRecord {
            run_id: run_id.to_string(),
            trial_id: trial_id.clone(),
            baseline_id: baseline_id.to_string(),
            workload_type: workload_type.to_string(),
            variant_id: variant.id.clone(),
            task_index: task_idx,
            task_id: task_id.clone(),
            repl_idx: repl,
            outcome: outcome.clone(),
            success: outcome == "success" && grade_error_reason.is_none(),
            status_code: status.clone(),
            container_mode,
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
        } else if status == "0" && outcome != "error" {
            trial_guard.complete("completed", None)?;
            None
        } else if status != "0" {
            trial_guard.complete("failed", Some("agent_exit_nonzero"))?;
            Some("agent_exit_nonzero".to_string())
        } else {
            trial_guard.complete("failed", Some("result_error"))?;
            Some("result_error".to_string())
        };

        apply_materialization_policy(&trial_dir, materialize_mode)?;

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

impl RunCoordinator {
    fn commit_skipped_pruned_slot(
        run_dir: &Path,
        schedule_progress: &mut ScheduleProgress,
        schedule_idx: usize,
        run_sink: &mut dyn RunSink,
    ) -> Result<()> {
        run_sink.flush()?;

        let mut next_progress = schedule_progress.clone();
        next_progress.completed_slots.push(SlotCompletion {
            schedule_index: schedule_idx,
            trial_id: String::new(),
            status: "skipped_pruned".to_string(),
        });
        next_progress.next_schedule_index = schedule_idx + 1;
        next_progress.updated_at = Utc::now().to_rfc3339();
        write_schedule_progress(run_dir, &next_progress)?;
        *schedule_progress = next_progress;
        Ok(())
    }

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
    ) -> Result<()> {
        for record in &trial_result.deferred_evidence_records {
            append_jsonl(evidence_records_path, record)?;
        }
        for record in &trial_result.deferred_chain_state_records {
            append_jsonl(task_chain_states_path, record)?;
        }
        for row in &trial_result.deferred_benchmark_prediction_records {
            append_jsonl(benchmark_predictions_path, row)?;
        }
        for row in &trial_result.deferred_benchmark_score_records {
            append_jsonl(benchmark_scores_path, row)?;
        }
        for row in &trial_result.deferred_trial_records {
            run_sink.append_trial_record(row)?;
        }
        run_sink.append_metric_rows(&trial_result.deferred_metric_rows)?;
        run_sink.append_event_rows(&trial_result.deferred_event_rows)?;
        run_sink.append_variant_snapshot(&trial_result.deferred_variant_snapshot_rows)?;
        run_sink.flush()?;

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
        });
        next_progress.next_schedule_index = schedule_idx + 1;
        next_progress.next_trial_index = trial_index;
        next_progress.pruned_variants = next_pruned_variants.iter().copied().collect();
        next_progress.consecutive_failures = next_consecutive_failures.clone();
        next_progress.updated_at = Utc::now().to_rfc3339();
        write_schedule_progress(run_dir, &next_progress)?;

        *schedule_progress = next_progress;
        *consecutive_failures = next_consecutive_failures;
        *pruned_variants = next_pruned_variants;
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum PendingSlotCommit {
    SkippedPruned,
    Trial(TrialExecutionResult),
}

struct DeterministicCommitter {
    next_commit_idx: usize,
    committed_keys: HashSet<String>,
    pending_by_schedule: BTreeMap<usize, PendingSlotCommit>,
}

impl DeterministicCommitter {
    fn from_progress(progress: &ScheduleProgress) -> Self {
        let mut committed_keys = HashSet::new();
        for slot in &progress.completed_slots {
            committed_keys.insert(Self::commit_key_for_slot_completion(slot));
        }
        Self {
            next_commit_idx: progress.next_schedule_index,
            committed_keys,
            pending_by_schedule: BTreeMap::new(),
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
        self.enqueue(schedule_idx, PendingSlotCommit::Trial(result))
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

    let _ = fs::remove_dir_all(&payload_dir);

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
    remote_endpoint: Option<&str>,
    remote_token_env: Option<&str>,
) -> Result<ScheduleEngineOutcome> {
    let benchmark_dir = run_dir.join("benchmark");
    let benchmark_predictions_path = benchmark_dir.join("predictions.jsonl");
    let benchmark_scores_path = benchmark_dir.join("scores.jsonl");

    let any_remote_executor = variant_runtime_profiles
        .iter()
        .any(|profile| matches!(profile.executor_kind, ExecutorKind::Remote));
    let all_remote_executor = any_remote_executor
        && variant_runtime_profiles
            .iter()
            .all(|profile| matches!(profile.executor_kind, ExecutorKind::Remote));
    if any_remote_executor && !all_remote_executor {
        return Err(anyhow!(
            "parallel worker engine does not support mixed local and remote executor kinds in one run"
        ));
    }
    let requested_dispatch_capacity = max_concurrency.max(1);
    let mut dispatch_capacity = requested_dispatch_capacity;
    let backend: Box<dyn WorkerBackend> = if all_remote_executor {
        let endpoint = remote_endpoint
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("remote executor requires --remote-endpoint"))?;
        let bearer_token = resolve_remote_bearer_token(remote_token_env)?;
        let protocol = Arc::new(HttpRemoteWorkerProtocol::new(endpoint, bearer_token)?);
        Box::new(RemoteWorkerBackend::new(protocol)?)
    } else {
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
        dispatch_capacity = local_backend.effective_max_in_flight();
        Box::new(local_backend)
    };

    let mut committer = DeterministicCommitter::from_progress(schedule_progress);
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
    write_run_control_v2(
        run_dir,
        run_id,
        "running",
        &in_flight_active_trials(&in_flight),
        None,
    )?;

    while committer.next_commit_idx < schedule.len() || !in_flight.is_empty() {
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
    remote_endpoint: Option<&str>,
    remote_token_env: Option<&str>,
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
        remote_endpoint,
        remote_token_env,
    )
}

fn run_experiment_with_behavior(
    path: &Path,
    use_container: bool,
    behavior: RunBehavior,
    overrides_path: Option<&Path>,
    execution: RunExecutionOptions,
) -> Result<RunResult> {
    let exp_dir = path
        .parent()
        .unwrap_or(Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));
    let project_root = find_project_root(&exp_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&exp_dir));
    let raw_yaml = fs::read_to_string(path)?;
    let yaml_value: serde_yaml::Value = serde_yaml::from_str(&raw_yaml)?;
    let mut json_value: Value = serde_json::to_value(yaml_value)?;
    if let Some(overrides_path) = overrides_path {
        json_value = apply_experiment_overrides(json_value, overrides_path, &project_root)?;
    }
    validate_required_fields(&json_value)?;
    let workload_type = experiment_workload_type(&json_value)?;

    let execution = normalize_execution_options(&execution);
    let materialize_mode = execution.materialize.unwrap_or(MaterializationMode::Full);

    let run_id = format!("run_{}", Utc::now().format("%Y%m%d_%H%M%S"));
    let run_dir = project_root.join(".lab").join("runs").join(&run_id);
    ensure_dir(&run_dir)?;
    write_run_control_v2(&run_dir, &run_id, "running", &[], None)?;
    write_run_session_state(&run_dir, &run_id, &behavior, &execution)?;
    let mut run_guard = RunControlGuard::new(&run_dir, &run_id);

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

    let dataset_path = resolve_dataset_path(&json_value, &exp_dir)?;
    let tasks = load_tasks(&dataset_path, &json_value)?;

    let (variants, baseline_id) = resolve_variant_plan(&json_value)?;
    write_resolved_variants(&run_dir, &baseline_id, &variants)?;
    let replications = json_value
        .pointer("/design/replications")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing /design/replications"))? as usize;

    let trials_dir = run_dir.join("trials");
    ensure_dir(&trials_dir)?;

    let evidence_dir = run_dir.join("evidence");
    ensure_dir(&evidence_dir)?;
    let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
    let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
    let benchmark_config = parse_benchmark_config(&json_value);
    let mut variant_runtime_profiles = Vec::with_capacity(variants.len());
    for variant in &variants {
        variant_runtime_profiles.push(resolve_variant_runtime_profile(
            &json_value,
            variant,
            &project_root,
            use_container,
            &behavior,
            &execution,
        )?);
    }
    let run_integration_level = variant_runtime_profiles
        .first()
        .map(|profile| profile.agent_runtime.integration_level.clone())
        .unwrap_or_else(|| "cli_basic".to_string());
    let all_container_mode = variant_runtime_profiles
        .iter()
        .all(|profile| profile.container_mode);

    let mut run_sink = JsonlRunSink::new(&run_dir)?;
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

    // Per-variant consecutive failure tracking (for pruning)
    let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
    let mut pruned_variants: HashSet<usize> = HashSet::new();

    let mut schedule_progress = ScheduleProgress {
        schema_version: "schedule_progress_v1".to_string(),
        run_id: run_id.clone(),
        total_slots: schedule.len(),
        next_schedule_index: 0,
        next_trial_index: 0,
        schedule: schedule.clone(),
        completed_slots: Vec::new(),
        pruned_variants: Vec::new(),
        consecutive_failures: BTreeMap::new(),
        use_container,
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
        execution.remote_endpoint.as_deref(),
        execution.remote_token_env.as_deref(),
    )?;
    run_sink.flush()?;
    if schedule_outcome != ScheduleEngineOutcome::Completed {
        run_guard.disarm();
        return Ok(RunResult { run_dir, run_id });
    }
    validate_jsonl_against_schema("evidence_record_v1.jsonschema", &evidence_records_path)?;
    validate_jsonl_against_schema("task_chain_state_v1.jsonschema", &task_chain_states_path)?;

    if let Some(adapter) = benchmark_config.adapter.as_ref() {
        let _scores_path = process_benchmark_outputs(
            &project_root,
            &run_dir,
            &run_id,
            adapter,
            &evidence_records_path,
            &task_chain_states_path,
        )?;
    }

    let grades = json!({
        "schema_version": "grades_v1",
        "integration_level": run_integration_level,
        "replay_grade": "best_effort",
        "isolation_grade": if all_container_mode {"bounded"} else {"leaky"},
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

    Ok(RunResult { run_dir, run_id })
}

pub fn describe_experiment(path: &Path) -> Result<ExperimentSummary> {
    describe_experiment_with_overrides(path, None)
}

pub fn describe_experiment_with_overrides(
    path: &Path,
    overrides_path: Option<&Path>,
) -> Result<ExperimentSummary> {
    let exp_dir = path
        .parent()
        .unwrap_or(Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));
    let project_root = find_project_root(&exp_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&exp_dir));
    let raw_yaml = fs::read_to_string(path)?;
    let yaml_value: serde_yaml::Value = serde_yaml::from_str(&raw_yaml)?;
    let mut json_value: Value = serde_json::to_value(yaml_value)?;
    if let Some(overrides_path) = overrides_path {
        json_value = apply_experiment_overrides(json_value, overrides_path, &project_root)?;
    }
    validate_required_fields(&json_value)?;

    let dataset_path = resolve_dataset_path(&json_value, &exp_dir)?;
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
        &project_root,
        false,
        &RunBehavior::default(),
        &RunExecutionOptions::default(),
    )?;
    let VariantRuntimeProfile {
        experiment: runtime_experiment,
        agent_runtime: runtime_agent,
        container_mode,
        configured_network_mode: network_mode,
        ..
    } = runtime_profile;
    let image = runtime_agent.container_image.clone().or_else(|| {
        runtime_experiment
            .pointer("/runtime/policy/sandbox/image")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    });

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

    Ok(ExperimentSummary {
        exp_id,
        workload_type,
        dataset_path,
        task_count,
        replications,
        variant_count,
        total_trials,
        agent_runtime_command: runtime_agent.command_raw,
        container_mode,
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
    })
}

// ---------------------------------------------------------------------------
// Trial scheduling
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchedulingPolicy {
    PairedInterleaved,
    VariantSequential,
    Randomized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatePolicy {
    IsolatePerTrial,
    PersistPerTask,
    Accumulate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskModel {
    Independent,
    Dependent,
}

impl TaskModel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::Dependent => "dependent",
        }
    }
}

#[derive(Debug, Clone)]
struct BenchmarkPolicyConfig {
    task_model: TaskModel,
    scoring_lifecycle: String,
    evaluator_mode: String,
    required_evidence_classes: Vec<String>,
    chain_failure_policy: String,
}

impl Default for BenchmarkPolicyConfig {
    fn default() -> Self {
        Self {
            task_model: TaskModel::Independent,
            scoring_lifecycle: "predict_then_score".to_string(),
            evaluator_mode: "custom".to_string(),
            required_evidence_classes: Vec::new(),
            chain_failure_policy: "continue_with_flag".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct BenchmarkAdapterConfig {
    command: Vec<String>,
    manifest: Option<Value>,
}

#[derive(Debug, Clone, Default)]
struct BenchmarkConfig {
    policy: BenchmarkPolicyConfig,
    adapter: Option<BenchmarkAdapterConfig>,
}

#[derive(Debug, Clone)]
struct EffectiveTaskPolicy {
    state_policy: StatePolicy,
    task_model: TaskModel,
    scoring_lifecycle: String,
    required_evidence_classes: Vec<String>,
    chain_failure_policy: String,
}

#[derive(Debug, Clone)]
struct ChainRuntimeState {
    chain_root_snapshot_ref: String,
    chain_root_snapshot_path: PathBuf,
    latest_snapshot_ref: String,
    latest_snapshot_path: PathBuf,
    step_index: usize,
}

#[derive(Debug, Clone)]
struct TaskBoundaryPolicy {
    require_workspace_materialization: bool,
}

impl Default for TaskBoundaryPolicy {
    fn default() -> Self {
        Self {
            require_workspace_materialization: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConcurrencyPolicyConfig {
    max_in_flight_per_variant: Option<usize>,
    require_chain_lease: bool,
}

impl Default for ConcurrencyPolicyConfig {
    fn default() -> Self {
        Self {
            max_in_flight_per_variant: None,
            require_chain_lease: true,
        }
    }
}

#[derive(Debug, Clone)]
struct PolicyConfig {
    scheduling: SchedulingPolicy,
    state: StatePolicy,
    retry_max_attempts: usize,
    retry_on: Vec<String>,
    pruning_max_consecutive_failures: Option<usize>,
    task_boundary: TaskBoundaryPolicy,
    concurrency: ConcurrencyPolicyConfig,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            scheduling: SchedulingPolicy::VariantSequential,
            state: StatePolicy::IsolatePerTrial,
            retry_max_attempts: 1,
            retry_on: vec![],
            pruning_max_consecutive_failures: None,
            task_boundary: TaskBoundaryPolicy::default(),
            concurrency: ConcurrencyPolicyConfig::default(),
        }
    }
}

fn parse_policies(json_value: &Value) -> PolicyConfig {
    let policies = json_value.pointer("/design/policies");
    let Some(p) = policies else {
        return PolicyConfig::default();
    };

    let scheduling = match p.pointer("/scheduling").and_then(|v| v.as_str()) {
        Some("paired_interleaved") => SchedulingPolicy::PairedInterleaved,
        Some("randomized") => SchedulingPolicy::Randomized,
        _ => SchedulingPolicy::VariantSequential,
    };
    let state = parse_state_policy_value(p.pointer("/state").and_then(|v| v.as_str()))
        .unwrap_or(StatePolicy::IsolatePerTrial);
    let retry_max_attempts = p
        .pointer("/retry/max_attempts")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;
    let retry_on = p
        .pointer("/retry/retry_on")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let pruning_max_consecutive_failures = p
        .pointer("/pruning/max_consecutive_failures")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let require_workspace_materialization = p
        .pointer("/task_boundary/require_workspace_materialization")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let max_in_flight_per_variant = p
        .pointer("/concurrency/max_in_flight_per_variant")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let require_chain_lease = p
        .pointer("/concurrency/require_chain_lease")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    PolicyConfig {
        scheduling,
        state,
        retry_max_attempts,
        retry_on,
        pruning_max_consecutive_failures,
        task_boundary: TaskBoundaryPolicy {
            require_workspace_materialization,
        },
        concurrency: ConcurrencyPolicyConfig {
            max_in_flight_per_variant,
            require_chain_lease,
        },
    }
}

fn parse_task_model(value: Option<&str>) -> TaskModel {
    match value {
        Some("dependent") => TaskModel::Dependent,
        _ => TaskModel::Independent,
    }
}

fn parse_state_policy_value(value: Option<&str>) -> Option<StatePolicy> {
    match value {
        Some("isolate_per_trial") => Some(StatePolicy::IsolatePerTrial),
        Some("persist_per_task") => Some(StatePolicy::PersistPerTask),
        Some("accumulate") => Some(StatePolicy::Accumulate),
        _ => None,
    }
}

fn parse_benchmark_config(json_value: &Value) -> BenchmarkConfig {
    let benchmark_root = json_value.pointer("/benchmark");
    let Some(root) = benchmark_root else {
        return BenchmarkConfig::default();
    };

    let policy = root.pointer("/policy");
    let mut policy_config = BenchmarkPolicyConfig::default();
    if let Some(p) = policy {
        policy_config.task_model =
            parse_task_model(p.pointer("/task_model").and_then(|v| v.as_str()));
        if let Some(v) = p.pointer("/scoring_lifecycle").and_then(|v| v.as_str()) {
            policy_config.scoring_lifecycle = v.to_string();
        }
        if let Some(v) = p.pointer("/evaluator_mode").and_then(|v| v.as_str()) {
            policy_config.evaluator_mode = v.to_string();
        }
        if let Some(v) = p.pointer("/chain_failure_policy").and_then(|v| v.as_str()) {
            policy_config.chain_failure_policy = v.to_string();
        }
        if let Some(arr) = p
            .pointer("/required_evidence_classes")
            .and_then(|v| v.as_array())
        {
            policy_config.required_evidence_classes = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
    }

    let adapter = root.pointer("/adapter").and_then(|a| {
        let command = a
            .pointer("/command")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        v.as_str().and_then(|s| {
                            let trimmed = s.trim();
                            if trimmed.is_empty() {
                                None
                            } else {
                                Some(trimmed.to_string())
                            }
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if command.is_empty() {
            return None;
        }
        let manifest = a.pointer("/manifest").cloned();
        Some(BenchmarkAdapterConfig { command, manifest })
    });

    BenchmarkConfig {
        policy: policy_config,
        adapter,
    }
}

fn resolve_effective_task_policy(
    experiment_policy: &PolicyConfig,
    benchmark_policy: &BenchmarkPolicyConfig,
    task_payload: &Value,
) -> EffectiveTaskPolicy {
    let override_obj = task_payload
        .get("policy_override")
        .and_then(|v| v.as_object());

    let state_override = override_obj
        .and_then(|o| o.get("state_policy"))
        .and_then(|v| v.as_str())
        .and_then(|s| parse_state_policy_value(Some(s)));
    let task_model_override = override_obj
        .and_then(|o| o.get("task_model"))
        .and_then(|v| v.as_str())
        .map(|s| parse_task_model(Some(s)));
    let scoring_lifecycle_override = override_obj
        .and_then(|o| o.get("scoring_lifecycle"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let chain_failure_override = override_obj
        .and_then(|o| o.get("chain_failure_policy"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let required_evidence_override = override_obj
        .and_then(|o| o.get("required_evidence_classes"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        });

    EffectiveTaskPolicy {
        state_policy: state_override.unwrap_or(experiment_policy.state),
        task_model: task_model_override.unwrap_or(benchmark_policy.task_model),
        scoring_lifecycle: scoring_lifecycle_override
            .unwrap_or_else(|| benchmark_policy.scoring_lifecycle.clone()),
        required_evidence_classes: required_evidence_override
            .unwrap_or_else(|| benchmark_policy.required_evidence_classes.clone()),
        chain_failure_policy: chain_failure_override
            .unwrap_or_else(|| benchmark_policy.chain_failure_policy.clone()),
    }
}

fn validate_required_evidence_classes(record: &Value, required: &[String]) -> Result<()> {
    if required.is_empty() {
        return Ok(());
    }
    for class_name in required {
        let pointer = format!("/evidence/{}", class_name);
        let value = record.pointer(&pointer);
        let missing = match value {
            None => true,
            Some(Value::Null) => true,
            Some(Value::String(s)) => s.trim().is_empty(),
            _ => false,
        };
        if missing {
            return Err(anyhow!(
                "missing required evidence class '{}'; pointer {}",
                class_name,
                pointer
            ));
        }
    }
    Ok(())
}

fn benchmark_identity_from_manifest(
    manifest: &Value,
) -> Result<(String, String, Option<String>, String)> {
    let adapter_id = manifest
        .pointer("/adapter_id")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("benchmark adapter manifest missing /adapter_id"))?
        .to_string();
    let name = manifest
        .pointer("/benchmark/name")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("benchmark adapter manifest missing /benchmark/name"))?
        .to_string();
    let version = manifest
        .pointer("/benchmark/version")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let split = manifest
        .pointer("/benchmark/split")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("benchmark adapter manifest missing /benchmark/split"))?
        .to_string();
    Ok((adapter_id, name, version, split))
}

fn read_jsonl_records(path: &Path) -> Result<Vec<Value>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read_to_string(path)?;
    let mut rows = Vec::new();
    for line in data.lines() {
        if line.trim().is_empty() {
            continue;
        }
        rows.push(serde_json::from_str::<Value>(line)?);
    }
    Ok(rows)
}

fn validate_json_file_against_schema(schema_name: &str, path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(anyhow!(
            "required artifact missing for schema {}: {}",
            schema_name,
            path.display()
        ));
    }
    let schema = compile_schema(schema_name)?;
    let raw = fs::read_to_string(path)?;
    let value: Value = serde_json::from_str(&raw)?;
    if let Err(errors) = schema.validate(&value) {
        let msgs = errors.map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
        return Err(anyhow!(
            "schema validation failed ({}) {}: {}",
            schema_name,
            path.display(),
            msgs
        ));
    }
    Ok(())
}

fn validate_jsonl_against_schema(schema_name: &str, path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(anyhow!(
            "required artifact missing for schema {}: {}",
            schema_name,
            path.display()
        ));
    }
    let schema = compile_schema(schema_name)?;
    let data = fs::read_to_string(path)?;
    for (idx, line) in data.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .map_err(|e| anyhow!("invalid json line {} in {}: {}", idx + 1, path.display(), e))?;
        match schema.validate(&value) {
            Ok(_) => {}
            Err(errors) => {
                let msgs = errors.map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
                return Err(anyhow!(
                    "schema validation failed ({}) {} line {}: {}",
                    schema_name,
                    path.display(),
                    idx + 1,
                    msgs
                ));
            }
        };
    }
    Ok(())
}

fn build_benchmark_summary(run_id: &str, manifest: &Value, score_rows: &[Value]) -> Result<Value> {
    let (adapter_id, name, version, split) = benchmark_identity_from_manifest(manifest)?;
    let evaluator = manifest
        .pointer("/evaluator")
        .cloned()
        .ok_or_else(|| anyhow!("benchmark adapter manifest missing /evaluator"))?;

    let mut totals = BTreeMap::from([
        ("pass".to_string(), 0usize),
        ("fail".to_string(), 0usize),
        ("missing".to_string(), 0usize),
        ("error".to_string(), 0usize),
    ]);
    let mut by_variant: BTreeMap<String, Vec<&Value>> = BTreeMap::new();

    for row in score_rows {
        let verdict = row
            .pointer("/verdict")
            .and_then(|v| v.as_str())
            .unwrap_or("error")
            .to_string();
        *totals.entry(verdict).or_default() += 1;
        let variant_id = row
            .pointer("/ids/variant_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        by_variant.entry(variant_id).or_default().push(row);
    }

    let mut variants = Vec::new();
    for (variant_id, rows) in by_variant {
        let total = rows.len();
        let pass = rows
            .iter()
            .filter(|r| r.pointer("/verdict").and_then(|v| v.as_str()) == Some("pass"))
            .count();
        let fail = rows
            .iter()
            .filter(|r| r.pointer("/verdict").and_then(|v| v.as_str()) == Some("fail"))
            .count();
        let missing = rows
            .iter()
            .filter(|r| r.pointer("/verdict").and_then(|v| v.as_str()) == Some("missing"))
            .count();
        let error = rows
            .iter()
            .filter(|r| r.pointer("/verdict").and_then(|v| v.as_str()) == Some("error"))
            .count();
        let pass_rate = if total > 0 {
            pass as f64 / total as f64
        } else {
            0.0
        };
        let primary_metric_name = rows
            .iter()
            .find_map(|r| {
                r.pointer("/primary_metric_name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| "resolved".to_string());
        let mut pm_sum = 0.0f64;
        let mut pm_count = 0usize;
        for row in rows {
            if let Some(v) = row
                .pointer("/primary_metric_value")
                .and_then(|v| v.as_f64())
            {
                pm_sum += v;
                pm_count += 1;
            }
        }
        let primary_metric_mean = if pm_count > 0 {
            pm_sum / pm_count as f64
        } else {
            0.0
        };
        variants.push(json!({
            "variant_id": variant_id,
            "total": total,
            "pass": pass,
            "fail": fail,
            "missing": missing,
            "error": error,
            "pass_rate": pass_rate,
            "primary_metric_name": primary_metric_name,
            "primary_metric_mean": primary_metric_mean
        }));
    }

    let mut benchmark = serde_json::Map::new();
    benchmark.insert("adapter_id".to_string(), json!(adapter_id));
    benchmark.insert("name".to_string(), json!(name));
    benchmark.insert("split".to_string(), json!(split));
    if let Some(version) = version {
        benchmark.insert("version".to_string(), json!(version));
    }

    Ok(json!({
        "schema_version": "benchmark_summary_v1",
        "created_at": Utc::now().to_rfc3339(),
        "run_id": run_id,
        "benchmark": Value::Object(benchmark),
        "evaluator": evaluator,
        "totals": {
            "trials": score_rows.len(),
            "pass": totals.get("pass").copied().unwrap_or(0),
            "fail": totals.get("fail").copied().unwrap_or(0),
            "missing": totals.get("missing").copied().unwrap_or(0),
            "error": totals.get("error").copied().unwrap_or(0)
        },
        "variants": variants
    }))
}

fn synthesize_benchmark_manifest_from_scores(score_rows: &[Value]) -> Option<Value> {
    let first = score_rows.first()?;
    let adapter_id = first
        .pointer("/benchmark/adapter_id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())?;
    let benchmark_name = first
        .pointer("/benchmark/name")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())?;
    let benchmark_split = first
        .pointer("/benchmark/split")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())?;
    let benchmark_version = first
        .pointer("/benchmark/version")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let evaluator = first.pointer("/evaluator").cloned().unwrap_or_else(|| {
        json!({
            "name": "unknown",
            "mode": "custom"
        })
    });

    let mut benchmark = serde_json::Map::new();
    benchmark.insert("name".to_string(), json!(benchmark_name));
    benchmark.insert("split".to_string(), json!(benchmark_split));
    if let Some(version) = benchmark_version {
        benchmark.insert("version".to_string(), json!(version));
    }

    Some(json!({
        "schema_version": "benchmark_adapter_manifest_v1",
        "adapter_id": adapter_id,
        "adapter_version": "unknown",
        "benchmark": Value::Object(benchmark),
        "execution_mode": "integrated_score",
        "record_schemas": {
            "prediction": "benchmark_prediction_record_v1",
            "score": "benchmark_score_record_v1"
        },
        "evaluator": evaluator
    }))
}

fn default_benchmark_manifest(adapter: &BenchmarkAdapterConfig, score_rows: &[Value]) -> Value {
    if let Some(manifest) = adapter.manifest.clone() {
        return manifest;
    }
    if let Some(manifest) = synthesize_benchmark_manifest_from_scores(score_rows) {
        return manifest;
    }
    let fallback_adapter_id = adapter
        .command
        .first()
        .map(|s| s.as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("benchmark_adapter");
    json!({
        "schema_version": "benchmark_adapter_manifest_v1",
        "adapter_id": fallback_adapter_id,
        "adapter_version": "unknown",
        "benchmark": {
            "name": "unknown",
            "split": "unknown"
        },
        "execution_mode": "integrated_score",
        "record_schemas": {
            "prediction": "benchmark_prediction_record_v1",
            "score": "benchmark_score_record_v1"
        },
        "evaluator": {
            "name": "unknown",
            "mode": "custom"
        }
    })
}

fn process_benchmark_outputs(
    _project_root: &Path,
    run_dir: &Path,
    run_id: &str,
    adapter: &BenchmarkAdapterConfig,
    _evidence_records_path: &Path,
    _task_chain_states_path: &Path,
) -> Result<PathBuf> {
    let benchmark_dir = run_dir.join("benchmark");
    ensure_dir(&benchmark_dir)?;
    let manifest_path = benchmark_dir.join("adapter_manifest.json");
    let predictions_path = benchmark_dir.join("predictions.jsonl");
    let scores_path = benchmark_dir.join("scores.jsonl");
    let summary_path = benchmark_dir.join("summary.json");

    if !predictions_path.exists() {
        atomic_write_bytes(&predictions_path, b"")?;
    }
    if !scores_path.exists() {
        atomic_write_bytes(&scores_path, b"")?;
    }

    validate_jsonl_against_schema(
        "benchmark_prediction_record_v1.jsonschema",
        &predictions_path,
    )?;
    validate_jsonl_against_schema("benchmark_score_record_v1.jsonschema", &scores_path)?;

    let scores = read_jsonl_records(&scores_path)?;
    let manifest = default_benchmark_manifest(adapter, &scores);
    atomic_write_json_pretty(&manifest_path, &manifest)?;
    validate_json_file_against_schema("benchmark_adapter_manifest_v1.jsonschema", &manifest_path)?;

    let summary = build_benchmark_summary(run_id, &manifest, &scores)?;
    atomic_write_json_pretty(&summary_path, &summary)?;
    validate_json_file_against_schema("benchmark_summary_v1.jsonschema", &summary_path)?;

    Ok(scores_path)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TrialSlot {
    variant_idx: usize,
    task_idx: usize,
    repl_idx: usize,
}

fn build_trial_schedule(
    variant_count: usize,
    task_count: usize,
    replications: usize,
    policy: SchedulingPolicy,
    random_seed: u64,
) -> Vec<TrialSlot> {
    let mut slots = Vec::with_capacity(variant_count * task_count * replications);

    match policy {
        SchedulingPolicy::VariantSequential => {
            for v in 0..variant_count {
                for t in 0..task_count {
                    for r in 0..replications {
                        slots.push(TrialSlot {
                            variant_idx: v,
                            task_idx: t,
                            repl_idx: r,
                        });
                    }
                }
            }
        }
        SchedulingPolicy::PairedInterleaved => {
            for t in 0..task_count {
                for v in 0..variant_count {
                    for r in 0..replications {
                        slots.push(TrialSlot {
                            variant_idx: v,
                            task_idx: t,
                            repl_idx: r,
                        });
                    }
                }
            }
        }
        SchedulingPolicy::Randomized => {
            // Build variant_sequential order then shuffle deterministically
            for v in 0..variant_count {
                for t in 0..task_count {
                    for r in 0..replications {
                        slots.push(TrialSlot {
                            variant_idx: v,
                            task_idx: t,
                            repl_idx: r,
                        });
                    }
                }
            }
            // Deterministic Fisher-Yates using LCG seeded by random_seed
            let mut rng_state: u64 = random_seed;
            for i in (1..slots.len()).rev() {
                // LCG: state = state * 6364136223846793005 + 1442695040888963407
                rng_state = rng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let j = (rng_state >> 33) as usize % (i + 1);
                slots.swap(i, j);
            }
        }
    }

    slots
}

// --- Schedule progress tracking for resumable runs ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SlotCompletion {
    schedule_index: usize,
    trial_id: String,
    status: String, // "completed" | "failed" | "skipped_pruned"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScheduleProgress {
    schema_version: String,
    run_id: String,
    total_slots: usize,
    next_schedule_index: usize,
    next_trial_index: usize,
    schedule: Vec<TrialSlot>,
    completed_slots: Vec<SlotCompletion>,
    pruned_variants: Vec<usize>,
    consecutive_failures: BTreeMap<usize, usize>,
    use_container: bool,
    updated_at: String,
}

fn schedule_progress_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("schedule_progress.json")
}

fn write_schedule_progress(run_dir: &Path, progress: &ScheduleProgress) -> Result<()> {
    let value = serde_json::to_value(progress)?;
    atomic_write_json_pretty(&schedule_progress_path(run_dir), &value)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResolvedVariantsManifest {
    schema_version: String,
    generated_at: String,
    baseline_id: String,
    variants: Vec<Variant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResolvedScheduleManifest {
    schema_version: String,
    generated_at: String,
    total_slots: usize,
    schedule: Vec<TrialSlot>,
}

fn resolved_variants_path(run_dir: &Path) -> PathBuf {
    run_dir.join("resolved_variants.json")
}

fn resolved_schedule_path(run_dir: &Path) -> PathBuf {
    run_dir.join("resolved_schedule.json")
}

fn write_resolved_variants(run_dir: &Path, baseline_id: &str, variants: &[Variant]) -> Result<()> {
    let manifest = ResolvedVariantsManifest {
        schema_version: "resolved_variants_v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        baseline_id: baseline_id.to_string(),
        variants: variants.to_vec(),
    };
    let value = serde_json::to_value(&manifest)?;
    atomic_write_json_pretty(&resolved_variants_path(run_dir), &value)?;
    let digest = canonical_json_digest(&value);
    atomic_write_bytes(&run_dir.join("resolved_variants.digest"), digest.as_bytes())?;
    Ok(())
}

fn write_resolved_schedule(run_dir: &Path, schedule: &[TrialSlot]) -> Result<()> {
    let manifest = ResolvedScheduleManifest {
        schema_version: "resolved_schedule_v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        total_slots: schedule.len(),
        schedule: schedule.to_vec(),
    };
    let value = serde_json::to_value(&manifest)?;
    atomic_write_json_pretty(&resolved_schedule_path(run_dir), &value)?;
    let digest = canonical_json_digest(&value);
    atomic_write_bytes(&run_dir.join("resolved_schedule.digest"), digest.as_bytes())?;
    Ok(())
}

fn load_run_variants(run_dir: &Path, experiment: &Value) -> Result<(Vec<Variant>, String)> {
    let manifest_path = resolved_variants_path(run_dir);
    if !manifest_path.exists() {
        return resolve_variant_plan(experiment);
    }

    let manifest: ResolvedVariantsManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if manifest.schema_version != "resolved_variants_v1" {
        return Err(anyhow!(
            "unsupported resolved variants schema_version in {}: {}",
            manifest_path.display(),
            manifest.schema_version
        ));
    }
    if manifest.variants.is_empty() {
        return Err(anyhow!(
            "resolved variants manifest has no variants: {}",
            manifest_path.display()
        ));
    }
    if !manifest
        .variants
        .iter()
        .any(|variant| variant.id == manifest.baseline_id)
    {
        return Err(anyhow!(
            "resolved variants manifest baseline '{}' not found in variants: {}",
            manifest.baseline_id,
            manifest_path.display()
        ));
    }
    Ok((manifest.variants, manifest.baseline_id))
}

fn should_retry_outcome(outcome: &str, exit_status: &str, retry_on: &[String]) -> bool {
    if retry_on.is_empty() {
        // When retry_on is unspecified, retry on any non-success
        return outcome == "error" || exit_status != "0";
    }
    for trigger in retry_on {
        match trigger.as_str() {
            "error" if outcome == "error" => return true,
            "failure" if exit_status != "0" => return true,
            "timeout" if outcome == "timeout" => return true,
            _ => {}
        }
    }
    false
}

fn benchmark_verdict_to_trial_outcome(verdict: &str) -> Option<&'static str> {
    match verdict {
        "pass" => Some("success"),
        "fail" => Some("failure"),
        "missing" => Some("missing"),
        "error" => Some("error"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Variant {
    id: String,
    bindings: Value,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    image: Option<String>,
    runtime_overrides: Option<Value>,
}

fn variant_bindings_for_summary(variant: &Variant) -> Value {
    if !variant.args.is_empty() || !variant.env.is_empty() || variant.image.is_some() {
        return json!({
            "args": variant.args,
            "env": variant.env,
            "image": variant.image,
        });
    }
    variant.bindings.clone()
}

fn variant_digest(variant: &Variant) -> Result<String> {
    let value = serde_json::to_value(variant)?;
    Ok(canonical_json_digest(&value))
}

fn resolve_variant_plan(json_value: &Value) -> Result<(Vec<Variant>, String)> {
    let baseline = json_value
        .pointer("/baseline/variant_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing /baseline/variant_id"))?
        .to_string();
    if json_value.get("variants").is_some() {
        return Err(anyhow!(
            "/variants is not supported; use /variant_plan for experiment variant plans"
        ));
    }

    let clean_contract = is_clean_contract_experiment(json_value);

    let mut variants = Vec::new();
    if clean_contract {
        let baseline_args =
            parse_string_array_field(json_value.pointer("/baseline/args"), "/baseline/args")?;
        let baseline_env =
            parse_string_map_field(json_value.pointer("/baseline/env"), "/baseline/env")?;
        let baseline_image = parse_optional_nonempty_string(
            json_value.pointer("/baseline/image"),
            "/baseline/image",
        )?;
        variants.push(Variant {
            id: baseline.clone(),
            bindings: json!({}),
            args: baseline_args,
            env: baseline_env,
            image: baseline_image,
            runtime_overrides: None,
        });
    } else {
        let baseline_bindings = json_value
            .pointer("/baseline/bindings")
            .cloned()
            .unwrap_or(json!({}));
        if !baseline_bindings.is_object() {
            return Err(anyhow!("invalid /baseline/bindings: expected object"));
        }
        let mut baseline_runtime_overrides = match json_value.pointer("/baseline/runtime_overrides")
        {
            None | Some(Value::Null) => None,
            Some(Value::Object(_)) => json_value.pointer("/baseline/runtime_overrides").cloned(),
            Some(_) => return Err(anyhow!("/baseline/runtime_overrides must be an object")),
        };
        if let Some(image) = parse_optional_nonempty_string(
            json_value.pointer("/baseline/image"),
            "/baseline/image",
        )? {
            let mut overrides = baseline_runtime_overrides.unwrap_or_else(|| json!({}));
            set_json_pointer_value(&mut overrides, "/agent/image", json!(image))?;
            baseline_runtime_overrides = Some(overrides);
        }
        variants.push(Variant {
            id: baseline.clone(),
            bindings: baseline_bindings,
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: baseline_runtime_overrides,
        });
    }

    let variant_list: &[Value] = match json_value.pointer("/variant_plan") {
        Some(value) => value.as_array().map(|v| v.as_slice()).ok_or_else(|| {
            anyhow!("/variant_plan must be an array of {{ variant_id, bindings }} objects")
        })?,
        None => &[],
    };
    for (idx, item) in variant_list.iter().enumerate() {
        let id = item
            .get("variant_id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "/variant_plan[{}] must include non-empty string variant_id",
                    idx
                )
            })?
            .to_string();
        if clean_contract {
            let args = parse_string_array_field(
                item.get("args"),
                &format!("/variant_plan[{}].args", idx),
            )?;
            let env =
                parse_string_map_field(item.get("env"), &format!("/variant_plan[{}].env", idx))?;
            let image = parse_optional_nonempty_string(
                item.get("image"),
                &format!("/variant_plan[{}].image", idx),
            )?;
            variants.push(Variant {
                id,
                bindings: json!({}),
                args,
                env,
                image,
                runtime_overrides: None,
            });
        } else {
            let bindings = item.get("bindings").cloned().unwrap_or(json!({}));
            if !bindings.is_object() {
                return Err(anyhow!("/variant_plan[{}].bindings must be an object", idx));
            }
            let mut runtime_overrides = match item.get("runtime_overrides") {
                None | Some(Value::Null) => None,
                Some(Value::Object(_)) => item.get("runtime_overrides").cloned(),
                Some(_) => {
                    return Err(anyhow!(
                        "/variant_plan[{}].runtime_overrides must be an object",
                        idx
                    ))
                }
            };
            if let Some(image) = parse_optional_nonempty_string(
                item.get("image"),
                &format!("/variant_plan[{}].image", idx),
            )? {
                let mut overrides = runtime_overrides.unwrap_or_else(|| json!({}));
                set_json_pointer_value(&mut overrides, "/agent/image", json!(image))?;
                runtime_overrides = Some(overrides);
            }
            variants.push(Variant {
                id,
                bindings,
                args: Vec::new(),
                env: BTreeMap::new(),
                image: None,
                runtime_overrides,
            });
        }
    }
    Ok((variants, baseline))
}

fn merge_json_value(base: &mut Value, patch: &Value) {
    match (base, patch) {
        (Value::Object(base_map), Value::Object(patch_map)) => {
            for (key, patch_value) in patch_map {
                if let Some(base_value) = base_map.get_mut(key) {
                    merge_json_value(base_value, patch_value);
                } else {
                    base_map.insert(key.clone(), patch_value.clone());
                }
            }
        }
        (base_slot, patch_value) => {
            *base_slot = patch_value.clone();
        }
    }
}

fn resolve_runtime_for_variant(experiment: &Value, variant: &Variant) -> Result<Value> {
    if is_clean_contract_experiment(experiment) {
        let mut resolved = experiment.clone();
        if let Some(image) = variant.image.as_ref() {
            set_json_pointer_value(&mut resolved, "/runtime/image", json!(image))?;
        }
        return Ok(resolved);
    }

    let mut resolved = experiment.clone();
    let Some(runtime_overrides) = variant.runtime_overrides.as_ref() else {
        return Ok(resolved);
    };

    let mut runtime = resolved
        .pointer("/runtime")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !runtime.is_object() {
        return Err(anyhow!("invalid /runtime: expected object"));
    }

    merge_json_value(&mut runtime, runtime_overrides);
    set_json_pointer_value(&mut resolved, "/runtime", runtime)?;
    Ok(resolved)
}

fn find_variant_by_id<'a>(variants: &'a [Variant], variant_id: &str) -> Result<&'a Variant> {
    let trimmed = variant_id.trim();
    if trimmed.is_empty() {
        return variants
            .first()
            .ok_or_else(|| anyhow!("no variants available in experiment"));
    }
    variants
        .iter()
        .find(|variant| variant.id == trimmed)
        .ok_or_else(|| anyhow!("variant '{}' not found in experiment", trimmed))
}

fn apply_experiment_overrides(
    mut experiment: Value,
    overrides_path: &Path,
    project_root: &Path,
) -> Result<Value> {
    let overrides = load_experiment_overrides(overrides_path)?;
    if overrides.values.is_empty() {
        return Ok(experiment);
    }

    let manifest_rel = overrides
        .manifest_path
        .clone()
        .unwrap_or_else(|| ".lab/knobs/manifest.json".to_string());
    let manifest_path = if Path::new(&manifest_rel).is_absolute() {
        PathBuf::from(&manifest_rel)
    } else {
        project_root.join(&manifest_rel)
    };
    let manifest = load_knob_manifest(&manifest_path)?;

    let mut by_id: BTreeMap<String, KnobDef> = BTreeMap::new();
    for knob in manifest.knobs {
        by_id.insert(knob.id.clone(), knob);
    }

    for (id, value) in overrides.values.iter() {
        let knob = by_id
            .get(id)
            .ok_or_else(|| anyhow!("override references unknown knob id: {}", id))?;
        validate_knob_value(knob, value)?;
        set_json_pointer_value(&mut experiment, &knob.json_pointer, value.clone())?;
    }

    Ok(experiment)
}

fn load_experiment_overrides(overrides_path: &Path) -> Result<ExperimentOverrides> {
    let overrides_schema = compile_schema("experiment_overrides_v1.jsonschema")?;
    let overrides_data = fs::read_to_string(overrides_path)?;
    let overrides_json: Value = serde_json::from_str(&overrides_data)?;
    if let Err(errors) = overrides_schema.validate(&overrides_json) {
        let mut msgs = Vec::new();
        for e in errors {
            msgs.push(e.to_string());
        }
        return Err(anyhow!(
            "overrides schema validation failed ({}): {}",
            overrides_path.display(),
            msgs.join("; ")
        ));
    }
    let overrides: ExperimentOverrides = serde_json::from_value(overrides_json)?;
    if overrides.schema_version != "experiment_overrides_v1" {
        return Err(anyhow!(
            "unsupported overrides schema_version: {}",
            overrides.schema_version
        ));
    }
    Ok(overrides)
}

fn load_knob_manifest(manifest_path: &Path) -> Result<KnobManifest> {
    let manifest_schema = compile_schema("knob_manifest_v1.jsonschema")?;
    let manifest_data = fs::read_to_string(manifest_path)?;
    let manifest_json: Value = serde_json::from_str(&manifest_data)?;
    if let Err(errors) = manifest_schema.validate(&manifest_json) {
        let mut msgs = Vec::new();
        for e in errors {
            msgs.push(e.to_string());
        }
        return Err(anyhow!(
            "knob manifest schema validation failed ({}): {}",
            manifest_path.display(),
            msgs.join("; ")
        ));
    }
    let manifest: KnobManifest = serde_json::from_value(manifest_json)?;
    if manifest.schema_version != "knob_manifest_v1" {
        return Err(anyhow!(
            "unsupported knob manifest schema_version: {}",
            manifest.schema_version
        ));
    }
    Ok(manifest)
}

fn validate_knob_value(knob: &KnobDef, value: &Value) -> Result<()> {
    if !value_matches_type(value, &knob.value_type) {
        return Err(anyhow!(
            "override value type mismatch for knob {}: expected {}, got {}",
            knob.id,
            knob.value_type,
            value_type_name(value)
        ));
    }

    if let Some(options) = knob.options.as_ref() {
        if !options.iter().any(|opt| opt == value) {
            return Err(anyhow!(
                "override value for knob {} is not in allowed options",
                knob.id
            ));
        }
    }

    if let Some(min) = knob.minimum {
        if let Some(v) = value.as_f64() {
            if v < min {
                return Err(anyhow!(
                    "override value for knob {} is below minimum {}",
                    knob.id,
                    min
                ));
            }
        }
    }
    if let Some(max) = knob.maximum {
        if let Some(v) = value.as_f64() {
            if v > max {
                return Err(anyhow!(
                    "override value for knob {} is above maximum {}",
                    knob.id,
                    max
                ));
            }
        }
    }
    Ok(())
}

fn value_matches_type(value: &Value, t: &str) -> bool {
    match t {
        "string" => value.is_string(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => false,
    }
}

fn value_type_name(value: &Value) -> &'static str {
    if value.is_string() {
        "string"
    } else if value.is_boolean() {
        "boolean"
    } else if value.is_number() {
        "number"
    } else if value.is_array() {
        "array"
    } else if value.is_object() {
        "object"
    } else {
        "null"
    }
}

fn decode_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn set_json_pointer_value(root: &mut Value, pointer: &str, new_value: Value) -> Result<()> {
    if pointer.is_empty() || pointer == "/" {
        *root = new_value;
        return Ok(());
    }
    if !pointer.starts_with('/') {
        return Err(anyhow!("json_pointer must start with '/': {}", pointer));
    }

    let tokens: Vec<String> = pointer
        .split('/')
        .skip(1)
        .map(decode_pointer_token)
        .collect();
    if tokens.is_empty() {
        *root = new_value;
        return Ok(());
    }

    let mut cur = root;
    for token in tokens.iter().take(tokens.len() - 1) {
        match cur {
            Value::Object(map) => {
                let entry = map.entry(token.clone()).or_insert_with(|| json!({}));
                cur = entry;
            }
            Value::Array(arr) => {
                let idx: usize = token.parse().map_err(|_| {
                    anyhow!(
                        "json_pointer token '{}' is not a valid array index in {}",
                        token,
                        pointer
                    )
                })?;
                if idx >= arr.len() {
                    return Err(anyhow!(
                        "json_pointer array index {} out of bounds in {}",
                        idx,
                        pointer
                    ));
                }
                cur = &mut arr[idx];
            }
            _ => {
                return Err(anyhow!(
                    "json_pointer traversal hit non-container at token '{}' in {}",
                    token,
                    pointer
                ));
            }
        }
    }

    let last = tokens.last().unwrap();
    match cur {
        Value::Object(map) => {
            map.insert(last.clone(), new_value);
            Ok(())
        }
        Value::Array(arr) => {
            let idx: usize = last.parse().map_err(|_| {
                anyhow!(
                    "json_pointer token '{}' is not a valid array index in {}",
                    last,
                    pointer
                )
            })?;
            if idx >= arr.len() {
                return Err(anyhow!(
                    "json_pointer array index {} out of bounds in {}",
                    idx,
                    pointer
                ));
            }
            arr[idx] = new_value;
            Ok(())
        }
        _ => Err(anyhow!(
            "json_pointer target is not an object/array for {}",
            pointer
        )),
    }
}

fn resolve_dataset_path(json_value: &Value, exp_dir: &Path) -> Result<PathBuf> {
    let rel = json_value
        .pointer("/dataset/path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("dataset.path missing"))?;
    let path = exp_dir.join(rel);
    Ok(path)
}

fn load_tasks(path: &Path, json_value: &Value) -> Result<Vec<Value>> {
    let data = fs::read_to_string(path)?;
    let mut tasks = Vec::new();
    for line in data.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let task: Value = serde_json::from_str(line)?;
        tasks.push(task);
    }
    if let Some(limit) = json_value
        .pointer("/dataset/limit")
        .and_then(|v| v.as_u64())
    {
        tasks.truncate(limit as usize);
    }
    Ok(tasks)
}

fn count_tasks(path: &Path, json_value: &Value) -> Result<usize> {
    let data = fs::read_to_string(path)?;
    let mut count = 0usize;
    for line in data.lines() {
        if line.trim().is_empty() {
            continue;
        }
        count += 1;
        if let Some(limit) = json_value
            .pointer("/dataset/limit")
            .and_then(|v| v.as_u64())
        {
            if count >= limit as usize {
                break;
            }
        }
    }
    Ok(count)
}

const TASK_BOUNDARY_V1_SCHEMA_VERSION: &str = "task_boundary_v1";
const TASK_BOUNDARY_V2_SCHEMA_VERSION: &str = "task_boundary_v2";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceFileSpec {
    path: String,
    content: String,
    #[serde(default)]
    encoding: Option<String>,
    #[serde(default)]
    executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MountReferenceSpec {
    dataset_pack_ref: String,
    mount_path: String,
    #[serde(default)]
    read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TaskBoundaryLimits {
    #[serde(default)]
    max_steps: Option<u64>,
    #[serde(default)]
    max_total_tokens: Option<u64>,
    #[serde(default)]
    max_tool_calls: Option<u64>,
    #[serde(default)]
    trial_seconds: Option<u64>,
}

impl TaskBoundaryLimits {
    fn is_empty(&self) -> bool {
        self.max_steps.is_none()
            && self.max_total_tokens.is_none()
            && self.max_tool_calls.is_none()
            && self.trial_seconds.is_none()
    }
}

#[derive(Debug, Clone)]
struct TaskBoundaryMaterialization {
    task_payload: Value,
    workspace_files: Vec<WorkspaceFileSpec>,
    mount_references: Vec<MountReferenceSpec>,
    limits: TaskBoundaryLimits,
    task_image: Option<String>,
    task_workspace: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedMountReference {
    host_path: PathBuf,
    mount_path: String,
}

fn default_task_boundary(task_payload: Value) -> TaskBoundaryMaterialization {
    let task_image = task_payload
        .pointer("/image")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    let task_workspace = task_payload
        .pointer("/workspace")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    TaskBoundaryMaterialization {
        task_payload,
        workspace_files: Vec::new(),
        mount_references: Vec::new(),
        limits: TaskBoundaryLimits::default(),
        task_image,
        task_workspace,
    }
}

fn parse_task_boundary_from_dataset_task(task: &Value) -> Result<TaskBoundaryMaterialization> {
    let schema_version = task.get("schema_version").and_then(|v| v.as_str());
    if schema_version != Some(TASK_BOUNDARY_V1_SCHEMA_VERSION)
        && schema_version != Some(TASK_BOUNDARY_V2_SCHEMA_VERSION)
    {
        return Ok(default_task_boundary(task.clone()));
    }
    let obj = task
        .as_object()
        .ok_or_else(|| anyhow!("task boundary must be an object"))?;

    let allowed = [
        "schema_version",
        "task",
        "workspace_files",
        "mount_references",
        "limits",
    ];
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(anyhow!(
                "task boundary contains unsupported key '{}'; expected task + workspace_files + mount_references + limits",
                key
            ));
        }
    }

    let task_payload = obj
        .get("task")
        .cloned()
        .ok_or_else(|| anyhow!("task boundary missing field: task"))?;
    if !task_payload.is_object() {
        return Err(anyhow!("task boundary field 'task' must be an object"));
    }
    let task_image = task_payload
        .pointer("/image")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    if task_payload.get("image").is_some() && task_image.is_none() {
        return Err(anyhow!(
            "task boundary field 'task.image' must be a non-empty string when provided"
        ));
    }
    let task_workspace = task_payload
        .pointer("/workspace")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);

    Ok(TaskBoundaryMaterialization {
        task_payload,
        workspace_files: parse_workspace_files(obj.get("workspace_files"))?,
        mount_references: parse_mount_references(obj.get("mount_references"))?,
        limits: parse_task_limits(obj.get("limits"))?,
        task_image,
        task_workspace,
    })
}

fn parse_task_boundary_from_trial_input(input: &Value) -> Result<TaskBoundaryMaterialization> {
    if let Some(task_payload) = input.pointer("/task").cloned() {
        if !task_payload.is_object() {
            return Err(anyhow!("trial_input /task must be an object"));
        }

        if let Some(ext) = input.pointer("/ext/task_boundary_v1") {
            parse_task_boundary_ext(ext, task_payload)
        } else if task_payload.get("schema_version").and_then(|v| v.as_str())
            == Some(TASK_BOUNDARY_V1_SCHEMA_VERSION)
            || task_payload.get("schema_version").and_then(|v| v.as_str())
                == Some(TASK_BOUNDARY_V2_SCHEMA_VERSION)
        {
            parse_task_boundary_from_dataset_task(&task_payload)
        } else {
            Ok(default_task_boundary(task_payload))
        }
    } else if input.is_object() {
        let looks_like_legacy_envelope = input.get("ids").is_some()
            || input.get("bindings").is_some()
            || input.get("dependencies").is_some()
            || input.get("policy").is_some()
            || input.get("runtime").is_some();
        if looks_like_legacy_envelope {
            return Err(anyhow!("trial_input missing required /task"));
        }
        if input.get("schema_version").and_then(|v| v.as_str())
            == Some(TASK_BOUNDARY_V1_SCHEMA_VERSION)
            || input.get("schema_version").and_then(|v| v.as_str())
                == Some(TASK_BOUNDARY_V2_SCHEMA_VERSION)
        {
            parse_task_boundary_from_dataset_task(input)
        } else {
            Ok(default_task_boundary(input.clone()))
        }
    } else {
        Err(anyhow!("trial_input missing required /task"))
    }
}

fn parse_task_boundary_ext(
    ext: &Value,
    task_payload: Value,
) -> Result<TaskBoundaryMaterialization> {
    let obj = ext
        .as_object()
        .ok_or_else(|| anyhow!("trial_input /ext/task_boundary_v1 must be an object"))?;
    if let Some(schema_version) = obj.get("schema_version") {
        if schema_version.as_str() != Some(TASK_BOUNDARY_V1_SCHEMA_VERSION) {
            return Err(anyhow!(
                "unsupported task boundary schema version in /ext/task_boundary_v1"
            ));
        }
    }
    let task_image = task_payload
        .pointer("/image")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    if task_payload.get("image").is_some() && task_image.is_none() {
        return Err(anyhow!(
            "trial_input /task.image must be a non-empty string when provided"
        ));
    }
    let task_workspace = task_payload
        .pointer("/workspace")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);

    Ok(TaskBoundaryMaterialization {
        task_payload,
        workspace_files: parse_workspace_files(obj.get("workspace_files"))?,
        mount_references: parse_mount_references(obj.get("mount_references"))?,
        limits: parse_task_limits(obj.get("limits"))?,
        task_image,
        task_workspace,
    })
}

fn validate_task_boundary_workspace_materialization(
    task_boundary: &TaskBoundaryMaterialization,
    task_boundary_policy: &TaskBoundaryPolicy,
) -> Result<()> {
    if !task_boundary_policy.require_workspace_materialization {
        return Ok(());
    }
    if !task_boundary.workspace_files.is_empty() || !task_boundary.mount_references.is_empty() {
        return Ok(());
    }
    let task_id = task_boundary
        .task_payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown_task>");
    Err(anyhow!(
        "task '{}' is missing required workspace materialization: provide task boundary workspace_files or mount_references",
        task_id
    ))
}

fn parse_workspace_files(value: Option<&Value>) -> Result<Vec<WorkspaceFileSpec>> {
    let Some(raw) = value else {
        return Ok(Vec::new());
    };
    let arr = raw
        .as_array()
        .ok_or_else(|| anyhow!("task boundary workspace_files must be an array"))?;

    let mut files = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        let file: WorkspaceFileSpec = serde_json::from_value(item.clone())
            .map_err(|e| anyhow!("invalid workspace_files[{}]: {}", idx, e))?;
        let _ = validate_workspace_relative_path(&file.path).map_err(|e| {
            anyhow!(
                "invalid workspace_files[{}].path '{}': {}",
                idx,
                file.path,
                e
            )
        })?;
        if let Some(encoding) = file.encoding.as_deref() {
            if encoding != "utf8" && encoding != "base64" {
                return Err(anyhow!(
                    "workspace_files[{}].encoding must be 'utf8' or 'base64'",
                    idx
                ));
            }
        }
        files.push(file);
    }
    Ok(files)
}

fn parse_mount_references(value: Option<&Value>) -> Result<Vec<MountReferenceSpec>> {
    let Some(raw) = value else {
        return Ok(Vec::new());
    };
    let arr = raw
        .as_array()
        .ok_or_else(|| anyhow!("task boundary mount_references must be an array"))?;

    let mut mounts = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        let mount: MountReferenceSpec = serde_json::from_value(item.clone())
            .map_err(|e| anyhow!("invalid mount_references[{}]: {}", idx, e))?;
        if !mount.read_only {
            return Err(anyhow!("mount_references[{}].read_only must be true", idx));
        }
        validate_container_workspace_path(&mount.mount_path).map_err(|e| {
            anyhow!(
                "invalid mount_references[{}].mount_path '{}': {}",
                idx,
                mount.mount_path,
                e
            )
        })?;
        let _ = parse_dataset_pack_ref_digest(&mount.dataset_pack_ref).map_err(|e| {
            anyhow!(
                "invalid mount_references[{}].dataset_pack_ref '{}': {}",
                idx,
                mount.dataset_pack_ref,
                e
            )
        })?;
        mounts.push(mount);
    }
    Ok(mounts)
}

fn parse_task_limits(value: Option<&Value>) -> Result<TaskBoundaryLimits> {
    let Some(raw) = value else {
        return Ok(TaskBoundaryLimits::default());
    };
    let limits: TaskBoundaryLimits =
        serde_json::from_value(raw.clone()).map_err(|e| anyhow!("invalid limits: {}", e))?;
    validate_limit_positive("max_steps", limits.max_steps)?;
    validate_limit_positive("max_total_tokens", limits.max_total_tokens)?;
    validate_limit_positive("max_tool_calls", limits.max_tool_calls)?;
    validate_limit_positive("trial_seconds", limits.trial_seconds)?;
    Ok(limits)
}

fn validate_limit_positive(name: &str, value: Option<u64>) -> Result<()> {
    if value == Some(0) {
        return Err(anyhow!("{} must be > 0 when provided", name));
    }
    Ok(())
}

fn validate_workspace_relative_path(path: &str) -> Result<PathBuf> {
    if path.trim().is_empty() {
        return Err(anyhow!("path cannot be empty"));
    }
    let p = Path::new(path);
    if p.is_absolute() {
        return Err(anyhow!(
            "path must be relative to {}",
            AGENTLAB_CONTRACT_WORKSPACE_DIR
        ));
    }
    let mut normalized = PathBuf::new();
    for component in p.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(seg) => normalized.push(seg),
            Component::ParentDir => {
                return Err(anyhow!("path cannot contain '..'"));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(anyhow!("path cannot be absolute"));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(anyhow!("path cannot resolve to empty"));
    }
    Ok(normalized)
}

fn validate_container_workspace_path(path: &str) -> Result<()> {
    if !(path == AGENTLAB_CONTRACT_WORKSPACE_DIR
        || path.starts_with(&format!("{}/", AGENTLAB_CONTRACT_WORKSPACE_DIR)))
    {
        return Err(anyhow!(
            "mount_path must be under {}",
            AGENTLAB_CONTRACT_WORKSPACE_DIR
        ));
    }
    let p = Path::new(path);
    if !p.is_absolute() {
        return Err(anyhow!("mount_path must be absolute"));
    }
    for component in p.components() {
        if matches!(component, Component::ParentDir) {
            return Err(anyhow!("mount_path cannot contain '..'"));
        }
    }
    Ok(())
}

fn parse_dataset_pack_ref_digest(dataset_pack_ref: &str) -> Result<String> {
    let digest = dataset_pack_ref
        .strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("dataset_pack_ref must start with 'sha256:'"))?;
    if digest.len() != 64 || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("dataset_pack_ref digest must be 64 hex characters"));
    }
    Ok(digest.to_ascii_lowercase())
}

fn resolve_dataset_pack_host_path(project_root: &Path, dataset_pack_ref: &str) -> Result<PathBuf> {
    let digest = parse_dataset_pack_ref_digest(dataset_pack_ref)?;
    let path = project_root
        .join(".lab")
        .join("dataset_packs")
        .join("sha256")
        .join(digest);
    if !path.exists() {
        return Err(anyhow!("dataset pack not found: {}", path.display()));
    }
    Ok(path)
}

fn resolve_task_mounts(
    project_root: &Path,
    mount_references: &[MountReferenceSpec],
    container_mode: bool,
) -> Result<Vec<ResolvedMountReference>> {
    if mount_references.is_empty() {
        return Ok(Vec::new());
    }
    if !container_mode {
        return Err(anyhow!("task mount_references require container executor"));
    }
    let mut mounts = Vec::with_capacity(mount_references.len());
    for mount in mount_references {
        let host_path = resolve_dataset_pack_host_path(project_root, &mount.dataset_pack_ref)?;
        mounts.push(ResolvedMountReference {
            host_path,
            mount_path: mount.mount_path.clone(),
        });
    }
    Ok(mounts)
}

fn materialize_workspace_files(
    paths: &TrialPaths,
    workspace_files: &[WorkspaceFileSpec],
) -> Result<()> {
    for file in workspace_files {
        let rel = validate_workspace_relative_path(&file.path)?;
        let host_path = paths.workspace.join(rel);
        let bytes = match file.encoding.as_deref() {
            None | Some("utf8") => file.content.as_bytes().to_vec(),
            Some("base64") => BASE64_STANDARD
                .decode(file.content.as_bytes())
                .map_err(|e| {
                    anyhow!(
                        "failed to decode base64 workspace file '{}': {}",
                        file.path,
                        e
                    )
                })?,
            Some(other) => {
                return Err(anyhow!(
                    "unsupported workspace file encoding '{}' for '{}'",
                    other,
                    file.path
                ));
            }
        };
        atomic_write_bytes(&host_path, &bytes)?;
        #[cfg(unix)]
        if file.executable {
            let metadata = fs::metadata(&host_path)?;
            let mut perms = metadata.permissions();
            perms.set_mode(perms.mode() | 0o111);
            fs::set_permissions(&host_path, perms)?;
        }
    }
    Ok(())
}

fn copy_staged_host_file(src: &Path, dst: &Path, required: bool, label: &str) -> Result<bool> {
    if !src.exists() {
        if required {
            return Err(anyhow!(
                "staged host file source missing for {}: {}",
                label,
                src.display()
            ));
        }
        return Ok(false);
    }
    if !src.is_file() {
        return Err(anyhow!(
            "staged host file source is not a file for {}: {}",
            label,
            src.display()
        ));
    }
    if let Some(parent) = dst.parent() {
        ensure_dir(parent)?;
    }
    fs::copy(src, dst).map_err(|e| {
        anyhow!(
            "failed to copy staged host file {} from {} to {}: {}",
            label,
            src.display(),
            dst.display(),
            e
        )
    })?;
    Ok(true)
}

fn stage_dependencies_for_trial(
    runtime_agent: &AgentRuntimeConfig,
    paths: &TrialPaths,
) -> Result<()> {
    for (idx, spec) in runtime_agent.dependency_file_staging.iter().enumerate() {
        let dst = map_container_path_to_host(&spec.destination_path, paths)?;
        copy_staged_host_file(
            &spec.source_from_host,
            &dst,
            spec.required,
            &format!("#{}", idx),
        )?;
        #[cfg(unix)]
        if dst
            .file_name()
            .and_then(|n| n.to_str())
            .map(|name| name == "master.key")
            .unwrap_or(false)
            && dst.exists()
        {
            let mut perms = fs::metadata(&dst)?.permissions();
            perms.set_mode(0o600);
            fs::set_permissions(&dst, perms)?;
        }
        #[cfg(unix)]
        if spec.read_only && dst.exists() {
            let mut perms = fs::metadata(&dst)?.permissions();
            perms.set_mode(0o444);
            fs::set_permissions(&dst, perms)?;
        }
    }
    Ok(())
}

fn task_boundary_ext_value(task_boundary: &TaskBoundaryMaterialization) -> Option<Value> {
    if task_boundary.workspace_files.is_empty()
        && task_boundary.mount_references.is_empty()
        && task_boundary.limits.is_empty()
    {
        return None;
    }

    Some(json!({
        "schema_version": TASK_BOUNDARY_V1_SCHEMA_VERSION,
        "workspace_files": task_boundary.workspace_files,
        "mount_references": task_boundary.mount_references,
        "limits": task_boundary.limits,
    }))
}

#[derive(Clone)]
struct DependencyFileStagingSpec {
    source_from_host: PathBuf,
    destination_path: String,
    required: bool,
    read_only: bool,
}

#[derive(Clone)]
struct AgentRuntimeIoConfig {
    input_arg: String,
    output_arg: String,
}

#[derive(Clone)]
struct AgentRuntimeConfig {
    adapter_ref: AgentAdapterRef,
    command_raw: Vec<String>,
    container_image: Option<String>,
    image_source: ImageSource,
    agent_artifact: Option<PathBuf>,
    io: AgentRuntimeIoConfig,
    clean_contract_v1: bool,
    integration_level: String,
    launch_mode: AgentLaunchMode,
    env: BTreeMap<String, String>,
    env_from_host: Vec<String>,
    trajectory_path: Option<String>,
    causal_extraction: Option<String>,
    default_timeout_ms: Option<u64>,
    tracing_mode: Option<String>,
    force_container: bool,
    dependency_file_staging: Vec<DependencyFileStagingSpec>,
    dependency_services: Vec<Value>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ImageSource {
    Global,
    PerTask,
}

impl ImageSource {
    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.unwrap_or("global") {
            "global" => Ok(Self::Global),
            "per_task" => Ok(Self::PerTask),
            other => Err(anyhow!(
                "runtime.agent.image_source must be 'global' or 'per_task' (got '{}')",
                other
            )),
        }
    }
}

fn resolve_host_path_from_spec(raw: &str, exp_dir: &Path) -> Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("path must not be empty"));
    }
    let expanded = if trimmed == "~" || trimmed.starts_with("~/") {
        let home = std::env::var("HOME")
            .map_err(|_| anyhow!("HOME env var is required to resolve '{}'", trimmed))?;
        if trimmed == "~" {
            PathBuf::from(home)
        } else {
            Path::new(&home).join(trimmed.trim_start_matches("~/"))
        }
    } else {
        PathBuf::from(trimmed)
    };
    if expanded.is_absolute() {
        Ok(normalize_path(&expanded))
    } else {
        Ok(normalize_path(&exp_dir.join(expanded)))
    }
}

fn parse_string_array_field(value: Option<&Value>, field: &str) -> Result<Vec<String>> {
    match value {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => {
            let mut parsed = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                let token = item
                    .as_str()
                    .ok_or_else(|| anyhow!("{}[{}] must be a string", field, idx))?;
                if token.trim().is_empty() {
                    return Err(anyhow!("{}[{}] must not be empty", field, idx));
                }
                parsed.push(token.to_string());
            }
            Ok(parsed)
        }
        Some(_) => Err(anyhow!("{} must be a string[]", field)),
    }
}

fn parse_string_map_field(value: Option<&Value>, field: &str) -> Result<BTreeMap<String, String>> {
    match value {
        None => Ok(BTreeMap::new()),
        Some(Value::Object(map)) => {
            let mut parsed = BTreeMap::new();
            for (key, value) in map {
                if key.trim().is_empty() {
                    return Err(anyhow!("{} contains an empty key", field));
                }
                let as_str = value
                    .as_str()
                    .ok_or_else(|| anyhow!("{}['{}'] must be a string", field, key))?;
                parsed.insert(key.clone(), as_str.to_string());
            }
            Ok(parsed)
        }
        Some(_) => Err(anyhow!("{} must be an object<string,string>", field)),
    }
}

fn parse_optional_nonempty_string(value: Option<&Value>, field: &str) -> Result<Option<String>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(_) => Err(anyhow!("{} must be a string", field)),
    }
}

fn parse_command_field(value: Option<&Value>, field: &str) -> Result<Option<Vec<String>>> {
    match value {
        None => Ok(None),
        Some(Value::String(s)) => {
            let token = s.trim();
            if token.is_empty() {
                return Err(anyhow!("{} must not be empty", field));
            }
            Ok(Some(vec![token.to_string()]))
        }
        Some(Value::Array(_)) => {
            let parts = parse_string_array_field(value, field)?;
            if parts.is_empty() {
                return Err(anyhow!("{} must not be empty", field));
            }
            Ok(Some(parts))
        }
        Some(_) => Err(anyhow!("{} must be a string or string[]", field)),
    }
}

fn parse_dependency_file_staging(
    json_value: &Value,
    exp_dir: &Path,
) -> Result<Vec<DependencyFileStagingSpec>> {
    let source_name = "runtime.dependencies.file_staging";
    let raw_items = json_value.pointer("/runtime/dependencies/file_staging");

    match raw_items {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => {
            let mut parsed = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                let obj = item
                    .as_object()
                    .ok_or_else(|| anyhow!("{}[{}] must be an object", source_name, idx))?;
                let source_raw = obj
                    .get("source_from_host")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("{}[{}].source_from_host missing", source_name, idx))?;
                let destination_path = obj
                    .get("destination_path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("{}[{}].destination_path missing", source_name, idx))?
                    .trim()
                    .to_string();
                if destination_path.is_empty() {
                    return Err(anyhow!(
                        "{}[{}].destination_path must not be empty",
                        source_name,
                        idx
                    ));
                }
                let required = obj
                    .get("required")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                let read_only = obj
                    .get("read_only")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                parsed.push(DependencyFileStagingSpec {
                    source_from_host: resolve_host_path_from_spec(source_raw, exp_dir)?,
                    destination_path,
                    required,
                    read_only,
                });
            }
            Ok(parsed)
        }
        Some(_) => Err(anyhow!("{} must be an array", source_name)),
    }
}

fn resolve_agent_runtime(json_value: &Value, exp_dir: &Path) -> Result<AgentRuntimeConfig> {
    if is_clean_contract_experiment(json_value) {
        let runtime_root = json_value
            .pointer("/runtime")
            .ok_or_else(|| anyhow!("runtime is required"))?;
        let container_image = runtime_root
            .pointer("/image")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("runtime.image is required"))?;
        let command = parse_command_field(runtime_root.pointer("/command"), "runtime.command")?
            .ok_or_else(|| anyhow!("runtime.command is required"))?;
        let default_timeout_ms = runtime_root
            .pointer("/timeout_ms")
            .and_then(|v| v.as_u64())
            .filter(|v| *v > 0);

        let launch_mode = AgentLaunchMode::parse(None)?;
        return Ok(AgentRuntimeConfig {
            adapter_ref: AgentAdapterRef::default(),
            command_raw: command,
            container_image: Some(container_image),
            image_source: ImageSource::Global,
            agent_artifact: None,
            io: AgentRuntimeIoConfig {
                input_arg: DEFAULT_CLEAN_TASK_PATH.to_string(),
                output_arg: DEFAULT_CLEAN_RESULT_PATH.to_string(),
            },
            clean_contract_v1: true,
            integration_level: "cli_basic".to_string(),
            launch_mode,
            env: BTreeMap::new(),
            env_from_host: Vec::new(),
            trajectory_path: None,
            causal_extraction: None,
            default_timeout_ms,
            tracing_mode: None,
            force_container: true,
            dependency_file_staging: Vec::new(),
            dependency_services: Vec::new(),
        });
    }

    if json_value.pointer("/runtime/harness").is_some() {
        return Err(anyhow!(
            "runtime.harness is not supported; use runtime.agent"
        ));
    }
    let agent = json_value
        .pointer("/runtime/agent")
        .ok_or_else(|| anyhow!("runtime.agent is required"))?;
    if agent.pointer("/mode").is_some()
        || agent.pointer("/known_agent_ref").is_some()
        || agent.pointer("/custom_image").is_some()
        || agent.pointer("/adapter").is_some()
        || agent.pointer("/aliases").is_some()
        || agent.pointer("/overrides").is_some()
    {
        return Err(anyhow!(
            "runtime.agent hard cut: use runtime.agent.command + runtime.agent.image (+ optional runtime.agent.io)"
        ));
    }

    let trajectory_path = json_value
        .pointer("/runtime/telemetry/trajectory_path")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| Some(DEFAULT_CONTAINER_TRAJECTORY_PATH.to_string()));
    let causal_extraction = json_value
        .pointer("/runtime/telemetry/causal_extraction")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let tracing_mode = None;

    let force_container = json_value
        .pointer("/runtime/policy/sandbox/mode")
        .and_then(|v| v.as_str())
        == Some("container");
    let dependency_file_staging = parse_dependency_file_staging(json_value, exp_dir)?;
    let dependency_services = json_value
        .pointer("/runtime/dependencies/services")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let runtime_agent_image = agent
        .pointer("/image")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let image_source = ImageSource::parse(agent.pointer("/image_source").and_then(|v| v.as_str()))?;
    let agent_artifact =
        parse_optional_nonempty_string(agent.pointer("/artifact"), "runtime.agent.artifact")?
            .map(|p| normalize_path(&exp_dir.join(p)));
    if image_source == ImageSource::PerTask && agent_artifact.is_none() {
        return Err(anyhow!(
            "runtime.agent.artifact is required when runtime.agent.image_source='per_task'"
        ));
    }

    let command = parse_command_field(agent.pointer("/command"), "runtime.agent.command")?
        .ok_or_else(|| anyhow!("runtime.agent.command is required"))?;
    let integration_level = agent
        .pointer("/integration_level")
        .and_then(|v| v.as_str())
        .unwrap_or("cli_basic")
        .to_string();
    let adapter_ref = AgentAdapterRef::default();
    let launch_mode =
        AgentLaunchMode::parse(agent.pointer("/launch/mode").and_then(|v| v.as_str()))?;
    let default_timeout_ms = agent
        .pointer("/default_timeout_ms")
        .and_then(|v| v.as_u64())
        .filter(|v| *v > 0);
    let env = parse_string_map_field(agent.pointer("/env"), "runtime.agent.env")?;
    let env_from_host = parse_string_array_field(
        agent.pointer("/env_from_host"),
        "runtime.agent.env_from_host",
    )?;
    let input_arg = agent
        .pointer("/io/input_arg")
        .and_then(|v| v.as_str())
        .unwrap_or("--input")
        .trim()
        .to_string();
    if input_arg.is_empty() {
        return Err(anyhow!("runtime.agent.io.input_arg must not be empty"));
    }
    let output_arg = agent
        .pointer("/io/output_arg")
        .and_then(|v| v.as_str())
        .unwrap_or("--output")
        .trim()
        .to_string();
    if output_arg.is_empty() {
        return Err(anyhow!("runtime.agent.io.output_arg must not be empty"));
    }

    let container_image = runtime_agent_image;
    let force_container = force_container || container_image.is_some();

    Ok(AgentRuntimeConfig {
        adapter_ref,
        command_raw: command,
        container_image,
        image_source,
        agent_artifact,
        io: AgentRuntimeIoConfig {
            input_arg,
            output_arg,
        },
        clean_contract_v1: false,
        integration_level,
        launch_mode,
        env,
        env_from_host,
        trajectory_path,
        causal_extraction,
        default_timeout_ms,
        tracing_mode,
        force_container,
        dependency_file_staging,
        dependency_services,
    })
}

fn resolve_agent_runtime_env(
    runtime_agent: &AgentRuntimeConfig,
) -> Result<BTreeMap<String, String>> {
    let mut merged = runtime_agent.env.clone();
    for key in &runtime_agent.env_from_host {
        let value = std::env::var(key).map_err(|_| {
            anyhow!(
                "missing required host env var for runtime agent env_from_host: {}",
                key
            )
        })?;
        merged.insert(key.clone(), value);
    }
    Ok(merged)
}

#[derive(Clone)]
struct VariantRuntimeProfile {
    experiment: Value,
    variant_args: Vec<String>,
    agent_runtime: AgentRuntimeConfig,
    agent_runtime_env: BTreeMap<String, String>,
    invocation_source: String,
    invocation_default_timeout_ms: Option<u64>,
    executor_kind: ExecutorKind,
    container_mode: bool,
    configured_network_mode: String,
    effective_network_mode: String,
}

fn resolve_variant_runtime_profile(
    experiment: &Value,
    variant: &Variant,
    project_root: &Path,
    use_container: bool,
    behavior: &RunBehavior,
    execution: &RunExecutionOptions,
) -> Result<VariantRuntimeProfile> {
    let variant_experiment = resolve_runtime_for_variant(experiment, variant)?;
    validate_required_fields(&variant_experiment)?;

    let mut agent_runtime = resolve_agent_runtime(&variant_experiment, project_root)?;
    let configured_network_mode = configured_network_mode(&variant_experiment)?;
    let effective_network_mode = behavior
        .network_mode_override
        .as_deref()
        .unwrap_or(configured_network_mode.as_str())
        .to_string();
    if behavior.require_network_none && effective_network_mode != "none" {
        return Err(anyhow!(
            "run-experiment requires network mode 'none' (variant '{}', effective mode: {})",
            variant.id,
            effective_network_mode
        ));
    }

    let executor_kind = execution.executor.unwrap_or_else(|| {
        if use_container || agent_runtime.force_container {
            ExecutorKind::LocalDocker
        } else {
            ExecutorKind::LocalProcess
        }
    });
    let container_mode = matches!(executor_kind, ExecutorKind::LocalDocker);
    agent_runtime.command_raw =
        resolve_agent_runtime_command(&agent_runtime.command_raw, project_root, container_mode);
    validate_agent_runtime_command(&agent_runtime.command_raw, project_root, container_mode)?;
    let invocation_default_timeout_ms = agent_runtime.default_timeout_ms;
    let mut agent_runtime_env = resolve_agent_runtime_env(&agent_runtime)?;
    for (key, value) in &variant.env {
        agent_runtime_env.insert(key.clone(), value.clone());
    }

    Ok(VariantRuntimeProfile {
        experiment: variant_experiment,
        variant_args: variant.args.clone(),
        agent_runtime,
        agent_runtime_env,
        invocation_source: "runtime_agent".to_string(),
        invocation_default_timeout_ms,
        executor_kind,
        container_mode,
        configured_network_mode,
        effective_network_mode,
    })
}

struct TrialPaths {
    in_dir: PathBuf,
    workspace: PathBuf,
    state: PathBuf,
    deps: PathBuf,
    dataset: PathBuf,
    out: PathBuf,
    tmp: PathBuf,
    runtime: RunnerRuntimeHostPaths,
    dataset_src: PathBuf,
    exp_dir: PathBuf,
}

impl TrialPaths {
    fn new(trial_dir: &Path, exp_dir: &Path, dataset_src: &Path) -> Result<Self> {
        let runtime = runner_runtime_host_paths(trial_dir);
        Ok(Self {
            in_dir: runtime.in_dir.clone(),
            workspace: runtime.workspace_dir.clone(),
            state: runtime.state_dir.clone(),
            deps: runtime.deps_dir.clone(),
            dataset: trial_dir.join("dataset"),
            out: runtime.out_dir.clone(),
            tmp: runtime.tmp_dir.clone(),
            runtime,
            dataset_src: dataset_src.to_path_buf(),
            exp_dir: exp_dir.to_path_buf(),
        })
    }

    fn prepare(&self, seed_workspace_from_exp_dir: bool) -> Result<()> {
        ensure_dir(&self.in_dir)?;
        ensure_dir(&self.workspace)?;
        ensure_dir(&self.state)?;
        ensure_dir(&self.deps)?;
        ensure_dir(&self.dataset)?;
        ensure_dir(&self.out)?;
        ensure_dir(&self.tmp)?;
        if seed_workspace_from_exp_dir {
            copy_dir_filtered(
                &self.exp_dir,
                &self.workspace,
                &[
                    ".lab",
                    ".git",
                    "node_modules",
                    ".venv",
                    "__pycache__",
                    ".tox",
                    ".mypy_cache",
                    ".pytest_cache",
                    ".ruff_cache",
                    "target",
                    "rust/target",
                    ".next",
                    ".nuxt",
                    ".turbo",
                    ".nx",
                    "coverage",
                    ".gradle",
                ],
            )?;
        }
        fs::copy(
            &self.dataset_src,
            self.dataset.join(self.dataset_src.file_name().unwrap()),
        )?;
        Ok(())
    }
}

fn build_agent_task(
    json_value: &Value,
    run_id: &str,
    trial_id: &str,
    variant: &Variant,
    task_idx: usize,
    repl: usize,
    task_boundary: &TaskBoundaryMaterialization,
    runtime_agent: &AgentRuntimeConfig,
) -> Value {
    let normalized_task_payload = normalize_task_prompt_aliases(&task_boundary.task_payload);
    if is_clean_contract_experiment(json_value) {
        return normalized_task_payload;
    }

    let mut policy = json_value
        .pointer("/runtime/policy")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if policy.pointer("/timeout_ms").is_none() {
        set_json_pointer_value(&mut policy, "/timeout_ms", json!(600000)).ok();
    }
    if let Some(trial_seconds) = task_boundary.limits.trial_seconds {
        set_json_pointer_value(&mut policy, "/timeout_ms", json!(trial_seconds * 1000)).ok();
    }

    let mut input = json!({
        "schema_version": "agent_task_v1",
        "ids": {
            "run_id": run_id,
            "trial_id": trial_id,
            "variant_id": variant.id,
            "task_id": task_boundary.task_payload.get("id").and_then(|v| v.as_str()).unwrap_or(&format!("task_{}", task_idx)),
            "repl_idx": repl
        },
        "task": normalized_task_payload,
        "bindings": variant.bindings.clone(),
        "dependencies": {
            "services": runtime_agent.dependency_services.clone()
        },
        "policy": policy,
    });
    if let Some(task_boundary_ext) = task_boundary_ext_value(task_boundary) {
        if let Some(obj) = input.as_object_mut() {
            obj.insert(
                "ext".to_string(),
                json!({ "task_boundary_v1": task_boundary_ext }),
            );
        }
    }
    input
}

fn normalize_task_prompt_aliases(task_payload: &Value) -> Value {
    let mut normalized = task_payload.clone();
    let canonical_prompt = normalized
        .pointer("/input/prompt")
        .and_then(Value::as_str)
        .or_else(|| normalized.pointer("/prompt").and_then(Value::as_str))
        .or_else(|| {
            normalized
                .pointer("/swebench/input/prompt")
                .and_then(Value::as_str)
        })
        .map(str::to_string);

    let Some(prompt) = canonical_prompt else {
        return normalized;
    };

    let Some(root_obj) = normalized.as_object_mut() else {
        return normalized;
    };

    // Canonicalize to task.input.prompt for runtime/harness consumption.
    let input_slot = root_obj
        .entry("input".to_string())
        .or_insert_with(|| json!({}));
    if !input_slot.is_object() {
        *input_slot = json!({});
    }
    if let Some(input_obj) = input_slot.as_object_mut() {
        input_obj.insert("prompt".to_string(), Value::String(prompt.clone()));
    }

    // Drop duplicated top-level prompt alias if it is identical.
    let drop_top_level_prompt = root_obj
        .get("prompt")
        .and_then(Value::as_str)
        .is_some_and(|value| value == prompt);
    if drop_top_level_prompt {
        root_obj.remove("prompt");
    }

    // Drop duplicated swebench.input.prompt alias if it is identical.
    if let Some(swebench_slot) = root_obj.get_mut("swebench") {
        if let Some(swebench_obj) = swebench_slot.as_object_mut() {
            let mut remove_input = false;
            if let Some(swebench_input_slot) = swebench_obj.get_mut("input") {
                if let Some(swebench_input_obj) = swebench_input_slot.as_object_mut() {
                    let drop_nested_prompt = swebench_input_obj
                        .get("prompt")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == prompt);
                    if drop_nested_prompt {
                        swebench_input_obj.remove("prompt");
                    }
                    if swebench_input_obj.is_empty() {
                        remove_input = true;
                    }
                }
            }
            if remove_input {
                swebench_obj.remove("input");
            }
        }
    }

    normalized
}

fn sanitize_for_fs(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "chain".to_string()
    } else {
        out
    }
}

fn append_jsonl(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, value)?;
    writeln!(&mut file)?;
    Ok(())
}

fn is_workspace_evidence_excluded(rel: &Path) -> bool {
    WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES
        .iter()
        .any(|prefix| rel.starts_with(prefix))
}

fn collect_workspace_snapshot_manifest(workspace: &Path) -> Result<Value> {
    let mut files: Vec<(String, String, u64)> = Vec::new();
    if workspace.exists() {
        let walker = walkdir::WalkDir::new(workspace).into_iter();
        for entry in walker {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let rel_path = entry.path().strip_prefix(workspace).unwrap_or(entry.path());
            if is_workspace_evidence_excluded(rel_path) {
                continue;
            }
            let rel = rel_path.to_string_lossy().to_string();
            let digest = sha256_file(entry.path())?;
            let size = entry.metadata()?.len();
            files.push((rel, digest, size));
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let total_bytes = files.iter().map(|(_, _, sz)| *sz).sum::<u64>();
    let rows = files
        .into_iter()
        .map(|(path, digest, size_bytes)| {
            json!({
                "path": path,
                "digest": digest,
                "size_bytes": size_bytes
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": "workspace_snapshot_v1",
        "captured_at": Utc::now().to_rfc3339(),
        "file_count": rows.len(),
        "total_bytes": total_bytes,
        "files": rows
    }))
}

fn snapshot_file_map(snapshot_manifest: &Value) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Some(arr) = snapshot_manifest.get("files").and_then(|v| v.as_array()) {
        for row in arr {
            let path = row.get("path").and_then(|v| v.as_str());
            let digest = row.get("digest").and_then(|v| v.as_str());
            if let (Some(path), Some(digest)) = (path, digest) {
                map.insert(path.to_string(), digest.to_string());
            }
        }
    }
    map
}

fn diff_workspace_snapshots(prev: &Value, post: &Value) -> Value {
    let prev_map = snapshot_file_map(prev);
    let post_map = snapshot_file_map(post);

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut modified = Vec::new();

    for (path, digest) in post_map.iter() {
        match prev_map.get(path) {
            None => added.push(path.clone()),
            Some(prev_digest) if prev_digest != digest => modified.push(path.clone()),
            _ => {}
        }
    }
    for path in prev_map.keys() {
        if !post_map.contains_key(path) {
            removed.push(path.clone());
        }
    }

    json!({
        "schema_version": "workspace_diff_v1",
        "captured_at": Utc::now().to_rfc3339(),
        "added": added,
        "removed": removed,
        "modified": modified,
        "summary": {
            "added_files": added.len(),
            "removed_files": removed.len(),
            "modified_files": modified.len()
        }
    })
}

fn derive_patch_from_diff(diff: &Value) -> Value {
    json!({
        "schema_version": "workspace_patch_v1",
        "format": "file_digest_delta",
        "generated_at": Utc::now().to_rfc3339(),
        "added": diff.get("added").cloned().unwrap_or(json!([])),
        "removed": diff.get("removed").cloned().unwrap_or(json!([])),
        "modified": diff.get("modified").cloned().unwrap_or(json!([])),
    })
}

fn restore_workspace_from_snapshot(snapshot_dir: &Path, workspace_dir: &Path) -> Result<()> {
    if workspace_dir.exists() {
        fs::remove_dir_all(workspace_dir)?;
    }
    ensure_dir(workspace_dir)?;
    copy_dir_filtered(snapshot_dir, workspace_dir, &[])?;
    Ok(())
}

fn resolve_chain_label(task_payload: &Value, task_id: &str, state_policy: StatePolicy) -> String {
    let explicit = task_payload
        .get("chain_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    if let Some(label) = explicit {
        return label;
    }
    match state_policy {
        StatePolicy::PersistPerTask => task_id.to_string(),
        StatePolicy::Accumulate => "global".to_string(),
        StatePolicy::IsolatePerTrial => task_id.to_string(),
    }
}

fn chain_root_workspace_dir_name(trial_id: &str) -> String {
    format!("chain_root_workspace_{}", sanitize_for_fs(trial_id))
}

fn rel_to_run_dir(path: &Path, run_dir: &Path) -> String {
    path.strip_prefix(run_dir)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

struct ProcessRunResult {
    status: String,
    stdout: String,
    stderr: String,
}

struct PreparedTrialIo {
    output_host: PathBuf,
    events_host: PathBuf,
    task_path: String,
    bindings_path: String,
    dependencies_path: String,
    policy_path: String,
    result_path: String,
    trajectory_path: String,
}

fn resolve_trial_timeout_ms(
    input: &Value,
    invocation_default_timeout_ms: Option<u64>,
) -> Option<u64> {
    input
        .pointer("/policy/timeout_ms")
        .and_then(|v| v.as_u64())
        .or(invocation_default_timeout_ms)
}

fn output_peer_path(output_path: &str, file_name: &str) -> String {
    let output = Path::new(output_path);
    if let Some(parent) = output.parent() {
        return parent.join(file_name).to_string_lossy().to_string();
    }
    file_name.to_string()
}

fn build_runtime_contract_env(
    run_id: &str,
    input: &Value,
    io: &PreparedTrialIo,
    timeout_ms: Option<u64>,
    clean_contract_v1: bool,
) -> BTreeMap<String, String> {
    if clean_contract_v1 {
        return BTreeMap::new();
    }
    let trial_id = input
        .pointer("/ids/trial_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let variant_id = input
        .pointer("/ids/variant_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let task_id = input
        .pointer("/ids/task_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let task_image = input
        .pointer("/task/image")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    let repl_idx = input
        .pointer("/ids/repl_idx")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let mut env = BTreeMap::new();
    env.insert(AGENTLAB_ENV_TASK_PATH.to_string(), io.task_path.clone());
    env.insert(
        AGENTLAB_ENV_BINDINGS_PATH.to_string(),
        io.bindings_path.clone(),
    );
    env.insert(
        AGENTLAB_ENV_DEPENDENCIES_PATH.to_string(),
        io.dependencies_path.clone(),
    );
    env.insert(AGENTLAB_ENV_POLICY_PATH.to_string(), io.policy_path.clone());
    env.insert(AGENTLAB_ENV_RESULT_PATH.to_string(), io.result_path.clone());
    env.insert(
        AGENTLAB_ENV_TRAJECTORY_PATH.to_string(),
        io.trajectory_path.clone(),
    );
    env.insert(AGENTLAB_ENV_RUN_ID.to_string(), run_id.to_string());
    env.insert(AGENTLAB_ENV_TRIAL_ID.to_string(), trial_id.to_string());
    env.insert(AGENTLAB_ENV_VARIANT_ID.to_string(), variant_id.to_string());
    env.insert(AGENTLAB_ENV_TASK_ID.to_string(), task_id.to_string());
    if let Some(task_image) = task_image {
        env.insert(AGENTLAB_ENV_TASK_IMAGE.to_string(), task_image);
    }
    env.insert(
        AGENTLAB_ENV_BENCHMARK_PREDICTION_PATH.to_string(),
        output_peer_path(&io.result_path, BENCHMARK_PREDICTION_FILENAME),
    );
    env.insert(
        AGENTLAB_ENV_BENCHMARK_SCORE_PATH.to_string(),
        output_peer_path(&io.result_path, BENCHMARK_SCORE_FILENAME),
    );
    env.insert(AGENTLAB_ENV_REPL_IDX.to_string(), repl_idx.to_string());
    if let Some(timeout_ms) = timeout_ms {
        env.insert(AGENTLAB_ENV_TIMEOUT_MS.to_string(), timeout_ms.to_string());
    }
    env
}

fn apply_agentlab_template(raw: &str, env: &BTreeMap<String, String>) -> String {
    let mut rendered = raw.to_string();
    for (key, value) in env {
        if !key.starts_with("AGENTLAB_") && key != "WORKSPACE" {
            continue;
        }
        let needle = format!("${{{}}}", key);
        if rendered.contains(&needle) {
            rendered = rendered.replace(&needle, value);
        }
    }
    rendered
}

fn apply_agentlab_template_to_command(
    command: &[String],
    env: &BTreeMap<String, String>,
) -> Vec<String> {
    command
        .iter()
        .map(|part| apply_agentlab_template(part, env))
        .collect::<Vec<_>>()
}

fn command_contract_capabilities() -> AgentAdapterCapabilities {
    AgentAdapterCapabilities {
        pause: true,
        control_ack: true,
        event_stream: true,
        strict_replay: false,
    }
}

fn run_command_contract_trial(request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
    write_adapter_continue_control(&request.trial_paths.runtime.control)?;
    if !request.container_mode && request.setup_command.is_some() {
        return Err(anyhow!(
            "setup command is only supported for container runs"
        ));
    }
    if request.container_mode {
        run_builtin_adapter_container(request)
    } else {
        run_builtin_adapter_local(request)
    }
}

fn pause_command_contract_trial(request: &AdapterPauseRequest<'_>) -> Result<AdapterPauseAck> {
    let control_path = Path::new(&request.control.command_path);
    let events_path =
        request.control.events_path.as_deref().ok_or_else(|| {
            anyhow!("pause_unsupported: active adapter control missing events path")
        })?;
    let events_path = Path::new(events_path);
    let deadline = Instant::now() + request.timeout;

    let seq_checkpoint = read_control_seq(control_path)? + 1;
    let checkpoint_version = write_adapter_control_action(
        control_path,
        seq_checkpoint,
        "checkpoint",
        Some(request.label),
        "lab_pause",
    )?;
    wait_for_adapter_control_ack(events_path, "checkpoint", &checkpoint_version, deadline)?;

    let seq_stop = read_control_seq(control_path)? + 1;
    let stop_version = write_adapter_control_action(
        control_path,
        seq_stop,
        "stop",
        Some(request.label),
        "lab_pause",
    )?;
    wait_for_adapter_control_ack(events_path, "stop", &stop_version, deadline)?;

    Ok(AdapterPauseAck {
        checkpoint_acked: true,
        stop_acked: true,
    })
}

fn prebuilt_adapter_profile_value(flavor: PrebuiltAdapterFlavor) -> &'static str {
    match flavor {
        PrebuiltAdapterFlavor::CodexCli => "codex_cli",
        PrebuiltAdapterFlavor::RexJesus => "rex_jesus",
    }
}

impl AgentAdapter for BuiltinCommandAdapter {
    fn capabilities(&self) -> AgentAdapterCapabilities {
        command_contract_capabilities()
    }

    fn run_trial(&self, request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
        run_command_contract_trial(request)
    }

    fn pause_trial(&self, request: &AdapterPauseRequest<'_>) -> Result<AdapterPauseAck> {
        pause_command_contract_trial(request)
    }
}

impl AgentAdapter for PrebuiltCommandAdapter {
    fn capabilities(&self) -> AgentAdapterCapabilities {
        command_contract_capabilities()
    }

    fn run_trial(&self, request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
        let mut adapter_overrides = request.runtime_overrides_env.clone();
        adapter_overrides.insert(
            "AGENTLAB_PREBUILT_ADAPTER".to_string(),
            prebuilt_adapter_profile_value(self.flavor).to_string(),
        );
        adapter_overrides.insert(
            "AGENTLAB_PREBUILT_ADAPTER_ID".to_string(),
            request.runtime.adapter_ref.id.clone(),
        );
        let prebuilt_request = AdapterRunRequest {
            runtime_experiment: request.runtime_experiment,
            runtime: request.runtime,
            variant_args: request.variant_args,
            runtime_env: request.runtime_env,
            runtime_overrides_env: &adapter_overrides,
            container_mode: request.container_mode,
            trial_paths: request.trial_paths,
            dynamic_mounts: request.dynamic_mounts,
            io_paths: request.io_paths,
            network_mode: request.network_mode,
            setup_command: request.setup_command,
            benchmark_adapter: request.benchmark_adapter,
            benchmark_grading_enabled: request.benchmark_grading_enabled,
            run_id: request.run_id,
            task_image: request.task_image,
            task_workspace: request.task_workspace,
            agent_artifact: request.agent_artifact,
        };
        run_command_contract_trial(&prebuilt_request)
    }

    fn pause_trial(&self, request: &AdapterPauseRequest<'_>) -> Result<AdapterPauseAck> {
        pause_command_contract_trial(request)
    }
}

fn run_builtin_adapter_local(request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
    let command = resolve_runtime_agent_command(request)?;
    let mut cmd = Command::new(&command[0]);
    cmd.args(&command[1..]);
    cmd.current_dir(&request.trial_paths.workspace);
    for (key, value) in request.runtime_overrides_env {
        cmd.env(key, value);
    }
    for (key, value) in request.runtime_env {
        cmd.env(key, value);
    }
    run_adapter_process(cmd, &request.io_paths.output_host, None)
}

fn resolve_benchmark_grader_command(request: &AdapterRunRequest<'_>) -> Option<Vec<String>> {
    if request.runtime.clean_contract_v1 || !request.benchmark_grading_enabled {
        return None;
    }
    let adapter = request.benchmark_adapter?;
    if adapter.command.is_empty() {
        return None;
    }
    let mut render_env = request.runtime_overrides_env.clone();
    for (key, value) in request.runtime_env {
        render_env.insert(key.clone(), value.clone());
    }
    let workspace = if request.container_mode {
        resolve_container_workspace(request)
            .unwrap_or(AGENTLAB_CONTRACT_WORKSPACE_DIR)
            .to_string()
    } else {
        request.trial_paths.workspace.to_string_lossy().to_string()
    };
    render_env.insert("WORKSPACE".to_string(), workspace);
    Some(apply_agentlab_template_to_command(
        &adapter.command,
        &render_env,
    ))
}

fn run_builtin_adapter_container(request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
    if request.network_mode == "allowlist_enforced" {
        return Err(anyhow!("allowlist_enforced not implemented in Rust runner"));
    }
    let image = resolve_container_image(request)?;
    let workspace = resolve_container_workspace(request);
    if request.runtime.image_source == ImageSource::PerTask {
        if request.agent_artifact.is_none() {
            return Err(anyhow!(
                "runtime.agent.artifact is required when runtime.agent.image_source='per_task'"
            ));
        }
        if let Some(workspace) = workspace {
            if !workspace.starts_with(AGENTLAB_CONTRACT_WORKSPACE_DIR) {
                let root_read_only = request
                    .runtime_experiment
                    .pointer("/runtime/policy/sandbox/root_read_only")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                if root_read_only {
                    return Err(anyhow!(
                        "per-task image workspace '{}' requires runtime.policy.sandbox.root_read_only=false",
                        workspace
                    ));
                }
            }
        }
    }

    if let Some(artifact) = request.agent_artifact {
        run_injected_container(request, &image, artifact, workspace)
    } else {
        run_baked_container(request, &image, workspace)
    }
}

fn resolve_container_image(request: &AdapterRunRequest<'_>) -> Result<String> {
    let task_image = request
        .task_image
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match request.runtime.image_source {
        ImageSource::PerTask => task_image.map(ToString::to_string).ok_or_else(|| {
            anyhow!("task.image is required when runtime.agent.image_source='per_task'")
        }),
        ImageSource::Global => request
            .runtime
            .container_image
            .clone()
            .ok_or_else(|| anyhow!("container image required for container mode")),
    }
}

fn resolve_container_workspace<'a>(request: &'a AdapterRunRequest<'_>) -> Option<&'a str> {
    request
        .task_workspace
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            if request.runtime.clean_contract_v1 {
                None
            } else {
                Some(AGENTLAB_CONTRACT_WORKSPACE_DIR)
            }
        })
}

fn append_container_sandbox_args(
    cmd: &mut Command,
    request: &AdapterRunRequest<'_>,
    workspace: Option<&str>,
) {
    let root_read_only = if request.runtime.clean_contract_v1 {
        true
    } else {
        request
            .runtime_experiment
            .pointer("/runtime/policy/sandbox/root_read_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
    };
    if root_read_only {
        cmd.arg("--read-only");
    }

    if !request.runtime.clean_contract_v1 {
        let run_as_user = request
            .runtime_experiment
            .pointer("/runtime/policy/sandbox/run_as_user")
            .and_then(|v| v.as_str());
        if let Some(user) = run_as_user {
            cmd.args(["-u", user]);
        }
    }

    if request.network_mode == "none" {
        cmd.arg("--network=none");
    }

    let no_new_privileges = if request.runtime.clean_contract_v1 {
        true
    } else {
        request
            .runtime_experiment
            .pointer("/runtime/policy/sandbox/hardening/no_new_privileges")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
    };
    if no_new_privileges {
        cmd.args(["--security-opt", "no-new-privileges"]);
    }

    let drop_all_caps = if request.runtime.clean_contract_v1 {
        true
    } else {
        request
            .runtime_experiment
            .pointer("/runtime/policy/sandbox/hardening/drop_all_caps")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
    };
    if drop_all_caps {
        cmd.args(["--cap-drop", "ALL"]);
    }

    let cpu_limit = if request.runtime.clean_contract_v1 {
        request
            .runtime_experiment
            .pointer("/runtime/resources/cpus")
            .and_then(|v| v.as_u64())
    } else {
        request
            .runtime_experiment
            .pointer("/runtime/policy/sandbox/resources/cpu_count")
            .and_then(|v| v.as_u64())
    };
    if let Some(cpu) = cpu_limit {
        cmd.arg("--cpus").arg(cpu.to_string());
    }
    let memory_limit_mb = if request.runtime.clean_contract_v1 {
        request
            .runtime_experiment
            .pointer("/runtime/resources/memory_mb")
            .and_then(|v| v.as_u64())
    } else {
        request
            .runtime_experiment
            .pointer("/runtime/policy/sandbox/resources/memory_mb")
            .and_then(|v| v.as_u64())
    };
    if let Some(mem) = memory_limit_mb {
        cmd.arg("--memory").arg(format!("{}m", mem));
    }

    if request.runtime.clean_contract_v1 {
        cmd.args([
            "-v",
            &format!(
                "{}:{}:ro",
                request.trial_paths.runtime.task.display(),
                HARNESS_TASK_PATH
            ),
        ]);
        cmd.args([
            "-v",
            &format!("{}:{}", request.trial_paths.out.display(), HARNESS_OUT_DIR),
        ]);
    } else {
        cmd.args([
            "-v",
            &format!(
                "{}:{}:ro",
                request.trial_paths.in_dir.display(),
                AGENTLAB_CONTRACT_IN_DIR
            ),
        ]);
        cmd.args([
            "-v",
            &format!(
                "{}:{}",
                request.trial_paths.out.display(),
                AGENTLAB_CONTRACT_OUT_DIR
            ),
        ]);
        cmd.args([
            "-v",
            &format!(
                "{}:{}",
                request.trial_paths.state.display(),
                AGENTLAB_CONTRACT_STATE_DIR
            ),
        ]);
        cmd.args([
            "-v",
            &format!(
                "{}:{}",
                request.trial_paths.deps.display(),
                AGENTLAB_CONTRACT_DEPS_DIR
            ),
        ]);
        cmd.args([
            "-v",
            &format!(
                "{}:{}",
                request.trial_paths.workspace.display(),
                AGENTLAB_CONTRACT_WORKSPACE_DIR
            ),
        ]);
        cmd.args([
            "-v",
            &format!("{}:/dataset:ro", request.trial_paths.dataset.display()),
        ]);
        for mount in request.dynamic_mounts {
            cmd.args([
                "-v",
                &format!("{}:{}:ro", mount.host_path.display(), mount.mount_path),
            ]);
        }
        cmd.args(["--tmpfs", "/tmp:rw"]);
        if let Some(workspace) = workspace {
            cmd.args(["-w", workspace]);
        }
    }
}

fn append_container_env_args(
    cmd: &mut Command,
    request: &AdapterRunRequest<'_>,
    workspace: Option<&str>,
) {
    for (key, value) in request.runtime_overrides_env {
        cmd.arg("-e").arg(format!("{}={}", key, value));
    }
    if !request.runtime.clean_contract_v1 {
        for (key, value) in request.runtime_env {
            cmd.arg("-e").arg(format!("{}={}", key, value));
        }
    }
    if let Some(workspace) = workspace {
        cmd.arg("-e").arg(format!("WORKSPACE={}", workspace));
    }
}

fn append_container_entrypoint(
    cmd: &mut Command,
    request: &AdapterRunRequest<'_>,
    command: &[String],
    grader_command: Option<Vec<String>>,
) {
    if let Some(grader_command) = grader_command {
        let setup_block = if let Some(setup) = request.setup_command {
            format!("{}\nsetup_status=$?", setup)
        } else {
            "setup_status=0".to_string()
        };
        let grade_error_marker_path = output_peer_path(
            &request.io_paths.result_path,
            BENCHMARK_GRADE_ERROR_FILENAME,
        );
        let wrapped = format!(
            "set +e\n\
             rm -f {marker}\n\
             {setup}\n\
             if [ \"$setup_status\" -ne 0 ]; then\n\
               exit \"$setup_status\"\n\
             fi\n\
             {agent}\n\
             agent_status=$?\n\
             export {agent_exit_env}=\"$agent_status\"\n\
             {grader}\n\
             grader_status=$?\n\
             if [ \"$grader_status\" -ne 0 ]; then\n\
               printf '%s\\n' \"grader_command_failed:$grader_status\" > {marker}\n\
             fi\n\
             if [ ! -s \"${{{score_env}}}\" ]; then\n\
               printf '%s\\n' \"score_record_missing\" >> {marker}\n\
             fi\n\
             if [ -s {marker} ]; then\n\
               exit {grade_error_code}\n\
             fi\n\
             if [ \"$agent_status\" -ne 0 ]; then\n\
               exit \"$agent_status\"\n\
             fi\n\
             exit 0",
            marker = shell_quote(&grade_error_marker_path),
            setup = setup_block,
            agent = shell_join(command),
            agent_exit_env = AGENTLAB_ENV_AGENT_EXIT_STATUS,
            grader = shell_join(&grader_command),
            score_env = AGENTLAB_ENV_BENCHMARK_SCORE_PATH,
            grade_error_code = BENCHMARK_GRADING_POLICY_EXIT_CODE,
        );
        cmd.arg("/bin/sh");
        cmd.arg("-lc");
        cmd.arg(wrapped);
    } else if let Some(setup) = request.setup_command {
        let wrapped = format!("{} && exec {}", setup, shell_join(command));
        cmd.arg("/bin/sh");
        cmd.arg("-lc");
        cmd.arg(wrapped);
    } else {
        cmd.args(command);
    }
}

fn run_baked_container(
    request: &AdapterRunRequest<'_>,
    image: &str,
    workspace: Option<&str>,
) -> Result<ProcessRunResult> {
    let command = resolve_runtime_agent_command(request)?;
    let grader_command = resolve_benchmark_grader_command(request);

    let mut cmd = Command::new("docker");
    cmd.arg("run").arg("--rm");
    append_container_sandbox_args(&mut cmd, request, workspace);
    append_container_env_args(&mut cmd, request, workspace);
    cmd.arg(image);
    append_container_entrypoint(&mut cmd, request, &command, grader_command);
    run_adapter_process(cmd, &request.io_paths.output_host, None)
}

struct ContainerCleanupGuard {
    container_id: String,
}

impl Drop for ContainerCleanupGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_id])
            .output();
    }
}

fn run_checked_command(mut cmd: Command, step: &str) -> Result<std::process::Output> {
    let out = cmd.output()?;
    if out.status.success() {
        return Ok(out);
    }
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let detail = if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        "command exited non-zero".to_string()
    };
    Err(anyhow!("{}: {}", step, detail))
}

fn run_injected_container(
    request: &AdapterRunRequest<'_>,
    image: &str,
    artifact: &Path,
    workspace: Option<&str>,
) -> Result<ProcessRunResult> {
    if !artifact.exists() {
        return Err(anyhow!(
            "runtime.agent.artifact not found: {}",
            artifact.display()
        ));
    }

    let command = resolve_runtime_agent_command(request)?;
    let grader_command = resolve_benchmark_grader_command(request);

    let mut create = Command::new("docker");
    create.arg("create");
    append_container_sandbox_args(&mut create, request, workspace);
    append_container_env_args(&mut create, request, workspace);
    create.arg(image);
    create.args(["tail", "-f", "/dev/null"]);
    let create_out = run_checked_command(create, "docker create failed")?;
    let container_id = String::from_utf8_lossy(&create_out.stdout)
        .trim()
        .to_string();
    if container_id.is_empty() {
        return Err(anyhow!("docker create failed: missing container id"));
    }
    let _cleanup = ContainerCleanupGuard {
        container_id: container_id.clone(),
    };

    let mut start = Command::new("docker");
    start.args(["start", &container_id]);
    run_checked_command(start, "docker start failed")?;

    let mut copy = Command::new("docker");
    copy.arg("cp");
    copy.arg(artifact);
    copy.arg(format!("{}:/tmp/agent.tar.gz", container_id));
    run_checked_command(copy, "docker cp failed")?;

    let mut unpack = Command::new("docker");
    unpack.args(["exec", &container_id, "/bin/sh", "-lc"]);
    unpack.arg(
        "mkdir -p /opt/agent && tar xzf /tmp/agent.tar.gz -C /opt/agent && rm -f /tmp/agent.tar.gz",
    );
    run_checked_command(unpack, "docker exec artifact unpack failed")?;

    let mut exec = Command::new("docker");
    exec.args(["exec"]);
    if let Some(workspace) = workspace {
        exec.args(["-w", workspace]);
    }
    append_container_env_args(&mut exec, request, workspace);
    exec.arg(&container_id);
    append_container_entrypoint(&mut exec, request, &command, grader_command);
    run_adapter_process(exec, &request.io_paths.output_host, None)
}

fn append_runtime_io_arg(command: &mut Vec<String>, arg_spec: &str, path: &str) -> Result<()> {
    let trimmed = arg_spec.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("runtime.agent.io argument spec must not be empty"));
    }
    if trimmed.contains("{path}") {
        command.push(trimmed.replace("{path}", path));
    } else if trimmed.ends_with('=') {
        command.push(format!("{}{}", trimmed, path));
    } else {
        command.push(trimmed.to_string());
        command.push(path.to_string());
    }
    Ok(())
}

fn resolve_runtime_agent_command(request: &AdapterRunRequest<'_>) -> Result<Vec<String>> {
    let mut render_env = request.runtime_env.clone();
    let workspace = if request.container_mode {
        resolve_container_workspace(request)
            .unwrap_or(AGENTLAB_CONTRACT_WORKSPACE_DIR)
            .to_string()
    } else {
        request.trial_paths.workspace.to_string_lossy().to_string()
    };
    render_env.insert("WORKSPACE".to_string(), workspace);
    let rendered = apply_agentlab_template_to_command(&request.runtime.command_raw, &render_env);
    if rendered.is_empty() {
        return Err(anyhow!("resolved runtime.agent command is empty"));
    }
    let mut command = rendered;
    if request.runtime.clean_contract_v1 {
        command.extend(request.variant_args.iter().cloned());
        command.push(request.io_paths.task_path.clone());
        command.push(request.io_paths.result_path.clone());
    } else {
        append_runtime_io_arg(
            &mut command,
            &request.runtime.io.input_arg,
            &request.io_paths.task_path,
        )?;
        append_runtime_io_arg(
            &mut command,
            &request.runtime.io.output_arg,
            &request.io_paths.result_path,
        )?;
    }
    Ok(command)
}

fn resolve_agent_runtime_command(
    command: &[String],
    exp_dir: &Path,
    container_mode: bool,
) -> Vec<String> {
    if container_mode {
        // Keep runtime.command tokens literal in containers so users can run "./agent.py"
        // from /agentlab/workspace without leaking host absolute paths into experiment YAML.
        return command.to_vec();
    }
    let mut resolved = Vec::new();
    for part in command {
        let p = Path::new(part);
        if p.is_relative() && command_part_looks_like_path(part) {
            resolved.push(
                normalize_path(&exp_dir.join(p))
                    .to_string_lossy()
                    .to_string(),
            );
        } else {
            resolved.push(part.clone());
        }
    }
    resolved
}

fn resolve_command_script_path(command: &[String], project_root: &Path) -> Option<PathBuf> {
    if command.is_empty() {
        return None;
    }
    let candidate_idx = if command_part_looks_like_path(&command[0]) {
        0
    } else if command.len() >= 2 && command_part_looks_like_path(&command[1]) {
        1
    } else {
        return None;
    };
    let candidate = Path::new(&command[candidate_idx]);
    if candidate.is_absolute() {
        return Some(normalize_path(candidate));
    }
    if candidate.as_os_str().is_empty() {
        return None;
    }
    Some(normalize_path(&project_root.join(candidate)))
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn validate_agent_runtime_command(
    command: &[String],
    project_root: &Path,
    container_mode: bool,
) -> Result<()> {
    if command.is_empty() {
        return Ok(());
    }
    // In container mode, absolute script paths can legitimately point to image paths
    // such as /opt/... that are not expected on host.
    if container_mode {
        let first = Path::new(&command[0]);
        if first.is_absolute() {
            return Ok(());
        }
        if command.len() >= 2 {
            let second = Path::new(&command[1]);
            if second.is_absolute() {
                return Ok(());
            }
        }
    }
    let path = resolve_command_script_path(command, project_root);
    if let Some(p) = path {
        if !p.exists() {
            let mut candidates: Vec<String> = Vec::new();
            for c in [
                "harness.js",
                "agentlab_demo_harness.js",
                "agentlab/harness.js",
                "harness.py",
                "main.py",
            ] {
                let cp = project_root.join(c);
                if cp.exists() {
                    candidates.push(cp.display().to_string());
                }
            }
            let hint = if candidates.is_empty() {
                "no common harness entrypoints found".to_string()
            } else {
                format!("candidates: {}", candidates.join(", "))
            };
            return Err(anyhow!(
                "agent entrypoint file not found on host: {} (update runtime.agent command). {}",
                p.display(),
                hint
            ));
        }
    }
    Ok(())
}

fn run_adapter_process(
    mut cmd: Command,
    output_path: &Path,
    start_response_path: Option<&Path>,
) -> Result<ProcessRunResult> {
    if output_path.exists() {
        if output_path.is_file() {
            fs::remove_file(output_path)?;
        } else {
            return Err(anyhow!(
                "output path must be a file: {}",
                output_path.display()
            ));
        }
    }

    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    if let Some(path) = start_response_path {
        wait_for_file(path, Duration::from_secs(10)).map_err(|err| {
            let _ = child.kill();
            let _ = child.wait();
            err
        })?;
    }
    let output = child.wait_with_output()?;

    Ok(ProcessRunResult {
        status: output
            .status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string()),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn wait_for_file(path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err(anyhow!(
        "timeout waiting for file {} after {:?}",
        path.display(),
        timeout
    ))
}

fn shell_join(parts: &[String]) -> String {
    parts
        .iter()
        .map(|p| shell_quote(p))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        "''".to_string()
    } else if s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-_./:".contains(c))
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\"'\"'"))
    }
}

fn resolve_trial_io_host_path(
    path: &str,
    paths: &TrialPaths,
    container_mode: bool,
) -> Result<PathBuf> {
    if container_mode {
        return map_container_path_to_host(path, paths);
    }
    map_contract_path_to_host(
        path,
        &ContractPathHostRoots::from_trial_paths(paths),
        ContractPathMode::RuntimeIo,
    )
}

fn prepare_io_paths(
    paths: &TrialPaths,
    container_mode: bool,
    input_bytes: &[u8],
    clean_contract_v1: bool,
) -> Result<PreparedTrialIo> {
    if clean_contract_v1 {
        let task_host = paths.runtime.task.clone();
        let result_host = paths.runtime.result.clone();
        let trajectory_host = paths.runtime.trajectory.clone();
        let output_host = result_host.clone();
        let events_host = trajectory_host.clone();

        if let Some(parent) = task_host.parent() {
            ensure_dir(parent)?;
        }
        if let Some(parent) = result_host.parent() {
            ensure_dir(parent)?;
        }
        if let Some(parent) = trajectory_host.parent() {
            ensure_dir(parent)?;
        }

        let input_value: Value = serde_json::from_slice(input_bytes)?;
        let task_value = input_value
            .pointer("/task")
            .cloned()
            .unwrap_or_else(|| input_value.clone());
        atomic_write_json_pretty(&task_host, &task_value)?;

        if result_host.exists() {
            let _ = fs::remove_file(&result_host);
        }
        if trajectory_host.exists() {
            let _ = fs::remove_file(&trajectory_host);
        }

        let task_path = if container_mode {
            DEFAULT_CLEAN_TASK_PATH.to_string()
        } else {
            task_host.to_string_lossy().to_string()
        };
        let result_path = if container_mode {
            DEFAULT_CLEAN_RESULT_PATH.to_string()
        } else {
            result_host.to_string_lossy().to_string()
        };
        let trajectory_path = if container_mode {
            DEFAULT_CONTAINER_TRAJECTORY_PATH.to_string()
        } else {
            trajectory_host.to_string_lossy().to_string()
        };

        return Ok(PreparedTrialIo {
            output_host,
            events_host,
            task_path,
            bindings_path: String::new(),
            dependencies_path: String::new(),
            policy_path: String::new(),
            result_path,
            trajectory_path,
        });
    }

    let (
        task_path,
        bindings_path,
        dependencies_path,
        policy_path,
        result_path,
        trajectory_path,
        task_host,
        bindings_host,
        dependencies_host,
        policy_host,
        result_host,
        trajectory_host,
        input_host,
        output_host,
        events_host,
        agentlabd_start_request_host,
        agentlabd_start_response_host,
    ) = if container_mode {
        (
            DEFAULT_CONTAINER_TASK_PATH.to_string(),
            DEFAULT_CONTAINER_BINDINGS_PATH.to_string(),
            DEFAULT_CONTAINER_DEPENDENCIES_PATH.to_string(),
            DEFAULT_CONTAINER_POLICY_PATH.to_string(),
            DEFAULT_CONTAINER_RESULT_PATH.to_string(),
            DEFAULT_CONTAINER_TRAJECTORY_PATH.to_string(),
            resolve_trial_io_host_path(DEFAULT_CONTAINER_TASK_PATH, paths, true)?,
            resolve_trial_io_host_path(DEFAULT_CONTAINER_BINDINGS_PATH, paths, true)?,
            resolve_trial_io_host_path(DEFAULT_CONTAINER_DEPENDENCIES_PATH, paths, true)?,
            resolve_trial_io_host_path(DEFAULT_CONTAINER_POLICY_PATH, paths, true)?,
            resolve_trial_io_host_path(DEFAULT_CONTAINER_RESULT_PATH, paths, true)?,
            resolve_trial_io_host_path(DEFAULT_CONTAINER_TRAJECTORY_PATH, paths, true)?,
            resolve_trial_io_host_path(DEFAULT_CONTAINER_TRIAL_INPUT_PATH, paths, true)?,
            resolve_trial_io_host_path(DEFAULT_CONTAINER_RESULT_PATH, paths, true)?,
            resolve_trial_io_host_path(DEFAULT_CONTAINER_TRAJECTORY_PATH, paths, true)?,
            resolve_trial_io_host_path(AGENTLAB_AGENTLABD_START_REQUEST_PATH, paths, true)?,
            resolve_trial_io_host_path(AGENTLAB_AGENTLABD_START_RESPONSE_PATH, paths, true)?,
        )
    } else {
        let task_host = paths.runtime.task.clone();
        let bindings_host = paths.runtime.bindings.clone();
        let dependencies_host = paths.runtime.dependencies.clone();
        let policy_host = paths.runtime.policy.clone();
        let result_host = paths.runtime.result.clone();
        let trajectory_host = paths.runtime.trajectory.clone();
        let input_host = paths.runtime.trial_input.clone();
        let output_host = result_host.clone();
        let events_host = trajectory_host.clone();
        (
            task_host.to_string_lossy().to_string(),
            bindings_host.to_string_lossy().to_string(),
            dependencies_host.to_string_lossy().to_string(),
            policy_host.to_string_lossy().to_string(),
            result_host.to_string_lossy().to_string(),
            trajectory_host.to_string_lossy().to_string(),
            task_host,
            bindings_host,
            dependencies_host,
            policy_host,
            result_host,
            trajectory_host,
            input_host,
            output_host,
            events_host,
            paths.runtime.agentlabd_start_request.clone(),
            paths.runtime.agentlabd_start_response.clone(),
        )
    };

    for host_path in [
        &task_host,
        &bindings_host,
        &dependencies_host,
        &policy_host,
        &result_host,
        &trajectory_host,
        &input_host,
    ] {
        if let Some(parent) = host_path.parent() {
            ensure_dir(parent)?;
        }
    }

    if let Some(parent) = input_host.parent() {
        ensure_dir(parent)?;
    }
    fs::write(&input_host, input_bytes)?;

    let input_value: Value = serde_json::from_slice(input_bytes)?;
    let task_value = input_value.pointer("/task").cloned().unwrap_or(json!({}));
    let bindings_value = input_value
        .pointer("/bindings")
        .cloned()
        .unwrap_or(json!({}));
    let dependencies_value = input_value
        .pointer("/dependencies")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let policy_value = input_value
        .pointer("/policy")
        .cloned()
        .unwrap_or_else(|| json!({}));

    atomic_write_json_pretty(&task_host, &task_value)?;
    atomic_write_json_pretty(&bindings_host, &bindings_value)?;
    atomic_write_json_pretty(&dependencies_host, &dependencies_value)?;
    atomic_write_json_pretty(&policy_host, &policy_value)?;

    if result_host.exists() {
        let _ = fs::remove_file(&result_host);
    }
    if trajectory_host.exists() {
        let _ = fs::remove_file(&trajectory_host);
    }
    if let Some(parent) = agentlabd_start_request_host.parent() {
        ensure_dir(parent)?;
    }
    if let Some(parent) = agentlabd_start_response_host.parent() {
        ensure_dir(parent)?;
    }
    if agentlabd_start_response_host.exists() {
        let _ = fs::remove_file(&agentlabd_start_response_host);
    }

    let _ = (agentlabd_start_request_host, agentlabd_start_response_host);

    Ok(PreparedTrialIo {
        output_host,
        events_host,
        task_path,
        bindings_path,
        dependencies_path,
        policy_path,
        result_path,
        trajectory_path,
    })
}

fn materialize_trial_result(trial_dir: &Path, output_path: &Path) -> Result<PathBuf> {
    let canonical_output = trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME);
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

fn write_adapter_continue_control(path: &Path) -> Result<()> {
    let _ = write_adapter_control_action(path, 0, "continue", None, "run_loop")?;
    Ok(())
}

fn write_adapter_control_action(
    path: &Path,
    seq: u64,
    action: &str,
    label: Option<&str>,
    requested_by: &str,
) -> Result<String> {
    let payload = json!({
        "schema_version": "control_plane_v1",
        "seq": seq,
        "action": action,
        "label": label,
        "requested_at": Utc::now().to_rfc3339(),
        "requested_by": requested_by,
    });
    let bytes = serde_json::to_vec_pretty(&payload)?;
    let version = sha256_bytes(&bytes);
    atomic_write_bytes(path, &bytes)?;
    Ok(version)
}

fn resolve_agent_runtime_manifest_path(
    paths: &TrialPaths,
    container_mode: bool,
) -> Result<PathBuf> {
    if container_mode {
        map_container_path_to_host(
            &format!("{}/harness_manifest.json", AGENTLAB_CONTRACT_OUT_DIR),
            paths,
        )
    } else {
        Ok(paths.out.join("harness_manifest.json"))
    }
}

fn resolve_exec_digest(command: &[String], exp_dir: &Path) -> Result<String> {
    if let Some(candidate_part) = resolve_command_digest_target(command) {
        let candidate = Path::new(candidate_part);
        let host_path = if candidate.is_relative() {
            exp_dir.join(candidate)
        } else {
            candidate.to_path_buf()
        };
        if host_path.exists() && host_path.is_file() {
            return sha256_file(&host_path);
        }
    }
    Ok(sha256_bytes(command.join(" ").as_bytes()))
}

fn write_state_inventory(
    trial_dir: &Path,
    json_value: &Value,
    agent_runtime: &AgentRuntimeConfig,
    container_mode: bool,
    paths: &TrialPaths,
    exec_digest: &str,
    effective_network_mode: &str,
    invocation_source: &str,
) -> Result<()> {
    let clean_contract_v1 = is_clean_contract_experiment(json_value);
    let sanitization_profile = json_value
        .pointer("/design/sanitization_profile")
        .and_then(|v| v.as_str())
        .unwrap_or("hermetic_functional");
    let integration_level = agent_runtime.integration_level.as_str();
    let mode_requested = if clean_contract_v1 {
        json_value
            .pointer("/runtime/network")
            .and_then(|v| v.as_str())
            .unwrap_or("none")
    } else {
        json_value
            .pointer("/runtime/policy/network/mode")
            .and_then(|v| v.as_str())
            .unwrap_or("none")
    };
    let mode_effective = if container_mode {
        effective_network_mode
    } else {
        "full"
    };
    let enforcement_effective = if container_mode && mode_requested == "none" {
        "docker_none"
    } else {
        "unknown"
    };

    let mounts = if container_mode && clean_contract_v1 {
        vec![
            json!({"name": "in", "path": HARNESS_IN_DIR, "writable": false}),
            json!({"name": "out", "path": HARNESS_OUT_DIR, "writable": true}),
        ]
    } else if container_mode {
        vec![
            json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
            json!({"name": "workspace", "path": AGENTLAB_CONTRACT_WORKSPACE_DIR, "writable": true}),
            json!({"name": "state", "path": AGENTLAB_CONTRACT_STATE_DIR, "writable": true}),
            json!({"name": "deps", "path": AGENTLAB_CONTRACT_DEPS_DIR, "writable": true}),
            json!({"name": "dataset", "path": "/dataset", "writable": false}),
            json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
            json!({"name": "tmp", "path": "/tmp", "writable": true}),
        ]
    } else {
        vec![
            json!({"name": "in", "path": paths.in_dir.to_string_lossy(), "writable": false}),
            json!({"name": "workspace", "path": paths.workspace.to_string_lossy(), "writable": true}),
            json!({"name": "state", "path": paths.state.to_string_lossy(), "writable": true}),
            json!({"name": "deps", "path": paths.deps.to_string_lossy(), "writable": true}),
            json!({"name": "dataset", "path": paths.dataset.to_string_lossy(), "writable": false}),
            json!({"name": "out", "path": paths.out.to_string_lossy(), "writable": true}),
            json!({"name": "tmp", "path": paths.tmp.to_string_lossy(), "writable": true}),
        ]
    };

    let state = json!({
        "schema_version": "state_inventory_v1",
        "sanitization_profile": sanitization_profile,
        "integration_level": integration_level,
        "mounts": mounts,
        "network": {
            "mode_requested": mode_requested,
            "mode_effective": mode_effective,
            "allowed_hosts": if clean_contract_v1 {
                json!([])
            } else {
                json_value.pointer("/runtime/policy/network/allowed_hosts").cloned().unwrap_or(json!([]))
            },
            "enforcement_effective": enforcement_effective,
            "egress_self_test": {
                "performed": false,
                "cases": []
            }
        },
        "agent_runtime_identity": {
            "name": agent_runtime.command_raw.get(0).cloned().unwrap_or("unknown".to_string()),
            "exec_digest": exec_digest,
            "entry_command": agent_runtime.command_raw.clone(),
            "invocation_source": invocation_source,
            "launch_mode": agent_runtime.launch_mode.as_str()
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

fn remove_path_if_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn apply_materialization_policy(trial_dir: &Path, mode: MaterializationMode) -> Result<()> {
    match mode {
        MaterializationMode::Full => return Ok(()),
        MaterializationMode::OutputsOnly => {
            for dir_name in ["workspace", "dataset", "state", "tmp", "artifacts"] {
                remove_path_if_exists(&trial_dir.join(dir_name))?;
            }
        }
        MaterializationMode::MetadataOnly | MaterializationMode::None => {
            for dir_name in ["workspace", "dataset", "state", "tmp", "artifacts", "out"] {
                remove_path_if_exists(&trial_dir.join(dir_name))?;
            }
            remove_path_if_exists(&trial_dir.join("trial_input.json"))?;
            remove_path_if_exists(&trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME))?;
            remove_path_if_exists(&trial_dir.join("harness_manifest.json"))?;
            remove_path_if_exists(&trial_dir.join("trace_manifest.json"))?;
            if matches!(mode, MaterializationMode::None) {
                remove_path_if_exists(&trial_dir.join("state_inventory.json"))?;
            }
        }
    }
    Ok(())
}

fn map_container_path_to_host(path: &str, paths: &TrialPaths) -> Result<PathBuf> {
    map_contract_path_to_host(
        path,
        &ContractPathHostRoots::from_trial_paths(paths),
        ContractPathMode::ContainerMount,
    )
}

fn load_event_rows(
    events_path: &Path,
    run_id: &str,
    trial_id: &str,
    variant_id: &str,
    task_id: &str,
    repl_idx: usize,
) -> Result<Vec<EventRow>> {
    let data = fs::read_to_string(events_path)?;
    let mut rows = Vec::new();
    for (seq, line) in data.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let payload: Value = serde_json::from_str(line)?;
        let event_type = payload
            .get("event_type")
            .and_then(Value::as_str)
            .or_else(|| payload.get("type").and_then(Value::as_str))
            .unwrap_or("unknown")
            .to_string();
        let ts = payload
            .get("ts")
            .and_then(Value::as_str)
            .or_else(|| payload.get("timestamp").and_then(Value::as_str))
            .map(str::to_string);
        rows.push(EventRow {
            run_id: run_id.to_string(),
            trial_id: trial_id.to_string(),
            variant_id: variant_id.to_string(),
            task_id: task_id.to_string(),
            repl_idx,
            seq,
            event_type,
            ts,
            payload,
        });
    }
    Ok(rows)
}

fn build_metric_rows(
    run_id: &str,
    trial_id: &str,
    variant_id: &str,
    task_id: &str,
    repl_idx: usize,
    outcome: &str,
    metrics: &Value,
    primary_metric_name: &str,
    primary_metric_value: &Value,
) -> Vec<MetricRow> {
    let mut rows = Vec::new();
    if let Some(metric_obj) = metrics.as_object() {
        for (metric_name, metric_value) in metric_obj {
            rows.push(MetricRow {
                run_id: run_id.to_string(),
                trial_id: trial_id.to_string(),
                variant_id: variant_id.to_string(),
                task_id: task_id.to_string(),
                repl_idx,
                outcome: outcome.to_string(),
                metric_name: metric_name.clone(),
                metric_value: metric_value.clone(),
                metric_source: None,
            });
        }
    }
    rows.push(MetricRow {
        run_id: run_id.to_string(),
        trial_id: trial_id.to_string(),
        variant_id: variant_id.to_string(),
        task_id: task_id.to_string(),
        repl_idx,
        outcome: outcome.to_string(),
        metric_name: primary_metric_name.to_string(),
        metric_value: primary_metric_value.clone(),
        metric_source: Some("primary".to_string()),
    });
    rows
}

fn build_variant_snapshot_rows(
    run_id: &str,
    trial_id: &str,
    variant_id: &str,
    baseline_id: &str,
    task_id: &str,
    repl_idx: usize,
    bindings: &Value,
) -> Vec<VariantSnapshotRow> {
    let mut rows = Vec::new();
    if let Some(bindings_obj) = bindings.as_object() {
        for (binding_name, binding_value) in bindings_obj {
            rows.push(VariantSnapshotRow {
                run_id: run_id.to_string(),
                trial_id: trial_id.to_string(),
                variant_id: variant_id.to_string(),
                baseline_id: baseline_id.to_string(),
                task_id: task_id.to_string(),
                repl_idx,
                binding_name: binding_name.clone(),
                binding_value: binding_value.clone(),
                binding_value_text: binding_value_to_text(binding_value),
            });
        }
    }
    rows
}

fn binding_value_to_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn copy_dir_filtered(src: &Path, dst: &Path, exclude: &[&str]) -> Result<()> {
    let walker = walkdir::WalkDir::new(src).into_iter().filter_entry(|e| {
        let rel = e.path().strip_prefix(src).unwrap_or(e.path());
        if rel.as_os_str().is_empty() {
            return true; // root entry
        }
        !exclude.iter().any(|ex| rel.starts_with(ex))
    });
    for entry in walker {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(src).unwrap();
        if rel.as_os_str().is_empty() {
            continue;
        }
        let target = dst.join(rel);
        if entry.file_type().is_dir() {
            ensure_dir(&target)?;
        } else if entry.file_type().is_symlink() {
            if let Some(parent) = target.parent() {
                ensure_dir(parent)?;
            }
            match fs::canonicalize(path) {
                Ok(real) if real.is_dir() => {
                    copy_dir_filtered(&real, &target, &[])?;
                }
                Ok(real) if real.is_file() => {
                    fs::copy(real, &target)?;
                }
                Ok(_) => {}
                Err(_) => {
                    // Preserve broken links instead of aborting trial setup.
                    let link_target = fs::read_link(path)?;
                    if target.exists() {
                        let _ = fs::remove_file(&target);
                    }
                    #[cfg(unix)]
                    {
                        symlink(&link_target, &target)?;
                    }
                }
            }
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                ensure_dir(parent)?;
            }
            fs::copy(path, target)?;
        }
    }
    Ok(())
}

fn command_part_looks_like_path(part: &str) -> bool {
    part.starts_with('.')
        || part.starts_with('/')
        || part.contains('/')
        || part.ends_with(".js")
        || part.ends_with(".mjs")
        || part.ends_with(".cjs")
        || part.ends_with(".ts")
        || part.ends_with(".py")
        || part.ends_with(".sh")
}

fn resolve_command_digest_target(command: &[String]) -> Option<&str> {
    if command.is_empty() {
        return None;
    }
    if command_part_looks_like_path(&command[0]) {
        return Some(command[0].as_str());
    }
    if command.len() >= 2 && command_part_looks_like_path(&command[1]) {
        return Some(command[1].as_str());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct TempDirGuard {
        path: PathBuf,
    }

    impl TempDirGuard {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "{}_{}_{}",
                prefix,
                std::process::id(),
                Utc::now().timestamp_micros()
            ));
            ensure_dir(&path).expect("temp dir");
            Self { path }
        }
    }

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn create_run_dir(prefix: &str, run_id: &str) -> (TempDirGuard, PathBuf) {
        let root = TempDirGuard::new(prefix);
        let run_dir = root.path.join(".lab").join("runs").join(run_id);
        ensure_dir(&run_dir).expect("run dir");
        (root, run_dir)
    }

    fn harness_success_command() -> Vec<String> {
        vec![
            "sh".to_string(),
            "-lc".to_string(),
            "printf '%s' '{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\",\"checkpoints\":[]}'".to_string(),
        ]
    }

    #[test]
    fn adapter_registry_supports_prebuilt_codex_and_rex_jesus() {
        let codex = adapter_registry_entry(&AgentAdapterRef {
            id: PREBUILT_CODEX_ADAPTER_ID.to_string(),
            version: PREBUILT_AGENT_ADAPTER_VERSION.to_string(),
        })
        .expect("codex prebuilt adapter");
        assert!(codex.capabilities().pause);

        let rex = adapter_registry_entry(&AgentAdapterRef {
            id: PREBUILT_REX_JESUS_ADAPTER_ID.to_string(),
            version: PREBUILT_AGENT_ADAPTER_VERSION.to_string(),
        })
        .expect("rex prebuilt adapter");
        assert!(rex.capabilities().pause);
    }

    #[test]
    fn adapter_registry_error_lists_supported_adapters() {
        let err = match adapter_registry_entry(&AgentAdapterRef {
            id: "unknown.adapter".to_string(),
            version: "v0".to_string(),
        }) {
            Ok(_) => panic!("unsupported adapter should fail"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains(&format!(
                "{}@{}",
                BUILTIN_COMMAND_ADAPTER_ID, BUILTIN_COMMAND_ADAPTER_VERSION
            )),
            "message should include builtin adapter: {}",
            msg
        );
        assert!(
            msg.contains(&format!(
                "{}@{}",
                PREBUILT_CODEX_ADAPTER_ID, PREBUILT_AGENT_ADAPTER_VERSION
            )),
            "message should include codex prebuilt adapter: {}",
            msg
        );
        assert!(
            msg.contains(&format!(
                "{}@{}",
                PREBUILT_REX_JESUS_ADAPTER_ID, PREBUILT_AGENT_ADAPTER_VERSION
            )),
            "message should include rex prebuilt adapter: {}",
            msg
        );
    }

    fn write_resolved_experiment(
        run_dir: &Path,
        integration_level: &str,
        include_events_path: bool,
    ) {
        let _ = include_events_path;

        let resolved = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl" },
            "design": { "sanitization_profile": "hermetic_functional", "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": {
                    "command": harness_success_command(),
                    "image": "img",
                    "integration_level": integration_level,
                    "io": { "input_arg": "--input", "output_arg": "--output" }
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "local" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });
        atomic_write_json_pretty(&run_dir.join("resolved_experiment.json"), &resolved)
            .expect("write resolved");
        let (variants, baseline_id) = resolve_variant_plan(&resolved).expect("variant plan");
        write_resolved_variants(run_dir, &baseline_id, &variants).expect("write resolved variants");
    }

    fn seed_parent_trial(
        run_dir: &Path,
        trial_id: &str,
        checkpoints: Value,
        trial_status: &str,
        pause_label: Option<&str>,
    ) -> PathBuf {
        let trial_dir = run_dir.join("trials").join(trial_id);
        ensure_dir(&trial_dir).expect("trial dir");
        ensure_dir(&trial_dir.join("workspace")).expect("workspace");
        ensure_dir(&trial_dir.join("state")).expect("state");
        ensure_dir(&trial_dir.join("dataset")).expect("dataset");

        fs::write(
            trial_dir.join("workspace").join("fixture.txt"),
            "workspace fixture",
        )
        .expect("workspace fixture");
        fs::write(
            trial_dir.join("dataset").join("tasks.jsonl"),
            "{\"id\":\"task_1\"}\n",
        )
        .expect("dataset file");

        let trial_input = json!({
            "schema_version": "agent_task_v1",
            "ids": { "trial_id": trial_id, "variant_id": "base", "task_id": "task_1", "repl_idx": 0 },
            "task": {
                "id": "task_1"
            },
            "bindings": {
                "existing": "value"
            },
            "runtime": {
                "paths": {
                    "workspace": trial_dir.join("workspace").to_string_lossy().to_string(),
                    "state": trial_dir.join("state").to_string_lossy().to_string(),
                    "dataset": trial_dir.join("dataset").to_string_lossy().to_string(),
                    "out": trial_dir.join("out").to_string_lossy().to_string(),
                    "tmp": trial_dir.join("tmp").to_string_lossy().to_string()
                },
                "network": { "mode_requested": "none" }
            }
        });
        atomic_write_json_pretty(&trial_dir.join("trial_input.json"), &trial_input)
            .expect("trial input");

        let trial_output = json!({
            "schema_version": "agent_result_v1",
            "outcome": "success",
            "checkpoints": checkpoints
        });
        atomic_write_json_pretty(
            &trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME),
            &trial_output,
        )
        .expect("trial output");

        write_trial_state(
            &trial_dir,
            trial_id,
            trial_status,
            pause_label,
            pause_label,
            if trial_status == "paused" {
                Some("paused_by_user")
            } else {
                None
            },
        )
        .expect("trial state");

        trial_dir
    }

    fn active_control_for_trial(trial_dir: &Path) -> ActiveAdapterControl {
        let control_path = trial_dir.join("state").join("lab_control.json");
        write_adapter_continue_control(&control_path).expect("control file");
        ActiveAdapterControl {
            adapter_id: BUILTIN_COMMAND_ADAPTER_ID.to_string(),
            adapter_version: BUILTIN_COMMAND_ADAPTER_VERSION.to_string(),
            command_path: control_path.to_string_lossy().to_string(),
            events_path: Some(
                trial_dir
                    .join("state")
                    .join("events.jsonl")
                    .to_string_lossy()
                    .to_string(),
            ),
        }
    }

    fn write_test_run_control(
        run_dir: &Path,
        run_id: &str,
        status: &str,
        active_trial_id: Option<&str>,
        active_control: Option<&ActiveAdapterControl>,
    ) {
        let active_trials = active_trial_id
            .map(|trial_id| {
                vec![RunControlActiveTrial {
                    trial_id: trial_id.to_string(),
                    worker_id: "worker_1".to_string(),
                    schedule_idx: None,
                    variant_id: None,
                    started_at: Some(Utc::now().to_rfc3339()),
                    control: active_control.cloned(),
                }]
            })
            .unwrap_or_default();
        write_run_control_v2(run_dir, run_id, status, &active_trials, None).expect("run control");
    }

    fn spawn_pause_ack_writer(
        control_path: PathBuf,
        events_path: PathBuf,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut seen_versions = std::collections::BTreeSet::new();
            while Instant::now() < deadline {
                let bytes = match fs::read(&control_path) {
                    Ok(b) => b,
                    Err(_) => {
                        thread::sleep(Duration::from_millis(20));
                        continue;
                    }
                };
                let value: Value = match serde_json::from_slice(&bytes) {
                    Ok(v) => v,
                    Err(_) => {
                        thread::sleep(Duration::from_millis(20));
                        continue;
                    }
                };
                let action = value
                    .pointer("/action")
                    .and_then(|v| v.as_str())
                    .unwrap_or("continue");
                if action != "checkpoint" && action != "stop" {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }

                let version = sha256_bytes(&bytes);
                if !seen_versions.insert(version.clone()) {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }

                if let Some(parent) = events_path.parent() {
                    let _ = ensure_dir(parent);
                }
                let ack = json!({
                    "event_type": "control_ack",
                    "action_observed": action,
                    "control_version": version
                });
                if let Ok(mut file) = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&events_path)
                {
                    let _ = writeln!(file, "{}", ack);
                }
                if action == "stop" {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        })
    }

    fn create_trial_paths_fixture(prefix: &str) -> (TempDirGuard, TrialPaths) {
        let root = TempDirGuard::new(prefix);
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        fs::write(exp_dir.join("README.md"), "fixture").expect("exp fixture");
        let dataset_src = root.path.join("tasks.jsonl");
        fs::write(&dataset_src, "{\"id\":\"task_1\"}\n").expect("dataset");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let paths = TrialPaths::new(&trial_dir, &exp_dir, &dataset_src).expect("trial paths");
        paths.prepare(true).expect("prepare");
        (root, paths)
    }

    #[test]
    fn contract_path_mapper_resolves_container_contract_paths() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_contract_mapper_container");
        let cases = vec![
            (
                format!("{}/trial_input.json", AGENTLAB_CONTRACT_IN_DIR),
                paths.in_dir.join("trial_input.json"),
            ),
            (
                format!("{}/events.jsonl", AGENTLAB_CONTRACT_STATE_DIR),
                paths.state.join("events.jsonl"),
            ),
            (
                format!("{}/result.json", AGENTLAB_CONTRACT_OUT_DIR),
                paths.out.join("result.json"),
            ),
        ];
        for (raw, expected) in cases {
            let resolved = map_container_path_to_host(&raw, &paths).expect("resolve path");
            assert_eq!(resolved, expected, "path mismatch for {}", raw);
        }

        let err = map_container_path_to_host("/stateful/not_state", &paths).expect_err("reject");
        assert!(
            err.to_string().contains("unsupported container mount path"),
            "unexpected error: {}",
            err
        );

        let err = map_container_path_to_host("/state/events.jsonl", &paths).expect_err("reject");
        assert!(
            err.to_string().contains("unsupported container mount path"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn contract_path_mapper_enforces_mode_specific_paths() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_contract_mapper_modes");
        let contract_deps = format!("{}/pkg.json", AGENTLAB_CONTRACT_DEPS_DIR);
        let resolved =
            resolve_trial_io_host_path(&contract_deps, &paths, false).expect("contract deps");
        assert_eq!(resolved, paths.deps.join("pkg.json"));

        let err = resolve_trial_io_host_path("/deps/pkg.json", &paths, false).expect_err("reject");
        assert!(
            err.to_string()
                .contains("unsupported runtime io path for non-container trials"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn contract_path_mapper_resolves_event_paths_and_rejects_invalid_roots() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_contract_mapper_events");
        let trial_dir = paths.in_dir.parent().expect("trial dir").to_path_buf();

        let in_path = format!("{}/trial_input.json", AGENTLAB_CONTRACT_IN_DIR);
        let resolved_in = resolve_event_path_for_trial(&in_path, &trial_dir).expect("in path");
        assert_eq!(resolved_in, trial_dir.join("in").join("trial_input.json"));

        let err = resolve_event_path_for_trial("/dataset/tasks.jsonl", &trial_dir)
            .expect_err("reject legacy dataset path");
        assert!(
            err.to_string()
                .contains("unsupported runtime event path for trial"),
            "unexpected error: {}",
            err
        );

        let err = resolve_event_path_for_trial("/harness/logs/events.jsonl", &trial_dir)
            .expect_err("reject");
        assert!(
            err.to_string()
                .contains("unsupported runtime event path for trial"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn run_session_state_roundtrip_normalizes_execution_options() {
        let (_root, run_dir) = create_run_dir("agentlab_run_session_state", "run_1");
        let behavior = RunBehavior {
            setup_command: Some("echo setup".to_string()),
            network_mode_override: Some("full".to_string()),
            require_network_none: false,
        };
        let execution = RunExecutionOptions {
            executor: Some(ExecutorKind::LocalProcess),
            materialize: None,
            remote_endpoint: None,
            remote_token_env: Some("TOKEN".to_string()),
        };
        write_run_session_state(&run_dir, "run_1", &behavior, &execution).expect("write state");
        let state = load_run_session_state(&run_dir).expect("load state");
        assert_eq!(state.schema_version, "run_session_state_v1");
        assert_eq!(state.run_id, "run_1");
        assert_eq!(state.behavior.setup_command.as_deref(), Some("echo setup"));
        assert_eq!(
            state.behavior.network_mode_override.as_deref(),
            Some("full")
        );
        assert_eq!(state.execution.executor, Some(ExecutorKind::LocalProcess));
        assert_eq!(state.execution.materialize, Some(MaterializationMode::Full));
        assert_eq!(state.execution.remote_token_env.as_deref(), Some("TOKEN"));
    }

    #[test]
    fn resolve_remote_bearer_token_reads_env_when_present() {
        let key = "AGENTLAB_TEST_REMOTE_TOKEN";
        let previous = env::var(key).ok();
        env::set_var(key, "token_123");
        let token = resolve_remote_bearer_token(Some(key)).expect("token resolution");
        assert_eq!(token.as_deref(), Some("token_123"));
        if let Some(previous) = previous {
            env::set_var(key, previous);
        } else {
            env::remove_var(key);
        }
    }

    #[test]
    fn resolve_remote_bearer_token_skips_unset_and_errors_for_missing_env() {
        assert!(resolve_remote_bearer_token(Some("unset"))
            .expect("unset should be treated as no-token")
            .is_none());
        let key = "AGENTLAB_TEST_REMOTE_TOKEN_MISSING";
        let previous = env::var(key).ok();
        env::remove_var(key);
        let err = resolve_remote_bearer_token(Some(key)).expect_err("missing env should fail");
        assert!(
            err.to_string().contains("is not set"),
            "unexpected error: {}",
            err
        );
        if let Some(previous) = previous {
            env::set_var(key, previous);
        }
    }

    #[test]
    fn remote_retry_settings_apply_env_overrides() {
        let attempts_key = AGENTLAB_REMOTE_PROTOCOL_RETRY_MAX_ATTEMPTS_ENV;
        let backoff_key = AGENTLAB_REMOTE_PROTOCOL_RETRY_BASE_BACKOFF_MS_ENV;
        let attempts_prev = env::var(attempts_key).ok();
        let backoff_prev = env::var(backoff_key).ok();

        env::set_var(attempts_key, "5");
        env::set_var(backoff_key, "75");
        let settings = resolve_remote_retry_settings_from_env().expect("settings");
        assert_eq!(settings.max_attempts, 5);
        assert_eq!(settings.base_backoff_ms, 75);

        if let Some(value) = attempts_prev {
            env::set_var(attempts_key, value);
        } else {
            env::remove_var(attempts_key);
        }
        if let Some(value) = backoff_prev {
            env::set_var(backoff_key, value);
        } else {
            env::remove_var(backoff_key);
        }
    }

    #[test]
    fn remote_protocol_timeout_settings_apply_env_overrides() {
        let connect_key = AGENTLAB_REMOTE_PROTOCOL_CONNECT_TIMEOUT_MS_ENV;
        let submit_key = AGENTLAB_REMOTE_PROTOCOL_SUBMIT_TIMEOUT_MS_ENV;
        let poll_grace_key = AGENTLAB_REMOTE_PROTOCOL_POLL_TIMEOUT_GRACE_MS_ENV;
        let pause_key = AGENTLAB_REMOTE_PROTOCOL_PAUSE_TIMEOUT_MS_ENV;
        let stop_key = AGENTLAB_REMOTE_PROTOCOL_STOP_TIMEOUT_MS_ENV;
        let connect_prev = env::var(connect_key).ok();
        let submit_prev = env::var(submit_key).ok();
        let poll_grace_prev = env::var(poll_grace_key).ok();
        let pause_prev = env::var(pause_key).ok();
        let stop_prev = env::var(stop_key).ok();

        env::set_var(connect_key, "2500");
        env::set_var(submit_key, "45000");
        env::set_var(poll_grace_key, "1500");
        env::set_var(pause_key, "12000");
        env::set_var(stop_key, "16000");
        let settings = resolve_remote_protocol_timeout_settings_from_env().expect("settings");
        assert_eq!(settings.connect_timeout_ms, 2500);
        assert_eq!(settings.submit_timeout_ms, 45000);
        assert_eq!(settings.poll_timeout_grace_ms, 1500);
        assert_eq!(settings.pause_timeout_ms, 12000);
        assert_eq!(settings.stop_timeout_ms, 16000);

        if let Some(value) = connect_prev {
            env::set_var(connect_key, value);
        } else {
            env::remove_var(connect_key);
        }
        if let Some(value) = submit_prev {
            env::set_var(submit_key, value);
        } else {
            env::remove_var(submit_key);
        }
        if let Some(value) = poll_grace_prev {
            env::set_var(poll_grace_key, value);
        } else {
            env::remove_var(poll_grace_key);
        }
        if let Some(value) = pause_prev {
            env::set_var(pause_key, value);
        } else {
            env::remove_var(pause_key);
        }
        if let Some(value) = stop_prev {
            env::set_var(stop_key, value);
        } else {
            env::remove_var(stop_key);
        }
    }

    #[test]
    fn continue_run_accepts_paused_and_interrupted_terminal_statuses() {
        for status in ["paused", "interrupted"] {
            let (_root, run_dir) = create_run_dir("agentlab_continue_statuses", "run_1");
            write_test_run_control(&run_dir, "run_1", status, None, None);

            let err =
                continue_run(&run_dir).expect_err("continue should reach run session state load");
            assert!(
                err.to_string().contains("run_session_state.json not found"),
                "status {} produced unexpected error: {}",
                status,
                err
            );
        }
    }

    #[test]
    fn continue_run_uses_persisted_behavior() {
        let (_root, run_dir) = create_run_dir("agentlab_continue_persisted_behavior", "run_1");
        let dataset_path = run_dir.join("tasks.jsonl");
        fs::write(&dataset_path, "{\"id\":\"task_1\"}\n").expect("dataset");
        let resolved = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": {
                "path": "tasks.jsonl",
                "provider": "local_jsonl",
                "suite_id": "s",
                "schema_version": "v1",
                "split_id": "dev",
                "limit": 1
            },
            "design": {
                "sanitization_profile": "hermetic_functional",
                "comparison": "paired",
                "replications": 1,
                "random_seed": 1,
                "shuffle_tasks": false,
                "max_concurrency": 1
            },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": {
                    "command": harness_success_command(),
                    "image": "img",
                    "integration_level": "cli_basic",
                    "io": { "input_arg": "--input", "output_arg": "--output" }
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "local" },
                    "network": { "mode": "full", "allowed_hosts": [] }
                }
            }
        });
        atomic_write_json_pretty(&run_dir.join("resolved_experiment.json"), &resolved)
            .expect("resolved");
        write_test_run_control(&run_dir, "run_1", "failed", None, None);
        let schedule = build_trial_schedule(1, 1, 1, parse_policies(&resolved).scheduling, 1);
        let schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: schedule.len(),
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule,
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            use_container: false,
            updated_at: Utc::now().to_rfc3339(),
        };
        write_schedule_progress(&run_dir, &schedule_progress).expect("progress");
        let behavior = RunBehavior {
            setup_command: None,
            network_mode_override: None,
            require_network_none: true,
        };
        write_run_session_state(
            &run_dir,
            "run_1",
            &behavior,
            &RunExecutionOptions::default(),
        )
        .expect("run session");

        let err = continue_run(&run_dir).expect_err("continue should honor persisted behavior");
        assert!(
            err.to_string()
                .contains("run-experiment requires network mode 'none'"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_script_path_supports_binary_first_commands() {
        let root = PathBuf::from("/tmp/agentlab_proj");
        let cmd = vec!["./harness".to_string(), "run".to_string()];
        let resolved = resolve_command_script_path(&cmd, &root).expect("expected path");
        assert_eq!(resolved, normalize_path(&root.join("harness")));
    }

    #[test]
    fn resolve_script_path_supports_interpreter_plus_script() {
        let root = PathBuf::from("/tmp/agentlab_proj");
        let cmd = vec![
            "node".to_string(),
            "./harness.js".to_string(),
            "run".to_string(),
        ];
        let resolved = resolve_command_script_path(&cmd, &root).expect("expected path");
        assert_eq!(resolved, normalize_path(&root.join("harness.js")));
    }

    #[test]
    fn resolve_agent_runtime_command_resolves_first_token_when_path_like() {
        let root = PathBuf::from("/tmp/agentlab_proj");
        let cmd = vec!["./harness".to_string(), "run".to_string()];
        let resolved = resolve_agent_runtime_command(&cmd, &root, false);
        assert_eq!(resolved[0], root.join("harness").to_string_lossy());
        assert_eq!(resolved[1], "run");
    }

    #[test]
    fn resolve_agent_runtime_command_keeps_relative_paths_in_container_mode() {
        let root = PathBuf::from("/tmp/agentlab_proj");
        let cmd = vec!["./agent.py".to_string(), "--flag".to_string()];
        let resolved = resolve_agent_runtime_command(&cmd, &root, true);
        assert_eq!(resolved, cmd);
    }

    #[test]
    fn apply_agentlab_template_supports_workspace_variable() {
        let mut env = BTreeMap::new();
        env.insert("WORKSPACE".to_string(), "/testbed".to_string());
        let rendered = apply_agentlab_template("${WORKSPACE}/repo", &env);
        assert_eq!(rendered, "/testbed/repo");
    }

    #[test]
    fn resolve_agent_runtime_parses_launch_mode_stdio() {
        let root = TempDirGuard::new("agentlab_launch_mode_parse");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": ["sh", "-lc", "echo ok"],
                    "image": "img",
                    "integration_level": "cli_basic",
                    "launch": { "mode": "stdio" }
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "container", "image": "img" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });

        let agent_runtime = resolve_agent_runtime(&spec, &exp_dir).expect("resolve runtime");
        assert_eq!(agent_runtime.launch_mode, AgentLaunchMode::Stdio);
    }

    #[test]
    fn resolve_agent_runtime_custom_image_supports_command_override_string() {
        let root = TempDirGuard::new("agentlab_command_override_string");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": "rex",
                    "image": "img"
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "container", "image": "img" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });

        let agent_runtime = resolve_agent_runtime(&spec, &exp_dir).expect("resolve runtime");
        assert_eq!(agent_runtime.command_raw, vec!["rex"]);
    }

    #[test]
    fn resolve_agent_runtime_per_task_requires_artifact() {
        let root = TempDirGuard::new("agentlab_per_task_requires_artifact");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": ["rex", "run"],
                    "image_source": "per_task"
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "container" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });

        let err = match resolve_agent_runtime(&spec, &exp_dir) {
            Ok(_) => panic!("missing artifact should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("runtime.agent.artifact is required"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_agent_runtime_rejects_legacy_aliases() {
        let root = TempDirGuard::new("agentlab_command_aliases");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": ["rex", "run-agent-loop"],
                    "aliases": {
                        "rex": ["bun", "./scripts/rex.js"]
                    },
                    "image": "img"
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "container", "image": "img" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });

        let err = match resolve_agent_runtime(&spec, &exp_dir) {
            Ok(_) => panic!("legacy aliases should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("hard cut"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn build_runtime_contract_env_includes_agentlabd_keys() {
        let io = PreparedTrialIo {
            output_host: PathBuf::from("/tmp/out.json"),
            events_host: PathBuf::from("/tmp/events.jsonl"),
            task_path: AGENTLAB_TASK_PATH.to_string(),
            bindings_path: AGENTLAB_BINDINGS_PATH.to_string(),
            dependencies_path: AGENTLAB_DEPENDENCIES_PATH.to_string(),
            policy_path: AGENTLAB_POLICY_PATH.to_string(),
            result_path: AGENTLAB_RESULT_PATH.to_string(),
            trajectory_path: AGENTLAB_TRAJECTORY_PATH.to_string(),
        };
        let input = json!({
            "ids": {
                "trial_id": "trial_1",
                "variant_id": "control",
                "task_id": "task_1",
                "repl_idx": 0
            }
        });
        let env = build_runtime_contract_env("run_1", &input, &io, Some(12345), false);
        assert_eq!(
            env.get(AGENTLAB_ENV_TASK_PATH).map(String::as_str),
            Some(AGENTLAB_TASK_PATH)
        );
        assert_eq!(
            env.get(AGENTLAB_ENV_BINDINGS_PATH).map(String::as_str),
            Some(AGENTLAB_BINDINGS_PATH)
        );
        assert_eq!(
            env.get(AGENTLAB_ENV_RESULT_PATH).map(String::as_str),
            Some(AGENTLAB_RESULT_PATH)
        );
    }

    #[test]
    fn build_runtime_contract_env_is_empty_for_clean_contract() {
        let io = PreparedTrialIo {
            output_host: PathBuf::from("/tmp/out.json"),
            events_host: PathBuf::from("/tmp/events.jsonl"),
            task_path: HARNESS_TASK_PATH.to_string(),
            bindings_path: String::new(),
            dependencies_path: String::new(),
            policy_path: String::new(),
            result_path: HARNESS_RESULT_PATH.to_string(),
            trajectory_path: String::new(),
        };
        let input = json!({ "id": "task_1" });
        let env = build_runtime_contract_env("run_1", &input, &io, Some(12345), true);
        assert!(
            env.is_empty(),
            "clean contract should not project AGENTLAB_* env vars"
        );
    }

    #[test]
    fn resolve_harness_parses_host_file_staging_entries() {
        let root = TempDirGuard::new("agentlab_host_file_staging_parse");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": ["sh", "-lc", "echo ok"],
                    "image": "img",
                    "env": {"A":"B"}
                },
                "dependencies": {
                    "file_staging": [
                        {
                            "source_from_host": "./secrets/graphd.db",
                            "destination_path": format!("{}/.graphd/graphd.db", AGENTLAB_CONTRACT_STATE_DIR),
                            "required": true
                        }
                    ]
                },
                "policy": {
                    "timeout_ms": 600000,
                    "network": {"mode":"none","allowed_hosts":[]},
                    "sandbox": { "mode": "container", "image": "img" }
                }
            }
        });

        let agent_runtime = resolve_agent_runtime(&spec, &exp_dir).expect("resolve runtime");
        assert_eq!(agent_runtime.dependency_file_staging.len(), 1);
        let entry = &agent_runtime.dependency_file_staging[0];
        assert_eq!(
            entry.source_from_host,
            normalize_path(&exp_dir.join("secrets/graphd.db"))
        );
        assert_eq!(
            entry.destination_path,
            format!("{}/.graphd/graphd.db", AGENTLAB_CONTRACT_STATE_DIR)
        );
        assert!(entry.required);
    }

    #[test]
    fn stage_dependencies_for_trial_copies_into_trial_namespaces() {
        let root = TempDirGuard::new("agentlab_host_file_staging_copy");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp");
        fs::write(exp_dir.join("fixture.txt"), "fixture").expect("exp fixture");
        let dataset_src = root.path.join("tasks.jsonl");
        fs::write(&dataset_src, "{\"id\":\"task_1\"}\n").expect("dataset");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let paths = TrialPaths::new(&trial_dir, &exp_dir, &dataset_src).expect("paths");
        paths.prepare(true).expect("prepare");

        let source_db = root.path.join("graphd.db");
        fs::write(&source_db, "db-bytes").expect("source db");

        let agent_runtime = AgentRuntimeConfig {
            adapter_ref: AgentAdapterRef::default(),
            command_raw: vec![],
            container_image: None,
            image_source: ImageSource::Global,
            agent_artifact: None,
            io: AgentRuntimeIoConfig {
                input_arg: "--input".to_string(),
                output_arg: "--output".to_string(),
            },
            clean_contract_v1: false,
            integration_level: "cli_basic".to_string(),
            launch_mode: AgentLaunchMode::File,
            env: BTreeMap::new(),
            env_from_host: vec![],
            trajectory_path: None,
            causal_extraction: None,
            default_timeout_ms: None,
            tracing_mode: None,
            force_container: true,
            dependency_file_staging: vec![
                DependencyFileStagingSpec {
                    source_from_host: source_db.clone(),
                    destination_path: format!("{}/.graphd/graphd.db", AGENTLAB_CONTRACT_STATE_DIR),
                    required: true,
                    read_only: false,
                },
                DependencyFileStagingSpec {
                    source_from_host: root.path.join("missing-wal"),
                    destination_path: format!(
                        "{}/.graphd/graphd.db-wal",
                        AGENTLAB_CONTRACT_STATE_DIR
                    ),
                    required: false,
                    read_only: false,
                },
            ],
            dependency_services: vec![],
        };

        stage_dependencies_for_trial(&agent_runtime, &paths).expect("stage host files");
        assert_eq!(
            fs::read_to_string(paths.state.join(".graphd").join("graphd.db")).expect("staged db"),
            "db-bytes"
        );
        assert!(
            !paths.state.join(".graphd").join("graphd.db-wal").exists(),
            "optional missing source should not create destination"
        );
    }

    #[test]
    fn replay_grade_maps_by_integration_level() {
        assert_eq!(replay_grade_for_integration("sdk_full"), "strict");
        assert_eq!(replay_grade_for_integration("sdk_control"), "checkpointed");
        assert_eq!(replay_grade_for_integration("cli_events"), "best_effort");
        assert_eq!(replay_grade_for_integration("cli_basic"), "best_effort");
    }

    #[test]
    fn run_operation_lock_is_exclusive() {
        let run_dir = std::env::temp_dir().join(format!(
            "agentlab_lock_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        ensure_dir(&run_dir).expect("temp run dir");

        let lock1 = acquire_run_operation_lock(&run_dir).expect("first lock must succeed");
        let err = acquire_run_operation_lock(&run_dir).expect_err("second lock must fail");
        assert!(
            err.to_string().contains("operation_in_progress"),
            "unexpected lock error: {}",
            err
        );
        drop(lock1);
        let lock2 = acquire_run_operation_lock(&run_dir).expect("lock should be re-acquirable");
        drop(lock2);
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn fork_selector_parser_accepts_supported_kinds() {
        match parse_fork_selector("checkpoint:ckpt_a").expect("checkpoint selector") {
            ForkSelector::Checkpoint(v) => assert_eq!(v, "ckpt_a"),
            _ => panic!("expected checkpoint"),
        }
        match parse_fork_selector("step:12").expect("step selector") {
            ForkSelector::Step(v) => assert_eq!(v, 12),
            _ => panic!("expected step"),
        }
        match parse_fork_selector("event_seq:34").expect("event_seq selector") {
            ForkSelector::EventSeq(v) => assert_eq!(v, 34),
            _ => panic!("expected event_seq"),
        }
        assert!(parse_fork_selector("bad").is_err());
        assert!(parse_fork_selector("unknown:1").is_err());
    }

    #[test]
    fn adapter_control_ack_received_matches_action_and_control_version() {
        let root = std::env::temp_dir().join(format!(
            "agentlab_ack_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        ensure_dir(&root).expect("temp dir");
        let events_path = root.join("harness_events.jsonl");
        let line = r#"{"event_type":"control_ack","seq":9,"step_index":2,"control_version":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","action_observed":"stop"}"#;
        atomic_write_bytes(&events_path, format!("{}\n", line).as_bytes()).expect("write events");

        assert!(adapter_control_ack_received(
            &events_path,
            "stop",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        .expect("parse ack"));
        assert!(!adapter_control_ack_received(
            &events_path,
            "checkpoint",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        .expect("parse ack"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_resume_selector_prefers_requested_label() {
        let root = std::env::temp_dir().join(format!(
            "agentlab_resume_sel_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        ensure_dir(&root).expect("root");
        let trial_dir = root.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let output = json!({
            "schema_version": "agent_result_v1",
            "outcome": "success",
            "checkpoints": [
                {"path": format!("{}/ckpt_a", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "a", "step": 1},
                {"path": format!("{}/ckpt_b", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "b", "step": 2}
            ]
        });
        atomic_write_json_pretty(&trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME), &output)
            .expect("write");
        let selector = resolve_resume_selector(&trial_dir, Some("a")).expect("selector");
        assert_eq!(selector, "checkpoint:a");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_resume_selector_defaults_to_latest_step() {
        let root = std::env::temp_dir().join(format!(
            "agentlab_resume_default_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        ensure_dir(&root).expect("root");
        let trial_dir = root.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let output = json!({
            "schema_version": "agent_result_v1",
            "outcome": "success",
            "checkpoints": [
                {"path": format!("{}/ckpt_a", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "a", "step": 3},
                {"path": format!("{}/ckpt_b", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "b", "step": 5}
            ]
        });
        atomic_write_json_pretty(&trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME), &output)
            .expect("write");
        let selector = resolve_resume_selector(&trial_dir, None).expect("selector");
        assert_eq!(selector, "checkpoint:b");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_resume_selector_errors_when_label_not_found() {
        let root = TempDirGuard::new("agentlab_resume_missing_label_test");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let output = json!({
            "schema_version": "agent_result_v1",
            "outcome": "success",
            "checkpoints": [
                {"path": format!("{}/ckpt_a", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "a", "step": 1}
            ]
        });
        atomic_write_json_pretty(&trial_dir.join(CANONICAL_TRIAL_RESULT_FILENAME), &output)
            .expect("write");
        let err = resolve_resume_selector(&trial_dir, Some("missing")).expect_err("should fail");
        assert!(
            err.to_string().contains("resume_checkpoint_not_found"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn parse_fork_selector_rejects_empty_checkpoint_name() {
        let err = match parse_fork_selector("checkpoint: ") {
            Ok(_) => panic!("empty checkpoint should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("checkpoint name empty"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_selector_checkpoint_non_strict_returns_none_when_path_missing() {
        let root = TempDirGuard::new("agentlab_fork_selector_path_missing");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let output = json!({
            "checkpoints": [
                {"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 3}
            ]
        });
        let selector = parse_fork_selector("checkpoint:cp1").expect("selector");
        let source = resolve_selector_checkpoint(&selector, Some(&output), &trial_dir, false)
            .expect("selector resolution");
        assert_eq!(source, None);
    }

    #[test]
    fn resolve_selector_checkpoint_strict_requires_existing_checkpoint_path() {
        let root = TempDirGuard::new("agentlab_fork_selector_strict_missing");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let output = json!({
            "checkpoints": [
                {"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 3}
            ]
        });
        let selector = parse_fork_selector("checkpoint:cp1").expect("selector");
        let err = resolve_selector_checkpoint(&selector, Some(&output), &trial_dir, true)
            .expect_err("strict resolution should fail");
        assert!(
            err.to_string().contains("strict_source_unavailable"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn fork_trial_non_strict_falls_back_to_input_only_when_checkpoint_missing() {
        let (_root, run_dir) = create_run_dir("agentlab_fork_input_fallback", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "completed",
            None,
        );

        let result = fork_trial(
            &run_dir,
            "trial_1",
            "checkpoint:cp1",
            &BTreeMap::new(),
            false,
        )
        .expect("fork should succeed");
        assert_eq!(result.fallback_mode, "input_only");
        assert_eq!(result.source_checkpoint, None);

        let manifest = load_json_file(&result.fork_dir.join("manifest.json")).expect("manifest");
        assert_eq!(
            manifest
                .pointer("/fallback_mode")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "input_only"
        );
        assert!(manifest.pointer("/source_checkpoint").is_some());
    }

    #[test]
    fn fork_trial_strict_requires_sdk_full_integration_level() {
        let (_root, run_dir) = create_run_dir("agentlab_fork_strict_level", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp1", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "completed",
            None,
        );

        let err = fork_trial(
            &run_dir,
            "trial_1",
            "checkpoint:cp1",
            &BTreeMap::new(),
            true,
        )
        .err()
        .expect("strict fork should fail for non-sdk_full");
        assert!(
            err.to_string()
                .contains("strict fork requires integration_level sdk_full"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn fork_trial_strict_fails_when_selected_checkpoint_is_unavailable() {
        let (_root, run_dir) = create_run_dir("agentlab_fork_strict_checkpoint", "run_1");
        write_resolved_experiment(&run_dir, "sdk_full", true);
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "completed",
            None,
        );

        let err = fork_trial(
            &run_dir,
            "trial_1",
            "checkpoint:cp1",
            &BTreeMap::new(),
            true,
        )
        .err()
        .expect("strict fork should fail when checkpoint bytes are unavailable");
        assert!(
            err.to_string().contains("strict_source_unavailable"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn pause_run_rejects_target_trial_that_is_not_active() {
        let (_root, run_dir) = create_run_dir("agentlab_pause_not_active", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(
            &run_dir,
            "run_1",
            "running",
            Some("trial_1"),
            Some(&control),
        );

        let err = pause_run(&run_dir, Some("trial_2"), Some("pause"), 1)
            .err()
            .expect("pause should reject non-active target");
        assert!(
            err.to_string().contains("pause_target_not_active"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn pause_run_uses_default_events_path_for_supported_integration_levels() {
        let (_root, run_dir) = create_run_dir("agentlab_pause_events_required", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", false);
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let control_path = trial_dir.join("state").join("lab_control.json");
        let events_path = trial_dir.join("state").join("events.jsonl");
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(
            &run_dir,
            "run_1",
            "running",
            Some("trial_1"),
            Some(&control),
        );

        let ack_thread = spawn_pause_ack_writer(control_path.clone(), events_path);
        let paused = pause_run(&run_dir, None, Some("pause"), 2).expect("pause success");
        ack_thread.join().expect("ack writer thread");
        assert_eq!(paused.label, "pause");
    }

    #[test]
    fn pause_run_completes_checkpoint_then_stop_and_updates_state() {
        let (_root, run_dir) = create_run_dir("agentlab_pause_success", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let control_path = trial_dir.join("state").join("lab_control.json");
        let events_path = trial_dir.join("state").join("events.jsonl");
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(
            &run_dir,
            "run_1",
            "running",
            Some("trial_1"),
            Some(&control),
        );

        let ack_thread = spawn_pause_ack_writer(control_path.clone(), events_path);
        let paused = pause_run(&run_dir, None, Some("manual_pause"), 2).expect("pause success");
        ack_thread.join().expect("ack writer thread");

        assert_eq!(paused.run_id, "run_1");
        assert_eq!(paused.trial_id, "trial_1");
        assert_eq!(paused.label, "manual_pause");
        assert!(paused.checkpoint_acked);
        assert!(paused.stop_acked);

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused"
        );
        assert_eq!(
            run_control
                .pointer("/active_trials/trial_1/trial_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "trial_1"
        );

        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(
            trial_state
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused"
        );
        assert_eq!(
            trial_state
                .pointer("/pause_label")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "manual_pause"
        );
        assert_eq!(
            trial_state
                .pointer("/checkpoint_selected")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "manual_pause"
        );
        assert_eq!(
            trial_state
                .pointer("/exit_reason")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused_by_user"
        );
    }

    #[test]
    fn resume_run_requires_run_to_be_paused() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_not_paused", "run_1");
        write_resolved_experiment(&run_dir, "sdk_full", true);
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp1", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "paused",
            Some("cp1"),
        );
        ensure_dir(&trial_dir.join("state").join("cp1")).expect("checkpoint path");
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(
            &run_dir,
            "run_1",
            "running",
            Some("trial_1"),
            Some(&control),
        );

        let err = resume_trial(&run_dir, None, None, &BTreeMap::new(), false)
            .err()
            .expect("resume should fail for non-paused run");
        assert!(
            err.to_string().contains("resume_non_paused"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resume_run_requires_trial_state_to_be_paused() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_trial_state", "run_1");
        write_resolved_experiment(&run_dir, "sdk_full", true);
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp1", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "completed",
            None,
        );
        ensure_dir(&trial_dir.join("state").join("cp1")).expect("checkpoint path");
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(&run_dir, "run_1", "paused", Some("trial_1"), Some(&control));

        let err = resume_trial(&run_dir, None, None, &BTreeMap::new(), false)
            .err()
            .expect("resume should fail when trial state is not paused");
        assert!(
            err.to_string().contains("resume_trial_not_paused"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resume_run_uses_pause_label_and_forks_with_binding_overrides() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_success", "run_1");
        write_resolved_experiment(&run_dir, "sdk_full", true);
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([
                {"path": format!("{}/cp_old", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp_old", "step": 1},
                {"path": format!("{}/cp_resume", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp_resume", "step": 2}
            ]),
            "paused",
            Some("cp_resume"),
        );
        ensure_dir(&trial_dir.join("state").join("cp_resume")).expect("checkpoint path");
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(&run_dir, "run_1", "paused", Some("trial_1"), Some(&control));

        let mut set_bindings = BTreeMap::new();
        set_bindings.insert("resume.override".to_string(), json!(42));
        let resumed =
            resume_trial(&run_dir, None, None, &set_bindings, false).expect("resume success");

        assert_eq!(resumed.trial_id, "trial_1");
        assert_eq!(resumed.selector, "checkpoint:cp_resume");
        assert_eq!(resumed.fork.parent_trial_id, "trial_1");
        assert_eq!(resumed.fork.fallback_mode, "checkpoint");
        assert!(resumed.fork.source_checkpoint.is_some());

        let fork_input = load_json_file(
            &resumed
                .fork
                .fork_dir
                .join("trial_1")
                .join("trial_input.json"),
        )
        .expect("fork trial input");
        assert_eq!(
            fork_input
                .pointer("/bindings/resume/override")
                .and_then(|v| v.as_i64())
                .unwrap_or_default(),
            42
        );
        assert_eq!(
            fork_input
                .pointer("/ext/fork/selector")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "checkpoint:cp_resume"
        );
    }

    #[test]
    fn validate_required_fields_passes_on_complete_spec() {
        let spec = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl", "provider": "local_jsonl", "suite_id": "s", "schema_version": "v1", "split_id": "dev", "limit": 50 },
            "design": { "sanitization_profile": "hermetic_functional", "comparison": "paired", "replications": 1, "random_seed": 1337, "shuffle_tasks": true, "max_concurrency": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": { "command": ["node", "h.js"] },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "local" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });
        validate_required_fields(&spec).expect("valid spec should pass");
    }

    #[test]
    fn validate_required_fields_reports_all_missing() {
        let spec = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n" },
            "dataset": { "path": "tasks.jsonl" },
            "design": {},
            "baseline": {},
            "runtime": {
                "agent": {},
                "policy": { "sandbox": { "mode": "local" }, "network": {} }
            }
        });
        let err = validate_required_fields(&spec).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("/experiment/workload_type"),
            "missing workload_type: {}",
            msg
        );
        assert!(
            msg.contains("/design/sanitization_profile"),
            "missing sanitization_profile: {}",
            msg
        );
        assert!(
            msg.contains("/design/replications"),
            "missing replications: {}",
            msg
        );
        assert!(
            msg.contains("/runtime/agent"),
            "missing runtime.agent: {}",
            msg
        );
        assert!(
            msg.contains("/runtime/policy/network/mode"),
            "missing network mode: {}",
            msg
        );
        assert!(
            msg.contains("/baseline/variant_id"),
            "missing baseline variant_id: {}",
            msg
        );
    }

    #[test]
    fn validate_required_fields_allows_missing_integration_level() {
        let spec = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl", "provider": "local_jsonl", "suite_id": "s", "schema_version": "v1", "split_id": "dev", "limit": 50 },
            "design": { "sanitization_profile": "hermetic_functional", "comparison": "paired", "replications": 1, "random_seed": 1337, "shuffle_tasks": true, "max_concurrency": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": { "command": ["node", "h.js"] },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "local" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });
        validate_required_fields(&spec).expect("missing integration_level should default");
    }

    #[test]
    fn validate_required_fields_requires_image_for_container_mode() {
        let spec = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl", "provider": "local_jsonl", "suite_id": "s", "schema_version": "v1", "split_id": "dev", "limit": 50 },
            "design": { "sanitization_profile": "hermetic_functional", "comparison": "paired", "replications": 1, "random_seed": 1337, "shuffle_tasks": true, "max_concurrency": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": {
                    "command": ["node", "/app/h.js"]
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "container" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });
        let err = validate_required_fields(&spec).expect_err("should fail");
        assert!(
            err.to_string().contains("/runtime/agent/image"),
            "missing sandbox image: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_allows_per_task_image_source_without_global_image() {
        let spec = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl", "provider": "local_jsonl", "suite_id": "s", "schema_version": "v1", "split_id": "dev", "limit": 50 },
            "design": { "sanitization_profile": "hermetic_functional", "comparison": "paired", "replications": 1, "random_seed": 1337, "shuffle_tasks": true, "max_concurrency": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": {
                    "command": ["node", "/app/h.js"],
                    "image_source": "per_task",
                    "artifact": ".lab/agents/rex-current.tar.gz"
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "container" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });
        validate_required_fields(&spec)
            .expect("per-task image mode should not require runtime.agent.image");
    }

    #[test]
    fn validate_required_fields_requires_artifact_for_per_task_image_source() {
        let spec = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl", "provider": "local_jsonl", "suite_id": "s", "schema_version": "v1", "split_id": "dev", "limit": 50 },
            "design": { "sanitization_profile": "hermetic_functional", "comparison": "paired", "replications": 1, "random_seed": 1337, "shuffle_tasks": true, "max_concurrency": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": {
                    "command": ["node", "/app/h.js"],
                    "image_source": "per_task"
                },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "container" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });
        let err =
            validate_required_fields(&spec).expect_err("missing per-task artifact should fail");
        assert!(
            err.to_string().contains("/runtime/agent/artifact"),
            "expected missing artifact error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_v1_accepts_flat_shape() {
        let spec = json!({
            "version": "1.0",
            "experiment": { "id": "e", "name": "n" },
            "dataset": { "path": "tasks.jsonl" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base" },
            "runtime": {
                "image": "my-harness:latest",
                "command": ["python", "harness.py"],
                "timeout_ms": 1000,
                "network": "none"
            }
        });
        validate_required_fields(&spec).expect("valid v1 spec should pass");
    }

    #[test]
    fn validate_required_fields_v1_reports_flat_runtime_requirements() {
        let spec = json!({
            "version": "1.0",
            "experiment": { "id": "e", "name": "n" },
            "dataset": { "path": "tasks.jsonl" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base" },
            "runtime": {}
        });
        let err = validate_required_fields(&spec).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("/runtime/image"),
            "missing runtime.image: {}",
            msg
        );
        assert!(
            msg.contains("/runtime/command"),
            "missing runtime.command: {}",
            msg
        );
    }

    #[test]
    fn resolve_variant_plan_rejects_legacy_variants_field() {
        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variants": [
                { "variant_id": "old", "bindings": { "temperature": 0.7 } }
            ]
        });

        let err = resolve_variant_plan(&spec).expect_err("legacy variants should fail");
        assert!(
            err.to_string().contains("/variants is not supported"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_variant_plan_rejects_bad_variant_plan_entry() {
        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [
                { "bindings": { "temperature": 0.8 } },
                { "variant_id": "t2", "bindings": [] }
            ]
        });

        let err = resolve_variant_plan(&spec).expect_err("bad variant plan should fail");
        assert!(
            err.to_string().contains("variant_plan[0]"),
            "unexpected error: {}",
            err
        );

        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [
                { "variant_id": "t2", "bindings": [] }
            ]
        });
        let err = resolve_variant_plan(&spec).expect_err("bad variant bindings type should fail");
        assert!(
            err.to_string().contains("variant_plan[0].bindings"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_variant_plan_uses_baseline_when_no_variant_plan_present() {
        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} }
        });

        let (variants, baseline_id) = resolve_variant_plan(&spec).expect("baseline only");
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].id, "base");
    }

    #[test]
    fn load_run_variants_falls_back_to_experiment_when_manifest_missing() {
        let (_root, run_dir) = create_run_dir("agentlab_variants_fallback", "run_1");
        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [{ "variant_id": "alt", "bindings": { "temperature": 1.2 } }]
        });

        let (variants, baseline_id) =
            load_run_variants(&run_dir, &spec).expect("load fallback variants");
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].id, "base");
        assert_eq!(variants[1].id, "alt");
    }

    #[test]
    fn load_run_variants_prefers_resolved_manifest_over_experiment() {
        let (_root, run_dir) = create_run_dir("agentlab_variants_manifest_preferred", "run_1");
        let original = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [{ "variant_id": "alt", "bindings": { "temperature": 1.2 } }]
        });
        let (resolved_variants, resolved_baseline) =
            resolve_variant_plan(&original).expect("resolve variants");
        write_resolved_variants(&run_dir, &resolved_baseline, &resolved_variants)
            .expect("write manifest");

        let changed = json!({
            "baseline": { "variant_id": "changed", "bindings": {} },
            "variant_plan": [{ "variant_id": "new", "bindings": { "temperature": 0.2 } }]
        });
        let (loaded_variants, loaded_baseline) =
            load_run_variants(&run_dir, &changed).expect("load manifest variants");

        assert_eq!(loaded_baseline, "base");
        assert_eq!(loaded_variants.len(), 2);
        assert_eq!(loaded_variants[0].id, "base");
        assert_eq!(loaded_variants[1].id, "alt");
    }

    #[test]
    fn resolve_variant_plan_parses_clean_contract_args_env_image() {
        let spec = json!({
            "version": "1.0",
            "baseline": {
                "variant_id": "control",
                "args": ["--temperature", "0.7"],
                "env": { "DEBUG": "0" }
            },
            "variant_plan": [
                {
                    "variant_id": "hot",
                    "args": ["--temperature", "0.9"],
                    "env": { "DEBUG": "1" },
                    "image": "my-harness-v2:latest"
                }
            ]
        });

        let (variants, baseline_id) = resolve_variant_plan(&spec).expect("variant plan");
        assert_eq!(baseline_id, "control");
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].args, vec!["--temperature", "0.7"]);
        assert_eq!(variants[1].env.get("DEBUG").map(String::as_str), Some("1"));
        assert_eq!(variants[1].image.as_deref(), Some("my-harness-v2:latest"));
    }

    #[test]
    fn variant_digest_changes_with_variant_configuration() {
        let base = Variant {
            id: "base".to_string(),
            bindings: json!({}),
            args: vec!["--temperature".to_string(), "0.7".to_string()],
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let mut changed = base.clone();
        changed.args = vec!["--temperature".to_string(), "1.2".to_string()];

        let base_digest = variant_digest(&base).expect("base digest");
        let changed_digest = variant_digest(&changed).expect("changed digest");
        assert_ne!(base_digest, changed_digest);
    }

    #[test]
    fn resolve_variant_plan_parses_runtime_overrides() {
        let spec = json!({
            "baseline": {
                "variant_id": "base",
                "bindings": {},
                "runtime_overrides": {
                    "policy": {
                        "timeout_ms": 123000
                    }
                }
            },
            "variant_plan": [
                {
                    "variant_id": "treatment",
                    "bindings": {},
                    "runtime_overrides": {
                        "agent": {
                            "custom_image": {
                                "image": "example:variant"
                            }
                        }
                    }
                }
            ]
        });

        let (variants, baseline_id) = resolve_variant_plan(&spec).expect("variant plan");
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 2);
        assert!(variants[0].runtime_overrides.is_some());
        assert!(variants[1].runtime_overrides.is_some());
    }

    #[test]
    fn resolve_variant_plan_rejects_invalid_runtime_overrides_shape() {
        let spec = json!({
            "baseline": {
                "variant_id": "base",
                "bindings": {},
                "runtime_overrides": "bad"
            }
        });
        let err = resolve_variant_plan(&spec).expect_err("baseline runtime_overrides should fail");
        assert!(
            err.to_string().contains("/baseline/runtime_overrides"),
            "unexpected error: {}",
            err
        );

        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [
                {
                    "variant_id": "treatment",
                    "bindings": {},
                    "runtime_overrides": "bad"
                }
            ]
        });
        let err = resolve_variant_plan(&spec).expect_err("variant runtime_overrides should fail");
        assert!(
            err.to_string()
                .contains("/variant_plan[0].runtime_overrides"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_runtime_for_variant_merges_runtime_overrides() {
        let base = json!({
            "runtime": {
                "agent": {
                    "mode": "custom_image",
                    "custom_image": {
                        "image": "base:image",
                        "entrypoint": ["echo", "base"]
                    },
                    "overrides": {
                        "env": {
                            "A": "1",
                            "B": "2"
                        }
                    }
                },
                "policy": {
                    "timeout_ms": 600000
                }
            }
        });
        let variant = Variant {
            id: "treatment".to_string(),
            bindings: json!({}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: Some(json!({
                "agent": {
                    "custom_image": {
                        "image": "treatment:image"
                    },
                    "overrides": {
                        "env": {
                            "B": "override",
                            "C": "3"
                        }
                    }
                },
                "policy": {
                    "timeout_ms": 900000
                }
            })),
        };

        let merged = resolve_runtime_for_variant(&base, &variant).expect("merge");
        assert_eq!(
            merged
                .pointer("/runtime/agent/custom_image/image")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "treatment:image"
        );
        assert_eq!(
            merged
                .pointer("/runtime/agent/custom_image/entrypoint")
                .and_then(|v| v.as_array())
                .map(|v| v.len())
                .unwrap_or(0),
            2
        );
        assert_eq!(
            merged
                .pointer("/runtime/agent/overrides/env/A")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "1"
        );
        assert_eq!(
            merged
                .pointer("/runtime/agent/overrides/env/B")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "override"
        );
        assert_eq!(
            merged
                .pointer("/runtime/agent/overrides/env/C")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "3"
        );
        assert_eq!(
            merged
                .pointer("/runtime/policy/timeout_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            900000
        );
    }

    #[test]
    fn validate_required_fields_requires_benchmark_adapter_command() {
        let spec = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl", "provider": "local_jsonl", "suite_id": "s", "schema_version": "v1", "split_id": "dev", "limit": 50 },
            "design": { "sanitization_profile": "hermetic_functional", "comparison": "paired", "replications": 1, "random_seed": 1337, "shuffle_tasks": true, "max_concurrency": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "benchmark": {
                "policy": { "task_model": "independent" }
            },
            "runtime": {
                "agent": { "mode": "custom_image", "custom_image": { "entrypoint": ["node", "h.js"] } },
                "policy": {
                    "timeout_ms": 600000,
                    "sandbox": { "mode": "local" },
                    "network": { "mode": "none", "allowed_hosts": [] }
                }
            }
        });
        let err = validate_required_fields(&spec).expect_err("should fail");
        assert!(
            err.to_string().contains("/benchmark/adapter/command"),
            "missing benchmark adapter command: {}",
            err
        );
    }

    #[test]
    fn process_benchmark_outputs_generates_summary_when_missing() {
        let root = TempDirGuard::new("agentlab_benchmark_adapter_summary");
        let project_root = root.path.join("project");
        let run_dir = root.path.join("run");
        ensure_dir(&project_root).expect("project root");
        ensure_dir(&run_dir).expect("run dir");

        let evidence_dir = run_dir.join("evidence");
        ensure_dir(&evidence_dir).expect("evidence dir");
        let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
        let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence records");
        fs::write(&task_chain_states_path, "").expect("task chain states");
        let benchmark_dir = run_dir.join("benchmark");
        ensure_dir(&benchmark_dir).expect("benchmark dir");
        fs::write(
            benchmark_dir.join("predictions.jsonl"),
            r#"{"schema_version":"benchmark_prediction_record_v1","ids":{"run_id":"run_123","trial_id":"trial_1","variant_id":"base","task_id":"task_1","repl_idx":0},"benchmark":{"adapter_id":"demo_adapter","name":"demo_suite","split":"dev"},"prediction":{"kind":"json","value":{"patch":"diff --git"}}}
"#,
        )
        .expect("predictions");
        fs::write(
            benchmark_dir.join("scores.jsonl"),
            r#"{"schema_version":"benchmark_score_record_v1","ids":{"run_id":"run_123","trial_id":"trial_1","variant_id":"base","task_id":"task_1","repl_idx":0},"benchmark":{"adapter_id":"demo_adapter","name":"demo_suite","split":"dev"},"verdict":"pass","primary_metric_name":"resolved","primary_metric_value":1.0,"metrics":{"resolved":1.0},"evaluator":{"name":"demo_eval","mode":"custom"}}
"#,
        )
        .expect("scores");

        let adapter = BenchmarkAdapterConfig {
            command: vec!["adapter".to_string(), "unused".to_string()],
            manifest: Some(json!(
                {"schema_version":"benchmark_adapter_manifest_v1","adapter_id":"demo_adapter","adapter_version":"1.0.0","benchmark":{"name":"demo_suite","split":"dev"},"execution_mode":"predict_then_score","record_schemas":{"prediction":"benchmark_prediction_record_v1","score":"benchmark_score_record_v1"},"evaluator":{"name":"demo_eval","mode":"custom"}}
            )),
        };

        let scores_path = process_benchmark_outputs(
            &project_root,
            &run_dir,
            "run_123",
            &adapter,
            &evidence_records_path,
            &task_chain_states_path,
        )
        .expect("benchmark processing should succeed");
        assert!(scores_path.exists(), "scores path should exist");

        let summary_path = run_dir.join("benchmark").join("summary.json");
        let summary = load_json_file(&summary_path).expect("summary");
        assert_eq!(
            summary
                .pointer("/schema_version")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "benchmark_summary_v1"
        );
        assert_eq!(
            summary
                .pointer("/benchmark/adapter_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "demo_adapter"
        );
        assert_eq!(
            summary
                .pointer("/totals/pass")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn process_benchmark_outputs_synthesizes_manifest_when_missing() {
        let root = TempDirGuard::new("agentlab_benchmark_manifest_fallback");
        let project_root = root.path.join("project");
        let run_dir = root.path.join("run");
        ensure_dir(&project_root).expect("project root");
        ensure_dir(&run_dir).expect("run dir");

        let evidence_dir = run_dir.join("evidence");
        ensure_dir(&evidence_dir).expect("evidence dir");
        let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
        let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence records");
        fs::write(&task_chain_states_path, "").expect("task chain states");
        let benchmark_dir = run_dir.join("benchmark");
        ensure_dir(&benchmark_dir).expect("benchmark dir");
        fs::write(
            benchmark_dir.join("scores.jsonl"),
            r#"{"schema_version":"benchmark_score_record_v1","ids":{"run_id":"run_456","trial_id":"trial_9","variant_id":"base","task_id":"task_9","repl_idx":0},"benchmark":{"adapter_id":"fallback_adapter","name":"suite_x","split":"dev"},"verdict":"fail","primary_metric_name":"resolved","primary_metric_value":0.0,"metrics":{"resolved":0.0},"evaluator":{"name":"fallback_eval","mode":"custom"}}
"#,
        )
        .expect("scores");

        let adapter = BenchmarkAdapterConfig {
            command: vec!["fallback_adapter".to_string()],
            manifest: None,
        };
        process_benchmark_outputs(
            &project_root,
            &run_dir,
            "run_456",
            &adapter,
            &evidence_records_path,
            &task_chain_states_path,
        )
        .expect("benchmark processing should succeed");

        let manifest =
            load_json_file(&benchmark_dir.join("adapter_manifest.json")).expect("manifest");
        assert_eq!(
            manifest
                .pointer("/adapter_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "fallback_adapter"
        );
    }

    #[test]
    fn p0_freeze_benchmark_adaptation_trial_shape_fixture_parses() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../testdata/p0_benchmark_adaptation_trial_shape.json"
        ))
        .expect("fixture json");
        let resolved = fixture
            .pointer("/resolved_experiment")
            .cloned()
            .expect("resolved fixture");
        let benchmark = parse_benchmark_config(&resolved);
        assert_eq!(benchmark.policy.task_model, TaskModel::Dependent);
        assert_eq!(benchmark.policy.scoring_lifecycle, "predict_then_score");
        assert_eq!(
            benchmark.policy.required_evidence_classes,
            vec!["agent_patch".to_string(), "grader_report".to_string()]
        );
        assert_eq!(
            benchmark
                .adapter
                .as_ref()
                .map(|adapter| adapter.command.len())
                .unwrap_or(0),
            2
        );

        let dataset_task = fixture
            .pointer("/dataset_task_row")
            .cloned()
            .expect("dataset task row");
        let boundary = parse_task_boundary_from_dataset_task(&dataset_task).expect("task boundary");
        assert_eq!(
            boundary
                .task_payload
                .pointer("/id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "swebench__django__12345"
        );
        assert_eq!(
            boundary
                .task_payload
                .pointer("/image")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "ghcr.io/acme/swebench-task:20260222"
        );
        assert_eq!(
            boundary
                .task_payload
                .pointer("/workspace/repo")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "django/django"
        );
        assert_eq!(boundary.workspace_files.len(), 1);
        assert_eq!(boundary.mount_references.len(), 1);
        assert_eq!(boundary.limits.max_steps, Some(12));
        assert_eq!(boundary.limits.max_total_tokens, Some(16000));
        assert_eq!(boundary.limits.max_tool_calls, Some(32));
        assert_eq!(boundary.limits.trial_seconds, Some(1800));
    }

    #[test]
    fn p6_run_control_v2_writer_emits_active_trials_without_legacy_mirrors() {
        let (_root, run_dir) = create_run_dir("agentlab_run_control_v2_writer", "run_1");
        write_test_run_control(&run_dir, "run_1", "running", Some("trial_1"), None);
        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");

        assert_eq!(
            run_control
                .pointer("/schema_version")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "run_control_v2"
        );
        assert_eq!(
            run_control
                .pointer("/active_trials/trial_1/trial_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "trial_1"
        );
        assert!(
            run_control.pointer("/active_trial_id").is_none(),
            "legacy /active_trial_id should be removed in P6 cleanup"
        );
        assert!(
            run_control.pointer("/active_adapter").is_none(),
            "legacy /active_adapter should be removed in P6 cleanup"
        );
    }

    #[test]
    fn p1_run_control_v2_schema_accepts_writer_payload() {
        let (_root, run_dir) = create_run_dir("agentlab_run_control_v2_schema", "run_1");
        write_test_run_control(&run_dir, "run_1", "running", Some("trial_1"), None);
        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        let schema = compile_schema("run_control_v2.jsonschema").expect("schema");
        match schema.validate(&run_control) {
            Ok(_) => {}
            Err(errors) => {
                let mut messages = Vec::new();
                for err in errors {
                    messages.push(err.to_string());
                }
                panic!(
                    "run_control_v2 schema validation failed: {}",
                    messages.join(" | ")
                );
            }
        };
    }

    #[test]
    fn p1_run_control_helpers_read_active_trial_and_control_from_v2_shape() {
        let run_control = json!({
            "schema_version": "run_control_v2",
            "run_id": "run_1",
            "status": "running",
            "active_trials": {
                "trial_alpha": {
                    "trial_id": "trial_alpha",
                    "worker_id": "worker_1",
                    "schedule_idx": 7,
                    "variant_id": "base",
                    "started_at": "2026-02-22T00:00:00Z",
                    "control": {
                        "id": "builtin.command_contract",
                        "version": "v1",
                        "command_path": "/tmp/control.json",
                        "events_path": "/tmp/events.jsonl"
                    }
                }
            },
            "updated_at": "2026-02-22T00:00:00Z"
        });

        let ids = run_control_active_trial_ids(&run_control);
        assert_eq!(ids, vec!["trial_alpha".to_string()]);
        let control = run_control_active_adapter_for_trial(&run_control, "trial_alpha")
            .expect("active adapter control");
        assert_eq!(
            control
                .pointer("/command_path")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "/tmp/control.json"
        );
    }

    fn worker_dispatch_fixture(schedule_idx: usize, trial_id: &str) -> TrialDispatch {
        TrialDispatch {
            run_id: "run_fixture".to_string(),
            trial_id: trial_id.to_string(),
            schedule_idx,
            slot: TrialSlot {
                variant_idx: 0,
                task_idx: schedule_idx,
                repl_idx: 0,
            },
            variant_id: "baseline".to_string(),
            task_id: format!("task_{}", schedule_idx),
            repl_idx: 0,
            runtime_profile: json!({ "runtime": { "agent": { "image": "img" } } }),
            task_payload: json!({ "id": format!("task_{}", schedule_idx) }),
            effective_policy: json!({ "timeout_ms": 1000 }),
        }
    }

    fn worker_completion_fixture(
        ticket: &WorkerTicket,
        schedule_idx: usize,
        classification: &str,
    ) -> TrialCompletion {
        TrialCompletion {
            ticket: ticket.clone(),
            schedule_idx,
            completion_seq: None,
            terminal_status: "succeeded".to_string(),
            classification: classification.to_string(),
            artifacts: json!({ "result": "ok" }),
            metrics: json!({ "latency_ms": 10 }),
            runtime_summary: json!({ "engine": "fixture" }),
        }
    }

    fn worker_completion_fixture_with_seq(
        ticket: &WorkerTicket,
        schedule_idx: usize,
        classification: &str,
        completion_seq: u64,
    ) -> TrialCompletion {
        let mut completion = worker_completion_fixture(ticket, schedule_idx, classification);
        completion.completion_seq = Some(completion_seq);
        completion
    }

    #[derive(Debug, Deserialize)]
    struct P2EDeterminismFixture {
        schema_version: String,
        arrivals: Vec<P2EDeterminismArrival>,
        expected_commit_schedule_idx: Vec<usize>,
    }

    #[derive(Debug, Deserialize)]
    struct P2EDeterminismArrival {
        tick: usize,
        schedule_idx: usize,
        trial_id: String,
        classification: String,
    }

    struct OutOfOrderCompletionSimulator {
        by_tick: BTreeMap<usize, Vec<TrialCompletion>>,
    }

    impl OutOfOrderCompletionSimulator {
        fn from_fixture(fixture: &P2EDeterminismFixture) -> Self {
            let mut by_tick: BTreeMap<usize, Vec<TrialCompletion>> = BTreeMap::new();
            for row in fixture.arrivals.iter() {
                let ticket = WorkerTicket {
                    worker_id: format!("worker_{}", row.trial_id),
                    ticket_id: format!("ticket_{}", row.trial_id),
                    trial_id: row.trial_id.clone(),
                };
                by_tick
                    .entry(row.tick)
                    .or_default()
                    .push(worker_completion_fixture(
                        &ticket,
                        row.schedule_idx,
                        row.classification.as_str(),
                    ));
            }
            Self { by_tick }
        }

        fn max_tick(&self) -> usize {
            self.by_tick.keys().copied().max().unwrap_or(0)
        }

        fn poll_tick(&mut self, tick: usize) -> Vec<TrialCompletion> {
            self.by_tick.remove(&tick).unwrap_or_default()
        }
    }

    fn load_p2e_determinism_fixture() -> P2EDeterminismFixture {
        let fixture: P2EDeterminismFixture =
            serde_json::from_str(include_str!("../testdata/p2e_determinism_fixture.json"))
                .expect("p2e fixture json");
        assert_eq!(fixture.schema_version, "p2e_determinism_fixture_v1");
        fixture
    }

    fn drain_ready_completions_in_schedule_order(
        pending: &mut BTreeMap<usize, TrialCompletion>,
        next_commit_idx: &mut usize,
    ) -> Vec<TrialCompletion> {
        let mut ready = Vec::new();
        loop {
            let Some(completion) = pending.remove(next_commit_idx) else {
                break;
            };
            *next_commit_idx += 1;
            ready.push(completion);
        }
        ready
    }

    #[test]
    fn p2c_local_thread_worker_backend_enforces_capacity_and_polls_completions() {
        let executor: Arc<LocalTrialExecutor> = Arc::new(|dispatch| {
            thread::sleep(Duration::from_millis(80));
            Ok(TrialCompletion {
                ticket: WorkerTicket {
                    worker_id: "ignored".to_string(),
                    ticket_id: "ignored".to_string(),
                    trial_id: dispatch.trial_id.clone(),
                },
                schedule_idx: usize::MAX,
                completion_seq: None,
                terminal_status: "succeeded".to_string(),
                classification: format!("completed_{}", dispatch.trial_id),
                artifacts: json!({}),
                metrics: json!({}),
                runtime_summary: json!({}),
            })
        });
        let backend = LocalThreadWorkerBackend::new(1, executor).expect("backend");

        let dispatch_a = worker_dispatch_fixture(0, "trial_alpha");
        let dispatch_b = worker_dispatch_fixture(1, "trial_beta");
        let ticket_a = backend.submit(dispatch_a.clone()).expect("submit A");
        let err = backend
            .submit(dispatch_b.clone())
            .expect_err("capacity should block submit B while A is in-flight");
        assert!(
            err.to_string().contains("at capacity"),
            "unexpected error: {}",
            err
        );

        let completions = backend
            .poll_completions(Duration::from_secs(2))
            .expect("poll completions");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].ticket.ticket_id, ticket_a.ticket_id);
        assert_eq!(completions[0].ticket.trial_id, dispatch_a.trial_id);
        assert_eq!(completions[0].schedule_idx, dispatch_a.schedule_idx);

        let ticket_b = backend
            .submit(dispatch_b.clone())
            .expect("submit B after drain");
        let completions = backend
            .poll_completions(Duration::from_secs(2))
            .expect("poll completions");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].ticket.ticket_id, ticket_b.ticket_id);
        assert_eq!(completions[0].schedule_idx, dispatch_b.schedule_idx);
    }

    #[test]
    fn p5b_local_worker_capacity_ceiling_resolves_with_warning() {
        let (effective, warning) = resolve_local_worker_max_in_flight(8, Some(3));
        assert_eq!(effective, 3);
        assert!(
            warning
                .as_deref()
                .unwrap_or("")
                .contains("capacity ceiling applied"),
            "expected capacity warning, got: {:?}",
            warning
        );

        let (effective_noop, warning_noop) = resolve_local_worker_max_in_flight(2, Some(4));
        assert_eq!(effective_noop, 2);
        assert!(warning_noop.is_none());
    }

    #[test]
    fn p5b_submit_backpressure_classifies_capacity_as_retryable() {
        let executor: Arc<LocalTrialExecutor> = Arc::new(|dispatch| {
            thread::sleep(Duration::from_millis(80));
            Ok(TrialCompletion {
                ticket: WorkerTicket {
                    worker_id: "ignored".to_string(),
                    ticket_id: "ignored".to_string(),
                    trial_id: dispatch.trial_id.clone(),
                },
                schedule_idx: dispatch.schedule_idx,
                completion_seq: None,
                terminal_status: "succeeded".to_string(),
                classification: "ok".to_string(),
                artifacts: json!({}),
                metrics: json!({}),
                runtime_summary: json!({}),
            })
        });
        let backend =
            LocalThreadWorkerBackend::new_with_ceiling(1, executor, None).expect("backend");
        let dispatch_a = worker_dispatch_fixture(0, "trial_a");
        let dispatch_b = worker_dispatch_fixture(1, "trial_b");

        let ticket_a = submit_dispatch_with_backpressure(&backend, dispatch_a.clone())
            .expect("submit A")
            .expect("ticket A");
        let blocked = submit_dispatch_with_backpressure(&backend, dispatch_b.clone())
            .expect("submit B should be classified as backpressure");
        assert!(
            blocked.is_none(),
            "capacity backpressure should return None instead of failing run"
        );

        let drained = backend
            .poll_completions(Duration::from_secs(2))
            .expect("drain completion");
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].ticket.ticket_id, ticket_a.ticket_id);

        let ticket_b = submit_dispatch_with_backpressure(&backend, dispatch_b.clone())
            .expect("submit B after drain")
            .expect("ticket B");
        assert_eq!(ticket_b.trial_id, dispatch_b.trial_id);
    }

    #[test]
    fn p5b_local_worker_backend_drains_burst_completions_without_loss() {
        let executor: Arc<LocalTrialExecutor> = Arc::new(|dispatch| {
            Ok(TrialCompletion {
                ticket: WorkerTicket {
                    worker_id: "ignored".to_string(),
                    ticket_id: "ignored".to_string(),
                    trial_id: dispatch.trial_id.clone(),
                },
                schedule_idx: dispatch.schedule_idx,
                completion_seq: None,
                terminal_status: "succeeded".to_string(),
                classification: "ok".to_string(),
                artifacts: json!({}),
                metrics: json!({}),
                runtime_summary: json!({}),
            })
        });
        let backend =
            LocalThreadWorkerBackend::new_with_ceiling(32, executor, None).expect("backend");

        let mut expected_ticket_ids: HashSet<String> = HashSet::new();
        for idx in 0..32usize {
            let dispatch = worker_dispatch_fixture(idx, &format!("trial_{}", idx));
            let ticket = backend.submit(dispatch).expect("submit burst dispatch");
            expected_ticket_ids.insert(ticket.ticket_id);
        }

        let mut seen_ticket_ids: HashSet<String> = HashSet::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while seen_ticket_ids.len() < expected_ticket_ids.len() {
            assert!(
                Instant::now() < deadline,
                "timed out draining burst completions: seen={} expected={}",
                seen_ticket_ids.len(),
                expected_ticket_ids.len()
            );
            let completions = backend
                .poll_completions(Duration::from_millis(250))
                .expect("poll burst completions");
            if completions.is_empty() {
                continue;
            }
            for completion in completions {
                assert!(
                    expected_ticket_ids.contains(&completion.ticket.ticket_id),
                    "unexpected completion ticket {}",
                    completion.ticket.ticket_id
                );
                assert!(
                    seen_ticket_ids.insert(completion.ticket.ticket_id.clone()),
                    "duplicate completion ticket {}",
                    completion.ticket.ticket_id
                );
            }
        }

        assert_eq!(seen_ticket_ids.len(), 32);
        let trailing = backend
            .poll_completions(Duration::from_millis(1))
            .expect("trailing poll");
        assert!(
            trailing.is_empty(),
            "completion queue should be fully drained"
        );
    }

    #[test]
    fn p2c_local_thread_worker_backend_ticket_map_drives_pause_and_stop() {
        let executor: Arc<LocalTrialExecutor> = Arc::new(|dispatch| {
            thread::sleep(Duration::from_millis(120));
            Ok(TrialCompletion {
                ticket: WorkerTicket {
                    worker_id: "ignored".to_string(),
                    ticket_id: "ignored".to_string(),
                    trial_id: dispatch.trial_id.clone(),
                },
                schedule_idx: 0,
                completion_seq: None,
                terminal_status: "succeeded".to_string(),
                classification: "ok".to_string(),
                artifacts: json!({}),
                metrics: json!({}),
                runtime_summary: json!({}),
            })
        });
        let backend = LocalThreadWorkerBackend::new(2, executor).expect("backend");
        let dispatch = worker_dispatch_fixture(2, "trial_pause");
        let ticket = backend.submit(dispatch.clone()).expect("submit");

        let ack = backend
            .request_pause(&ticket.worker_id, "checkpoint_now")
            .expect("pause ack");
        assert_eq!(ack.worker_id, ticket.worker_id);
        assert_eq!(ack.trial_id, ticket.trial_id);
        assert_eq!(ack.label, "checkpoint_now");
        assert!(ack.accepted);

        backend
            .request_stop(&ticket.worker_id, "unit test stop")
            .expect("stop should accept known worker");
        let err = backend
            .request_pause("unknown.worker", "x")
            .expect_err("unknown worker should fail");
        assert!(
            err.to_string().contains("unknown active worker"),
            "unexpected error: {}",
            err
        );

        let _ = backend
            .poll_completions(Duration::from_secs(2))
            .expect("drain completion");
    }

    #[test]
    fn p2d_remote_backend_fake_harness_round_trips_protocol_contract() {
        let harness = Arc::new(FakeRemoteWorkerHarness::new());
        let backend = RemoteWorkerBackend::new(harness.clone()).expect("remote backend");
        let dispatch = worker_dispatch_fixture(7, "trial_remote_1");
        let ticket = backend.submit(dispatch.clone()).expect("submit");

        let pause = backend
            .request_pause(&ticket.worker_id, "checkpoint_1")
            .expect("pause");
        assert!(pause.accepted);
        assert_eq!(pause.worker_id, ticket.worker_id);
        assert_eq!(pause.trial_id, ticket.trial_id);

        backend
            .request_stop(&ticket.worker_id, "done")
            .expect("stop");

        harness
            .enqueue_completion(worker_completion_fixture(
                &ticket,
                dispatch.schedule_idx,
                "remote_ok",
            ))
            .expect("enqueue completion");

        let completions = backend
            .poll_completions(Duration::from_millis(250))
            .expect("poll");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].ticket.ticket_id, ticket.ticket_id);
        assert_eq!(completions[0].classification, "remote_ok");

        let submit_requests = harness.submit_requests().expect("submit requests");
        assert_eq!(submit_requests.len(), 1);
        assert_eq!(submit_requests[0].schema_version, REMOTE_SUBMIT_SCHEMA_V1);
        assert_eq!(submit_requests[0].dispatch.trial_id, dispatch.trial_id);

        let poll_requests = harness.poll_requests().expect("poll requests");
        assert_eq!(poll_requests.len(), 1);
        assert_eq!(poll_requests[0].schema_version, REMOTE_POLL_SCHEMA_V1);
        assert_eq!(poll_requests[0].timeout_ms, 250);

        let pause_requests = harness.pause_requests().expect("pause requests");
        assert_eq!(pause_requests.len(), 1);
        assert_eq!(pause_requests[0].schema_version, REMOTE_PAUSE_SCHEMA_V1);
        let stop_requests = harness.stop_requests().expect("stop requests");
        assert_eq!(stop_requests.len(), 1);
        assert_eq!(stop_requests[0].schema_version, REMOTE_STOP_SCHEMA_V1);
    }

    #[test]
    fn p2d_remote_backend_pause_and_stop_require_active_worker() {
        let harness = Arc::new(FakeRemoteWorkerHarness::new());
        let backend = RemoteWorkerBackend::new(harness).expect("remote backend");
        let pause_err = backend
            .request_pause("unknown.worker", "checkpoint")
            .expect_err("pause should reject unknown worker");
        assert!(
            pause_err.to_string().contains("unknown active worker"),
            "unexpected pause error: {}",
            pause_err
        );
        let stop_err = backend
            .request_stop("unknown.worker", "stop")
            .expect_err("stop should reject unknown worker");
        assert!(
            stop_err.to_string().contains("unknown active worker"),
            "unexpected stop error: {}",
            stop_err
        );
    }

    #[test]
    fn p2d_remote_backend_rejects_mismatched_completion_contracts() {
        let harness = Arc::new(FakeRemoteWorkerHarness::new());
        let backend = RemoteWorkerBackend::new(harness.clone()).expect("remote backend");
        let dispatch = worker_dispatch_fixture(9, "trial_remote_contract");
        let ticket = backend.submit(dispatch.clone()).expect("submit");

        let mut mismatched = worker_completion_fixture(&ticket, dispatch.schedule_idx, "bad");
        mismatched.ticket.trial_id = "wrong_trial".to_string();
        harness
            .enqueue_completion(mismatched)
            .expect("enqueue bad completion");
        let err = backend
            .poll_completions(Duration::from_millis(1))
            .expect_err("mismatch should fail");
        assert!(
            err.to_string().contains(REMOTE_BACKEND_QUARANTINED_PREFIX),
            "unexpected error: {}",
            err
        );
        assert!(
            err.to_string().contains("did not match submitted trial_id"),
            "unexpected error: {}",
            err
        );

        let err = backend
            .submit(worker_dispatch_fixture(10, "trial_after_fault"))
            .expect_err("quarantined backend should reject subsequent submissions");
        assert!(
            err.to_string().contains(REMOTE_BACKEND_QUARANTINED_PREFIX),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p2d_remote_backend_rejects_mismatched_completion_worker_id() {
        let harness = Arc::new(FakeRemoteWorkerHarness::new());
        let backend = RemoteWorkerBackend::new(harness.clone()).expect("remote backend");
        let dispatch = worker_dispatch_fixture(13, "trial_remote_worker_mismatch");
        let ticket = backend.submit(dispatch.clone()).expect("submit");

        let mut mismatched = worker_completion_fixture(&ticket, dispatch.schedule_idx, "bad");
        mismatched.ticket.worker_id = "wrong.worker".to_string();
        harness
            .enqueue_completion(mismatched)
            .expect("enqueue bad completion");
        let err = backend
            .poll_completions(Duration::from_millis(1))
            .expect_err("mismatch should fail");
        assert!(
            err.to_string().contains(REMOTE_BACKEND_QUARANTINED_PREFIX),
            "unexpected error: {}",
            err
        );
        assert!(
            err.to_string()
                .contains("did not match submitted worker_id"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p2d_remote_backend_dedupes_duplicate_delivery_by_completion_seq() {
        let harness = Arc::new(FakeRemoteWorkerHarness::new());
        let backend = RemoteWorkerBackend::new(harness.clone()).expect("remote backend");
        let dispatch = worker_dispatch_fixture(11, "trial_remote_dupe");
        let ticket = backend.submit(dispatch.clone()).expect("submit");

        harness
            .enqueue_completion(worker_completion_fixture_with_seq(
                &ticket,
                dispatch.schedule_idx,
                "remote_ok",
                7,
            ))
            .expect("enqueue completion A");
        harness
            .enqueue_completion(worker_completion_fixture_with_seq(
                &ticket,
                dispatch.schedule_idx,
                "remote_ok",
                7,
            ))
            .expect("enqueue duplicate completion A");

        let completions = backend
            .poll_completions(Duration::from_millis(1))
            .expect("poll completions");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].ticket.ticket_id, ticket.ticket_id);
        assert_eq!(completions[0].completion_seq, Some(7));

        let trailing = backend
            .poll_completions(Duration::from_millis(1))
            .expect("trailing poll");
        assert!(
            trailing.is_empty(),
            "duplicate completion should be dropped instead of redelivered"
        );
    }

    #[test]
    fn p2d_remote_backend_retries_retryable_submit_errors() {
        #[derive(Clone)]
        struct RetryableSubmitProtocol {
            submit_attempts: Arc<AtomicUsize>,
        }

        impl RetryableSubmitProtocol {
            fn new() -> Self {
                Self {
                    submit_attempts: Arc::new(AtomicUsize::new(0)),
                }
            }
        }

        impl RemoteWorkerProtocol for RetryableSubmitProtocol {
            fn submit(&self, request: RemoteSubmitRequest) -> Result<RemoteSubmitResponse> {
                let attempt = self.submit_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    return Err(anyhow!(
                        "remote worker http POST http://127.0.0.1:7777/v1/worker/submit failed: 503 service unavailable"
                    ));
                }
                Ok(RemoteSubmitResponse {
                    schema_version: REMOTE_SUBMIT_SCHEMA_V1.to_string(),
                    ticket: WorkerTicket {
                        worker_id: "retry.worker.1".to_string(),
                        ticket_id: "retry.ticket.1".to_string(),
                        trial_id: request.dispatch.trial_id,
                    },
                })
            }

            fn poll(&self, _request: RemotePollRequest) -> Result<RemotePollResponse> {
                Ok(RemotePollResponse {
                    schema_version: REMOTE_POLL_SCHEMA_V1.to_string(),
                    completions: Vec::new(),
                })
            }

            fn pause(&self, request: RemotePauseRequest) -> Result<RemotePauseResponse> {
                Ok(RemotePauseResponse {
                    schema_version: REMOTE_PAUSE_SCHEMA_V1.to_string(),
                    ack: WorkerPauseAck {
                        worker_id: request.worker_id,
                        trial_id: "trial_x".to_string(),
                        label: request.label,
                        accepted: true,
                    },
                })
            }

            fn stop(&self, _request: RemoteStopRequest) -> Result<RemoteStopResponse> {
                Ok(RemoteStopResponse {
                    schema_version: REMOTE_STOP_SCHEMA_V1.to_string(),
                    accepted: true,
                })
            }
        }

        let protocol = RetryableSubmitProtocol::new();
        let attempts = protocol.submit_attempts.clone();
        let backend = RemoteWorkerBackend::new(Arc::new(protocol)).expect("remote backend");
        let dispatch = worker_dispatch_fixture(12, "trial_remote_retry");
        let ticket = backend
            .submit(dispatch.clone())
            .expect("submit should retry");
        assert_eq!(ticket.ticket_id, "retry.ticket.1");
        assert_eq!(ticket.trial_id, dispatch.trial_id);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn p2d_remote_backend_retries_typed_retryable_submit_errors() {
        #[derive(Clone)]
        struct TypedRetryableSubmitProtocol {
            submit_attempts: Arc<AtomicUsize>,
        }

        impl TypedRetryableSubmitProtocol {
            fn new() -> Self {
                Self {
                    submit_attempts: Arc::new(AtomicUsize::new(0)),
                }
            }
        }

        impl RemoteWorkerProtocol for TypedRetryableSubmitProtocol {
            fn submit(&self, request: RemoteSubmitRequest) -> Result<RemoteSubmitResponse> {
                let attempt = self.submit_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 2 {
                    return Err(anyhow!(RemoteProtocolError::retryable(
                        "synthetic retryable transport fault"
                    )));
                }
                Ok(RemoteSubmitResponse {
                    schema_version: REMOTE_SUBMIT_SCHEMA_V1.to_string(),
                    ticket: WorkerTicket {
                        worker_id: "typed.retry.worker.1".to_string(),
                        ticket_id: "typed.retry.ticket.1".to_string(),
                        trial_id: request.dispatch.trial_id,
                    },
                })
            }

            fn poll(&self, _request: RemotePollRequest) -> Result<RemotePollResponse> {
                Ok(RemotePollResponse {
                    schema_version: REMOTE_POLL_SCHEMA_V1.to_string(),
                    completions: Vec::new(),
                })
            }

            fn pause(&self, request: RemotePauseRequest) -> Result<RemotePauseResponse> {
                Ok(RemotePauseResponse {
                    schema_version: REMOTE_PAUSE_SCHEMA_V1.to_string(),
                    ack: WorkerPauseAck {
                        worker_id: request.worker_id,
                        trial_id: "trial_x".to_string(),
                        label: request.label,
                        accepted: true,
                    },
                })
            }

            fn stop(&self, _request: RemoteStopRequest) -> Result<RemoteStopResponse> {
                Ok(RemoteStopResponse {
                    schema_version: REMOTE_STOP_SCHEMA_V1.to_string(),
                    accepted: true,
                })
            }
        }

        let protocol = TypedRetryableSubmitProtocol::new();
        let attempts = protocol.submit_attempts.clone();
        let backend = RemoteWorkerBackend::new(Arc::new(protocol)).expect("remote backend");
        let dispatch = worker_dispatch_fixture(14, "trial_remote_retry_typed");
        let ticket = backend.submit(dispatch).expect("submit should retry");
        assert_eq!(ticket.ticket_id, "typed.retry.ticket.1");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn p2d_remote_backend_rejects_schema_version_mismatch() {
        #[derive(Clone)]
        struct BadSchemaProtocol;
        impl RemoteWorkerProtocol for BadSchemaProtocol {
            fn submit(&self, request: RemoteSubmitRequest) -> Result<RemoteSubmitResponse> {
                Ok(RemoteSubmitResponse {
                    schema_version: "remote_worker_submit_v0".to_string(),
                    ticket: WorkerTicket {
                        worker_id: "w1".to_string(),
                        ticket_id: "t1".to_string(),
                        trial_id: request.dispatch.trial_id,
                    },
                })
            }

            fn poll(&self, _request: RemotePollRequest) -> Result<RemotePollResponse> {
                Ok(RemotePollResponse {
                    schema_version: REMOTE_POLL_SCHEMA_V1.to_string(),
                    completions: Vec::new(),
                })
            }

            fn pause(&self, request: RemotePauseRequest) -> Result<RemotePauseResponse> {
                Ok(RemotePauseResponse {
                    schema_version: REMOTE_PAUSE_SCHEMA_V1.to_string(),
                    ack: WorkerPauseAck {
                        worker_id: request.worker_id,
                        trial_id: "trial_x".to_string(),
                        label: request.label,
                        accepted: true,
                    },
                })
            }

            fn stop(&self, _request: RemoteStopRequest) -> Result<RemoteStopResponse> {
                Ok(RemoteStopResponse {
                    schema_version: REMOTE_STOP_SCHEMA_V1.to_string(),
                    accepted: true,
                })
            }
        }

        let backend =
            RemoteWorkerBackend::new(Arc::new(BadSchemaProtocol)).expect("remote backend");
        let dispatch = worker_dispatch_fixture(3, "trial_schema");
        let err = backend
            .submit(dispatch)
            .expect_err("schema mismatch should fail");
        assert!(
            err.to_string().contains("submit schema_version"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p2e_out_of_order_completion_simulator_replays_fixture_ticks() {
        let fixture = load_p2e_determinism_fixture();
        let mut simulator = OutOfOrderCompletionSimulator::from_fixture(&fixture);

        let tick0 = simulator.poll_tick(0);
        assert_eq!(tick0.len(), 2);
        assert_eq!(tick0[0].schedule_idx, 2);
        assert_eq!(tick0[0].classification, "arrive_2");
        assert_eq!(tick0[1].schedule_idx, 0);
        assert_eq!(tick0[1].classification, "arrive_0");

        let tick1 = simulator.poll_tick(1);
        assert_eq!(tick1.len(), 2);
        assert_eq!(tick1[0].schedule_idx, 3);
        assert_eq!(tick1[0].classification, "arrive_3");
        assert_eq!(tick1[1].schedule_idx, 1);
        assert_eq!(tick1[1].classification, "arrive_1");

        let tick2 = simulator.poll_tick(2);
        assert!(tick2.is_empty(), "fixture should have no tick=2 arrivals");
    }

    #[test]
    fn p2e_determinism_fixture_commits_contiguously_despite_out_of_order_arrivals() {
        let fixture = load_p2e_determinism_fixture();
        let mut simulator = OutOfOrderCompletionSimulator::from_fixture(&fixture);
        let max_tick = simulator.max_tick();

        let mut pending: BTreeMap<usize, TrialCompletion> = BTreeMap::new();
        let mut next_commit_idx = 0usize;
        let mut committed_schedule_idx = Vec::new();
        for tick in 0..=max_tick {
            for completion in simulator.poll_tick(tick) {
                pending.insert(completion.schedule_idx, completion);
            }
            let ready =
                drain_ready_completions_in_schedule_order(&mut pending, &mut next_commit_idx);
            for completion in ready {
                committed_schedule_idx.push(completion.schedule_idx);
            }
        }
        let trailing =
            drain_ready_completions_in_schedule_order(&mut pending, &mut next_commit_idx);
        for completion in trailing {
            committed_schedule_idx.push(completion.schedule_idx);
        }

        assert_eq!(
            committed_schedule_idx, fixture.expected_commit_schedule_idx,
            "commits must be deterministic and contiguous by schedule_idx"
        );
        assert!(
            pending.is_empty(),
            "pending completion buffer should fully drain by final commit"
        );
    }

    fn write_run_control_v2_multi_active_fixture(run_dir: &Path, status: &str, trials: &[&str]) {
        let mut active_trials = serde_json::Map::new();
        for (idx, trial_id) in trials.iter().enumerate() {
            active_trials.insert(
                (*trial_id).to_string(),
                json!({
                    "trial_id": trial_id,
                    "worker_id": format!("worker_{}", idx),
                    "schedule_idx": idx,
                    "variant_id": "base",
                    "started_at": "2026-02-22T00:00:00Z",
                    "control": {
                        "id": BUILTIN_COMMAND_ADAPTER_ID,
                        "version": BUILTIN_COMMAND_ADAPTER_VERSION,
                        "command_path": format!("/tmp/{}.control.json", trial_id),
                        "events_path": format!("/tmp/{}.events.jsonl", trial_id)
                    }
                }),
            );
        }
        let payload = json!({
            "schema_version": "run_control_v2",
            "run_id": "run_1",
            "status": status,
            "active_trials": active_trials,
            "updated_at": "2026-02-22T00:00:00Z"
        });
        atomic_write_json_pretty(&run_control_path(run_dir), &payload)
            .expect("run control fixture");
    }

    #[test]
    fn p2e_pause_scaffolding_marks_interrupted_when_multi_flight_pause_fails() {
        let (_root, run_dir) = create_run_dir("agentlab_p2e_pause_scaffold", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        write_run_control_v2_multi_active_fixture(&run_dir, "running", &["trial_a", "trial_b"]);

        let err = match pause_run(&run_dir, None, Some("checkpoint"), 1) {
            Ok(_) => {
                panic!("pause fan-out should fail when fixture trial dirs/controls are absent")
            }
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("pause_partial_failure"),
            "unexpected error: {}",
            err
        );
        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "interrupted"
        );
    }

    #[test]
    fn p2e_resume_scaffolding_requires_trial_id_when_multi_flight_is_active() {
        let (_root, run_dir) = create_run_dir("agentlab_p2e_resume_scaffold", "run_1");
        write_run_control_v2_multi_active_fixture(&run_dir, "paused", &["trial_a", "trial_b"]);

        let err = match resume_trial(&run_dir, None, None, &BTreeMap::new(), false) {
            Ok(_) => {
                panic!("resume without trial_id should fail when multiple active trials exist")
            }
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("resume_multiple_active_trials"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p3a_deterministic_committer_buffers_out_of_order_and_dedupes_commits() {
        let (_root, run_dir) = create_run_dir("agentlab_p3a_committer", "run_1");
        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 3,
            next_schedule_index: 0,
            next_trial_index: 2,
            schedule: vec![
                TrialSlot {
                    variant_idx: 0,
                    task_idx: 0,
                    repl_idx: 0,
                },
                TrialSlot {
                    variant_idx: 0,
                    task_idx: 1,
                    repl_idx: 0,
                },
                TrialSlot {
                    variant_idx: 0,
                    task_idx: 2,
                    repl_idx: 0,
                },
            ],
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            use_container: false,
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut run_sink = JsonlRunSink::new(&run_dir).expect("sink");
        let mut committer = DeterministicCommitter::from_progress(&schedule_progress);
        let policy_config = PolicyConfig::default();
        let evidence_records_path = run_dir.join("runtime").join("p3a_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p3a_chain_state.jsonl");
        let benchmark_predictions_path = run_dir.join("runtime").join("p3a_predictions.jsonl");
        let benchmark_scores_path = run_dir.join("runtime").join("p3a_scores.jsonl");
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();

        let inserted = committer
            .enqueue_trial(
                1,
                TrialExecutionResult::minimal("trial_2".to_string(), "completed", Some(0)),
            )
            .expect("enqueue idx=1");
        assert!(inserted, "first enqueue should be accepted");
        assert_eq!(
            committer
                .drain_ready(
                    &run_dir,
                    &policy_config,
                    &evidence_records_path,
                    &chain_state_path,
                    &benchmark_predictions_path,
                    &benchmark_scores_path,
                    &mut schedule_progress,
                    2,
                    &mut pruned_variants,
                    &mut consecutive_failures,
                    &mut run_sink
                )
                .expect("drain"),
            0,
            "idx=1 cannot commit until idx=0 arrives"
        );

        committer
            .enqueue_trial(
                0,
                TrialExecutionResult::minimal("trial_1".to_string(), "completed", Some(0)),
            )
            .expect("enqueue idx=0");
        assert_eq!(
            committer
                .drain_ready(
                    &run_dir,
                    &policy_config,
                    &evidence_records_path,
                    &chain_state_path,
                    &benchmark_predictions_path,
                    &benchmark_scores_path,
                    &mut schedule_progress,
                    2,
                    &mut pruned_variants,
                    &mut consecutive_failures,
                    &mut run_sink
                )
                .expect("drain"),
            2,
            "contiguous commit should drain idx=0 and idx=1"
        );
        assert_eq!(
            schedule_progress
                .completed_slots
                .iter()
                .map(|slot| slot.schedule_index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let duplicate_committed = committer
            .enqueue_trial(
                1,
                TrialExecutionResult::minimal("trial_2".to_string(), "completed", Some(0)),
            )
            .expect("enqueue duplicate committed");
        assert!(
            !duplicate_committed,
            "duplicate completion for committed slot must be idempotently dropped"
        );
    }

    #[test]
    fn p3b_benchmark_preflight_stages_frozen_input_and_records_task_image() {
        let root = TempDirGuard::new("agentlab_p3b_preflight");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");
        let trial_input_path = trial_dir.join("trial_input.json");
        atomic_write_json_pretty(
            &trial_input_path,
            &json!({
                "schema_version": "agent_task_v1",
                "ids": { "trial_id": "trial_1" }
            }),
        )
        .expect("trial input");

        let benchmark = BenchmarkConfig {
            policy: BenchmarkPolicyConfig::default(),
            adapter: Some(BenchmarkAdapterConfig {
                command: vec!["echo".to_string(), "ok".to_string()],
                manifest: None,
            }),
        };
        stage_benchmark_trial_preflight(
            &benchmark,
            &trial_dir,
            "run_1",
            "trial_1",
            4,
            "candidate",
            &json!({
                "id": "task_9",
                "image": "ghcr.io/acme/task:20260222",
                "grading": { "enabled": false }
            }),
            &trial_input_path,
        )
        .expect("preflight");

        let preflight =
            load_json_file(&trial_dir.join("benchmark_preflight.json")).expect("preflight json");
        assert_eq!(
            preflight
                .pointer("/task_image")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "ghcr.io/acme/task:20260222"
        );
        assert_eq!(
            preflight
                .pointer("/grading/enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            false
        );
        assert!(
            trial_dir
                .join("artifacts")
                .join("benchmark_frozen_agent_input")
                .join("trial_input.json")
                .exists(),
            "frozen trial_input must be staged for grading/replay"
        );
    }

    #[test]
    fn p3c_run_control_v2_writer_supports_multi_flight_active_trials() {
        let (_root, run_dir) = create_run_dir("agentlab_p3c_run_control", "run_1");
        let active_trials = vec![
            RunControlActiveTrial {
                trial_id: "trial_1".to_string(),
                worker_id: "worker_a".to_string(),
                schedule_idx: Some(1),
                variant_id: Some("base".to_string()),
                started_at: Some("2026-02-22T00:00:00Z".to_string()),
                control: None,
            },
            RunControlActiveTrial {
                trial_id: "trial_2".to_string(),
                worker_id: "worker_b".to_string(),
                schedule_idx: Some(2),
                variant_id: Some("candidate".to_string()),
                started_at: Some("2026-02-22T00:00:01Z".to_string()),
                control: None,
            },
        ];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None)
            .expect("write run control v2");
        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/active_trials/trial_1/schedule_idx")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            1
        );
        assert_eq!(
            run_control
                .pointer("/active_trials/trial_2/variant_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "candidate"
        );
        assert!(
            run_control.pointer("/active_trial_id").is_none(),
            "legacy active_trial_id mirror field should be removed"
        );
        assert!(
            run_control.pointer("/active_adapter").is_none(),
            "legacy active_adapter mirror field should be removed"
        );
    }

    #[test]
    fn p4_cutover_uses_parallel_engine_path_for_isolate_policy() {
        let (_root, run_dir) = create_run_dir("agentlab_p4_parallel_path", "run_1");
        write_run_control_v2(&run_dir, "run_1", "paused", &[], None).expect("run control");
        let trials_dir = run_dir.join("trials");
        let evidence_dir = run_dir.join("evidence");
        ensure_dir(&trials_dir).expect("trials dir");
        ensure_dir(&evidence_dir).expect("evidence dir");
        let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
        let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&task_chain_states_path, "").expect("chain rows");

        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 0,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: Vec::new(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            use_container: false,
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut trial_index = 0_usize;
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut run_sink = JsonlRunSink::new(&run_dir).expect("sink");
        execute_schedule_engine(
            ScheduleEngineMode::ContinueRun,
            &run_dir,
            "run_1",
            "agent_runtime",
            &run_dir,
            &run_dir.join("dataset.jsonl"),
            &[],
            &[],
            &[],
            &PolicyConfig::default(),
            &BenchmarkConfig::default(),
            &[],
            &RunBehavior::default(),
            MaterializationMode::Full,
            &TaskBoundaryPolicy::default(),
            &trials_dir,
            &evidence_dir,
            &evidence_records_path,
            &task_chain_states_path,
            &mut schedule_progress,
            &mut trial_index,
            &mut consecutive_failures,
            &mut pruned_variants,
            &[],
            "base",
            &mut run_sink,
            2,
            None,
            None,
        )
        .expect("parallel engine should no-op cleanly");

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "running"
        );
        let active_trials = run_control
            .pointer("/active_trials")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert!(
            active_trials.is_empty(),
            "parallel engine should end with no active trials"
        );
    }

    #[test]
    fn p5a_recovered_active_trials_commit_as_worker_lost_deterministically() {
        let (_root, run_dir) = create_run_dir("agentlab_p5a_worker_lost", "run_1");
        let trials_dir = run_dir.join("trials");
        let evidence_dir = run_dir.join("evidence");
        ensure_dir(&trials_dir).expect("trials dir");
        ensure_dir(&evidence_dir).expect("evidence dir");
        let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
        let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&task_chain_states_path, "").expect("chain rows");

        let variants = vec![Variant {
            id: "base".to_string(),
            bindings: json!({}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        }];
        let schedule = vec![TrialSlot {
            variant_idx: 0,
            task_idx: 0,
            repl_idx: 0,
        }];
        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 1,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: schedule.clone(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            use_container: false,
            updated_at: Utc::now().to_rfc3339(),
        };
        let recovered_active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_orphan".to_string(),
            worker_id: "worker_dead".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        let mut trial_index = 0_usize;
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut run_sink = JsonlRunSink::new(&run_dir).expect("sink");
        let policy_config = PolicyConfig {
            pruning_max_consecutive_failures: Some(1),
            ..PolicyConfig::default()
        };
        execute_schedule_engine(
            ScheduleEngineMode::ContinueRun,
            &run_dir,
            "run_1",
            "agent_runtime",
            &run_dir,
            &run_dir.join("dataset.jsonl"),
            &variants,
            &[json!({"id":"task_1"})],
            &schedule,
            &policy_config,
            &BenchmarkConfig::default(),
            &[],
            &RunBehavior::default(),
            MaterializationMode::Full,
            &TaskBoundaryPolicy::default(),
            &trials_dir,
            &evidence_dir,
            &evidence_records_path,
            &task_chain_states_path,
            &mut schedule_progress,
            &mut trial_index,
            &mut consecutive_failures,
            &mut pruned_variants,
            &recovered_active_trials,
            "base",
            &mut run_sink,
            1,
            None,
            None,
        )
        .expect("parallel recovery handling");

        assert_eq!(schedule_progress.next_schedule_index, 1);
        assert_eq!(schedule_progress.completed_slots.len(), 1);
        assert_eq!(schedule_progress.completed_slots[0].schedule_index, 0);
        assert_eq!(
            schedule_progress.completed_slots[0].trial_id,
            "trial_orphan"
        );
        assert_eq!(schedule_progress.completed_slots[0].status, "failed");
        assert_eq!(consecutive_failures.get(&0).copied().unwrap_or(0), 1);
        assert!(pruned_variants.contains(&0));
    }

    #[test]
    fn p5a_pause_run_fans_out_to_all_active_trials() {
        let (_root, run_dir) = create_run_dir("agentlab_p5a_pause_fanout", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_a_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let trial_b_dir = seed_parent_trial(&run_dir, "trial_2", json!([]), "running", None);
        let control_a = active_control_for_trial(&trial_a_dir);
        let control_b = active_control_for_trial(&trial_b_dir);

        let active_trials = vec![
            RunControlActiveTrial {
                trial_id: "trial_1".to_string(),
                worker_id: "worker_a".to_string(),
                schedule_idx: Some(1),
                variant_id: Some("base".to_string()),
                started_at: Some(Utc::now().to_rfc3339()),
                control: Some(control_a.clone()),
            },
            RunControlActiveTrial {
                trial_id: "trial_2".to_string(),
                worker_id: "worker_b".to_string(),
                schedule_idx: Some(2),
                variant_id: Some("base".to_string()),
                started_at: Some(Utc::now().to_rfc3339()),
                control: Some(control_b.clone()),
            },
        ];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None).expect("control");

        let ack_a = spawn_pause_ack_writer(
            trial_a_dir.join("state").join("lab_control.json"),
            trial_a_dir.join("state").join("events.jsonl"),
        );
        let ack_b = spawn_pause_ack_writer(
            trial_b_dir.join("state").join("lab_control.json"),
            trial_b_dir.join("state").join("events.jsonl"),
        );
        let paused = pause_run(&run_dir, None, Some("fanout_pause"), 2).expect("pause fanout");
        ack_a.join().expect("ack a");
        ack_b.join().expect("ack b");

        assert_eq!(paused.run_id, "run_1");
        assert_eq!(paused.trial_id, "multi");
        assert_eq!(paused.label, "fanout_pause");
        assert!(paused.checkpoint_acked);
        assert!(paused.stop_acked);

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused"
        );
        let active = run_control
            .pointer("/active_trials")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert_eq!(active.len(), 2);

        let trial_a_state = load_json_file(&trial_a_dir.join("trial_state.json")).expect("a state");
        assert_eq!(
            trial_a_state
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused"
        );
        let trial_b_state = load_json_file(&trial_b_dir.join("trial_state.json")).expect("b state");
        assert_eq!(
            trial_b_state
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused"
        );
    }

    #[test]
    fn p5a_pause_run_partial_fanout_sets_interrupted_and_keeps_survivor_active() {
        let (_root, run_dir) = create_run_dir("agentlab_p5a_pause_partial", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_a_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let trial_b_dir = seed_parent_trial(&run_dir, "trial_2", json!([]), "running", None);
        let control_a = active_control_for_trial(&trial_a_dir);
        let control_b = active_control_for_trial(&trial_b_dir);

        let active_trials = vec![
            RunControlActiveTrial {
                trial_id: "trial_1".to_string(),
                worker_id: "worker_a".to_string(),
                schedule_idx: Some(1),
                variant_id: Some("base".to_string()),
                started_at: Some(Utc::now().to_rfc3339()),
                control: Some(control_a.clone()),
            },
            RunControlActiveTrial {
                trial_id: "trial_2".to_string(),
                worker_id: "worker_b".to_string(),
                schedule_idx: Some(2),
                variant_id: Some("base".to_string()),
                started_at: Some(Utc::now().to_rfc3339()),
                control: Some(control_b.clone()),
            },
        ];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None).expect("control");

        let ack_a = spawn_pause_ack_writer(
            trial_a_dir.join("state").join("lab_control.json"),
            trial_a_dir.join("state").join("events.jsonl"),
        );
        let err = match pause_run(&run_dir, None, Some("fanout_pause"), 1) {
            Ok(_) => panic!("partial pause should fail"),
            Err(err) => err,
        };
        ack_a.join().expect("ack a");
        assert!(
            err.to_string().contains("pause_partial_failure"),
            "unexpected error: {}",
            err
        );

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "interrupted"
        );
        let active = run_control
            .pointer("/active_trials")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert_eq!(active.len(), 1);
        assert!(active.contains_key("trial_2"));

        let trial_a_state = load_json_file(&trial_a_dir.join("trial_state.json")).expect("a state");
        assert_eq!(
            trial_a_state
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused"
        );
        let trial_b_state = load_json_file(&trial_b_dir.join("trial_state.json")).expect("b state");
        assert_eq!(
            trial_b_state
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "running"
        );
    }

    #[test]
    fn p7_pause_run_routes_worker_control_when_active_adapter_is_absent() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_pause_worker_control", "run_1");
        let active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_parallel_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None).expect("control");

        let responder_run_dir = run_dir.clone();
        let responder = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                let Some(state) =
                    load_parallel_worker_control_state(&responder_run_dir).expect("load state")
                else {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                };
                let Some(request) = state.request else {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                };
                if request.action != ParallelWorkerControlAction::Pause {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
                write_parallel_worker_control_response(
                    &responder_run_dir,
                    ParallelWorkerControlResponse {
                        request_id: request.request_id,
                        action: ParallelWorkerControlAction::Pause,
                        status: PARALLEL_WORKER_CONTROL_RESPONSE_COMPLETED.to_string(),
                        processed_at: Utc::now().to_rfc3339(),
                        processed_trial_ids: vec!["trial_1".to_string()],
                        failed_trials: Vec::new(),
                        checkpoint_acked: Some(true),
                        stop_acked: Some(true),
                        message: None,
                    },
                )
                .expect("write response");
                return;
            }
            panic!("timed out waiting for pause control request");
        });

        let paused = pause_run(&run_dir, None, Some("worker_pause"), 2).expect("pause");
        responder.join().expect("responder");
        assert_eq!(paused.run_id, "run_1");
        assert_eq!(paused.trial_id, "trial_1");
        assert_eq!(paused.label, "worker_pause");
        assert!(paused.checkpoint_acked);
        assert!(paused.stop_acked);
    }

    #[test]
    fn p7_scheduler_processes_worker_pause_request_via_backend() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_scheduler_pause_request", "run_1");
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let harness = Arc::new(FakeRemoteWorkerHarness::new());
        let backend = RemoteWorkerBackend::new(harness.clone()).expect("backend");

        let dispatch = worker_dispatch_fixture(0, "trial_1");
        let ticket = backend.submit(dispatch.clone()).expect("submit");
        let mut in_flight: HashMap<String, InFlightDispatch> = HashMap::new();
        in_flight.insert(
            ticket.ticket_id.clone(),
            InFlightDispatch {
                schedule_idx: dispatch.schedule_idx,
                trial_id: dispatch.trial_id.clone(),
                variant_idx: dispatch.slot.variant_idx,
                variant_id: dispatch.variant_id.clone(),
                worker_id: ticket.worker_id.clone(),
                started_at: Utc::now().to_rfc3339(),
            },
        );
        let mut in_flight_by_variant = BTreeMap::new();
        in_flight_by_variant.insert(dispatch.slot.variant_idx, 1);
        write_run_control_v2(
            &run_dir,
            "run_1",
            "running",
            &in_flight_active_trials(&in_flight),
            None,
        )
        .expect("run control");

        write_parallel_worker_control_request(
            &run_dir,
            ParallelWorkerControlRequest {
                request_id: "req_pause_1".to_string(),
                action: ParallelWorkerControlAction::Pause,
                requested_at: Utc::now().to_rfc3339(),
                target_trial_ids: vec!["trial_1".to_string()],
                label: Some("fanout_pause".to_string()),
                reason: None,
            },
        )
        .expect("request");

        let outcome = process_parallel_worker_control_request(
            &run_dir,
            "run_1",
            &backend,
            &mut in_flight,
            &mut in_flight_by_variant,
        )
        .expect("process request")
        .expect("control outcome");
        assert_eq!(outcome, ScheduleEngineOutcome::Paused);
        assert!(in_flight.is_empty());

        let pause_requests = harness.pause_requests().expect("pause requests");
        assert_eq!(pause_requests.len(), 1);
        assert_eq!(pause_requests[0].worker_id, ticket.worker_id);
        let stop_requests = harness.stop_requests().expect("stop requests");
        assert_eq!(stop_requests.len(), 1);
        assert_eq!(stop_requests[0].worker_id, ticket.worker_id);

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused"
        );
        assert_eq!(
            run_control
                .pointer("/active_trials/trial_1/trial_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "trial_1"
        );
        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(
            trial_state
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "paused"
        );
    }

    #[test]
    fn p7_scheduler_processes_worker_stop_request_via_backend() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_scheduler_stop_request", "run_1");
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let harness = Arc::new(FakeRemoteWorkerHarness::new());
        let backend = RemoteWorkerBackend::new(harness.clone()).expect("backend");

        let dispatch = worker_dispatch_fixture(0, "trial_1");
        let ticket = backend.submit(dispatch.clone()).expect("submit");
        let mut in_flight: HashMap<String, InFlightDispatch> = HashMap::new();
        in_flight.insert(
            ticket.ticket_id.clone(),
            InFlightDispatch {
                schedule_idx: dispatch.schedule_idx,
                trial_id: dispatch.trial_id.clone(),
                variant_idx: dispatch.slot.variant_idx,
                variant_id: dispatch.variant_id.clone(),
                worker_id: ticket.worker_id.clone(),
                started_at: Utc::now().to_rfc3339(),
            },
        );
        let mut in_flight_by_variant = BTreeMap::new();
        in_flight_by_variant.insert(dispatch.slot.variant_idx, 1);
        write_run_control_v2(
            &run_dir,
            "run_1",
            "running",
            &in_flight_active_trials(&in_flight),
            None,
        )
        .expect("run control");

        write_parallel_worker_control_request(
            &run_dir,
            ParallelWorkerControlRequest {
                request_id: "req_stop_1".to_string(),
                action: ParallelWorkerControlAction::Stop,
                requested_at: Utc::now().to_rfc3339(),
                target_trial_ids: vec!["trial_1".to_string()],
                label: None,
                reason: Some("killed_by_user".to_string()),
            },
        )
        .expect("request");

        let outcome = process_parallel_worker_control_request(
            &run_dir,
            "run_1",
            &backend,
            &mut in_flight,
            &mut in_flight_by_variant,
        )
        .expect("process request")
        .expect("control outcome");
        assert_eq!(outcome, ScheduleEngineOutcome::Killed);
        assert!(in_flight.is_empty());

        let stop_requests = harness.stop_requests().expect("stop requests");
        assert_eq!(stop_requests.len(), 1);
        assert_eq!(stop_requests[0].worker_id, ticket.worker_id);

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "killed"
        );
        let active = run_control
            .pointer("/active_trials")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert!(
            active.is_empty(),
            "active trials should be empty after kill"
        );
        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(
            trial_state
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "killed"
        );
    }

    fn p7_trial_result_with_trial_record(schedule_idx: usize) -> TrialExecutionResult {
        let trial_id = format!("trial_{}", schedule_idx + 1);
        let mut result = TrialExecutionResult::minimal(trial_id.clone(), "completed", Some(0));
        result.deferred_trial_records.push(TrialRecord {
            run_id: "run_1".to_string(),
            trial_id,
            baseline_id: "base".to_string(),
            workload_type: "agent_harness".to_string(),
            variant_id: "base".to_string(),
            task_index: schedule_idx,
            task_id: format!("task_{}", schedule_idx),
            repl_idx: 0,
            outcome: "success".to_string(),
            success: true,
            status_code: "0".to_string(),
            container_mode: false,
            integration_level: "cli_basic".to_string(),
            network_mode_requested: "none".to_string(),
            network_mode_effective: "none".to_string(),
            primary_metric_name: "success".to_string(),
            primary_metric_value: json!(1.0),
            metrics: json!({"success": 1.0, "status_code": "0"}),
            bindings: json!({}),
            hook_events_total: 0,
            has_hook_events: false,
        });
        result
    }

    struct FlushFailRunSink;

    impl RunSink for FlushFailRunSink {
        fn write_run_manifest(&mut self, _run: &RunManifestRecord) -> Result<()> {
            Ok(())
        }

        fn append_trial_record(&mut self, _row: &TrialRecord) -> Result<()> {
            Ok(())
        }

        fn append_metric_rows(&mut self, _rows: &[MetricRow]) -> Result<()> {
            Ok(())
        }

        fn append_event_rows(&mut self, _rows: &[EventRow]) -> Result<()> {
            Ok(())
        }

        fn append_variant_snapshot(&mut self, _rows: &[VariantSnapshotRow]) -> Result<()> {
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Err(anyhow::anyhow!("flush_failed"))
        }
    }

    #[test]
    fn p7_chain_root_workspace_is_trial_scoped() {
        let a = chain_root_workspace_dir_name("trial_1");
        let b = chain_root_workspace_dir_name("trial_2");
        let c = chain_root_workspace_dir_name("trial/3");

        assert_ne!(a, b);
        assert!(a.starts_with("chain_root_workspace_"));
        assert!(b.starts_with("chain_root_workspace_"));
        assert!(
            !c.contains('/'),
            "trial-scoped chain root workspace path should be filesystem-safe"
        );
    }

    #[test]
    fn p7_commit_trial_slot_does_not_advance_progress_when_flush_fails() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_commit_flush_fail", "run_1");
        ensure_dir(&run_dir.join("runtime")).expect("runtime dir");
        let evidence_records_path = run_dir.join("runtime").join("p7_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p7_chain_state.jsonl");
        let benchmark_predictions_path = run_dir.join("runtime").join("p7_predictions.jsonl");
        let benchmark_scores_path = run_dir.join("runtime").join("p7_scores.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&chain_state_path, "").expect("chain rows");

        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 1,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: vec![TrialSlot {
                variant_idx: 0,
                task_idx: 0,
                repl_idx: 0,
            }],
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            use_container: false,
            updated_at: "2026-02-22T00:00:00Z".to_string(),
        };
        write_schedule_progress(&run_dir, &schedule_progress).expect("progress");

        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let trial_result =
            TrialExecutionResult::minimal("trial_1".to_string(), "completed", Some(0));
        let mut sink = FlushFailRunSink;
        let err = RunCoordinator::commit_trial_slot(
            &run_dir,
            &PolicyConfig::default(),
            &evidence_records_path,
            &chain_state_path,
            &benchmark_predictions_path,
            &benchmark_scores_path,
            &mut schedule_progress,
            0,
            1,
            &mut pruned_variants,
            &mut consecutive_failures,
            &trial_result,
            &mut sink,
        )
        .expect_err("flush failure should abort slot commit");
        assert!(
            err.to_string().contains("flush_failed"),
            "unexpected error: {}",
            err
        );
        assert_eq!(schedule_progress.next_schedule_index, 0);
        assert!(
            schedule_progress.completed_slots.is_empty(),
            "slot should not be committed when sink flush fails"
        );
        assert!(pruned_variants.is_empty());
        assert!(consecutive_failures.is_empty());

        let persisted: ScheduleProgress = serde_json::from_slice(
            &fs::read(schedule_progress_path(&run_dir)).expect("read persisted progress"),
        )
        .expect("deserialize persisted progress");
        assert_eq!(persisted.next_schedule_index, 0);
        assert!(persisted.completed_slots.is_empty());
    }

    fn p7_commit_trial_rows_for_arrival_order(
        prefix: &str,
        arrival_order: &[usize],
    ) -> (Vec<String>, Vec<usize>) {
        let (_root, run_dir) = create_run_dir(prefix, "run_1");
        let slot_count = arrival_order.len();
        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: slot_count,
            next_schedule_index: 0,
            next_trial_index: slot_count,
            schedule: (0..slot_count)
                .map(|idx| TrialSlot {
                    variant_idx: 0,
                    task_idx: idx,
                    repl_idx: 0,
                })
                .collect(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            use_container: false,
            updated_at: Utc::now().to_rfc3339(),
        };
        let policy_config = PolicyConfig::default();
        let evidence_records_path = run_dir.join("runtime").join("p7_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p7_chain_state.jsonl");
        let benchmark_predictions_path = run_dir.join("runtime").join("p7_predictions.jsonl");
        let benchmark_scores_path = run_dir.join("runtime").join("p7_scores.jsonl");
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut run_sink = BufferedRunSink::default();
        let mut committer = DeterministicCommitter::from_progress(&schedule_progress);

        for schedule_idx in arrival_order {
            let inserted = committer
                .enqueue_trial(
                    *schedule_idx,
                    p7_trial_result_with_trial_record(*schedule_idx),
                )
                .expect("enqueue trial");
            assert!(inserted, "arrival order should not contain duplicates");
            let _ = committer
                .drain_ready(
                    &run_dir,
                    &policy_config,
                    &evidence_records_path,
                    &chain_state_path,
                    &benchmark_predictions_path,
                    &benchmark_scores_path,
                    &mut schedule_progress,
                    slot_count,
                    &mut pruned_variants,
                    &mut consecutive_failures,
                    &mut run_sink,
                )
                .expect("drain ready");
        }
        let _ = committer
            .drain_ready(
                &run_dir,
                &policy_config,
                &evidence_records_path,
                &chain_state_path,
                &benchmark_predictions_path,
                &benchmark_scores_path,
                &mut schedule_progress,
                slot_count,
                &mut pruned_variants,
                &mut consecutive_failures,
                &mut run_sink,
            )
            .expect("final drain");

        let committed_trial_ids = run_sink
            .trial_records
            .iter()
            .map(|row| row.trial_id.clone())
            .collect::<Vec<_>>();
        let committed_schedule_idx = schedule_progress
            .completed_slots
            .iter()
            .map(|slot| slot.schedule_index)
            .collect::<Vec<_>>();
        (committed_trial_ids, committed_schedule_idx)
    }

    #[test]
    fn p7_concurrency_cap_honors_max_in_flight_four() {
        let current_in_flight = Arc::new(AtomicUsize::new(0));
        let peak_in_flight = Arc::new(AtomicUsize::new(0));
        let current_in_flight_for_worker = current_in_flight.clone();
        let peak_in_flight_for_worker = peak_in_flight.clone();
        let executor: Arc<LocalTrialExecutor> = Arc::new(move |dispatch| {
            let running_now = current_in_flight_for_worker.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = peak_in_flight_for_worker.fetch_max(running_now, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(120));
            current_in_flight_for_worker.fetch_sub(1, Ordering::SeqCst);
            Ok(TrialCompletion {
                ticket: WorkerTicket {
                    worker_id: "ignored".to_string(),
                    ticket_id: "ignored".to_string(),
                    trial_id: dispatch.trial_id.clone(),
                },
                schedule_idx: dispatch.schedule_idx,
                completion_seq: None,
                terminal_status: "succeeded".to_string(),
                classification: "ok".to_string(),
                artifacts: json!({}),
                metrics: json!({}),
                runtime_summary: json!({}),
            })
        });
        let backend =
            LocalThreadWorkerBackend::new_with_ceiling(4, executor, Some(4)).expect("backend");
        assert_eq!(backend.effective_max_in_flight(), 4);

        let total_slots = 16usize;
        let mut next_schedule_idx = 0usize;
        let mut completed = 0usize;
        let mut in_flight_ticket_ids: HashSet<String> = HashSet::new();
        let mut peak_scheduler_in_flight = 0usize;
        let deadline = Instant::now() + Duration::from_secs(10);

        while completed < total_slots {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for p7 concurrency test to drain: completed={} total={}",
                completed,
                total_slots
            );
            while next_schedule_idx < total_slots && in_flight_ticket_ids.len() < 4 {
                let dispatch = worker_dispatch_fixture(
                    next_schedule_idx,
                    &format!("trial_{}", next_schedule_idx + 1),
                );
                let ticket = backend.submit(dispatch).expect("submit under cap");
                assert!(
                    in_flight_ticket_ids.insert(ticket.ticket_id.clone()),
                    "duplicate ticket id {}",
                    ticket.ticket_id
                );
                next_schedule_idx += 1;
            }
            peak_scheduler_in_flight = peak_scheduler_in_flight.max(in_flight_ticket_ids.len());

            let completions = backend
                .poll_completions(Duration::from_millis(250))
                .expect("poll completions");
            if completions.is_empty() {
                continue;
            }
            for completion in completions {
                assert!(
                    in_flight_ticket_ids.remove(completion.ticket.ticket_id.as_str()),
                    "completion for unknown ticket {}",
                    completion.ticket.ticket_id
                );
                completed += 1;
            }
        }

        assert_eq!(peak_scheduler_in_flight, 4);
        assert!(
            peak_in_flight.load(Ordering::SeqCst) <= 4,
            "executor observed in-flight count above cap"
        );
        assert!(
            peak_in_flight.load(Ordering::SeqCst) >= 2,
            "expected at least some parallel overlap at max_concurrency=4"
        );
    }

    #[test]
    fn p7_parallel_and_serial_equivalent_final_aggregates_ordering_normalized() {
        let serial_arrivals = [0usize, 1, 2, 3];
        let parallel_arrivals = [2usize, 0, 3, 1];

        let (serial_trial_ids, serial_commit_idx) =
            p7_commit_trial_rows_for_arrival_order("agentlab_p7_serial_parity", &serial_arrivals);
        let (parallel_trial_ids, parallel_commit_idx) = p7_commit_trial_rows_for_arrival_order(
            "agentlab_p7_parallel_parity",
            &parallel_arrivals,
        );

        assert_eq!(serial_commit_idx, vec![0, 1, 2, 3]);
        assert_eq!(parallel_commit_idx, serial_commit_idx);
        assert_eq!(
            parallel_trial_ids, serial_trial_ids,
            "ordering-normalized final aggregates should match serial-equivalent output"
        );
    }

    #[test]
    fn p7_release_gate_rejects_non_isolate_state_policy() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_release_gate", "run_1");
        write_run_control_v2(&run_dir, "run_1", "paused", &[], None).expect("run control");
        let trials_dir = run_dir.join("trials");
        let evidence_dir = run_dir.join("evidence");
        ensure_dir(&trials_dir).expect("trials dir");
        ensure_dir(&evidence_dir).expect("evidence dir");
        let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
        let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&task_chain_states_path, "").expect("chain rows");

        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 0,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: Vec::new(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            use_container: false,
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut trial_index = 0_usize;
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut run_sink = JsonlRunSink::new(&run_dir).expect("sink");
        let policy_config = PolicyConfig {
            state: StatePolicy::PersistPerTask,
            ..PolicyConfig::default()
        };
        let err = execute_schedule_engine(
            ScheduleEngineMode::ContinueRun,
            &run_dir,
            "run_1",
            "agent_runtime",
            &run_dir,
            &run_dir.join("dataset.jsonl"),
            &[],
            &[],
            &[],
            &policy_config,
            &BenchmarkConfig::default(),
            &[],
            &RunBehavior::default(),
            MaterializationMode::Full,
            &TaskBoundaryPolicy::default(),
            &trials_dir,
            &evidence_dir,
            &evidence_records_path,
            &task_chain_states_path,
            &mut schedule_progress,
            &mut trial_index,
            &mut consecutive_failures,
            &mut pruned_variants,
            &[],
            "base",
            &mut run_sink,
            4,
            None,
            None,
        )
        .expect_err("non-isolate policy should be rejected by hard cutover release gate");
        assert!(
            err.to_string().contains("supports only isolate_per_trial"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn parse_task_boundary_extracts_runtime_fields() {
        let task = json!({
            "schema_version": "task_boundary_v1",
            "task": {
                "id": "task_1",
                "prompt": "solve this"
            },
            "workspace_files": [
                { "path": "notes/input.txt", "content": "hello" }
            ],
            "mount_references": [
                {
                    "dataset_pack_ref": format!("sha256:{}", "a".repeat(64)),
                    "mount_path": format!("{}/dataset_pack", AGENTLAB_CONTRACT_WORKSPACE_DIR),
                    "read_only": true
                }
            ],
            "limits": {
                "max_steps": 8,
                "max_total_tokens": 2048,
                "max_tool_calls": 4,
                "trial_seconds": 120
            }
        });

        let parsed = parse_task_boundary_from_dataset_task(&task).expect("parse boundary");
        assert_eq!(
            parsed
                .task_payload
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "task_1"
        );
        assert_eq!(parsed.workspace_files.len(), 1);
        assert_eq!(parsed.mount_references.len(), 1);
        assert_eq!(parsed.limits.max_steps, Some(8));
        assert_eq!(parsed.limits.max_total_tokens, Some(2048));
        assert_eq!(parsed.limits.max_tool_calls, Some(4));
        assert_eq!(parsed.limits.trial_seconds, Some(120));
    }

    #[test]
    fn parse_task_boundary_v2_extracts_task_image_and_workspace() {
        let task = json!({
            "schema_version": "task_boundary_v2",
            "task": {
                "id": "task_1",
                "image": "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest",
                "workspace": "/testbed",
                "prompt": "solve this"
            },
            "workspace_files": [],
            "mount_references": [],
            "limits": {}
        });

        let parsed = parse_task_boundary_from_dataset_task(&task).expect("parse boundary");
        assert_eq!(
            parsed.task_image.as_deref(),
            Some("swebench/sweb.eval.x86_64.astropy__astropy-12907:latest")
        );
        assert_eq!(parsed.task_workspace.as_deref(), Some("/testbed"));
    }

    #[test]
    fn parse_task_boundary_rejects_unsupported_keys() {
        let task = json!({
            "schema_version": "task_boundary_v1",
            "task": { "id": "task_1" },
            "workspace_files": [],
            "mount_references": [],
            "limits": {},
            "benchmark_kind": "custom_magic"
        });
        let err = parse_task_boundary_from_dataset_task(&task).expect_err("should fail");
        assert!(
            err.to_string().contains("unsupported key"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn parse_policies_reads_task_boundary_workspace_materialization_flag() {
        let spec = json!({
            "design": {
                "policies": {
                    "task_boundary": {
                        "require_workspace_materialization": true
                    }
                }
            }
        });
        let policy = parse_policies(&spec);
        assert!(policy.task_boundary.require_workspace_materialization);
    }

    #[test]
    fn task_boundary_workspace_materialization_uses_policy_not_benchmark_identity() {
        let boundary = TaskBoundaryMaterialization {
            task_payload: json!({
                "id": "swebench_like_id",
                "swebench": { "repo": "x/y" }
            }),
            workspace_files: Vec::new(),
            mount_references: Vec::new(),
            limits: TaskBoundaryLimits::default(),
            task_image: None,
            task_workspace: None,
        };
        let not_required = TaskBoundaryPolicy {
            require_workspace_materialization: false,
        };
        validate_task_boundary_workspace_materialization(&boundary, &not_required)
            .expect("should not require materialization when policy flag is false");

        let required = TaskBoundaryPolicy {
            require_workspace_materialization: true,
        };
        let err = validate_task_boundary_workspace_materialization(&boundary, &required)
            .expect_err("policy-required materialization should fail when task boundary is empty");
        assert!(
            err.to_string()
                .contains("missing required workspace materialization"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn parse_task_boundary_from_trial_input_requires_task() {
        let input = json!({
            "schema_version": "agent_task_v1",
            "ids": { "trial_id": "trial_1" },
            "runtime": {
                "paths": {
                    "workspace": "/tmp/workspace"
                }
            }
        });

        let err =
            parse_task_boundary_from_trial_input(&input).expect_err("should require task field");
        assert!(
            err.to_string().contains("missing required /task"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn parse_task_boundary_from_trial_input_accepts_clean_task_payload() {
        let input = json!({
            "id": "task_1",
            "prompt": "hello"
        });
        let parsed = parse_task_boundary_from_trial_input(&input).expect("clean payload");
        assert_eq!(
            parsed
                .task_payload
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "task_1"
        );
    }

    #[test]
    fn parse_task_boundary_from_trial_input_preserves_task_image_and_workspace() {
        let input = json!({
            "schema_version": "agent_task_v1",
            "task": {
                "id": "task_1",
                "image": "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest",
                "workspace": "/testbed",
                "input": { "prompt": "hello" }
            }
        });
        let parsed = parse_task_boundary_from_trial_input(&input).expect("agent task input");
        assert_eq!(
            parsed.task_image.as_deref(),
            Some("swebench/sweb.eval.x86_64.astropy__astropy-12907:latest")
        );
        assert_eq!(parsed.task_workspace.as_deref(), Some("/testbed"));
    }

    #[test]
    fn materialize_workspace_files_writes_utf8_and_base64() {
        let root = TempDirGuard::new("agentlab_task_boundary_workspace_files");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        fs::write(exp_dir.join("README.md"), "fixture").expect("exp fixture");
        let dataset_src = root.path.join("tasks.jsonl");
        fs::write(&dataset_src, "{\"id\":\"task_1\"}\n").expect("dataset");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let paths = TrialPaths::new(&trial_dir, &exp_dir, &dataset_src).expect("trial paths");
        paths.prepare(true).expect("prepare");

        let files = vec![
            WorkspaceFileSpec {
                path: "notes/plain.txt".to_string(),
                content: "hello world".to_string(),
                encoding: Some("utf8".to_string()),
                executable: false,
            },
            WorkspaceFileSpec {
                path: "notes/decoded.txt".to_string(),
                content: "aGVsbG8gYmFzZTY0".to_string(),
                encoding: Some("base64".to_string()),
                executable: false,
            },
        ];

        materialize_workspace_files(&paths, &files).expect("materialize");
        assert_eq!(
            fs::read_to_string(paths.workspace.join("notes/plain.txt")).expect("plain"),
            "hello world"
        );
        assert_eq!(
            fs::read_to_string(paths.workspace.join("notes/decoded.txt")).expect("decoded"),
            "hello base64"
        );
    }

    #[test]
    fn resolve_task_mounts_requires_container_and_existing_pack() {
        let root = TempDirGuard::new("agentlab_task_boundary_mounts");
        let digest = "b".repeat(64);
        let pack_dir = root.path.join(".lab").join("dataset_packs").join("sha256");
        ensure_dir(&pack_dir).expect("pack dir");
        fs::write(pack_dir.join(&digest), "pack bytes").expect("pack file");

        let refs = vec![MountReferenceSpec {
            dataset_pack_ref: format!("sha256:{}", digest),
            mount_path: format!("{}/dataset_pack", AGENTLAB_CONTRACT_WORKSPACE_DIR),
            read_only: true,
        }];
        let resolved = resolve_task_mounts(&root.path, &refs, true).expect("resolve mounts");
        assert_eq!(resolved.len(), 1);
        assert!(
            resolved[0].host_path.ends_with(Path::new(&digest)),
            "unexpected host path: {}",
            resolved[0].host_path.display()
        );

        let err =
            resolve_task_mounts(&root.path, &refs, false).expect_err("local mode should fail");
        assert!(
            err.to_string().contains("require container"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn build_trial_input_uses_run_id_and_limits() {
        let root = TempDirGuard::new("agentlab_task_boundary_trial_input");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp");
        fs::write(exp_dir.join("harness.sh"), "#!/bin/sh\n").expect("harness");
        let dataset_src = root.path.join("tasks.jsonl");
        fs::write(&dataset_src, "{\"id\":\"task_1\"}\n").expect("dataset");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let paths = TrialPaths::new(&trial_dir, &exp_dir, &dataset_src).expect("paths");
        paths.prepare(true).expect("prepare");

        let json_value = json!({
            "design": { "sanitization_profile": "hermetic_functional" },
            "runtime": {
                "agent": {
                    "command": ["sh", "-lc", "echo ok"],
                    "image": "img"
                },
                "dependencies": { "services": [] },
                "policy": {
                    "timeout_ms": 600000,
                    "network": { "mode": "none", "allowed_hosts": [] },
                    "sandbox": { "mode": "container", "image": "img" }
                }
            }
        });
        let runtime_agent =
            resolve_agent_runtime(&json_value, &exp_dir).expect("resolve runtime agent");
        let variant = Variant {
            id: "baseline".to_string(),
            bindings: json!({ "model": "demo" }),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let task_boundary = TaskBoundaryMaterialization {
            task_payload: json!({ "id": "task_1", "prompt": "x" }),
            workspace_files: vec![WorkspaceFileSpec {
                path: "input.txt".to_string(),
                content: "hello".to_string(),
                encoding: Some("utf8".to_string()),
                executable: false,
            }],
            mount_references: vec![MountReferenceSpec {
                dataset_pack_ref: format!("sha256:{}", "c".repeat(64)),
                mount_path: format!("{}/dataset_pack", AGENTLAB_CONTRACT_WORKSPACE_DIR),
                read_only: true,
            }],
            limits: TaskBoundaryLimits {
                max_steps: Some(12),
                max_total_tokens: Some(4096),
                max_tool_calls: Some(9),
                trial_seconds: Some(90),
            },
            task_image: None,
            task_workspace: None,
        };

        let input = build_agent_task(
            &json_value,
            "run_actual_1",
            "trial_1",
            &variant,
            0,
            0,
            &task_boundary,
            &runtime_agent,
        );

        assert_eq!(
            input
                .pointer("/ids/run_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "run_actual_1"
        );
        assert_eq!(
            input
                .pointer("/policy/timeout_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            90000
        );
        assert_eq!(
            input
                .pointer("/ext/task_boundary_v1/workspace_files/0/path")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "input.txt"
        );
    }

    #[test]
    fn normalize_task_prompt_aliases_deduplicates_identical_fields() {
        let task = json!({
            "id": "swebench_astropy_astropy_12907",
            "input": { "prompt": "same prompt", "repo": "astropy/astropy" },
            "prompt": "same prompt",
            "swebench": {
                "input": { "prompt": "same prompt", "base_commit": "abc123" }
            }
        });

        let normalized = normalize_task_prompt_aliases(&task);
        assert_eq!(
            normalized
                .pointer("/input/prompt")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "same prompt"
        );
        assert!(
            normalized.pointer("/prompt").is_none(),
            "top-level duplicated prompt should be removed"
        );
        assert!(
            normalized.pointer("/swebench/input/prompt").is_none(),
            "nested duplicated prompt should be removed"
        );
        assert_eq!(
            normalized
                .pointer("/swebench/input/base_commit")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "abc123"
        );
    }

    #[test]
    fn normalize_task_prompt_aliases_preserves_distinct_prompt_fields() {
        let task = json!({
            "id": "task_1",
            "input": { "prompt": "canonical prompt" },
            "prompt": "different top-level prompt",
            "swebench": {
                "input": { "prompt": "different nested prompt" }
            }
        });

        let normalized = normalize_task_prompt_aliases(&task);
        assert_eq!(
            normalized
                .pointer("/input/prompt")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "canonical prompt"
        );
        assert_eq!(
            normalized
                .pointer("/prompt")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "different top-level prompt"
        );
        assert_eq!(
            normalized
                .pointer("/swebench/input/prompt")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "different nested prompt"
        );
    }

    // -----------------------------------------------------------------------
    // build_trial_schedule tests
    // -----------------------------------------------------------------------

    #[test]
    fn schedule_variant_sequential_orders_variant_then_task_then_repl() {
        let slots = build_trial_schedule(2, 3, 2, SchedulingPolicy::VariantSequential, 1);
        assert_eq!(slots.len(), 12); // 2 variants * 3 tasks * 2 repls

        // First 6 slots should be variant 0
        for slot in &slots[0..6] {
            assert_eq!(slot.variant_idx, 0);
        }
        // Last 6 slots should be variant 1
        for slot in &slots[6..12] {
            assert_eq!(slot.variant_idx, 1);
        }

        // Within variant 0: task 0 repl 0, task 0 repl 1, task 1 repl 0, ...
        assert_eq!(slots[0].task_idx, 0);
        assert_eq!(slots[0].repl_idx, 0);
        assert_eq!(slots[1].task_idx, 0);
        assert_eq!(slots[1].repl_idx, 1);
        assert_eq!(slots[2].task_idx, 1);
        assert_eq!(slots[2].repl_idx, 0);
    }

    #[test]
    fn schedule_paired_interleaved_orders_task_then_variant_then_repl() {
        let slots = build_trial_schedule(2, 3, 2, SchedulingPolicy::PairedInterleaved, 1);
        assert_eq!(slots.len(), 12);

        // First 4 slots should all be task 0 (2 variants * 2 repls)
        for slot in &slots[0..4] {
            assert_eq!(slot.task_idx, 0);
        }
        // Within task 0: variant 0 repl 0, variant 0 repl 1, variant 1 repl 0, variant 1 repl 1
        assert_eq!(slots[0].variant_idx, 0);
        assert_eq!(slots[0].repl_idx, 0);
        assert_eq!(slots[1].variant_idx, 0);
        assert_eq!(slots[1].repl_idx, 1);
        assert_eq!(slots[2].variant_idx, 1);
        assert_eq!(slots[2].repl_idx, 0);
        assert_eq!(slots[3].variant_idx, 1);
        assert_eq!(slots[3].repl_idx, 1);
    }

    #[test]
    fn schedule_paired_interleaved_pairs_variants_on_same_task() {
        // Key A/B test property: for each task, all variants run before moving to next task
        let slots = build_trial_schedule(3, 4, 1, SchedulingPolicy::PairedInterleaved, 1);
        assert_eq!(slots.len(), 12); // 3 variants * 4 tasks * 1 repl

        for task_idx in 0..4 {
            let task_slots: Vec<_> = slots.iter().filter(|s| s.task_idx == task_idx).collect();
            assert_eq!(task_slots.len(), 3); // one per variant
            let variant_ids: Vec<_> = task_slots.iter().map(|s| s.variant_idx).collect();
            assert_eq!(variant_ids, vec![0, 1, 2]);
        }
    }

    #[test]
    fn schedule_randomized_contains_all_slots() {
        let slots = build_trial_schedule(2, 3, 2, SchedulingPolicy::Randomized, 42);
        assert_eq!(slots.len(), 12);

        // Every (variant, task, repl) triple should appear exactly once
        let mut seen = HashSet::new();
        for slot in &slots {
            let key = (slot.variant_idx, slot.task_idx, slot.repl_idx);
            assert!(seen.insert(key), "duplicate slot: {:?}", key);
        }
        assert_eq!(seen.len(), 12);
    }

    #[test]
    fn schedule_randomized_is_deterministic_with_same_seed() {
        let a = build_trial_schedule(2, 4, 2, SchedulingPolicy::Randomized, 1337);
        let b = build_trial_schedule(2, 4, 2, SchedulingPolicy::Randomized, 1337);
        for (sa, sb) in a.iter().zip(b.iter()) {
            assert_eq!(sa.variant_idx, sb.variant_idx);
            assert_eq!(sa.task_idx, sb.task_idx);
            assert_eq!(sa.repl_idx, sb.repl_idx);
        }
    }

    #[test]
    fn schedule_randomized_different_seed_produces_different_order() {
        let a = build_trial_schedule(2, 4, 2, SchedulingPolicy::Randomized, 1);
        let b = build_trial_schedule(2, 4, 2, SchedulingPolicy::Randomized, 2);
        // With 16 slots, the probability of identical ordering is negligible
        let same = a.iter().zip(b.iter()).all(|(sa, sb)| {
            sa.variant_idx == sb.variant_idx
                && sa.task_idx == sb.task_idx
                && sa.repl_idx == sb.repl_idx
        });
        assert!(!same, "different seeds should produce different orderings");
    }

    #[test]
    fn schedule_single_variant_single_task_single_repl() {
        for policy in [
            SchedulingPolicy::VariantSequential,
            SchedulingPolicy::PairedInterleaved,
            SchedulingPolicy::Randomized,
        ] {
            let slots = build_trial_schedule(1, 1, 1, policy, 1);
            assert_eq!(slots.len(), 1);
            assert_eq!(slots[0].variant_idx, 0);
            assert_eq!(slots[0].task_idx, 0);
            assert_eq!(slots[0].repl_idx, 0);
        }
    }

    #[test]
    fn schedule_empty_when_zero_tasks() {
        let slots = build_trial_schedule(2, 0, 3, SchedulingPolicy::VariantSequential, 1);
        assert!(slots.is_empty());
    }

    // -----------------------------------------------------------------------
    // should_retry_outcome tests
    // -----------------------------------------------------------------------

    #[test]
    fn retry_with_empty_retry_on_retries_any_failure() {
        // Empty retry_on means retry on any non-success
        assert!(should_retry_outcome("error", "0", &[]));
        assert!(should_retry_outcome("success", "1", &[])); // exit nonzero
        assert!(!should_retry_outcome("success", "0", &[])); // success — no retry
    }

    #[test]
    fn retry_on_error_only_retries_error_outcome() {
        let triggers = vec!["error".to_string()];
        assert!(should_retry_outcome("error", "0", &triggers));
        assert!(should_retry_outcome("error", "1", &triggers));
        assert!(!should_retry_outcome("success", "0", &triggers));
        assert!(!should_retry_outcome("success", "1", &triggers)); // exit nonzero but not "error"
    }

    #[test]
    fn retry_on_failure_retries_nonzero_exit() {
        let triggers = vec!["failure".to_string()];
        assert!(should_retry_outcome("success", "1", &triggers));
        assert!(should_retry_outcome("error", "137", &triggers));
        assert!(!should_retry_outcome("success", "0", &triggers));
        assert!(!should_retry_outcome("error", "0", &triggers)); // error outcome but exit 0
    }

    #[test]
    fn retry_on_timeout_retries_timeout_outcome() {
        let triggers = vec!["timeout".to_string()];
        assert!(should_retry_outcome("timeout", "0", &triggers));
        assert!(should_retry_outcome("timeout", "1", &triggers));
        assert!(!should_retry_outcome("error", "0", &triggers));
        assert!(!should_retry_outcome("success", "0", &triggers));
    }

    #[test]
    fn retry_on_multiple_triggers() {
        let triggers = vec!["error".to_string(), "timeout".to_string()];
        assert!(should_retry_outcome("error", "0", &triggers));
        assert!(should_retry_outcome("timeout", "0", &triggers));
        assert!(!should_retry_outcome("success", "1", &triggers)); // failure not in triggers
    }

    #[test]
    fn benchmark_verdict_maps_to_trial_outcome() {
        assert_eq!(benchmark_verdict_to_trial_outcome("pass"), Some("success"));
        assert_eq!(benchmark_verdict_to_trial_outcome("fail"), Some("failure"));
        assert_eq!(
            benchmark_verdict_to_trial_outcome("missing"),
            Some("missing")
        );
        assert_eq!(benchmark_verdict_to_trial_outcome("error"), Some("error"));
        assert_eq!(benchmark_verdict_to_trial_outcome("unknown"), None);
    }

    // -----------------------------------------------------------------------
    // parse_policies tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_policies_defaults_when_no_policies_section() {
        let spec = json!({
            "design": {
                "replications": 1,
                "random_seed": 1
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.scheduling, SchedulingPolicy::VariantSequential);
        assert_eq!(config.state, StatePolicy::IsolatePerTrial);
        assert_eq!(config.retry_max_attempts, 1);
        assert!(config.retry_on.is_empty());
        assert!(config.pruning_max_consecutive_failures.is_none());
        assert_eq!(config.concurrency.max_in_flight_per_variant, None);
        assert!(config.concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_reads_all_fields() {
        let spec = json!({
            "design": {
                "policies": {
                    "scheduling": "paired_interleaved",
                    "state": "persist_per_task",
                    "retry": {
                        "max_attempts": 3,
                        "retry_on": ["error", "timeout"]
                    },
                    "pruning": {
                        "max_consecutive_failures": 5
                    },
                    "concurrency": {
                        "max_in_flight_per_variant": 2,
                        "require_chain_lease": false
                    }
                }
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.scheduling, SchedulingPolicy::PairedInterleaved);
        assert_eq!(config.state, StatePolicy::PersistPerTask);
        assert_eq!(config.retry_max_attempts, 3);
        assert_eq!(config.retry_on, vec!["error", "timeout"]);
        assert_eq!(config.pruning_max_consecutive_failures, Some(5));
        assert_eq!(config.concurrency.max_in_flight_per_variant, Some(2));
        assert!(!config.concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_handles_randomized_scheduling() {
        let spec = json!({
            "design": {
                "policies": {
                    "scheduling": "randomized",
                    "state": "accumulate",
                    "retry": { "max_attempts": 1 }
                }
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.scheduling, SchedulingPolicy::Randomized);
        assert_eq!(config.state, StatePolicy::Accumulate);
    }

    #[test]
    fn parse_policies_unknown_scheduling_defaults_to_variant_sequential() {
        let spec = json!({
            "design": {
                "policies": {
                    "scheduling": "unknown_value",
                    "state": "unknown_state",
                    "retry": { "max_attempts": 1 }
                }
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.scheduling, SchedulingPolicy::VariantSequential);
        assert_eq!(config.state, StatePolicy::IsolatePerTrial);
        assert!(config.concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_missing_retry_defaults_to_one_attempt() {
        let spec = json!({
            "design": {
                "policies": {
                    "scheduling": "variant_sequential",
                    "state": "isolate_per_trial"
                }
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.retry_max_attempts, 1);
        assert!(config.retry_on.is_empty());
        assert!(config.concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_reads_concurrency_fields() {
        let spec = json!({
            "design": {
                "policies": {
                    "concurrency": {
                        "max_in_flight_per_variant": 4,
                        "require_chain_lease": true
                    }
                }
            }
        });

        let config = parse_policies(&spec);
        assert_eq!(config.concurrency.max_in_flight_per_variant, Some(4));
        assert!(config.concurrency.require_chain_lease);
    }
}
