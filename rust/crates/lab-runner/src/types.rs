use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::sink::{EventRow, MetricRow, TrialRecord, VariantSnapshotRow};

// ---------------------------------------------------------------------------
// Constants from runner_part1_core.rs
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_CONTAINER_TASK_PATH: &str = lab_core::AGENTLAB_TASK_PATH;
pub(crate) const DEFAULT_CONTAINER_BINDINGS_PATH: &str = lab_core::AGENTLAB_BINDINGS_PATH;
pub(crate) const DEFAULT_CONTAINER_DEPENDENCIES_PATH: &str = lab_core::AGENTLAB_DEPENDENCIES_PATH;
pub(crate) const DEFAULT_CONTAINER_POLICY_PATH: &str = lab_core::AGENTLAB_POLICY_PATH;
pub(crate) const DEFAULT_CONTAINER_RESULT_PATH: &str = lab_core::AGENTLAB_RESULT_PATH;
pub(crate) const DEFAULT_CONTAINER_TRAJECTORY_PATH: &str = lab_core::AGENTLAB_TRAJECTORY_PATH;
pub(crate) const DEFAULT_CONTAINER_TRIAL_INPUT_PATH: &str = lab_core::AGENTLAB_TRIAL_INPUT_PATH;
pub(crate) const DEFAULT_CONTAINER_CONTROL_PATH: &str = lab_core::AGENTLAB_CONTROL_PATH;
pub(crate) const AGENTLAB_ENV_TASK_IMAGE: &str = "AGENTLAB_TASK_IMAGE";
pub(crate) const AGENTLAB_ENV_BENCHMARK_PREDICTION_PATH: &str =
    "AGENTLAB_BENCHMARK_PREDICTION_PATH";
pub(crate) const AGENTLAB_ENV_BENCHMARK_SCORE_PATH: &str = "AGENTLAB_BENCHMARK_SCORE_PATH";
pub(crate) const AGENTLAB_ENV_AGENT_EXIT_STATUS: &str = "AGENTLAB_AGENT_EXIT_STATUS";
pub(crate) const AGENTLAB_ENV_PREFLIGHT_SMOKE: &str = "AGENTLAB_PREFLIGHT_SMOKE";
pub(crate) const BENCHMARK_PREDICTION_FILENAME: &str = "benchmark_prediction.json";
pub(crate) const BENCHMARK_SCORE_FILENAME: &str = "benchmark_score.json";
pub(crate) const BENCHMARK_GRADE_ERROR_FILENAME: &str = "benchmark_grade_error.txt";
pub(crate) const AGENT_ARTIFACT_PATH_ENV_VALUE: &str =
    "PATH=/opt/agent/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
pub(crate) const AGENT_ARTIFACT_SCRIPT_SOURCE_EXTENSIONS: &[&str] =
    &[".js", ".mjs", ".cjs", ".ts", ".tsx", ".py", ".rb", ".sh"];
pub(crate) const AGENT_ARTIFACT_ENTRYPOINT_HEAD_BYTES: usize = 16 * 1024;
pub(crate) const BENCHMARK_GRADING_POLICY_EXIT_CODE: i32 = 125;
pub(crate) const AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT_ENV: &str =
    "AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT";
pub(crate) const AGENTLAB_MIN_FREE_BYTES_ENV: &str = "AGENTLAB_MIN_FREE_BYTES";
pub(crate) const AGENTLAB_MAX_RUN_BYTES_ENV: &str = "AGENTLAB_MAX_RUN_BYTES";
pub(crate) const AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES_ENV: &str =
    "AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES";
pub(crate) const AGENTLAB_PROGRESS_LOG_ENV: &str = "AGENTLAB_PROGRESS_LOG";
pub(crate) const AGENTLAB_PREFLIGHT_IMAGE_PROBE_PARALLELISM_ENV: &str =
    "AGENTLAB_PREFLIGHT_IMAGE_PROBE_PARALLELISM";
pub(crate) const DEFAULT_MIN_FREE_BYTES: u64 = 20 * 1024 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_WORKSPACE_BUNDLE_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_PREFLIGHT_IMAGE_PROBE_PARALLELISM: usize = 2;
pub(crate) const MAX_PREFLIGHT_IMAGE_PROBE_PARALLELISM: usize = 8;
pub(crate) const DEFAULT_PREFLIGHT_CONTRACT_SMOKE_TIMEOUT_MS: u64 = 10_000;
pub(crate) const LOCAL_WORKER_CAPACITY_ERROR_PREFIX: &str = "local worker backend at capacity:";
pub(crate) const LOCAL_WORKER_MAX_COMPLETIONS_PER_POLL: usize = 256;
pub(crate) const RUNTIME_DISK_HEADROOM_CHECK_INTERVAL_SECONDS: u64 = 1;
pub(crate) const RUNTIME_RUN_SIZE_CHECK_INTERVAL_SECONDS: u64 = 5;
pub(crate) const RUN_DIR_CREATE_MAX_ATTEMPTS: usize = 64;
pub(crate) const PARALLEL_WORKER_CONTROL_SCHEMA_V1: &str = "parallel_worker_control_v1";
pub(crate) const PARALLEL_WORKER_CONTROL_RESPONSE_COMPLETED: &str = "completed";
pub(crate) const PARALLEL_WORKER_CONTROL_RESPONSE_FAILED: &str = "failed";
pub(crate) const KILL_RUN_WORKER_CONTROL_TIMEOUT_SECONDS: u64 = 30;
pub(crate) const OPERATION_LEASE_TTL_SECONDS: i64 = 30;
pub(crate) const ENGINE_LEASE_HEARTBEAT_SECONDS: i64 = 2;
pub(crate) const ENGINE_LEASE_TTL_SECONDS: i64 = 6;
pub(crate) const WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES: &[&str] = &[
    "logs",
    ".haiku",
    ".graphd",
    ".watcher",
    ".agentlab_generated",
    ".claude",
    ".cockpit",
    "auth-states",
];

pub(crate) const BUILTIN_COMMAND_ADAPTER_ID: &str = "builtin.command_contract";
pub(crate) const BUILTIN_COMMAND_ADAPTER_VERSION: &str = "v1";
pub(crate) const PREBUILT_CODEX_ADAPTER_ID: &str = "prebuilt.codex_cli";
pub(crate) const PREBUILT_REX_JESUS_ADAPTER_ID: &str = "prebuilt.rex_jesus";
pub(crate) const PREBUILT_AGENT_ADAPTER_VERSION: &str = "v1";

pub(crate) const RUNTIME_KEY_RUN_CONTROL: &str = "run_control_v2";
pub(crate) const RUNTIME_KEY_RUN_SESSION_STATE: &str = "run_session_state_v1";
pub(crate) const RUNTIME_KEY_PARALLEL_WORKER_CONTROL: &str = "parallel_worker_control_v1";
pub(crate) const RUNTIME_KEY_SCHEDULE_PROGRESS: &str = "schedule_progress_v2";
pub(crate) const RUNTIME_KEY_ENGINE_LEASE: &str = "engine_lease_v1";

pub(crate) const RUN_CONTROL_UNKNOWN_WORKER_ID: &str = "worker.unknown";

// ---------------------------------------------------------------------------
// Type declarations from runner_part1_core.rs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentAdapterRef {
    pub(crate) id: String,
    pub(crate) version: String,
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
pub(crate) struct AgentAdapterCapabilities {
    pub(crate) pause: bool,
    pub(crate) control_ack: bool,
    pub(crate) event_stream: bool,
    pub(crate) strict_replay: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ActiveAdapterControl {
    #[serde(
        rename = "id",
        alias = "adapter_id",
        default = "default_active_adapter_id"
    )]
    pub(crate) adapter_id: String,
    #[serde(
        rename = "version",
        alias = "adapter_version",
        default = "default_active_adapter_version"
    )]
    pub(crate) adapter_version: String,
    pub(crate) command_path: String,
    #[serde(default)]
    pub(crate) events_path: Option<String>,
}

pub(crate) fn default_active_adapter_id() -> String {
    BUILTIN_COMMAND_ADAPTER_ID.to_string()
}

pub(crate) fn default_active_adapter_version() -> String {
    BUILTIN_COMMAND_ADAPTER_VERSION.to_string()
}

#[derive(Debug, Clone)]
pub(crate) struct AdapterPauseAck {
    pub(crate) checkpoint_acked: bool,
    pub(crate) stop_acked: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkerTicket {
    pub(crate) worker_id: String,
    pub(crate) ticket_id: String,
    pub(crate) trial_id: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TrialCompletion {
    pub(crate) ticket: WorkerTicket,
    pub(crate) schedule_idx: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) completion_seq: Option<u64>,
    pub(crate) terminal_status: String,
    pub(crate) classification: String,
    pub(crate) artifacts: Value,
    pub(crate) metrics: Value,
    pub(crate) runtime_summary: Value,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkerPauseAck {
    pub(crate) worker_id: String,
    pub(crate) trial_id: String,
    pub(crate) label: String,
    pub(crate) accepted: bool,
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

pub struct RecoverResult {
    pub run_id: String,
    pub previous_status: String,
    pub recovered_status: String,
    pub rewound_to_schedule_idx: usize,
    pub active_trials_released: usize,
    pub committed_slots_verified: usize,
    pub notes: Vec<String>,
}

pub(crate) enum ForkSelector {
    Checkpoint(String),
    Step(u64),
    EventSeq(u64),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RunOperationType {
    Continue,
    Recover,
    Pause,
    Kill,
    Resume,
    Fork,
    Replay,
}

impl RunOperationType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Recover => "recover",
            Self::Pause => "pause",
            Self::Kill => "kill",
            Self::Resume => "resume",
            Self::Fork => "fork",
            Self::Replay => "replay",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OperationLeaseRecord {
    pub(crate) schema_version: String,
    pub(crate) operation_id: String,
    pub(crate) op_type: String,
    pub(crate) owner_pid: u32,
    pub(crate) owner_host: String,
    pub(crate) acquired_at: String,
    pub(crate) expires_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) stale_takeover_of: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EngineLeaseRecord {
    pub(crate) schema_version: String,
    pub(crate) run_id: String,
    pub(crate) owner_id: String,
    pub(crate) pid: u32,
    pub(crate) hostname: String,
    pub(crate) started_at: String,
    pub(crate) heartbeat_at: String,
    pub(crate) expires_at: String,
    pub(crate) epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SlotCommitRowCounts {
    pub(crate) trials: usize,
    pub(crate) metrics: usize,
    pub(crate) events: usize,
    pub(crate) variant_snapshots: usize,
    pub(crate) evidence: usize,
    pub(crate) chain_states: usize,
    pub(crate) predictions: usize,
    pub(crate) scores: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SlotCommitRecord {
    pub(crate) schema_version: String,
    pub(crate) record_type: String,
    pub(crate) run_id: String,
    pub(crate) schedule_idx: usize,
    pub(crate) slot_commit_id: String,
    pub(crate) trial_id: String,
    pub(crate) slot_status: String,
    pub(crate) attempt: usize,
    pub(crate) recorded_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) expected_rows: Option<SlotCommitRowCounts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) payload_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) written_rows: Option<SlotCommitRowCounts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) facts_fsync_completed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) runtime_fsync_completed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PendingTrialCompletionRecord {
    pub(crate) schema_version: String,
    pub(crate) schedule_idx: usize,
    pub(crate) trial_result: TrialExecutionResult,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunBehavior {
    pub network_mode_override: Option<String>,
    pub require_network_none: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutorKind {
    LocalDocker,
}

#[cfg(test)]
impl ExecutorKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LocalDocker => "local_docker",
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
    #[cfg(test)]
    pub executor: Option<ExecutorKind>,
    pub materialize: Option<MaterializationMode>,
    #[serde(skip, default)]
    pub runtime_env: BTreeMap<String, String>,
    #[serde(skip, default)]
    pub runtime_env_files: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunSessionState {
    pub(crate) schema_version: String,
    pub(crate) run_id: String,
    pub(crate) behavior: RunBehavior,
    pub(crate) execution: RunExecutionOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduleEngineOutcome {
    Completed,
    Paused,
    Killed,
    Interrupted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParallelWorkerControlAction {
    Pause,
    Stop,
}

impl ParallelWorkerControlAction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Stop => "stop",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ParallelWorkerControlRequest {
    pub(crate) request_id: String,
    pub(crate) action: ParallelWorkerControlAction,
    pub(crate) requested_at: String,
    pub(crate) target_trial_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ParallelWorkerControlResponse {
    pub(crate) request_id: String,
    pub(crate) action: ParallelWorkerControlAction,
    pub(crate) status: String,
    pub(crate) processed_at: String,
    pub(crate) processed_trial_ids: Vec<String>,
    pub(crate) failed_trials: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) checkpoint_acked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) stop_acked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ParallelWorkerControlState {
    pub(crate) schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) request: Option<ParallelWorkerControlRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) response: Option<ParallelWorkerControlResponse>,
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RunControlActiveTrial {
    pub(crate) trial_id: String,
    pub(crate) worker_id: String,
    pub(crate) schedule_idx: Option<usize>,
    pub(crate) variant_id: Option<String>,
    pub(crate) started_at: Option<String>,
    pub(crate) control: Option<ActiveAdapterControl>,
}

#[derive(Debug, Clone)]
pub(crate) struct RunControlPauseMetadata {
    pub(crate) label: String,
    pub(crate) requested_at: String,
    pub(crate) requested_by: Option<String>,
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
    pub image: Option<String>,
    pub network_mode: String,
    pub trajectory_path: Option<String>,
    pub causal_extraction: Option<String>,
    pub scheduling: String,
    pub state_policy: String,
    pub comparison: String,
    pub retry_max_attempts: usize,
    pub preflight_warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BuildResult {
    pub package_dir: PathBuf,
    pub manifest_path: PathBuf,
    pub checksums_path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedExperimentInput {
    pub(crate) json_value: Value,
    pub(crate) exp_dir: PathBuf,
    pub(crate) project_root: PathBuf,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ExperimentOverrides {
    pub(crate) schema_version: String,
    #[serde(default)]
    pub(crate) manifest_path: Option<String>,
    #[serde(default)]
    pub(crate) values: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct KnobManifest {
    pub(crate) schema_version: String,
    pub(crate) knobs: Vec<KnobDef>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct KnobDef {
    pub(crate) id: String,
    pub(crate) json_pointer: String,
    #[serde(rename = "type")]
    pub(crate) value_type: String,
    #[serde(default)]
    pub(crate) options: Option<Vec<Value>>,
    #[serde(default)]
    pub(crate) minimum: Option<f64>,
    #[serde(default)]
    pub(crate) maximum: Option<f64>,
}

// ---------------------------------------------------------------------------
// Type declarations from runner_part3_engine.rs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduleEngineMode {
    FreshRun,
    ContinueRun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TrialExecutionResult {
    pub(crate) trial_id: String,
    pub(crate) slot_status: String,
    #[serde(default)]
    pub(crate) variant_idx: Option<usize>,
    #[serde(default)]
    pub(crate) deferred_trial_records: Vec<TrialRecord>,
    #[serde(default)]
    pub(crate) deferred_metric_rows: Vec<MetricRow>,
    #[serde(default)]
    pub(crate) deferred_event_rows: Vec<EventRow>,
    #[serde(default)]
    pub(crate) deferred_variant_snapshot_rows: Vec<VariantSnapshotRow>,
    #[serde(default)]
    pub(crate) deferred_evidence_records: Vec<Value>,
    #[serde(default)]
    pub(crate) deferred_chain_state_records: Vec<Value>,
    #[serde(default)]
    pub(crate) deferred_benchmark_prediction_records: Vec<Value>,
    #[serde(default)]
    pub(crate) deferred_benchmark_score_records: Vec<Value>,
    #[serde(default)]
    pub(crate) failure_classification: Option<String>,
}

impl TrialExecutionResult {
    pub(crate) fn minimal(trial_id: String, slot_status: &str, variant_idx: Option<usize>) -> Self {
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

    pub(crate) fn worker_lost(
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
pub(crate) struct BufferedRunSink {
    pub(crate) trial_records: Vec<TrialRecord>,
    pub(crate) metric_rows: Vec<MetricRow>,
    pub(crate) event_rows: Vec<EventRow>,
    pub(crate) variant_snapshot_rows: Vec<VariantSnapshotRow>,
}

// Preflight types from runner_part3_engine.rs

#[derive(Debug, Clone, Serialize)]
pub enum PreflightSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreflightCheck {
    pub name: &'static str,
    pub passed: bool,
    pub severity: PreflightSeverity,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreflightReport {
    pub passed: bool,
    pub checks: Vec<PreflightCheck>,
}

impl std::fmt::Display for PreflightReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for check in &self.checks {
            let icon = if check.passed {
                "PASS"
            } else {
                match check.severity {
                    PreflightSeverity::Error => "FAIL",
                    PreflightSeverity::Warning => "WARN",
                }
            };
            writeln!(f, "[{}] {}: {}", icon, check.name, check.message)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Type declarations from runner_part4_preflight_policy.rs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchedulingPolicy {
    PairedInterleaved,
    VariantSequential,
    Randomized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatePolicy {
    IsolatePerTrial,
    PersistPerTask,
    Accumulate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskModel {
    Independent,
    Dependent,
}

impl TaskModel {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::Dependent => "dependent",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BenchmarkPolicyConfig {
    pub(crate) task_model: TaskModel,
    pub(crate) scoring_lifecycle: String,
    pub(crate) evaluator_mode: String,
    pub(crate) required_evidence_classes: Vec<String>,
    pub(crate) chain_failure_policy: String,
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
pub(crate) struct BenchmarkGraderConfig {
    pub(crate) command: Vec<String>,
}

#[cfg(test)]
type BenchmarkAdapterConfig = BenchmarkGraderConfig;

#[derive(Debug, Clone, Default)]
pub(crate) struct BenchmarkConfig {
    pub(crate) policy: BenchmarkPolicyConfig,
    pub(crate) grader: Option<BenchmarkGraderConfig>,
    #[cfg(test)]
    pub(crate) adapter: Option<BenchmarkGraderConfig>,
}

#[derive(Debug, Clone)]
pub(crate) struct EffectiveTaskPolicy {
    pub(crate) state_policy: StatePolicy,
    pub(crate) task_model: TaskModel,
    pub(crate) scoring_lifecycle: String,
    pub(crate) required_evidence_classes: Vec<String>,
    pub(crate) chain_failure_policy: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ChainRuntimeState {
    pub(crate) chain_root_snapshot_ref: String,
    pub(crate) chain_root_snapshot_manifest: Value,
    pub(crate) latest_snapshot_ref: String,
    pub(crate) latest_workspace_ref: Option<String>,
    pub(crate) step_index: usize,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TaskBoundaryPolicy {
    #[cfg(test)]
    pub(crate) require_workspace_materialization: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConcurrencyPolicyConfig {
    pub(crate) max_in_flight_per_variant: Option<usize>,
    pub(crate) require_chain_lease: bool,
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
pub(crate) struct PolicyConfig {
    pub(crate) scheduling: SchedulingPolicy,
    pub(crate) state: StatePolicy,
    pub(crate) retry_max_attempts: usize,
    pub(crate) retry_on: Vec<String>,
    pub(crate) pruning_max_consecutive_failures: Option<usize>,
    pub(crate) task_boundary: TaskBoundaryPolicy,
    pub(crate) concurrency: ConcurrencyPolicyConfig,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TrialSlot {
    pub(crate) variant_idx: usize,
    pub(crate) task_idx: usize,
    pub(crate) repl_idx: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SlotCompletion {
    pub(crate) schedule_index: usize,
    pub(crate) trial_id: String,
    pub(crate) status: String, // "completed" | "failed" | "skipped_pruned"
    #[serde(default)]
    pub(crate) slot_commit_id: String,
    #[serde(default = "default_slot_attempt")]
    pub(crate) attempt: usize,
}

pub(crate) fn default_slot_attempt() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScheduleProgress {
    pub(crate) schema_version: String,
    pub(crate) run_id: String,
    pub(crate) total_slots: usize,
    pub(crate) next_schedule_index: usize,
    pub(crate) next_trial_index: usize,
    pub(crate) schedule: Vec<TrialSlot>,
    pub(crate) completed_slots: Vec<SlotCompletion>,
    pub(crate) pruned_variants: Vec<usize>,
    pub(crate) consecutive_failures: BTreeMap<usize, usize>,
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResolvedVariantsManifest {
    pub(crate) schema_version: String,
    pub(crate) generated_at: String,
    pub(crate) baseline_id: String,
    pub(crate) variants: Vec<ResolvedVariant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResolvedVariant {
    pub(crate) variant_digest: String,
    #[serde(flatten)]
    pub(crate) variant: Variant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskEnvironmentSpec {
    pub(crate) image: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspaceMode {
    Scratch,
    Patch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspaceBaseKind {
    Empty,
    DatasetPack,
    GitCheckout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceBaseSpec {
    pub(crate) kind: WorkspaceBaseKind,
    #[serde(default)]
    pub(crate) dataset_pack_ref: Option<String>,
    #[serde(default)]
    pub(crate) repo: Option<String>,
    #[serde(default)]
    pub(crate) commit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceOverlaySpec {
    pub(crate) path: String,
    pub(crate) content: String,
    #[serde(default)]
    pub(crate) encoding: Option<String>,
    #[serde(default)]
    pub(crate) executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceAuxMountSpec {
    pub(crate) dataset_pack_ref: String,
    pub(crate) mount_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceSpec {
    pub(crate) mode: WorkspaceMode,
    pub(crate) base: WorkspaceBaseSpec,
    #[serde(default)]
    pub(crate) overlays: Vec<WorkspaceOverlaySpec>,
    #[serde(default)]
    pub(crate) aux_mounts: Vec<WorkspaceAuxMountSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskDeclarationLimits {
    #[serde(default)]
    pub(crate) max_steps: Option<u64>,
    #[serde(default)]
    pub(crate) max_total_tokens: Option<u64>,
    #[serde(default)]
    pub(crate) max_tool_calls: Option<u64>,
    #[serde(default)]
    pub(crate) trial_seconds: Option<u64>,
}

pub(crate) type TaskBoundaryLimits = TaskDeclarationLimits;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskSpec {
    pub(crate) task: Value,
    pub(crate) environment: TaskEnvironmentSpec,
    pub(crate) workspace: WorkspaceSpec,
    pub(crate) dependencies: Value,
    pub(crate) limits: TaskDeclarationLimits,
}

impl TaskSpec {
    pub(crate) fn task_id(&self, task_idx: usize) -> String {
        self.task
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| format!("task_{}", task_idx))
    }

    pub(crate) fn into_task_declaration(self) -> TaskDeclaration {
        TaskDeclaration {
            schema_version: "task_declaration_v1".to_string(),
            task: self.task,
            environment: self.environment,
            workspace: self.workspace,
            dependencies: self.dependencies,
            limits: self.limits,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskDeclaration {
    pub(crate) schema_version: String,
    pub(crate) task: Value,
    pub(crate) environment: TaskEnvironmentSpec,
    pub(crate) workspace: WorkspaceSpec,
    #[serde(default)]
    pub(crate) dependencies: Value,
    #[serde(default)]
    pub(crate) limits: TaskDeclarationLimits,
}

impl TaskDeclaration {
    pub(crate) fn task_id(&self, task_idx: usize) -> String {
        self.task
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| format!("task_{}", task_idx))
    }

    pub(crate) fn task_image(&self) -> &str {
        self.environment.image.as_str()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedMountReference {
    pub(crate) host_path: String,
    pub(crate) mount_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedContractFilePaths {
    pub(crate) trial_input: String,
    pub(crate) task: String,
    pub(crate) bindings: String,
    pub(crate) dependencies: String,
    pub(crate) policy: String,
    pub(crate) result: String,
    pub(crate) trajectory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedTaskEnvironmentManifest {
    pub(crate) schema_version: String,
    pub(crate) declaration: TaskDeclaration,
    pub(crate) declaration_digest: String,
    pub(crate) run_id: String,
    pub(crate) trial_id: String,
    pub(crate) variant_id: String,
    pub(crate) task_id: String,
    pub(crate) task_index: usize,
    pub(crate) repl_idx: usize,
    pub(crate) task_image: String,
    pub(crate) workspace_root: String,
    pub(crate) aux_mounts: Vec<PreparedMountReference>,
    pub(crate) contract_files: PreparedContractFilePaths,
    pub(crate) runtime_env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResolvedScheduleManifest {
    pub(crate) schema_version: String,
    pub(crate) generated_at: String,
    pub(crate) total_slots: usize,
    pub(crate) schedule: Vec<TrialSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Variant {
    pub(crate) id: String,
    pub(crate) bindings: Value,
    pub(crate) args: Vec<String>,
    pub(crate) env: BTreeMap<String, String>,
    pub(crate) image: Option<String>,
    pub(crate) runtime_overrides: Option<Value>,
}

// ---------------------------------------------------------------------------
// Type declarations from runner_part5_runtime_io.rs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct ResolvedMountReference {
    pub(crate) host_path: PathBuf,
    pub(crate) mount_path: String,
}

pub(crate) struct ProcessRunResult {
    pub(crate) status: String,
}

pub(crate) struct PreparedTrialIo {
    pub(crate) input_host: PathBuf,
    pub(crate) output_host: PathBuf,
    pub(crate) events_host: PathBuf,
    pub(crate) task_path: String,
    pub(crate) bindings_path: String,
    pub(crate) dependencies_path: String,
    pub(crate) policy_path: String,
    pub(crate) result_path: String,
    pub(crate) trajectory_path: String,
}
