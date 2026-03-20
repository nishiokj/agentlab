use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::persistence::rows::{EventRow, MetricRow, TrialRecord, VariantSnapshotRow};

// ---------------------------------------------------------------------------
// Constants from runner_part1_core.rs
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_CONTAINER_RESULT_PATH: &str = lab_core::AGENTLAB_RESULT_PATH;
pub(crate) const DEFAULT_CONTAINER_TRAJECTORY_PATH: &str = lab_core::AGENTLAB_TRAJECTORY_PATH;
pub(crate) const DEFAULT_CONTAINER_TRIAL_INPUT_PATH: &str = lab_core::AGENTLAB_TRIAL_INPUT_PATH;
pub(crate) const DEFAULT_CONTAINER_GRADER_INPUT_PATH: &str = lab_core::AGENTLAB_GRADER_INPUT_PATH;
pub(crate) const DEFAULT_CONTAINER_RAW_GRADER_OUTPUT_PATH: &str =
    lab_core::AGENTLAB_RAW_GRADER_OUTPUT_PATH;
pub(crate) const DEFAULT_CONTAINER_MAPPED_GRADER_OUTPUT_PATH: &str =
    lab_core::AGENTLAB_MAPPED_GRADER_OUTPUT_PATH;
pub(crate) const DEFAULT_CONTAINER_CONTROL_PATH: &str = "/agentlab/in/runtime/lab_control.json";
pub(crate) const AGENTLAB_ENV_TASK_IMAGE: &str = "AGENTLAB_TASK_IMAGE";
pub(crate) const AGENTLAB_ENV_AGENT_EXIT_STATUS: &str = "AGENTLAB_AGENT_EXIT_STATUS";
pub(crate) const AGENTLAB_ENV_PREFLIGHT_SMOKE: &str = "AGENTLAB_PREFLIGHT_SMOKE";
pub(crate) const BENCHMARK_GRADE_ERROR_FILENAME: &str = "benchmark_grade_error.txt";
pub(crate) const RAW_GRADER_OUTPUT_FILENAME: &str = "raw_grader_output.json";
pub(crate) const MAPPED_GRADER_OUTPUT_FILENAME: &str = "mapped_grader_output.json";
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
pub(crate) const RUNTIME_DISK_HEADROOM_CHECK_INTERVAL_SECONDS: u64 = 1;
pub(crate) const RUNTIME_RUN_SIZE_CHECK_INTERVAL_SECONDS: u64 = 5;
pub(crate) const RUN_DIR_CREATE_MAX_ATTEMPTS: usize = 64;
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
pub(crate) const RUNTIME_KEY_SCHEDULE_PROGRESS: &str = "schedule_progress_v2";
pub(crate) const RUNTIME_KEY_ENGINE_LEASE: &str = "engine_lease_v1";

pub(crate) const RUN_CONTROL_UNKNOWN_WORKER_ID: &str = "worker.unknown";
pub(crate) const PACKAGED_RUNTIME_ASSETS_DIR: &str = "runtime_assets";
pub(crate) const STAGING_MANIFEST_FILE: &str = "staging_manifest.json";
pub(crate) const STAGING_MANIFEST_SCHEMA_VERSION: &str = "runtime_path_staging_manifest_v1";

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

#[cfg(test)]
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

#[cfg(test)]
pub(crate) fn default_active_adapter_id() -> String {
    BUILTIN_COMMAND_ADAPTER_ID.to_string()
}

#[cfg(test)]
pub(crate) fn default_active_adapter_version() -> String {
    BUILTIN_COMMAND_ADAPTER_VERSION.to_string()
}

#[derive(Debug)]
pub struct RunResult {
    pub run_dir: PathBuf,
    pub run_id: String,
}

#[derive(Debug)]
pub struct ReplayResult {
    pub replay_dir: PathBuf,
    pub replay_id: String,
    pub parent_trial_id: String,
    pub strict: bool,
    pub replay_grade: String,
    pub harness_status: String,
}

#[derive(Debug)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduleEngineOutcome {
    Completed,
    Paused,
    Killed,
    Interrupted,
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
    pub(crate) deferred_trial_conclusion_records: Vec<Value>,
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
            deferred_trial_conclusion_records: Vec::new(),
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

// Preflight types from runner_part3_engine.rs

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
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

pub(crate) type BenchmarkGraderConfig = GradingConfig;

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
    pub(crate) grader_input: String,
    pub(crate) result: String,
    pub(crate) raw_grader_output: String,
    pub(crate) mapped_grader_output: String,
    pub(crate) trajectory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedTaskEnvironmentManifest {
    pub(crate) schema_version: String,
    pub(crate) declaration: Value,
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
    #[serde(default)]
    pub(crate) task_sandbox_plan: Option<crate::trial::state::TaskSandboxPlan>,
}

impl PreparedTaskEnvironmentManifest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.schema_version != "prepared_task_environment_v1" {
            return Err(anyhow!(
                "invalid prepared_task_environment schema_version: {}",
                self.schema_version
            ));
        }
        for (name, value) in [
            ("declaration_digest", self.declaration_digest.as_str()),
            ("run_id", self.run_id.as_str()),
            ("trial_id", self.trial_id.as_str()),
            ("variant_id", self.variant_id.as_str()),
            ("task_id", self.task_id.as_str()),
            ("task_image", self.task_image.as_str()),
            ("workspace_root", self.workspace_root.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(anyhow!(
                    "prepared_task_environment manifest missing required field '{}'",
                    name
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn task_sandbox_image(&self) -> &str {
        self.task_sandbox_plan
            .as_ref()
            .map(|plan| plan.image.as_str())
            .unwrap_or(self.task_image.as_str())
    }

    pub(crate) fn task_sandbox_workdir(&self) -> Option<&str> {
        self.task_sandbox_plan
            .as_ref()
            .map(|plan| plan.workdir.as_str())
    }
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
// Async Docker cutover contracts
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactType {
    PatchSubmission,
    TextResponse,
    StructuredJson,
    FileRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GradingStrategy {
    InTaskImage,
    Injected,
    Separate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GraderConclusionMode {
    Direct,
    Mapper,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraderConclusionConfig {
    pub(crate) mode: GraderConclusionMode,
    #[serde(default)]
    pub(crate) mapper: Option<String>,
}

impl Default for GraderConclusionConfig {
    fn default() -> Self {
        Self {
            mode: GraderConclusionMode::Direct,
            mapper: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct InTaskImageGradingConfig {
    #[serde(default)]
    pub(crate) hidden_paths: Vec<String>,
    #[serde(default)]
    pub(crate) revealed_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InjectedGradingConfig {
    pub(crate) bundle: String,
    pub(crate) copy_dest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeparateGradingConfig {
    pub(crate) image: String,
    pub(crate) workdir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GradingConfig {
    pub(crate) strategy: GradingStrategy,
    pub(crate) command: Vec<String>,
    pub(crate) conclusion: GraderConclusionConfig,
    #[serde(default)]
    pub(crate) in_task_image: Option<InTaskImageGradingConfig>,
    #[serde(default)]
    pub(crate) injected: Option<InjectedGradingConfig>,
    #[serde(default)]
    pub(crate) separate: Option<SeparateGradingConfig>,
}

impl GradingConfig {
    pub(crate) fn in_task_image(command: Vec<String>) -> Self {
        Self {
            strategy: GradingStrategy::InTaskImage,
            command,
            conclusion: GraderConclusionConfig::default(),
            in_task_image: Some(InTaskImageGradingConfig::default()),
            injected: None,
            separate: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactEnvelopeV1 {
    pub(crate) schema_version: String,
    pub(crate) artifact_type: ArtifactType,
    pub(crate) artifact: Value,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContractIds {
    pub(crate) run_id: String,
    pub(crate) trial_id: String,
    pub(crate) variant_id: String,
    pub(crate) task_id: String,
    #[serde(default)]
    pub(crate) repl_idx: Option<u32>,
    #[serde(default)]
    pub(crate) schedule_idx: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraderInputAgentPhase {
    #[serde(default)]
    pub(crate) exit_code: Option<i32>,
    pub(crate) timed_out: bool,
    pub(crate) result_present: bool,
    pub(crate) result_schema_valid: bool,
    pub(crate) started_at: String,
    pub(crate) ended_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CandidateArtifactState {
    Missing,
    Invalid,
    Valid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum CandidateArtifactSource {
    #[serde(rename = "result.inline")]
    ResultInline,
    #[serde(rename = "result.file_ref")]
    ResultFileRef,
    #[serde(rename = "none")]
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateArtifactRecord {
    pub(crate) state: CandidateArtifactState,
    pub(crate) artifact_type: ArtifactType,
    pub(crate) source: CandidateArtifactSource,
    #[serde(default)]
    pub(crate) payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspaceDeltaState {
    Available,
    Missing,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceDeltaContract {
    pub(crate) state: WorkspaceDeltaState,
    #[serde(default)]
    pub(crate) diff_path: Option<String>,
    #[serde(default)]
    pub(crate) patch_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraderInputPaths {
    pub(crate) result_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraderInputV1 {
    pub(crate) schema_version: String,
    pub(crate) ids: ContractIds,
    pub(crate) task: Value,
    pub(crate) artifact_type: ArtifactType,
    pub(crate) agent_phase: GraderInputAgentPhase,
    pub(crate) candidate_artifact: CandidateArtifactRecord,
    pub(crate) workspace_delta: WorkspaceDeltaContract,
    pub(crate) paths: GraderInputPaths,
    pub(crate) workdir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrialConclusionPrimaryMetric {
    pub(crate) name: String,
    pub(crate) value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrialConclusionGrader {
    pub(crate) name: String,
    pub(crate) strategy: GradingStrategy,
    #[serde(default)]
    pub(crate) version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrialConclusionV1 {
    pub(crate) schema_version: String,
    pub(crate) payload: Value,
    #[serde(default)]
    pub(crate) reported_outcome: Option<String>,
    #[serde(default)]
    pub(crate) primary_metric: Option<TrialConclusionPrimaryMetric>,
    pub(crate) grader: TrialConclusionGrader,
}

// ---------------------------------------------------------------------------
// Type declarations from runner_part5_runtime_io.rs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct ResolvedMountReference {
    pub(crate) host_path: PathBuf,
    pub(crate) mount_path: String,
}

pub(crate) struct PreparedTrialIo {
    pub(crate) trial_input_host: PathBuf,
    pub(crate) grader_input_host: PathBuf,
    pub(crate) result_host: PathBuf,
    pub(crate) events_host: PathBuf,
    pub(crate) trial_input_path: String,
    pub(crate) grader_input_path: String,
    pub(crate) result_path: String,
    pub(crate) raw_grader_output_path: String,
    pub(crate) mapped_grader_output_path: String,
    pub(crate) trajectory_path: String,
    #[cfg(test)]
    pub(crate) input_host: PathBuf,
    #[cfg(test)]
    pub(crate) output_host: PathBuf,
}
