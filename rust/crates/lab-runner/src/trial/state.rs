use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};

use crate::config::{atomic_write_json_pretty, load_json_file};
use crate::model::{CandidateArtifactRecord, GradingStrategy};
use crate::trial::spec::TaskMaterializationSpec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TrialPhase {
    Pending,
    AgentMaterializing,
    AgentRunning,
    AgentFinished,
    GraderMaterializing,
    GraderRunning,
    GraderMapping,
    Paused,
    CommitPending,
    Committed,
    Killed,
    Abandoned,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContractFileState {
    Missing,
    PresentInvalid,
    Valid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AttemptSlotRef {
    pub(crate) schedule_idx: u32,
    pub(crate) variant_id: String,
    pub(crate) task_id: String,
    pub(crate) repl_idx: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrialAttemptKey {
    pub(crate) schedule_idx: u32,
    pub(crate) attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TelemetryPhase {
    Agent,
    Grader,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CollectMode {
    Tail,
    AfterPhase,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeclaredTelemetryMount {
    pub(crate) id: String,
    pub(crate) phase: TelemetryPhase,
    pub(crate) host_dir: String,
    pub(crate) container_dir: String,
    pub(crate) rel_path: String,
    #[serde(default)]
    pub(crate) schema: Option<String>,
    pub(crate) collect_mode: CollectMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AttemptFsLayout {
    pub(crate) attempt_dir: String,
    pub(crate) in_dir: String,
    pub(crate) out_dir: String,
    pub(crate) telemetry_mounts: Vec<DeclaredTelemetryMount>,
    pub(crate) logs_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentPhaseRecord {
    pub(crate) started_at: String,
    pub(crate) ended_at: String,
    #[serde(default)]
    pub(crate) exit_code: Option<i32>,
    #[serde(default)]
    pub(crate) signal: Option<String>,
    pub(crate) timed_out: bool,
    pub(crate) result_state: ContractFileState,
    pub(crate) stdout_path: String,
    pub(crate) stderr_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GradingPhaseRecord {
    pub(crate) started_at: String,
    pub(crate) ended_at: String,
    #[serde(default)]
    pub(crate) exit_code: Option<i32>,
    #[serde(default)]
    pub(crate) signal: Option<String>,
    pub(crate) timed_out: bool,
    pub(crate) raw_output_state: ContractFileState,
    pub(crate) stdout_path: String,
    pub(crate) stderr_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraderMappingPhaseRecord {
    pub(crate) started_at: String,
    pub(crate) ended_at: String,
    pub(crate) mapped_output_state: ContractFileState,
    pub(crate) stdout_path: String,
    pub(crate) stderr_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IoMountPlan {
    pub(crate) in_dir: String,
    pub(crate) out_dir: String,
    pub(crate) telemetry_mounts: Vec<DeclaredTelemetryMount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactMountPlan {
    pub(crate) host_artifact_path: String,
    pub(crate) container_artifact_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskSandboxPlan {
    pub(crate) image: String,
    pub(crate) workdir: String,
    pub(crate) materialization: TaskMaterializationSpec,
    pub(crate) io_mounts: IoMountPlan,
    pub(crate) artifact_mount: ArtifactMountPlan,
    pub(crate) network_mode: String,
    pub(crate) time_limit_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskSandboxState {
    pub(crate) container_id: String,
    pub(crate) image: String,
    pub(crate) workdir: String,
    pub(crate) materialization: TaskMaterializationSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GradingSandboxDetails {
    InTaskImage {
        hidden_paths: Vec<String>,
        revealed_paths: Vec<String>,
    },
    Injected {
        bundle_host_path: String,
        copy_dest: String,
    },
    Separate {
        image: String,
        workdir: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GraderOutputMode {
    DirectMapped,
    RawThenMap { mapper_ref: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GradingSandboxPlan {
    pub(crate) strategy: GradingStrategy,
    pub(crate) command: Vec<String>,
    pub(crate) io_mounts: IoMountPlan,
    pub(crate) output_mode: GraderOutputMode,
    pub(crate) details: GradingSandboxDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraderMappingPlan {
    pub(crate) mapper_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GradingSandboxState {
    pub(crate) container_id: String,
    pub(crate) strategy: GradingStrategy,
    pub(crate) workdir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrialAttemptState {
    pub(crate) key: TrialAttemptKey,
    pub(crate) slot: AttemptSlotRef,
    pub(crate) phase: TrialPhase,
    #[serde(default)]
    pub(crate) paused_from_phase: Option<TrialPhase>,
    pub(crate) fs: AttemptFsLayout,
    #[serde(default)]
    pub(crate) task_sandbox: Option<TaskSandboxState>,
    #[serde(default)]
    pub(crate) grading_sandbox: Option<GradingSandboxState>,
    #[serde(default)]
    pub(crate) agent_phase: Option<AgentPhaseRecord>,
    #[serde(default)]
    pub(crate) grading_phase: Option<GradingPhaseRecord>,
    #[serde(default)]
    pub(crate) mapping_phase: Option<GraderMappingPhaseRecord>,
    #[serde(default)]
    pub(crate) candidate_artifact: Option<CandidateArtifactRecord>,
}

pub(crate) const TRIAL_RUNTIME_STATE_FILE: &str = "trial_runtime_state.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PersistedTrialAttemptState {
    pub(crate) schema_version: String,
    pub(crate) updated_at: String,
    pub(crate) state: TrialAttemptState,
}

pub(crate) fn new_trial_attempt_state(
    trial_dir: &Path,
    schedule_idx: usize,
    attempt_no: u32,
    task_id: &str,
    variant_id: &str,
    repl_idx: usize,
    in_dir: &Path,
    out_dir: &Path,
) -> TrialAttemptState {
    TrialAttemptState {
        key: TrialAttemptKey {
            schedule_idx: schedule_idx as u32,
            attempt: attempt_no,
        },
        slot: AttemptSlotRef {
            schedule_idx: schedule_idx as u32,
            variant_id: variant_id.to_string(),
            task_id: task_id.to_string(),
            repl_idx: repl_idx as u32,
        },
        phase: TrialPhase::Pending,
        paused_from_phase: None,
        fs: AttemptFsLayout {
            attempt_dir: trial_dir.to_string_lossy().to_string(),
            in_dir: in_dir.to_string_lossy().to_string(),
            out_dir: out_dir.to_string_lossy().to_string(),
            telemetry_mounts: Vec::new(),
            logs_dir: trial_dir.join("logs").to_string_lossy().to_string(),
        },
        task_sandbox: None,
        grading_sandbox: None,
        agent_phase: None,
        grading_phase: None,
        mapping_phase: None,
        candidate_artifact: None,
    }
}

pub(crate) fn trial_runtime_state_path(trial_dir: &Path) -> PathBuf {
    trial_dir.join(TRIAL_RUNTIME_STATE_FILE)
}

fn persist_trial_attempt_state(
    trial_dir: &Path,
    state: &TrialAttemptState,
) -> Result<PersistedTrialAttemptState> {
    let payload = PersistedTrialAttemptState {
        schema_version: "trial_runtime_state_v1".to_string(),
        updated_at: Utc::now().to_rfc3339(),
        state: state.clone(),
    };
    atomic_write_json_pretty(
        trial_runtime_state_path(trial_dir).as_path(),
        &serde_json::to_value(&payload)?,
    )?;
    Ok(payload)
}

pub(crate) fn write_trial_attempt_state(trial_dir: &Path, state: &TrialAttemptState) -> Result<()> {
    let _ = persist_trial_attempt_state(trial_dir, state)?;
    Ok(())
}

pub(crate) fn load_trial_attempt_state(trial_dir: &Path) -> Result<PersistedTrialAttemptState> {
    let value = load_json_file(&trial_runtime_state_path(trial_dir))?;
    Ok(serde_json::from_value(value)?)
}

pub(crate) fn trial_attempt_state_exists(trial_dir: &Path) -> bool {
    trial_runtime_state_path(trial_dir).exists()
}

pub(crate) fn trial_attempt_container_ids(state: &TrialAttemptState) -> Vec<String> {
    let mut container_ids = Vec::new();
    if let Some(task) = state.task_sandbox.as_ref() {
        container_ids.push(task.container_id.clone());
    }
    if let Some(grading) = state.grading_sandbox.as_ref() {
        if !container_ids.iter().any(|id| id == &grading.container_id) {
            container_ids.push(grading.container_id.clone());
        }
    }
    container_ids
}

pub(crate) fn load_trial_attempt_container_ids(trial_dir: &Path) -> Result<Vec<String>> {
    if !trial_attempt_state_exists(trial_dir) {
        return Ok(Vec::new());
    }
    let persisted = load_trial_attempt_state(trial_dir)?;
    Ok(trial_attempt_container_ids(&persisted.state))
}

pub(crate) fn trial_phase_requires_recovery_release(phase: &TrialPhase) -> bool {
    !matches!(
        phase,
        TrialPhase::Paused | TrialPhase::Committed | TrialPhase::Killed | TrialPhase::Abandoned
    )
}

pub(crate) fn update_trial_attempt_state<F>(
    trial_dir: &Path,
    mutate: F,
) -> Result<Option<PersistedTrialAttemptState>>
where
    F: FnOnce(&mut TrialAttemptState),
{
    if !trial_attempt_state_exists(trial_dir) {
        return Ok(None);
    }
    let mut persisted = load_trial_attempt_state(trial_dir)?;
    mutate(&mut persisted.state);
    Ok(Some(persist_trial_attempt_state(
        trial_dir,
        &persisted.state,
    )?))
}

pub(crate) fn set_trial_attempt_phase(
    trial_dir: &Path,
    state: &mut TrialAttemptState,
    phase: TrialPhase,
) -> Result<()> {
    state.phase = phase;
    if state.phase != TrialPhase::Paused {
        state.paused_from_phase = None;
    }
    write_trial_attempt_state(trial_dir, state)
}

pub(crate) fn reconcile_trial_attempt_phase(
    trial_dir: &Path,
    phase: TrialPhase,
) -> Result<Option<PersistedTrialAttemptState>> {
    update_trial_attempt_state(trial_dir, |state| {
        state.phase = phase;
        if state.phase != TrialPhase::Paused {
            state.paused_from_phase = None;
        }
    })
}

pub(crate) fn reconcile_trial_attempt_as_abandoned(
    trial_dir: &Path,
) -> Result<Option<PersistedTrialAttemptState>> {
    update_trial_attempt_state(trial_dir, |state| {
        if !matches!(
            state.phase,
            TrialPhase::Committed | TrialPhase::Paused | TrialPhase::Killed
        ) {
            state.phase = TrialPhase::Abandoned;
            state.paused_from_phase = None;
        }
    })
}

pub(crate) fn reconcile_trial_attempt_as_paused(
    trial_dir: &Path,
) -> Result<Option<PersistedTrialAttemptState>> {
    update_trial_attempt_state(trial_dir, |state| {
        if !matches!(state.phase, TrialPhase::Committed | TrialPhase::Killed) {
            if state.phase != TrialPhase::Paused {
                state.paused_from_phase = Some(state.phase.clone());
            }
            state.phase = TrialPhase::Paused;
        }
    })
}

pub(crate) fn reconcile_trial_attempt_as_committed(
    trial_dir: &Path,
) -> Result<Option<PersistedTrialAttemptState>> {
    reconcile_trial_attempt_phase(trial_dir, TrialPhase::Committed)
}

pub(crate) fn reconcile_trial_attempt_as_killed(
    trial_dir: &Path,
) -> Result<Option<PersistedTrialAttemptState>> {
    update_trial_attempt_state(trial_dir, |state| {
        if state.phase != TrialPhase::Committed {
            state.phase = TrialPhase::Killed;
            state.paused_from_phase = None;
        }
    })
}

fn infer_resumed_phase(state: &TrialAttemptState) -> TrialPhase {
    if let Some(phase) = state.paused_from_phase.clone() {
        return phase;
    }
    if state.grading_sandbox.is_some() {
        return TrialPhase::GraderRunning;
    }
    TrialPhase::AgentRunning
}

pub(crate) fn reconcile_trial_attempt_as_resumed(
    trial_dir: &Path,
) -> Result<Option<PersistedTrialAttemptState>> {
    update_trial_attempt_state(trial_dir, |state| {
        if state.phase == TrialPhase::Paused {
            state.phase = infer_resumed_phase(state);
            state.paused_from_phase = None;
        }
    })
}

// ---------------------------------------------------------------------------
// Legacy trial_state.json (write_trial_state / TrialStateGuard)
// ---------------------------------------------------------------------------

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
