#[cfg(test)]
use crate::model::ExecutorKind;
use crate::model::{
    MaterializationMode, TrialExecutionResult, TrialSlot, RUNTIME_KEY_RUN_SESSION_STATE,
    RUNTIME_KEY_SCHEDULE_PROGRESS,
};
use crate::persistence::store::SqliteRunStore;

use anyhow::{anyhow, Result};
use chrono::Utc;
use lab_core::sha256_bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunBehavior {
    pub network_mode_override: Option<String>,
    pub require_network_none: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunExecutionOptions {
    #[cfg(test)]
    pub(crate) executor: Option<ExecutorKind>,
    pub materialize: Option<MaterializationMode>,
    #[serde(skip, default)]
    pub runtime_env: BTreeMap<String, String>,
    #[serde(skip, default)]
    pub runtime_env_files: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SlotCommitRowCounts {
    pub(crate) trials: usize,
    pub(crate) metrics: usize,
    pub(crate) events: usize,
    pub(crate) variant_snapshots: usize,
    pub(crate) evidence: usize,
    pub(crate) chain_states: usize,
    #[serde(default)]
    pub(crate) conclusions: usize,
    #[serde(default)]
    pub(crate) predictions: usize,
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SlotCompletion {
    pub(crate) schedule_index: usize,
    pub(crate) trial_id: String,
    pub(crate) status: String,
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
pub(crate) struct RunSessionState {
    pub(crate) schema_version: String,
    pub(crate) run_id: String,
    pub(crate) behavior: RunBehavior,
    pub(crate) execution: RunExecutionOptions,
}

pub(crate) fn normalize_execution_options(execution: &RunExecutionOptions) -> RunExecutionOptions {
    RunExecutionOptions {
        #[cfg(test)]
        executor: execution.executor,
        materialize: Some(execution.materialize.unwrap_or(MaterializationMode::Full)),
        runtime_env: execution.runtime_env.clone(),
        runtime_env_files: execution.runtime_env_files.clone(),
    }
}

pub(crate) fn execution_options_for_session_state(
    execution: &RunExecutionOptions,
) -> RunExecutionOptions {
    RunExecutionOptions {
        #[cfg(test)]
        executor: execution.executor,
        materialize: Some(execution.materialize.unwrap_or(MaterializationMode::Full)),
        runtime_env: BTreeMap::new(),
        runtime_env_files: Vec::new(),
    }
}

pub(crate) fn new_schedule_progress(run_id: &str, schedule: &[TrialSlot]) -> ScheduleProgress {
    ScheduleProgress {
        schema_version: "schedule_progress_v2".to_string(),
        run_id: run_id.to_string(),
        total_slots: schedule.len(),
        next_schedule_index: 0,
        next_trial_index: 0,
        schedule: schedule.to_vec(),
        completed_slots: Vec::new(),
        pruned_variants: Vec::new(),
        consecutive_failures: BTreeMap::new(),
        updated_at: Utc::now().to_rfc3339(),
    }
}

pub(crate) fn legacy_slot_commit_id(run_id: &str, slot: &SlotCompletion) -> String {
    let raw = format!(
        "legacy:{}:{}:{}:{}",
        run_id, slot.schedule_index, slot.trial_id, slot.status
    );
    let digest = sha256_bytes(raw.as_bytes());
    format!("legacy_{}", &digest[..24])
}

pub(crate) fn normalize_schedule_progress(progress: &mut ScheduleProgress) {
    progress.schema_version = "schedule_progress_v2".to_string();
    for slot in &mut progress.completed_slots {
        if slot.attempt == 0 {
            slot.attempt = 1;
        }
        if slot.slot_commit_id.trim().is_empty() {
            slot.slot_commit_id = legacy_slot_commit_id(&progress.run_id, slot);
        }
    }
}

pub(crate) fn load_schedule_progress(run_dir: &Path) -> Result<ScheduleProgress> {
    let store = SqliteRunStore::open(run_dir)?;
    let Some(value) = store.get_runtime_json(RUNTIME_KEY_SCHEDULE_PROGRESS)? else {
        return Err(anyhow!(
            "schedule_progress_v2 not found in sqlite runtime_kv for {}",
            run_dir.display()
        ));
    };
    let mut progress: ScheduleProgress = serde_json::from_value(value)?;
    if progress.schema_version != "schedule_progress_v2" {
        return Err(anyhow!(
            "unsupported schedule_progress schema_version '{}' for {}",
            progress.schema_version,
            run_dir.display()
        ));
    }
    normalize_schedule_progress(&mut progress);
    Ok(progress)
}

pub(crate) fn write_schedule_progress(run_dir: &Path, progress: &ScheduleProgress) -> Result<()> {
    let mut next = progress.clone();
    normalize_schedule_progress(&mut next);
    let value = serde_json::to_value(next)?;
    let mut store = SqliteRunStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_SCHEDULE_PROGRESS, &value)
}

pub(crate) fn write_run_session_state(
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
    let mut store = SqliteRunStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_RUN_SESSION_STATE, &payload)
}

pub(crate) fn load_run_session_state(run_dir: &Path) -> Result<RunSessionState> {
    let store = SqliteRunStore::open(run_dir)?;
    if let Some(value) = store.get_runtime_json(RUNTIME_KEY_RUN_SESSION_STATE)? {
        return Ok(serde_json::from_value(value)?);
    }
    Err(anyhow!(
        "run_session_state_v1 not found in sqlite runtime_kv — this run predates continue behavior persistence"
    ))
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
