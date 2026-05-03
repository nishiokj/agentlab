use crate::*;

use anyhow::{anyhow, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunSessionState {
    pub(crate) schema_version: String,
    pub(crate) run_id: String,
    pub(crate) behavior: RunBehavior,
    pub(crate) execution: RunExecutionOptions,
}

#[derive(Debug, Clone)]
pub(crate) struct RunControlActiveTrial {
    pub(crate) trial_id: String,
    pub(crate) worker_id: String,
    pub(crate) schedule_idx: Option<usize>,
    pub(crate) variant_id: Option<String>,
    pub(crate) started_at: Option<String>,
    #[cfg(test)]
    pub(crate) control: Option<ActiveAdapterControl>,
}

#[derive(Debug, Clone)]
pub(crate) struct RunControlPauseMetadata {
    pub(crate) label: String,
    pub(crate) requested_at: String,
    pub(crate) requested_by: Option<String>,
}

pub(crate) fn normalize_execution_options(execution: &RunExecutionOptions) -> RunExecutionOptions {
    RunExecutionOptions {
        #[cfg(test)]
        executor: execution.executor,
        materialize: Some(execution.materialize.unwrap_or(MaterializationMode::Full)),
        runtime_env: execution.runtime_env.clone(),
        runtime_env_files: execution.runtime_env_files.clone(),
        secret_files: execution.secret_files.clone(),
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
        secret_files: BTreeMap::new(),
    }
}

pub(crate) fn run_control_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("run_control.json")
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
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_RUN_SESSION_STATE, &payload)
}

pub(crate) fn load_run_session_state(run_dir: &Path) -> Result<RunSessionState> {
    let store = BackingSqliteStore::open(run_dir)?;
    if let Some(value) = store.get_runtime_json(RUNTIME_KEY_RUN_SESSION_STATE)? {
        return Ok(serde_json::from_value(value)?);
    }
    Err(anyhow!(
        "run_session_state_v1 not found in sqlite runtime_kv — this run predates continue behavior persistence"
    ))
}

pub(crate) fn run_control_active_trial_ids(run_control: &Value) -> Vec<String> {
    run_control
        .pointer("/active_trials")
        .and_then(|v| v.as_object())
        .map(|active_trials| active_trials.keys().cloned().collect())
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn run_control_active_adapter_for_trial(
    run_control: &Value,
    trial_id: &str,
) -> Option<Value> {
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

pub(crate) fn run_control_active_trials(run_control: &Value) -> Vec<RunControlActiveTrial> {
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
            active.push(RunControlActiveTrial {
                trial_id: trial_id.to_string(),
                worker_id,
                schedule_idx,
                variant_id,
                started_at,
                #[cfg(test)]
                control: None,
            });
        }
    }
    active
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
            }),
        );
    }
    payload
}

pub(crate) fn write_run_control_v2(
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

pub(crate) struct RunControlGuard {
    run_dir: PathBuf,
    run_id: String,
    done: bool,
}

impl RunControlGuard {
    pub(crate) fn new(run_dir: &Path, run_id: &str) -> Self {
        Self {
            run_dir: run_dir.to_path_buf(),
            run_id: run_id.to_string(),
            done: false,
        }
    }

    pub(crate) fn complete(&mut self, status: &str) -> Result<()> {
        write_run_control_v2(&self.run_dir, &self.run_id, status, &[], None)?;
        self.done = true;
        Ok(())
    }

    pub(crate) fn disarm(&mut self) {
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
