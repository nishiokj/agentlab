use crate::backend::docker::{ContainerHandle, DockerRuntime};
use crate::config::load_json_file;
use crate::trial::state::write_trial_state;
use crate::experiment::lease::{acquire_run_operation_lease, RunOperationType};
use crate::experiment::runner::{fork_trial_inner, resolve_resume_selector};
use crate::model::{
    ForkResult, RUNTIME_KEY_RUN_CONTROL, RUN_CONTROL_UNKNOWN_WORKER_ID,
};
#[cfg(test)]
use crate::model::ActiveAdapterControl;
use crate::persistence::store::SqliteRunStore;
use crate::trial::state::{
    load_trial_attempt_container_ids, load_trial_attempt_state, reconcile_trial_attempt_as_killed,
    reconcile_trial_attempt_as_paused, reconcile_trial_attempt_as_resumed,
    trial_attempt_container_ids, trial_attempt_state_exists, TrialPhase,
};
use crate::INTERRUPTED;

use anyhow::{anyhow, Result};
use chrono::Utc;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct PauseResult {
    pub run_id: String,
    pub trial_id: String,
    pub label: String,
    pub checkpoint_acked: bool,
    pub stop_acked: bool,
}

#[derive(Debug)]
pub struct KillResult {
    pub run_id: String,
    pub run_dir: std::path::PathBuf,
    pub previous_status: String,
    pub killed_trials: Vec<String>,
}

#[derive(Debug)]
pub struct ResumeResult {
    pub trial_id: String,
    pub mode: ResumeMode,
    pub selector: Option<String>,
    pub fork: Option<ForkResult>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeMode {
    RuntimeUnpause,
    Fork,
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

pub(crate) struct RunControlGuard {
    run_dir: PathBuf,
    run_id: String,
    done: bool,
}

enum ActiveTrialControlMode {
    RuntimeContainers,
}

pub(crate) fn run_control_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("run_control.json")
}

pub(crate) fn load_run_control(run_dir: &Path) -> Result<Value> {
    load_json_file(&run_control_path(run_dir))
}

pub(crate) fn run_control_status<'a>(run_control: &'a Value) -> &'a str {
    run_control
        .pointer("/status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
}

pub(crate) fn run_control_run_id(run_control: &Value) -> Option<String> {
    run_control
        .pointer("/run_id")
        .and_then(Value::as_str)
        .map(str::to_string)
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
                .map(str::to_string);
            let started_at = entry
                .pointer("/started_at")
                .and_then(|v| v.as_str())
                .map(str::to_string);
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
    let mut store = SqliteRunStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_RUN_CONTROL, &payload)
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

fn runtime_trial_container_handles(trial_dir: &Path) -> Result<Vec<ContainerHandle>> {
    Ok(load_trial_attempt_container_ids(trial_dir)?
        .into_iter()
        .map(|container_id| ContainerHandle { container_id })
        .collect())
}

fn resolve_kill_trial_control_mode(
    trial_dir: &Path,
    trial_id: &str,
) -> Result<ActiveTrialControlMode> {
    let runtime_handles = runtime_trial_container_handles(trial_dir)?;
    if !runtime_handles.is_empty() {
        return Ok(ActiveTrialControlMode::RuntimeContainers);
    }
    if trial_attempt_state_exists(trial_dir) {
        return Err(anyhow!(
            "kill_missing_runtime_container: active runtime state exists for {} but no persisted container ids were found",
            trial_id
        ));
    }
    Err(anyhow!(
        "kill_missing_runtime_container: no persisted runtime state or container ids exist for {}",
        trial_id
    ))
}

fn resolve_active_trial_control_mode(
    trial_dir: &Path,
    active: &RunControlActiveTrial,
) -> Result<ActiveTrialControlMode> {
    let runtime_handles = runtime_trial_container_handles(trial_dir)?;
    if !runtime_handles.is_empty() {
        return Ok(ActiveTrialControlMode::RuntimeContainers);
    }
    if trial_attempt_state_exists(trial_dir) {
        return Err(anyhow!(
            "pause_missing_runtime_container: active runtime state exists for {} but no persisted container ids were found",
            active.trial_id
        ));
    }
    Err(anyhow!(
        "pause_missing_runtime_container: no persisted runtime state or container ids exist for {}",
        active.trial_id
    ))
}

fn pause_trial_runtime_containers(trial_dir: &Path) -> Result<()> {
    let handles = runtime_trial_container_handles(trial_dir)?;
    if handles.is_empty() {
        return Err(anyhow!(
            "pause_missing_runtime_container: no persisted runtime containers were recorded for {}",
            trial_dir.display()
        ));
    }
    let docker = DockerRuntime::connect()?;
    for handle in &handles {
        docker.pause_container(handle)?;
    }
    let _ = reconcile_trial_attempt_as_paused(trial_dir)?;
    Ok(())
}

pub(crate) fn kill_trial_runtime_containers_best_effort(trial_dir: &Path) -> Result<bool> {
    let handles = runtime_trial_container_handles(trial_dir)?;
    if handles.is_empty() {
        return Ok(false);
    }
    let docker = DockerRuntime::connect()?;
    for handle in &handles {
        if let Err(err) = docker.kill_container(handle) {
            if !err.to_string().contains("not found") {
                return Err(err);
            }
        }
        if let Err(err) = docker.remove_container(handle, true) {
            if !err.to_string().contains("not found") {
                return Err(err);
            }
        }
    }
    let _ = reconcile_trial_attempt_as_killed(trial_dir)?;
    Ok(true)
}

fn format_trial_phase(phase: &TrialPhase) -> String {
    serde_json::to_value(phase)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{phase:?}"))
}

fn resume_trial_runtime_containers(trial_dir: &Path) -> Result<bool> {
    if !trial_attempt_state_exists(trial_dir) {
        return Ok(false);
    }
    let persisted = load_trial_attempt_state(trial_dir)?;
    if persisted.state.phase != TrialPhase::Paused {
        return Err(anyhow!(
            "resume_trial_not_paused: runtime phase is {}",
            format_trial_phase(&persisted.state.phase)
        ));
    }
    let handles: Vec<ContainerHandle> = trial_attempt_container_ids(&persisted.state)
        .into_iter()
        .map(|container_id| ContainerHandle { container_id })
        .collect();
    if handles.is_empty() {
        return Err(anyhow!(
            "resume_missing_runtime_container: paused runtime state exists for {} but no persisted container ids were found",
            trial_dir.display()
        ));
    }
    let docker = DockerRuntime::connect()?;
    for handle in &handles {
        docker.unpause_container(handle)?;
    }
    let _ = reconcile_trial_attempt_as_resumed(trial_dir)?;
    Ok(true)
}

pub fn pause_run(
    run_dir: &Path,
    trial_id: Option<&str>,
    label: Option<&str>,
    _timeout_seconds: u64,
) -> Result<PauseResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Pause)?;
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;
    let run_control = load_run_control(&run_dir)?;
    let status = run_control_status(&run_control);
    if status != "running" {
        return Err(anyhow!("pause_non_running: run status is {}", status));
    }

    let run_id = run_control_run_id(&run_control).unwrap_or_else(|| "run".to_string());
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
    let active_by_id: HashMap<String, RunControlActiveTrial> = run_control_active_trials(&run_control)
        .into_iter()
        .map(|entry| (entry.trial_id.clone(), entry))
        .collect();

    let mut paused_active_trials: Vec<RunControlActiveTrial> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    let checkpoint_acked_all = true;
    let stop_acked_all = true;

    for target_trial in &target_trials {
        let Some(active) = active_by_id.get(target_trial).cloned() else {
            failures.push(format!(
                "{}: pause_missing_active_trial_metadata",
                target_trial
            ));
            continue;
        };

        let trial_dir = run_dir.join("trials").join(target_trial);
        if !trial_dir.exists() {
            failures.push(format!("{}: pause_trial_not_found", target_trial));
            continue;
        }
        let pause_requested = match resolve_active_trial_control_mode(&trial_dir, &active) {
            Ok(ActiveTrialControlMode::RuntimeContainers) => {
                pause_trial_runtime_containers(&trial_dir)
            }
            Err(err) => {
                failures.push(format!("{}: pause request failed ({})", target_trial, err));
                continue;
            }
        };
        if let Err(err) = pause_requested {
            failures.push(format!("{}: pause request failed ({})", target_trial, err));
            continue;
        }
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

        paused_active_trials.push(active);
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
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Kill)?;
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;
    let run_control = load_run_control(&run_dir)?;
    let status = run_control_status(&run_control).to_string();

    match status.as_str() {
        "completed" | "failed" | "killed" => {
            return Err(anyhow!(
                "kill_terminal_status: run is already '{}', nothing to kill",
                status
            ));
        }
        _ => {}
    }

    let run_id = run_control_run_id(&run_control).unwrap_or_else(|| "run".to_string());

    let active_trial_ids = run_control_active_trial_ids(&run_control);
    let active_by_id: HashMap<String, RunControlActiveTrial> = run_control_active_trials(&run_control)
        .into_iter()
        .map(|entry| (entry.trial_id.clone(), entry))
        .collect();
    let mut survivor_active_trials: Vec<RunControlActiveTrial> = Vec::new();
    let mut killed_trials: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    for trial_id in &active_trial_ids {
        let trial_dir = run_dir.join("trials").join(trial_id);
        let kill_result = match resolve_kill_trial_control_mode(&trial_dir, trial_id) {
            Ok(ActiveTrialControlMode::RuntimeContainers) => {
                kill_trial_runtime_containers_best_effort(&trial_dir).and_then(|killed| {
                    if killed {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "kill_missing_runtime_container: no persisted runtime containers were recorded for {}",
                            trial_id
                        ))
                    }
                })
            }
            Err(err) => Err(err),
        };
        if let Err(err) = kill_result {
            failures.push(format!("{}: kill request failed ({})", trial_id, err));
            if let Some(active) = active_by_id.get(trial_id).cloned() {
                survivor_active_trials.push(active);
            }
            continue;
        }
        if trial_dir.exists() {
            if let Err(err) = write_trial_state(
                &trial_dir,
                trial_id,
                "killed",
                None,
                None,
                Some("killed_by_user"),
            ) {
                failures.push(format!(
                    "{}: failed to write trial_state ({})",
                    trial_id, err
                ));
                continue;
            }
        }
        killed_trials.push(trial_id.clone());
    }

    if failures.is_empty() {
        write_run_control_v2(&run_dir, &run_id, "killed", &[], None)?;
        return Ok(KillResult {
            run_id,
            run_dir: run_dir.to_path_buf(),
            previous_status: status,
            killed_trials,
        });
    }

    write_run_control_v2(
        &run_dir,
        &run_id,
        "interrupted",
        &survivor_active_trials,
        None,
    )?;
    Err(anyhow!(
        "kill_partial_failure: killed {} of {} active trial(s); failures: {}",
        killed_trials.len(),
        active_trial_ids.len(),
        failures.join(" | ")
    ))
}

pub fn resume_trial(
    run_dir: &Path,
    trial_id: Option<&str>,
    label: Option<&str>,
    set_bindings: &BTreeMap<String, Value>,
    strict: bool,
) -> Result<ResumeResult> {
    let _op_lease = acquire_run_operation_lease(run_dir, RunOperationType::Resume)?;
    let run_dir = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run_dir not found: {}", run_dir.display()))?;
    let run_control = load_run_control(&run_dir)?;
    let status = run_control_status(&run_control);
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
    let run_id = run_control_run_id(&run_control).unwrap_or_else(|| "run".to_string());

    if trial_attempt_state_exists(&trial_dir) {
        if label.is_some() || !set_bindings.is_empty() || strict {
            return Err(anyhow!(
                "resume_runtime_unpause_unsupported: live runtime resume does not support label selection, --set overrides, or --strict"
            ));
        }
        if resume_trial_runtime_containers(&trial_dir)? {
            write_trial_state(&trial_dir, &target_trial, "running", None, None, None)?;
            let active_trials = run_control_active_trials(&run_control);
            write_run_control_v2(&run_dir, &run_id, "running", &active_trials, None)?;
            return Ok(ResumeResult {
                trial_id: target_trial,
                mode: ResumeMode::RuntimeUnpause,
                selector: None,
                fork: None,
            });
        }
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
    let selector = resolve_resume_selector(&run_dir, &run_id, &target_trial, label.or(pause_label))?;

    let fork = fork_trial_inner(&run_dir, &target_trial, &selector, set_bindings, strict)?;
    Ok(ResumeResult {
        trial_id: target_trial,
        mode: ResumeMode::Fork,
        selector: Some(selector),
        fork: Some(fork),
    })
}
