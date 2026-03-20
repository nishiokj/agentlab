use anyhow::{anyhow, Result};
use lab_core::{
    AGENTLAB_CONTRACT_GRADER_AUX_DIR, AGENTLAB_CONTRACT_IN_DIR, AGENTLAB_CONTRACT_OUT_DIR,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::backend::docker::{ContainerHandle, DockerRuntime, ExecSpec};
use crate::config::{atomic_write_json_pretty, trial_conclusion_outcome_to_trial_outcome};
use crate::trial::execution::AdapterRunRequest;
use crate::experiment::runner::agent_artifact_archive_flag;
use crate::model::*;
use crate::trial::execution::validate_container_workspace_path;
use crate::util::{copy_file_if_exists, sanitize_for_fs, shell_quote};
use crate::trial::artifacts::{
    artifact_type_from_trial_input, extract_candidate_artifact_record, trial_output_payload_view,
};
use crate::trial::env::{
    benchmark_grader_uses_mapper, resolve_benchmark_conclusion_mapper_command,
    resolve_benchmark_grader_command, ResolvedGradingPhase,
};
use crate::trial::prepare::TrialPaths;
use crate::trial::state::{
    GraderOutputMode, GradingSandboxDetails, GradingSandboxPlan, IoMountPlan,
};

pub(crate) struct HiddenAssetBinding {
    pub(crate) hidden_path: String,
    pub(crate) revealed_path: String,
    pub(crate) stash_container_path: String,
}

const INJECTED_BUNDLE_SOURCE_MOUNT_PATH: &str = "/agentlab/_materialize/injected_bundle_src";

pub(crate) fn task_grading_enabled(task_payload: &Value) -> bool {
    task_payload
        .pointer("/grading/enabled")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

pub(crate) fn benchmark_retry_inputs(
    benchmark_grading_enabled: bool,
    trial_output: &Value,
    trial_conclusion_row: Option<&Value>,
    grade_error_reason: Option<&str>,
    agent_exit_status: &str,
) -> (String, String) {
    let agent_outcome = trial_output_payload_view(trial_output)
        .get("outcome")
        .and_then(Value::as_str)
        .unwrap_or("error");
    if !benchmark_grading_enabled {
        return (agent_outcome.to_string(), agent_exit_status.to_string());
    }
    if grade_error_reason.is_some() {
        return ("error".to_string(), "0".to_string());
    }
    if let Some(mapped_outcome) = trial_conclusion_row
        .and_then(|row| row.pointer("/reported_outcome"))
        .and_then(Value::as_str)
        .and_then(trial_conclusion_outcome_to_trial_outcome)
    {
        return (mapped_outcome.to_string(), "0".to_string());
    }
    if trial_conclusion_row.is_some() {
        return ("missing".to_string(), "0".to_string());
    }
    ("error".to_string(), "0".to_string())
}

pub(crate) fn mapped_grader_output_state(
    trial_conclusion_row: Option<&Value>,
    grade_error_reason: Option<&str>,
) -> Option<&'static str> {
    if trial_conclusion_row.is_some() {
        Some("valid")
    } else if let Some(reason) = grade_error_reason {
        if reason.starts_with("mapped_grader_output_invalid:") {
            Some("present_invalid")
        } else if reason.starts_with("mapped_grader_output_missing:") {
            Some("missing")
        } else {
            Some("missing")
        }
    } else {
        None
    }
}

fn resolve_in_task_image_hidden_asset_pairs(
    grader: &BenchmarkGraderConfig,
) -> Result<Vec<(String, String)>> {
    if !matches!(grader.strategy, GradingStrategy::InTaskImage) {
        return Ok(Vec::new());
    }
    let Some(config) = grader.in_task_image.as_ref() else {
        return Ok(Vec::new());
    };
    if config.hidden_paths.is_empty() && config.revealed_paths.is_empty() {
        return Ok(Vec::new());
    }
    if config.hidden_paths.is_empty() {
        return Err(anyhow!(
            "in_task_image grading revealed_paths requires hidden_paths to be configured"
        ));
    }
    if !config.revealed_paths.is_empty() && config.revealed_paths.len() != config.hidden_paths.len()
    {
        return Err(anyhow!(
            "in_task_image hidden_paths and revealed_paths must have matching lengths"
        ));
    }

    let mut bindings = Vec::with_capacity(config.hidden_paths.len());
    for (idx, hidden_path) in config.hidden_paths.iter().enumerate() {
        let revealed_path = config
            .revealed_paths
            .get(idx)
            .cloned()
            .unwrap_or_else(|| hidden_path.clone());
        validate_container_workspace_path(hidden_path).map_err(|err| {
            anyhow!(
                "invalid in_task_image hidden_paths[{}] '{}': {}",
                idx,
                hidden_path,
                err
            )
        })?;
        validate_container_workspace_path(&revealed_path).map_err(|err| {
            anyhow!(
                "invalid in_task_image revealed_paths[{}] '{}': {}",
                idx,
                revealed_path,
                err
            )
        })?;
        bindings.push((hidden_path.clone(), revealed_path));
    }
    Ok(bindings)
}

fn validate_in_task_image_hidden_asset_isolation(
    grader: &BenchmarkGraderConfig,
) -> Result<()> {
    let _ = resolve_in_task_image_hidden_asset_pairs(grader)?;
    Ok(())
}

pub(crate) fn build_hidden_asset_bindings(
    grader: &BenchmarkGraderConfig,
) -> Result<Vec<HiddenAssetBinding>> {
    resolve_in_task_image_hidden_asset_pairs(grader)?
        .into_iter()
        .enumerate()
        .map(|(idx, (hidden_path, revealed_path))| {
            Ok(HiddenAssetBinding {
                hidden_path: hidden_path.clone(),
                revealed_path,
                stash_container_path: format!(
                    "/tmp/agentlab_hidden_stash_{:02}_{}",
                    idx,
                    sanitize_for_fs(&hidden_path)
                ),
            })
        })
        .collect()
}

fn internal_exec_log_paths(trial_dir: &Path, label: &str) -> (PathBuf, PathBuf) {
    let name = sanitize_for_fs(label);
    let log_dir = trial_dir.join("logs").join("runtime");
    (
        log_dir.join(format!("{}_stdout.log", name)),
        log_dir.join(format!("{}_stderr.log", name)),
    )
}

fn run_exec_checked(
    docker: &DockerRuntime,
    handle: &ContainerHandle,
    trial_dir: &Path,
    label: &str,
    command: Vec<String>,
    workdir: Option<&str>,
    timeout_ms: u64,
) -> Result<()> {
    let exec = docker.exec(
        handle,
        &ExecSpec {
            command,
            env: BTreeMap::new(),
            workdir: workdir.map(str::to_string),
        },
    )?;
    let (stdout_path, stderr_path) = internal_exec_log_paths(trial_dir, label);
    let stream = docker.stream_exec_output(
        &exec,
        &stdout_path,
        &stderr_path,
        Some(Duration::from_millis(timeout_ms.max(1_000))),
    )?;
    let status = docker
        .wait_exec(&exec)
        .unwrap_or(crate::backend::docker::ExecStatus {
            exit_code: None,
            running: false,
        });
    if stream.timed_out {
        let stdout = fs::read_to_string(&stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        return Err(anyhow!(
            "container command '{}' timed out; stdout:\n{}\nstderr:\n{}\nlogs: {}, {}",
            label,
            stdout,
            stderr,
            stdout_path.display(),
            stderr_path.display()
        ));
    }
    if status.exit_code != Some(0) {
        let stdout = fs::read_to_string(&stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        return Err(anyhow!(
            "container command '{}' failed with exit status {}; stdout:\n{}\nstderr:\n{}\nlogs: {}, {}",
            label,
            status
                .exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            stdout,
            stderr,
            stdout_path.display(),
            stderr_path.display()
        ));
    }
    Ok(())
}

fn run_shell_checked(
    docker: &DockerRuntime,
    handle: &ContainerHandle,
    trial_dir: &Path,
    label: &str,
    script: &str,
    workdir: Option<&str>,
    timeout_ms: u64,
) -> Result<()> {
    run_exec_checked(
        docker,
        handle,
        trial_dir,
        label,
        vec![
            "/bin/sh".to_string(),
            "-lc".to_string(),
            format!("set -e\n{}", script),
        ],
        workdir,
        timeout_ms,
    )
}

pub(crate) fn stash_hidden_assets(
    docker: &DockerRuntime,
    handle: &ContainerHandle,
    trial_dir: &Path,
    bindings: &[HiddenAssetBinding],
    timeout_ms: u64,
) -> Result<()> {
    for (idx, binding) in bindings.iter().enumerate() {
        run_shell_checked(
            docker,
            handle,
            trial_dir,
            &format!("hide_hidden_asset_{}", idx),
            &format!(
                "mkdir -p {stash_parent}\nrm -rf {stash}\nmv {hidden} {stash}",
                stash_parent = shell_quote(
                    Path::new(&binding.stash_container_path)
                        .parent()
                        .and_then(|value| value.to_str())
                        .unwrap_or("/tmp")
                ),
                stash = shell_quote(&binding.stash_container_path),
                hidden = shell_quote(&binding.hidden_path),
            ),
            None,
            timeout_ms,
        )?;
    }
    Ok(())
}

pub(crate) fn reveal_hidden_assets(
    docker: &DockerRuntime,
    handle: &ContainerHandle,
    trial_dir: &Path,
    bindings: &[HiddenAssetBinding],
    timeout_ms: u64,
) -> Result<()> {
    for (idx, binding) in bindings.iter().enumerate() {
        let reveal_parent = Path::new(&binding.revealed_path)
            .parent()
            .and_then(|value| value.to_str())
            .unwrap_or("/");
        run_shell_checked(
            docker,
            handle,
            trial_dir,
            &format!("reveal_hidden_asset_{}", idx),
            &format!(
                "mkdir -p {parent}\nrm -rf {revealed}\nmv {stash} {revealed}",
                parent = shell_quote(reveal_parent),
                revealed = shell_quote(&binding.revealed_path),
                stash = shell_quote(&binding.stash_container_path),
            ),
            None,
            timeout_ms,
        )?;
    }
    Ok(())
}

pub(crate) fn materialize_injected_grader_bundle(
    docker: &DockerRuntime,
    handle: &ContainerHandle,
    trial_dir: &Path,
    resolved: &ResolvedGradingPhase,
    timeout_ms: u64,
) -> Result<()> {
    let source = resolved
        .injected_bundle_host_path
        .as_ref()
        .ok_or_else(|| anyhow!("injected grading missing resolved bundle host path"))?;
    let copy_dest = resolved
        .injected_copy_dest
        .as_deref()
        .ok_or_else(|| anyhow!("injected grading missing copy destination"))?;
    validate_container_workspace_path(copy_dest)?;
    let quoted_dest = shell_quote(copy_dest);
    let extract_script = if source.is_dir() {
        format!(
            "cp -R {src}/. {dest}",
            src = shell_quote(INJECTED_BUNDLE_SOURCE_MOUNT_PATH),
            dest = quoted_dest
        )
    } else if let Some(tar_flag) = agent_artifact_archive_flag(source) {
        format!(
            "tar {tar_flag} {src} -C {dest}",
            tar_flag = tar_flag,
            src = shell_quote(INJECTED_BUNDLE_SOURCE_MOUNT_PATH),
            dest = quoted_dest
        )
    } else {
        format!(
            "cp {src} {dest}/",
            src = shell_quote(INJECTED_BUNDLE_SOURCE_MOUNT_PATH),
            dest = quoted_dest
        )
    };
    run_shell_checked(
        docker,
        handle,
        trial_dir,
        "injected_grader_bundle",
        &format!(
            "mkdir -p {dest}\nfind {dest} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +\n{extract}",
            dest = quoted_dest,
            extract = extract_script,
        ),
        None,
        timeout_ms,
    )
}

pub(crate) fn validate_benchmark_grading_contract(request: &AdapterRunRequest<'_>) -> Result<()> {
    if !request.benchmark_grading_enabled {
        return Ok(());
    }
    let grader = request
        .benchmark_grader
        .ok_or_else(|| anyhow!("benchmark grading enabled without grader config"))?;
    validate_in_task_image_hidden_asset_isolation(grader)?;
    if resolve_benchmark_grader_command(request)?.is_none() {
        return Err(anyhow!(
            "benchmark grading is mandatory but no grader command resolved for this trial"
        ));
    }
    if benchmark_grader_uses_mapper(Some(grader))
        && resolve_benchmark_conclusion_mapper_command(request, grader)?.is_none()
    {
        return Err(anyhow!(
            "benchmark grading mapper mode requires a conclusion mapper command for every trial"
        ));
    }
    Ok(())
}

pub(crate) fn build_grading_sandbox_plan(
    grader: &BenchmarkGraderConfig,
    resolved: &ResolvedGradingPhase,
) -> Result<GradingSandboxPlan> {
    let details = match grader.strategy {
        GradingStrategy::InTaskImage => {
            validate_in_task_image_hidden_asset_isolation(grader)?;
            let config = grader.in_task_image.clone().unwrap_or_default();
            GradingSandboxDetails::InTaskImage {
                hidden_paths: config.hidden_paths,
                revealed_paths: config.revealed_paths,
            }
        }
        GradingStrategy::Injected => {
            let bundle_host_path = resolved
                .injected_bundle_host_path
                .as_ref()
                .map(|path| path.to_string_lossy().to_string())
                .or_else(|| grader.injected.as_ref().map(|config| config.bundle.clone()))
                .ok_or_else(|| anyhow!("injected grading missing injected config"))?;
            let copy_dest = resolved
                .injected_copy_dest
                .clone()
                .or_else(|| {
                    grader
                        .injected
                        .as_ref()
                        .map(|config| config.copy_dest.clone())
                })
                .ok_or_else(|| anyhow!("injected grading missing injected copy destination"))?;
            GradingSandboxDetails::Injected {
                bundle_host_path,
                copy_dest,
            }
        }
        GradingStrategy::Separate => GradingSandboxDetails::Separate {
            image: resolved.image.clone(),
            workdir: resolved.workdir.clone(),
        },
    };
    Ok(GradingSandboxPlan {
        strategy: grader.strategy.clone(),
        command: resolved.command.clone(),
        io_mounts: IoMountPlan {
            in_dir: AGENTLAB_CONTRACT_IN_DIR.to_string(),
            out_dir: AGENTLAB_CONTRACT_OUT_DIR.to_string(),
            telemetry_mounts: Vec::new(),
        },
        output_mode: if benchmark_grader_uses_mapper(Some(grader)) {
            GraderOutputMode::RawThenMap {
                mapper_ref: grader.conclusion.mapper.clone().unwrap_or_default(),
            }
        } else {
            GraderOutputMode::DirectMapped
        },
        details,
    })
}

fn stage_grader_aux_copy(
    trial_paths: &TrialPaths,
    filename: &str,
    source: &Path,
) -> Result<Option<String>> {
    if !source.exists() {
        return Ok(None);
    }
    let host_path = trial_paths.in_dir.join("grader").join(filename);
    copy_file_if_exists(source, &host_path)?;
    Ok(Some(format!(
        "{}/{}",
        AGENTLAB_CONTRACT_GRADER_AUX_DIR, filename
    )))
}

fn build_grader_input_value(
    trial_input: &Value,
    trial_output: &Value,
    trial_paths: &TrialPaths,
    task_workdir: &str,
    agent_exit_status: &str,
    result_parse_error: Option<&str>,
    started_at: &str,
    ended_at: &str,
    diff_path: Option<&Path>,
    patch_path: Option<&Path>,
) -> Result<GraderInputV1> {
    let ids = ContractIds {
        run_id: trial_input
            .pointer("/ids/run_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        trial_id: trial_input
            .pointer("/ids/trial_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        variant_id: trial_input
            .pointer("/ids/variant_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        task_id: trial_input
            .pointer("/ids/task_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        repl_idx: trial_input
            .pointer("/ids/repl_idx")
            .and_then(Value::as_u64)
            .map(|value| value as u32),
        schedule_idx: trial_input
            .pointer("/ids/schedule_idx")
            .and_then(Value::as_u64)
            .map(|value| value as u32),
    };
    let artifact_type = artifact_type_from_trial_input(trial_input);
    let candidate_artifact = extract_candidate_artifact_record(trial_output, artifact_type.clone());
    let diff_container_path = match diff_path {
        Some(path) => stage_grader_aux_copy(trial_paths, "workspace_diff_incremental.json", path)?,
        None => None,
    };
    let patch_container_path = match patch_path {
        Some(path) => stage_grader_aux_copy(trial_paths, "workspace_patch_incremental.json", path)?,
        None => None,
    };
    Ok(GraderInputV1 {
        schema_version: "grader_input_v1".to_string(),
        ids,
        task: trial_input
            .pointer("/task")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
        artifact_type,
        agent_phase: GraderInputAgentPhase {
            exit_code: agent_exit_status.parse::<i32>().ok(),
            timed_out: false,
            result_present: !matches!(candidate_artifact.state, CandidateArtifactState::Missing),
            result_schema_valid: result_parse_error.is_none()
                && matches!(candidate_artifact.state, CandidateArtifactState::Valid),
            started_at: started_at.to_string(),
            ended_at: ended_at.to_string(),
        },
        candidate_artifact,
        workspace_delta: WorkspaceDeltaContract {
            state: if diff_container_path.is_some() {
                WorkspaceDeltaState::Available
            } else {
                WorkspaceDeltaState::Missing
            },
            diff_path: diff_container_path,
            patch_path: patch_container_path,
        },
        paths: GraderInputPaths {
            result_path: DEFAULT_CONTAINER_RESULT_PATH.to_string(),
        },
        workdir: task_workdir.to_string(),
    })
}

pub(crate) fn write_grader_input_file(
    io_paths: &PreparedTrialIo,
    trial_input: &Value,
    trial_output: &Value,
    trial_paths: &TrialPaths,
    task_workdir: &str,
    agent_exit_status: &str,
    result_parse_error: Option<&str>,
    started_at: &str,
    ended_at: &str,
    diff_path: Option<&Path>,
    patch_path: Option<&Path>,
) -> Result<()> {
    let grader_input = build_grader_input_value(
        trial_input,
        trial_output,
        trial_paths,
        task_workdir,
        agent_exit_status,
        result_parse_error,
        started_at,
        ended_at,
        diff_path,
        patch_path,
    )?;
    atomic_write_json_pretty(
        &io_paths.grader_input_host,
        &serde_json::to_value(grader_input)?,
    )?;
    Ok(())
}
