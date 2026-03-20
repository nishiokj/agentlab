use anyhow::{anyhow, Result};
use lab_core::{
    canonical_json_digest, ensure_dir, runner_runtime_host_paths, ArtifactStore,
    RunnerRuntimeHostPaths, AGENTLAB_CONTRACT_IN_DIR, AGENTLAB_CONTRACT_OUT_DIR,
    AGENTLAB_ENV_GRADER_INPUT_PATH, AGENTLAB_ENV_MAPPED_GRADER_OUTPUT_PATH,
    AGENTLAB_ENV_RAW_GRADER_OUTPUT_PATH, AGENTLAB_ENV_REPL_IDX, AGENTLAB_ENV_RESULT_PATH,
    AGENTLAB_ENV_RUN_ID, AGENTLAB_ENV_TASK_ID, AGENTLAB_ENV_TIMEOUT_MS,
    AGENTLAB_ENV_TRAJECTORY_PATH, AGENTLAB_ENV_TRIAL_ID, AGENTLAB_ENV_TRIAL_INPUT_PATH,
    AGENTLAB_ENV_VARIANT_ID,
};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::{atomic_write_json_pretty, load_json_file};
use crate::model::{
    PreparedContractFilePaths, PreparedMountReference, PreparedTaskEnvironmentManifest,
    PreparedTrialIo, ResolvedMountReference, Variant, AGENTLAB_ENV_TASK_IMAGE,
    DEFAULT_CONTAINER_GRADER_INPUT_PATH, DEFAULT_CONTAINER_MAPPED_GRADER_OUTPUT_PATH,
    DEFAULT_CONTAINER_RAW_GRADER_OUTPUT_PATH, DEFAULT_CONTAINER_RESULT_PATH,
    DEFAULT_CONTAINER_TRAJECTORY_PATH, DEFAULT_CONTAINER_TRIAL_INPUT_PATH,
};
use crate::persistence::rows::infer_run_dir_from_path;
use crate::experiment::runtime::AgentRuntimeConfig;
use crate::util::sanitize_for_fs;
use crate::trial::spec::TaskBoundaryMaterialization;
use crate::trial::state::{ArtifactMountPlan, IoMountPlan, TaskSandboxPlan};

#[derive(Debug, Clone)]
pub(crate) struct TrialPaths {
    pub(crate) trial_dir: PathBuf,
    pub(crate) scratch_dir: PathBuf,
    pub(crate) in_dir: PathBuf,
    pub(crate) workspace: PathBuf,
    pub(crate) state: PathBuf,
    pub(crate) out: PathBuf,
    pub(crate) tmp: PathBuf,
    pub(crate) runtime: RunnerRuntimeHostPaths,
    pub(crate) exp_dir: PathBuf,
}

pub(crate) fn trial_runtime_scratch_dir(trial_dir: &Path) -> PathBuf {
    let root = infer_run_dir_from_path(trial_dir).unwrap_or_else(|| trial_dir.to_path_buf());
    let trial_label = trial_dir
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("trial");
    static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
    root.join(".scratch").join(format!(
        "{}_{}_{}",
        sanitize_for_fs(trial_label),
        std::process::id(),
        seq
    ))
}

impl TrialPaths {
    pub(crate) fn new(trial_dir: &Path, exp_dir: &Path) -> Result<Self> {
        let scratch_dir = trial_runtime_scratch_dir(trial_dir);
        let runtime = runner_runtime_host_paths(&scratch_dir);
        Ok(Self {
            trial_dir: trial_dir.to_path_buf(),
            scratch_dir,
            in_dir: runtime.in_dir.clone(),
            workspace: runtime.workspace_dir.clone(),
            state: runtime.state_dir.clone(),
            out: runtime.out_dir.clone(),
            tmp: runtime.tmp_dir.clone(),
            runtime,
            exp_dir: exp_dir.to_path_buf(),
        })
    }

    pub(crate) fn prepare(&self, seed_workspace_from_exp_dir: bool) -> Result<()> {
        ensure_dir(&self.in_dir)?;
        ensure_dir(&self.workspace)?;
        ensure_dir(&self.state)?;
        ensure_dir(&self.out)?;
        ensure_dir(&self.tmp)?;
        if seed_workspace_from_exp_dir {
            crate::util::copy_dir_filtered(
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
        Ok(())
    }

    pub(crate) fn cleanup_scratch(&self) -> Result<()> {
        crate::util::remove_path_if_exists(&self.scratch_dir)
    }
}

impl Drop for TrialPaths {
    fn drop(&mut self) {
        let _ = crate::util::remove_path_if_exists(&self.scratch_dir);
    }
}

pub(crate) fn normalize_task_prompt_aliases(task_payload: &Value) -> Value {
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

    let input_slot = root_obj
        .entry("input".to_string())
        .or_insert_with(|| json!({}));
    if !input_slot.is_object() {
        *input_slot = json!({});
    }
    if let Some(input_obj) = input_slot.as_object_mut() {
        input_obj.insert("prompt".to_string(), Value::String(prompt.clone()));
    }

    let drop_top_level_prompt = root_obj
        .get("prompt")
        .and_then(Value::as_str)
        .is_some_and(|value| value == prompt);
    if drop_top_level_prompt {
        root_obj.remove("prompt");
    }

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

pub(crate) fn build_trial_input(
    json_value: &Value,
    run_id: &str,
    trial_id: &str,
    variant: &Variant,
    _task_idx: usize,
    repl: usize,
    task_boundary: &TaskBoundaryMaterialization,
) -> Value {
    let normalized_task_payload = normalize_task_prompt_aliases(&task_boundary.task_payload);
    let time_limit_ms = task_boundary.time_limit_ms.unwrap_or(600_000);
    let requested_network_mode = json_value
        .pointer("/policy/task_sandbox/network")
        .and_then(Value::as_str)
        .unwrap_or("none");
    let allowed_hosts = json_value
        .pointer("/policy/task_sandbox/allowed_hosts")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let sanitization_profile = json_value
        .pointer("/policy/sanitization_profile")
        .and_then(Value::as_str)
        .unwrap_or("hermetic_functional");
    let integration_level = json_value
        .pointer("/runtime/agent_runtime/integration_level")
        .and_then(Value::as_str)
        .unwrap_or("cli_basic");
    let artifact_type = json_value
        .pointer("/agent/artifact_type")
        .and_then(Value::as_str)
        .unwrap_or("structured_json");

    let mut input = json!({
        "schema_version": "trial_input_v1",
        "ids": {
            "run_id": run_id,
            "trial_id": trial_id,
            "variant_id": variant.id,
            "task_id": task_boundary.task_id.as_str(),
            "repl_idx": repl
        },
        "task": normalized_task_payload,
        "artifact_type": artifact_type,
        "design": {
            "sanitization_profile": sanitization_profile,
            "integration_level": integration_level
        },
        "runtime": {
            "network_mode": requested_network_mode,
            "allowed_hosts": allowed_hosts,
            "task_image": task_boundary.task_image,
            "workdir": task_boundary.task_workdir,
            "time_limit_ms": time_limit_ms
        }
    });
    if let Some(obj) = input.as_object_mut() {
        obj.remove("ext");
    }
    input
}

pub(crate) fn prepared_task_environment_manifest_path(trial_dir: &Path) -> PathBuf {
    trial_dir
        .join("runtime")
        .join("prepared_task_environment.json")
}

pub(crate) fn write_prepared_task_environment_manifest(
    trial_dir: &Path,
    manifest: &PreparedTaskEnvironmentManifest,
) -> Result<()> {
    let manifest_path = prepared_task_environment_manifest_path(trial_dir);
    atomic_write_json_pretty(&manifest_path, &serde_json::to_value(manifest)?)?;
    Ok(())
}

pub(crate) fn load_prepared_task_environment_manifest(
    trial_dir: &Path,
) -> Result<PreparedTaskEnvironmentManifest> {
    let manifest_path = prepared_task_environment_manifest_path(trial_dir);
    if !manifest_path.exists() {
        return Err(anyhow!(
            "prepared_task_environment manifest missing for trial '{}': {}",
            trial_dir
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("unknown"),
            manifest_path.display()
        ));
    }
    let value = load_json_file(&manifest_path)?;
    let manifest: PreparedTaskEnvironmentManifest =
        serde_json::from_value(value).map_err(|err| {
            anyhow!(
                "invalid prepared_task_environment manifest at {}: {}",
                manifest_path.display(),
                err
            )
        })?;
    manifest.validate()?;
    Ok(manifest)
}

pub(crate) fn resolve_trial_timeout_ms(input: &Value) -> Option<u64> {
    input.pointer("/policy/timeout_ms").and_then(|v| v.as_u64())
}

pub(crate) fn build_runtime_contract_env(
    run_id: &str,
    input: &Value,
    io: &PreparedTrialIo,
    task_image: Option<&str>,
    timeout_ms: Option<u64>,
) -> std::collections::BTreeMap<String, String> {
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
    let repl_idx = input
        .pointer("/ids/repl_idx")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let mut env = std::collections::BTreeMap::new();
    env.insert(
        AGENTLAB_ENV_TRIAL_INPUT_PATH.to_string(),
        io.trial_input_path.clone(),
    );
    env.insert(
        AGENTLAB_ENV_GRADER_INPUT_PATH.to_string(),
        io.grader_input_path.clone(),
    );
    env.insert(AGENTLAB_ENV_RESULT_PATH.to_string(), io.result_path.clone());
    env.insert(
        AGENTLAB_ENV_RAW_GRADER_OUTPUT_PATH.to_string(),
        io.raw_grader_output_path.clone(),
    );
    env.insert(
        AGENTLAB_ENV_MAPPED_GRADER_OUTPUT_PATH.to_string(),
        io.mapped_grader_output_path.clone(),
    );
    env.insert(
        AGENTLAB_ENV_TRAJECTORY_PATH.to_string(),
        io.trajectory_path.clone(),
    );
    env.insert(AGENTLAB_ENV_RUN_ID.to_string(), run_id.to_string());
    env.insert(AGENTLAB_ENV_TRIAL_ID.to_string(), trial_id.to_string());
    env.insert(AGENTLAB_ENV_VARIANT_ID.to_string(), variant_id.to_string());
    env.insert(AGENTLAB_ENV_TASK_ID.to_string(), task_id.to_string());
    if let Some(task_image) = task_image.map(str::trim).filter(|v| !v.is_empty()) {
        env.insert(AGENTLAB_ENV_TASK_IMAGE.to_string(), task_image.to_string());
    }
    if let Some(timeout_ms) = timeout_ms {
        env.insert(AGENTLAB_ENV_TIMEOUT_MS.to_string(), timeout_ms.to_string());
    }
    env.insert(AGENTLAB_ENV_REPL_IDX.to_string(), repl_idx.to_string());
    env
}

pub(crate) fn resolve_trial_io_host_path(path: &str, paths: &TrialPaths) -> Result<PathBuf> {
    crate::trial::execution::map_container_path_to_host(path, paths)
}

pub(crate) fn prepare_io_paths(paths: &TrialPaths, input_bytes: &[u8]) -> Result<PreparedTrialIo> {
    let trial_input_path = DEFAULT_CONTAINER_TRIAL_INPUT_PATH.to_string();
    let grader_input_path = DEFAULT_CONTAINER_GRADER_INPUT_PATH.to_string();
    let result_path = DEFAULT_CONTAINER_RESULT_PATH.to_string();
    let raw_grader_output_path = DEFAULT_CONTAINER_RAW_GRADER_OUTPUT_PATH.to_string();
    let mapped_grader_output_path = DEFAULT_CONTAINER_MAPPED_GRADER_OUTPUT_PATH.to_string();
    let trajectory_path = DEFAULT_CONTAINER_TRAJECTORY_PATH.to_string();
    let trial_input_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_TRIAL_INPUT_PATH, paths)?;
    let grader_input_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_GRADER_INPUT_PATH, paths)?;
    let result_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_RESULT_PATH, paths)?;
    let raw_grader_output_host =
        resolve_trial_io_host_path(DEFAULT_CONTAINER_RAW_GRADER_OUTPUT_PATH, paths)?;
    let mapped_grader_output_host =
        resolve_trial_io_host_path(DEFAULT_CONTAINER_MAPPED_GRADER_OUTPUT_PATH, paths)?;
    let trajectory_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_TRAJECTORY_PATH, paths)?;
    let events_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_TRAJECTORY_PATH, paths)?;

    for host_path in [
        &trial_input_host,
        &grader_input_host,
        &result_host,
        &raw_grader_output_host,
        &mapped_grader_output_host,
        &trajectory_host,
    ] {
        if let Some(parent) = host_path.parent() {
            ensure_dir(parent)?;
        }
    }

    std::fs::write(&trial_input_host, input_bytes)?;

    if result_host.exists() {
        let _ = std::fs::remove_file(&result_host);
    }
    if raw_grader_output_host.exists() {
        let _ = std::fs::remove_file(&raw_grader_output_host);
    }
    if mapped_grader_output_host.exists() {
        let _ = std::fs::remove_file(&mapped_grader_output_host);
    }
    if trajectory_host.exists() {
        let _ = std::fs::remove_file(&trajectory_host);
    }
    if grader_input_host.exists() {
        let _ = std::fs::remove_file(&grader_input_host);
    }

    Ok(PreparedTrialIo {
        trial_input_host,
        grader_input_host,
        result_host,
        events_host,
        trial_input_path,
        grader_input_path,
        result_path,
        raw_grader_output_path,
        mapped_grader_output_path,
        trajectory_path,
        #[cfg(test)]
        input_host: resolve_trial_io_host_path(DEFAULT_CONTAINER_TRIAL_INPUT_PATH, paths)?,
        #[cfg(test)]
        output_host: resolve_trial_io_host_path(DEFAULT_CONTAINER_RESULT_PATH, paths)?,
    })
}

pub(crate) struct PreparedTaskEnvironment {
    pub(crate) manifest: PreparedTaskEnvironmentManifest,
    pub(crate) trial_paths: TrialPaths,
    pub(crate) io_paths: PreparedTrialIo,
    pub(crate) dynamic_mounts: Vec<ResolvedMountReference>,
    pub(crate) trial_input: Value,
}

fn build_task_sandbox_plan(
    task_boundary: &TaskBoundaryMaterialization,
    agent_runtime: &AgentRuntimeConfig,
    time_limit_ms: u64,
) -> TaskSandboxPlan {
    TaskSandboxPlan {
        image: task_boundary.task_image.clone(),
        workdir: task_boundary.task_workdir.clone(),
        materialization: task_boundary.materialization.clone(),
        io_mounts: IoMountPlan {
            in_dir: AGENTLAB_CONTRACT_IN_DIR.to_string(),
            out_dir: AGENTLAB_CONTRACT_OUT_DIR.to_string(),
            telemetry_mounts: Vec::new(),
        },
        artifact_mount: ArtifactMountPlan {
            host_artifact_path: agent_runtime.agent_artifact.to_string_lossy().to_string(),
            container_artifact_dir: "/opt/agent".to_string(),
        },
        network_mode: agent_runtime.network.clone(),
        time_limit_ms,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_task_environment_with_paths(
    trial_paths: TrialPaths,
    project_root: &Path,
    trial_dir: &Path,
    run_id: &str,
    trial_id: &str,
    trial_experiment: &Value,
    variant: &Variant,
    task_idx: usize,
    repl: usize,
    task_boundary: &TaskBoundaryMaterialization,
    agent_runtime: &AgentRuntimeConfig,
) -> Result<PreparedTaskEnvironment> {
    trial_paths.prepare(false)?;
    let dynamic_mounts: Vec<ResolvedMountReference> = Vec::new();

    let input = build_trial_input(
        trial_experiment,
        run_id,
        trial_id,
        variant,
        task_idx,
        repl,
        task_boundary,
    );
    let input_bytes = serde_json::to_vec_pretty(&input)?;
    let io_paths = prepare_io_paths(&trial_paths, &input_bytes)?;
    let resolved_time_limit_ms = resolve_trial_timeout_ms(&input).unwrap_or(600000);
    let runtime_env = build_runtime_contract_env(
        run_id,
        &input,
        &io_paths,
        Some(task_boundary.task_image.as_str()),
        Some(resolved_time_limit_ms),
    );
    let manifest = PreparedTaskEnvironmentManifest {
        schema_version: "prepared_task_environment_v1".to_string(),
        declaration: task_boundary.declaration.clone(),
        declaration_digest: canonical_json_digest(&task_boundary.declaration),
        run_id: run_id.to_string(),
        trial_id: trial_id.to_string(),
        variant_id: variant.id.clone(),
        task_id: task_boundary.task_id.clone(),
        task_index: task_idx,
        repl_idx: repl,
        task_image: task_boundary.task_image.clone(),
        workspace_root: trial_paths.workspace.to_string_lossy().to_string(),
        aux_mounts: dynamic_mounts
            .iter()
            .map(|mount| PreparedMountReference {
                host_path: mount.host_path.to_string_lossy().to_string(),
                mount_path: mount.mount_path.clone(),
            })
            .collect(),
        contract_files: PreparedContractFilePaths {
            trial_input: io_paths.trial_input_path.clone(),
            grader_input: io_paths.grader_input_path.clone(),
            result: io_paths.result_path.clone(),
            raw_grader_output: io_paths.raw_grader_output_path.clone(),
            mapped_grader_output: io_paths.mapped_grader_output_path.clone(),
            trajectory: io_paths.trajectory_path.clone(),
        },
        runtime_env: runtime_env.clone(),
        task_sandbox_plan: Some(build_task_sandbox_plan(
            task_boundary,
            agent_runtime,
            resolved_time_limit_ms,
        )),
    };
    write_prepared_task_environment_manifest(trial_dir, &manifest)?;

    Ok(PreparedTaskEnvironment {
        manifest,
        trial_paths,
        io_paths,
        dynamic_mounts,
        trial_input: input,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_task_environment(
    project_root: &Path,
    trial_dir: &Path,
    run_id: &str,
    trial_id: &str,
    trial_experiment: &Value,
    variant: &Variant,
    task_idx: usize,
    repl: usize,
    task_boundary: &TaskBoundaryMaterialization,
    agent_runtime: &AgentRuntimeConfig,
) -> Result<PreparedTaskEnvironment> {
    let trial_paths = TrialPaths::new(trial_dir, project_root)?;
    prepare_task_environment_with_paths(
        trial_paths,
        project_root,
        trial_dir,
        run_id,
        trial_id,
        trial_experiment,
        variant,
        task_idx,
        repl,
        task_boundary,
        agent_runtime,
    )
}
