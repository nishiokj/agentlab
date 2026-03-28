use anyhow::{anyhow, Result};
use lab_core::{AGENTLAB_CONTRACT_RUNTIME_AUX_DIR, AGENTLAB_TASK_WORKDIR_PLACEHOLDER};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::experiment::preflight::is_runner_staged_script_path;
use crate::experiment::runtime::TASK_WORKDIR_TEMPLATE_PLACEHOLDER;
use crate::model::{
    BenchmarkGraderConfig, GraderConclusionMode, GradingStrategy, ResolvedMountReference,
    AGENT_ARTIFACT_PATH_ENV_VALUE, MAPPED_GRADER_OUTPUT_FILENAME, RAW_GRADER_OUTPUT_FILENAME,
};
use crate::package::staging::matches_contract_runtime_root;
use crate::trial::execution::AdapterRunRequest;
use crate::trial::execution::{
    map_container_path_to_host, resolve_container_workspace, resolve_task_sandbox_image,
};

pub(crate) struct ResolvedGradingPhase {
    pub(crate) image: String,
    pub(crate) workdir: String,
    pub(crate) command: Vec<String>,
    pub(crate) extra_mounts: Vec<ResolvedMountReference>,
    pub(crate) injected_bundle_host_path: Option<PathBuf>,
    pub(crate) injected_copy_dest: Option<String>,
}

fn resolve_grading_bundle_host_path(
    request: &AdapterRunRequest<'_>,
    raw_bundle: &str,
) -> Result<PathBuf> {
    let rendered = replace_task_workdir_placeholder(raw_bundle, request.task_workdir);
    if rendered.starts_with("/agentlab/") || rendered.starts_with(AGENTLAB_TASK_WORKDIR_PLACEHOLDER)
    {
        return map_container_path_to_host(&rendered, request.trial_paths);
    }
    Ok(PathBuf::from(rendered))
}

pub(crate) fn resolve_grading_phase(
    request: &AdapterRunRequest<'_>,
    grader: &BenchmarkGraderConfig,
    base_command: &[String],
) -> Result<ResolvedGradingPhase> {
    let task_image = resolve_task_sandbox_image(request)?;
    let task_workdir = resolve_container_workspace(request)?;
    match grader.strategy {
        GradingStrategy::InTaskImage => Ok(ResolvedGradingPhase {
            image: task_image,
            workdir: task_workdir.to_string(),
            command: base_command.to_vec(),
            extra_mounts: Vec::new(),
            injected_bundle_host_path: None,
            injected_copy_dest: None,
        }),
        GradingStrategy::Separate => {
            let separate = grader.separate.as_ref().ok_or_else(|| {
                anyhow!("benchmark.grader.separate is required when strategy='separate'")
            })?;
            Ok(ResolvedGradingPhase {
                image: separate.image.clone(),
                workdir: separate.workdir.clone(),
                command: base_command.to_vec(),
                extra_mounts: Vec::new(),
                injected_bundle_host_path: None,
                injected_copy_dest: None,
            })
        }
        GradingStrategy::Injected => {
            let injected = grader.injected.as_ref().ok_or_else(|| {
                anyhow!("benchmark.grader.injected is required when strategy='injected'")
            })?;
            let bundle_host_path = resolve_grading_bundle_host_path(request, &injected.bundle)?;
            if !bundle_host_path.exists() {
                return Err(anyhow!(
                    "benchmark grader bundle not found for injected strategy: {}",
                    bundle_host_path.display()
                ));
            }
            Ok(ResolvedGradingPhase {
                image: task_image,
                workdir: task_workdir.to_string(),
                command: base_command.to_vec(),
                extra_mounts: Vec::new(),
                injected_bundle_host_path: Some(bundle_host_path),
                injected_copy_dest: Some(injected.copy_dest.clone()),
            })
        }
    }
}

pub(crate) fn resolve_benchmark_grader_command(
    request: &AdapterRunRequest<'_>,
) -> Result<Option<Vec<String>>> {
    if !request.benchmark_grading_enabled {
        return Ok(None);
    }
    let Some(grader) = request.benchmark_grader else {
        return Ok(None);
    };
    if grader.command.is_empty() {
        return Ok(None);
    }
    let workspace = resolve_container_workspace(request)?;
    let rendered = grader
        .command
        .iter()
        .map(|token| replace_task_workdir_placeholder(token, workspace))
        .collect::<Vec<_>>();
    if let Some(script_path) = rendered.get(1).map(|value| value.trim()) {
        if Path::new(script_path).is_absolute()
            && !is_runner_staged_script_path(script_path)
            && !matches_contract_runtime_root(script_path, workspace)
        {
            return Err(anyhow!(
                "forbidden benchmark adapter script path '{}': script must be under {} or the task workdir",
                script_path,
                AGENTLAB_CONTRACT_RUNTIME_AUX_DIR
            ));
        }
    }
    Ok(Some(rendered))
}

pub(crate) fn benchmark_grader_uses_mapper(grader: Option<&BenchmarkGraderConfig>) -> bool {
    grader.is_some_and(|grader| matches!(grader.conclusion.mode, GraderConclusionMode::Mapper))
}

pub(crate) fn benchmark_grader_expected_output_filename(
    grader: Option<&BenchmarkGraderConfig>,
) -> &'static str {
    if benchmark_grader_uses_mapper(grader) {
        RAW_GRADER_OUTPUT_FILENAME
    } else {
        MAPPED_GRADER_OUTPUT_FILENAME
    }
}

pub(crate) fn resolve_benchmark_conclusion_mapper_command(
    request: &AdapterRunRequest<'_>,
    grader: &BenchmarkGraderConfig,
) -> Result<Option<Vec<String>>> {
    if !matches!(grader.conclusion.mode, GraderConclusionMode::Mapper) {
        return Ok(None);
    }
    let mapper = grader
        .conclusion
        .mapper
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "benchmark.grader.conclusion.mapper is required when benchmark.grader.conclusion.mode='mapper'"
            )
        })?;
    let workspace = resolve_container_workspace(request)?;
    let rendered = replace_task_workdir_placeholder(mapper, workspace);
    if Path::new(&rendered).is_absolute()
        && !is_runner_staged_script_path(&rendered)
        && !matches_contract_runtime_root(&rendered, workspace)
    {
        return Err(anyhow!(
            "forbidden benchmark conclusion mapper path '{}': mapper must be under {} or the task workdir",
            rendered,
            AGENTLAB_CONTRACT_RUNTIME_AUX_DIR
        ));
    }
    Ok(Some(vec![rendered]))
}

pub(crate) fn resolve_runtime_agent_command(
    request: &AdapterRunRequest<'_>,
) -> Result<Vec<String>> {
    if request.runtime.command_raw.is_empty() {
        return Err(anyhow!("resolved runtime.agent_runtime.command is empty"));
    }
    let mut command = request
        .runtime
        .command_raw
        .iter()
        .map(|token| replace_task_workdir_placeholder(token, request.task_workdir))
        .collect::<Vec<_>>();
    command.extend(
        request
            .variant_args
            .iter()
            .map(|token| replace_task_workdir_placeholder(token, request.task_workdir)),
    );
    #[cfg(test)]
    {
        if !request.runtime.io.input_arg.trim().is_empty() {
            command.push(request.runtime.io.input_arg.clone());
            command.push(request.io_paths.trial_input_path.clone());
        }
        if !request.runtime.io.output_arg.trim().is_empty() {
            command.push(request.runtime.io.output_arg.clone());
            command.push(request.io_paths.result_path.clone());
        }
    }
    Ok(command)
}

pub(crate) fn replace_task_workdir_placeholder(raw: &str, task_workdir: &str) -> String {
    raw.replace(TASK_WORKDIR_TEMPLATE_PLACEHOLDER, task_workdir)
}

pub(crate) fn build_exec_env(
    request: &AdapterRunRequest<'_>,
    workspace: &str,
    extra_env: Option<(&str, &str)>,
    include_agent_path: bool,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for (key, value) in request.runtime_overrides_env {
        env.insert(
            key.clone(),
            replace_task_workdir_placeholder(value, workspace),
        );
    }
    for (key, value) in request.runtime_env {
        env.insert(
            key.clone(),
            replace_task_workdir_placeholder(value, workspace),
        );
    }
    if include_agent_path && request.agent_artifact.is_some() && !env.contains_key("PATH") {
        if let Some((_, value)) = AGENT_ARTIFACT_PATH_ENV_VALUE.split_once('=') {
            env.insert("PATH".to_string(), value.to_string());
        }
    }
    env.insert("WORKSPACE".to_string(), workspace.to_string());
    if let Some((key, value)) = extra_env {
        env.insert(key.to_string(), value.to_string());
    }
    env
}
