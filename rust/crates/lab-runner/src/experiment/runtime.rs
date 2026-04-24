use anyhow::{anyhow, Context, Result};
use lab_core::{sha256_bytes, sha256_file, AGENTLAB_TASK_WORKDIR_PLACEHOLDER};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::*;
use crate::experiment::runner::configured_network_mode;
use crate::experiment::state::{RunBehavior, RunExecutionOptions};
use crate::model::*;
use crate::package::authoring::{
    compute_artifact_content_digest, contains_removed_runtime_template,
    resolve_dx_artifact_path, resolve_existing_public_path_reference,
};
use crate::package::sealed::*;
use crate::package::staging::*;
use crate::package::validate::*;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_TASK_WORKDIR_FALLBACK: &str = "/workspace";

pub(crate) const TASK_WORKDIR_TEMPLATE_PLACEHOLDER: &str = AGENTLAB_TASK_WORKDIR_PLACEHOLDER;

// ---------------------------------------------------------------------------
// #[cfg(test)] companion types for AgentRuntimeConfig
// ---------------------------------------------------------------------------

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImageSource {
    Global,
    PerTask,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgentExecutionExecutor {
    Docker,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct AgentExecutionConfig {
    pub(crate) executor: Option<AgentExecutionExecutor>,
    pub(crate) image: Option<String>,
    pub(crate) network: String,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct AgentRuntimeIoConfig {
    pub(crate) input_arg: String,
    pub(crate) output_arg: String,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgentLaunchMode {
    File,
    Stdio,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct WorkspacePatchSpec {
    pub(crate) source_from_host: PathBuf,
    pub(crate) target_path: String,
}

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct DependencyFileStagingSpec {
    pub(crate) source_from_host: PathBuf,
    pub(crate) destination_path: String,
    pub(crate) required: bool,
    pub(crate) read_only: bool,
}

pub(crate) enum PathResolutionContext<'a> {
    Build {
        exp_dir: &'a Path,
        project_root: &'a Path,
    },
    Run {
        package_dir: &'a Path,
        variant_id: &'a str,
    },
}

#[derive(Clone)]
pub(crate) struct AgentRuntimeConfig {
    pub(crate) adapter_ref: AgentAdapterRef,
    pub(crate) command_raw: Vec<String>,
    pub(crate) image: String,
    pub(crate) network: String,
    pub(crate) agent_artifact: PathBuf,
    pub(crate) agent_artifact_digest: Option<String>,
    pub(crate) agent_artifact_resolved_path: Option<PathBuf>,
    pub(crate) integration_level: String,
    pub(crate) env: BTreeMap<String, String>,
    pub(crate) env_from_host: Vec<String>,
    pub(crate) trajectory_path: Option<String>,
    pub(crate) causal_extraction: Option<String>,
    #[cfg(test)]
    pub(crate) sandbox_image: Option<String>,
    #[cfg(test)]
    pub(crate) image_source: ImageSource,
    #[cfg(test)]
    pub(crate) execution: AgentExecutionConfig,
    #[cfg(test)]
    pub(crate) io: AgentRuntimeIoConfig,
    #[cfg(test)]
    pub(crate) launch_mode: AgentLaunchMode,
    #[cfg(test)]
    pub(crate) workspace_patches: Vec<WorkspacePatchSpec>,
    #[cfg(test)]
    pub(crate) default_timeout_ms: Option<u64>,
    #[cfg(test)]
    pub(crate) tracing_mode: Option<String>,
    #[cfg(test)]
    pub(crate) force_container: bool,
    pub(crate) dependency_file_staging: Vec<DependencyFileStagingSpec>,
    #[cfg(test)]
    pub(crate) dependency_services: Vec<Value>,
}

#[derive(Clone)]
pub(crate) struct VariantRuntimeProfile {
    pub(crate) experiment: Value,
    pub(crate) variant_args: Vec<String>,
    pub(crate) agent_runtime: AgentRuntimeConfig,
    pub(crate) agent_runtime_env: BTreeMap<String, String>,
    pub(crate) invocation_source: String,
    pub(crate) configured_network_mode: String,
    pub(crate) effective_network_mode: String,
}

// ---------------------------------------------------------------------------
// Binding / template resolution
// ---------------------------------------------------------------------------

pub(crate) fn binding_lookup<'a>(bindings: &'a Value, key: &str) -> Option<&'a Value> {
    if key.trim().is_empty() {
        return None;
    }
    let pointer = format!("/{}", key.split('.').collect::<Vec<_>>().join("/"));
    bindings.pointer(&pointer)
}

pub(crate) fn binding_lookup_string(
    bindings: &Value,
    key: &str,
    field_name: &str,
) -> Result<Option<String>> {
    let Some(value) = binding_lookup(bindings, key) else {
        return Ok(None);
    };
    let token = match value {
        Value::String(v) => v.clone(),
        Value::Number(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        _ => {
            return Err(anyhow!(
                "{} runtime binding '{}' must resolve to string|number|bool (got {})",
                field_name,
                key,
                value_type_name(value)
            ))
        }
    };
    Ok(Some(token))
}

pub(crate) fn resolve_runtime_binding_value(
    name: &str,
    bindings: &Value,
    runtime_env_inputs: &BTreeMap<String, String>,
    field_name: &str,
) -> Result<String> {
    if name == "WORKSPACE" {
        return Ok(TASK_WORKDIR_TEMPLATE_PLACEHOLDER.to_string());
    }
    if let Some(value) = binding_lookup_string(bindings, name, field_name)? {
        return Ok(value);
    }
    if let Some(value) = runtime_env_inputs.get(name) {
        return Ok(value.clone());
    }
    if let Ok(value) = std::env::var(name) {
        return Ok(value);
    }
    Err(anyhow!(
        "{} references missing runtime binding ${}; provide it in variant bindings or launch-time env",
        field_name,
        name
    ))
}

pub(crate) fn render_runtime_template(
    raw: &str,
    bindings: &Value,
    runtime_env_inputs: &BTreeMap<String, String>,
    field_name: &str,
) -> Result<String> {
    if contains_removed_runtime_template(raw) {
        return Err(anyhow!(
            "{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
            field_name
        ));
    }
    let chars: Vec<char> = raw.chars().collect();
    let mut idx = 0usize;
    let mut out = String::new();
    while idx < chars.len() {
        let ch = chars[idx];
        if ch != '$' {
            out.push(ch);
            idx += 1;
            continue;
        }
        if idx + 1 >= chars.len() {
            out.push(ch);
            idx += 1;
            continue;
        }
        let start = chars[idx + 1];
        if !(start == '_' || start.is_ascii_alphabetic()) {
            out.push(ch);
            idx += 1;
            continue;
        }
        let mut end = idx + 2;
        while end < chars.len() {
            let next = chars[end];
            if next == '_' || next.is_ascii_alphanumeric() {
                end += 1;
            } else {
                break;
            }
        }
        let name: String = chars[idx + 1..end].iter().collect();
        out.push_str(&resolve_runtime_binding_value(
            &name,
            bindings,
            runtime_env_inputs,
            field_name,
        )?);
        idx = end;
    }
    Ok(out)
}

pub(crate) fn resolve_command_templates(
    command: &[String],
    bindings: &Value,
    runtime_env_inputs: &BTreeMap<String, String>,
) -> Result<Vec<String>> {
    let mut resolved = Vec::with_capacity(command.len());
    for (idx, token) in command.iter().enumerate() {
        resolved.push(render_runtime_template(
            token,
            bindings,
            runtime_env_inputs,
            &format!("runtime.agent_runtime.command[{}]", idx),
        )?);
    }
    Ok(resolved)
}

pub(crate) fn resolve_env_templates(
    env: &BTreeMap<String, String>,
    bindings: &Value,
    runtime_env_inputs: &BTreeMap<String, String>,
    field_prefix: &str,
) -> Result<BTreeMap<String, String>> {
    let mut resolved = BTreeMap::new();
    for (key, value) in env {
        resolved.insert(
            key.clone(),
            render_runtime_template(
                value,
                bindings,
                runtime_env_inputs,
                &format!("{}.{}", field_prefix, key),
            )?,
        );
    }
    Ok(resolved)
}

pub(crate) fn parse_command_field(
    value: Option<&Value>,
    field: &str,
) -> Result<Option<Vec<String>>> {
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

// ---------------------------------------------------------------------------
// Agent runtime resolution
// ---------------------------------------------------------------------------

pub(crate) fn resolve_agent_runtime(
    json_value: &Value,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<AgentRuntimeConfig> {
    resolve_agent_runtime_with_context(
        json_value,
        PathResolutionContext::Build {
            exp_dir,
            project_root,
        },
    )
}

pub(crate) fn resolve_packaged_agent_runtime(
    json_value: &Value,
    package_dir: &Path,
    variant_id: &str,
) -> Result<AgentRuntimeConfig> {
    resolve_agent_runtime_with_context(
        json_value,
        PathResolutionContext::Run {
            package_dir,
            variant_id,
        },
    )
}

pub(crate) fn resolve_agent_artifact_path_for_context(
    raw: &str,
    field_name: &str,
    context: &PathResolutionContext<'_>,
) -> Result<PathBuf> {
    match context {
        PathResolutionContext::Build {
            exp_dir,
            project_root,
        } => {
            let trimmed = raw.trim();
            if trimmed.starts_with("./") || trimmed.starts_with("../") || trimmed.contains('/') {
                Ok(normalize_path(&exp_dir.join(trimmed)))
            } else {
                Ok(resolve_dx_artifact_path(trimmed, exp_dir, project_root))
            }
        }
        PathResolutionContext::Run { package_dir, .. } => {
            let candidate = PathBuf::from(raw);
            if candidate.is_absolute() {
                Ok(normalize_path(&candidate))
            } else {
                resolve_package_path_under_root(package_dir, raw, field_name)
            }
        }
    }
}

pub(crate) fn resolve_runtime_source_path_for_context(
    raw: &str,
    field_name: &str,
    context: &PathResolutionContext<'_>,
) -> Result<PathBuf> {
    let candidate = PathBuf::from(raw);
    match context {
        PathResolutionContext::Build { exp_dir, .. } => Ok(if candidate.is_absolute() {
            normalize_path(&candidate)
        } else {
            normalize_path(&exp_dir.join(candidate))
        }),
        PathResolutionContext::Run { package_dir, .. } => {
            if candidate.is_absolute() {
                Ok(normalize_path(&candidate))
            } else {
                resolve_package_path_under_root(package_dir, raw, field_name)
            }
        }
    }
}

pub(crate) fn resolve_agent_runtime_with_context(
    json_value: &Value,
    context: PathResolutionContext<'_>,
) -> Result<AgentRuntimeConfig> {
    if json_value.pointer("/runtime/harness").is_some() {
        return Err(anyhow!(
            "runtime.harness is not supported; use runtime.agent_runtime"
        ));
    }
    let agent = json_value
        .pointer("/runtime/agent_runtime")
        .ok_or_else(|| anyhow!("runtime.agent_runtime is required"))?;
    if agent.pointer("/io").is_some()
        || agent.pointer("/execution").is_some()
        || agent.pointer("/workspace_patches").is_some()
        || agent.pointer("/launch").is_some()
        || agent.pointer("/env_from_host").is_some()
        || agent.pointer("/binding_args").is_some()
        || agent.pointer("/support_files").is_some()
    {
        return Err(anyhow!(
            "runtime.agent_runtime hard cut: use runtime.agent_runtime.{{artifact,image,command,env,network}}"
        ));
    }
    for (pointer, message) in [
        (
            "/runtime/dependencies/file_staging",
            "runtime.dependencies.file_staging is not supported; package files in the agent artifact or task rows",
        ),
        (
            "/runtime/dependencies/assets",
            "runtime.dependencies.assets is not supported; task-owned inputs must be embedded in task rows",
        ),
        (
            "/runtime/dependencies/secret_files",
            "runtime.dependencies.secret_files is not supported; inject secrets at launch time instead of authored host paths",
        ),
        (
            "/benchmark/grader/support_files",
            "benchmark.grader.support_files is not supported; reference grader files directly in benchmark.grader.command or use runner-owned built-ins",
        ),
        (
            "/benchmark/adapter/support_files",
            "benchmark.adapter.support_files is not supported; benchmark assets must be runner-owned sealed assets",
        ),
    ] {
        if json_value.pointer(pointer).is_some() {
            return Err(anyhow!("{}", message));
        }
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

    let execution_image =
        parse_optional_nonempty_string(agent.pointer("/image"), "runtime.agent_runtime.image")?
            .ok_or_else(|| anyhow!("runtime.agent_runtime.image is required"))?;
    let execution_network = agent
        .pointer("/network")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("none")
        .to_string();
    #[cfg(test)]
    let execution_network_for_test = execution_network.clone();
    let artifact_raw = parse_optional_nonempty_string(
        agent.pointer("/artifact"),
        "runtime.agent_runtime.artifact",
    )?
    .ok_or_else(|| anyhow!("runtime.agent_runtime.artifact is required"))?;
    let agent_artifact = resolve_agent_artifact_path_for_context(
        &artifact_raw,
        "runtime.agent_runtime.artifact",
        &context,
    )?;
    let agent_artifact_digest = parse_optional_nonempty_string(
        agent.pointer("/artifact_digest"),
        "runtime.agent_runtime.artifact_digest",
    )?;
    let agent_artifact_resolved_path = parse_optional_nonempty_string(
        agent.pointer("/artifact_resolved_path"),
        "runtime.agent_runtime.artifact_resolved_path",
    )?
    .map(|raw| {
        resolve_runtime_source_path_for_context(
            &raw,
            "runtime.agent_runtime.artifact_resolved_path",
            &context,
        )
    })
    .transpose()?;

    let command = parse_command_field(agent.pointer("/command"), "runtime.agent_runtime.command")?
        .ok_or_else(|| anyhow!("runtime.agent_runtime.command is required"))?;
    let integration_level = agent
        .pointer("/integration_level")
        .and_then(|v| v.as_str())
        .unwrap_or("cli_basic")
        .to_string();
    let adapter_ref = AgentAdapterRef::default();
    let env = parse_string_map_field(agent.pointer("/env"), "runtime.agent_runtime.env")?;
    let allow_internal_contract_paths = matches!(context, PathResolutionContext::Run { .. });
    for (key, value) in &env {
        if contains_removed_runtime_template(value) {
            return Err(anyhow!(
                "runtime.agent_runtime.env.{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                key
            ));
        }
        if !allow_internal_contract_paths && value.trim().starts_with("/agentlab/") {
            return Err(anyhow!(
                "runtime.agent_runtime.env.{} leaks runner topology; remove internal /agentlab paths from public authoring",
                key
            ));
        }
    }
    for (idx, token) in command.iter().enumerate() {
        if contains_removed_runtime_template(token) {
            return Err(anyhow!(
                "runtime.agent_runtime.command[{}] uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                idx
            ));
        }
        if !allow_internal_contract_paths && token.trim().starts_with("/agentlab/") {
            return Err(anyhow!(
                "runtime.agent_runtime.command[{}] leaks runner topology; remove internal /agentlab paths from public authoring",
                idx
            ));
        }
    }
    if agent
        .pointer("/secret_env")
        .map(|value| !value.is_null())
        .unwrap_or(false)
    {
        return Err(anyhow!(
            "runtime.agent_runtime.secret_env is not supported; use $NAME runtime bindings in runtime.agent_runtime.command or runtime.agent_runtime.env"
        ));
    }
    let env_from_host = Vec::new();
    let dependency_file_staging = match &context {
        PathResolutionContext::Build { exp_dir, .. } => {
            derive_public_path_staging_specs(&command, &env, exp_dir)?
        }
        PathResolutionContext::Run {
            package_dir,
            variant_id,
        } => {
            reject_packaged_public_path_references(&command, &env, package_dir)?;
            load_staging_specs_from_package(package_dir, variant_id)?
        }
    };

    Ok(AgentRuntimeConfig {
        adapter_ref,
        command_raw: command,
        image: execution_image,
        network: execution_network,
        agent_artifact,
        agent_artifact_digest,
        agent_artifact_resolved_path,
        integration_level,
        env,
        env_from_host,
        trajectory_path,
        causal_extraction,
        #[cfg(test)]
        sandbox_image: None,
        #[cfg(test)]
        image_source: ImageSource::PerTask,
        #[cfg(test)]
        execution: AgentExecutionConfig {
            executor: Some(AgentExecutionExecutor::Docker),
            image: Some(
                agent
                    .pointer("/image")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            ),
            network: execution_network_for_test,
        },
        #[cfg(test)]
        io: AgentRuntimeIoConfig {
            input_arg: "--input".to_string(),
            output_arg: "--output".to_string(),
        },
        #[cfg(test)]
        launch_mode: AgentLaunchMode::File,
        #[cfg(test)]
        workspace_patches: Vec::new(),
        #[cfg(test)]
        default_timeout_ms: None,
        #[cfg(test)]
        tracing_mode: None,
        #[cfg(test)]
        force_container: true,
        dependency_file_staging,
        #[cfg(test)]
        dependency_services: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// Runtime environment
// ---------------------------------------------------------------------------

pub(crate) fn parse_runtime_env_file(path: &Path) -> Result<BTreeMap<String, String>> {
    let content = fs::read_to_string(path)
        .map_err(|err| anyhow!("failed to read env file {}: {}", path.display(), err))?;
    let mut values = BTreeMap::new();
    for (line_no, raw_line) in content.lines().enumerate() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let body = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((raw_key, raw_value)) = body.split_once('=') else {
            return Err(anyhow!(
                "invalid env file {}:{} (expected KEY=VALUE)",
                path.display(),
                line_no + 1
            ));
        };
        let key = raw_key.trim();
        if key.is_empty() {
            return Err(anyhow!(
                "invalid env file {}:{} (empty key)",
                path.display(),
                line_no + 1
            ));
        }
        let mut value = raw_value.trim().to_string();
        if (value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\''))
        {
            value = value[1..value.len() - 1].to_string();
        }
        values.insert(key.to_string(), value);
    }
    Ok(values)
}

pub(crate) fn resolve_runtime_env_inputs(
    execution: &RunExecutionOptions,
) -> Result<BTreeMap<String, String>> {
    let mut resolved = BTreeMap::new();
    let cwd =
        std::env::current_dir().map_err(|err| anyhow!("failed to resolve current dir: {}", err))?;
    for raw_path in &execution.runtime_env_files {
        let path = if raw_path.is_absolute() {
            raw_path.clone()
        } else {
            cwd.join(raw_path)
        };
        let file_values = parse_runtime_env_file(&path)?;
        for (key, value) in file_values {
            resolved.insert(key, value);
        }
    }
    for (key, value) in &execution.runtime_env {
        resolved.insert(key.clone(), value.clone());
    }
    Ok(resolved)
}

pub(crate) fn resolve_agent_runtime_env(
    runtime_agent: &AgentRuntimeConfig,
    bindings: &Value,
    runtime_env_inputs: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    resolve_env_templates(
        &runtime_agent.env,
        bindings,
        runtime_env_inputs,
        "runtime.agent_runtime.env",
    )
}

pub(crate) fn ensure_required_runtime_env_present(
    runtime_agent: &AgentRuntimeConfig,
    resolved_env: &BTreeMap<String, String>,
) -> Result<()> {
    for key in &runtime_agent.env_from_host {
        if !resolved_env.contains_key(key) {
            return Err(anyhow!(
                "missing required runtime env var for runtime.agent_runtime.env_from_host: {} (provide via host env, --env, or --env-file)",
                key
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_agent_artifact_pin(runtime_agent: &AgentRuntimeConfig) -> Result<()> {
    let artifact = &runtime_agent.agent_artifact;
    if let Some(expected_path) = runtime_agent.agent_artifact_resolved_path.as_ref() {
        let normalized = normalize_path(artifact);
        let expected = normalize_path(expected_path);
        if normalized != expected {
            return Err(anyhow!(
                "runtime.agent_runtime.artifact path mismatch: expected {}, got {}",
                expected.display(),
                normalized.display()
            ));
        }
    }
    if let Some(expected_digest) = runtime_agent.agent_artifact_digest.as_ref() {
        let expected = expected_digest
            .trim()
            .strip_prefix("sha256:")
            .unwrap_or(expected_digest);
        let actual_full = compute_artifact_content_digest(artifact)?;
        let actual = actual_full
            .trim()
            .strip_prefix("sha256:")
            .unwrap_or(actual_full.as_str());
        if !expected.eq_ignore_ascii_case(actual) {
            return Err(anyhow!(
                "runtime.agent_runtime.artifact digest mismatch: expected sha256:{}, got sha256:{}",
                expected,
                actual
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark runtime assets
// ---------------------------------------------------------------------------

pub(crate) fn resolve_benchmark_runtime_assets(
    experiment: &Value,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<Vec<DependencyFileStagingSpec>> {
    let mut support_files = derive_public_command_path_staging_specs(
        &parse_string_array_field(
            experiment.pointer("/benchmark/grader/command"),
            "benchmark.grader.command",
        )?,
        exp_dir,
        "benchmark.grader.command",
    )?;
    merge_dependency_file_staging(
        &mut support_files,
        derive_public_command_path_staging_specs(
            &parse_string_array_field(
                experiment.pointer("/benchmark/adapter/command"),
                "benchmark.adapter.command",
            )?,
            exp_dir,
            "benchmark.adapter.command",
        )?,
    );
    merge_dependency_file_staging(
        &mut support_files,
        parse_build_runtime_asset_specs(
            experiment.pointer("/benchmark/grader/_runtime_assets"),
            "benchmark.grader._runtime_assets",
            exp_dir,
            project_root,
        )?,
    );
    merge_dependency_file_staging(
        &mut support_files,
        parse_build_runtime_asset_specs(
            experiment.pointer("/benchmark/adapter/_runtime_assets"),
            "benchmark.adapter._runtime_assets",
            exp_dir,
            project_root,
        )?,
    );
    if let Some(mapper) = experiment
        .pointer("/benchmark/grader/conclusion/mapper")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(rel) = resolve_existing_public_path_reference(
            mapper,
            exp_dir,
            "benchmark.grader.conclusion.mapper",
        )? {
            let source = normalize_path(&exp_dir.join(&rel));
            fs::metadata(&source).with_context(|| {
                format!(
                    "failed to read benchmark.grader.conclusion.mapper public path reference '{}' resolved to '{}'",
                    mapper,
                    source.display()
                )
            })?;
            merge_dependency_file_staging(
                &mut support_files,
                vec![DependencyFileStagingSpec {
                    source_from_host: source,
                    destination_path: task_workdir_support_destination_path(
                        &rel.to_string_lossy().replace('\\', "/"),
                    ),
                    required: true,
                    read_only: true,
                }],
            );
        }
    }
    Ok(support_files)
}

// ---------------------------------------------------------------------------
// Variant profile
// ---------------------------------------------------------------------------

pub(crate) fn preview_agent_command(profile: &VariantRuntimeProfile) -> Vec<String> {
    let mut command = profile.agent_runtime.command_raw.clone();
    command.extend(profile.variant_args.iter().cloned());
    command
}

// TODO: Remove when workspace concept is eliminated.
pub(crate) fn value_contains_host_scratch_path(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.contains("/.lab/runs/") || trimmed.contains("/.scratch/")
}

pub(crate) fn profile_is_hermetic(profile: &VariantRuntimeProfile) -> bool {
    let command = preview_agent_command(profile);
    profile.agent_runtime.image.trim().is_empty() == false
        && command_contains_scientific_bypass(&command).is_none()
        && !command
            .iter()
            .any(|value| value_contains_host_scratch_path(value))
        && !profile
            .agent_runtime_env
            .values()
            .any(|value| value_contains_host_scratch_path(value))
}

pub(crate) fn resolve_run_isolation_grade(
    variant_runtime_profiles: &[VariantRuntimeProfile],
    _behavior: &RunBehavior,
) -> &'static str {
    if !variant_runtime_profiles.is_empty()
        && variant_runtime_profiles.iter().all(profile_is_hermetic)
    {
        return "hermetic";
    }
    "invalid"
}

pub(crate) fn resolve_variant_runtime_profile_with_context(
    experiment: &Value,
    variant: &Variant,
    context: PathResolutionContext<'_>,
    behavior: &RunBehavior,
    execution: &RunExecutionOptions,
) -> Result<VariantRuntimeProfile> {
    let variant_experiment = resolve_runtime_for_variant(experiment, variant)?;
    validate_required_fields(&variant_experiment)?;

    let mut agent_runtime = match context {
        PathResolutionContext::Build {
            exp_dir,
            project_root,
        } => resolve_agent_runtime(&variant_experiment, exp_dir, project_root)?,
        PathResolutionContext::Run { package_dir, .. } => {
            resolve_packaged_agent_runtime(&variant_experiment, package_dir, &variant.id)?
        }
    };
    let validate_root = match context {
        PathResolutionContext::Build { exp_dir, .. } => exp_dir,
        PathResolutionContext::Run { package_dir, .. } => package_dir,
    };
    if let PathResolutionContext::Build {
        exp_dir,
        project_root,
    } = context
    {
        merge_dependency_file_staging(
            &mut agent_runtime.dependency_file_staging,
            resolve_benchmark_runtime_assets(&variant_experiment, exp_dir, project_root)?,
        );
    }
    validate_agent_artifact_pin(&agent_runtime)?;
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

    let runtime_env_inputs = resolve_runtime_env_inputs(execution)?;
    agent_runtime.command_raw = resolve_agent_runtime_command(
        &agent_runtime.command_raw,
        &variant.bindings,
        &runtime_env_inputs,
    )?;
    validate_agent_runtime_command(&agent_runtime.command_raw, validate_root)?;
    let mut agent_runtime_env =
        resolve_agent_runtime_env(&agent_runtime, &variant.bindings, &runtime_env_inputs)?;
    let resolved_variant_env = resolve_env_templates(
        &variant.env,
        &variant.bindings,
        &runtime_env_inputs,
        "variant.env",
    )?;
    for (key, value) in resolved_variant_env {
        agent_runtime_env.insert(key, value);
    }
    let variant_args =
        resolve_command_templates(&variant.args, &variant.bindings, &runtime_env_inputs)?;

    Ok(VariantRuntimeProfile {
        experiment: variant_experiment,
        variant_args,
        agent_runtime,
        agent_runtime_env,
        invocation_source: "runtime_agent".to_string(),
        configured_network_mode,
        effective_network_mode,
    })
}

pub(crate) fn resolve_variant_runtime_profile(
    experiment: &Value,
    variant: &Variant,
    root_dir: &Path,
    behavior: &RunBehavior,
    execution: &RunExecutionOptions,
) -> Result<VariantRuntimeProfile> {
    let context = if root_dir.join(STAGING_MANIFEST_FILE).is_file() {
        PathResolutionContext::Run {
            package_dir: root_dir,
            variant_id: &variant.id,
        }
    } else {
        PathResolutionContext::Build {
            exp_dir: root_dir,
            project_root: root_dir,
        }
    };
    resolve_variant_runtime_profile_with_context(experiment, variant, context, behavior, execution)
}

// ---------------------------------------------------------------------------
// Agent runtime command helpers
// ---------------------------------------------------------------------------

pub(crate) fn resolve_agent_runtime_command(
    command: &[String],
    bindings: &Value,
    runtime_env_inputs: &BTreeMap<String, String>,
) -> Result<Vec<String>> {
    resolve_command_templates(command, bindings, runtime_env_inputs)
}

pub(crate) fn validate_agent_runtime_command(
    command: &[String],
    _project_root: &Path,
) -> Result<()> {
    if command.is_empty() {
        return Err(anyhow!("runtime.agent_runtime.command must not be empty"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Digest / path helpers
// ---------------------------------------------------------------------------

pub(crate) fn command_part_looks_like_path(part: &str) -> bool {
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

pub(crate) fn resolve_command_digest_target(command: &[String]) -> Option<&str> {
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

pub(crate) fn resolve_exec_digest(command: &[String], exp_dir: &Path) -> Result<String> {
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

// ---------------------------------------------------------------------------
// Package staging helpers (used by both build and run contexts)
// ---------------------------------------------------------------------------

pub(crate) fn reject_packaged_public_path_references(
    command: &[String],
    env: &BTreeMap<String, String>,
    package_dir: &Path,
) -> Result<()> {
    for (idx, token) in command.iter().enumerate() {
        if idx == 0 {
            continue;
        }
        let field = format!("runtime.agent_runtime.command[{}]", idx);
        if let Some(rel) = resolve_existing_public_path_reference(token, package_dir, &field)? {
            return Err(anyhow!(
                "{} still contains unresolved package-relative path '{}'; rebuild the sealed package with the build-time runtime path cutover (resolved path: {})",
                field,
                token,
                rel.display()
            ));
        }
    }
    for (key, value) in env {
        let field = format!("runtime.agent_runtime.env.{}", key);
        if let Some(rel) = resolve_existing_public_path_reference(value, package_dir, &field)? {
            return Err(anyhow!(
                "{} still contains unresolved package-relative path '{}'; rebuild the sealed package with the build-time runtime path cutover (resolved path: {})",
                field,
                value,
                rel.display()
            ));
        }
    }
    Ok(())
}

pub(crate) fn load_staging_specs_from_package(
    package_dir: &Path,
    variant_id: &str,
) -> Result<Vec<DependencyFileStagingSpec>> {
    let manifest_path =
        resolve_package_path_under_root(package_dir, STAGING_MANIFEST_FILE, STAGING_MANIFEST_FILE)?;
    let manifest_bytes = fs::read(&manifest_path).with_context(|| {
        format!(
            "failed to read runtime staging manifest at {}",
            manifest_path.display()
        )
    })?;
    let manifest: RuntimePathStagingManifest = serde_json::from_slice(&manifest_bytes)
        .with_context(|| {
            format!(
                "failed to parse runtime staging manifest JSON at {}",
                manifest_path.display()
            )
        })?;
    if manifest.schema_version != STAGING_MANIFEST_SCHEMA_VERSION {
        return Err(anyhow!(
            "runtime staging manifest schema_version must be '{}' (found '{}')",
            STAGING_MANIFEST_SCHEMA_VERSION,
            manifest.schema_version
        ));
    }
    let entries = manifest.variants.get(variant_id).ok_or_else(|| {
        anyhow!(
            "runtime staging manifest missing entries for variant '{}' in {}",
            variant_id,
            manifest_path.display()
        )
    })?;
    let mut specs = Vec::with_capacity(entries.len());
    for (idx, entry) in entries.iter().enumerate() {
        let source_from_host = resolve_package_path_under_root(
            package_dir,
            &entry.packaged_path,
            &format!(
                "staging_manifest.variants.{}[{}].packaged_path",
                variant_id, idx
            ),
        )?;
        fs::metadata(&source_from_host).with_context(|| {
            format!(
                "failed to read packaged runtime staging source '{}' for staging_manifest.variants.{}[{}]",
                source_from_host.display(),
                variant_id,
                idx
            )
        })?;
        specs.push(DependencyFileStagingSpec {
            source_from_host,
            destination_path: validate_runner_staged_destination_path(
                &entry.runtime_path,
                &format!(
                    "staging_manifest.variants.{}[{}].runtime_path",
                    variant_id, idx
                ),
            )?,
            required: entry.required,
            read_only: entry.read_only,
        });
    }
    Ok(specs)
}

pub(crate) fn derive_public_command_path_staging_specs(
    command: &[String],
    exp_dir: &Path,
    field_name: &str,
) -> Result<Vec<DependencyFileStagingSpec>> {
    let mut specs = Vec::new();
    let mut seen = HashSet::new();
    for (idx, token) in command.iter().enumerate() {
        if idx == 0 {
            continue;
        }
        let Some(rel) = resolve_existing_public_path_reference(
            token,
            exp_dir,
            &format!("{}[{}]", field_name, idx),
        )?
        else {
            continue;
        };
        let key = rel.to_string_lossy().to_string();
        if !seen.insert(key.clone()) {
            continue;
        }
        let source = normalize_path(&exp_dir.join(&rel));
        fs::metadata(&source).with_context(|| {
            format!(
                "failed to read {}[{}] public path reference '{}' resolved to '{}'",
                field_name,
                idx,
                token,
                source.display()
            )
        })?;
        specs.push(DependencyFileStagingSpec {
            source_from_host: source,
            destination_path: task_workdir_support_destination_path(&key.replace('\\', "/")),
            required: true,
            read_only: true,
        });
    }
    Ok(specs)
}

pub(crate) fn derive_public_path_staging_specs(
    command: &[String],
    env: &BTreeMap<String, String>,
    exp_dir: &Path,
) -> Result<Vec<DependencyFileStagingSpec>> {
    let mut specs = derive_public_command_path_staging_specs(
        command,
        exp_dir,
        "runtime.agent_runtime.command",
    )?;
    let mut seen = HashSet::new();
    for spec in &specs {
        if let Some(rel) = strip_task_workdir_support_destination_path(&spec.destination_path) {
            seen.insert(rel.to_string());
        }
    }
    for (key_name, value) in env {
        let Some(rel) = resolve_existing_public_path_reference(
            value,
            exp_dir,
            &format!("runtime.agent_runtime.env.{}", key_name),
        )?
        else {
            continue;
        };
        let key = rel.to_string_lossy().to_string();
        if !seen.insert(key.clone()) {
            continue;
        }
        let source = normalize_path(&exp_dir.join(&rel));
        fs::metadata(&source).with_context(|| {
            format!(
                "failed to read runtime.agent_runtime.env.{} public path reference '{}' resolved to '{}'",
                key_name,
                value,
                source.display()
            )
        })?;
        specs.push(DependencyFileStagingSpec {
            source_from_host: source,
            destination_path: task_workdir_support_destination_path(&key.replace('\\', "/")),
            required: true,
            read_only: true,
        });
    }
    Ok(specs)
}

pub(crate) fn normalize_staged_support_source_path(
    raw: &str,
    exp_dir: &Path,
    project_root: &Path,
    field_name: &str,
) -> Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} must not be empty", field_name));
    }
    let candidate = PathBuf::from(trimmed);
    let resolved = if candidate.is_absolute() {
        normalize_path(&candidate)
    } else {
        normalize_path(&exp_dir.join(candidate))
    };
    let root_cmp = canonicalize_best_effort(project_root);
    let resolved_cmp = canonicalize_best_effort(&resolved);
    if !resolved_cmp.starts_with(&root_cmp) {
        return Err(anyhow!(
            "{} resolves outside project root: {}",
            field_name,
            resolved.display()
        ));
    }
    fs::metadata(&resolved).with_context(|| {
        format!(
            "failed to read {} source path '{}'",
            field_name,
            resolved.display()
        )
    })?;
    Ok(resolved)
}

pub(crate) fn parse_build_runtime_asset_specs(
    value: Option<&Value>,
    field_name: &str,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<Vec<DependencyFileStagingSpec>> {
    let Some(raw) = value else {
        return Ok(Vec::new());
    };
    let items = raw
        .as_array()
        .ok_or_else(|| anyhow!("{} must be an array", field_name))?;
    let mut specs = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| anyhow!("{}[{}] must be an object", field_name, idx))?;
        let source_from_host = obj
            .get("build_source_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].build_source_path is required", field_name, idx))?;
        let destination_path = obj
            .get("runtime_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].runtime_path is required", field_name, idx))?;
        let required = obj.get("required").and_then(Value::as_bool).unwrap_or(true);
        let read_only = obj
            .get("read_only")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        specs.push(DependencyFileStagingSpec {
            source_from_host: normalize_staged_support_source_path(
                source_from_host,
                exp_dir,
                project_root,
                &format!("{}[{}].build_source_path", field_name, idx),
            )?,
            destination_path: validate_runner_staged_destination_path(
                destination_path,
                &format!("{}[{}].runtime_path", field_name, idx),
            )?,
            required,
            read_only,
        });
    }
    Ok(specs)
}

pub(crate) fn merge_dependency_file_staging(
    base: &mut Vec<DependencyFileStagingSpec>,
    extra: Vec<DependencyFileStagingSpec>,
) {
    for next in extra {
        if let Some(existing) = base
            .iter_mut()
            .find(|entry| entry.destination_path == next.destination_path)
        {
            *existing = next;
        } else {
            base.push(next);
        }
    }
}

// ---------------------------------------------------------------------------
// Scientific bypass detection (used by profile_is_hermetic)
// ---------------------------------------------------------------------------

pub(crate) fn command_contains_scientific_bypass(command: &[String]) -> Option<String> {
    for token in command {
        let trimmed = token.trim();
        if trimmed == "--dangerous" || trimmed.contains("dangerous_mode") {
            return Some(trimmed.to_string());
        }
        for fragment in trimmed.split_whitespace() {
            if fragment == "--dangerous" || fragment.contains("dangerous_mode") {
                return Some(fragment.to_string());
            }
        }
    }
    None
}
