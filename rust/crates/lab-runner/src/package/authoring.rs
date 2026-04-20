use anyhow::{anyhow, Context, Result};
use lab_core::{sha256_bytes, sha256_file, AGENTLAB_TASK_WORKDIR_PLACEHOLDER};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use crate::config::*;
use crate::model::*;
use crate::package::staging::task_workdir_support_destination_path;

pub(crate) fn load_authoring_input_for_build(
    path: &Path,
    overrides_path: Option<&Path>,
) -> Result<LoadedExperimentInput> {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if canonical.is_dir() {
        return Err(anyhow!(
            "build_input_invalid_kind: expected authoring spec file, got directory '{}'",
            canonical.display()
        ));
    }

    if canonical
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "manifest.json")
    {
        return Err(anyhow!(
            "build_input_invalid_kind: expected authoring spec file, got sealed package manifest"
        ));
    }

    let exp_dir = canonical
        .parent()
        .unwrap_or(Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));
    let project_root = find_project_root(&exp_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&exp_dir));
    let raw_yaml = fs::read_to_string(&canonical)?;
    let yaml_value: serde_yaml::Value = serde_yaml::from_str(&raw_yaml)?;
    let mut json_value: Value = serde_json::to_value(yaml_value)?;
    if let Some(overrides_path) = overrides_path {
        json_value = apply_experiment_overrides(json_value, overrides_path, &project_root)?;
    }
    json_value = normalize_experiment_authoring(json_value, &exp_dir, &project_root)?;
    Ok(LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root,
    })
}

pub(crate) fn is_dx_contract_authoring(json_value: &Value) -> bool {
    json_value.pointer("/agent").is_some()
        || json_value.pointer("/overrides").is_some()
        || json_value.pointer("/baseline/id").is_some()
        || matches!(json_value.pointer("/benchmark"), Some(Value::String(_)))
        || json_value.pointer("/variants").is_some()
}

fn resolve_default_owner() -> String {
    let owner_from_git = Command::new("git")
        .args(["config", "--get", "user.name"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty());
    owner_from_git
        .or_else(|| {
            std::env::var("USER")
                .ok()
                .map(|user| user.trim().to_string())
        })
        .filter(|owner| !owner.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn tokenize_command_string(raw: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    for ch in raw.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if in_single {
            if ch == '\'' {
                in_single = false;
            } else {
                current.push(ch);
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                in_double = false;
            } else if ch == '\\' {
                escaped = true;
            } else {
                current.push(ch);
            }
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '\'' => in_single = true,
            '"' => in_double = true,
            c if c.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if escaped || in_single || in_double {
        return Err(anyhow!("agent.command has unclosed quote/escape"));
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    if tokens.is_empty() {
        return Err(anyhow!("agent.command must not be empty"));
    }
    Ok(tokens)
}

fn parse_dx_command_field_named(value: Option<&Value>, field: &str) -> Result<Vec<String>> {
    match value {
        Some(Value::String(raw)) => tokenize_command_string(raw),
        Some(Value::Array(_)) => {
            let parts = parse_string_array_field(value, field)?;
            if parts.is_empty() {
                return Err(anyhow!("{} must not be empty", field));
            }
            Ok(parts)
        }
        Some(_) => Err(anyhow!("{} must be a string or string[]", field)),
        None => Err(anyhow!("{} is required", field)),
    }
}

pub(crate) fn resolve_dx_artifact_path(raw: &str, exp_dir: &Path, project_root: &Path) -> PathBuf {
    let trimmed = raw.trim();
    let candidate = Path::new(trimmed);
    if candidate.is_absolute() {
        return normalize_path(candidate);
    }
    if trimmed.starts_with("./") || trimmed.starts_with("../") || trimmed.contains('/') {
        return normalize_path(&exp_dir.join(candidate));
    }

    let agents_root = project_root.join(".lab").join("agents");
    let direct = agents_root.join(trimmed);
    if direct.exists() {
        return normalize_path(&direct);
    }
    for ext in [".tar.gz", ".tgz", ".tar"] {
        let with_ext = agents_root.join(format!("{}{}", trimmed, ext));
        if with_ext.exists() {
            return normalize_path(&with_ext);
        }
    }
    normalize_path(&direct)
}

pub(crate) fn compute_artifact_content_digest(path: &Path) -> Result<String> {
    if path.is_file() {
        return sha256_file(path);
    }
    if !path.is_dir() {
        return Err(anyhow!(
            "artifact path must be a file or directory: {}",
            path.display()
        ));
    }

    let mut lines = Vec::new();
    for entry in walkdir::WalkDir::new(path)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let p = entry.path();
        if p == path {
            continue;
        }
        let rel = p
            .strip_prefix(path)
            .unwrap_or(p)
            .to_string_lossy()
            .replace('\\', "/");
        let meta = fs::symlink_metadata(p)?;
        if meta.file_type().is_symlink() {
            let target = fs::read_link(p)
                .map(|v| v.to_string_lossy().to_string())
                .unwrap_or_else(|_| "<unreadable>".to_string());
            lines.push(format!("L {} -> {}", rel, target));
        } else if meta.is_dir() {
            lines.push(format!("D {}", rel));
        } else if meta.is_file() {
            lines.push(format!("F {} {}", rel, sha256_file(p)?));
        }
    }
    lines.sort();
    Ok(sha256_bytes(lines.join("\n").as_bytes()))
}

#[derive(Debug, Clone)]
struct DxResolvedAgentBuild {
    artifact_raw: String,
    artifact_path: PathBuf,
    artifact_digest: String,
    image: String,
    command_base: Vec<String>,
    command: Vec<String>,
    env_base: BTreeMap<String, String>,
    env: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct DxVariantSpec {
    id: String,
    baseline: bool,
    agent_ref: String,
    config: Value,
    env: BTreeMap<String, String>,
}

pub(crate) fn uses_new_variant_agent_model(json_value: &Value) -> bool {
    if matches!(json_value.pointer("/agent_builds"), Some(Value::Array(_))) {
        return true;
    }
    let Some(Value::Array(variants)) = json_value.pointer("/variants") else {
        return false;
    };
    variants.iter().any(|variant| {
        variant.get("agent_ref").is_some()
            || variant.get("config").is_some()
            || variant.get("baseline").is_some()
    })
}

fn reject_removed_dx_agent_fields(root: &Value, root_name: &str) -> Result<()> {
    let removed = [
        ("arg_map", "put public argv directly in agent.command using $binding placeholders"),
        (
            "bindings_to_args",
            "put public argv directly in agent.command using $binding placeholders",
        ),
        (
            "default_config",
            "package agent config inside the agent artifact; authored override file wiring is not supported",
        ),
        (
            "config_files",
            "package agent config inside the agent artifact; authored host-path staging is not supported",
        ),
        ("provider_env", "bind runtime values directly with $NAME in agent.command or agent.env"),
        (
            "support_files",
            "package support files inside the agent artifact; authored host-path staging is not supported",
        ),
        ("env_from_host", "bind runtime values directly with $NAME in agent.command or agent.env"),
    ];
    for (field, guidance) in removed {
        if root.get(field).is_some() {
            return Err(anyhow!(
                "{}.{} was removed in the hard cutover; {}",
                root_name,
                field,
                guidance
            ));
        }
    }
    Ok(())
}

pub(crate) fn contains_removed_runtime_template(raw: &str) -> bool {
    raw.contains("${")
}

pub(crate) fn resolve_existing_public_path_reference(
    raw: &str,
    exp_dir: &Path,
    field_name: &str,
) -> Result<Option<PathBuf>> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.starts_with('/')
        || trimmed.starts_with('-')
        || trimmed.starts_with(AGENTLAB_TASK_WORKDIR_PLACEHOLDER)
        || trimmed.contains('$')
        || trimmed.contains("://")
    {
        return Ok(None);
    }
    let rel = validate_dx_support_file_relpath(trimmed, field_name)?;
    let resolved = normalize_path(&exp_dir.join(&rel));
    match fs::metadata(&resolved) {
        Ok(_) => Ok(Some(PathBuf::from(rel))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if trimmed.starts_with("./") || trimmed.contains('/') {
                return Err(anyhow!(
                    "{} public path '{}' resolved to missing source '{}'",
                    field_name,
                    trimmed,
                    resolved.display()
                ));
            }
            Ok(None)
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "failed to read {} public path reference '{}' resolved to '{}'",
                field_name,
                trimmed,
                resolved.display()
            )
        }),
    }
}

fn validate_dx_command_and_env_surface(
    command: &[String],
    env: &BTreeMap<String, String>,
    root_name: &str,
    exp_dir: &Path,
) -> Result<()> {
    for (idx, token) in command.iter().enumerate() {
        let field = format!("{}.command[{}]", root_name, idx);
        if contains_removed_runtime_template(token) {
            return Err(anyhow!(
                "{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                field
            ));
        }
        if token.trim().starts_with("/agentlab/") {
            return Err(anyhow!(
                "{} leaks runner topology; remove internal /agentlab paths from public authoring",
                field
            ));
        }
        if idx > 0 {
            let _ = resolve_existing_public_path_reference(token, exp_dir, &field)?;
        }
    }
    for (key, value) in env {
        let field = format!("{}.env.{}", root_name, key);
        if contains_removed_runtime_template(value) {
            return Err(anyhow!(
                "{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                field
            ));
        }
        if value.trim().starts_with("/agentlab/") {
            return Err(anyhow!(
                "{} leaks runner topology; remove internal /agentlab paths from public authoring",
                field
            ));
        }
        let _ = resolve_existing_public_path_reference(value, exp_dir, &field)?;
    }
    Ok(())
}

pub(crate) fn validate_dx_support_file_relpath(raw: &str, field_name: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} must not be empty", field_name));
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err(anyhow!("{} must be relative", field_name));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(seg) => normalized.push(seg),
            Component::ParentDir => {
                return Err(anyhow!("{} cannot contain '..'", field_name));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(anyhow!("{} must be relative", field_name));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(anyhow!("{} cannot resolve to empty", field_name));
    }
    Ok(normalized.to_string_lossy().replace('\\', "/"))
}

fn dx_runtime_asset_value(build_source_path: &Path, runtime_path: &str) -> Value {
    json!({
        "build_source_path": build_source_path.to_string_lossy().to_string(),
        "runtime_path": runtime_path,
        "required": true,
        "read_only": true
    })
}

fn parse_dx_agent_build(
    root: &Value,
    root_name: &str,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<DxResolvedAgentBuild> {
    reject_removed_dx_agent_fields(root, root_name)?;
    let artifact_raw = root
        .get("artifact")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("{}.artifact is required", root_name))?
        .to_string();
    let artifact_path = resolve_dx_artifact_path(&artifact_raw, exp_dir, project_root);
    fs::metadata(&artifact_path).with_context(|| {
        format!(
            "failed to read {}.artifact source path '{}' (artifact value '{}')",
            root_name,
            artifact_path.display(),
            artifact_raw
        )
    })?;
    let artifact_digest = compute_artifact_content_digest(&artifact_path)?;
    let command_base =
        parse_dx_command_field_named(root.get("command"), &format!("{}.command", root_name))?;
    let command = command_base.clone();
    let image = root
        .get("image")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("{}.image is required", root_name))?
        .to_string();
    let env_base = parse_string_map_field(root.get("env"), &format!("{}.env", root_name))?;
    let env = env_base.clone();
    validate_dx_command_and_env_surface(&command_base, &env_base, root_name, exp_dir)?;
    Ok(DxResolvedAgentBuild {
        artifact_raw,
        artifact_path,
        artifact_digest,
        image,
        command_base,
        command,
        env_base,
        env,
    })
}

fn runtime_override_for_variant_build(
    build: &DxResolvedAgentBuild,
    variant_env: &BTreeMap<String, String>,
) -> Value {
    let mut merged_env = build.env.clone();
    for (key, value) in variant_env {
        merged_env.insert(key.clone(), value.clone());
    }
    json!({
        "agent_runtime": {
            "command": build.command.clone(),
            "artifact": build.artifact_path.to_string_lossy().to_string(),
            "artifact_digest": build.artifact_digest.clone(),
            "artifact_resolved_path": build.artifact_path.to_string_lossy().to_string(),
            "image": build.image.clone(),
            "env": merged_env
        }
    })
}

pub(crate) fn builtin_benchmark_assets_root() -> Result<PathBuf> {
    let candidate = normalize_path(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.."));
    if candidate.join("bench").exists() && candidate.join("adapters").exists() {
        return Ok(candidate);
    }
    Err(anyhow!(
        "failed to resolve built-in benchmark assets root from {}",
        candidate.display()
    ))
}

pub(crate) fn rewrite_new_variant_agent_model(
    json_value: &Value,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<Value> {
    let mut rewritten = json_value.clone();
    let mut builds_by_id: BTreeMap<String, DxResolvedAgentBuild> = BTreeMap::new();

    if let Some(agent_builds) = json_value.pointer("/agent_builds") {
        let items = agent_builds
            .as_array()
            .ok_or_else(|| anyhow!("agent_builds must be an array"))?;
        if items.is_empty() {
            return Err(anyhow!("agent_builds must include at least one build"));
        }
        for (idx, item) in items.iter().enumerate() {
            let item_obj = item
                .as_object()
                .ok_or_else(|| anyhow!("agent_builds[{}] must be an object", idx))?;
            let id = item_obj
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| anyhow!("agent_builds[{}].id is required", idx))?
                .to_string();
            if builds_by_id.contains_key(&id) {
                return Err(anyhow!("agent_builds contains duplicate id '{}'", id));
            }
            let parsed = parse_dx_agent_build(
                item,
                &format!("agent_builds[{}]", idx),
                exp_dir,
                project_root,
            )?;
            builds_by_id.insert(id, parsed);
        }
    } else {
        let legacy_agent = json_value
            .pointer("/agent")
            .ok_or_else(|| anyhow!("agent_builds is required when agent section is missing"))?;
        let parsed = parse_dx_agent_build(legacy_agent, "agent", exp_dir, project_root)?;
        builds_by_id.insert("default".to_string(), parsed);
    }

    let variants = json_value
        .pointer("/variants")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("variants must be an array"))?;
    if variants.is_empty() {
        return Err(anyhow!("variants must include at least one entry"));
    }

    let default_build_ref = if builds_by_id.len() == 1 {
        builds_by_id.keys().next().cloned()
    } else {
        None
    };

    let mut parsed_variants = Vec::with_capacity(variants.len());
    for (idx, item) in variants.iter().enumerate() {
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("variants[{}].id is required", idx))?
            .to_string();
        let baseline = item
            .get("baseline")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let config = item
            .get("config")
            .or_else(|| item.get("bindings"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !config.is_object() {
            return Err(anyhow!("variants[{}].config must be an object", idx));
        }
        let env = parse_string_map_field(item.get("env"), &format!("variants[{}].env", idx))?;
        let agent_ref = item
            .get("agent_ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string)
            .or_else(|| default_build_ref.clone())
            .ok_or_else(|| {
                anyhow!(
                    "variants[{}].agent_ref is required when multiple agent_builds are declared",
                    idx
                )
            })?;
        if !builds_by_id.contains_key(&agent_ref) {
            return Err(anyhow!(
                "variants[{}].agent_ref '{}' does not match any agent_builds[].id",
                idx,
                agent_ref
            ));
        }
        parsed_variants.push(DxVariantSpec {
            id,
            baseline,
            agent_ref,
            config,
            env,
        });
    }

    let baseline_indices = parsed_variants
        .iter()
        .enumerate()
        .filter_map(|(idx, variant)| variant.baseline.then_some(idx))
        .collect::<Vec<_>>();
    let baseline_idx = if baseline_indices.len() == 1 {
        baseline_indices[0]
    } else if baseline_indices.is_empty() && parsed_variants.len() == 1 {
        0
    } else if baseline_indices.is_empty() {
        return Err(anyhow!(
            "exactly one variants[].baseline=true is required when more than one variant is declared"
        ));
    } else {
        return Err(anyhow!(
            "exactly one variants[].baseline=true is required (found {})",
            baseline_indices.len()
        ));
    };

    let baseline_variant = parsed_variants[baseline_idx].clone();
    let baseline_build = builds_by_id
        .get(&baseline_variant.agent_ref)
        .ok_or_else(|| anyhow!("internal error: baseline agent build missing"))?;

    let mut baseline_agent_env = baseline_build.env_base.clone();
    for (key, value) in &baseline_variant.env {
        baseline_agent_env.insert(key.clone(), value.clone());
    }
    let baseline_agent = json!({
        "artifact": baseline_build.artifact_raw.clone(),
        "image": baseline_build.image.clone(),
        "command": baseline_build.command_base.clone(),
        "env": baseline_agent_env,
    });
    set_json_pointer_value(&mut rewritten, "/agent", baseline_agent)?;
    set_json_pointer_value(
        &mut rewritten,
        "/baseline",
        json!({
            "id": baseline_variant.id,
            "bindings": baseline_variant.config,
        }),
    )?;

    let mut treatment_variants = Vec::new();
    for (idx, variant) in parsed_variants.iter().enumerate() {
        if idx == baseline_idx {
            continue;
        }
        let mut entry = json!({
            "id": variant.id,
            "bindings": variant.config,
            "agent_ref": variant.agent_ref,
        });
        let variant_build = builds_by_id
            .get(&variant.agent_ref)
            .ok_or_else(|| anyhow!("internal error: missing build for variant {}", variant.id))?;
        if variant.agent_ref != baseline_variant.agent_ref || !variant.env.is_empty() {
            set_json_pointer_value(
                &mut entry,
                "/runtime_overrides",
                runtime_override_for_variant_build(variant_build, &variant.env),
            )?;
        }
        treatment_variants.push(entry);
    }
    set_json_pointer_value(
        &mut rewritten,
        "/variants",
        Value::Array(treatment_variants),
    )?;
    if rewritten.pointer("/agent_builds").is_some() {
        set_json_pointer_value(&mut rewritten, "/agent_builds", Value::Null)?;
    }
    Ok(rewritten)
}

fn resolve_builtin_benchmark_dataset_path(
    json_value: &Value,
    builtin_benchmark: &str,
    project_root: &Path,
) -> Result<String> {
    if let Some(dataset) = json_value.pointer("/dataset") {
        require_exact_object_keys(dataset, &["path"], "dataset")?;
        let path = dataset
            .pointer("/path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("dataset.path must be a non-empty string"))?;
        return Ok(path.to_string());
    }
    let default_name = match builtin_benchmark {
        "bench_v0" => "bench_v0.task_spec.jsonl",
        "swebench_lite_curated" => "swebench_lite_curated.task_spec.jsonl",
        _ => unreachable!(),
    };
    Ok(project_root
        .join(".lab")
        .join("experiments")
        .join("data")
        .join(default_name)
        .to_string_lossy()
        .to_string())
}

pub(crate) fn normalize_experiment_authoring(
    json_value: Value,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<Value> {
    if !is_dx_contract_authoring(&json_value) {
        return Ok(json_value);
    }
    let mut json_value = json_value;
    if uses_new_variant_agent_model(&json_value) {
        json_value = rewrite_new_variant_agent_model(&json_value, exp_dir, project_root)?;
    }

    let experiment_id = json_value
        .pointer("/experiment/id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("experiment.id is required"))?
        .to_string();
    let experiment_name = json_value
        .pointer("/experiment/name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| experiment_id.clone());
    let experiment_description = json_value
        .pointer("/experiment/description")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    let experiment_tags =
        parse_string_array_field(json_value.pointer("/experiment/tags"), "experiment.tags")?;
    let owner = json_value
        .pointer("/experiment/owner")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(resolve_default_owner);

    let benchmark_name = json_value
        .pointer("/benchmark")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("benchmark is required and must be a non-empty string"))?;
    let builtin_benchmark = match benchmark_name {
        "bench_v0" => "bench_v0",
        "swebench_lite" | "swebench-lite" | "swebench_lite_curated" | "swebench-lite-curated" => {
            "swebench_lite_curated"
        }
        other => {
            return Err(anyhow!(
                "unknown benchmark '{}': supported built-ins are 'bench_v0' and 'swebench_lite_curated' (alias: 'swebench_lite')",
                other
            ));
        }
    };

    let baseline_id = json_value
        .pointer("/baseline/id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("baseline.id is required"))?
        .to_string();
    let baseline_bindings = json_value
        .pointer("/baseline/bindings")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !baseline_bindings.is_object() {
        return Err(anyhow!("baseline.bindings must be an object"));
    }

    let mut variant_plan = Vec::new();
    if let Some(items) = json_value.pointer("/variants") {
        let arr = items
            .as_array()
            .ok_or_else(|| anyhow!("variants must be an array"))?;
        for (idx, item) in arr.iter().enumerate() {
            let id = item
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| anyhow!("variants[{}].id is required", idx))?;
            let bindings = item.get("bindings").cloned().unwrap_or_else(|| json!({}));
            if !bindings.is_object() {
                return Err(anyhow!("variants[{}].bindings must be an object", idx));
            }
            let mut variant_entry = json!({
                "variant_id": id,
                "bindings": bindings
            });
            if let Some(runtime_overrides) = item.get("runtime_overrides") {
                if !runtime_overrides.is_object() {
                    return Err(anyhow!(
                        "variants[{}].runtime_overrides must be an object",
                        idx
                    ));
                }
                set_json_pointer_value(
                    &mut variant_entry,
                    "/runtime_overrides",
                    runtime_overrides.clone(),
                )?;
            }
            variant_plan.push(variant_entry);
        }
    }

    let has_variant_plan = !variant_plan.is_empty();
    let comparison = if has_variant_plan { "paired" } else { "none" };
    let scheduling = if has_variant_plan {
        "paired_interleaved"
    } else {
        "variant_sequential"
    };
    let builtin_assets_root = builtin_benchmark_assets_root()?;
    let dataset_path =
        resolve_builtin_benchmark_dataset_path(&json_value, builtin_benchmark, project_root)?;

    let agent_root = json_value
        .pointer("/agent")
        .ok_or_else(|| anyhow!("agent section is required"))?;
    let agent_build = parse_dx_agent_build(agent_root, "agent", exp_dir, project_root)?;
    let (
        dataset_suite_id,
        dataset_split_id,
        metrics,
        benchmark_policy,
        benchmark_grader_command,
        benchmark_grader_runtime_assets,
    ) = match builtin_benchmark {
        "bench_v0" => (
            "bench_v0",
            "test",
            json!([
                { "id": "duration_ms", "source": "runner", "weight": 0, "primary": false },
                { "id": "turn_count", "source": "runner", "weight": 0, "primary": false },
                { "id": "resolved", "source": "output", "json_pointer": "/metrics/resolved", "weight": 1, "direction": "maximize", "primary": true },
                { "id": "hidden_cases_passed", "source": "output", "json_pointer": "/metrics/hidden_cases_passed", "weight": 0, "primary": false },
                { "id": "hidden_cases_total", "source": "output", "json_pointer": "/metrics/hidden_cases_total", "weight": 0, "primary": false }
            ]),
            json!({
                "task_model": "independent",
                "evaluator_mode": "custom",
                "scoring_lifecycle": "predict_then_score",
                "chain_failure_policy": "continue_with_flag"
            }),
            json!([
                "python3",
                task_workdir_support_destination_path(
                    "bench/integration/agentlab/bench_benchmark_adapter.py"
                )
            ]),
            json!([dx_runtime_asset_value(
                &builtin_assets_root.join("bench"),
                &task_workdir_support_destination_path("bench")
            )]),
        ),
        "swebench_lite_curated" => (
            "swebench_lite_curated",
            "test",
            json!([
                { "id": "duration_ms", "source": "runner", "weight": 0, "primary": false },
                { "id": "turn_count", "source": "runner", "weight": 0, "primary": false },
                { "id": "success", "source": "output", "json_pointer": "/metrics/success", "weight": 1, "direction": "maximize", "primary": true }
            ]),
            json!({
                "task_model": "independent",
                "evaluator_mode": "custom",
                "scoring_lifecycle": "integrated_score",
                "chain_failure_policy": "continue_with_flag"
            }),
            json!([
                "python3",
                task_workdir_support_destination_path("swebench/swebench_task_container_grader.py")
            ]),
            json!([dx_runtime_asset_value(
                &builtin_assets_root.join("adapters").join("swebench"),
                &task_workdir_support_destination_path("swebench")
            )]),
        ),
        _ => unreachable!(),
    };

    let timeout_ms = json_value
        .pointer("/timeout_ms")
        .or_else(|| json_value.pointer("/agent/timeout_ms"))
        .and_then(Value::as_u64)
        .unwrap_or(600_000);
    let network_mode = json_value
        .pointer("/overrides/network")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("none")
        .to_string();
    if network_mode != "none" && network_mode != "full" && network_mode != "allowlist_enforced" {
        return Err(anyhow!(
            "overrides.network must be one of: none, full, allowlist_enforced (got '{}')",
            network_mode
        ));
    }
    let limit = json_value.pointer("/limit").and_then(Value::as_u64);
    let replications = json_value
        .pointer("/replications")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .max(1);
    let random_seed = json_value
        .pointer("/random_seed")
        .and_then(Value::as_u64)
        .unwrap_or(1);
    let max_concurrency = json_value
        .pointer("/concurrency")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .max(1);

    let mut resolved = json!({
        "experiment": {
            "id": experiment_id,
            "name": experiment_name,
            "owner": owner,
            "workload_type": "agent_runtime",
            "tags": experiment_tags
        },
        "dataset": {
            "provider": "local_jsonl",
            "path": dataset_path,
            "suite_id": dataset_suite_id,
            "split_id": dataset_split_id
        },
        "design": {
            "sanitization_profile": "hermetic_functional",
            "comparison": comparison,
            "replications": replications,
            "random_seed": random_seed,
            "shuffle_tasks": true,
            "max_concurrency": max_concurrency,
            "policies": {
                "scheduling": scheduling,
                "retry": {
                    "max_attempts": 1
                }
            }
        },
        "metrics": metrics,
        "baseline": {
            "variant_id": baseline_id,
            "bindings": baseline_bindings
        },
        "benchmark": {
            "policy": benchmark_policy,
            "grader": {
                "command": benchmark_grader_command,
                "_runtime_assets": benchmark_grader_runtime_assets
            }
        },
        "runtime": {
            "agent_runtime": {
                "command": agent_build.command.clone(),
                "artifact": agent_build.artifact_path.to_string_lossy().to_string(),
                "artifact_digest": agent_build.artifact_digest.clone(),
                "artifact_resolved_path": agent_build.artifact_path.to_string_lossy().to_string(),
                "image": agent_build.image.clone(),
                "env": agent_build.env.clone(),
                "network": network_mode
            }
        },
        "policy": {
            "timeout_ms": timeout_ms,
            "task_sandbox": {
                "profile": if benchmark_name == "swebench_lite_curated" { "swebench_testbed" } else { "default" },
                "network": network_mode
            }
        },
        "validity": {
            "fail_on_state_leak": true,
            "fail_on_profile_invariant_violation": true
        }
    });
    if let Some(description) = experiment_description {
        set_json_pointer_value(&mut resolved, "/experiment/description", json!(description))?;
    }
    if let Some(limit) = limit {
        set_json_pointer_value(&mut resolved, "/dataset/limit", json!(limit))?;
    }
    if !variant_plan.is_empty() {
        set_json_pointer_value(&mut resolved, "/variant_plan", Value::Array(variant_plan))?;
    }
    Ok(resolved)
}
