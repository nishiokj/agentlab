use anyhow::{anyhow, Context, Result};
use lab_core::{
    AGENTLAB_CONTRACT_RUNTIME_AUX_DIR, AGENTLAB_RUNNER_SUPPORT_REL_DIR,
    AGENTLAB_TASK_WORKDIR_PLACEHOLDER,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::config::*;
use crate::model::*;
use crate::package::authoring::{
    contains_removed_runtime_template, resolve_existing_public_path_reference,
    validate_public_authoring_relpath,
};
use crate::package::compile::*;
use crate::package::sealed::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimePathStagingManifestEntry {
    pub(crate) original_relative_path: String,
    pub(crate) packaged_path: String,
    pub(crate) runtime_path: String,
    pub(crate) required: bool,
    pub(crate) read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimePathStagingManifest {
    pub(crate) schema_version: String,
    pub(crate) variants: BTreeMap<String, Vec<RuntimePathStagingManifestEntry>>,
}

pub(crate) fn task_workdir_support_relative_path(rel_path: &str) -> String {
    let rel = rel_path.trim().trim_start_matches('/');
    if rel.is_empty() {
        AGENTLAB_RUNNER_SUPPORT_REL_DIR.to_string()
    } else {
        format!("{}/{}", AGENTLAB_RUNNER_SUPPORT_REL_DIR, rel)
    }
}

pub(crate) fn task_workdir_support_destination_path(rel_path: &str) -> String {
    format!(
        "{}/{}",
        AGENTLAB_TASK_WORKDIR_PLACEHOLDER,
        task_workdir_support_relative_path(rel_path)
    )
}

pub(crate) fn strip_task_workdir_support_destination_path(path: &str) -> Option<&str> {
    let prefix = format!(
        "{}/{}",
        AGENTLAB_TASK_WORKDIR_PLACEHOLDER, AGENTLAB_RUNNER_SUPPORT_REL_DIR
    );
    if path == prefix {
        return Some("");
    }
    let rest = path.strip_prefix(&prefix)?;
    if rest.starts_with('/') {
        Some(rest.trim_start_matches('/'))
    } else {
        None
    }
}

pub(crate) fn validate_runner_staged_destination_path(
    raw: &str,
    field_name: &str,
) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} must not be empty", field_name));
    }
    let task_support_prefix = format!(
        "{}/{}",
        AGENTLAB_TASK_WORKDIR_PLACEHOLDER, AGENTLAB_RUNNER_SUPPORT_REL_DIR
    );
    if trimmed == task_support_prefix || trimmed.starts_with(&format!("{}/", task_support_prefix)) {
        let rest = trimmed
            .strip_prefix(AGENTLAB_TASK_WORKDIR_PLACEHOLDER)
            .unwrap_or_default();
        for component in Path::new(rest).components() {
            if matches!(component, Component::ParentDir) {
                return Err(anyhow!("{} cannot contain '..'", field_name));
            }
        }
        return Ok(trimmed.to_string());
    }
    let path = Path::new(trimmed);
    if !path.is_absolute() {
        return Err(anyhow!(
            "{} must be under {}/{} or {}",
            field_name,
            AGENTLAB_TASK_WORKDIR_PLACEHOLDER,
            AGENTLAB_RUNNER_SUPPORT_REL_DIR,
            AGENTLAB_CONTRACT_RUNTIME_AUX_DIR
        ));
    }
    if !(trimmed == AGENTLAB_CONTRACT_RUNTIME_AUX_DIR
        || trimmed.starts_with(&format!("{}/", AGENTLAB_CONTRACT_RUNTIME_AUX_DIR)))
    {
        return Err(anyhow!(
            "{} must be under {}/{} or {}",
            field_name,
            AGENTLAB_TASK_WORKDIR_PLACEHOLDER,
            AGENTLAB_RUNNER_SUPPORT_REL_DIR,
            AGENTLAB_CONTRACT_RUNTIME_AUX_DIR
        ));
    }
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(anyhow!("{} cannot contain '..'", field_name));
        }
    }
    Ok(trimmed.to_string())
}

pub(crate) fn stage_source_into_package(
    raw_source: &str,
    exp_dir: &Path,
    package_dir: &Path,
    subdir: &str,
    prefix: &str,
    copies: &mut BTreeMap<String, String>,
    counter: &mut usize,
) -> Result<String> {
    let raw_path = PathBuf::from(raw_source);
    let resolved = if raw_path.is_absolute() {
        normalize_path(&raw_path)
    } else {
        normalize_path(&exp_dir.join(raw_path))
    };
    let key = resolved.to_string_lossy().to_string();
    if let Some(existing) = copies.get(&key) {
        return Ok(existing.clone());
    }
    fs::metadata(&resolved).with_context(|| {
        format!(
            "package build failed to read staged source '{}' resolved from '{}'",
            resolved.display(),
            raw_source
        )
    })?;
    let name = resolved
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}_{}", prefix, counter));
    let rel_path = PathBuf::from(subdir).join(format!("{:03}_{}", *counter, name));
    let destination = package_dir.join(&rel_path);
    copy_path_into_package(&resolved, &destination)?;
    *counter += 1;
    let rel_portable = as_portable_rel(&rel_path);
    copies.insert(key, rel_portable.clone());
    Ok(rel_portable)
}

pub(crate) fn stage_public_runtime_path_reference(
    rel: &Path,
    exp_dir: &Path,
    package_dir: &Path,
    copies: &mut BTreeMap<String, String>,
    manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
    field_name: &str,
) -> Result<String> {
    let rel_portable = as_portable_rel(rel);
    let resolved = normalize_path(&exp_dir.join(rel));
    fs::metadata(&resolved).with_context(|| {
        format!(
            "package build failed to read {} public path reference '{}' resolved to '{}'",
            field_name,
            rel_portable,
            resolved.display()
        )
    })?;
    if copies.contains_key(&rel_portable) {
        return Ok(task_workdir_support_destination_path(&rel_portable));
    }
    let packaged_rel = PathBuf::from(PACKAGED_RUNTIME_ASSETS_DIR).join(rel);
    let packaged_rel_portable = as_portable_rel(&packaged_rel);
    let destination = package_dir.join(&packaged_rel);
    copy_path_into_package(&resolved, &destination)?;
    copies.insert(rel_portable.clone(), packaged_rel_portable.clone());
    manifest_entries.push(RuntimePathStagingManifestEntry {
        original_relative_path: rel_portable.clone(),
        packaged_path: packaged_rel_portable,
        runtime_path: task_workdir_support_destination_path(&rel_portable),
        required: true,
        read_only: true,
    });
    Ok(task_workdir_support_destination_path(&rel_portable))
}

pub(crate) fn is_runner_staged_destination_path(raw: &str) -> bool {
    strip_task_workdir_support_destination_path(raw).is_some()
        || raw == AGENTLAB_CONTRACT_RUNTIME_AUX_DIR
        || raw.starts_with(&format!("{}/", AGENTLAB_CONTRACT_RUNTIME_AUX_DIR))
}

pub(crate) fn rewrite_packaged_runtime_asset_entries(
    entries: Option<&mut Value>,
    field_name: &str,
    exp_dir: &Path,
    package_dir: &Path,
    file_copies: &mut BTreeMap<String, String>,
    file_counter: &mut usize,
) -> Result<()> {
    let Some(items) = entries.and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for (idx, item) in items.iter_mut().enumerate() {
        let raw = item
            .get("build_source_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].build_source_path is required", field_name, idx))?;
        let rel = stage_source_into_package(
            raw,
            exp_dir,
            package_dir,
            PACKAGED_RUNTIME_ASSETS_DIR,
            "dep",
            file_copies,
            file_counter,
        )
        .with_context(|| {
            format!(
                "failed to stage {}[{}].build_source_path '{}' into sealed package",
                field_name, idx, raw
            )
        })?;
        if let Some(obj) = item.as_object_mut() {
            obj.remove("build_source_path");
        }
        set_json_pointer_value(item, "/packaged_path", json!(rel))?;
    }
    Ok(())
}

pub(crate) fn rewrite_optional_package_source_path(
    value: Option<&mut Value>,
    field_name: &str,
    exp_dir: &Path,
    package_dir: &Path,
    subdir: &str,
    prefix: &str,
    file_copies: &mut BTreeMap<String, String>,
    file_counter: &mut usize,
) -> Result<()> {
    let Some(item) = value else {
        return Ok(());
    };
    let Some(raw) = item
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let rel = stage_source_into_package(
        raw,
        exp_dir,
        package_dir,
        subdir,
        prefix,
        file_copies,
        file_counter,
    )
    .with_context(|| {
        format!(
            "failed to stage {} '{}' into sealed package",
            field_name, raw
        )
    })?;
    *item = Value::String(rel);
    Ok(())
}

pub(crate) fn stage_optional_public_runtime_path_for_package(
    value: Option<&mut Value>,
    field_name: &str,
    exp_dir: &Path,
    package_dir: &Path,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    let Some(item) = value else {
        return Ok(());
    };
    let Some(raw) = item
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    if is_runner_staged_destination_path(raw) {
        return Ok(());
    }
    let Some(rel) = resolve_existing_public_path_reference(raw, exp_dir, field_name)? else {
        return Ok(());
    };
    let contract_path = stage_public_runtime_path_reference(
        &rel,
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
        field_name,
    )?;
    *item = Value::String(contract_path);
    Ok(())
}

pub(crate) fn stage_command_path_refs_for_package(
    command_root: Option<&mut Value>,
    field_name: &str,
    exp_dir: &Path,
    package_dir: &Path,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    let Some(items) = command_root.and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for idx in 0..items.len() {
        let token = items[idx]
            .as_str()
            .ok_or_else(|| anyhow!("{}[{}] must be a string", field_name, idx))?;
        if contains_removed_runtime_template(token) {
            return Err(anyhow!(
                "{}[{}] uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                field_name,
                idx
            ));
        }
        if idx == 0 {
            continue;
        }
        if is_runner_staged_destination_path(token) {
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
        let contract_path = stage_public_runtime_path_reference(
            &rel,
            exp_dir,
            package_dir,
            public_path_copies,
            staging_manifest_entries,
            &format!("{}[{}]", field_name, idx),
        )?;
        items[idx] = Value::String(contract_path);
    }
    Ok(())
}

pub(crate) fn stage_runtime_command_env_path_refs_for_package(
    runtime_root: &mut Value,
    exp_dir: &Path,
    package_dir: &Path,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    stage_command_path_refs_for_package(
        runtime_root.pointer_mut("/agent_runtime/command"),
        "runtime.agent_runtime.command",
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    if let Some(items) = runtime_root
        .pointer_mut("/agent_runtime/env")
        .and_then(Value::as_object_mut)
    {
        let keys = items.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let raw = items
                .get(&key)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("runtime.agent_runtime.env.{} must be a string", key))?;
            if contains_removed_runtime_template(raw) {
                return Err(anyhow!(
                    "runtime.agent_runtime.env.{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                    key
                ));
            }
            if raw.trim().starts_with("/agentlab/") {
                return Err(anyhow!(
                    "runtime.agent_runtime.env.{} leaks runner topology; remove internal /agentlab paths from public authoring",
                    key
                ));
            }
            if is_runner_staged_destination_path(raw) {
                continue;
            }
            let Some(rel) = resolve_existing_public_path_reference(
                raw,
                exp_dir,
                &format!("runtime.agent_runtime.env.{}", key),
            )?
            else {
                continue;
            };
            let contract_path = stage_public_runtime_path_reference(
                &rel,
                exp_dir,
                package_dir,
                public_path_copies,
                staging_manifest_entries,
                &format!("runtime.agent_runtime.env.{}", key),
            )?;
            items.insert(key, Value::String(contract_path));
        }
    }
    Ok(())
}

pub(crate) fn collect_command_staging_entries(
    command_root: Option<&Value>,
    field_name: &str,
    catalog: &BTreeMap<String, RuntimePathStagingManifestEntry>,
    seen: &mut HashSet<String>,
    entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    let Some(items) = command_root.and_then(Value::as_array) else {
        return Ok(());
    };
    for (idx, item) in items.iter().enumerate() {
        if idx == 0 {
            continue;
        }
        let Some(runtime_path) = item.as_str().map(str::trim) else {
            return Err(anyhow!("{}[{}] must be a string", field_name, idx));
        };
        if strip_task_workdir_support_destination_path(runtime_path).is_none() {
            continue;
        }
        if !seen.insert(runtime_path.to_string()) {
            continue;
        }
        let entry = lookup_runtime_staging_entry(catalog, runtime_path).ok_or_else(|| {
            anyhow!(
                "{}[{}] references packaged dependency '{}' with no staging manifest entry",
                field_name,
                idx,
                runtime_path
            )
        })?;
        entries.push(entry);
    }
    Ok(())
}

pub(crate) fn collect_runtime_command_env_staging_entries(
    experiment: &Value,
    catalog: &BTreeMap<String, RuntimePathStagingManifestEntry>,
) -> Result<Vec<RuntimePathStagingManifestEntry>> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();

    collect_command_staging_entries(
        experiment.pointer("/runtime/agent_runtime/command"),
        "runtime.agent_runtime.command",
        catalog,
        &mut seen,
        &mut entries,
    )?;
    collect_command_staging_entries(
        experiment.pointer("/benchmark/grader/command"),
        "benchmark.grader.command",
        catalog,
        &mut seen,
        &mut entries,
    )?;
    collect_command_staging_entries(
        experiment.pointer("/benchmark/adapter/command"),
        "benchmark.adapter.command",
        catalog,
        &mut seen,
        &mut entries,
    )?;

    if let Some(items) = experiment
        .pointer("/runtime/agent_runtime/env")
        .and_then(Value::as_object)
    {
        for (key, value) in items {
            let Some(runtime_path) = value.as_str().map(str::trim) else {
                return Err(anyhow!(
                    "runtime.agent_runtime.env.{} must be a string",
                    key
                ));
            };
            if strip_task_workdir_support_destination_path(runtime_path).is_none() {
                continue;
            }
            if !seen.insert(runtime_path.to_string()) {
                continue;
            }
            let entry = lookup_runtime_staging_entry(catalog, runtime_path).ok_or_else(|| {
                anyhow!(
                    "runtime.agent_runtime.env.{} references packaged dependency '{}' with no staging manifest entry",
                    key,
                    runtime_path
                )
            })?;
            entries.push(entry);
        }
    }

    Ok(entries)
}

pub(crate) fn lookup_runtime_staging_entry(
    catalog: &BTreeMap<String, RuntimePathStagingManifestEntry>,
    runtime_path: &str,
) -> Option<RuntimePathStagingManifestEntry> {
    if let Some(entry) = catalog.get(runtime_path) {
        return Some(entry.clone());
    }
    catalog
        .values()
        .filter(|entry| matches_contract_runtime_root(runtime_path, &entry.runtime_path))
        .max_by_key(|entry| entry.runtime_path.len())
        .cloned()
}

pub(crate) fn matches_contract_runtime_root(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn collect_packaged_runtime_asset_entries(
    value: Option<&Value>,
    field_name: &str,
) -> Result<Vec<RuntimePathStagingManifestEntry>> {
    let Some(items) = value else {
        return Ok(Vec::new());
    };
    let arr = items
        .as_array()
        .ok_or_else(|| anyhow!("{} must be an array", field_name))?;
    let mut entries = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| anyhow!("{}[{}] must be an object", field_name, idx))?;
        let packaged_path = obj
            .get("packaged_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].packaged_path is required", field_name, idx))?;
        let runtime_path = obj
            .get("runtime_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].runtime_path is required", field_name, idx))?;
        entries.push(RuntimePathStagingManifestEntry {
            original_relative_path: packaged_path.to_string(),
            packaged_path: validate_public_authoring_relpath(
                packaged_path,
                &format!("{}[{}].packaged_path", field_name, idx),
            )?,
            runtime_path: validate_runner_staged_destination_path(
                runtime_path,
                &format!("{}[{}].runtime_path", field_name, idx),
            )?,
            required: obj.get("required").and_then(Value::as_bool).unwrap_or(true),
            read_only: obj
                .get("read_only")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        });
    }
    Ok(entries)
}

pub(crate) fn merge_runtime_path_staging_entries(
    base: &mut Vec<RuntimePathStagingManifestEntry>,
    extra: Vec<RuntimePathStagingManifestEntry>,
) {
    for next in extra {
        if let Some(existing) = base
            .iter_mut()
            .find(|entry| entry.runtime_path == next.runtime_path)
        {
            *existing = next;
        } else {
            base.push(next);
        }
    }
}

pub(crate) fn write_runtime_staging_manifest(
    package_dir: &Path,
    experiment: &Value,
    entries: &[RuntimePathStagingManifestEntry],
) -> Result<()> {
    let (variants, _) = resolve_variant_plan(experiment)?;
    let mut variants_manifest: BTreeMap<String, Vec<RuntimePathStagingManifestEntry>> =
        BTreeMap::new();
    for variant in &variants {
        let variant_experiment = resolve_runtime_for_variant(experiment, variant)?;
        let mut variant_catalog_entries = entries.to_vec();
        merge_runtime_path_staging_entries(
            &mut variant_catalog_entries,
            collect_packaged_runtime_asset_entries(
                variant_experiment.pointer("/benchmark/grader/_runtime_assets"),
                "benchmark.grader._runtime_assets",
            )?,
        );
        merge_runtime_path_staging_entries(
            &mut variant_catalog_entries,
            collect_packaged_runtime_asset_entries(
                variant_experiment.pointer("/benchmark/adapter/_runtime_assets"),
                "benchmark.adapter._runtime_assets",
            )?,
        );
        let variant_catalog = variant_catalog_entries
            .iter()
            .cloned()
            .map(|entry| (entry.runtime_path.clone(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut variant_entries =
            collect_runtime_command_env_staging_entries(&variant_experiment, &variant_catalog)?;
        merge_runtime_path_staging_entries(&mut variant_entries, variant_catalog_entries);
        variant_entries.sort_by(|left, right| {
            left.runtime_path
                .cmp(&right.runtime_path)
                .then(left.packaged_path.cmp(&right.packaged_path))
        });
        for (idx, entry) in variant_entries.iter().enumerate() {
            let packaged_source = resolve_package_path_under_root(
                package_dir,
                &entry.packaged_path,
                &format!(
                    "staging_manifest.variants.{}[{}].packaged_path",
                    variant.id, idx
                ),
            )?;
            fs::metadata(&packaged_source).with_context(|| {
                format!(
                    "failed to read packaged runtime staging source '{}' for variant '{}'",
                    packaged_source.display(),
                    variant.id
                )
            })?;
        }
        variants_manifest.insert(variant.id.clone(), variant_entries);
    }
    let manifest_value = serde_json::to_value(RuntimePathStagingManifest {
        schema_version: STAGING_MANIFEST_SCHEMA_VERSION.to_string(),
        variants: variants_manifest,
    })?;
    atomic_write_json_pretty(&package_dir.join(STAGING_MANIFEST_FILE), &manifest_value)
}

pub(crate) fn rewrite_runtime_paths_for_package(
    runtime_root: &mut Value,
    exp_dir: &Path,
    package_dir: &Path,
    artifact_copies: &mut BTreeMap<String, String>,
    _file_copies: &mut BTreeMap<String, String>,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
    artifact_counter: &mut usize,
    _file_counter: &mut usize,
) -> Result<()> {
    if let Some(raw) = runtime_root
        .pointer("/agent_runtime/artifact")
        .and_then(Value::as_str)
    {
        let rel = stage_source_into_package(
            raw,
            exp_dir,
            package_dir,
            "agent_builds",
            "build",
            artifact_copies,
            artifact_counter,
        )?;
        set_json_pointer_value(runtime_root, "/agent_runtime/artifact", json!(rel.clone()))?;
        set_json_pointer_value(
            runtime_root,
            "/agent_runtime/artifact_resolved_path",
            json!(rel),
        )?;
    }
    stage_runtime_command_env_path_refs_for_package(
        runtime_root,
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    Ok(())
}

pub(crate) fn rewrite_benchmark_paths_for_package(
    benchmark_root: &mut Value,
    exp_dir: &Path,
    package_dir: &Path,
    file_copies: &mut BTreeMap<String, String>,
    file_counter: &mut usize,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    rewrite_packaged_runtime_asset_entries(
        benchmark_root.pointer_mut("/grader/_runtime_assets"),
        "benchmark.grader._runtime_assets",
        exp_dir,
        package_dir,
        file_copies,
        file_counter,
    )?;
    rewrite_packaged_runtime_asset_entries(
        benchmark_root.pointer_mut("/adapter/_runtime_assets"),
        "benchmark.adapter._runtime_assets",
        exp_dir,
        package_dir,
        file_copies,
        file_counter,
    )?;
    stage_command_path_refs_for_package(
        benchmark_root.pointer_mut("/grader/command"),
        "benchmark.grader.command",
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    stage_optional_public_runtime_path_for_package(
        benchmark_root.pointer_mut("/grader/conclusion/mapper"),
        "benchmark.grader.conclusion.mapper",
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    rewrite_optional_package_source_path(
        benchmark_root.pointer_mut("/grader/injected/bundle"),
        "benchmark.grader.injected.bundle",
        exp_dir,
        package_dir,
        "files",
        "grader_bundle",
        file_copies,
        file_counter,
    )?;
    stage_command_path_refs_for_package(
        benchmark_root.pointer_mut("/adapter/command"),
        "benchmark.adapter.command",
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    Ok(())
}
