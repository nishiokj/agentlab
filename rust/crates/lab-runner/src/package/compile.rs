use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use lab_core::{canonical_json_digest, ensure_dir, sha256_file};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use crate::config::*;
use crate::experiment::runner::*;
use crate::model::*;
use crate::package::authoring::*;
use crate::package::staging::*;
use crate::package::validate::*;
use crate::experiment::preflight::resolve_dataset_path;
use crate::util::{copy_dir_preserve_all, sanitize_for_fs};
use crate::trial::spec::{parse_task_row, TaskMaterializationKind, TaskRow};

pub(crate) fn sanitize_name_for_path(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "experiment".to_string()
    } else {
        trimmed.to_string()
    }
}

pub(crate) fn as_portable_rel(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

pub(crate) fn copy_path_into_package(source: &Path, destination: &Path) -> Result<()> {
    if source.is_dir() {
        ensure_dir(destination)?;
        return copy_dir_preserve_all(source, destination, &[]);
    }
    if source.is_file() {
        if let Some(parent) = destination.parent() {
            ensure_dir(parent)?;
        }
        fs::copy(source, destination)?;
        return Ok(());
    }
    Err(anyhow!(
        "package build expected file or directory source, got: {}",
        source.display()
    ))
}

pub(crate) fn packaged_task_bundle_rel_path(
    task_id: &str,
    task_idx: usize,
    source: Option<&Path>,
) -> PathBuf {
    let stem = format!("{}_{}", sanitize_for_fs(task_id), task_idx + 1);
    let base = PathBuf::from("tasks").join("task_bundles");
    let Some(source) = source else {
        return base.join(stem);
    };
    let Some(name) = source.file_name().and_then(|value| value.to_str()) else {
        return base.join(stem);
    };
    if source.is_dir() {
        return base.join(stem);
    }
    base.join(format!("{}_{}", stem, name))
}

pub(crate) fn resolve_task_bundle_source_for_package(
    raw: &str,
    dataset_dir: &Path,
    exp_dir: &Path,
) -> Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("task bundle ref cannot be empty"));
    }
    let source = Path::new(trimmed);
    if source.is_absolute() {
        return Ok(source.to_path_buf());
    }
    let dataset_candidate = dataset_dir.join(source);
    if dataset_candidate.exists() {
        return Ok(dataset_candidate);
    }
    let exp_candidate = exp_dir.join(source);
    if exp_candidate.exists() {
        return Ok(exp_candidate);
    }
    Err(anyhow!(
        "task bundle ref '{}' could not be resolved relative to dataset or experiment directory",
        raw
    ))
}

pub(crate) fn stage_task_row_bundle_for_package(
    task_row: &TaskRow,
    task_idx: usize,
    dataset_dir: &Path,
    exp_dir: &Path,
    package_dir: &Path,
) -> Result<TaskRow> {
    let mut staged = task_row.clone();
    if !matches!(
        staged.materialization.kind,
        TaskMaterializationKind::BaseImageBundle
    ) {
        return Ok(staged);
    }
    let raw_bundle_ref = staged
        .materialization
        .task_bundle_ref
        .as_deref()
        .ok_or_else(|| {
            anyhow!(
                "task '{}' is missing materialization.task_bundle_ref for base_image_bundle",
                staged.id
            )
        })?;
    let source = resolve_task_bundle_source_for_package(raw_bundle_ref, dataset_dir, exp_dir)?;
    let bundle_rel = packaged_task_bundle_rel_path(&staged.id, task_idx, Some(&source));
    copy_path_into_package(&source, &package_dir.join(&bundle_rel))?;
    staged.materialization.task_bundle_ref = Some(as_portable_rel(&bundle_rel));
    Ok(staged)
}

pub(crate) fn compile_tasks_for_package(
    tasks: &[Value],
    _project_root: &Path,
    exp_dir: &Path,
    dataset_path: &Path,
    package_dir: &Path,
) -> Result<Vec<Value>> {
    let dataset_dir = dataset_path.parent().unwrap_or(exp_dir);
    let mut compiled = Vec::with_capacity(tasks.len());
    for (idx, task) in tasks.iter().enumerate() {
        let task_row = parse_task_row(task).with_context(|| {
            format!("package build task {} is not a valid task_row_v1", idx + 1)
        })?;
        let row =
            stage_task_row_bundle_for_package(&task_row, idx, dataset_dir, exp_dir, package_dir)?;
        compiled.push(serde_json::to_value(row)?);
    }
    Ok(compiled)
}

pub(crate) fn write_packaged_tasks(path: &Path, tasks: &[Value]) -> Result<()> {
    let mut bytes = Vec::new();
    for task in tasks {
        serde_json::to_writer(&mut bytes, task)?;
        bytes.push(b'\n');
    }
    atomic_write_bytes(path, &bytes)
}

pub(crate) fn load_task_rows_for_build(path: &Path, json_value: &Value) -> Result<Vec<Value>> {
    let limit = json_value
        .pointer("/dataset/limit")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    let dataset_ref = json_value
        .pointer("/dataset/path")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let dataset_suite = json_value
        .pointer("/dataset/suite_id")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");
    let file = fs::File::open(path).with_context(|| {
        format!(
            "failed to open dataset file '{}' (resolved from dataset.path='{}', dataset.suite_id='{}')",
            path.display(),
            dataset_ref,
            dataset_suite
        )
    })?;
    let reader = BufReader::new(file);
    let mut tasks = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if limit.is_some_and(|max| tasks.len() >= max) {
            break;
        }
        let task: Value = serde_json::from_str(trimmed)?;
        let task_id = task
            .pointer("/task/id")
            .or_else(|| task.pointer("/id"))
            .and_then(Value::as_str)
            .unwrap_or("<unknown_task>");
        if parse_task_row(&task).is_err() {
            return Err(anyhow!(
                "dataset row {} task '{}' is not a valid task_row_v1",
                idx + 1,
                task_id
            ));
        }
        tasks.push(task);
    }
    Ok(tasks)
}

pub fn build_experiment_package(
    path: &Path,
    overrides_path: Option<&Path>,
    out_dir: Option<&Path>,
) -> Result<BuildResult> {
    let loaded = load_authoring_input_for_build(path, overrides_path)?;
    let mut json_value = loaded.json_value.clone();
    validate_required_fields(&json_value)?;

    let experiment_id = json_value
        .pointer("/experiment/id")
        .and_then(Value::as_str)
        .unwrap_or("experiment");
    let package_dir = if let Some(out_dir) = out_dir {
        out_dir.to_path_buf()
    } else {
        let ts = Utc::now().format("%Y%m%d_%H%M%S_%6f");
        loaded
            .project_root
            .join(".lab")
            .join("builds")
            .join(format!("{}_{}", sanitize_name_for_path(experiment_id), ts))
    };
    if package_dir.exists() {
        if !package_dir.is_dir() {
            return Err(anyhow!(
                "build output path exists and is not a directory: {}",
                package_dir.display()
            ));
        }
        let mut entries = fs::read_dir(&package_dir)?;
        if entries.next().is_some() {
            return Err(anyhow!(
                "build output directory must be empty: {}",
                package_dir.display()
            ));
        }
    } else {
        ensure_dir(&package_dir)?;
    }

    ensure_dir(&package_dir.join("agent_builds"))?;
    ensure_dir(&package_dir.join("tasks"))?;
    ensure_dir(&package_dir.join("files"))?;
    ensure_dir(&package_dir.join(PACKAGED_RUNTIME_ASSETS_DIR))?;

    let dataset_path = resolve_dataset_path(&json_value, &loaded.exp_dir)?;
    let dataset_target = package_dir.join("tasks").join("tasks.jsonl");
    let raw_tasks = load_task_rows_for_build(&dataset_path, &json_value)?;
    let packaged_tasks = compile_tasks_for_package(
        &raw_tasks,
        &loaded.project_root,
        &loaded.exp_dir,
        &dataset_path,
        &package_dir,
    )?;
    write_packaged_tasks(&dataset_target, &packaged_tasks)?;
    let dataset_rel = PathBuf::from("tasks").join("tasks.jsonl");
    set_json_pointer_value(
        &mut json_value,
        "/dataset/path",
        json!(as_portable_rel(&dataset_rel)),
    )?;

    let mut artifact_copies: BTreeMap<String, String> = BTreeMap::new();
    let mut file_copies: BTreeMap<String, String> = BTreeMap::new();
    let mut public_path_copies: BTreeMap<String, String> = BTreeMap::new();
    let mut staging_manifest_entries = Vec::new();
    let mut artifact_counter = 0usize;
    let mut file_counter = 0usize;

    if let Some(runtime) = json_value.pointer_mut("/runtime") {
        rewrite_runtime_paths_for_package(
            runtime,
            &loaded.exp_dir,
            &package_dir,
            &mut artifact_copies,
            &mut file_copies,
            &mut public_path_copies,
            &mut staging_manifest_entries,
            &mut artifact_counter,
            &mut file_counter,
        )?;
    }
    if let Some(runtime_overrides) = json_value.pointer_mut("/baseline/runtime_overrides") {
        rewrite_runtime_paths_for_package(
            runtime_overrides,
            &loaded.exp_dir,
            &package_dir,
            &mut artifact_copies,
            &mut file_copies,
            &mut public_path_copies,
            &mut staging_manifest_entries,
            &mut artifact_counter,
            &mut file_counter,
        )?;
    }
    if let Some(variant_plan) = json_value
        .pointer_mut("/variant_plan")
        .and_then(Value::as_array_mut)
    {
        for variant in variant_plan.iter_mut() {
            if let Some(runtime_overrides) = variant.get_mut("runtime_overrides") {
                rewrite_runtime_paths_for_package(
                    runtime_overrides,
                    &loaded.exp_dir,
                    &package_dir,
                    &mut artifact_copies,
                    &mut file_copies,
                    &mut public_path_copies,
                    &mut staging_manifest_entries,
                    &mut artifact_counter,
                    &mut file_counter,
                )?;
            }
        }
    }
    if let Some(variants) = json_value
        .pointer_mut("/variants")
        .and_then(Value::as_array_mut)
    {
        for variant in variants.iter_mut() {
            if let Some(runtime_overrides) = variant.get_mut("runtime_overrides") {
                rewrite_runtime_paths_for_package(
                    runtime_overrides,
                    &loaded.exp_dir,
                    &package_dir,
                    &mut artifact_copies,
                    &mut file_copies,
                    &mut public_path_copies,
                    &mut staging_manifest_entries,
                    &mut artifact_counter,
                    &mut file_counter,
                )?;
            }
        }
    }
    if let Some(benchmark) = json_value.pointer_mut("/benchmark") {
        rewrite_benchmark_paths_for_package(
            benchmark,
            &loaded.exp_dir,
            &package_dir,
            &mut file_copies,
            &mut file_counter,
            &mut public_path_copies,
            &mut staging_manifest_entries,
        )?;
    }

    validate_packaged_runtime_artifacts(&package_dir, &json_value)?;
    write_runtime_staging_manifest(&package_dir, &json_value, &staging_manifest_entries)?;

    let resolved_for_manifest = json_value.clone();
    atomic_write_json_pretty(
        &package_dir.join("resolved_experiment.json"),
        &resolved_for_manifest,
    )?;

    let manifest_path = package_dir.join("manifest.json");
    let checksums_path = package_dir.join("checksums.json");
    let lock_path = package_dir.join("package.lock");
    let mut checksums: BTreeMap<String, String> = BTreeMap::new();
    for entry in walkdir::WalkDir::new(&package_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
    {
        let path = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }
        if path == checksums_path || path == manifest_path || path == lock_path {
            continue;
        }
        let rel = path
            .strip_prefix(&package_dir)
            .map(as_portable_rel)
            .unwrap_or_else(|_| path.display().to_string());
        checksums.insert(rel, sha256_file(path)?);
    }
    let checksums_value = json!({
        "schema_version": "sealed_package_checksums_v2",
        "files": checksums,
    });
    atomic_write_json_pretty(&checksums_path, &checksums_value)?;
    let package_digest = canonical_json_digest(
        checksums_value
            .pointer("/files")
            .ok_or_else(|| anyhow!("build failed to materialize checksums files map"))?,
    );
    atomic_write_json_pretty(
        &lock_path,
        &json!({
            "schema_version": "sealed_package_lock_v1",
            "package_digest": package_digest.clone(),
        }),
    )?;
    let package_manifest = json!({
        "schema_version": "sealed_run_package_v2",
        "created_at": Utc::now().to_rfc3339(),
        "resolved_experiment": resolved_for_manifest,
        "checksums_ref": "checksums.json",
        "package_digest": package_digest,
    });
    atomic_write_json_pretty(&manifest_path, &package_manifest)?;

    Ok(BuildResult {
        package_dir,
        manifest_path,
        checksums_path,
    })
}
