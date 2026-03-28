use anyhow::{anyhow, Result};
use lab_core::{canonical_json_digest, sha256_file};
use serde_json::Value;
use std::path::{Path, PathBuf};

use crate::config::*;
use crate::model::STAGING_MANIFEST_FILE;
use crate::model::*;

pub(crate) fn resolve_package_path_under_root(
    package_dir: &Path,
    rel_path: &str,
    field_name: &str,
) -> Result<PathBuf> {
    let trimmed = rel_path.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} must be a non-empty relative path", field_name));
    }
    if Path::new(trimmed).is_absolute() {
        return Err(anyhow!("{} must be relative to package root", field_name));
    }
    let resolved = normalize_path(&package_dir.join(trimmed));
    let root = canonicalize_best_effort(package_dir);
    let resolved_cmp = canonicalize_best_effort(&resolved);
    if !resolved_cmp.starts_with(&root) {
        return Err(anyhow!(
            "{} escapes package root: '{}' (root: {})",
            field_name,
            rel_path,
            root.display()
        ));
    }
    Ok(resolved)
}

pub(crate) fn verify_sealed_package_integrity(
    package_dir: &Path,
    manifest: &Value,
) -> Result<Value> {
    require_exact_object_keys(
        manifest,
        &[
            "schema_version",
            "created_at",
            "resolved_experiment",
            "checksums_ref",
            "package_digest",
        ],
        "sealed package manifest",
    )?;
    if manifest.pointer("/schema_version").and_then(Value::as_str) != Some("sealed_run_package_v2")
    {
        return Err(anyhow!(
            "preflight_failed: manifest schema_version must be 'sealed_run_package_v2'"
        ));
    }
    let checksums_ref = manifest
        .pointer("/checksums_ref")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed package manifest missing checksums_ref"))?;
    let checksums_path =
        resolve_package_path_under_root(package_dir, checksums_ref, "checksums_ref")?;
    let checksums = load_json_file(&checksums_path)?;
    if checksums.pointer("/schema_version").and_then(Value::as_str)
        != Some("sealed_package_checksums_v2")
    {
        return Err(anyhow!(
            "preflight_failed: checksums schema_version must be 'sealed_package_checksums_v2'"
        ));
    }
    let files = checksums
        .pointer("/files")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("preflight_failed: checksums.json missing object field 'files'"))?;
    for (rel, expected_digest) in files {
        let expected = expected_digest.as_str().ok_or_else(|| {
            anyhow!(
                "preflight_failed: checksums entry '{}' must be a string digest",
                rel
            )
        })?;
        let file_path = resolve_package_path_under_root(package_dir, rel, "checksums.files")?;
        if !file_path.is_file() {
            return Err(anyhow!(
                "preflight_failed: checksummed file missing: {}",
                file_path.display()
            ));
        }
        let actual = sha256_file(&file_path)?;
        if !expected.eq_ignore_ascii_case(actual.as_str()) {
            return Err(anyhow!(
                "preflight_failed: checksum mismatch for '{}' (expected {}, got {})",
                rel,
                expected,
                actual
            ));
        }
    }
    if !files.contains_key("resolved_experiment.json") {
        return Err(anyhow!(
            "preflight_failed: checksums must include 'resolved_experiment.json'"
        ));
    }
    if !files.contains_key(STAGING_MANIFEST_FILE) {
        return Err(anyhow!(
            "preflight_failed: checksums must include '{}'",
            STAGING_MANIFEST_FILE
        ));
    }
    let computed_digest = canonical_json_digest(
        checksums
            .pointer("/files")
            .ok_or_else(|| anyhow!("preflight_failed: checksums missing files object"))?,
    );
    let manifest_digest = manifest
        .pointer("/package_digest")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed package manifest missing package_digest"))?;
    if computed_digest != manifest_digest {
        return Err(anyhow!(
            "preflight_failed: package digest mismatch (manifest={}, computed={})",
            manifest_digest,
            computed_digest
        ));
    }
    let lock_path = package_dir.join("package.lock");
    let lock = load_json_file(&lock_path).map_err(|err| {
        anyhow!(
            "preflight_failed: package.lock missing or unreadable at {}: {}",
            lock_path.display(),
            err
        )
    })?;
    if lock.pointer("/package_digest").and_then(Value::as_str) != Some(manifest_digest) {
        return Err(anyhow!(
            "preflight_failed: package.lock digest does not match manifest package_digest"
        ));
    }
    let resolved_path = resolve_package_path_under_root(
        package_dir,
        "resolved_experiment.json",
        "checksums.files",
    )?;
    let resolved_experiment = load_json_file(&resolved_path).map_err(|err| {
        anyhow!(
            "preflight_failed: resolved_experiment.json missing or unreadable at {}: {}",
            resolved_path.display(),
            err
        )
    })?;
    let staging_manifest_path =
        resolve_package_path_under_root(package_dir, STAGING_MANIFEST_FILE, "checksums.files")?;
    load_json_file(&staging_manifest_path).map_err(|err| {
        anyhow!(
            "preflight_failed: {} missing or unreadable at {}: {}",
            STAGING_MANIFEST_FILE,
            staging_manifest_path.display(),
            err
        )
    })?;
    Ok(resolved_experiment)
}

pub(crate) fn load_sealed_package_for_run(path: &Path) -> Result<LoadedExperimentInput> {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let (manifest_path, exp_dir) = if canonical.is_dir() {
        let manifest = canonical.join("manifest.json");
        if !manifest.is_file() {
            return Err(anyhow!(
                "run_input_invalid_kind: expected sealed package dir or manifest"
            ));
        }
        (manifest, canonical)
    } else if canonical
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "manifest.json")
    {
        let exp_dir = canonical
            .parent()
            .ok_or_else(|| anyhow!("manifest has no parent directory"))?
            .to_path_buf();
        (canonical, exp_dir)
    } else {
        return Err(anyhow!(
            "run_input_invalid_kind: expected sealed package dir or manifest"
        ));
    };
    let manifest = load_json_file(&manifest_path)?;
    let json_value = verify_sealed_package_integrity(&exp_dir, &manifest)?;
    let project_root = find_project_root(&exp_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&exp_dir));
    Ok(LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root,
    })
}

pub(crate) fn resolve_dataset_path_in_package(
    json_value: &Value,
    package_dir: &Path,
) -> Result<PathBuf> {
    let rel = json_value
        .pointer("/dataset/path")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("dataset.path missing"))?;
    resolve_package_path_under_root(package_dir, rel, "dataset.path")
}
