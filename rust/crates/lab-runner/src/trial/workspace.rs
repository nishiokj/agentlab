use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::Utc;
use lab_core::{ensure_dir, sha256_file, ArtifactStore};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::backend::docker::{ContainerHandle, DockerRuntime, ExecSpec};
use crate::config::atomic_write_bytes;
use crate::engine::parse_max_workspace_bundle_bytes_from_env;
use crate::model::{StatePolicy, AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES_ENV, WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES};
use crate::runtime::{sanitize_for_fs, shell_quote, validate_container_workspace_path};

pub(crate) const WORKSPACE_SOURCE_MOUNT_PATH: &str = "/agentlab/_materialize/workspace_src";

pub(crate) fn is_workspace_evidence_excluded(rel: &Path) -> bool {
    if WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES
        .iter()
        .any(|prefix| rel.starts_with(prefix))
    {
        return true;
    }

    for component in rel.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        let name = name.to_string_lossy();
        if name == "node_modules"
            || name == ".git"
            || name == ".pnpm-store"
            || name == ".yarn"
            || name == "__pycache__"
            || name == ".pytest_cache"
            || name == ".mypy_cache"
            || name == ".ruff_cache"
            || name == "target"
            || name == ".DS_Store"
            || name.starts_with("._")
        {
            return true;
        }
    }

    false
}

pub(crate) fn collect_workspace_snapshot_manifest(workspace: &Path) -> Result<Value> {
    let mut files: Vec<(String, String, u64)> = Vec::new();
    if workspace.exists() {
        let walker = walkdir::WalkDir::new(workspace).into_iter();
        for entry in walker {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let rel_path = entry.path().strip_prefix(workspace).unwrap_or(entry.path());
            if is_workspace_evidence_excluded(rel_path) {
                continue;
            }
            let rel = rel_path.to_string_lossy().to_string();
            let digest = sha256_file(entry.path())?;
            let size = entry.metadata()?.len();
            files.push((rel, digest, size));
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let total_bytes = files.iter().map(|(_, _, sz)| *sz).sum::<u64>();
    let rows = files
        .into_iter()
        .map(|(path, digest, size_bytes)| {
            json!({
                "path": path,
                "digest": digest,
                "size_bytes": size_bytes
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": "workspace_snapshot_v1",
        "captured_at": Utc::now().to_rfc3339(),
        "file_count": rows.len(),
        "total_bytes": total_bytes,
        "files": rows
    }))
}

fn snapshot_file_map(snapshot_manifest: &Value) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Some(arr) = snapshot_manifest.get("files").and_then(|v| v.as_array()) {
        for row in arr {
            let path = row.get("path").and_then(Value::as_str);
            let digest = row.get("digest").and_then(Value::as_str);
            if let (Some(path), Some(digest)) = (path, digest) {
                map.insert(path.to_string(), digest.to_string());
            }
        }
    }
    map
}

pub(crate) fn diff_workspace_snapshots(prev: &Value, post: &Value) -> Value {
    let prev_map = snapshot_file_map(prev);
    let post_map = snapshot_file_map(post);

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut modified = Vec::new();

    for (path, digest) in post_map.iter() {
        match prev_map.get(path) {
            None => added.push(path.clone()),
            Some(prev_digest) if prev_digest != digest => modified.push(path.clone()),
            _ => {}
        }
    }
    for path in prev_map.keys() {
        if !post_map.contains_key(path) {
            removed.push(path.clone());
        }
    }

    json!({
        "schema_version": "workspace_diff_v1",
        "captured_at": Utc::now().to_rfc3339(),
        "added": added,
        "removed": removed,
        "modified": modified,
        "summary": {
            "added_files": added.len(),
            "removed_files": removed.len(),
            "modified_files": modified.len()
        }
    })
}

pub(crate) fn derive_patch_from_diff(diff: &Value) -> Value {
    json!({
        "schema_version": "workspace_patch_v1",
        "format": "file_digest_delta",
        "generated_at": Utc::now().to_rfc3339(),
        "added": diff.get("added").cloned().unwrap_or(json!([])),
        "removed": diff.get("removed").cloned().unwrap_or(json!([])),
        "modified": diff.get("modified").cloned().unwrap_or(json!([])),
    })
}

pub(crate) fn workspace_diff_is_empty(diff: &Value) -> bool {
    ["added", "removed", "modified"].iter().all(|field| {
        diff.get(field)
            .and_then(Value::as_array)
            .map_or(true, Vec::is_empty)
    })
}

pub(crate) fn capture_workspace_object_ref(
    artifact_store: &ArtifactStore,
    workspace_dir: &Path,
) -> Result<String> {
    let max_bundle_bytes = parse_max_workspace_bundle_bytes_from_env()?;
    capture_workspace_object_ref_with_limit(artifact_store, workspace_dir, max_bundle_bytes)
}

pub(crate) fn capture_workspace_object_ref_with_limit(
    artifact_store: &ArtifactStore,
    workspace_dir: &Path,
    max_bundle_bytes: u64,
) -> Result<String> {
    let mut files: Vec<Value> = Vec::new();
    let mut total_bytes = 0u64;
    if workspace_dir.exists() {
        let walker = walkdir::WalkDir::new(workspace_dir).into_iter();
        for entry in walker {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let rel_path = entry
                .path()
                .strip_prefix(workspace_dir)
                .unwrap_or(entry.path());
            if is_workspace_evidence_excluded(rel_path) {
                continue;
            }
            let size_bytes = entry.metadata()?.len();
            total_bytes = total_bytes.saturating_add(size_bytes);
            if total_bytes > max_bundle_bytes {
                return Err(anyhow!(
                    "workspace bundle capture exceeded {} bytes while reading '{}' (current_total_bytes={} env_var={}): persistent workspace state stores full file contents; reduce workspace size, exclude large generated files, switch to isolate_per_trial, or raise the limit explicitly",
                    max_bundle_bytes,
                    rel_path.to_string_lossy(),
                    total_bytes,
                    AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES_ENV
                ));
            }
            let bytes = fs::read(entry.path())?;
            #[cfg(unix)]
            let executable = entry.metadata()?.permissions().mode() & 0o111 != 0;
            #[cfg(not(unix))]
            let executable = false;
            files.push(json!({
                "path": rel_path.to_string_lossy().to_string(),
                "encoding": "base64",
                "content": BASE64_STANDARD.encode(bytes),
                "executable": executable,
            }));
        }
    }
    files.sort_by(|a, b| {
        a.get("path")
            .and_then(Value::as_str)
            .unwrap_or("")
            .cmp(b.get("path").and_then(Value::as_str).unwrap_or(""))
    });
    let payload = json!({
        "schema_version": "workspace_bundle_v1",
        "captured_at": Utc::now().to_rfc3339(),
        "files": files,
    });
    let bytes = serde_json::to_vec_pretty(&payload)?;
    artifact_store.put_bytes(&bytes)
}

pub(crate) fn restore_workspace_from_object_ref(
    artifact_store: &ArtifactStore,
    object_ref: &str,
    workspace_dir: &Path,
) -> Result<()> {
    let payload = artifact_store.read_ref(object_ref)?;
    let bundle: Value = serde_json::from_slice(&payload)?;
    if bundle.get("schema_version").and_then(Value::as_str) != Some("workspace_bundle_v1") {
        return Err(anyhow!(
            "unsupported workspace bundle schema for {}",
            object_ref
        ));
    }
    if workspace_dir.exists() {
        fs::remove_dir_all(workspace_dir)?;
    }
    ensure_dir(workspace_dir)?;
    let files = bundle
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("workspace bundle missing files array"))?;
    for row in files {
        let path = row
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("workspace bundle row missing path"))?;
        let rel = crate::runtime::validate_workspace_relative_path(path)?;
        let content = row
            .get("content")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("workspace bundle row missing content"))?;
        let bytes = BASE64_STANDARD
            .decode(content.as_bytes())
            .map_err(|err| anyhow!("workspace bundle base64 decode failed: {}", err))?;
        let host_path = workspace_dir.join(rel);
        atomic_write_bytes(&host_path, &bytes)?;
        #[cfg(unix)]
        if row
            .get("executable")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let metadata = fs::metadata(&host_path)?;
            let mut perms = metadata.permissions();
            perms.set_mode(perms.mode() | 0o111);
            fs::set_permissions(&host_path, perms)?;
        }
    }
    Ok(())
}

pub(crate) fn resolve_chain_label(
    task_payload: &Value,
    task_id: &str,
    state_policy: StatePolicy,
) -> String {
    let explicit = task_payload
        .get("chain_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    if let Some(label) = explicit {
        return label;
    }
    match state_policy {
        StatePolicy::PersistPerTask => task_id.to_string(),
        StatePolicy::Accumulate => "global".to_string(),
        StatePolicy::IsolatePerTrial => task_id.to_string(),
    }
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

pub(crate) fn sync_host_workspace_to_container(
    docker: &DockerRuntime,
    handle: &ContainerHandle,
    trial_dir: &Path,
    workdir: &str,
    label: &str,
    timeout_ms: u64,
) -> Result<()> {
    validate_container_workspace_path(workdir)?;
    run_shell_checked(
        docker,
        handle,
        trial_dir,
        label,
        &format!(
            "mkdir -p {dest}\nfind {dest} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +\ncp -R {src}/. {dest}",
            dest = shell_quote(workdir),
            src = shell_quote(WORKSPACE_SOURCE_MOUNT_PATH),
        ),
        None,
        timeout_ms,
    )
}

pub(crate) fn sync_container_workspace_to_host(
    docker: &DockerRuntime,
    handle: &ContainerHandle,
    trial_dir: &Path,
    workdir: &str,
    host_workspace: &Path,
) -> Result<()> {
    let staging_root = trial_dir.join("runtime").join("workspace_sync");
    docker
        .copy_from_container(handle, workdir, &staging_root)
        .with_context(|| format!("failed to copy container workspace {} to host", workdir))?;

    let copied_root = {
        let nested_candidate = workdir
            .split('/')
            .filter(|segment| !segment.is_empty())
            .fold(staging_root.clone(), |acc, segment| acc.join(segment));
        if nested_candidate.exists() {
            nested_candidate
        } else {
            let source_name = Path::new(workdir)
                .file_name()
                .and_then(|value| value.to_str());
            let entries = fs::read_dir(&staging_root)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .with_context(|| format!("failed to read {}", staging_root.display()))?;
            if entries.len() == 1 {
                let entry = &entries[0];
                if source_name.is_some_and(|value| entry.file_name() == value) {
                    entry.path()
                } else {
                    staging_root.clone()
                }
            } else {
                staging_root.clone()
            }
        }
    };

    if host_workspace.exists() {
        fs::remove_dir_all(host_workspace)?;
    }
    if let Some(parent) = host_workspace.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::rename(&copied_root, host_workspace).with_context(|| {
        format!(
            "failed to move copied workspace {} into {}",
            copied_root.display(),
            host_workspace.display()
        )
    })?;
    if copied_root != staging_root && staging_root.exists() {
        fs::remove_dir_all(&staging_root)?;
    }
    Ok(())
}
