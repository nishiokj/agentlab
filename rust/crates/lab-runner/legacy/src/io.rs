use crate::*;

pub(crate) struct PreparedTaskEnvironment {
    pub(crate) manifest: PreparedTaskEnvironmentManifest,
    pub(crate) trial_paths: TrialPaths,
    pub(crate) io_paths: PreparedTrialIo,
    pub(crate) dynamic_mounts: Vec<ResolvedMountReference>,
    pub(crate) trial_input: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskBoundaryMaterialization {
    pub(crate) declaration: Value,
    pub(crate) task_payload: Value,
    pub(crate) workspace: WorkspaceSpec,
    pub(crate) dependencies: Value,
    pub(crate) materialization: TaskMaterializationSpec,
    pub(crate) task_id: String,
    pub(crate) task_image: String,
    pub(crate) task_workdir: String,
    pub(crate) time_limit_ms: Option<u64>,
}

const DEFAULT_TASK_WORKDIR_FALLBACK: &str = "/workspace";

pub(crate) fn parse_task_row(task: &Value) -> Result<TaskRow> {
    let obj = task
        .as_object()
        .ok_or_else(|| anyhow!("task row must be an object"))?;
    if obj.get("schema_version").and_then(Value::as_str) != Some("task_row_v1") {
        return Err(anyhow!("task row schema_version must be 'task_row_v1'"));
    }
    let task_row: TaskRow =
        serde_json::from_value(task.clone()).map_err(|e| anyhow!("invalid task row: {}", e))?;
    validate_task_row(&task_row)?;
    Ok(task_row)
}

pub(crate) fn materialize_task_row(task_row: TaskRow) -> TaskBoundaryMaterialization {
    TaskBoundaryMaterialization {
        declaration: serde_json::to_value(&task_row).unwrap_or_else(|_| json!({})),
        task_payload: task_row.task.clone(),
        workspace: WorkspaceSpec {
            mode: WorkspaceMode::Scratch,
            base: WorkspaceBaseSpec {
                kind: WorkspaceBaseKind::Empty,
                dataset_pack_ref: None,
                repo: None,
                commit: None,
            },
            overlays: Vec::new(),
            aux_mounts: Vec::new(),
        },
        dependencies: json!({}),
        materialization: task_row.materialization.clone(),
        task_id: task_row.task_id(0),
        task_image: task_row.image.clone(),
        task_workdir: task_row.workdir.clone(),
        time_limit_ms: task_row.time_limit_ms,
    }
}

pub(crate) fn materialize_packaged_task_boundary(
    task: &Value,
) -> Result<TaskBoundaryMaterialization> {
    match task.get("schema_version").and_then(Value::as_str) {
        Some("task_row_v1") => Ok(materialize_task_row(parse_task_row(task)?)),
        Some(other) => Err(anyhow!(
            "packaged task schema_version '{}' is not supported at runtime; expected 'task_row_v1'",
            other
        )),
        None => Err(anyhow!(
            "packaged task row missing schema_version; expected 'task_row_v1'"
        )),
    }
}

pub(crate) fn parse_task_boundary_from_packaged_task(
    task: &Value,
) -> Result<TaskBoundaryMaterialization> {
    materialize_packaged_task_boundary(task)
}

pub(crate) fn validate_task_row(task_row: &TaskRow) -> Result<()> {
    if task_row.id.trim().is_empty() {
        return Err(anyhow!("task row field 'id' must be a non-empty string"));
    }
    if task_row.image.trim().is_empty() {
        return Err(anyhow!("task row field 'image' must be a non-empty string"));
    }
    if task_row.workdir.trim().is_empty() {
        return Err(anyhow!(
            "task row field 'workdir' must be a non-empty string"
        ));
    }
    if !Path::new(task_row.workdir.trim()).is_absolute() {
        return Err(anyhow!("task row field 'workdir' must be an absolute path"));
    }
    if !task_row.task.is_object() {
        return Err(anyhow!("task row field 'task' must be an object"));
    }
    if task_row.time_limit_ms == Some(0) {
        return Err(anyhow!(
            "task row field 'time_limit_ms' must be > 0 when provided"
        ));
    }
    match task_row.materialization.kind {
        TaskMaterializationKind::TaskImage => {
            if task_row.materialization.task_bundle_ref.is_some() {
                return Err(anyhow!(
                    "task row materialization.kind='task_image' does not allow task_bundle_ref"
                ));
            }
        }
        TaskMaterializationKind::BaseImageBundle => {
            let _task_bundle_ref = task_row
                .materialization
                .task_bundle_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "task row materialization.task_bundle_ref is required for base_image_bundle"
                    )
                })?;
        }
    }
    Ok(())
}

pub(crate) fn validate_task_boundary_workspace_materialization(
    task_boundary: &TaskBoundaryMaterialization,
) -> Result<()> {
    if task_boundary.workspace.mode != WorkspaceMode::Patch {
        return Ok(());
    }
    if task_boundary.workspace.base.kind != WorkspaceBaseKind::Empty {
        return Ok(());
    }
    let task_id = task_boundary.task_id.as_str();
    Err(anyhow!(
        "task '{}' uses workspace.mode='patch' but workspace.base.kind='empty'; patch tasks require a real base (dataset_pack or git_checkout)",
        task_id
    ))
}

pub(crate) fn validate_workspace_relative_path(path: &str) -> Result<PathBuf> {
    if path.trim().is_empty() {
        return Err(anyhow!("path cannot be empty"));
    }
    let p = Path::new(path);
    if p.is_absolute() {
        return Err(anyhow!("path must be relative to the task workdir"));
    }
    let mut normalized = PathBuf::new();
    for component in p.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(seg) => normalized.push(seg),
            Component::ParentDir => {
                return Err(anyhow!("path cannot contain '..'"));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(anyhow!("path cannot be absolute"));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(anyhow!("path cannot resolve to empty"));
    }
    Ok(normalized)
}

pub(crate) fn validate_container_workspace_path(path: &str) -> Result<()> {
    let p = Path::new(path);
    if !p.is_absolute() {
        return Err(anyhow!("mount_path must be absolute"));
    }
    for component in p.components() {
        if matches!(component, Component::ParentDir) {
            return Err(anyhow!("mount_path cannot contain '..'"));
        }
    }
    Ok(())
}

pub(crate) fn parse_dataset_pack_ref_digest(dataset_pack_ref: &str) -> Result<String> {
    let digest = dataset_pack_ref
        .strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("dataset_pack_ref must start with 'sha256:'"))?;
    if digest.len() != 64 || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("dataset_pack_ref digest must be 64 hex characters"));
    }
    Ok(digest.to_ascii_lowercase())
}

pub(crate) fn resolve_dataset_pack_host_path(
    project_root: &Path,
    dataset_pack_ref: &str,
) -> Result<PathBuf> {
    let digest = parse_dataset_pack_ref_digest(dataset_pack_ref)?;
    let path = project_root
        .join(".lab")
        .join("dataset_packs")
        .join("sha256")
        .join(digest);
    if !path.exists() {
        return Err(anyhow!("dataset pack not found: {}", path.display()));
    }
    Ok(path)
}

pub(crate) fn resolve_workspace_aux_mounts(
    project_root: &Path,
    aux_mounts: &[WorkspaceAuxMountSpec],
) -> Result<Vec<ResolvedMountReference>> {
    if aux_mounts.is_empty() {
        return Ok(Vec::new());
    }
    let mut mounts = Vec::with_capacity(aux_mounts.len());
    for mount in aux_mounts {
        let host_path = resolve_dataset_pack_host_path(project_root, &mount.dataset_pack_ref)?;
        mounts.push(ResolvedMountReference {
            host_path,
            mount_path: mount.mount_path.clone(),
        });
    }
    Ok(mounts)
}

pub(crate) fn git_repo_cache_dir(project_root: &Path, repo: &str) -> PathBuf {
    project_root
        .join(".lab")
        .join("git_checkouts")
        .join(sanitize_for_fs(repo))
}

pub(crate) fn git_checkout_clone_url(repo: &str) -> String {
    if repo.contains("://") || repo.starts_with("git@") {
        repo.to_string()
    } else {
        format!("https://github.com/{}.git", repo.trim_end_matches(".git"))
    }
}

pub(crate) fn git_commit_available(repo_dir: &Path, commit: &str) -> Result<bool> {
    let output = Command::new("git")
        .args([
            "-C",
            repo_dir.to_string_lossy().as_ref(),
            "rev-parse",
            "--verify",
            &format!("{}^{{commit}}", commit),
        ])
        .output()?;
    Ok(output.status.success())
}

pub(crate) fn ensure_git_checkout_cache(
    project_root: &Path,
    repo: &str,
    commit: &str,
) -> Result<PathBuf> {
    let cache_dir = git_repo_cache_dir(project_root, repo);
    if !cache_dir.exists() {
        if let Some(parent) = cache_dir.parent() {
            ensure_dir(parent)?;
        }
        let clone_url = git_checkout_clone_url(repo);
        let mut clone = Command::new("git");
        clone.args([
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            clone_url.as_str(),
            cache_dir.to_string_lossy().as_ref(),
        ]);
        run_checked_command(
            clone,
            &format!("failed to clone workspace.base git repo '{}'", repo),
        )?;
    }
    if !git_commit_available(&cache_dir, commit)? {
        let mut fetch = Command::new("git");
        fetch.args([
            "-C",
            cache_dir.to_string_lossy().as_ref(),
            "fetch",
            "--depth",
            "1",
            "origin",
            commit,
        ]);
        run_checked_command(
            fetch,
            &format!(
                "failed to fetch commit '{}' for workspace.base repo '{}'",
                commit, repo
            ),
        )?;
    }
    Ok(cache_dir)
}

pub(crate) fn git_checkout_staging_dir(project_root: &Path, repo: &str, commit: &str) -> PathBuf {
    project_root
        .join(".lab")
        .join("git_checkout_staging")
        .join(format!(
            "{}_{}_{}",
            sanitize_for_fs(repo),
            sanitize_for_fs(commit),
            Utc::now().timestamp_micros()
        ))
}

pub(crate) fn prepare_git_checkout_worktree(
    project_root: &Path,
    repo: &str,
    commit: &str,
) -> Result<(PathBuf, PathBuf)> {
    let cache_dir = ensure_git_checkout_cache(project_root, repo, commit)?;
    let staging_dir = git_checkout_staging_dir(project_root, repo, commit);
    if staging_dir.exists() {
        let _ = fs::remove_dir_all(&staging_dir);
    }
    if let Some(parent) = staging_dir.parent() {
        ensure_dir(parent)?;
    }

    let mut worktree_add = Command::new("git");
    worktree_add.args([
        "-C",
        cache_dir.to_string_lossy().as_ref(),
        "worktree",
        "add",
        "--detach",
        staging_dir.to_string_lossy().as_ref(),
        commit,
    ]);
    if let Err(err) = run_checked_command(
        worktree_add,
        &format!(
            "failed to stage workspace.base git checkout '{}' at '{}'",
            repo, commit
        ),
    ) {
        let _ = fs::remove_dir_all(&staging_dir);
        return Err(err);
    }
    Ok((cache_dir, staging_dir))
}

pub(crate) fn cleanup_git_checkout_worktree(cache_dir: &Path, staging_dir: &Path) -> Result<()> {
    let mut remove = Command::new("git");
    remove.args([
        "-C",
        cache_dir.to_string_lossy().as_ref(),
        "worktree",
        "remove",
        "--force",
        staging_dir.to_string_lossy().as_ref(),
    ]);
    if let Err(err) = run_checked_command(
        remove,
        &format!(
            "failed to clean staged workspace.base git checkout '{}'",
            staging_dir.display()
        ),
    ) {
        let _ = fs::remove_dir_all(staging_dir);
        return Err(err);
    }
    Ok(())
}

pub(crate) fn hydrate_git_checkout_cache(
    project_root: &Path,
    repo: &str,
    commit: &str,
) -> Result<PathBuf> {
    let (cache_dir, staging_dir) = prepare_git_checkout_worktree(project_root, repo, commit)?;
    cleanup_git_checkout_worktree(&cache_dir, &staging_dir)?;
    Ok(cache_dir)
}

pub(crate) fn materialize_workspace_git_checkout(
    project_root: &Path,
    paths: &TrialPaths,
    repo: &str,
    commit: &str,
) -> Result<()> {
    materialize_workspace_git_checkout_to_dir(project_root, &paths.workspace, repo, commit)
}

pub(crate) fn materialize_workspace_git_checkout_to_dir(
    project_root: &Path,
    workspace_dir: &Path,
    repo: &str,
    commit: &str,
) -> Result<()> {
    let (cache_dir, staging_dir) = prepare_git_checkout_worktree(project_root, repo, commit)?;
    if workspace_dir.exists() {
        fs::remove_dir_all(workspace_dir)?;
    }
    ensure_dir(workspace_dir)?;
    let result = copy_dir_filtered(&staging_dir, workspace_dir, &[".git"]);
    let cleanup_result = cleanup_git_checkout_worktree(&cache_dir, &staging_dir);
    result?;
    cleanup_result?;
    Ok(())
}

pub(crate) fn materialize_workspace_base_to_dir(
    project_root: &Path,
    workspace_dir: &Path,
    base: &WorkspaceBaseSpec,
) -> Result<()> {
    if workspace_dir.exists() {
        fs::remove_dir_all(workspace_dir)?;
    }
    ensure_dir(workspace_dir)?;
    match base.kind {
        WorkspaceBaseKind::Empty => Ok(()),
        WorkspaceBaseKind::DatasetPack => {
            let dataset_pack_ref = base
                .dataset_pack_ref
                .as_deref()
                .ok_or_else(|| anyhow!("workspace.base.dataset_pack_ref missing"))?;
            let source = resolve_dataset_pack_host_path(project_root, dataset_pack_ref)?;
            if !source.is_dir() {
                return Err(anyhow!(
                    "workspace.base dataset pack must resolve to a directory: {}",
                    source.display()
                ));
            }
            copy_dir_filtered(&source, workspace_dir, &[])?;
            Ok(())
        }
        WorkspaceBaseKind::GitCheckout => {
            let repo = base
                .repo
                .as_deref()
                .ok_or_else(|| anyhow!("workspace.base.repo missing"))?;
            let commit = base
                .commit
                .as_deref()
                .ok_or_else(|| anyhow!("workspace.base.commit missing"))?;
            materialize_workspace_git_checkout_to_dir(project_root, workspace_dir, repo, commit)
        }
    }
}

pub(crate) fn materialize_workspace_base(
    project_root: &Path,
    paths: &TrialPaths,
    base: &WorkspaceBaseSpec,
) -> Result<()> {
    materialize_workspace_base_to_dir(project_root, &paths.workspace, base)
}

pub(crate) fn materialize_workspace_overlays_to_dir(
    workspace_dir: &Path,
    workspace_overlays: &[WorkspaceOverlaySpec],
) -> Result<()> {
    for file in workspace_overlays {
        let rel = validate_workspace_relative_path(&file.path)?;
        let host_path = workspace_dir.join(rel);
        let bytes = match file.encoding.as_deref() {
            None | Some("utf8") => file.content.as_bytes().to_vec(),
            Some("base64") => BASE64_STANDARD
                .decode(file.content.as_bytes())
                .map_err(|e| {
                    anyhow!(
                        "failed to decode base64 workspace overlay '{}': {}",
                        file.path,
                        e
                    )
                })?,
            Some(other) => {
                return Err(anyhow!(
                    "unsupported workspace overlay encoding '{}' for '{}'",
                    other,
                    file.path
                ));
            }
        };
        atomic_write_bytes(&host_path, &bytes)?;
        #[cfg(unix)]
        if file.executable {
            let metadata = fs::metadata(&host_path)?;
            let mut perms = metadata.permissions();
            perms.set_mode(perms.mode() | 0o111);
            fs::set_permissions(&host_path, perms)?;
        }
    }
    Ok(())
}

pub(crate) fn materialize_workspace_overlays(
    paths: &TrialPaths,
    workspace_overlays: &[WorkspaceOverlaySpec],
) -> Result<()> {
    materialize_workspace_overlays_to_dir(&paths.workspace, workspace_overlays)
}

pub(crate) fn copy_staged_host_path(
    src: &Path,
    dst: &Path,
    required: bool,
    label: &str,
) -> Result<bool> {
    if !src.exists() {
        if required {
            return Err(anyhow!(
                "staged host path source missing for {}: {}",
                label,
                src.display()
            ));
        }
        return Ok(false);
    }
    if let Some(parent) = dst.parent() {
        ensure_dir(parent)?;
    }
    if src.is_dir() {
        ensure_dir(dst)?;
        copy_dir_filtered(src, dst, &[]).map_err(|e| {
            anyhow!(
                "failed to copy staged host directory {} from {} to {}: {}",
                label,
                src.display(),
                dst.display(),
                e
            )
        })?;
        return Ok(true);
    }
    if !src.is_file() {
        return Err(anyhow!(
            "staged host path source is not a file or directory for {}: {}",
            label,
            src.display()
        ));
    }
    fs::copy(src, dst).map_err(|e| {
        anyhow!(
            "failed to copy staged host file {} from {} to {}: {}",
            label,
            src.display(),
            dst.display(),
            e
        )
    })?;
    Ok(true)
}

#[cfg(unix)]
pub(crate) fn set_staged_path_read_only(dst: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if dst.is_dir() {
        for entry in walkdir::WalkDir::new(dst)
            .into_iter()
            .filter_map(|entry| entry.ok())
        {
            let entry_path = entry.path();
            let mut perms = fs::metadata(entry_path)?.permissions();
            perms.set_mode(if entry.file_type().is_dir() {
                0o555
            } else {
                0o444
            });
            fs::set_permissions(entry_path, perms)?;
        }
        return Ok(());
    }

    let mut perms = fs::metadata(dst)?.permissions();
    perms.set_mode(0o444);
    fs::set_permissions(dst, perms)?;
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn set_staged_path_read_only(_dst: &Path) -> Result<()> {
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TaskDependencyFileSpec {
    path: String,
    content: String,
    #[serde(default = "default_task_dependency_encoding")]
    encoding: String,
    #[serde(default)]
    executable: bool,
}

pub(crate) fn default_task_dependency_encoding() -> String {
    "utf8".to_string()
}

pub(crate) fn parse_task_dependency_files_value(
    dependencies: &Value,
) -> Result<Vec<TaskDependencyFileSpec>> {
    let Some(files) = dependencies.get("files").filter(|value| !value.is_null()) else {
        return Ok(Vec::new());
    };
    serde_json::from_value(files.clone())
        .map_err(|err| anyhow!("invalid task dependencies.files: {}", err))
}

pub(crate) fn materialize_task_dependencies_to_dir(
    dependencies: &Value,
    destination_dir: &Path,
) -> Result<()> {
    for (idx, spec) in parse_task_dependency_files_value(dependencies)?
        .iter()
        .enumerate()
    {
        let rel = validate_workspace_relative_path(&spec.path).map_err(|err| {
            anyhow!(
                "task dependencies.files[{}].path '{}' invalid: {}",
                idx,
                spec.path,
                err
            )
        })?;
        let bytes = match spec.encoding.as_str() {
            "utf8" => spec.content.as_bytes().to_vec(),
            "base64" => BASE64_STANDARD
                .decode(spec.content.as_bytes())
                .map_err(|err| {
                    anyhow!(
                        "task dependencies.files[{}] base64 decode failed: {}",
                        idx,
                        err
                    )
                })?,
            other => {
                return Err(anyhow!(
                    "task dependencies.files[{}].encoding must be 'utf8' or 'base64' (got '{}')",
                    idx,
                    other
                ))
            }
        };
        let dst = destination_dir.join(rel);
        atomic_write_bytes(&dst, &bytes)?;
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(&dst)?.permissions();
            perms.set_mode(if spec.executable { 0o555 } else { 0o444 });
            fs::set_permissions(&dst, perms)?;
        }
    }
    Ok(())
}

pub(crate) fn materialize_task_dependencies_for_trial(
    task_boundary: &TaskBoundaryMaterialization,
    paths: &TrialPaths,
) -> Result<()> {
    materialize_task_dependencies_to_dir(
        &task_boundary.dependencies,
        &paths.workspace.join(AGENTLAB_RUNNER_SUPPORT_REL_DIR),
    )
}

pub(crate) fn container_workspace_rel_path(mount_path: &str) -> Result<PathBuf> {
    validate_container_workspace_path(mount_path)?;
    if mount_path == "/" {
        return Ok(PathBuf::new());
    }
    let rel = mount_path
        .strip_prefix('/')
        .ok_or_else(|| anyhow!("mount_path must be absolute"))?;
    validate_workspace_relative_path(rel)
}

pub(crate) fn materialize_workspace_aux_mounts_to_dir(
    project_root: &Path,
    aux_mounts: &[WorkspaceAuxMountSpec],
    workspace_dir: &Path,
) -> Result<()> {
    for mount in aux_mounts {
        let host_path = resolve_dataset_pack_host_path(project_root, &mount.dataset_pack_ref)?;
        let rel = container_workspace_rel_path(&mount.mount_path)?;
        let dst = workspace_dir.join(rel);
        if dst.exists() {
            remove_path_if_exists(&dst)?;
        }
        copy_staged_host_path(&host_path, &dst, true, "task_bundle_aux_mount")?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn parse_workspace_patches(
    value: Option<&Value>,
    exp_dir: &Path,
) -> Result<Vec<WorkspacePatchSpec>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let items = value
        .as_array()
        .ok_or_else(|| anyhow!("workspace_patches must be an array"))?;
    let mut patches = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| anyhow!("workspace_patches[{}] must be an object", idx))?;
        let source = obj
            .get("source_from_host")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("workspace_patches[{}].source_from_host is required", idx))?;
        let target = obj
            .get("target_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("workspace_patches[{}].target_path is required", idx))?;
        patches.push(WorkspacePatchSpec {
            source_from_host: normalize_path(&exp_dir.join(source)),
            target_path: target.to_string(),
        });
    }
    Ok(patches)
}

pub(crate) fn stage_dependencies_for_trial(
    runtime: &AgentRuntimeConfig,
    paths: &TrialPaths,
) -> Result<()> {
    for entry in &runtime.dependency_file_staging {
        let dst = map_container_path_to_host(&entry.destination_path, paths)?;
        let copied = copy_staged_host_path(
            &entry.source_from_host,
            &dst,
            entry.required,
            &entry.destination_path,
        )?;
        if copied && entry.read_only {
            set_staged_path_read_only(&dst)?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn stage_workspace_patches_for_trial(
    runtime: &AgentRuntimeConfig,
    paths: &TrialPaths,
) -> Result<()> {
    for patch in &runtime.workspace_patches {
        let rel = validate_workspace_relative_path(&patch.target_path)?;
        let dst = paths.workspace.join(rel);
        copy_staged_host_path(&patch.source_from_host, &dst, true, &patch.target_path)?;
    }
    Ok(())
}

pub(crate) const TASK_WORKDIR_TEMPLATE_PLACEHOLDER: &str = AGENTLAB_TASK_WORKDIR_PLACEHOLDER;

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

#[derive(Debug, Clone)]
pub(crate) struct DependencyFileStagingSpec {
    pub(crate) source_from_host: PathBuf,
    pub(crate) destination_path: String,
    pub(crate) required: bool,
    pub(crate) read_only: bool,
}

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

pub(crate) fn preview_agent_command(profile: &VariantRuntimeProfile) -> Vec<String> {
    let mut command = profile.agent_runtime.command_raw.clone();
    command.extend(profile.variant_args.iter().cloned());
    command
}

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

#[derive(Debug)]
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
            copy_dir_filtered(
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
        remove_path_if_exists(&self.scratch_dir)
    }
}

impl Drop for TrialPaths {
    fn drop(&mut self) {
        let _ = remove_path_if_exists(&self.scratch_dir);
    }
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

pub(crate) fn build_task_sandbox_plan(
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
    Ok(manifest)
}

pub(crate) fn resolve_task_bundle_host_path(
    project_root: &Path,
    trial_dir: &Path,
    task_bundle_ref: &str,
) -> Result<PathBuf> {
    let raw = task_bundle_ref.trim();
    if raw.is_empty() {
        return Err(anyhow!("task bundle ref cannot be empty"));
    }
    let bundle_path = Path::new(raw);
    if bundle_path.is_absolute() {
        return Ok(bundle_path.to_path_buf());
    }
    if let Some(run_dir) = infer_run_dir_from_path(trial_dir) {
        let run_candidate = run_dir.join(bundle_path);
        if run_candidate.exists() {
            return Ok(run_candidate);
        }
    }
    let project_candidate = project_root.join(bundle_path);
    if project_candidate.exists() {
        return Ok(project_candidate);
    }
    Err(anyhow!(
        "task bundle ref '{}' could not be resolved relative to run_dir or project_root",
        task_bundle_ref
    ))
}

pub(crate) fn materialize_task_bundle_for_trial(
    project_root: &Path,
    trial_dir: &Path,
    paths: &TrialPaths,
    task_boundary: &TaskBoundaryMaterialization,
) -> Result<()> {
    let task_bundle_ref = task_boundary
        .materialization
        .task_bundle_ref
        .as_deref()
        .ok_or_else(|| {
            anyhow!(
                "task '{}' is missing materialization.task_bundle_ref for base_image_bundle",
                task_boundary.task_id
            )
        })?;
    let source = resolve_task_bundle_host_path(project_root, trial_dir, task_bundle_ref)?;
    if paths.workspace.exists() {
        fs::remove_dir_all(&paths.workspace)?;
    }
    ensure_dir(&paths.workspace)?;
    if source.is_dir() {
        copy_dir_filtered(&source, &paths.workspace, &[])?;
        return Ok(());
    }
    if !source.is_file() {
        return Err(anyhow!(
            "task bundle source is not a file or directory: {}",
            source.display()
        ));
    }
    let Some(tar_flag) = agent_artifact_archive_flag(&source) else {
        return Err(anyhow!(
            "task bundle archive must use .tar/.tar.gz/.tgz: {}",
            source.display()
        ));
    };
    let bundle_arg = source.to_string_lossy().to_string();
    let workspace_arg = paths.workspace.to_string_lossy().to_string();
    let unpack_out = Command::new("tar")
        .args([tar_flag, bundle_arg.as_str(), "-C", workspace_arg.as_str()])
        .output()?;
    if !unpack_out.status.success() {
        return Err(anyhow!(
            "failed to unpack task bundle {}: {}",
            source.display(),
            output_error_detail(&unpack_out)
        ));
    }
    Ok(())
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
    existing_workspace_ref: Option<&str>,
) -> Result<PreparedTaskEnvironment> {
    trial_paths.prepare(false)?;
    if let Some(workspace_ref) = existing_workspace_ref {
        let artifact_store = ArtifactStore::new(
            infer_run_dir_from_path(trial_dir)
                .unwrap_or_else(|| trial_dir.to_path_buf())
                .join("artifacts"),
        );
        restore_workspace_from_object_ref(&artifact_store, workspace_ref, &trial_paths.workspace)?;
    } else {
        match task_boundary.materialization.kind {
            TaskMaterializationKind::TaskImage => {
                materialize_workspace_base(
                    project_root,
                    &trial_paths,
                    &task_boundary.workspace.base,
                )?;
                materialize_workspace_overlays(&trial_paths, &task_boundary.workspace.overlays)?;
            }
            TaskMaterializationKind::BaseImageBundle => {
                materialize_task_bundle_for_trial(
                    project_root,
                    trial_dir,
                    &trial_paths,
                    task_boundary,
                )?;
            }
        }
    }
    if matches!(
        task_boundary.materialization.kind,
        TaskMaterializationKind::TaskImage
    ) {
        materialize_task_dependencies_for_trial(task_boundary, &trial_paths)?;
    }
    stage_dependencies_for_trial(agent_runtime, &trial_paths)?;
    let dynamic_mounts = if matches!(
        task_boundary.materialization.kind,
        TaskMaterializationKind::TaskImage
    ) {
        resolve_workspace_aux_mounts(project_root, &task_boundary.workspace.aux_mounts)?
    } else {
        Vec::new()
    };

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
    existing_workspace_ref: Option<&str>,
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
        existing_workspace_ref,
    )
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

    // Canonicalize to task.input.prompt for runtime/harness consumption.
    let input_slot = root_obj
        .entry("input".to_string())
        .or_insert_with(|| json!({}));
    if !input_slot.is_object() {
        *input_slot = json!({});
    }
    if let Some(input_obj) = input_slot.as_object_mut() {
        input_obj.insert("prompt".to_string(), Value::String(prompt.clone()));
    }

    // Drop duplicated top-level prompt alias if it is identical.
    let drop_top_level_prompt = root_obj
        .get("prompt")
        .and_then(Value::as_str)
        .is_some_and(|value| value == prompt);
    if drop_top_level_prompt {
        root_obj.remove("prompt");
    }

    // Drop duplicated swebench.input.prompt alias if it is identical.
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

pub(crate) fn sanitize_for_fs(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "chain".to_string()
    } else {
        out
    }
}

pub(crate) fn append_jsonl_file(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

pub(crate) fn append_jsonl(path: &Path, value: &Value) -> Result<()> {
    let mut row = value.clone();
    if let (Some(run_dir), Some(table)) = (
        infer_run_dir_from_path(path),
        json_row_table_from_path(path),
    ) {
        if !path_uses_sqlite_json_row_ingest(&run_dir, path) {
            validate_schema_contract_value(
                &row,
                format!("jsonl row append for {}", path.display()).as_str(),
            )?;
            return append_jsonl_file(path, &row);
        }
        if row.pointer("/run_id").is_none() {
            if let Some(control) =
                BackingSqliteStore::open(&run_dir)?.get_runtime_json(RUNTIME_KEY_RUN_CONTROL)?
            {
                if let Some(run_id) = control.pointer("/run_id").and_then(Value::as_str) {
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert("run_id".to_string(), json!(run_id));
                    }
                }
            }
        }
        validate_schema_contract_value(
            &row,
            format!("jsonl row append for {}", path.display()).as_str(),
        )?;
        if row_has_sqlite_identity_fields(&row) {
            let mut store = BackingSqliteStore::open(&run_dir)?;
            return store.upsert_json_row(table, &row);
        }
        return Err(anyhow!(
            "jsonl append rejected for {}: missing sqlite identity fields (run_id, schedule_idx, attempt, row_seq, slot_commit_id)",
            path.display()
        ));
    }
    Err(anyhow!(
        "jsonl append rejected for {}: path is not mapped to a sqlite json row table",
        path.display()
    ))
}

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

pub(crate) fn snapshot_file_map(snapshot_manifest: &Value) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Some(arr) = snapshot_manifest.get("files").and_then(|v| v.as_array()) {
        for row in arr {
            let path = row.get("path").and_then(|v| v.as_str());
            let digest = row.get("digest").and_then(|v| v.as_str());
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
        let rel = validate_workspace_relative_path(path)?;
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

pub(crate) fn resolve_trial_timeout_ms(input: &Value) -> Option<u64> {
    input.pointer("/policy/timeout_ms").and_then(|v| v.as_u64())
}

pub(crate) fn output_peer_path(output_path: &str, file_name: &str) -> String {
    let output = Path::new(output_path);
    if let Some(parent) = output.parent() {
        return parent.join(file_name).to_string_lossy().to_string();
    }
    file_name.to_string()
}

pub(crate) fn build_runtime_contract_env(
    run_id: &str,
    input: &Value,
    io: &PreparedTrialIo,
    task_image: Option<&str>,
    timeout_ms: Option<u64>,
) -> BTreeMap<String, String> {
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

    let mut env = BTreeMap::new();
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
    env.insert(AGENTLAB_ENV_REPL_IDX.to_string(), repl_idx.to_string());
    if let Some(timeout_ms) = timeout_ms {
        env.insert(AGENTLAB_ENV_TIMEOUT_MS.to_string(), timeout_ms.to_string());
    }
    env
}

pub(crate) struct ResolvedGradingPhase {
    pub(crate) image: String,
    pub(crate) workdir: String,
    pub(crate) command: Vec<String>,
    pub(crate) extra_mounts: Vec<ResolvedMountReference>,
    pub(crate) injected_bundle_host_path: Option<PathBuf>,
    pub(crate) injected_copy_dest: Option<String>,
}

pub(crate) fn resolve_grading_bundle_host_path(
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

pub(crate) fn resolve_task_sandbox_image(request: &AdapterRunRequest<'_>) -> Result<String> {
    let image = request.task_image.trim();
    if image.is_empty() {
        return Err(anyhow!("task image is required for task sandbox"));
    }
    Ok(image.to_string())
}

pub(crate) fn resolve_container_workspace<'a>(
    request: &'a AdapterRunRequest<'_>,
) -> Result<&'a str> {
    let workdir = request.task_workdir.trim();
    if workdir.is_empty() {
        return Err(anyhow!("task workdir is required for task sandbox"));
    }
    Ok(workdir)
}

pub(crate) fn run_checked_command(mut cmd: Command, step: &str) -> Result<std::process::Output> {
    let out = cmd.output()?;
    if out.status.success() {
        return Ok(out);
    }
    let detail = output_error_detail(&out);
    Err(anyhow!("{}: {}", step, detail))
}

pub(crate) fn output_error_detail(out: &Output) -> String {
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        "command exited non-zero".to_string()
    }
}

#[cfg(test)]
pub(crate) fn resolve_local_image_alias(image: &str) -> Option<String> {
    image
        .strip_prefix("swebench/")
        .filter(|candidate| candidate.starts_with("sweb.eval."))
        .map(ToString::to_string)
}

pub(crate) fn resolve_container_platform(image: &str) -> Option<&'static str> {
    let normalized = image.strip_prefix("swebench/").unwrap_or(image);
    if normalized.starts_with("sweb.eval.x86_64.") {
        return Some("linux/amd64");
    }
    if normalized.starts_with("sweb.eval.aarch64.") || normalized.starts_with("sweb.eval.arm64.") {
        return Some("linux/arm64");
    }
    None
}

pub(crate) fn resolve_container_image_digest(image: &str) -> Option<String> {
    let runtime = crate::backend::docker::DockerRuntime::connect().ok()?;
    let metadata = runtime.ensure_image(image).ok()?;
    metadata
        .repo_digests
        .first()
        .and_then(|value| value.rsplit_once('@').map(|(_, digest)| digest.to_string()))
        .or(metadata.image_id)
}

pub(crate) fn agent_artifact_cache_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn repair_agent_artifact_layout(unpacked_dir: &Path) -> Result<()> {
    let packages_root = unpacked_dir.join("packages");
    let nested_packages_root = packages_root.join("packages");
    if !packages_root.is_dir() || !nested_packages_root.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(&nested_packages_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let shim_path = packages_root.join(&name);
        if shim_path.exists() {
            continue;
        }
        let nested_rel = Path::new("packages").join(&name);
        let nested_abs = packages_root.join(&nested_rel);
        if !nested_abs.exists() {
            continue;
        }
        symlink(&nested_rel, &shim_path).map_err(|err| {
            anyhow!(
                "failed to create artifact layout shim {} -> {}: {}",
                shim_path.display(),
                nested_rel.display(),
                err
            )
        })?;
    }
    Ok(())
}

pub(crate) fn resolve_agent_artifact_mount_dir(artifact: &Path) -> Result<PathBuf> {
    if artifact.is_dir() {
        return Ok(fs::canonicalize(artifact).unwrap_or_else(|_| artifact.to_path_buf()));
    }
    if !artifact.exists() {
        return Err(anyhow!(
            "runtime.agent_runtime.artifact not found: {}",
            artifact.display()
        ));
    }
    if !artifact.is_file() {
        return Err(anyhow!(
            "runtime.agent_runtime.artifact must be a file or directory: {}",
            artifact.display()
        ));
    }
    let artifact_path = fs::canonicalize(artifact).unwrap_or_else(|_| artifact.to_path_buf());
    let artifact_name = artifact_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let tar_flag = if artifact_name.ends_with(".tar.gz") || artifact_name.ends_with(".tgz") {
        "-xzf"
    } else if artifact_name.ends_with(".tar") {
        "-xf"
    } else {
        return Err(anyhow!(
            "runtime.agent_runtime.artifact '{}' must be a directory or .tar/.tar.gz archive",
            artifact_path.display()
        ));
    };

    let digest = sha256_file(&artifact_path)?;
    // Docker `-v` uses ':' as a delimiter in mount specs, so the host path
    // component must be colon-free.
    let digest_path_component = digest.replace(':', "_");
    let cache_root = artifact_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".agentlab_artifact_cache");
    ensure_dir(&cache_root)?;
    let unpacked_dir = cache_root.join(&digest_path_component);
    let ready_marker = unpacked_dir.join(".agentlab_ready");
    if ready_marker.exists() {
        repair_agent_artifact_layout(&unpacked_dir)?;
        return Ok(unpacked_dir);
    }

    let _guard = agent_artifact_cache_lock()
        .lock()
        .map_err(|_| anyhow!("agent artifact cache lock poisoned"))?;
    if ready_marker.exists() {
        repair_agent_artifact_layout(&unpacked_dir)?;
        return Ok(unpacked_dir);
    }

    if unpacked_dir.exists() {
        fs::remove_dir_all(&unpacked_dir)?;
    }
    let staging_dir = cache_root.join(format!(
        "{}.tmp.{}.{}",
        digest_path_component,
        std::process::id(),
        Utc::now().timestamp_micros()
    ));
    if staging_dir.exists() {
        let _ = fs::remove_dir_all(&staging_dir);
    }
    ensure_dir(&staging_dir)?;
    let unpack_out = Command::new("tar")
        .args([
            tar_flag,
            artifact_path.to_string_lossy().as_ref(),
            "-C",
            staging_dir.to_string_lossy().as_ref(),
        ])
        .output()?;
    if !unpack_out.status.success() {
        let _ = fs::remove_dir_all(&staging_dir);
        return Err(anyhow!(
            "failed to unpack runtime.agent_runtime.artifact {}: {}",
            artifact_path.display(),
            output_error_detail(&unpack_out),
        ));
    }
    if let Err(err) = fs::rename(&staging_dir, &unpacked_dir) {
        let _ = fs::remove_dir_all(&staging_dir);
        return Err(anyhow!(
            "failed to finalize unpacked runtime.agent_runtime.artifact {} into {}: {}",
            artifact_path.display(),
            unpacked_dir.display(),
            err
        ));
    }
    repair_agent_artifact_layout(&unpacked_dir)?;
    fs::write(&ready_marker, digest.as_bytes())?;
    Ok(unpacked_dir)
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

pub(crate) struct PreflightProbeRoot {
    path: PathBuf,
}

impl Drop for PreflightProbeRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub(crate) struct PreflightProbeContext {
    _root: PreflightProbeRoot,
    trial_paths: TrialPaths,
    io_paths: PreparedTrialIo,
    dynamic_mounts: Vec<ResolvedMountReference>,
    runtime_env: BTreeMap<String, String>,
    task_image: String,
    task_workdir: String,
    task_materialization_kind: TaskMaterializationKind,
}

pub(crate) fn create_preflight_probe_root(label: &str) -> Result<PreflightProbeRoot> {
    let root = std::env::temp_dir().join(format!(
        "agentlab_preflight_probe_{}_{}_{}",
        sanitize_for_fs(label),
        std::process::id(),
        Utc::now().timestamp_micros()
    ));
    ensure_dir(&root)?;
    Ok(PreflightProbeRoot { path: root })
}

pub(crate) fn select_preflight_probe_task(
    tasks: &[Value],
    image: &str,
) -> Result<(usize, TaskBoundaryMaterialization)> {
    let mut parse_errors = Vec::new();
    for (idx, task) in tasks.iter().enumerate() {
        let boundary = match parse_task_boundary_from_packaged_task(task) {
            Ok(boundary) => boundary,
            Err(err) => {
                parse_errors.push(format!("line {}: {}", idx + 1, err));
                continue;
            }
        };
        if boundary.task_image.as_str() == image {
            return Ok((idx, boundary));
        }
    }
    if !parse_errors.is_empty() {
        return Err(anyhow!(
            "failed to parse representative task boundary rows: {}",
            format_preview(&parse_errors, 3)
        ));
    }
    Err(anyhow!(
        "no representative task spec row found for image '{}'",
        image
    ))
}

pub(crate) fn build_preflight_probe_context(
    runtime_profile: &VariantRuntimeProfile,
    variant: &Variant,
    tasks: &[Value],
    image: &str,
    project_root: &Path,
) -> Result<PreflightProbeContext> {
    let (task_idx, task_boundary) = match select_preflight_probe_task(tasks, image) {
        Ok(selected) => selected,
        Err(err) if tasks.is_empty() => (
            0,
            TaskBoundaryMaterialization {
                declaration: json!({
                    "schema_version": "task_row_v1",
                    "id": "preflight_probe_task",
                    "image": image,
                    "workdir": "/workspace/task",
                    "task": {},
                    "materialization": { "kind": "task_image" }
                }),
                task_payload: json!({}),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::Scratch,
                    base: WorkspaceBaseSpec {
                        kind: WorkspaceBaseKind::Empty,
                        dataset_pack_ref: None,
                        repo: None,
                        commit: None,
                    },
                    overlays: Vec::new(),
                    aux_mounts: Vec::new(),
                },
                dependencies: json!({}),
                materialization: TaskMaterializationSpec {
                    kind: TaskMaterializationKind::TaskImage,
                    task_bundle_ref: None,
                },
                task_id: "preflight_probe_task".to_string(),
                task_image: image.to_string(),
                task_workdir: "/workspace/task".to_string(),
                time_limit_ms: None,
            },
        ),
        Err(err) => return Err(err),
    };
    let probe_root = create_preflight_probe_root(&format!("{}_{}", variant.id, image))?;
    let trial_dir = probe_root.path.join("trial_1");
    ensure_dir(&trial_dir)?;
    let prepared = prepare_task_environment(
        project_root,
        &trial_dir,
        "preflight_probe",
        "trial_preflight",
        &runtime_profile.experiment,
        variant,
        task_idx,
        0,
        &task_boundary,
        &runtime_profile.agent_runtime,
        None,
    )?;
    let probe_task_image = prepared.manifest.task_sandbox_image().to_string();
    let probe_task_workdir = prepared
        .manifest
        .task_sandbox_workdir()
        .unwrap_or(task_boundary.task_workdir.as_str())
        .to_string();
    let mut input = prepared.trial_input.clone();
    set_json_pointer_value(
        &mut input,
        "/runtime/control_plane/path",
        json!(DEFAULT_CONTAINER_CONTROL_PATH),
    )?;
    set_json_pointer_value(&mut input, "/runtime/control_plane/mode", json!("file"))?;
    let input_bytes = serde_json::to_vec_pretty(&input)?;
    let io_paths = prepare_io_paths(&prepared.trial_paths, &input_bytes)?;
    let smoke_timeout_ms = resolve_trial_timeout_ms(&input)
        .map(|value| value.min(DEFAULT_PREFLIGHT_CONTRACT_SMOKE_TIMEOUT_MS));
    let mut runtime_env = build_runtime_contract_env(
        "preflight_probe",
        &input,
        &io_paths,
        Some(probe_task_image.as_str()),
        smoke_timeout_ms,
    );
    runtime_env.insert(AGENTLAB_ENV_PREFLIGHT_SMOKE.to_string(), "1".to_string());
    Ok(PreflightProbeContext {
        _root: probe_root,
        trial_paths: prepared.trial_paths,
        io_paths,
        dynamic_mounts: prepared.dynamic_mounts,
        runtime_env,
        task_image: probe_task_image,
        task_workdir: probe_task_workdir,
        task_materialization_kind: task_boundary.materialization.kind.clone(),
    })
}

pub(crate) fn build_preflight_probe_request<'a>(
    context: &'a PreflightProbeContext,
    runtime_profile: &'a VariantRuntimeProfile,
    benchmark_grader: Option<&'a BenchmarkGraderConfig>,
    benchmark_grading_enabled: bool,
) -> AdapterRunRequest<'a> {
    AdapterRunRequest {
        runtime_experiment: &runtime_profile.experiment,
        runtime: &runtime_profile.agent_runtime,
        variant_args: &runtime_profile.variant_args,
        runtime_env: &context.runtime_env,
        runtime_overrides_env: &runtime_profile.agent_runtime_env,
        trial_paths: &context.trial_paths,
        dynamic_mounts: &context.dynamic_mounts,
        io_paths: &context.io_paths,
        network_mode: runtime_profile.effective_network_mode.as_str(),
        benchmark_grader,
        benchmark_grading_enabled,
        run_id: "preflight_probe",
        task_image: context.task_image.as_str(),
        task_workdir: context.task_workdir.as_str(),
        task_materialization_kind: context.task_materialization_kind.clone(),
        agent_artifact: runtime_profile
            .agent_runtime
            .agent_artifact
            .exists()
            .then_some(runtime_profile.agent_runtime.agent_artifact.as_path()),
    }
}

pub(crate) struct PreflightContractSmokeExecution {
    status: String,
    stdout: String,
    stderr: String,
}

pub(crate) fn read_optional_text_file(path: &Path) -> Result<String> {
    if !path.exists() {
        return Ok(String::new());
    }
    Ok(fs::read_to_string(path)?)
}

pub(crate) fn run_preflight_contract_smoke(
    request: &AdapterRunRequest<'_>,
) -> Result<PreflightContractSmokeExecution> {
    let prepared_manifest =
        load_prepared_task_environment_manifest(&request.trial_paths.trial_dir)?;
    let runtime_outcome = crate::trial::execution::execute_trial_runtime(
        &request.trial_paths.trial_dir,
        0,
        1,
        request,
        &prepared_manifest.task_id,
        &prepared_manifest.variant_id,
        prepared_manifest.repl_idx,
        prepared_manifest
            .task_sandbox_plan
            .as_ref()
            .ok_or_else(|| anyhow!("preflight probe missing task sandbox plan"))?,
    )?;
    let stdout =
        read_optional_text_file(&request.trial_paths.trial_dir.join("harness_stdout.log"))?;
    let stderr =
        read_optional_text_file(&request.trial_paths.trial_dir.join("harness_stderr.log"))?;
    Ok(PreflightContractSmokeExecution {
        status: runtime_outcome.agent_exit_status,
        stdout,
        stderr,
    })
}

pub(crate) fn detect_known_probe_output_blockers(stdout: &str, stderr: &str) -> Vec<String> {
    let mut blockers = Vec::new();
    for line in stdout.lines().chain(stderr.lines()) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        if lower.contains("cpu lacks avx support") {
            blockers.push(trimmed.to_string());
            continue;
        }
        if lower.contains("missing env var ") || lower.contains("fatal error: missing env var") {
            blockers.push(trimmed.to_string());
            continue;
        }
        if lower.contains("references tool") && lower.contains("which is not available") {
            blockers.push(trimmed.to_string());
        }
    }
    blockers.sort();
    blockers.dedup();
    blockers
}

pub(crate) fn summarize_preflight_failure_logs(stdout: &str, stderr: &str) -> Vec<String> {
    let mut lines = Vec::new();
    for line in stderr.lines().chain(stdout.lines()) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        lines.push(trimmed.to_string());
        if lines.len() >= 3 {
            break;
        }
    }
    lines
}

pub(crate) fn validate_preflight_result_payload(path: &Path) -> Vec<String> {
    let mut failures = Vec::new();
    if !path.exists() {
        failures.push(format!(
            "contract smoke did not write result payload: {}",
            path.display()
        ));
        return failures;
    }
    if !path.is_file() {
        failures.push(format!(
            "contract smoke result payload path is not a file: {}",
            path.display()
        ));
        return failures;
    }
    let raw = match fs::read_to_string(path) {
        Ok(value) => value,
        Err(err) => {
            failures.push(format!(
                "failed to read contract smoke result payload at {}: {}",
                path.display(),
                err
            ));
            return failures;
        }
    };
    if raw.trim().is_empty() {
        failures.push(format!(
            "contract smoke wrote an empty result payload: {}",
            path.display()
        ));
        return failures;
    }
    let value = match serde_json::from_str::<Value>(&raw) {
        Ok(value) => value,
        Err(err) => {
            failures.push(format!(
                "failed to parse agent result JSON at {}: {}",
                path.display(),
                err
            ));
            return failures;
        }
    };
    match value.pointer("/schema_version").and_then(Value::as_str) {
        Some("agent_result_v1") => {}
        Some(other) => failures.push(format!(
            "result payload schema_version was '{}', expected 'agent_result_v1' at {}",
            other,
            path.display()
        )),
        None => failures.push(format!(
            "result payload missing schema_version 'agent_result_v1' at {}",
            path.display()
        )),
    }
    if value
        .pointer("/outcome")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        failures.push(format!(
            "result payload missing non-empty outcome at {}",
            path.display()
        ));
    }
    failures
}

pub(crate) fn validate_preflight_benchmark_smoke_outputs(
    request: &AdapterRunRequest<'_>,
    status: &str,
) -> Vec<String> {
    let mut failures = Vec::new();
    let mapped_grader_output_path = request.trial_paths.out.join(MAPPED_GRADER_OUTPUT_FILENAME);
    let grade_error_path = request.trial_paths.out.join(BENCHMARK_GRADE_ERROR_FILENAME);

    let mapped_output_valid = match load_optional_json_record_with_schema(
        "trial_conclusion_v1.jsonschema",
        &mapped_grader_output_path,
    ) {
        Ok(Some(_)) => true,
        Ok(None) => {
            failures.push(format!(
                "contract smoke did not write mapped grader output: {}",
                mapped_grader_output_path.display()
            ));
            false
        }
        Err(err) => {
            failures.push(format!("mapped grader output invalid: {}", err));
            false
        }
    };
    if !mapped_output_valid && grade_error_path.exists() {
        let marker_reason =
            fs::read_to_string(&grade_error_path).unwrap_or_else(|_| "grade_error".to_string());
        let reason = marker_reason.trim();
        failures.push(format!(
            "benchmark smoke recorded grade error: {}",
            if reason.is_empty() {
                "grade_error"
            } else {
                reason
            }
        ));
    } else if !mapped_output_valid && status == BENCHMARK_GRADING_POLICY_EXIT_CODE.to_string() {
        failures.push(format!(
            "benchmark smoke exited with grading policy code {} without a grade error marker",
            BENCHMARK_GRADING_POLICY_EXIT_CODE
        ));
    }
    failures
}

pub(crate) fn collect_preflight_contract_smoke_failures(
    request: &AdapterRunRequest<'_>,
    execution: &PreflightContractSmokeExecution,
) -> Vec<String> {
    let mut failures = detect_known_probe_output_blockers(&execution.stdout, &execution.stderr);
    let log_summaries = summarize_preflight_failure_logs(&execution.stdout, &execution.stderr);
    let mut contract_failed = false;
    if execution.status != "0" {
        failures.push(format!(
            "contract smoke exited with status {}",
            execution.status
        ));
        contract_failed = true;
    }
    let result_failures = validate_preflight_result_payload(&request.io_paths.result_host);
    if !result_failures.is_empty() {
        contract_failed = true;
    }
    failures.extend(result_failures);
    if contract_failed {
        failures.extend(log_summaries.clone());
    }
    if request.benchmark_grading_enabled {
        let benchmark_failures =
            validate_preflight_benchmark_smoke_outputs(request, &execution.status);
        failures.extend(benchmark_failures);
    }
    let mut seen = HashSet::new();
    failures
        .into_iter()
        .filter(|failure| seen.insert(failure.clone()))
        .collect()
}

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

pub(crate) fn shell_join(parts: &[String]) -> String {
    parts
        .iter()
        .map(|p| shell_quote(p))
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        "''".to_string()
    } else if s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-_./:".contains(c))
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\"'\"'"))
    }
}

pub(crate) fn trial_output_error_payload(code: &str, message: &str) -> Value {
    json!({
        "schema_version": "agent_result_v1",
        "outcome": "error",
        "error": {
            "code": code,
            "message": message,
        },
    })
}

pub(crate) fn load_trial_output_resilient(path: &Path) -> Result<(Value, Option<String>)> {
    if !path.exists() {
        return Ok((
            trial_output_error_payload("result_missing", "agent did not write a result payload"),
            None,
        ));
    }

    let bytes = fs::read(path)?;
    match serde_json::from_slice(&bytes) {
        Ok(value) => Ok((value, None)),
        Err(err) => {
            let detail = format!(
                "failed to parse agent result JSON at {}: {}",
                path.display(),
                err
            );
            Ok((
                trial_output_error_payload("result_parse_error", &detail),
                Some(detail),
            ))
        }
    }
}

pub(crate) fn result_file_ref_path(result_value: &Value) -> Option<&str> {
    result_value
        .get("artifact")
        .and_then(Value::as_str)
        .or_else(|| {
            result_value
                .pointer("/artifact/path")
                .and_then(Value::as_str)
        })
}

pub(crate) fn artifact_type_from_trial_input(trial_input: &Value) -> ArtifactType {
    match trial_input
        .pointer("/artifact_type")
        .and_then(Value::as_str)
        .unwrap_or("structured_json")
    {
        "patch_submission" => ArtifactType::PatchSubmission,
        "text_response" => ArtifactType::TextResponse,
        "file_ref" => ArtifactType::FileRef,
        _ => ArtifactType::StructuredJson,
    }
}

pub(crate) fn extract_candidate_artifact_record(
    result_value: &Value,
    expected_artifact_type: ArtifactType,
) -> CandidateArtifactRecord {
    if result_value
        .pointer("/error/code")
        .and_then(Value::as_str)
        .is_some_and(|code| code == "result_missing")
    {
        return CandidateArtifactRecord {
            state: CandidateArtifactState::Missing,
            artifact_type: expected_artifact_type,
            source: CandidateArtifactSource::None,
            payload: None,
        };
    }

    match serde_json::from_value::<ArtifactEnvelopeV1>(result_value.clone()) {
        Ok(envelope) if envelope.artifact_type == expected_artifact_type => {
            let source = if matches!(envelope.artifact_type, ArtifactType::FileRef) {
                CandidateArtifactSource::ResultFileRef
            } else {
                CandidateArtifactSource::ResultInline
            };
            let state = if matches!(envelope.artifact_type, ArtifactType::FileRef) {
                match result_file_ref_path(result_value) {
                    Some(path)
                        if path == DEFAULT_CONTAINER_RESULT_PATH
                            || path.starts_with(&format!("{}/", AGENTLAB_CONTRACT_OUT_DIR)) =>
                    {
                        CandidateArtifactState::Valid
                    }
                    _ => CandidateArtifactState::Invalid,
                }
            } else {
                CandidateArtifactState::Valid
            };
            CandidateArtifactRecord {
                state,
                artifact_type: envelope.artifact_type,
                source,
                payload: Some(envelope.artifact),
            }
        }
        Ok(envelope) => CandidateArtifactRecord {
            state: CandidateArtifactState::Invalid,
            artifact_type: envelope.artifact_type,
            source: CandidateArtifactSource::None,
            payload: Some(envelope.artifact),
        },
        Err(_) => CandidateArtifactRecord {
            state: CandidateArtifactState::Invalid,
            artifact_type: expected_artifact_type,
            source: CandidateArtifactSource::None,
            payload: Some(result_value.clone()),
        },
    }
}

pub(crate) fn stage_grader_aux_copy(
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

pub(crate) fn build_grader_input_value(
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
            .unwrap_or_else(|| json!({})),
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

pub(crate) fn resolve_trial_io_host_path(path: &str, paths: &TrialPaths) -> Result<PathBuf> {
    map_container_path_to_host(path, paths)
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

    fs::write(&trial_input_host, input_bytes)?;

    if result_host.exists() {
        let _ = fs::remove_file(&result_host);
    }
    if raw_grader_output_host.exists() {
        let _ = fs::remove_file(&raw_grader_output_host);
    }
    if mapped_grader_output_host.exists() {
        let _ = fs::remove_file(&mapped_grader_output_host);
    }
    if trajectory_host.exists() {
        let _ = fs::remove_file(&trajectory_host);
    }
    if grader_input_host.exists() {
        let _ = fs::remove_file(&grader_input_host);
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

pub(crate) fn materialize_trial_result(trial_dir: &Path, output_path: &Path) -> Result<PathBuf> {
    let canonical_output = trial_dir.join("result.json");
    if output_path != canonical_output {
        if canonical_output.exists() {
            let _ = fs::remove_file(&canonical_output);
        }
        if output_path.exists() {
            if let Some(parent) = canonical_output.parent() {
                ensure_dir(parent)?;
            }
            fs::copy(output_path, &canonical_output)?;
        }
    }
    Ok(canonical_output)
}

pub(crate) fn copy_file_if_exists(src: &Path, dst: &Path) -> Result<()> {
    if !src.exists() {
        return Ok(());
    }
    if let Some(parent) = dst.parent() {
        ensure_dir(parent)?;
    }
    if dst.exists() {
        remove_path_if_exists(dst)?;
    }
    fs::copy(src, dst)?;
    Ok(())
}

pub(crate) fn copy_dir_preserve_contents(src: &Path, dst: &Path) -> Result<()> {
    if !src.exists() {
        return Ok(());
    }
    remove_path_if_exists(dst)?;
    ensure_dir(dst)?;
    let walker = walkdir::WalkDir::new(src).into_iter();
    for entry in walker {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(src).unwrap_or(path);
        if rel.as_os_str().is_empty() {
            continue;
        }
        let target = dst.join(rel);
        if entry.file_type().is_dir() {
            ensure_dir(&target)?;
        } else if entry.file_type().is_symlink() {
            if let Some(parent) = target.parent() {
                ensure_dir(parent)?;
            }
            match fs::canonicalize(path) {
                Ok(real) if real.is_dir() => preserve_symlink(path, &target)?,
                Ok(real) if real.is_file() => {
                    fs::copy(real, &target)?;
                }
                Ok(_) => preserve_symlink(path, &target)?,
                Err(_) => preserve_symlink(path, &target)?,
            }
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                ensure_dir(parent)?;
            }
            fs::copy(path, &target)?;
        }
    }
    Ok(())
}

pub(crate) fn materialize_trial_runtime_layout(
    trial_dir: &Path,
    paths: &TrialPaths,
    mode: MaterializationMode,
) -> Result<()> {
    match mode {
        MaterializationMode::Full => {
            copy_dir_preserve_contents(&paths.in_dir, &trial_dir.join("in"))?;
            copy_dir_preserve_contents(&paths.out, &trial_dir.join("out"))?;
            copy_dir_preserve_contents(&paths.state, &trial_dir.join("state"))?;
            copy_dir_preserve_contents(&paths.workspace, &trial_dir.join("workspace"))?;
            copy_dir_preserve_contents(&paths.tmp, &trial_dir.join("tmp"))?;
            copy_file_if_exists(
                &paths.runtime.trial_input,
                &trial_dir.join("trial_input.json"),
            )?;
            copy_file_if_exists(
                &paths.out.join("harness_manifest.json"),
                &trial_dir.join("harness_manifest.json"),
            )?;
            let _ = materialize_trial_result(trial_dir, &paths.runtime.result)?;
        }
        MaterializationMode::OutputsOnly => {
            copy_dir_preserve_contents(&paths.out, &trial_dir.join("out"))?;
            copy_file_if_exists(
                &paths.out.join("harness_manifest.json"),
                &trial_dir.join("harness_manifest.json"),
            )?;
            let _ = materialize_trial_result(trial_dir, &paths.runtime.result)?;
        }
        MaterializationMode::MetadataOnly | MaterializationMode::None => {}
    }
    apply_materialization_policy(trial_dir, mode)
}

#[cfg(test)]
pub(crate) fn write_adapter_continue_control(path: &Path) -> Result<()> {
    let _ = write_adapter_control_action(path, 0, "continue", None, "run_loop")?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn write_adapter_control_action(
    path: &Path,
    seq: u64,
    action: &str,
    label: Option<&str>,
    requested_by: &str,
) -> Result<String> {
    let payload = json!({
        "schema_version": "control_plane_v1",
        "seq": seq,
        "action": action,
        "label": label,
        "requested_at": Utc::now().to_rfc3339(),
        "requested_by": requested_by,
    });
    let bytes = serde_json::to_vec_pretty(&payload)?;
    let version = sha256_bytes(&bytes);
    atomic_write_json_pretty(path, &payload)?;
    Ok(version)
}

pub(crate) fn resolve_agent_runtime_manifest_path(paths: &TrialPaths) -> Result<PathBuf> {
    map_container_path_to_host(
        &format!("{}/harness_manifest.json", AGENTLAB_CONTRACT_OUT_DIR),
        paths,
    )
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

pub(crate) fn write_state_inventory(
    trial_dir: &Path,
    json_value: &Value,
    agent_runtime: &AgentRuntimeConfig,
    _paths: &TrialPaths,
    exec_digest: &str,
    effective_network_mode: &str,
    invocation_source: &str,
    task_sandbox_image: Option<&str>,
    task_sandbox_workdir: Option<&str>,
) -> Result<()> {
    let sanitization_profile = json_value
        .pointer("/design/sanitization_profile")
        .and_then(|v| v.as_str())
        .unwrap_or("hermetic_functional");
    let integration_level = agent_runtime.integration_level.as_str();
    let mode_requested = json_value
        .pointer("/policy/task_sandbox/network")
        .and_then(|v| v.as_str())
        .unwrap_or("none");
    let mode_effective = effective_network_mode;
    let enforcement_effective = if mode_requested == "none" {
        "docker_none"
    } else {
        "unknown"
    };
    let workspace_path = task_sandbox_workdir.unwrap_or(DEFAULT_TASK_WORKDIR_FALLBACK);

    let mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    let mut agent_runtime_mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    agent_runtime_mounts.push(json!({
        "name": "agent_bundle",
        "path": "/opt/agent",
        "writable": false
    }));
    let mut task_sandbox_mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    let sandbox_profile = json_value
        .pointer("/policy/task_sandbox/profile")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    if sandbox_profile == "swebench_testbed" {
        task_sandbox_mounts.push(json!({
            "name": "testbed",
            "path": "/testbed",
            "writable": true
        }));
    }
    let agent_runtime_image = Some(agent_runtime.image.as_str());
    let agent_runtime_image_digest = agent_runtime_image.and_then(resolve_container_image_digest);
    let task_sandbox_image_digest = task_sandbox_image.and_then(resolve_container_image_digest);

    let state = json!({
        "schema_version": "state_inventory_v1",
        "sanitization_profile": sanitization_profile,
        "integration_level": integration_level,
        "mounts": mounts,
        "network": {
            "mode_requested": mode_requested,
            "mode_effective": mode_effective,
            "allowed_hosts": json_value
                .pointer("/policy/task_sandbox/allowed_hosts")
                .cloned()
                .unwrap_or(json!([])),
            "enforcement_effective": enforcement_effective,
            "egress_self_test": {
                "performed": false,
                "cases": []
            }
        },
        "harness_identity": {
            "name": agent_runtime.command_raw.first().cloned().unwrap_or("unknown".to_string()),
            "exec_digest": exec_digest,
            "entry_command": agent_runtime.command_raw.clone()
        },
        "planes": {
            "agent_runtime": {
                "executor": "docker",
                "image": agent_runtime_image,
                "image_digest": agent_runtime_image_digest,
                "workdir": workspace_path,
                "mounts": agent_runtime_mounts,
                "network_mode": agent_runtime.network
            },
            "task_sandbox": {
                "executor": "docker",
                "image": task_sandbox_image,
                "image_digest": task_sandbox_image_digest,
                "workdir": workspace_path,
                "mounts": task_sandbox_mounts,
                "network_mode": mode_effective
            }
        },
        "ext": {
            "agent_runtime_identity": {
                "invocation_source": invocation_source
            }
        },
        "violations": {
            "state_leak": false,
            "profile_invariant_violation": false,
            "notes": []
        }
    });
    atomic_write_json_pretty(&trial_dir.join("state_inventory.json"), &state)?;
    Ok(())
}

pub(crate) fn remove_path_if_exists(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() || meta.is_file() => {
            make_path_tree_writable(path)?;
            fs::remove_file(path)?
        }
        Ok(meta) if meta.is_dir() => {
            make_path_tree_writable(path)?;
            fs::remove_dir_all(path)?
        }
        Ok(_) => {
            make_path_tree_writable(path)?;
            fs::remove_file(path)?
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn make_path_tree_writable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    if meta.file_type().is_symlink() {
        return Ok(());
    }

    if meta.is_dir() {
        for entry in walkdir::WalkDir::new(path)
            .follow_links(false)
            .contents_first(true)
        {
            let entry = entry?;
            if entry.file_type().is_symlink() {
                continue;
            }
            let entry_path = entry.path();
            let metadata = fs::metadata(entry_path)?;
            let mut perms = metadata.permissions();
            let desired_mode = if entry.file_type().is_dir() {
                perms.mode() | 0o700
            } else {
                perms.mode() | 0o600
            };
            if perms.mode() != desired_mode {
                perms.set_mode(desired_mode);
                fs::set_permissions(entry_path, perms)?;
            }
        }
        return Ok(());
    }

    let mut perms = meta.permissions();
    let desired_mode = perms.mode() | 0o600;
    if perms.mode() != desired_mode {
        perms.set_mode(desired_mode);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn make_path_tree_writable(_path: &Path) -> Result<()> {
    Ok(())
}

pub(crate) fn preserve_symlink(path: &Path, target: &Path) -> Result<()> {
    let link_target = fs::read_link(path)?;
    remove_path_if_exists(target)?;
    #[cfg(unix)]
    {
        symlink(&link_target, target)?;
    }
    Ok(())
}

pub(crate) fn apply_materialization_policy(
    trial_dir: &Path,
    mode: MaterializationMode,
) -> Result<()> {
    match mode {
        MaterializationMode::Full => return Ok(()),
        MaterializationMode::OutputsOnly => {
            for dir_name in ["workspace", "state", "tmp", "artifacts"] {
                remove_path_if_exists(&trial_dir.join(dir_name))?;
            }
        }
        MaterializationMode::MetadataOnly | MaterializationMode::None => {
            for dir_name in ["workspace", "state", "tmp", "artifacts", "out"] {
                remove_path_if_exists(&trial_dir.join(dir_name))?;
            }
            remove_path_if_exists(&trial_dir.join("trial_input.json"))?;
            remove_path_if_exists(&trial_dir.join("result.json"))?;
            remove_path_if_exists(&trial_dir.join("harness_manifest.json"))?;
            remove_path_if_exists(&trial_dir.join("trace_manifest.json"))?;
            if matches!(mode, MaterializationMode::None) {
                remove_path_if_exists(&trial_dir.join("state_inventory.json"))?;
            }
        }
    }
    Ok(())
}

pub(crate) fn map_container_path_to_host(path: &str, paths: &TrialPaths) -> Result<PathBuf> {
    map_contract_path_to_host(
        path,
        &ContractPathHostRoots::from_trial_paths(paths),
        ContractPathMode::ContainerMount,
    )
}

pub(crate) fn load_event_rows(
    events_path: &Path,
    run_id: &str,
    trial_id: &str,
    schedule_idx: usize,
    variant_id: &str,
    task_id: &str,
    repl_idx: usize,
) -> Result<Vec<EventRow>> {
    let mut rows = Vec::new();
    let file = fs::File::open(events_path)?;
    let reader = BufReader::new(file);
    for (seq, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (event_type, ts, payload) = match serde_json::from_str::<Value>(trimmed) {
            Ok(payload) => {
                let event_type = payload
                    .get("event_type")
                    .and_then(Value::as_str)
                    .or_else(|| payload.get("type").and_then(Value::as_str))
                    .unwrap_or("unknown")
                    .to_string();
                let ts = payload
                    .get("ts")
                    .and_then(Value::as_str)
                    .or_else(|| payload.get("timestamp").and_then(Value::as_str))
                    .map(str::to_string);
                (event_type, ts, payload)
            }
            Err(err) => (
                "trajectory_parse_error".to_string(),
                None,
                json!({
                    "event_type": "trajectory_parse_error",
                    "error": err.to_string(),
                    "raw_line": trimmed,
                }),
            ),
        };
        rows.push(EventRow {
            run_id: run_id.to_string(),
            trial_id: trial_id.to_string(),
            schedule_idx,
            slot_commit_id: String::new(),
            attempt: 0,
            row_seq: seq,
            variant_id: variant_id.to_string(),
            task_id: task_id.to_string(),
            repl_idx,
            seq,
            event_type,
            ts,
            payload,
        });
    }
    Ok(rows)
}

pub(crate) fn build_metric_rows(
    run_id: &str,
    trial_id: &str,
    schedule_idx: usize,
    variant_id: &str,
    task_id: &str,
    repl_idx: usize,
    outcome: &str,
    metrics: &Value,
    primary_metric_name: &str,
    primary_metric_value: &Value,
) -> Vec<MetricRow> {
    let mut rows = Vec::new();
    if let Some(metric_obj) = metrics.as_object() {
        for (row_seq, (metric_name, metric_value)) in metric_obj.iter().enumerate() {
            rows.push(MetricRow {
                run_id: run_id.to_string(),
                trial_id: trial_id.to_string(),
                schedule_idx,
                slot_commit_id: String::new(),
                attempt: 0,
                row_seq,
                variant_id: variant_id.to_string(),
                task_id: task_id.to_string(),
                repl_idx,
                outcome: outcome.to_string(),
                metric_name: metric_name.clone(),
                metric_value: metric_value.clone(),
                metric_source: None,
            });
        }
    }
    rows.push(MetricRow {
        run_id: run_id.to_string(),
        trial_id: trial_id.to_string(),
        schedule_idx,
        slot_commit_id: String::new(),
        attempt: 0,
        row_seq: rows.len(),
        variant_id: variant_id.to_string(),
        task_id: task_id.to_string(),
        repl_idx,
        outcome: outcome.to_string(),
        metric_name: primary_metric_name.to_string(),
        metric_value: primary_metric_value.clone(),
        metric_source: Some("primary".to_string()),
    });
    rows
}

pub(crate) fn build_variant_snapshot_rows(
    run_id: &str,
    trial_id: &str,
    schedule_idx: usize,
    variant_id: &str,
    baseline_id: &str,
    task_id: &str,
    repl_idx: usize,
    bindings: &Value,
) -> Vec<VariantSnapshotRow> {
    let mut rows = Vec::new();
    if let Some(bindings_obj) = bindings.as_object() {
        for (row_seq, (binding_name, binding_value)) in bindings_obj.iter().enumerate() {
            rows.push(VariantSnapshotRow {
                run_id: run_id.to_string(),
                trial_id: trial_id.to_string(),
                schedule_idx,
                slot_commit_id: String::new(),
                attempt: 0,
                row_seq,
                variant_id: variant_id.to_string(),
                baseline_id: baseline_id.to_string(),
                task_id: task_id.to_string(),
                repl_idx,
                binding_name: binding_name.clone(),
                binding_value: binding_value.clone(),
                binding_value_text: binding_value_to_text(binding_value),
            });
        }
    }
    rows
}

pub(crate) fn binding_value_to_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

pub(crate) fn copy_dir_with_policy(
    src: &Path,
    dst: &Path,
    exclude: &[&str],
    respect_workspace_evidence_exclusions: bool,
) -> Result<()> {
    let walker = walkdir::WalkDir::new(src).into_iter().filter_entry(|e| {
        let rel = e.path().strip_prefix(src).unwrap_or(e.path());
        if rel.as_os_str().is_empty() {
            return true; // root entry
        }
        if exclude.iter().any(|ex| rel.starts_with(ex)) {
            return false;
        }
        if respect_workspace_evidence_exclusions && is_workspace_evidence_excluded(rel) {
            return false;
        }
        true
    });
    for entry in walker {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(src).unwrap();
        if rel.as_os_str().is_empty() {
            continue;
        }
        let target = dst.join(rel);
        if entry.file_type().is_dir() {
            ensure_dir(&target)?;
        } else if entry.file_type().is_symlink() {
            if let Some(parent) = target.parent() {
                ensure_dir(parent)?;
            }
            match fs::canonicalize(path) {
                Ok(real) if real.is_dir() => preserve_symlink(path, &target)?,
                Ok(real) if real.is_file() => {
                    fs::copy(real, &target)?;
                }
                Ok(_) => preserve_symlink(path, &target)?,
                Err(_) => preserve_symlink(path, &target)?,
            }
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                ensure_dir(parent)?;
            }
            fs::copy(path, target)?;
        }
    }
    Ok(())
}

pub(crate) fn copy_dir_filtered(src: &Path, dst: &Path, exclude: &[&str]) -> Result<()> {
    copy_dir_with_policy(src, dst, exclude, true)
}

pub(crate) fn copy_dir_preserve_all(src: &Path, dst: &Path, exclude: &[&str]) -> Result<()> {
    copy_dir_with_policy(src, dst, exclude, false)
}

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
