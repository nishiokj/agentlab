struct PreparedTaskEnvironment {
    manifest: PreparedTaskEnvironmentManifest,
    trial_paths: TrialPaths,
    io_paths: PreparedTrialIo,
    dynamic_mounts: Vec<ResolvedMountReference>,
    trial_input: Value,
}

#[derive(Debug, Clone)]
struct TaskBoundaryMaterialization {
    declaration: TaskDeclaration,
    task_payload: Value,
    environment: TaskEnvironmentSpec,
    workspace: WorkspaceSpec,
    dependencies: Value,
    limits: TaskDeclarationLimits,
    task_image: String,
}

pub(crate) fn parse_task_spec(task: &Value) -> Result<TaskSpec> {
    let task_spec: TaskSpec =
        serde_json::from_value(task.clone()).map_err(|e| anyhow!("invalid public task_spec: {}", e))?;
    validate_task_spec(&task_spec)?;
    Ok(task_spec)
}

pub(crate) fn compile_task_spec(task_spec: TaskSpec) -> Result<TaskDeclaration> {
    let declaration = task_spec.into_task_declaration();
    validate_task_declaration(&declaration)?;
    Ok(declaration)
}

pub(crate) fn parse_task_declaration(task: &Value) -> Result<TaskDeclaration> {
    let obj = task
        .as_object()
        .ok_or_else(|| anyhow!("task declaration must be an object"))?;
    if obj.get("schema_version").and_then(Value::as_str) != Some("task_declaration_v1") {
        return Err(anyhow!(
            "task declaration schema_version must be 'task_declaration_v1'"
        ));
    }
    let declaration: TaskDeclaration =
        serde_json::from_value(task.clone()).map_err(|e| anyhow!("invalid task declaration: {}", e))?;
    validate_task_declaration(&declaration)?;
    Ok(declaration)
}

fn materialize_task_boundary(declaration: TaskDeclaration) -> TaskBoundaryMaterialization {
    TaskBoundaryMaterialization {
        task_payload: declaration.task.clone(),
        environment: declaration.environment.clone(),
        workspace: declaration.workspace.clone(),
        dependencies: declaration.dependencies.clone(),
        limits: declaration.limits.clone(),
        task_image: declaration.environment.image.clone(),
        declaration,
    }
}

fn parse_task_boundary_from_packaged_task(task: &Value) -> Result<TaskBoundaryMaterialization> {
    Ok(materialize_task_boundary(parse_task_declaration(task)?))
}

fn validate_task_spec(task_spec: &TaskSpec) -> Result<()> {
    if !task_spec.task.is_object() {
        return Err(anyhow!("public task_spec field 'task' must be an object"));
    }
    let task_id = task_spec
        .task
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("public task_spec field 'task.id' must be a non-empty string"))?;
    if task_spec.task.get("image").is_some() {
        return Err(anyhow!(
            "public task_spec field 'task.image' is not allowed for task '{}'; use environment.image",
            task_id
        ));
    }
    if task_spec.task.get("workspace").is_some() {
        return Err(anyhow!(
            "public task_spec field 'task.workspace' is not allowed for task '{}'; use workspace",
            task_id
        ));
    }
    parse_task_environment(Some(&serde_json::to_value(&task_spec.environment)?))?;
    parse_workspace_spec(Some(&serde_json::to_value(&task_spec.workspace)?))?;
    parse_task_dependencies(Some(&task_spec.dependencies))?;
    parse_task_limits(Some(&serde_json::to_value(&task_spec.limits)?))?;
    Ok(())
}

fn validate_task_declaration(declaration: &TaskDeclaration) -> Result<()> {
    if !declaration.task.is_object() {
        return Err(anyhow!("task declaration field 'task' must be an object"));
    }
    if declaration.task.get("image").is_some() {
        return Err(anyhow!(
            "task declaration field 'task.image' was removed; use environment.image"
        ));
    }
    if declaration.task.get("workspace").is_some() {
        return Err(anyhow!(
            "task declaration field 'task.workspace' was removed; sandbox topology is runner-owned"
        ));
    }
    parse_task_environment(Some(&serde_json::to_value(&declaration.environment)?))?;
    parse_workspace_spec(Some(&serde_json::to_value(&declaration.workspace)?))?;
    parse_task_dependencies(Some(&declaration.dependencies))?;
    parse_task_limits(Some(&serde_json::to_value(&declaration.limits)?))?;
    Ok(())
}

fn validate_task_boundary_workspace_materialization(
    task_boundary: &TaskBoundaryMaterialization,
) -> Result<()> {
    if task_boundary.workspace.mode != WorkspaceMode::Patch {
        return Ok(());
    }
    if task_boundary.workspace.base.kind != WorkspaceBaseKind::Empty {
        return Ok(());
    }
    let task_id = task_boundary
        .task_payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown_task>");
    Err(anyhow!(
        "task '{}' uses workspace.mode='patch' but workspace.base.kind='empty'; patch tasks require a real base (dataset_pack or git_checkout)",
        task_id
    ))
}

fn parse_task_environment(value: Option<&Value>) -> Result<TaskEnvironmentSpec> {
    let raw = value.ok_or_else(|| anyhow!("task declaration missing field: environment"))?;
    let environment: TaskEnvironmentSpec =
        serde_json::from_value(raw.clone()).map_err(|e| anyhow!("invalid environment: {}", e))?;
    if environment.image.trim().is_empty() {
        return Err(anyhow!("environment.image must be a non-empty string"));
    }
    Ok(environment)
}

fn parse_task_dependencies(value: Option<&Value>) -> Result<Value> {
    match value {
        None => Ok(json!({})),
        Some(raw) if raw.is_object() => Ok(raw.clone()),
        Some(_) => Err(anyhow!("task dependencies must be an object")),
    }
}

fn validate_workspace_base(base: &WorkspaceBaseSpec) -> Result<()> {
    match base.kind {
        WorkspaceBaseKind::Empty => {
            if base.dataset_pack_ref.is_some() || base.repo.is_some() || base.commit.is_some() {
                return Err(anyhow!(
                    "workspace.base.kind='empty' does not allow dataset_pack_ref, repo, or commit"
                ));
            }
        }
        WorkspaceBaseKind::DatasetPack => {
            let dataset_pack_ref = base.dataset_pack_ref.as_deref().ok_or_else(|| {
                anyhow!("workspace.base.dataset_pack_ref is required for dataset_pack")
            })?;
            let _ = parse_dataset_pack_ref_digest(dataset_pack_ref).map_err(|e| {
                anyhow!(
                    "invalid workspace.base.dataset_pack_ref '{}': {}",
                    dataset_pack_ref,
                    e
                )
            })?;
            if base.repo.is_some() || base.commit.is_some() {
                return Err(anyhow!(
                    "workspace.base.kind='dataset_pack' does not allow repo or commit"
                ));
            }
        }
        WorkspaceBaseKind::GitCheckout => {
            let repo = base
                .repo
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("workspace.base.repo is required for git_checkout"))?;
            let commit = base
                .commit
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("workspace.base.commit is required for git_checkout"))?;
            if repo.starts_with('/') {
                return Err(anyhow!(
                    "workspace.base.repo must be a repo identifier, not a path"
                ));
            }
            if commit.contains(char::is_whitespace) {
                return Err(anyhow!("workspace.base.commit must not contain whitespace"));
            }
            if base.dataset_pack_ref.is_some() {
                return Err(anyhow!(
                    "workspace.base.kind='git_checkout' does not allow dataset_pack_ref"
                ));
            }
        }
    }
    Ok(())
}

fn parse_workspace_spec(value: Option<&Value>) -> Result<WorkspaceSpec> {
    let raw = value.ok_or_else(|| anyhow!("task declaration missing field: workspace"))?;
    let workspace: WorkspaceSpec =
        serde_json::from_value(raw.clone()).map_err(|e| anyhow!("invalid workspace: {}", e))?;
    validate_workspace_base(&workspace.base)?;
    for (idx, overlay) in workspace.overlays.iter().enumerate() {
        let _ = validate_workspace_relative_path(&overlay.path).map_err(|e| {
            anyhow!(
                "invalid workspace.overlays[{}].path '{}': {}",
                idx,
                overlay.path,
                e
            )
        })?;
        if let Some(encoding) = overlay.encoding.as_deref() {
            if encoding != "utf8" && encoding != "base64" {
                return Err(anyhow!(
                    "workspace.overlays[{}].encoding must be 'utf8' or 'base64'",
                    idx
                ));
            }
        }
    }
    for (idx, mount) in workspace.aux_mounts.iter().enumerate() {
        validate_container_workspace_path(&mount.mount_path).map_err(|e| {
            anyhow!(
                "invalid workspace.aux_mounts[{}].mount_path '{}': {}",
                idx,
                mount.mount_path,
                e
            )
        })?;
        let _ = parse_dataset_pack_ref_digest(&mount.dataset_pack_ref).map_err(|e| {
            anyhow!(
                "invalid workspace.aux_mounts[{}].dataset_pack_ref '{}': {}",
                idx,
                mount.dataset_pack_ref,
                e
            )
        })?;
    }
    Ok(workspace)
}

fn parse_task_limits(value: Option<&Value>) -> Result<TaskDeclarationLimits> {
    let Some(raw) = value else {
        return Ok(TaskDeclarationLimits::default());
    };
    let limits: TaskDeclarationLimits =
        serde_json::from_value(raw.clone()).map_err(|e| anyhow!("invalid limits: {}", e))?;
    validate_limit_positive("max_steps", limits.max_steps)?;
    validate_limit_positive("max_total_tokens", limits.max_total_tokens)?;
    validate_limit_positive("max_tool_calls", limits.max_tool_calls)?;
    validate_limit_positive("trial_seconds", limits.trial_seconds)?;
    Ok(limits)
}

fn validate_limit_positive(name: &str, value: Option<u64>) -> Result<()> {
    if value == Some(0) {
        return Err(anyhow!("{} must be > 0 when provided", name));
    }
    Ok(())
}

fn validate_workspace_relative_path(path: &str) -> Result<PathBuf> {
    if path.trim().is_empty() {
        return Err(anyhow!("path cannot be empty"));
    }
    let p = Path::new(path);
    if p.is_absolute() {
        return Err(anyhow!(
            "path must be relative to {}",
            AGENTLAB_CONTRACT_WORKSPACE_DIR
        ));
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

fn validate_container_workspace_path(path: &str) -> Result<()> {
    if !(path == AGENTLAB_CONTRACT_WORKSPACE_DIR
        || path.starts_with(&format!("{}/", AGENTLAB_CONTRACT_WORKSPACE_DIR)))
    {
        return Err(anyhow!(
            "mount_path must be under {}",
            AGENTLAB_CONTRACT_WORKSPACE_DIR
        ));
    }
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

fn parse_dataset_pack_ref_digest(dataset_pack_ref: &str) -> Result<String> {
    let digest = dataset_pack_ref
        .strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("dataset_pack_ref must start with 'sha256:'"))?;
    if digest.len() != 64 || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("dataset_pack_ref digest must be 64 hex characters"));
    }
    Ok(digest.to_ascii_lowercase())
}

fn resolve_dataset_pack_host_path(project_root: &Path, dataset_pack_ref: &str) -> Result<PathBuf> {
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

fn resolve_workspace_aux_mounts(
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

fn git_repo_cache_dir(project_root: &Path, repo: &str) -> PathBuf {
    project_root
        .join(".lab")
        .join("git_checkouts")
        .join(sanitize_for_fs(repo))
}

fn git_checkout_clone_url(repo: &str) -> String {
    if repo.contains("://") || repo.starts_with("git@") {
        repo.to_string()
    } else {
        format!("https://github.com/{}.git", repo.trim_end_matches(".git"))
    }
}

fn git_commit_available(repo_dir: &Path, commit: &str) -> Result<bool> {
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

fn ensure_git_checkout_cache(project_root: &Path, repo: &str, commit: &str) -> Result<PathBuf> {
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

fn git_checkout_staging_dir(project_root: &Path, repo: &str, commit: &str) -> PathBuf {
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

fn prepare_git_checkout_worktree(
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

fn cleanup_git_checkout_worktree(cache_dir: &Path, staging_dir: &Path) -> Result<()> {
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

fn hydrate_git_checkout_cache(project_root: &Path, repo: &str, commit: &str) -> Result<PathBuf> {
    let (cache_dir, staging_dir) = prepare_git_checkout_worktree(project_root, repo, commit)?;
    cleanup_git_checkout_worktree(&cache_dir, &staging_dir)?;
    Ok(cache_dir)
}

fn materialize_workspace_git_checkout(
    project_root: &Path,
    paths: &TrialPaths,
    repo: &str,
    commit: &str,
) -> Result<()> {
    let (cache_dir, staging_dir) = prepare_git_checkout_worktree(project_root, repo, commit)?;
    if paths.workspace.exists() {
        fs::remove_dir_all(&paths.workspace)?;
    }
    ensure_dir(&paths.workspace)?;
    let result = copy_dir_filtered(&staging_dir, &paths.workspace, &[".git"]);
    let cleanup_result = cleanup_git_checkout_worktree(&cache_dir, &staging_dir);
    result?;
    cleanup_result?;
    Ok(())
}

fn materialize_workspace_base(
    project_root: &Path,
    paths: &TrialPaths,
    base: &WorkspaceBaseSpec,
) -> Result<()> {
    if paths.workspace.exists() {
        fs::remove_dir_all(&paths.workspace)?;
    }
    ensure_dir(&paths.workspace)?;
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
            copy_dir_filtered(&source, &paths.workspace, &[])?;
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
            materialize_workspace_git_checkout(project_root, paths, repo, commit)
        }
    }
}

fn materialize_workspace_overlays(
    paths: &TrialPaths,
    workspace_overlays: &[WorkspaceOverlaySpec],
) -> Result<()> {
    for file in workspace_overlays {
        let rel = validate_workspace_relative_path(&file.path)?;
        let host_path = paths.workspace.join(rel);
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

fn copy_staged_host_path(src: &Path, dst: &Path, required: bool, label: &str) -> Result<bool> {
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
fn set_staged_path_read_only(dst: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if dst.is_dir() {
        for entry in walkdir::WalkDir::new(dst).into_iter().filter_map(|entry| entry.ok()) {
            let entry_path = entry.path();
            let mut perms = fs::metadata(entry_path)?.permissions();
            perms.set_mode(if entry.file_type().is_dir() { 0o555 } else { 0o444 });
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
fn set_staged_path_read_only(_dst: &Path) -> Result<()> {
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
struct TaskDependencyFileSpec {
    path: String,
    content: String,
    #[serde(default = "default_task_dependency_encoding")]
    encoding: String,
    #[serde(default)]
    executable: bool,
}

fn default_task_dependency_encoding() -> String {
    "utf8".to_string()
}

fn parse_task_dependency_files(
    task_boundary: &TaskBoundaryMaterialization,
) -> Result<Vec<TaskDependencyFileSpec>> {
    let Some(files) = task_boundary
        .dependencies
        .get("files")
        .filter(|value| !value.is_null())
    else {
        return Ok(Vec::new());
    };
    serde_json::from_value(files.clone())
        .map_err(|err| anyhow!("invalid task dependencies.files: {}", err))
}

fn materialize_task_dependencies_for_trial(
    task_boundary: &TaskBoundaryMaterialization,
    paths: &TrialPaths,
) -> Result<()> {
    for (idx, spec) in parse_task_dependency_files(task_boundary)?
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
        let dst = paths.deps.join(rel);
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

#[cfg(test)]
fn parse_workspace_patches(
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

fn stage_dependencies_for_trial(runtime: &AgentRuntimeConfig, paths: &TrialPaths) -> Result<()> {
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
fn stage_workspace_patches_for_trial(
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

#[derive(Debug, Clone)]
struct BindingArgProjectionSpec {
    binding: String,
    flag: String,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImageSource {
    Global,
    PerTask,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentExecutionExecutor {
    Docker,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct AgentExecutionConfig {
    executor: Option<AgentExecutionExecutor>,
    image: Option<String>,
    network: String,
    root_read_only: bool,
    user: Option<String>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct AgentRuntimeIoConfig {
    input_arg: String,
    output_arg: String,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentLaunchMode {
    File,
    Stdio,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct WorkspacePatchSpec {
    source_from_host: PathBuf,
    target_path: String,
}

#[derive(Debug, Clone)]
struct DependencyFileStagingSpec {
    source_from_host: PathBuf,
    destination_path: String,
    required: bool,
    read_only: bool,
}

#[derive(Clone)]
struct AgentRuntimeConfig {
    adapter_ref: AgentAdapterRef,
    command_raw: Vec<String>,
    image: String,
    network: String,
    root_read_only: bool,
    user: Option<String>,
    agent_artifact: PathBuf,
    agent_artifact_digest: Option<String>,
    agent_artifact_resolved_path: Option<PathBuf>,
    integration_level: String,
    env: BTreeMap<String, String>,
    env_from_host: Vec<String>,
    bindings_to_args: Vec<BindingArgProjectionSpec>,
    trajectory_path: Option<String>,
    causal_extraction: Option<String>,
    #[cfg(test)]
    sandbox_image: Option<String>,
    #[cfg(test)]
    image_source: ImageSource,
    #[cfg(test)]
    execution: AgentExecutionConfig,
    #[cfg(test)]
    io: AgentRuntimeIoConfig,
    #[cfg(test)]
    launch_mode: AgentLaunchMode,
    #[cfg(test)]
    workspace_patches: Vec<WorkspacePatchSpec>,
    #[cfg(test)]
    default_timeout_ms: Option<u64>,
    #[cfg(test)]
    tracing_mode: Option<String>,
    #[cfg(test)]
    force_container: bool,
    dependency_file_staging: Vec<DependencyFileStagingSpec>,
    #[cfg(test)]
    dependency_services: Vec<Value>,
}

fn parse_command_field(value: Option<&Value>, field: &str) -> Result<Option<Vec<String>>> {
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

fn parse_bindings_to_args(value: Option<&Value>) -> Result<Vec<BindingArgProjectionSpec>> {
    let Some(raw) = value else {
        return Ok(Vec::new());
    };
    let items = raw
        .as_array()
        .ok_or_else(|| anyhow!("runtime.agent_runtime.binding_args must be an array"))?;
    let mut parsed = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        let obj = item.as_object().ok_or_else(|| {
            anyhow!(
                "runtime.agent_runtime.binding_args[{}] must be an object",
                idx
            )
        })?;
        let binding = obj
            .get("key")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "runtime.agent_runtime.binding_args[{}].key must be a non-empty string",
                    idx
                )
            })?;
        let flag = obj
            .get("flag")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "runtime.agent_runtime.binding_args[{}].flag must be a non-empty string",
                    idx
                )
            })?;
        parsed.push(BindingArgProjectionSpec {
            binding: binding.to_string(),
            flag: flag.to_string(),
        });
    }
    Ok(parsed)
}

fn derive_public_path_staging_specs(
    command: &[String],
    env: &BTreeMap<String, String>,
    exp_dir: &Path,
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
            &format!("runtime.agent_runtime.command[{}]", idx),
        )? else {
            continue;
        };
        let key = rel.to_string_lossy().to_string();
        if !seen.insert(key.clone()) {
            continue;
        }
        let source = normalize_path(&exp_dir.join(&rel));
        if !source.exists() {
            return Err(anyhow!(
                "runtime.agent_runtime.command[{}] resolved path does not exist: {}",
                idx,
                source.display()
            ));
        }
        specs.push(DependencyFileStagingSpec {
            source_from_host: source,
            destination_path: format!(
                "{}/{}",
                AGENTLAB_CONTRACT_WORKSPACE_DIR,
                key.replace('\\', "/")
            ),
            required: true,
            read_only: true,
        });
    }
    for (key_name, value) in env {
        let Some(rel) = resolve_existing_public_path_reference(
            value,
            exp_dir,
            &format!("runtime.agent_runtime.env.{}", key_name),
        )? else {
            continue;
        };
        let key = rel.to_string_lossy().to_string();
        if !seen.insert(key.clone()) {
            continue;
        }
        let source = normalize_path(&exp_dir.join(&rel));
        if !source.exists() {
            return Err(anyhow!(
                "runtime.agent_runtime.env.{} resolved path does not exist: {}",
                key_name,
                source.display()
            ));
        }
        specs.push(DependencyFileStagingSpec {
            source_from_host: source,
            destination_path: format!(
                "{}/{}",
                AGENTLAB_CONTRACT_WORKSPACE_DIR,
                key.replace('\\', "/")
            ),
            required: true,
            read_only: true,
        });
    }
    Ok(specs)
}

fn normalize_staged_support_source_path(
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
    if !resolved.exists() {
        return Err(anyhow!(
            "{} resolved path does not exist: {}",
            field_name,
            resolved.display()
        ));
    }
    Ok(resolved)
}

fn validate_support_destination_path(raw: &str, field_name: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} must not be empty", field_name));
    }
    let path = Path::new(trimmed);
    if !path.is_absolute() {
        return Err(anyhow!("{} must be an absolute contract path", field_name));
    }
    if !(trimmed == AGENTLAB_CONTRACT_DEPS_DIR
        || trimmed.starts_with(&format!("{}/", AGENTLAB_CONTRACT_DEPS_DIR))
        || trimmed == AGENTLAB_CONTRACT_STATE_DIR
        || trimmed.starts_with(&format!("{}/", AGENTLAB_CONTRACT_STATE_DIR)))
    {
        return Err(anyhow!(
            "{} must be under {} or {}",
            field_name,
            AGENTLAB_CONTRACT_DEPS_DIR,
            AGENTLAB_CONTRACT_STATE_DIR
        ));
    }
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(anyhow!("{} cannot contain '..'", field_name));
        }
    }
    Ok(trimmed.to_string())
}

fn parse_support_file_staging_specs(
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
            .get("source_from_host")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].source_from_host is required", field_name, idx))?;
        let destination_path = obj
            .get("destination_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].destination_path is required", field_name, idx))?;
        let required = obj.get("required").and_then(Value::as_bool).unwrap_or(true);
        let read_only = obj.get("read_only").and_then(Value::as_bool).unwrap_or(true);
        specs.push(DependencyFileStagingSpec {
            source_from_host: normalize_staged_support_source_path(
                source_from_host,
                exp_dir,
                project_root,
                &format!("{}[{}].source_from_host", field_name, idx),
            )?,
            destination_path: validate_support_destination_path(
                destination_path,
                &format!("{}[{}].destination_path", field_name, idx),
            )?,
            required,
            read_only,
        });
    }
    Ok(specs)
}

fn merge_dependency_file_staging(
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

fn binding_lookup<'a>(bindings: &'a Value, key: &str) -> Option<&'a Value> {
    if key.trim().is_empty() {
        return None;
    }
    let pointer = format!("/{}", key.split('.').collect::<Vec<_>>().join("/"));
    bindings.pointer(&pointer)
}

fn binding_lookup_string(bindings: &Value, key: &str, field_name: &str) -> Result<Option<String>> {
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

fn resolve_runtime_binding_value(
    name: &str,
    bindings: &Value,
    runtime_env_inputs: &BTreeMap<String, String>,
    field_name: &str,
) -> Result<String> {
    if name == "WORKSPACE" {
        return Ok(AGENTLAB_CONTRACT_WORKSPACE_DIR.to_string());
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

fn render_runtime_template(
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

fn resolve_command_templates(
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

fn resolve_env_templates(
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

fn project_bindings_to_args(
    bindings: &Value,
    specs: &[BindingArgProjectionSpec],
) -> Result<Vec<String>> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    let mut projected = Vec::with_capacity(specs.len() * 2);
    for spec in specs {
        let value = binding_lookup(bindings, &spec.binding).ok_or_else(|| {
            anyhow!(
                "missing required config key '{}' for runtime.agent_runtime.binding_args",
                spec.binding
            )
        })?;
        let token = match value {
            Value::String(v) => v.clone(),
            Value::Number(v) => v.to_string(),
            Value::Bool(v) => v.to_string(),
            _ => {
                return Err(anyhow!(
                    "binding '{}' must resolve to string|number|bool (got {})",
                    spec.binding,
                    value_type_name(value)
                ));
            }
        };
        projected.push(spec.flag.clone());
        projected.push(token);
    }
    Ok(projected)
}

fn resolve_agent_runtime(
    json_value: &Value,
    exp_dir: &Path,
    project_root: &Path,
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
            "runtime.agent_runtime hard cut: use runtime.agent_runtime.{{artifact,image,command,env,network,root_read_only,user}}"
        ));
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
    let execution_root_read_only = agent
        .pointer("/root_read_only")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let execution_user =
        parse_optional_nonempty_string(agent.pointer("/user"), "runtime.agent_runtime.user")?;
    #[cfg(test)]
    let execution_network_for_test = execution_network.clone();
    #[cfg(test)]
    let execution_user_for_test = execution_user.clone();
    let artifact_raw = parse_optional_nonempty_string(
        agent.pointer("/artifact"),
        "runtime.agent_runtime.artifact",
    )?
    .ok_or_else(|| anyhow!("runtime.agent_runtime.artifact is required"))?;
    let agent_artifact = {
        let raw = artifact_raw.trim();
        if raw.starts_with("./") || raw.starts_with("../") || raw.contains('/') {
            normalize_path(&exp_dir.join(raw))
        } else {
            resolve_dx_artifact_path(raw, exp_dir, project_root)
        }
    };
    let agent_artifact_digest = parse_optional_nonempty_string(
        agent.pointer("/artifact_digest"),
        "runtime.agent_runtime.artifact_digest",
    )?;
    let agent_artifact_resolved_path = parse_optional_nonempty_string(
        agent.pointer("/artifact_resolved_path"),
        "runtime.agent_runtime.artifact_resolved_path",
    )?
    .map(|raw| {
        let path = PathBuf::from(raw);
        if path.is_absolute() {
            normalize_path(&path)
        } else {
            normalize_path(&exp_dir.join(path))
        }
    });

    let command = parse_command_field(agent.pointer("/command"), "runtime.agent_runtime.command")?
        .ok_or_else(|| anyhow!("runtime.agent_runtime.command is required"))?;
    let integration_level = agent
        .pointer("/integration_level")
        .and_then(|v| v.as_str())
        .unwrap_or("cli_basic")
        .to_string();
    let adapter_ref = AgentAdapterRef::default();
    let env = parse_string_map_field(agent.pointer("/env"), "runtime.agent_runtime.env")?;
    for (key, value) in &env {
        if contains_removed_runtime_template(value) {
            return Err(anyhow!(
                "runtime.agent_runtime.env.{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                key
            ));
        }
        if value.trim().starts_with("/agentlab/") {
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
        if token.trim().starts_with("/agentlab/") {
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
    let bindings_to_args = Vec::new();
    let env_from_host = Vec::new();
    let mut dependency_file_staging = derive_public_path_staging_specs(&command, &env, exp_dir)?;
    merge_dependency_file_staging(
        &mut dependency_file_staging,
        parse_support_file_staging_specs(
            json_value.pointer("/runtime/dependencies/file_staging"),
            "runtime.dependencies.file_staging",
            exp_dir,
            project_root,
        )?,
    );

    Ok(AgentRuntimeConfig {
        adapter_ref,
        command_raw: command,
        image: execution_image,
        network: execution_network,
        root_read_only: execution_root_read_only,
        user: execution_user,
        agent_artifact,
        agent_artifact_digest,
        agent_artifact_resolved_path,
        integration_level,
        env,
        env_from_host,
        bindings_to_args,
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
            root_read_only: execution_root_read_only,
            user: execution_user_for_test,
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

fn parse_runtime_env_file(path: &Path) -> Result<BTreeMap<String, String>> {
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

fn resolve_runtime_env_inputs(execution: &RunExecutionOptions) -> Result<BTreeMap<String, String>> {
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

fn resolve_agent_runtime_env(
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

fn ensure_required_runtime_env_present(
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

fn validate_agent_artifact_pin(runtime_agent: &AgentRuntimeConfig) -> Result<()> {
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

fn resolve_benchmark_support_files(
    experiment: &Value,
    exp_dir: &Path,
    project_root: &Path,
) -> Result<Vec<DependencyFileStagingSpec>> {
    let mut support_files = parse_support_file_staging_specs(
        experiment.pointer("/benchmark/grader/support_files"),
        "benchmark.grader.support_files",
        exp_dir,
        project_root,
    )?;
    merge_dependency_file_staging(
        &mut support_files,
        parse_support_file_staging_specs(
            experiment.pointer("/benchmark/adapter/support_files"),
            "benchmark.adapter.support_files",
            exp_dir,
            project_root,
        )?,
    );
    Ok(support_files)
}

#[derive(Clone)]
struct VariantRuntimeProfile {
    experiment: Value,
    variant_args: Vec<String>,
    agent_runtime: AgentRuntimeConfig,
    agent_runtime_env: BTreeMap<String, String>,
    invocation_source: String,
    configured_network_mode: String,
    effective_network_mode: String,
}

fn command_contains_scientific_bypass(command: &[String]) -> Option<String> {
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

fn preview_agent_command(profile: &VariantRuntimeProfile) -> Vec<String> {
    let mut command = profile.agent_runtime.command_raw.clone();
    command.extend(profile.variant_args.iter().cloned());
    command
}

fn value_contains_host_scratch_path(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.contains("/.lab/runs/") || trimmed.contains("/.scratch/")
}

fn profile_is_hermetic(profile: &VariantRuntimeProfile) -> bool {
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

fn resolve_run_isolation_grade(
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

fn resolve_variant_runtime_profile(
    experiment: &Value,
    variant: &Variant,
    project_root: &Path,
    behavior: &RunBehavior,
    execution: &RunExecutionOptions,
) -> Result<VariantRuntimeProfile> {
    let variant_experiment = resolve_runtime_for_variant(experiment, variant)?;
    validate_required_fields(&variant_experiment)?;

    let mut agent_runtime = resolve_agent_runtime(&variant_experiment, project_root, project_root)?;
    merge_dependency_file_staging(
        &mut agent_runtime.dependency_file_staging,
        resolve_benchmark_support_files(&variant_experiment, project_root, project_root)?,
    );
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
    validate_agent_runtime_command(&agent_runtime.command_raw, project_root)?;
    let mut agent_runtime_env =
        resolve_agent_runtime_env(&agent_runtime, &variant.bindings, &runtime_env_inputs)?;
    let resolved_variant_env =
        resolve_env_templates(&variant.env, &variant.bindings, &runtime_env_inputs, "variant.env")?;
    for (key, value) in resolved_variant_env {
        agent_runtime_env.insert(key, value);
    }
    let variant_args = resolve_command_templates(&variant.args, &variant.bindings, &runtime_env_inputs)?;

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

struct TrialPaths {
    trial_dir: PathBuf,
    scratch_dir: PathBuf,
    in_dir: PathBuf,
    workspace: PathBuf,
    state: PathBuf,
    deps: PathBuf,
    out: PathBuf,
    tmp: PathBuf,
    runtime: RunnerRuntimeHostPaths,
    exp_dir: PathBuf,
}

fn trial_runtime_scratch_dir(trial_dir: &Path) -> PathBuf {
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
    fn new(trial_dir: &Path, exp_dir: &Path) -> Result<Self> {
        let scratch_dir = trial_runtime_scratch_dir(trial_dir);
        let runtime = runner_runtime_host_paths(&scratch_dir);
        Ok(Self {
            trial_dir: trial_dir.to_path_buf(),
            scratch_dir,
            in_dir: runtime.in_dir.clone(),
            workspace: runtime.workspace_dir.clone(),
            state: runtime.state_dir.clone(),
            deps: runtime.deps_dir.clone(),
            out: runtime.out_dir.clone(),
            tmp: runtime.tmp_dir.clone(),
            runtime,
            exp_dir: exp_dir.to_path_buf(),
        })
    }

    fn prepare(&self, seed_workspace_from_exp_dir: bool) -> Result<()> {
        ensure_dir(&self.in_dir)?;
        ensure_dir(&self.workspace)?;
        ensure_dir(&self.state)?;
        ensure_dir(&self.deps)?;
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

    fn cleanup_scratch(&self) -> Result<()> {
        remove_path_if_exists(&self.scratch_dir)
    }
}

impl Drop for TrialPaths {
    fn drop(&mut self) {
        let _ = remove_path_if_exists(&self.scratch_dir);
    }
}

fn build_agent_task(
    json_value: &Value,
    run_id: &str,
    trial_id: &str,
    variant: &Variant,
    task_idx: usize,
    repl: usize,
    task_boundary: &TaskBoundaryMaterialization,
) -> Value {
    let normalized_task_payload = normalize_task_prompt_aliases(&task_boundary.task_payload);
    let mut policy = json_value
        .pointer("/policy")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if policy.pointer("/timeout_ms").is_none() {
        set_json_pointer_value(&mut policy, "/timeout_ms", json!(600000)).ok();
    }
    if let Some(trial_seconds) = task_boundary.limits.trial_seconds {
        set_json_pointer_value(&mut policy, "/timeout_ms", json!(trial_seconds * 1000)).ok();
    }

    let mut input = json!({
        "schema_version": "agent_task_v1",
        "ids": {
            "run_id": run_id,
            "trial_id": trial_id,
            "variant_id": variant.id,
            "task_id": task_boundary.task_payload.get("id").and_then(|v| v.as_str()).unwrap_or(&format!("task_{}", task_idx)),
            "repl_idx": repl
        },
        "task": normalized_task_payload,
        "bindings": variant.bindings.clone(),
        "dependencies": task_boundary.dependencies.clone(),
        "policy": policy,
    });
    if let Some(obj) = input.as_object_mut() {
        obj.remove("ext");
    }
    input
}

fn prepared_task_environment_manifest_path(trial_dir: &Path) -> PathBuf {
    trial_dir
        .join("runtime")
        .join("prepared_task_environment.json")
}

fn write_prepared_task_environment_manifest(
    trial_dir: &Path,
    manifest: &PreparedTaskEnvironmentManifest,
) -> Result<()> {
    let manifest_path = prepared_task_environment_manifest_path(trial_dir);
    atomic_write_json_pretty(&manifest_path, &serde_json::to_value(manifest)?)?;
    Ok(())
}

fn load_prepared_task_environment_manifest(
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

#[allow(clippy::too_many_arguments)]
fn prepare_task_environment(
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
    trial_paths.prepare(false)?;
    materialize_task_dependencies_for_trial(task_boundary, &trial_paths)?;
    stage_dependencies_for_trial(agent_runtime, &trial_paths)?;
    if let Some(workspace_ref) = existing_workspace_ref {
        let artifact_store = ArtifactStore::new(
            infer_run_dir_from_path(trial_dir)
                .unwrap_or_else(|| trial_dir.to_path_buf())
                .join("artifacts"),
        );
        restore_workspace_from_object_ref(&artifact_store, workspace_ref, &trial_paths.workspace)?;
    } else {
        materialize_workspace_base(project_root, &trial_paths, &task_boundary.workspace.base)?;
    }
    materialize_workspace_overlays(&trial_paths, &task_boundary.workspace.overlays)?;
    let dynamic_mounts =
        resolve_workspace_aux_mounts(project_root, &task_boundary.workspace.aux_mounts)?;

    let input = build_agent_task(
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
    let runtime_env = build_runtime_contract_env(
        run_id,
        &input,
        &io_paths,
        Some(task_boundary.task_image.as_str()),
        resolve_trial_timeout_ms(&input),
    );
    let manifest = PreparedTaskEnvironmentManifest {
        schema_version: "prepared_task_environment_v1".to_string(),
        declaration: task_boundary.declaration.clone(),
        declaration_digest: canonical_json_digest(&serde_json::to_value(&task_boundary.declaration)?),
        run_id: run_id.to_string(),
        trial_id: trial_id.to_string(),
        variant_id: variant.id.clone(),
        task_id: task_boundary
            .task_payload
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or(&format!("task_{}", task_idx))
            .to_string(),
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
            trial_input: io_paths.input_host.to_string_lossy().to_string(),
            task: io_paths.task_path.clone(),
            bindings: io_paths.bindings_path.clone(),
            dependencies: io_paths.dependencies_path.clone(),
            policy: io_paths.policy_path.clone(),
            result: io_paths.result_path.clone(),
            trajectory: io_paths.trajectory_path.clone(),
        },
        runtime_env: runtime_env.clone(),
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

fn normalize_task_prompt_aliases(task_payload: &Value) -> Value {
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

fn sanitize_for_fs(raw: &str) -> String {
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

fn infer_run_dir_from_path(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        if run_sqlite_path(ancestor).exists() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

fn json_row_table_from_path(path: &Path) -> Option<JsonRowTable> {
    let name = path.file_name()?.to_string_lossy().to_ascii_lowercase();
    if name.contains("evidence") {
        return Some(JsonRowTable::Evidence);
    }
    if name.contains("task_chain") || name.contains("chain_state") {
        return Some(JsonRowTable::ChainState);
    }
    if name.contains("prediction") {
        return Some(JsonRowTable::BenchmarkPrediction);
    }
    if name.contains("score") {
        return Some(JsonRowTable::BenchmarkScore);
    }
    None
}

fn row_has_sqlite_identity_fields(row: &Value) -> bool {
    row.pointer("/run_id")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
        && row
            .pointer("/schedule_idx")
            .and_then(Value::as_u64)
            .is_some()
        && row.pointer("/attempt").and_then(Value::as_u64).is_some()
        && row.pointer("/row_seq").and_then(Value::as_u64).is_some()
        && row
            .pointer("/slot_commit_id")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
}

fn path_uses_sqlite_json_row_ingest(run_dir: &Path, path: &Path) -> bool {
    !path.starts_with(run_dir.join("runtime").join("worker_payload"))
}

fn append_jsonl_file(path: &Path, value: &Value) -> Result<()> {
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

fn append_jsonl(path: &Path, value: &Value) -> Result<()> {
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

fn is_workspace_evidence_excluded(rel: &Path) -> bool {
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

fn collect_workspace_snapshot_manifest(workspace: &Path) -> Result<Value> {
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
            let path = row.get("path").and_then(|v| v.as_str());
            let digest = row.get("digest").and_then(|v| v.as_str());
            if let (Some(path), Some(digest)) = (path, digest) {
                map.insert(path.to_string(), digest.to_string());
            }
        }
    }
    map
}

fn diff_workspace_snapshots(prev: &Value, post: &Value) -> Value {
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

fn derive_patch_from_diff(diff: &Value) -> Value {
    json!({
        "schema_version": "workspace_patch_v1",
        "format": "file_digest_delta",
        "generated_at": Utc::now().to_rfc3339(),
        "added": diff.get("added").cloned().unwrap_or(json!([])),
        "removed": diff.get("removed").cloned().unwrap_or(json!([])),
        "modified": diff.get("modified").cloned().unwrap_or(json!([])),
    })
}

fn workspace_diff_is_empty(diff: &Value) -> bool {
    ["added", "removed", "modified"].iter().all(|field| {
        diff.get(field)
            .and_then(Value::as_array)
            .map_or(true, Vec::is_empty)
    })
}

fn capture_workspace_object_ref(
    artifact_store: &ArtifactStore,
    workspace_dir: &Path,
) -> Result<String> {
    let max_bundle_bytes = parse_max_workspace_bundle_bytes_from_env()?;
    capture_workspace_object_ref_with_limit(artifact_store, workspace_dir, max_bundle_bytes)
}

fn capture_workspace_object_ref_with_limit(
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

fn restore_workspace_from_object_ref(
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

fn resolve_chain_label(task_payload: &Value, task_id: &str, state_policy: StatePolicy) -> String {
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

fn resolve_trial_timeout_ms(input: &Value) -> Option<u64> {
    input.pointer("/policy/timeout_ms").and_then(|v| v.as_u64())
}

fn output_peer_path(output_path: &str, file_name: &str) -> String {
    let output = Path::new(output_path);
    if let Some(parent) = output.parent() {
        return parent.join(file_name).to_string_lossy().to_string();
    }
    file_name.to_string()
}

fn build_runtime_contract_env(
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
    env.insert(AGENTLAB_ENV_TASK_PATH.to_string(), io.task_path.clone());
    env.insert(
        AGENTLAB_ENV_BINDINGS_PATH.to_string(),
        io.bindings_path.clone(),
    );
    env.insert(
        AGENTLAB_ENV_DEPENDENCIES_PATH.to_string(),
        io.dependencies_path.clone(),
    );
    env.insert(AGENTLAB_ENV_POLICY_PATH.to_string(), io.policy_path.clone());
    env.insert(AGENTLAB_ENV_RESULT_PATH.to_string(), io.result_path.clone());
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
    env.insert(
        AGENTLAB_ENV_BENCHMARK_PREDICTION_PATH.to_string(),
        output_peer_path(&io.result_path, BENCHMARK_PREDICTION_FILENAME),
    );
    env.insert(
        AGENTLAB_ENV_BENCHMARK_SCORE_PATH.to_string(),
        output_peer_path(&io.result_path, BENCHMARK_SCORE_FILENAME),
    );
    env.insert(AGENTLAB_ENV_REPL_IDX.to_string(), repl_idx.to_string());
    if let Some(timeout_ms) = timeout_ms {
        env.insert(AGENTLAB_ENV_TIMEOUT_MS.to_string(), timeout_ms.to_string());
    }
    env
}

fn command_contract_capabilities() -> AgentAdapterCapabilities {
    AgentAdapterCapabilities {
        pause: true,
        control_ack: true,
        event_stream: true,
        strict_replay: false,
    }
}

fn run_command_contract_trial(request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
    write_adapter_continue_control(&request.trial_paths.runtime.control)?;
    run_external_agent_runtime_trial(request)
}

fn pause_command_contract_trial(request: &AdapterPauseRequest<'_>) -> Result<AdapterPauseAck> {
    let control_path = Path::new(&request.control.command_path);
    let events_path =
        request.control.events_path.as_deref().ok_or_else(|| {
            anyhow!("pause_unsupported: active adapter control missing events path")
        })?;
    let events_path = Path::new(events_path);
    let deadline = Instant::now() + request.timeout;

    let seq_checkpoint = read_control_seq(control_path)? + 1;
    let checkpoint_version = write_adapter_control_action(
        control_path,
        seq_checkpoint,
        "checkpoint",
        Some(request.label),
        "lab_pause",
    )?;
    wait_for_adapter_control_ack(events_path, "checkpoint", &checkpoint_version, deadline)?;

    let seq_stop = read_control_seq(control_path)? + 1;
    let stop_version = write_adapter_control_action(
        control_path,
        seq_stop,
        "stop",
        Some(request.label),
        "lab_pause",
    )?;
    wait_for_adapter_control_ack(events_path, "stop", &stop_version, deadline)?;

    Ok(AdapterPauseAck {
        checkpoint_acked: true,
        stop_acked: true,
    })
}

fn prebuilt_adapter_profile_value(flavor: PrebuiltAdapterFlavor) -> &'static str {
    match flavor {
        PrebuiltAdapterFlavor::CodexCli => "codex_cli",
        PrebuiltAdapterFlavor::RexJesus => "rex_jesus",
    }
}

impl AgentAdapter for BuiltinCommandAdapter {
    fn capabilities(&self) -> AgentAdapterCapabilities {
        command_contract_capabilities()
    }

    fn run_trial(&self, request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
        run_command_contract_trial(request)
    }

    fn pause_trial(&self, request: &AdapterPauseRequest<'_>) -> Result<AdapterPauseAck> {
        pause_command_contract_trial(request)
    }
}

impl AgentAdapter for PrebuiltCommandAdapter {
    fn capabilities(&self) -> AgentAdapterCapabilities {
        command_contract_capabilities()
    }

    fn run_trial(&self, request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
        let mut adapter_overrides = request.runtime_overrides_env.clone();
        adapter_overrides.insert(
            "AGENTLAB_PREBUILT_ADAPTER".to_string(),
            prebuilt_adapter_profile_value(self.flavor).to_string(),
        );
        adapter_overrides.insert(
            "AGENTLAB_PREBUILT_ADAPTER_ID".to_string(),
            request.runtime.adapter_ref.id.clone(),
        );
        let prebuilt_request = AdapterRunRequest {
            runtime_experiment: request.runtime_experiment,
            runtime: request.runtime,
            variant_args: request.variant_args,
            runtime_env: request.runtime_env,
            runtime_overrides_env: &adapter_overrides,
            trial_paths: request.trial_paths,
            dynamic_mounts: request.dynamic_mounts,
            io_paths: request.io_paths,
            network_mode: request.network_mode,
            benchmark_grader: request.benchmark_grader,
            benchmark_grading_enabled: request.benchmark_grading_enabled,
            run_id: request.run_id,
            task_image: request.task_image,
            agent_artifact: request.agent_artifact,
        };
        run_command_contract_trial(&prebuilt_request)
    }

    fn pause_trial(&self, request: &AdapterPauseRequest<'_>) -> Result<AdapterPauseAck> {
        pause_command_contract_trial(request)
    }
}

fn run_container_sidecar_command(
    request: &AdapterRunRequest<'_>,
    image: &str,
    workspace: Option<&str>,
    command: &[String],
    runtime_overrides_env: &BTreeMap<String, String>,
    label: &str,
) -> Result<Output> {
    let sidecar_request = AdapterRunRequest {
        runtime_experiment: request.runtime_experiment,
        runtime: request.runtime,
        variant_args: &[],
        runtime_env: request.runtime_env,
        runtime_overrides_env,
        trial_paths: request.trial_paths,
        dynamic_mounts: request.dynamic_mounts,
        io_paths: request.io_paths,
        network_mode: request.network_mode,
        benchmark_grader: None,
        benchmark_grading_enabled: false,
        run_id: request.run_id,
        task_image: request.task_image,
        agent_artifact: None,
    };
    let mut cmd = build_baked_container_command(
        &sidecar_request,
        ContainerPlane::TaskSandbox,
        image,
        workspace,
        command,
        None,
    );
    let output = cmd.output()?;
    if output.status.success() {
        return Ok(output);
    }
    Err(anyhow!(
        "{} failed: {}",
        label,
        output_error_detail(&output)
    ))
}

fn run_external_agent_runtime_trial(request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
    let command = resolve_runtime_agent_command(request)?;
    if let Some(bundle) = request.agent_artifact {
        validate_agent_artifact_pin(request.runtime)?;
        if !bundle.exists() {
            return Err(anyhow!(
                "runtime.agent_runtime.artifact not found: {}",
                bundle.display()
            ));
        }
        let bundle_context = format!("runtime.agent_runtime.artifact {}", bundle.display());
        validate_agent_artifact_path(bundle, &command, bundle_context.as_str())?;
    }

    let task_sandbox_image = resolve_task_sandbox_image(request)?;
    ensure_container_image_ready(&task_sandbox_image)?;
    let workspace = resolve_container_workspace(request);

    let image = resolve_agent_execution_image(request)?;
    ensure_container_image_ready(&image)?;
    let agent_request = AdapterRunRequest {
        runtime_experiment: request.runtime_experiment,
        runtime: request.runtime,
        variant_args: request.variant_args,
        runtime_env: request.runtime_env,
        runtime_overrides_env: request.runtime_overrides_env,
        trial_paths: request.trial_paths,
        dynamic_mounts: request.dynamic_mounts,
        io_paths: request.io_paths,
        network_mode: request.network_mode,
        benchmark_grader: request.benchmark_grader,
        benchmark_grading_enabled: request.benchmark_grading_enabled,
        run_id: request.run_id,
        task_image: request.task_image,
        agent_artifact: request.agent_artifact,
    };
    let cmd = build_baked_container_command(
        &agent_request,
        ContainerPlane::AgentRuntime,
        &image,
        workspace,
        &command,
        None,
    );
    let stdout_log_path = request.trial_paths.trial_dir.join("harness_stdout.log");
    let stderr_log_path = request.trial_paths.trial_dir.join("harness_stderr.log");
    let agent_result = run_adapter_process(
        cmd,
        &request.io_paths.output_host,
        None,
        &stdout_log_path,
        &stderr_log_path,
    )?;

    if let Some(grader_command) = resolve_benchmark_grader_command(request)? {
        let grade_error_marker_path = request.trial_paths.out.join(BENCHMARK_GRADE_ERROR_FILENAME);
        let result_path = &request.io_paths.output_host;
        if !result_path.exists() || result_path.metadata().map(|meta| meta.len()).unwrap_or(0) == 0
        {
            fs::write(&grade_error_marker_path, b"result_missing\n")?;
            if agent_result.status == "0" {
                return Ok(ProcessRunResult {
                    status: BENCHMARK_GRADING_POLICY_EXIT_CODE.to_string(),
                });
            }
            return Ok(agent_result);
        }

        let mut overrides = request.runtime_overrides_env.clone();
        overrides.insert(
            AGENTLAB_ENV_AGENT_EXIT_STATUS.to_string(),
            agent_result.status.clone(),
        );
        if let Err(err) = run_container_sidecar_command(
            request,
            &task_sandbox_image,
            workspace,
            &grader_command,
            &overrides,
            "benchmark grader",
        ) {
            fs::write(
                &grade_error_marker_path,
                format!("grader_command_failed:{}\n", err).into_bytes(),
            )?;
            return Ok(ProcessRunResult {
                status: BENCHMARK_GRADING_POLICY_EXIT_CODE.to_string(),
            });
        }
        if !request
            .trial_paths
            .out
            .join(BENCHMARK_SCORE_FILENAME)
            .exists()
        {
            fs::write(&grade_error_marker_path, b"score_record_missing\n")?;
            return Ok(ProcessRunResult {
                status: BENCHMARK_GRADING_POLICY_EXIT_CODE.to_string(),
            });
        }
    }

    Ok(agent_result)
}

fn resolve_benchmark_grader_command(
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
    let rendered = grader.command.clone();
    if let Some(script_path) = rendered.get(1).map(|value| value.trim()) {
        if Path::new(script_path).is_absolute() && !is_runner_staged_script_path(script_path) {
            return Err(anyhow!(
                "forbidden benchmark grader script path '{}': script must be under {} or {}",
                script_path,
                AGENTLAB_CONTRACT_DEPS_DIR,
                AGENTLAB_CONTRACT_STATE_DIR
            ));
        }
    }
    Ok(Some(rendered))
}

fn resolve_agent_execution_image(request: &AdapterRunRequest<'_>) -> Result<String> {
    Ok(request.runtime.image.clone())
}

fn resolve_task_sandbox_image(request: &AdapterRunRequest<'_>) -> Result<String> {
    request
        .task_image
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("task environment.image is required for task sandbox"))
}

fn resolve_container_workspace<'a>(request: &'a AdapterRunRequest<'_>) -> Option<&'a str> {
    let _ = request;
    Some(AGENTLAB_CONTRACT_WORKSPACE_DIR)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerPlane {
    AgentRuntime,
    TaskSandbox,
}

fn append_container_sandbox_args(
    cmd: &mut Command,
    request: &AdapterRunRequest<'_>,
    plane: ContainerPlane,
    workspace: Option<&str>,
) {
    let root_read_only = match plane {
        ContainerPlane::AgentRuntime => request.runtime.root_read_only,
        ContainerPlane::TaskSandbox => true,
    };
    if root_read_only {
        cmd.arg("--read-only");
    }

    let run_as_user = match plane {
        ContainerPlane::AgentRuntime => request.runtime.user.as_deref(),
        ContainerPlane::TaskSandbox => None,
    };
    if let Some(user) = run_as_user {
        cmd.args(["-u", user]);
    }

    let network_mode = match plane {
        ContainerPlane::AgentRuntime => request.runtime.network.as_str(),
        ContainerPlane::TaskSandbox => request.network_mode,
    };
    if network_mode == "none" {
        cmd.arg("--network=none");
    }

    let no_new_privileges = request
        .runtime_experiment
        .pointer("/policy/task_sandbox/hardening/no_new_privileges")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if no_new_privileges {
        cmd.args(["--security-opt", "no-new-privileges"]);
    }

    let drop_all_caps = request
        .runtime_experiment
        .pointer("/policy/task_sandbox/hardening/drop_all_caps")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if drop_all_caps {
        cmd.args(["--cap-drop", "ALL"]);
    }

    let cpu_limit = request
        .runtime_experiment
        .pointer("/policy/task_sandbox/resources/cpu_count")
        .and_then(|v| v.as_u64());
    if let Some(cpu) = cpu_limit {
        cmd.arg("--cpus").arg(cpu.to_string());
    }
    let memory_limit_mb = request
        .runtime_experiment
        .pointer("/policy/task_sandbox/resources/memory_mb")
        .and_then(|v| v.as_u64());
    if let Some(mem) = memory_limit_mb {
        cmd.arg("--memory").arg(format!("{}m", mem));
    }

    cmd.args([
        "-v",
        &format!(
            "{}:{}:ro",
            request.trial_paths.in_dir.display(),
            AGENTLAB_CONTRACT_IN_DIR
        ),
    ]);
    cmd.args([
        "-v",
        &format!(
            "{}:{}",
            request.trial_paths.out.display(),
            AGENTLAB_CONTRACT_OUT_DIR
        ),
    ]);
    cmd.args([
        "-v",
        &format!(
            "{}:{}",
            request.trial_paths.state.display(),
            AGENTLAB_CONTRACT_STATE_DIR
        ),
    ]);
    cmd.args([
        "-v",
        &format!(
            "{}:{}",
            request.trial_paths.deps.display(),
            AGENTLAB_CONTRACT_DEPS_DIR
        ),
    ]);
    cmd.args([
        "-v",
        &format!(
            "{}:{}",
            request.trial_paths.workspace.display(),
            AGENTLAB_CONTRACT_WORKSPACE_DIR
        ),
    ]);
    let sandbox_profile = request
        .runtime_experiment
        .pointer("/policy/task_sandbox/profile")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    if sandbox_profile == "swebench_testbed" {
        cmd.args([
            "-v",
            &format!("{}:/testbed", request.trial_paths.workspace.display()),
        ]);
    }
    for mount in request.dynamic_mounts {
        cmd.args([
            "-v",
            &format!("{}:{}:ro", mount.host_path.display(), mount.mount_path),
        ]);
    }
    if matches!(plane, ContainerPlane::AgentRuntime) {
        if let Some(bundle) = request.agent_artifact {
            if let Ok(bundle_root) = resolve_agent_artifact_mount_dir(bundle) {
                cmd.args(["-v", &format!("{}:/opt/agent:ro", bundle_root.display())]);
            }
        }
    }
    cmd.args(["--tmpfs", "/tmp:rw"]);
    // Mask legacy image-level /workspace so agent cannot read image internals.
    cmd.args(["--tmpfs", "/workspace:rw"]);
    // Mask benchmark internals bundled in task images from agent runtime view.
    if matches!(plane, ContainerPlane::AgentRuntime) {
        cmd.args(["--tmpfs", "/opt/bench:rw"]);
    }
    if let Some(workspace) = workspace {
        cmd.args(["-w", workspace]);
    }
}

fn append_container_env_args(
    cmd: &mut Command,
    request: &AdapterRunRequest<'_>,
    workspace: Option<&str>,
) {
    let mut path_overridden = false;
    for (key, value) in request.runtime_overrides_env {
        if key == "PATH" {
            path_overridden = true;
        }
        cmd.arg("-e").arg(format!("{}={}", key, value));
    }
    for (key, value) in request.runtime_env {
        if key == "PATH" {
            path_overridden = true;
        }
        cmd.arg("-e").arg(format!("{}={}", key, value));
    }
    if request.agent_artifact.is_some() && !path_overridden {
        cmd.arg("-e").arg(AGENT_ARTIFACT_PATH_ENV_VALUE);
    }
    if let Some(workspace) = workspace {
        cmd.arg("-e").arg(format!("WORKSPACE={}", workspace));
    }
}

fn append_container_entrypoint(
    cmd: &mut Command,
    request: &AdapterRunRequest<'_>,
    command: &[String],
    grader_command: Option<Vec<String>>,
) {
    if let Some(grader_command) = grader_command {
        let artifact_path_export = if request.agent_artifact.is_some() {
            format!("export {}\n", AGENT_ARTIFACT_PATH_ENV_VALUE)
        } else {
            String::new()
        };
        let grade_error_marker_path = output_peer_path(
            &request.io_paths.result_path,
            BENCHMARK_GRADE_ERROR_FILENAME,
        );
        let wrapped = format!(
            "set +e\n\
             rm -f {marker}\n\
             {artifact_path_export}\
             {agent}\n\
             agent_status=$?\n\
             export {agent_exit_env}=\"$agent_status\"\n\
             if [ ! -s {result_path} ]; then\n\
               printf '%s\\n' \"result_missing\" > {marker}\n\
               if [ \"$agent_status\" -ne 0 ]; then\n\
                 exit \"$agent_status\"\n\
               fi\n\
               exit {grade_error_code}\n\
             fi\n\
             {grader}\n\
             grader_status=$?\n\
             if [ \"$grader_status\" -ne 0 ]; then\n\
               printf '%s\\n' \"grader_command_failed:$grader_status\" > {marker}\n\
             fi\n\
             if [ ! -s \"${{{score_env}}}\" ]; then\n\
               printf '%s\\n' \"score_record_missing\" >> {marker}\n\
             fi\n\
             if [ -s {marker} ]; then\n\
               exit {grade_error_code}\n\
             fi\n\
             if [ \"$agent_status\" -ne 0 ]; then\n\
               exit \"$agent_status\"\n\
             fi\n\
             exit 0",
            marker = shell_quote(&grade_error_marker_path),
            artifact_path_export = artifact_path_export,
            agent = shell_join(command),
            agent_exit_env = AGENTLAB_ENV_AGENT_EXIT_STATUS,
            result_path = shell_quote(&request.io_paths.result_path),
            grader = shell_join(&grader_command),
            score_env = AGENTLAB_ENV_BENCHMARK_SCORE_PATH,
            grade_error_code = BENCHMARK_GRADING_POLICY_EXIT_CODE,
        );
        cmd.arg("/bin/sh");
        cmd.arg("-lc");
        cmd.arg(wrapped);
    } else {
        cmd.args(command);
    }
}

fn build_baked_container_command(
    request: &AdapterRunRequest<'_>,
    plane: ContainerPlane,
    image: &str,
    workspace: Option<&str>,
    command: &[String],
    grader_command: Option<Vec<String>>,
) -> Command {
    let mut cmd = Command::new("docker");
    cmd.arg("run").arg("--rm").args(["--pull", "never"]);
    append_container_platform_arg(&mut cmd, image);
    append_container_sandbox_args(&mut cmd, request, plane, workspace);
    append_container_env_args(&mut cmd, request, workspace);
    cmd.arg(image);
    append_container_entrypoint(&mut cmd, request, command, grader_command);
    cmd
}

fn run_checked_command(mut cmd: Command, step: &str) -> Result<std::process::Output> {
    let out = cmd.output()?;
    if out.status.success() {
        return Ok(out);
    }
    let detail = output_error_detail(&out);
    Err(anyhow!("{}: {}", step, detail))
}

fn output_error_detail(out: &Output) -> String {
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

fn resolve_local_image_alias(image: &str) -> Option<String> {
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

pub(crate) fn append_container_platform_arg(cmd: &mut Command, image: &str) {
    if let Some(platform) = resolve_container_platform(image) {
        cmd.arg("--platform").arg(platform);
    }
}

fn ensure_container_image_ready(image: &str) -> Result<()> {
    let inspect_output = Command::new("docker")
        .args(["image", "inspect", image])
        .output()?;
    if inspect_output.status.success() {
        return Ok(());
    }
    if let Some(local_alias) = resolve_local_image_alias(image) {
        let alias_inspect = Command::new("docker")
            .args(["image", "inspect", &local_alias])
            .output()?;
        if alias_inspect.status.success() {
            emit_preflight_log(format!(
                "container image '{}' missing canonical tag; tagging local alias '{}'",
                image, local_alias
            ));
            let tag_output = Command::new("docker")
                .args(["image", "tag", &local_alias, image])
                .output()?;
            if tag_output.status.success() {
                return Ok(());
            }
            return Err(anyhow!(
                "container image alias '{}' found locally, but failed to tag as '{}': {}",
                local_alias,
                image,
                output_error_detail(&tag_output),
            ));
        }
    }
    emit_preflight_log(format!(
        "container image '{}' not found locally; pulling",
        image
    ));
    let pull_started = Instant::now();
    let pull_output = Command::new("docker").args(["pull", image]).output()?;
    if pull_output.status.success() {
        emit_preflight_log(format!(
            "pulled '{}' in {:.1}s",
            image,
            pull_started.elapsed().as_secs_f32()
        ));
        return Ok(());
    }
    Err(anyhow!(
        "container image not available: {} (pull: {})",
        image,
        output_error_detail(&pull_output),
    ))
}

fn resolve_container_image_digest(image: &str) -> Option<String> {
    let inspect_output = Command::new("docker")
        .args([
            "image",
            "inspect",
            image,
            "--format",
            "{{index .RepoDigests 0}}",
        ])
        .output()
        .ok()?;
    if !inspect_output.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&inspect_output.stdout)
        .trim()
        .to_string();
    if let Some((_, digest)) = raw.rsplit_once('@') {
        if digest.starts_with("sha256:") {
            return Some(digest.to_string());
        }
    }

    let id_output = Command::new("docker")
        .args(["image", "inspect", image, "--format", "{{.Id}}"])
        .output()
        .ok()?;
    if !id_output.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&id_output.stdout)
        .trim()
        .to_string();
    if raw.starts_with("sha256:") {
        Some(raw)
    } else {
        None
    }
}

fn agent_artifact_cache_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn repair_agent_artifact_layout(unpacked_dir: &Path) -> Result<()> {
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

fn resolve_runtime_agent_command(request: &AdapterRunRequest<'_>) -> Result<Vec<String>> {
    if request.runtime.command_raw.is_empty() {
        return Err(anyhow!("resolved runtime.agent_runtime.command is empty"));
    }
    let mut command = request.runtime.command_raw.clone();
    command.extend(request.variant_args.iter().cloned());
    Ok(command)
}

struct PreflightProbeRoot {
    path: PathBuf,
}

impl Drop for PreflightProbeRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct PreflightProbeContext {
    _root: PreflightProbeRoot,
    trial_paths: TrialPaths,
    io_paths: PreparedTrialIo,
    dynamic_mounts: Vec<ResolvedMountReference>,
    runtime_env: BTreeMap<String, String>,
    task_image: Option<String>,
}

fn create_preflight_probe_root(label: &str) -> Result<PreflightProbeRoot> {
    let root = std::env::temp_dir().join(format!(
        "agentlab_preflight_probe_{}_{}_{}",
        sanitize_for_fs(label),
        std::process::id(),
        Utc::now().timestamp_micros()
    ));
    ensure_dir(&root)?;
    Ok(PreflightProbeRoot { path: root })
}

fn select_preflight_probe_task(
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

fn build_preflight_probe_context(
    runtime_profile: &VariantRuntimeProfile,
    variant: &Variant,
    tasks: &[Value],
    image: &str,
    project_root: &Path,
) -> Result<PreflightProbeContext> {
    let (task_idx, task_boundary) = select_preflight_probe_task(tasks, image)?;
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
        Some(task_boundary.task_image.as_str()),
        smoke_timeout_ms,
    );
    runtime_env.insert(AGENTLAB_ENV_PREFLIGHT_SMOKE.to_string(), "1".to_string());
    Ok(PreflightProbeContext {
        _root: probe_root,
        trial_paths: prepared.trial_paths,
        io_paths,
        dynamic_mounts: prepared.dynamic_mounts,
        runtime_env,
        task_image: Some(task_boundary.task_image),
    })
}

fn build_preflight_probe_request<'a>(
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
        task_image: context.task_image.as_deref(),
        agent_artifact: Some(runtime_profile.agent_runtime.agent_artifact.as_path()),
    }
}

struct PreflightContractSmokeExecution {
    status: String,
    stdout: String,
    stderr: String,
}

fn read_optional_text_file(path: &Path) -> Result<String> {
    if !path.exists() {
        return Ok(String::new());
    }
    Ok(fs::read_to_string(path)?)
}

fn run_preflight_contract_smoke(
    request: &AdapterRunRequest<'_>,
) -> Result<PreflightContractSmokeExecution> {
    let adapter = adapter_registry_entry(&request.runtime.adapter_ref)?;
    let proc_result = adapter.run_trial(request)?;
    let stdout =
        read_optional_text_file(&request.trial_paths.trial_dir.join("harness_stdout.log"))?;
    let stderr =
        read_optional_text_file(&request.trial_paths.trial_dir.join("harness_stderr.log"))?;
    Ok(PreflightContractSmokeExecution {
        status: proc_result.status,
        stdout,
        stderr,
    })
}

fn detect_known_probe_output_blockers(stdout: &str, stderr: &str) -> Vec<String> {
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

fn summarize_preflight_failure_logs(stdout: &str, stderr: &str) -> Vec<String> {
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

fn validate_preflight_result_payload(path: &Path) -> Vec<String> {
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

fn validate_preflight_benchmark_smoke_outputs(
    request: &AdapterRunRequest<'_>,
    status: &str,
) -> Vec<String> {
    let mut failures = Vec::new();
    let prediction_path = request.trial_paths.out.join(BENCHMARK_PREDICTION_FILENAME);
    let score_path = request.trial_paths.out.join(BENCHMARK_SCORE_FILENAME);
    let grade_error_path = request.trial_paths.out.join(BENCHMARK_GRADE_ERROR_FILENAME);

    match load_optional_json_record_with_schema(
        "benchmark_prediction_record_v1.jsonschema",
        &prediction_path,
    ) {
        Ok(Some(_)) => {}
        Ok(None) => failures.push(format!(
            "contract smoke did not write benchmark prediction record: {}",
            prediction_path.display()
        )),
        Err(err) => failures.push(format!("benchmark prediction record invalid: {}", err)),
    }
    match load_optional_json_record_with_schema("benchmark_score_record_v1.jsonschema", &score_path)
    {
        Ok(Some(_)) => {}
        Ok(None) => failures.push(format!(
            "contract smoke did not write benchmark score record: {}",
            score_path.display()
        )),
        Err(err) => failures.push(format!("benchmark score record invalid: {}", err)),
    }
    if grade_error_path.exists() {
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
    } else if status == BENCHMARK_GRADING_POLICY_EXIT_CODE.to_string() {
        failures.push(format!(
            "benchmark smoke exited with grading policy code {} without a grade error marker",
            BENCHMARK_GRADING_POLICY_EXIT_CODE
        ));
    }
    failures
}

fn collect_preflight_contract_smoke_failures(
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
    let result_failures = validate_preflight_result_payload(&request.io_paths.output_host);
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

fn resolve_agent_runtime_command(
    command: &[String],
    bindings: &Value,
    runtime_env_inputs: &BTreeMap<String, String>,
) -> Result<Vec<String>> {
    resolve_command_templates(command, bindings, runtime_env_inputs)
}

fn validate_agent_runtime_command(command: &[String], _project_root: &Path) -> Result<()> {
    if command.is_empty() {
        return Err(anyhow!("runtime.agent_runtime.command must not be empty"));
    }
    Ok(())
}

fn ensure_clean_output_file(path: &Path, label: &str) -> Result<()> {
    if !path.exists() {
        if let Some(parent) = path.parent() {
            ensure_dir(parent)?;
        }
        return Ok(());
    }
    if path.is_file() {
        fs::remove_file(path)?;
        if let Some(parent) = path.parent() {
            ensure_dir(parent)?;
        }
        return Ok(());
    }
    Err(anyhow!("{} must be a file path: {}", label, path.display()))
}

fn copy_stream_to_file<R: Read>(mut reader: R, mut file: fs::File) -> Result<()> {
    let mut buf = [0u8; 8192];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])?;
    }
    file.flush()?;
    Ok(())
}

fn terminate_child_process(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn run_adapter_process(
    mut cmd: Command,
    output_path: &Path,
    start_response_path: Option<&Path>,
    stdout_log_path: &Path,
    stderr_log_path: &Path,
) -> Result<ProcessRunResult> {
    ensure_clean_output_file(output_path, "output path")?;
    ensure_clean_output_file(stdout_log_path, "stdout log path")?;
    ensure_clean_output_file(stderr_log_path, "stderr log path")?;

    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    let stdout_pipe = match child.stdout.take() {
        Some(pipe) => pipe,
        None => {
            terminate_child_process(&mut child);
            return Err(anyhow!("failed to capture child stdout pipe"));
        }
    };
    let stderr_pipe = match child.stderr.take() {
        Some(pipe) => pipe,
        None => {
            terminate_child_process(&mut child);
            return Err(anyhow!("failed to capture child stderr pipe"));
        }
    };

    let stdout_file = match fs::File::create(stdout_log_path) {
        Ok(file) => file,
        Err(err) => {
            terminate_child_process(&mut child);
            return Err(err.into());
        }
    };
    let stderr_file = match fs::File::create(stderr_log_path) {
        Ok(file) => file,
        Err(err) => {
            terminate_child_process(&mut child);
            return Err(err.into());
        }
    };

    let stdout_handle = match thread::Builder::new()
        .name("agentlab-stdout-capture".to_string())
        .spawn(move || copy_stream_to_file(stdout_pipe, stdout_file))
    {
        Ok(handle) => handle,
        Err(err) => {
            terminate_child_process(&mut child);
            return Err(anyhow!("failed to spawn stdout capture thread: {}", err));
        }
    };
    let stderr_handle = match thread::Builder::new()
        .name("agentlab-stderr-capture".to_string())
        .spawn(move || copy_stream_to_file(stderr_pipe, stderr_file))
    {
        Ok(handle) => handle,
        Err(err) => {
            terminate_child_process(&mut child);
            let _ = stdout_handle.join();
            return Err(anyhow!("failed to spawn stderr capture thread: {}", err));
        }
    };

    if let Some(path) = start_response_path {
        if let Err(err) = wait_for_file(path, Duration::from_secs(10)) {
            terminate_child_process(&mut child);
            let _ = stdout_handle.join();
            let _ = stderr_handle.join();
            return Err(err);
        }
    }

    let status = child.wait()?;
    match stdout_handle.join() {
        Ok(result) => result?,
        Err(_) => return Err(anyhow!("stdout capture thread panicked")),
    }
    match stderr_handle.join() {
        Ok(result) => result?,
        Err(_) => return Err(anyhow!("stderr capture thread panicked")),
    }

    Ok(ProcessRunResult {
        status: status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string()),
    })
}

fn wait_for_file(path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err(anyhow!(
        "timeout waiting for file {} after {:?}",
        path.display(),
        timeout
    ))
}

fn shell_join(parts: &[String]) -> String {
    parts
        .iter()
        .map(|p| shell_quote(p))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(s: &str) -> String {
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

fn trial_output_error_payload(code: &str, message: &str) -> Value {
    json!({
        "schema_version": "agent_result_v1",
        "outcome": "error",
        "error": {
            "code": code,
            "message": message,
        },
    })
}

fn load_trial_output_resilient(path: &Path) -> Result<(Value, Option<String>)> {
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

fn resolve_trial_io_host_path(path: &str, paths: &TrialPaths) -> Result<PathBuf> {
    map_container_path_to_host(path, paths)
}

fn prepare_io_paths(paths: &TrialPaths, input_bytes: &[u8]) -> Result<PreparedTrialIo> {
    let task_path = DEFAULT_CONTAINER_TASK_PATH.to_string();
    let bindings_path = DEFAULT_CONTAINER_BINDINGS_PATH.to_string();
    let dependencies_path = DEFAULT_CONTAINER_DEPENDENCIES_PATH.to_string();
    let policy_path = DEFAULT_CONTAINER_POLICY_PATH.to_string();
    let result_path = DEFAULT_CONTAINER_RESULT_PATH.to_string();
    let trajectory_path = DEFAULT_CONTAINER_TRAJECTORY_PATH.to_string();
    let task_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_TASK_PATH, paths)?;
    let bindings_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_BINDINGS_PATH, paths)?;
    let dependencies_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_DEPENDENCIES_PATH, paths)?;
    let policy_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_POLICY_PATH, paths)?;
    let result_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_RESULT_PATH, paths)?;
    let trajectory_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_TRAJECTORY_PATH, paths)?;
    let input_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_TRIAL_INPUT_PATH, paths)?;
    let output_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_RESULT_PATH, paths)?;
    let events_host = resolve_trial_io_host_path(DEFAULT_CONTAINER_TRAJECTORY_PATH, paths)?;

    for host_path in [
        &task_host,
        &bindings_host,
        &dependencies_host,
        &policy_host,
        &result_host,
        &trajectory_host,
        &input_host,
    ] {
        if let Some(parent) = host_path.parent() {
            ensure_dir(parent)?;
        }
    }

    if let Some(parent) = input_host.parent() {
        ensure_dir(parent)?;
    }
    fs::write(&input_host, input_bytes)?;

    let input_value: Value = serde_json::from_slice(input_bytes)?;
    let task_value = input_value.pointer("/task").cloned().unwrap_or(json!({}));
    let bindings_value = input_value
        .pointer("/bindings")
        .cloned()
        .unwrap_or(json!({}));
    let dependencies_value = input_value
        .pointer("/dependencies")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let policy_value = input_value
        .pointer("/policy")
        .cloned()
        .unwrap_or_else(|| json!({}));

    atomic_write_json_pretty(&task_host, &task_value)?;
    atomic_write_json_pretty(&bindings_host, &bindings_value)?;
    atomic_write_json_pretty(&dependencies_host, &dependencies_value)?;
    atomic_write_json_pretty(&policy_host, &policy_value)?;

    if result_host.exists() {
        let _ = fs::remove_file(&result_host);
    }
    if trajectory_host.exists() {
        let _ = fs::remove_file(&trajectory_host);
    }

    Ok(PreparedTrialIo {
        input_host,
        output_host,
        events_host,
        task_path,
        bindings_path,
        dependencies_path,
        policy_path,
        result_path,
        trajectory_path,
    })
}

fn materialize_trial_result(trial_dir: &Path, output_path: &Path) -> Result<PathBuf> {
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

fn copy_file_if_exists(src: &Path, dst: &Path) -> Result<()> {
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

fn copy_dir_preserve_contents(src: &Path, dst: &Path) -> Result<()> {
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

fn materialize_trial_runtime_layout(
    trial_dir: &Path,
    paths: &TrialPaths,
    mode: MaterializationMode,
) -> Result<()> {
    match mode {
        MaterializationMode::Full => {
            copy_dir_preserve_contents(&paths.in_dir, &trial_dir.join("in"))?;
            copy_dir_preserve_contents(&paths.out, &trial_dir.join("out"))?;
            copy_dir_preserve_contents(&paths.state, &trial_dir.join("state"))?;
            copy_dir_preserve_contents(&paths.deps, &trial_dir.join("deps"))?;
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

fn write_adapter_continue_control(path: &Path) -> Result<()> {
    let _ = write_adapter_control_action(path, 0, "continue", None, "run_loop")?;
    Ok(())
}

fn write_adapter_control_action(
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

fn resolve_agent_runtime_manifest_path(paths: &TrialPaths) -> Result<PathBuf> {
    map_container_path_to_host(
        &format!("{}/harness_manifest.json", AGENTLAB_CONTRACT_OUT_DIR),
        paths,
    )
}

fn resolve_exec_digest(command: &[String], exp_dir: &Path) -> Result<String> {
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

fn write_state_inventory(
    trial_dir: &Path,
    json_value: &Value,
    agent_runtime: &AgentRuntimeConfig,
    _paths: &TrialPaths,
    exec_digest: &str,
    effective_network_mode: &str,
    invocation_source: &str,
    task_sandbox_image: Option<&str>,
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

    let mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workspace", "path": AGENTLAB_CONTRACT_WORKSPACE_DIR, "writable": true}),
        json!({"name": "state", "path": AGENTLAB_CONTRACT_STATE_DIR, "writable": true}),
        json!({"name": "deps", "path": AGENTLAB_CONTRACT_DEPS_DIR, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    let mut agent_runtime_mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workspace", "path": AGENTLAB_CONTRACT_WORKSPACE_DIR, "writable": true}),
        json!({"name": "state", "path": AGENTLAB_CONTRACT_STATE_DIR, "writable": true}),
        json!({"name": "deps", "path": AGENTLAB_CONTRACT_DEPS_DIR, "writable": true}),
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
        json!({"name": "workspace", "path": AGENTLAB_CONTRACT_WORKSPACE_DIR, "writable": true}),
        json!({"name": "state", "path": AGENTLAB_CONTRACT_STATE_DIR, "writable": true}),
        json!({"name": "deps", "path": AGENTLAB_CONTRACT_DEPS_DIR, "writable": true}),
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
                "workspace": AGENTLAB_CONTRACT_WORKSPACE_DIR,
                "mounts": agent_runtime_mounts,
                "network_mode": agent_runtime.network
            },
            "task_sandbox": {
                "executor": "docker",
                "image": task_sandbox_image,
                "image_digest": task_sandbox_image_digest,
                "workspace": AGENTLAB_CONTRACT_WORKSPACE_DIR,
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

fn remove_path_if_exists(path: &Path) -> Result<()> {
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
fn make_path_tree_writable(path: &Path) -> Result<()> {
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
fn make_path_tree_writable(_path: &Path) -> Result<()> {
    Ok(())
}

fn preserve_symlink(path: &Path, target: &Path) -> Result<()> {
    let link_target = fs::read_link(path)?;
    remove_path_if_exists(target)?;
    #[cfg(unix)]
    {
        symlink(&link_target, target)?;
    }
    Ok(())
}

fn apply_materialization_policy(trial_dir: &Path, mode: MaterializationMode) -> Result<()> {
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

fn map_container_path_to_host(path: &str, paths: &TrialPaths) -> Result<PathBuf> {
    map_contract_path_to_host(
        path,
        &ContractPathHostRoots::from_trial_paths(paths),
        ContractPathMode::ContainerMount,
    )
}

fn load_event_rows(
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

fn build_metric_rows(
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

fn build_variant_snapshot_rows(
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

fn binding_value_to_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn copy_dir_with_policy(
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

fn copy_dir_filtered(src: &Path, dst: &Path, exclude: &[&str]) -> Result<()> {
    copy_dir_with_policy(src, dst, exclude, true)
}

fn copy_dir_preserve_all(src: &Path, dst: &Path, exclude: &[&str]) -> Result<()> {
    copy_dir_with_policy(src, dst, exclude, false)
}

fn command_part_looks_like_path(part: &str) -> bool {
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

fn resolve_command_digest_target(command: &[String]) -> Option<&str> {
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
