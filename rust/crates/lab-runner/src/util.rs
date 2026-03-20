use anyhow::{anyhow, Result};
use lab_core::ensure_dir;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};

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

pub(crate) fn copy_dir_with_policy(
    src: &Path,
    dst: &Path,
    exclude: &[&str],
) -> Result<()> {
    let walker = walkdir::WalkDir::new(src).into_iter().filter_entry(|e| {
        let rel = e.path().strip_prefix(src).unwrap_or(e.path());
        if rel.as_os_str().is_empty() {
            return true; // root entry
        }
        if exclude.iter().any(|ex| rel.starts_with(ex)) {
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
    copy_dir_with_policy(src, dst, exclude)
}

pub(crate) fn copy_dir_preserve_all(src: &Path, dst: &Path, exclude: &[&str]) -> Result<()> {
    copy_dir_with_policy(src, dst, exclude)
}

pub(crate) fn output_peer_path(output_path: &str, file_name: &str) -> String {
    let output = Path::new(output_path);
    if let Some(parent) = output.parent() {
        return parent.join(file_name).to_string_lossy().to_string();
    }
    file_name.to_string()
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
