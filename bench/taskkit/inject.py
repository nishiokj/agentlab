"""Bug/feature injection pipeline.

Standardizes how tasks inject bugs/features into repo snapshots.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Any

from bench.runner.patch_policy import PatchPolicy
from bench.tools.patch import parse_patch_files


def validate_injection_patch(
    patch_path: Path,
    deny_patterns: list[str] | None = None,
) -> list[str]:
    """Validate an injection patch.

    Returns list of errors (empty = valid).
    """
    errors = []
    if not patch_path.exists():
        errors.append(f"Injection patch not found: {patch_path}")
        return errors

    patch_text = patch_path.read_text()
    if not patch_text.strip():
        errors.append("Injection patch is empty")
        return errors

    files = parse_patch_files(patch_text)
    if not files:
        errors.append("Injection patch touches no files")
        return errors

    # Check against deny patterns
    if deny_patterns:
        import fnmatch
        for fpath in files:
            for pattern in deny_patterns:
                if fnmatch.fnmatch(fpath, pattern):
                    errors.append(
                        f"Injection patch modifies denylisted file: {fpath} "
                        f"(matches {pattern})"
                    )
    return errors


def get_injection_manifest(patch_path: Path) -> dict[str, Any]:
    """Get a manifest of files changed by an injection patch."""
    patch_text = patch_path.read_text()
    files = parse_patch_files(patch_text)
    return {
        "patch_file": str(patch_path),
        "files_changed": files,
        "patch_hash": hashlib.sha256(patch_text.encode()).hexdigest(),
    }


def apply_injection(
    workspace: Path,
    patch_path: Path,
) -> dict[str, Any]:
    """Apply an injection patch to a workspace.

    Returns manifest of changes.
    """
    patch_text = patch_path.read_text()
    files = parse_patch_files(patch_text)

    result = subprocess.run(
        ["git", "apply", "--verbose", "-"],
        input=patch_text,
        capture_output=True,
        text=True,
        cwd=str(workspace),
        timeout=30,
    )

    success = result.returncode == 0
    if not success:
        result2 = subprocess.run(
            ["patch", "-p1", "--batch", "--forward"],
            input=patch_text,
            capture_output=True,
            text=True,
            cwd=str(workspace),
            timeout=30,
        )
        success = result2.returncode == 0

    return {
        "success": success,
        "files_changed": files,
        "patch_hash": hashlib.sha256(patch_text.encode()).hexdigest(),
        "error": result.stderr if not success else None,
    }


def compute_workspace_tree_hash(workspace: Path) -> str:
    """Compute a deterministic hash of the workspace file tree."""
    import os
    h = hashlib.sha256()
    for root, dirs, files in os.walk(workspace):
        dirs.sort()
        for fname in sorted(files):
            fpath = Path(root) / fname
            relpath = fpath.relative_to(workspace).as_posix()
            h.update(relpath.encode())
            h.update(fpath.read_bytes())
    return h.hexdigest()
