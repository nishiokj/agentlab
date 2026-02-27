"""Unified diff patch application with workspace enforcement and policy checking."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from bench.tools.protocol import ApplyPatchResponse


def parse_patch_files(patch_text: str) -> list[str]:
    """Extract file paths touched by a unified diff patch."""
    files: set[str] = set()
    for line in patch_text.splitlines():
        # Match --- a/path and +++ b/path
        m = re.match(r'^(?:\+\+\+|---)\s+[ab]/(.+)', line)
        if m:
            files.add(m.group(1))
        # Also match diff --git a/path b/path
        m = re.match(r'^diff --git a/(.+?) b/(.+)', line)
        if m:
            files.add(m.group(1))
            files.add(m.group(2))
    return sorted(files)


def check_patch_escapes_workspace(patch_text: str) -> list[str]:
    """Check if any paths in the patch try to escape the workspace."""
    violations = []
    for fpath in parse_patch_files(patch_text):
        normalized = Path(fpath).as_posix()
        if normalized.startswith("..") or normalized.startswith("/"):
            violations.append(f"Path escapes workspace: {fpath}")
    return violations


def apply_patch(
    workspace: Path,
    patch_text: str,
    policy_check: callable | None = None,
) -> ApplyPatchResponse:
    """Apply a unified diff patch to the workspace.

    Args:
        workspace: Root directory for patch application.
        patch_text: Unified diff content.
        policy_check: Optional callable(list[str]) -> list[str] returning policy violations.

    Returns:
        ApplyPatchResponse with applied/rejected files and policy violations.
    """
    if not patch_text.strip():
        return ApplyPatchResponse(success=False, rejected_files=[], policy_violations=["Empty patch"])

    # Check for workspace escapes
    escape_violations = check_patch_escapes_workspace(patch_text)
    if escape_violations:
        return ApplyPatchResponse(
            success=False,
            policy_violations=escape_violations,
        )

    # Extract files and check policy
    files = parse_patch_files(patch_text)
    if policy_check is not None:
        violations = policy_check(files)
        if violations:
            return ApplyPatchResponse(
                success=False,
                applied_files=[],
                rejected_files=files,
                policy_violations=violations,
            )

    # Apply with git apply
    try:
        result = subprocess.run(
            ["git", "apply", "--stat", "--summary", "-"],
            input=patch_text,
            capture_output=True,
            text=True,
            cwd=str(workspace),
            timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Actually apply
    try:
        result = subprocess.run(
            ["git", "apply", "--verbose", "-"],
            input=patch_text,
            capture_output=True,
            text=True,
            cwd=str(workspace),
            timeout=30,
        )
        if result.returncode == 0:
            return ApplyPatchResponse(
                success=True,
                applied_files=files,
                rejected_files=[],
                policy_violations=[],
            )
        else:
            # Try with --3way or plain patch
            result2 = subprocess.run(
                ["patch", "-p1", "--batch", "--forward"],
                input=patch_text,
                capture_output=True,
                text=True,
                cwd=str(workspace),
                timeout=30,
            )
            if result2.returncode == 0:
                return ApplyPatchResponse(
                    success=True,
                    applied_files=files,
                    rejected_files=[],
                    policy_violations=[],
                )
            return ApplyPatchResponse(
                success=False,
                applied_files=[],
                rejected_files=files,
                policy_violations=[f"Patch failed: {result.stderr.strip()}"],
            )
    except subprocess.TimeoutExpired:
        return ApplyPatchResponse(
            success=False,
            policy_violations=["Patch application timed out"],
        )
    except FileNotFoundError:
        return ApplyPatchResponse(
            success=False,
            policy_violations=["Neither git nor patch command found"],
        )
