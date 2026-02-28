"""Patch parsing and safety checks for taskkit flows."""

from __future__ import annotations

import re
from pathlib import Path


def parse_patch_files(patch_text: str) -> list[str]:
    """Extract workspace-relative file paths touched by a unified diff patch."""
    files: set[str] = set()
    for line in patch_text.splitlines():
        m = re.match(r"^(?:\+\+\+|---)\s+[ab]/(.+)", line)
        if m:
            files.add(m.group(1))
        m = re.match(r"^diff --git a/(.+?) b/(.+)", line)
        if m:
            files.add(m.group(1))
            files.add(m.group(2))
    return sorted(files)


def check_patch_escapes_workspace(patch_text: str) -> list[str]:
    """Return violations for patch paths that attempt to escape workspace root."""
    violations: list[str] = []
    for fpath in parse_patch_files(patch_text):
        normalized = Path(fpath).as_posix()
        if normalized.startswith("..") or normalized.startswith("/"):
            violations.append(f"Path escapes workspace: {fpath}")
    return violations

