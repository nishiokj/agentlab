"""Patch policy enforcement for benchmark task grading."""

from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import Any

from bench.paths import HARNESS_DENY_PATTERNS, REPO_CONFIG_DENY_PATTERNS


def _glob_match(filepath: str, pattern: str) -> bool:
    if "**" in pattern:
        parts = pattern.split("**")
        if len(parts) == 2:
            prefix = parts[0].rstrip("/")
            suffix = parts[1].lstrip("/")
            if prefix and not filepath.startswith(prefix.rstrip("*")):
                return False
            if suffix:
                remaining = filepath[len(prefix):].lstrip("/") if prefix else filepath
                return fnmatch.fnmatch(remaining, suffix) or fnmatch.fnmatch(filepath, suffix)
            return True
    return fnmatch.fnmatch(filepath, pattern)


class PatchPolicy:
    """Evaluates whether an edited file set is allowed by task policy."""

    def __init__(
        self,
        allow_edit_globs: list[str] | None = None,
        deny_edit_globs: list[str] | None = None,
    ) -> None:
        self.allow_edit_globs = allow_edit_globs or ["**/*.py"]
        self.deny_edit_globs = list(HARNESS_DENY_PATTERNS) + list(REPO_CONFIG_DENY_PATTERNS)
        if deny_edit_globs:
            self.deny_edit_globs.extend(deny_edit_globs)

    @classmethod
    def from_task(cls, task_data: dict[str, Any]) -> PatchPolicy:
        policy = task_data.get("patch_policy", {})
        return cls(
            allow_edit_globs=policy.get("allow_edit_globs"),
            deny_edit_globs=policy.get("deny_edit_globs"),
        )

    def check_files(self, files: list[str]) -> list[str]:
        violations: list[str] = []
        for fpath in files:
            for pattern in self.deny_edit_globs:
                if _glob_match(fpath, pattern):
                    violations.append(
                        f"POLICY_VIOLATION: '{fpath}' matches deny pattern '{pattern}'"
                    )
                    break
            else:
                allowed = any(_glob_match(fpath, pattern) for pattern in self.allow_edit_globs)
                if not allowed:
                    violations.append(
                        f"POLICY_VIOLATION: '{fpath}' does not match any allow pattern"
                    )
        return violations

