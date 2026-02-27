"""Patch and command policy enforcement.

Prevents trivial bypass by enforcing strict rules on what files
agents may edit and what commands they may run.
"""

from __future__ import annotations

import fnmatch
from pathlib import Path, PurePosixPath
from typing import Any

from bench.paths import HARNESS_DENY_PATTERNS, REPO_CONFIG_DENY_PATTERNS


def _glob_match(filepath: str, pattern: str) -> bool:
    """Match a filepath against a glob pattern, supporting ** for recursive matching."""
    # PurePosixPath.match handles ** correctly in Python 3.12+,
    # but for 3.11 we need a manual approach
    if "**" in pattern:
        # Split pattern on ** and check each part
        parts = pattern.split("**")
        if len(parts) == 2:
            prefix = parts[0].rstrip("/")
            suffix = parts[1].lstrip("/")
            # Check if path starts with prefix (if non-empty)
            if prefix and not filepath.startswith(prefix.rstrip("*")):
                return False
            # Check if the remaining path matches the suffix pattern
            if suffix:
                # Get everything after the prefix
                remaining = filepath[len(prefix):].lstrip("/") if prefix else filepath
                return fnmatch.fnmatch(remaining, suffix) or fnmatch.fnmatch(filepath, suffix)
            return True
        return fnmatch.fnmatch(filepath, pattern)
    return fnmatch.fnmatch(filepath, pattern)


class PatchPolicy:
    """Evaluates whether a patch or command is allowed per task policy."""

    def __init__(
        self,
        allow_edit_globs: list[str] | None = None,
        deny_edit_globs: list[str] | None = None,
        allow_run_globs: list[str] | None = None,
    ) -> None:
        self.allow_edit_globs = allow_edit_globs or ["**/*.py"]
        self.deny_edit_globs = list(HARNESS_DENY_PATTERNS) + list(REPO_CONFIG_DENY_PATTERNS)
        if deny_edit_globs:
            self.deny_edit_globs.extend(deny_edit_globs)
        self.allow_run_globs = allow_run_globs or ["python", "python3", "pytest"]

    @classmethod
    def from_task(cls, task_data: dict[str, Any]) -> PatchPolicy:
        """Create policy from task.yaml data."""
        policy = task_data.get("patch_policy", {})
        return cls(
            allow_edit_globs=policy.get("allow_edit_globs"),
            deny_edit_globs=policy.get("deny_edit_globs"),
            allow_run_globs=policy.get("allow_run_globs"),
        )

    def check_files(self, files: list[str]) -> list[str]:
        """Check a list of file paths against the policy.

        Returns a list of violation descriptions (empty = all OK).
        """
        violations = []
        for fpath in files:
            # Check deny list first (deny overrides allow)
            for pattern in self.deny_edit_globs:
                if _glob_match(fpath, pattern):
                    violations.append(
                        f"POLICY_VIOLATION: '{fpath}' matches deny pattern '{pattern}'"
                    )
                    break
            else:
                # Check allow list
                allowed = any(
                    _glob_match(fpath, pattern)
                    for pattern in self.allow_edit_globs
                )
                if not allowed:
                    violations.append(
                        f"POLICY_VIOLATION: '{fpath}' does not match any allow pattern"
                    )
        return violations

    def check_command(self, command: list[str]) -> list[str]:
        """Check if a command is allowed.

        Returns a list of violation descriptions (empty = OK).
        """
        if not command:
            return ["POLICY_VIOLATION: empty command"]
        cmd_name = Path(command[0]).name
        if cmd_name not in self.allow_run_globs:
            return [
                f"POLICY_VIOLATION: command '{cmd_name}' not in allowlist "
                f"({', '.join(self.allow_run_globs)})"
            ]
        return []

    def as_policy_check(self):
        """Return a callable suitable for apply_patch's policy_check parameter."""
        return self.check_files


class PatchClassifier:
    """Classify policy violations with actionable labels."""

    LABELS = {
        "harness_edit": "Attempted to edit benchmark harness files",
        "config_edit": "Attempted to edit repo configuration files",
        "test_runner_edit": "Attempted to edit test runner configuration",
        "disallowed_file": "Edited a file not in the allow list",
        "disallowed_command": "Ran a disallowed command",
        "workspace_escape": "Attempted to access files outside workspace",
    }

    @staticmethod
    def classify(violation: str) -> str:
        """Return a classification label for a policy violation string."""
        v_lower = violation.lower()
        if "bench/" in v_lower or "schemas/" in v_lower or "scripts/" in v_lower:
            return "harness_edit"
        if any(p in v_lower for p in ["pytest.ini", "setup.cfg", "tox.ini", "noxfile"]):
            return "config_edit"
        if "conftest" in v_lower:
            return "test_runner_edit"
        if "escape" in v_lower:
            return "workspace_escape"
        if "command" in v_lower:
            return "disallowed_command"
        return "disallowed_file"
