"""Command policy enforcement.

Separate module for command allowlist management.
Works with PatchPolicy for unified policy enforcement.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


# Default command allowlist
DEFAULT_ALLOWED_COMMANDS = [
    "python",
    "python3",
    "pytest",
    "pip",
    "bash",
    "sh",
    "cat",
    "echo",
    "head",
    "tail",
    "grep",
    "find",
    "ls",
    "wc",
    "sort",
    "diff",
]


class CommandPolicy:
    """Command allowlist enforcement."""

    def __init__(self, allowed_commands: list[str] | None = None):
        self.allowed_commands = allowed_commands or DEFAULT_ALLOWED_COMMANDS

    @classmethod
    def from_task(cls, task_data: dict[str, Any]) -> CommandPolicy:
        policy = task_data.get("patch_policy", {})
        return cls(allowed_commands=policy.get("allow_run_globs"))

    def check(self, command: list[str]) -> list[str]:
        """Check if a command is allowed. Returns violations."""
        if not command:
            return ["POLICY_VIOLATION: empty command"]
        cmd_name = Path(command[0]).name
        if cmd_name not in self.allowed_commands:
            return [
                f"POLICY_VIOLATION: command '{cmd_name}' not in allowlist"
            ]
        return []
