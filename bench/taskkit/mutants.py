"""Mutant gate automation.

Applies mutant patches and verifies hidden suite fails for each one,
ensuring the test suite is robust enough to detect common bypass strategies.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from bench.taskkit.hidden_runner import run_hidden_suite, HiddenSuiteResult


@dataclass
class MutantResult:
    """Result of running the hidden suite against one mutant."""
    mutant_id: str
    patch_file: str
    killed: bool  # True if hidden suite FAILS (expected behavior)
    hidden_result: dict[str, Any] = field(default_factory=dict)
    failure_type: str | None = None  # "assertion", "error", "timeout", etc.
    failing_case_ids: list[str] = field(default_factory=list)


@dataclass
class MutantGateResult:
    """Aggregate result of the mutant gate."""
    mutants_total: int = 0
    mutants_killed: int = 0
    unkilled_list: list[str] = field(default_factory=list)
    results: list[MutantResult] = field(default_factory=list)
    passed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


MUTANT_STRATEGIES = [
    "swallow_error",
    "default_return",
    "special_case",
    "weaken_validation",
    "incorrect_boundary",
    "skip_step",
    "wrong_type",
    "off_by_one",
    "missing_edge_case",
    "hardcode_value",
]


def list_mutant_patches(task_dir: Path) -> list[Path]:
    """List all mutant patch files in the task's mutants/ directory."""
    mutants_dir = task_dir / "mutants"
    if not mutants_dir.is_dir():
        return []
    patches = sorted(
        p for p in mutants_dir.glob("*.patch")
        if p.name.startswith("M") and p.name[1:3].isdigit()
    )
    return patches


def apply_mutant_patch(workspace: Path, patch_path: Path) -> bool:
    """Apply a mutant patch to the workspace. Returns True if successful."""
    import subprocess
    patch_text = patch_path.read_text()
    result = subprocess.run(
        ["git", "apply", "--verbose", "-"],
        input=patch_text,
        capture_output=True,
        text=True,
        cwd=str(workspace),
        timeout=30,
    )
    if result.returncode == 0:
        return True
    # Try plain patch
    result2 = subprocess.run(
        ["patch", "-p1", "--batch", "--forward"],
        input=patch_text,
        capture_output=True,
        text=True,
        cwd=str(workspace),
        timeout=30,
    )
    return result2.returncode == 0


def revert_mutant_patch(workspace: Path, patch_path: Path) -> bool:
    """Revert a mutant patch from the workspace."""
    import subprocess
    patch_text = patch_path.read_text()
    result = subprocess.run(
        ["git", "apply", "--reverse", "-"],
        input=patch_text,
        capture_output=True,
        text=True,
        cwd=str(workspace),
        timeout=30,
    )
    return result.returncode == 0


def classify_failure(hidden_result: HiddenSuiteResult) -> str:
    """Classify why the hidden suite failed for a mutant."""
    if hidden_result.timed_out:
        return "timeout"
    if hidden_result.errors > 0:
        return "error"
    if hidden_result.failed > 0:
        return "assertion"
    return "unknown"


def run_mutant_gate(
    task_dir: Path,
    workspace: Path,
    hidden_dir: Path,
    task_data: dict[str, Any],
    min_mutants: int = 10,
    hidden_timeout: int = 60,
) -> MutantGateResult:
    """Run the mutant gate: apply each mutant and verify hidden suite fails.

    Args:
        task_dir: Task bundle directory.
        workspace: Workspace with the correct solution applied.
        hidden_dir: Hidden directory with runner.py and cases.jsonl.
        task_data: Task configuration.
        min_mutants: Minimum required mutants.
        hidden_timeout: Timeout for hidden suite per mutant.
    """
    patches = list_mutant_patches(task_dir)
    if len(patches) < min_mutants:
        return MutantGateResult(
            mutants_total=len(patches),
            passed=False,
        )

    results: list[MutantResult] = []
    killed_count = 0
    unkilled: list[str] = []

    for patch_path in patches:
        mutant_id = patch_path.stem  # e.g. M01

        # Apply mutant
        applied = apply_mutant_patch(workspace, patch_path)
        if not applied:
            results.append(MutantResult(
                mutant_id=mutant_id,
                patch_file=patch_path.name,
                killed=False,
                failure_type="apply_failed",
            ))
            unkilled.append(mutant_id)
            continue

        # Run hidden suite (should FAIL)
        hidden_result = run_hidden_suite(
            workspace=workspace,
            hidden_dir=hidden_dir,
            task_data=task_data,
            timeout=hidden_timeout,
        )

        # Mutant is "killed" if hidden suite does NOT all pass
        killed = not hidden_result.all_passed
        if killed:
            killed_count += 1
            failing_ids = [
                r.case_id for r in hidden_result.case_results if not r.passed
            ]
        else:
            failing_ids = []
            unkilled.append(mutant_id)

        results.append(MutantResult(
            mutant_id=mutant_id,
            patch_file=patch_path.name,
            killed=killed,
            hidden_result=hidden_result.to_dict(),
            failure_type=classify_failure(hidden_result) if killed else None,
            failing_case_ids=failing_ids,
        ))

        # Revert mutant
        revert_mutant_patch(workspace, patch_path)

    return MutantGateResult(
        mutants_total=len(patches),
        mutants_killed=killed_count,
        unkilled_list=unkilled,
        results=results,
        passed=killed_count == len(patches) and len(patches) >= min_mutants,
    )
