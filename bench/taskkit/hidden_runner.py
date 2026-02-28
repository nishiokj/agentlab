"""Hidden suite runner (benchmark-owned).

Executes hidden/runner.py against cases.jsonl in a controlled
environment with deterministic settings and timeout enforcement.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from bench.taskkit.determinism import enforce_determinism_env, stable_json


def _as_non_negative_float(value: Any) -> float:
    """Convert a value to a non-negative float."""
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value) if value >= 0 else 0.0
    if isinstance(value, str):
        try:
            parsed = float(value)
            return parsed if parsed >= 0 else 0.0
        except ValueError:
            return 0.0
    return 0.0


@dataclass
class CaseResult:
    """Result of running a single hidden test case."""
    case_id: str
    passed: bool
    error_type: str | None = None
    error_message: str | None = None
    duration_ms: float = 0.0
    output_summary: str = ""


@dataclass
class HiddenSuiteResult:
    """Aggregate result of the hidden test suite."""
    total: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    timed_out: bool = False
    duration_ms: float = 0.0
    case_results: list[CaseResult] = field(default_factory=list)
    error_message: str | None = None

    @property
    def all_passed(self) -> bool:
        return self.passed == self.total and self.total > 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_hidden_suite(
    workspace: Path,
    hidden_dir: Path,
    task_data: dict[str, Any],
    timeout: int = 60,
    per_case_timeout: int = 5,
    determinism_env: dict[str, str] | None = None,
) -> HiddenSuiteResult:
    """Execute the hidden test suite.

    The hidden runner (hidden/runner.py) is invoked as a subprocess
    with the workspace path and cases.jsonl path as arguments.
    It must output JSONL results to stdout.

    Args:
        workspace: Path to the (grader) workspace with the code under test.
        hidden_dir: Path to the hidden/ directory containing runner.py and cases.jsonl.
        task_data: Task configuration dict.
        timeout: Total suite timeout in seconds.
        per_case_timeout: Per-case timeout (passed to runner).
        determinism_env: Additional env vars for determinism.
    """
    runner_py = hidden_dir / "runner.py"
    cases_jsonl = hidden_dir / "cases.jsonl"

    if not runner_py.exists():
        return HiddenSuiteResult(error_message=f"Hidden runner not found: {runner_py}")
    if not cases_jsonl.exists():
        return HiddenSuiteResult(error_message=f"Cases file not found: {cases_jsonl}")

    env = enforce_determinism_env(determinism_env)
    env["WORKSPACE"] = str(workspace)
    env["CASES_JSONL"] = str(cases_jsonl)
    env["PER_CASE_TIMEOUT"] = str(per_case_timeout)
    # Clean HOME
    env["HOME"] = str(hidden_dir.parent)

    start = time.monotonic()
    try:
        proc = subprocess.run(
            [sys.executable, str(runner_py), str(workspace), str(cases_jsonl)],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=str(workspace),
        )
        duration_ms = (time.monotonic() - start) * 1000
    except subprocess.TimeoutExpired as e:
        duration_ms = (time.monotonic() - start) * 1000
        return HiddenSuiteResult(
            timed_out=True,
            duration_ms=duration_ms,
            error_message=f"Hidden suite timed out after {timeout}s",
        )

    # Parse results from stdout (JSONL format)
    case_results: list[CaseResult] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            if not isinstance(record, dict):
                continue
            case_results.append(CaseResult(
                case_id=record.get("case_id", "unknown"),
                passed=bool(record.get("passed", False)),
                error_type=record.get("error_type"),
                error_message=record.get("error_message"),
                duration_ms=_as_non_negative_float(record.get("duration_ms", 0)),
                output_summary=record.get("output_summary", "")[:512],
            ))
        except json.JSONDecodeError:
            continue

    total = len(case_results)
    passed = sum(1 for r in case_results if r.passed)
    failed = sum(1 for r in case_results if not r.passed and r.error_type is None)
    errors = sum(1 for r in case_results if r.error_type is not None)

    return HiddenSuiteResult(
        total=total,
        passed=passed,
        failed=failed,
        errors=errors,
        timed_out=False,
        duration_ms=duration_ms,
        case_results=case_results,
        error_message=proc.stderr.strip()[:1024] if proc.returncode != 0 else None,
    )
