"""Task-level grading used by the AgentLab bench adapter."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from bench.config import BenchConfig
from bench.taskkit.hidden_runner import run_hidden_suite, HiddenSuiteResult
from bench.taskkit.loader import load_task, prepare_grader_workspace
from bench.taskkit.patch_utils import check_patch_escapes_workspace, parse_patch_files
from bench.taskkit.policy import PatchPolicy


def _count_patch_stats(patch_text: str | None) -> dict[str, int]:
    if not patch_text:
        return {"files_changed": 0, "lines_added": 0, "lines_removed": 0}

    files = parse_patch_files(patch_text)
    lines_added = 0
    lines_removed = 0
    for line in patch_text.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            lines_added += 1
        elif line.startswith("-") and not line.startswith("---"):
            lines_removed += 1

    return {
        "files_changed": len(files),
        "lines_added": lines_added,
        "lines_removed": lines_removed,
    }


def _compute_score(
    task_id: str,
    failure_label: str | None = None,
    public_pass: bool = False,
    hidden_pass: bool = False,
    policy_pass: bool = True,
    policy_violations: list[str] | None = None,
    hidden_result: HiddenSuiteResult | None = None,
    patch_text: str | None = None,
) -> dict[str, Any]:
    if policy_violations:
        policy_pass = False

    overall_pass = public_pass and hidden_pass and policy_pass and failure_label is None
    if failure_label and overall_pass:
        overall_pass = False

    hidden_cases_total = None
    hidden_cases_passed = None
    if hidden_result is not None:
        hidden_cases_total = hidden_result.total
        hidden_cases_passed = hidden_result.passed

    return {
        "task_id": task_id,
        "public_pass": public_pass,
        "hidden_pass": hidden_pass,
        "policy_pass": policy_pass,
        "overall_pass": overall_pass,
        "failure_label": failure_label,
        "metrics": {
            "patch": _count_patch_stats(patch_text),
            "token_usage": None,
            "coverage": None,
            "hidden_cases_total": hidden_cases_total,
            "hidden_cases_passed": hidden_cases_passed,
        },
    }


def _apply_patch_text(workspace: Path, patch_text: str) -> bool:
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
    result2 = subprocess.run(
        ["patch", "-p1", "--batch", "--forward"],
        input=patch_text,
        capture_output=True,
        text=True,
        cwd=str(workspace),
        timeout=30,
    )
    return result2.returncode == 0


def grade_patch_for_task(
    task_dir: Path,
    patch_text: str | None,
    config: BenchConfig,
) -> dict[str, Any]:
    """Grade an agent patch for a task bundle and return score payload."""
    task_data = load_task(task_dir, config)
    task_id = task_data["task_id"]

    if not patch_text or not patch_text.strip():
        return _compute_score(task_id=task_id, failure_label="NO_PATCH")

    escape_violations = check_patch_escapes_workspace(patch_text)
    if escape_violations:
        return _compute_score(
            task_id=task_id,
            failure_label="POLICY_VIOLATION",
            policy_violations=escape_violations,
            patch_text=patch_text,
        )

    policy = PatchPolicy.from_task(task_data)
    patch_files = parse_patch_files(patch_text)
    violations = policy.check_files(patch_files)
    if violations:
        return _compute_score(
            task_id=task_id,
            failure_label="POLICY_VIOLATION",
            policy_violations=violations,
            patch_text=patch_text,
        )

    work_root = Path(tempfile.mkdtemp(prefix=f"bench_adapter_grade_{task_id}_"))
    try:
        manifest = prepare_grader_workspace(
            task_dir=task_dir,
            task_data=task_data,
            config=config,
            work_root=work_root,
            agent_patch_path=None,
        )
        workspace = Path(manifest["workspace"])
        hidden_dir = Path(manifest["hidden_dir"]) if manifest.get("hidden_dir") else None

        if not _apply_patch_text(workspace, patch_text):
            return _compute_score(
                task_id=task_id,
                failure_label="PATCH_APPLY_FAIL",
                patch_text=patch_text,
            )

        public_pass = True
        public_cmd = str(task_data.get("public_command", "") or "")
        if public_cmd:
            try:
                pub_result = subprocess.run(
                    ["bash", "-c", public_cmd],
                    capture_output=True,
                    text=True,
                    cwd=str(workspace),
                    timeout=int(task_data.get("time_limits", {}).get("public_timeout", 30)),
                )
                public_pass = pub_result.returncode == 0
            except subprocess.TimeoutExpired:
                public_pass = False

        if hidden_dir is None or not hidden_dir.is_dir():
            return _compute_score(
                task_id=task_id,
                failure_label="HIDDEN_FAIL",
                public_pass=public_pass,
                hidden_pass=False,
                patch_text=patch_text,
            )

        hidden_result = run_hidden_suite(
            workspace=workspace,
            hidden_dir=hidden_dir,
            task_data=task_data,
            timeout=int(task_data.get("time_limits", {}).get("hidden_timeout", 60)),
        )
        if hidden_result.timed_out:
            return _compute_score(
                task_id=task_id,
                failure_label="HIDDEN_TIMEOUT",
                public_pass=public_pass,
                hidden_pass=False,
                hidden_result=hidden_result,
                patch_text=patch_text,
            )

        hidden_pass = hidden_result.all_passed
        if not public_pass:
            failure_label = "PUBLIC_FAIL"
        elif not hidden_pass:
            failure_label = "HIDDEN_FAIL"
        else:
            failure_label = None

        return _compute_score(
            task_id=task_id,
            failure_label=failure_label,
            public_pass=public_pass,
            hidden_pass=hidden_pass,
            hidden_result=hidden_result,
            patch_text=patch_text,
        )
    finally:
        shutil.rmtree(work_root, ignore_errors=True)

