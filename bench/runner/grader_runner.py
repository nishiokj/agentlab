"""Grader run harness.

Grades an agent run in a fresh sandbox:
1. Prepares fresh workspace from repo snapshot + injection
2. Applies agent patch with policy enforcement
3. Runs public repro command
4. Runs hidden suite via benchmark-owned runner
5. Produces score.json
"""

from __future__ import annotations

import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from bench.config import BenchConfig
from bench.runner.patch_policy import PatchPolicy
from bench.runner.scoring import compute_score
from bench.taskkit.loader import load_task, prepare_grader_workspace
from bench.taskkit.hidden_runner import run_hidden_suite
from bench.taskkit.determinism import enforce_determinism_env, stable_json_pretty
from bench.tools.patch import parse_patch_files


def grade_task(
    run_dir: Path,
    task_dir: Path,
    config: BenchConfig,
) -> dict[str, Any]:
    """Grade a single task run and produce score.json.

    Args:
        run_dir: Directory containing agent outputs (patch.diff, trace.jsonl).
        task_dir: Task bundle directory.
        config: Benchmark configuration.

    Returns:
        Score dict conforming to score.schema.json.
    """
    task_data = load_task(task_dir, config)
    task_id = task_data["task_id"]

    # Read agent summary
    summary_path = run_dir / "agent_run_summary.json"
    agent_summary = {}
    if summary_path.exists():
        agent_summary = json.loads(summary_path.read_text())

    run_id = agent_summary.get("run_id", f"grade_{task_id}")
    failure_label = agent_summary.get("failure_label")

    # Early exits for pre-grade failures
    if failure_label == "AGENT_TIMEOUT":
        return compute_score(
            run_id=run_id, task_id=task_id,
            failure_label="AGENT_TIMEOUT",
            agent_summary=agent_summary,
        )

    # Check for patch
    patch_path = run_dir / "patch.diff"
    if not patch_path.exists() or not patch_path.read_text().strip():
        return compute_score(
            run_id=run_id, task_id=task_id,
            failure_label="NO_PATCH",
            agent_summary=agent_summary,
        )

    patch_text = patch_path.read_text()

    # Policy check
    policy = PatchPolicy.from_task(task_data)
    patch_files = parse_patch_files(patch_text)
    violations = policy.check_files(patch_files)
    if violations:
        return compute_score(
            run_id=run_id, task_id=task_id,
            failure_label="POLICY_VIOLATION",
            policy_violations=violations,
            agent_summary=agent_summary,
        )

    # Prepare fresh grader workspace
    work_root = Path(tempfile.mkdtemp(prefix=f"bench_grader_{task_id}_"))
    try:
        manifest = prepare_grader_workspace(
            task_dir, task_data, config, work_root, patch_path,
        )
        workspace = Path(manifest["workspace"])
        hidden_dir = Path(manifest["hidden_dir"]) if manifest.get("hidden_dir") else None

        # Apply agent patch
        import subprocess
        result = subprocess.run(
            ["git", "apply", "--verbose", "-"],
            input=patch_text,
            capture_output=True,
            text=True,
            cwd=str(workspace),
            timeout=30,
        )
        if result.returncode != 0:
            # Try plain patch
            result2 = subprocess.run(
                ["patch", "-p1", "--batch", "--forward"],
                input=patch_text,
                capture_output=True,
                text=True,
                cwd=str(workspace),
                timeout=30,
            )
            if result2.returncode != 0:
                return compute_score(
                    run_id=run_id, task_id=task_id,
                    failure_label="PATCH_APPLY_FAIL",
                    agent_summary=agent_summary,
                )

        # Run public repro (optional, recorded but not blocking)
        public_pass = True
        env = enforce_determinism_env(task_data.get("determinism_env"))
        public_cmd = task_data.get("public_command", "")
        if public_cmd:
            try:
                pub_result = subprocess.run(
                    ["bash", "-c", public_cmd],
                    capture_output=True, text=True,
                    cwd=str(workspace),
                    timeout=task_data.get("time_limits", {}).get("public_timeout", 30),
                    env=env,
                )
                public_pass = pub_result.returncode == 0
            except subprocess.TimeoutExpired:
                public_pass = False

        # Run hidden suite
        hidden_pass = False
        hidden_result = None
        if hidden_dir and hidden_dir.is_dir():
            hidden_result = run_hidden_suite(
                workspace=workspace,
                hidden_dir=hidden_dir,
                task_data=task_data,
                timeout=task_data.get("time_limits", {}).get("hidden_timeout", 60),
            )
            hidden_pass = hidden_result.all_passed

            if hidden_result.timed_out:
                return compute_score(
                    run_id=run_id, task_id=task_id,
                    failure_label="HIDDEN_TIMEOUT",
                    public_pass=public_pass,
                    hidden_result=hidden_result,
                    agent_summary=agent_summary,
                )

        # Compute final score
        if not public_pass:
            failure_label = "PUBLIC_FAIL"
        elif not hidden_pass:
            failure_label = "HIDDEN_FAIL"
        else:
            failure_label = None

        return compute_score(
            run_id=run_id, task_id=task_id,
            failure_label=failure_label,
            public_pass=public_pass,
            hidden_pass=hidden_pass,
            hidden_result=hidden_result,
            agent_summary=agent_summary,
            patch_text=patch_text,
        )

    finally:
        shutil.rmtree(work_root, ignore_errors=True)


def grade_run(run_dir: Path, config: BenchConfig) -> dict[str, Any]:
    """Grade a run directory (may contain multiple tasks)."""
    summary_path = run_dir / "agent_run_summary.json"
    if summary_path.exists():
        agent_summary = json.loads(summary_path.read_text())
        task_dir_str = agent_summary.get("task_dir")
        if task_dir_str:
            task_dir = Path(task_dir_str)
            score = grade_task(run_dir, task_dir, config)
            (run_dir / "score.json").write_text(stable_json_pretty(score))
            return score

    return {"error": "No agent_run_summary.json found or missing task_dir"}
