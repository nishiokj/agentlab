"""Agent run harness.

Orchestrates an agent run:
1. Prepares workspace from task loader
2. Starts tool server
3. Runs agent command
4. Collects patch.diff and trace.jsonl
5. Writes agent_run_summary.json
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bench.config import BenchConfig
from bench.taskkit.loader import load_task, prepare_agent_workspace
from bench.taskkit.determinism import enforce_determinism_env, stable_json_pretty


def _generate_run_id(agent: str, task_id: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{agent}_{task_id}_{ts}"


def run_agent_task(
    task_dir: Path,
    agent_command: list[str],
    config: BenchConfig,
    runs_dir: Path,
    timeout: int = 1200,
) -> dict[str, Any]:
    """Run an agent on a single task.

    Returns the agent run summary dict.
    """
    task_data = load_task(task_dir, config)
    task_id = task_data["task_id"]
    run_id = _generate_run_id(agent_command[0] if agent_command else "unknown", task_id)

    # Create run output directory
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Prepare workspace in temp dir
    work_root = Path(tempfile.mkdtemp(prefix=f"bench_agent_{task_id}_"))

    try:
        manifest = prepare_agent_workspace(task_dir, task_data, config, work_root)
        workspace = Path(manifest["workspace"])

        # Set up environment
        env = enforce_determinism_env(task_data.get("determinism_env"))
        env["WORKSPACE"] = str(workspace)
        env["TASK_ID"] = task_id
        env["RUN_ID"] = run_id
        env["HOME"] = str(work_root / "home")
        (work_root / "home").mkdir(exist_ok=True)

        # Run agent
        start_time = time.monotonic()
        timed_out = False
        failure_label = None

        try:
            proc = subprocess.run(
                agent_command,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
                cwd=str(workspace),
            )
            exit_code = proc.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            exit_code = -1
            failure_label = "AGENT_TIMEOUT"

        wall_clock_s = time.monotonic() - start_time

        # Collect patch
        patch_file = workspace / "patch.diff"
        if patch_file.exists():
            shutil.copy2(patch_file, run_dir / "patch.diff")
        else:
            # Try to generate diff from git
            diff_result = subprocess.run(
                ["git", "diff"], capture_output=True, text=True,
                cwd=str(workspace), timeout=30,
            )
            if diff_result.stdout.strip():
                (run_dir / "patch.diff").write_text(diff_result.stdout)
            else:
                failure_label = failure_label or "NO_PATCH"

        # Collect trace
        trace_file = workspace / "trace.jsonl"
        if trace_file.exists():
            shutil.copy2(trace_file, run_dir / "trace.jsonl")

        # Collect agent metrics if available
        metrics_file = workspace / "agent_metrics.json"
        if metrics_file.exists():
            shutil.copy2(metrics_file, run_dir / "agent_metrics.json")

        # Write summary
        summary = {
            "run_id": run_id,
            "task_id": task_id,
            "agent_command": agent_command,
            "exit_code": exit_code,
            "wall_clock_s": round(wall_clock_s, 2),
            "timed_out": timed_out,
            "failure_label": failure_label,
            "has_patch": (run_dir / "patch.diff").exists(),
            "has_trace": (run_dir / "trace.jsonl").exists(),
            "task_dir": str(task_dir),
        }
        (run_dir / "agent_run_summary.json").write_text(stable_json_pretty(summary))
        return summary

    finally:
        shutil.rmtree(work_root, ignore_errors=True)


def run_agent_suite(
    suite: str,
    agent: str,
    runs_dir: Path,
    config: BenchConfig,
    max_tasks: int | None = None,
    timeout: int = 1200,
) -> list[dict[str, Any]]:
    """Run an agent across all tasks in a suite."""
    suite_dir = config.tasks_dir / suite
    if not suite_dir.is_dir():
        raise FileNotFoundError(f"Suite not found: {suite_dir}")

    task_dirs = sorted(
        d for d in suite_dir.iterdir()
        if d.is_dir() and d.name.startswith("TASK")
    )
    if max_tasks:
        task_dirs = task_dirs[:max_tasks]

    # Resolve agent command
    if agent == "dummy":
        agent_command = ["python", "-m", "bench.dummy_agent.agent"]
    else:
        agent_command = [agent]

    results = []
    for task_dir in task_dirs:
        try:
            result = run_agent_task(
                task_dir=task_dir,
                agent_command=agent_command,
                config=config,
                runs_dir=runs_dir,
                timeout=timeout,
            )
            results.append(result)
        except Exception as e:
            results.append({
                "task_dir": str(task_dir),
                "error": str(e),
                "failure_label": "AGENT_ERROR",
            })

    return results
