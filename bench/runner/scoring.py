"""Score computation for graded runs.

Produces score.json conforming to score.schema.json.
"""

from __future__ import annotations

import json
from typing import Any

from bench.tools.patch import parse_patch_files


def _count_patch_stats(patch_text: str | None) -> dict[str, int]:
    """Count files changed, lines added, lines removed from a unified diff."""
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


def _extract_token_usage(agent_summary: dict[str, Any]) -> dict[str, int] | None:
    """Extract token usage from agent metrics if available."""
    # Look for agent_metrics.json data
    return agent_summary.get("token_usage")


def compute_score(
    run_id: str,
    task_id: str,
    failure_label: str | None = None,
    public_pass: bool = False,
    hidden_pass: bool = False,
    policy_pass: bool = True,
    policy_violations: list[str] | None = None,
    hidden_result: Any | None = None,
    agent_summary: dict[str, Any] | None = None,
    patch_text: str | None = None,
) -> dict[str, Any]:
    """Compute a score dict conforming to score.schema.json."""
    if policy_violations:
        policy_pass = False

    overall_pass = public_pass and hidden_pass and policy_pass and failure_label is None

    if failure_label and overall_pass:
        overall_pass = False

    agent_summary = agent_summary or {}

    # Tool call count from trace
    tool_calls = agent_summary.get("tool_calls", 0)
    wall_clock_s = agent_summary.get("wall_clock_s", 0.0)

    # Patch stats
    patch_stats = _count_patch_stats(patch_text)

    # Hidden case stats
    hidden_cases_total = None
    hidden_cases_passed = None
    if hidden_result is not None:
        hidden_cases_total = getattr(hidden_result, "total", None)
        hidden_cases_passed = getattr(hidden_result, "passed", None)

    return {
        "run_id": run_id,
        "task_id": task_id,
        "public_pass": public_pass,
        "hidden_pass": hidden_pass,
        "policy_pass": policy_pass,
        "overall_pass": overall_pass,
        "failure_label": failure_label,
        "metrics": {
            "tool_calls": tool_calls,
            "wall_clock_s": wall_clock_s,
            "patch": patch_stats,
            "token_usage": _extract_token_usage(agent_summary),
            "coverage": None,
            "hidden_cases_total": hidden_cases_total,
            "hidden_cases_passed": hidden_cases_passed,
        },
    }
