"""Telemetry extraction and failure taxonomy.

Extracts standardized metrics from traces and scores for reporting.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


# Failure taxonomy - mutually exclusive labels
FAILURE_LABELS = [
    "AGENT_TIMEOUT",
    "AGENT_ERROR",
    "NO_PATCH",
    "PATCH_APPLY_FAIL",
    "POLICY_VIOLATION",
    "PUBLIC_FAIL",
    "HIDDEN_FAIL",
    "HIDDEN_TIMEOUT",
    "HIDDEN_ERROR",
    "GRADER_ERROR",
]


def count_tool_calls(trace_path: Path) -> int:
    """Count tool calls from a trace.jsonl file."""
    if not trace_path.exists():
        return 0
    count = 0
    for line in trace_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            if record.get("event_type") == "tool_call":
                count += 1
        except json.JSONDecodeError:
            continue
    return count


def extract_timing(trace_path: Path) -> dict[str, float]:
    """Extract timing metrics from a trace file."""
    if not trace_path.exists():
        return {"total_ms": 0, "tool_time_ms": 0}
    total_ms = 0.0
    tool_time_ms = 0.0
    for line in trace_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            dur = record.get("duration_ms", 0)
            if record.get("event_type") == "tool_call":
                tool_time_ms += dur
            total_ms = max(total_ms, dur)
        except json.JSONDecodeError:
            continue
    return {"total_ms": total_ms, "tool_time_ms": tool_time_ms}


def classify_failure(score: dict[str, Any]) -> str | None:
    """Return the failure label from a score dict, or None if passed."""
    if score.get("overall_pass"):
        return None
    return score.get("failure_label")


def extract_metrics(run_dir: Path) -> dict[str, Any]:
    """Extract telemetry from a run directory.

    Returns a normalized metrics dict with stable ordering.
    """
    score_path = run_dir / "score.json"
    trace_path = run_dir / "trace.jsonl"
    agent_metrics_path = run_dir / "agent_metrics.json"

    metrics: dict[str, Any] = {
        "tool_calls": 0,
        "wall_clock_s": 0.0,
        "tool_time_s": 0.0,
        "patch_files_changed": 0,
        "patch_lines_added": 0,
        "patch_lines_removed": 0,
        "token_usage": None,
        "failure_label": None,
    }

    if trace_path.exists():
        metrics["tool_calls"] = count_tool_calls(trace_path)
        timing = extract_timing(trace_path)
        metrics["tool_time_s"] = round(timing["tool_time_ms"] / 1000, 2)

    if score_path.exists():
        score = json.loads(score_path.read_text())
        metrics["failure_label"] = classify_failure(score)
        score_metrics = score.get("metrics", {})
        metrics["wall_clock_s"] = score_metrics.get("wall_clock_s", 0.0)
        patch = score_metrics.get("patch", {})
        metrics["patch_files_changed"] = patch.get("files_changed", 0)
        metrics["patch_lines_added"] = patch.get("lines_added", 0)
        metrics["patch_lines_removed"] = patch.get("lines_removed", 0)
        metrics["token_usage"] = score_metrics.get("token_usage")

    if agent_metrics_path.exists():
        try:
            agent_metrics = json.loads(agent_metrics_path.read_text())
            if "token_usage" in agent_metrics:
                metrics["token_usage"] = agent_metrics["token_usage"]
        except json.JSONDecodeError:
            pass

    return metrics


def failure_breakdown(scores: list[dict[str, Any]]) -> dict[str, int]:
    """Compute failure label counts from a list of scores."""
    breakdown: dict[str, int] = {label: 0 for label in FAILURE_LABELS}
    breakdown["PASS"] = 0
    for score in scores:
        if score.get("overall_pass"):
            breakdown["PASS"] += 1
        else:
            label = score.get("failure_label", "GRADER_ERROR")
            breakdown[label] = breakdown.get(label, 0) + 1
    return breakdown
