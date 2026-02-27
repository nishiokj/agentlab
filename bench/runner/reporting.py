"""Report generation.

Aggregates scores across runs and produces:
- report.json: per-task and per-agent summary
- report.html: human-readable report
- pareto.csv: Pareto front analysis
"""

from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from typing import Any

from bench.config import BenchConfig
from bench.runner.telemetry import extract_metrics, failure_breakdown, FAILURE_LABELS


def _collect_scores(runs_dir: Path) -> list[dict[str, Any]]:
    """Collect all score.json files from a runs directory."""
    scores = []
    for score_file in sorted(runs_dir.rglob("score.json")):
        try:
            score = json.loads(score_file.read_text())
            score["_run_dir"] = str(score_file.parent)
            scores.append(score)
        except json.JSONDecodeError:
            continue
    return scores


def _per_task_summary(scores: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate scores by task_id."""
    by_task: dict[str, list[dict]] = {}
    for score in scores:
        tid = score.get("task_id", "unknown")
        by_task.setdefault(tid, []).append(score)

    summary = {}
    for tid, task_scores in sorted(by_task.items()):
        total = len(task_scores)
        passed = sum(1 for s in task_scores if s.get("overall_pass"))
        summary[tid] = {
            "total_runs": total,
            "passed": passed,
            "pass_rate": round(passed / total, 4) if total > 0 else 0,
            "failure_breakdown": failure_breakdown(task_scores),
        }
    return summary


def _per_agent_summary(scores: list[dict[str, Any]], runs_dir: Path) -> dict[str, Any]:
    """Aggregate scores by agent (derived from run_id prefix)."""
    by_agent: dict[str, list[dict]] = {}
    for score in scores:
        run_id = score.get("run_id", "")
        # Agent is everything before the first _TASK
        agent = run_id.split("_TASK")[0] if "_TASK" in run_id else "unknown"
        by_agent.setdefault(agent, []).append(score)

    summary = {}
    for agent, agent_scores in sorted(by_agent.items()):
        total = len(agent_scores)
        passed = sum(1 for s in agent_scores if s.get("overall_pass"))

        # Aggregate metrics
        total_tool_calls = sum(
            s.get("metrics", {}).get("tool_calls", 0) for s in agent_scores
        )
        total_wall_clock = sum(
            s.get("metrics", {}).get("wall_clock_s", 0) for s in agent_scores
        )

        summary[agent] = {
            "total_tasks": total,
            "passed": passed,
            "pass_rate": round(passed / total, 4) if total > 0 else 0,
            "avg_tool_calls": round(total_tool_calls / total, 1) if total > 0 else 0,
            "avg_wall_clock_s": round(total_wall_clock / total, 1) if total > 0 else 0,
            "failure_breakdown": failure_breakdown(agent_scores),
        }
    return summary


def _compute_pareto(scores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compute Pareto front on (pass_rate vs cost metrics).

    Returns sorted list of Pareto-optimal points.
    """
    # Group by agent
    by_agent: dict[str, list[dict]] = {}
    for score in scores:
        run_id = score.get("run_id", "")
        agent = run_id.split("_TASK")[0] if "_TASK" in run_id else "unknown"
        by_agent.setdefault(agent, []).append(score)

    points = []
    for agent, agent_scores in by_agent.items():
        total = len(agent_scores)
        passed = sum(1 for s in agent_scores if s.get("overall_pass"))
        pass_rate = passed / total if total > 0 else 0

        total_tool_calls = sum(
            s.get("metrics", {}).get("tool_calls", 0) for s in agent_scores
        )
        total_wall_clock = sum(
            s.get("metrics", {}).get("wall_clock_s", 0) for s in agent_scores
        )

        # Nullable token usage
        token_totals = [
            s.get("metrics", {}).get("token_usage", {}).get("total_tokens")
            for s in agent_scores
            if s.get("metrics", {}).get("token_usage") is not None
        ]
        total_tokens = sum(t for t in token_totals if t is not None) if token_totals else None

        points.append({
            "agent": agent,
            "pass_rate": round(pass_rate, 4),
            "total_tasks": total,
            "avg_tool_calls": round(total_tool_calls / total, 1) if total > 0 else 0,
            "avg_wall_clock_s": round(total_wall_clock / total, 1) if total > 0 else 0,
            "total_tokens": total_tokens,
        })

    # Sort by pass_rate descending, then by avg_tool_calls ascending
    points.sort(key=lambda p: (-p["pass_rate"], p["avg_tool_calls"]))

    # Simple Pareto: keep points where no other point dominates on both axes
    pareto = []
    for p in points:
        dominated = False
        for q in points:
            if q is p:
                continue
            if (q["pass_rate"] >= p["pass_rate"] and
                q["avg_tool_calls"] <= p["avg_tool_calls"] and
                (q["pass_rate"] > p["pass_rate"] or q["avg_tool_calls"] < p["avg_tool_calls"])):
                dominated = True
                break
        if not dominated:
            pareto.append(p)

    return pareto


def _generate_html(report: dict[str, Any]) -> str:
    """Generate a simple HTML report."""
    lines = [
        "<!DOCTYPE html>",
        "<html><head><title>Benchmark Report</title>",
        "<style>",
        "body { font-family: monospace; margin: 2em; }",
        "table { border-collapse: collapse; margin: 1em 0; }",
        "th, td { border: 1px solid #ccc; padding: 0.5em; text-align: left; }",
        "th { background: #f0f0f0; }",
        ".pass { color: green; } .fail { color: red; }",
        "</style></head><body>",
        "<h1>Benchmark Report</h1>",
    ]

    # Per-task table
    lines.append("<h2>Per-Task Results</h2>")
    lines.append("<table><tr><th>Task</th><th>Runs</th><th>Passed</th><th>Rate</th></tr>")
    for tid, data in sorted(report.get("per_task", {}).items()):
        rate = data.get("pass_rate", 0)
        cls = "pass" if rate == 1.0 else "fail"
        lines.append(
            f'<tr><td>{tid}</td><td>{data["total_runs"]}</td>'
            f'<td class="{cls}">{data["passed"]}</td>'
            f'<td>{rate:.1%}</td></tr>'
        )
    lines.append("</table>")

    # Per-agent table
    lines.append("<h2>Per-Agent Results</h2>")
    lines.append("<table><tr><th>Agent</th><th>Tasks</th><th>Passed</th><th>Rate</th>"
                 "<th>Avg Tools</th><th>Avg Time(s)</th></tr>")
    for agent, data in sorted(report.get("per_agent", {}).items()):
        rate = data.get("pass_rate", 0)
        cls = "pass" if rate == 1.0 else "fail"
        lines.append(
            f'<tr><td>{agent}</td><td>{data["total_tasks"]}</td>'
            f'<td class="{cls}">{data["passed"]}</td>'
            f'<td>{rate:.1%}</td>'
            f'<td>{data["avg_tool_calls"]}</td>'
            f'<td>{data["avg_wall_clock_s"]}</td></tr>'
        )
    lines.append("</table>")

    # Failure breakdown
    lines.append("<h2>Failure Breakdown</h2>")
    lines.append("<table><tr><th>Label</th><th>Count</th></tr>")
    for label, count in sorted(report.get("failure_breakdown", {}).items()):
        if count > 0:
            lines.append(f'<tr><td>{label}</td><td>{count}</td></tr>')
    lines.append("</table>")

    lines.append("</body></html>")
    return "\n".join(lines)


def _generate_pareto_csv(pareto: list[dict[str, Any]]) -> str:
    """Generate Pareto front as CSV."""
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "agent", "pass_rate", "total_tasks", "avg_tool_calls",
        "avg_wall_clock_s", "total_tokens",
    ])
    writer.writeheader()
    for row in pareto:
        writer.writerow(row)
    return output.getvalue()


def generate_report(
    runs_dir: Path,
    out_dir: Path,
    config: BenchConfig,
) -> dict[str, Any]:
    """Generate aggregate reports from run results."""
    out_dir.mkdir(parents=True, exist_ok=True)

    scores = _collect_scores(runs_dir)
    if not scores:
        empty = {"error": "No scores found", "per_task": {}, "per_agent": {},
                 "failure_breakdown": {}, "pareto": []}
        (out_dir / "report.json").write_text(json.dumps(empty, indent=2, sort_keys=True))
        return empty

    report = {
        "total_runs": len(scores),
        "per_task": _per_task_summary(scores),
        "per_agent": _per_agent_summary(scores, runs_dir),
        "failure_breakdown": failure_breakdown(scores),
        "pareto": _compute_pareto(scores),
    }

    # Write outputs
    (out_dir / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True))
    (out_dir / "report.html").write_text(_generate_html(report))
    (out_dir / "pareto.csv").write_text(_generate_pareto_csv(report["pareto"]))

    return report
