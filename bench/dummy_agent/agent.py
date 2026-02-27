"""Dummy agent for smoke testing.

Makes a few tool calls and produces a trivial patch.
This agent is not expected to solve any real task; it exists
to exercise the harness, tool server, telemetry, and reporting.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

TOOL_SERVER_URL = os.environ.get("TOOL_SERVER_URL", "http://127.0.0.1:8080")
WORKSPACE = Path(os.environ.get("WORKSPACE", "."))
TASK_ID = os.environ.get("TASK_ID", "unknown")
RUN_ID = os.environ.get("RUN_ID", "dummy_run")


def _make_tool_call(tool: str, params: dict) -> dict:
    """Make an HTTP tool call to the tool server."""
    try:
        import httpx
        resp = httpx.post(f"{TOOL_SERVER_URL}/{tool}", json=params, timeout=30)
        return resp.json()
    except Exception:
        return {"error": "Tool server not available (standalone mode)"}


def _record_trace(records: list[dict], tool: str, params: dict, result: dict,
                  start: float, end: float) -> None:
    records.append({
        "run_id": RUN_ID,
        "task_id": TASK_ID,
        "phase": "agent",
        "event_type": "tool_call",
        "ts_start": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
        "ts_end": datetime.fromtimestamp(end, tz=timezone.utc).isoformat(),
        "duration_ms": round((end - start) * 1000, 2),
        "tool_name": tool,
        "input": params,
        "output_summary": json.dumps(result)[:512],
        "exit_code": result.get("exit_code"),
        "error_type": None,
        "error_message": result.get("error"),
        "workspace_relpaths_touched": [],
    })


def main() -> None:
    trace_records: list[dict] = []

    # Record phase start
    phase_start = time.time()
    trace_records.append({
        "run_id": RUN_ID, "task_id": TASK_ID, "phase": "agent",
        "event_type": "phase_start",
        "ts_start": datetime.fromtimestamp(phase_start, tz=timezone.utc).isoformat(),
        "ts_end": datetime.fromtimestamp(phase_start, tz=timezone.utc).isoformat(),
        "duration_ms": 0, "tool_name": None, "input": None,
        "output_summary": None, "exit_code": None,
        "error_type": None, "error_message": None,
        "workspace_relpaths_touched": None,
    })

    # 1. List workspace
    t0 = time.time()
    result = _make_tool_call("list_dir", {"path": ".", "recursive": False})
    _record_trace(trace_records, "list_dir", {"path": "."}, result, t0, time.time())

    # 2. Read issue
    t0 = time.time()
    result = _make_tool_call("read_file", {"path": "ISSUE.md", "max_bytes": 4096})
    _record_trace(trace_records, "read_file", {"path": "ISSUE.md"}, result, t0, time.time())

    # 3. Search for a keyword
    t0 = time.time()
    result = _make_tool_call("search", {"pattern": "def ", "max_results": 10})
    _record_trace(trace_records, "search", {"pattern": "def "}, result, t0, time.time())

    # 4. Create a dummy patch (touch a comment)
    dummy_patch = ""  # Empty patch - dummy agent doesn't solve anything

    # Record phase end
    phase_end = time.time()
    trace_records.append({
        "run_id": RUN_ID, "task_id": TASK_ID, "phase": "agent",
        "event_type": "phase_end",
        "ts_start": datetime.fromtimestamp(phase_end, tz=timezone.utc).isoformat(),
        "ts_end": datetime.fromtimestamp(phase_end, tz=timezone.utc).isoformat(),
        "duration_ms": round((phase_end - phase_start) * 1000, 2),
        "tool_name": None, "input": None, "output_summary": None,
        "exit_code": None, "error_type": None, "error_message": None,
        "workspace_relpaths_touched": None,
    })

    # Write trace
    trace_path = WORKSPACE / "trace.jsonl"
    with open(trace_path, "w") as f:
        for record in trace_records:
            f.write(json.dumps(record, sort_keys=True) + "\n")

    # Write agent metrics
    metrics_path = WORKSPACE / "agent_metrics.json"
    metrics_path.write_text(json.dumps({
        "tool_calls": len([r for r in trace_records if r["event_type"] == "tool_call"]),
        "wall_clock_s": round(phase_end - phase_start, 2),
    }, sort_keys=True))

    print(f"Dummy agent completed for {TASK_ID}: {len(trace_records)} trace records")


if __name__ == "__main__":
    main()
