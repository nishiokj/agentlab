"""Tests for telemetry extraction."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bench.runner.telemetry import (
    count_tool_calls,
    extract_timing,
    classify_failure,
    failure_breakdown,
    extract_metrics,
    FAILURE_LABELS,
)


@pytest.fixture
def trace_file(tmp_path):
    trace = tmp_path / "trace.jsonl"
    records = [
        {"event_type": "phase_start", "duration_ms": 0},
        {"event_type": "tool_call", "tool_name": "search", "duration_ms": 100},
        {"event_type": "tool_call", "tool_name": "read_file", "duration_ms": 50},
        {"event_type": "tool_call", "tool_name": "apply_patch", "duration_ms": 200},
        {"event_type": "phase_end", "duration_ms": 5000},
    ]
    trace.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    return trace


class TestTelemetry:
    def test_count_tool_calls(self, trace_file):
        count = count_tool_calls(trace_file)
        assert count == 3

    def test_count_tool_calls_empty(self, tmp_path):
        assert count_tool_calls(tmp_path / "nonexistent.jsonl") == 0

    def test_extract_timing(self, trace_file):
        timing = extract_timing(trace_file)
        assert timing["tool_time_ms"] == 350  # 100 + 50 + 200
        assert timing["total_ms"] == 5000

    def test_classify_failure_pass(self):
        score = {"overall_pass": True, "failure_label": None}
        assert classify_failure(score) is None

    def test_classify_failure_timeout(self):
        score = {"overall_pass": False, "failure_label": "AGENT_TIMEOUT"}
        assert classify_failure(score) == "AGENT_TIMEOUT"

    def test_failure_breakdown(self):
        scores = [
            {"overall_pass": True},
            {"overall_pass": True},
            {"overall_pass": False, "failure_label": "HIDDEN_FAIL"},
            {"overall_pass": False, "failure_label": "NO_PATCH"},
            {"overall_pass": False, "failure_label": "HIDDEN_FAIL"},
        ]
        bd = failure_breakdown(scores)
        assert bd["PASS"] == 2
        assert bd["HIDDEN_FAIL"] == 2
        assert bd["NO_PATCH"] == 1

    def test_failure_labels_exhaustive(self):
        """All failure labels from score schema are in the taxonomy."""
        schema_labels = [
            "AGENT_TIMEOUT", "AGENT_ERROR", "NO_PATCH",
            "PATCH_APPLY_FAIL", "POLICY_VIOLATION", "PUBLIC_FAIL",
            "HIDDEN_FAIL", "HIDDEN_TIMEOUT", "HIDDEN_ERROR", "GRADER_ERROR",
        ]
        for label in schema_labels:
            assert label in FAILURE_LABELS

    def test_identical_traces_produce_identical_metrics(self, tmp_path):
        """Telemetry is deterministic for identical inputs."""
        for name in ["run1", "run2"]:
            d = tmp_path / name
            d.mkdir()
            (d / "trace.jsonl").write_text(
                '{"event_type":"tool_call","tool_name":"search","duration_ms":100}\n'
            )
            (d / "score.json").write_text(json.dumps({
                "run_id": "test", "task_id": "TASK001",
                "overall_pass": True, "failure_label": None,
                "metrics": {
                    "tool_calls": 1, "wall_clock_s": 0.1,
                    "patch": {"files_changed": 0, "lines_added": 0, "lines_removed": 0},
                    "token_usage": None, "coverage": None,
                },
            }))

        m1 = extract_metrics(tmp_path / "run1")
        m2 = extract_metrics(tmp_path / "run2")
        assert m1 == m2
