"""Tests for schema validation utilities."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bench.config import BenchConfig
from bench.taskkit.schema import validate_all_schemas, validate_json, load_schema

BENCH_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def config():
    return BenchConfig.from_root(BENCH_ROOT)


@pytest.fixture
def task_schema(config):
    return load_schema(config.schemas_dir / "task.schema.json")


@pytest.fixture
def trace_schema(config):
    return load_schema(config.schemas_dir / "trace.schema.json")


@pytest.fixture
def score_schema(config):
    return load_schema(config.schemas_dir / "score.schema.json")


class TestSchemaValidation:
    def test_all_schemas_valid(self, config):
        errors = validate_all_schemas(config.schemas_dir)
        assert errors == [], f"Schema validation errors: {errors}"

    def test_task_schema_rejects_missing_fields(self, task_schema):
        errors = validate_json({}, task_schema)
        assert len(errors) > 0
        # Should mention required fields
        required_fields = [
            "task_id", "repo_id", "repo_snapshot",
            "baseline_injection_patch", "public_command",
            "hidden_command", "time_limits", "determinism_env",
            "patch_policy",
        ]
        for field in required_fields:
            assert any(field in e for e in errors), f"Missing error for required field: {field}"

    def test_task_schema_accepts_valid(self, task_schema):
        valid_task = {
            "task_id": "TASK001",
            "repo_id": "click",
            "repo_snapshot": "click/src.tar.zst",
            "baseline_injection_patch": "injection.patch",
            "public_command": "bash run_public.sh",
            "hidden_command": "python runner.py /workspace cases.jsonl",
            "time_limits": {
                "agent_timeout": 1200,
                "grade_timeout": 300,
                "hidden_timeout": 60,
                "public_timeout": 30,
            },
            "determinism_env": {
                "PYTHONHASHSEED": "0",
                "TZ": "UTC",
                "LC_ALL": "C.UTF-8",
                "LANG": "C.UTF-8",
            },
            "patch_policy": {
                "allow_edit_globs": ["src/**/*.py"],
                "deny_edit_globs": [],
            },
        }
        errors = validate_json(valid_task, task_schema)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_task_schema_rejects_unknown_fields(self, task_schema):
        valid_task = {
            "task_id": "TASK001",
            "repo_id": "click",
            "repo_snapshot": "click/src.tar.zst",
            "baseline_injection_patch": "injection.patch",
            "public_command": "bash run_public.sh",
            "hidden_command": "python runner.py",
            "time_limits": {
                "agent_timeout": 1200,
                "grade_timeout": 300,
                "hidden_timeout": 60,
                "public_timeout": 30,
            },
            "determinism_env": {
                "PYTHONHASHSEED": "0",
                "TZ": "UTC",
                "LC_ALL": "C.UTF-8",
                "LANG": "C.UTF-8",
            },
            "patch_policy": {
                "allow_edit_globs": ["**/*.py"],
                "deny_edit_globs": [],
            },
            "unknown_field": "should be rejected",
        }
        errors = validate_json(valid_task, task_schema)
        assert len(errors) > 0

    def test_trace_schema_accepts_valid(self, trace_schema):
        valid_trace = {
            "run_id": "test_run_001",
            "task_id": "TASK001",
            "phase": "agent",
            "event_type": "tool_call",
            "ts_start": "2024-01-01T00:00:00+00:00",
            "ts_end": "2024-01-01T00:00:01+00:00",
            "duration_ms": 1000,
            "tool_name": "search",
            "input": {"pattern": "def foo"},
            "output_summary": "Found 3 matches",
            "exit_code": None,
            "error_type": None,
            "error_message": None,
            "workspace_relpaths_touched": [],
        }
        errors = validate_json(valid_trace, trace_schema)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_score_schema_accepts_valid(self, score_schema):
        valid_score = {
            "run_id": "test_run_001",
            "task_id": "TASK001",
            "public_pass": True,
            "hidden_pass": True,
            "policy_pass": True,
            "overall_pass": True,
            "failure_label": None,
            "metrics": {
                "tool_calls": 5,
                "wall_clock_s": 45.2,
                "patch": {
                    "files_changed": 1,
                    "lines_added": 3,
                    "lines_removed": 1,
                },
                "token_usage": None,
                "coverage": None,
                "hidden_cases_total": 50,
                "hidden_cases_passed": 50,
            },
        }
        errors = validate_json(valid_score, score_schema)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_score_schema_rejects_missing_metrics(self, score_schema):
        invalid = {
            "run_id": "test",
            "task_id": "TASK001",
            "public_pass": True,
            "hidden_pass": True,
            "policy_pass": True,
            "overall_pass": True,
            "failure_label": None,
        }
        errors = validate_json(invalid, score_schema)
        assert len(errors) > 0
