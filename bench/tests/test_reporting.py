"""Tests for report generation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bench.config import BenchConfig
from bench.runner.reporting import generate_report

BENCH_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def config():
    return BenchConfig.from_root(BENCH_ROOT)


@pytest.fixture
def multi_run_fixture(tmp_path):
    """Create a fixture with multiple run results."""
    runs_dir = tmp_path / "runs"
    for i in range(3):
        run_dir = runs_dir / f"agent1_TASK{i+1:03d}_20240101_000000"
        run_dir.mkdir(parents=True)
        passed = i < 2
        (run_dir / "score.json").write_text(json.dumps({
            "run_id": f"agent1_TASK{i+1:03d}_20240101_000000",
            "task_id": f"TASK{i+1:03d}",
            "public_pass": passed,
            "hidden_pass": passed,
            "policy_pass": True,
            "overall_pass": passed,
            "failure_label": None if passed else "HIDDEN_FAIL",
            "metrics": {
                "tool_calls": 5 + i,
                "wall_clock_s": 30.0 + i * 10,
                "patch": {"files_changed": 1, "lines_added": 3, "lines_removed": 1},
                "token_usage": None,
                "coverage": None,
                "hidden_cases_total": 50,
                "hidden_cases_passed": 50 if passed else 30,
            },
        }, indent=2))
    return runs_dir


class TestReporting:
    def test_generate_report_succeeds(self, multi_run_fixture, tmp_path, config):
        out_dir = tmp_path / "report_out"
        result = generate_report(multi_run_fixture, out_dir, config)

        assert (out_dir / "report.json").exists()
        assert (out_dir / "report.html").exists()
        assert (out_dir / "pareto.csv").exists()

        assert result["total_runs"] == 3

    def test_per_task_summary(self, multi_run_fixture, tmp_path, config):
        out_dir = tmp_path / "report_out"
        result = generate_report(multi_run_fixture, out_dir, config)

        per_task = result["per_task"]
        assert "TASK001" in per_task
        assert per_task["TASK001"]["passed"] == 1

    def test_failure_breakdown(self, multi_run_fixture, tmp_path, config):
        out_dir = tmp_path / "report_out"
        result = generate_report(multi_run_fixture, out_dir, config)

        bd = result["failure_breakdown"]
        assert bd["PASS"] == 2
        assert bd["HIDDEN_FAIL"] == 1

    def test_pareto_deterministic(self, multi_run_fixture, tmp_path, config):
        out1 = tmp_path / "out1"
        out2 = tmp_path / "out2"
        r1 = generate_report(multi_run_fixture, out1, config)
        r2 = generate_report(multi_run_fixture, out2, config)
        assert r1["pareto"] == r2["pareto"]

    def test_empty_runs(self, tmp_path, config):
        runs_dir = tmp_path / "empty_runs"
        runs_dir.mkdir()
        out_dir = tmp_path / "empty_out"
        result = generate_report(runs_dir, out_dir, config)
        assert "error" in result

    def test_report_json_is_valid_json(self, multi_run_fixture, tmp_path, config):
        out_dir = tmp_path / "report_out"
        generate_report(multi_run_fixture, out_dir, config)
        report = json.loads((out_dir / "report.json").read_text())
        assert "per_task" in report
        assert "per_agent" in report
