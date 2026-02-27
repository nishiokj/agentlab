"""End-to-end smoke tests.

Validates the full pipeline works with synthetic data,
without requiring Docker or real repo snapshots.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bench.config import BenchConfig
from bench.taskkit.schema import validate_all_schemas, validate_json, load_schema
from bench.runner.scoring import compute_score, _count_patch_stats
from bench.runner.telemetry import failure_breakdown

BENCH_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def config():
    return BenchConfig.from_root(BENCH_ROOT)


class TestE2ESmoke:
    def test_cli_help(self):
        """CLI --help exits 0."""
        import subprocess
        result = subprocess.run(
            ["python", "-m", "bench.cli", "--help"],
            capture_output=True, text=True,
            cwd=str(BENCH_ROOT),
        )
        assert result.returncode == 0
        assert "Deterministic offline benchmark" in result.stdout

    def test_validate_schemas_command(self):
        """CLI validate-schemas succeeds."""
        import subprocess
        result = subprocess.run(
            ["python", "-m", "bench.cli", "validate-schemas"],
            capture_output=True, text=True,
            cwd=str(BENCH_ROOT),
        )
        assert result.returncode == 0
        assert "All schemas valid" in result.stdout

    def test_schemas_are_valid(self, config):
        errors = validate_all_schemas(config.schemas_dir)
        assert errors == []

    def test_score_computation(self):
        """Score computation produces valid output."""
        score = compute_score(
            run_id="test_001",
            task_id="TASK001",
            public_pass=True,
            hidden_pass=True,
            policy_pass=True,
            patch_text="--- a/main.py\n+++ b/main.py\n@@ -1 +1 @@\n-old\n+new\n",
        )
        assert score["overall_pass"] is True
        assert score["failure_label"] is None
        assert score["metrics"]["patch"]["files_changed"] == 1

    def test_score_with_failure(self):
        score = compute_score(
            run_id="test_002",
            task_id="TASK001",
            failure_label="HIDDEN_FAIL",
            public_pass=True,
            hidden_pass=False,
        )
        assert score["overall_pass"] is False
        assert score["failure_label"] == "HIDDEN_FAIL"

    def test_patch_stats(self):
        patch = """diff --git a/src/main.py b/src/main.py
--- a/src/main.py
+++ b/src/main.py
@@ -1,3 +1,4 @@
 def hello():
-    return 'hello'
+    return 'hello world'
+    # comment
"""
        stats = _count_patch_stats(patch)
        assert stats["files_changed"] == 1
        assert stats["lines_added"] == 2
        assert stats["lines_removed"] == 1

    def test_failure_breakdown_handles_all_labels(self):
        scores = [{"overall_pass": True}]
        bd = failure_breakdown(scores)
        assert bd["PASS"] == 1
        assert all(bd[label] == 0 for label in [
            "AGENT_TIMEOUT", "AGENT_ERROR", "NO_PATCH",
        ])

    def test_score_validates_against_schema(self, config):
        """Computed score validates against score.schema.json."""
        score = compute_score(
            run_id="test_003",
            task_id="TASK001",
            public_pass=True,
            hidden_pass=True,
            policy_pass=True,
        )
        schema = load_schema(config.schemas_dir / "score.schema.json")
        errors = validate_json(score, schema)
        assert errors == [], f"Score validation errors: {errors}"
