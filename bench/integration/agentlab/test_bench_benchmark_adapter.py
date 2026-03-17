#!/usr/bin/env python3

from __future__ import annotations

import os
import unittest
from pathlib import Path

from bench.integration.agentlab import bench_benchmark_adapter as adapter
from bench.taskkit.schema import validate_with_schema_file

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMAS_DIR = REPO_ROOT / "schemas"


class BenchBenchmarkAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prev = {key: os.environ.get(key) for key in self._keys()}
        os.environ["AGENTLAB_RUN_ID"] = "run_1"
        os.environ["AGENTLAB_TRIAL_ID"] = "trial_1"
        os.environ["AGENTLAB_VARIANT_ID"] = "baseline"
        os.environ["AGENTLAB_TASK_ID"] = "task_1"
        os.environ["AGENTLAB_REPL_IDX"] = "0"

    def tearDown(self) -> None:
        for key, value in self.prev.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    @staticmethod
    def _keys() -> list[str]:
        return [
            "AGENTLAB_RUN_ID",
            "AGENTLAB_TRIAL_ID",
            "AGENTLAB_VARIANT_ID",
            "AGENTLAB_TASK_ID",
            "AGENTLAB_REPL_IDX",
            "AGENTLAB_GRADING_STRATEGY",
            "AGENTLAB_GRADER_STRATEGY",
        ]

    def test_inv01_bench_adapter_trial_conclusion_validates_schema(self) -> None:
        task = {
            "id": "task_1",
            "benchmark": {"adapter_id": "bench_v0", "name": "bench", "split": "test"},
        }
        payload = adapter.build_trial_conclusion(
            task,
            patch_text="diff --git a b",
            score=None,
            error_message="grader exploded",
        )
        errors = validate_with_schema_file(
            payload,
            SCHEMAS_DIR / "trial_conclusion_v1.jsonschema",
        )
        self.assertEqual(errors, [], f"schema validation errors: {errors}")

    def test_inv01_bench_adapter_missing_grader_fails_schema_validation(self) -> None:
        task = {
            "id": "task_1",
            "benchmark": {"adapter_id": "bench_v0", "name": "bench", "split": "test"},
        }
        payload = adapter.build_trial_conclusion(
            task,
            patch_text="diff --git a b",
            score=None,
            error_message="grader exploded",
        )
        payload.pop("grader")
        errors = validate_with_schema_file(
            payload,
            SCHEMAS_DIR / "trial_conclusion_v1.jsonschema",
        )
        self.assertTrue(errors, "missing grader must fail schema validation")

    def test_inv01_bench_adapter_missing_patch_maps_to_missing(self) -> None:
        task = {
            "id": "task_1",
            "benchmark": {"adapter_id": "bench_v0", "name": "bench", "split": "test"},
        }
        payload = adapter.build_trial_conclusion(
            task,
            patch_text=None,
            score={"failure_label": "NO_PATCH", "overall_pass": False},
            error_message=None,
        )
        self.assertEqual(payload["payload"]["verdict"], "missing")
        self.assertEqual(payload["reported_outcome"], "missing")


if __name__ == "__main__":
    unittest.main()
