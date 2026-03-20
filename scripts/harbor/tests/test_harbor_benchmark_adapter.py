#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

from adapters.harbor import harbor_benchmark_adapter as adapter
from bench.taskkit.schema import validate_with_schema_file

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMAS_DIR = REPO_ROOT / "schemas"


class HarborBenchmarkAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prev = {k: os.environ.get(k) for k in self._keys()}
        os.environ["AGENTLAB_RUN_ID"] = "run_1"
        os.environ["AGENTLAB_TRIAL_ID"] = "trial_1"
        os.environ["AGENTLAB_VARIANT_ID"] = "baseline"
        os.environ["AGENTLAB_TASK_ID"] = "tb2_task_1"
        os.environ["AGENTLAB_REPL_IDX"] = "0"
        os.environ.pop("HARBOR_EVALUATOR_CMD_JSON", None)
        os.environ.pop("HARBOR_EVALUATOR_CMD", None)

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
            "HARBOR_EVALUATOR_CMD_JSON",
            "HARBOR_EVALUATOR_CMD",
            "AGENTLAB_GRADING_STRATEGY",
            "AGENTLAB_GRADER_STRATEGY",
        ]

    def _assert_schema_valid(self, payload: dict[str, object], schema_file: str) -> None:
        errors = validate_with_schema_file(payload, SCHEMAS_DIR / schema_file)
        self.assertEqual(errors, [], f"schema validation errors: {errors}")

    def test_inv01_fallback_conclusion_matches_schema_shape(self) -> None:
        task = {
            "id": "tb2_task_1",
            "benchmark": {
                "adapter_id": "harbor_tb2",
                "name": "terminal_bench_2",
                "split": "test",
            },
        }
        grader_input = {
            "ids": {
                "run_id": "run_1",
                "trial_id": "trial_1",
                "variant_id": "baseline",
                "task_id": "tb2_task_1",
                "repl_idx": 0,
            },
            "agent_phase": {
                "exit_code": 0,
            },
            "candidate_artifact": {
                "state": "valid",
            },
        }
        candidate_payload = {"outcome": True, "output": {"patch": "diff --git a b"}}

        conclusion = adapter.build_trial_conclusion(task, grader_input, candidate_payload, None)

        self.assertEqual(conclusion["payload"]["benchmark"]["adapter_id"], "harbor_tb2")
        self.assertEqual(conclusion["payload"]["prediction"]["kind"], "patch")
        self.assertEqual(conclusion["reported_outcome"], "success")
        self.assertEqual(conclusion["primary_metric"]["name"], "resolved")
        self.assertEqual(conclusion["primary_metric"]["value"], 1.0)
        self._assert_schema_valid(conclusion, "trial_conclusion_v1.jsonschema")

    def test_external_evaluator_output_is_used(self) -> None:
        payload = {
            "verdict": "fail",
            "primary_metric_name": "resolved",
            "primary_metric_value": 0.0,
            "metrics": {"resolved": 0.0},
            "evaluator": {"name": "harbor_official", "mode": "official"},
        }
        cmd = [sys.executable, "-c", f"import json;print(json.dumps({payload!r}))"]
        os.environ["HARBOR_EVALUATOR_CMD_JSON"] = json.dumps(cmd)

        task = {"id": "tb2_task_1"}
        candidate_payload = {"outcome": True}

        evaluation = adapter.run_external_evaluator(task, candidate_payload)
        self.assertIsInstance(evaluation, dict)

        conclusion = adapter.build_trial_conclusion(task, {"ids": {}}, candidate_payload, evaluation)

        self.assertEqual(conclusion["payload"]["verdict"], "fail")
        self.assertEqual(conclusion["primary_metric"]["value"], 0.0)
        self.assertEqual(conclusion["payload"]["evaluator"]["name"], "harbor_official")
        self.assertEqual(conclusion["payload"]["evaluator"]["mode"], "official")

    def test_invalid_evaluator_cmd_json_raises_structured_error(self) -> None:
        os.environ["HARBOR_EVALUATOR_CMD_JSON"] = "{invalid"
        with self.assertRaises(adapter.HarborAdapterError) as ctx:
            adapter.run_external_evaluator({}, {})
        self.assertEqual(ctx.exception.code, "config.invalid_evaluator_cmd_json")
        self.assertEqual(ctx.exception.exit_code, 21)


if __name__ == "__main__":
    unittest.main()
