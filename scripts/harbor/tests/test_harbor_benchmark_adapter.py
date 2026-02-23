#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import harbor_benchmark_adapter as adapter


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
            "AGENTLAB_AGENT_EXIT_STATUS",
            "HARBOR_EVALUATOR_CMD_JSON",
            "HARBOR_EVALUATOR_CMD",
        ]

    def test_fallback_records_match_schema_shape(self) -> None:
        os.environ["AGENTLAB_AGENT_EXIT_STATUS"] = "0"
        task = {
            "id": "tb2_task_1",
            "benchmark": {
                "adapter_id": "harbor_tb2",
                "name": "terminal_bench_2",
                "split": "test",
            },
        }
        result = {"outcome": True, "output": {"patch": "diff --git a b"}}

        prediction = adapter.build_prediction_record(task, result, None)
        score = adapter.build_score_record(task, result, None)

        self.assertEqual(prediction["schema_version"], "benchmark_prediction_record_v1")
        self.assertEqual(prediction["ids"]["trial_id"], "trial_1")
        self.assertEqual(prediction["benchmark"]["adapter_id"], "harbor_tb2")
        self.assertEqual(prediction["prediction"]["kind"], "patch")

        self.assertEqual(score["schema_version"], "benchmark_score_record_v1")
        self.assertEqual(score["verdict"], "pass")
        self.assertEqual(score["primary_metric_name"], "resolved")
        self.assertEqual(score["primary_metric_value"], 1.0)
        self.assertEqual(score["metrics"]["resolved"], 1.0)
        self.assertIn("evaluator", score)

    def test_external_evaluator_output_is_used(self) -> None:
        payload = {
            "verdict": "fail",
            "primary_metric_name": "resolved",
            "primary_metric_value": 0.0,
            "metrics": {"resolved": 0.0},
            "prediction": {"kind": "text", "value": "no patch"},
            "evaluator": {"name": "harbor_official", "mode": "official"},
        }
        cmd = [sys.executable, "-c", f"import json;print(json.dumps({payload!r}))"]
        os.environ["HARBOR_EVALUATOR_CMD_JSON"] = json.dumps(cmd)

        task = {"id": "tb2_task_1"}
        result = {"outcome": True}

        evaluation = adapter.run_external_evaluator(task, result)
        self.assertIsInstance(evaluation, dict)

        prediction = adapter.build_prediction_record(task, result, evaluation)
        score = adapter.build_score_record(task, result, evaluation)

        self.assertEqual(prediction["prediction"]["kind"], "text")
        self.assertEqual(score["verdict"], "fail")
        self.assertEqual(score["primary_metric_value"], 0.0)
        self.assertEqual(score["evaluator"]["name"], "harbor_official")
        self.assertEqual(score["evaluator"]["mode"], "official")

    def test_invalid_evaluator_cmd_json_raises_structured_error(self) -> None:
        os.environ["HARBOR_EVALUATOR_CMD_JSON"] = "{invalid"
        with self.assertRaises(adapter.HarborAdapterError) as ctx:
            adapter.run_external_evaluator({}, {})
        self.assertEqual(ctx.exception.code, "config.invalid_evaluator_cmd_json")
        self.assertEqual(ctx.exception.exit_code, 21)


if __name__ == "__main__":
    unittest.main()
