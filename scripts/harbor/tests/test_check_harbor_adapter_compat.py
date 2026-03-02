#!/usr/bin/env python3

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from adapters.harbor import check_harbor_adapter_compat as compat


class HarborCompatProbeTests(unittest.TestCase):
    def test_extract_error_code(self) -> None:
        stderr = "harbor_benchmark_adapter.py error_code=evaluator.command_failed message=oops"
        self.assertEqual(compat._extract_error_code(stderr), "evaluator.command_failed")

    def test_actionable_message_for_evaluator_failure(self) -> None:
        message = compat._actionable_error("evaluator.command_failed", "stderr")
        self.assertIn("Harbor evaluator command failed", message)
        self.assertIn("API drift", message)

    def test_actionable_message_for_config_failure(self) -> None:
        message = compat._actionable_error("config.missing_env", "stderr")
        self.assertIn("configuration failure", message)
        self.assertIn("env vars", message)

    def test_inv01_harbor_compat_probe_rejects_schema_invalid_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            prediction_path = tmp_dir / "prediction.json"
            score_path = tmp_dir / "score.json"
            prediction_path.write_text(
                json.dumps(
                    {
                        "schema_version": "benchmark_prediction_record_v1",
                        "ids": {
                            "run_id": "run_1",
                            "trial_id": "trial_1",
                            "variant_id": "base",
                            "task_id": "task_1",
                            "repl_idx": 0,
                        },
                        "benchmark": {"adapter_id": "harbor_tb2", "name": "tb2", "split": "test"},
                        "prediction": {"kind": "text", "value": "ok"},
                    }
                ),
                encoding="utf-8",
            )
            score_path.write_text(
                json.dumps(
                    {
                        "schema_version": "benchmark_score_record_v1",
                        "schedule_idx": 0,
                        "slot_commit_id": "slot_pending",
                        "attempt": 1,
                        "row_seq": 0,
                        "ids": {
                            "run_id": "run_1",
                            "trial_id": "trial_1",
                            "variant_id": "base",
                            "task_id": "task_1",
                            "repl_idx": 0,
                        },
                        "benchmark": {"adapter_id": "harbor_tb2", "name": "tb2", "split": "test"},
                        "verdict": "pass",
                        "primary_metric_name": "resolved",
                        "primary_metric_value": 1.0,
                        "evaluator": {"name": "eval", "mode": "custom"},
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError) as ctx:
                compat._validate_outputs(prediction_path, score_path, False)
            self.assertIn("schema validation failed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
