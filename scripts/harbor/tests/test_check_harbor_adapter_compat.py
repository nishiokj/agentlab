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
            mapped_output_path = tmp_dir / "mapped_grader_output.json"
            mapped_output_path.write_text(
                json.dumps(
                    {
                        "schema_version": "trial_conclusion_v1",
                        "payload": {
                            "verdict": "pass",
                        },
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError) as ctx:
                compat._validate_output(mapped_output_path, False)
            self.assertIn("schema validation failed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
