#!/usr/bin/env python3

from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
