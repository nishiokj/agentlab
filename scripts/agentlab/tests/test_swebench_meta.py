#!/usr/bin/env python3

from __future__ import annotations

import os
import unittest

from adapters.swebench import swebench_official_benchmark_adapter as official_adapter
from adapters.swebench import swebench_task_container_grader as task_grader


class ExtractSwebenchMetaTests(unittest.TestCase):
    def test_extract_meta_from_nested_task_swebench_input(self) -> None:
        payload = {
            "task": {
                "swebench": {
                    "input": {
                        "repo": "astropy/astropy",
                        "instance_id": "astropy__astropy-12907",
                        "base_commit": "deadbeef",
                    }
                }
            }
        }
        expected = {
            "repo": "astropy/astropy",
            "instance_id": "astropy__astropy-12907",
            "base_commit": "deadbeef",
            "problem_statement": None,
        }
        self.assertEqual(task_grader.extract_swebench_meta(payload), expected)
        self.assertEqual(official_adapter.extract_swebench_meta(payload), expected)

    def test_extract_meta_from_top_level_swebench_input(self) -> None:
        payload = {
            "swebench": {
                "input": {
                    "repo": "django/django",
                    "instance_id": "django__django-12345",
                    "base_commit": "cafebabe",
                    "problem_statement": "Fix regression",
                }
            }
        }
        expected = {
            "repo": "django/django",
            "instance_id": "django__django-12345",
            "base_commit": "cafebabe",
            "problem_statement": "Fix regression",
        }
        self.assertEqual(task_grader.extract_swebench_meta(payload), expected)
        self.assertEqual(official_adapter.extract_swebench_meta(payload), expected)

    def test_extract_meta_uses_instance_id_fallbacks(self) -> None:
        payload = {
            "task": {
                "input": {
                    "instance_id": "pytest__dev-00001",
                }
            }
        }
        meta = task_grader.extract_swebench_meta(payload)
        self.assertEqual(meta["instance_id"], "pytest__dev-00001")
        self.assertIsNone(meta["repo"])
        self.assertIsNone(meta["base_commit"])
        self.assertIsNone(meta["problem_statement"])

    def test_extract_meta_missing_or_invalid_returns_none(self) -> None:
        payload = {
            "task": {
                "swebench": {"input": {"repo": 42, "instance_id": [], "base_commit": ""}}
            },
            "input": {"instance_id": {"bad": "shape"}},
        }
        expected = {
            "repo": None,
            "instance_id": None,
            "base_commit": None,
            "problem_statement": None,
        }
        self.assertEqual(task_grader.extract_swebench_meta(payload), expected)
        self.assertEqual(official_adapter.extract_swebench_meta(payload), expected)


class PredictionRecordTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prev = {k: os.environ.get(k) for k in self._keys()}
        os.environ["AGENTLAB_RUN_ID"] = "run_test"
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
        ]

    def test_prediction_record_contains_non_null_instance_id_for_nested_payload(self) -> None:
        payload = {
            "task": {
                "id": "task_1",
                "swebench": {"input": {"instance_id": "numpy__numpy-98765"}},
            }
        }
        record = task_grader.build_prediction_record(payload, {"patch": "diff --git a b"})
        self.assertEqual(
            record["ext"]["swebench"]["instance_id"],
            "numpy__numpy-98765",
        )

    def test_prediction_record_contains_non_null_instance_id_for_top_level_payload(self) -> None:
        payload = {
            "id": "task_1",
            "swebench": {"input": {"instance_id": "pandas__pandas-54321"}},
        }
        record = official_adapter.build_prediction_record(payload, {"patch": "diff --git a b"})
        self.assertEqual(
            record["ext"]["swebench"]["instance_id"],
            "pandas__pandas-54321",
        )


if __name__ == "__main__":
    unittest.main()
