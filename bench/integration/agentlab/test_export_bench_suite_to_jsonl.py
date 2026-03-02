from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from bench.integration.agentlab import export_bench_suite_to_jsonl as exporter


def _write_task_yaml(task_dir: Path, payload: dict[str, object]) -> None:
    (task_dir / "task.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


class ExportBenchSuiteToJsonlTests(unittest.TestCase):
    def test_build_task_row_fails_when_task_image_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK001"
            task_dir.mkdir(parents=True)
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK001",
                    "repo_id": "repo_a",
                    "public_command": "bash .bench_public/run_public.sh",
                    "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl",
                },
            )

            with self.assertRaisesRegex(ValueError, "task.image missing"):
                exporter._build_task_row(
                    root=root,
                    suite="v0",
                    split="test",
                    benchmark_name="bench",
                    adapter_id="bench_v0",
                    task_dir=task_dir,
                    default_task_image=None,
                    default_task_workspace=exporter.DEFAULT_TASK_WORKSPACE,
                    require_task_image=True,
                )

    def test_build_task_row_applies_default_task_image_and_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK001"
            task_dir.mkdir(parents=True)
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK001",
                    "repo_id": "repo_a",
                    "public_command": "bash .bench_public/run_public.sh",
                    "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl",
                },
            )
            (task_dir / "issue.md").write_text("Fix regression", encoding="utf-8")

            row = exporter._build_task_row(
                root=root,
                suite="v0",
                split="test",
                benchmark_name="bench",
                adapter_id="bench_v0",
                task_dir=task_dir,
                default_task_image="python:3.11-slim",
                default_task_workspace=exporter.DEFAULT_TASK_WORKSPACE,
                require_task_image=True,
            )

            self.assertEqual(row["schema_version"], exporter.TASK_BOUNDARY_SCHEMA_VERSION)
            self.assertEqual(row["workspace_files"], [])
            self.assertEqual(row["mount_references"], [])
            self.assertEqual(row["task"]["id"], "TASK001")
            self.assertEqual(row["task"]["image"], "python:3.11-slim")
            self.assertEqual(row["task"]["workspace"], exporter.DEFAULT_TASK_WORKSPACE)
            self.assertEqual(row["task"]["input"]["prompt"], "Fix regression")

    def test_build_task_row_prefers_task_yaml_image_and_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK002"
            task_dir.mkdir(parents=True)
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK002",
                    "repo_id": "repo_b",
                    "image": "ghcr.io/example/bench-task:latest",
                    "workspace": "/workspace",
                    "public_command": "bash .bench_public/run_public.sh",
                    "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl",
                },
            )

            row = exporter._build_task_row(
                root=root,
                suite="v0",
                split="test",
                benchmark_name="bench",
                adapter_id="bench_v0",
                task_dir=task_dir,
                default_task_image="python:3.11-slim",
                default_task_workspace=exporter.DEFAULT_TASK_WORKSPACE,
                require_task_image=True,
            )

            self.assertEqual(row["task"]["image"], "ghcr.io/example/bench-task:latest")
            self.assertEqual(row["task"]["workspace"], "/workspace")


if __name__ == "__main__":
    unittest.main()
