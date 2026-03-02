#!/usr/bin/env python3

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from adapters.harbor import export_harbor_to_agentlab_jsonl as exporter


class HarborExporterTests(unittest.TestCase):
    def test_parse_task_dir_maps_task_boundary_v2(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "task_alpha"
            task_dir.mkdir(parents=True)
            (task_dir / "task.toml").write_text(
                "\n".join(
                    [
                        'id = "tb2_alpha"',
                        'prompt = "Fix the failing command"',
                        "",
                        "[environment]",
                        'image = "ghcr.io/example/tb2:alpha"',
                        'workspace = "/workspace/project"',
                        "",
                        "[limits]",
                        "max_runtime_seconds = 30",
                    ]
                ),
                encoding="utf-8",
            )

            config = exporter.ExportConfig(
                benchmark_name="terminal_bench_2",
                adapter_id="harbor_tb2",
                split="test",
            )
            row = exporter.parse_task_dir(task_dir, config)

            self.assertEqual(row["schema_version"], "task_boundary_v2")
            self.assertEqual(row["task"]["id"], "tb2_alpha")
            self.assertEqual(row["task"]["benchmark"]["adapter_id"], "harbor_tb2")
            self.assertEqual(row["task"]["benchmark"]["name"], "terminal_bench_2")
            self.assertEqual(row["task"]["benchmark"]["split"], "test")
            self.assertEqual(row["task"]["image"], "ghcr.io/example/tb2:alpha")
            self.assertEqual(row["task"]["workspace"], "/workspace/project")
            self.assertEqual(row["limits"]["max_runtime_ms"], 30000)

    def test_prompt_falls_back_to_prompt_txt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "task_prompt_sidecar"
            task_dir.mkdir(parents=True)
            (task_dir / "task.toml").write_text('id = "tb2_sidecar"\n', encoding="utf-8")
            (task_dir / "prompt.txt").write_text("Diagnose and patch bug\n", encoding="utf-8")

            row = exporter.parse_task_dir(task_dir, exporter.ExportConfig())
            self.assertEqual(row["task"]["prompt"], "Diagnose and patch bug")

    def test_require_task_image_fails_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "task_missing_image"
            task_dir.mkdir(parents=True)
            (task_dir / "task.toml").write_text('id = "tb2_no_image"\n', encoding="utf-8")

            with self.assertRaises(ValueError):
                exporter.parse_task_dir(
                    task_dir,
                    exporter.ExportConfig(require_task_image=True),
                )

    def test_default_task_image_is_applied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "task_default_image"
            task_dir.mkdir(parents=True)
            (task_dir / "task.toml").write_text('id = "tb2_default_image"\n', encoding="utf-8")

            row = exporter.parse_task_dir(
                task_dir,
                exporter.ExportConfig(default_task_image="python:3.11-slim"),
            )
            self.assertEqual(row["task"]["image"], "python:3.11-slim")

    def test_write_jsonl_emits_one_record_per_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.jsonl"
            rows = [
                {"schema_version": "task_boundary_v2", "task": {"id": "a"}, "workspace_files": [], "mount_references": [], "limits": {}},
                {"schema_version": "task_boundary_v2", "task": {"id": "b"}, "workspace_files": [], "mount_references": [], "limits": {}},
            ]
            count = exporter.write_jsonl(output, rows)
            self.assertEqual(count, 2)

            lines = [line for line in output.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(lines), 2)
            decoded = [json.loads(line) for line in lines]
            self.assertEqual(decoded[0]["task"]["id"], "a")
            self.assertEqual(decoded[1]["task"]["id"], "b")


if __name__ == "__main__":
    unittest.main()
