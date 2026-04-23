from __future__ import annotations

import subprocess
import tarfile
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


def _write_repo_snapshot(
    root: Path,
    snapshot_rel: str,
    files: dict[str, str],
) -> Path:
    snapshot_path = root / "bench" / "benchmark" / "repos" / snapshot_rel
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        src_root = Path(tmp)
        for rel, content in files.items():
            target = src_root / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        with tarfile.open(snapshot_path, mode="w") as archive:
            archive.add(src_root, arcname=".")

    return snapshot_path


class ExportBenchSuiteToJsonlTests(unittest.TestCase):
    def test_build_task_row_fails_when_task_image_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK001"
            task_dir.mkdir(parents=True)
            _write_repo_snapshot(
                root,
                "repo_a/src.tar",
                {"src/file.txt": "before\n"},
            )
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK001",
                    "repo_id": "repo_a",
                    "repo_snapshot": "repo_a/src.tar",
                    "public_command": "bash .bench_public/run_public.sh",
                    "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl",
                },
            )

            with self.assertRaisesRegex(
                ValueError, "environment.image could not be resolved"
            ):
                exporter._build_task_row(
                    root=root,
                    suite="v0",
                    split="test",
                    benchmark_name="bench",
                    adapter_id="bench_v0",
                    task_dir=task_dir,
                    default_task_image=None,
                    dataset_pack_root=exporter.DEFAULT_DATASET_PACK_ROOT,
                    require_task_image=True,
                )

    def test_build_task_row_emits_workspace_base_and_materializes_dataset_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK001"
            task_dir.mkdir(parents=True)
            _write_repo_snapshot(
                root,
                "repo_a/src.tar",
                {
                    "src/file.txt": "before\n",
                    "bench/benchmark/tasks/SHOULD_NOT_LEAK.md": "sensitive\n",
                },
            )
            (task_dir / "injection.patch").write_text(
                "\n".join(
                    [
                        "diff --git a/src/file.txt b/src/file.txt",
                        "--- a/src/file.txt",
                        "+++ b/src/file.txt",
                        "@@ -1 +1 @@",
                        "-before",
                        "+after",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK001",
                    "repo_id": "repo_a",
                    "repo_snapshot": "repo_a/src.tar",
                    "baseline_injection_patch": "injection.patch",
                    "public_command": "bash .bench_public/run_public.sh",
                    "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl",
                    "description": "Risk scorer regression in dependency impact scoring.",
                    "difficulty": "hard",
                    "tags": ["repo_a", "scorer_v1"],
                },
            )
            (task_dir / "issue.md").write_text("Fix regression", encoding="utf-8")
            public_dir = task_dir / "public"
            public_dir.mkdir()
            (public_dir / "repro.md").write_text("Run bash .bench_public/run_public.sh", encoding="utf-8")
            run_public = public_dir / "run_public.sh"
            run_public.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            run_public.chmod(0o755)

            row = exporter._build_task_row(
                root=root,
                suite="v0",
                split="test",
                benchmark_name="bench",
                adapter_id="bench_v0",
                task_dir=task_dir,
                default_task_image="bench-v0-workspace",
                dataset_pack_root=exporter.DEFAULT_DATASET_PACK_ROOT,
                require_task_image=True,
            )

            self.assertNotIn("schema_version", row)
            self.assertEqual(row["task"]["id"], "TASK001")
            self.assertEqual(
                row["environment"]["image"], "bench-v0-workspace-task001:latest"
            )
            self.assertEqual(
                row["task"]["input"]["prompt"],
                "\n\n".join(
                    [
                        "Fix regression",
                        "\n".join(
                            [
                                "Task metadata:",
                                "- Task description: Risk scorer regression in dependency impact scoring.",
                                "- Difficulty: hard",
                                "- Tags: repo_a, scorer_v1",
                            ]
                        ),
                    ]
                ),
            )
            self.assertEqual(
                row["workspace"]["overlays"],
                [
                    {
                        "path": "ISSUE.md",
                        "content": "Fix regression",
                        "encoding": "utf8",
                        "executable": False,
                    },
                    {
                        "path": ".bench_public/repro.md",
                        "content": "Run bash .bench_public/run_public.sh",
                        "encoding": "utf8",
                        "executable": False,
                    },
                    {
                        "path": ".bench_public/run_public.sh",
                        "content": "#!/usr/bin/env bash\nexit 0\n",
                        "encoding": "utf8",
                        "executable": True,
                    },
                ],
            )
            self.assertEqual(row["workspace"]["mode"], "patch")
            self.assertEqual(row["workspace"]["base"]["kind"], "dataset_pack")
            self.assertEqual(row["workspace"]["aux_mounts"], [])
            self.assertEqual(row["limits"], {})

            base_ref = row["workspace"]["base"]["dataset_pack_ref"]
            self.assertTrue(base_ref.startswith("sha256:"))
            digest = base_ref.split(":", 1)[1]
            pack_dir = root / ".lab" / "dataset_packs" / "sha256" / digest
            self.assertTrue(pack_dir.exists())
            self.assertEqual(
                (pack_dir / "src/file.txt").read_text(encoding="utf-8"),
                "after\n",
            )
            self.assertFalse((pack_dir / "bench" / "benchmark" / "tasks").exists())

    def test_build_task_row_prefers_task_yaml_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK002"
            task_dir.mkdir(parents=True)
            _write_repo_snapshot(
                root,
                "repo_b/src.tar",
                {"src/file.txt": "v\n"},
            )
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK002",
                    "repo_id": "repo_b",
                    "repo_snapshot": "repo_b/src.tar",
                    "image": "ghcr.io/example/bench-task:latest",
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
                default_task_image="bench-v0-workspace",
                dataset_pack_root=exporter.DEFAULT_DATASET_PACK_ROOT,
                require_task_image=True,
            )

            self.assertEqual(
                row["environment"]["image"], "ghcr.io/example/bench-task:latest"
            )

    def test_build_task_row_rebuilds_stale_dataset_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK004"
            task_dir.mkdir(parents=True)
            _write_repo_snapshot(
                root,
                "repo_d/src.tar",
                {"src/file.txt": "before\n"},
            )
            (task_dir / "injection.patch").write_text(
                "\n".join(
                    [
                        "diff --git a/src/file.txt b/src/file.txt",
                        "--- a/src/file.txt",
                        "+++ b/src/file.txt",
                        "@@ -1 +1 @@",
                        "-before",
                        "+after",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK004",
                    "repo_id": "repo_d",
                    "repo_snapshot": "repo_d/src.tar",
                    "baseline_injection_patch": "injection.patch",
                    "public_command": "bash .bench_public/run_public.sh",
                    "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl",
                },
            )

            first = exporter._build_task_row(
                root=root,
                suite="v0",
                split="test",
                benchmark_name="bench",
                adapter_id="bench_v0",
                task_dir=task_dir,
                default_task_image="bench-v0-workspace",
                dataset_pack_root=exporter.DEFAULT_DATASET_PACK_ROOT,
                require_task_image=True,
            )

            digest = first["workspace"]["base"]["dataset_pack_ref"].split(":", 1)[1]
            pack_dir = root / ".lab" / "dataset_packs" / "sha256" / digest
            (pack_dir / "src" / "file.txt").write_text("corrupted\n", encoding="utf-8")

            second = exporter._build_task_row(
                root=root,
                suite="v0",
                split="test",
                benchmark_name="bench",
                adapter_id="bench_v0",
                task_dir=task_dir,
                default_task_image="bench-v0-workspace",
                dataset_pack_root=exporter.DEFAULT_DATASET_PACK_ROOT,
                require_task_image=True,
            )

            self.assertEqual(
                second["workspace"]["base"]["dataset_pack_ref"],
                first["workspace"]["base"]["dataset_pack_ref"],
            )
            self.assertEqual(
                (pack_dir / "src" / "file.txt").read_text(encoding="utf-8"),
                "after\n",
            )

    def test_materialize_workspace_base_pack_applies_patch_inside_git_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True, text=True)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK005"
            task_dir.mkdir(parents=True)
            _write_repo_snapshot(
                root,
                "repo_e/src.tar",
                {"src/file.txt": "before\n"},
            )
            (task_dir / "injection.patch").write_text(
                "\n".join(
                    [
                        "diff --git a/src/file.txt b/src/file.txt",
                        "--- a/src/file.txt",
                        "+++ b/src/file.txt",
                        "@@ -1 +1 @@",
                        "-before",
                        "+after",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK005",
                    "repo_id": "repo_e",
                    "repo_snapshot": "repo_e/src.tar",
                    "baseline_injection_patch": "injection.patch",
                    "public_command": "bash .bench_public/run_public.sh",
                    "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl",
                },
            )

            digest = exporter._materialize_workspace_base_pack(
                root=root,
                task_dir=task_dir,
                task_yaml=exporter._load_task_yaml(task_dir),
                dataset_pack_root=exporter.DEFAULT_DATASET_PACK_ROOT,
            )

            pack_dir = root / ".lab" / "dataset_packs" / "sha256" / digest
            self.assertEqual(
                (pack_dir / "src" / "file.txt").read_text(encoding="utf-8"),
                "after\n",
            )

    def test_task_prompt_falls_back_to_task_yaml_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / "bench" / "benchmark" / "tasks" / "v0" / "TASK003"
            task_dir.mkdir(parents=True)
            _write_repo_snapshot(
                root,
                "repo_c/src.tar",
                {"src/file.txt": "v\n"},
            )
            _write_task_yaml(
                task_dir,
                {
                    "task_id": "TASK003",
                    "repo_id": "repo_c",
                    "repo_snapshot": "repo_c/src.tar",
                    "description": "Unified diff parser regression in PR review ingestion.",
                    "tags": ["repo_c", "diff_v1"],
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
                default_task_image="bench-v0-workspace",
                dataset_pack_root=exporter.DEFAULT_DATASET_PACK_ROOT,
                require_task_image=True,
            )

            self.assertEqual(
                row["task"]["input"]["prompt"],
                "\n".join(
                    [
                        "Task metadata:",
                        "- Task description: Unified diff parser regression in PR review ingestion.",
                        "- Tags: repo_c, diff_v1",
                    ]
                ),
            )
            self.assertEqual(row["workspace"]["overlays"], [])


if __name__ == "__main__":
    unittest.main()
