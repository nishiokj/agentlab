#!/usr/bin/env python3
"""Export bench task bundles as AgentLab task-boundary JSONL."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

DEFAULT_SUITE = "v0"
DEFAULT_SPLIT = "test"
DEFAULT_BENCHMARK_NAME = "bench"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _task_prompt(task_dir: Path) -> str:
    issue = task_dir / "issue.md"
    if not issue.exists():
        return ""
    return issue.read_text(encoding="utf-8", errors="replace").strip()


def _load_task_yaml(task_dir: Path) -> dict[str, Any]:
    task_yaml = task_dir / "task.yaml"
    if not task_yaml.exists():
        raise FileNotFoundError(f"missing task.yaml: {task_yaml}")
    payload = yaml.safe_load(task_yaml.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"task.yaml must decode to an object: {task_yaml}")
    return payload


def _build_task_row(
    root: Path,
    suite: str,
    split: str,
    benchmark_name: str,
    adapter_id: str,
    task_dir: Path,
) -> dict[str, Any]:
    task_yaml = _load_task_yaml(task_dir)
    task_id = task_dir.name
    task_payload: dict[str, Any] = {
        "id": task_id,
        "repo_id": task_yaml.get("repo_id"),
        "task_dir": str(task_dir.relative_to(root)),
        "bench": {
            "suite": suite,
            "task_dir": str(task_dir.relative_to(root)),
        },
        "benchmark": {
            "adapter_id": adapter_id,
            "name": benchmark_name,
            "split": split,
        },
        "input": {
            "prompt": _task_prompt(task_dir),
        },
    }

    public_command = task_yaml.get("public_command")
    if isinstance(public_command, str) and public_command.strip():
        task_payload["public_command"] = public_command.strip()

    hidden_command = task_yaml.get("hidden_command")
    if isinstance(hidden_command, str) and hidden_command.strip():
        task_payload["hidden_command"] = hidden_command.strip()

    row: dict[str, Any] = {
        "schema_version": "task_boundary_v1",
        "task": task_payload,
        "workspace_files": [],
        "mount_references": [],
    }
    return row


def _iter_task_dirs(suite_dir: Path) -> list[Path]:
    return sorted(
        p for p in suite_dir.iterdir() if p.is_dir() and p.name.startswith("TASK")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Export bench suite to AgentLab task JSONL")
    parser.add_argument(
        "--suite",
        default=DEFAULT_SUITE,
        help="Suite under bench/benchmark/tasks/ (default: v0)",
    )
    parser.add_argument(
        "--split",
        default=DEFAULT_SPLIT,
        help=f"Benchmark split label (default: {DEFAULT_SPLIT})",
    )
    parser.add_argument(
        "--benchmark-name",
        default=DEFAULT_BENCHMARK_NAME,
        help=f"Benchmark name field (default: {DEFAULT_BENCHMARK_NAME})",
    )
    parser.add_argument(
        "--adapter-id",
        default="bench_v0",
        help="Benchmark adapter_id written into task rows (default: bench_v0)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSONL path (default: data/bench_<suite>_task_boundary_v1.jsonl)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of tasks to export",
    )
    args = parser.parse_args()

    root = _repo_root()
    suite_dir = root / "bench" / "benchmark" / "tasks" / args.suite
    if not suite_dir.exists():
        raise FileNotFoundError(f"suite directory not found: {suite_dir}")

    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = root / out_path
    else:
        out_path = root / "data" / f"bench_{args.suite}_task_boundary_v1.jsonl"

    task_dirs = _iter_task_dirs(suite_dir)
    if args.limit and args.limit > 0:
        task_dirs = task_dirs[: args.limit]

    rows = [
        _build_task_row(
            root=root,
            suite=args.suite,
            split=args.split,
            benchmark_name=args.benchmark_name,
            adapter_id=args.adapter_id,
            task_dir=task_dir,
        )
        for task_dir in task_dirs
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    out_path.write_text(body + ("\n" if rows else ""), encoding="utf-8")

    print(
        json.dumps(
            {
                "suite": args.suite,
                "count": len(rows),
                "output": str(out_path),
                "split": args.split,
                "benchmark_name": args.benchmark_name,
                "adapter_id": args.adapter_id,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
