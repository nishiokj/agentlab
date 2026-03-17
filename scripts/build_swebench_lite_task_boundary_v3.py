#!/usr/bin/env python3
"""Convert curated SWE-bench Lite rows into AgentLab task_row_v1 rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_INPUT = (
    ".lab/runs/run_20260223_025729/trials/trial_1/dataset/swebench_lite_curated.jsonl"
)
DEFAULT_OUTPUT = ".lab/experiments/data/swebench_lite_curated.task_spec.jsonl"
DEFAULT_BENCHMARK_NAME = "swebench_lite_curated"
DEFAULT_SPLIT = "test"
DEFAULT_ADAPTER_ID = "swebench_task_container_grader"


def _load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        payload = json.loads(stripped)
        if not isinstance(payload, dict):
            raise ValueError(f"line {idx} in {path} is not a JSON object")
        rows.append(payload)
    return rows


def _require_string(obj: dict[str, Any], key: str, *, label: str) -> str:
    value = obj.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"{label} missing non-empty string field '{key}'")


def _to_boundary_row(
    row: dict[str, Any],
    *,
    benchmark_name: str,
    split: str,
    adapter_id: str,
) -> dict[str, Any]:
    task_id = _require_string(row, "task_id", label="task row")
    input_obj = row.get("input")
    if not isinstance(input_obj, dict):
        raise ValueError(f"{task_id}: input must be an object")

    repo = _require_string(input_obj, "repo", label=task_id)
    instance_id = _require_string(input_obj, "instance_id", label=task_id)
    base_commit = _require_string(input_obj, "base_commit", label=task_id)
    prompt = _require_string(input_obj, "prompt", label=task_id)
    image = f"swebench/sweb.eval.x86_64.{instance_id}:latest"

    task_payload: dict[str, Any] = {
        "id": task_id,
        "benchmark": {
            "adapter_id": adapter_id,
            "name": benchmark_name,
            "split": split,
        },
        "input": {
            "prompt": prompt,
        },
        "swebench": {
            "input": {
                "repo": repo,
                "instance_id": instance_id,
                "base_commit": base_commit,
            }
        },
    }

    hints = input_obj.get("hints_text")
    if isinstance(hints, str) and hints.strip():
        task_payload["swebench"]["input"]["hints_text"] = hints.strip()

    metadata = row.get("metadata")
    if isinstance(metadata, dict) and metadata:
        task_payload["metadata"] = metadata

    return {
        "schema_version": "task_row_v1",
        "id": task_id,
        "image": image,
        "workdir": "/testbed",
        "task": task_payload,
        "materialization": {
            "kind": "task_image",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Source curated SWE-bench JSONL")
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output task JSONL path",
    )
    parser.add_argument("--benchmark-name", default=DEFAULT_BENCHMARK_NAME)
    parser.add_argument("--split", default=DEFAULT_SPLIT)
    parser.add_argument("--adapter-id", default=DEFAULT_ADAPTER_ID)
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()
    rows = _load_rows(input_path)
    converted = [
        _to_boundary_row(
            row,
            benchmark_name=args.benchmark_name,
            split=args.split,
            adapter_id=args.adapter_id,
        )
        for row in rows
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in converted),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "input": str(input_path),
                "output": str(output_path),
                "count": len(converted),
                "benchmark": args.benchmark_name,
                "split": args.split,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
