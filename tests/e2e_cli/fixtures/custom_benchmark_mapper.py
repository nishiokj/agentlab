#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


DEFAULT_RAW_OUTPUT_PATH = "/agentlab/out/raw_grader_output.json"
DEFAULT_MAPPED_OUTPUT_PATH = "/agentlab/out/mapped_grader_output.json"


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")


def _path_from_env(name: str, default: str) -> str:
    raw = os.environ.get(name, "").strip()
    if raw:
        return raw
    return default


def _int_value(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return default
    return default


def main() -> int:
    raw = _read_json(_path_from_env("AGENTLAB_RAW_GRADER_OUTPUT_PATH", DEFAULT_RAW_OUTPUT_PATH))
    forced_exit = _int_value(raw.get("mapper_exit_code"), 0)
    if forced_exit != 0:
        print(f"custom_benchmark_mapper.py forced exit {forced_exit}", file=sys.stderr)
        return forced_exit

    resolved = raw.get("resolved", 0.0)
    verdict = raw.get("verdict", "error")
    conclusion = {
        "schema_version": "trial_conclusion_v1",
        "payload": raw,
        "reported_outcome": {
            "pass": "success",
            "fail": "failure",
            "missing": "missing",
            "error": "error",
        }.get(verdict, "error"),
        "primary_metric": {
            "name": "resolved",
            "value": resolved,
        },
        "grader": {
            "name": "custom_benchmark_mapper",
            "strategy": os.environ.get("AGENTLAB_GRADING_STRATEGY", "in_task_image"),
            "version": "v1",
        },
    }
    _write_json(
        _path_from_env("AGENTLAB_MAPPED_GRADER_OUTPUT_PATH", DEFAULT_MAPPED_OUTPUT_PATH),
        conclusion,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"custom_benchmark_mapper.py error: {exc}", file=sys.stderr)
        raise SystemExit(1)
