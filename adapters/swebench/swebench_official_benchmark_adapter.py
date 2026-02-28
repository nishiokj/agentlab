#!/usr/bin/env python3
"""SWE-bench official adapter shim with aligned metadata extraction."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

try:  # Import as package when available.
    from ._swebench_meta import extract_swebench_meta as _extract_swebench_meta
except ImportError:  # pragma: no cover - supports direct script execution.
    from _swebench_meta import extract_swebench_meta as _extract_swebench_meta


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")


def _env_int(name: str, fallback: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return fallback
    try:
        return int(raw)
    except ValueError:
        return fallback


def _task_id(task_payload: Any) -> str:
    if isinstance(task_payload, dict):
        if isinstance(task_payload.get("id"), str) and task_payload["id"].strip():
            return task_payload["id"].strip()
        nested_task = task_payload.get("task")
        if isinstance(nested_task, dict):
            value = nested_task.get("id")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return "task_unknown"


def extract_swebench_meta(payload: Any) -> dict[str, str | None]:
    return _extract_swebench_meta(payload)


def _extract_benchmark_spec(task_payload: Any) -> dict[str, str]:
    default = {
        "adapter_id": "swebench_official",
        "name": "swebench",
        "split": "test",
    }
    if not isinstance(task_payload, dict):
        return default
    candidate = task_payload.get("benchmark")
    if isinstance(candidate, dict):
        adapter_id = candidate.get("adapter_id")
        name = candidate.get("name")
        split = candidate.get("split")
        if isinstance(adapter_id, str) and adapter_id.strip():
            default["adapter_id"] = adapter_id.strip()
        if isinstance(name, str) and name.strip():
            default["name"] = name.strip()
        if isinstance(split, str) and split.strip():
            default["split"] = split.strip()
    return default


def build_prediction_record(task_payload: Any, evaluation_output: Any) -> dict[str, Any]:
    patch = ""
    if isinstance(evaluation_output, dict):
        patch_value = evaluation_output.get("patch")
        if isinstance(patch_value, str):
            patch = patch_value
    return {
        "schema_version": "benchmark_prediction_record_v1",
        "ids": {
            "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
            "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
            "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
            "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
            "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
        },
        "benchmark": _extract_benchmark_spec(task_payload),
        "prediction": {"kind": "patch", "value": patch},
        "ext": {"swebench": extract_swebench_meta(task_payload)},
    }


def build_score_record(task_payload: Any, evaluation_output: Any) -> dict[str, Any]:
    verdict = "fail"
    if isinstance(evaluation_output, dict):
        raw_verdict = evaluation_output.get("verdict")
        if isinstance(raw_verdict, str) and raw_verdict in {"pass", "fail", "error"}:
            verdict = raw_verdict
    resolved = 1.0 if verdict == "pass" else 0.0
    return {
        "schema_version": "benchmark_score_record_v1",
        "ids": {
            "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
            "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
            "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
            "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
            "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
        },
        "benchmark": _extract_benchmark_spec(task_payload),
        "verdict": verdict,
        "primary_metric_name": "resolved",
        "primary_metric_value": resolved,
        "metrics": {"resolved": resolved},
        "evaluator": {"name": "swebench_official", "mode": "official"},
        "ext": {"swebench": extract_swebench_meta(task_payload)},
    }


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required env var: {name}")
    return value


def main() -> int:
    task_path = _required_env("AGENTLAB_TASK_PATH")
    result_path = _required_env("AGENTLAB_RESULT_PATH")
    prediction_path = _required_env("AGENTLAB_BENCHMARK_PREDICTION_PATH")
    score_path = _required_env("AGENTLAB_BENCHMARK_SCORE_PATH")

    task_payload = _read_json(task_path)
    evaluation_output = _read_json(result_path)

    _write_json(prediction_path, build_prediction_record(task_payload, evaluation_output))
    _write_json(score_path, build_score_record(task_payload, evaluation_output))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"swebench_official_benchmark_adapter.py error: {exc}", file=sys.stderr)
        raise SystemExit(1)
