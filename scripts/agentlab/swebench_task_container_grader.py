#!/usr/bin/env python3
"""In-container grader that writes benchmark prediction and score records."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

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


def _extract_prediction(agent_result: Any) -> dict[str, Any]:
    if isinstance(agent_result, dict):
        output_patch = None
        output_value = agent_result.get("output")
        if isinstance(output_value, dict):
            output_patch = output_value.get("patch")
        patch = agent_result.get("patch") or agent_result.get("prediction") or output_patch
        if isinstance(patch, str) and patch.strip():
            return {"kind": "patch", "value": patch}
    return {"kind": "text", "value": ""}


def _extract_benchmark_spec(task_payload: Any) -> dict[str, str]:
    default = {
        "adapter_id": "swebench_task_container_grader",
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


def extract_swebench_meta(payload: Any) -> dict[str, str | None]:
    """Stable public helper for tests and downstream scripts."""

    return _extract_swebench_meta(payload)


def build_prediction_record(task_payload: Any, agent_result: Any) -> dict[str, Any]:
    swebench_meta = extract_swebench_meta(task_payload)
    benchmark = _extract_benchmark_spec(task_payload)
    return {
        "schema_version": "benchmark_prediction_record_v1",
        "ids": {
            "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
            "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
            "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
            "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
            "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
        },
        "benchmark": benchmark,
        "prediction": _extract_prediction(agent_result),
        "ext": {"swebench": swebench_meta},
    }


def build_score_record(task_payload: Any, agent_result: Any) -> dict[str, Any]:
    del agent_result  # score is derived from agent exit status here.
    benchmark = _extract_benchmark_spec(task_payload)
    agent_exit = _env_int("AGENTLAB_AGENT_EXIT_STATUS", 0)
    resolved = 1.0 if agent_exit == 0 else 0.0
    verdict = "pass" if resolved == 1.0 else "fail"
    return {
        "schema_version": "benchmark_score_record_v1",
        "ids": {
            "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
            "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
            "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
            "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
            "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
        },
        "benchmark": benchmark,
        "verdict": verdict,
        "primary_metric_name": "resolved",
        "primary_metric_value": resolved,
        "metrics": {"resolved": resolved},
        "evaluator": {"name": "task_container_grader", "mode": "custom"},
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
    agent_result = _read_json(result_path)

    prediction = build_prediction_record(task_payload, agent_result)
    score = build_score_record(task_payload, agent_result)

    _write_json(prediction_path, prediction)
    _write_json(score_path, score)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"swebench_task_container_grader.py error: {exc}", file=sys.stderr)
        raise SystemExit(1)
