#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required env var: {name}")
    return value


def _env_int(name: str, fallback: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return fallback
    try:
        return int(raw)
    except ValueError:
        return fallback


def _identity_fields() -> dict[str, Any]:
    slot_commit_id = os.environ.get("AGENTLAB_SLOT_COMMIT_ID", "").strip() or "slot_pending"
    return {
        "schedule_idx": _env_int("AGENTLAB_SCHEDULE_IDX", 0),
        "slot_commit_id": slot_commit_id,
        "attempt": max(_env_int("AGENTLAB_ATTEMPT", 1), 1),
        "row_seq": max(_env_int("AGENTLAB_ROW_SEQ", 0), 0),
    }


def _task_id(task_payload: Any) -> str:
    if isinstance(task_payload, dict):
        task_id = task_payload.get("id")
        if isinstance(task_id, str) and task_id.strip():
            return task_id.strip()
        nested = task_payload.get("task")
        if isinstance(nested, dict):
            task_id = nested.get("id")
            if isinstance(task_id, str) and task_id.strip():
                return task_id.strip()
    return "task_unknown"


def _benchmark_spec(task_payload: Any) -> dict[str, str]:
    default = {
        "adapter_id": "custom_benchmark_grader",
        "name": "custom_e2e_benchmark",
        "split": "test",
    }
    if not isinstance(task_payload, dict):
        return default
    candidate = task_payload.get("benchmark")
    if isinstance(candidate, dict):
        for key in ("adapter_id", "name", "split"):
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                default[key] = value.strip()
    return default


def _prediction(result_payload: Any) -> dict[str, Any]:
    if isinstance(result_payload, dict):
        prediction = result_payload.get("prediction")
        if isinstance(prediction, str) and prediction.strip():
            return {"kind": "text", "value": prediction}
        output = result_payload.get("output")
        if isinstance(output, dict):
            patch = output.get("patch")
            if isinstance(patch, str) and patch.strip():
                return {"kind": "patch", "value": patch}
    return {"kind": "text", "value": ""}


def _objective_value(result_payload: Any) -> float:
    if not isinstance(result_payload, dict):
        return 0.0
    objective = result_payload.get("objective")
    if not isinstance(objective, dict):
        return 0.0
    value = objective.get("value")
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return 0.0
    return 0.0


def main() -> int:
    task_path = _required_env("AGENTLAB_TASK_PATH")
    result_path = _required_env("AGENTLAB_RESULT_PATH")
    prediction_path = _required_env("AGENTLAB_BENCHMARK_PREDICTION_PATH")
    score_path = _required_env("AGENTLAB_BENCHMARK_SCORE_PATH")

    task_payload = _read_json(task_path)
    result_payload = _read_json(result_path)
    benchmark = _benchmark_spec(task_payload)
    objective_value = _objective_value(result_payload)
    resolved = 1.0 if objective_value > 0.0 else 0.0
    verdict = "pass" if resolved == 1.0 else "fail"

    ids = {
        "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
        "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
        "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
        "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
        "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
    }
    prediction = {
        "schema_version": "benchmark_prediction_record_v1",
        **_identity_fields(),
        "ids": ids,
        "benchmark": benchmark,
        "prediction": _prediction(result_payload),
    }
    score = {
        "schema_version": "benchmark_score_record_v1",
        **_identity_fields(),
        "ids": ids,
        "benchmark": benchmark,
        "verdict": verdict,
        "primary_metric_name": "resolved",
        "primary_metric_value": resolved,
        "metrics": {"resolved": resolved},
        "evaluator": {
            "name": "custom_benchmark_grader",
            "mode": "custom",
        },
    }
    _write_json(prediction_path, prediction)
    _write_json(score_path, score)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"custom_benchmark_grader.py error: {exc}", file=sys.stderr)
        raise SystemExit(1)
