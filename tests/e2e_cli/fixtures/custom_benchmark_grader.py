#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


DEFAULT_MAPPED_OUTPUT_PATH = "/agentlab/out/mapped_grader_output.json"
VALID_GRADING_STRATEGIES = {"in_task_image", "injected", "separate"}


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


def _load_grader_input() -> dict[str, Any]:
    payload = _read_json(_required_env("AGENTLAB_GRADER_INPUT_PATH"))
    if isinstance(payload, dict):
        return payload
    raise RuntimeError("grader input must be a JSON object")


def _task_payload(grader_input: dict[str, Any]) -> dict[str, Any]:
    payload = grader_input.get("task")
    if isinstance(payload, dict):
        return payload
    return {}


def _candidate_artifact(grader_input: dict[str, Any]) -> dict[str, Any]:
    payload = grader_input.get("candidate_artifact")
    if isinstance(payload, dict):
        return payload
    return {}


def _candidate_payload(grader_input: dict[str, Any]) -> dict[str, Any]:
    candidate = _candidate_artifact(grader_input)
    if candidate.get("state") != "valid":
        return {}
    payload = candidate.get("payload")
    if isinstance(payload, dict):
        return payload
    return {}


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


def _prediction(artifact_payload: Any) -> dict[str, Any]:
    if isinstance(artifact_payload, dict):
        prediction = artifact_payload.get("prediction")
        if isinstance(prediction, str) and prediction.strip():
            return {"kind": "text", "value": prediction}
        patch = artifact_payload.get("patch")
        if isinstance(patch, str) and patch.strip():
            return {"kind": "patch", "value": patch}
        output = artifact_payload.get("output")
        if isinstance(output, dict):
            nested_patch = output.get("patch")
            if isinstance(nested_patch, str) and nested_patch.strip():
                return {"kind": "patch", "value": nested_patch}
    return {"kind": "text", "value": ""}


def _objective_value(artifact_payload: Any) -> float:
    if not isinstance(artifact_payload, dict):
        return 0.0
    direct = artifact_payload.get("objective_value")
    if isinstance(direct, bool):
        return float(direct)
    if isinstance(direct, (int, float)):
        return float(direct)
    objective = artifact_payload.get("objective")
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


def _reported_outcome(verdict: str) -> str:
    return {
        "pass": "success",
        "fail": "failure",
        "missing": "missing",
        "error": "error",
    }.get(verdict, "error")


def _grader_strategy() -> str:
    for env_name in ("AGENTLAB_GRADING_STRATEGY", "AGENTLAB_GRADER_STRATEGY"):
        raw = os.environ.get(env_name, "").strip()
        if raw in VALID_GRADING_STRATEGIES:
            return raw
    return "in_task_image"


def _mapped_output_path() -> str:
    raw = os.environ.get("AGENTLAB_MAPPED_GRADER_OUTPUT_PATH", "").strip()
    if raw:
        return raw
    return DEFAULT_MAPPED_OUTPUT_PATH


def main() -> int:
    grader_input = _load_grader_input()
    task_payload = _task_payload(grader_input)
    candidate = _candidate_artifact(grader_input)
    artifact_payload = _candidate_payload(grader_input)
    benchmark = _benchmark_spec(task_payload)

    objective_value = _objective_value(artifact_payload)
    if candidate.get("state") == "missing":
        verdict = "missing"
    elif candidate.get("state") == "invalid":
        verdict = "error"
    else:
        verdict = "pass" if objective_value > 0.0 else "fail"
    resolved = 1.0 if verdict == "pass" else 0.0

    payload = {
        "benchmark": benchmark,
        "ids": grader_input.get("ids", {}),
        "task_id": _task_id(task_payload),
        "verdict": verdict,
        "resolved": resolved,
        "objective_value": objective_value,
        "prediction": _prediction(artifact_payload),
        "candidate_artifact_state": candidate.get("state", "missing"),
    }

    conclusion = {
        "schema_version": "trial_conclusion_v1",
        "payload": payload,
        "reported_outcome": _reported_outcome(verdict),
        "primary_metric": {
            "name": "resolved",
            "value": resolved,
        },
        "grader": {
            "name": "custom_benchmark_grader",
            "strategy": _grader_strategy(),
            "version": "v1",
        },
    }
    _write_json(_mapped_output_path(), conclusion)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"custom_benchmark_grader.py error: {exc}", file=sys.stderr)
        raise SystemExit(1)
