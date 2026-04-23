#!/usr/bin/env python3
"""SWE-bench conclusion mapper/direct grader shim for the cutover contract."""

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


DEFAULT_MAPPED_OUTPUT_PATH = "/agentlab/out/mapped_grader_output.json"
VALID_GRADING_STRATEGIES = {"in_task_image", "injected", "separate"}


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


def _load_evaluation_output(grader_input: dict[str, Any]) -> Any:
    raw_path = os.environ.get("AGENTLAB_RAW_GRADER_OUTPUT_PATH", "").strip()
    if raw_path:
        candidate = Path(raw_path)
        if candidate.exists() and candidate.is_file():
            return _read_json(candidate)
    candidate_artifact = _candidate_artifact(grader_input)
    if candidate_artifact.get("state") == "valid":
        return candidate_artifact.get("payload")
    return {}


def _extract_prediction(evaluation_output: Any) -> dict[str, Any]:
    if isinstance(evaluation_output, dict):
        patch_value = evaluation_output.get("patch")
        if isinstance(patch_value, str) and patch_value.strip():
            return {"kind": "patch", "value": patch_value}
        prediction = evaluation_output.get("prediction")
        if isinstance(prediction, dict):
            kind = prediction.get("kind")
            if isinstance(kind, str) and kind.strip():
                return {
                    "kind": kind.strip(),
                    "value": prediction.get("value"),
                }
    return {"kind": "text", "value": ""}


def _normalize_verdict(evaluation_output: Any, candidate_state: str) -> str:
    if isinstance(evaluation_output, dict):
        raw_verdict = evaluation_output.get("verdict")
        if isinstance(raw_verdict, str) and raw_verdict in {"pass", "fail", "error", "missing"}:
            return raw_verdict
    if candidate_state == "missing":
        return "missing"
    if candidate_state == "invalid":
        return "error"
    return "fail"


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


def build_trial_conclusion(task_payload: Any, grader_input: dict[str, Any], evaluation_output: Any) -> dict[str, Any]:
    candidate = _candidate_artifact(grader_input)
    verdict = _normalize_verdict(evaluation_output, str(candidate.get("state", "missing")))
    resolved = 1.0 if verdict == "pass" else 0.0

    payload = {
        "benchmark": _extract_benchmark_spec(task_payload),
        "ids": grader_input.get("ids", {}),
        "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
        "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
        "verdict": verdict,
        "resolved": resolved,
        "prediction": _extract_prediction(evaluation_output),
        "candidate_artifact_state": candidate.get("state", "missing"),
        "swebench": extract_swebench_meta(task_payload),
    }
    if isinstance(evaluation_output, dict):
        payload["evaluation_output"] = evaluation_output

    return {
        "schema_version": "trial_conclusion_v1",
        "payload": payload,
        "reported_outcome": _reported_outcome(verdict),
        "primary_metric": {
            "name": "resolved",
            "value": resolved,
        },
        "grader": {
            "name": "swebench",
            "strategy": _grader_strategy(),
            "version": "v1",
        },
    }


def main() -> int:
    grader_input = _load_grader_input()
    task_payload = _task_payload(grader_input)
    evaluation_output = _load_evaluation_output(grader_input)
    conclusion = build_trial_conclusion(task_payload, grader_input, evaluation_output)
    _write_json(_mapped_output_path(), conclusion)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"swebench_official_benchmark_adapter.py error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
