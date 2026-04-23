#!/usr/bin/env python3
"""SWE-bench in-task-image grader mapper.

Reads the AgentLab grader input, inspects the candidate artifact and agent
phase to determine a verdict, then writes a trial_conclusion_v1 JSON.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# SWE-bench metadata extraction (inlined from _swebench_meta.py)
# ---------------------------------------------------------------------------

_SENTINEL = object()


def _read_path(payload: Any, path: tuple[str, ...]) -> Any:
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return _SENTINEL
        current = current.get(key, _SENTINEL)
        if current is _SENTINEL:
            return _SENTINEL
    return current


def _coerce_str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _contextual_payloads(payload: Any) -> list[Any]:
    contexts: list[Any] = [payload]
    if isinstance(payload, dict):
        task_value = payload.get("task")
        if isinstance(task_value, dict):
            contexts.append(task_value)
    return contexts


def _first_string(payload: Any, candidates: list[tuple[str, ...]]) -> str | None:
    for context in _contextual_payloads(payload):
        for candidate in candidates:
            value = _read_path(context, candidate)
            if value is not _SENTINEL:
                coerced = _coerce_str(value)
                if coerced is not None:
                    return coerced
    return None


def extract_swebench_meta(payload: Any) -> dict[str, str | None]:
    base_paths: dict[str, list[tuple[str, ...]]] = {
        "repo": [
            ("task", "swebench", "input", "repo"),
            ("swebench", "input", "repo"),
        ],
        "base_commit": [
            ("task", "swebench", "input", "base_commit"),
            ("swebench", "input", "base_commit"),
        ],
        "instance_id": [
            ("task", "swebench", "input", "instance_id"),
            ("swebench", "input", "instance_id"),
            ("task", "input", "instance_id"),
            ("input", "instance_id"),
        ],
        "problem_statement": [
            ("task", "swebench", "input", "problem_statement"),
            ("swebench", "input", "problem_statement"),
            ("task", "input", "problem_statement"),
            ("input", "problem_statement"),
        ],
    }
    return {key: _first_string(payload, paths) for key, paths in base_paths.items()}


# ---------------------------------------------------------------------------
# Grader logic
# ---------------------------------------------------------------------------

VALID_STRATEGIES = {"in_task_image", "injected", "separate"}


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
        tid = task_payload.get("id")
        if isinstance(tid, str) and tid.strip():
            return tid.strip()
        nested = task_payload.get("task")
        if isinstance(nested, dict):
            tid = nested.get("id")
            if isinstance(tid, str) and tid.strip():
                return tid.strip()
    return "task_unknown"


def _extract_prediction(candidate: dict[str, Any]) -> dict[str, Any]:
    if candidate.get("state") != "valid":
        return {"kind": "text", "value": ""}
    payload = candidate.get("payload")
    if isinstance(payload, dict):
        patch = payload.get("patch")
        if isinstance(patch, str) and patch.strip():
            return {"kind": "patch", "value": patch}
        value = payload.get("value")
        if isinstance(value, str) and value.strip():
            return {"kind": "text", "value": value}
    if isinstance(payload, str) and payload.strip():
        return {"kind": "text", "value": payload}
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
        for key in ("adapter_id", "name", "split"):
            val = candidate.get(key)
            if isinstance(val, str) and val.strip():
                default[key] = val.strip()
    return default


def _grader_strategy() -> str:
    for env_name in ("AGENTLAB_GRADING_STRATEGY", "AGENTLAB_GRADER_STRATEGY"):
        raw = os.environ.get(env_name, "").strip()
        if raw in VALID_STRATEGIES:
            return raw
    return "in_task_image"


def _reported_outcome(verdict: str) -> str:
    return {
        "pass": "success",
        "fail": "failure",
        "missing": "missing",
        "error": "error",
    }.get(verdict, "error")


def main() -> int:
    grader_input_path = os.environ.get("AGENTLAB_GRADER_INPUT_PATH")
    if not grader_input_path:
        print("error: AGENTLAB_GRADER_INPUT_PATH not set", file=sys.stderr)
        return 1

    mapped_output_path = os.environ.get(
        "AGENTLAB_MAPPED_GRADER_OUTPUT_PATH",
        "/agentlab/out/mapped_grader_output.json",
    )

    grader_input = _read_json(grader_input_path)
    if not isinstance(grader_input, dict):
        print("error: grader input must be a JSON object", file=sys.stderr)
        return 1

    task_payload = grader_input.get("task", {})
    if not isinstance(task_payload, dict):
        task_payload = {}

    candidate = grader_input.get("candidate_artifact", {})
    if not isinstance(candidate, dict):
        candidate = {}

    agent_phase = grader_input.get("agent_phase")
    exit_code = None
    if isinstance(agent_phase, dict):
        exit_code = agent_phase.get("exit_code")

    # Determine verdict from candidate state and exit code.
    if candidate.get("state") == "missing":
        verdict = "missing"
    elif candidate.get("state") == "invalid":
        verdict = "error"
    elif exit_code == 0 or exit_code is None:
        verdict = "pass"
    else:
        verdict = "fail"

    resolved = 1.0 if verdict == "pass" else 0.0

    conclusion = {
        "schema_version": "trial_conclusion_v1",
        "payload": {
            "benchmark": _extract_benchmark_spec(task_payload),
            "ids": grader_input.get("ids", {}),
            "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
            "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
            "verdict": verdict,
            "resolved": resolved,
            "prediction": _extract_prediction(candidate),
            "candidate_artifact_state": candidate.get("state", "missing"),
            "swebench": extract_swebench_meta(task_payload),
        },
        "reported_outcome": _reported_outcome(verdict),
        "primary_metric": {
            "name": "resolved",
            "value": resolved,
        },
        "grader": {
            "name": "swebench_task_container_grader",
            "strategy": _grader_strategy(),
            "version": "v1",
        },
    }

    _write_json(mapped_output_path, conclusion)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"grader.py error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
