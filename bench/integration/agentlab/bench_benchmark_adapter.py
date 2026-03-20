#!/usr/bin/env python3
"""Bench benchmark grader for the AgentLab cutover contract.

Consumes ``grader_input_v1`` and emits ``trial_conclusion_v1`` to the mapped
grader output path.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bench.config import BenchConfig
from bench.taskkit.grading import grade_patch_for_task, grade_patch_for_task_data

DEFAULT_ADAPTER_ID = "bench_v0"
DEFAULT_BENCHMARK_NAME = "bench"
DEFAULT_BENCHMARK_SPLIT = "test"
DEFAULT_MAPPED_OUTPUT_PATH = "/agentlab/out/mapped_grader_output.json"
VALID_GRADING_STRATEGIES = {"in_task_image", "injected", "separate"}


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required env var: {name}")
    return value


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")


def _load_grader_input() -> dict[str, Any]:
    payload = _read_json(_required_env("AGENTLAB_GRADER_INPUT_PATH"))
    if isinstance(payload, dict):
        return payload
    raise RuntimeError("grader input must be a JSON object")


def _mapped_output_path() -> str:
    raw = os.environ.get("AGENTLAB_MAPPED_GRADER_OUTPUT_PATH", "").strip()
    if raw:
        return raw
    return DEFAULT_MAPPED_OUTPUT_PATH


def _grader_strategy() -> str:
    for env_name in ("AGENTLAB_GRADING_STRATEGY", "AGENTLAB_GRADER_STRATEGY"):
        raw = os.environ.get(env_name, "").strip()
        if raw in VALID_GRADING_STRATEGIES:
            return raw
    return "in_task_image"


def _repo_root() -> Path:
    return REPO_ROOT


def _task_payload(grader_input: dict[str, Any]) -> dict[str, Any]:
    value = grader_input.get("task")
    if isinstance(value, dict):
        return value
    return {}


def _task_id(task_payload: Any) -> str:
    if isinstance(task_payload, dict):
        candidate = task_payload.get("id")
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
        nested_task = task_payload.get("task")
        if isinstance(nested_task, dict):
            nested_id = nested_task.get("id")
            if isinstance(nested_id, str) and nested_id.strip():
                return nested_id.strip()
    return "task_unknown"


def _extract_benchmark_spec(task_payload: Any) -> dict[str, str]:
    default = {
        "adapter_id": os.environ.get("BENCH_ADAPTER_ID", DEFAULT_ADAPTER_ID),
        "name": os.environ.get("BENCH_BENCHMARK_NAME", DEFAULT_BENCHMARK_NAME),
        "split": os.environ.get("BENCH_BENCHMARK_SPLIT", DEFAULT_BENCHMARK_SPLIT),
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


def _task_record(task_payload: Any) -> dict[str, Any] | None:
    if not isinstance(task_payload, dict):
        return None
    nested = task_payload.get("task")
    if isinstance(nested, dict):
        return nested
    return task_payload


def _resolve_task_dir(task_payload: Any, task_id: str) -> Path:
    root = _repo_root()
    record = _task_record(task_payload)

    if isinstance(record, dict):
        direct = record.get("task_dir")
        if isinstance(direct, str) and direct.strip():
            direct_path = Path(direct.strip())
            if not direct_path.is_absolute():
                direct_path = root / direct_path
            return direct_path

        bench = record.get("bench")
        if isinstance(bench, dict):
            nested = bench.get("task_dir")
            if isinstance(nested, str) and nested.strip():
                nested_path = Path(nested.strip())
                if not nested_path.is_absolute():
                    nested_path = root / nested_path
                return nested_path
            suite = bench.get("suite")
            if isinstance(suite, str) and suite.strip():
                return root / "bench" / "benchmark" / "tasks" / suite.strip() / task_id

    return root / "bench" / "benchmark" / "tasks" / "v0" / task_id


def _task_data_from_payload(task_payload: Any) -> dict[str, Any] | None:
    record = _task_record(task_payload)
    if not isinstance(record, dict):
        return None

    repo_id = record.get("repo_id")
    baseline_injection_patch = record.get("baseline_injection_patch")
    if not isinstance(repo_id, str) or not repo_id.strip():
        return None
    if not isinstance(baseline_injection_patch, str) or not baseline_injection_patch.strip():
        return None

    task_data: dict[str, Any] = {
        "task_id": _task_id(task_payload),
        "repo_id": repo_id.strip(),
        "baseline_injection_patch": baseline_injection_patch.strip(),
    }

    public_command = record.get("public_command")
    if isinstance(public_command, str) and public_command.strip():
        task_data["public_command"] = public_command.strip()

    hidden_command = record.get("hidden_command")
    if isinstance(hidden_command, str) and hidden_command.strip():
        task_data["hidden_command"] = hidden_command.strip()

    time_limits = record.get("time_limits")
    if isinstance(time_limits, dict):
        task_data["time_limits"] = time_limits

    patch_policy = record.get("patch_policy")
    if isinstance(patch_policy, dict):
        task_data["patch_policy"] = patch_policy

    determinism_env = record.get("determinism_env")
    if isinstance(determinism_env, dict):
        task_data["determinism_env"] = determinism_env

    return task_data


def _extract_patch_from_value(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    if not isinstance(value, dict):
        return None

    for direct in (value.get("patch"), value.get("prediction")):
        if isinstance(direct, str) and direct.strip():
            return direct

    answer = value.get("answer")
    if isinstance(answer, dict):
        candidate = answer.get("patch")
        if isinstance(candidate, str) and candidate.strip():
            return candidate
        nested = answer.get("value")
        if isinstance(nested, str) and nested.strip().startswith("diff --git"):
            return nested
    elif isinstance(answer, str) and answer.strip().startswith("diff --git"):
        return answer

    output = value.get("output")
    if isinstance(output, dict):
        candidate = output.get("patch")
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    return None


def _candidate_artifact(grader_input: dict[str, Any]) -> dict[str, Any]:
    payload = grader_input.get("candidate_artifact")
    if isinstance(payload, dict):
        return payload
    return {}


def _patch_from_candidate(grader_input: dict[str, Any]) -> str | None:
    candidate = _candidate_artifact(grader_input)
    if candidate.get("state") != "valid":
        return None

    artifact_type = candidate.get("artifact_type")
    payload = candidate.get("payload")
    if artifact_type == "patch_submission" and isinstance(payload, dict):
        patch = payload.get("patch")
        if isinstance(patch, str) and patch.strip():
            return patch
        value = payload.get("value")
        if isinstance(value, str) and value.strip():
            return value
    if artifact_type == "text_response" and isinstance(payload, str) and payload.strip():
        return payload
    return _extract_patch_from_value(payload)


def _patch_from_workspace_delta(grader_input: dict[str, Any]) -> str | None:
    workspace_delta = grader_input.get("workspace_delta")
    if not isinstance(workspace_delta, dict):
        return None
    if workspace_delta.get("state") != "available":
        return None
    patch_path = workspace_delta.get("patch_path")
    if not isinstance(patch_path, str) or not patch_path.strip():
        return None
    candidate = Path(patch_path)
    if not candidate.exists() or not candidate.is_file():
        return None
    text = candidate.read_text(encoding="utf-8", errors="replace")
    return text if text.strip() else None


def _verdict_from_score(score: dict[str, Any] | None) -> str:
    if not isinstance(score, dict):
        return "error"
    if score.get("overall_pass") is True:
        return "pass"
    if score.get("failure_label") == "NO_PATCH":
        return "missing"
    return "fail"


def _reported_outcome(verdict: str) -> str:
    return {
        "pass": "success",
        "fail": "failure",
        "missing": "missing",
        "error": "error",
    }.get(verdict, "error")


def build_trial_conclusion(
    task_payload: Any,
    *,
    patch_text: str | None,
    score: dict[str, Any] | None,
    error_message: str | None,
) -> dict[str, Any]:
    verdict = _verdict_from_score(score)
    resolved = 1.0 if verdict == "pass" else 0.0

    payload: dict[str, Any] = {
        "benchmark": _extract_benchmark_spec(task_payload),
        "verdict": verdict,
        "resolved": resolved,
        "has_patch": patch_text is not None,
    }
    if isinstance(score, dict):
        payload["public_pass"] = bool(score.get("public_pass", False))
        payload["hidden_pass"] = bool(score.get("hidden_pass", False))
        payload["policy_pass"] = bool(score.get("policy_pass", False))
        failure_label = score.get("failure_label")
        if isinstance(failure_label, str):
            payload["failure_label"] = failure_label
        raw_metrics = score.get("metrics")
        if isinstance(raw_metrics, dict):
            payload["metrics"] = raw_metrics
    if error_message:
        payload["grader_error"] = error_message

    return {
        "schema_version": "trial_conclusion_v1",
        "payload": payload,
        "reported_outcome": _reported_outcome(verdict),
        "primary_metric": {
            "name": "resolved",
            "value": resolved,
        },
        "grader": {
            "name": "bench_grader",
            "strategy": _grader_strategy(),
            "version": "v1",
        },
    }


def _grade_with_bench(task_payload: Any, patch_text: str | None) -> tuple[dict[str, Any] | None, str | None]:
    root = _repo_root()
    cfg = BenchConfig.from_root(root)
    task_id = _task_id(task_payload)
    task_dir = _resolve_task_dir(task_payload, task_id)

    if not task_dir.exists():
        return None, f"task directory not found: {task_dir}"

    try:
        task_data = _task_data_from_payload(task_payload)
        if task_data is not None:
            score = grade_patch_for_task_data(
                task_dir=task_dir,
                task_data=task_data,
                patch_text=patch_text,
                config=cfg,
            )
        else:
            score = grade_patch_for_task(task_dir=task_dir, patch_text=patch_text, config=cfg)
        return score, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def main() -> int:
    grader_input = _load_grader_input()
    task_payload = _task_payload(grader_input)
    patch_text = _patch_from_candidate(grader_input) or _patch_from_workspace_delta(grader_input)
    score, grade_error = _grade_with_bench(task_payload, patch_text)
    conclusion = build_trial_conclusion(
        task_payload,
        patch_text=patch_text,
        score=score,
        error_message=grade_error,
    )
    _write_json(_mapped_output_path(), conclusion)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"bench_benchmark_adapter.py error: {exc}", file=sys.stderr)
        raise SystemExit(1)
