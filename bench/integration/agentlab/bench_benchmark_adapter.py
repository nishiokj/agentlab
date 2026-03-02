#!/usr/bin/env python3
"""Bench benchmark adapter for AgentLab benchmark protocol v1.

Consumes AGENTLAB task/result files and emits:
- AGENTLAB_BENCHMARK_PREDICTION_PATH
- AGENTLAB_BENCHMARK_SCORE_PATH
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
from bench.taskkit.grading import grade_patch_for_task

DEFAULT_ADAPTER_ID = "bench_v0"
DEFAULT_BENCHMARK_NAME = "bench"
DEFAULT_BENCHMARK_SPLIT = "test"


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


def _env_int(name: str, fallback: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return fallback
    try:
        return int(raw)
    except ValueError:
        return fallback


def _repo_root() -> Path:
    return REPO_ROOT


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


def _resolve_task_dir(task_payload: Any, task_id: str) -> Path:
    root = _repo_root()

    if isinstance(task_payload, dict):
        direct = task_payload.get("task_dir")
        if isinstance(direct, str) and direct.strip():
            direct_path = Path(direct.strip())
            if not direct_path.is_absolute():
                direct_path = root / direct_path
            return direct_path

        bench = task_payload.get("bench")
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


def _extract_patch_text(agent_result: Any) -> str | None:
    if not isinstance(agent_result, dict):
        return None

    # First, direct payload fields.
    for value in (
        agent_result.get("patch"),
        agent_result.get("prediction"),
    ):
        if isinstance(value, str) and value.strip():
            return value

    answer = agent_result.get("answer")
    if isinstance(answer, dict):
        candidate = answer.get("patch")
        if isinstance(candidate, str) and candidate.strip():
            return candidate
        value = answer.get("value")
        if isinstance(value, str) and value.strip().startswith("diff --git"):
            return value
    elif isinstance(answer, str) and answer.strip().startswith("diff --git"):
        return answer

    output = agent_result.get("output")
    if isinstance(output, dict):
        candidate = output.get("patch")
        if isinstance(candidate, str) and candidate.strip():
            return candidate

    # Then artifact file references if present.
    workspace = Path(os.environ.get("WORKSPACE", ".")).resolve()
    artifacts = agent_result.get("artifacts")
    if isinstance(artifacts, list):
        for item in artifacts:
            if not isinstance(item, dict):
                continue
            logical_name = item.get("logical_name")
            path_value = item.get("path")
            if not isinstance(path_value, str) or not path_value.strip():
                continue
            candidate = Path(path_value)
            if not candidate.is_absolute():
                candidate = workspace / candidate
            if not candidate.exists() or not candidate.is_file():
                continue
            lname = logical_name.strip().lower() if isinstance(logical_name, str) else ""
            if lname.endswith("patch") or candidate.suffix in {".patch", ".diff"}:
                text = candidate.read_text(encoding="utf-8", errors="replace")
                if text.strip():
                    return text

    return None


def _ids(task_payload: Any) -> dict[str, Any]:
    return {
        "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
        "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
        "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
        "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
        "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
    }


def _prediction_record(task_payload: Any, patch_text: str | None) -> dict[str, Any]:
    prediction: dict[str, Any]
    if patch_text is not None:
        prediction = {"kind": "patch", "value": patch_text}
    else:
        prediction = {"kind": "text", "value": ""}

    return {
        "schema_version": "benchmark_prediction_record_v1",
        "ids": _ids(task_payload),
        "benchmark": _extract_benchmark_spec(task_payload),
        "prediction": prediction,
        "ext": {
            "bench": {
                "has_patch": patch_text is not None,
            }
        },
    }


def _verdict_from_score(score: dict[str, Any] | None) -> str:
    if not isinstance(score, dict):
        return "error"
    if score.get("overall_pass") is True:
        return "pass"
    if score.get("failure_label") == "NO_PATCH":
        return "missing"
    return "fail"


def _score_record(
    task_payload: Any,
    score: dict[str, Any] | None,
    error_message: str | None,
) -> dict[str, Any]:
    verdict = _verdict_from_score(score)
    resolved = 1.0 if verdict == "pass" else 0.0

    metrics: dict[str, Any] = {
        "resolved": resolved,
    }
    if isinstance(score, dict):
        metrics["public_pass"] = bool(score.get("public_pass", False))
        metrics["hidden_pass"] = bool(score.get("hidden_pass", False))
        metrics["policy_pass"] = bool(score.get("policy_pass", False))
        failure_label = score.get("failure_label")
        if isinstance(failure_label, str):
            metrics["failure_label"] = failure_label

        raw_metrics = score.get("metrics")
        if isinstance(raw_metrics, dict):
            hidden_total = raw_metrics.get("hidden_cases_total")
            hidden_passed = raw_metrics.get("hidden_cases_passed")
            if isinstance(hidden_total, int):
                metrics["hidden_cases_total"] = float(hidden_total)
            if isinstance(hidden_passed, int):
                metrics["hidden_cases_passed"] = float(hidden_passed)

    payload: dict[str, Any] = {
        "schema_version": "benchmark_score_record_v1",
        "ids": _ids(task_payload),
        "benchmark": _extract_benchmark_spec(task_payload),
        "verdict": verdict,
        "primary_metric_name": "resolved",
        "primary_metric_value": resolved,
        "metrics": metrics,
        "evaluator": {"name": "bench_grader", "mode": "custom"},
        "ext": {
            "bench": {
                "failure_label": score.get("failure_label") if isinstance(score, dict) else None,
                "overall_pass": score.get("overall_pass") if isinstance(score, dict) else None,
            }
        },
    }

    if error_message:
        payload["error"] = {
            "error_type": "BENCH_GRADER_ERROR",
            "message": error_message,
        }

    return payload


def _grade_with_bench(task_payload: Any, patch_text: str | None) -> tuple[dict[str, Any] | None, str | None]:
    root = _repo_root()
    cfg = BenchConfig.from_root(root)
    task_id = _task_id(task_payload)
    task_dir = _resolve_task_dir(task_payload, task_id)

    if not task_dir.exists():
        return None, f"task directory not found: {task_dir}"

    try:
        score = grade_patch_for_task(task_dir=task_dir, patch_text=patch_text, config=cfg)
        return score, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def main() -> int:
    task_path = _required_env("AGENTLAB_TASK_PATH")
    result_path = _required_env("AGENTLAB_RESULT_PATH")
    prediction_path = _required_env("AGENTLAB_BENCHMARK_PREDICTION_PATH")
    score_path = _required_env("AGENTLAB_BENCHMARK_SCORE_PATH")

    task_payload = _read_json(task_path)
    agent_result = _read_json(result_path)
    patch_text = _extract_patch_text(agent_result)

    score, grade_error = _grade_with_bench(task_payload, patch_text)

    prediction = _prediction_record(task_payload, patch_text)
    score_record = _score_record(task_payload, score, grade_error)

    _write_json(prediction_path, prediction)
    _write_json(score_path, score_record)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"bench_benchmark_adapter.py error: {exc}", file=sys.stderr)
        raise SystemExit(1)
