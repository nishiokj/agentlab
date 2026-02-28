#!/usr/bin/env python3
"""Harbor benchmark adapter that writes AgentLab benchmark records."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

DEFAULT_ADAPTER_ID = "harbor_tb2"
DEFAULT_BENCHMARK_NAME = "terminal_bench_2"
DEFAULT_SPLIT = "test"


class HarborAdapterError(RuntimeError):
    def __init__(self, code: str, message: str, *, exit_code: int = 1) -> None:
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code


def _read_json(path: str | Path) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise HarborAdapterError("io.file_not_found", f"missing JSON file: {path}", exit_code=22) from exc
    except json.JSONDecodeError as exc:
        raise HarborAdapterError("io.invalid_json", f"invalid JSON at {path}: {exc}", exit_code=22) from exc


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")
    except Exception as exc:
        raise HarborAdapterError(
            "io.write_failed",
            f"unable to write JSON file: {path}: {exc}",
            exit_code=22,
        ) from exc


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise HarborAdapterError(
            "config.missing_env",
            f"missing required env var: {name}",
            exit_code=21,
        )
    return value


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
        "adapter_id": os.environ.get("HARBOR_ADAPTER_ID", DEFAULT_ADAPTER_ID),
        "name": os.environ.get("HARBOR_BENCHMARK_NAME", DEFAULT_BENCHMARK_NAME),
        "split": os.environ.get("HARBOR_BENCHMARK_SPLIT", DEFAULT_SPLIT),
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


def _extract_prediction(agent_result: Any) -> dict[str, Any]:
    if isinstance(agent_result, dict):
        output = agent_result.get("output")
        output_patch = output.get("patch") if isinstance(output, dict) else None
        patch = agent_result.get("patch") or agent_result.get("prediction") or output_patch
        if isinstance(patch, str) and patch.strip():
            return {"kind": "patch", "value": patch}
        if isinstance(output, dict):
            response = output.get("response") or output.get("text")
            if isinstance(response, str):
                return {"kind": "text", "value": response}
        if "output" in agent_result:
            return {"kind": "json", "value": agent_result["output"]}
    return {"kind": "text", "value": ""}


def _parse_evaluator_command() -> list[str] | None:
    cmd_json = os.environ.get("HARBOR_EVALUATOR_CMD_JSON")
    if cmd_json:
        try:
            parsed = json.loads(cmd_json)
        except json.JSONDecodeError as exc:
            raise HarborAdapterError(
                "config.invalid_evaluator_cmd_json",
                f"HARBOR_EVALUATOR_CMD_JSON is not valid JSON: {exc}",
                exit_code=21,
            ) from exc
        if not isinstance(parsed, list) or not parsed or not all(isinstance(v, str) and v for v in parsed):
            raise HarborAdapterError(
                "config.invalid_evaluator_cmd_json",
                "HARBOR_EVALUATOR_CMD_JSON must be a JSON array of non-empty strings",
                exit_code=21,
            )
        return parsed
    cmd_shell = os.environ.get("HARBOR_EVALUATOR_CMD")
    if cmd_shell:
        parsed = shlex.split(cmd_shell)
        if not parsed:
            raise HarborAdapterError(
                "config.invalid_evaluator_cmd",
                "HARBOR_EVALUATOR_CMD is empty",
                exit_code=21,
            )
        return parsed
    return None


def run_external_evaluator(task_payload: Any, agent_result: Any) -> dict[str, Any] | None:
    command = _parse_evaluator_command()
    if command is None:
        return None

    with tempfile.TemporaryDirectory(prefix="harbor_adapter_") as tmp:
        tmp_dir = Path(tmp)
        task_path = tmp_dir / "task.json"
        result_path = tmp_dir / "result.json"
        output_path = tmp_dir / "evaluation.json"
        task_path.write_text(json.dumps(task_payload), encoding="utf-8")
        result_path.write_text(json.dumps(agent_result), encoding="utf-8")

        env = os.environ.copy()
        env["HARBOR_TASK_PATH"] = str(task_path)
        env["HARBOR_AGENT_RESULT_PATH"] = str(result_path)
        env["HARBOR_EVALUATION_OUTPUT_PATH"] = str(output_path)

        proc = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
        if proc.returncode != 0:
            detail = proc.stderr.strip() or proc.stdout.strip() or "evaluator returned non-zero"
            raise HarborAdapterError(
                "evaluator.command_failed",
                f"Harbor evaluator command failed: {detail}",
                exit_code=23,
            )

        stdout = proc.stdout.strip()
        if stdout:
            try:
                parsed = json.loads(stdout)
            except json.JSONDecodeError as exc:
                raise HarborAdapterError(
                    "evaluator.invalid_json",
                    f"Harbor evaluator stdout is not valid JSON: {exc}",
                    exit_code=24,
                ) from exc
        elif output_path.exists():
            try:
                parsed = json.loads(output_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise HarborAdapterError(
                    "evaluator.invalid_json",
                    f"Harbor evaluator output file is not valid JSON: {exc}",
                    exit_code=24,
                ) from exc
        else:
            raise HarborAdapterError(
                "evaluator.missing_output",
                "Harbor evaluator command produced no JSON (stdout empty and HARBOR_EVALUATION_OUTPUT_PATH missing)",
                exit_code=24,
            )
        if not isinstance(parsed, dict):
            raise HarborAdapterError(
                "evaluator.invalid_payload",
                "Harbor evaluator JSON must be an object",
                exit_code=24,
            )
        return parsed


def _normalize_verdict(value: Any) -> str:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"pass", "fail", "error"}:
            return lowered
    return "error"


def _fallback_verdict(agent_result: Any) -> str:
    if isinstance(agent_result, dict):
        explicit = _normalize_verdict(agent_result.get("verdict"))
        if explicit != "error":
            return explicit
        outcome = agent_result.get("outcome")
        if isinstance(outcome, bool):
            return "pass" if outcome else "fail"
    return "pass" if _env_int("AGENTLAB_AGENT_EXIT_STATUS", 0) == 0 else "fail"


def _ids(task_payload: Any) -> dict[str, Any]:
    return {
        "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
        "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
        "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
        "task_id": os.environ.get("AGENTLAB_TASK_ID", _task_id(task_payload)),
        "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
    }


def build_prediction_record(
    task_payload: Any, agent_result: Any, evaluation: dict[str, Any] | None
) -> dict[str, Any]:
    prediction = None
    if isinstance(evaluation, dict):
        candidate = evaluation.get("prediction")
        if isinstance(candidate, dict):
            kind = candidate.get("kind")
            if isinstance(kind, str) and kind:
                prediction = {
                    "kind": kind,
                    "value": candidate.get("value"),
                }
    if prediction is None:
        prediction = _extract_prediction(agent_result)

    mode = "external" if evaluation is not None else "fallback"
    return {
        "schema_version": "benchmark_prediction_record_v1",
        "ids": _ids(task_payload),
        "benchmark": _extract_benchmark_spec(task_payload),
        "prediction": prediction,
        "ext": {
            "harbor": {
                "evaluation_mode": mode,
            }
        },
    }


def build_score_record(task_payload: Any, agent_result: Any, evaluation: dict[str, Any] | None) -> dict[str, Any]:
    verdict = _fallback_verdict(agent_result)
    primary_metric_name = "resolved"
    primary_metric_value = 1.0 if verdict == "pass" else 0.0
    metrics: dict[str, float] = {"resolved": primary_metric_value}
    evaluator = {"name": "harbor_adapter_fallback", "mode": "custom"}

    if isinstance(evaluation, dict):
        verdict = _normalize_verdict(evaluation.get("verdict"))
        name = evaluation.get("primary_metric_name")
        if isinstance(name, str) and name.strip():
            primary_metric_name = name.strip()
        value = evaluation.get("primary_metric_value")
        if isinstance(value, (float, int)):
            primary_metric_value = float(value)
        raw_metrics = evaluation.get("metrics")
        if isinstance(raw_metrics, dict):
            converted: dict[str, float] = {}
            for key, raw in raw_metrics.items():
                if isinstance(key, str) and isinstance(raw, (float, int)):
                    converted[key] = float(raw)
            if converted:
                metrics = converted
        raw_eval = evaluation.get("evaluator")
        if isinstance(raw_eval, dict):
            eval_name = raw_eval.get("name")
            eval_mode = raw_eval.get("mode")
            evaluator = {
                "name": eval_name.strip() if isinstance(eval_name, str) and eval_name.strip() else "harbor_evaluator",
                "mode": eval_mode.strip() if isinstance(eval_mode, str) and eval_mode.strip() else "custom",
            }

    if primary_metric_name not in metrics and isinstance(primary_metric_value, float):
        metrics[primary_metric_name] = primary_metric_value

    mode = "external" if evaluation is not None else "fallback"
    return {
        "schema_version": "benchmark_score_record_v1",
        "ids": _ids(task_payload),
        "benchmark": _extract_benchmark_spec(task_payload),
        "verdict": verdict,
        "primary_metric_name": primary_metric_name,
        "primary_metric_value": primary_metric_value,
        "metrics": metrics,
        "evaluator": evaluator,
        "ext": {
            "harbor": {
                "evaluation_mode": mode,
            }
        },
    }


def main() -> int:
    task_path = _required_env("AGENTLAB_TASK_PATH")
    result_path = _required_env("AGENTLAB_RESULT_PATH")
    prediction_path = _required_env("AGENTLAB_BENCHMARK_PREDICTION_PATH")
    score_path = _required_env("AGENTLAB_BENCHMARK_SCORE_PATH")

    task_payload = _read_json(task_path)
    agent_result = _read_json(result_path)
    evaluation = run_external_evaluator(task_payload, agent_result)

    prediction = build_prediction_record(task_payload, agent_result, evaluation)
    score = build_score_record(task_payload, agent_result, evaluation)

    _write_json(prediction_path, prediction)
    _write_json(score_path, score)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HarborAdapterError as exc:  # pragma: no cover
        print(
            f"harbor_benchmark_adapter.py error_code={exc.code} message={exc}",
            file=sys.stderr,
        )
        raise SystemExit(exc.exit_code)
    except Exception as exc:  # pragma: no cover
        print(
            f"harbor_benchmark_adapter.py error_code=internal.unhandled message={exc}",
            file=sys.stderr,
        )
        raise SystemExit(99)
