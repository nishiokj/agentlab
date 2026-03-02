#!/usr/bin/env python3
"""Phase 3 Harbor compatibility probe for adapter + evaluator contract."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


ERROR_CODE_RE = re.compile(r"error_code=([A-Za-z0-9._-]+)")
DEFAULT_ADAPTER_SCRIPT = str((Path(__file__).resolve().parent / "harbor_benchmark_adapter.py"))


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--python-bin",
        default=sys.executable,
        help="Python interpreter to execute adapter with.",
    )
    parser.add_argument(
        "--adapter-script",
        default=DEFAULT_ADAPTER_SCRIPT,
        help="Path to Harbor adapter script.",
    )
    parser.add_argument(
        "--require-evaluator-cmd",
        action="store_true",
        help="Fail if neither HARBOR_EVALUATOR_CMD_JSON nor HARBOR_EVALUATOR_CMD is set.",
    )
    parser.add_argument(
        "--expect-external-evaluator",
        action="store_true",
        help="Require score/prediction ext.harbor.evaluation_mode == 'external'.",
    )
    return parser.parse_args(argv)


def _actionable_error(error_code: str | None, stderr: str) -> str:
    if error_code is None:
        return (
            "Adapter exited non-zero without a typed error code.\n"
            "Action: inspect adapter stderr and ensure Harbor adapter still emits structured errors."
        )
    if error_code.startswith("config."):
        return (
            f"Adapter configuration failure ({error_code}).\n"
            "Action: check required env vars and evaluator command wiring for this lane."
        )
    if error_code == "evaluator.command_failed":
        return (
            "Harbor evaluator command failed.\n"
            "Action: this often indicates Harbor API drift in your evaluator wrapper.\n"
            "Verify installed Harbor version for this lane and update evaluator glue."
        )
    if error_code in {"evaluator.invalid_json", "evaluator.missing_output", "evaluator.invalid_payload"}:
        return (
            f"Harbor evaluator contract mismatch ({error_code}).\n"
            "Action: evaluator must output one JSON object with expected fields."
        )
    if error_code.startswith("io."):
        return (
            f"Adapter I/O failure ({error_code}).\n"
            "Action: verify trial file paths and permissions for adapter inputs/outputs."
        )
    return (
        f"Unhandled adapter error class ({error_code}).\n"
        "Action: inspect stderr and update compatibility probe/adapter mapping if Harbor behavior changed."
    )


def _extract_error_code(stderr: str) -> str | None:
    match = ERROR_CODE_RE.search(stderr)
    if match is None:
        return None
    return match.group(1)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}, got {type(payload).__name__}")
    return payload


def _assert_schema(payload: dict[str, Any], expected: str, label: str) -> None:
    schema = payload.get("schema_version")
    if schema != expected:
        raise ValueError(
            f"{label} schema mismatch: expected '{expected}', got '{schema}'"
        )


def _assert_eval_mode(payload: dict[str, Any], expected_external: bool, label: str) -> None:
    ext = payload.get("ext")
    mode = None
    if isinstance(ext, dict):
        harbor = ext.get("harbor")
        if isinstance(harbor, dict):
            mode = harbor.get("evaluation_mode")
    if expected_external and mode != "external":
        raise ValueError(
            f"{label} expected ext.harbor.evaluation_mode='external', got '{mode}'"
        )


def _validate_outputs(prediction_path: Path, score_path: Path, expect_external: bool) -> None:
    prediction = _load_json(prediction_path)
    score = _load_json(score_path)
    _assert_schema(prediction, "benchmark_prediction_record_v1", "prediction")
    _assert_schema(score, "benchmark_score_record_v1", "score")
    if expect_external:
        _assert_eval_mode(prediction, True, "prediction")
        _assert_eval_mode(score, True, "score")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    adapter_script = Path(args.adapter_script).expanduser().resolve()
    if not adapter_script.is_file():
        print(
            f"compat probe failed: adapter script not found at {adapter_script}",
            file=sys.stderr,
        )
        return 2

    has_eval_cmd = bool(os.environ.get("HARBOR_EVALUATOR_CMD_JSON") or os.environ.get("HARBOR_EVALUATOR_CMD"))
    if args.require_evaluator_cmd and not has_eval_cmd:
        print(
            "compat probe failed: evaluator command is required but missing.\n"
            "Set HARBOR_EVALUATOR_CMD_JSON (preferred) or HARBOR_EVALUATOR_CMD.",
            file=sys.stderr,
        )
        return 2

    expect_external = bool(args.expect_external_evaluator or args.require_evaluator_cmd)

    with tempfile.TemporaryDirectory(prefix="harbor_phase3_compat_") as tmp:
        tmp_dir = Path(tmp)
        task_path = tmp_dir / "task.json"
        result_path = tmp_dir / "result.json"
        prediction_path = tmp_dir / "benchmark_prediction.json"
        score_path = tmp_dir / "benchmark_score.json"

        task_payload = {
            "id": "tb2_phase3_probe_task",
            "benchmark": {
                "adapter_id": "harbor_tb2",
                "name": "terminal_bench_2",
                "split": "test",
            },
        }
        result_payload = {
            "outcome": True,
            "output": {"text": "phase3 compat probe"},
        }
        task_path.write_text(json.dumps(task_payload), encoding="utf-8")
        result_path.write_text(json.dumps(result_payload), encoding="utf-8")

        env = os.environ.copy()
        env.update(
            {
                "AGENTLAB_TASK_PATH": str(task_path),
                "AGENTLAB_RESULT_PATH": str(result_path),
                "AGENTLAB_BENCHMARK_PREDICTION_PATH": str(prediction_path),
                "AGENTLAB_BENCHMARK_SCORE_PATH": str(score_path),
                "AGENTLAB_RUN_ID": "run_harbor_phase3_probe",
                "AGENTLAB_TRIAL_ID": "trial_harbor_phase3_probe",
                "AGENTLAB_VARIANT_ID": "control",
                "AGENTLAB_TASK_ID": "tb2_phase3_probe_task",
                "AGENTLAB_REPL_IDX": "0",
                "AGENTLAB_AGENT_EXIT_STATUS": "0",
            }
        )

        proc = subprocess.run(
            [args.python_bin, str(adapter_script)],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        if proc.returncode != 0:
            stderr = proc.stderr.strip()
            error_code = _extract_error_code(stderr)
            print(
                "compat probe failed: adapter returned non-zero.\n"
                f"error_code={error_code or 'unknown'}\n"
                f"{_actionable_error(error_code, stderr)}\n"
                f"adapter_stderr={stderr or '<empty>'}\n"
                f"adapter_stdout={proc.stdout.strip() or '<empty>'}",
                file=sys.stderr,
            )
            return proc.returncode

        try:
            _validate_outputs(prediction_path, score_path, expect_external)
        except Exception as exc:
            print(
                "compat probe failed: adapter outputs are invalid.\n"
                f"Action: update Harbor evaluator/adapter mapping for this lane.\n"
                f"details={exc}",
                file=sys.stderr,
            )
            return 3

    print("harbor compat probe passed", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
