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

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bench.taskkit.schema import validate_with_schema_file


ERROR_CODE_RE = re.compile(r"error_code=([A-Za-z0-9._-]+)")
DEFAULT_ADAPTER_SCRIPT = str((Path(__file__).resolve().parent / "harbor_benchmark_adapter.py"))
SCHEMAS_DIR = REPO_ROOT / "schemas"


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
        help="Require conclusion payload.harbor.evaluation_mode == 'external'.",
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


def _validate_schema(payload: dict[str, Any], schema_file: str, label: str) -> None:
    errors = validate_with_schema_file(payload, SCHEMAS_DIR / schema_file)
    if errors:
        raise ValueError(f"{label} schema validation failed: {'; '.join(errors)}")


def _assert_eval_mode(payload: dict[str, Any], expected_external: bool) -> None:
    raw_payload = payload.get("payload")
    mode = None
    if isinstance(raw_payload, dict):
        harbor = raw_payload.get("harbor")
        if isinstance(harbor, dict):
            mode = harbor.get("evaluation_mode")
    if expected_external and mode != "external":
        raise ValueError(
            f"conclusion expected payload.harbor.evaluation_mode='external', got '{mode}'"
        )


def _validate_output(mapped_output_path: Path, expect_external: bool) -> None:
    mapped = _load_json(mapped_output_path)
    _validate_schema(mapped, "trial_conclusion_v1.jsonschema", "mapped_output")
    if expect_external:
        _assert_eval_mode(mapped, True)


def _grader_input_payload() -> dict[str, Any]:
    return {
        "schema_version": "grader_input_v1",
        "ids": {
            "run_id": "run_harbor_phase3_probe",
            "trial_id": "trial_harbor_phase3_probe",
            "variant_id": "control",
            "task_id": "tb2_phase3_probe_task",
            "repl_idx": 0,
        },
        "task": {
            "id": "tb2_phase3_probe_task",
            "benchmark": {
                "adapter_id": "harbor_tb2",
                "name": "terminal_bench_2",
                "split": "test",
            },
        },
        "artifact_type": "structured_json",
        "agent_phase": {
            "exit_code": 0,
            "timed_out": False,
            "result_present": True,
            "result_schema_valid": True,
            "started_at": "2026-03-17T00:00:00Z",
            "ended_at": "2026-03-17T00:00:01Z",
        },
        "candidate_artifact": {
            "state": "valid",
            "artifact_type": "structured_json",
            "source": "result.inline",
            "payload": {
                "outcome": True,
                "output": {"text": "phase3 compat probe"},
            },
        },
        "workspace_delta": {
            "state": "missing",
        },
        "paths": {
            "result_path": "/agentlab/out/result.json",
        },
        "workdir": "/workspace",
    }


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
        grader_input_path = tmp_dir / "grader_input.json"
        mapped_output_path = tmp_dir / "mapped_grader_output.json"

        grader_input_path.write_text(json.dumps(_grader_input_payload()), encoding="utf-8")

        env = os.environ.copy()
        env.update(
            {
                "AGENTLAB_GRADER_INPUT_PATH": str(grader_input_path),
                "AGENTLAB_MAPPED_GRADER_OUTPUT_PATH": str(mapped_output_path),
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
            _validate_output(mapped_output_path, expect_external)
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
