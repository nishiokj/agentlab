#!/usr/bin/env python3
"""Minimal real SWE-bench grader for one locally available SWE-bench Lite task.

This intentionally does real patch application and pytest execution. It is not
the old shim that treats an agent exit code as a verdict.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


DEFAULT_MAPPED_OUTPUT_PATH = "/agentlab/out/mapped_grader_output.json"
DEFAULT_RAW_OUTPUT_PATH = "/agentlab/out/raw_grader_output.json"
DEFAULT_METADATA_DIR = "/testbed/.agentlab/support/swebench/official_metadata"
DEFAULT_TESTBED_PYTHON = "/opt/miniconda3/envs/testbed/bin/python"


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required env var: {name}")
    return value


def _list_field(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str) and value.strip():
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return []


def _instance_id(task: dict[str, Any]) -> str:
    value = task.get("swebench", {}).get("input", {}).get("instance_id")
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise RuntimeError("grader input task is missing swebench.input.instance_id")


def _base_commit(task: dict[str, Any], official: dict[str, Any]) -> str:
    value = task.get("swebench", {}).get("input", {}).get("base_commit")
    if isinstance(value, str) and value.strip():
        return value.strip()
    value = official.get("base_commit")
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise RuntimeError("missing base_commit")


def _candidate_patch(grader_input: dict[str, Any]) -> str:
    candidate = grader_input.get("candidate_artifact")
    if not isinstance(candidate, dict) or candidate.get("state") != "valid":
        return ""
    payload = candidate.get("payload")
    if isinstance(payload, dict):
        patch = payload.get("patch")
        if isinstance(patch, str):
            return patch
    return ""


def _run(argv: list[str], *, cwd: Path, timeout: int = 300) -> dict[str, Any]:
    proc = subprocess.run(
        argv,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    return {
        "argv": argv,
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-12000:],
        "stderr_tail": proc.stderr[-12000:],
    }


def _apply_patch(repo: Path, patch_text: str, label: str) -> dict[str, Any]:
    if not patch_text.strip():
        return {"label": label, "returncode": 2, "stdout_tail": "", "stderr_tail": "empty patch"}
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=f".{label}.patch") as patch_file:
        patch_file.write(patch_text)
        patch_file.flush()
        result = _run(["git", "apply", "--whitespace=nowarn", patch_file.name], cwd=repo)
    result["label"] = label
    return result


def _load_official(instance_id: str) -> dict[str, Any]:
    metadata_dir = Path(os.environ.get("SWEBENCH_OFFICIAL_METADATA_DIR", DEFAULT_METADATA_DIR))
    metadata_path = metadata_dir / f"{instance_id}.json"
    if not metadata_path.exists():
        raise RuntimeError(f"official SWE-bench metadata not found: {metadata_path}")
    return _read_json(metadata_path)


def _mapped_conclusion(
    *,
    grader_input: dict[str, Any],
    raw: dict[str, Any],
    resolved: bool,
    reason: str,
) -> dict[str, Any]:
    ids = grader_input.get("ids", {})
    task = grader_input.get("task", {})
    return {
        "schema_version": "trial_conclusion_v1",
        "payload": {
            "benchmark": {
                "adapter_id": "swebench_official_single_grader",
                "name": "swebench_lite_curated",
                "split": "test",
            },
            "ids": ids,
            "task_id": ids.get("task_id") or task.get("id") or "task_unknown",
            "verdict": "pass" if resolved else "fail",
            "resolved": 1.0 if resolved else 0.0,
            "metrics": {
                "success": 1.0 if resolved else 0.0,
                "resolved": 1.0 if resolved else 0.0,
                "fail_to_pass_passed": raw.get("fail_to_pass_passed", False),
                "pass_to_pass_passed": raw.get("pass_to_pass_passed", False),
            },
            "reason": reason,
            "swebench": raw.get("swebench", {}),
        },
        "reported_outcome": "success" if resolved else "failure",
        "primary_metric": {
            "name": "resolved",
            "value": 1.0 if resolved else 0.0,
        },
        "grader": {
            "name": "swebench_official_single_grader",
            "strategy": os.environ.get("AGENTLAB_GRADING_STRATEGY", "in_task_image"),
            "version": "2026-04-30",
        },
    }


def grade() -> tuple[dict[str, Any], dict[str, Any]]:
    grader_input = _read_json(_required_env("AGENTLAB_GRADER_INPUT_PATH"))
    task = grader_input.get("task")
    if not isinstance(task, dict):
        raise RuntimeError("grader input task must be an object")
    instance_id = _instance_id(task)
    official = _load_official(instance_id)
    repo = Path(grader_input.get("workdir") or "/testbed")
    candidate_patch = _candidate_patch(grader_input)
    fail_to_pass = _list_field(official.get("FAIL_TO_PASS"))
    pass_to_pass = _list_field(official.get("PASS_TO_PASS"))

    raw: dict[str, Any] = {
        "schema_version": "swebench_real_grader_raw_v1",
        "swebench": {
            "instance_id": instance_id,
            "base_commit": _base_commit(task, official),
            "fail_to_pass": fail_to_pass,
            "pass_to_pass_count": len(pass_to_pass),
        },
        "candidate_patch_bytes": len(candidate_patch.encode("utf-8")),
        "steps": [],
    }

    if not candidate_patch.strip():
        raw["failure_label"] = "NO_PATCH"
        return raw, _mapped_conclusion(
            grader_input=grader_input,
            raw=raw,
            resolved=False,
            reason="candidate patch was empty or invalid",
        )

    raw["steps"].append(_run(["git", "reset", "--hard", _base_commit(task, official)], cwd=repo))
    candidate_apply = _apply_patch(repo, candidate_patch, "candidate")
    raw["steps"].append(candidate_apply)
    if candidate_apply["returncode"] != 0:
        raw["failure_label"] = "CANDIDATE_PATCH_APPLY_FAILED"
        return raw, _mapped_conclusion(
            grader_input=grader_input,
            raw=raw,
            resolved=False,
            reason="candidate patch did not apply",
        )

    test_apply = _apply_patch(repo, str(official.get("test_patch") or ""), "test")
    raw["steps"].append(test_apply)
    if test_apply["returncode"] != 0:
        raw["failure_label"] = "TEST_PATCH_APPLY_FAILED"
        return raw, _mapped_conclusion(
            grader_input=grader_input,
            raw=raw,
            resolved=False,
            reason="official test_patch did not apply",
        )

    python = os.environ.get("SWEBENCH_TESTBED_PYTHON", DEFAULT_TESTBED_PYTHON)
    fail_run = _run([python, "-m", "pytest", "-q", *fail_to_pass], cwd=repo, timeout=600)
    pass_run = _run([python, "-m", "pytest", "-q", *pass_to_pass], cwd=repo, timeout=600)
    raw["steps"].append({"label": "fail_to_pass", **fail_run})
    raw["steps"].append({"label": "pass_to_pass", **pass_run})
    raw["fail_to_pass_passed"] = fail_run["returncode"] == 0
    raw["pass_to_pass_passed"] = pass_run["returncode"] == 0
    resolved = bool(raw["fail_to_pass_passed"] and raw["pass_to_pass_passed"])
    raw["failure_label"] = None if resolved else "TESTS_FAILED"
    return raw, _mapped_conclusion(
        grader_input=grader_input,
        raw=raw,
        resolved=resolved,
        reason="all selected SWE-bench tests passed" if resolved else "selected SWE-bench tests failed",
    )


def main() -> int:
    raw_output_path = os.environ.get("AGENTLAB_RAW_GRADER_OUTPUT_PATH", DEFAULT_RAW_OUTPUT_PATH)
    mapped_output_path = os.environ.get("AGENTLAB_MAPPED_GRADER_OUTPUT_PATH", DEFAULT_MAPPED_OUTPUT_PATH)
    raw, mapped = grade()
    _write_json(raw_output_path, raw)
    _write_json(mapped_output_path, mapped)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"swebench_official_single_grader.py error: {exc}", file=sys.stderr)
        raise SystemExit(1)
