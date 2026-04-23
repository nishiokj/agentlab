#!/usr/bin/env python3
"""Bench runtime adapter for AgentLab.

Runs a bench-style agent command that writes ``patch.diff`` in the task workdir
and emits ``artifact_envelope_v1`` to ``AGENTLAB_RESULT_PATH``.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

VALID_ARTIFACT_TYPES = {
    "patch_submission",
    "text_response",
    "structured_json",
    "file_ref",
}


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


def _env_int(name: str, fallback: int) -> int:
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
    return "task_unknown"


def _load_trial_input(path: str) -> dict[str, Any]:
    data = _read_json(path)
    if isinstance(data, dict):
        return data
    raise RuntimeError("trial input must be a JSON object")


def _extract_task_payload(trial_input: dict[str, Any]) -> dict[str, Any]:
    value = trial_input.get("task")
    if isinstance(value, dict):
        return value
    return trial_input


def _extract_workspace_path(trial_input: dict[str, Any]) -> Path | None:
    runtime = trial_input.get("runtime")
    if not isinstance(runtime, dict):
        return None
    workdir = runtime.get("workdir")
    if not isinstance(workdir, str) or not workdir.strip():
        return None
    return Path(workdir)


def _load_structured_input(path: str) -> tuple[dict[str, Any], dict[str, Any]]:
    trial_input = _load_trial_input(path)
    task_payload = _extract_task_payload(trial_input)
    if not isinstance(task_payload, dict):
        raise RuntimeError("trial input task payload must be an object")
    return trial_input, task_payload


def _workspace_path(trial_input: dict[str, Any]) -> Path:
    explicit = _extract_workspace_path(trial_input)
    if explicit is not None:
        return explicit
    raw = os.environ.get("WORKSPACE")
    if raw:
        return Path(raw).resolve()
    return Path.cwd()


def _parse_command(raw: Any) -> list[str] | None:
    if isinstance(raw, list) and raw and all(isinstance(v, str) and v.strip() for v in raw):
        return [v.strip() for v in raw]
    if isinstance(raw, str) and raw.strip():
        return shlex.split(raw.strip())
    return None


def _resolve_agent_command(task_payload: dict[str, Any]) -> list[str]:
    env_json = os.environ.get("AGENTLAB_BENCH_AGENT_COMMAND_JSON")
    if env_json:
        try:
            parsed = json.loads(env_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid AGENTLAB_BENCH_AGENT_COMMAND_JSON: {exc}") from exc
        command = _parse_command(parsed)
        if command:
            return command
        raise RuntimeError("AGENTLAB_BENCH_AGENT_COMMAND_JSON must be a non-empty string or list")

    env_shell = os.environ.get("AGENTLAB_BENCH_AGENT_COMMAND")
    command = _parse_command(env_shell)
    if command:
        return command

    command = _parse_command(task_payload.get("bench_agent_command"))
    if command:
        return command

    command = _parse_command(task_payload.get("agent_command"))
    if command:
        return command

    raise RuntimeError(
        "No bench agent command configured. Set AGENTLAB_BENCH_AGENT_COMMAND(_JSON) "
        "or provide bench_agent_command/agent_command in the task payload."
    )


def _patch_text(workspace: Path) -> str | None:
    candidate = workspace / "patch.diff"
    if not candidate.exists():
        return None
    text = candidate.read_text(encoding="utf-8", errors="replace")
    return text if text.strip() else None


def _artifact_type(trial_input: dict[str, Any]) -> str:
    value = trial_input.get("artifact_type")
    if isinstance(value, str) and value in VALID_ARTIFACT_TYPES:
        return value
    return "patch_submission"


def _artifact_payload(
    artifact_type: str,
    *,
    patch: str | None,
    _workspace: Path,
    result_dir: Path,
    outcome: str,
    exit_code: int,
    timed_out: bool,
    wall_clock_s: float,
    stdout: str,
    stderr: str,
    error: dict[str, str] | None,
) -> Any:
    if artifact_type == "patch_submission":
        return {
            "patch_format": "unified_diff",
            "patch": patch or "",
        }
    if artifact_type == "text_response":
        if patch is not None:
            return patch
        if stderr.strip():
            return stderr[-4000:]
        return stdout[-4000:]
    if artifact_type == "file_ref":
        artifact_name = "bench_runtime_result.json"
        target = result_dir / artifact_name
        _write_json(
            target,
            {
                "outcome": outcome,
                "exit_code": exit_code,
                "timed_out": timed_out,
                "wall_clock_s": wall_clock_s,
                "patch": patch,
                "stdout_tail": stdout[-4000:],
                "stderr_tail": stderr[-4000:],
                "error": error,
            },
        )
        return {
            "path": f"/agentlab/out/{artifact_name}",
            "logical_name": "bench_runtime_result",
            "mime_type": "application/json",
        }
    return {
        "outcome": outcome,
        "patch": patch,
        "metrics": {
            "exit_code": exit_code,
            "wall_clock_s": wall_clock_s,
            "has_patch": patch is not None,
            "timed_out": timed_out,
        },
        "stdout_tail": stdout[-4000:],
        "stderr_tail": stderr[-4000:],
        "error": error,
    }


def _legacy_agent_result(
    *,
    task_id: str,
    outcome: str,
    patch: str | None,
    exit_code: int,
    timed_out: bool,
    wall_clock_s: float,
    stdout: str,
    stderr: str,
    error: dict[str, str] | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": "agent_result_v1",
        "ids": {
            "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
            "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
            "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
            "task_id": os.environ.get("AGENTLAB_TASK_ID", task_id),
            "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
        },
        "outcome": outcome,
        "metrics": {
            "exit_code": exit_code,
            "wall_clock_s": wall_clock_s,
            "has_patch": patch is not None,
            "timed_out": timed_out,
        },
        "ext": {
            "bench_runtime_adapter": {
                "command": [],
                "workspace": "",
                "stdout_tail": stdout[-4000:],
                "stderr_tail": stderr[-4000:],
            }
        },
    }
    if patch is not None:
        result["answer"] = {"patch": patch}
    if error is not None:
        result["error"] = error
    return result


def main() -> int:
    task_path = _required_env("AGENTLAB_TRIAL_INPUT_PATH")
    result_path = _required_env("AGENTLAB_RESULT_PATH")
    trial_input, task_payload = _load_structured_input(task_path)

    task_id = _task_id(task_payload)
    workspace = _workspace_path(trial_input)
    workspace.mkdir(parents=True, exist_ok=True)

    command = _resolve_agent_command(task_payload)
    timeout_ms = max(1000, _env_int("AGENTLAB_TIMEOUT_MS", 600000))
    timeout_s = max(1, timeout_ms // 1000)

    env = os.environ.copy()
    env["WORKSPACE"] = str(workspace)
    env["TASK_ID"] = task_id

    started = time.monotonic()
    exit_code = 0
    timed_out = False
    stdout = ""
    stderr = ""
    error: dict[str, str] | None = None

    try:
        proc = subprocess.run(
            command,
            cwd=str(workspace),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        exit_code = int(proc.returncode)
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        exit_code = 124
        stdout = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
        stderr = (exc.stderr or "") if isinstance(exc.stderr, str) else ""
        error = {
            "error_type": "AGENT_TIMEOUT",
            "message": f"agent command timed out after {timeout_s}s",
        }
    except Exception as exc:  # noqa: BLE001
        exit_code = 1
        error = {
            "error_type": "AGENT_ENV_ERROR",
            "message": str(exc),
        }

    wall_clock_s = round(time.monotonic() - started, 3)
    patch = _patch_text(workspace)

    if error is not None:
        outcome = "error"
    elif exit_code == 0:
        outcome = "success"
    else:
        outcome = "failure"

    artifact_type = _artifact_type(trial_input)
    result = {
        "schema_version": "artifact_envelope_v1",
        "artifact_type": artifact_type,
        "artifact": _artifact_payload(
            artifact_type,
            patch=patch,
            _workspace=workspace,
            result_dir=Path(result_path).parent,
            outcome=outcome,
            exit_code=exit_code,
            timed_out=timed_out,
            wall_clock_s=wall_clock_s,
            stdout=stdout,
            stderr=stderr,
            error=error,
        ),
        "metadata": {
            "task_id": task_id,
            "command": command,
            "workspace": str(workspace),
            "outcome": outcome,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "wall_clock_s": wall_clock_s,
        },
    }
    legacy_result = _legacy_agent_result(
        task_id=task_id,
        outcome=outcome,
        patch=patch,
        exit_code=exit_code,
        timed_out=timed_out,
        wall_clock_s=wall_clock_s,
        stdout=stdout,
        stderr=stderr,
        error=error,
    )

    if os.environ.get("AGENTLAB_PREFLIGHT_SMOKE", "").strip().lower() in {"1", "true", "yes", "on"}:
        _write_json(result_path, legacy_result)
    else:
        _write_json(result_path, result)

    trajectory_path = os.environ.get("AGENTLAB_TRAJECTORY_PATH")
    if trajectory_path:
        event = {
            "event_type": "bench_runtime_adapter.end",
            "task_id": task_id,
            "exit_code": exit_code,
            "outcome": outcome,
            "has_patch": patch is not None,
            "timed_out": timed_out,
            "wall_clock_s": wall_clock_s,
        }
        out = Path(trajectory_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(event, separators=(",", ":")) + "\n", encoding="utf-8")

    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"bench_runtime_adapter.py error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
