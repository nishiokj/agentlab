#!/usr/bin/env python3
"""Bench runtime adapter for AgentLab.

Runs a bench-style agent command that writes ``patch.diff`` in WORKSPACE and
emits ``agent_result_v1`` to AGENTLAB_RESULT_PATH.
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
        nested_task = task_payload.get("task")
        if isinstance(nested_task, dict):
            nested_id = nested_task.get("id")
            if isinstance(nested_id, str) and nested_id.strip():
                return nested_id.strip()
    return "task_unknown"


def _load_bindings(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    data = _read_json(p)
    if isinstance(data, dict):
        return data
    return {}


def _parse_command(raw: Any) -> list[str] | None:
    if isinstance(raw, list) and raw and all(isinstance(v, str) and v.strip() for v in raw):
        return [v.strip() for v in raw]
    if isinstance(raw, str) and raw.strip():
        return shlex.split(raw.strip())
    return None


def _resolve_agent_command(bindings: dict[str, Any]) -> list[str]:
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

    command = _parse_command(bindings.get("bench_agent_command"))
    if command:
        return command

    command = _parse_command(bindings.get("agent_command"))
    if command:
        return command

    raise RuntimeError(
        "No bench agent command configured. Set AGENTLAB_BENCH_AGENT_COMMAND(_JSON) "
        "or provide bench_agent_command/agent_command in bindings."
    )


def _result_ids(task_id: str) -> dict[str, Any]:
    return {
        "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
        "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
        "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
        "task_id": os.environ.get("AGENTLAB_TASK_ID", task_id),
        "repl_idx": _env_int("AGENTLAB_REPL_IDX", 0),
    }


def _workspace_path() -> Path:
    raw = os.environ.get("WORKSPACE")
    if raw:
        return Path(raw).resolve()
    default = Path("/agentlab/workspace")
    if default.exists():
        return default
    return Path.cwd()


def _patch_text(workspace: Path) -> str | None:
    candidate = workspace / "patch.diff"
    if not candidate.exists():
        return None
    text = candidate.read_text(encoding="utf-8", errors="replace")
    return text if text.strip() else None


def main() -> int:
    task_path = _required_env("AGENTLAB_TASK_PATH")
    result_path = _required_env("AGENTLAB_RESULT_PATH")
    bindings_path = os.environ.get("AGENTLAB_BINDINGS_PATH")

    task_payload = _read_json(task_path)
    bindings = _load_bindings(bindings_path)

    task_id = _task_id(task_payload)
    workspace = _workspace_path()
    workspace.mkdir(parents=True, exist_ok=True)

    command = _resolve_agent_command(bindings)
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

    result: dict[str, Any] = {
        "schema_version": "agent_result_v1",
        "ids": _result_ids(task_id),
        "outcome": outcome,
        "metrics": {
            "exit_code": exit_code,
            "wall_clock_s": wall_clock_s,
            "has_patch": patch is not None,
            "timed_out": timed_out,
        },
        "ext": {
            "bench_runtime_adapter": {
                "command": command,
                "workspace": str(workspace),
                "stdout_tail": stdout[-4000:],
                "stderr_tail": stderr[-4000:],
            }
        },
    }

    if patch is not None:
        result["answer"] = {"patch": patch}

    if error is not None:
        result["error"] = error

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
        raise SystemExit(1)
