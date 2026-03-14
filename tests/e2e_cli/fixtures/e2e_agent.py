#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_bindings() -> dict[str, Any]:
    raw = os.environ.get("AGENTLAB_BINDINGS_PATH", "").strip()
    if not raw:
        return {}
    path = Path(raw)
    if not path.exists():
        return {}
    value = _read_json(path)
    return value if isinstance(value, dict) else {}


def _workspace_root() -> Path:
    raw = os.environ.get("WORKSPACE", "").strip()
    if raw:
        return Path(raw)
    return Path.cwd()


def _contract_path(env_name: str, cli_value: str | None) -> Path:
    raw = os.environ.get(env_name, "").strip()
    if raw:
        return Path(raw)
    if cli_value:
        return Path(cli_value)
    raise SystemExit(f"missing required contract path: {env_name}")


def _is_preflight_smoke() -> bool:
    raw = os.environ.get("AGENTLAB_PREFLIGHT_SMOKE", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _task_id(task_payload: dict[str, Any]) -> str:
    candidate = task_payload.get("id")
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()
    return "task_unknown"


def _variant_label(bindings: dict[str, Any]) -> str:
    candidate = bindings.get("variant_label")
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()
    return "control"


def _coerce_float(value: Any, default: float) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return default
    return default


def _coerce_int(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return default
    return default


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _observe_path(workspace: Path, spec: dict[str, Any]) -> dict[str, Any]:
    raw_path = spec.get("path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return {"exists": False, "error": "missing observation path"}
    path = Path(raw_path)
    resolved = path if path.is_absolute() else workspace / path
    observation: dict[str, Any] = {
        "path": raw_path,
        "resolved_path": str(resolved),
        "exists": resolved.exists(),
    }
    if resolved.is_dir():
        observation["kind"] = "directory"
        observation["entries"] = sorted(p.name for p in resolved.iterdir())
        return observation
    if not resolved.exists():
        return observation
    observation["kind"] = "file"
    try:
        text = resolved.read_text(encoding="utf-8")
        observation["text"] = text
        expected_text = spec.get("expect_text")
        if isinstance(expected_text, str):
            observation["matches_expected_text"] = expected_text in text
    except Exception as exc:  # noqa: BLE001
        observation["read_error"] = str(exc)
    if spec.get("expect_read_only") is True:
        try:
            with resolved.open("a", encoding="utf-8") as handle:
                handle.write("\nwrite_probe\n")
            observation["write_blocked"] = False
        except OSError:
            observation["write_blocked"] = True
    return observation


def _write_trajectory(event_type: str, payload: dict[str, Any]) -> None:
    raw = os.environ.get("AGENTLAB_TRAJECTORY_PATH", "").strip()
    if not raw:
        return
    path = Path(raw)
    path.parent.mkdir(parents=True, exist_ok=True)
    event = {"event_type": event_type, **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def _write_raw_trajectory_line(line: str) -> None:
    raw = os.environ.get("AGENTLAB_TRAJECTORY_PATH", "").strip()
    if not raw:
        return
    path = Path(raw)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip("\n") + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="AgentLab E2E agent fixture")
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--config")
    args = parser.parse_args()

    task_path = _contract_path("AGENTLAB_TASK_PATH", args.input)
    result_path = _contract_path("AGENTLAB_RESULT_PATH", args.output)
    task_payload = _read_json(task_path)
    if not isinstance(task_payload, dict):
        raise SystemExit("task payload must be an object")
    bindings = _load_bindings()
    workspace = _workspace_root()
    workspace.mkdir(parents=True, exist_ok=True)

    task_id = _task_id(task_payload)
    variant_label = _variant_label(bindings)
    expected_variant = task_payload.get("expected_variant", variant_label)
    force_outcome = bindings.get("force_outcome")
    if isinstance(force_outcome, str) and force_outcome in {
        "success",
        "failure",
        "missing",
        "error",
    }:
        outcome = force_outcome
    elif variant_label == expected_variant:
        outcome = "success"
    else:
        outcome = "failure"

    sleep_ms = _coerce_int(bindings.get("sleep_ms", task_payload.get("sleep_ms")), 0)
    if sleep_ms > 0:
        time.sleep(min(sleep_ms, 5_000) / 1000.0)

    observations_spec = task_payload.get("observe")
    observations: dict[str, Any] = {}
    if isinstance(observations_spec, dict):
        for name, spec in sorted(observations_spec.items()):
            if isinstance(spec, dict):
                observations[name] = _observe_path(workspace, spec)

    resolved_value = _coerce_float(task_payload.get("resolved_if_match"), 1.0)
    unresolved_value = _coerce_float(task_payload.get("resolved_if_miss"), 0.0)
    score_bias = _coerce_float(bindings.get("score_bias"), 0.0)
    objective_value = (
        resolved_value if outcome == "success" else unresolved_value
    ) + score_bias

    report = {
        "task_id": task_id,
        "variant_label": variant_label,
        "expected_variant": expected_variant,
        "outcome": outcome,
        "objective_value": objective_value,
        "bindings": bindings,
        "observations": observations,
        "cwd": str(Path.cwd()),
        "runtime_inputs": {
            "config_arg": args.config or "",
            "e2e_config_path": os.environ.get("E2E_CONFIG_PATH", ""),
        },
        "env": {
            "run_id": os.environ.get("AGENTLAB_RUN_ID", ""),
            "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", ""),
            "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", ""),
            "task_id": os.environ.get("AGENTLAB_TASK_ID", ""),
            "workspace": str(workspace),
        },
    }
    derived_metrics: dict[str, float | int] = {}
    for name, observation in observations.items():
        derived_metrics[f"obs_{name}_exists"] = 1 if observation.get("exists") else 0
        if "matches_expected_text" in observation:
            derived_metrics[f"obs_{name}_text_match"] = (
                1 if observation.get("matches_expected_text") else 0
            )
        if "write_blocked" in observation:
            derived_metrics[f"obs_{name}_write_blocked"] = (
                1 if observation.get("write_blocked") else 0
            )
    report_path = workspace / "artifacts" / "agent_report.json"
    _write_json(report_path, report)
    _write_json(result_path.parent / "agent_report.json", report)

    checkpoint_rel = Path(".agentlab_generated") / "checkpoints" / "final.json"
    checkpoint_abs = workspace / checkpoint_rel
    _write_json(
        checkpoint_abs,
        {
            "task_id": task_id,
            "variant_label": variant_label,
            "objective_value": objective_value,
        },
    )

    preflight_smoke = _is_preflight_smoke()

    if _coerce_bool(bindings.get("emit_invalid_trajectory_json"), False):
        _write_raw_trajectory_line("{invalid trajectory json")
    _write_trajectory(
        "e2e_agent.start",
        {
            "task_id": task_id,
            "variant_label": variant_label,
        },
    )

    result = {
        "schema_version": "agent_result_v1",
        "ids": {
            "run_id": os.environ.get("AGENTLAB_RUN_ID", "run_unknown"),
            "trial_id": os.environ.get("AGENTLAB_TRIAL_ID", "trial_unknown"),
            "variant_id": os.environ.get("AGENTLAB_VARIANT_ID", "variant_unknown"),
            "task_id": os.environ.get("AGENTLAB_TASK_ID", task_id),
            "repl_idx": _coerce_int(os.environ.get("AGENTLAB_REPL_IDX"), 0),
        },
        "outcome": outcome,
        "objective": {
            "name": "resolved",
            "value": objective_value,
            "direction": "maximize",
        },
        "metrics": {
            "resolved": objective_value,
            "workspace_file_count": sum(
                1
                for path in workspace.rglob("*")
                if path.is_file()
            ),
            "observation_count": len(observations),
            **derived_metrics,
        },
        "artifacts": [
            {
                "path": str(report_path.relative_to(workspace)),
                "logical_name": "agent_report",
                "mime_type": "application/json",
            }
        ],
        "checkpoints": [
            {
                "path": f"/agentlab/workspace/{checkpoint_rel.as_posix()}",
                "logical_name": "final",
                "step": 1,
            }
        ],
        "ext": {
            "e2e_agent": report,
        },
    }

    if outcome == "error":
        result["error"] = {
            "error_type": "FIXTURE_ERROR",
            "message": "forced error outcome",
        }

    exit_code = _coerce_int(bindings.get("exit_code"), 0)
    if not preflight_smoke:
        exit_code = _coerce_int(bindings.get("runtime_only_exit_code"), exit_code)

    emit_invalid_result_json = _coerce_bool(bindings.get("emit_invalid_result_json"), False)
    skip_result_write = _coerce_bool(bindings.get("skip_result_write"), False)
    if not preflight_smoke:
        emit_invalid_result_json = _coerce_bool(
            bindings.get("runtime_only_emit_invalid_result_json"),
            emit_invalid_result_json,
        )
        skip_result_write = _coerce_bool(
            bindings.get("runtime_only_skip_result_write"),
            skip_result_write,
        )

    if emit_invalid_result_json:
        target = result_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{invalid result json\n", encoding="utf-8")
    elif exit_code == 0 and not skip_result_write:
        _write_json(result_path, result)

    _write_trajectory(
        "e2e_agent.finish",
        {
            "task_id": task_id,
            "variant_label": variant_label,
            "outcome": outcome,
            "objective_value": objective_value,
        },
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
