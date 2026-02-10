import copy
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

import yaml

from agentlab_core.canonical_json import canonical_dumps
from agentlab_core.hashing import sha256_bytes, sha256_file


def resolve_experiment_config(experiment: Dict[str, Any], base_dir: str) -> Dict[str, Any]:
    exp = copy.deepcopy(experiment)
    version = exp.get("version")
    if version != "0.3":
        raise ValueError("Only version 0.3 is supported")

    dataset = exp.get("dataset") or {}
    dataset_path = dataset.get("path")
    if not dataset_path:
        raise ValueError("dataset.path is required")
    if not os.path.isabs(dataset_path):
        dataset_path = os.path.normpath(os.path.join(base_dir, dataset_path))
    dataset["path"] = dataset_path

    if "content_hash" not in dataset or not dataset.get("content_hash"):
        dataset["content_hash"] = sha256_file(dataset_path)

    exp["dataset"] = dataset
    exp["registered_at"] = datetime.now(timezone.utc).isoformat()

    # Normalize runtime.harness
    runtime = exp.get("runtime") or {}
    harness = (runtime.get("harness") or {})
    if harness.get("mode") != "cli":
        raise ValueError("runtime.harness.mode must be 'cli' for this runner")
    if not harness.get("command"):
        raise ValueError("runtime.harness.command is required")
    # Resolve any relative file args in the command to absolute paths so the runner
    # can execute the harness from an arbitrary CWD (e.g., trial output dir).
    cmd = list(harness["command"])
    resolved_cmd = []
    for arg in cmd:
        if not isinstance(arg, str):
            resolved_cmd.append(arg)
            continue
        if os.path.isabs(arg):
            resolved_cmd.append(arg)
            continue
        # Heuristic: if arg looks like a path and exists relative to experiment dir, absolutize it.
        looks_like_path = arg.startswith("./") or (os.sep in arg)
        if looks_like_path:
            candidate = os.path.normpath(os.path.join(base_dir, arg))
            if os.path.exists(candidate):
                resolved_cmd.append(candidate)
                continue
        resolved_cmd.append(arg)
    harness["command"] = resolved_cmd
    if "integration_level" not in harness:
        harness["integration_level"] = "cli_basic"
    if "control_plane" not in harness:
        harness["control_plane"] = {"mode": "file", "path": "/state/lab_control.json"}
    runtime["harness"] = harness
    exp["runtime"] = runtime

    return exp


class ExperimentResolver:
    def __init__(self, experiment_path: str) -> None:
        self.experiment_path = experiment_path
        self.base_dir = os.path.dirname(os.path.abspath(experiment_path))

    def load(self) -> Dict[str, Any]:
        with open(self.experiment_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError("Experiment YAML must be a mapping")
        return data

    def resolve(self) -> Dict[str, Any]:
        return resolve_experiment_config(self.load(), self.base_dir)

    def digest(self, resolved: Dict[str, Any]) -> str:
        return sha256_bytes(canonical_dumps(resolved).encode("utf-8"))
