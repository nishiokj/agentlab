#!/usr/bin/env python3
"""Run the official SWE-bench harness on patches emitted by an AgentLab run."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HARNESS_PYTHON = REPO_ROOT / ".lab/swebench-harness-py312-venv/bin/python"
DEFAULT_HARNESS_SOURCE = REPO_ROOT / ".lab/upstream/SWE-bench"
DEFAULT_DOCKER_HOST = "unix:///Users/jevinnishioka/.orbstack/run/docker.sock"


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def extract_patch(result: dict[str, Any]) -> str | None:
    if result.get("schema_version") == "artifact_envelope_v1":
        if result.get("artifact_type") != "patch_submission":
            return None
        artifact = result.get("artifact")
        if isinstance(artifact, dict) and isinstance(artifact.get("patch"), str):
            return artifact["patch"]
        return None
    answer = result.get("answer")
    if isinstance(answer, dict) and isinstance(answer.get("patch"), str):
        return answer["patch"]
    return None


def extract_instance_id(grader_input: dict[str, Any], result: dict[str, Any]) -> str | None:
    candidates = [
        grader_input.get("task", {}).get("swebench", {}).get("input", {}).get("instance_id"),
        result.get("metadata", {}).get("instance_id"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def extract_variant_id(grader_input: dict[str, Any], result: dict[str, Any]) -> str:
    candidates = [
        grader_input.get("ids", {}).get("variant_id"),
        result.get("metadata", {}).get("ids", {}).get("variant_id"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return "variant_unknown"


def collect_predictions(run_dir: Path) -> dict[str, list[dict[str, Any]]]:
    by_variant: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trial_dir in sorted((run_dir / "trials").glob("trial_*")):
        result_path = trial_dir / "result.json"
        grader_input_path = trial_dir / "in/grader_input.json"
        if not result_path.exists() or not grader_input_path.exists():
            continue
        result = read_json(result_path)
        grader_input = read_json(grader_input_path)
        patch = extract_patch(result)
        instance_id = extract_instance_id(grader_input, result)
        variant_id = extract_variant_id(grader_input, result)
        if not instance_id:
            print(f"warning: skipping {trial_dir}: missing instance_id", file=sys.stderr)
            continue
        if patch is None:
            print(f"warning: skipping {trial_dir}: missing patch_submission", file=sys.stderr)
            continue
        by_variant[variant_id].append(
            {
                "instance_id": instance_id,
                "model_name_or_path": variant_id,
                "model_patch": patch,
            }
        )
    return dict(by_variant)


def run_harness(
    *,
    harness_python: Path,
    harness_source: Path,
    predictions_path: Path,
    instance_ids: list[str],
    variant_id: str,
    output_dir: Path,
    dataset_name: str,
    split: str,
    namespace: str,
    timeout: int,
    max_workers: int,
) -> None:
    run_id = f"{output_dir.name}_{variant_id}"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(harness_source)
    env.setdefault("DOCKER_HOST", DEFAULT_DOCKER_HOST)
    cmd = [
        str(harness_python),
        "-m",
        "swebench.harness.run_evaluation",
        "--dataset_name",
        dataset_name,
        "--split",
        split,
        "--predictions_path",
        str(predictions_path),
        "--instance_ids",
        *instance_ids,
        "--max_workers",
        str(max_workers),
        "--timeout",
        str(timeout),
        "--run_id",
        run_id,
        "--namespace",
        namespace,
        "--cache_level",
        "instance",
        "--clean",
        "false",
        "--report_dir",
        str(output_dir),
    ]
    subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dataset-name", default="princeton-nlp/SWE-bench_Lite")
    parser.add_argument("--split", default="test")
    parser.add_argument("--namespace", default="swebench")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--harness-python", type=Path, default=DEFAULT_HARNESS_PYTHON)
    parser.add_argument("--harness-source", type=Path, default=DEFAULT_HARNESS_SOURCE)
    args = parser.parse_args()

    run_dir = args.run_dir.resolve()
    if not (run_dir / "trials").is_dir():
        raise SystemExit(f"not an AgentLab run directory: {run_dir}")
    if not args.harness_python.exists():
        raise SystemExit(f"official SWE-bench harness python not found: {args.harness_python}")
    if not args.harness_source.exists():
        raise SystemExit(f"official SWE-bench source checkout not found: {args.harness_source}")

    output_dir = (args.output_dir or run_dir / "official_swebench_eval").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    predictions = collect_predictions(run_dir)
    if not predictions:
        raise SystemExit("no patch predictions found in run")

    manifest = {"run_dir": str(run_dir), "variants": {}}
    for variant_id, rows in sorted(predictions.items()):
        variant_dir = output_dir / variant_id
        predictions_path = variant_dir / "predictions.jsonl"
        write_jsonl(predictions_path, rows)
        instance_ids = [row["instance_id"] for row in rows]
        manifest["variants"][variant_id] = {
            "prediction_count": len(rows),
            "instance_ids": instance_ids,
            "predictions_path": str(predictions_path),
        }
        run_harness(
            harness_python=args.harness_python,
            harness_source=args.harness_source,
            predictions_path=predictions_path,
            instance_ids=instance_ids,
            variant_id=variant_id,
            output_dir=variant_dir,
            dataset_name=args.dataset_name,
            split=args.split,
            namespace=args.namespace,
            timeout=args.timeout,
            max_workers=args.max_workers,
        )
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
