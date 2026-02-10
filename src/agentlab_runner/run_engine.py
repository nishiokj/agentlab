import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from agentlab_core.canonical_json import canonical_dumps
from agentlab_core import new_run_id, new_trial_id
from agentlab_core.hashing import sha256_bytes
from agentlab_runner.experiment_resolver import ExperimentResolver, resolve_experiment_config
from agentlab_runner.dataset_loader import load_jsonl
from agentlab_runner.harness_executor import HarnessExecutor
from agentlab_runner.harness_manifest import HarnessManifest
from agentlab_runner.hook_collector import HookCollector
from agentlab_runner.schemas import SchemaRegistry
from agentlab_runner.integration import Evidence, derive_effective_level, derive_replay_grade
from agentlab_report import build_report
from agentlab_analysis import run_analysis
from agentlab_core.artifact_store import ArtifactStore
from agentlab_runner.provenance import write_attestation, capture_sbom_stub
from agentlab_runner.debug_bundle import build_debug_bundle
from agentlab_runner.control_plane import write_control_action


def _run_root(base: str) -> str:
    return os.path.join(base, ".lab", "runs")


def _schema_dir(base_dir: str) -> str:
    # Prefer explicit override so `lab` can be run from arbitrary harness repos.
    schema_dir = os.getenv("AGENTLAB_SCHEMA_DIR")
    if schema_dir and os.path.isdir(schema_dir):
        return schema_dir

    # Default: look for a sibling `schemas/` directory relative to this package (dev mode).
    pkg_default = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "schemas"))
    if os.path.isdir(pkg_default):
        return pkg_default

    # Fallback: allow schemas in the current working directory.
    return os.path.join(base_dir, "schemas")


def _write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _variant_plan_entries(exp: Dict[str, Any]) -> List[Dict[str, Any]]:
    baseline = exp.get("baseline") or {}
    base_id = baseline.get("variant_id", "base")
    variants = [{"variant_id": base_id, "bindings": baseline.get("bindings", {})}]

    # Prefer `variant_plan`; keep `variants` for backward compatibility.
    plan = exp.get("variant_plan")
    if plan is None:
        plan = exp.get("variants", [])
    for v in plan:
        variants.append({"variant_id": v.get("variant_id"), "bindings": v.get("bindings", {})})
    return variants


def _analysis_evidence(hooks_ok: bool, traces_ok: bool) -> Dict[str, bool]:
    return {"hooks": hooks_ok, "traces": traces_ok, "framework_events": False}


def validate_resolved_experiment(resolved: Dict[str, Any]) -> Dict[str, Any]:
    if resolved.get("analysis_plan") is None:
        raise ValueError("analysis_plan is required")
    if (resolved.get("dataset") or {}).get("provider") not in ("local_jsonl", None):
        raise ValueError("Only dataset.provider=local_jsonl is supported")
    return resolved


def validate_experiment_spec(
    experiment: Dict[str, Any],
    base_dir: Optional[str] = None,
) -> Dict[str, Any]:
    resolved = resolve_experiment_config(experiment, os.path.abspath(base_dir or os.getcwd()))
    return validate_resolved_experiment(resolved)


def validate_experiment(experiment_path: str) -> Dict[str, Any]:
    resolver = ExperimentResolver(experiment_path)
    return validate_resolved_experiment(resolver.resolve())


def _run_resolved_experiment(
    resolved: Dict[str, Any],
    digest: str,
    allow_missing_manifest: bool = False,
    base_dir: Optional[str] = None,
    sdk_evidence: bool = False,
) -> Tuple[str, str]:
    run_id = new_run_id()
    base_dir = os.path.abspath(base_dir or os.getcwd())
    schema_dir = _schema_dir(base_dir)
    run_dir = os.path.join(_run_root(base_dir), run_id)

    os.makedirs(run_dir, exist_ok=True)
    _write_json(os.path.join(run_dir, "resolved_experiment.json"), resolved)
    with open(os.path.join(run_dir, "resolved_experiment.digest"), "w", encoding="utf-8") as f:
        f.write(digest)

    manifest = {
        "schema_version": "manifest_v1",
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "runner_version": "0.1",
        "resolved_experiment": {"digest": digest},
    }
    _write_json(os.path.join(run_dir, "manifest.json"), manifest)

    variant_plan_entries = _variant_plan_entries(resolved)

    # write resolved variants (artifact path kept for backward compatibility)
    variants_dir = os.path.join(run_dir, "variants")
    for v in variant_plan_entries:
        vdir = os.path.join(variants_dir, v["variant_id"])
        _write_json(os.path.join(vdir, "resolved_variant.json"), v)

    dataset = resolved.get("dataset") or {}
    tasks = load_jsonl(dataset["path"], limit=dataset.get("limit"))

    harness = resolved.get("runtime", {}).get("harness", {})
    command = harness.get("command")
    configured_integration_level = harness.get("integration_level", "cli_basic")

    registry = SchemaRegistry(schema_dir)
    executor = HarnessExecutor(registry)
    hook_collector = HookCollector(registry)

    hooks_ok = False
    traces_ok = False

    for task_idx, task in enumerate(tasks):
        for repl_idx in range(int((resolved.get("design") or {}).get("replications", 1))):
            for v in variant_plan_entries:
                trial_id = new_trial_id()
                trial_dir = os.path.join(run_dir, "trials", trial_id)
                paths = {
                    "workspace": os.path.join(trial_dir, "workspace"),
                    "state": os.path.join(trial_dir, "state"),
                    "cache": os.path.join(trial_dir, "cache"),
                    "dataset": dataset["path"],
                    "out": trial_dir,
                    "tmp": os.path.join(trial_dir, "tmp"),
                }
                for p in paths.values():
                    if p and p.startswith(run_dir):
                        os.makedirs(p, exist_ok=True)

                trial_input = {
                    "schema_version": "trial_input_v1",
                    "ids": {
                        "run_id": run_id,
                        "trial_id": trial_id,
                        "variant_id": v["variant_id"],
                        "task_id": task.get("task_id", f"task_{task_idx}"),
                        "repl_idx": repl_idx,
                    },
                    "task": task,
                    "bindings": v.get("bindings", {}),
                    "design": {
                        "sanitization_profile": (resolved.get("design") or {}).get(
                            "sanitization_profile", "hermetic_functional_v2"
                        ),
                        "integration_level": configured_integration_level,
                    },
                    "runtime": {
                        "paths": paths,
                        "network": {
                            "mode_requested": (resolved.get("runtime") or {}).get("network", {}).get(
                                "mode", "none"
                            ),
                            "allowed_hosts": (resolved.get("runtime") or {}).get("network", {}).get(
                                "allowed_hosts", []
                            ),
                        },
                        "control_plane": {
                            "mode": (harness.get("control_plane") or {}).get("mode", "file"),
                            "path": os.path.join(paths["state"], "lab_control.json"),
                        },
                    },
                }

                # Initialize control-plane file so cli_events harnesses can compute control_version.
                write_control_action(
                    trial_input["runtime"]["control_plane"]["path"], {"action": "continue"}
                )

                input_path = os.path.join(trial_dir, "trial_input.json")
                output_path = os.path.join(trial_dir, "trial_output.json")
                env = os.environ.copy()
                env["AGENTLAB_TRIAL_INPUT"] = input_path
                env["AGENTLAB_TRIAL_OUTPUT"] = output_path
                # Local runner: treat the trial dir as /out so harness can read/write by convention.
                executor.run(command, trial_input, input_path, output_path, env=env, cwd=trial_dir)

                # copy metrics for analysis convenience
                with open(output_path, "r", encoding="utf-8") as f:
                    output = json.load(f)
                _write_json(os.path.join(trial_dir, "metrics.json"), output)

                manifest_path = os.path.join(trial_dir, "harness_manifest.json")
                if configured_integration_level != "cli_basic":
                    if not os.path.exists(manifest_path):
                        if allow_missing_manifest:
                            # Best effort mode: continue this trial without manifest-derived evidence.
                            pass
                        else:
                            raise FileNotFoundError("harness_manifest.json required")

                if os.path.exists(manifest_path):
                    manifest_obj = HarnessManifest.load(manifest_path, registry)
                    events_path = os.path.join(trial_dir, "harness_events.jsonl")
                    if os.path.exists(events_path):
                        hook_collector.collect(events_path, manifest_obj)
                        hooks_ok = True

    # Run analysis + report
    evidence = _analysis_evidence(hooks_ok, traces_ok)
    baseline_id = (resolved.get("baseline") or {}).get("variant_id", "base")
    variant_ids = [v["variant_id"] for v in variant_plan_entries if v["variant_id"] != baseline_id]
    if variant_ids:
        run_analysis(run_dir, baseline_id, variant_ids, evidence)
    else:
        # Ensure report can still render even for single-variant runs.
        _write_json(os.path.join(run_dir, "analysis", "comparisons.json"), {"comparisons": []})

    report_dir = os.path.join(run_dir, "report")
    build_report(run_dir, report_dir)

    # write grades summary (best effort)
    evidence_flags = Evidence(hooks=hooks_ok, traces=traces_ok)
    effective_level = derive_effective_level(configured_integration_level, evidence_flags)
    grades = {
        "schema_version": "grades_v1",
        "integration_level": effective_level,
        "replay_grade": derive_replay_grade(effective_level, has_checkpoints=False),
        "isolation_grade": "leaky",
        "comparability_grade": "unknown",
        "provenance_grade": "partial",
        "privacy_grade": "unknown",
        "evidence": {"hooks": hooks_ok, "traces": traces_ok, "sdk": sdk_evidence},
    }
    grades_path = os.path.join(run_dir, "grades.json")
    _write_json(grades_path, grades)

    # provenance + attestation
    artifact_store = ArtifactStore(os.path.join(run_dir, "artifacts"))
    sbom_ref = capture_sbom_stub(run_dir, artifact_store)
    hooks_schema_version = None
    if hooks_ok and os.path.exists(os.path.join(run_dir, "trials")):
        # best effort: check first manifest
        for trial_id in os.listdir(os.path.join(run_dir, "trials")):
            mpath = os.path.join(run_dir, "trials", trial_id, "harness_manifest.json")
            if os.path.exists(mpath):
                manifest_obj = HarnessManifest.load(mpath, registry)
                hooks_schema_version = manifest_obj.hooks_schema_version
                break
    write_attestation(
        run_dir=run_dir,
        artifact_store=artifact_store,
        grades=grades,
        hooks_schema_version=hooks_schema_version,
        trace_ingestion={"mode": "hooks"} if hooks_ok else {"mode": "none"},
        sbom_artifact_ref=sbom_ref,
    )

    # debug bundle (optional)
    bundle_path = os.path.join(run_dir, "debug_bundles", f"{run_id}.zip")
    build_debug_bundle(run_dir, bundle_path)

    return run_id, report_dir


def run_experiment_spec(
    experiment: Dict[str, Any],
    allow_missing_manifest: bool = False,
    resolution_base_dir: Optional[str] = None,
    run_base_dir: Optional[str] = None,
) -> Tuple[str, str]:
    effective_run_base = os.path.abspath(run_base_dir or os.getcwd())
    effective_resolution_base = os.path.abspath(
        resolution_base_dir or run_base_dir or os.getcwd()
    )
    resolved = validate_experiment_spec(experiment, base_dir=effective_resolution_base)
    digest = sha256_bytes(canonical_dumps(resolved).encode("utf-8"))
    return _run_resolved_experiment(
        resolved,
        digest,
        allow_missing_manifest=allow_missing_manifest,
        base_dir=effective_run_base,
        sdk_evidence=True,
    )


def run_experiment(
    experiment_path: str,
    allow_missing_manifest: bool = False,
) -> Tuple[str, str]:
    resolver = ExperimentResolver(experiment_path)
    resolved = validate_resolved_experiment(resolver.resolve())
    digest = resolver.digest(resolved)
    return _run_resolved_experiment(
        resolved,
        digest,
        allow_missing_manifest=allow_missing_manifest,
        base_dir=os.getcwd(),
        sdk_evidence=False,
    )


def _find_trial(trial_id: str, base_dir: str) -> Tuple[str, str]:
    root = _run_root(base_dir)
    if not os.path.isdir(root):
        raise FileNotFoundError("No runs directory found")
    for run_id in os.listdir(root):
        tdir = os.path.join(root, run_id, "trials", trial_id)
        if os.path.isdir(tdir):
            return run_id, tdir
    raise FileNotFoundError("Trial not found")


def replay_trial(trial_id: str, strict: bool = False) -> str:
    if strict:
        raise RuntimeError("Strict replay is not supported in this runner")
    base_dir = os.getcwd()
    run_id, tdir = _find_trial(trial_id, base_dir)
    with open(os.path.join(_run_root(base_dir), run_id, "resolved_experiment.json"), "r", encoding="utf-8") as f:
        resolved = json.load(f)
    harness = resolved.get("runtime", {}).get("harness", {})

    input_path = os.path.join(tdir, "trial_input.json")
    replay_out = os.path.join(tdir, "trial_output_replay.json")
    # Ensure control-plane file exists for harnesses that expect it.
    cp_path = os.path.join(tdir, "state", "lab_control.json")
    if not os.path.exists(cp_path):
        write_control_action(cp_path, {"action": "continue"})

    registry = SchemaRegistry(_schema_dir(base_dir))
    executor = HarnessExecutor(registry)
    with open(input_path, "r", encoding="utf-8") as f:
        trial_input = json.load(f)
    env = os.environ.copy()
    env["AGENTLAB_TRIAL_INPUT"] = input_path
    env["AGENTLAB_TRIAL_OUTPUT"] = replay_out
    executor.run(harness.get("command"), trial_input, input_path, replay_out, env=env, cwd=tdir)
    return replay_out


def fork_trial(trial_id: str, at_selector: str, bindings: Dict[str, str]) -> str:
    base_dir = os.getcwd()
    run_id, tdir = _find_trial(trial_id, base_dir)
    with open(os.path.join(_run_root(base_dir), run_id, "resolved_experiment.json"), "r", encoding="utf-8") as f:
        resolved = json.load(f)
    harness = resolved.get("runtime", {}).get("harness", {})

    with open(os.path.join(tdir, "trial_input.json"), "r", encoding="utf-8") as f:
        trial_input = json.load(f)

    forked_trial_id = new_trial_id()
    new_dir = os.path.join(_run_root(base_dir), run_id, "trials", forked_trial_id)
    os.makedirs(new_dir, exist_ok=True)

    trial_input["ids"]["trial_id"] = forked_trial_id
    trial_input.setdefault("ext", {})["fork"] = {
        "parent_trial_id": trial_id,
        "at": at_selector,
        "bindings": bindings,
    }
    trial_input["bindings"].update(bindings)

    runtime = trial_input.setdefault("runtime", {})
    paths = runtime.setdefault("paths", {})
    paths["workspace"] = os.path.join(new_dir, "workspace")
    paths["state"] = os.path.join(new_dir, "state")
    paths["cache"] = os.path.join(new_dir, "cache")
    paths["out"] = new_dir
    paths["tmp"] = os.path.join(new_dir, "tmp")
    runtime["control_plane"] = {
        **(runtime.get("control_plane") or {}),
        "path": os.path.join(paths["state"], "lab_control.json"),
    }

    input_path = os.path.join(new_dir, "trial_input.json")
    output_path = os.path.join(new_dir, "trial_output.json")
    cp_path = os.path.join(new_dir, "state", "lab_control.json")
    write_control_action(cp_path, {"action": "continue"})

    registry = SchemaRegistry(_schema_dir(base_dir))
    executor = HarnessExecutor(registry)
    env = os.environ.copy()
    env["AGENTLAB_TRIAL_INPUT"] = input_path
    env["AGENTLAB_TRIAL_OUTPUT"] = output_path
    executor.run(harness.get("command"), trial_input, input_path, output_path, env=env, cwd=new_dir)

    return forked_trial_id
