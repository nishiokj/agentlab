import argparse
import json
import os
import sys
import subprocess
from typing import List, Optional

from agentlab_runner import SchemaRegistry, HarnessManifest, HookCollector, HookValidationError
from agentlab_cli.init_scaffold import scaffold
from agentlab_sdk import AgentLabClient


def _schema_dir() -> str:
    env = os.getenv("AGENTLAB_SCHEMA_DIR")
    if env and os.path.isdir(env):
        return env
    cwd = os.getcwd()
    candidate = os.path.join(cwd, "schemas")
    if os.path.isdir(candidate):
        return candidate
    # Installed mode: schemas are embedded as package resources. Return any existing
    # directory so SchemaRegistry can fall back to package resources on missing files.
    return cwd


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _client() -> AgentLabClient:
    return AgentLabClient(base_dir=os.getcwd())


def cmd_schema_validate(args: argparse.Namespace) -> int:
    schema_dir = _schema_dir()
    registry = SchemaRegistry(schema_dir)

    schema_name = args.schema
    if os.path.exists(schema_name):
        # allow absolute path by copying into registry cache
        with open(schema_name, "r", encoding="utf-8") as f:
            schema = json.load(f)
        registry._cache[os.path.basename(schema_name)] = schema
        schema_name = os.path.basename(schema_name)

    data = _load_json(args.file)
    registry.validate(schema_name, data)
    print("OK")
    return 0


def cmd_hooks_validate(args: argparse.Namespace) -> int:
    registry = SchemaRegistry(_schema_dir())
    if bool(args.run_dir) == bool(args.manifest):
        raise ValueError("Provide either --run-dir or (--manifest and --events)")
    if args.manifest and not args.events:
        raise ValueError("--events is required when using --manifest")
    if args.run_dir:
        run_dir = args.run_dir
        trials_dir = os.path.join(run_dir, "trials")
        if args.trial_id:
            trial_dir = os.path.join(trials_dir, args.trial_id)
            if not os.path.isdir(trial_dir):
                raise FileNotFoundError("trial_id not found under run-dir")
        else:
            trial_ids = [t for t in os.listdir(trials_dir) if os.path.isdir(os.path.join(trials_dir, t))]
            if not trial_ids:
                raise FileNotFoundError("No trials found under run-dir")
            trial_dir = os.path.join(trials_dir, sorted(trial_ids)[0])
        manifest_path = os.path.join(trial_dir, "harness_manifest.json")
        events_path = os.path.join(trial_dir, "harness_events.jsonl")
    else:
        manifest_path = args.manifest
        events_path = args.events

    if not os.path.exists(manifest_path):
        print(
            f"error: missing {manifest_path}. This trial likely ran with integration_level=cli_basic."
        )
        print("hint: re-run with `lab init --integration-level cli_events` (or set it in experiment.yaml).")
        return 1
    if not os.path.exists(events_path):
        print(f"error: missing {events_path}. Harness did not emit hook events.")
        return 1

    manifest = HarnessManifest.load(manifest_path, registry)
    collector = HookCollector(registry)
    try:
        result = collector.collect(events_path, manifest)
    except HookValidationError as e:
        if args.explain:
            _print_hook_explain(e)
        else:
            print(f"error: {e}")
        return 1
    except Exception as e:
        if args.explain:
            print(f"error: {e}")
        else:
            print(f"error: {e}")
        return 1

    print(f"OK: {len(result.events)} events, turn_count={result.turn_count}")
    return 0


def _print_hook_explain(e: HookValidationError) -> None:
    parts = ["Hook validation failed."]
    if e.line_num is not None:
        parts.append(f"line: {e.line_num}")
    if e.seq is not None:
        parts.append(f"seq: {e.seq}")
    if e.event_type is not None:
        parts.append(f"event_type: {e.event_type}")
    print(" | ".join(parts))
    print(f"message: {e}")
    if e.details:
        print(f"details: {e.details}")
    if e.raw_line:
        print("event_line:")
        print(e.raw_line)


def cmd_analyze(args: argparse.Namespace) -> int:
    evidence = {
        "hooks": args.evidence_hooks,
        "traces": args.evidence_traces,
        "framework_events": args.evidence_framework,
    }
    _client().analyze(
        run_dir=args.run_dir,
        baseline_id=args.baseline,
        variant_ids=args.variant,
        evidence_sources=evidence,
        random_seed=args.seed,
    )
    print("Analysis complete")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    out_dir = _client().report(run_dir=args.run_dir, out_dir=args.out_dir)
    print(f"Report written to {out_dir}")
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    evidence = {
        "hooks": args.evidence_hooks,
        "traces": args.evidence_traces,
        "framework_events": args.evidence_framework,
    }
    out_dir = _client().compare(
        run_dir=args.run_dir,
        baseline_id=args.baseline,
        variant_ids=args.variant,
        evidence_sources=evidence,
        random_seed=args.seed,
        out_dir=args.out_dir,
    )
    print(f"Compare complete. Report at {out_dir}")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    print("AgentLab doctor")
    schema_dir = _schema_dir()
    print(f"schema_dir: {schema_dir} (files if present; otherwise package resources)")
    try:
        import jsonschema  # noqa: F401
        print("jsonschema: OK")
    except Exception as e:
        print(f"jsonschema: error: {e}")
        return 1
    try:
        import pyarrow  # noqa: F401
        print("pyarrow: OK (optional)")
    except Exception:
        print("pyarrow: not installed (optional)")
    return 0


def cmd_not_implemented(name: str) -> int:
    print(f"Command '{name}' is not implemented yet.")
    return 2


def cmd_validate(args: argparse.Namespace) -> int:
    _client().validate(args.experiment)
    print("OK")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    result = _client().run(
        args.experiment,
        allow_missing_harness_manifest=args.allow_missing_harness_manifest,
    )
    run_id = result.run_id
    report_dir = result.report_dir
    report_path = os.path.join(report_dir, "index.html")
    print(f"run_id: {run_id}")
    print(f"report: {report_dir}")
    if args.open:
        _open_path(report_path)
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    out_path = _client().replay(args.trial_id, strict=args.strict)
    print(f"replay_output: {out_path}")
    return 0


def cmd_fork(args: argparse.Namespace) -> int:
    bindings = {}
    for item in args.set or []:
        if "=" not in item:
            raise ValueError("--set must be key=value")
        k, v = item.split("=", 1)
        bindings[k] = v
    new_trial_id = _client().fork(args.from_trial, args.at, bindings)
    print(f"forked_trial_id: {new_trial_id}")
    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    bundle = _client().publish(args.run_dir, args.out)
    print(f"bundle: {bundle}")
    return 0


def _open_path(path: str) -> None:
    if not os.path.exists(path):
        return
    if sys.platform.startswith("darwin"):
        subprocess.run(["open", path], check=False)
        return
    if sys.platform.startswith("linux"):
        subprocess.run(["xdg-open", path], check=False)
        return
    if sys.platform.startswith("win"):
        subprocess.run(["cmd", "/c", "start", path], check=False)


def cmd_init(args: argparse.Namespace) -> int:
    repo_dir = os.path.abspath(args.dir)
    experiment_path = os.path.join(repo_dir, args.experiment)
    tasks_path = os.path.join(repo_dir, args.tasks)
    manifest_path = os.path.join(repo_dir, args.manifest)

    cmd = args.command if args.command else None
    created, warnings = scaffold(
        repo_dir=repo_dir,
        experiment_path=experiment_path,
        tasks_path=tasks_path,
        manifest_path=manifest_path,
        command=cmd,
        integration_level=args.integration_level,
        step_semantics=args.step_semantics,
        demo_harness=args.demo_harness,
        typescript_wrapper=args.typescript_wrapper,
        force=args.force,
    )
    for path in created:
        print(f"created: {path}")
    for w in warnings:
        print(f"warning: {w}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lab")
    sub = parser.add_subparsers(dest="command", required=True)

    p_schema = sub.add_parser("schema-validate", help="Validate JSON against a schema")
    p_schema.add_argument("--schema", required=True, help="Schema filename or path")
    p_schema.add_argument("--file", required=True, help="JSON file to validate")
    p_schema.set_defaults(func=cmd_schema_validate)

    p_hooks = sub.add_parser("hooks-validate", help="Validate harness events JSONL")
    p_hooks.add_argument("--manifest", help="harness_manifest.json")
    p_hooks.add_argument("--events", help="harness_events.jsonl")
    p_hooks.add_argument("--run-dir", help="Run directory to validate a trial from")
    p_hooks.add_argument("--trial-id", help="Trial id under run-dir/trials (defaults to first)")
    p_hooks.add_argument(
        "--explain",
        action="store_true",
        help="Print detailed failure context (line/seq/event_type and the failing line)",
    )
    p_hooks.set_defaults(func=cmd_hooks_validate)

    p_analyze = sub.add_parser("analyze", help="Run analysis plan for a run directory")
    p_analyze.add_argument("--run-dir", required=True)
    p_analyze.add_argument("--baseline", required=True)
    p_analyze.add_argument("--variant", required=True, action="append")
    p_analyze.add_argument("--seed", type=int, default=1337)
    p_analyze.add_argument("--evidence-hooks", action="store_true")
    p_analyze.add_argument("--evidence-traces", action="store_true")
    p_analyze.add_argument("--evidence-framework", action="store_true")
    p_analyze.set_defaults(func=cmd_analyze)

    p_report = sub.add_parser("report", help="Build a static HTML report")
    p_report.add_argument("--run-dir", required=True)
    p_report.add_argument("--out-dir")
    p_report.set_defaults(func=cmd_report)

    p_compare = sub.add_parser("compare", help="Run analysis and build report")
    p_compare.add_argument("--run-dir", required=True)
    p_compare.add_argument("--baseline", required=True)
    p_compare.add_argument("--variant", required=True, action="append")
    p_compare.add_argument("--seed", type=int, default=1337)
    p_compare.add_argument("--evidence-hooks", action="store_true")
    p_compare.add_argument("--evidence-traces", action="store_true")
    p_compare.add_argument("--evidence-framework", action="store_true")
    p_compare.add_argument("--out-dir")
    p_compare.set_defaults(func=cmd_compare)

    p_doctor = sub.add_parser("doctor", help="Check environment and schemas")
    p_doctor.set_defaults(func=cmd_doctor)

    p_validate = sub.add_parser("validate", help="Validate experiment YAML")
    p_validate.add_argument("experiment", help="Path to experiment.yaml")
    p_validate.set_defaults(func=cmd_validate)

    p_run = sub.add_parser("run", help="Run experiment")
    p_run.add_argument("experiment", help="Path to experiment.yaml")
    p_run.add_argument("--allow-missing-harness-manifest", action="store_true")
    p_run.add_argument("--open", action="store_true", help="Open the HTML report after the run")
    p_run.set_defaults(func=cmd_run)

    p_replay = sub.add_parser("replay", help="Replay trial (best effort)")
    p_replay.add_argument("trial_id")
    p_replay.add_argument("--strict", action="store_true")
    p_replay.set_defaults(func=cmd_replay)

    p_fork = sub.add_parser("fork", help="Fork trial at step (best effort)")
    p_fork.add_argument("--from", dest="from_trial", required=True)
    p_fork.add_argument("--at", required=True)
    p_fork.add_argument("--set", action="append")
    p_fork.set_defaults(func=cmd_fork)

    p_publish = sub.add_parser("publish", help="Package an auditable bundle")
    p_publish.add_argument("--run-dir", required=True)
    p_publish.add_argument("--out", help="Output zip path")
    p_publish.set_defaults(func=cmd_publish)

    p_init = sub.add_parser("init", help="Scaffold experiment and dataset files")
    p_init.add_argument("--dir", default=".", help="Target directory (default: .)")
    p_init.add_argument("--experiment", default="experiment.yaml")
    p_init.add_argument("--tasks", default="tasks.jsonl")
    p_init.add_argument("--manifest", default="harness_manifest.json")
    p_init.add_argument("--force", action="store_true")
    p_init.add_argument("--command", nargs="+", help="Harness command array")
    p_init.add_argument(
        "--integration-level",
        default="cli_basic",
        choices=["cli_basic", "cli_events", "otel", "sdk_control", "sdk_full"],
    )
    p_init.add_argument("--step-semantics", default="none")
    p_init.add_argument(
        "--demo-harness",
        default="none",
        choices=["none", "node", "python"],
        help="Create a runnable demo harness in the target directory",
    )
    p_init.add_argument(
        "--typescript-wrapper",
        action="store_true",
        help="Create agentlab/harness.ts + a runnable agentlab/harness.js wrapper and update command to use it",
    )
    p_init.set_defaults(func=cmd_init)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
