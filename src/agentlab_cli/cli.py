import argparse
import json
import os
import sys
from typing import List

from agentlab_runner import SchemaRegistry, HarnessManifest, HookCollector
from agentlab_analysis import run_analysis
from agentlab_report import build_report


def _schema_dir() -> str:
    env = os.getenv("AGENTLAB_SCHEMA_DIR")
    if env and os.path.isdir(env):
        return env
    cwd = os.getcwd()
    candidate = os.path.join(cwd, "schemas")
    if os.path.isdir(candidate):
        return candidate
    raise FileNotFoundError("Schema directory not found; set AGENTLAB_SCHEMA_DIR")


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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
    manifest = HarnessManifest.load(args.manifest, registry)
    collector = HookCollector(registry)
    result = collector.collect(args.events, manifest)
    print(f"OK: {len(result.events)} events, turn_count={result.turn_count}")
    return 0


def cmd_analyze(args: argparse.Namespace) -> int:
    evidence = {
        "hooks": args.evidence_hooks,
        "traces": args.evidence_traces,
        "framework_events": args.evidence_framework,
    }
    run_analysis(
        run_dir=args.run_dir,
        baseline_id=args.baseline,
        variant_ids=args.variant,
        evidence_sources=evidence,
        random_seed=args.seed,
    )
    print("Analysis complete")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    out_dir = args.out_dir or os.path.join(args.run_dir, "report")
    build_report(args.run_dir, out_dir)
    print(f"Report written to {out_dir}")
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    evidence = {
        "hooks": args.evidence_hooks,
        "traces": args.evidence_traces,
        "framework_events": args.evidence_framework,
    }
    run_analysis(
        run_dir=args.run_dir,
        baseline_id=args.baseline,
        variant_ids=args.variant,
        evidence_sources=evidence,
        random_seed=args.seed,
    )
    out_dir = args.out_dir or os.path.join(args.run_dir, "report")
    build_report(args.run_dir, out_dir)
    print(f"Compare complete. Report at {out_dir}")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    print("AgentLab doctor")
    try:
        schema_dir = _schema_dir()
        print(f"schema_dir: {schema_dir}")
    except Exception as e:
        print(f"schema_dir: error: {e}")
        return 1
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lab")
    sub = parser.add_subparsers(dest="command", required=True)

    p_schema = sub.add_parser("schema-validate", help="Validate JSON against a schema")
    p_schema.add_argument("--schema", required=True, help="Schema filename or path")
    p_schema.add_argument("--file", required=True, help="JSON file to validate")
    p_schema.set_defaults(func=cmd_schema_validate)

    p_hooks = sub.add_parser("hooks-validate", help="Validate harness events JSONL")
    p_hooks.add_argument("--manifest", required=True, help="harness_manifest.json")
    p_hooks.add_argument("--events", required=True, help="harness_events.jsonl")
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

    p_validate = sub.add_parser("validate", help="(stub) Validate experiment YAML")
    p_validate.set_defaults(func=lambda args: cmd_not_implemented("validate"))

    p_run = sub.add_parser("run", help="(stub) Run experiment")
    p_run.set_defaults(func=lambda args: cmd_not_implemented("run"))

    p_replay = sub.add_parser("replay", help="(stub) Replay trial")
    p_replay.set_defaults(func=lambda args: cmd_not_implemented("replay"))

    p_fork = sub.add_parser("fork", help="(stub) Fork trial at step")
    p_fork.set_defaults(func=lambda args: cmd_not_implemented("fork"))

    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
