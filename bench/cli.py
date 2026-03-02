"""CLI entrypoint for benchmark generation and task-set management.

Usage:
    python -m bench.cli --help
    python -m bench.cli validate-schemas
    python -m bench.cli validate-task <path> [--strict]
    python -m bench.cli validate-suite <suite> [--jobs N] [--repeat N] [--check-determinism] [--strict]
    python -m bench.cli import-suite --source <path> --suite <name> --repo-map <json>
    python -m bench.cli admit-task <task_dir>
    python -m bench.cli repo-smoke --repo <repo_id>
    python -m bench.cli new-task <task_id> --repo <repo_id>
    python -m bench.cli suite-summary <suite> --out <path>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from bench.config import BenchConfig

BENCH_ROOT = Path(__file__).resolve().parent.parent


@click.group()
@click.version_option(version="0.1.0", prog_name="bench")
@click.pass_context
def main(ctx: click.Context) -> None:
    """Task generation and validation CLI for bench task sets."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = BenchConfig.from_root(BENCH_ROOT)


@main.command("validate-schemas")
@click.pass_context
def validate_schemas(ctx: click.Context) -> None:
    """Validate all schemas in schemas/."""
    from bench.taskkit.schema import validate_all_schemas

    cfg = ctx.obj["config"]
    errors = validate_all_schemas(cfg.schemas_dir)
    if errors:
        for e in errors:
            click.echo(f"FAIL: {e}", err=True)
        sys.exit(1)
    click.echo("All schemas valid.")


@main.command("validate-task")
@click.argument("task_path", type=click.Path(exists=True))
@click.option("--strict", is_flag=True, help="Enable strict execution gates")
@click.pass_context
def validate_task(ctx: click.Context, task_path: str, strict: bool) -> None:
    """Validate a single task bundle."""
    from bench.taskkit.validate_task import run_validate_task

    cfg = ctx.obj["config"]
    result = run_validate_task(Path(task_path).resolve(), cfg, strict=strict, write_report=True)
    click.echo(json.dumps(result, indent=2, sort_keys=True))
    if not result.get("valid", False):
        sys.exit(1)


@main.command("validate-suite")
@click.argument("suite")
@click.option("--jobs", default=1, type=int, help="Parallel workers")
@click.option("--repeat", default=1, type=int, help="Repeat count for determinism check")
@click.option("--check-determinism", is_flag=True, help="Compare repeated runs for determinism")
@click.option("--strict", is_flag=True, help="Enable strict per-task validation gates")
@click.option("--out", type=click.Path(), default=None)
@click.pass_context
def validate_suite(
    ctx: click.Context,
    suite: str,
    jobs: int,
    repeat: int,
    check_determinism: bool,
    strict: bool,
    out: str | None,
) -> None:
    """Validate all tasks in a suite."""
    from bench.taskkit.validate_task import run_validate_suite

    cfg = ctx.obj["config"]
    result = run_validate_suite(
        suite=suite,
        config=cfg,
        jobs=jobs,
        repeat=repeat,
        check_determinism=check_determinism,
        strict=strict,
    )
    output = json.dumps(result, indent=2, sort_keys=True)
    if out:
        Path(out).write_text(output)
    click.echo(output)
    if not result.get("all_valid", False):
        sys.exit(1)


@main.command("import-suite")
@click.option("--source", required=True, type=click.Path(exists=True))
@click.option("--suite", required=True)
@click.option(
    "--repo-map",
    required=True,
    help="JSON object mapping source repo IDs to canonical repo IDs",
)
@click.pass_context
def import_suite(ctx: click.Context, source: str, suite: str, repo_map: str) -> None:
    """Import external tasks into canonical benchmark layout."""
    from bench.taskkit.importer import import_suite as _import_suite

    cfg = ctx.obj["config"]
    try:
        mapping = json.loads(repo_map)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"Invalid --repo-map JSON: {exc}") from exc

    result = _import_suite(
        source=Path(source).resolve(),
        suite=suite,
        repo_map=mapping,
        config=cfg,
    )
    click.echo(json.dumps(result, indent=2, sort_keys=True))


@main.command("admit-task")
@click.argument("task_dir", type=click.Path(exists=True))
@click.pass_context
def admit_task(ctx: click.Context, task_dir: str) -> None:
    """Admit a task only if strict validation passes."""
    from bench.taskkit.admission import admit_task as _admit

    cfg = ctx.obj["config"]
    try:
        result = _admit(Path(task_dir).resolve(), cfg, strict=True)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Admission failed: {exc}", err=True)
        sys.exit(1)
    click.echo(json.dumps(result, indent=2, sort_keys=True))


@main.command("repo-smoke")
@click.option("--repo", required=True, help="Repo ID (for example: jesus)")
@click.pass_context
def repo_smoke(ctx: click.Context, repo: str) -> None:
    """Run a minimal smoke check for a pinned repo snapshot."""
    cfg = ctx.obj["config"]
    repo_dir = cfg.repos_dir / repo
    if not repo_dir.exists():
        click.echo(f"Repo directory not found: {repo_dir}", err=True)
        sys.exit(1)
    click.echo(f"Repo smoke for {repo}: OK")


@main.command("new-task")
@click.argument("task_id")
@click.option("--repo", required=True, help="Target repo ID")
@click.option("--suite", default="v0", help="Suite name")
@click.pass_context
def new_task(ctx: click.Context, task_id: str, repo: str, suite: str) -> None:
    """Create a new task from the template."""
    import shutil

    cfg = ctx.obj["config"]
    template_dir = cfg.bench_dir / "taskkit" / "templates" / "TASK_TEMPLATE"
    target_dir = cfg.tasks_dir / suite / task_id
    if target_dir.exists():
        click.echo(f"Task already exists: {target_dir}", err=True)
        sys.exit(1)
    shutil.copytree(template_dir, target_dir)
    click.echo(f"Created task at {target_dir}")


@main.command("suite-summary")
@click.argument("suite")
@click.option("--out", required=True, type=click.Path())
@click.pass_context
def suite_summary(ctx: click.Context, suite: str, out: str) -> None:
    """Generate a suite validation summary."""
    from bench.taskkit.validate_task import generate_suite_summary

    cfg = ctx.obj["config"]
    result = generate_suite_summary(suite, cfg)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(json.dumps(result, indent=2, sort_keys=True))
    click.echo(f"Suite summary written to {out}")


if __name__ == "__main__":
    main()
