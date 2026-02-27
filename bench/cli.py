"""CLI entrypoint for the benchmark suite.

Usage:
    python -m bench.cli --help
    python -m bench.cli validate-schemas
    python -m bench.cli validate-task <path>
    python -m bench.cli validate-suite <suite> [--jobs N] [--repeat N] [--check-determinism]
    python -m bench.cli build-images
    python -m bench.cli run --suite <suite> --agent <agent> --runs-dir <dir> [--max-tasks N]
    python -m bench.cli grade --run-dir <dir>
    python -m bench.cli report --runs <dir> --out <dir>
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
    """Deterministic offline benchmark for coding agents."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = BenchConfig.from_root(BENCH_ROOT)


# ---------------------------------------------------------------------------
# validate-schemas
# ---------------------------------------------------------------------------
@main.command("validate-schemas")
@click.pass_context
def validate_schemas(ctx: click.Context) -> None:
    """Validate all JSON schemas in schemas/ are well-formed."""
    from bench.taskkit.schema import validate_all_schemas

    cfg = ctx.obj["config"]
    errors = validate_all_schemas(cfg.schemas_dir)
    if errors:
        for e in errors:
            click.echo(f"FAIL: {e}", err=True)
        sys.exit(1)
    click.echo("All schemas valid.")


# ---------------------------------------------------------------------------
# validate-task
# ---------------------------------------------------------------------------
@main.command("validate-task")
@click.argument("task_path", type=click.Path(exists=True))
@click.pass_context
def validate_task(ctx: click.Context) -> None:
    """Validate a single task bundle against all acceptance criteria."""
    from bench.taskkit.validate_task import run_validate_task

    cfg = ctx.obj["config"]
    task_path = Path(ctx.params["task_path"]).resolve()
    result = run_validate_task(task_path, cfg)
    click.echo(json.dumps(result, indent=2))
    if not result.get("valid", False):
        sys.exit(1)


# ---------------------------------------------------------------------------
# validate-suite
# ---------------------------------------------------------------------------
@main.command("validate-suite")
@click.argument("suite")
@click.option("--jobs", default=1, type=int, help="Parallel workers")
@click.option("--repeat", default=1, type=int, help="Repeat count for determinism check")
@click.option("--check-determinism", is_flag=True, help="Compare repeated runs for determinism")
@click.option("--out", type=click.Path(), default=None)
@click.pass_context
def validate_suite(ctx: click.Context, suite: str, jobs: int, repeat: int,
                   check_determinism: bool, out: str | None) -> None:
    """Validate all tasks in a suite."""
    from bench.taskkit.validate_task import run_validate_suite

    cfg = ctx.obj["config"]
    result = run_validate_suite(
        suite=suite, config=cfg, jobs=jobs, repeat=repeat,
        check_determinism=check_determinism,
    )
    output = json.dumps(result, indent=2)
    if out:
        Path(out).write_text(output)
    click.echo(output)
    if not result.get("all_valid", False):
        sys.exit(1)


# ---------------------------------------------------------------------------
# build-images
# ---------------------------------------------------------------------------
@main.command("build-images")
@click.option("--no-cache", is_flag=True)
@click.pass_context
def build_images(ctx: click.Context, no_cache: bool) -> None:
    """Build Docker images for agent and grader sandboxes."""
    from bench.runner.sandbox import build_images as _build

    cfg = ctx.obj["config"]
    _build(cfg, no_cache=no_cache)
    click.echo("Images built successfully.")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------
@main.command("run")
@click.option("--suite", required=True, help="Suite name (e.g. v0)")
@click.option("--agent", required=True, help="Agent identifier")
@click.option("--runs-dir", required=True, type=click.Path(), help="Output runs directory")
@click.option("--max-tasks", default=None, type=int, help="Limit number of tasks")
@click.option("--timeout", default=1200, type=int, help="Per-task agent timeout (seconds)")
@click.pass_context
def run(ctx: click.Context, suite: str, agent: str, runs_dir: str,
        max_tasks: int | None, timeout: int) -> None:
    """Run an agent on benchmark tasks."""
    from bench.runner.agent_runner import run_agent_suite

    cfg = ctx.obj["config"]
    run_agent_suite(
        suite=suite, agent=agent, runs_dir=Path(runs_dir),
        config=cfg, max_tasks=max_tasks, timeout=timeout,
    )


# ---------------------------------------------------------------------------
# grade
# ---------------------------------------------------------------------------
@main.command("grade")
@click.option("--run-dir", required=True, type=click.Path(exists=True))
@click.pass_context
def grade(ctx: click.Context, run_dir: str) -> None:
    """Grade an agent run and produce score.json."""
    from bench.runner.grader_runner import grade_run

    cfg = ctx.obj["config"]
    result = grade_run(Path(run_dir), cfg)
    click.echo(json.dumps(result, indent=2))


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------
@main.command("report")
@click.option("--runs", required=True, type=click.Path(exists=True))
@click.option("--out", required=True, type=click.Path())
@click.pass_context
def report(ctx: click.Context, runs: str, out: str) -> None:
    """Generate aggregate reports from run results."""
    from bench.runner.reporting import generate_report

    cfg = ctx.obj["config"]
    generate_report(runs_dir=Path(runs), out_dir=Path(out), config=cfg)
    click.echo(f"Report generated at {out}")


# ---------------------------------------------------------------------------
# repo-smoke
# ---------------------------------------------------------------------------
@main.command("repo-smoke")
@click.option("--repo", required=True, help="Repo ID (e.g. click, rich, jinja2)")
@click.pass_context
def repo_smoke(ctx: click.Context, repo: str) -> None:
    """Run a minimal smoke test for a pinned repo snapshot."""
    cfg = ctx.obj["config"]
    repo_dir = cfg.repos_dir / repo
    if not repo_dir.exists():
        click.echo(f"Repo directory not found: {repo_dir}", err=True)
        sys.exit(1)
    click.echo(f"Repo smoke for {repo}: OK (stub)")


# ---------------------------------------------------------------------------
# new-task
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# suite-summary
# ---------------------------------------------------------------------------
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
    Path(out).write_text(json.dumps(result, indent=2))
    click.echo(f"Suite summary written to {out}")


if __name__ == "__main__":
    main()
