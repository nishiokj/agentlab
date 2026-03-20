from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import tarfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
LAB_RUNNER_SRC_DIR = REPO_ROOT / "rust" / "crates" / "lab-runner" / "src"
LAB_RUNNER_RESET_SPEC_PATH = REPO_ROOT / "docs" / "LAB_RUNNER_CURRENT_STATE_RESET_SPEC.md"
DEFAULT_RUN_TIMEOUT_SECONDS = 240

pytestmark = pytest.mark.e2e_cli


def _docker_auths_without_creds_store() -> dict[str, Any]:
    config_path = Path.home() / ".docker" / "config.json"
    if not config_path.exists():
        return {"auths": {}}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"auths": {}}
    auths = payload.get("auths")
    if isinstance(auths, dict):
        return {"auths": auths}
    return {"auths": {}}


@dataclass(frozen=True)
class ProjectLayout:
    root: Path
    lab_dir: Path
    experiments_dir: Path
    builds_dir: Path
    runs_dir: Path
    agents_dir: Path
    dataset_packs_dir: Path


def _run(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected_exit: int = 0,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env is not None:
        merged_env.update(env)
    proc = subprocess.run(
        args,
        cwd=str(cwd),
        env=merged_env,
        capture_output=True,
        text=True,
        check=False,
        timeout=DEFAULT_RUN_TIMEOUT_SECONDS,
    )
    if proc.returncode != expected_exit:
        raise AssertionError(
            "command failed\n"
            f"args={args}\n"
            f"cwd={cwd}\n"
            f"expected_exit={expected_exit}\n"
            f"actual_exit={proc.returncode}\n"
            f"stdout={proc.stdout}\n"
            f"stderr={proc.stderr}"
        )
    return proc


def _run_json(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected_exit: int = 0,
) -> dict[str, Any]:
    proc = _run(args, cwd=cwd, env=env, expected_exit=expected_exit)
    stdout = proc.stdout.strip()
    if not stdout:
        raise AssertionError(f"expected JSON output from {args}, got empty stdout")
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"invalid JSON output from {args}: {stdout}") from exc


def _run_lab(
    lab_cli_bin: Path,
    *args: str | Path,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected_exit: int = 0,
) -> dict[str, Any]:
    return _run_json(
        [str(lab_cli_bin), *(str(arg) for arg in args)],
        cwd=cwd,
        env=env,
        expected_exit=expected_exit,
    )


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _load_package_manifest(package_dir: Path) -> dict[str, Any]:
    return _read_json(package_dir / "manifest.json")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in rows)
    path.write_text(body + ("\n" if rows else ""), encoding="utf-8")


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _relpath(path: Path, start: Path) -> str:
    return os.path.relpath(path, start)


def _only_trial_dir(run_dir: Path) -> Path:
    trials = sorted((run_dir / "trials").glob("trial_*"))
    assert len(trials) == 1, trials
    return trials[0]


def _latest_tree_mtime(root: Path) -> float:
    newest = 0.0
    if not root.exists():
        return newest
    for path in root.rglob("*"):
        if path.is_file():
            newest = max(newest, path.stat().st_mtime)
    return newest


def _load_single_json_row(run_dir: Path, table: str) -> dict[str, Any]:
    conn = sqlite3.connect(run_dir / "run.sqlite")
    try:
        row = conn.execute(f"SELECT row_json FROM {table} LIMIT 1").fetchone()
    finally:
        conn.close()
    assert row is not None, table
    return json.loads(row[0])


def _load_runtime_kv_json(run_dir: Path, key: str) -> dict[str, Any]:
    conn = sqlite3.connect(run_dir / "run.sqlite")
    try:
        row = conn.execute(
            "SELECT value_json FROM runtime_kv WHERE key = ?",
            (key,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None, key
    return json.loads(row[0])


def _run_failed_checks(payload: dict[str, Any], name: str) -> list[dict[str, Any]]:
    checks = payload.get("checks", [])
    return [check for check in checks if not check["passed"] and check["name"] == name]


def _assert_preflight_checks_present(
    payload: dict[str, Any],
    expected_names: set[str],
) -> None:
    actual_names = {check["name"] for check in payload.get("checks", [])}
    missing = sorted(expected_names - actual_names)
    assert not missing, {
        "missing": missing,
        "actual": sorted(actual_names),
    }


def _runner_rs_files() -> list[Path]:
    return sorted(
        path
        for path in LAB_RUNNER_SRC_DIR.rglob("*.rs")
        if "legacy" not in path.parts and path.name != "tests.rs"
    )


def _relative_runner_paths(paths: list[Path]) -> list[str]:
    return [path.relative_to(LAB_RUNNER_SRC_DIR).as_posix() for path in sorted(paths)]


def _find_runner_source_hits(
    *,
    files: list[Path],
    needles: list[str],
) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for path in files:
        content = path.read_text(encoding="utf-8")
        matched = [needle for needle in needles if needle in content]
        if matched:
            hits[path.relative_to(LAB_RUNNER_SRC_DIR).as_posix()] = matched
    return hits


def _goal_state_contract_reason() -> str:
    return (
        "goal-state module contract from "
        f"{LAB_RUNNER_RESET_SPEC_PATH.relative_to(REPO_ROOT).as_posix()} is not closed yet"
    )


def _make_project(root: Path, artifact_bundle: Path) -> ProjectLayout:
    lab_dir = root / ".lab"
    experiments_dir = lab_dir / "experiments"
    builds_dir = lab_dir / "builds"
    runs_dir = lab_dir / "runs"
    agents_dir = lab_dir / "agents"
    dataset_packs_dir = lab_dir / "dataset_packs" / "sha256"
    for path in [
        experiments_dir,
        builds_dir,
        runs_dir,
        agents_dir,
        dataset_packs_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)
    shutil.copy2(artifact_bundle, agents_dir / "agent-runtime.tar.gz")
    return ProjectLayout(
        root=root,
        lab_dir=lab_dir,
        experiments_dir=experiments_dir,
        builds_dir=builds_dir,
        runs_dir=runs_dir,
        agents_dir=agents_dir,
        dataset_packs_dir=dataset_packs_dir,
    )


def _task_row(
    *,
    task_id: str,
    task_image: str,
    expected_variant: str = "control",
    resolved_if_match: float = 1.0,
    resolved_if_miss: float = 0.0,
    observe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task_payload: dict[str, Any] = {
        "id": task_id,
        "expected_variant": expected_variant,
        "resolved_if_match": resolved_if_match,
        "resolved_if_miss": resolved_if_miss,
    }
    if observe is not None:
        task_payload["observe"] = observe
    return {
        "schema_version": "task_row_v1",
        "id": task_id,
        "image": task_image,
        "workdir": "/workspace/task",
        "time_limit_ms": 300_000,
        "task": task_payload,
        "materialization": {
            "kind": "task_image",
        },
    }


def _init_agent_eval_experiment(lab_cli_bin: Path, project: ProjectLayout) -> Path:
    _run(
        [
            str(lab_cli_bin),
            "init",
            "--profile",
            "agent-eval",
            "--in-place",
            "--force",
        ],
        cwd=project.root,
    )
    experiment_path = project.root / "experiment.yaml"
    assert experiment_path.exists(), experiment_path
    return experiment_path


def _create_simple_project(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    image_tag: str,
    *,
    exp_id: str,
    rows: list[dict[str, Any]],
    baseline_bindings: dict[str, Any] | None = None,
    variant_plan: list[dict[str, Any]] | None = None,
    agent_command: list[str] | None = None,
    agent_env: dict[str, str] | None = None,
) -> tuple[ProjectLayout, Path]:
    project = _make_project(tmp_path, artifact_bundle)
    experiment_path = _init_agent_eval_experiment(lab_cli_bin, project)
    dataset_path = project.root / "tasks.jsonl"
    grader_path = _copy_custom_benchmark_grader(project)

    variant_controls: dict[str, dict[str, Any]] = {}
    baseline_controls = baseline_bindings or {"variant_label": "control"}
    if baseline_controls:
        variant_controls["control"] = json.loads(json.dumps(baseline_controls))
    for item in variant_plan or []:
        variant_id = item.get("variant_id") or item.get("id")
        bindings = item.get("bindings")
        if isinstance(variant_id, str) and variant_id.strip() and isinstance(bindings, dict) and bindings:
            variant_controls[variant_id.strip()] = json.loads(json.dumps(bindings))

    dataset_rows = [json.loads(json.dumps(row)) for row in rows]
    if variant_controls:
        for row in dataset_rows:
            task = row.get("task")
            if isinstance(task, dict):
                task["variant_controls"] = json.loads(json.dumps(variant_controls))
    _write_jsonl(dataset_path, dataset_rows)

    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["experiment"]["id"] = exp_id
    experiment["experiment"]["name"] = exp_id
    experiment["experiment"]["owner"] = "e2e"
    experiment["experiment"]["description"] = f"CLI e2e fixture for {exp_id}"
    experiment["experiment"]["tags"] = ["e2e", "cli", "docker"]
    experiment["dataset"]["suite_id"] = exp_id
    experiment["dataset"]["path"] = _relpath(dataset_path, project.root)
    experiment["dataset"]["split_id"] = "test"
    experiment["dataset"]["limit"] = len(dataset_rows)
    experiment["design"]["comparison"] = "paired"
    experiment["design"]["replications"] = 1
    experiment["design"]["random_seed"] = 42
    experiment["design"]["shuffle_tasks"] = False
    experiment["design"]["max_concurrency"] = 1
    experiment["baseline"]["variant_id"] = "control"
    experiment["baseline"]["bindings"] = baseline_controls
    experiment["variant_plan"] = variant_plan or []

    runtime = experiment["runtime"]["agent_runtime"]
    runtime["command"] = agent_command or ["e2e-agent"]
    runtime["artifact"] = _relpath(project.agents_dir / "agent-runtime.tar.gz", project.root)
    runtime["image"] = image_tag
    runtime["network"] = "none"
    runtime["env"] = agent_env or {}
    experiment["benchmark"] = {
        "policy": {
            "task_model": "independent",
            "evaluator_mode": "custom",
            "scoring_lifecycle": "predict_then_score",
            "chain_failure_policy": "continue_with_flag",
        },
        "grader": {
            "strategy": "in_task_image",
            "command": [
                "python3",
                _relpath(grader_path, project.root),
            ],
            "conclusion": {
                "mode": "direct",
            },
            "in_task_image": {
                "hidden_paths": [],
                "revealed_paths": [],
            },
        },
    }
    _write_yaml(experiment_path, experiment)
    return project, experiment_path


def _copy_custom_benchmark_grader(project: ProjectLayout) -> Path:
    support_dir = project.root / "benchmark_support"
    support_dir.mkdir(parents=True, exist_ok=True)
    target = support_dir / "custom_benchmark_grader.py"
    shutil.copy2(FIXTURES_DIR / "custom_benchmark_grader.py", target)
    return target


def _copy_custom_benchmark_mapper(project: ProjectLayout) -> Path:
    support_dir = project.root / "benchmark_support"
    support_dir.mkdir(parents=True, exist_ok=True)
    target = support_dir / "custom_benchmark_mapper.py"
    shutil.copy2(FIXTURES_DIR / "custom_benchmark_mapper.py", target)
    return target


def _create_custom_benchmark_project(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    image_tag: str,
    *,
    exp_id: str,
    rows: list[dict[str, Any]] | None = None,
    baseline_bindings: dict[str, Any] | None = None,
) -> tuple[ProjectLayout, Path]:
    normalized_rows = rows or [_task_row(task_id="TASK_CUSTOM_BENCHMARK", task_image=image_tag)]
    project, experiment_path = _create_simple_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        image_tag,
        exp_id=exp_id,
        rows=normalized_rows,
        baseline_bindings=baseline_bindings or {"variant_label": "control"},
    )
    grader_path = _copy_custom_benchmark_grader(project)
    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["benchmark"] = {
        "policy": {
            "task_model": "independent",
            "evaluator_mode": "custom",
            "scoring_lifecycle": "predict_then_score",
            "chain_failure_policy": "continue_with_flag",
        },
        "grader": {
            "strategy": "in_task_image",
            "command": [
                "python3",
                _relpath(grader_path, project.root),
            ],
            "conclusion": {
                "mode": "direct",
            },
            "in_task_image": {
                "hidden_paths": [],
                "revealed_paths": [],
            },
        },
    }
    _write_yaml(experiment_path, experiment)
    return project, experiment_path


def _build_package(
    lab_cli_bin: Path,
    project: ProjectLayout,
    experiment_path: Path,
    build_name: str,
) -> Path:
    package_dir = project.builds_dir / build_name
    payload = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert payload["ok"] is True
    return Path(payload["package_dir"])


def _run_package(
    lab_cli_bin: Path,
    project: ProjectLayout,
    package_dir: Path,
) -> dict[str, Any]:
    payload = _run_lab(
        lab_cli_bin,
        "run",
        package_dir,
        "--materialize",
        "full",
        "--json",
        cwd=project.root,
    )
    assert payload["ok"] is True
    return payload


def _load_agent_report(trial_dir: Path) -> dict[str, Any]:
    return _read_json(trial_dir / "out" / "agent_report.json")


@pytest.fixture(scope="session")
def lab_cli_bin() -> Path:
    env_value = os.environ.get("LAB_CLI_BIN", "").strip()
    if env_value:
        path = Path(env_value).expanduser()
    else:
        path = REPO_ROOT / "rust" / "target" / "release" / "lab-cli"
        source_mtime = max(
            _latest_tree_mtime(REPO_ROOT / "rust" / "crates" / "lab-cli" / "src"),
            _latest_tree_mtime(REPO_ROOT / "rust" / "crates" / "lab-runner" / "src"),
        )
        if not path.exists() or path.stat().st_mtime < source_mtime:
            _run(
                ["cargo", "build", "-p", "lab-cli", "--release"],
                cwd=REPO_ROOT / "rust",
            )
    assert path.exists(), f"lab-cli binary not found: {path}"
    return path


@pytest.fixture(scope="session", autouse=True)
def docker_cli_env(tmp_path_factory: pytest.TempPathFactory) -> None:
    docker_config_dir = tmp_path_factory.mktemp("docker-config")
    (docker_config_dir / "config.json").write_text(
        json.dumps(_docker_auths_without_creds_store(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    previous_docker_config = os.environ.get("DOCKER_CONFIG")
    os.environ["DOCKER_CONFIG"] = str(docker_config_dir)
    try:
        yield
    finally:
        if previous_docker_config is None:
            os.environ.pop("DOCKER_CONFIG", None)
        else:
            os.environ["DOCKER_CONFIG"] = previous_docker_config


@pytest.fixture(scope="session", autouse=True)
def verify_docker() -> None:
    _run(["docker", "info"], cwd=REPO_ROOT)


@pytest.fixture(scope="session")
def fixture_image_tag() -> str:
    return f"agentlab-e2e-fixture:{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="session")
def fixture_image(fixture_image_tag: str) -> str:
    _run(
        [
            "docker",
            "build",
            "-t",
            fixture_image_tag,
            str(FIXTURES_DIR),
        ],
        cwd=REPO_ROOT,
    )
    return fixture_image_tag


@pytest.fixture(scope="session")
def artifact_bundle(tmp_path_factory: pytest.TempPathFactory) -> Path:
    staging_root = tmp_path_factory.mktemp("agent-artifact-staging")
    bundle_root = staging_root / "bundle"
    bin_dir = bundle_root / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    wrapper_path = bin_dir / "e2e-agent"
    wrapper_path.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"\n'
        'exec python3 "$SCRIPT_DIR/e2e_agent.py" "$@"\n',
        encoding="utf-8",
    )
    os.chmod(wrapper_path, 0o755)

    shutil.copy2(FIXTURES_DIR / "e2e_agent.py", bin_dir / "e2e_agent.py")

    manifest = {
        "entrypoint": "bin/e2e-agent",
    }
    (bundle_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    bundle_path = tmp_path_factory.mktemp("agent-artifact") / "agent-runtime.tar.gz"
    with tarfile.open(bundle_path, "w:gz") as archive:
        for path in sorted(bundle_root.rglob("*")):
            archive.add(path, arcname=str(path.relative_to(bundle_root)))
    return bundle_path


@pytest.mark.e2e_cli_surface
def test_init_scaffolds_agent_eval_profile(tmp_path: Path, lab_cli_bin: Path) -> None:
    _run(
        [
            str(lab_cli_bin),
            "init",
            "--profile",
            "agent-eval",
            "--in-place",
        ],
        cwd=tmp_path,
    )
    experiment_path = tmp_path / "experiment.yaml"
    tasks_path = tmp_path / "tasks.jsonl"
    assert experiment_path.exists()
    assert tasks_path.exists()

    rows = _read_jsonl(tasks_path)
    assert len(rows) == 1
    assert rows[0]["schema_version"] == "task_row_v1"
    assert rows[0]["materialization"]["kind"] == "task_image"
    assert rows[0]["workdir"] == "/workspace/task"
    assert rows[0]["task"]["id"] == "TASK001"


@pytest.mark.e2e_cli_surface
def test_lab_runner_live_tree_has_no_legacy_imports() -> None:
    offenders = _find_runner_source_hits(
        files=_runner_rs_files(),
        needles=["crate::legacy::", 'include!("../legacy/', 'include!("legacy/'],
    )
    assert not offenders, offenders


@pytest.mark.e2e_cli_surface
def test_lab_runner_lib_rs_stays_curated() -> None:
    lib_rs = (LAB_RUNNER_SRC_DIR / "lib.rs").read_text(encoding="utf-8")
    disallowed = re.findall(
        r"(?m)^(?!\s*pub\s+static\s+INTERRUPTED\b)\s*(?:pub\s+)?(?:fn|struct|enum|trait|impl|type|const)\b",
        lib_rs,
    )
    assert not disallowed, lib_rs


@pytest.mark.e2e_cli_surface
@pytest.mark.xfail(strict=True, reason=_goal_state_contract_reason())
def test_lab_runner_goal_state_module_layout_contract() -> None:
    root_rs_files = sorted(
        path.name
        for path in LAB_RUNNER_SRC_DIR.glob("*.rs")
        if path.name != "tests.rs"
    )
    assert root_rs_files == ["lib.rs"], root_rs_files

    expected_paths = {
        "backend/docker.rs",
        "experiment/commit.rs",
        "experiment/control.rs",
        "experiment/describe.rs",
        "experiment/lease.rs",
        "experiment/preflight.rs",
        "experiment/recover.rs",
        "experiment/replay.rs",
        "experiment/run.rs",
        "experiment/state.rs",
        "package/authoring.rs",
        "package/compile.rs",
        "package/resolved.rs",
        "package/sealed.rs",
        "package/staging.rs",
        "package/validate.rs",
        "persistence/files.rs",
        "persistence/journal.rs",
        "persistence/rows.rs",
        "persistence/store.rs",
        "trial/artifacts.rs",
        "trial/env.rs",
        "trial/events.rs",
        "trial/execution.rs",
        "trial/grade.rs",
        "trial/preflight.rs",
        "trial/prepare.rs",
        "trial/run.rs",
        "trial/spec.rs",
        "trial/state.rs",
        "trial/workspace.rs",
    }
    actual_paths = set(_relative_runner_paths(_runner_rs_files()))
    missing = sorted(expected_paths - actual_paths)
    unexpected = sorted(actual_paths - expected_paths - {"lib.rs"})
    assert not missing and not unexpected, {
        "missing": missing,
        "unexpected": unexpected,
    }


@pytest.mark.e2e_cli_surface
@pytest.mark.xfail(strict=True, reason=_goal_state_contract_reason())
def test_lab_runner_goal_state_import_layering_contract() -> None:
    package_hits = _find_runner_source_hits(
        files=sorted(LAB_RUNNER_SRC_DIR.joinpath("package").rglob("*.rs")),
        needles=[
            "crate::experiment::",
            "crate::trial::",
            "crate::config::",
            "crate::engine::",
            "crate::model::",
            "crate::preflight::",
            "crate::runtime::",
        ],
    )
    assert not package_hits, package_hits

    trial_hits = _find_runner_source_hits(
        files=sorted(LAB_RUNNER_SRC_DIR.joinpath("trial").rglob("*.rs")),
        needles=[
            "crate::experiment::",
            "crate::config::",
            "crate::engine::",
            "crate::model::",
            "crate::preflight::",
            "crate::runtime::",
        ],
    )
    assert not trial_hits, trial_hits

    persistence_hits = _find_runner_source_hits(
        files=sorted(LAB_RUNNER_SRC_DIR.joinpath("persistence").rglob("*.rs")),
        needles=[
            "crate::experiment::",
            "crate::package::",
            "crate::trial::",
        ],
    )
    assert not persistence_hits, persistence_hits


@pytest.mark.e2e_build_preflight
def test_build_describe_and_preflight_task_row_package(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [_task_row(task_id="TASK_BUILD_PREFLIGHT", task_image=fixture_image)]
    project, experiment_path = _create_simple_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        fixture_image,
        exp_id="build_describe_and_preflight_task_row_package",
        rows=rows,
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "build_describe_and_preflight_task_row_package",
    )
    assert (package_dir / "manifest.json").exists()
    assert (package_dir / "checksums.json").exists()
    assert (package_dir / "resolved_experiment.json").exists()
    assert (package_dir / "tasks" / "tasks.jsonl").exists()
    manifest = _load_package_manifest(package_dir)
    artifact_path = (
        manifest["resolved_experiment"]["runtime"]["agent_runtime"]["artifact"]
    )
    assert manifest["resolved_experiment"]["dataset"]["path"] == "tasks/tasks.jsonl"
    assert artifact_path.startswith("agent_builds/")
    assert not Path(artifact_path).is_absolute()
    assert (package_dir / artifact_path).exists()

    describe = _run_lab(
        lab_cli_bin,
        "describe",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert describe["ok"] is True
    assert describe["summary"]["experiment"] == "build_describe_and_preflight_task_row_package"
    assert describe["summary"]["tasks"] == 1
    assert describe["summary"]["variant_count"] == 1
    assert describe["summary"]["total_trials"] == 1
    assert describe["summary"]["image"] == fixture_image

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is True
    _assert_preflight_checks_present(
        preflight,
        {
            "dataset_task_ids",
            "disk_headroom",
            "agent_runtime_hermetic",
            "dangerous_mode_forbidden",
            "workspace_contract_not_host_path",
            "task_sandbox_bash_plane",
            "agent_bundle_container_compatible",
            "container_ready",
            "agent_runtime_reachable",
            "benchmark_grader_reachable",
            "dependency_files_exist",
            "workspace_patch_sources_exist",
        },
    )
    assert all(check["passed"] for check in preflight["checks"]), preflight["checks"]


@pytest.mark.e2e_runtime
def test_run_persists_sqlite_runtime_state_and_container_execution_facts(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(
            task_id="TASK_RUNTIME_STATE",
            task_image=fixture_image,
            observe={
                "workspace_root": {
                    "path": ".",
                }
            },
        )
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        fixture_image,
        exp_id="run_persists_sqlite_runtime_state_and_container_execution_facts",
        rows=rows,
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "run_runtime_state_pkg",
    )

    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)

    assert (run_dir / "run.sqlite").exists()
    assert (trial_dir / "trial_runtime_state.json").exists()
    assert (trial_dir / "state_inventory.json").exists()

    trial_row = _load_single_json_row(run_dir, "trial_rows")
    run_control = _load_runtime_kv_json(run_dir, "run_control_v2")
    runtime_state = _read_json(trial_dir / "trial_runtime_state.json")
    state_inventory = _read_json(trial_dir / "state_inventory.json")
    agent_report = _load_agent_report(trial_dir)

    assert trial_row["outcome"] == "success"
    assert trial_row["success"] is True
    assert run_control["status"] == "completed"

    assert runtime_state["schema_version"] == "trial_runtime_state_v1"
    assert runtime_state["state"]["phase"] == "committed"
    assert runtime_state["state"]["agent_phase"]["result_state"] == "valid"
    assert runtime_state["state"]["candidate_artifact"]["state"] == "valid"
    assert runtime_state["state"]["workspace_delta"]["observation_kind"] == "container_tree"

    assert state_inventory["planes"]["agent_runtime"]["executor"] == "docker"
    assert state_inventory["planes"]["task_sandbox"]["executor"] == "docker"

    assert agent_report["env"]["workspace"] == agent_report["cwd"]
    assert Path(agent_report["env"]["workspace"]).is_absolute()


@pytest.mark.e2e_runtime
def test_run_records_truthful_failure_for_nonzero_agent_exit(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [_task_row(task_id="TASK_NONZERO_EXIT", task_image=fixture_image)]
    project, experiment_path = _create_simple_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        fixture_image,
        exp_id="run_records_truthful_failure_for_nonzero_agent_exit",
        rows=rows,
        baseline_bindings={
            "variant_label": "control",
            "runtime_only_exit_code": 17,
        },
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "run_nonzero_exit_pkg",
    )

    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)
    trial_row = _load_single_json_row(run_dir, "trial_rows")
    runtime_state = _read_json(trial_dir / "trial_runtime_state.json")

    assert trial_row["outcome"] == "error"
    assert trial_row["success"] is False
    assert trial_row["status_code"] == "17"
    assert runtime_state["state"]["phase"] == "committed"
    assert runtime_state["state"]["agent_phase"]["exit_code"] == 17


@pytest.mark.e2e_runtime
def test_replay_creates_new_trial_artifacts_from_committed_trial(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [_task_row(task_id="TASK_REPLAY", task_image=fixture_image)]
    project, experiment_path = _create_simple_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        fixture_image,
        exp_id="replay_creates_new_trial_artifacts_from_committed_trial",
        rows=rows,
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "replay_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])

    replay = _run_lab(
        lab_cli_bin,
        "replay",
        "--run-dir",
        run_dir,
        "--trial-id",
        "trial_1",
        "--json",
        cwd=project.root,
    )
    assert replay["ok"] is True
    replay_dir = Path(replay["replay"]["replay_dir"])
    replay_trial_dir = replay_dir / "trial_1"
    runtime_state = _read_json(replay_trial_dir / "trial_runtime_state.json")

    assert replay_dir.exists()
    assert replay["replay"]["parent_trial_id"] == "trial_1"
    assert str(replay["replay"]["harness_status"]) == "0"
    assert runtime_state["state"]["phase"] == "committed"


@pytest.mark.e2e_benchmark
def test_custom_benchmark_run_records_trial_conclusion_and_grading_state(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project, experiment_path = _create_custom_benchmark_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        fixture_image,
        exp_id="custom_benchmark_run_records_trial_conclusion_and_grading_state",
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "custom_benchmark_pkg",
    )

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is True
    _assert_preflight_checks_present(
        preflight,
        {
            "dataset_task_ids",
            "disk_headroom",
            "agent_runtime_hermetic",
            "dangerous_mode_forbidden",
            "workspace_contract_not_host_path",
            "task_sandbox_bash_plane",
            "agent_bundle_container_compatible",
            "container_ready",
            "agent_runtime_reachable",
            "benchmark_grader_reachable",
            "dependency_files_exist",
            "workspace_patch_sources_exist",
        },
    )

    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)
    runtime_state = _read_json(trial_dir / "trial_runtime_state.json")
    mapped_output = _read_json(trial_dir / "out" / "mapped_grader_output.json")
    trial_row = _load_single_json_row(run_dir, "trial_rows")
    conclusion_row = _load_single_json_row(run_dir, "benchmark_conclusion_rows")

    assert (trial_dir / "benchmark_preflight.json").exists()
    assert mapped_output["schema_version"] == "trial_conclusion_v1"
    assert mapped_output["reported_outcome"] == "success"
    assert conclusion_row["reported_outcome"] == "success"
    assert trial_row["outcome"] == "success"
    assert trial_row["primary_metric_name"] == "resolved"
    assert trial_row["primary_metric_value"] == 1.0
    assert runtime_state["state"]["grading_phase"]["raw_output_state"] == "valid"
    assert runtime_state["state"]["mapping_phase"] is None


@pytest.mark.e2e_benchmark
def test_custom_benchmark_run_records_grader_failure_as_grading_failed(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project, experiment_path = _create_custom_benchmark_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        fixture_image,
        exp_id="custom_benchmark_run_records_grader_failure_as_grading_failed",
    )
    dataset_path = project.root / "tasks.jsonl"
    rows = _read_jsonl(dataset_path)
    rows[0]["task"]["grading_behavior"] = {"grader_exit_code": 9}
    _write_jsonl(dataset_path, rows)

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "custom_benchmark_grader_failure_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)
    runtime_state = _read_json(trial_dir / "trial_runtime_state.json")
    trial_row = _load_single_json_row(run_dir, "trial_rows")

    assert trial_row["outcome"] == "grading_failed"
    assert trial_row["success"] is False
    assert trial_row["primary_metric_name"] == "grading_failed"
    assert trial_row["metrics"]["grade_error"] is True
    assert "mapped_grader_output_invalid" in trial_row["metrics"]["grade_error_reason"]
    assert runtime_state["state"]["phase"] == "committed"
    assert runtime_state["state"]["grading_phase"]["exit_code"] == 9
    assert runtime_state["state"]["grading_phase"]["raw_output_state"] == "missing"
    assert runtime_state["state"]["mapping_phase"] is None


@pytest.mark.e2e_benchmark
def test_custom_benchmark_run_records_mapper_failure_as_grading_failed(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project, experiment_path = _create_custom_benchmark_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        fixture_image,
        exp_id="custom_benchmark_run_records_mapper_failure_as_grading_failed",
    )
    dataset_path = project.root / "tasks.jsonl"
    rows = _read_jsonl(dataset_path)
    rows[0]["task"]["grading_behavior"] = {
        "emit_raw_only": True,
        "mapper_exit_code": 11,
    }
    _write_jsonl(dataset_path, rows)

    mapper_path = _copy_custom_benchmark_mapper(project)
    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["benchmark"]["grader"]["conclusion"] = {
        "mode": "mapper",
        "mapper": _relpath(mapper_path, project.root),
    }
    _write_yaml(experiment_path, experiment)

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "custom_benchmark_mapper_failure_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)
    runtime_state = _read_json(trial_dir / "trial_runtime_state.json")
    trial_row = _load_single_json_row(run_dir, "trial_rows")

    assert trial_row["outcome"] == "grading_failed"
    assert trial_row["success"] is False
    assert trial_row["primary_metric_name"] == "grading_failed"
    assert trial_row["metrics"]["grade_error"] is True
    assert "mapped_grader_output_invalid" in trial_row["metrics"]["grade_error_reason"]
    assert runtime_state["state"]["phase"] == "committed"
    assert runtime_state["state"]["grading_phase"]["exit_code"] == 0
    assert runtime_state["state"]["grading_phase"]["raw_output_state"] == "valid"
    assert runtime_state["state"]["mapping_phase"]["mapped_output_state"] == "missing"


@pytest.mark.e2e_benchmark
def test_preflight_rejects_benchmark_grading_opt_out(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project, experiment_path = _create_custom_benchmark_project(
        tmp_path,
        lab_cli_bin,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_rejects_benchmark_grading_opt_out",
    )
    dataset_path = project.root / "tasks.jsonl"
    rows = _read_jsonl(dataset_path)
    rows[0]["task"]["grading"] = {"enabled": False}
    _write_jsonl(dataset_path, rows)

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "benchmark_grading_opt_out_pkg",
    )
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )

    assert preflight["ok"] is False
    failed_checks = _run_failed_checks(preflight, "dataset_task_ids")
    assert failed_checks, preflight["checks"]
    assert any(
        "Milestone 4 requires mapped grading output" in check["message"]
        for check in failed_checks
    ), failed_checks
