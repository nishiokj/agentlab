from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
DEFAULT_RUN_TIMEOUT_SECONDS = 240
BENCH_SUPPORT_PATHS = [
    "bench/__init__.py",
    "bench/config.py",
    "bench/paths.py",
    "bench/integration/__init__.py",
    "bench/integration/agentlab",
    "bench/taskkit",
    "bench/benchmark/tasks",
    "schemas",
]


@dataclass(frozen=True)
class ProjectLayout:
    root: Path
    lab_dir: Path
    experiments_dir: Path
    experiment_data_dir: Path
    agents_dir: Path
    builds_dir: Path
    runs_dir: Path
    dataset_packs_dir: Path


def _run(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected_exit: int = 0,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        args,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=False,
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
    except json.JSONDecodeError as exc:  # pragma: no cover
        raise AssertionError(f"invalid JSON output from {args}: {stdout}") from exc


def _run_lab(
    lab_cli_bin: Path,
    *args: str | Path,
    cwd: Path,
    expected_exit: int = 0,
) -> dict[str, Any]:
    rendered = [str(lab_cli_bin), *(str(arg) for arg in args)]
    return _run_json(rendered, cwd=cwd, expected_exit=expected_exit)


def _run_python(*args: str | Path, cwd: Path, expected_exit: int = 0) -> subprocess.CompletedProcess[str]:
    rendered = [sys.executable, *(str(arg) for arg in args)]
    return _run(rendered, cwd=cwd, expected_exit=expected_exit)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in rows)
    path.write_text(body + ("\n" if rows else ""), encoding="utf-8")


def _table_rows(table: dict[str, Any]) -> list[dict[str, Any]]:
    rows = table["rows"]
    if rows and isinstance(rows[0], dict):
        return rows
    columns = table["columns"]
    return [dict(zip(columns, row)) for row in rows]


def _only_trial_dir(run_dir: Path) -> Path:
    trials = sorted((run_dir / "trials").glob("trial_*"))
    assert len(trials) == 1, trials
    return trials[0]


def _make_project(root: Path, artifact_bundle: Path) -> ProjectLayout:
    lab_dir = root / ".lab"
    experiments_dir = lab_dir / "experiments"
    experiment_data_dir = experiments_dir / "data"
    agents_dir = lab_dir / "agents"
    builds_dir = lab_dir / "builds"
    runs_dir = lab_dir / "runs"
    dataset_packs_dir = lab_dir / "dataset_packs" / "sha256"
    for path in [
        experiments_dir,
        experiment_data_dir,
        agents_dir,
        builds_dir,
        runs_dir,
        dataset_packs_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)
    shutil.copy2(artifact_bundle, agents_dir / "agent-runtime.tar.gz")
    return ProjectLayout(
        root=root,
        lab_dir=lab_dir,
        experiments_dir=experiments_dir,
        experiment_data_dir=experiment_data_dir,
        agents_dir=agents_dir,
        builds_dir=builds_dir,
        runs_dir=runs_dir,
        dataset_packs_dir=dataset_packs_dir,
    )


def _relpath(path: Path, start: Path) -> str:
    return os.path.relpath(path, start)


def _copy_repo_subset(destination_root: Path, paths: list[str]) -> None:
    for rel in paths:
        source = REPO_ROOT / rel
        target = destination_root / rel
        if source.is_dir():
            shutil.copytree(source, target, dirs_exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)


def _materialize_dataset_pack(project: ProjectLayout, files: dict[str, str]) -> str:
    digest_source = json.dumps(files, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(digest_source).hexdigest()
    pack_dir = project.dataset_packs_dir / digest
    pack_dir.mkdir(parents=True, exist_ok=True)
    for rel, content in files.items():
        target = pack_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return f"sha256:{digest}"


def _workspace_file(path: str, content: str, executable: bool = False) -> dict[str, Any]:
    return {
        "path": path,
        "content": content,
        "encoding": "utf8",
        "executable": executable,
    }


def _mount_reference(dataset_pack_ref: str, mount_path: str) -> dict[str, Any]:
    return {
        "dataset_pack_ref": dataset_pack_ref,
        "mount_path": mount_path,
        "read_only": True,
    }


def _task_row(
    *,
    task_id: str,
    expected_variant: str = "control",
    task_image: str | None = None,
    workspace_seed_ref: str | None = None,
    workspace_files: list[dict[str, Any]] | None = None,
    mount_references: list[dict[str, Any]] | None = None,
    observe: dict[str, Any] | None = None,
    resolved_if_match: float = 1.0,
    resolved_if_miss: float = 0.0,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": task_id,
        "expected_variant": expected_variant,
        "resolved_if_match": resolved_if_match,
        "resolved_if_miss": resolved_if_miss,
    }
    if task_image is not None:
        task["image"] = task_image
    if observe is not None:
        task["observe"] = observe
    row: dict[str, Any] = {
        "schema_version": "task_boundary_v2",
        "task": task,
        "workspace_files": workspace_files or [],
        "mount_references": mount_references or [],
        "limits": {},
    }
    if workspace_seed_ref is not None:
        row["workspace_seed"] = {"dataset_pack_ref": workspace_seed_ref}
    return row


def _base_experiment(
    *,
    exp_id: str,
    dataset_path: str,
    artifact_path: str,
    image_tag: str,
    image_source: str = "global",
    agent_command: list[str] | None = None,
    baseline_bindings: dict[str, Any] | None = None,
    variant_plan: list[dict[str, Any]] | None = None,
    comparison: str = "paired",
    require_workspace_materialization: bool = False,
    benchmark: bool = False,
    state_policy: str | None = None,
) -> dict[str, Any]:
    runtime_agent: dict[str, Any] = {
        "command": agent_command or ["e2e-agent"],
        "artifact": artifact_path,
        "io": {
            "input_arg": "--input",
            "output_arg": "--output",
        },
        "env": {},
        "env_from_host": [],
    }
    if image_source == "per_task":
        runtime_agent["image_source"] = "per_task"
    else:
        runtime_agent["image"] = image_tag
    experiment: dict[str, Any] = {
        "version": "0.5",
        "experiment": {
            "id": exp_id,
            "name": exp_id,
            "workload_type": "agent_runtime",
            "owner": "e2e",
            "description": f"CLI E2E fixture for {exp_id}",
            "tags": ["e2e", "cli", "docker"],
        },
        "dataset": {
            "suite_id": exp_id,
            "provider": "local_jsonl",
            "path": dataset_path,
            "schema_version": "task_boundary_v2",
            "split_id": "test",
            "limit": 10,
        },
        "design": {
            "sanitization_profile": "hermetic_functional",
            "comparison": comparison,
            "replications": 1,
            "random_seed": 42,
            "shuffle_tasks": False,
            "max_concurrency": 1,
        },
        "metrics": [
            {
                "id": "resolved",
                "source": "output",
                "json_pointer": "/metrics/resolved" if benchmark else "/objective/value",
                "weight": 1,
                "direction": "maximize",
                "primary": True,
            }
        ],
        "baseline": {
            "variant_id": "control",
            "bindings": baseline_bindings or {"variant_label": "control"},
        },
        "variant_plan": variant_plan or [],
        "runtime": {
            "agent": runtime_agent,
            "dependencies": {
                "file_staging": [],
                "services": [],
            },
            "policy": {
                "timeout_ms": DEFAULT_RUN_TIMEOUT_SECONDS * 1000,
                "sandbox": {
                    "mode": "container",
                    "root_read_only": True,
                    "hardening": {
                        "no_new_privileges": True,
                        "drop_all_caps": True,
                    },
                    "resources": {
                        "cpu_count": 1,
                        "memory_mb": 1024,
                    },
                },
                "network": {
                    "mode": "none",
                    "allowed_hosts": [],
                },
            },
        },
        "validity": {
            "fail_on_state_leak": True,
            "fail_on_profile_invariant_violation": True,
        },
        "artifacts": {
            "collect": ["artifacts/**", "output/**"],
            "diff": True,
        },
    }
    policies: dict[str, Any] = {}
    if require_workspace_materialization:
        policies["task_boundary"] = {
            "require_workspace_materialization": True,
        }
    if state_policy is not None:
        policies["state"] = state_policy
    if policies:
        experiment["design"]["policies"] = policies
    if benchmark:
        experiment["benchmark"] = {
            "policy": {
                "evaluator_mode": "custom",
                "scoring_lifecycle": "predict_then_score",
            },
            "adapter": {
                "command": [
                    "python3",
                    "/opt/agent/bench/integration/agentlab/bench_benchmark_adapter.py",
                ]
            },
        }
    return experiment


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
        "--executor",
        "local_docker",
        "--materialize",
        "full",
        "--json",
        cwd=project.root,
    )
    assert payload["ok"] is True
    return payload


def _create_simple_project(
    tmp_path: Path,
    artifact_bundle: Path,
    image_tag: str,
    *,
    exp_id: str,
    rows: list[dict[str, Any]],
    image_source: str = "global",
    baseline_bindings: dict[str, Any] | None = None,
    variant_plan: list[dict[str, Any]] | None = None,
    comparison: str = "paired",
    benchmark: bool = False,
    require_workspace_materialization: bool = False,
    agent_command: list[str] | None = None,
    state_policy: str | None = None,
) -> tuple[ProjectLayout, Path]:
    project = _make_project(tmp_path, artifact_bundle)
    dataset_path = project.experiment_data_dir / f"{exp_id}.task_boundary_v2.jsonl"
    _write_jsonl(dataset_path, rows)
    experiment_path = project.experiments_dir / f"{exp_id}.yaml"
    experiment = _base_experiment(
        exp_id=exp_id,
        dataset_path=_relpath(dataset_path, project.experiments_dir),
        artifact_path=_relpath(project.agents_dir / "agent-runtime.tar.gz", project.experiments_dir),
        image_tag=image_tag,
        image_source=image_source,
        baseline_bindings=baseline_bindings,
        variant_plan=variant_plan,
        comparison=comparison,
        benchmark=benchmark,
        require_workspace_materialization=require_workspace_materialization,
        agent_command=agent_command,
        state_policy=state_policy,
    )
    experiment["dataset"]["limit"] = len(rows)
    _write_yaml(experiment_path, experiment)
    return project, experiment_path


def _export_benchmark_dataset(
    *,
    project: ProjectLayout,
    base_task_image: str,
    limit: int = 1,
) -> tuple[Path, list[dict[str, Any]]]:
    dataset_path = project.experiment_data_dir / "bench_v0.task_boundary_v2.jsonl"
    script = REPO_ROOT / "bench" / "integration" / "agentlab" / "export_bench_suite_to_jsonl.py"
    _run_python(
        script,
        "--suite",
        "v0",
        "--output",
        dataset_path,
        "--default-task-image",
        base_task_image,
        "--require-task-image",
        "--dataset-pack-root",
        project.dataset_packs_dir,
        "--limit",
        str(limit),
        cwd=REPO_ROOT,
    )
    rows = [
        json.loads(line)
        for line in dataset_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert rows, "expected exported benchmark dataset rows"
    return dataset_path, rows


def _prepare_benchmark_experiment(
    tmp_path: Path,
    artifact_bundle: Path,
    image_tag: str,
) -> tuple[ProjectLayout, Path, list[dict[str, Any]]]:
    project = _make_project(tmp_path, artifact_bundle)
    base_task_image = f"agentlab-e2e-task-{uuid.uuid4().hex[:10]}"
    dataset_path, rows = _export_benchmark_dataset(
        project=project,
        base_task_image=base_task_image,
        limit=1,
    )
    task_image = rows[0]["task"]["image"]
    _run(["docker", "tag", image_tag, task_image], cwd=project.root)

    support_root = project.root / "bench_support"
    support_root.mkdir(parents=True, exist_ok=True)
    grader_wrapper = support_root / "bench_benchmark_adapter_entry.py"
    grader_wrapper.write_text(
        "from __future__ import annotations\n"
        "import runpy\n"
        "runpy.run_path('/opt/agent/bench/integration/agentlab/bench_benchmark_adapter.py', run_name='__main__')\n",
        encoding="utf-8",
    )

    experiment_path = project.experiments_dir / "bench_v0_per_task.yaml"
    experiment = _base_experiment(
        exp_id="bench_v0_per_task_e2e",
        dataset_path=_relpath(dataset_path, project.experiments_dir),
        artifact_path=_relpath(project.agents_dir / "agent-runtime.tar.gz", project.experiments_dir),
        image_tag=image_tag,
        image_source="per_task",
        benchmark=True,
        agent_command=[
            "python3",
            "/opt/agent/bench/integration/agentlab/bench_runtime_adapter.py",
        ],
        baseline_bindings={
            "variant_label": "control",
            "bench_agent_command": ["write-empty-patch"],
        },
    )
    experiment["runtime"]["dependencies"]["file_staging"] = [
        {
            "source_from_host": _relpath(grader_wrapper, project.experiments_dir),
            "destination_path": "/agentlab/deps/bench_benchmark_adapter_entry.py",
            "required": True,
            "read_only": True,
        },
    ]
    experiment["benchmark"]["adapter"]["command"] = [
        "python3",
        "/agentlab/deps/bench_benchmark_adapter_entry.py",
    ]
    experiment["dataset"]["limit"] = len(rows)
    _write_yaml(experiment_path, experiment)
    return project, experiment_path, rows


def _assert_command_failed(payload: dict[str, Any], needle: str) -> None:
    assert payload["ok"] is False
    assert payload["error"]["code"] == "command_failed"
    assert needle in payload["error"]["message"]


@pytest.fixture(scope="session")
def lab_cli_bin() -> Path:
    env_value = os.environ.get("LAB_CLI_BIN", "").strip()
    if env_value:
        path = Path(env_value)
    else:
        path = REPO_ROOT / "rust" / "target" / "release" / "lab-cli"
    assert path.exists(), f"lab-cli binary not found: {path}"
    os.utime(path, None)
    _run([str(path), "--help"], cwd=REPO_ROOT)
    return path


@pytest.fixture(scope="session")
def fixture_image_tag() -> str:
    return f"agentlab-e2e-fixture:{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="session", autouse=True)
def verify_docker() -> None:
    _run(["docker", "info"], cwd=REPO_ROOT)


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

    helper_path = bin_dir / "write-empty-patch"
    helper_path.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        ": > patch.diff\n",
        encoding="utf-8",
    )
    os.chmod(helper_path, 0o755)

    shutil.copy2(FIXTURES_DIR / "e2e_agent.py", bin_dir / "e2e_agent.py")

    manifest = {
        "entrypoint": "bin/e2e-agent",
    }
    (bundle_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    _copy_repo_subset(bundle_root, BENCH_SUPPORT_PATHS)

    bundle_path = tmp_path_factory.mktemp("agent-artifact") / "agent-runtime.tar.gz"
    with tarfile.open(bundle_path, "w:gz") as archive:
        for path in sorted(bundle_root.rglob("*")):
            archive.add(path, arcname=str(path.relative_to(bundle_root)))
    return bundle_path


def test_build_preflight_describe_happy_path(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_HAPPY", expected_variant="control"),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_preflight_describe_happy_path",
        rows=rows,
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "build_preflight_describe_happy_path_pkg",
    )
    manifest = _read_json(package_dir / "manifest.json")
    checksums = _read_json(package_dir / "checksums.json")
    package_lock = _read_json(package_dir / "package.lock")
    assert manifest["schema_version"] == "sealed_run_package_v2"
    assert checksums["schema_version"] == "sealed_package_checksums_v2"
    assert package_lock["schema_version"] == "sealed_package_lock_v1"
    assert (package_dir / "resolved_experiment.json").exists()
    assert (package_dir / "tasks" / "tasks.jsonl").exists()

    describe = _run_lab(
        lab_cli_bin,
        "describe",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert describe["ok"] is True
    summary = describe["summary"]
    assert summary["experiment"] == "build_preflight_describe_happy_path"
    assert summary["tasks"] == 1
    assert summary["variant_count"] == 1
    assert summary["total_trials"] == 1
    assert summary["image"] == fixture_image

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is True
    assert preflight["command"] == "preflight"
    assert preflight["checks"]
    assert all(check["passed"] for check in preflight["checks"])


def test_run_plane_rejects_authoring_input(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_AUTHORING_REJECT"),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="run_plane_rejects_authoring_input",
        rows=rows,
    )

    describe = _run_lab(
        lab_cli_bin,
        "describe",
        experiment_path,
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(describe, "run_input_invalid_kind")

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        experiment_path,
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(preflight, "run_input_invalid_kind")

    run = _run_lab(
        lab_cli_bin,
        "run",
        experiment_path,
        "--executor",
        "local_docker",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(run, "run_input_invalid_kind")


def test_tampered_package_fails_preflight(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_TAMPER"),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="tampered_package_fails_preflight",
        rows=rows,
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "tampered_package_fails_preflight_pkg",
    )
    tasks_path = package_dir / "tasks" / "tasks.jsonl"
    original = tasks_path.read_text(encoding="utf-8")
    tasks_path.write_text(original.replace("TASK_TAMPER", "TASK_TAMPER_MUTATED"), encoding="utf-8")

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(preflight, "checksum mismatch")


def test_missing_per_task_image_fails_preflight(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(
            task_id="TASK_MISSING_IMAGE",
            workspace_files=[_workspace_file("README.md", "materialized")],
        ),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="missing_per_task_image_fails_preflight",
        rows=rows,
        image_source="per_task",
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "missing_per_task_image_pkg",
    )
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is False
    messages = [check["message"] for check in preflight["checks"] if not check["passed"]]
    assert any("tasks missing task.image in per-task mode" in message for message in messages)


def test_run_happy_path_writes_sqlite_and_artifacts(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_RUN_HAPPY"),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="run_happy_path_writes_sqlite_and_artifacts",
        rows=rows,
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "run_happy_path_pkg",
    )
    describe = _run_lab(
        lab_cli_bin,
        "describe",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert describe["ok"] is True
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is True

    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    assert run_dir.exists()
    assert Path(run_payload["artifacts"]["run_sqlite_path"]).exists()

    variant_summary = _run_lab(
        lab_cli_bin,
        "views",
        run_dir,
        "variant_summary",
        "--json",
        cwd=project.root,
    )
    assert variant_summary["ok"] is True
    assert len(variant_summary["result"]["rows"]) == 1

    query = _run_lab(
        lab_cli_bin,
        "query",
        run_dir,
        "SELECT COUNT(*) AS n FROM trials",
        "--json",
        cwd=project.root,
    )
    assert query["ok"] is True
    assert int(_table_rows(query["result"])[0]["n"]) == 1

    trials = _run_lab(
        lab_cli_bin,
        "query",
        run_dir,
        "SELECT outcome, primary_metric_name, primary_metric_value, bindings FROM trials LIMIT 1",
        "--json",
        cwd=project.root,
    )
    row = _table_rows(trials["result"])[0]
    assert row["outcome"] == "success"
    assert row["primary_metric_name"] == "resolved"
    assert float(row["primary_metric_value"]) == 1.0
    assert row["bindings"]["variant_label"] == "control"


def test_materialize_full_persists_workspace_seed_files_and_mounts(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project = _make_project(tmp_path, artifact_bundle)
    seed_ref = _materialize_dataset_pack(project, {"seed/README.txt": "seed ready\n"})
    mount_ref = _materialize_dataset_pack(project, {"reference.txt": "mounted reference\n"})
    dataset_path = project.experiment_data_dir / "materialize_full.task_boundary_v2.jsonl"
    rows = [
        _task_row(
            task_id="TASK_MATERIALIZE",
            workspace_seed_ref=seed_ref,
            workspace_files=[_workspace_file("overlay/config.txt", "overlay ready\n")],
            mount_references=[_mount_reference(mount_ref, "/agentlab/workspace/mounted")],
            observe={
                "seed_file": {
                    "path": "seed/README.txt",
                    "expect_text": "seed ready",
                },
                "overlay_file": {
                    "path": "overlay/config.txt",
                    "expect_text": "overlay ready",
                },
                "mount_file": {
                    "path": "mounted/reference.txt",
                    "expect_text": "mounted reference",
                    "expect_read_only": True,
                },
            },
        ),
    ]
    _write_jsonl(dataset_path, rows)
    experiment_path = project.experiments_dir / "materialize_full.yaml"
    experiment = _base_experiment(
        exp_id="materialize_full_persists_workspace_seed_files_and_mounts",
        dataset_path=_relpath(dataset_path, project.experiments_dir),
        artifact_path=_relpath(project.agents_dir / "agent-runtime.tar.gz", project.experiments_dir),
        image_tag=fixture_image,
        require_workspace_materialization=True,
    )
    experiment["dataset"]["limit"] = 1
    _write_yaml(experiment_path, experiment)

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "materialize_full_pkg",
    )
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is True

    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    metrics = _run_lab(
        lab_cli_bin,
        "query",
        run_dir,
        "SELECT metric_name, metric_value FROM metrics_long WHERE metric_name LIKE 'obs_%' ORDER BY metric_name",
        "--json",
        cwd=project.root,
    )
    observed = {
        row["metric_name"]: int(row["metric_value"])
        for row in _table_rows(metrics["result"])
    }
    assert observed["obs_seed_file_exists"] == 1
    assert observed["obs_seed_file_text_match"] == 1
    assert observed["obs_overlay_file_exists"] == 1
    assert observed["obs_overlay_file_text_match"] == 1
    assert observed["obs_mount_file_exists"] == 1
    assert observed["obs_mount_file_text_match"] == 1
    assert observed["obs_mount_file_write_blocked"] == 1


def test_ab_run_exposes_views_and_query_surface(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_AB_1", expected_variant="treatment"),
        _task_row(task_id="TASK_AB_2", expected_variant="treatment"),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="ab_run_exposes_views_and_query_surface",
        rows=rows,
        baseline_bindings={"variant_label": "control"},
        variant_plan=[
            {
                "variant_id": "treatment",
                "bindings": {"variant_label": "treatment"},
            }
        ],
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "ab_run_pkg",
    )
    describe = _run_lab(
        lab_cli_bin,
        "describe",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert describe["summary"]["variant_count"] == 2
    assert describe["summary"]["total_trials"] == 4

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is True

    run_payload = _run_package(lab_cli_bin, project, package_dir)
    assert run_payload["post_run_stats"]["view_set"] == "ab_test"
    run_dir = Path(run_payload["run"]["run_dir"])

    views_listing = _run_lab(
        lab_cli_bin,
        "views",
        run_dir,
        "--json",
        cwd=project.root,
    )
    view_names = {row["name"] for row in views_listing["available_views"]}
    assert "comparison_summary" in view_names
    assert "scoreboard" in view_names

    comparison_summary = _run_lab(
        lab_cli_bin,
        "views",
        run_dir,
        "comparison_summary",
        "--json",
        cwd=project.root,
    )
    assert comparison_summary["ok"] is True
    assert len(comparison_summary["result"]["rows"]) == 1

    variant_summary = _run_lab(
        lab_cli_bin,
        "views",
        run_dir,
        "variant_summary",
        "--json",
        cwd=project.root,
    )
    summary_rows = _table_rows(variant_summary["result"])
    assert {row["variant_id"] for row in summary_rows} == {"control", "treatment"}

    query = _run_lab(
        lab_cli_bin,
        "query",
        run_dir,
        "SELECT COUNT(*) AS n FROM trials",
        "--json",
        cwd=project.root,
    )
    assert int(_table_rows(query["result"])[0]["n"]) == 4


def test_benchmark_export_build_run_writes_prediction_and_score(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project, experiment_path, rows = _prepare_benchmark_experiment(
        tmp_path,
        artifact_bundle,
        fixture_image,
    )
    assert rows[0]["schema_version"] == "task_boundary_v2"
    assert rows[0]["task"]["image"]
    assert rows[0]["workspace_seed"]["dataset_pack_ref"].startswith("sha256:")

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "bench_v0_per_task_pkg",
    )
    describe = _run_lab(
        lab_cli_bin,
        "describe",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert describe["ok"] is True
    assert describe["summary"]["tasks"] == 1

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is True

    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)
    assert (trial_dir / "benchmark_preflight.json").exists()

    conn = sqlite3.connect(run_dir / "run.sqlite")
    try:
        prediction_row = conn.execute(
            "SELECT row_json FROM benchmark_prediction_rows LIMIT 1"
        ).fetchone()
        score_row = conn.execute(
            "SELECT row_json FROM benchmark_score_rows LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert prediction_row is not None
    assert score_row is not None
    prediction = json.loads(prediction_row[0])
    score = json.loads(score_row[0])
    assert prediction["schema_version"] == "benchmark_prediction_record_v1"
    assert prediction["prediction"]["kind"] == "text"
    assert score["schema_version"] == "benchmark_score_record_v1"
    assert score["verdict"] == "missing"
    assert score["primary_metric_value"] == 0.0


def test_build_run_executes_end_to_end_in_one_command(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_BUILD_RUN"),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_run_executes_end_to_end_in_one_command",
        rows=rows,
    )

    run_payload = _run_lab(
        lab_cli_bin,
        "build-run",
        experiment_path,
        "--out",
        project.builds_dir / "build_run_pkg",
        "--executor",
        "local_docker",
        "--materialize",
        "full",
        "--json",
        cwd=project.root,
    )
    assert run_payload["ok"] is True
    assert Path(run_payload["package_dir"]).exists()
    run_dir = Path(run_payload["run"]["run_dir"])
    assert run_dir.exists()
    assert Path(run_payload["artifacts"]["run_sqlite_path"]).exists()
    assert run_payload["summary"]["tasks"] == 1

    trials = _run_lab(
        lab_cli_bin,
        "query",
        run_dir,
        "SELECT COUNT(*) AS n FROM trials",
        "--json",
        cwd=project.root,
    )
    assert int(_table_rows(trials["result"])[0]["n"]) == 1


def test_runs_command_lists_completed_run(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_RUNS"),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="runs_command_lists_completed_run",
        rows=rows,
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "runs_listing_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_id = run_payload["run"]["run_id"]

    runs = _run_lab(
        lab_cli_bin,
        "runs",
        "--json",
        cwd=project.root,
    )
    rows = _table_rows(runs["result"])
    matching = [row for row in rows if row["run_id"] == run_id]
    assert matching, rows
    assert matching[0]["experiment"] == "runs_command_lists_completed_run"
