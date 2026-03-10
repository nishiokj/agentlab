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

pytestmark = pytest.mark.e2e_cli


def _preferred_docker_host() -> str | None:
    explicit = os.environ.get("DOCKER_HOST", "").strip()
    if explicit:
        return explicit
    for candidate in [
        Path.home() / ".docker" / "run" / "docker.sock",
        Path.home() / ".orbstack" / "run" / "docker.sock",
        Path("/var/run/docker.sock"),
    ]:
        if candidate.exists():
            return f"unix://{candidate}"
    return None


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


def _build_repo_subset_archive(project: ProjectLayout, archive_name: str, paths: list[str]) -> Path:
    staging_root = project.root / f".{archive_name}_staging"
    if staging_root.exists():
        shutil.rmtree(staging_root)
    staging_root.mkdir(parents=True, exist_ok=True)
    _copy_repo_subset(staging_root, paths)

    archive_path = project.root / f"{archive_name}.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        for path in sorted(staging_root.rglob("*")):
            archive.add(path, arcname=str(path.relative_to(staging_root)))
    return archive_path


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


def _workspace_overlay(path: str, content: str, executable: bool = False) -> dict[str, Any]:
    return {
        "path": path,
        "content": content,
        "encoding": "utf8",
        "executable": executable,
    }


def _aux_mount(dataset_pack_ref: str, mount_path: str) -> dict[str, Any]:
    return {
        "dataset_pack_ref": dataset_pack_ref,
        "mount_path": mount_path,
    }


def _task_row(
    *,
    task_id: str,
    expected_variant: str = "control",
    task_image: str | None = None,
    workspace_base_ref: str | None = None,
    workspace_overlays: list[dict[str, Any]] | None = None,
    workspace_aux_mounts: list[dict[str, Any]] | None = None,
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
        environment = {"image": task_image}
    else:
        environment = None
    if observe is not None:
        task["observe"] = observe
    workspace_base: dict[str, Any]
    workspace_mode: str
    if workspace_base_ref is not None:
        workspace_mode = "patch"
        workspace_base = {
            "kind": "dataset_pack",
            "dataset_pack_ref": workspace_base_ref,
        }
    else:
        workspace_mode = "scratch"
        workspace_base = {"kind": "empty"}
    row: dict[str, Any] = {
        "schema_version": "task_boundary_v3",
        "task": task,
        "workspace": {
            "mode": workspace_mode,
            "base": workspace_base,
            "overlays": workspace_overlays or [],
            "aux_mounts": workspace_aux_mounts or [],
        },
        "limits": {},
    }
    if environment is not None:
        row["environment"] = environment
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
        "bundle": artifact_path,
        "io": {
            "input_arg": "--input",
            "output_arg": "--output",
        },
        "env": {},
        "env_from_host": [],
    }
    runtime_sandbox: dict[str, Any] = {
        "executor": "docker",
        "image_source": image_source,
        "profile": "default",
        "network": "none",
    }
    if image_source != "per_task":
        runtime_sandbox["image"] = image_tag
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
            "schema_version": "task_boundary_v3",
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
            "sandbox": runtime_sandbox,
            "dependencies": {
                "file_staging": [],
                "services": [],
            },
            "policy": {
                "timeout_ms": DEFAULT_RUN_TIMEOUT_SECONDS * 1000,
                "network": {
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
    dataset_path = project.experiment_data_dir / f"{exp_id}.task_boundary_v3.jsonl"
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
    dataset_path = project.experiment_data_dir / "bench_v0.task_boundary_v3.jsonl"
    script = REPO_ROOT / "bench" / "integration" / "agentlab" / "export_bench_suite_to_jsonl.py"
    _run_python(
        script,
        "--suite",
        "v0",
        "--output",
        dataset_path,
        "--default-task-image",
        base_task_image,
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
    task_image = rows[0]["environment"]["image"]
    _run(["docker", "tag", image_tag, task_image], cwd=project.root)

    support_root = project.root / "bench_support"
    support_root.mkdir(parents=True, exist_ok=True)
    support_archive = _build_repo_subset_archive(project, "bench_support", BENCH_SUPPORT_PATHS)
    grader_wrapper = support_root / "bench_benchmark_adapter_entry.py"
    grader_wrapper.write_text(
        "from __future__ import annotations\n"
        "import pathlib\n"
        "import runpy\n"
        "import tarfile\n"
        "archive = pathlib.Path('/agentlab/deps/bench_support.tar.gz')\n"
        "support_root = pathlib.Path('/tmp/agentlab_bench_support')\n"
        "adapter_path = support_root / 'bench/integration/agentlab/bench_benchmark_adapter.py'\n"
        "if not adapter_path.exists():\n"
        "    support_root.mkdir(parents=True, exist_ok=True)\n"
        "    with tarfile.open(archive, 'r:gz') as bundle:\n"
        "        bundle.extractall(support_root)\n"
        "runpy.run_path(str(adapter_path), run_name='__main__')\n",
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
        {
            "source_from_host": _relpath(support_archive, project.experiments_dir),
            "destination_path": "/agentlab/deps/bench_support.tar.gz",
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


def _failed_checks(payload: dict[str, Any], *, name: str | None = None) -> list[dict[str, Any]]:
    checks = [check for check in payload["checks"] if not check["passed"]]
    if name is not None:
        checks = [check for check in checks if check["name"] == name]
    return checks


def _assert_failed_check_contains(payload: dict[str, Any], check_name: str, needle: str) -> None:
    checks = _failed_checks(payload, name=check_name)
    assert checks, payload["checks"]
    assert any(needle in check["message"] for check in checks), checks


@pytest.fixture(scope="session")
def lab_cli_bin() -> Path:
    env_value = os.environ.get("LAB_CLI_BIN", "").strip()
    if env_value:
        path = Path(env_value)
    else:
        _run(["cargo", "build", "-p", "lab-cli", "--release"], cwd=REPO_ROOT / "rust")
        path = REPO_ROOT / "rust" / "target" / "release" / "lab-cli"
    assert path.exists(), f"lab-cli binary not found: {path}"
    _run([str(path), "--help"], cwd=REPO_ROOT)
    return path


@pytest.fixture(scope="session", autouse=True)
def docker_cli_env(tmp_path_factory: pytest.TempPathFactory) -> None:
    docker_config_dir = tmp_path_factory.mktemp("docker-config")
    (docker_config_dir / "config.json").write_text(
        json.dumps(_docker_auths_without_creds_store(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    previous: dict[str, str | None] = {
        "DOCKER_HOST": os.environ.get("DOCKER_HOST"),
        "DOCKER_CONFIG": os.environ.get("DOCKER_CONFIG"),
        "DOCKER_CONTEXT": os.environ.get("DOCKER_CONTEXT"),
    }
    preferred_host = _preferred_docker_host()
    if preferred_host is not None:
        os.environ["DOCKER_HOST"] = preferred_host
    os.environ["DOCKER_CONFIG"] = str(docker_config_dir)
    os.environ.pop("DOCKER_CONTEXT", None)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


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

    compat_probe_path = bin_dir / "compat-probe-agent"
    compat_probe_path.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'if [ "${AGENTLAB_PREFLIGHT_SMOKE:-0}" = "1" ]; then\n'
        "    echo \"warn: CPU lacks AVX support, strange crashes may occur.\" >&2\n"
        "    echo \"[harness] Agent 'coding' references tool 'Skill' which is not available\"\n"
        "fi\n"
        "out=''\n"
        "while [ \"$#\" -gt 0 ]; do\n"
        "  case \"$1\" in\n"
        "    --output)\n"
        "      out=\"$2\"\n"
        "      shift 2\n"
        "      ;;\n"
        "    --input)\n"
        "      shift 2\n"
        "      ;;\n"
        "    *)\n"
        "      shift\n"
        "      ;;\n"
        "  esac\n"
        "done\n"
        "test -n \"$out\"\n"
        "mkdir -p \"$(dirname \"$out\")\"\n"
        "printf '%s\\n' '{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\",\"objective\":{\"name\":\"resolved\",\"value\":1.0}}' > \"$out\"\n",
        encoding="utf-8",
    )
    os.chmod(compat_probe_path, 0o755)

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


#
# Build and preflight boundaries
#


@pytest.mark.e2e_build_preflight
def test_build_preflight_describe_happy_path(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_HAPPY", expected_variant="control", task_image=fixture_image),
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


@pytest.mark.e2e_build_preflight
def test_run_plane_rejects_authoring_input(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_AUTHORING_REJECT", task_image=fixture_image),
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


@pytest.mark.e2e_build_preflight
def test_tampered_package_fails_preflight(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_TAMPER", task_image=fixture_image),
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


@pytest.mark.e2e_build_preflight
def test_missing_per_task_image_fails_preflight(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(
            task_id="TASK_MISSING_IMAGE",
            task_image="",
            workspace_overlays=[_workspace_overlay("README.md", "materialized")],
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
    _assert_failed_check_contains(
        preflight,
        "container_ready",
        "failed to parse task image boundary rows",
    )


@pytest.mark.e2e_build_preflight
def test_build_rejects_legacy_experiment_version(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_LEGACY_VERSION", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_rejects_legacy_experiment_version",
        rows=rows,
    )
    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["version"] = "1.0"
    _write_yaml(experiment_path, experiment)

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "legacy_version_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(build, "legacy experiment version '1.0'")


@pytest.mark.e2e_build_preflight
def test_preflight_rejects_known_agent_runtime_compatibility_blockers(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_COMPAT_BLOCKER", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_rejects_known_agent_runtime_compatibility_blockers",
        rows=rows,
        agent_command=["compat-probe-agent"],
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "compatibility_blocker_pkg",
    )
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is False
    _assert_failed_check_contains(
        preflight,
        "agent_runtime_reachable",
        "CPU lacks AVX support",
    )
    _assert_failed_check_contains(
        preflight,
        "agent_runtime_reachable",
        "references tool 'Skill' which is not available",
    )


@pytest.mark.e2e_build_preflight
def test_build_rejects_missing_agent_entrypoint_command(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_MISSING_ENTRYPOINT", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_rejects_missing_agent_entrypoint_command",
        rows=rows,
        agent_command=["missing-e2e-agent"],
    )

    payload = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "missing_agent_entrypoint_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(payload, "did not resolve to artifact executable")


@pytest.mark.e2e_build_preflight
def test_preflight_rejects_nonzero_contract_smoke_exit(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_SMOKE_EXIT_NONZERO", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_rejects_nonzero_contract_smoke_exit",
        rows=rows,
        baseline_bindings={"variant_label": "control", "exit_code": 17},
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "preflight_nonzero_smoke_pkg",
    )
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is False
    _assert_failed_check_contains(
        preflight,
        "agent_runtime_reachable",
        "contract smoke exited with status 17",
    )


@pytest.mark.e2e_build_preflight
def test_preflight_rejects_missing_result_payload_contract_smoke(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_SMOKE_RESULT_MISSING", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_rejects_missing_result_payload_contract_smoke",
        rows=rows,
        baseline_bindings={"variant_label": "control", "skip_result_write": True},
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "preflight_missing_result_smoke_pkg",
    )
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is False
    _assert_failed_check_contains(
        preflight,
        "agent_runtime_reachable",
        "contract smoke did not write result payload",
    )


#
# Runtime contract boundaries
#


@pytest.mark.e2e_runtime
def test_run_happy_path_writes_sqlite_and_artifacts(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_RUN_HAPPY", task_image=fixture_image),
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


@pytest.mark.e2e_runtime
def test_run_records_nonzero_agent_exit_as_failed_trial(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_EXIT_NONZERO", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="run_records_nonzero_agent_exit_as_failed_trial",
        rows=rows,
        baseline_bindings={"variant_label": "control", "runtime_only_exit_code": 17},
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "run_nonzero_exit_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    conn = sqlite3.connect(run_dir / "run.sqlite")
    try:
        raw_row = conn.execute("SELECT row_json FROM trial_rows LIMIT 1").fetchone()
        raw_run_control = conn.execute(
            "SELECT value_json FROM runtime_kv WHERE key = 'run_control_v2'"
        ).fetchone()
    finally:
        conn.close()
    assert raw_row is not None
    assert raw_run_control is not None
    row = json.loads(raw_row[0])
    assert row["outcome"] == "error"
    assert row["status_code"] == "17"
    assert row["success"] is False
    assert row["primary_metric_value"] == 0.0

    run_control = json.loads(raw_run_control[0])
    assert run_control["status"] == "completed"
    trial_state = _read_json(_only_trial_dir(run_dir) / "trial_state.json")
    assert trial_state["status"] == "failed"
    assert trial_state["exit_reason"] == "agent_exit_nonzero"


@pytest.mark.e2e_runtime
def test_run_records_malformed_result_payload_as_failed_trial(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_BAD_RESULT", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="run_records_malformed_result_payload_as_failed_trial",
        rows=rows,
        baseline_bindings={
            "variant_label": "control",
            "runtime_only_emit_invalid_result_json": True,
        },
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "run_bad_result_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    conn = sqlite3.connect(run_dir / "run.sqlite")
    try:
        raw_row = conn.execute("SELECT row_json FROM trial_rows LIMIT 1").fetchone()
        raw_run_control = conn.execute(
            "SELECT value_json FROM runtime_kv WHERE key = 'run_control_v2'"
        ).fetchone()
    finally:
        conn.close()
    assert raw_row is not None
    assert raw_run_control is not None
    row = json.loads(raw_row[0])
    assert row["outcome"] == "error"
    assert row["status_code"] == "0"
    assert row["success"] is False
    assert row["primary_metric_name"] == "success"
    assert row["primary_metric_value"] == 0.0

    run_control = json.loads(raw_run_control[0])
    assert run_control["status"] == "completed"
    trial_state = _read_json(_only_trial_dir(run_dir) / "trial_state.json")
    assert trial_state["status"] == "failed"
    assert trial_state["exit_reason"] == "result_parse_error"


@pytest.mark.e2e_runtime
def test_run_records_missing_result_payload_as_failed_trial(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_RESULT_MISSING", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="run_records_missing_result_payload_as_failed_trial",
        rows=rows,
        baseline_bindings={
            "variant_label": "control",
            "runtime_only_skip_result_write": True,
        },
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "run_missing_result_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    conn = sqlite3.connect(run_dir / "run.sqlite")
    try:
        raw_row = conn.execute("SELECT row_json FROM trial_rows LIMIT 1").fetchone()
        raw_run_control = conn.execute(
            "SELECT value_json FROM runtime_kv WHERE key = 'run_control_v2'"
        ).fetchone()
    finally:
        conn.close()
    assert raw_row is not None
    assert raw_run_control is not None
    row = json.loads(raw_row[0])
    assert row["outcome"] == "error"
    assert row["status_code"] == "0"
    assert row["success"] is False
    assert row["primary_metric_name"] == "success"
    assert row["primary_metric_value"] == 0.0

    run_control = json.loads(raw_run_control[0])
    assert run_control["status"] == "completed"
    trial_state = _read_json(_only_trial_dir(run_dir) / "trial_state.json")
    assert trial_state["status"] == "failed"
    assert trial_state["exit_reason"] == "result_error"


@pytest.mark.e2e_runtime
def test_failed_run_cleans_trial_scratch(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_SCRATCH_CLEANUP", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="failed_run_cleans_trial_scratch",
        rows=rows,
        baseline_bindings={
            "variant_label": "control",
            "runtime_only_skip_result_write": True,
        },
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "failed_run_cleans_trial_scratch_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    scratch_root = run_dir / ".scratch"
    assert not scratch_root.exists() or not any(scratch_root.iterdir()), list(scratch_root.glob("*"))


@pytest.mark.e2e_runtime
def test_run_ignores_malformed_trajectory_lines_and_records_parse_error_event(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_BAD_TRAJECTORY", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="run_ignores_malformed_trajectory_lines_and_records_parse_error_event",
        rows=rows,
        baseline_bindings={"variant_label": "control", "emit_invalid_trajectory_json": True},
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "run_bad_trajectory_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    conn = sqlite3.connect(run_dir / "run.sqlite")
    try:
        raw_trial = conn.execute("SELECT row_json FROM trial_rows LIMIT 1").fetchone()
        raw_events = conn.execute("SELECT row_json FROM event_rows ORDER BY seq").fetchall()
    finally:
        conn.close()
    assert raw_trial is not None
    row = json.loads(raw_trial[0])
    assert row["outcome"] == "success"
    assert row["status_code"] == "0"

    event_rows = [json.loads(raw_row[0]) for raw_row in raw_events]
    parse_error = next(row for row in event_rows if row["event_type"] == "trajectory_parse_error")
    assert "invalid trajectory json" in parse_error["payload"]["raw_line"]
    assert parse_error["payload"]["error"]
    assert any(row["event_type"] == "e2e_agent.start" for row in event_rows)
    assert any(row["event_type"] == "e2e_agent.finish" for row in event_rows)


#
# Materialization and query surfaces
#


@pytest.mark.e2e_cli_surface
def test_materialize_full_persists_workspace_base_overlays_and_aux_mounts(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project = _make_project(tmp_path, artifact_bundle)
    seed_ref = _materialize_dataset_pack(project, {"seed/README.txt": "seed ready\n"})
    mount_ref = _materialize_dataset_pack(project, {"reference.txt": "mounted reference\n"})
    dataset_path = project.experiment_data_dir / "materialize_full.task_boundary_v3.jsonl"
    rows = [
        _task_row(
            task_id="TASK_MATERIALIZE",
            task_image=fixture_image,
            workspace_base_ref=seed_ref,
            workspace_overlays=[_workspace_overlay("overlay/config.txt", "overlay ready\n")],
            workspace_aux_mounts=[_aux_mount(mount_ref, "/agentlab/workspace/mounted")],
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
        exp_id="materialize_full_persists_workspace_base_overlays_and_aux_mounts",
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
    assert observed["obs_mount_file_exists"] == 0
    assert "obs_mount_file_text_match" not in observed
    assert "obs_mount_file_write_blocked" not in observed


@pytest.mark.e2e_cli_surface
def test_ab_run_exposes_views_and_query_surface(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_AB_1", expected_variant="treatment", task_image=fixture_image),
        _task_row(task_id="TASK_AB_2", expected_variant="treatment", task_image=fixture_image),
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


#
# Benchmark contract boundaries
#


@pytest.mark.e2e_benchmark
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
    assert rows[0]["schema_version"] == "task_boundary_v3"
    assert rows[0]["environment"]["image"]
    assert rows[0]["workspace"]["base"]["dataset_pack_ref"].startswith("sha256:")

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


@pytest.mark.e2e_build_preflight
@pytest.mark.e2e_benchmark
def test_benchmark_preflight_rejects_unreachable_grader_command(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project, experiment_path, _ = _prepare_benchmark_experiment(
        tmp_path,
        artifact_bundle,
        fixture_image,
    )
    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["benchmark"]["adapter"]["command"] = [
        "python3",
        "/agentlab/deps/missing_benchmark_adapter_entry.py",
    ]
    _write_yaml(experiment_path, experiment)

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "bench_v0_unreachable_grader_pkg",
    )
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is False
    _assert_failed_check_contains(
        preflight,
        "benchmark_grader_reachable",
        "benchmark grader contract smoke failed in required images",
    )
    _assert_failed_check_contains(
        preflight,
        "benchmark_grader_reachable",
        "contract smoke exited with status 125",
    )


@pytest.mark.e2e_build_preflight
@pytest.mark.e2e_benchmark
def test_benchmark_preflight_rejects_grader_that_never_writes_score_contract(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project, experiment_path, _ = _prepare_benchmark_experiment(
        tmp_path,
        artifact_bundle,
        fixture_image,
    )
    support_root = project.root / "bench_support"
    grader_wrapper = support_root / "bench_benchmark_adapter_entry.py"
    grader_wrapper.write_text(
        "from __future__ import annotations\n"
        "print('grader launched but wrote nothing')\n",
        encoding="utf-8",
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "bench_v0_missing_score_contract_pkg",
    )
    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is False
    _assert_failed_check_contains(
        preflight,
        "benchmark_grader_reachable",
        "contract smoke did not write benchmark prediction record",
    )


@pytest.mark.e2e_benchmark
def test_benchmark_run_marks_missing_result_before_grader_executes(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project = _make_project(tmp_path, artifact_bundle)
    rows = [
        _task_row(task_id="TASK_BENCH_RESULT_MISSING", task_image=fixture_image),
    ]
    dataset_path = project.experiment_data_dir / "benchmark_missing_result.task_boundary_v3.jsonl"
    _write_jsonl(dataset_path, rows)

    support_root = project.root / "bench_support"
    support_root.mkdir(parents=True, exist_ok=True)
    support_archive = _build_repo_subset_archive(project, "bench_support", BENCH_SUPPORT_PATHS)
    grader_wrapper = support_root / "bench_benchmark_adapter_entry.py"
    grader_wrapper.write_text(
        "from __future__ import annotations\n"
        "import pathlib\n"
        "import runpy\n"
        "import tarfile\n"
        "archive = pathlib.Path('/agentlab/deps/bench_support.tar.gz')\n"
        "support_root = pathlib.Path('/tmp/agentlab_bench_support')\n"
        "adapter_path = support_root / 'bench/integration/agentlab/bench_benchmark_adapter.py'\n"
        "if not adapter_path.exists():\n"
        "    support_root.mkdir(parents=True, exist_ok=True)\n"
        "    with tarfile.open(archive, 'r:gz') as bundle:\n"
        "        bundle.extractall(support_root)\n"
        "runpy.run_path(str(adapter_path), run_name='__main__')\n",
        encoding="utf-8",
    )

    experiment_path = project.experiments_dir / "benchmark_missing_result.yaml"
    experiment = _base_experiment(
        exp_id="benchmark_run_marks_missing_result_before_grader_executes",
        dataset_path=_relpath(dataset_path, project.experiments_dir),
        artifact_path=_relpath(project.agents_dir / "agent-runtime.tar.gz", project.experiments_dir),
        image_tag=fixture_image,
        benchmark=True,
        baseline_bindings={
            "variant_label": "control",
            "runtime_only_skip_result_write": True,
        },
    )
    experiment["dataset"]["limit"] = 1
    experiment["runtime"]["dependencies"]["file_staging"] = [
        {
            "source_from_host": _relpath(grader_wrapper, project.experiments_dir),
            "destination_path": "/agentlab/deps/bench_benchmark_adapter_entry.py",
            "required": True,
            "read_only": True,
        },
        {
            "source_from_host": _relpath(support_archive, project.experiments_dir),
            "destination_path": "/agentlab/deps/bench_support.tar.gz",
            "required": True,
            "read_only": True,
        },
    ]
    experiment["benchmark"]["adapter"]["command"] = [
        "python3",
        "/agentlab/deps/bench_benchmark_adapter_entry.py",
    ]
    _write_yaml(experiment_path, experiment)

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "benchmark_missing_result_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)

    conn = sqlite3.connect(run_dir / "run.sqlite")
    try:
        raw_row = conn.execute("SELECT row_json FROM trial_rows LIMIT 1").fetchone()
    finally:
        conn.close()
    assert raw_row is not None
    row = json.loads(raw_row[0])
    assert row["outcome"] == "error"
    assert row["success"] is False
    assert row["metrics"]["grade_error"] is True
    assert row["metrics"]["grade_error_reason"] == "result_missing"

    harness_stderr = (trial_dir / "harness_stderr.log").read_text(encoding="utf-8")
    assert "No such file or directory: '/agentlab/out/result.json'" not in harness_stderr


#
# CLI operator surfaces
#


@pytest.mark.e2e_cli_surface
def test_build_run_executes_end_to_end_in_one_command(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_BUILD_RUN", task_image=fixture_image),
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


@pytest.mark.e2e_cli_surface
def test_runs_command_lists_completed_run(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_RUNS", task_image=fixture_image),
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
