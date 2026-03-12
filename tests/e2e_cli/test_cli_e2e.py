from __future__ import annotations

import base64
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
    payload, _ = _run_json_with_process(args, cwd=cwd, env=env, expected_exit=expected_exit)
    return payload


def _run_json_with_process(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected_exit: int = 0,
) -> tuple[dict[str, Any], subprocess.CompletedProcess[str]]:
    proc = _run(args, cwd=cwd, env=env, expected_exit=expected_exit)
    stdout = proc.stdout.strip()
    if not stdout:
        raise AssertionError(f"expected JSON output from {args}, got empty stdout")
    try:
        return json.loads(stdout), proc
    except json.JSONDecodeError as exc:  # pragma: no cover
        raise AssertionError(f"invalid JSON output from {args}: {stdout}") from exc


def _run_lab(
    lab_cli_bin: Path,
    *args: str | Path,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected_exit: int = 0,
) -> dict[str, Any]:
    rendered = [str(lab_cli_bin), *(str(arg) for arg in args)]
    return _run_json(rendered, cwd=cwd, env=env, expected_exit=expected_exit)


def _run_lab_with_process(
    lab_cli_bin: Path,
    *args: str | Path,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected_exit: int = 0,
) -> tuple[dict[str, Any], subprocess.CompletedProcess[str]]:
    rendered = [str(lab_cli_bin), *(str(arg) for arg in args)]
    return _run_json_with_process(rendered, cwd=cwd, env=env, expected_exit=expected_exit)


def _run_python(*args: str | Path, cwd: Path, expected_exit: int = 0) -> subprocess.CompletedProcess[str]:
    rendered = [sys.executable, *(str(arg) for arg in args)]
    return _run(rendered, cwd=cwd, expected_exit=expected_exit)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


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


def _dependency_file(
    path: str,
    content: str,
    *,
    encoding: str = "utf8",
    executable: bool = False,
) -> dict[str, Any]:
    return {
        "path": path,
        "content": content,
        "encoding": encoding,
        "executable": executable,
    }


def _dependency_file_from_host(
    path: str,
    source: Path,
    *,
    executable: bool = False,
) -> dict[str, Any]:
    return _dependency_file(
        path,
        base64.b64encode(source.read_bytes()).decode("ascii"),
        encoding="base64",
        executable=executable,
    )


def _task_row(
    *,
    task_id: str,
    expected_variant: str = "control",
    task_image: str | None = None,
    workspace_base_ref: str | None = None,
    workspace_overlays: list[dict[str, Any]] | None = None,
    workspace_aux_mounts: list[dict[str, Any]] | None = None,
    dependency_files: list[dict[str, Any]] | None = None,
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
        "task": task,
        "workspace": {
            "mode": workspace_mode,
            "base": workspace_base,
            "overlays": workspace_overlays or [],
            "aux_mounts": workspace_aux_mounts or [],
        },
        "dependencies": {
            "files": dependency_files or [],
        },
        "limits": {},
    }
    if environment is not None:
        row["environment"] = environment
    return row


def _strict_task_declaration(row: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(json.dumps(row))
    payload["schema_version"] = "task_declaration_v1"
    return payload


def _assert_packaged_task_declaration(
    packaged_row: dict[str, Any],
    public_row: dict[str, Any],
) -> None:
    assert packaged_row["schema_version"] == "task_declaration_v1"
    assert packaged_row["task"] == public_row["task"]
    assert packaged_row["environment"] == public_row["environment"]
    assert packaged_row["dependencies"] == public_row["dependencies"]
    assert packaged_row["workspace"]["mode"] == public_row["workspace"]["mode"]
    assert packaged_row["workspace"]["overlays"] == public_row["workspace"]["overlays"]
    assert packaged_row["workspace"]["aux_mounts"] == public_row["workspace"]["aux_mounts"]
    assert packaged_row["workspace"]["base"]["kind"] == public_row["workspace"]["base"]["kind"]
    assert packaged_row["limits"] == {
        "max_steps": None,
        "max_total_tokens": None,
        "max_tool_calls": None,
        "trial_seconds": None,
    }


def _init_agent_eval_experiment(project: ProjectLayout) -> Path:
    lab_cli_env = os.environ.get("LAB_CLI_BIN", "").strip()
    if lab_cli_env:
        lab_cli_path = Path(lab_cli_env).expanduser()
    else:
        lab_cli_path = REPO_ROOT / "rust" / "target" / "release" / "lab-cli"
    assert lab_cli_path.exists(), f"lab-cli binary not found: {lab_cli_path}"
    _run(
        [str(lab_cli_path), "init", "--profile", "agent-eval", "--in-place", "--force"],
        cwd=project.root,
    )
    experiment_path = project.root / "experiment.yaml"
    assert experiment_path.exists(), experiment_path
    return experiment_path


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
    *,
    run_args: tuple[str | Path, ...] = (),
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    expected_exit: int = 0,
) -> dict[str, Any]:
    payload = _run_lab(
        lab_cli_bin,
        "run",
        package_dir,
        *run_args,
        "--materialize",
        "full",
        "--json",
        cwd=cwd or project.root,
        env=env,
        expected_exit=expected_exit,
    )
    if expected_exit == 0:
        assert payload["ok"] is True
    return payload


def _run_package_with_process(
    lab_cli_bin: Path,
    project: ProjectLayout,
    package_dir: Path,
    *,
    run_args: tuple[str | Path, ...] = (),
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    expected_exit: int = 0,
) -> tuple[dict[str, Any], subprocess.CompletedProcess[str]]:
    payload, proc = _run_lab_with_process(
        lab_cli_bin,
        "run",
        package_dir,
        *run_args,
        "--materialize",
        "full",
        "--json",
        cwd=cwd or project.root,
        env=env,
        expected_exit=expected_exit,
    )
    if expected_exit == 0:
        assert payload["ok"] is True
    return payload, proc


def _load_agent_report(trial_dir: Path) -> dict[str, Any]:
    return _read_json(trial_dir / "out" / "agent_report.json")


def _sha256_bytes(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _reseal_package_dir(package_dir: Path) -> None:
    checksums: dict[str, str] = {}
    for path in sorted(package_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.name in {"manifest.json", "checksums.json", "package.lock"}:
            continue
        rel = path.relative_to(package_dir).as_posix()
        checksums[rel] = _sha256_bytes(path.read_bytes())

    _write_json(
        package_dir / "checksums.json",
        {
            "schema_version": "sealed_package_checksums_v2",
            "files": checksums,
        },
    )
    package_digest = _sha256_bytes(_canonical_json(checksums).encode("utf-8"))
    _write_json(
        package_dir / "package.lock",
        {
            "schema_version": "sealed_package_lock_v1",
            "package_digest": package_digest,
        },
    )
    manifest = _read_json(package_dir / "manifest.json")
    manifest["package_digest"] = package_digest
    _write_json(package_dir / "manifest.json", manifest)


def _assert_text_tree_excludes_host_paths(root: Path, disallowed_paths: list[Path]) -> None:
    candidate_suffixes = {".json", ".jsonl", ".log", ".md", ".txt", ".yaml", ".yml"}
    needles = [str(path) for path in disallowed_paths]
    matches: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix not in candidate_suffixes and path.name not in {
            "run_stdout",
            "run_stderr",
        }:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for needle in needles:
            if needle and needle in text:
                matches.append(f"{path}: {needle}")
                break
    assert not matches, "run outputs leaked source-tree host paths:\n" + "\n".join(matches)


def _assert_command_output_excludes_host_paths(
    proc: subprocess.CompletedProcess[str],
    disallowed_paths: list[Path],
) -> None:
    needles = [str(path) for path in disallowed_paths]
    matches: list[str] = []
    for label, text in [("stdout", proc.stdout), ("stderr", proc.stderr)]:
        for needle in needles:
            if needle and needle in text:
                matches.append(f"{label}: {needle}")
    assert not matches, "command output leaked source-tree host paths:\n" + "\n".join(matches)


def _assert_message_excludes_host_paths(payload: dict[str, Any], disallowed_paths: list[Path]) -> None:
    message = payload["error"]["message"]
    for path in disallowed_paths:
        assert str(path) not in message, message


def _prepare_runtime_file_ref_project(
    tmp_path: Path,
    artifact_bundle: Path,
    fixture_image: str,
    *,
    exp_id: str,
) -> tuple[ProjectLayout, Path, Path, str]:
    source_root = tmp_path / exp_id
    support_file = source_root / "overrides" / "defaults.json"
    support_file.parent.mkdir(parents=True, exist_ok=True)
    support_file.write_text('{"profile":"portable-run"}\n', encoding="utf-8")
    runtime_config_path = "/agentlab/deps/overrides/defaults.json"
    rows = [
        _task_row(
            task_id="TASK_MOVED_PACKAGE",
            task_image=fixture_image,
            observe={
                "runtime_support_file": {
                    "path": runtime_config_path,
                    "expect_text": '"profile":"portable-run"',
                }
            },
        ),
    ]
    project, experiment_path = _create_simple_project(
        source_root,
        artifact_bundle,
        fixture_image,
        exp_id=exp_id,
        rows=rows,
        agent_command=["e2e-agent", "--config", "overrides/defaults.json"],
        agent_env={"E2E_CONFIG_PATH": "overrides/defaults.json"},
    )
    return project, experiment_path, source_root, runtime_config_path


def _assert_trial_hermetic(run_dir: Path, trial_dir: Path) -> None:
    attestation = _read_json(run_dir / "attestation.json")
    grades = attestation.get("grades") or attestation.get("grades_summary") or {}
    assert grades.get("isolation_grade") == "hermetic"

    inventory = _read_json(trial_dir / "state_inventory.json")
    assert inventory["planes"]["agent_runtime"]["executor"] == "docker"
    assert inventory["planes"]["task_sandbox"]["executor"] == "docker"


def _create_simple_project(
    tmp_path: Path,
    artifact_bundle: Path,
    image_tag: str,
    *,
    exp_id: str,
    rows: list[dict[str, Any]],
    baseline_bindings: dict[str, Any] | None = None,
    variant_plan: list[dict[str, Any]] | None = None,
    comparison: str = "paired",
    benchmark: bool = False,
    agent_command: list[str] | None = None,
    agent_env: dict[str, str] | None = None,
    agent_network: str = "none",
    agent_root_read_only: bool = True,
    agent_user: str | None = None,
    state_policy: str | None = None,
    project: ProjectLayout | None = None,
) -> tuple[ProjectLayout, Path]:
    assert not benchmark, "benchmark happy-path tests must use built-in benchmark authoring"
    project = project or _make_project(tmp_path, artifact_bundle)
    experiment_path = _init_agent_eval_experiment(project)
    dataset_path = project.root / "tasks.jsonl"
    dataset_rows = [json.loads(json.dumps(row)) for row in rows]
    _write_jsonl(dataset_path, dataset_rows)
    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["experiment"]["id"] = exp_id
    experiment["experiment"]["name"] = exp_id
    experiment["experiment"]["owner"] = "e2e"
    experiment["experiment"]["description"] = f"CLI E2E fixture for {exp_id}"
    experiment["experiment"]["tags"] = ["e2e", "cli", "docker"]
    experiment["dataset"]["suite_id"] = exp_id
    experiment["dataset"]["path"] = _relpath(dataset_path, project.root)
    experiment["dataset"]["split_id"] = "test"
    experiment["dataset"]["limit"] = len(dataset_rows)
    experiment["design"]["comparison"] = comparison
    experiment["design"]["replications"] = 1
    experiment["design"]["random_seed"] = 42
    experiment["design"]["shuffle_tasks"] = False
    experiment["design"]["max_concurrency"] = 1
    policies = experiment["design"].setdefault("policies", {})
    if state_policy is None:
        policies.pop("state", None)
        if not policies:
            experiment["design"].pop("policies", None)
    else:
        policies["state"] = state_policy
    experiment["baseline"]["variant_id"] = "control"
    experiment["baseline"]["bindings"] = baseline_bindings or {"variant_label": "control"}
    experiment["variant_plan"] = variant_plan or []
    runtime_agent_runtime = experiment["runtime"]["agent_runtime"]
    runtime_agent_runtime["command"] = agent_command or ["e2e-agent"]
    runtime_agent_runtime["artifact"] = _relpath(project.agents_dir / "agent-runtime.tar.gz", project.root)
    runtime_agent_runtime["image"] = image_tag
    runtime_agent_runtime["network"] = agent_network
    runtime_agent_runtime["root_read_only"] = agent_root_read_only
    runtime_agent_runtime["env"] = agent_env or {}
    if agent_user is None:
        runtime_agent_runtime.pop("user", None)
    else:
        runtime_agent_runtime["user"] = agent_user
    _write_yaml(experiment_path, experiment)
    return project, experiment_path


def _copy_custom_benchmark_grader(project: ProjectLayout) -> Path:
    support_dir = project.root / "benchmark_support"
    support_dir.mkdir(parents=True, exist_ok=True)
    target = support_dir / "custom_benchmark_grader.py"
    shutil.copy2(FIXTURES_DIR / "custom_benchmark_grader.py", target)
    return target


def _create_custom_benchmark_project(
    tmp_path: Path,
    artifact_bundle: Path,
    image_tag: str,
    *,
    exp_id: str,
    baseline_bindings: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> tuple[ProjectLayout, Path, list[dict[str, Any]]]:
    rows = rows or [_task_row(task_id="TASK_CUSTOM_BENCHMARK", task_image=image_tag)]
    for row in rows:
        row["task"]["benchmark"] = {
            "adapter_id": "custom_benchmark_grader",
            "name": "custom_e2e_benchmark",
            "split": "test",
        }
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        image_tag,
        exp_id=exp_id,
        rows=rows,
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
            "command": [
                "python3",
                _relpath(grader_path, project.root),
            ],
        },
    }
    _write_yaml(experiment_path, experiment)
    return project, experiment_path, rows


def _prepare_custom_benchmark_experiment(
    tmp_path: Path,
    artifact_bundle: Path,
    image_tag: str,
) -> tuple[ProjectLayout, Path, list[dict[str, Any]]]:
    rows = [_task_row(task_id="TASK_CUSTOM_BENCHMARK", task_image=image_tag)]
    project, experiment_path, rows = _create_custom_benchmark_project(
        tmp_path,
        artifact_bundle,
        image_tag=image_tag,
        exp_id="custom_benchmark_e2e",
        baseline_bindings={"variant_label": "control"},
        rows=rows,
    )
    return project, experiment_path, rows


def _write_builtin_benchmark_authoring(
    *,
    benchmark_name: str,
    artifact_bundle: Path,
    agent_image: str,
    exp_id: str,
    limit: int = 1,
    baseline_bindings: dict[str, Any] | None = None,
) -> Path:
    experiments_dir = REPO_ROOT / ".lab" / "experiments"
    experiments_dir.mkdir(parents=True, exist_ok=True)
    experiment_path = experiments_dir / f"_tmp_{exp_id}_{uuid.uuid4().hex[:8]}.yaml"
    payload = {
        "experiment": {
            "id": exp_id,
            "name": exp_id,
            "tags": ["e2e", "cli", "builtin-benchmark", benchmark_name],
        },
        "benchmark": benchmark_name,
        "limit": limit,
        "agent": {
            "artifact": str(artifact_bundle),
            "image": agent_image,
            "command": ["e2e-agent"],
        },
        "baseline": {
            "id": "control",
            "bindings": baseline_bindings or {"variant_label": "control"},
        },
        "overrides": {
            "network": "none",
            "root_read_only": True,
        },
    }
    _write_yaml(experiment_path, payload)
    return experiment_path


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

    previous_docker_config = os.environ.get("DOCKER_CONFIG")
    os.environ["DOCKER_CONFIG"] = str(docker_config_dir)
    try:
        yield
    finally:
        if previous_docker_config is None:
            os.environ.pop("DOCKER_CONFIG", None)
        else:
            os.environ["DOCKER_CONFIG"] = previous_docker_config


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
        "out=\"${AGENTLAB_RESULT_PATH:-}\"\n"
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
def test_preflight_and_run_accept_env_file_for_required_runtime_env(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_ENV_FILE_HAPPY", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_and_run_accept_env_file_for_required_runtime_env",
        rows=rows,
        agent_env={"OPENAI_API_KEY": "$OPENAI_API_KEY"},
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "preflight_env_file_happy_path_pkg",
    )
    env_file = project.root / "runtime.env"
    env_file.write_text("OPENAI_API_KEY=test-token\n", encoding="utf-8")

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--env-file",
        env_file,
        "--json",
        cwd=project.root,
    )
    assert preflight["ok"] is True

    run_payload = _run_package(
        lab_cli_bin,
        project,
        package_dir,
        run_args=("--env-file", env_file),
    )
    assert Path(run_payload["run"]["run_dir"]).exists()


@pytest.mark.e2e_build_preflight
def test_build_rewrites_runtime_file_refs_to_runner_owned_deps_paths(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    (
        project,
        experiment_path,
        source_root,
        runtime_config_path,
    ) = _prepare_runtime_file_ref_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_rewrites_runtime_file_refs_to_runner_owned_deps_paths",
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "rewrite_runtime_file_refs_pkg",
    )
    resolved_experiment = _read_json(package_dir / "resolved_experiment.json")
    runtime = resolved_experiment["runtime"]["agent_runtime"]
    assert runtime["command"] == ["e2e-agent", "--config", runtime_config_path]
    assert runtime["env"]["E2E_CONFIG_PATH"] == runtime_config_path
    _assert_text_tree_excludes_host_paths(package_dir, [source_root])


@pytest.mark.e2e_build_preflight
def test_preflight_rejects_stale_package_with_legacy_runtime_relative_paths(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    (
        project,
        experiment_path,
        source_root,
        _runtime_config_path,
    ) = _prepare_runtime_file_ref_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_rejects_stale_package_with_legacy_runtime_relative_paths",
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "stale_runtime_relative_paths_pkg",
    )

    legacy_rel = "overrides/defaults.json"
    legacy_runtime_copy = package_dir / legacy_rel
    legacy_runtime_copy.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(package_dir / "deps" / legacy_rel, legacy_runtime_copy)

    resolved_experiment = _read_json(package_dir / "resolved_experiment.json")
    runtime = resolved_experiment["runtime"]["agent_runtime"]
    runtime["command"][2] = legacy_rel
    runtime["env"]["E2E_CONFIG_PATH"] = legacy_rel
    _write_json(package_dir / "resolved_experiment.json", resolved_experiment)

    manifest = _read_json(package_dir / "manifest.json")
    manifest["resolved_experiment"] = resolved_experiment
    _write_json(package_dir / "manifest.json", manifest)
    _reseal_package_dir(package_dir)

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(preflight, "unresolved package-relative path")
    _assert_message_excludes_host_paths(preflight, [source_root, package_dir])


@pytest.mark.e2e_build_preflight
def test_run_from_moved_package_uses_sealed_runtime_paths_without_host_path_leakage(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    (
        project,
        experiment_path,
        source_root,
        runtime_config_path,
    ) = _prepare_runtime_file_ref_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="run_from_moved_package_uses_sealed_runtime_paths_without_host_path_leakage",
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "moved_package_runtime_path_refs_pkg",
    )

    isolated_root = tmp_path / "isolated_host"
    isolated_root.mkdir(parents=True, exist_ok=True)
    moved_package_dir = isolated_root / "portable_package"
    shutil.copytree(package_dir, moved_package_dir)
    shutil.rmtree(source_root)

    run_payload, run_proc = _run_package_with_process(
        lab_cli_bin,
        project,
        moved_package_dir,
        cwd=isolated_root,
    )
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)
    agent_report = _load_agent_report(trial_dir)
    support_observation = agent_report["observations"]["runtime_support_file"]
    runtime_inputs = agent_report["runtime_inputs"]
    assert runtime_inputs["config_arg"] == runtime_config_path
    assert runtime_inputs["e2e_config_path"] == runtime_config_path
    assert support_observation["exists"] is True
    assert support_observation["kind"] == "file"
    assert support_observation["matches_expected_text"] is True
    _assert_command_output_excludes_host_paths(run_proc, [source_root, package_dir])
    _assert_text_tree_excludes_host_paths(run_dir, [source_root, package_dir])


@pytest.mark.e2e_build_preflight
def test_preflight_surfaces_missing_runtime_env_var(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_ENV_FILE_MISSING", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_surfaces_missing_runtime_env_var",
        rows=rows,
        agent_env={"OPENAI_API_KEY": "$OPENAI_API_KEY"},
    )
    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "preflight_missing_runtime_env_pkg",
    )
    preflight_env = dict(os.environ)
    preflight_env.pop("OPENAI_API_KEY", None)

    preflight = _run_lab(
        lab_cli_bin,
        "preflight",
        package_dir,
        "--json",
        cwd=project.root,
        env=preflight_env,
        expected_exit=1,
    )
    _assert_command_failed(preflight, "missing runtime binding $OPENAI_API_KEY")


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
def test_missing_per_task_image_fails_build(
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
        exp_id="missing_per_task_image_fails_build",
        rows=rows,
    )

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "missing_per_task_image_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(build, "environment.image must be a non-empty string")


@pytest.mark.e2e_build_preflight
def test_build_rejects_removed_experiment_version_field(
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
        exp_id="build_rejects_removed_experiment_version_field",
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
    _assert_command_failed(build, "legacy experiment version '1.0' is not supported")


@pytest.mark.e2e_build_preflight
def test_build_accepts_public_task_spec_rows_and_writes_packaged_task_declarations(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_UNVERSIONED_DECLARATION", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_accepts_public_task_spec_rows_and_writes_packaged_task_declarations",
        rows=rows,
    )

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "public_task_spec_pkg",
        "--json",
        cwd=project.root,
        expected_exit=0,
    )
    assert build["ok"] is True
    package_dir = Path(build["package_dir"])
    task_rows = _read_jsonl(package_dir / "tasks" / "tasks.jsonl")
    assert len(task_rows) == 1
    _assert_packaged_task_declaration(task_rows[0], rows[0])
    assert "image" not in task_rows[0]["task"]
    assert "workspace" not in task_rows[0]["task"]


@pytest.mark.e2e_build_preflight
def test_build_rejects_runtime_command_that_mentions_runner_owned_agentlab_path(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_COMMAND_PATH_LEAK", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_rejects_runtime_command_that_mentions_runner_owned_agentlab_path",
        rows=rows,
    )
    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["runtime"]["agent_runtime"]["command"] = ["e2e-agent", "/agentlab/in/task.json"]
    _write_yaml(experiment_path, experiment)

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "command_path_leak_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(build, "leaks runner topology")


@pytest.mark.e2e_build_preflight
def test_build_rejects_runtime_env_that_mentions_runner_owned_agentlab_path(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_ENV_PATH_LEAK", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_rejects_runtime_env_that_mentions_runner_owned_agentlab_path",
        rows=rows,
        agent_env={"TASK_PATH": "/agentlab/in/task.json"},
    )

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "env_path_leak_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(build, "leaks runner topology")


@pytest.mark.e2e_build_preflight
def test_build_rejects_removed_runtime_template_syntax_in_public_authoring(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_REMOVED_TEMPLATE_SYNTAX", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_rejects_removed_runtime_template_syntax_in_public_authoring",
        rows=rows,
    )
    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["runtime"]["agent_runtime"]["command"] = ["e2e-agent", "${WORKSPACE}"]
    _write_yaml(experiment_path, experiment)

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "removed_runtime_template_syntax_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(build, "uses removed '${...}' syntax")


@pytest.mark.e2e_build_preflight
def test_build_rejects_removed_runtime_agent_internal_fields(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_REMOVED_RUNTIME_FIELDS", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_rejects_removed_runtime_agent_internal_fields",
        rows=rows,
    )
    experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
    experiment["runtime"]["agent_runtime"]["env_from_host"] = ["OPENAI_API_KEY"]
    _write_yaml(experiment_path, experiment)

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "removed_runtime_agent_internal_fields_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(build, "env_from_host was removed")


@pytest.mark.e2e_build_preflight
def test_build_rejects_task_row_that_mentions_runner_owned_task_workspace(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_TASK_WORKSPACE_LEAK", task_image=fixture_image),
    ]
    rows[0]["task"]["workspace"] = "/agentlab/workspace"
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_rejects_task_row_that_mentions_runner_owned_task_workspace",
        rows=rows,
    )

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "task_workspace_leak_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(build, "task.workspace")


@pytest.mark.e2e_build_preflight
def test_build_rejects_aux_mount_outside_runner_owned_workspace_root(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project = _make_project(tmp_path, artifact_bundle)
    mount_ref = _materialize_dataset_pack(project, {"reference.txt": "mounted reference\n"})
    rows = [
        _task_row(
            task_id="TASK_AUX_MOUNT_ESCAPE",
            task_image=fixture_image,
            workspace_aux_mounts=[_aux_mount(mount_ref, "/agentlab/state/escape")],
        ),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="build_rejects_aux_mount_outside_runner_owned_workspace_root",
        rows=rows,
        project=project,
    )

    build = _run_lab(
        lab_cli_bin,
        "build",
        experiment_path,
        "--out",
        project.builds_dir / "aux_mount_escape_pkg",
        "--json",
        cwd=project.root,
        expected_exit=1,
    )
    _assert_command_failed(build, "mount_path must be under")


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


@pytest.mark.e2e_build_preflight
def test_preflight_rejects_dangerous_agent_command(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_DANGEROUS_REJECT", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="preflight_rejects_dangerous_agent_command",
        rows=rows,
        agent_command=["e2e-agent", "--dangerous"],
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "preflight_rejects_dangerous_agent_command_pkg",
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
        "dangerous_mode_forbidden",
        "--dangerous",
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


@pytest.mark.e2e_runtime
def test_run_accepts_strict_task_declarations_and_writes_prepared_environment_manifest(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_STRICT_DECLARATION", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="run_accepts_strict_task_declarations_and_writes_prepared_environment_manifest",
        rows=rows,
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "strict_task_declaration_pkg",
    )
    task_rows = _read_jsonl(package_dir / "tasks" / "tasks.jsonl")
    assert len(task_rows) == 1
    _assert_packaged_task_declaration(task_rows[0], rows[0])
    assert "image" not in task_rows[0]["task"]
    assert "workspace" not in task_rows[0]["task"]

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
    prepared_manifest_path = trial_dir / "runtime" / "prepared_task_environment.json"
    assert prepared_manifest_path.exists(), prepared_manifest_path
    prepared_manifest = _read_json(prepared_manifest_path)
    assert prepared_manifest["schema_version"] == "prepared_task_environment_v1"


@pytest.mark.e2e_runtime
def test_scientific_run_executes_agent_in_container_not_host(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_HERMETIC_AGENT_RUNTIME", task_image=fixture_image),
    ]
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="scientific_run_executes_agent_in_container_not_host",
        rows=rows,
    )

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "scientific_run_executes_agent_in_container_not_host_pkg",
    )
    run_payload = _run_package(lab_cli_bin, project, package_dir)
    run_dir = Path(run_payload["run"]["run_dir"])
    trial_dir = _only_trial_dir(run_dir)
    agent_report = _load_agent_report(trial_dir)

    assert agent_report["env"]["workspace"] == "/agentlab/workspace"
    assert agent_report["cwd"] == "/agentlab/workspace"
    assert ".scratch" not in json.dumps(agent_report, sort_keys=True)

    harness_stdout = (trial_dir / "harness_stdout.log").read_text(encoding="utf-8")
    harness_stderr = (trial_dir / "harness_stderr.log").read_text(encoding="utf-8")
    assert ".scratch" not in harness_stdout
    assert ".scratch" not in harness_stderr

    _assert_trial_hermetic(run_dir, trial_dir)


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
    project, experiment_path = _create_simple_project(
        tmp_path,
        artifact_bundle,
        fixture_image,
        exp_id="materialize_full_persists_workspace_base_overlays_and_aux_mounts",
        rows=rows,
        project=project,
    )

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
def test_custom_benchmark_build_preflight_run_writes_prediction_and_score(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    project, experiment_path, rows = _prepare_custom_benchmark_experiment(
        tmp_path,
        artifact_bundle,
        fixture_image,
    )
    assert rows[0]["environment"]["image"]

    package_dir = _build_package(
        lab_cli_bin,
        project,
        experiment_path,
        "custom_benchmark_pkg",
    )
    resolved_experiment = _read_json(package_dir / "resolved_experiment.json")
    assert resolved_experiment["dataset"]["suite_id"] == "custom_benchmark_e2e"
    grader_command_path = resolved_experiment["benchmark"]["grader"]["command"][1]
    assert grader_command_path.endswith("custom_benchmark_grader.py")
    assert str(project.root) not in grader_command_path
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
        trial_row = conn.execute("SELECT row_json FROM trial_rows LIMIT 1").fetchone()
    finally:
        conn.close()
    assert prediction_row is not None
    assert score_row is not None
    assert trial_row is not None
    prediction = json.loads(prediction_row[0])
    score = json.loads(score_row[0])
    trial = json.loads(trial_row[0])
    assert prediction["schema_version"] == "benchmark_prediction_record_v1"
    assert prediction["prediction"]["kind"] == "text"
    assert score["schema_version"] == "benchmark_score_record_v1"
    assert score["verdict"] == "pass"
    assert score["primary_metric_value"] == 1.0
    assert trial["outcome"] == "success"
    assert trial["primary_metric_name"] == "resolved"
    assert trial["primary_metric_value"] == 1.0


@pytest.mark.e2e_benchmark
def test_custom_benchmark_run_marks_missing_result_before_grader_executes(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    rows = [
        _task_row(task_id="TASK_CUSTOM_BENCHMARK_MISSING_RESULT", task_image=fixture_image),
    ]
    project, experiment_path, _ = _create_custom_benchmark_project(
        tmp_path,
        artifact_bundle,
        image_tag=fixture_image,
        exp_id="custom_benchmark_run_marks_missing_result_before_grader_executes",
        baseline_bindings={
            "variant_label": "control",
            "runtime_only_skip_result_write": True,
        },
        rows=rows,
    )

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
        prediction_row = conn.execute(
            "SELECT row_json FROM benchmark_prediction_rows LIMIT 1"
        ).fetchone()
        score_row = conn.execute(
            "SELECT row_json FROM benchmark_score_rows LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert raw_row is not None
    assert prediction_row is None
    assert score_row is None
    row = json.loads(raw_row[0])
    assert row["outcome"] == "error"
    assert row["success"] is False
    assert row["metrics"]["grade_error"] is True
    assert row["metrics"]["grade_error_reason"] == "result_missing"

    harness_stderr = (trial_dir / "harness_stderr.log").read_text(encoding="utf-8")
    assert "No such file or directory: '/agentlab/out/result.json'" not in harness_stderr


@pytest.mark.e2e_benchmark
def test_builtin_bench_v0_build_preflight_run_uses_real_builtin_benchmark_assets(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    experiment_path = _write_builtin_benchmark_authoring(
        benchmark_name="bench_v0",
        artifact_bundle=artifact_bundle,
        agent_image=fixture_image,
        exp_id="builtin_bench_v0_build_preflight_run_e2e",
    )
    package_dir = tmp_path / "builtin_bench_v0_pkg"
    try:
        build = _run_lab(
            lab_cli_bin,
            "build",
            experiment_path,
            "--out",
            package_dir,
            "--json",
            cwd=REPO_ROOT,
        )
        assert build["ok"] is True
        resolved_experiment = _read_json(package_dir / "resolved_experiment.json")
        assert resolved_experiment["dataset"]["suite_id"] == "bench_v0"
        assert (
            resolved_experiment["benchmark"]["grader"]["command"][1]
            == "/agentlab/deps/bench/integration/agentlab/bench_benchmark_adapter.py"
        )

        preflight = _run_lab(
            lab_cli_bin,
            "preflight",
            package_dir,
            "--json",
            cwd=REPO_ROOT,
        )
        assert preflight["ok"] is True, json.dumps(preflight, indent=2, sort_keys=True)

        run_payload = _run_lab(
            lab_cli_bin,
            "run",
            package_dir,
            "--materialize",
            "full",
            "--json",
            cwd=REPO_ROOT,
        )
        assert run_payload["ok"] is True
        run_dir = Path(run_payload["run"]["run_dir"])

        conn = sqlite3.connect(run_dir / "run.sqlite")
        try:
            prediction_row = conn.execute(
                "SELECT row_json FROM benchmark_prediction_rows LIMIT 1"
            ).fetchone()
            score_row = conn.execute(
                "SELECT row_json FROM benchmark_score_rows LIMIT 1"
            ).fetchone()
            trial_row = conn.execute("SELECT row_json FROM trial_rows LIMIT 1").fetchone()
        finally:
            conn.close()
        assert prediction_row is not None
        assert score_row is not None
        assert trial_row is not None
        prediction = json.loads(prediction_row[0])
        score = json.loads(score_row[0])
        trial = json.loads(trial_row[0])
        assert prediction["benchmark"]["adapter_id"] == "bench_v0"
        assert score["benchmark"]["name"] == "bench"
        assert score["verdict"] == "missing"
        assert score.get("error") is None
        assert trial["outcome"] == "missing"
        assert trial["success"] is False
        assert trial["primary_metric_name"] == "resolved"
        assert trial["primary_metric_value"] == 0.0
    finally:
        experiment_path.unlink(missing_ok=True)


@pytest.mark.e2e_benchmark
def test_builtin_swebench_lite_build_preflight_run_uses_real_builtin_benchmark_assets(
    tmp_path: Path,
    lab_cli_bin: Path,
    artifact_bundle: Path,
    fixture_image: str,
) -> None:
    experiment_path = _write_builtin_benchmark_authoring(
        benchmark_name="swebench_lite_curated",
        artifact_bundle=artifact_bundle,
        agent_image=fixture_image,
        exp_id="builtin_swebench_lite_build_preflight_run_e2e",
    )
    package_dir = tmp_path / "builtin_swebench_lite_pkg"
    try:
        build = _run_lab(
            lab_cli_bin,
            "build",
            experiment_path,
            "--out",
            package_dir,
            "--json",
            cwd=REPO_ROOT,
        )
        assert build["ok"] is True
        resolved_experiment = _read_json(package_dir / "resolved_experiment.json")
        assert resolved_experiment["dataset"]["suite_id"] == "swebench_lite_curated"
        assert (
            resolved_experiment["benchmark"]["grader"]["command"][1]
            == "/agentlab/deps/swebench/swebench_task_container_grader.py"
        )

        preflight = _run_lab(
            lab_cli_bin,
            "preflight",
            package_dir,
            "--json",
            cwd=REPO_ROOT,
        )
        assert preflight["ok"] is True, json.dumps(preflight, indent=2, sort_keys=True)

        run_payload = _run_lab(
            lab_cli_bin,
            "run",
            package_dir,
            "--materialize",
            "full",
            "--json",
            cwd=REPO_ROOT,
        )
        assert run_payload["ok"] is True
        run_dir = Path(run_payload["run"]["run_dir"])

        conn = sqlite3.connect(run_dir / "run.sqlite")
        try:
            prediction_row = conn.execute(
                "SELECT row_json FROM benchmark_prediction_rows LIMIT 1"
            ).fetchone()
            score_row = conn.execute(
                "SELECT row_json FROM benchmark_score_rows LIMIT 1"
            ).fetchone()
            trial_row = conn.execute("SELECT row_json FROM trial_rows LIMIT 1").fetchone()
        finally:
            conn.close()
        assert prediction_row is not None
        assert score_row is not None
        assert trial_row is not None
        prediction = json.loads(prediction_row[0])
        score = json.loads(score_row[0])
        trial = json.loads(trial_row[0])
        assert prediction["benchmark"]["adapter_id"] == "swebench_task_container_grader"
        assert score["benchmark"]["name"] == "swebench_lite_curated"
        assert score["verdict"] == "pass"
        assert trial["outcome"] == "success"
        assert trial["primary_metric_name"] == "resolved"
        assert trial["primary_metric_value"] == 1.0
    finally:
        experiment_path.unlink(missing_ok=True)


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
        agent_env={"OPENAI_API_KEY": "$OPENAI_API_KEY"},
    )
    env_file = project.root / "runtime.env"
    env_file.write_text("OPENAI_API_KEY=test-token\n", encoding="utf-8")

    run_payload = _run_lab(
        lab_cli_bin,
        "build-run",
        experiment_path,
        "--out",
        project.builds_dir / "build_run_pkg",
        "--env-file",
        env_file,
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
