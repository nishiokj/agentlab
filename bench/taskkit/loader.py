"""Task bundle loader.

Materializes agent and grader workspaces from a task bundle directory
and repo snapshot.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml

from bench.config import BenchConfig
from bench.taskkit.schema import load_task_yaml, validate_json, load_schema


def load_task(task_dir: Path, config: BenchConfig) -> dict[str, Any]:
    """Load and validate a task from its directory."""
    task_data = load_task_yaml(task_dir)
    schema = load_schema(config.schemas_dir / "task.schema.json")
    errors = validate_json(task_data, schema)
    if errors:
        raise ValueError(f"Task validation failed for {task_dir}:\n" + "\n".join(errors))
    return task_data


def unpack_repo_snapshot(
    repo_id: str,
    config: BenchConfig,
    target_dir: Path,
) -> None:
    """Unpack a repo snapshot archive into the target directory."""
    archive = config.repos_dir / repo_id / "src.tar.zst"
    if not archive.exists():
        raise FileNotFoundError(f"Repo snapshot not found: {archive}")

    target_dir.mkdir(parents=True, exist_ok=True)

    # Try zstd decompression + tar extraction
    try:
        subprocess.run(
            ["tar", "--zstd", "-xf", str(archive), "-C", str(target_dir)],
            check=True,
            capture_output=True,
            timeout=120,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fallback: try with zstd pipe
        try:
            zstd = subprocess.Popen(
                ["zstd", "-d", "-c", str(archive)],
                stdout=subprocess.PIPE,
            )
            subprocess.run(
                ["tar", "-xf", "-", "-C", str(target_dir)],
                stdin=zstd.stdout,
                check=True,
                timeout=120,
            )
            zstd.wait()
        except FileNotFoundError:
            raise RuntimeError(f"Cannot decompress {archive}: install zstd or tar with zstd support")


def apply_injection_patch(
    workspace: Path,
    task_dir: Path,
    injection_patch_path: str,
) -> None:
    """Apply the baseline injection patch to the workspace."""
    patch_file = task_dir / injection_patch_path
    if not patch_file.exists():
        raise FileNotFoundError(f"Injection patch not found: {patch_file}")

    patch_text = patch_file.read_text()
    result = subprocess.run(
        ["git", "apply", "--verbose", "-"],
        input=patch_text,
        capture_output=True,
        text=True,
        cwd=str(workspace),
        timeout=30,
    )
    if result.returncode != 0:
        # Try plain patch
        result2 = subprocess.run(
            ["patch", "-p1", "--batch", "--forward"],
            input=patch_text,
            capture_output=True,
            text=True,
            cwd=str(workspace),
            timeout=30,
        )
        if result2.returncode != 0:
            raise RuntimeError(
                f"Failed to apply injection patch:\ngit: {result.stderr}\npatch: {result2.stderr}"
            )


def prepare_agent_workspace(
    task_dir: Path,
    task_data: dict[str, Any],
    config: BenchConfig,
    work_root: Path,
) -> dict[str, Any]:
    """Prepare the agent workspace and return a manifest.

    The agent workspace contains:
    - Unpacked repo snapshot with injection patch applied
    - Public artifacts (repro.md, run_public.sh, public_cases.jsonl)

    Hidden and private files are NEVER included.
    """
    workspace = work_root / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    # 1. Unpack repo
    unpack_repo_snapshot(task_data["repo_id"], config, workspace)

    # 2. Init git for patch application
    subprocess.run(["git", "init"], cwd=str(workspace), capture_output=True, timeout=10)
    subprocess.run(["git", "add", "."], cwd=str(workspace), capture_output=True, timeout=30)
    subprocess.run(
        ["git", "commit", "-m", "baseline", "--allow-empty"],
        cwd=str(workspace), capture_output=True, timeout=30,
        env={"GIT_AUTHOR_NAME": "bench", "GIT_AUTHOR_EMAIL": "bench@bench",
             "GIT_COMMITTER_NAME": "bench", "GIT_COMMITTER_EMAIL": "bench@bench",
             "HOME": str(work_root), "PATH": "/usr/bin:/bin:/usr/local/bin"},
    )

    # 3. Apply injection patch
    apply_injection_patch(workspace, task_dir, task_data["baseline_injection_patch"])

    # 4. Copy public artifacts
    public_src = task_dir / "public"
    public_dst = workspace / ".bench_public"
    if public_src.is_dir():
        shutil.copytree(public_src, public_dst, dirs_exist_ok=True)

    # 5. Copy issue description
    issue_md = task_dir / "issue.md"
    if issue_md.exists():
        shutil.copy2(issue_md, workspace / "ISSUE.md")

    manifest = {
        "task_id": task_data["task_id"],
        "repo_id": task_data["repo_id"],
        "workspace": str(workspace),
        "public_dir": str(public_dst) if public_dst.exists() else None,
        "issue_md": str(workspace / "ISSUE.md") if issue_md.exists() else None,
        "includes_hidden": False,
        "includes_private": False,
    }
    manifest_path = work_root / "agent_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest


def prepare_grader_workspace(
    task_dir: Path,
    task_data: dict[str, Any],
    config: BenchConfig,
    work_root: Path,
    agent_patch_path: Path | None = None,
) -> dict[str, Any]:
    """Prepare a fresh grader workspace.

    The grader workspace contains:
    - Fresh repo snapshot with injection patch applied
    - Agent's patch (applied after policy check)
    - Hidden test artifacts
    - NO private solution

    Returns grader manifest.
    """
    workspace = work_root / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    # 1. Fresh unpack
    unpack_repo_snapshot(task_data["repo_id"], config, workspace)

    # 2. Init git
    subprocess.run(["git", "init"], cwd=str(workspace), capture_output=True, timeout=10)
    subprocess.run(["git", "add", "."], cwd=str(workspace), capture_output=True, timeout=30)
    subprocess.run(
        ["git", "commit", "-m", "baseline", "--allow-empty"],
        cwd=str(workspace), capture_output=True, timeout=30,
        env={"GIT_AUTHOR_NAME": "bench", "GIT_AUTHOR_EMAIL": "bench@bench",
             "GIT_COMMITTER_NAME": "bench", "GIT_COMMITTER_EMAIL": "bench@bench",
             "HOME": str(work_root), "PATH": "/usr/bin:/bin:/usr/local/bin"},
    )

    # 3. Apply injection patch
    apply_injection_patch(workspace, task_dir, task_data["baseline_injection_patch"])

    # 4. Copy hidden artifacts
    hidden_src = task_dir / "hidden"
    hidden_dst = work_root / "hidden"
    if hidden_src.is_dir():
        shutil.copytree(hidden_src, hidden_dst, dirs_exist_ok=True)

    # 5. Copy public artifacts for public_command execution during grading.
    public_src = task_dir / "public"
    public_dst = workspace / ".bench_public"
    if public_src.is_dir():
        shutil.copytree(public_src, public_dst, dirs_exist_ok=True)

    manifest = {
        "task_id": task_data["task_id"],
        "repo_id": task_data["repo_id"],
        "workspace": str(workspace),
        "hidden_dir": str(hidden_dst) if hidden_dst.exists() else None,
        "public_dir": str(public_dst) if public_dst.exists() else None,
        "agent_patch": str(agent_patch_path) if agent_patch_path else None,
        "includes_hidden": True,
        "includes_private": False,
    }
    manifest_path = work_root / "grader_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest
