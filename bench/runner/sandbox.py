"""Docker sandbox management.

Handles building and running agent/grader containers with
network disabled and deterministic environment.
"""

from __future__ import annotations

import subprocess
import logging
from pathlib import Path
from typing import Any

from bench.config import BenchConfig

logger = logging.getLogger("bench.runner.sandbox")

IMAGE_PREFIX = "bench"
BASE_IMAGE = f"{IMAGE_PREFIX}-base:dev"
AGENT_IMAGE = f"{IMAGE_PREFIX}-agent:dev"
GRADER_IMAGE = f"{IMAGE_PREFIX}-grader:dev"


def build_images(config: BenchConfig, no_cache: bool = False) -> None:
    """Build all Docker images."""
    docker_dir = config.bench_dir / "docker"

    for name, dockerfile in [
        ("base", "base.Dockerfile"),
        ("agent", "agent.Dockerfile"),
        ("grader", "grader.Dockerfile"),
    ]:
        tag = f"{IMAGE_PREFIX}-{name}:dev"
        cmd = [
            "docker", "build",
            "-f", str(docker_dir / dockerfile),
            "-t", tag,
        ]
        if no_cache:
            cmd.append("--no-cache")
        cmd.append(str(config.root))

        logger.info(f"Building {tag}...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to build {tag}:\n{result.stderr}")
        logger.info(f"Built {tag}")


def run_in_container(
    image: str,
    workspace_mount: Path,
    command: list[str],
    env: dict[str, str] | None = None,
    timeout: int = 1200,
    network: str = "none",
    extra_mounts: dict[str, str] | None = None,
    work_dir: str = "/workspace",
) -> subprocess.CompletedProcess:
    """Run a command in a Docker container.

    Args:
        image: Docker image tag.
        workspace_mount: Host path to mount as /workspace.
        command: Command to run.
        env: Environment variables.
        timeout: Wall-clock timeout.
        network: Docker network mode (default: none for offline).
        extra_mounts: Additional host_path -> container_path mounts.
        work_dir: Working directory inside container.
    """
    cmd = [
        "docker", "run",
        "--rm",
        f"--network={network}",
        "-v", f"{workspace_mount}:{work_dir}",
        "-w", work_dir,
    ]

    # Determinism env
    default_env = {
        "PYTHONHASHSEED": "0",
        "TZ": "UTC",
        "LC_ALL": "C.UTF-8",
        "LANG": "C.UTF-8",
        "SOURCE_DATE_EPOCH": "1700000000",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "HOME": "/tmp/benchhome",
    }
    if env:
        default_env.update(env)

    for k, v in default_env.items():
        cmd.extend(["-e", f"{k}={v}"])

    if extra_mounts:
        for host_path, container_path in extra_mounts.items():
            cmd.extend(["-v", f"{host_path}:{container_path}:ro"])

    cmd.append(image)
    cmd.extend(command)

    try:
        return subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        return subprocess.CompletedProcess(
            cmd, returncode=-1,
            stdout=(e.stdout or b"").decode("utf-8", errors="replace"),
            stderr=f"Container timed out after {timeout}s",
        )
