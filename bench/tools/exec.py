"""Command execution tool with timeout and output truncation."""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

from bench.tools.protocol import RunResponse, truncate_output

MAX_OUTPUT_BYTES = 64 * 1024  # 64KB per stream


def run_command(
    workspace: Path,
    command: list[str],
    timeout: int = 30,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    allowed_commands: list[str] | None = None,
) -> RunResponse:
    """Run a command in the workspace with timeout enforcement.

    Args:
        workspace: Workspace root directory.
        command: Command and arguments.
        timeout: Wall-clock timeout in seconds.
        cwd: Working directory relative to workspace (default: workspace root).
        env: Additional environment variables.
        allowed_commands: If set, only these command basenames are allowed.

    Returns:
        RunResponse with exit_code, stdout, stderr, timing.
    """
    if not command:
        return RunResponse(exit_code=-1, stderr="Empty command", duration_ms=0)

    # Command allowlist check
    cmd_name = Path(command[0]).name
    if allowed_commands is not None and cmd_name not in allowed_commands:
        return RunResponse(
            exit_code=-1,
            stderr=f"Command not allowed: {cmd_name}. Allowed: {', '.join(allowed_commands)}",
            duration_ms=0,
        )

    # Resolve working directory
    work_dir = workspace
    if cwd:
        work_dir = (workspace / cwd).resolve()
        if not str(work_dir).startswith(str(workspace.resolve())):
            return RunResponse(exit_code=-1, stderr="cwd escapes workspace", duration_ms=0)

    import os
    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    start = time.monotonic()
    timed_out = False
    try:
        proc = subprocess.run(
            command,
            capture_output=True,
            timeout=timeout,
            cwd=str(work_dir),
            env=run_env,
        )
        exit_code = proc.returncode
        stdout_raw = proc.stdout[:MAX_OUTPUT_BYTES].decode("utf-8", errors="replace")
        stderr_raw = proc.stderr[:MAX_OUTPUT_BYTES].decode("utf-8", errors="replace")
    except subprocess.TimeoutExpired as e:
        timed_out = True
        exit_code = -1
        stdout_raw = (e.stdout or b"")[:MAX_OUTPUT_BYTES].decode("utf-8", errors="replace")
        stderr_raw = (e.stderr or b"")[:MAX_OUTPUT_BYTES].decode("utf-8", errors="replace")
    except FileNotFoundError:
        elapsed = (time.monotonic() - start) * 1000
        return RunResponse(exit_code=-1, stderr=f"Command not found: {command[0]}", duration_ms=elapsed)

    elapsed = (time.monotonic() - start) * 1000

    stdout_out, stdout_trunc = truncate_output(stdout_raw)
    stderr_out, stderr_trunc = truncate_output(stderr_raw)

    return RunResponse(
        exit_code=exit_code,
        stdout=stdout_out,
        stderr=stderr_out,
        truncated=stdout_trunc or stderr_trunc,
        duration_ms=round(elapsed, 2),
        timed_out=timed_out,
    )
