"""HTTP tool server for agents inside the container.

Listens on localhost:PORT and serves the five tools:
    search, list_dir, read_file, apply_patch, run

Usage:
    python -m bench.tools.server --workspace /workspace --port 8080
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from dataclasses import asdict
from pathlib import Path
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from bench.tools import fs, exec as tool_exec, patch as tool_patch
from bench.tools.protocol import ToolName

logger = logging.getLogger("bench.tools.server")

# Module-level state set during startup
_workspace: Path = Path(".")
_policy_check = None
_allowed_commands: list[str] | None = None


def _error_response(msg: str, status: int = 400) -> JSONResponse:
    return JSONResponse({"error": msg}, status_code=status)


async def healthz(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


async def handle_search(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        result = fs.search(
            workspace=_workspace,
            pattern=body.get("pattern", ""),
            glob_filter=body.get("glob_filter", "**/*"),
            max_results=body.get("max_results", 50),
            case_sensitive=body.get("case_sensitive", True),
        )
        return JSONResponse(asdict(result))
    except Exception as e:
        return _error_response(str(e), 500)


async def handle_list_dir(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        result = fs.list_dir(
            workspace=_workspace,
            path=body.get("path", "."),
            recursive=body.get("recursive", False),
            max_entries=body.get("max_entries", 500),
        )
        return JSONResponse(asdict(result))
    except PermissionError as e:
        return _error_response(str(e), 403)
    except FileNotFoundError as e:
        return _error_response(str(e), 404)
    except Exception as e:
        return _error_response(str(e), 500)


async def handle_read_file(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        result = fs.read_file(
            workspace=_workspace,
            path=body.get("path", ""),
            offset=body.get("offset", 0),
            max_bytes=body.get("max_bytes", 64 * 1024),
        )
        return JSONResponse(asdict(result))
    except PermissionError as e:
        return _error_response(str(e), 403)
    except FileNotFoundError as e:
        return _error_response(str(e), 404)
    except Exception as e:
        return _error_response(str(e), 500)


async def handle_apply_patch(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        result = tool_patch.apply_patch(
            workspace=_workspace,
            patch_text=body.get("patch", ""),
            policy_check=_policy_check,
        )
        return JSONResponse(asdict(result))
    except Exception as e:
        return _error_response(str(e), 500)


async def handle_run(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        result = tool_exec.run_command(
            workspace=_workspace,
            command=body.get("command", []),
            timeout=body.get("timeout", 30),
            cwd=body.get("cwd"),
            allowed_commands=_allowed_commands,
        )
        return JSONResponse(asdict(result))
    except Exception as e:
        return _error_response(str(e), 500)


routes = [
    Route("/healthz", healthz, methods=["GET"]),
    Route("/search", handle_search, methods=["POST"]),
    Route("/list_dir", handle_list_dir, methods=["POST"]),
    Route("/read_file", handle_read_file, methods=["POST"]),
    Route("/apply_patch", handle_apply_patch, methods=["POST"]),
    Route("/run", handle_run, methods=["POST"]),
]

app = Starlette(routes=routes)


def create_app(
    workspace: Path,
    policy_check=None,
    allowed_commands: list[str] | None = None,
) -> Starlette:
    """Create a configured tool server app."""
    global _workspace, _policy_check, _allowed_commands
    _workspace = workspace.resolve()
    _policy_check = policy_check
    _allowed_commands = allowed_commands
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark tool server")
    parser.add_argument("--workspace", type=str, required=True, help="Workspace root")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to bind")
    args = parser.parse_args()

    import uvicorn

    workspace = Path(args.workspace).resolve()
    if not workspace.is_dir():
        print(f"Workspace not found: {workspace}", file=sys.stderr)
        sys.exit(1)

    create_app(workspace)
    logger.info(f"Starting tool server on {args.host}:{args.port} workspace={workspace}")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
