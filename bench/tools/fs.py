"""Filesystem tools: search, list_dir, read_file.

All operations enforce workspace root containment.
"""

from __future__ import annotations

import fnmatch
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bench.tools.protocol import (
    DirEntry,
    ListDirResponse,
    ReadFileResponse,
    SearchMatch,
    SearchResponse,
    truncate_output,
)


def _resolve_and_check(workspace: Path, relpath: str) -> Path:
    """Resolve a path and ensure it's within the workspace."""
    resolved = (workspace / relpath).resolve()
    if not str(resolved).startswith(str(workspace.resolve())):
        raise PermissionError(f"Path escapes workspace: {relpath}")
    return resolved


def search(
    workspace: Path,
    pattern: str,
    glob_filter: str = "**/*",
    max_results: int = 50,
    case_sensitive: bool = True,
) -> SearchResponse:
    """Search for a pattern in files within workspace using ripgrep if available, else fallback."""
    matches: list[SearchMatch] = []
    rg_args = ["rg", "--no-heading", "--line-number", "--color=never"]
    if not case_sensitive:
        rg_args.append("-i")
    if glob_filter != "**/*":
        rg_args.extend(["--glob", glob_filter])
    rg_args.extend(["--max-count", str(max_results * 2)])
    rg_args.append(pattern)
    rg_args.append(str(workspace))

    try:
        result = subprocess.run(
            rg_args, capture_output=True, text=True, timeout=30, cwd=str(workspace)
        )
        for line in result.stdout.splitlines()[:max_results * 2]:
            # Format: path:line_number:content
            parts = line.split(":", 2)
            if len(parts) >= 3:
                fpath = parts[0]
                try:
                    lnum = int(parts[1])
                except ValueError:
                    continue
                content = parts[2]
                # Make path relative to workspace
                try:
                    relpath = str(Path(fpath).relative_to(workspace))
                except ValueError:
                    relpath = fpath
                matches.append(SearchMatch(path=relpath, line_number=lnum, line_content=content))
    except (FileNotFoundError, subprocess.TimeoutExpired):
        # Fallback: simple regex search
        regex = re.compile(pattern if case_sensitive else pattern, re.IGNORECASE if not case_sensitive else 0)
        for root, _, files in os.walk(workspace):
            for fname in sorted(files):
                if len(matches) >= max_results:
                    break
                fpath = Path(root) / fname
                if not fnmatch.fnmatch(str(fpath.relative_to(workspace)), glob_filter.replace("**/*", "*")):
                    continue
                try:
                    text = fpath.read_text(errors="replace")
                    for i, line in enumerate(text.splitlines(), 1):
                        if regex.search(line):
                            matches.append(SearchMatch(
                                path=str(fpath.relative_to(workspace)),
                                line_number=i,
                                line_content=line[:500],
                            ))
                            if len(matches) >= max_results:
                                break
                except (PermissionError, OSError):
                    continue

    # Deterministic sort: by path then line number
    matches.sort(key=lambda m: (m.path, m.line_number))
    truncated = len(matches) > max_results
    matches = matches[:max_results]
    return SearchResponse(matches=matches, truncated=truncated, total_matches=len(matches))


def list_dir(
    workspace: Path,
    path: str = ".",
    recursive: bool = False,
    max_entries: int = 500,
) -> ListDirResponse:
    """List directory entries within workspace."""
    target = _resolve_and_check(workspace, path)
    if not target.is_dir():
        raise FileNotFoundError(f"Not a directory: {path}")

    entries: list[DirEntry] = []
    if recursive:
        for root, dirs, files in os.walk(target):
            dirs.sort()
            for name in sorted(dirs) + sorted(files):
                if len(entries) >= max_entries:
                    break
                fpath = Path(root) / name
                stat = fpath.stat()
                entries.append(DirEntry(
                    name=str(fpath.relative_to(target)),
                    is_dir=fpath.is_dir(),
                    size=stat.st_size if not fpath.is_dir() else 0,
                    mtime=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                ))
            if len(entries) >= max_entries:
                break
    else:
        for entry in sorted(target.iterdir(), key=lambda p: p.name):
            if len(entries) >= max_entries:
                break
            stat = entry.stat()
            entries.append(DirEntry(
                name=entry.name,
                is_dir=entry.is_dir(),
                size=stat.st_size if not entry.is_dir() else 0,
                mtime=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            ))

    return ListDirResponse(entries=entries, truncated=len(entries) >= max_entries)


def read_file(
    workspace: Path,
    path: str,
    offset: int = 0,
    max_bytes: int = 64 * 1024,
) -> ReadFileResponse:
    """Read a file with byte-range support."""
    target = _resolve_and_check(workspace, path)
    if not target.is_file():
        raise FileNotFoundError(f"Not a file: {path}")

    total_bytes = target.stat().st_size
    with open(target, "rb") as f:
        f.seek(offset)
        raw = f.read(max_bytes + 1)

    truncated = len(raw) > max_bytes
    if truncated:
        raw = raw[:max_bytes]

    content = raw.decode("utf-8", errors="replace")
    if truncated:
        content += "\n... [TRUNCATED at {max_bytes} bytes]"

    return ReadFileResponse(
        content=content,
        truncated=truncated,
        total_bytes=total_bytes,
        encoding="utf-8",
    )
