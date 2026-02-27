"""Tool protocol definitions for the agent tool server.

Defines request/response shapes aligned with the trace schema.
Transport: HTTP on localhost inside the agent container.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ToolName(str, Enum):
    SEARCH = "search"
    LIST_DIR = "list_dir"
    READ_FILE = "read_file"
    APPLY_PATCH = "apply_patch"
    RUN = "run"


# ---------------------------------------------------------------------------
# Request types
# ---------------------------------------------------------------------------

@dataclass
class SearchRequest:
    pattern: str
    glob_filter: str = "**/*"
    max_results: int = 50
    case_sensitive: bool = True


@dataclass
class ListDirRequest:
    path: str = "."
    recursive: bool = False
    max_entries: int = 500


@dataclass
class ReadFileRequest:
    path: str = ""
    offset: int = 0
    max_bytes: int = 64 * 1024  # 64KB


@dataclass
class ApplyPatchRequest:
    patch: str = ""  # Unified diff content


@dataclass
class RunRequest:
    command: list[str] = field(default_factory=list)
    timeout: int = 30
    cwd: str | None = None


# ---------------------------------------------------------------------------
# Response types
# ---------------------------------------------------------------------------

@dataclass
class SearchMatch:
    path: str
    line_number: int
    line_content: str


@dataclass
class SearchResponse:
    matches: list[SearchMatch] = field(default_factory=list)
    truncated: bool = False
    total_matches: int = 0


@dataclass
class DirEntry:
    name: str
    is_dir: bool
    size: int = 0
    mtime: str = ""


@dataclass
class ListDirResponse:
    entries: list[DirEntry] = field(default_factory=list)
    truncated: bool = False


@dataclass
class ReadFileResponse:
    content: str = ""
    truncated: bool = False
    total_bytes: int = 0
    encoding: str = "utf-8"


@dataclass
class ApplyPatchResponse:
    applied_files: list[str] = field(default_factory=list)
    rejected_files: list[str] = field(default_factory=list)
    policy_violations: list[str] = field(default_factory=list)
    success: bool = False


@dataclass
class RunResponse:
    exit_code: int = -1
    stdout: str = ""
    stderr: str = ""
    truncated: bool = False
    duration_ms: float = 0.0
    timed_out: bool = False


# ---------------------------------------------------------------------------
# Generic tool envelope
# ---------------------------------------------------------------------------

@dataclass
class ToolRequest:
    tool: ToolName
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResponse:
    tool: ToolName
    success: bool = True
    result: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


# ---------------------------------------------------------------------------
# Output truncation / redaction
# ---------------------------------------------------------------------------

MAX_OUTPUT_LEN = 4096  # Max chars in trace output_summary


def truncate_output(text: str, max_len: int = MAX_OUTPUT_LEN) -> tuple[str, bool]:
    """Truncate text for trace recording. Returns (text, was_truncated)."""
    if len(text) <= max_len:
        return text, False
    return text[:max_len - 20] + "\n... [TRUNCATED]", True
