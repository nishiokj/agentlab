#!/usr/bin/env python3
"""Export Harbor task definitions into AgentLab task_boundary_v2 JSONL."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

try:  # Python 3.11+
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]

TASK_BOUNDARY_SCHEMA = "task_boundary_v2"
DEFAULT_BENCHMARK_NAME = "terminal_bench_2"
DEFAULT_ADAPTER_ID = "harbor_tb2"
DEFAULT_SPLIT = "test"


@dataclass(frozen=True)
class ExportConfig:
    benchmark_name: str = DEFAULT_BENCHMARK_NAME
    adapter_id: str = DEFAULT_ADAPTER_ID
    split: str = DEFAULT_SPLIT
    id_prefix: str = ""
    include_raw_toml: bool = False
    require_task_image: bool = False
    default_task_image: str | None = None
    default_task_workspace: str | None = None


def _candidate_string(value: Any) -> str | None:
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed:
            return trimmed
    return None


def _parse_scalar(raw: str) -> Any:
    token = raw.strip()
    if len(token) >= 2 and token[0] == token[-1] == '"':
        return token[1:-1]
    if len(token) >= 2 and token[0] == token[-1] == "'":
        return token[1:-1]
    if token.lower() in {"true", "false"}:
        return token.lower() == "true"
    if re.fullmatch(r"-?[0-9]+", token):
        return int(token)
    return token


def _load_toml(task_toml: Path) -> dict[str, Any]:
    if tomllib is not None:
        with task_toml.open("rb") as handle:
            doc = tomllib.load(handle)
        if not isinstance(doc, dict):
            raise ValueError(f"task.toml is not an object: {task_toml}")
        return doc

    # Minimal fallback TOML parser for key/value + [section] forms.
    root: dict[str, Any] = {}
    section: dict[str, Any] = root
    for raw_line in task_toml.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            name = line[1:-1].strip()
            if not name:
                continue
            current = root
            for part in name.split("."):
                if part not in current or not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]
            section = current
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        section[key] = _parse_scalar(value)
    return root


def _lookup_path(obj: Any, path: tuple[str, ...]) -> Any:
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _first_string(doc: dict[str, Any], paths: Iterable[tuple[str, ...]]) -> str | None:
    for path in paths:
        found = _candidate_string(_lookup_path(doc, path))
        if found is not None:
            return found
    return None


def _sanitize_task_id(raw: str, fallback: str) -> str:
    chosen = raw.strip() if raw.strip() else fallback
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", chosen).strip("._-")
    return cleaned or fallback


def _read_prompt_sidecar(task_dir: Path) -> str | None:
    candidates = ("prompt.txt", "prompt.md", "task.txt", "task.md", "README.md")
    for name in candidates:
        candidate = task_dir / name
        if not candidate.is_file():
            continue
        text = candidate.read_text(encoding="utf-8", errors="replace").strip()
        if text:
            return text
    return None


def _extract_task_id(doc: dict[str, Any], task_dir: Path, config: ExportConfig) -> str:
    task_id = _first_string(
        doc,
        (
            ("task", "id"),
            ("id",),
            ("task", "name"),
            ("name",),
        ),
    )
    if task_id is None:
        task_id = task_dir.name
    task_id = _sanitize_task_id(task_id, task_dir.name)
    if config.id_prefix:
        task_id = f"{config.id_prefix}{task_id}"
    return task_id


def _extract_prompt(doc: dict[str, Any], task_dir: Path) -> str | None:
    prompt = _first_string(
        doc,
        (
            ("task", "prompt"),
            ("task", "instruction"),
            ("prompt",),
            ("instruction",),
            ("description",),
            ("task", "description"),
        ),
    )
    return prompt if prompt is not None else _read_prompt_sidecar(task_dir)


def _extract_image(doc: dict[str, Any]) -> str | None:
    return _first_string(
        doc,
        (
            ("task", "image"),
            ("environment", "image"),
            ("container", "image"),
            ("image",),
        ),
    )


def _extract_workspace(doc: dict[str, Any]) -> str | None:
    return _first_string(
        doc,
        (
            ("task", "workspace"),
            ("environment", "workspace"),
            ("workspace",),
        ),
    )


def _extract_limits(doc: dict[str, Any]) -> dict[str, int]:
    raw = _lookup_path(doc, ("limits",))
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    max_tool_calls = raw.get("max_tool_calls")
    max_runtime_ms = raw.get("max_runtime_ms")
    max_output_bytes = raw.get("max_output_bytes")
    max_runtime_seconds = raw.get("max_runtime_seconds")
    if isinstance(max_tool_calls, int) and max_tool_calls > 0:
        out["max_tool_calls"] = max_tool_calls
    if isinstance(max_runtime_ms, int) and max_runtime_ms > 0:
        out["max_runtime_ms"] = max_runtime_ms
    elif isinstance(max_runtime_seconds, int) and max_runtime_seconds > 0:
        out["max_runtime_ms"] = max_runtime_seconds * 1000
    if isinstance(max_output_bytes, int) and max_output_bytes > 0:
        out["max_output_bytes"] = max_output_bytes
    return out


def parse_task_dir(task_dir: Path, config: ExportConfig) -> dict[str, Any]:
    task_toml = task_dir / "task.toml"
    if not task_toml.is_file():
        raise FileNotFoundError(f"missing task.toml: {task_toml}")

    doc = _load_toml(task_toml)

    task_id = _extract_task_id(doc, task_dir, config)
    prompt = _extract_prompt(doc, task_dir)
    task_image = _extract_image(doc)
    task_workspace = _extract_workspace(doc)
    if task_image is None and config.default_task_image is not None:
        task_image = config.default_task_image
    if task_workspace is None and config.default_task_workspace is not None:
        task_workspace = config.default_task_workspace
    if config.require_task_image and task_image is None:
        raise ValueError(
            f"task.image missing for task '{task_id}' in {task_toml}; "
            "required for runtime.agent.image_source='per_task'"
        )
    limits = _extract_limits(doc)

    task_payload: dict[str, Any] = {
        "id": task_id,
        "source": "harbor",
        "benchmark": {
            "adapter_id": config.adapter_id,
            "name": config.benchmark_name,
            "split": config.split,
        },
        "harbor": {
            "task_dir": str(task_dir),
            "task_toml": str(task_toml),
        },
    }
    if prompt is not None:
        task_payload["prompt"] = prompt
    if task_image is not None:
        task_payload["image"] = task_image
    if task_workspace is not None:
        task_payload["workspace"] = task_workspace
    if config.include_raw_toml:
        task_payload["harbor"]["task_toml_raw"] = doc

    return {
        "schema_version": TASK_BOUNDARY_SCHEMA,
        "task": task_payload,
        "workspace_files": [],
        "mount_references": [],
        "limits": limits,
    }


def _read_registry_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            maybe_tasks = payload.get("tasks")
            if isinstance(maybe_tasks, list):
                return [row for row in maybe_tasks if isinstance(row, dict)]
        raise ValueError(f"unsupported registry JSON shape: {path}")

    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        trimmed = line.strip()
        if not trimmed:
            continue
        row = json.loads(trimmed)
        if isinstance(row, dict):
            records.append(row)
    return records


def task_dirs_from_registry(registry_path: Path, registry_root: Path) -> list[Path]:
    records = _read_registry_records(registry_path)
    out: set[Path] = set()
    for row in records:
        raw_path = row.get("path") or row.get("task_path") or row.get("task_dir")
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        candidate = Path(raw_path.strip())
        if not candidate.is_absolute():
            candidate = (registry_root / candidate).resolve()
        if candidate.is_dir():
            out.add(candidate)
    return sorted(out)


def task_dirs_from_roots(roots: Iterable[Path]) -> list[Path]:
    out: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        if root.is_file() and root.name == "task.toml":
            out.add(root.parent.resolve())
            continue
        if root.is_dir() and (root / "task.toml").is_file():
            out.add(root.resolve())
            continue
        if root.is_dir():
            for task_toml in root.rglob("task.toml"):
                out.add(task_toml.parent.resolve())
    return sorted(out)


def export_rows(task_dirs: list[Path], config: ExportConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for task_dir in task_dirs:
        try:
            rows.append(parse_task_dir(task_dir, config))
        except Exception as exc:
            raise RuntimeError(f"failed parsing task dir '{task_dir}': {exc}") from exc
    return rows


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
            count += 1
    return count


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks-root", action="append", default=[], help="Task root to scan recursively for task.toml.")
    parser.add_argument("--task-dir", action="append", default=[], help="Explicit Harbor task directory.")
    parser.add_argument("--registry-json", help="Path to Harbor dataset registry (.json or .jsonl).")
    parser.add_argument("--registry-root", help="Base dir for relative registry task paths.")
    parser.add_argument("--output", required=True, help="Output JSONL path.")
    parser.add_argument("--benchmark-name", default=DEFAULT_BENCHMARK_NAME)
    parser.add_argument("--adapter-id", default=DEFAULT_ADAPTER_ID)
    parser.add_argument("--split", default=DEFAULT_SPLIT)
    parser.add_argument("--id-prefix", default="")
    parser.add_argument("--limit", type=int, default=0, help="Optional cap after mapping.")
    parser.add_argument("--include-raw-toml", action="store_true")
    parser.add_argument(
        "--require-task-image",
        action="store_true",
        help="Fail if any mapped task lacks task.image (for per_task image mode).",
    )
    parser.add_argument(
        "--default-task-image",
        default=None,
        help="Fallback image for tasks that do not define image.",
    )
    parser.add_argument(
        "--default-task-workspace",
        default=None,
        help="Fallback workspace path for tasks that do not define workspace.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    config = ExportConfig(
        benchmark_name=args.benchmark_name,
        adapter_id=args.adapter_id,
        split=args.split,
        id_prefix=args.id_prefix,
        include_raw_toml=args.include_raw_toml,
        require_task_image=bool(args.require_task_image),
        default_task_image=args.default_task_image.strip() if isinstance(args.default_task_image, str) and args.default_task_image.strip() else None,
        default_task_workspace=args.default_task_workspace.strip() if isinstance(args.default_task_workspace, str) and args.default_task_workspace.strip() else None,
    )

    roots = [Path(p).expanduser().resolve() for p in args.tasks_root + args.task_dir]
    task_dirs = task_dirs_from_roots(roots)

    if args.registry_json:
        registry_path = Path(args.registry_json).expanduser().resolve()
        registry_root = (
            Path(args.registry_root).expanduser().resolve()
            if args.registry_root
            else registry_path.parent
        )
        task_dirs.extend(task_dirs_from_registry(registry_path, registry_root))
        task_dirs = sorted(set(task_dirs))

    if not task_dirs:
        print("no Harbor tasks found (provide --tasks-root, --task-dir, or --registry-json)", file=sys.stderr)
        return 2

    rows = export_rows(task_dirs, config)
    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    output_path = Path(args.output).expanduser().resolve()
    count = write_jsonl(output_path, rows)
    print(
        f"wrote {count} row(s) to {output_path} from {len(task_dirs)} Harbor task dir(s)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
