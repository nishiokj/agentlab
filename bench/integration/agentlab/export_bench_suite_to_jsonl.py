#!/usr/bin/env python3
"""Export bench task bundles as strict AgentLab task_boundary_v2 JSONL.

Phase 2 hard-cut:
- task workspace is runner-owned (no task.workspace emission)
- rows must include runner-owned workspace_seed.dataset_pack_ref
- dataset packs are materialized at export/build time (not at run time)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml

DEFAULT_SUITE = "v0"
DEFAULT_SPLIT = "test"
DEFAULT_BENCHMARK_NAME = "bench"
TASK_BOUNDARY_SCHEMA_VERSION = "task_boundary_v2"
PACK_FORMAT_VERSION = "bench_workspace_seed_pack_v1"
DEFAULT_DATASET_PACK_ROOT = ".lab/dataset_packs/sha256"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _task_tags(task_yaml: dict[str, Any]) -> list[str]:
    raw = task_yaml.get("tags")
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        tag = _candidate_string(item)
        if tag is not None:
            out.append(tag)
    return out


def _workspace_overlay_files(task_dir: Path) -> list[dict[str, Any]]:
    overlays: list[dict[str, Any]] = []

    issue = task_dir / "issue.md"
    if issue.exists():
        overlays.append(
            {
                "path": "ISSUE.md",
                "content": issue.read_text(encoding="utf-8", errors="replace"),
                "encoding": "utf8",
                "executable": False,
            }
        )

    public_dir = task_dir / "public"
    if public_dir.is_dir():
        for path in sorted(p for p in public_dir.rglob("*") if p.is_file()):
            rel = path.relative_to(public_dir).as_posix()
            overlays.append(
                {
                    "path": f".bench_public/{rel}",
                    "content": path.read_text(encoding="utf-8", errors="replace"),
                    "encoding": "utf8",
                    "executable": path.suffix == ".sh" or os.access(path, os.X_OK),
                }
            )

    return overlays


def _task_prompt(task_dir: Path, task_yaml: dict[str, Any]) -> str:
    issue = task_dir / "issue.md"
    prompt_parts: list[str] = []

    if issue.exists():
        issue_text = issue.read_text(encoding="utf-8", errors="replace").strip()
        if issue_text:
            prompt_parts.append(issue_text)

    metadata_lines: list[str] = []
    description = _candidate_string(task_yaml.get("description"))
    if description is not None:
        metadata_lines.append(f"- Task description: {description}")
    difficulty = _candidate_string(task_yaml.get("difficulty"))
    if difficulty is not None:
        metadata_lines.append(f"- Difficulty: {difficulty}")
    tags = _task_tags(task_yaml)
    if tags:
        metadata_lines.append(f"- Tags: {', '.join(tags)}")

    if metadata_lines:
        prompt_parts.append("Task metadata:\n" + "\n".join(metadata_lines))

    return "\n\n".join(prompt_parts)


def _load_task_yaml(task_dir: Path) -> dict[str, Any]:
    task_yaml = task_dir / "task.yaml"
    if not task_yaml.exists():
        raise FileNotFoundError(f"missing task.yaml: {task_yaml}")
    payload = yaml.safe_load(task_yaml.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"task.yaml must decode to an object: {task_yaml}")
    return payload


def _candidate_string(value: Any) -> str | None:
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed:
            return trimmed
    return None


def _per_task_image_name(base_image: str, task_id: str) -> str:
    base = base_image.split(":")[0]
    return f"{base}-{task_id.lower()}:latest"


def _resolve_task_image(
    task_yaml: dict[str, Any],
    default_task_image: str | None,
    task_id: str,
    require_task_image: bool,
) -> str | None:
    explicit = _candidate_string(task_yaml.get("image"))
    if explicit:
        return explicit
    if default_task_image:
        return _per_task_image_name(default_task_image, task_id)
    if require_task_image:
        raise ValueError(f"task.image missing for task '{task_id}'")
    return None


def _resolve_repo_snapshot_path(root: Path, task_dir: Path, task_yaml: dict[str, Any]) -> Path:
    raw = _candidate_string(task_yaml.get("repo_snapshot"))
    if raw is None:
        raise ValueError(f"task '{task_dir.name}' missing repo_snapshot")

    raw_path = Path(raw)
    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.append(task_dir / raw_path)
        candidates.append(root / raw_path)
        candidates.append(root / "bench" / "benchmark" / "repos" / raw_path)

    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"repo_snapshot not found for task '{task_dir.name}': {raw} (searched {len(candidates)} locations)"
    )


def _resolve_injection_patch_path(task_dir: Path, task_yaml: dict[str, Any]) -> Path | None:
    raw = _candidate_string(task_yaml.get("baseline_injection_patch"))
    if raw is None:
        return None
    patch_path = Path(raw)
    if not patch_path.is_absolute():
        patch_path = task_dir / patch_path
    if not patch_path.exists():
        raise FileNotFoundError(f"injection patch not found: {patch_path}")
    return patch_path


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _workspace_seed_digest(repo_snapshot: Path, injection_patch: Path | None) -> str:
    hasher = hashlib.sha256()
    hasher.update(PACK_FORMAT_VERSION.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(_sha256_file(repo_snapshot).encode("ascii"))
    hasher.update(b"\0")
    if injection_patch is not None:
        hasher.update(_sha256_file(injection_patch).encode("ascii"))
    return hasher.hexdigest()


def _run_checked(command: list[str], cwd: Path | None = None) -> None:
    proc = subprocess.run(
        command,
        cwd=str(cwd) if cwd is not None else None,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode == 0:
        return
    detail = (proc.stderr or proc.stdout or "command failed").strip()
    raise RuntimeError(f"command failed ({' '.join(command)}): {detail}")


def _extract_repo_snapshot(repo_snapshot: Path, destination: Path) -> None:
    lower = repo_snapshot.name.lower()
    if lower.endswith(".tar.zst") or lower.endswith(".tzst"):
        _run_checked(
            ["tar", "--zstd", "-xf", str(repo_snapshot), "-C", str(destination)]
        )
        return
    if lower.endswith(".tar.gz") or lower.endswith(".tgz"):
        _run_checked(["tar", "-xzf", str(repo_snapshot), "-C", str(destination)])
        return
    if lower.endswith(".tar"):
        _run_checked(["tar", "-xf", str(repo_snapshot), "-C", str(destination)])
        return
    raise ValueError(
        f"unsupported repo_snapshot archive format for {repo_snapshot} (expected .tar/.tar.gz/.tar.zst)"
    )


def _apply_injection_patch(workspace_root: Path, injection_patch: Path | None) -> None:
    if injection_patch is None:
        return
    _run_checked(
        ["git", "apply", "--whitespace=nowarn", str(injection_patch)],
        cwd=workspace_root,
    )


def _remove_sensitive_paths(workspace_root: Path) -> None:
    # Hard boundary: keep repo-under-test code, hide benchmark internals.
    blocked = (
        ".lab",
        "bench/benchmark/tasks",
        "bench/benchmark/repos",
        "bench/agentlab",
    )
    for rel in blocked:
        target = workspace_root / rel
        if not target.exists():
            continue
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()


def _dataset_pack_root(root: Path, dataset_pack_root: str) -> Path:
    path = Path(dataset_pack_root)
    if path.is_absolute():
        return path
    return root / path


def _materialize_workspace_seed_pack(
    root: Path,
    task_dir: Path,
    task_yaml: dict[str, Any],
    dataset_pack_root: str,
) -> str:
    repo_snapshot = _resolve_repo_snapshot_path(root, task_dir, task_yaml)
    injection_patch = _resolve_injection_patch_path(task_dir, task_yaml)
    digest = _workspace_seed_digest(repo_snapshot, injection_patch)

    pack_root = _dataset_pack_root(root, dataset_pack_root)
    pack_root.mkdir(parents=True, exist_ok=True)
    pack_dir = pack_root / digest
    if pack_dir.exists():
        return digest

    tmp_dir = Path(
        tempfile.mkdtemp(
            prefix=f"{digest[:12]}.tmp.",
            dir=str(pack_root),
        )
    )
    try:
        _extract_repo_snapshot(repo_snapshot, tmp_dir)
        _apply_injection_patch(tmp_dir, injection_patch)
        _remove_sensitive_paths(tmp_dir)
        metadata = {
            "schema_version": "workspace_seed_pack_v1",
            "pack_format_version": PACK_FORMAT_VERSION,
            "digest": digest,
            "repo_snapshot": str(repo_snapshot.relative_to(root) if repo_snapshot.is_relative_to(root) else repo_snapshot),
            "injection_patch": (
                str(injection_patch.relative_to(root))
                if injection_patch is not None and injection_patch.is_relative_to(root)
                else (str(injection_patch) if injection_patch is not None else None)
            ),
        }
        (tmp_dir / ".agentlab_workspace_seed_pack.json").write_text(
            json.dumps(metadata, separators=(",", ":"), sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(tmp_dir, pack_dir)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    return digest


def _build_task_row(
    root: Path,
    suite: str,
    split: str,
    benchmark_name: str,
    adapter_id: str,
    task_dir: Path,
    default_task_image: str | None,
    dataset_pack_root: str,
    require_task_image: bool,
) -> dict[str, Any]:
    task_yaml = _load_task_yaml(task_dir)
    task_id = task_dir.name
    task_image = _resolve_task_image(
        task_yaml=task_yaml,
        default_task_image=default_task_image,
        task_id=task_id,
        require_task_image=require_task_image,
    )
    workspace_seed_digest = _materialize_workspace_seed_pack(
        root=root,
        task_dir=task_dir,
        task_yaml=task_yaml,
        dataset_pack_root=dataset_pack_root,
    )

    task_payload: dict[str, Any] = {
        "id": task_id,
        "repo_id": task_yaml.get("repo_id"),
        "task_dir": str(task_dir.relative_to(root)),
        "bench": {
            "suite": suite,
            "task_dir": str(task_dir.relative_to(root)),
        },
        "benchmark": {
            "adapter_id": adapter_id,
            "name": benchmark_name,
            "split": split,
        },
        "input": {
            "prompt": _task_prompt(task_dir, task_yaml),
        },
    }

    if task_image:
        task_payload["image"] = task_image

    public_command = task_yaml.get("public_command")
    if isinstance(public_command, str) and public_command.strip():
        task_payload["public_command"] = public_command.strip()

    hidden_command = task_yaml.get("hidden_command")
    if isinstance(hidden_command, str) and hidden_command.strip():
        task_payload["hidden_command"] = hidden_command.strip()

    row: dict[str, Any] = {
        "schema_version": TASK_BOUNDARY_SCHEMA_VERSION,
        "task": task_payload,
        "workspace_seed": {
            "dataset_pack_ref": f"sha256:{workspace_seed_digest}",
        },
        "workspace_files": _workspace_overlay_files(task_dir),
        "mount_references": [],
    }
    return row


def _iter_task_dirs(suite_dir: Path) -> list[Path]:
    return sorted(
        p for p in suite_dir.iterdir() if p.is_dir() and p.name.startswith("TASK")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Export bench suite to AgentLab task JSONL")
    parser.add_argument(
        "--suite",
        default=DEFAULT_SUITE,
        help="Suite under bench/benchmark/tasks/ (default: v0)",
    )
    parser.add_argument(
        "--split",
        default=DEFAULT_SPLIT,
        help=f"Benchmark split label (default: {DEFAULT_SPLIT})",
    )
    parser.add_argument(
        "--benchmark-name",
        default=DEFAULT_BENCHMARK_NAME,
        help=f"Benchmark name field (default: {DEFAULT_BENCHMARK_NAME})",
    )
    parser.add_argument(
        "--adapter-id",
        default="bench_v0",
        help="Benchmark adapter_id written into task rows (default: bench_v0)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSONL path (default: data/bench_<suite>.task_boundary_v2.jsonl)",
    )
    parser.add_argument(
        "--image",
        default=None,
        help="Legacy alias for --default-task-image",
    )
    parser.add_argument(
        "--default-task-image",
        default=None,
        help=(
            "Base Docker image for per-task image naming. Rows emit {base}-{task_id}:latest "
            "when task.yaml does not provide image."
        ),
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help="Deprecated/ignored (runner-owned workspace boundary)",
    )
    parser.add_argument(
        "--default-task-workspace",
        default=None,
        help="Deprecated/ignored (runner-owned workspace boundary)",
    )
    parser.add_argument(
        "--dataset-pack-root",
        default=DEFAULT_DATASET_PACK_ROOT,
        help=f"Dataset pack root (default: {DEFAULT_DATASET_PACK_ROOT})",
    )
    parser.add_argument(
        "--require-task-image",
        action="store_true",
        default=False,
        help="Fail export when task.image cannot be resolved",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of tasks to export",
    )
    args = parser.parse_args()

    root = _repo_root()
    suite_dir = root / "bench" / "benchmark" / "tasks" / args.suite
    if not suite_dir.exists():
        raise FileNotFoundError(f"suite directory not found: {suite_dir}")

    schema_tag = TASK_BOUNDARY_SCHEMA_VERSION
    default_task_image = _candidate_string(args.default_task_image) or _candidate_string(args.image)

    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = root / out_path
    else:
        out_path = root / "data" / f"bench_{args.suite}.{schema_tag}.jsonl"

    task_dirs = _iter_task_dirs(suite_dir)
    if args.limit and args.limit > 0:
        task_dirs = task_dirs[: args.limit]

    rows = [
        _build_task_row(
            root=root,
            suite=args.suite,
            split=args.split,
            benchmark_name=args.benchmark_name,
            adapter_id=args.adapter_id,
            task_dir=task_dir,
            default_task_image=default_task_image,
            dataset_pack_root=args.dataset_pack_root,
            require_task_image=args.require_task_image,
        )
        for task_dir in task_dirs
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    out_path.write_text(body + ("\n" if rows else ""), encoding="utf-8")

    print(
        json.dumps(
            {
                "suite": args.suite,
                "count": len(rows),
                "output": str(out_path),
                "split": args.split,
                "benchmark_name": args.benchmark_name,
                "adapter_id": args.adapter_id,
                "schema_version": schema_tag,
                "default_task_image": default_task_image,
                "dataset_pack_root": str(_dataset_pack_root(root, args.dataset_pack_root)),
                "require_task_image": args.require_task_image,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
