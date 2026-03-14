#!/usr/bin/env python3
"""Fast targeting and smell metrics for adversarial test review."""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable


TEST_PATH_RE = re.compile(
    r"(^|/)(__tests__|tests)(/|$)|\.(test|spec)\.[A-Za-z0-9]+$"
)
IMPORT_RE = re.compile(
    r"""^\s*import(?:["'\s\w{},*]+from\s+)?["']([^"']+)["']""",
    re.MULTILINE,
)


@dataclass(frozen=True)
class SmellPattern:
    code: str
    label: str
    points: int
    regex: re.Pattern[str]


LINE_PATTERNS = [
    SmellPattern(
        "shallow-assertion",
        "Shallow assertion",
        -1,
        re.compile(r"\.toBeDefined\(|\.toBeTruthy\(|\.toHaveProperty\("),
    ),
    SmellPattern(
        "skip-gate",
        "Silent skip or gating",
        -2,
        re.compile(r"\b(?:describe|it|test)\.skip\(|\?\s*describe\s*:\s*describe\.skip"),
    ),
    SmellPattern(
        "owned-code-mock",
        "Mocking or spying",
        -2,
        re.compile(r"\b(?:vi|jest)\.(?:mock|fn|spyOn)\(|mockResolvedValue|mockRejectedValue"),
    ),
    SmellPattern(
        "snapshot",
        "Snapshot assertion",
        -1,
        re.compile(r"\.toMatchSnapshot\("),
    ),
    SmellPattern(
        "nothrow-only",
        "not.toThrow-style weak assertion",
        -1,
        re.compile(r"\.not\.toThrow\("),
    ),
    SmellPattern(
        "impl-rationalization",
        "Comment rationalizes current implementation",
        -3,
        re.compile(r"current implementation|by design|tried first|matches first|priority ordering", re.I),
    ),
    SmellPattern(
        "conditional-assertion",
        "Conditional assertion block",
        -1,
        re.compile(r"^\s*if\s*\(.*\)\s*\{?\s*$"),
    ),
]


def run_git(args: list[str]) -> str:
    proc = subprocess.run(
        ["git", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout


def is_test_path(path: str) -> bool:
    return bool(TEST_PATH_RE.search(path))


def unique(seq: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def expand_test_path(path: str) -> list[str]:
    candidate = pathlib.Path(path)
    if candidate.is_dir():
        return sorted(
            str(found)
            for found in candidate.rglob("*")
            if found.is_file() and is_test_path(str(found))
        )
    return [path] if is_test_path(path) else []


def recent_test_paths(selector: str) -> list[str]:
    paths: list[str] = []
    if selector in {"recent", "working-tree"}:
        for line in run_git(["status", "--porcelain"]).splitlines():
            if not line:
                continue
            path = line[3:] if len(line) > 3 else ""
            if path and is_test_path(path):
                paths.extend(expand_test_path(path))
    if selector in {"recent", "head"}:
        head_ref = run_git(["rev-parse", "--verify", "HEAD~1"]).strip()
        if head_ref:
            for path in run_git(["diff", "--name-only", "--diff-filter=AM", "HEAD~1", "HEAD"]).splitlines():
                if is_test_path(path):
                    paths.extend(expand_test_path(path))
    return unique(paths)


def window_match(lines: list[str], index: int, pattern: re.Pattern[str], window: int = 3) -> bool:
    end = min(len(lines), index + window + 1)
    blob = "\n".join(lines[index:end])
    return bool(pattern.search(blob))


def smell_hits(path: pathlib.Path) -> dict:
    content = path.read_text(encoding="utf-8")
    lines = content.splitlines()
    hits: list[dict] = []

    for idx, line in enumerate(lines, start=1):
        for pattern in LINE_PATTERNS:
            if pattern.regex.search(line):
                if pattern.code == "conditional-assertion":
                    if not window_match(lines, idx - 1, re.compile(r"\bexpect\("), window=4):
                        continue
                hits.append(
                    {
                        "code": pattern.code,
                        "label": pattern.label,
                        "line": idx,
                        "points": pattern.points,
                        "excerpt": line.strip(),
                    }
                )

        lower_line = line.lower()
        suspicious_identity = (
            "same reference" in lower_line
            or "identity" in lower_line
            or "in-place" in lower_line
            or "as-is" in lower_line
            or "pass-through" in lower_line
            or re.search(r"\.toBe\((?:existing|entry|err|obj|reg\d*|mock[A-Z_a-z]|CONNECTOR_|REGISTRY\.)", line)
        )
        if suspicious_identity and ".toBe(" in line:
            hits.append(
                {
                    "code": "identity-assertion",
                    "label": "Possible same-reference or identity assertion",
                    "line": idx,
                    "points": -2,
                    "excerpt": line.strip(),
                }
            )

        if "Object.keys(" in line and window_match(lines, idx - 1, re.compile(r"\.toHaveLength\(")):
            hits.append(
                {
                    "code": "exact-key-count",
                    "label": "Exact object key count assertion",
                    "line": idx,
                    "points": -2,
                    "excerpt": line.strip(),
                }
            )

        if ("params.keys(" in line or "searchParams" in line) and window_match(
            lines, idx - 1, re.compile(r"\.toHaveLength\(")
        ):
            hits.append(
                {
                    "code": "exact-param-count",
                    "label": "Exact query-param count assertion",
                    "line": idx,
                    "points": -2,
                    "excerpt": line.strip(),
                }
            )

    imports = sorted(
        {
            match.group(1)
            for match in IMPORT_RE.finditer(content)
            if match.group(1).startswith(".") or "/" in match.group(1)
        }
    )
    tests = len(re.findall(r"\b(?:it|test)\s*(?:\.each)?\s*\(", content))
    total_points = sum(hit["points"] for hit in hits)
    return {
        "path": str(path),
        "test_count": tests,
        "imports": imports,
        "penalty_points": total_points,
        "hit_count": len(hits),
        "hits": hits,
    }


def resolve_paths(args: argparse.Namespace) -> list[str]:
    if args.paths:
        return unique([p for p in args.paths if is_test_path(p)])
    if getattr(args, "selector", None):
        return recent_test_paths(args.selector)
    return recent_test_paths("recent")


def cmd_recent(args: argparse.Namespace) -> int:
    paths = recent_test_paths(args.selector)
    if args.format == "json":
        print(json.dumps({"selector": args.selector, "paths": paths}, indent=2))
    else:
        for path in paths:
            print(path)
    return 0


def cmd_smells(args: argparse.Namespace) -> int:
    paths = resolve_paths(args)
    reports = [smell_hits(pathlib.Path(path)) for path in paths if pathlib.Path(path).is_file()]
    print(json.dumps({"files": reports}, indent=2))
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    paths = resolve_paths(args)
    reports = [smell_hits(pathlib.Path(path)) for path in paths if pathlib.Path(path).is_file()]
    summary = {
        "selector": getattr(args, "selector", None),
        "file_count": len(reports),
        "total_tests": sum(report["test_count"] for report in reports),
        "total_penalty_points": sum(report["penalty_points"] for report in reports),
        "files": sorted(reports, key=lambda report: (report["penalty_points"], -report["hit_count"])),
    }
    if args.format == "json":
        print(json.dumps(summary, indent=2))
        return 0

    print(
        f"files={summary['file_count']} tests={summary['total_tests']} "
        f"penalty_points={summary['total_penalty_points']}"
    )
    for report in summary["files"]:
        print(
            f"{report['path']}: tests={report['test_count']} hits={report['hit_count']} "
            f"penalty={report['penalty_points']}"
        )
        for hit in report["hits"][: args.max_hits]:
            print(f"  L{hit['line']}: {hit['code']} {hit['excerpt']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    recent = subparsers.add_parser("recent", help="List recent test paths")
    recent.add_argument("--selector", choices=["recent", "working-tree", "head"], default="recent")
    recent.add_argument("--format", choices=["text", "json"], default="text")
    recent.set_defaults(func=cmd_recent)

    smells = subparsers.add_parser("smells", help="Emit line-level smell hits for test files")
    smells.add_argument("paths", nargs="*")
    smells.add_argument("--selector", choices=["recent", "working-tree", "head"], default=None)
    smells.set_defaults(func=cmd_smells)

    summary = subparsers.add_parser("summary", help="Summarize penalty signals for test files")
    summary.add_argument("paths", nargs="*")
    summary.add_argument("--selector", choices=["recent", "working-tree", "head"], default="recent")
    summary.add_argument("--format", choices=["text", "json"], default="text")
    summary.add_argument("--max-hits", type=int, default=5)
    summary.set_defaults(func=cmd_summary)

    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
