from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def candidate_lab_binaries(repo_root: Path) -> List[Path]:
    return [
        repo_root / "lab",
        repo_root / "rust" / "agentlab" / "target" / "debug" / "lab-cli",
        repo_root / "rust" / "agentlab" / "target" / "release" / "lab-cli",
    ]


def resolve_lab_binary(repo_root: Path, provided: Optional[str] = None) -> str:
    if provided:
        return provided
    for cand in candidate_lab_binaries(repo_root):
        if cand.exists() and os.access(cand, os.X_OK):
            return str(cand)
    return "lab-cli"


def parse_key_value_lines(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or ":" not in line:
            continue
        key, val = line.split(":", 1)
        out[key.strip()] = val.strip()
    return out


def parse_run_identity(text: str) -> Tuple[Optional[str], Optional[str]]:
    kv = parse_key_value_lines(text)
    return kv.get("run_id"), kv.get("run_dir")


def read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
            except json.JSONDecodeError:
                continue
    return rows


def list_run_dirs(repo_root: Path) -> List[Path]:
    runs_root = repo_root / ".lab" / "runs"
    if not runs_root.exists():
        return []
    dirs = [p for p in runs_root.iterdir() if p.is_dir()]
    return sorted(dirs, key=lambda p: p.name, reverse=True)


def detect_harness_candidates(repo_root: Path) -> List[Dict[str, str]]:
    candidates: List[Dict[str, str]] = []
    checks: List[Tuple[str, str, str]] = [
        ("python3", "./harness.py", "Detected harness.py"),
        ("python3", "./train.py", "Detected train.py"),
        ("node", "./agentlab/harness.js run", "Detected AgentLab wrapper"),
        ("node", "./harness.js", "Detected harness.js"),
        ("node", "./agentlab_demo_harness.js run", "Detected demo harness"),
        ("bun", "./scripts/agentlab/run_cli.ts", "Detected legacy TypeScript wrapper"),
    ]
    for cmd, args, why in checks:
        first_path = args.split(" ")[0].lstrip("./")
        if (repo_root / first_path).exists():
            full = f"{cmd} {args}"
            candidates.append({"command": full, "why": why})
    if (repo_root / "Cargo.toml").exists():
        candidates.append(
            {
                "command": "./target/release/train",
                "why": "Detected Cargo.toml (Rust project)",
            }
        )
    if (repo_root / "go.mod").exists():
        candidates.append(
            {
                "command": "./train",
                "why": "Detected go.mod (Go project)",
            }
        )
    return candidates


def run_grade_summary(attestation: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not attestation:
        return {}
    grades = attestation.get("grades", {})
    if not isinstance(grades, dict):
        return {}
    out: Dict[str, str] = {}
    for k in [
        "integration_level",
        "replay_grade",
        "isolation_grade",
        "comparability_grade",
        "provenance_grade",
        "privacy_grade",
    ]:
        v = grades.get(k)
        if isinstance(v, str):
            out[k] = v
    return out
