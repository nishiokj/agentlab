"""Canonical path constants for the benchmark."""

from __future__ import annotations

from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent.parent

# Schema files
TASK_SCHEMA = BENCH_ROOT / "schemas" / "task.schema.json"
TRACE_SCHEMA = BENCH_ROOT / "schemas" / "trace.schema.json"
SCORE_SCHEMA = BENCH_ROOT / "schemas" / "score.schema.json"

# Docker
DOCKERFILES_DIR = BENCH_ROOT / "bench" / "docker"

# Repos
REPOS_DIR = BENCH_ROOT / "repos"

# Tasks
TASKS_DIR = BENCH_ROOT / "tasks"

# Runs / Reports
RUNS_DIR = BENCH_ROOT / "runs"
REPORTS_DIR = BENCH_ROOT / "reports"

# Benchmark harness paths that agents must NOT modify
HARNESS_DENY_PATTERNS = [
    "bench/**",
    "schemas/**",
    "scripts/**",
    "Makefile",
    "pyproject.toml",
    "requirements.txt",
    "requirements.in",
]

# Default repo config files that agents must NOT modify (unless task allowlists)
REPO_CONFIG_DENY_PATTERNS = [
    "pytest.ini",
    "setup.cfg",
    "pyproject.toml",
    "conftest.py",
    "**/conftest.py",
    ".github/**",
    "tox.ini",
    "noxfile.py",
]
