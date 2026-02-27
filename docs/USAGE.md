# Benchmark Usage Guide

## Prerequisites

- Docker >= 24
- Linux x86_64
- 4+ CPU cores, 16GB RAM, 50GB free disk
- Python 3.11.x

## Quick Start

```bash
# 1. Bootstrap development environment
make bootstrap

# 2. Validate schemas
python -m bench.cli validate-schemas

# 3. Build Docker images
bash scripts/build_images.sh

# 4. Validate a single task
python -m bench.cli validate-task tasks/v0/TASK001

# 5. Validate entire suite
python -m bench.cli validate-suite v0 --jobs 4

# 6. Run an agent on the benchmark
python -m bench.cli run --suite v0 --agent dummy --runs-dir runs/my_run --max-tasks 3

# 7. Grade and generate reports
python -m bench.cli report --runs runs/my_run --out reports/my_report
```

## Docker Architecture

### Images

- **bench-base:dev**: Shared base with Python 3.11, git, ripgrep, patch
- **bench-agent:dev**: Agent sandbox with tool server
- **bench-grader:dev**: Grader sandbox with hidden runner

### Runtime Isolation

Both agent and grader containers run with `--network none` (no network
access at runtime). Build-time network access is allowed for installing
pinned dependencies.

### Determinism Settings

All containers enforce:
- `PYTHONHASHSEED=0`
- `TZ=UTC`
- `LC_ALL=C.UTF-8`
- `LANG=C.UTF-8`
- `SOURCE_DATE_EPOCH=1700000000`
- `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`
- `HOME=/tmp/benchhome` (clean home directory)

## Agent Sandbox vs Grader Sandbox

| Property | Agent | Grader |
|----------|-------|--------|
| Network | none | none |
| Workspace | repo + injection + public | repo + injection + agent patch |
| Hidden tests | NOT available | Available |
| Private solution | NOT available | NOT available |
| Tool server | Running on localhost | Not needed |
| Time limit | 20 min default | 5 min default |

## Tool Server

The agent container runs an HTTP tool server on `localhost:8080` with five tools:

- `search`: ripgrep-like text search
- `list_dir`: directory listing
- `read_file`: file reading with byte ranges
- `apply_patch`: unified diff application with policy enforcement
- `run`: command execution with timeout

## Patch Policy

Agents are restricted in what files they can edit. Default deny patterns:
- Benchmark harness files (`bench/**`, `schemas/**`, `scripts/**`)
- Repo config files (`pytest.ini`, `conftest.py`, `pyproject.toml`, etc.)

Tasks can customize via `policy/allow_edit_globs.txt` and `policy/deny_edit_globs.txt`.
