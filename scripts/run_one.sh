#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

TASK=""
AGENT="dummy"
RUNS_DIR="runs"
TIMEOUT=1200

while [[ $# -gt 0 ]]; do
    case $1 in
        --task) TASK="$2"; shift 2 ;;
        --agent) AGENT="$2"; shift 2 ;;
        --runs-dir) RUNS_DIR="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --help)
            echo "Usage: $0 --task TASK_ID [--agent AGENT] [--runs-dir DIR] [--timeout SEC]"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$TASK" ]; then
    echo "Error: --task is required"
    exit 1
fi

cd "$ROOT_DIR"
python -m bench.cli run --suite v0 --agent "$AGENT" --runs-dir "$RUNS_DIR" --max-tasks 1 --timeout "$TIMEOUT"
