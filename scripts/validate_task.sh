#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <task_path>"
    echo "Example: $0 tasks/v0/TASK001"
    exit 1
fi

TASK_PATH="$1"

cd "$ROOT_DIR"
python -m bench.cli validate-task "$TASK_PATH"
