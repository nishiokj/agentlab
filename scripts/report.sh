#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

RUNS_DIR="${1:-runs}"
OUT_DIR="${2:-reports}"

cd "$ROOT_DIR"
python -m bench.cli report --runs "$RUNS_DIR" --out "$OUT_DIR"
