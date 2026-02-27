#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

SUITE="${1:-v0}"
JOBS="${2:-4}"

cd "$ROOT_DIR"
python -m bench.cli validate-suite "$SUITE" --jobs "$JOBS"
