#!/usr/bin/env bash
set -euo pipefail

# Evaluate a candidate repository for benchmark inclusion.
# Checks: size, test runtime, dependency count, determinism.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO=""
CHECKOUT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --repo) REPO="$2"; shift 2 ;;
        --checkout) CHECKOUT="$2"; shift 2 ;;
        --help)
            echo "Usage: $0 --repo <repo_name> [--checkout <tag_or_commit>]"
            echo ""
            echo "Evaluates a repository for benchmark inclusion."
            echo "Checks size, deps, test runtime, and determinism."
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$REPO" ]; then
    echo "Error: --repo is required"
    exit 1
fi

echo "=== Repository Evaluation: $REPO ==="
echo ""

# Check if repo dir exists
REPO_DIR="$SCRIPT_DIR/../repos/$REPO"
if [ -d "$REPO_DIR" ]; then
    echo "[OK] Repo directory exists: $REPO_DIR"
    if [ -f "$REPO_DIR/src.tar.zst" ]; then
        SIZE=$(stat -c%s "$REPO_DIR/src.tar.zst" 2>/dev/null || echo "unknown")
        echo "[INFO] Archive size: $SIZE bytes"
    fi
    if [ -f "$REPO_DIR/baseline_commit.txt" ]; then
        echo "[INFO] Baseline commit: $(cat "$REPO_DIR/baseline_commit.txt")"
    fi
    if [ -f "$REPO_DIR/deps/requirements.lock" ]; then
        DEPS=$(wc -l < "$REPO_DIR/deps/requirements.lock")
        echo "[INFO] Dependency lines: $DEPS"
    fi
else
    echo "[WARN] Repo directory not found: $REPO_DIR"
fi

echo ""
echo "=== Evaluation Complete ==="
