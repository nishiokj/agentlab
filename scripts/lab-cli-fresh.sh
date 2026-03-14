#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="$(
python3 - "${BASH_SOURCE[0]}" <<'PY'
import os
import sys

print(os.path.realpath(sys.argv[1]))
PY
)"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"

resolve_experiments_root() {
  local candidate
  for candidate in \
    "${AGENTLAB_EXPERIMENTS_ROOT:-}" \
    "$SCRIPT_DIR/.." \
    "$SCRIPT_DIR/../Experiments" \
    "$SCRIPT_DIR/Experiments"
  do
    if [[ -n "$candidate" && -f "$candidate/rust/Cargo.toml" ]]; then
      (cd "$candidate" && pwd)
      return 0
    fi
  done
  return 1
}

ROOT_DIR="$(resolve_experiments_root)" || {
  echo "[lab-cli-fresh] unable to locate sibling Experiments repo; set AGENTLAB_EXPERIMENTS_ROOT" >&2
  exit 1
}

RUST_DIR="$ROOT_DIR/rust"
BINARY="$RUST_DIR/target/release/lab-cli"

latest_watch_json="$(
python3 - "$RUST_DIR" <<'PY'
from pathlib import Path
import json
import sys

rust_dir = Path(sys.argv[1])
watch_paths = [
    rust_dir / "Cargo.toml",
    rust_dir / "Cargo.lock",
    rust_dir / "crates" / "lab-cli" / "Cargo.toml",
    rust_dir / "crates" / "lab-cli" / "src",
    rust_dir / "crates" / "lab-runner" / "src",
]

latest_mtime = 0.0
latest_path = None
for path in watch_paths:
    if not path.exists():
        continue
    if path.is_file():
        mtime = path.stat().st_mtime
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_path = path
        continue
    for child in path.rglob("*"):
        if not child.is_file():
            continue
        mtime = child.stat().st_mtime
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_path = child

print(json.dumps({
    "mtime": latest_mtime,
    "path": str(latest_path) if latest_path else "",
}))
PY
)"

latest_watch_mtime="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["mtime"])' "$latest_watch_json")"
latest_watch_path="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["path"])' "$latest_watch_json")"

needs_build=0
if [[ ! -x "$BINARY" ]]; then
  needs_build=1
else
  binary_mtime="$(python3 -c 'import os,sys; print(os.path.getmtime(sys.argv[1]))' "$BINARY")"
  if python3 - "$binary_mtime" "$latest_watch_mtime" <<'PY'
import sys
binary_mtime = float(sys.argv[1])
watch_mtime = float(sys.argv[2])
raise SystemExit(0 if binary_mtime >= watch_mtime else 1)
PY
  then
    :
  else
    needs_build=1
  fi
fi

if [[ "$needs_build" -eq 1 ]]; then
  echo "[lab-cli-fresh] rebuilding lab-cli because watched source is newer: ${latest_watch_path}" >&2
  (cd "$RUST_DIR" && cargo build -p lab-cli --release)
fi

exec "$BINARY" "$@"
