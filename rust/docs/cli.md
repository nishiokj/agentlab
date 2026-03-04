# CLI Reference

This document covers common `lab-cli` commands in the Rust workspace.

## Build

```bash
cargo build -p lab-cli --release
./target/release/lab-cli --help
```

## Run/Inspect Basics

```bash
# List standardized views for a run
./target/release/lab-cli views <run_id_or_run_dir>

# Query raw/standard views with SQL
./target/release/lab-cli query <run_id_or_run_dir> "SELECT * FROM trials LIMIT 20"

# Live scoreboard
./target/release/lab-cli scoreboard <run_id_or_run_dir> --interval-seconds 2
```

## Live Traces

Use `views-live` with the `trace` view to watch event-level trace rows update.

```bash
# Live side-by-side trace diagnostics (AB runs)
./target/release/lab-cli views-live <run_id_or_run_dir> trace --interval-seconds 1 --limit 200

# Alias for trace
./target/release/lab-cli views-live <run_id_or_run_dir> trace-diff --interval-seconds 1
```

Useful flags:

- `--no-clear`: keep previous refresh output on screen
- `--once`: render one refresh and exit

Notes:

- `trace`/`trace-diff` are AB-oriented standardized views (backed by `ab_trace_row_side_by_side`).
- If a run/view-set does not expose `trace`, use `lab query` directly against `events`.

```bash
./target/release/lab-cli query <run_id_or_run_dir> \
  "SELECT run_id, trial_id, event_type, ts FROM events ORDER BY ts DESC LIMIT 200"
```

## View Discovery

```bash
# show standardized view surface
./target/release/lab-cli views <run_id_or_run_dir>

# include raw/internal views
./target/release/lab-cli views <run_id_or_run_dir> --all
```

## Help

```bash
./target/release/lab-cli views-live --help
./target/release/lab-cli views --help
./target/release/lab-cli query --help
./target/release/lab-cli scoreboard --help
```
