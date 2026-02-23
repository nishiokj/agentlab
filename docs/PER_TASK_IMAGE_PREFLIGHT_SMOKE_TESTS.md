# Per-Task Image Preflight and Smoke Tests

Status: executable test plan  
Owner: `lab-runner` / `lab-cli`  
Scope: boundary checks for `task_boundary_v2` + `runtime.agent.image_source=per_task`

## Purpose

Validate the hard boundaries before full runs:

1. Experiment/runtime contract is valid.
2. Dataset task contract is valid.
3. Container/image prerequisites are valid.
4. One-trial execution path is valid (including grading artifacts when enabled).

## Boundaries Under Test

1. `runtime.agent.image_source=per_task` requires container sandbox mode.
2. `runtime.agent.artifact` is required for per-task image mode.
3. `task.image` is required at execution time for each task in per-task mode.
4. Per-task workspace wiring (`task.workspace`) is honored.
5. Grading writes expected score artifacts (when benchmark adapter is enabled).

## Test Harness

```bash
set -euo pipefail

lab() {
  cargo run --manifest-path rust/Cargo.toml -p lab-cli -- "$@"
}

EXP="${EXP:-.lab/experiments/swebench_lite_curated.yaml}"
RUN_SCRIPT="${RUN_SCRIPT:-scripts/agentlab/run_curated_experiment.sh}"
```

## Preflight Tests

### PF-01 Happy path preflight passes

```bash
lab preflight "$EXP"
lab preflight "$EXP" --json | jq -e '.ok == true'
```

Expected:

1. Exit code `0`.
2. JSON output includes checks named:
   `probe_trial_input`, `dataset_task_ids`, `benchmark_grader_reachable`, `container_ready`, `dependency_files_exist`.

### PF-02 Reject per-task image source in local sandbox mode

Use an experiment fixture configured with:

1. `/runtime/agent/image_source: per_task`
2. `/runtime/policy/sandbox/mode: local`

Command:

```bash
lab preflight path/to/fixture_local_mode_invalid.yaml
```

Expected:

1. Exit code non-zero.
2. Error contains:
   `/runtime/agent/image_source (value 'per_task' requires /runtime/policy/sandbox/mode='container')`.

### PF-03 Reject per-task image source without artifact

Use an experiment fixture configured with:

1. `/runtime/agent/image_source: per_task`
2. no `/runtime/agent/artifact`

Command:

```bash
lab preflight path/to/fixture_missing_artifact.yaml
```

Expected:

1. Exit code non-zero.
2. Error contains: `/runtime/agent/artifact`.

### PF-04 Detect missing/unpullable per-task images

Use a dataset fixture where at least one row has:

1. `schema_version: task_boundary_v2`
2. `task.image: does/not/exist:latest`

Command:

```bash
lab preflight path/to/fixture_missing_image.yaml
```

Expected:

1. Exit code non-zero.
2. `container_ready` check reports image validation failure after preflight attempts `docker pull`.
3. Preflight scans all task rows in `per_task` mode and fails if any row has missing/invalid `task.image`.

Note:

1. Preflight now attempts `docker pull` for per-task images that are not already local.
2. The check fails only when an image is neither local nor pullable.

## Smoke Tests

### SM-01 One-trial run succeeds through runner path

```bash
AGENTLAB_LIMIT=1 bash "$RUN_SCRIPT"
RUN_DIR="$(find .lab/runs -mindepth 1 -maxdepth 1 -type d -name 'run_*' | sort | tail -n 1)"
test -n "$RUN_DIR"
test -f "$RUN_DIR/runtime/run_control.json"
jq -e '.status == "completed" or .status == "failed" or .status == "cancelled" or .status == "canceled"' \
  "$RUN_DIR/runtime/run_control.json" >/dev/null
```

Expected:

1. Run reaches terminal state (not `running`).
2. Trial artifacts exist under `$RUN_DIR/trials/trial_*`.

### SM-02 Benchmark preflight artifact is written per trial

```bash
RUN_DIR="$(find .lab/runs -mindepth 1 -maxdepth 1 -type d -name 'run_*' | sort | tail -n 1)"
TRIAL_DIR="$(find "$RUN_DIR/trials" -mindepth 1 -maxdepth 1 -type d | sort | head -n 1)"
test -f "$TRIAL_DIR/benchmark_preflight.json"
jq -e '.schema_version == "benchmark_trial_preflight_v1"' "$TRIAL_DIR/benchmark_preflight.json" >/dev/null
jq -e '.task_id != null' "$TRIAL_DIR/benchmark_preflight.json" >/dev/null
```

Expected:

1. `benchmark_preflight.json` exists.
2. It includes `task_id` and preflight metadata for replay/grading.

### SM-03 Missing `task.image` fails at execution boundary in per-task mode

Use a dataset fixture where one row omits `task.image` and run with `image_source=per_task`.

Command:

```bash
lab run path/to/fixture_missing_task_image.yaml --executor local_docker
```

Expected:

1. Exit code non-zero.
2. Error contains:
   `task.image is required for task ... when runtime.agent.image_source='per_task'`.

### SM-04 Grading artifact contract check (when benchmark adapter enabled)

```bash
RUN_DIR="$(find .lab/runs -mindepth 1 -maxdepth 1 -type d -name 'run_*' | sort | tail -n 1)"
TRIAL_DIR="$(find "$RUN_DIR/trials" -mindepth 1 -maxdepth 1 -type d | sort | head -n 1)"
if [[ -f "$TRIAL_DIR/out/benchmark_score.json" ]]; then
  jq -e '.' "$TRIAL_DIR/out/benchmark_score.json" >/dev/null
fi
```

Expected:

1. If grading is enabled for the task, `benchmark_score.json` exists and is valid JSON.
2. If grading fails, runner marks trial with `grade_error` classification.

## Exit Criteria

All of the following must be true:

1. PF-01 passes.
2. PF-02, PF-03, PF-04 fail for the expected reason (correct boundary rejection).
3. SM-01 and SM-02 pass.
4. SM-03 fails with the expected per-task image boundary error.
5. SM-04 grading artifact behavior matches task grading policy.
