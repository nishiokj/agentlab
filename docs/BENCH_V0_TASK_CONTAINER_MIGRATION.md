# Bench v0 Task-Container Migration Plan

Date: 2026-03-01
Owner: Bench / Runner
Status: Implemented (Hard Cutover)

## Goal

Migrate `bench v0` from `task_boundary_v1` (repo-snapshot model) to a per-task container model using:

- `task_boundary_v2` rows with `task.image` (and optional `task.workspace`)
- `runtime.agent.image_source: per_task`
- `runtime.agent.artifact` injection in container mode

## Current State

`bench v0` now exports `task_boundary_v2` only and enforces `task.image` resolution.

- Exporter hard-cut contract:
  - `bench/integration/agentlab/export_bench_suite_to_jsonl.py`
  - v1 emission removed
  - unresolved `task.image` is rejected
- Bench per-task experiment added:
  - `.lab/experiments/bench_v0_per_task.yaml`
- Bench preflight/smoke script added:
  - `scripts/agentlab/smoke_bench_v0_per_task.sh`
- Runner requirements remain enforced:
  - container sandbox mode
  - `runtime.agent.artifact`
  - `task.image` in dataset rows

## Target Architecture

1. Bench dataset exporter emits `task_boundary_v2` records.
2. Each task record has:
   - `task.id`
   - `task.image`
   - optional `task.workspace`
   - existing benchmark metadata fields
3. Experiment runs in container mode with:
   - `runtime.agent.image_source: per_task`
   - `runtime.agent.artifact: <tar.gz>`
4. Runner injects artifact into selected task image and executes both:
   - agent runtime command
   - benchmark adapter command (if configured)

## Migration Scope

### In Scope

- Bench exporter changes (`v2` emission + image fields)
- Task-image mapping strategy and image readiness checks
- Experiment YAML for per-task execution
- Preflight/smoke coverage for per-task constraints

### Out of Scope (Minimal Migration)

- Rewriting bench grading semantics
- Replacing bench taskkit with a new evaluator framework
- Runner core feature work (already implemented for this path)

## Implementation Plan

## Phase 1: Exporter + Contract

Update `bench/integration/agentlab/export_bench_suite_to_jsonl.py`:

1. Emit `schema_version: task_boundary_v2`.
2. Add `task.image`.
3. Optionally add `task.workspace`.
4. Add CLI flags:
   - `--require-task-image`
   - `--default-task-image <image>`
   - `--default-task-workspace <path>`
5. Keep `workspace_files` and `mount_references` behavior unchanged.

Suggested behavior:

- If `--require-task-image` and no image resolves for a task, fail export.
- If no per-task image is available yet, allow temporary fallback via `--default-task-image`.

## Phase 2: Task Image Strategy

Choose one of:

1. **Single shared image for all v0 tasks (fastest)**:
   - Populate same `task.image` for every row.
   - Still use per-task model contractually.
2. **True per-task images (recommended end state)**:
   - Build/publish image per task or per task-family.

Image requirements for current bench v0 workload:

- `/bin/sh`
- Python runtime
- tooling used by grading path (`git`, `patch`)
- `bun` (hidden runners invoke `bun -e` in current tasks)

## Phase 3: Experiment Wiring

Create a bench per-task experiment yaml (new file under `.lab/experiments/`):

1. `dataset.schema_version: task_boundary_v2`
2. `runtime.policy.sandbox.mode: container`
3. `runtime.agent.image_source: per_task`
4. `runtime.agent.artifact: <artifact path>`
5. Runtime + benchmark adapter commands pointing to paths available in the injected artifact/image.

## Phase 4: Validation and Smoke

Run this gate sequence:

1. `lab preflight <bench_per_task_experiment.yaml>`
2. Ensure no failures on:
   - missing artifact
   - missing task images
   - non-pullable images
   - missing `/bin/sh`
3. Run 1-task smoke trial:
   - verify trial completes
   - verify benchmark score artifact behavior

## Risks and Mitigations

1. **Image/tool mismatch** (missing `bun`, `git`, or `patch`)
   - Add explicit image smoke checks before full runs.
2. **Artifact path/import issues for bench adapter**
   - Ensure injected artifact preserves expected module-relative layout.
3. **Workspace path mismatch**
   - Set `task.workspace` explicitly if task image does not use `/agentlab/workspace`.
4. **Cold-start pull latency**
   - Pre-pull unique task images before large runs.

## Effort Estimate

## Minimal Migration (recommended first pass)

- 3 to 6 engineering days
- Output: bench v0 runs in per-task container mode with current grading semantics intact

## Full Native Task-Container Grading Refactor

- 1 to 2 weeks
- Output: grading path no longer re-materializes from repo snapshots and is fully container-native end-to-end

## Acceptance Criteria

1. Exporter produces valid `task_boundary_v2` rows for bench v0.
2. All rows have resolvable `task.image` for per-task runs.
3. `lab preflight` passes for the per-task bench experiment.
4. One-trial run executes successfully in per-task mode.
5. Benchmark prediction/score artifacts are generated as expected.

## Proposed First Deliverable

1. Exporter update + tests.
2. One bench per-task experiment YAML.
3. One smoke script that runs preflight + limit-1 trial.
