# Hard Cutover: Runner Cleanup and Decoupling

## Intent

This is a hard cutover plan. Backward-compatible slop is out of scope.

Required outcomes:

1. `continue` is one concept with one execution engine.
2. No benchmark-specific hardcoding in runner behavior.
3. No duplicated path/mount mapping logic.
4. No hidden coupling to specific experiment/benchmark families.

## Non-Negotiable Rules

1. No logic may branch on benchmark names, benchmark-specific payload keys, or task ID prefixes.
2. Trial execution logic must exist in one loop implementation.
3. Container path-to-host mapping must exist in one module/function family.
4. Contract paths come from `lab-core` constants only, not ad hoc string literals in runner flow.

## Current Violations (Must Be Removed)

1. Benchmark hardcoding:
`rust/crates/lab-runner/src/lib.rs:5037` through `rust/crates/lab-runner/src/lib.rs:5043`

- `task_requires_explicit_workspace_materialization` keys off `swebench` and `swebench_`.

2. Duplicate trial loop implementations:
`rust/crates/lab-runner/src/lib.rs:959` and `rust/crates/lab-runner/src/lib.rs:2908`

- `continue_run` and `run_experiment_with_behavior` duplicate major loop behavior.

3. Duplicate path/mount mapping:
`rust/crates/lab-runner/src/lib.rs:2397`, `rust/crates/lab-runner/src/lib.rs:7029`, `rust/crates/lab-runner/src/lib.rs:7600`

- Same path prefixes are interpreted in multiple places with overlapping logic.

4. Behavioral drift risk in continue:
`rust/crates/lab-runner/src/lib.rs:912`

- `continue_run` reconstructs with `RunBehavior::default()`, which can diverge from original run semantics.

## Target Architecture

### 1) Unified Run Engine

Create a single `RunEngine`/`ExecutionEngine` that owns schedule traversal and trial execution:

1. `run` and `continue` become entry points that load state and call one engine method.
2. Engine consumes `RunSessionState` (run metadata, schedule progress, policy, behavior, runtime profiles).
3. Engine emits deterministic progress updates and final analysis write.

Cutover effect:

1. Delete duplicated schedule loops in `continue_run` and `run_experiment_with_behavior`.
2. Keep only one path for trial execution, retry/pruning, evidence writes, and schedule progress updates.

### 2) Continue as a Single Concept

Define `continue` as:

1. "resume the persisted run state from its next schedule slot, regardless of why it stopped."

Operationally:

1. Persist full behavior/config required to reproduce run semantics.
2. `continue` reloads persisted state instead of reconstructing defaults.
3. Pause/stop/failure share the same continuation state machine.

Cutover effect:

1. Remove special-case divergence between failed-run continue and other resume paths.
2. Standardize control transitions via explicit run status state machine.

### 3) Benchmark-Agnostic Task Boundary Policy

Replace benchmark-name inference with explicit schema-driven policy:

1. Add explicit task-boundary requirement flag in config/schema (or explicit policy default).
2. Validate workspace materialization based on policy, not task ID or benchmark identity.
3. Keep `benchmark` section optional and orthogonal to runner core behavior.

Cutover effect:

1. Delete `task_requires_explicit_workspace_materialization` benchmark heuristics.
2. Any benchmark can opt in/out via declared policy, not implicit string checks.

### 4) Single Path Mapping Layer

Introduce one mapping utility that owns contract path translation:

1. One function family for runtime path resolution.
2. One source of truth for allowed contract mounts.
3. One validation strategy for absolute/relative paths by mode.

Cutover effect:

1. Remove duplicate prefix parsing in `resolve_event_path_for_trial`, `resolve_trial_io_host_path`, and `map_container_path_to_host`.
2. Keep compatibility aliases only if they are centralized and tested in one place.

## Cutover Work Plan

### Phase A: Structural Refactor

1. Extract shared trial execution pipeline from `run_experiment_with_behavior`.
2. Move schedule traversal into reusable engine method.
3. Change `continue_run` to load progress + call same engine.

### Phase B: Policy Decoupling

1. Remove benchmark-name checks from task boundary validation.
2. Add explicit policy schema and parser for workspace materialization requirements.
3. Update docs and schema validations accordingly.

### Phase C: Path/Contract Consolidation

1. Implement one contract path mapper.
2. Replace all direct prefix branches with mapper calls.
3. Remove dead path logic and duplicate branches.

### Phase D: Continue Semantics Hardening

1. Persist effective `RunBehavior` and runtime execution options in run state.
2. Make continue consume persisted behavior.
3. Unify terminal-state handling for failed/paused/interrupted continuation.

### Phase E: Cleanup

1. Delete obsolete helpers and duplicated code blocks.
2. Remove temporary compatibility branches no longer needed after migration.
3. Update tests to target only the new model.

## Explicit Delete/Replace Targets

1. Delete benchmark-specific materialization check:
- `task_requires_explicit_workspace_materialization` and its swebench coupling.

2. Replace duplicated loops with one engine:
- `continue_run` schedule loop.
- `run_experiment_with_behavior` schedule loop.

3. Replace duplicate path handlers:
- `resolve_event_path_for_trial` ad hoc prefix handling.
- duplicate prefix branches in `resolve_trial_io_host_path`.
- duplicate prefix branches in `map_container_path_to_host`.

## Acceptance Gates

A cutover is complete only when all are true:

1. There is exactly one schedule traversal implementation for run/continue.
2. `rg -n "swebench|starts_with\\(\"swebench_\"\\)" rust/crates/lab-runner/src/lib.rs` returns no behavior-driving hits.
3. Path prefix parsing exists in one shared mapper module/function family.
4. Continue from paused/failed/interrupted reuses identical trial execution semantics.
5. Existing durability tests pass and new parity tests pass:
- fresh run vs continue must produce identical outputs from same state and inputs.

## Test Additions Required

1. Continue parity:
- Start run, stop after N slots, continue, compare against uninterrupted run.

2. Benchmark-agnostic boundary:
- Non-swebench tasks with explicit workspace policy must validate identically.

3. Path mapper contract:
- Table-driven tests for all supported contract paths and aliases.

4. Behavior persistence:
- Dev/strict/custom behavior runs continue with identical effective behavior.

## Scope Notes

1. This repo currently contains proxy references primarily in docs, not runner implementation.
2. Proxy/runtime transport cutover should be tracked separately if implemented in another repo.
