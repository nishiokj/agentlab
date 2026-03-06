# Patch Spec: Runner Module Decomposition (include! Removal)

Status: Draft v1
Date: 2026-03-06

## Goal

Replace the `include!()` monolith in `lab-runner` with proper Rust modules. Enforce dependency direction at compile time, eliminate 20 dead-code warnings, and make each module independently testable.

The runner is currently ~29K lines in a single compilation scope via six `include!()` files. Functions call freely across files, circular dependencies are invisible, and dead code accumulates silently. This patch decomposes the runner into modules with explicit boundaries, where the compiler enforces what can depend on what.

---

## Current State

### File layout

```
lab-runner/src/
  lib.rs                          (12 lines — just include! directives)
  runner_part1_core.rs            (2,119 lines)
  runner_part2_lifecycle.rs       (3,799 lines)
  runner_part3_engine.rs          (3,366 lines)
  runner_part4_preflight_policy.rs (3,180 lines)
  runner_part5_runtime_io.rs      (4,052 lines)
  runner_part6_tests.rs           (11,320 lines)
  sink.rs                         (295 lines — already a proper module)
  persistence/
    sqlite_store.rs               (851 lines — already a proper module)
```

### Problems

1. **No enforced boundaries.** All six `include!()` files share one scope. Any function can call any other. The compiler cannot detect architectural violations.

2. **Circular dependencies.** Cross-file call analysis shows cycles in every direction: part1 calls part3/part4, part3 calls part2, part4 calls part2/part3. These aren't real architectural needs — they're shared utility functions dumped in whichever file had room.

3. **20 dead-code warnings.** Functions with zero callers: 17 in lab-runner, 3 in lab-analysis. The flat scope makes it impossible to notice when a function loses its last caller.

4. **11K-line test file.** Tests for all modules in one file, no isolation, no ability to run tests for a single module.

### Dependency map (current, with cycles)

```
part1 -> part3(1), part4(3)
part2 -> part1(19), part3(3), part4(12), part5(21)
part3 -> part1(21), part2(9), part4(27), part5(31)
part4 -> part1(3), part2(2), part3(2), part5(10)
part5 -> part2(14), part4(4)
part6 -> all (test-only)
```

---

## Target State

### File layout

```
lab-runner/src/
  lib.rs                  (module declarations + pub use re-exports)
  types.rs                (shared types, constants, enums, adapter traits)
  config.rs               (sealed package loading, variant/task resolution, schedule building)
  preflight.rs            (preflight checks, validation)
  trial.rs                (single-trial execution: workspace, mounts, container IO, result collection)
  engine.rs               (schedule orchestration, worker dispatch, result draining)
  lifecycle.rs            (continue, recover, replay, fork, pause, kill, resume)
  sink.rs                 (unchanged)
  persistence/
    sqlite_store.rs       (unchanged)
```

### Dependency graph (target, acyclic)

```
          lifecycle
              |
           engine
          /      \
   preflight    trial
          \      /
           config
              |
            types
              |
        sink + persistence
```

Each layer imports only from below. No cycles. The compiler enforces this — a module cannot import from a module that imports it.

---

## Module Definitions

### `types` — Shared vocabulary

**Extracted from:** part1_core (type/const/enum declarations only)

**Contains:**
- All public result types: `RunResult`, `ReplayResult`, `ForkResult`, `PauseResult`, `KillResult`, `ResumeResult`, `RecoverResult`, `BuildResult`, `ExperimentSummary`
- Configuration types: `RunBehavior`, `RunExecutionOptions`, `ExecutorKind`, `MaterializationMode`
- Internal shared types: `TrialSlot`, `TrialDispatch`, `AgentRuntimeConfig`, `AgentAdapterRef`, `ImageSource`, `AgentLaunchMode`, `PolicyConfig`, `StatePolicy`
- Adapter trait definitions
- All shared constants (container paths, env var names, protocol constants)

**Does not contain:** Any logic, any function bodies. Pure declarations.

**Why this is a module:** Everything else imports these types. Extracting them first breaks the majority of "cross-part" dependencies that are actually just type references, not logic calls.

### `config` — Input pipeline

**Extracted from:** part3_engine + part4_preflight_policy (config resolution functions)

**Contains:**
- `load_sealed_package_for_run()` — sealed package loading and integrity verification
- `resolve_variant_plan()` — variant resolution from experiment JSON
- `load_tasks()` — task loading from dataset path
- `resolve_dataset_path_in_package()` — dataset path resolution
- `build_trial_schedule()` — schedule construction from variants x tasks x replications
- `parse_policies()` — policy config extraction
- `parse_benchmark_config()` — benchmark adapter config extraction
- `resolve_variant_runtime_profile()` — per-variant runtime resolution
- Related helpers: `experiment_max_concurrency()`, `experiment_random_seed()`, `experiment_workload_type()`

**Boundary type:** `LoadedExperimentInput { json_value: Value, exp_dir: PathBuf, project_root: PathBuf }`

**Why this is a module:** Both engine and preflight consume resolved config but neither owns it. Currently these functions are split across part3 and part4, which creates the part3 <-> part4 cycle. Extracting them breaks that cycle.

### `preflight` — Validation

**Extracted from:** part4_preflight_policy (check collection and execution)

**Contains:**
- `preflight_experiment()` — public entry point
- `collect_preflight_checks()` — check collection
- Individual check functions: image reachability, grader reachability, container readiness, per-task image scanning, schema validation
- `PreflightReport`, `PreflightCheck`, `PreflightSeverity` types

**Depends on:** `types`, `config`

**Why this is a module:** Preflight is a pure validation pass — it reads resolved config and produces a report. It doesn't execute trials or manage state. Separating it means preflight checks can be tested by constructing config fixtures directly.

### `trial` — Single-trial execution

**Extracted from:** part5_runtime_io + trial execution functions from part3

**Contains:**
- Task boundary parsing: `parse_task_boundary_from_dataset_task()`, `parse_task_boundary_from_trial_input()`
- Workspace materialization: `materialize_workspace_seed()`, `materialize_workspace_files()`
- Mount resolution: `resolve_task_mounts()`
- Container invocation: `append_container_volume_args()`, container launch, IO wiring
- Result collection: stdout/stderr capture, result.json parsing, evidence extraction
- The single-trial executor function that engine calls

**Boundary type:** `fn execute_trial(dispatch: &TrialDispatch, ctx: &TrialContext) -> Result<TrialExecutionResult>`

**Depends on:** `types`, `config` (for resolved runtime profiles)

**Why this is a module:** This is the unit of work. It takes a dispatch ticket + context, does everything needed for one trial, returns a result. It doesn't know about schedules, concurrency, or lifecycle. Testing: mock the container runtime, assert workspace layout, verify mount resolution.

### `engine` — Orchestration

**Extracted from:** part3_engine (schedule loop, worker dispatch, packaging)

**Contains:**
- `run_experiment_with_behavior()` — the main execution entry point
- `execute_schedule_engine_parallel()` — the schedule loop
- `LocalThreadWorkerBackend` — worker thread management
- `DeterministicCommitter` — result ordering and drain
- `BufferedRunSink` — per-worker result buffering
- `build_experiment_package()` — packaging
- `describe_experiment()` — summary generation
- Run directory creation, run control file management

**Depends on:** `types`, `config`, `trial`, `sink`

**Why this is a module:** The engine wires config resolution -> trial execution -> persistence. It manages concurrency and ordering. It's the only module that touches all others, which is correct — it's the coordinator.

### `lifecycle` — Run lifecycle operations

**Extracted from:** part2_lifecycle + entrypoint wrappers from part1

**Contains:**
- `continue_run_with_options()` — resume from failed/paused/interrupted
- `recover_run()` — reset stuck state
- `replay_trial()` — re-execute a specific trial
- `fork_trial()` — branch from a trial with modified bindings
- `pause_run()` — pause active trial
- `kill_run()` — terminate active run
- `resume_trial()` — resume paused trial
- Lease management: `acquire_run_operation_lease()`, engine lease heartbeat
- Run state reconstruction: `load_run_session_state()`, `load_schedule_progress()`

**Depends on:** `types`, `config`, `engine`

**Why this is a module:** Each lifecycle operation is a thin entrypoint that reconstructs state from disk and delegates to the engine. They share lease/state management patterns but don't need to know trial execution internals.

---

## Dead Code Disposition

Each dead-code warning is resolved as part of the migration. No `#[allow(dead_code)]` suppression.

### Delete (orphaned — callers removed, no future use)

| Item | File | Reason |
|------|------|--------|
| `CANONICAL_TRIAL_RESULT_FILENAME` | part1_core:87 | Superseded by direct path construction |
| `has_lineage_for_trial()` | sqlite_store:386 | No callers in runner or CLI |
| `latest_runtime_operation()` | sqlite_store:447 | No callers |
| `row_count()` | sqlite_store:846 | No callers |
| `LOAD_SQL_FILE` | lab-analysis:41 | No callers |
| `RunAnalysisContext.analysis_dir` | lab-analysis:99 | Field never read |
| `build_load_sql_relative()` | lab-analysis:422 | No callers |

### Audit before delete (possibly pre-built for upcoming features)

| Item | File | Action |
|------|------|--------|
| `BenchmarkAdapterConfig.manifest` | part4:1696 | Check if benchmark adapter integration needs this |
| `benchmark_identity_from_manifest()` | part4:1976 | Part of benchmark pipeline — verify against adapter protocol |
| `read_jsonl_records()` | part4:2004 | Generic utility — may be duplicated elsewhere |
| `validate_json_file_against_schema()` | part4:2039 | Schema validation — check if used in preflight |
| `validate_jsonl_against_schema()` | part4:2062 | Same |
| `build_benchmark_summary()` | part4:2098 | Benchmark pipeline scaffolding |
| `synthesize_benchmark_manifest_from_scores()` | part4:2214 | Same |
| `default_benchmark_manifest()` | part4:2266 | Same |
| `process_benchmark_outputs()` | part4:2299 | Same |
| `TaskBoundaryLimits::is_empty()` | part5:14 | Check if limits enforcement uses this |
| `restore_workspace_from_snapshot()` | part5:2060 | Chain state / snapshot recovery — may be needed for lifecycle |
| `chain_root_workspace_dir_name()` | part5:2084 | Same |
| `rel_to_run_dir()` | part5:2088 | Utility — check for duplicates |
| `materialize_trial_result()` | part5:3564 | Materialization pipeline — verify against MaterializationMode |
| `apply_materialization_policy()` | part5:3750 | Same |

---

## Migration Plan

### Phase 1: Extract `types` module

1. Create `types.rs`.
2. Move all shared type declarations, constants, and enums from part1_core.
3. Add `pub(crate)` visibility to internal types, `pub` to external API types.
4. Update `lib.rs` to `mod types;` and keep remaining `include!()` files importing from `types` via `use crate::types::*`.
5. Delete orphaned dead code from the "Delete" table above.

**Verification:** `cargo build` compiles. Warning count drops. All existing tests pass.

### Phase 2: Extract `config` module

1. Create `config.rs`.
2. Move config resolution functions from part3 and part4 into `config.rs`.
3. These functions currently call each other freely — the move is mechanical since they're going to the same module.
4. Functions remaining in part3/part4 now import from `crate::config`.
5. This breaks the part3 <-> part4 cycle.

**Verification:** `cargo build` compiles. Cycle between part3 and part4 is gone.

### Phase 3: Extract `trial` module

1. Create `trial.rs`.
2. Move workspace materialization, mount resolution, container IO, and single-trial execution from part5 (and trial-execution bits from part3).
3. Define `TrialContext` struct bundling the runtime dependencies (container runtime access, filesystem paths, sink reference).
4. Part5's remaining code should be minimal or empty.

**Verification:** `cargo build` compiles. part5 is deleted or empty.

### Phase 4: Extract `preflight` module

1. Create `preflight.rs`.
2. Move preflight check functions from part4.
3. Part4's remaining code should be minimal or empty.

**Verification:** `cargo build` compiles. part4 is deleted or empty.

### Phase 5: Extract `engine` and `lifecycle` modules

1. Create `engine.rs` from remaining part3 (schedule loop, workers, packaging).
2. Create `lifecycle.rs` from part2 + entrypoint wrappers from part1.
3. Part1, part2, part3 are now deleted or empty.

**Verification:** `cargo build` compiles. All `include!()` directives removed from `lib.rs`.

### Phase 6: Split tests

1. Move tests from part6 into `#[cfg(test)] mod tests` blocks in each new module file, or into per-module test files under `tests/`.
2. Each module's tests import only that module's public API.
3. Integration tests that span multiple modules go in `tests/integration.rs`.

**Verification:** `cargo test` passes. No test regressions.

---

## `lib.rs` After Migration

```rust
mod types;
mod config;
mod preflight;
mod trial;
mod engine;
mod lifecycle;
mod sink;
mod persistence;

// Public API consumed by lab-cli
pub use types::{
    BuildResult, ExperimentSummary, ExecutorKind, ForkResult, KillResult,
    MaterializationMode, PauseResult, RecoverResult, ReplayResult, ResumeResult,
    RunBehavior, RunExecutionOptions, RunResult,
};
pub use config::validate_knob_overrides;
pub use engine::{build_experiment_package, describe_experiment, run_experiment_with_options};
pub use lifecycle::{
    continue_run_with_options, fork_trial, kill_run, pause_run,
    recover_run, replay_trial, resume_trial,
};
pub use preflight::{preflight_experiment, PreflightReport};
```

---

## Resolving Circular Dependencies

The current cycles arise from shared utility functions placed in the wrong file. During migration, each cross-file call falls into one of three categories:

### 1. Type reference — moves to `types`
Most "part1 -> part3" and "part1 -> part4" calls are type construction or constant access. These move to `types` and the cycle disappears.

### 2. Shared config logic — moves to `config`
The part3 <-> part4 cycle is entirely config resolution functions (27 calls from part3 -> part4). Moving these to `config` breaks the cycle.

### 3. True upward dependency — invert with a trait or parameter
If a lower module genuinely needs to call a higher module, pass the capability as a parameter or trait object. Example: if `trial` needs to report progress to the engine, accept `&dyn ProgressReporter` rather than importing engine directly.

Any remaining cycles after phases 1-5 indicate a function is in the wrong module. The fix is always to move the function down, never to allow an upward import.

---

## Acceptance Criteria

1. All `include!()` directives removed from `lib.rs`.
2. `cargo build` produces zero warnings (dead code eliminated).
3. All existing tests pass without modification to test logic (only import paths change).
4. No module imports from a module above it in the dependency graph.
5. Public API surface is unchanged — lab-cli compiles without changes beyond import paths.
6. Each module can be tested in isolation by constructing its inputs directly.

---

## Risks and Mitigations

1. **Risk:** Circular dependency surfaces during extraction that requires significant refactoring.
   - Mitigation: Phased approach — each phase is independently compilable. If a cycle appears, move the shared function down before proceeding.

2. **Risk:** Test breakage from visibility changes (functions that were accessible in flat scope become private).
   - Mitigation: Tests that need cross-module access use `pub(crate)`. Integration tests use only the public API.

3. **Risk:** Merge conflicts with concurrent work on the runner.
   - Mitigation: Execute on a dedicated branch. Each phase is a separate commit for clean rebase.

4. **Risk:** Large diff obscures subtle behavior changes.
   - Mitigation: Phases 1-5 are pure moves — no logic changes. Dead code deletion is separate from moves. Behavior changes (trait extraction, parameter threading) are called out explicitly.
