# Host-Side Path Contract

## Problem

The container boundary enforces a clean write contract — agents can only write to mounted paths (`/agentlab/in`, `/agentlab/out`, `/agentlab/state`), defined as constants in `lab-core`. The bind mount faithfully transfers agent output to the host. On the host side, that contract dissolves.

Host-side paths are constructed ad-hoc via `.join()` calls with string literals scattered across four crates (`lab-runner`, `lab-cli`, `lab-analysis`, `lab-provenance`). These crates agree on the filesystem layout by convention, not by any enforced contract. The layout is the most coupled API in the system — every component depends on it — and the only one without a formal definition.

Additionally, files are mutable after write. There is no enforcement that trial outputs, once written, remain unmodified by subsequent phases.

## Failure Modes

1. **Path drift** — a crate constructs a wrong path (typo, stale string after rename). Both sides compile. Silent failure at runtime.
2. **Post-write mutation** — a bug in analysis modifies trial outputs, or a subsequent phase corrupts a completed trial. No enforcement prevents this.

## Design Context

The filesystem-as-API choice is deliberate. Agents write files. The bind mount maps container writes directly to host storage with zero translation — no serialization layer, no IPC, no sidecar. The write _is_ the persistence. This eliminates a failure-prone seam and is the right trade-off.

What's missing is extending the same rigor from the container side (typed constants, mount topology as enforcement) to the host side.

## Progress

### Done: Container→Host path mapping (typed)

The old `resolve_event_path_for_trial` was a chain of `if let Some(rest) = path.strip_prefix(...)` with hardcoded strings like `"/state"`, `"/out"`, `"/workspace"`. This has been replaced with a typed system:

- `ContractPathRoot` enum — `In`, `State`, `Out`, `Deps`, `Workspace`
- `ContractPathHostRoots` struct — holds the host-side base directory for each root
- `ContractPathMode` enum — `ContainerMount`, `RuntimeIo`, `RuntimeEvents` (controls which roots are valid per context)
- `map_contract_path_to_host()` — resolves any container-absolute path to its host equivalent through the contract constants

This makes the container-to-host path mapping typed and centralized. Adding a new contract root requires updating the enum and the mapping — the compiler enforces completeness via match exhaustiveness.

### Done: Per-variant runtime resolution

`VariantRuntimeProfile` consolidates the previously duplicated "resolve harness → resolve invocation → patch fields" sequence into a single function (`resolve_variant_runtime_profile`). Each variant now gets its own resolved runtime config including harness, executor, container mode, and network mode — with support for `runtime_overrides` per variant.

This eliminated copy-pasted resolution logic from `run_experiment_with_behavior`, `replay_trial`, `fork_trial_inner`, and `pause_run`.

### Done: Resumable runs via `execute_schedule_engine`

The trial loop was extracted into `execute_schedule_engine()` with a `ScheduleEngineMode` (FreshRun / ContinueRun). Both paths share the same execution logic. `ScheduleProgress` tracks slot-by-slot state and is persisted to `schedule_progress.json`. `continue_run` reconstructs from `run_session_state.json` + `schedule_progress.json`.

## Remaining Changes

### 1. Centralize host-side path construction in `lab-core`

**Current state:** The container→host mapping is now typed (`ContractPathRoot`), but the host-side layout above the trial level — `.lab` → `runs` → `{run_id}` → `trials` | `runtime` | `analysis` | `evidence` | `benchmark` | `replays` | `forks` — is still ad-hoc `.join()` calls scattered across four crates (`lab-runner`, `lab-cli`, `lab-analysis`, `lab-provenance`).

Examples still present in the codebase:

```rust
// lab-runner: run_experiment_with_behavior
let run_dir = project_root.join(".lab").join("runs").join(&run_id);
let trials_dir = run_dir.join("trials");
let analysis_dir = run_dir.join("analysis");
let evidence_dir = run_dir.join("evidence");
let evidence_records_path = evidence_dir.join("evidence_records.jsonl");

// lab-runner: continue_run (same pattern duplicated)
let trials_dir = run_dir.join("trials");
let analysis_dir = run_dir.join("analysis");
let evidence_dir = run_dir.join("evidence");

// lab-runner: isolated helpers
fn run_control_path(run_dir: &Path) -> PathBuf { run_dir.join("runtime").join("run_control.json") }
fn run_session_state_path(run_dir: &Path) -> PathBuf { run_dir.join("runtime").join("run_session_state.json") }
fn schedule_progress_path(run_dir: &Path) -> PathBuf { run_dir.join("runtime").join("schedule_progress.json") }

// lab-cli: resolve_run_dir_arg
let from_cwd = cwd.join(".lab").join("runs").join(run);

// lab-analysis: table file constants are local, not shared
const TABLE_TRIALS: &str = "trials.jsonl";
const TABLE_METRICS_LONG: &str = "metrics_long.jsonl";
```

**Change:** Add structs and constructors to `lab-core` for every level of the host hierarchy.

```rust
// ── project level ──

pub const LAB_ROOT_DIR: &str = ".lab";

pub struct ProjectPaths {
    pub root: PathBuf,
    pub lab: PathBuf,               // .lab
    pub runs: PathBuf,              // .lab/runs
    pub knobs_manifest: PathBuf,    // .lab/knobs/manifest.json
    pub knobs_overrides: PathBuf,   // .lab/knobs/overrides.json
    pub dataset_packs: PathBuf,     // .lab/dataset_packs/sha256
}

pub fn project_paths(root: &Path) -> ProjectPaths { ... }

// ── run level ──

pub struct RunPaths {
    pub dir: PathBuf,
    pub trials: PathBuf,                // trials/
    pub runtime: PathBuf,               // runtime/
    pub operation_lock: PathBuf,        // runtime/operation.lock
    pub run_control: PathBuf,           // runtime/run_control.json
    pub run_session_state: PathBuf,     // runtime/run_session_state.json
    pub schedule_progress: PathBuf,     // runtime/schedule_progress.json
    pub analysis: PathBuf,              // analysis/
    pub evidence: PathBuf,              // evidence/
    pub evidence_records: PathBuf,      // evidence/evidence_records.jsonl
    pub task_chain_states: PathBuf,     // evidence/task_chain_states.jsonl
    pub artifacts: PathBuf,             // artifacts/
    pub benchmark: PathBuf,             // benchmark/
    pub benchmark_adapter_manifest: PathBuf, // benchmark/adapter_manifest.json
    pub benchmark_predictions: PathBuf, // benchmark/predictions.jsonl
    pub benchmark_scores: PathBuf,      // benchmark/scores.jsonl
    pub benchmark_summary: PathBuf,     // benchmark/summary.json
    pub forks: PathBuf,                 // forks/
    pub replays: PathBuf,               // replays/
    pub debug_bundles: PathBuf,         // debug_bundles/
    pub resolved_experiment: PathBuf,   // resolved_experiment.json
    pub resolved_digest: PathBuf,       // resolved_experiment.digest
    pub manifest: PathBuf,              // manifest.json
    pub attestation: PathBuf,           // attestation.json
}

pub fn run_paths(project: &ProjectPaths, run_id: &str) -> RunPaths { ... }

// ── trial level ──
// Note: the existing TrialPaths in lab-runner already handles trial internals.
// This struct covers the runner-owned files at the trial root level.

pub struct TrialHostPaths {
    pub dir: PathBuf,
    pub metadata: PathBuf,          // trial_metadata.json
    pub state: PathBuf,             // trial_state.json
    pub result: PathBuf,            // result.json (CANONICAL_TRIAL_RESULT_FILENAME)
    pub input: PathBuf,             // trial_input.json
    pub dataset: PathBuf,           // dataset/
    pub tmp: PathBuf,               // tmp/
}

pub fn trial_host_paths(run: &RunPaths, trial_id: &str) -> TrialHostPaths { ... }

// ── analysis level ──

pub struct AnalysisPaths {
    pub dir: PathBuf,
    pub tables: PathBuf,                    // tables/
    pub trials_table: PathBuf,              // tables/trials.jsonl
    pub metrics_long_table: PathBuf,        // tables/metrics_long.jsonl
    pub bindings_long_table: PathBuf,       // tables/bindings_long.jsonl
    pub event_counts_by_trial: PathBuf,     // tables/event_counts_by_trial.jsonl
    pub event_counts_by_variant: PathBuf,   // tables/event_counts_by_variant.jsonl
    pub variant_summary: PathBuf,           // tables/variant_summary.jsonl
    pub load_sql: PathBuf,                  // tables/load_duckdb.sql
    pub duckdb: PathBuf,                    // agentlab.duckdb
    pub summary: PathBuf,                   // summary.json
    pub comparisons: PathBuf,               // comparisons.json
    pub view_context: PathBuf,              // duckdb_view_context.json
}

pub fn analysis_paths(run: &RunPaths) -> AnalysisPaths { ... }
```

Then replace every contract-owned `.join()` call in `lab-runner`, `lab-cli`, `lab-analysis`, and `lab-provenance` with field access on these structs. This is mostly mechanical — grep for `.join("` in each crate and replace. The isolated helper functions (`run_control_path`, `run_session_state_path`, `schedule_progress_path`) become field access on `RunPaths`.

### 2. Seal finalized directories after each write phase

**Change:** Add a `seal_dir` utility to `lab-core` and call it at phase boundaries in lab-runner.

```rust
pub fn seal_dir(path: &Path) -> Result<()> {
    // Recursively set read-only permissions on all files and directories
}
```

Phase boundaries (called within `execute_schedule_engine` and post-analysis):

| After... | Seal |
|---|---|
| Writing trial inputs | `trial.runtime.in_dir` |
| Trial finalization completes (including `apply_materialization_policy`) | `trial.dir` |
| Run-level analysis and benchmark outputs complete | `analysis.dir`, `benchmark.dir` |

Important ordering constraint: do **not** seal `out/` or `state/` before `apply_materialization_policy`, because non-`Full` materialization modes may delete those directories.

This is a best-effort OS guardrail against accidental mutation, not tamper-proof immutability. Read-only permission should be treated as an additional invariant signal, not the sole completion oracle.

`continue_run` should remain source-of-truth on `schedule_progress.json` / `run_session_state.json` and **not** infer progress by scanning sealed directories. Optional consistency checks can verify that completed slots map to finalized trial directories.

### 3. Project root marker as constant

**Current state:** `resolve_project_root()` in lab-cli and `find_project_root()` in lab-runner both use `".lab"` as a string literal; `find_project_root_from_run_dir()` also assumes the `.lab/runs/{run_id}` topology by fixed parent traversal.

**Change:** Both should reference `LAB_ROOT_DIR` from `lab-core`. The marker string exists in exactly one place. Consider moving `resolve_project_root()` itself into `lab-core` since both crates need it.

## Required Decisions Before Implementation

The migration is blocked until each item below has an explicit decision and owner.

### A) Mount Contract Abstraction

Current runner execution is host-path bind-mount based (`docker run -v host:container`), including task `mount_references` resolved from `.lab/dataset_packs/sha256/*`.

Required decision:

1. Define a mount contract independent of local host paths:
   1. logical mount source (`blob_ref` / `dataset_pack_ref` / `ephemeral`)
   2. destination path (still under contract roots, e.g. `/agentlab/workspace/...`)
   3. mutability (`ro` / `rw`)
2. Keep destination validation (`workspace` subtree restriction) as a contract invariant.
3. Make executor adapters responsible for realizing mount sources in each environment (local docker vs cloud worker).

### B) Replay/Fork Compatibility Under Materialization Policies

`replay` / `fork` currently rely on trial-local artifacts (`trial_input.json`, workspace/dataset presence).

Required decision:

1. Either:
   1. enforce `materialize=full` for runs intended to support replay/fork, or
   2. make replay/fork source-of-truth refs in evidence (rehydrate required files on demand).
2. Define deterministic user-facing errors when required artifacts were intentionally pruned.

### C) Sealing Semantics and Lifecycle State

Read-only permissions are an accidental-mutation guardrail, not tamper-proof immutability.

Required decision:

1. Keep sealing as best-effort integrity guard only.
2. Define explicit phase-state files (or schema fields) as authoritative completion state.
3. Keep strict ordering: finalize writes -> apply materialization policy -> seal finalized directories.

### D) Contract Surface and Ownership

Path ownership is currently split: some contract types/helpers live in `lab-runner`, others in `lab-core`.

Required decision:

1. Move all contract path roots + host path constructors into `lab-core`.
2. Leave crate-local, operation-specific temp paths outside the shared contract.
3. Include `lab-provenance` in migration scope (not just runner/cli/analysis).

### E) Backward Compatibility and Mixed Runs

Existing runs predate parts of this migration (`continue`, schedule progress, sealing, centralized path helpers).

Required decision:

1. Define compatibility policy:
   1. read old runs as-is with fallback path resolution, or
   2. require migration command/tooling.
2. Define behavior for partially migrated runs (old layout + new code) with explicit error surfaces.

### F) Remote Execution Decoupling

Executor location, durable storage, and local materialization are distinct concerns and must remain independent.

Required decision:

1. Treat `executor` as compute placement only.
2. Treat storage sink (`file`, `blob`) as durability plane only.
3. Treat `materialize` as local cache/inspection policy only.
4. Do not encode remote assumptions into path contracts.

### G) Test Matrix as Acceptance Gate

Migration is not complete until contract invariants are covered by tests.

Required minimum matrix:

1. Path constructor parity tests across all contract-owned directories/files.
2. Materialization + sealing ordering tests for all modes.
3. Replay/fork behavior tests for each supported materialization mode (success or explicit deterministic failure).
4. Continue/resume tests proving `schedule_progress` remains source-of-truth independent of directory seal state.
5. Mount validation and mount realization tests (including task mount destination constraints).

## What doesn't change

- The filesystem layout itself
- JSONL as source of truth
- DuckDB as disposable query lens
- Container-side constants (already correct)
- `runner_runtime_host_paths()` (already correct for trial internals)
- `ContractPathRoot` / `map_contract_path_to_host()` (already correct for container→host mapping)
- `VariantRuntimeProfile` (already correct for per-variant resolution)
- `execute_schedule_engine` / `ScheduleProgress` (already correct for resumable runs)
- JSON schema validation via `lab-schemas`

## Scope

More than a small mechanical patch:

- New host-path types/constants/helpers in `lab-core` (`LAB_ROOT_DIR`, project/run/trial/analysis/benchmark paths, optional `resolve_project_root`, `seal_dir`)
- Replacement across `lab-runner`, `lab-cli`, `lab-analysis`, and `lab-provenance`
- Tests updated for path-constructor usage and sealing/materialization ordering
