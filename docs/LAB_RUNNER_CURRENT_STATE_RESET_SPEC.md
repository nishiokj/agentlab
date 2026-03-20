# Lab Runner Current-State Reset Spec

This document replaces `docs/ASYNC_DOCKER_CUTOVER_PROPOSAL.md` as the implementation spec for runner cleanup and module closure.

It is grounded in the live tree as of 2026-03-19. It does not assume that prior D0-D8 stages achieved closure. Historical proposal text may remain in the repo, but it is not a source of truth for sequencing or acceptance.

## Objective

Reach a runner layout where:

- there is no live legacy code path in the compile tree
- there are no broad top-level umbrella files owning mixed concerns
- every hot-path contract has a single module owner
- package, experiment, trial, persistence, and backend boundaries are explicit and reviewable
- recovery, preflight, control, and persistence loaders reject obsolete shapes instead of normalizing them

This reset spec is strict on purpose. "Transitional" only means "scheduled for deletion in a named stage." It does not mean "acceptable as a live owner."

## Boundary Hierarchy

This section is authoritative. File layout exists to serve these boundaries, not the other way around.

### Scope Definitions

- Package scope: everything required to turn authoring input into a sealed package
- Experiment scope: everything required to run, control, recover, describe, replay, or fork a whole experiment starting from a sealed package
- Trial scope: everything required to turn one compiled task contract into a trial plan, materialize one sandbox from that plan plus the backend contract, execute the agent, grade the result, and reconcile the final trial outcome
- Persistence scope: durable state, schemas, row contracts, and storage access
- Backend scope: external runtime drivers such as Docker
- Crate scope: public API assembly only
- Legacy scope: archive only

### Key Terms

- Compiled task contract: the sealed-package task payload consumed by trial code. It is already resolved enough that run-side code does not invent new authoring semantics.
- Trial plan: the pure per-trial execution plan derived from the compiled task contract plus experiment policy. It contains the sandbox contract, execution contract, grading contract, and persistence handoff contract for one trial.
- Sandbox materialization: backend-specific realization of the trial plan into a runnable sandbox. It is limited to explicit runtime contract assets and mounts. It is not a host-worktree sync pipeline.
- Live events: the stream of agent/runtime events emitted while the sandbox is running.
- Final agent result: the terminal ungraded result emitted by trial execution before grading.
- Graded result: the grader-owned outcome produced from the final agent result and the grading contract.

### Boundary Responsibility Map

#### Crate

- API
  - declare modules
  - expose curated public re-exports
  - own the global interrupt flag only

#### Package

- Authoring
  - load authoring YAML
  - apply knob overrides
  - normalize authoring-only forms into the compiled experiment shape
- Compile
  - compile the packaged task dataset
  - stage package-owned runtime assets and files
  - write the sealed package directory
- Sealed
  - verify package integrity
  - resolve package-relative paths
  - load sealed package input for run and preflight
- Resolved
  - parse compiled experiment policy, benchmark, variant, and task inputs
  - build variant-plan and schedule inputs
  - expose read-only compiled contracts to experiment and trial scopes

#### Experiment

- Run
  - create a run directory from a sealed package
  - copy sealed package payload into the run directory
  - derive run-local manifests and runtime state
  - launch trial
  - track in-flight dispatch
  - drain ordered commits
- Resources
  - cap total dispatch concurrency
  - enforce per-variant in-flight limits
  - enforce disk headroom
  - enforce max run-size budget
- Preflight
  - evaluate machine-state-dependent checks against a sealed package
  - aggregate the public preflight report
- Control
  - pause active trials
  - kill active trials
  - resume paused trials
- Commit
  - commit completed trial results in schedule order
  - write schedule progress and journal records
  - manage pending completion state
- Recover
  - reconcile interrupted run state
  - rewind divergent schedule state
  - release abandoned in-flight trials
- Replay
  - replay a prior trial
  - fork from a prior trial or checkpoint
- Describe
  - summarize the sealed package and run plan
- Lease
  - acquire operation leases
  - maintain and adopt engine leases
- State
  - define run-session and schedule-progress contracts
  - load and store run-level persisted state without compatibility rewriting

#### Trial

- Planning
  - parse the compiled task contract
  - compile the task contract into a pure trial plan
  - derive sandbox, execution, grading, and persistence handoff contracts
- Sandbox
  - materialize the sandbox from the trial plan plus backend capabilities
  - stage only explicit runtime contract assets
  - mount only explicit runtime contract paths
- Execution
  - start the task sandbox
  - execute the agent/runtime commands
  - collect live events
  - collect the final agent result
- Grade
  - execute the grader against the final agent result
  - collect the graded result
- Reconcile
  - reconcile the final agent result and graded result
  - build the final `TrialExecutionResult`
  - hand persistence-owned records to experiment and persistence owners
- Preflight
  - stage dynamic contract-smoke inputs
  - execute and validate contract smoke
- State
  - own trial runtime state and trial-state transitions
  - own trial guards and attempt-phase transitions
- Env
  - resolve runtime commands, env, and bindings
  - resolve grading commands and phases
- Contracts
  - define pure trial-side request, result, sandbox, and grading contracts
- Artifacts
  - interpret sandbox output payloads
  - extract candidate artifacts from explicit output contracts
- Artifacts
- Events
  - build metric, event, and variant snapshot rows
- Spec
  - define and parse the compiled task contract

#### Persistence

- Store
  - own sqlite schema and storage tables
  - read and write runtime key-value state
  - read and write durable row inserts
- Journal
  - append and load slot-commit and run-journal records
- Rows
  - define row contracts and row enums
- Files
  - own durable atomic file IO for runner-managed persisted files

#### Backend

- Docker
  - connect to Docker
  - manage container lifecycle, exec, copy, and stream operations

#### Legacy

- Archive
  - hold deleted historical code only
  - never compile
  - never be imported by live code

## Hard Rules

These rules apply to every stage.

1. No live code may import from `rust/crates/lab-runner/legacy/`.
2. No new crate-root umbrella file may be introduced under `rust/crates/lab-runner/src/`.
3. `lib.rs` may contain only module declarations, the global interrupt flag, and curated public re-exports.
4. Cross-module contracts must live with the module that authoritatively creates them. They may not be parked in a shared `model.rs`-style bucket.
5. Package code may not depend on experiment orchestration code.
6. Trial code may not depend on experiment orchestration code.
7. Persistence code may not depend on experiment or trial operation modules.
8. Recovery and state loaders must reject obsolete persisted shapes. They may not fill missing fields, synthesize IDs, rewrite schema versions, or silently coerce old payloads.
9. A file is not considered "removed" if its responsibilities reappear under a different broad host or under a new neutral-sounding helper bucket.
10. Wildcard imports from mixed-owner files are forbidden during the reset. They hide contract movement and blur ownership review.

## Current State Audit

### Archived Legacy

Archived files now live under `rust/crates/lab-runner/legacy/` and are compile-disconnected. That directory is archive-only. Nothing in `src/` may import, include, or copy logic back from it.

### Live Compile Tree

The live compile tree currently contains:

- crate-root files: `config.rs`, `engine.rs`, `lib.rs`, `model.rs`, `preflight.rs`, `runtime.rs`
- domain directories: `backend/`, `experiment/`, `package/`, `persistence/`, `trial/`

That means the codebase is currently a mixed model: domain modules exist, but key contracts and hot-path helpers still live in top-level umbrellas.

### Broad Host Inventory

| Live file | LOC | Current breadth | Required disposition |
| --- | ---: | --- | --- |
| `src/config.rs` | 1169 | Durable file writes, runtime-key JSON loading, path helpers, policy parsing, benchmark parsing, variant resolution, dataset loading, schedule building, JSON pointer mutation, overrides | Split by owner and delete |
| `src/engine.rs` | 480 | Public run entrypoints, `AdapterRunRequest`, trial-state file writes, `TrialStateGuard`, progress logging helpers | Split by owner and delete |
| `src/model.rs` | 940 | Global constants, public API result types, schedule/policy/workspace contracts, grading contracts, `TrialExecutionResult` | Split by owner and delete |
| `src/preflight.rs` | 1505 | Public preflight entrypoints, sealed package resolution, runtime profile resolution, machine-state checks, dataset/grader checks | Split by owner and delete |
| `src/runtime.rs` | 3178 | Workspace materialization, git checkout staging, dependency staging, env/command resolution, image/artifact helpers, preflight smoke, disk/run-size guardrails, generic file helpers | Split by owner and delete |
| `src/experiment/runner.rs` | 3938 | Fresh run, continue, schedule engine, describe, recover, replay, fork, authoring normalization, many local helpers | Split by owner and delete |
| `src/trial/schedule.rs` | 860 | Scheduled-trial request assembly, task preparation, adapter request build, runtime invocation, evidence and finalization | Rename/split by owner and delete |

### Current Dependency Footprint of Umbrella Files

The umbrella files are still active shared owners, not dead leftovers:

- `config.rs` is referenced by 20 source files in `src/`
- `engine.rs` is referenced by 12 source files in `src/`
- `model.rs` is referenced by 28 source files in `src/`
- `preflight.rs` is referenced by 8 source files in `src/`
- `runtime.rs` is referenced by 14 source files in `src/`

### Current Cross-Domain Leaks That Must Be Closed

- `package/authoring.rs` imports `experiment::runner::normalize_experiment_authoring`
- `package/compile.rs` imports top-level `preflight` and `runtime`
- `trial/schedule.rs` imports `config::*`, `engine::{AdapterRunRequest, TrialStateGuard, write_trial_state}`, `model::*`, and `runtime::*`
- `trial/execution.rs` imports `engine::AdapterRunRequest`
- `experiment/state.rs` still normalizes persisted schedule progress and synthesizes legacy slot commit IDs
- `persistence/store.rs` imports `experiment::state::PendingTrialCompletionRecord`

Those are not stylistic issues. They are direct evidence that module ownership is still unresolved.

## Current Live Flow

This section describes the actual current run path, not the intended path.

### Build and Package Flow

1. `package::authoring::load_authoring_input_for_build` loads authoring YAML, applies overrides, and calls `experiment::runner::normalize_experiment_authoring`.
2. `package::compile::build_experiment_package` validates the resolved experiment, stages runtime assets, loads task rows, and writes the sealed package.
3. `package::sealed::load_sealed_package_for_run` verifies the sealed package and returns `LoadedExperimentInput`.

Current package artifacts written today are:

- `tasks/tasks.jsonl`
- `agent_builds/`
- `files/`
- `runtime_assets/`
- `staging_manifest.json`
- `resolved_experiment.json`
- `checksums.json`
- `package.lock`
- `manifest.json`

Current problem: build/package flow still depends on top-level `config.rs`, `runtime.rs`, `preflight.rs`, and `experiment::runner`, and the compiled experiment payload is currently duplicated in both `resolved_experiment.json` and `manifest.json`.

### Preflight Flow

1. `preflight::preflight_experiment_with_options` loads a sealed package.
2. It resolves tasks, variants, benchmark config, and runtime profiles.
3. It calls `collect_preflight_checks`.
4. Dynamic contract smoke uses helpers in `runtime.rs`, not in a trial-owned preflight module.

Current problem: public preflight, package resolution, runtime-profile resolution, and machine-state checks all live in one top-level file, while probe execution helpers live in a different top-level file.

### Fresh Run and Continue Flow

1. `engine::run_experiment*` forwards into `experiment::runner::run_experiment_with_behavior`.
2. `experiment::runner` creates the run directory, writes run control/session state, starts the engine lease heartbeat, copies packaged assets, resolves variants/tasks/schedule, and enters the schedule engine.
3. `engine::continue_run` forwards into `experiment::runner::continue_run_with_options`.

Current problem: public API, run orchestration, schedule engine, and control-plane state are split between `engine.rs`, `experiment/runner.rs`, `experiment/control.rs`, `experiment/state.rs`, and `experiment/lease.rs`, with hot-path contracts still owned by top-level files.

### Trial Hot Path

1. `experiment::runner::execute_local_trial` creates `ScheduledTrialRequest`.
2. `trial::schedule::prepare_scheduled_trial` prepares the task environment, writes trial state, stages preflight, and bootstraps attempt objects.
3. `trial::schedule::execute_scheduled_trial_attempt` builds `AdapterRunRequest` and calls `trial::execution::execute_trial_runtime`.
4. `trial::execution` materializes containers, runs the agent and grader, snapshots workspace state, and produces `TrialRuntimeOutcome`.
5. `trial::schedule::finalize_scheduled_trial` turns runtime output into deferred rows and artifacts and returns `TrialExecutionResult`.
6. `experiment::commit::DeterministicCommitter` commits results in schedule order.

Current problem: the hot path crosses `experiment/runner.rs`, `trial/schedule.rs`, `engine.rs`, `model.rs`, and `runtime.rs`. That is exactly the boundary ambiguity this reset must remove.

### Control, Recovery, Replay, and Fork

1. `experiment::control` owns run pause, kill, and resume.
2. `experiment::runner` owns recover, replay, fork, resume selector parsing, and authoring normalization.
3. `experiment::lease` owns operation leases and engine lease heartbeat/adoption.

Current problem: `experiment::runner.rs` is not just a run entrypoint file. It is also a recovery, replay, fork, describe, and authoring-normalization bucket.

## Goal State

### Allowed Top-Level Structure

The final live tree under `rust/crates/lab-runner/src/` must contain:

- `lib.rs`
- `backend/`
- `experiment/`
- `package/`
- `persistence/`
- `trial/`

The following live crate-root files must not exist at closure:

- `config.rs`
- `engine.rs`
- `model.rs`
- `preflight.rs`
- `runtime.rs`

### Target File Layout

This layout is subordinate to the boundary map above. If a file name and a boundary conflict, the boundary wins and the file must be renamed or split.

The required target layout is:

```text
src/
  lib.rs
  backend/
    docker.rs
  package/
    authoring.rs
    compile.rs
    resolved.rs
    sealed.rs
    staging.rs
    validate.rs
  experiment/
    commit.rs
    control.rs
    describe.rs
    lease.rs
    preflight.rs
    recover.rs
    replay.rs
    run.rs
    state.rs
  persistence/
    files.rs
    journal.rs
    rows.rs
    store.rs
  trial/
    artifacts.rs
    env.rs
    events.rs
    execution.rs
    grade.rs
    preflight.rs
    prepare.rs
    run.rs
    spec.rs
    state.rs
    workspace.rs
```

Notes:

- `persistence/files.rs` is allowed only as a narrow durable-file IO owner for runner-managed persisted artifacts. It may not become a new generic dump.
- `trial/run.rs` replaces the current mixed responsibilities in `trial/schedule.rs`, `engine.rs`, and `model.rs`.
- `experiment/run.rs` replaces the run-specific subset of `experiment/runner.rs`.

### Dependency Rules

The target layering is:

- `backend/*` may not import `package/*`, `experiment/*`, `trial/*`, or `persistence/*`.
- `persistence/*` may not import `package/*`, `experiment/*`, or `trial/*`.
- `package/*` may depend on `persistence::files` for narrow durable writes, but may not depend on `experiment/*` or `trial/*`.
- `trial/*` may depend on `backend/*`, `persistence/*`, and `package::resolved` / `package::sealed` contracts, but may not depend on `experiment/*`.
- `experiment/*` may depend on `package/*`, `trial/*`, `persistence/*`, and `backend/*`.
- `lib.rs` may not own business logic.

### Contract Ownership Rules

These contracts must move to owned modules:

| Contract cluster | Current owner | Final owner |
| --- | --- | --- |
| Public run entrypoints | `engine.rs` | `experiment::run` with re-export from `lib.rs` |
| Public preflight entrypoints and report types | `preflight.rs`, `model.rs` | `experiment::preflight` with re-export from `lib.rs` |
| Public build result type | `model.rs` | `package::compile` with re-export from `lib.rs` |
| Public describe/recover/replay/fork result types | `model.rs` | their owning `experiment::*` files with re-export from `lib.rs` |
| Run behavior, execution options, run session state, schedule progress | `experiment::state`, `model.rs` | `experiment::state` |
| Run control record and active-trial view | `experiment::control`, `model.rs` | `experiment::control` |
| Lease records and lease timing constants | `experiment::lease`, `model.rs` | `experiment::lease` |
| Policy config, benchmark config, variant plan, task loading, schedule build | `config.rs`, `model.rs` | `package::resolved` |
| Adapter request, scheduled-trial request, trial runtime result, trial execution result | `engine.rs`, `trial/schedule.rs`, `model.rs` | `trial::run` |
| Trial runtime state, trial-state file writer, trial guard | `engine.rs`, `trial::state.rs` | `trial::state` |
| Prepared task environment manifest and runtime IO contract paths | `model.rs`, `trial::prepare.rs` | `trial::prepare` |
| Workspace and task-boundary contracts | `model.rs`, `trial::spec.rs` | `trial::spec` and `trial::workspace` |
| Row structs and sqlite persistence payloads | `model.rs`, `persistence::*` | `persistence::rows` and `persistence::store` |

### File Disposition Rules

Each current broad host has an explicit disposition:

#### `src/config.rs`

Must be fully eliminated.

Move its contents as follows:

- atomic durable file writes -> `persistence::files`
- package path helpers and project-root resolution -> `package::authoring` and `package::sealed`
- resolved experiment parsing and JSON pointer mutation used by build/load -> `package::resolved`
- override and knob application -> `package::authoring` and `package::validate`
- dataset load/count, variant plan resolution, policy parsing, schedule building -> `package::resolved`
- trial-outcome mapping helpers -> `trial::grade`

`config.rs` may not survive under a new name like `utils.rs`, `common.rs`, `helpers.rs`, or `shared.rs`.

#### `src/engine.rs`

Must be fully eliminated.

Move its contents as follows:

- public run entrypoints -> `experiment::run`
- `AdapterRunRequest` -> `trial::run`
- `write_trial_state` and `TrialStateGuard` -> `trial::state`
- run/preflight/slot progress emitters -> owning operation modules (`experiment::run`, `experiment::commit`, `experiment::preflight`)
- workspace bundle env parsing -> `trial::workspace`

#### `src/model.rs`

Must be fully eliminated.

Its contents must be moved to owner modules. No replacement shared model bucket is allowed.

#### `src/preflight.rs`

Must be fully eliminated.

Move its contents as follows:

- public preflight entrypoints and report assembly -> `experiment::preflight`
- dataset and benchmark sanity checks -> `package::resolved` or `package::validate`
- dynamic trial smoke staging/probe execution -> `trial::preflight`

#### `src/runtime.rs`

Must be fully eliminated.

Move its contents as follows:

- workspace base/overlay materialization and git checkout staging -> `trial::workspace`
- dependency and task-bundle staging -> `trial::prepare`
- runtime binding, env, command, and agent runtime resolution -> `trial::env`
- artifact unpack, platform checks, image digest helpers needed for execution -> `trial::execution` or `backend::docker`
- disk/run-size/local-worker capacity guardrails -> `experiment::run`
- preflight probe context/build/execute/validate helpers -> `trial::preflight`
- path-copy/remove helpers -> owning modules or `persistence::files` if they are truly runner-persistence-specific

#### `src/experiment/runner.rs`

Must be fully eliminated.

Move its contents as follows:

- fresh run and continue orchestration -> `experiment::run`
- describe flow -> `experiment::describe`
- recover flow -> `experiment::recover`
- replay, fork, selector parsing, checkpoint resolution -> `experiment::replay`
- authoring normalization -> `package::authoring`

#### `src/trial/schedule.rs`

Must be renamed and split into `trial::run`.

The current file name is misleading. It does not own scheduling policy; it owns scheduled-trial assembly and finalization. That ownership must become explicit.

### Compiled YAML to Run Boundary

This boundary is mandatory.

#### Authoring-side input

Only the build path may accept:

- authoring YAML
- knob overrides
- authoring-time normalization
- authoring-relative host paths

That logic belongs to `package::authoring`, `package::validate`, `package::resolved`, and `package::compile`.

#### Sealed package output

The build path must emit a sealed package directory containing:

- `resolved_experiment.json` as the only authoritative compiled experiment payload
- `tasks/tasks.jsonl` as the compiled task dataset used by run and preflight
- `staging_manifest.json` as the runtime path staging catalog
- packaged assets under `agent_builds/`, `files/`, and `runtime_assets/`
- integrity metadata in `checksums.json`, `package.lock`, and `manifest.json`

`manifest.json` is package metadata and integrity metadata only. It must not duplicate or reinterpret the compiled experiment payload.

#### Run-side input

Run, continue, describe, preflight, recover, replay, and fork may accept only:

- a sealed package directory
- a sealed package `manifest.json`
- a run directory created from a sealed package

They may not accept authoring YAML or apply authoring-time transforms.

#### Forbidden run-side behavior

The run path may not:

- re-read authoring YAML
- apply knob overrides
- call authoring normalization
- resolve host-relative authoring paths
- rebuild the compiled experiment from source authoring input

The run path may only:

- verify the sealed package
- copy the sealed package payload into the run directory
- derive run-local state such as `resolved_variants.json`, `resolved_schedule.json`, run control, session state, lease state, and sqlite rows

## Goal-State Flow

### Build Flow

1. `package::authoring` loads authoring input and applies overrides.
2. `package::authoring` performs authoring normalization.
3. `package::validate` validates authoring and resolved package invariants.
4. `package::resolved` resolves tasks, variants, policies, and schedule inputs.
5. `package::staging` stages package-owned runtime assets.
6. `package::compile` writes the sealed package.

### Preflight Flow

1. `package::sealed` loads and verifies a sealed package.
2. `package::resolved` resolves tasks, variants, policies, and runtime inputs from the sealed package.
3. `experiment::preflight` composes machine-state-dependent checks and returns `PreflightReport`.
4. `trial::preflight` owns dynamic contract smoke staging and execution helpers used by preflight.

### Run Flow

1. `experiment::run` owns the public run entrypoints.
2. `experiment::run` creates run directory state, run/session records, and the engine lease.
3. `package::resolved` supplies resolved tasks, variants, policies, and schedule.
4. `experiment::run` owns the schedule engine and concurrency/headroom enforcement.
5. `trial::run` owns single-trial prepare/attempt/finalize.
6. `experiment::commit` owns deterministic ordered slot commit.
7. `persistence::*` owns durable sqlite and row storage.

### Control and Recovery Flow

1. `experiment::control` owns pause, kill, and resume.
2. `experiment::recover` owns recovery and schedule rewind.
3. `experiment::replay` owns replay and fork.
4. `experiment::lease` owns operation leases and engine lease heartbeat/adoption.

## Non-Goals

The reset does not attempt to preserve the following:

- crate-root umbrella files as "temporary compatibility" owners
- silent migration of old runtime state
- package build depending on experiment orchestration helpers
- a neutral shared type bucket replacing `model.rs`
- a neutral shared runtime helper bucket replacing `runtime.rs`

## Reset Stages

Stage names intentionally do not reuse D0-D8.

### R0 - Freeze the Boundary Rules

Deliverables:

- ratify this spec as the live source of truth
- treat `docs/ASYNC_DOCKER_CUTOVER_PROPOSAL.md` as historical only
- forbid new imports from `config.rs`, `engine.rs`, `model.rs`, `preflight.rs`, `runtime.rs`

Done when:

- no new call sites are added against the broad hosts
- every follow-on PR cites the target owner from this spec for moved code

### R1 - Package Boundary Extraction

Deliverables:

- create `package/resolved.rs`
- move package-owned parse/load/resolve helpers out of `config.rs`
- move `normalize_experiment_authoring` out of `experiment::runner` into `package::authoring`
- make `resolved_experiment.json` the sole compiled experiment payload and reduce `manifest.json` to package metadata plus integrity metadata
- remove package imports of `experiment::*`, top-level `preflight`, and top-level `runtime`

Done when:

- `package/*` imports only `package/*`, `persistence::files`, and external crates
- package build does not import `experiment::runner`
- package build does not import crate-root umbrella files
- run/preflight/describe accept sealed package input only and never authoring YAML

### R2 - Trial Hot-Path Contract Extraction

Deliverables:

- create `trial/run.rs`
- move `AdapterRunRequest`, `ScheduledTrialRequest`, `PreparedScheduledTrial`, `TrialRuntimeOutcome`, and `TrialExecutionResult` into trial-owned files
- move `write_trial_state` and `TrialStateGuard` into `trial::state`
- move remaining trial-result constants out of `model.rs` into trial owners

Done when:

- no `trial/*` file imports `engine.rs`
- no hot-path contract type is defined in `model.rs`
- `trial/schedule.rs` is deleted

### R3 - Runtime Umbrella Breakup

Deliverables:

- move workspace materialization helpers into `trial::workspace`
- move task/dependency staging into `trial::prepare`
- move runtime binding/env/command resolution into `trial::env`
- move preflight probe helpers into `trial::preflight`
- move run-capacity and disk-budget helpers into `experiment::run`

Done when:

- no `crate::runtime` imports remain
- `runtime.rs` is deleted

### R4 - Preflight Split

Deliverables:

- create `experiment/preflight.rs`
- move public preflight entrypoints and report types there
- keep only per-trial smoke staging/execution helpers in `trial::preflight`
- move dataset/task/grader static sanity checks into package-owned modules

Done when:

- no `crate::preflight` imports remain
- `src/preflight.rs` is deleted

### R5 - Experiment Operation Split

Deliverables:

- create `experiment/run.rs`, `experiment/describe.rs`, `experiment/recover.rs`, and `experiment/replay.rs`
- move run orchestration, describe, recover, replay, fork, and selector/checkpoint helpers out of `experiment/runner.rs`
- keep `experiment/commit.rs`, `experiment/control.rs`, `experiment/lease.rs`, and `experiment/state.rs` narrow

Done when:

- `experiment/runner.rs` is deleted
- `package/*` no longer imports experiment modules
- each experiment file has a single operation family

### R6 - Shared Model Elimination

Deliverables:

- move every remaining type and constant out of `model.rs`
- update imports to owner modules
- re-export only public API types from `lib.rs`

Done when:

- no `crate::model` imports remain
- `model.rs` is deleted

### R7 - Persistence and State Hardening

Deliverables:

- remove persistence imports of experiment/trial operation modules
- move pending-completion record ownership into `persistence::store` or another persistence-owned file
- remove compatibility normalization from `experiment::state`
- make loaders reject obsolete schema versions or missing required fields

Done when:

- `persistence/*` imports no `experiment/*` or `trial/*`
- `legacy_slot_commit_id` and `normalize_schedule_progress` are gone
- persisted-state loaders are reject-only for obsolete shapes

### R8 - Crate Root and Legacy Closure

Deliverables:

- thin `lib.rs` to declarations and re-exports only
- delete `config.rs`, `engine.rs`, `model.rs`, `preflight.rs`, `runtime.rs`
- keep `legacy/` archive-only and compile-disconnected
- document final owner map in `ARCHITECTURE.md`

Done when:

- the only crate-root live Rust file is `lib.rs`
- there are no imports from `legacy/`
- closure checks below pass

## Closure Checks

The reset is complete only when all of the following are true:

1. `rg -n "use crate::config|crate::config::|config::" rust/crates/lab-runner/src --glob '!tests.rs'` returns no results.
2. `rg -n "use crate::engine|crate::engine::|engine::" rust/crates/lab-runner/src --glob '!tests.rs'` returns no results.
3. `rg -n "use crate::model|crate::model::|model::" rust/crates/lab-runner/src --glob '!tests.rs'` returns no results.
4. `rg -n "use crate::preflight|crate::preflight::|preflight::" rust/crates/lab-runner/src --glob '!tests.rs'` returns no results.
5. `rg -n "use crate::runtime|crate::runtime::|runtime::" rust/crates/lab-runner/src --glob '!tests.rs'` returns no results.
6. `rg --files rust/crates/lab-runner/src | rg '^rust/crates/lab-runner/src/(config|engine|model|preflight|runtime)\\.rs$'` returns no results.
7. `rg -n "legacy_slot_commit_id|normalize_schedule_progress" rust/crates/lab-runner/src` returns no results.
8. `rg -n "normalize_experiment_authoring" rust/crates/lab-runner/src` reports only package-owned locations.
9. `cargo check -p lab-runner` passes.
10. `rg --files rust/crates/lab-runner/src | rg '(^rust/crates/lab-runner/src/experiment/runner\\.rs$|^rust/crates/lab-runner/src/trial/schedule\\.rs$)'` returns no results.
11. The hot path can be traced in order without crossing a broad host:
    `experiment::run -> trial::run -> trial::execution -> experiment::commit -> persistence::*`

## Failure Conditions

The reset fails if any of the following happen:

- a deleted umbrella file is replaced by a new neutral helper bucket
- compatibility normalization remains in live loaders
- package code still depends on experiment orchestration
- trial code still depends on experiment orchestration
- persistence still depends on experiment or trial operation state
- hot-path request/result contracts still live outside their owner modules

That is the standard for closure.
