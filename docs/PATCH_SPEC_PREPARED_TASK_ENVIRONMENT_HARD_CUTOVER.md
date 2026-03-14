# Patch Spec: Prepared Task Environment Hard Cutover

Status: Draft (Hard Cut Required)  
Date: 2026-03-11  
Owner: `lab-runner`, `lab-cli`, benchmark compilers/exporters, schemas, docs  
Priority: P0 Blocker

Supersedes:
1. `docs/RUNNER_OWNED_WORKSPACE_MATERIALIZATION_SPEC.md`
2. `docs/PATCH_SPEC_TASK_SANDBOX_WORKSPACE_ROOT_HARD_CUTOVER.md`
3. `docs/PATCH_SPEC_TASK_AGENT_RUNTIME_CONTRACT_HARD_CUTOVER.md`
4. Any design that allows run/preflight/replay/fork to rediscover task topology by reparsing raw task JSON or trial input blobs

## 1. Intent

Hard-cut the runner onto one explicit setup boundary:

1. `TaskDeclaration`
2. `PreparedTaskEnvironment`
3. `RunningTrial`
4. `FinishedTrial`

The only runtime handoff is:

1. parse a strict task declaration
2. prepare a complete runner-owned task environment
3. execute against that prepared environment

This patch exists because task setup is currently fragmented across:

1. raw task JSON loading in `config.rs`
2. ad hoc parsing in `io.rs`
3. trial setup sequencing in `lifecycle.rs`
4. replay/fork reconstruction in `runner.rs`
5. preflight probe setup in `io.rs` and `validations.rs`

That fragmentation causes benchmark-specific topology to leak across stages. SWE-bench exposed it through `git_checkout`, but the bug class is general: the runner currently prepares a task environment by composing unrelated helpers that do not share one typed contract.

This patch makes preparation a single runner-owned transaction. There is no fallback path and no backwards compatibility.

## 2. Non-Negotiable Invariants

1. Task declarations are semantic only. They never own runner mount topology, contract path layout, host paths, or compatibility aliases.
2. `PreparedTaskEnvironment` is the only object execution consumes.
3. No run-plane code reparses `trial_input` to rediscover `environment`, `workspace`, `dependencies`, or `limits`.
4. No stage after preparation may depend on source-specific transport details such as git alternates, promisor config, dataset-pack internals, or exporter-only conventions.
5. Workspace preparation must hand off a self-contained filesystem tree, not a cache representation.
6. Replay, fork, and preflight use the same preparation contract as run. They do not maintain side-channel setup logic.
7. Compatibility aliases such as `/testbed` are runner-owned sandbox-profile behavior only. They are not benchmark-authored task fields.
8. Removed schemas, fields, fixtures, and code paths fail hard. There are no warning-only shims.

## 3. Public Model

## 3.1 Trial State Machine

Publicly, the runner reasons about a task in exactly four states:

1. `Declared`
2. `Prepared`
3. `Running`
4. `Finished`

Private source-specific substeps are allowed only inside the transition from `Declared` to `Prepared`.

There is no public runner state named "executable in its own topology". That is an internal concern of a workspace source adapter, not of the run plane.

## 3.2 `TaskDeclaration`

`TaskDeclaration` is the strict task row shape stored in sealed packages and loaded by run/preflight/describe.

It owns exactly:

1. `task`
2. `environment`
3. `workspace`
4. `dependencies`
5. `limits`

It does not own:

1. contract mount roots
2. sandbox working directory selection
3. runner control-plane files
4. compatibility aliases such as `/testbed`
5. host-side staging paths
6. agent runtime placement

The package boundary must contain typed `TaskDeclaration` rows only. Raw `serde_json::Value` task rows are not acceptable after this cutover.

## 3.3 `PreparedTaskEnvironment`

`PreparedTaskEnvironment` is the only object the execution plane consumes.

It includes:

1. the typed `TaskDeclaration`
2. prepared host roots for `in`, `out`, `state`, `deps`, and `workspace`
3. a fully materialized workspace root
4. resolved auxiliary mounts
5. canonical task/bindings/dependencies/policy files
6. canonical `AGENTLAB_*` env
7. resolved task-sandbox image and sandbox profile
8. resolved agent-runtime launch plan
9. preparation provenance and digests
10. a sealed `prepared_task_environment_v1` manifest persisted in the trial directory

Execution reads this object and does not perform setup discovery.

## 3.4 Benchmark Generality

This model is intentionally benchmark-agnostic:

1. Harbor remains a boundary plugin on the dataset seam and scoring seam.
2. `bench_v0` remains runner-owned built-in benchmark resolution.
3. SWE-bench remains a `git_checkout`-backed workspace source with a runner-owned sandbox profile.

The runner sees one preparation contract. Benchmarks vary only in how they produce a `TaskDeclaration`.

## 4. Current Code Problems

## 4.1 Raw JSON task rows survive too long

Current state:

1. `config::load_tasks()` returns `Vec<Value>`
2. `validations.rs` reparses representative rows
3. `lifecycle.rs` reparses per trial
4. `runner.rs` reparses from stored `trial_input` for replay/fork

This keeps task setup weakly typed and spreads validation across the run plane.

## 4.2 Preparation is not a single contract

Current trial setup is assembled manually in multiple places from:

1. `materialize_task_dependencies_for_trial`
2. `stage_dependencies_for_trial`
3. `materialize_workspace_base`
4. `restore_workspace_from_object_ref`
5. `materialize_workspace_overlays`
6. `resolve_workspace_aux_mounts`
7. `prepare_io_paths`
8. `build_runtime_contract_env`

The same sequence is duplicated in:

1. `lifecycle.rs`
2. `runner.rs` replay
3. `runner.rs` fork
4. `io.rs` preflight probe construction

That duplication is the architecture bug.

## 4.3 Workspace source adapters leak representation details

Current `git_checkout` preparation in `io.rs`:

1. `ensure_git_checkout_cache(...)`
2. `materialize_workspace_git_checkout(...)`

returns a cache topology that is still coupled to partial-clone/promisor behavior. It does not actually complete the abstraction to "prepared workspace root".

## 4.4 Trial input is incorrectly treated as setup source of truth

Current replay/fork reconstruct task setup by:

1. loading `trial_input`
2. reading `/ext/task_spec`
3. calling `parse_task_boundary_from_trial_input(...)`

That makes an agent-facing input blob double as an environment-authority record. This is the wrong boundary.

## 4.5 Harbor currently has split reality

Authoritative Harbor design and exporter now reject task-owned workspace topology and emit runner-owned fields, but the checked-in Harbor smoke dataset still reflects legacy `task_boundary_v2` shape with `task.image` and `task.workspace`.

That stale dataset must not survive this cutover.

## 5. Target Preparation Pipeline

The runner must prepare task execution in one ordered transaction:

1. Load `TaskDeclaration`
2. Validate declaration
3. Allocate staging roots for trial preparation
4. Materialize workspace base into staging
5. Restore chain/checkpoint state when policy requires it
6. Apply workspace overlays
7. Materialize task dependencies
8. Stage runtime dependencies
9. Resolve aux mounts
10. Write canonical contract IO files
11. Build runtime env
12. Resolve sandbox profile and launch topology
13. Capture preparation provenance and pre-snapshot
14. Persist `prepared_task_environment_v1`
15. Atomically commit staging roots into live trial roots

Only after step 15 does the task become `Prepared`.

If any step before 15 fails:

1. execution never starts
2. no partially prepared environment is considered live
3. the failure is a preparation failure, not a harness failure

## 5.1 Atomicity Requirement

Preparation must be atomic from the runner's perspective.

Implementation rule:

1. prepare under `trial_dir/_prepare/<nonce>/...`
2. validate the complete environment
3. rename staged roots into `trial_dir/in`, `trial_dir/out`, `trial_dir/state`, `trial_dir/deps`, `trial_dir/workspace`, and `trial_dir/runtime/prepared_task_environment.json`

There is no in-place incremental assembly of the live trial roots.

## 5.2 Ordering Rules

Preparation order is driven by root ownership:

1. `workspace` preparation owns source hydration, restore, and overlays
2. `deps` and `state` staging are separate and must never mutate `workspace`
3. contract IO files are written only after all roots are finalized
4. mount resolution happens against finalized staged roots, not half-built directories
5. launch topology is derived from prepared roots, not from declaration fields alone

This makes setup deterministic across benchmarks without inventing benchmark-specific flows.

## 6. Workspace Source Boundary

Replace source-specific setup helpers with explicit workspace source adapters:

1. `EmptyWorkspaceSource`
2. `DatasetPackWorkspaceSource`
3. `GitCheckoutWorkspaceSource`

Each adapter implements one contract:

1. input: `WorkspaceBaseSpec`
2. output: `PreparedWorkspaceBase`

`PreparedWorkspaceBase` contains:

1. a self-contained directory tree path under preparation staging
2. provenance metadata
3. source digest / identity

It does not contain:

1. cache repo paths
2. alternates references
3. promisor assumptions
4. mutable handles to source caches

This is the line that fixes the SWE-bench class of bug without overfitting to SWE-bench.

## 7. Integration Plan

## 7.1 Build and Package

`build_experiment_package(...)` remains the authoring-to-package boundary, but its task-row handling changes hard:

1. resolve/export tasks
2. validate every row against strict `task_declaration_v1`
3. reject legacy row shapes immediately
4. write only strict task rows into `tasks/tasks.jsonl`

The build step must stop copying opaque dataset rows into packages.

Required changes:

1. replace `config::load_tasks()`-style raw JSON loading for package checks with typed declaration validation
2. reject legacy Harbor datasets and any `task_boundary_v2` artifact at build time
3. regenerate built-in benchmark task datasets to the strict declaration shape

## 7.2 Run

`run_experiment_with_behavior(...)` changes from "parse and assemble trial state inline" to:

1. load typed declarations
2. schedule declarations plus variants
3. call `prepare_task_environment(...)` per dispatch
4. execute from the resulting `PreparedTaskEnvironment`

`lifecycle.rs` no longer manually sequences workspace/dependency/IO preparation.

## 7.3 Preflight

Preflight must use the same preparation codepath.

Required changes:

1. replace `select_preflight_probe_task(...)` and `build_preflight_probe_context(...)`
2. preflight builds a real `PreparedTaskEnvironment` for the representative task(s)
3. contract smoke executes against that prepared environment

This makes preflight truthful. Any preparation failure becomes visible before run.

## 7.4 Describe

`describe_experiment(...)` and summary paths should load typed declarations, not raw JSON rows.

Describe does not need full preparation, but it must report strict parse failures with row numbers and task IDs rather than allowing untyped tasks to survive until run.

## 7.5 Replay and Fork

Replay and fork must stop reparsing `trial_input` as task-boundary source of truth.

Hard cut:

1. persist `prepared_task_environment_v1` per trial
2. replay loads that manifest plus the recorded declaration digest and runtime identity
3. fork loads that manifest plus checkpoint lineage
4. if the required prepared-environment manifest or workspace provenance is missing, replay/fork fail hard

Delete the current fallback behavior:

1. no `input_only` fallback mode
2. no reconstruction from `/ext/task_spec`
3. no best-effort recreation from mutated trial input blobs

## 7.6 Built-in Benchmarks

Built-in benchmark resolution in `runner.rs` remains, but it must resolve into strict declarations only.

`bench_v0`:

1. remains `dataset_pack`-backed
2. may stage grader support under `/agentlab/deps`
3. may use per-task images

SWE-bench:

1. remains `git_checkout`-backed
2. may use a sandbox profile that mounts `/agentlab/workspace` at `/testbed`
3. may stage grader support under `/agentlab/deps`

Neither benchmark gets a private run-plane topology path.

## 7.7 Harbor

Harbor remains outside runner core.

Hard rules:

1. authoritative Harbor path is exporter -> strict `TaskDeclaration` rows -> runner -> Harbor adapter
2. no runner ingestion of Harbor task directories
3. no Harbor-specific topology code in runner core
4. stale checked-in `task_boundary_v2` Harbor smoke dataset is deleted or regenerated before cutover lands

## 8. Exact Code to Delete or Replace

The following code must not survive this cutover in its current role.

## 8.1 Replace with typed declaration loading

1. `config.rs`: `load_tasks(...)`
2. `io.rs`: `parse_task_boundary_from_dataset_task(...)`
3. `io.rs`: `parse_task_boundary_from_trial_input(...)`
4. `io.rs`: `parse_task_boundary_ext(...)`
5. `io.rs`: `validate_task_boundary_workspace_materialization(...)`

Replacement:

1. strict row validation + deserialization into `TaskDeclaration`
2. replay/fork consume `PreparedTaskEnvironment` manifests, not trial-input task parsing

## 8.2 Replace with one preparation entrypoint

Delete all open-coded preparation sequences in:

1. `lifecycle.rs`
2. `runner.rs` replay
3. `runner.rs` fork
4. `io.rs` preflight probe construction

Replacement:

1. `prepare_task_environment(...)`

## 8.3 Delete source-representation leakage

Delete or replace:

1. `io.rs`: `ensure_git_checkout_cache(...)` returning a repo path as the preparation handoff
2. `io.rs`: `materialize_workspace_git_checkout(...)` as the primary source-boundary abstraction

Replacement:

1. workspace source adapter returning `PreparedWorkspaceBase`

## 8.4 Delete trial-input authority for setup

Delete or replace:

1. `io.rs`: `task_boundary_ext_value(...)`
2. `io.rs`: insertion of `/ext/task_spec` inside `build_agent_task(...)`
3. replay/fork code that reads `/ext/task_spec` from `trial_input`

`trial_input` remains an agent-facing input artifact only. It is not a setup manifest.

## 8.5 Delete dead or misleading setup helpers

Delete:

1. `io.rs`: `stage_workspace_inputs_for_trial(...)`

This function is empty and encodes the wrong idea: setup should not be an open-ended bag of late mutations.

## 8.6 Delete fallback fork behavior

Delete:

1. fork manifest `fallback_mode: input_only`
2. non-strict checkpoint fallback that reverts to raw input-only reconstruction

## 8.7 Delete stale Harbor data and legacy topology artifacts

Delete or regenerate:

1. `.lab/experiments/data/terminal_bench2_harbor_smoke.task_boundary_v2.jsonl`
2. docs and fixtures that still bless `task.image`, `task.workspace`, `workspace_files`, or `mount_references`

## 9. Concrete Module Shape

The current `io.rs` and run-plane setup logic should be split logically as follows:

1. `task_decl.rs`
   strict declaration schema, parse, validation
2. `workspace_source.rs`
   `empty`, `dataset_pack`, `git_checkout`
3. `prepare.rs`
   `prepare_task_environment(...)`, staging, atomic commit, provenance
4. `contract_io.rs`
   canonical task/bindings/dependencies/policy file emission and runtime env generation
5. `launch.rs`
   container launch topology derived from `PreparedTaskEnvironment`

This is not an optional cleanup. It is how the compiler enforces the new boundary.

## 10. Schema and Artifact Changes

## 10.1 New strict task row schema

Introduce:

1. `task_declaration_v1.jsonschema`

Hard rules:

1. closed-world schema
2. required version field
3. no legacy aliases
4. no `task.image`
5. no `task.workspace`
6. no `workspace_files`
7. no `mount_references`

## 10.2 New prepared manifest

Introduce:

1. `prepared_task_environment_v1.jsonschema`

Persist per trial:

1. declaration digest
2. workspace source provenance
3. workspace snapshot manifest
4. resolved mounts
5. canonical contract file paths
6. task sandbox image and profile
7. agent runtime identity
8. launch topology digest

## 10.3 Trial artifact authority

After cutover:

1. `prepared_task_environment.json` is the authoritative setup record
2. `trial_input.json` is an agent-facing execution artifact
3. replay/fork authority comes from prepared-environment manifests and lineage refs, not from `trial_input`

## 11. Second-Order Effects

## 11.1 Truthful preflight becomes more expensive

Preflight will do real preparation work. That is acceptable and required.

Expected effect:

1. slower preflight for `git_checkout` and heavyweight dataset-pack tasks
2. more real blocker detection before run

This is a feature, not regression.

## 11.2 Replay/Fork get stricter

Historical runs without prepared-environment manifests will no longer be replayable/forkable through the new path.

Hard rule:

1. no compatibility bridge for old runs

## 11.3 Exporters and built-in datasets fail faster

Harbor exporters, built-in benchmark compilers, and any checked-in task datasets that emit legacy task shapes will fail build/package validation immediately.

## 11.4 Agent input shrinks conceptually

Removing `/ext/task_spec` from `trial_input` means any code that treated agent input as a source of sandbox truth will break.

That break is intentional.

## 11.5 Evidence model becomes cleaner

Preparation provenance moves out of inferred trial-input state and into a first-class prepared-environment manifest. This should simplify:

1. evidence attribution
2. workspace source debugging
3. variant parity checks
4. postmortem analysis

## 11.6 Benchmark profile handling becomes explicit

Profile-specific aliases such as SWE-bench `/testbed` must be derived from sandbox profile logic only. This will flush any remaining hidden dependence on task-authored workspace paths.

## 12. Test Plan

## 12.1 Unit

1. strict `TaskDeclaration` schema parse/validation
2. each workspace source adapter produces a self-contained prepared base
3. `prepare_task_environment(...)` produces identical manifests for identical inputs
4. `PreparedTaskEnvironment` launch topology is derived without reading raw task JSON
5. `git_checkout` preparation does not rely on partial-clone cache topology after handoff

## 12.2 Integration

1. build rejects legacy Harbor and legacy task-boundary shapes
2. preflight uses real preparation and fails on preparation blockers
3. run uses only prepared environments
4. replay fails cleanly if prepared-environment manifest is absent
5. fork fails cleanly if checkpoint lineage or prepared-environment manifest is absent

## 12.3 E2E

1. real `bench_v0` declaration -> build -> preflight -> run
2. real `swebench_lite_curated` declaration -> build -> preflight -> run
3. Harbor exporter -> strict declaration dataset -> build -> preflight -> run

All three must pass through the same preparation entrypoint.

## 13. Acceptance Criteria

1. No run-plane entrypoint loads task rows as raw `Value` and then reparses them ad hoc.
2. No run-plane code reconstructs environment topology from `trial_input`.
3. Workspace source adapters hand off self-contained prepared roots only.
4. `prepare_task_environment(...)` is the only task-setup entrypoint used by run, preflight, replay, and fork.
5. Harbor, `bench_v0`, and SWE-bench all execute through the same prepared-environment boundary.
6. Legacy Harbor smoke data and legacy task-boundary artifacts are removed.
7. There is no compatibility mode, fallback parser, or legacy replay/fork path.
