# Patch Spec: Task, Agent Runtime, and Trial Contract Hard Cutover

Status: Draft  
Date: 2026-03-10  
Priority: P0 Blocker  
Owners:
1. `rust/crates/lab-runner`
2. `rust/crates/lab-cli`
3. `schemas`
4. `docs`

Supersedes:
1. Any design or code path that treats the agent runtime as a mixed task/runtime/sandbox object
2. Any design or code path that allows multiple runtime execution shapes for scientific runs

## 1. Intent

Hard-cut the runner onto a boundary-first model with exactly four concepts:

1. `TaskSpec`
2. `AgentRuntimeSpec`
3. `TrialContract`
4. `AgentSession`

This patch exists because the current system still mixes:

1. task-owned content
2. runtime-owned executable concerns
3. runner-owned contract topology
4. obsolete dual-path execution flags

The result is harder to reason about than it should be. The same logical run still carries dead switches such as `use_container` and `container_mode`, runtime-owned workspace mutation channels, and command synthesis that duplicates the contract.

## 2. Non-Negotiable Invariants

These are hard gates, not guidance:

1. There is exactly one scientific execution shape: `agent_runtime container + task_sandbox container + shared /agentlab contract`.
2. Tasks own logical content only. They do not own mount topology, runner control files, host paths, or agent runtime placement.
3. Agent runtimes own executable concerns only: artifact, image, command, env, user, rootfs/network policy.
4. The runner owns the trial contract completely: fixed paths, fixed env vars, fixed cwd, fixed output conventions.
5. Agent sessions are ephemeral per-trial resources. A runtime spec is not a live session.
6. Commands are literal `argv`. The runner does not synthesize IO flags or maintain alternate launch modes.
7. The source of truth is workspace mutation plus contract outputs, not agent-declared patches.
8. Scientific runs never branch on host vs container execution.
9. Old configs and persisted state are rejected. There is no compatibility shim.

## 3. Current Ambiguity and Dead Paths

### 3.1 Mixed runtime object

`AgentRuntimeConfig` currently mixes:

1. agent executable concerns
2. task sandbox image selection
3. dependency staging
4. workspace patching
5. launch-mode semantics

Current definition:

1. `rust/crates/lab-runner/src/io.rs` `AgentRuntimeConfig`
2. `rust/crates/lab-runner/src/io.rs` `AgentExecutionConfig`
3. `rust/crates/lab-runner/src/io.rs` `AgentRuntimeIoConfig`

This must be split.

### 3.2 Dead execution switches

The code still carries dual-path toggles even though the scientific path is container-only:

1. `use_container` in `rust/crates/lab-runner/src/core.rs`
2. `container_mode` in `VariantRuntimeProfile`, `AdapterRunRequest`, and multiple IO helpers
3. `ScheduleProgress.use_container`

Current dead branches include:

1. `resolve_agent_runtime_command(..., container_mode)` in `rust/crates/lab-runner/src/io.rs`
2. `validate_agent_runtime_command(..., container_mode)` in `rust/crates/lab-runner/src/io.rs`
3. `prepare_io_paths(..., container_mode)` in `rust/crates/lab-runner/src/io.rs`
4. `resolve_agent_runtime_manifest_path(..., container_mode)` in `rust/crates/lab-runner/src/io.rs`
5. `write_state_inventory(..., container_mode)` in `rust/crates/lab-runner/src/io.rs`

These must be deleted or collapsed to the one surviving path.

### 3.3 Runtime-owned mutation/staging channels

The runtime still owns task-shaping behavior that should belong to task materialization:

1. `runtime.agent.workspace_patches`
2. `runtime.dependencies.file_staging`
3. `runtime.dependencies.services`

Current code:

1. `parse_workspace_patches()` in `rust/crates/lab-runner/src/io.rs`
2. `stage_workspace_patches_for_trial()` in `rust/crates/lab-runner/src/io.rs`
3. `parse_dependency_file_staging()` in `rust/crates/lab-runner/src/io.rs`

These are obsolete in the target model.

### 3.4 Redundant command contract

The runner currently both:

1. exports stable contract env vars under `/agentlab/*`
2. synthesizes `runtime.agent.io.input_arg` and `runtime.agent.io.output_arg` into the command line

Current code:

1. contract env assembly in `build_runtime_contract_env()` in `rust/crates/lab-runner/src/io.rs`
2. command synthesis in `append_runtime_io_arg()` and `resolve_runtime_agent_command()` in `rust/crates/lab-runner/src/io.rs`

This is the wrong abstraction. The contract should be primary; generated IO flags should be deleted.

### 3.5 Mixed task-sandbox ownership

Task image selection is still split across task and runtime:

1. task-owned `environment.image`
2. runtime-owned `runtime.sandbox.image_source`
3. runtime-owned `runtime.sandbox.image`

Current code:

1. task parsing in `parse_task_boundary_from_trial_input()` in `rust/crates/lab-runner/src/io.rs`
2. sandbox image selection in `resolve_task_sandbox_image()` in `rust/crates/lab-runner/src/io.rs`

Task image ownership must be singular and obvious.

## 4. Target Public Model

## 4.1 TaskSpec

Replace `task_boundary_v3` with `task_spec_v1`.

`TaskSpec` owns:

1. `task`
2. `environment.image`
3. `workspace`
4. `dependencies`
5. `limits`

`TaskSpec` does not own:

1. host paths
2. reserved mount points
3. agent runtime image
4. runner control files
5. physical mount topology

The `task` object remains opaque benchmark metadata plus prompt/instructions.

## 4.2 AgentRuntimeSpec

Replace the current mixed `AgentRuntimeConfig` with a runtime-only spec.

`AgentRuntimeSpec` owns:

1. `artifact`
2. `artifact_digest`
3. `artifact_resolved_path`
4. `image`
5. `command`
6. `env`
7. `env_from_host`
8. `binding_args`
9. `network`
10. `root_read_only`
11. `user`

`AgentRuntimeSpec` does not own:

1. task sandbox image selection
2. workspace mutation
3. dependency staging
4. launch mode
5. IO arg templates

## 4.3 TrialContract

The contract becomes the only runner-agent ABI:

1. `/agentlab/in`
2. `/agentlab/out`
3. `/agentlab/state`
4. `/agentlab/workspace`
5. `/agentlab/deps`

And the only required env surface:

1. `AGENTLAB_TASK_PATH`
2. `AGENTLAB_BINDINGS_PATH`
3. `AGENTLAB_DEPENDENCIES_PATH`
4. `AGENTLAB_POLICY_PATH`
5. `AGENTLAB_RESULT_PATH`
6. `AGENTLAB_TRAJECTORY_PATH`
7. `AGENTLAB_BENCHMARK_PREDICTION_PATH`
8. `AGENTLAB_BENCHMARK_SCORE_PATH`
9. identity env such as run/trial/task/variant ids

The agent starts in `/agentlab/workspace`.

No runtime-owned alternate ABI survives.

## 4.4 GraderSpec

Replace user-facing `benchmark.adapter` with a minimal `benchmark.grader` contract:

1. `command`

Delete `manifest` until there is a real second benchmark that needs it.

Benchmark-specific metadata flows through:

1. `task.task`
2. contract files in `/agentlab/in`
3. standardized prediction/score record outputs

## 4.5 AgentSession

`AgentSession` remains internal and ephemeral:

1. created per trial
2. runs in the `agent_runtime` plane
3. shares the trial contract filesystem with the task sandbox
4. is never exposed as user config

If session reuse is ever needed, it must be introduced later as a new first-class resource, not hidden inside `AgentRuntimeSpec`.

## 5. Hard Schema Changes

### 5.1 Resolved experiment

Create `resolved_experiment_v0_6.jsonschema`.

Delete from resolved experiment:

1. `runtime.agent.io`
2. `runtime.agent.execution.executor`
3. `runtime.agent.launch_mode`
4. `runtime.agent.workspace_patches`
5. `runtime.dependencies.file_staging`
6. `runtime.dependencies.services`
7. `runtime.sandbox.image_source`
8. `runtime.sandbox.image`
9. `benchmark.adapter.manifest`

Introduce:

1. `runtime.agent_runtime`
2. `policy.task_sandbox`
3. `benchmark.grader`

### 5.2 Task schema

Create `task_spec_v1.jsonschema`.

Delete `task_boundary_v3` as the canonical task shape after the cutover. Old rows must fail validation.

### 5.3 Schedule and persisted runtime state

Delete:

1. `ScheduleProgress.use_container`
2. any persisted `container_mode` shape
3. any persisted launch-mode or executor-choice state

Persist only the surviving single runtime behavior.

## 6. Code Changes Required

### 6.1 Parsing and types

Rewrite:

1. `rust/crates/lab-runner/src/io.rs`
2. `rust/crates/lab-runner/src/types.rs`
3. `rust/crates/lab-runner/src/config.rs`

Hard deletes:

1. `AgentLaunchMode`
2. `AgentRuntimeIoConfig`
3. `AgentExecutionExecutor`
4. `ExecutorKind`
5. `TaskBoundaryPolicy.require_workspace_materialization`

### 6.2 Orchestration

Simplify:

1. `rust/crates/lab-runner/src/core.rs`
2. `rust/crates/lab-runner/src/runner.rs`
3. `rust/crates/lab-runner/src/lifecycle.rs`

Hard deletes:

1. `run_experiment(path, use_container)`
2. `run_experiment_with_options(path, use_container, ...)`
3. any function argument or struct field named `use_container`
4. any function argument or struct field named `container_mode`

### 6.3 Command handling

Delete:

1. `append_runtime_io_arg()`
2. `preview_agent_command()` IO-arg synthesis
3. host/container command resolution branches

Replace with:

1. literal `argv` validation
2. explicit shell usage only when the command itself names a shell
3. contract-only path discovery through env vars

### 6.4 Task materialization

Delete runtime-owned task mutation:

1. `parse_workspace_patches()`
2. `stage_workspace_patches_for_trial()`
3. `parse_dependency_file_staging()`
4. DX translators that emit `workspace_patches` or runtime `file_staging`

Replace with:

1. task-owned workspace seed/overlays
2. task-owned read-only dependencies materialized into `/agentlab/deps`

### 6.5 Build translator

Delete or reject the following authoring keys from the DX build path in `rust/crates/lab-runner/src/runner.rs`:

1. `/agent/workspace_patches`
2. `/agent/io/input`
3. `/agent/io/input_arg`
4. `/agent/io/output`
5. `/agent/io/output_arg`
6. `/agent/config_files`
7. `/agent/default_config`

These keys currently generate resolved-experiment fields that blur the contract boundary.

If authoring conveniences survive later, they must compile into task-owned sealed dependencies or agent-owned immutable artifacts, not runtime staging directives.

## 7. Obsolete Runtime Behaviors to Delete

The following behaviors must not survive the cutover:

1. alternate runtime path resolution based on `container_mode`
2. task-sandbox host-path inventories
3. runtime-side workspace patch injection
4. runtime-side dependency file staging from host paths
5. agent launch modes other than the file contract
6. user-visible executor choice for scientific runs
7. task image resolution through runtime sandbox image switches

Concrete hot spots:

1. `resolve_agent_runtime_command()` in `rust/crates/lab-runner/src/io.rs`
2. `prepare_io_paths()` in `rust/crates/lab-runner/src/io.rs`
3. `resolve_agent_runtime_manifest_path()` in `rust/crates/lab-runner/src/io.rs`
4. `write_state_inventory()` in `rust/crates/lab-runner/src/io.rs`
5. `resolve_variant_runtime_profile()` in `rust/crates/lab-runner/src/io.rs`
6. `run_experiment*()` signatures in `rust/crates/lab-runner/src/core.rs`
7. `ScheduleProgress.use_container` in `rust/crates/lab-runner/src/types.rs`

## 8. Cutover Rules

This is a hard cut, not a migration window.

Rules:

1. old resolved experiments fail validation
2. old task rows fail validation
3. paused runs created before the cutover fail on `continue`
4. `fork` and `replay` reject pre-cutover state
5. docs and CLI examples show only the new shapes

No auto-rewriter is provided in this patch.

## 9. Acceptance Criteria

The patch is complete only when all of the following are true:

1. `rg` over runner code returns zero matches for `use_container`
2. `rg` over runner code returns zero matches for `container_mode`
3. `rg` over runner code returns zero matches for `runtime.agent.io`
4. `rg` over runner code returns zero matches for `runtime.agent.workspace_patches`
5. `rg` over runner code returns zero matches for `runtime.dependencies.file_staging`
6. `rg` over runner code returns zero matches for `runtime.sandbox.image_source`
7. `rg` over runner code returns zero matches for `runtime.sandbox.image`
8. `state_inventory` records only contract paths, never host-path mount layouts
9. `run`, `continue`, `fork`, and `replay` all use the same single runtime model
10. the only surviving scientific behavior is: task-owned content, agent-runtime-owned executable, runner-owned contract

## 10. Explicit Design Decision

The runner will be easier to reason about if it stops pretending that task shape, runtime shape, and mount shape are configurable peers.

After this patch:

1. tasks describe the problem
2. agent runtimes describe the executable
3. the contract describes the ABI
4. the runner decides how to mount and execute them

That is the entire model.
