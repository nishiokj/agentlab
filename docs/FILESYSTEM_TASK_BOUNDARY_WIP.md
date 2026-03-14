# Filesystem Task Boundary Contract (WIP)

Status: work in progress.

This is the secondary benchmark task boundary. The primary target contract is the image-backed task boundary.

This document exists so the non-image path is explicit, bounded, and clearly marked as unsettled instead of being inferred from runner behavior.

## Intent

Use this contract when the benchmark does not supply a task image.

The benchmark may still supply:

- task payload
- a writable workspace seed
- public benchmark assets
- a grader command
- hidden grader-only assets

The benchmark may not supply:

- host paths
- runner-owned mount paths
- absolute container paths

All benchmark paths in this contract are relative to the workspace root.

## Contract Shape

```yaml
schema_version: filesystem_task_boundary_v1
kind: filesystem

task_id: string

benchmark:
  adapter_id: string
  name: string
  split: string

task_payload: object

runtime:
  profile: string
  workdir: string
  env: {string: string}

agent_exec:
  executable: string
  args: [string]
  env: {string: string}

workspace:
  seed:
    mode: artifact | git_checkout | empty
    artifact_ref: string?
    repo: string?
    commit: string?
  overlays:
    - artifact_ref: string
      target_path: string
      executable: bool
  public_assets:
    - artifact_ref: string
      target_path: string
      read_only: bool
  capture:
    mode: none | patch | archive
    root: string

grading:
  enabled: bool
  strategy: relaunch_same_runtime | same_container
  exec:
    executable: string
    args: [string]
    workdir: string
    env: {string: string}
  hidden_assets:
    - artifact_ref: string
      target_path: string
      read_only: bool

toolchain:
  required_commands: [string]
  required_env: [string]

limits:
  trial_seconds: integer?
  grading_seconds: integer?
```

## Field Semantics

### Top-Level Identity

- `schema_version`: resolved boundary schema version. This is not authored experiment input.
- `kind`: fixed discriminator for this boundary type.
- `task_id`: stable per-task identifier.

### Benchmark Identity

- `benchmark.adapter_id`: benchmark integration identifier.
- `benchmark.name`: benchmark family or suite name.
- `benchmark.split`: logical dataset split.
- `task_payload`: opaque task payload consumed by the agent contract and grader contract.

### Runtime

- `runtime.profile`: named runner-managed execution profile.
- `runtime.workdir`: workspace-relative working directory for the agent phase.
- `runtime.env`: benchmark-owned environment variables applied to both agent and grader phases unless overridden more narrowly.

This contract does not let the benchmark dictate container topology directly. It selects a runner-managed runtime profile.

### Agent Execution

- `agent_exec.executable`: executable name or path resolved inside the runtime profile.
- `agent_exec.args`: argv passed to the executable.
- `agent_exec.env`: agent-only environment additions.

The worker is responsible for wiring runner-owned input and output paths into this command. Those runner-owned paths are not part of this contract.

### Workspace

- `workspace.seed.mode=artifact`: start from a sealed artifact reference.
- `workspace.seed.mode=git_checkout`: start from a repo and commit resolved by the runtime profile.
- `workspace.seed.mode=empty`: start from an empty writable workspace.
- `workspace.overlays`: files materialized into the workspace before agent execution.
- `workspace.public_assets`: benchmark assets visible during both agent and grader phases.
- `workspace.capture.mode=patch`: worker computes a diff rooted at `workspace.capture.root`.
- `workspace.capture.mode=archive`: worker archives the rooted subtree.
- `workspace.capture.mode=none`: no workspace capture.

All `target_path` and `root` values are relative to the workspace root.

### Grading

- `grading.enabled`: grading is part of the task boundary.
- `grading.strategy=relaunch_same_runtime`: preferred when grader-only assets must remain hidden from the agent.
- `grading.strategy=same_container`: only valid when there are no grader-only hidden assets.
- `grading.exec`: explicit grader execution contract.
- `grading.hidden_assets`: assets materialized only for the grading phase.

If `grading.enabled=false`, `grading.exec` and `grading.hidden_assets` must be absent.

### Toolchain

- `toolchain.required_commands`: commands that must exist in the selected runtime profile.
- `toolchain.required_env`: env variables the runtime profile must provide.

Preflight should fail if the runtime profile cannot satisfy this toolchain contract.

### Limits

- `limits.trial_seconds`: cap for the agent phase.
- `limits.grading_seconds`: cap for the grading phase.

## Execution Semantics

The worker executes this boundary in two phases:

1. Materialize the writable workspace from `workspace.seed`.
2. Apply `workspace.overlays` and `workspace.public_assets`.
3. Launch the runner-managed runtime profile.
4. Run `agent_exec` with cwd=`runtime.workdir`.
5. If grading is enabled and strategy is `relaunch_same_runtime`, relaunch the same runtime profile with the mutated workspace and add `grading.hidden_assets`.
6. Run `grading.exec`.
7. Capture workspace outputs according to `workspace.capture`.

## Explicit Non-Goals

This contract does not currently settle:

- what `runtime.profile` names are valid
- how git checkout hydration should be cached
- whether `same_container` is worth supporting at all
- whether the grader should always inherit `runtime.workdir` when `grading.exec.workdir` is omitted

Those are intentionally left open. This contract is not ready to hard-cut over.
