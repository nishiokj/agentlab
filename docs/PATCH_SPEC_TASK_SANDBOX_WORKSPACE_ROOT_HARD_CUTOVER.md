# Patch Spec: Task Sandbox + Workspace Root Hard Cutover

Status: Draft (Hard Cut Required)  
Date: 2026-03-09  
Owner: `lab-runner`, `lab-cli`, `schemas`, benchmark compilers  
Priority: P0 Blocker  
Supersedes:
1. `docs/PER_TASK_IMAGE_PATCH_SPEC.md`
2. `docs/RUNNER_OWNED_WORKSPACE_MATERIALIZATION_SPEC.md` for runtime topology decisions

## 1. Intent

This patch hard-cuts the runner onto one generalized runtime model:

1. `agent_runtime` is isolated from benchmark execution.
2. `task_sandbox` is the only place tools and tests execute.
3. `workspace_root` is the only writable task state inside the sandbox.
4. benchmark/task data never declares sandbox-internal filesystem paths.

This patch exists to delete the current class of failures where implementation details for one benchmark shape leak into the runtime contract for all benchmarks.

The hard rule is:

1. benchmark semantics compile into a generic task boundary
2. runner owns runtime topology
3. agent never runs inside the task sandbox

## 2. Canonical Terms

These terms are mandatory. Existing overlapping names are removed.

1. `agent_runtime`
   The process environment that runs the planner/orchestrator/adapter logic.

2. `task_sandbox`
   The isolated execution environment where shell commands, file reads, file edits, tests, and graders run.

3. `workspace_root`
   The logical writable task filesystem root inside the sandbox. This is the only tree that counts as editable task state and diff source.

4. `workspace_base`
   The immutable source used to hydrate `workspace_root` before a trial begins.

5. `workspace_overlays`
   Inline file writes applied after base hydration.

6. `aux_mounts`
   Read-only extra data mounts that are visible in the sandbox but are not editable task state.

7. `sandbox_profile`
   A runner-owned execution profile that may define compatibility mount aliases, env normalization, and other sandbox topology details without exposing those details in the task boundary.

8. `materialization_plan`
   The resolved runner-owned plan that produces the sandbox-visible `workspace_root` from `workspace_base`, `workspace_overlays`, and `aux_mounts`.

## 3. Non-Negotiable Invariants

1. `agent_runtime` is never launched inside `task_sandbox`.
2. The agent bundle is never mounted into `task_sandbox`.
3. The agent cannot read its own executable, source tree, or bundled dependencies through sandbox file tools.
4. `workspace_root` is the only writable task state that participates in patch/diff evidence.
5. Benchmark and task boundaries must not declare absolute in-sandbox paths such as `/testbed` or `/workspace`.
6. `task_sandbox` may contain more filesystem than `workspace_root`, but only `workspace_root` is agent-editable task state.
7. All benchmark-specific semantics must be compiled into generic runner contracts before execution.
8. The run plane must remain benchmark-agnostic. First-class benchmark behavior belongs in build-time compilers and benchmark adapters, not runner topology parsing.
9. There is no compatibility branch for removed fields or legacy topology concepts.

## 4. Hard Deletions

The following concepts are deleted, not deprecated:

1. `task.workspace`
2. `task.image` as a task payload field
3. `workspace_seed`
4. `workspace_files`
5. `mount_references`
6. `runtime.agent.image`
7. `runtime.agent.image_source`
8. `runtime.agent.artifact`
9. Agent execution by `docker exec ... /opt/agent/...` inside the task sandbox
10. Any contract that makes the agent bundle visible at `/opt/agent` inside the task sandbox
11. Any benchmark/task field that sets runner cwd or mount targets inside the sandbox
12. Any preflight check that treats a prompt-only workspace as sufficient for a patch task

## 5. New Runtime Model

### 5.1 Plane Separation

There are exactly two runtime planes:

1. `agent_runtime`
2. `task_sandbox`

There is exactly one logical writable task root:

1. `workspace_root`

The generalized execution flow is:

1. build compiles benchmark/task inputs into generic sealed boundaries
2. runner resolves `workspace_base`
3. runner hydrates host-side writable workspace
4. runner mounts hydrated workspace into `task_sandbox`
5. runner starts `agent_runtime` outside the sandbox
6. file/shell tools are executed in `task_sandbox` through a runner-owned tool executor boundary
7. diffs, evidence, and grading are scoped to `workspace_root`

### 5.2 Logical Root vs Compatibility Aliases

The agent-facing logical root is always:

1. `/agentlab/workspace`

Runner may additionally mount the same hydrated host workspace at profile-defined compatibility aliases inside the sandbox, for example:

1. `/testbed`

Those aliases are:

1. runner-owned
2. profile-defined
3. invisible to the task boundary contract
4. not user/task-authored absolute paths

This preserves one stable logical contract for agents while still supporting benchmark images that expect a repo at a legacy path.

## 6. Generic Task Boundary v3

The new sealed task boundary schema is:

```json
{
  "schema_version": "task_boundary_v3",
  "task": {
    "id": "TASK001",
    "input": {},
    "benchmark": {}
  },
  "environment": {
    "image": "registry/image:tag"
  },
  "workspace": {
    "mode": "scratch",
    "base": {
      "kind": "empty"
    },
    "overlays": [],
    "aux_mounts": []
  },
  "limits": {}
}
```

Allowed top-level keys are exactly:

1. `schema_version`
2. `task`
3. `environment`
4. `workspace`
5. `limits`

All unknown keys are hard errors.

### 6.1 `environment`

`environment` describes execution environment only.

Allowed fields:

1. `image`

Optional additions may be added later, but only if they describe sandbox environment and not task semantics.

### 6.2 `workspace`

`workspace` is the generic task-state contract.

Allowed fields:

1. `mode`
2. `base`
3. `overlays`
4. `aux_mounts`

#### `workspace.mode`

Allowed values:

1. `scratch`
2. `patch`

Rules:

1. `scratch` means the task starts from `workspace.base` and may be empty.
2. `patch` means the task must start from a real source state suitable for editing and diffing.
3. `patch` tasks must not pass validation with prompt-only overlays and no real base.

#### `workspace.base`

Allowed base kinds:

1. `empty`
2. `dataset_pack`
3. `git_checkout`

Examples:

```json
{ "kind": "empty" }
```

```json
{ "kind": "dataset_pack", "dataset_pack_ref": "sha256:..." }
```

```json
{ "kind": "git_checkout", "repo": "astropy/astropy", "commit": "d16bfe05..." }
```

No base kind may reference an in-image filesystem path.

#### `workspace.overlays`

`workspace.overlays` replaces `workspace_files`.

Each overlay entry may include:

1. `path`
2. `content`
3. `encoding`
4. `executable`

Paths are always relative to logical `workspace_root`.

#### `workspace.aux_mounts`

`workspace.aux_mounts` replaces `mount_references`.

Each aux mount is read-only and not part of diffable task state.

## 7. Runtime Configuration Cutover

Runtime configuration must split agent concerns from sandbox concerns.

### 7.1 New Runtime Shape

```yaml
runtime:
  agent:
    adapter: rex
    bundle: .lab/agents/rex-current.tar.gz
    command:
      - rex
      - run
  sandbox:
    executor: docker
    image_source: per_task
    image: null
    profile: default
    network: none
    root_read_only: true
```

Rules:

1. `runtime.agent` owns only agent-runtime concerns.
2. `runtime.sandbox` owns only task-sandbox concerns.
3. `image_source` and `image` live under `runtime.sandbox`, never under `runtime.agent`.
4. `bundle` lives under `runtime.agent`, never under `runtime.sandbox`.
5. `runtime.agent` may not carry container image fields.
6. `runtime.sandbox` may not carry agent bundle fields.

### 7.2 `sandbox.profile`

`sandbox.profile` selects runner-owned topology details such as:

1. compatibility mount aliases
2. sandbox env normalization
3. test/grader invocation assumptions tied to environment shape

Profiles are selected by build-time configuration or benchmark compiler rules. They are not task boundary path fields.

## 8. First-Class Benchmark Compilation

First-class benchmark integrations must compile benchmark-specific rows into the generic `task_boundary_v3` contract during build/package time.

### 8.1 SWE-bench Lite

SWE-bench Lite must compile to:

1. `environment.image` from the per-task benchmark image
2. `workspace.mode = patch`
3. `workspace.base.kind = git_checkout` or `dataset_pack`
4. `workspace.overlays` containing `ISSUE.md` and any task prompt material
5. `sandbox.profile = swebench_testbed` or equivalent runner-owned profile

SWE-bench Lite must not rely on:

1. `task.workspace`
2. in-image repo paths in the task boundary
3. the agent running inside the task image

### 8.2 Bench v0

Bench v0 must compile to one of:

1. `workspace.mode = scratch`, `workspace.base.kind = empty`
2. `workspace.mode = scratch`, `workspace.base.kind = dataset_pack`
3. `workspace.mode = patch`, `workspace.base.kind = dataset_pack` or `git_checkout`

Bench v0 uses the same runner contract. It does not get a special runtime model.

### 8.3 Non-First-Class Benchmarks

Non-first-class benchmarks work by authoring or generating the same generic boundary:

1. `environment`
2. `workspace`
3. `limits`

They must not extend runner topology with benchmark-specific path fields.

If a non-first-class benchmark cannot express itself using `environment + workspace + limits`, the missing capability must be added as a generalized runner contract, not hacked in as a benchmark-specific field.

## 9. Preflight Hard Gates

Preflight must verify the real runtime contract, not approximations.

Required checks:

1. `agent_runtime` is launched outside the task sandbox.
2. Agent bundle is not mounted into the task sandbox.
3. `workspace_root` is writable.
4. `workspace_root` is hydrated from a valid `workspace.base`.
5. `patch` mode tasks have a non-empty real base and do not rely only on overlays.
6. If `sandbox.profile` defines compatibility aliases, they resolve to the same hydrated workspace as logical `/agentlab/workspace`.
7. Tool executor file operations are scoped to logical `workspace_root`.
8. The sealed package contains no removed fields.

## 10. Concrete Runner Cutover

The following code-level changes are required.

### 10.1 Delete

Delete or hard-disable the following runtime path in runner execution:

1. `resolve_container_workspace(...)`
2. `run_injected_container(...)` as the agent-launch path
3. `build_injected_container_mounted_command(...)`
4. any `append_container_entrypoint(...)` branch that launches the agent inside the sandbox
5. any contract that passes `task_workspace` or equivalent sandbox cwd field from task data

### 10.2 Replace

Replace current mixed requests with two typed requests:

1. `AgentRuntimeRequest`
2. `TaskSandboxRequest`

`TaskBoundaryMaterialization` must stop carrying old topology fields and instead carry typed:

1. `environment`
2. `workspace`

### 10.3 Add

Add runner-owned components:

1. `WorkspaceMaterializer`
2. `TaskSandboxProfileRegistry`
3. `TaskSandboxExecutor`
4. `AgentRuntimeAdapter`
5. `MaterializationPlanRecord`

## 11. Delete Matrix

| Old concept | Status | Replacement |
|---|---|---|
| `task.image` | delete | `environment.image` |
| `task.workspace` | delete | runner-owned `sandbox.profile` compatibility aliases |
| `workspace_seed` | delete | `workspace.base` |
| `workspace_files` | delete | `workspace.overlays` |
| `mount_references` | delete | `workspace.aux_mounts` |
| `runtime.agent.image_source` | delete | `runtime.sandbox.image_source` |
| `runtime.agent.image` | delete | `runtime.sandbox.image` |
| `runtime.agent.artifact` | delete | `runtime.agent.bundle` |
| agent bundle mounted at `/opt/agent` in task sandbox | delete | agent runtime isolated outside sandbox |
| agent cwd derived from benchmark task field | delete | logical `/agentlab/workspace` + optional runner-owned compatibility aliases |

## 12. Acceptance Gates

This cutover is complete only when all are true:

1. No trial execution path launches the agent inside the task sandbox.
2. No task boundary row with `task.workspace` or `task.image` is accepted.
3. No task boundary row with `workspace_seed`, `workspace_files`, or `mount_references` is accepted.
4. A patch-mode SWE-bench Lite task runs with:
   1. agent outside sandbox
   2. logical `/agentlab/workspace`
   3. compatibility alias mount for image expectations
   4. real workspace base hydration
5. A bench v0 scratch task runs without any benchmark-specific runtime branch.
6. Sandbox file tools cannot read the agent bundle.
7. Diff evidence is scoped only to `workspace_root`.

## 13. Required Tests

1. `patch` task with overlays only fails preflight.
2. `scratch` task with `base.kind = empty` passes and starts from an empty workspace root.
3. SWE-bench-style sandbox profile mounts the same workspace at `/agentlab/workspace` and `/testbed`.
4. Agent bundle path is absent from sandbox-visible filesystem.
5. File tools reject reads/writes outside logical `workspace_root`.
6. Bench v0 and SWE-bench Lite both execute through the same `TaskSandboxExecutor` abstraction.

## 14. Compatibility Policy

Hard cutover only.

Allowed migration aid:

1. build-time conversion tooling that rewrites `task_boundary_v2` and old runtime fields into the new shapes

Not allowed:

1. silent fallback
2. run-time aliasing
3. benchmark-specific topology interpretation in the run plane

## 15. Summary

The generalized design is:

1. one agent runtime
2. one task sandbox
3. one logical workspace root
4. one generic workspace contract
5. zero benchmark-specific sandbox paths in task boundaries

Any benchmark integration that cannot fit this model must add a new generalized contract capability or a new runner-owned sandbox profile. It must not add another benchmark-shaped escape hatch.
