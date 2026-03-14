# Patch Spec: Hermetic Agent Runtime Hard Cutover

Status: Draft  
Date: 2026-03-10  
Priority: P0 Blocker  
Owners:
1. `rust/crates/lab-runner`
2. `rust/crates/lab-cli`
3. `schemas`
4. `tests/e2e_cli`

Supersedes:
1. `docs/PATCH_SPEC_TASK_SANDBOX_WORKSPACE_ROOT_HARD_CUTOVER.md` sections that normalize host-launched agents
2. Any doc or code path that treats `local_docker` as scientifically isolated while spawning the agent on the host

## 1. Intent

Hard-cut the scientific run plane onto a model where:

1. the runner may orchestrate on the host
2. the agent never executes on the host
3. the task sandbox never exposes host paths to the agent
4. the only mutable task state is a sealed workspace snapshot

This patch exists because the current model is not scientifically defensible. It allows host-side agent execution, host-path rewrites, and mutable host state to influence trial behavior.

## 2. Current Failure

The current implementation does all of the following in the scientific `run` path:

1. materializes a host scratch workspace under `.lab/runs/<run_id>/.scratch/.../workspace`
2. rewrites `/agentlab/...` contract paths back to host paths before spawn
3. exports `WORKSPACE=<host scratch path>`
4. launches the agent as a host process with `current_dir(host_workspace)`
5. allows experiment configs to pass `--dangerous` through to the agent process

Primary broken functions:

1. `run_external_agent_runtime_trial()` in `rust/crates/lab-runner/src/io.rs`
2. `run_builtin_adapter_local()` in `rust/crates/lab-runner/src/io.rs`
3. `build_builtin_adapter_local_command()` in `rust/crates/lab-runner/src/io.rs`
4. `map_contract_path_to_host()` in `rust/crates/lab-runner/src/runner.rs`

The result is not hermetic. A benchmark run can observe host filesystem state and can change behavior across runs even when the sealed package is unchanged.

## 3. Non-Negotiable Invariants

These are hard gates, not guidance:

1. `run` and `build-run` must never spawn an agent process on the host.
2. `run` and `build-run` must fail if the resolved agent launch path is local-process or host-shell based.
3. `WORKSPACE` visible to the agent must always be a contract path such as `/agentlab/workspace`, never a host path.
4. The agent command argv and env visible to the agent must never contain `.lab/runs/.../.scratch/...` host paths.
5. `--dangerous` is forbidden in scientific runs.
6. `dangerous_mode.set` or equivalent bypass APIs are forbidden in scientific runs.
7. The task workspace snapshot used by the run must be fully attributable from sealed inputs and recorded digests.
8. If a run is not hermetic, the run must fail. It must not complete with a softer isolation grade.
9. `run-dev` is allowed to remain non-hermetic, but its outputs must not be presented as scientific benchmark evidence.

## 4. New Runtime Model

There are exactly three execution planes:

1. `runner_host`
   Trusted orchestrator only. It may materialize inputs, manage containers, persist evidence, and proxy tool requests.
2. `agent_runtime`
   Hermetic container where the agent product executes.
3. `task_sandbox`
   Hermetic container where benchmark shell commands, tests, graders, and sandbox-sensitive file operations execute.

There is exactly one logical writable task root:

1. `/agentlab/workspace`

There is exactly one physical mutable workspace snapshot per trial:

1. a runner-owned sealed workspace directory on the host
2. mounted into both `agent_runtime` and `task_sandbox` at `/agentlab/workspace`
3. never exposed to the agent as a host path

## 5. Hard Contract Changes

### 5.1 Scientific execution modes

The scientific run plane becomes:

1. `run`
2. `build-run`

The non-scientific developer plane becomes:

1. `run-dev`

Rules:

1. `run` and `build-run` require hermetic agent execution.
2. `local_process` is removed from `run` and `build-run`.
3. `local_docker` is retained only if its semantics are changed to "agent_runtime container plus task_sandbox container".
4. `run-dev` may retain local host execution for debugging, but it must write an attestation marking the run non-hermetic and non-comparable.

### 5.2 Runtime shape

`runtime.agent` remains the agent product contract, but gains an explicit execution environment:

```yaml
runtime:
  agent:
    bundle: .lab/agents/rex-current.tar.gz
    command: ["/opt/agent/bin/rex", "run"]
    io:
      input_arg: --input-file
      output_arg: --output
    execution:
      executor: docker
      image: ghcr.io/acme/agent-runtime:sha256-...
      network: none
      root_read_only: true
      user: agentlab
  sandbox:
    executor: docker
    image_source: per_task
    image: null
    profile: swebench_testbed
    network: none
```

Rules:

1. `runtime.agent.execution.executor` is required in scientific runs.
2. The only valid scientific value is `docker`.
3. `runtime.agent.execution.image` is required in scientific runs.
4. `runtime.agent.bundle` is mounted or unpacked into `agent_runtime` at `/opt/agent`.
5. Host-specific bundle names such as `*.host.tar.gz` are forbidden in scientific runs unless their manifest explicitly declares Linux container compatibility.

### 5.3 Provenance additions

The sealed package and run attestation must record:

1. agent runtime image digest
2. task sandbox image digest
3. agent bundle digest
4. workspace base digest
5. workspace overlay digest
6. tool gateway protocol version

## 6. Tool Execution Model

This is the part that must not be hand-waved.

The agent needs somewhere to run, but scientific isolation forbids direct host execution. Therefore the agent runtime must be hermetic and tool effects must be mediated.

### 6.1 Required split

1. The agent product runs inside `agent_runtime`.
2. The runner owns a tool gateway.
3. The tool gateway executes benchmark-sensitive operations in `task_sandbox`.
4. The gateway returns normalized tool responses to the agent.

### 6.2 Required behavior by tool class

1. `Bash`
   Must execute in `task_sandbox`, with cwd `/agentlab/workspace`, never in `runner_host` or `agent_runtime`.
2. `Read`, `Write`, `Edit`, `Glob`, `Grep`
   Preferred final state: execute through the gateway against `/agentlab/workspace` with sandbox-normalized semantics.
3. Tests and graders
   Must execute in `task_sandbox`.

### 6.3 Allowed transitional shortcut

There is exactly one allowed transitional shortcut while the gateway is being cut over:

1. runner-mediated file operations may mutate the sealed workspace snapshot directly on the host

Only if all are true:

1. the agent does not receive the host path
2. the operation is strictly scoped to the sealed workspace snapshot
3. `Bash` is already sandbox-only
4. the run is still marked hermetic only after this shortcut is deleted

This shortcut is not acceptable as the final architecture.

## 7. Required Deletions

Delete from the scientific execution path:

1. `run_builtin_adapter_local()` in `rust/crates/lab-runner/src/io.rs`
2. `build_builtin_adapter_local_command()` in `rust/crates/lab-runner/src/io.rs`
3. any call that sets agent `current_dir(host_workspace)`
4. any call that exports `WORKSPACE=<host path>` to the agent
5. any rewrite of `/agentlab/...` to host paths for agent argv or env
6. any acceptance of `--dangerous` in experiment agent commands
7. any acceptance of scientific `local_process`
8. any attestation path that marks host-launched agents as merely `"bounded"`

Delete or narrow in code:

1. `map_contract_path_to_host()` usage for agent launch argv/env
2. `resolve_trial_io_host_path()` usage for agent launch argv/env
3. the current `isolation_grade` computation in `lifecycle.rs` and `runner.rs`

## 8. New Runner Responsibilities

### 8.1 Trial startup

For each trial:

1. materialize sealed workspace snapshot on the host
2. stage in/out/state/deps directories on the host
3. create `task_sandbox` container
4. create `agent_runtime` container
5. mount identical contract paths into both containers
6. start tool gateway and control plane
7. launch the agent inside `agent_runtime`

### 8.2 Container mounts

Required mounts in `agent_runtime`:

1. `/agentlab/in` read-only
2. `/agentlab/out` writable
3. `/agentlab/state` writable
4. `/agentlab/deps` read-only
5. `/agentlab/workspace` writable
6. `/opt/agent` read-only

Required mounts in `task_sandbox`:

1. `/agentlab/in` read-only
2. `/agentlab/out` writable
3. `/agentlab/state` writable
4. `/agentlab/deps` read-only
5. `/agentlab/workspace` writable
6. optional compatibility alias such as `/testbed`, mapped to the same workspace

### 8.3 Control plane

The runner owns:

1. agent start/stop
2. tool request routing
3. checkpoint capture
4. evidence capture

The agent product does not choose its own privilege model in scientific runs.

## 9. CLI and Validation Changes

### 9.1 `lab-cli`

`lab-cli run` and `lab-cli build-run` must reject:

1. experiments with agent command tokens containing `--dangerous`
2. experiments missing `runtime.agent.execution.image`
3. experiments resolving to host-local agent execution
4. sealed packages built with older host-agent metadata

### 9.2 `preflight`

`preflight` must add hard-fail checks:

1. `agent_runtime_hermetic`
2. `dangerous_mode_forbidden`
3. `workspace_contract_not_host_path`
4. `task_sandbox_bash_plane`
5. `agent_bundle_container_compatible`

### 9.3 Attestation

Replace:

1. `"bounded"`
2. `"leaky"`

With:

1. `"hermetic"`
2. `"non_hermetic_dev"`
3. `"invalid"`

Scientific `run` may only emit `"hermetic"`.

## 10. File-Level Patch Plan

### 10.1 Runner core

Primary files:

1. `rust/crates/lab-runner/src/io.rs`
2. `rust/crates/lab-runner/src/lifecycle.rs`
3. `rust/crates/lab-runner/src/runner.rs`
4. `rust/crates/lab-runner/src/validations.rs`
5. `rust/crates/lab-core/src/lib.rs`
6. `rust/crates/lab-cli/src/main.rs`

Required changes:

1. add scientific agent execution config parsing and validation
2. add containerized agent launch path
3. add tool gateway abstraction
4. remove host launch path from scientific run
5. keep local path only under `run-dev`
6. record agent runtime image digests in sealed package and attestation

### 10.2 Schemas

Primary files:

1. `schemas/resolved_experiment_v0_5.jsonschema`
2. any runtime config schema consumed by build/preflight
3. trial metadata and state inventory schemas

Required changes:

1. encode `runtime.agent.execution`
2. encode hermetic execution plane data in `trial_metadata`
3. encode `agent_runtime` mounts and image digest in `state_inventory`

### 10.3 Docs

Primary files:

1. `docs/USAGE.md`
2. `docs/README.md` references
3. any patch specs that normalize host-launched agents

Required changes:

1. scientific docs must state "agent runs in agent_runtime container"
2. `run-dev` docs must state "host execution, not scientifically comparable"
3. remove all examples that pass `--dangerous` in benchmark experiments

## 11. E2E Plan For `tests/e2e_cli/test_cli_e2e.py`

This file already has the right structural leverage:

1. `_base_experiment()`
2. `_create_simple_project()`
3. `_run_package()`
4. real Docker images
5. real artifact bundles

The test plan should expand this file rather than creating mocked unit-only substitutes.

### 11.1 Helper additions

Add helper builders:

1. `_agent_execution(image_tag: str, *, network: str = "none") -> dict[str, Any]`
2. `_variant_entry(variant_id: str, bindings: dict[str, Any], runtime_overrides: dict[str, Any] | None = None) -> dict[str, Any]`
3. `_assert_trial_hermetic(trial_dir: Path) -> None`
4. `_load_agent_report(trial_dir: Path) -> dict[str, Any]`
5. `_query_trial_rows(run_dir: Path) -> list[dict[str, Any]]`

Extend `_base_experiment()` to accept:

1. `agent_execution_image`
2. `agent_execution_overrides`
3. richer `variant_plan` entries with per-variant runtime overrides

### 11.2 Fixture additions

Keep the current simple command-contract fixture, but add two more bundles:

1. `probe-agent`
   Writes cwd, env, mount observations, and path probes into `agent_report.json`.
2. `tool-probe-agent`
   Uses the real bridge/tool protocol to issue at least one `Read`, one `Write`, and one `Bash`.

Add a distinct agent runtime image fixture:

1. task image contains marker `TASK_PLANE=task_sandbox`
2. agent runtime image contains marker `TASK_PLANE=agent_runtime`

This lets e2e prove where `Bash` actually ran.

### 11.3 Mandatory new e2e cases

Add the following large end-to-end tests:

1. `test_scientific_run_executes_agent_in_container_not_host`
   Assert agent report `env.workspace == "/agentlab/workspace"` and no `.scratch/` host path appears in agent-visible env.
2. `test_preflight_rejects_dangerous_agent_command`
   Assert build or preflight fails when experiment agent command contains `--dangerous`.
3. `test_run_rejects_nonhermetic_agent_execution`
   Assert scientific `run` rejects local-process agent execution.
4. `test_task_sandbox_bash_executes_in_task_plane`
   Assert `Bash` observes the task image marker, not the agent runtime marker.
5. `test_ab_variant_matrix_preserves_hermeticity`
   Use baseline plus at least two treatment variants in one run and assert every trial remains hermetic.
6. `test_agent_cannot_observe_host_sentinel`
   Create a host-only sentinel outside mounted paths and assert the probe agent cannot see it.

### 11.4 Variant encoding strategy

Do not encode variant coverage as separate pytest cases when one experiment run can cover the matrix more faithfully.

Use `variant_plan` in a single sealed experiment to encode:

1. baseline control
2. treatment with different bindings
3. treatment with different runtime overrides
4. invariant case with same bindings but different declared execution metadata

Recommended pattern inside `test_cli_e2e.py`:

1. baseline in `baseline_bindings`
2. all other matrix points in `variant_plan`
3. task rows declaring `expected_variant`
4. one run, one SQLite query pass, one artifact assertion pass

This keeps tests realistic:

1. real scheduler behavior
2. real variant accounting
3. real evidence capture
4. no mocked variant layer

### 11.5 Required assertions per trial

For every trial in the variant matrix, assert:

1. `trial_metadata.runtime.container_mode` is no longer treated as sufficient evidence
2. `state_inventory` records both `agent_runtime` and `task_sandbox`
3. agent report cwd is `/agentlab/workspace` or a contract alias, never a host path
4. no host `.scratch` path appears in `harness_stdout.log`, `harness_stderr.log`, or agent-visible env
5. run attestation marks isolation as `hermetic`

## 12. Acceptance Gates

Cutover is complete only when all are true:

1. `run` and `build-run` contain no scientific host-agent launch path.
2. agent argv/env never receive host workspace paths.
3. `--dangerous` is rejected in scientific runs.
4. `run-dev` is the only remaining local host-execution path.
5. the e2e tests above pass against real Docker-backed fixtures.
6. the AB test path in `test_cli_e2e.py` proves hermeticity per variant, not only aggregate correctness.
7. attestation never reports a successful scientific run with non-hermetic isolation.

## 13. Immediate Migration Targets

The first configs that must be fixed after runner cutover are the `jesus` experiment YAMLs that currently hard-code `--dangerous`, including:

1. `.lab/experiments/swebench_lite_glm5_vs_codex_low_v0.yaml`
2. `.lab/experiments/swebench_lite_glm5_vs_codex_v0.yaml`

They must be migrated to:

1. no `--dangerous`
2. explicit `runtime.agent.execution.image`
3. bundle metadata that is container-compatible

Until that migration happens, those experiments are not scientifically valid.
