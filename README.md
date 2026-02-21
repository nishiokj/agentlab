# AgentLab

AgentLab is an experiment runner for agent-runtime workloads. It executes trials in isolated environments, injects dependencies and policy, and produces causal/evidence artifacts for scoring and analysis.

Primary design goals:

1. Isolation by default.
2. Opinionated experimental design.
3. Runner-owned causal extraction and analysis.

## Runtime Model (Hard Cut)

AgentLab is now `runtime.agent`-first and does not support legacy harness runtime config in new specs.

- Use `runtime.agent`, `runtime.dependencies`, and `runtime.policy`.
- Do not use `runtime.harness` (rejected by the runner).
- Runner invokes one command per trial.
- Runner owns sandboxing/network/timeouts/dependency staging.

Source of truth:

1. Runtime/data contracts: `schemas/*.jsonschema`
2. Runtime enforcement and lifecycle: `rust/crates/lab-runner/src/lib.rs`

## Boundaries

The active boundaries are:

1. Task boundary: dataset row -> `task_boundary_v1` payload.
2. Agent runtime boundary: one command invocation with fixed mounts/env contract.
3. Dependency boundary: file staging + service descriptors exposed to the trial.
4. Policy boundary: timeout/network/sandbox enforced by runner.
5. Analysis boundary: trajectory/events/artifacts normalized into run-level tables.

Removed as first-class public boundaries:

1. Harness boundary.
2. Control-plane protocol boundary.
3. Cross-trial runtime state boundary for agent execution.

## Where Experiment State Machine Is Handled

The experiment and trial lifecycle state machine is runner-owned inside:

- `rust/crates/lab-runner/src/lib.rs`

Key state files emitted by the runner:

- Run-level: `.lab/runs/<run_id>/runtime/run_control.json`
- Trial-level: `.lab/runs/<run_id>/trials/<trial_id>/trial_state.json`

The scheduler/execution loop is in `run_experiment_with_behavior(...)` in the same file.

## Task vs Variant in the New Model

Variants are still defined at experiment design level (`baseline` + `variant_plan`), not embedded in dataset rows.

At trial materialization time, the runner bundles task + variant + policy into `agent_task_v1`:

- `task`: problem/question payload from dataset row.
- `bindings`: selected variant bindings for that trial.
- `dependencies`: services declared for the trial.
- `policy`: timeout/network/sandbox policy for that trial.

This means the trial payload contains both task semantics and variant semantics, while ownership remains clean:

1. Dataset owns task rows.
2. Design owns variants.
3. Runner composes both into per-trial `agent_task_v1`.

## Resolved Experiment Shape (v0.5)

```yaml
version: '0.5'
experiment:
  id: swebench_agent_runtime
  name: SWE-bench Agent Runtime
  workload_type: agent_runtime

dataset:
  path: ./data/tasks.boundary.jsonl
  provider: local_jsonl
  suite_id: swebench_lite_curated
  schema_version: task_jsonl_v1
  split_id: test
  limit: 50

design:
  sanitization_profile: hermetic_functional
  comparison: paired
  replications: 1
  random_seed: 42
  shuffle_tasks: true
  max_concurrency: 1

baseline:
  variant_id: control
  image: ghcr.io/org/agent-runtime-control@sha256:...
  bindings:
    model: gpt-4o-mini

variant_plan:
  - variant_id: treatment
    image: ghcr.io/org/agent-runtime-treatment@sha256:...
    bindings:
      model: gpt-4.1

runtime:
  agent:
    command: ["rex"] # required: single runtime command
    image: ghcr.io/org/agent-runtime@sha256:... # required for container mode; variant image can override
    io: # optional: defaults shown
      input_arg: --input
      output_arg: --output

  dependencies:
    file_staging:
      - source_from_host: ./deps/sqlite/main.db
        destination_path: /agentlab/deps/sqlite/main.db
        required: true
      - source_from_host: ./deps/ast/index.tar.zst
        destination_path: /agentlab/deps/ast/index.tar.zst
        required: false
    services:
      - id: sqlite-main
        kind: sqlite
        path: /agentlab/deps/sqlite/main.db
      - id: ast-index
        kind: filesystem
        path: /agentlab/deps/ast

  policy:
    timeout_ms: 600000
    network:
      mode: none
      allowed_hosts: []
    sandbox:
      mode: container
      root_read_only: true
      hardening:
        no_new_privileges: true
        drop_all_caps: true
      resources:
        cpu_count: 4
        memory_mb: 8192

  telemetry:
    trajectory_path: /agentlab/out/trajectory.jsonl
    causal_extraction: event_envelope_v1

validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
```

## Agent Runtime Contract

Runner mounts (container mode):

1. `/agentlab/in` (ro)
2. `/agentlab/out` (rw)
3. `/agentlab/state` (rw, runner metadata/state)
4. `/agentlab/workspace` (rw)
5. `/agentlab/deps` (rw/ro based on staging/policy)

Runner env vars provided to the runtime command:

1. `AGENTLAB_TASK_PATH`
2. `AGENTLAB_BINDINGS_PATH`
3. `AGENTLAB_DEPENDENCIES_PATH`
4. `AGENTLAB_POLICY_PATH`
5. `AGENTLAB_RESULT_PATH`
6. `AGENTLAB_TRAJECTORY_PATH`
7. `AGENTLAB_TIMEOUT_MS`
8. `AGENTLAB_RUN_ID`
9. `AGENTLAB_TRIAL_ID`
10. `AGENTLAB_VARIANT_ID`
11. `AGENTLAB_TASK_ID`
12. `AGENTLAB_REPL_IDX`

Not part of this contract:

1. No required control-plane handshake.
2. No required control socket/file protocol for successful trial completion.
3. No runner-managed cross-trial chain state semantics for agent execution.

## Minimal Runtime Program

Your program should:

1. Read task/bindings/dependencies/policy from env-provided paths.
2. Execute autonomously.
3. Write `agent_result_v1` to `AGENTLAB_RESULT_PATH`.
4. Optionally append trajectory events to `AGENTLAB_TRAJECTORY_PATH`.

```python
import json
import os

task = json.load(open(os.environ["AGENTLAB_TASK_PATH"], "r", encoding="utf-8"))
bindings = json.load(open(os.environ["AGENTLAB_BINDINGS_PATH"], "r", encoding="utf-8"))
deps = json.load(open(os.environ["AGENTLAB_DEPENDENCIES_PATH"], "r", encoding="utf-8"))
policy = json.load(open(os.environ["AGENTLAB_POLICY_PATH"], "r", encoding="utf-8"))

# ... run your runtime command ...

result = {
    "schema_version": "agent_result_v1",
    "ids": {
        "run_id": os.environ["AGENTLAB_RUN_ID"],
        "trial_id": os.environ["AGENTLAB_TRIAL_ID"],
        "variant_id": os.environ["AGENTLAB_VARIANT_ID"],
        "task_id": os.environ["AGENTLAB_TASK_ID"],
        "repl_idx": int(os.environ["AGENTLAB_REPL_IDX"]),
    },
    "outcome": "success",
    "answer": {"message": "done"},
    "metrics": {"latency_ms": 1234},
}

with open(os.environ["AGENTLAB_RESULT_PATH"], "w", encoding="utf-8") as f:
    json.dump(result, f)
```

## What You Need To Bring

Minimum inputs for a runnable experiment:

1. Dataset rows (`dataset.path`).
2. Agent runtime (`runtime.agent.command` and optional `runtime.agent.io` flags).
3. Container image (`runtime.agent.image` for baseline/default, optionally overridden per variant via `baseline.image` and `variant_plan[].image`).
4. Baseline variant bindings (plus optional treatments).
5. Policy (`timeout`, `network`, `sandbox`).
6. Optional staged dependency files/services for sqlite, AST indexes, or other local state.

## Runtime Source of Truth

`runtime.agent` is intentionally small:

1. `runtime.agent.command` is the only runtime command the runner invokes.
2. `runtime.agent.image` is the default container image.
3. `baseline.image` and `variant_plan[].image` can override image per variant.
4. `runtime.agent.io.input_arg` / `output_arg` define how runner-appended file paths are passed to your command.

The command is executed by runner inside that runtime context. It is not a host-side path lookup API.

Runner automatically appends the resolved trial input/output paths to your command using these IO arg settings.

For reproducible frozen agents:

1. Build agent image with runtime + code.
2. Pin image by digest (`image@sha256:...`).
3. Set `runtime.agent.image` to that digest and optionally override per variant.
4. Stage large mutable inputs (sqlite/db/index files) via `runtime.dependencies.file_staging`.

## Path and CWD Semantics

You can run from any shell directory if you pass the experiment path correctly.

Resolution behavior:

1. `dataset.path` resolves relative to the experiment file directory.
2. `runtime.dependencies.file_staging[*].source_from_host` resolves relative to project root (parent of `.lab`) when relative.
3. In container execution, `runtime.agent.command` tokens are treated as literal command tokens.
4. In local-process execution, path-like tokens may be host-resolved for compatibility.

## CLI Quick Start

Build:

```bash
cd rust
cargo build -p lab-cli --release
```

Initialize config:

```bash
./target/release/lab-cli init
```

Validate and inspect resolved plan:

```bash
./target/release/lab-cli describe .lab/experiment.yaml
```

Run:

```bash
./target/release/lab-cli run .lab/experiment.yaml
```

Machine-readable mode:

```bash
./target/release/lab-cli describe .lab/experiment.yaml --json
./target/release/lab-cli run .lab/experiment.yaml --json
```

## Run Artifacts

Run root:

```text
.lab/runs/<run_id>/
```

Important paths:

1. `resolved_experiment.json`
2. `evidence/evidence_records.jsonl`
3. `evidence/task_chain_states.jsonl`
4. `benchmark/predictions.jsonl`
5. `benchmark/scores.jsonl`
6. `benchmark/summary.json`
7. `analysis/tables/*.jsonl`

Per trial:

1. `trials/<trial_id>/trial_input.json` (runner input envelope)
2. `trials/<trial_id>/in/task.json`
3. `trials/<trial_id>/in/bindings.json`
4. `trials/<trial_id>/in/dependencies.json`
5. `trials/<trial_id>/in/policy.json`
6. `trials/<trial_id>/out/result.json` (agent-written contract file)
7. `trials/<trial_id>/result.json` (runner-canonicalized output)
8. `trials/<trial_id>/out/trajectory.jsonl` (if emitted)
9. `trials/<trial_id>/workspace/`
10. `trials/<trial_id>/deps/`
11. `trials/<trial_id>/trial_state.json`

## Scaling Status

Current status:

1. Contract has `design.max_concurrency`.
2. Main execution loop is still sequential while adapter-runtime parallel execution lands.

Planned Phase 2:

1. Bounded worker pool up to `max_concurrency`.
2. Isolated per-trial execution workers.
3. Deterministic reducer for run-level JSONL/tables.
4. Stable sorted outputs by canonical trial identity.
5. Backpressure and per-trial failure isolation.

See full details in:

- `docs/plan.md`

## Notes

1. If your agent needs sqlite indices, AST stores, or similar assets, use `runtime.dependencies.file_staging` + `runtime.dependencies.services`.
2. For reproducibility in container mode, pin `runtime.agent.image` (and per-variant `baseline.image` / `variant_plan[].image`) by digest (`image@sha256:...`).
3. Keep runtime YAML minimal: command + image + policy; package dependencies/env/runtime setup inside the image.
4. Keep agent runtime implementations stateless across trials; each trial should be self-sufficient and isolated.
