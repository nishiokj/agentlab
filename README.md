# AgentLab

AgentLab runs controlled agent-runtime experiments.
It executes one runtime command per trial, enforces policy, and writes append-only facts.

## Public API (What Is Stable)

Treat only these as stable:

1. Schemas in `schemas/*.jsonschema`.
2. CLI behavior exposed by `lab-cli --help`.
3. Run directory contracts under `.lab/runs/<run_id>/`.

Everything else is internal implementation detail and may change without notice.

## Canonical Primitives

Use these names consistently in docs, code comments, and UX.

| Primitive | Definition | Owner |
| --- | --- | --- |
| `Experiment` | Declarative config: dataset + design + runtime + policy. | User |
| `Task` | One dataset row (`task_jsonl_v1`). | Dataset provider |
| `Variant` | Bindings/image override applied across tasks. | Experiment design |
| `Trial` | `Task x Variant x Replication` execution unit. | Runner |
| `Runtime` | The single command invocation contract (`runtime.agent`). | Runtime author |
| `Policy` | Timeout/network/sandbox limits enforced by runner. | Runner config |
| `Result` | Agent-written `agent_result_v1` output for one trial. | Runtime |
| `Facts` | Runner-written immutable JSONL records. | Runner |
| `Views` | Analysis-derived query surfaces over facts. | Analysis layer |

## Boundary Rules (Hard)

1. Runner executes exactly one runtime command per trial.
2. Runtime does not own scheduler state, dispatch, or run control.
3. Runner enforces policy; runtime cannot override policy at execution time.
4. Runner appends immutable facts; analysis computes aggregates and live views.
5. Benchmark-specific logic stays in adapters, not runner core.

### Not Public

The following are intentionally not public primitives:

- Internal Rust symbols/functions.
- Worker/coordinator internals.
- In-memory state machine structure.
- Patch-spec and migration doc internals.

## Minimal Experiment Shape

```yaml
version: "0.5"
experiment:
  id: exp_local
  workload_type: agent_runtime

dataset:
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_jsonl_v1

design:
  comparison: paired
  replications: 1
  max_concurrency: 1

baseline:
  variant_id: base
  bindings: {}

variant_plan: []

runtime:
  agent:
    command: ["python", "./examples/clean_harness/harness.py"]
    image: python:3.11-slim
    io:
      input_arg: "{path}"
      output_arg: "{path}"
  policy:
    timeout_ms: 600000
    network:
      mode: none
    sandbox:
      mode: container
```

## Runtime Contract

Runner provides input/output paths via env vars:

- `AGENTLAB_TASK_PATH`
- `AGENTLAB_BINDINGS_PATH`
- `AGENTLAB_DEPENDENCIES_PATH`
- `AGENTLAB_POLICY_PATH`
- `AGENTLAB_RESULT_PATH`
- `AGENTLAB_TRAJECTORY_PATH`

Runtime requirements per trial:

1. Read task/bindings/dependencies/policy from provided paths.
2. Execute autonomously.
3. Write `agent_result_v1` JSON to `AGENTLAB_RESULT_PATH`.
4. Optionally append trajectory events to `AGENTLAB_TRAJECTORY_PATH`.

Container mounts:

- `/agentlab/in` (ro)
- `/agentlab/out` (rw)
- `/agentlab/state` (rw)
- `/agentlab/workspace` (rw)
- `/agentlab/deps` (ro/rw based on policy and staging)

## 5-Minute Quickstart

From repo root (`/Users/jevinnishioka/Desktop/Experiments`):

```bash
# build CLI
cargo build --manifest-path rust/Cargo.toml -p lab-cli --release

# check runner crate
cargo check --manifest-path rust/Cargo.toml -p lab-runner

# initialize local config
rust/target/release/lab-cli init

# validate environment + resolved plan
rust/target/release/lab-cli preflight .lab/experiment.yaml
rust/target/release/lab-cli describe .lab/experiment.yaml --json

# run with Docker
rust/target/release/lab-cli run .lab/experiment.yaml --executor local_docker

# fallback without Docker
rust/target/release/lab-cli run .lab/experiment.yaml --executor local_process
```

Notes:

1. If `preflight` reports `container_ready=false`, use `local_process` or start Docker.
2. If `local_process` fails with `No such file or directory (os error 2)` and command is `python`, switch to `python3` in your experiment.

## Run Outputs (Contract-Level)

Run root:

```text
.lab/runs/<run_id>/
```

Key outputs:

- `resolved_experiment.json`
- `runtime/run_control.json`
- `trials/<trial_id>/trial_state.json`
- `trials/<trial_id>/out/result.json`
- `trials/<trial_id>/result.json`
- `facts/run_manifest.json`
- `facts/trials.jsonl`
- `facts/events.jsonl`
- `facts/metrics_long.jsonl`

## Repository Pointers

- `rust/crates/lab-cli/`: CLI surface.
- `rust/crates/lab-runner/`: execution engine.
- `schemas/`: contract source of truth.
- `adapters/`: benchmark adapters.
- `bench/`: task tooling + integration bridge.

## Deep-Dive Docs

- `docs/AGENTLAB_ONBOARDING.md`: hands-on onboarding flow.
- `docs/ARCHITECTURE.md`: boundary diagrams and architecture.
- `docs/USAGE.md`: benchmark task tooling usage.

