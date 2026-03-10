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
| `Experiment` | Declarative config: DX authoring (`benchmark + agent + baseline + variants + overrides`) normalized to internal dataset/design/runtime/policy. | User |
| `Task` | One dataset row (`task_boundary_v3`). | Dataset provider |
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

## Task Sandbox Hard Cutover

`task_boundary_v3` is now the only accepted runner boundary. The runner owns sandbox topology.

1. Allowed task-boundary keys are fixed: `schema_version`, `task`, `environment`, `workspace`, `limits`.
2. `environment.image` replaces `task.image`.
3. `workspace.base`, `workspace.overlays`, and `workspace.aux_mounts` replace `workspace_seed`, `workspace_files`, and `mount_references`.
4. `task.workspace` is deleted; sandbox topology is selected by runner-owned `runtime.sandbox.profile`.
5. `workspace.mode = "patch"` requires a real base (`dataset_pack` or `git_checkout`), not overlays-only materialization.
6. The logical writable task root inside the sandbox is always `/agentlab/workspace`.
7. Agents run outside the task sandbox; compatibility aliases such as `/testbed` are runner-owned profile behavior.

### Rebuild Bench v0 Dataset For `/jesus`

Run from `Experiments` root:

```bash
python3 bench/integration/agentlab/export_bench_suite_to_jsonl.py \
  --suite v0 \
  --output /Users/jevinnishioka/Desktop/jesus/.lab/experiments/data/bench_v0.task_boundary_v3.jsonl \
  --default-task-image bench-v0-workspace \
  --dataset-pack-root /Users/jevinnishioka/Desktop/jesus/.lab/dataset_packs/sha256
```

Then run from `/Users/jevinnishioka/Desktop/jesus`:

```bash
/Users/jevinnishioka/Desktop/Experiments/rust/target/release/lab-cli preflight \
  .lab/experiments/bench_v0_glm5_vs_codex_spark_tarbell_latest_v0.yaml

/Users/jevinnishioka/Desktop/Experiments/rust/target/release/lab-cli run \
  .lab/experiments/bench_v0_glm5_vs_codex_spark_tarbell_latest_v0.yaml \
  --executor local_docker
```

### Not Public

The following are intentionally not public primitives:

- Internal Rust symbols/functions.
- Worker/coordinator internals.
- In-memory state machine structure.
- Patch-spec and migration doc internals.

## Minimal DX Experiment Shape

```yaml
experiment:
  id: bench_v0_qwen35b_a3b_only
  name: "Bench v0: Qwen3.5 35B A3B (LM Studio)"
  tags: [bench-v0, single-variant, lmstudio, qwen3.5-35b-a3b]

benchmark: bench_v0
limit: 20

agent:
  artifact: rex-minimal-linux-dir
  command: [rex, run, --dangerous]
  default_config: overrides/defaults.bench-lmstudio-headless.json
  provider_env:
    - provider: z.ai-coder
      env: ZAI_CODER_API_KEY
  io: { input: --input-file, output: --output }
  env:
    MEMORY_DAEMON_URL: ""
  config_files:
    - overrides/defaults.bench-lmstudio-headless.json
    - overrides/providers.lmstudio-docker.ts
    - overrides/providers.lmstudio-docker.js
  workspace_patches:
    overrides/providers.lmstudio-docker.ts: packages/core/types/src/providers.ts
    overrides/providers.lmstudio-docker.js: packages/core/types/dist/providers.js
  arg_map:
    - key: model_provider
      flag: --provider
    - key: model
      flag: --model

baseline:
  id: qwen_35b_a3b
  bindings:
    model_provider: lmstudio
    model: qwen3.5-35b-a3b

overrides:
  network: full
  root_read_only: false
```

DX authoring notes:

1. Built-in benchmark registry currently supports `benchmark: bench_v0`.
2. `agent.artifact` resolves short names from `.lab/agents/`.
3. `agent.default_config` stages the file (if needed) and appends `--config /agentlab/deps/<file>` when command does not already set `--config`.
4. `agent.provider_env` appends `--provider-env provider=ENV` and auto-adds those env vars to `agent.env_from_host`.
5. If staged config files include `.config/...` and `agent.env.HOME` is unset, HOME defaults to `/agentlab/deps` for runtime auth lookup.
6. In DX mode, legacy fields (`dataset`, `design`, `runtime`, `variant_plan`, `baseline.variant_id`) are rejected.
7. `arg_map` items must use `key` + `flag`. Legacy aliases are not accepted.
8. DX authoring has no top-level `version` field.
9. Persistent workspace carry-forward stores full workspace file contents. By default the runner fails fast above `AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES=268435456` bytes to avoid unbounded memory growth during bundle capture.

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

# create a DX experiment file using the "Minimal DX Experiment Shape" above
mkdir -p .lab/experiments
$EDITOR .lab/experiments/bench_v0_qwen35b_a3b_only.yaml

# validate environment + resolved plan
rust/target/release/lab-cli preflight .lab/experiments/bench_v0_qwen35b_a3b_only.yaml
rust/target/release/lab-cli describe .lab/experiments/bench_v0_qwen35b_a3b_only.yaml --json

# run with Docker
rust/target/release/lab-cli run .lab/experiments/bench_v0_qwen35b_a3b_only.yaml --executor local_docker

# fallback without Docker
rust/target/release/lab-cli run .lab/experiments/bench_v0_qwen35b_a3b_only.yaml --executor local_process
```

Notes:

1. If `preflight` reports `container_ready=false`, use `local_process` or start Docker.
2. If `local_process` fails with command-not-found for `rex`, verify `runtime.agent.bundle` resolves under `.lab/agents/` and the bundle contains an executable `bin/rex`.
3. If `preflight` reports missing config files, place them under `.lab/experiments/overrides/` or use absolute paths.

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
