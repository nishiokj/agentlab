# AgentLab

AgentLab is a build-first system for running controlled agent evaluations.

The product is this sequence:

`author -> build -> verify -> run -> inspect`

Everything else hangs off those five boundaries.

## The Five Stages

1. `Author`
   You write `experiment.yaml` and `tasks.jsonl` containing `task_row_v1` rows.
2. `Build`
   AgentLab resolves paths, bundles files, seals artifacts, and emits a portable package.
3. `Verify`
   `preflight` checks that the package can actually run.
4. `Run`
   The runner executes trials from the sealed package boundary.
5. `Inspect`
   You read the run outputs, facts, and resolved state without guessing.

Do not skip boundaries. Do not hand-author package internals. Do not treat run directories as inputs.

## 1. Author

Start by choosing the experiment shape you want:

- `agent-eval`: one variant, no comparison
- `ab-test`: paired baseline vs treatments
- `sweep`: many variants over a parameter surface
- `regression`: repeated tracking over time

Scaffold one:

```bash
cargo build --manifest-path rust/Cargo.toml -p lab-cli --release
LAB="$(pwd)/rust/target/release/lab-cli"

mkdir -p /tmp/agentlab-demo
cd /tmp/agentlab-demo

"$LAB" init --profile ab-test --in-place
```

### What You Actually Edit

Small project, explicit boundary:

```text
agentlab-demo/
├── experiment.yaml
├── tasks.jsonl
├── agents/
│   └── rex-current.linux-x64.tar.gz
└── bench/
    └── integration/
        └── agentlab/
            └── my_benchmark_grader.py
```

`experiment.yaml` is the control plane. `tasks.jsonl` is the workload, and each line must be a `task_row_v1`. Variants are first-class: the baseline is one variant, each entry in `variant_plan` is another.

### Example `experiment.yaml`

This example is intentionally realistic. It shows:

- experiment type: paired A/B
- the control and treatment variants first
- model choice through variant bindings
- API keys supplied at launch time
- benchmark grading through a custom grader
- concurrency and timeout

```yaml
experiment:
  id: bench_demo_glm5_vs_codex_low
  name: Bench Demo: GLM-5 vs Codex Low
  workload_type: agent_runtime
  tags: [bench-demo, ab-test]

baseline:
  variant_id: glm_5
  bindings:
    model_provider: z.ai-coder
    model: glm-5
    reasoning: off

variant_plan:
  - variant_id: codex_low
    bindings:
      model_provider: codex
      model: gpt-5.3-codex
      reasoning: low

dataset:
  suite_id: bench_demo
  provider: local_jsonl
  path: tasks.jsonl
  split_id: test
  limit: 20

runtime:
  agent_runtime:
    artifact: agents/rex-current.linux-x64.tar.gz
    image: ghcr.io/acme/rex-agent:latest
    command:
      - rex
      - run
      - --provider
      - $model_provider
      - --model
      - $model
      - --reasoning
      - $reasoning
    env:
      OPENAI_API_KEY: $OPENAI_API_KEY
      ZAI_CODER_API_KEY: $ZAI_CODER_API_KEY
    network: full

benchmark:
  grader:
    strategy: in_task_image
    command: [python3, bench/integration/agentlab/my_benchmark_grader.py]
    conclusion:
      mode: direct
    in_task_image:
      hidden_paths: []
      revealed_paths: []
  policy:
    evaluator_mode: custom
    scoring_lifecycle: predict_then_score
    task_model: independent
    chain_failure_policy: continue_with_flag

metrics:
  - id: duration_ms
    source: runner
    primary: false
    weight: 0
  - id: resolved
    source: output
    json_pointer: /metrics/resolved
    direction: maximize
    primary: true
    weight: 1

design:
  replications: 1
  max_concurrency: 2

policy:
  timeout_ms: 600000
  task_sandbox:
    network: full
```

### Variant Boundary

Variant bindings are the experiment surface for behavior differences.

- `baseline` is not special runtime machinery. It is the control variant.
- `variant_plan[*]` are treatment variants.
- `$NAME` in command/env resolves from variant bindings first.
- If a binding is not provided by the variant, `lab run --env`, `lab run --env-file`, then host env are consulted.

That means:

- choose the model in bindings
- choose reasoning mode in bindings
- keep runtime command stable
- compare variants without rewriting the runtime itself

### Example `tasks.jsonl`

Each line is one `task_row_v1`. Benchmark-specific meaning lives inside `task.*`. Runner-owned execution fields live at the top level of the row.

```json
{
  "schema_version": "task_row_v1",
  "id": "TASK001",
  "image": "ghcr.io/acme/task-image:latest",
  "workdir": "/workspace/task",
  "time_limit_ms": 600000,
  "task": {
    "id": "TASK001",
    "prompt": "Fix the failing scorer regression.",
    "benchmark": {
      "adapter_id": "bench_demo",
      "name": "bench",
      "split": "test"
    },
    "input": {
      "prompt": "The public smoke test fails. Fix the bug without breaking existing behavior."
    },
    "public_command": "bash .bench_public/run_public.sh",
    "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl"
  },
  "materialization": {
    "kind": "task_image"
  }
}
```

### Benchmark Boundary

Any benchmark works if it can emit valid task rows and grade the agent result.

The benchmark owns:

- task semantics
- prompt/input payload
- public and hidden commands
- benchmark-specific metadata inside `task.*`

The runner owns:

- task image selection
- declared task `workdir`
- task materialization mode
- `time_limit_ms`
- per-trial lifecycle
- scheduling and retries

Rule of thumb: benchmark meaning goes in `task.*`; execution topology goes in top-level task row fields like `image`, `workdir`, `materialization`, and `time_limit_ms`.

### Grader Boundary

The grader reads `grader_input_v1` and writes the canonical mapped conclusion.

The grader sees these env vars:

- `AGENTLAB_GRADER_INPUT_PATH`
- `AGENTLAB_RAW_GRADER_OUTPUT_PATH`
- `AGENTLAB_MAPPED_GRADER_OUTPUT_PATH`

Minimal direct-mode grader shape:

```python
import json
import os

grader_input = json.load(open(os.environ["AGENTLAB_GRADER_INPUT_PATH"]))

conclusion = {
    "schema_version": "trial_conclusion_v1",
    "payload": {
        "task_id": grader_input["ids"]["task_id"],
        "resolved": 1.0,
    },
    "reported_outcome": "success",
    "primary_metric": {
        "name": "resolved",
        "value": 1.0,
    },
    "grader": {
        "name": "custom_grader",
        "strategy": "in_task_image",
    },
}

json.dump(conclusion, open(os.environ["AGENTLAB_MAPPED_GRADER_OUTPUT_PATH"], "w"))
```

## 2. Build

Build takes authored inputs and produces a sealed package:

```bash
"$LAB" build experiment.yaml --out .lab/builds/bench-demo
```

Typical package shape:

```text
.lab/builds/bench-demo/
├── manifest.json
├── resolved_experiment.json
├── checksums.json
├── package.lock
├── tasks/
│   └── tasks.jsonl
├── agent_builds/
│   └── 000_rex-current.linux-x64.tar.gz
├── runtime_assets/
│   └── ...
└── files/
    └── ...
```

What build does:

- rewrites `dataset.path` to the packaged task file
- stages runtime support files under package-controlled paths
- copies agent artifacts into `agent_builds/`
- seals the package with manifests and checksums

### What Resolution Looks Like

Your authored experiment is not the runtime object. The package contains the resolved form.

Excerpt from `resolved_experiment.json`:

```json
{
  "benchmark": {
    "grader": {
      "command": [
        "python3",
        "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/bench/integration/agentlab/my_benchmark_grader.py"
      ]
    }
  },
  "dataset": {
    "path": "tasks/tasks.jsonl",
    "provider": "local_jsonl",
    "suite_id": "bench_demo",
    "split_id": "test",
    "limit": 20
  },
  "runtime": {
    "agent_runtime": {
      "artifact": "agent_builds/000_rex-current.linux-x64.tar.gz"
    }
  }
}
```

That is the build boundary in one glance:

- authored relative paths become package-owned runtime paths
- task support assets are staged under the task workdir support area at runtime
- the package becomes portable
- the resolved object is the thing the runner actually executes

## 3. Verify

Verify the package before you waste time on a run:

```bash
"$LAB" describe .lab/builds/bench-demo
"$LAB" preflight .lab/builds/bench-demo --env-file .env
```

Use `preflight` to catch:

- missing artifacts
- missing packaged runtime assets
- bad image references
- missing launch-time secrets
- broken grader reachability

## 4. Run

Run the sealed package, not the authored YAML:

```bash
"$LAB" run .lab/builds/bench-demo \
  --env OPENAI_API_KEY=... \
  --env ZAI_CODER_API_KEY=... \
  --env-file .env \
  --materialize full
```

Tighter loop:

```bash
"$LAB" build-run experiment.yaml --out .lab/builds/bench-demo --materialize full
```

If a run stops mid-schedule, continue the existing run directory:

```bash
"$LAB" continue .lab/runs/<run_id> --env-file .env
```

Common knobs:

- choose experiment type with `lab init --profile ...`
- choose models with `baseline.bindings` and `variant_plan[*].bindings`
- choose concurrency with `design.max_concurrency`
- choose replications with `design.replications`
- choose timeout with `policy.timeout_ms`
- provide API keys with `lab run --env` and `--env-file`

## Runtime Contract

The agent process runs against a stable contract. Consume this contract. Do not infer hidden topology.

Filesystem:

- cwd: the task row `workdir`
- `/agentlab/in`
- `/agentlab/out`
- `/agentlab/metrics`
- `<workdir>/.agentlab/support` for runner-staged support assets when needed

No `bindings.json`, no `/agentlab/deps`, and no fixed `/agentlab/workspace` compatibility root are part of the supported contract.

Important env vars:

- `AGENTLAB_TRIAL_INPUT_PATH`
- `AGENTLAB_GRADER_INPUT_PATH`
- `AGENTLAB_RESULT_PATH`
- `AGENTLAB_RAW_GRADER_OUTPUT_PATH`
- `AGENTLAB_MAPPED_GRADER_OUTPUT_PATH`
- `AGENTLAB_TRAJECTORY_PATH`
- `AGENTLAB_RUN_ID`
- `AGENTLAB_TRIAL_ID`
- `AGENTLAB_VARIANT_ID`
- `AGENTLAB_TASK_ID`
- `AGENTLAB_TIMEOUT_MS`

Current runtime also exports `WORKSPACE=<workdir>` as a convenience env. Treat `trial_input_v1.runtime.workdir` and cwd as primary.

## 5. Inspect

Run outputs live under:

```text
.lab/runs/<run_id>/
```

High-signal files:

- `resolved_experiment.json`
- `runtime/run_control.json`
- `trials/<trial_id>/trial_state.json`
- `trials/<trial_id>/out/result.json`
- `facts/run_manifest.json`
- `facts/trials.jsonl`
- `facts/events.jsonl`
- `facts/metrics_long.jsonl`

Useful commands:

```bash
"$LAB" runs
"$LAB" variants <run_id>
"$LAB" views <run_id>
"$LAB" query <run_id> "SELECT * FROM trials LIMIT 20"
```

## Public Surfaces

If you are building tooling against AgentLab, these are the supported surfaces:

1. `lab` or `lab-cli` subcommands
2. Scaffolds produced by `lab init`
3. Schemas in [`schemas/`](schemas/)
4. Sealed package contents
5. Run outputs under `.lab/runs/<run_id>/`
6. The runtime contract documented above

Do not build tooling against undocumented package internals.

## Avoid Legacy Inputs

These are removed from the happy path:

- `version`
- `dataset.schema_version`
- `task.schema_version`
- `runtime.agent`
- `runtime.sandbox`
- `runtime.dependencies.file_staging`
- `benchmark.grader.support_files`
- `benchmark.adapter.support_files`
- `benchmark.adapter`
- `--executor`

## Working On AgentLab

If you are here to work on AgentLab itself, not just use it:

- [`rust/`](rust/) is the CLI, runner, and analysis stack
- [`schemas/`](schemas/) defines public contracts
- [`sdk/`](sdk/) is the TypeScript SDK surface
- [`tests/`](tests/) covers CLI and boundary behavior
- [`docs/`](docs/) holds design records, migrations, and invariants
- [`bench/`](bench/) and [`adapters/`](adapters/) hold benchmark-side tooling

Common development commands:

```bash
make bootstrap
make test
make validate-schemas

cargo test --manifest-path rust/Cargo.toml -p lab-runner
pytest tests/e2e_cli -q
```

For repo-wide rules, read [`TESTS.md`](TESTS.md).
