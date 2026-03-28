# AgentLab

Run controlled evaluations of AI agents. Define an experiment, point it at your agent, get scored results.

## Quickstart

```bash
# Build the CLI (one time)
cargo build --manifest-path rust/Cargo.toml -p lab-cli --release
LAB="$(pwd)/rust/target/release/lab-cli"

# Scaffold an experiment
mkdir my-eval && cd my-eval
"$LAB" init --profile agent-eval --in-place

# Edit experiment.yaml and tasks.jsonl, then:
"$LAB" build-run experiment.yaml --out .lab/builds/run1 \
  --env OPENAI_API_KEY=... \
  --materialize full

# See results
"$LAB" views .lab/runs/<run_id>
"$LAB" query .lab/runs/<run_id> "SELECT * FROM trials"
```

Profiles: `agent-eval` (single variant), `ab-test` (A/B comparison), `sweep` (parameter grid), `regression` (tracking over time).

## Experiment Config

`experiment.yaml` is the control plane. This example runs two model variants head-to-head with a custom grader:

```yaml
experiment:
  id: glm5_vs_codex
  name: GLM-5 vs Codex
  workload_type: agent_runtime

baseline:
  variant_id: glm_5
  bindings:
    model_provider: z.ai-coder
    model: glm-5

variant_plan:
  - variant_id: codex
    bindings:
      model_provider: codex
      model: gpt-5.3-codex

dataset:
  suite_id: my_benchmark
  provider: local_jsonl
  path: tasks.jsonl

runtime:
  agent_runtime:
    artifact: agents/my-agent.tar.gz
    image: ghcr.io/my-org/agent-image:latest
    command: [my-agent, run, --provider, $model_provider, --model, $model]
    env:
      API_KEY: $API_KEY
    network: full

benchmark:
  grader:
    strategy: in_task_image
    command: [python3, bench/grader.py]
    conclusion:
      mode: direct

metrics:
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

### Variants

Variants are how you compare different configurations without changing the runtime setup.

- `baseline` is the control variant
- Each entry in `variant_plan` is a treatment variant
- `$NAME` in `command` or `env` resolves from that variant's bindings
- Unresolved bindings fall through to `--env`, `--env-file`, then host environment

### Tasks

`tasks.jsonl` — one JSON object per line, each a `task_row_v1`:

```json
{
  "schema_version": "task_row_v1",
  "id": "TASK001",
  "image": "ghcr.io/my-org/task-image:latest",
  "workdir": "/workspace/task",
  "time_limit_ms": 600000,
  "task": {
    "id": "TASK001",
    "input": {
      "prompt": "Fix the failing test without breaking existing behavior."
    }
  },
  "materialization": {
    "kind": "task_image"
  }
}
```

Top-level fields (`image`, `workdir`, `time_limit_ms`, `materialization`) control execution. Everything inside `task` is benchmark-specific meaning passed through to your agent and grader.

## Grader

The grader reads a structured input and writes a conclusion. It runs after your agent finishes.

Env vars available to the grader:

| Variable | Purpose |
|----------|---------|
| `AGENTLAB_GRADER_INPUT_PATH` | JSON with trial IDs, agent output, and task context |
| `AGENTLAB_MAPPED_GRADER_OUTPUT_PATH` | Where to write the conclusion |

Minimal grader:

```python
import json, os

grader_input = json.load(open(os.environ["AGENTLAB_GRADER_INPUT_PATH"]))

conclusion = {
    "schema_version": "trial_conclusion_v1",
    "reported_outcome": "success",
    "primary_metric": {"name": "resolved", "value": 1.0},
    "payload": {"task_id": grader_input["ids"]["task_id"], "resolved": 1.0},
    "grader": {"name": "my_grader", "strategy": "in_task_image"},
}

json.dump(conclusion, open(os.environ["AGENTLAB_MAPPED_GRADER_OUTPUT_PATH"], "w"))
```

## Agent Runtime Contract

Your agent process runs inside a container with this contract:

**Filesystem:**

| Path | Access | Purpose |
|------|--------|---------|
| cwd (task `workdir`) | read/write | Working directory |
| `/agentlab/in/` | read | Trial input |
| `/agentlab/out/` | write | Agent output |

**Environment variables:**

| Variable | Value |
|----------|-------|
| `AGENTLAB_TRIAL_INPUT_PATH` | Path to trial input JSON |
| `AGENTLAB_RESULT_PATH` | Where to write your result |
| `AGENTLAB_RUN_ID` | Current run identifier |
| `AGENTLAB_TRIAL_ID` | Current trial identifier |
| `AGENTLAB_VARIANT_ID` | Which variant is running |
| `AGENTLAB_TASK_ID` | Which task is running |
| `AGENTLAB_TIMEOUT_MS` | Time limit in milliseconds |

Read the trial input. Do your work. Write a result JSON to the result path.

## Workflow

```
author  -->  build  -->  verify  -->  run  -->  inspect
```

| Stage | Command | What it does |
|-------|---------|-------------|
| Author | Edit `experiment.yaml` + `tasks.jsonl` | Define the experiment |
| Build | `lab build experiment.yaml --out .lab/builds/x` | Seal a portable package |
| Verify | `lab preflight .lab/builds/x --env-file .env` | Catch problems before running |
| Run | `lab run .lab/builds/x --env-file .env` | Execute all trials |
| Inspect | `lab views <run_id>` | Read results |

Or skip straight to results: `lab build-run experiment.yaml --out .lab/builds/x --env-file .env`

### Inspect Commands

```bash
"$LAB" runs                                           # list runs
"$LAB" variants <run_id>                              # show resolved variants
"$LAB" views <run_id>                                 # summary tables
"$LAB" query <run_id> "SELECT * FROM trials LIMIT 20" # SQL over results
```

### Resume a Stopped Run

```bash
"$LAB" continue .lab/runs/<run_id> --env-file .env
```

## Reference

**Experiment knobs:**

| Field | Purpose |
|-------|---------|
| `design.replications` | Repeat count per task/variant |
| `design.max_concurrency` | Parallel trial limit |
| `policy.timeout_ms` | Per-trial time limit |
| `policy.task_sandbox.network` | `none` or `full` |
| `runtime.agent_runtime.network` | Agent network access |
| `--env KEY=VAL` | Runtime secrets |
| `--env-file .env` | Secrets from file |

**Run outputs** live under `.lab/runs/<run_id>/`:

| File | Content |
|------|---------|
| `trials/<trial_id>/trial_state.json` | Trial status |
| `trials/<trial_id>/out/result.json` | Agent output |
| `facts/trials.jsonl` | All trial records |
| `facts/metrics_long.jsonl` | All metrics |
| `facts/events.jsonl` | Hook events |
