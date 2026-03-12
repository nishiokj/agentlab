# AgentLab

AgentLab builds, validates, and runs controlled agent evaluations against task datasets.

The user-facing workflow is:

1. Author an experiment.
2. Provide task data plus an agent runtime artifact and image.
3. Build a runnable package.
4. Preflight the package.
5. Run it.
6. Inspect run outputs with queries and views.

## Boundaries

Keep these boundaries separate:

| Boundary | Owned by | What belongs there |
| --- | --- | --- |
| Experiment authoring | User | `experiment.yaml`, `tasks.jsonl`, variant bindings, relative file refs, `lab build/run` flags |
| Command contract | Runner and agent code | argv, cwd, mounted `/agentlab/*` dirs, contract file paths, `AGENTLAB_*` env vars |
| Run internals | Runner | package layout details, schedule state, sqlite internals, materialization internals |

The experiment YAML is not the same thing as the runtime contract seen by the agent process.

## Mental Model

| Term | Meaning |
| --- | --- |
| `Experiment` | The authoring config you pass to `lab build`. |
| `Task` | One dataset row from `tasks.jsonl`. |
| `Variant` | One set of bindings applied across tasks. |
| `Trial` | One `task x variant x replication` execution unit. |
| `Agent runtime` | The external agent executable and image from `runtime.agent_runtime`. |
| `Task sandbox` | The task/grader plane driven by `task.environment.image` and `policy.task_sandbox`. |
| `Package` | The built, resolved experiment directory produced by `lab build`. |
| `Run` | The execution of a package under `.lab/runs/<run_id>/`. |

## Public Surface

Treat these as the supported operator surfaces:

1. `lab` or `lab-cli` subcommands and their documented behavior.
2. Init scaffolds produced by `lab init`.
3. Schemas in [`schemas/`](schemas/).
4. Run outputs under `.lab/runs/<run_id>/`.
5. The documented command-contract paths and env vars in this README.

Do not build tooling against undocumented package internals.

## Quickstart

If you are building the Rust CLI from this repo, the produced binary is `lab-cli`:

```bash
cargo build --manifest-path rust/Cargo.toml -p lab-cli --release
LAB="$(pwd)/rust/target/release/lab-cli"
```

If you already have an installed `lab` wrapper, use that instead.

Create a new project directory and scaffold an experiment:

```bash
mkdir -p /tmp/agentlab-demo
cd /tmp/agentlab-demo

"$LAB" init --profile agent-eval --in-place
```

Then edit the scaffold and run the workflow:

```bash
$EDITOR experiment.yaml
$EDITOR tasks.jsonl

"$LAB" build experiment.yaml --out .lab/builds/demo
"$LAB" describe .lab/builds/demo
"$LAB" preflight .lab/builds/demo
"$LAB" run .lab/builds/demo --materialize full
```

If your agent needs launch-time secrets or runtime bindings:

```bash
"$LAB" preflight .lab/builds/demo \
  --env OPENAI_API_KEY=... \
  --env-file .env

"$LAB" run .lab/builds/demo \
  --env OPENAI_API_KEY=... \
  --env-file .env \
  --materialize full
```

## Init Profiles

Current built-in profiles:

- `agent-eval`: single-variant isolated agent evaluation
- `ab-test`: paired two-variant comparison
- `sweep`: independent parameter sweep
- `regression`: fixed-suite pass-rate tracking

List them with:

```bash
"$LAB" init
```

## Constructing An Experiment

Start from the scaffold:

```bash
"$LAB" init --profile agent-eval --in-place
```

Current `agent-eval` scaffolds produce:

```yaml
experiment:
  id: my_eval
  name: My Agent Evaluation
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  split_id: dev
  limit: 50
design:
  sanitization_profile: hermetic_functional
  comparison: paired
  replications: 3
  random_seed: 42
  shuffle_tasks: true
  max_concurrency: 1
baseline:
  variant_id: control
  bindings: {}
variant_plan: []
runtime:
  agent_runtime:
    command: [python, harness.py]
    artifact: ./agents/my-agent-runtime.tar.gz
    image: ghcr.io/acme/agent-runtime:latest
    network: none
    root_read_only: true
policy:
  timeout_ms: 300000
  task_sandbox:
    profile: default
    network: none
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
```

The fields most users need to change first are:

1. `dataset.path`
2. `runtime.agent_runtime.artifact`
3. `runtime.agent_runtime.image`
4. `runtime.agent_runtime.command`
5. `baseline.bindings`
6. `variant_plan[*].bindings`
7. `policy.timeout_ms`

Add `runtime.agent_runtime.env` only when the agent actually needs environment variables.

## Authoring Rules

Keep the experiment YAML on the authoring side of the boundary:

- Put public argv directly in `runtime.agent_runtime.command`.
- Put public env directly in `runtime.agent_runtime.env`.
- Plain relative paths in command/env are build-time file refs resolved from the experiment root.
- `$NAME` means a runtime binding resolved from variant bindings first, then `lab run --env/--env-file`, then host env.
- Do not author `/agentlab/...` paths in the YAML.
- Do not hand-author package internals or run-directory internals.

Example:

```yaml
baseline:
  variant_id: control
  bindings:
    MODEL: gpt-4.1-mini
variant_plan:
  - variant_id: higher_temp
    bindings:
      MODEL: gpt-4.1-mini
      TEMPERATURE: "0.7"
runtime:
  agent_runtime:
    command:
      - python
      - harness.py
      - --model
      - $MODEL
      - --prompt-file
      - prompts/system.txt
    env:
      OPENAI_API_KEY: $OPENAI_API_KEY
      TEMPERATURE: $TEMPERATURE
      POLICY_FILE: configs/policy.json
    artifact: ./agents/my-agent-runtime.tar.gz
    image: ghcr.io/acme/agent-runtime:latest
    network: none
    root_read_only: true
```

Interpretation:

- `prompts/system.txt` and `configs/policy.json` are experiment-relative files staged at build time.
- `$MODEL` comes from variant bindings.
- `$OPENAI_API_KEY` comes from launch-time env.

## Writing Tasks

Task datasets are JSONL. Each row is the current task contract.

Minimal task row:

```json
{
  "task": {
    "id": "TASK001",
    "prompt": "Fix the failing test."
  },
  "environment": {
    "image": "ghcr.io/acme/task-image:latest"
  },
  "workspace": {
    "mode": "scratch",
    "base": { "kind": "empty" },
    "overlays": [],
    "aux_mounts": []
  },
  "dependencies": {
    "files": []
  },
  "limits": {}
}
```

Rules that matter:

1. Allowed top-level keys are `task`, `environment`, `workspace`, `dependencies`, `limits`.
2. `environment.image` is required for task-sandbox execution.
3. `workspace.mode = "patch"` requires a real base such as `dataset_pack` or `git_checkout`.
4. `workspace.base.kind = "empty"` does not allow `dataset_pack_ref`, `repo`, or `commit`.
5. `workspace.aux_mounts[*].mount_path` must stay under `/agentlab/workspace/...`.
6. Tasks declare logical content. The runner owns physical sandbox topology.

Common workspace bases:

- `empty`: start from an empty workspace
- `dataset_pack`: materialize a staged dataset pack by digest
- `git_checkout`: materialize a repo checkout at a specific commit

## What The Agent Process Sees

For the current command-contract runtime, the runner launches the agent container with:

- cwd set to `/agentlab/workspace`
- mounted contract dirs:
  - `/agentlab/in`
  - `/agentlab/out`
  - `/agentlab/state`
  - `/agentlab/workspace`
  - `/agentlab/deps`
- contract env vars such as:
  - `AGENTLAB_TASK_PATH`
  - `AGENTLAB_BINDINGS_PATH`
  - `AGENTLAB_POLICY_PATH`
  - `AGENTLAB_RESULT_PATH`
  - `AGENTLAB_TRAJECTORY_PATH`
  - `AGENTLAB_RUN_ID`
  - `AGENTLAB_TRIAL_ID`
  - `AGENTLAB_VARIANT_ID`
  - `AGENTLAB_TASK_ID`
  - `AGENTLAB_TIMEOUT_MS`

Current runtime also exports `WORKSPACE=/agentlab/workspace` as a convenience env for agent code.
Treat cwd as the primary contract, and keep `WORKSPACE` out of experiment authoring.

This means:

- Agent code may consume the documented `AGENTLAB_*` handles at runtime.
- Experiment YAML should not try to describe runner topology directly.
- A generic CLI tool will only work if your command or wrapper knows how to consume the command contract.

## Build, Preflight, Run

Use these commands in order:

1. `lab build <experiment.yaml> --out <package_dir>`
2. `lab describe <package_dir>`
3. `lab preflight <package_dir>`
4. `lab run <package_dir> --materialize full`

For a one-step flow:

```bash
"$LAB" build-run experiment.yaml --out .lab/builds/demo --materialize full
```

`preflight` is the right place to catch missing artifacts, unavailable images, and missing launch-time env before starting a scientific run.

## Run Outputs

Run data lives under:

```text
.lab/runs/<run_id>/
```

High-signal outputs:

- `resolved_experiment.json`
- `runtime/run_control.json`
- `trials/<trial_id>/trial_state.json`
- `trials/<trial_id>/out/result.json`
- `facts/run_manifest.json`
- `facts/trials.jsonl`
- `facts/events.jsonl`
- `facts/metrics_long.jsonl`

Useful inspection commands:

```bash
"$LAB" runs
"$LAB" query <run_id> "SELECT * FROM trials LIMIT 20"
"$LAB" views <run_id>
"$LAB" variants <run_id>
```

## Removed Or Legacy Inputs

These are removed and should not appear in new experiments, tasks, docs, or happy-path tests:

- `version`
- `dataset.schema_version`
- `task.schema_version`
- `runtime.agent`
- `runtime.sandbox`
- `runtime.agent_runtime.io`
- `runtime.agent_runtime.workspace_patches`
- `runtime.dependencies.file_staging`
- `env_from_host`
- `binding_args`
- `support_files`
- `provider_env`
- `default_config`
- `config_files`

## Testing And Debugging

For testing standards and the E2E boundary rules, see [TESTS.md](TESTS.md).
