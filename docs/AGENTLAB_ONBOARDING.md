# AgentLab Onboarding

This repo is `runtime.agent` plus `runtime.sandbox`.

## Starter File

- `experiment.yaml`

## Mental Model

For each task/variant/replication, runner:

1. materializes trial input files,
2. executes one runtime command,
3. reads `agent_result_v1`, and
4. appends run facts/evidence.

Your runtime program should:

1. read task/bindings/dependencies/policy inputs,
2. run autonomously,
3. write `agent_result_v1` to the output path.

## Runtime Contract

Use the split hard-cut contract in `experiment.yaml`:

- `runtime.agent.bundle` (required)
- `runtime.agent.command` (required)
- `runtime.sandbox.executor: docker` (required)
- `runtime.sandbox.image_source` plus `runtime.sandbox.image` for global-image mode
- `runtime.sandbox.profile` and `runtime.sandbox.network`
- optional `runtime.agent.io.input_arg` / `output_arg`
- optional `runtime.agent.env` / `env_from_host`

Runner env vars include:

- `AGENTLAB_TASK_PATH`
- `AGENTLAB_BINDINGS_PATH`
- `AGENTLAB_DEPENDENCIES_PATH`
- `AGENTLAB_POLICY_PATH`
- `AGENTLAB_RESULT_PATH`
- `AGENTLAB_TRAJECTORY_PATH`

## Try It

```bash
# from repository root
cargo build --manifest-path rust/Cargo.toml -p lab-cli --release
rust/target/release/lab-cli preflight .lab/experiment.yaml
rust/target/release/lab-cli describe .lab/experiment.yaml --json

# docker path
rust/target/release/lab-cli run .lab/experiment.yaml --executor local_docker --json

# fallback when Docker is unavailable for the external agent runtime
rust/target/release/lab-cli run .lab/experiment.yaml --executor local_process --json
```

If local-process execution fails with `No such file or directory (os error 2)` and your experiment uses `python`, switch the command to `python3` in `.lab/experiment.yaml`.
