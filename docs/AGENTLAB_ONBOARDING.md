# AgentLab Onboarding

This repo is `runtime.agent`-first.

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

Use `runtime.agent` in `experiment.yaml`:

- `runtime.agent.command` (required)
- `runtime.agent.image` (required for container mode)
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
cd rust
cargo run -p lab-cli -- describe ../.lab/experiment.yaml
cargo run -p lab-cli -- run ../.lab/experiment.yaml --executor local_docker
```
