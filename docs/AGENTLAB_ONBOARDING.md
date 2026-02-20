# AgentLab Onboarding

This repo was scaffolded with `lab init`.

## What You Just Got

- `experiment.yaml`
- `tasks.jsonl`

## Mental Model

AgentLab runs an agent runtime command for each task/variant/replication.

- The runner writes `trial_input.json` for a trial.
- Your runtime reads `trial_input.json` and writes `result.json`.
- AgentLab analyzes results and generates a report.

There are currently two CLIs:
- Python `lab` (legacy/dev flows)
- Rust `lab-cli` (primary runtime/runner path)

In local (non-container) mode, the runtime command is executed with:

- CWD set to the trial output directory (`.../.lab/runs/<run_id>/trials/<trial_id>/`).
- Env vars:
  - `AGENTLAB_TRIAL_INPUT`
  - `AGENTLAB_RESULT_PATH`
  - `AGENTLAB_TRAJECTORY_PATH` (optional)
  - task/bindings/dependencies/policy paths
- Control is adapter-owned; runner persists active control metadata in `runtime/run_control.json`.

## `tasks.jsonl`

`tasks.jsonl` is the dataset: one JSON object per line. The object is passed to the runtime as `trial_input.task`.

## How To Try

1. Update `experiment.yaml` to use `runtime.agent` (`known_agent_ref` or `custom_image`) and optional `runtime.agent.adapter` (`builtin.command_contract`, `prebuilt.codex_cli`, `prebuilt.rex_jesus`). You can also use `runtime.agent.command` (string|string[]) plus optional `runtime.agent.aliases` for short commands like `rex`.
2. Implement your runtime command to read `trial_input.json` and write `result.json`.
3. Run (Python): `lab run experiment.yaml`
4. Run (Rust, containerized): `cargo run -p lab-cli -- run experiment.yaml --container`
5. Open: `.lab/runs/<run_id>/report/index.html` (Python only for now)
