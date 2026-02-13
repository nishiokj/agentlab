# AgentLab Onboarding

This repo was scaffolded with `lab init`.

## What You Just Got

- `experiment.yaml`
- `tasks.jsonl`
- `harness_manifest.json`

## Mental Model

AgentLab runs a *harness* (your program) for each task/variant/replication.

- The runner writes `trial_input.json` for a trial.
- Your harness reads `trial_input.json` and writes `trial_output.json`.
- AgentLab analyzes results and generates a report.

There are currently two CLIs:
- Python `lab` (default, installed via `pip install -e .`)
- Rust `lab-cli` (experimental, run via `cargo`)

In local (non-container) mode, the harness is executed with:

- CWD set to the trial output directory (`.../.lab/runs/<run_id>/trials/<trial_id>/`).
- Env vars:
  - `AGENTLAB_CONTROL_PATH` = transport path (`/run/ipc/harness.sock` for UDS by default)
  - `AGENTLAB_CONTROL_MODE` = `uds` or `file`
  - `AGENTLAB_HARNESS_ROOT` = experiment root mounted as `/harness` in container mode
- `trial_input_path` and `trial_output_path` are provided by `/runtime/harness/input_path` and `/runtime/harness/output_path` and are mounted inside the trial context.

## `tasks.jsonl`

`tasks.jsonl` is the dataset: one JSON object per line. The object is passed to the harness as `trial_input.task`.

## `harness_manifest.json`

This is *metadata about your harness* (integration level, step semantics, identity).
In richer integration modes (`cli_events`, `otel`), the framework requires the harness to emit a `harness_manifest.json` per-trial (or copy a static one into the trial output directory).

Start with `cli_basic` if you're just trying things out.

## How To Try

1. Update `experiment.yaml` to point `runtime.harness.command` at your harness CLI (or re-run `lab init --demo-harness node`).
2. Implement the harness to read `trial_input.json` and write `trial_output.json`.
3. Run (Python): `lab run experiment.yaml`
4. Run (Rust, containerized): `cargo run -p lab-cli -- run experiment.yaml --container`
5. Open: `.lab/runs/<run_id>/report/index.html` (Python only for now)
