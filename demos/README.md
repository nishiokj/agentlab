# Demos

## Real Small Problem Set: SWE-bench Mini

This folder includes a tiny, real issue set derived from SWE-bench task instances:

- `swebench_mini_tasks.jsonl`: 4 Astropy issue summaries with expected difficulty labels.
- `experiment.yaml`: runner config wired for event/output/artifact metrics.
- `agentlab_demo_harness.js`: sample harness that emits hook events, response metrics, and workspace artifacts.

## Why this is useful for harness testing

It exercises all three metric sources with a tiny runtime footprint:

- `events`: token counts and turn counts from `harness_events.jsonl`
- `output`: correctness and response metrics from `trial_output.json`
- `artifacts`: diff/file-count metrics from files written under `workspace/artifacts/`

## Run

From repo root:

```bash
./lab describe demos/experiment.yaml --json
./lab run demos/experiment.yaml --json
```

If your `lab` binary is not in repo root, pass `--runner-bin` via SDK or set `AGENTLAB_RUNNER_BIN`.
