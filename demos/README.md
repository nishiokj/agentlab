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
scripts/lab-cli-fresh.sh build demos/experiment.yaml --out .lab/builds/demo --json
scripts/lab-cli-fresh.sh describe .lab/builds/demo --json
scripts/lab-cli-fresh.sh run .lab/builds/demo --materialize full --json
```

Or build and run in one step:

```bash
scripts/lab-cli-fresh.sh build-run demos/experiment.yaml --out .lab/builds/demo --materialize full --json
```

The current runner uses Docker for the task sandbox, so Docker or OrbStack must be running and able to provide the `node:20-alpine` image.
