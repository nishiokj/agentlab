# Examples

Use these files as concrete examples before editing a real experiment.

## Small demo authoring file

`../../demos/experiment.yaml`

Why read it:

- small paired experiment
- easy to scan metrics, baseline, and variant structure
- good template for a minimal controlled edit

## Real strict agent-runtime example

`../../.lab/experiments/swebench_lite_curated.yaml`

Why read it:

- strict containerized runtime
- real `baseline` plus `variant_plan`
- useful for paired benchmark comparisons

## Per-task image example

`../../.lab/experiments/bench_v0_per_task.yaml`

Why read it:

- demonstrates `image_source: per_task`
- shows benchmark adapter wiring
- useful when diagnosing container and dataset-specific preflight failures
