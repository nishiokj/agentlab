# Clean Harness Example

Reference example for the current `runtime.agent` contract (`version: "0.5"`).

## Files

- `experiment.yaml`: runnable spec using `runtime.agent`.
- `tasks.jsonl`: sample dataset.
- `harness.py`: minimal runtime program.
- `Dockerfile`: container image for the runtime.

## Runtime Contract (Current)

Runner executes one command per trial and appends input/output paths using `runtime.agent.io` (defaults: `--input <path> --output <path>`).

Runner also sets these env vars:

- `AGENTLAB_TASK_PATH`
- `AGENTLAB_BINDINGS_PATH`
- `AGENTLAB_DEPENDENCIES_PATH`
- `AGENTLAB_POLICY_PATH`
- `AGENTLAB_RESULT_PATH`

This example harness supports both:

- `--input/--output` flags (default runner behavior), and
- env fallbacks (`AGENTLAB_TASK_PATH`, `AGENTLAB_RESULT_PATH`).

It also reads variant bindings from `AGENTLAB_BINDINGS_PATH`.

## Variant Behavior

`variant_plan[].bindings` is passed through to the runtime (via `bindings.json`).

In this example:

- `model` and `temperature` bindings control the harness output.
- `variant_plan[].image` demonstrates per-variant image overrides.

## Quick Start

```bash
# Build image
docker build -t my-harness examples/clean_harness/

# Validate the experiment
cd rust
cargo run -p lab-cli -- describe ../examples/clean_harness/experiment.yaml

# Run (container mode)
cargo run -p lab-cli -- run ../examples/clean_harness/experiment.yaml --executor local_docker
```
