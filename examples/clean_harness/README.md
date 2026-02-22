# Clean Harness Boundary

This is the north-star reference for the experiment contract. A harness is just a program — it reads a task, does work, writes a result.

## The Contract

### What you provide

| File | Purpose |
|---|---|
| `experiment.yaml` | Declares the experiment: dataset, design, variants, runtime defaults |
| `tasks.jsonl` | Your dataset — pure domain data, no runner metadata |
| `harness.py` | Your evaluation program |
| `Dockerfile` | Packages your harness into a container image |

### What the runner does (your harness doesn't care)

1. Mounts `/in/task.json` (read-only) — one file, the current task payload
2. Mounts `/out/` (read-write) — your harness writes `result.json` here
3. Constructs the invocation: `{command} {variant_args} /in/task.json /out/result.json`
4. Applies image overrides, env overrides, resource limits, network policy
5. Reads `/out/result.json` after the harness exits
6. Records trial metadata and runs analysis — all outside the container

### What your harness does

1. Parses its own CLI args (temperature, model, whatever it accepts)
2. Reads the task from the input path (positional arg or `/in/task.json`)
3. Does work
4. Writes result to the output path

### Runner invocation (what actually happens)

```
docker run --rm \
  --network=none \
  --read-only \
  --cpus=2 --memory=2048m \
  -v {task_file}:/in/task.json:ro \
  -v {out_dir}:/out \
  -e DEBUG=1 \                          # only if variant specifies env
  my-harness:latest \
  python harness.py \
  --temperature 0.9 \                   # variant args injected here
  /in/task.json /out/result.json        # runner always appends these
```

## Result Schema

Write `/out/result.json`. Only `outcome` is required:

```json
{
  "outcome": "success",
  "objective": {"name": "accuracy", "value": 0.95},
  "metrics": {"latency_ms": 1200, "tokens": 450}
}
```

- `outcome`: `"success"` | `"failure"` | `"error"`
- `objective`: primary metric the analysis keys on (optional)
- `metrics`: arbitrary key-value bag for secondary measurements (optional)

## How Variants Work

Variants express what changes between experimental conditions. The runner applies them at the invocation layer — the harness just sees different CLI args or runs as a different image.

| What varies | How it's expressed | Runner action |
|---|---|---|
| A hyperparameter | `args: [--temperature, "0.9"]` | Appends to command |
| The model | `args: [--model, claude-4]` | Appends to command |
| The harness itself | `image: harness-v2:latest` | Different container image |
| An API key or flag | `env: {API_KEY: "..."}` | Sets container env var |
| Multiple things | Combine `image` + `args` + `env` | All applied together |

The `control` variant (baseline) uses the runtime defaults with no overrides.

## Anti-Patterns

**Reading `AGENTLAB_*` env vars in the harness.**
Your harness shouldn't know the runner exists. If it needs configuration, accept it as a CLI argument.

**Unpacking "bindings" inside the harness.**
If you want `--temperature 0.9`, say `args: [--temperature, "0.9"]` in the variant plan. Don't indirect through a bindings map that the harness must understand.

**Specifying `io.input_arg` / `io.output_arg` in the experiment config.**
The runner always appends input and output as the last two positional args. The harness parses them. There's nothing to configure.

**Putting runner operational concerns in the experiment file.**
Fields like `telemetry`, `integration_level`, `control_plane`, `validity`, `sanitization_profile`, `shuffle_tasks`, `max_concurrency` — these are runner internals, not experiment design decisions. They don't belong in the contract the user writes.

**Multiple mount points and decomposed input files.**
The harness sees two paths: `/in/task.json` (its input) and `/out/` (where it writes). That's it — no experiment metadata, variant configs, or runner state mounted into the container.

## Quick Start

```bash
# Build your harness image
docker build -t my-harness examples/clean_harness/

# Run it locally to test (no runner needed)
echo '{"id":"test","prompt":"Say hello"}' > /tmp/task.json
mkdir -p /tmp/out
docker run --rm \
  -v /tmp/task.json:/in/task.json:ro \
  -v /tmp/out:/out \
  my-harness \
  --temperature 0.5 \
  /in/task.json /out/result.json

cat /tmp/out/result.json
```
