# Bench x Experiment Runner Integration

This bridge keeps `bench` task/grading semantics while executing via `lab-cli`.

## Hard Cutover Contract

Bench v0 integration is now hard-cut to `task_boundary_v3` plus split runtime config:

1. dataset rows are `task_boundary_v3`
2. each row must resolve `environment.image`
3. runtime must use `runtime.sandbox.image_source: per_task`
4. runtime must set `runtime.agent.bundle`
5. benchmark adapter support must be staged under `/agentlab/deps`

## Components

1. `bench/integration/agentlab/export_bench_suite_to_jsonl.py`
2. `bench/integration/agentlab/bench_runtime_adapter.py`
3. `bench/integration/agentlab/bench_benchmark_adapter.py`
4. `.lab/experiments/bench_v0_per_task.yaml`
5. `scripts/agentlab/smoke_bench_v0_per_task.sh`

## 1) Export bench tasks to `task_boundary_v3`

All rows must have `environment.image`. Bench v0 task bundles currently do not
embed images in `task.yaml`, so pass a default image at export time.

```bash
python3 bench/integration/agentlab/export_bench_suite_to_jsonl.py \
  --suite v0 \
  --output .lab/experiments/data/bench_v0.task_boundary_v3.jsonl \
  --default-task-image <task-image>
```

Default output (when `--output` is omitted):

1. `data/bench_v0.task_boundary_v3.jsonl`

Each row includes:

1. `schema_version: task_boundary_v3`
2. `task.id`
3. `environment.image`
4. `workspace.mode`
5. `workspace.base`
6. optional `workspace.overlays` and `workspace.aux_mounts`
7. `task.task_dir`
8. `task.benchmark` metadata
9. issue text plus task metadata (`description`, `difficulty`, `tags`) as `task.input.prompt`

## 2) Per-task experiment wiring

Use an external agent bundle plus staged benchmark adapter support:

```yaml
runtime:
  agent:
    bundle: ../agents/agent-runtime.tar.gz
    command:
      - python3
      - /opt/agent/bench/integration/agentlab/bench_runtime_adapter.py
  sandbox:
    executor: docker
    image_source: per_task
    profile: default
    network: none
  dependencies:
    file_staging:
      - source_from_host: ./data/bench_benchmark_adapter_entry.py
        destination_path: /agentlab/deps/bench_benchmark_adapter_entry.py
      - source_from_host: ./data/bench_support.tar.gz
        destination_path: /agentlab/deps/bench_support.tar.gz

benchmark:
  adapter:
    command:
      - python3
      - /agentlab/deps/bench_benchmark_adapter_entry.py
```

Reference experiment: `.lab/experiments/bench_v0_per_task.yaml`.

## 3) Smoke gate (preflight + limit-1 run)

```bash
BENCH_DEFAULT_TASK_IMAGE=<task-image> \
BENCH_AGENT_ARTIFACT=.lab/agents/agent-runtime.tar.gz \
scripts/agentlab/smoke_bench_v0_per_task.sh
```

The smoke script writes `.lab/experiments/data/bench_benchmark_adapter_entry.py`
plus `.lab/experiments/data/bench_support.tar.gz`, then runs the reference
experiment against those staged files.

## 4) Agent command selection

`bench_runtime_adapter.py` resolves the underlying bench-style agent command by:

1. `AGENTLAB_BENCH_AGENT_COMMAND_JSON`
2. `AGENTLAB_BENCH_AGENT_COMMAND`
3. variant bindings `bench_agent_command`
4. variant bindings `agent_command`
5. no fallback beyond explicit command/bindings

The adapter sets `WORKSPACE` and `TASK_ID`, runs the command, reads `patch.diff`,
and emits `agent_result_v1`.

## 5) Grading semantics

`bench_benchmark_adapter.py` uses `bench.taskkit.grading.grade_patch_for_task`.

Verdict mapping:

1. `overall_pass=true` -> `pass`
2. `failure_label=NO_PATCH` -> `missing`
3. other failures -> `fail`
4. grading exceptions -> `error`

This keeps benchmark scoring in bench logic while emitting AgentLab benchmark
records (`benchmark_prediction_record_v1`, `benchmark_score_record_v1`).
