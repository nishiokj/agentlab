# Bench x Experiment Runner Integration

This bridge keeps `bench` task/grading semantics while executing via `lab-cli`.

## Hard Cutover Contract

Bench v0 integration is now hard-cut to per-task container mode:

1. dataset rows are `task_boundary_v2` only
2. each row must resolve `task.image`
3. runtime must use `runtime.agent.image_source: per_task`
4. runtime must set `runtime.agent.artifact`

## Components

1. `bench/integration/agentlab/export_bench_suite_to_jsonl.py`
2. `bench/integration/agentlab/bench_runtime_adapter.py`
3. `bench/integration/agentlab/bench_benchmark_adapter.py`
4. `.lab/experiments/bench_v0_per_task.yaml`
5. `scripts/agentlab/smoke_bench_v0_per_task.sh`

## 1) Export bench tasks to `task_boundary_v2`

All rows must have `task.image`. Bench v0 task bundles currently do not embed
images in `task.yaml`, so pass a default image at export time.

```bash
python3 bench/integration/agentlab/export_bench_suite_to_jsonl.py \
  --suite v0 \
  --output .lab/experiments/data/bench_v0.task_boundary_v2.jsonl \
  --default-task-image <task-image> \
  --default-task-workspace /agentlab/workspace
```

Default output (when `--output` is omitted):

1. `data/bench_v0.task_boundary_v2.jsonl`

Each row includes:

1. `schema_version: task_boundary_v2`
2. `task.id`
3. `task.image`
4. optional `task.workspace`
5. `task.task_dir`
6. `task.benchmark` metadata
7. issue text plus task metadata (`description`, `difficulty`, `tags`) as `task.input.prompt`

## 2) Per-task experiment wiring

Use injected artifact paths in container mode:

```yaml
runtime:
  agent:
    image_source: per_task
    artifact: ../agents/agent-runtime.tar.gz
    command:
      - python3
      - /opt/agent/bench/integration/agentlab/bench_runtime_adapter.py

benchmark:
  adapter:
    command:
      - python3
      - /opt/agent/bench/integration/agentlab/bench_benchmark_adapter.py
```

Reference experiment: `.lab/experiments/bench_v0_per_task.yaml`.

## 3) Smoke gate (preflight + limit-1 run)

```bash
BENCH_DEFAULT_TASK_IMAGE=<task-image> \
BENCH_AGENT_ARTIFACT=.lab/agents/agent-runtime.tar.gz \
scripts/agentlab/smoke_bench_v0_per_task.sh
```

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
