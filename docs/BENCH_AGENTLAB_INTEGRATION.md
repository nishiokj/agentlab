# Bench x Experiment Runner Integration

This bridge keeps `bench` task/grading semantics while executing via `lab-cli`.

## What was added

1. `bench/integration/agentlab/export_bench_suite_to_jsonl.py`
2. `bench/integration/agentlab/bench_runtime_adapter.py`
3. `bench/integration/agentlab/bench_benchmark_adapter.py`

## 1) Export bench tasks to AgentLab dataset

```bash
python3 bench/integration/agentlab/export_bench_suite_to_jsonl.py --suite v0
```

Default output:

- `data/bench_v0_task_boundary_v1.jsonl`

Each row includes:

1. `task.id`
2. `task.task_dir`
3. `task.benchmark` metadata
4. issue text as `task.input.prompt`

## 2) Experiment wiring

Use `bench_runtime_adapter.py` as `runtime.agent.command` and
`bench_benchmark_adapter.py` as `benchmark.adapter.command`.

Example command snippets in experiment YAML:

```yaml
runtime:
  agent:
    command:
      - python3
      - /opt/agent/bench/integration/agentlab/bench_runtime_adapter.py

benchmark:
  adapter:
    command:
      - python3
      - /opt/agent/bench/integration/agentlab/bench_benchmark_adapter.py
```

For local executor, use absolute host paths instead of `/opt/agent/...`.

## 3) Agent command selection

`bench_runtime_adapter.py` resolves the underlying bench-style agent command by:

1. `AGENTLAB_BENCH_AGENT_COMMAND_JSON`
2. `AGENTLAB_BENCH_AGENT_COMMAND`
3. variant bindings `bench_agent_command`
4. variant bindings `agent_command`
5. no fallback beyond explicit command/bindings

The adapter sets `WORKSPACE` and `TASK_ID`, runs the command, reads `patch.diff`,
and emits `agent_result_v1`.

## 4) Grading semantics

`bench_benchmark_adapter.py` uses `bench.taskkit.grading.grade_patch_for_task`.

Verdict mapping:

1. `overall_pass=true` -> `pass`
2. `failure_label=NO_PATCH` -> `missing`
3. other failures -> `fail`
4. grading exceptions -> `error`

This keeps benchmark scoring in bench logic while emitting AgentLab benchmark
records (`benchmark_prediction_record_v1`, `benchmark_score_record_v1`).
