# Harbor Phase 1 Runbook

This runbook covers the implemented Phase 1 path in the AgentLab repository.

## Files Added

1. `scripts/harbor/export_harbor_to_agentlab_jsonl.py`
2. `scripts/harbor/harbor_benchmark_adapter.py`
3. `scripts/harbor/run_terminal_bench2_harbor.sh`
4. `.lab/experiments/terminal_bench2_harbor.yaml`
5. `scripts/harbor/tests/test_export_harbor_to_agentlab_jsonl.py`
6. `scripts/harbor/tests/test_harbor_benchmark_adapter.py`

## Quick Start

1. Build mapped dataset from Harbor task directories:

```bash
python3 scripts/harbor/export_harbor_to_agentlab_jsonl.py \
  --tasks-root /path/to/harbor/tasks \
  --output .lab/experiments/data/terminal_bench2_harbor.task_boundary_v2.jsonl
```

2. Run experiment:

```bash
scripts/harbor/run_terminal_bench2_harbor.sh
```

The wrapper also supports auto-build:

```bash
HARBOR_TASKS_ROOT=/path/to/harbor/tasks \
scripts/harbor/run_terminal_bench2_harbor.sh
```

## Registry Input (Optional)

You can build from a Harbor registry JSON/JSONL:

```bash
python3 scripts/harbor/export_harbor_to_agentlab_jsonl.py \
  --registry-json /path/to/registry.jsonl \
  --registry-root /path/to/tasks/root \
  --output .lab/experiments/data/terminal_bench2_harbor.task_boundary_v2.jsonl
```

## Evaluator Wiring

By default, the adapter uses fallback scoring from AgentLab outputs.

To call an external Harbor evaluator command:

1. Set `HARBOR_EVALUATOR_CMD_JSON` (preferred, JSON array), or
2. Set `HARBOR_EVALUATOR_CMD` (shell string).

Example:

```bash
export HARBOR_EVALUATOR_CMD_JSON='["python3","/path/to/harbor_eval.py"]'
```

The adapter passes:

1. `HARBOR_TASK_PATH`
2. `HARBOR_AGENT_RESULT_PATH`
3. `HARBOR_EVALUATION_OUTPUT_PATH`

Evaluator contract:

1. Exit `0` on success.
2. Emit JSON to stdout (or write JSON to `HARBOR_EVALUATION_OUTPUT_PATH`).
3. JSON may include:
   1. `verdict` (`pass|fail|error`)
   2. `primary_metric_name`
   3. `primary_metric_value`
   4. `metrics` object
   5. `prediction`
   6. `evaluator`

## Tests

Run Harbor Phase 1 tests:

```bash
python3 -m unittest discover -s scripts/harbor/tests -p 'test_*.py'
```
