# Harbor Phase 2 Runbook

This runbook covers the hardening additions for Harbor integration.

For compatibility monitoring lanes, see `docs/HARBOR_PHASE3_RUNBOOK.md`.

## What Phase 2 Added

1. Per-task image mode coverage:
   1. `.lab/experiments/terminal_bench2_harbor_per_task.yaml`
   2. exporter guardrails for `task.image`
2. One-task smoke fixture:
   1. `scripts/harbor/fixtures/tb2_smoke_task/task.toml`
   2. `.lab/experiments/terminal_bench2_harbor_smoke.yaml`
   3. `scripts/harbor/smoke_terminal_bench2_harbor.sh`
3. Strict adapter error taxonomy:
   1. `adapters/harbor/harbor_benchmark_adapter.py`

## Per-Task Image Dataset Build

Use `--require-task-image` to fail fast when any task does not define `task.image`:

```bash
python3 adapters/harbor/export_harbor_to_agentlab_jsonl.py \
  --tasks-root /path/to/harbor/tasks \
  --output .lab/experiments/data/terminal_bench2_harbor.task_boundary_v2.jsonl \
  --require-task-image
```

If your Harbor tasks are missing image fields and you want a default:

```bash
python3 adapters/harbor/export_harbor_to_agentlab_jsonl.py \
  --tasks-root /path/to/harbor/tasks \
  --output .lab/experiments/data/terminal_bench2_harbor.task_boundary_v2.jsonl \
  --default-task-image python:3.11-slim
```

## Smoke Fixture

Build + describe smoke and per-task experiments:

```bash
scripts/harbor/smoke_terminal_bench2_harbor.sh
```

Run the smoke experiment end-to-end:

```bash
HARBOR_SMOKE_RUN=1 scripts/harbor/smoke_terminal_bench2_harbor.sh
```

Run per-task experiment end-to-end (requires artifact tarball):

```bash
HARBOR_SMOKE_RUN_PER_TASK=1 \
HARBOR_AGENT_ARTIFACT=.lab/agents/agent-runtime.tar.gz \
scripts/harbor/smoke_terminal_bench2_harbor.sh
```

## Adapter Error Taxonomy

`harbor_benchmark_adapter.py` now emits typed failure codes on stderr:

1. `config.missing_env` (exit 21)
2. `config.invalid_evaluator_cmd_json` (exit 21)
3. `config.invalid_evaluator_cmd` (exit 21)
4. `io.file_not_found` (exit 22)
5. `io.invalid_json` (exit 22)
6. `io.write_failed` (exit 22)
7. `evaluator.command_failed` (exit 23)
8. `evaluator.invalid_json` (exit 24)
9. `evaluator.missing_output` (exit 24)
10. `evaluator.invalid_payload` (exit 24)
11. `internal.unhandled` (exit 99)

Example stderr line:

```text
harbor_benchmark_adapter.py error_code=evaluator.command_failed message=...
```

## Harbor Wrapper Updates

`scripts/harbor/run_terminal_bench2_harbor.sh` now supports:

1. `PYTHON_BIN`
2. `HARBOR_REQUIRE_TASK_IMAGE=1`
3. `HARBOR_DEFAULT_TASK_IMAGE=<image>`
4. `HARBOR_DEFAULT_TASK_WORKSPACE=<path>`

## Tests

```bash
python3 -m unittest discover -s scripts/harbor/tests -p 'test_*.py'
```
