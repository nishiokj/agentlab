# Benchmark Adapter Protocol v1 (Draft)

## Purpose

Define one integration contract that works for:

- open-source benchmark suites with official evaluators (for example SWE-bench Lite),
- internal/private benchmark suites,
- simple JSONL task sets.

The protocol keeps scoring semantics owned by the benchmark while AgentLab owns:

- containerized execution,
- trial scheduling and provenance,
- telemetry ingestion (hooks/traces),
- analytics tables and reporting.

## Core Principles

1. Separate `predict` from `score`.
2. Treat benchmark evaluator as source of truth when official tooling exists.
3. Keep benchmark-specific logic in adapters, not in runner core.
4. Preserve full provenance for benchmark identity, adapter version, and evaluator version.

## Adapter Lifecycle

Every adapter implements a 4-step lifecycle.

1. `prepare`
- Resolves benchmark metadata and task inventory.
- Emits task records consumable by trial scheduling.

2. `predict`
- Executes harness on each task.
- Produces one prediction record per trial.
- Does not assign benchmark truth labels.

3. `score`
- Invokes benchmark scoring logic (official or configured custom evaluator).
- Produces one score record per trial.

4. `aggregate`
- Reduces score records into benchmark summary metrics.
- Writes analysis-friendly artifacts for run/report.

## Standard Artifacts

Adapter outputs live under run scope:

- `.lab/runs/<run_id>/benchmark/adapter_manifest.json`
- `.lab/runs/<run_id>/benchmark/predictions.jsonl`
- `.lab/runs/<run_id>/benchmark/scores.jsonl`
- `.lab/runs/<run_id>/benchmark/summary.json`

Optional:

- `.lab/runs/<run_id>/benchmark/tasks.jsonl`
- `.lab/runs/<run_id>/benchmark/evaluator_logs/...`

## Required Schemas

Protocol v1 uses these schemas:

- `benchmark_adapter_manifest_v1.jsonschema`
- `benchmark_prediction_record_v1.jsonschema`
- `benchmark_score_record_v1.jsonschema`

## Execution Modes

`execution_mode` in adapter manifest:

- `predict_then_score` (recommended default)
- `integrated_score` (adapter computes score inline, used when benchmark has no external evaluator)

When official evaluator exists, adapters should use `predict_then_score` and set:

- `evaluator.mode = official`

## Identity and Provenance Requirements

Adapter manifest must include:

- adapter identity (`adapter_id`, `adapter_version`),
- benchmark identity (`benchmark.name`, `benchmark.version`, `benchmark.split`),
- evaluator identity,
- schema versions for prediction and score records.

Each score record must include:

- trial ids,
- evaluator metadata,
- primary metric and verdict,
- artifact references used for auditing.

## Trial-Level Mapping

Adapter records map to trial ids already used by AgentLab:

- `run_id`, `trial_id`, `variant_id`, `task_id`, `repl_idx`

This keeps joins deterministic across:

- runner trial tables (`analysis/tables/trials.jsonl`),
- hook event tables (`event_counts_*`),
- benchmark outputs (`predictions.jsonl`, `scores.jsonl`).

## Analytics Contract

### Minimum analytics-ready fields

Prediction records:

- ids
- prediction payload or artifact ref
- lightweight generation metrics (optional)

Score records:

- ids
- `verdict`
- `primary_metric_name`
- `primary_metric_value`
- `metrics` object for additional benchmark metrics

### Recommended derived views

When loading into DuckDB, create:

- `benchmark_predictions`
- `benchmark_scores`
- `benchmark_variant_summary`

`benchmark_variant_summary` should aggregate:

- pass rate (`verdict = pass`),
- mean/median primary metric,
- missing/error rates,
- joinable keys back to trial/event/token tables.

## Benchmark Governance Rules

Adapters must state:

- benchmark license/terms reference,
- evaluator source and version pinning strategy,
- split policy (`dev`, `validation`, `test`, internal),
- allowed tuning policy for each split.

## Compatibility Policy

Protocol versions are explicit:

- `schema_version` in every adapter artifact.
- unknown major versions must fail validation.
- minor additions must be backward compatible.

## Example: SWE-bench Lite Mapping

1. `prepare`: enumerate SWE tasks and fixed repo commits.
2. `predict`: harness emits candidate patch per trial.
3. `score`: official SWE evaluator applies patch and executes tests.
4. `scores.jsonl`: include `resolved` as primary metric and `verdict` from evaluator output.

This keeps benchmark truth in SWE evaluator while AgentLab handles run discipline and analytics.
