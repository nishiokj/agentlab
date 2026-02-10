# Workload Protocol v1 (Draft)

## Goal

Support both agent harness experiments and deep-learning training runs with one runner contract.

## Workload Types

- `agent_harness`
- `trainer`
- `custom` (future extension)

## Shared Trial Contract

The runner contract remains:

- input: `trial_input_v1`
- output: `trial_output_v1`

`trial_input_v1` now carries:

- `workload.type`

`trial_output_v1` now supports:

- `objective` for primary metric
- `metrics` for secondary metrics
- `checkpoints` for checkpoint lineage

## Adapter Manifest

Adapters declare capabilities using:

- `workload_adapter_manifest_v1.jsonschema`

This keeps runner core generic while allowing per-workload execution semantics through adapter metadata.

## Analytics Mapping

For all workload types, runner emits:

- `analysis/tables/trials.jsonl`
- `analysis/tables/metrics_long.jsonl`

For trainer workloads, runner also captures:

- `primary_metric_name`
- `primary_metric_value`
- `workload_type`

in trial rows, and emits primary objective into `metrics_long` with:

- `metric_source = "primary"`

## Non-Goals

- This protocol does not force any specific training framework.
- It does not define distributed training internals.
- It focuses on orchestration, comparability, provenance, and analytics shape.
