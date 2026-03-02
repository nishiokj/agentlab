# Benchmark Namespace

This directory contains the in-house benchmark domain as one coherent unit.

## Benchmark Name

The in-house benchmark is **AgentLab Bench v0**.

It is a deterministic, patch-based coding benchmark centered on task bundles in
`bench/benchmark/tasks/v0/`.

## Scope

In scope:

- Task bundle format and task-set curation.
- Validation/admission gates for tasks.
- Deterministic grading and scoring.
- North-star audit/proof artifacts.

Out of scope:

- Generic experiment runner runtime state (`.lab/...`).
- External benchmark adapters (Harbor / SWE-bench), which are under
  top-level `adapters/`.

## Layout

- `bench/benchmark/tasks/`: curated, importable task sets.
- `bench/benchmark/repos/`: frozen source snapshots used by tasks.
- `bench/benchmark/runs/`: placeholder for run outputs when needed.
- `bench/benchmark/reports/`: placeholder for generated reports when needed.

## Generation vs Task Sets

Generation/validation code lives in the Python package:

- `bench/taskkit/` (import, inject, casegen, mutants, validation).

Created/importable task sets live as data under:

- `bench/benchmark/tasks/`.

## Scope Clarification

This namespace is intentionally limited to benchmark data and task authoring/
validation concerns. Standalone benchmark harness execution is not part of this
namespace; trial execution belongs to the experiment runner.
