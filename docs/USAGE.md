# Benchmark Usage Guide

## Scope

This guide covers task-set generation and validation only.
Trial execution and run orchestration are handled by the experiment runner.

## Prerequisites

- Python 3.11.x

## Quick Start

```bash
# 1. Bootstrap development environment
make bootstrap

# 2. Validate schemas
python -m bench.cli validate-schemas

# 3. Validate a single task
python -m bench.cli validate-task bench/benchmark/tasks/v0/TASK001 --strict

# 4. Validate entire suite
python -m bench.cli validate-suite v0 --strict --repeat 5 --check-determinism

# 5. Admit a task
python -m bench.cli admit-task bench/benchmark/tasks/v0/TASK001
```

## Commands

- `validate-schemas`: validate JSON schemas in `schemas/`
- `validate-task`: run task-level checks and strict gates
- `validate-suite`: run suite-level validation and determinism replay
- `import-suite`: import external tasks into canonical benchmark layout
- `admit-task`: fail-closed admission gate for strict tasks
- `new-task`: scaffold a new task from template
- `suite-summary`: write suite summary JSON
