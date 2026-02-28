# Task Authoring Guide

## Overview

Each task is a self-contained bundle under `bench/benchmark/tasks/v0/TASK###/` that defines
a coding challenge for agents to solve.

## Task Bundle Structure

```
bench/benchmark/tasks/v0/TASK001/
  task.yaml              # Task metadata and configuration
  issue.md               # Issue description (what the agent sees)
  public/
    repro.md             # Public reproduction steps
    run_public.sh        # Public test command
    public_cases.jsonl   # Optional public test cases
  hidden/
    runner.py            # Benchmark-owned hidden test runner
    cases.jsonl          # >=50 deterministic hidden test cases
    expected.jsonl       # Optional expected outputs
  mutants/
    README.md            # Mutant strategy documentation
    M01.patch            # Mutant patches (>=10 required)
    M02.patch
    ...
    M10.patch
  policy/
    allow_edit_globs.txt # Files agent may edit
    deny_edit_globs.txt  # Files agent must not edit
    allow_run_globs.txt  # Commands agent may run
  private/
    solution.patch       # Reference solution (NEVER mounted to agent)
```

## Writing issue.md

### DO:
- Describe the **symptom** (what's wrong)
- Include **reproduction steps** (commands to run)
- State **acceptance criteria** (expected behavior)
- Use natural language descriptions

### DON'T:
- Include file paths (e.g., "edit `src/parser.py`")
- Include exact fix hints (e.g., "change the return type")
- Include line numbers
- Reference internal implementation details

## Deterministic Case Generation

### Checklist:
- [ ] Use seeded RNG (`bench.taskkit.determinism.get_seeded_rng(42)`)
- [ ] Use `stable_json()` for serialization (sorted keys, no floats)
- [ ] Sort all collections before output
- [ ] No dependency on wall-clock time, timezone, or locale
- [ ] No network access required
- [ ] At least 50 cases covering:
  - Normal inputs
  - Edge cases (empty, zero, boundary values)
  - Error conditions
  - Large inputs

## Writing hidden/runner.py

The hidden runner must:
1. Accept `workspace` and `cases.jsonl` as positional args
2. Import the code under test from the workspace
3. Execute each case deterministically
4. Output JSONL results to stdout with fields:
   - `case_id`: matches the case ID
   - `passed`: boolean
   - `error_type`: null or error classification
   - `error_message`: null or error details
   - `duration_ms`: execution time
   - `output_summary`: brief output description

## Designing Mutants

### Mutant Pattern Checklist:
- [ ] Swallow error (catch and ignore exception)
- [ ] Default return (return a default value instead of computing)
- [ ] Special-case (hardcode result for specific inputs)
- [ ] Weaken validation (skip a validation step)
- [ ] Incorrect boundary (off-by-one, wrong comparison)
- [ ] Skip step (remove a processing step)
- [ ] Wrong type (return wrong type that might pass shallow checks)
- [ ] Off-by-one (loop bounds, index calculations)
- [ ] Missing edge case (skip handling of empty/null/special values)
- [ ] Hardcode value (return a constant instead of computing)

Each mutant should cause **at least 1 hidden case to fail**.
At least 80% of mutant failures should be assertion/test mismatches
(not harness crashes).

## Validation

```bash
# Validate your task
python -m bench.cli validate-task bench/benchmark/tasks/v0/TASK001

# Check the full suite
python -m bench.cli validate-suite v0
```

Validate-task checks:
- Required files exist
- task.yaml validates against schema
- >=50 hidden cases
- >=10 mutant patches
- Issue prompt doesn't leak file paths
- Private solution exists
