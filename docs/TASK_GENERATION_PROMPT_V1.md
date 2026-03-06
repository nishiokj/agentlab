# V1 Task Generation Prompt

> Status: Reference input for migration. Execute from `docs/V1_BENCHMARK_MIGRATION_PLAN.md`.


You are generating a self-contained benchmark task for evaluating coding agents. Each task is a complete package: a synthetic codebase, a bug or missing feature, a test suite that validates fixes, and mutation patches that prove the test suite is robust.

This document is your complete specification. Follow it exactly.

---

## 1. What You Are Producing

A task bundle is a directory with this exact structure:

```
TASK###/
  task.yaml                    # Task metadata
  issue.md                     # Problem description (what the agent sees)
  injection.patch              # Patch that creates the broken/incomplete starting state
  repo_source/                 # The synthetic codebase (you create this)
    src/
      __init__.py
      module_a.py              # At least 3 modules with cross-imports
      module_b.py
      module_c.py
      ...
    tests/
      test_public.py           # Subset of tests the agent can see
    README.md
  public/
    repro.md                   # Reproduction steps
    run_public.sh              # Public test command (must actually test something)
  hidden/
    runner.py                  # Benchmark-owned test harness
    cases.jsonl                # >= 50 test cases with input_data AND expected
    reference/                 # Optional reference files the runner needs
  mutants/
    README.md                  # Documents each mutant's strategy
    M01.patch                  # >= 10 mutant patches
    M02.patch
    ...
    M10.patch
  policy/
    allow_edit_globs.txt       # Files the agent may edit (one glob per line)
    deny_edit_globs.txt        # Files the agent must not edit (one glob per line)
  private/
    solution.patch             # The correct fix (never shown to agent)
```

After you produce this bundle, the repo_source/ directory will be packed into a compressed archive:
```bash
cd TASK###/repo_source && tar --zstd -cf ../../repos/<repo_id>/src.tar.zst .
```

---

## 2. Task Types

Each task has a `task_type` field and may include a `task_profile` block for type-specific difficulty shape.

### 2a. `bugfix`

**What it is:** A bug exists in the codebase. The agent must find and fix it.

**How to construct:**
1. Write the codebase in its CORRECT state first. This is your repo_source/.
2. Write `private/solution.patch` — this is the diff from broken → fixed.
3. Write `injection.patch` — this is the diff from correct → broken. It is the INVERSE of solution.patch. When injection.patch is applied to the correct codebase, it produces the broken state the agent starts with.
4. The agent sees the broken state. They must produce a patch equivalent to solution.patch.

**Bugfix profiles (`task_profile.bugfix_profile`):**
- `standard` (default): normal diagnosis path.
- `deep_diagnosis`: symptom is intentionally separated from root cause by >= 2 module hops. This replaces the old `multi_file_debug` type.
- Rationale: many hard real bugs have small patches but expensive diagnosis. Profiled bugfix keeps this as first-class difficulty without a separate task type.

**Difficulty calibration:**
- The bug must require understanding control flow across >= 2 branches, OR data flow across >= 2 modules, OR interaction across >= 2 functions.
- A one-line constant flip, string edit, or variable rename is NOT sufficient.
- For `standard`, solution.patch should touch >= 4 non-blank lines AND (>= 2 hunks OR >= 2 files OR >= 8 lines).
- For `deep_diagnosis`, smaller patches are allowed, but root cause distance must be >= 2 modules and the issue must describe only the symptom.

### 2b. `feature`

**What it is:** A feature is missing. The agent must implement it from a spec.

**How to construct:**
1. Write the codebase WITH the feature implemented. This is your repo_source/.
2. Write `injection.patch` — removes/stubs the feature (e.g., replaces the implementation with `raise NotImplementedError` or deletes the function body).
3. Write `private/solution.patch` — re-implements the feature (inverse of injection.patch).
4. The issue.md describes the desired feature behavior without revealing implementation details.

**Difficulty calibration:**
- The feature must require implementing logic across >= 2 functions or modules.
- solution.patch must touch >= 8 non-blank lines.

### 2c. `refactor`

**What it is:** The code works but has structural problems (duplication, god classes, tight coupling). The agent must restructure it while preserving behavior.

**How to construct:**
1. Write the codebase in its REFACTORED (clean) state. This is your repo_source/.
2. Write `injection.patch` — applies the bad structure (introduces duplication, inlines abstractions, merges classes).
3. Write `private/solution.patch` — refactors back to clean state.
4. The test suite validates BEHAVIOR, not structure. The same tests must pass before and after refactoring.

**Difficulty calibration:**
- The refactoring must affect >= 2 files.
- solution.patch must touch >= 4 non-blank lines.
- The refactoring must involve a recognizable pattern (e.g., extract method, strategy pattern, remove duplication).

### 2d. `agentic_search`

**What it is:** The agent receives fuzzy instructions and must search the codebase to find specific information.

**How to construct:**
1. Write a codebase with 10+ files containing scattered information.
2. The answer is a file the agent must create (e.g., `answer.md` or `findings.json`) listing what they found.
3. `injection.patch` removes the answer file. `solution.patch` creates it.
4. The issue.md gives fuzzy natural-language instructions about what to find. It may reference concepts but NOT specific file paths.
5. The hidden test suite validates the answer file's content against expected findings.

**Difficulty calibration:**
- The codebase must have >= 10 files.
- The answer must require examining >= 3 files to assemble.
- Red herrings (files that look relevant but aren't) should exist.

### 2e. `code_review`

**What it is:** The agent reviews a piece of code and must produce a structured review identifying specific issues.

**How to construct:**
1. Write a codebase with planted issues (bugs, security problems, performance issues).
2. The agent must create a review file (e.g., `review.json`) listing the issues found.
3. `injection.patch` removes the review file. `solution.patch` creates it.
4. The issue.md asks the agent to review specific files or a diff.
5. The hidden test suite validates the review against known issues.

**Difficulty calibration:**
- At least 3 distinct issues must be planted.
- Issues should span different categories (correctness, security, performance).

### 2f. `greenfield`

**What it is:** The agent builds a small application from scratch given a specification and a public test suite.

**How to construct:**
1. Write the complete application. This is your repo_source/.
2. `injection.patch` removes the implementation, leaving only the spec (README.md) and public tests (tests/test_public.py). The src/ files are either deleted or replaced with empty stubs.
3. `solution.patch` restores the full implementation.
4. The issue.md contains the full specification. The public tests provide a subset of the acceptance criteria.
5. The hidden test suite covers the full spec including edge cases not in public tests.

**Difficulty calibration:**
- The application must have >= 3 modules.
- solution.patch must touch >= 20 non-blank lines.
- The spec must be unambiguous enough for deterministic validation.

### 2g. Optional Secondary Metrics (Continuous)

Use secondary metrics only as additional reporting axes; they do NOT replace correctness pass/fail.
Rationale: for tasks that share the same required end state, continuous efficiency metrics give more model signal than binary pass/fail alone.

`performance_continuous_v1` is the default secondary metric profile for eligible tasks (`bugfix`, `feature`, `refactor`):
- Correctness gate stays binary. Performance is reported only for correctness-passing solutions.
- Report continuous values: throughput, p95 latency, peak RSS memory, and scaling trend.
- Use hidden workload holdouts with deterministic seeds and repeated runs.
- Enforce output-equivalence/invariant checks in the same run so "faster but wrong" receives no performance score.
- Do not attach this metric to `greenfield`, `agentic_search`, or `code_review`.

---

## 3. Creating the Synthetic Codebase (repo_source/)

This is the most important part. The codebase must be realistic, non-trivial, and deterministic.

### Requirements

- **Language:** Python 3.10+ using ONLY the standard library. No pip dependencies.
- **Minimum complexity:** At least 3 modules (`.py` files in `src/`) with cross-module imports.
- **Realistic code:** Include type hints, docstrings, and realistic function/class/variable names. The code should look like it belongs in a real library, not a toy example.
- **Deterministic:** No randomness without explicit seeding. No timestamps, no timezone-dependent behavior, no locale-dependent string operations.
- **No network access:** No imports of `requests`, `urllib`, `http`, `socket`, `aiohttp`, etc.
- **No filesystem side effects:** Functions under test should be pure or operate on passed-in paths, not hardcoded locations.
- **Self-contained:** All imports resolve within the project. No external dependencies.

### Structure

```
repo_source/
  src/
    __init__.py                # Package init, may define public API
    core.py                    # Core data types and logic
    processing.py              # Processing/transformation layer
    output.py                  # Output formatting/serialization
    utils.py                   # Shared utilities (optional)
  tests/
    __init__.py
    test_public.py             # Public tests (3-10 tests, basic smoke tests)
  README.md                    # Library description (1-2 paragraphs)
```

Modules MUST import from each other. The dependency graph should have depth >= 2 (e.g., output.py imports from processing.py which imports from core.py).

### What Makes a Good Synthetic Codebase

**Good:** A library that parses, transforms, and outputs data. Examples:
- A config file parser that validates, merges, and serializes config objects
- A graph library that builds, traverses, and queries dependency graphs
- A template engine that tokenizes, parses, and renders templates
- A schema validator that parses schemas, validates data, and reports errors
- A task scheduler that parses schedules, resolves dependencies, and orders execution

**Bad:**
- A single function with no module interactions
- Code that's just data classes with no logic
- A wrapper around stdlib that adds no real logic
- Anything that requires network, database, or external services

### Source Code Quality Standards

The code you write will be read by an agent trying to fix it. It must be:
- **Readable:** Clear function names, reasonable abstractions, no obfuscation
- **Testable:** Functions accept inputs and return outputs (no global state mutation)
- **Non-trivial:** Contains actual algorithms, not just data shuffling
- **Realistic:** Could plausibly exist in a real project

---

## 4. Writing the Hidden Test Runner (hidden/runner.py)

The runner is invoked by the benchmark grader:
```bash
python hidden/runner.py <workspace_path> <cases_jsonl_path>
```

It reads test cases from `cases.jsonl`, executes each against the workspace code, and outputs JSONL results to stdout.

### Specification-Based Testing Pattern

The runner compares the workspace code's output against the `expected` field in each test case. It does NOT compare against a reference implementation.

```python
#!/usr/bin/env python3
"""Hidden test runner — specification-based."""
from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path


# ─── TASK-SPECIFIC CONFIGURATION ──────────────────────────────
# Edit these for each task.
MODULE_REL = "src/processing.py"   # Module path relative to workspace
FUNCTION_NAME = "process_data"     # Function to test


def import_target(workspace: Path):
    """Import the target function from the workspace."""
    src_dir = workspace / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir.parent))
    mod_path = MODULE_REL.replace("/", ".").removesuffix(".py")
    mod = importlib.import_module(mod_path)
    importlib.reload(mod)  # Ensure fresh import
    return getattr(mod, FUNCTION_NAME)


def execute(fn, input_data: dict):
    """Call the function with the test case input. Adapt to function signature."""
    # TASK-SPECIFIC: unpack input_data to match function signature.
    # Examples:
    #   return fn(input_data["text"])
    #   return fn(**input_data)
    #   return fn(input_data["items"], key=input_data.get("sort_key"))
    return fn(**input_data)


def compare(actual, expected: dict) -> tuple[bool, str]:
    """Compare actual output against expected specification.

    Returns (passed, error_message).
    TASK-SPECIFIC: adapt comparison logic.
    """
    want = expected.get("output")
    if actual == want:
        return True, ""
    return False, f"got {actual!r}, want {want!r}"


def run_case(workspace: Path, case: dict) -> dict:
    case_id = case["case_id"]
    expected = case.get("expected", {})
    start = time.monotonic()
    error_type = None
    error_message = None

    try:
        fn = import_target(workspace)
        actual = execute(fn, case["input_data"])
        passed, err = compare(actual, expected)
        if not passed:
            error_type = "AssertionError"
            error_message = err
    except Exception as e:
        passed = False
        error_type = type(e).__name__
        error_message = str(e)[:512]

    return {
        "case_id": case_id,
        "passed": passed,
        "error_type": error_type if not passed else None,
        "error_message": error_message if not passed else None,
        "duration_ms": round((time.monotonic() - start) * 1000, 2),
        "output_summary": "",
    }


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python runner.py <workspace> <cases.jsonl>", file=sys.stderr)
        sys.exit(1)
    workspace = Path(sys.argv[1])
    cases_path = Path(sys.argv[2])
    for line in cases_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        case = json.loads(line)
        result = run_case(workspace, case)
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
```

### Customization Points

You MUST customize three functions for each task:

1. **`import_target()`** — Set `MODULE_REL` and `FUNCTION_NAME` to point at the code under test.
2. **`execute()`** — Unpack `input_data` to match the function's signature.
3. **`compare()`** — Define how to compare actual output against `expected`. This may be:
   - Direct equality (`actual == expected["output"]`)
   - Set equality for unordered results (`set(actual) == set(expected["output"])`)
   - Approximate equality for floats (`abs(actual - expected["output"]) < 1e-6`)
   - Structural comparison for nested objects

### Multi-Function Testing

If a task tests multiple functions (e.g., `parse()` and `format()`), use `case_type` to dispatch:

```python
def execute(fn_map: dict, case: dict):
    case_type = case["case_type"]
    fn = fn_map[case_type]
    return fn(**case["input_data"])
```

### What the Runner Must NOT Do

- Do NOT import a reference implementation and compare against it
- Do NOT use subprocess to call external tools (bun, node, etc.) unless the task specifically requires it
- Do NOT access the network
- Do NOT read from hidden/ or private/ directories
- Do NOT hardcode expected values in the runner — they come from cases.jsonl

---

## 5. Generating Test Cases (hidden/cases.jsonl)

Each line in cases.jsonl is a JSON object representing one test case.

### Case Schema

```json
{
  "case_id": "test_000",
  "case_type": "process_data",
  "input_data": {
    "text": "hello world",
    "options": {"uppercase": true}
  },
  "expected": {
    "output": "HELLO WORLD"
  },
  "tags": ["normal", "uppercase"],
  "timeout_s": 5
}
```

**Required fields:**
- `case_id` — Unique string. Format: `<prefix>_<NNN>` (e.g., `parse_000`, `validate_042`).
- `case_type` — String identifying which function/behavior is being tested.
- `input_data` — Dict of inputs to pass to the function under test.
- `expected` — Dict containing the expected output. MUST have an `output` key.
- `tags` — List of strings categorizing the case.
- `timeout_s` — Per-case timeout in seconds (default 5).

### Case Count and Distribution

**Minimum: 50 cases.** Recommended: 60-100.

Distribution:
- **60% normal inputs** — Standard, expected usage patterns
- **20% edge cases** — Empty inputs, boundary values, zero-length collections, single-element inputs, maximum-size inputs
- **10% error conditions** — Invalid inputs that should raise exceptions or return error indicators
- **10% stress inputs** — Large inputs, deeply nested structures, many elements

### Determinism Requirements

- Cases must be deterministically ordered (sorted by case_id).
- All JSON serialization must use `json.dumps(obj, sort_keys=True)`.
- If you use randomness to generate inputs, use a seeded RNG: `rng = random.Random(42)`.
- No floating point values that could have platform-dependent representations. Use integers or strings.
- No dependency on system time, locale, or timezone.

### Computing Expected Values

You MUST compute the `expected.output` for each case yourself. Since you wrote the codebase, you know what the correct output is for any given input. Think through the function's logic for each input and write the expected output.

For complex cases, trace through the code step by step:
1. What does the function receive?
2. What does each conditional/branch produce?
3. What is the final return value?

If the expected output is a complex object (dict, list), serialize it deterministically (sorted keys).

---

## 6. Writing Mutant Patches (mutants/M01.patch ... M10+.patch)

Mutants are intentionally broken versions of the CORRECT code (not the injected/broken code). Each mutant must be detected (killed) by the hidden test suite.

### Requirements

- Minimum 10 mutant patches.
- Minimum 8 distinct strategy categories.
- Each patch MUST have `# strategy: <name>` as a comment in the first 5 lines of the diff context.
- Each mutant must cause at least 1 hidden case to fail.
- At least 80% of mutant failures should be assertion mismatches (wrong output), not crashes.

### Strategy Categories

Pick at least 8 from this list:

| Strategy | What It Does | Example |
|---|---|---|
| `swallow_error` | Catch and ignore an exception | `try: ... except: pass` |
| `default_return` | Return a default value instead of computing | `return []` instead of actual logic |
| `special_case` | Hardcode result for specific input | `if len(items) == 1: return items[0]` |
| `weaken_validation` | Skip a validation check | Remove `if not valid: raise` |
| `incorrect_boundary` | Off-by-one in comparison | `>=` instead of `>` |
| `skip_step` | Remove a processing step | Delete a loop iteration or function call |
| `wrong_type` | Return wrong type | Return `str` instead of `int` |
| `off_by_one` | Index or count off by one | `range(len(x))` instead of `range(len(x) - 1)` |
| `missing_edge_case` | Don't handle empty/null/special values | Remove `if not items: return []` |
| `hardcode_value` | Return a constant | `return 0` instead of computed value |
| `partial_implementation` | Implement only part of the feature | Handle only the first case in a match |
| `wrong_interface` | Wrong function signature or return shape | Return `(a, b)` instead of `{"a": a, "b": b}` |
| `fix_symptom_not_cause` | Fix the wrong thing | Patch module C instead of module A |
| `wrong_module` | Edit the wrong file | Correct logic but in wrong location |
| `revert_refactor` | Undo part of the refactoring | Re-inline an extracted method |
| `partial_refactor` | Incomplete structural change | Move half the logic, leave half behind |

### Mutant Patch Format

Each patch is a unified diff that applies to the CORRECT codebase (repo_source/ state, before injection.patch). Example:

```diff
# strategy: off_by_one
--- a/src/processing.py
+++ b/src/processing.py
@@ -42,7 +42,7 @@
     def chunk_items(self, items: list, size: int) -> list[list]:
         chunks = []
-        for i in range(0, len(items), size):
+        for i in range(0, len(items) - 1, size):
             chunks.append(items[i:i + size])
         return chunks
```

### mutants/README.md

Document each mutant:

```markdown
| Mutant | Strategy | Description |
|--------|----------|-------------|
| M01 | swallow_error | Catch ValueError in parse_header and return empty dict |
| M02 | off_by_one | Loop bound off by one in chunk_items |
| ...  | ... | ... |
```

---

## 7. Writing issue.md

The issue is what the agent sees. It describes the PROBLEM, not the solution.

### Rules

- Describe the **symptom** (what observable behavior is wrong).
- Include **reproduction steps** (reference the public test).
- State **acceptance criteria** (what "fixed" looks like).
- Maximum 500 words.

### Deny Patterns (issue.md MUST NOT contain)

- File paths like `src/module.py` or `tests/test_foo.py`
- Line numbers like "line 42" or "L42"
- Function/class names from the implementation (use behavioral descriptions instead)
- References to `hidden/`, `private/`, or `solution.patch`
- Phrases like "edit file", "modify line", "change the return type"

Exception: For `agentic_search` tasks, file paths in the issue are allowed since the task is about finding things.

### Template

```markdown
# [Short symptom-focused title]

## What's wrong

[1-3 sentences. Observable symptom only. What output is wrong? What behavior fails?]

## How to reproduce

```bash
cd /workspace
bash .bench_public/run_public.sh
```

[Describe what the output looks like and why it's wrong.]

## Expected behavior

[What SHOULD happen. Be specific about expected output/behavior.]

## Acceptance criteria

- [ ] [Behavioral criterion 1]
- [ ] [Behavioral criterion 2]
- [ ] Existing behavior is preserved for normal inputs
```

---

## 8. Writing public/run_public.sh

This script runs a REAL test that the agent can use to check their work. It must NOT be a stub.

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Run the public test suite
python -m pytest tests/test_public.py -v --tb=short 2>&1

exit $?
```

The public test suite (`tests/test_public.py`) should contain 3-10 tests that cover basic functionality. These tests should FAIL on the injected/broken code and PASS on the correct code. They are a SUBSET of the hidden test cases — the hidden suite covers more edge cases, error conditions, and stress inputs.

---

## 9. Writing task.yaml

```yaml
task_id: "TASK###"
task_type: "bugfix"             # One of: bugfix, feature, refactor, agentic_search, code_review, greenfield
task_profile:                   # Optional task-type-specific profile
  bugfix_profile: "deep_diagnosis"   # Optional for bugfix: standard | deep_diagnosis
repo_id: "synth_task###"        # Unique repo identifier
repo_snapshot: "synth_task###/src.tar.zst"
baseline_injection_patch: "injection.patch"
public_command: "bash .bench_public/run_public.sh"
hidden_command: "python hidden/runner.py /workspace hidden/cases.jsonl"
time_limits:
  agent_timeout: 1200
  grade_timeout: 300
  hidden_timeout: 60
  public_timeout: 30
determinism_env:
  PYTHONHASHSEED: "0"
  TZ: "UTC"
  LC_ALL: "C.UTF-8"
  LANG: "C.UTF-8"
  SOURCE_DATE_EPOCH: "1700000000"
  PYTEST_DISABLE_PLUGIN_AUTOLOAD: "1"
patch_policy:
  allow_edit_globs:
    - "src/**/*.py"
  deny_edit_globs:
    - "tests/**"
secondary_metrics:              # Optional, continuous reporting only
  - name: "performance_continuous_v1"
    applies_if: "correctness_pass"
    report:
      - "throughput_ops_per_s"
      - "p95_latency_ms"
      - "peak_rss_mb"
      - "scaling_exponent"
description: "One-sentence description of what this task tests."
difficulty: "hard"
tags:
  - "v1"
  - "bugfix"
```

---

## 10. Writing policy/ Files

### allow_edit_globs.txt
One glob pattern per line. Files the agent is allowed to modify.
```
src/**/*.py
```

### deny_edit_globs.txt
One glob pattern per line. Files the agent must NOT modify.
```
tests/**
bench/**
schemas/**
```

---

## 11. Patch Construction

### injection.patch

This is a unified diff that transforms the CORRECT codebase into the BROKEN starting state. When the benchmark harness applies this patch to repo_source/, it produces the code the agent sees.

Think of it as: `correct_code + injection.patch = broken_code`

### private/solution.patch

This is a unified diff that transforms the BROKEN state back to the CORRECT state. It is the inverse of injection.patch.

Think of it as: `broken_code + solution.patch = correct_code`

### Constructing Both

1. Write the correct codebase (repo_source/).
2. Copy it. Apply the changes that create the broken state.
3. `diff -ruN repo_source/ broken_copy/ > injection.patch`
4. `diff -ruN broken_copy/ repo_source/ > private/solution.patch`

Or equivalently, write both patches by hand as unified diffs, ensuring they are exact inverses.

---

## 12. Self-Validation Checklist

Before submitting your task bundle, verify:

- [ ] repo_source/ has >= 3 Python modules with cross-module imports
- [ ] repo_source/ uses only Python stdlib (no pip dependencies)
- [ ] All functions are deterministic (no randomness, no timestamps, no locale)
- [ ] cases.jsonl has >= 50 lines
- [ ] Every case in cases.jsonl has `case_id`, `case_type`, `input_data`, `expected`, `tags`, `timeout_s`
- [ ] Every case has `expected.output` with the CORRECT computed value
- [ ] runner.py reads `case["expected"]` and compares against actual output
- [ ] runner.py does NOT use reference comparison or import from hidden/reference/
- [ ] issue.md contains NO file paths, NO line numbers, NO function names
- [ ] issue.md is <= 500 words
- [ ] run_public.sh runs a real test (not `echo "ok"; exit 0`)
- [ ] tests/test_public.py has 3-10 real tests
- [ ] injection.patch and solution.patch are exact inverses
- [ ] injection.patch applied to correct code produces broken code
- [ ] solution.patch applied to broken code produces correct code
- [ ] >= 10 mutant patches exist
- [ ] >= 8 distinct strategy categories across mutants
- [ ] Each mutant has `# strategy: <name>` in the first 5 lines
- [ ] Each mutant, applied to the correct code, causes at least 1 hidden case to fail
- [ ] task.yaml has `task_type` field set correctly
- [ ] task.yaml `task_type` is one of: bugfix, feature, refactor, agentic_search, code_review, greenfield
- [ ] If `task_type=bugfix` and `task_profile.bugfix_profile=deep_diagnosis`, root cause is >= 2 module hops from symptom
- [ ] If `task_type=bugfix` and `task_profile.bugfix_profile=deep_diagnosis`, issue.md describes symptom only (no root-cause hints)
- [ ] If `secondary_metrics` includes `performance_continuous_v1`, correctness remains a binary gate
- [ ] If `secondary_metrics` includes `performance_continuous_v1`, performance metrics are computed only for correctness-passing solutions
- [ ] If `secondary_metrics` includes `performance_continuous_v1`, hidden holdout workloads use deterministic seeds and repeated runs
- [ ] If `secondary_metrics` includes `performance_continuous_v1`, output-equivalence/invariant checks prevent "faster but wrong" scoring
- [ ] task.yaml `difficulty` is "medium" or "hard"
- [ ] No network access in runner.py, run_public.sh, or any test code
- [ ] No references to secrets, API keys, or credentials

### Validation Commands (run after packing repo)

```bash
python -m bench.cli validate-task bench/benchmark/tasks/v1/TASK### --strict
python -m bench.cli admit-task bench/benchmark/tasks/v1/TASK###
```

---

## 13. Anti-Patterns (Do NOT Do These)

1. **Stub public tests:** `echo "public repro"; exit 0` — the public test must run real assertions.
2. **Reference comparison runners:** Importing a copy of the correct function and comparing outputs. Use specification-based testing with `expected` from cases.jsonl.
3. **Identical issue descriptions:** Each task must have a unique, specific issue.md describing its particular problem.
4. **Copy-pasted mutant READMEs:** Each mutant README must describe the actual mutations applied to THIS task's code.
5. **Trivial bugs:** Changing a string constant, flipping a boolean, renaming a variable. Bugs must require reasoning about program behavior.
6. **Leaked solutions in issue.md:** Mentioning file paths, function names, or line numbers tells the agent where to look.
7. **Non-deterministic test cases:** Using `time.time()`, `random.random()` without seeding, or locale-dependent string operations.
8. **Single-module codebases:** A codebase with one file is too simple. Require >= 3 modules with cross-imports.
9. **Open-ended performance tasks:** Asking for "optimize as much as possible" without fixed workloads, repeated measurements, and invariant-preserving checks.
