# Bench v0 Preflight Blockers

Date: 2026-03-11

Scope:
- `jesus/.lab/experiments/bench_v0_glm5_vs_codex_low_reasoning.yaml`
- `Experiments` runner / built-in benchmark topology
- `jesus` `rex` artifact runtime

This file tracks the concrete failures hit while trying to build and preflight the
bench-v0 GLM-5 vs Codex experiment, plus the test gaps that should have caught them.

## Closed

### 1. Built-in benchmark support files resolved from the experiment project root

Symptom:
- Built package contained `jesus/bench`, not `Experiments/bench`.
- Sealed package missed `bench/integration/agentlab/bench_benchmark_adapter.py`.

Why it failed:
- Built-in `benchmark: bench_v0` was resolving support files via `project_root.join("bench")`.
- For an experiment under `jesus`, `project_root` was `/Users/jevinnishioka/Desktop/jesus`.
- The runner later executed `/agentlab/deps/bench/integration/agentlab/bench_benchmark_adapter.py`,
  but that file had never been copied into the package.

Fix:
- Built-in benchmark assets now resolve from the runner repo root, not the experiment project root.

Test gap:
- Missing regression test proving built-in benchmark support files do not drift to a local
  `bench/` directory in the experiment repo.

Status:
- Fixed.
- Regression tests added in `lab-runner`:
  - `normalize_authoring_uses_runner_repo_for_bench_builtin_support_files`
  - `normalize_authoring_supports_swebench_lite_builtin_registry`

### 2. `rex run` hard-failed on unrelated `--provider-env` mappings

Symptom:
- `codex` preflight smoke failed before model selection because `rex` required
  `ZAI_CODER_API_KEY` even when the selected provider was `codex`.

Why it failed:
- `maybeSaveProviderKeys(...)` iterated every `--provider-env` mapping unconditionally.

Fix:
- `rex` now saves provider keys only for the provider selected for the current run.

Test gap:
- Missing run-cli test covering mixed provider mappings with a selected provider that
  does not use all configured env mappings.

Status:
- Fixed in `jesus/packages/infra/harness-daemon/src/cli/run.ts`.

### 3. `lab-cli preflight` could not receive runtime env via `--env` / `--env-file`

Symptom:
- Preflight required ambient host env export even though runtime env resolution already
  supports `--env` and `--env-file` elsewhere.

Why it failed:
- `preflight` command path always used `RunExecutionOptions::default()`.

Fix:
- `lab-cli preflight` now accepts `--env` and `--env-file`.
- `lab-runner` now has `preflight_experiment_with_options(...)`.

Test gap:
- Missing CLI parity test proving `preflight` supports the same runtime env injection
  surface as `run` / `build-run`.

Status:
- Fixed.

### 4. Preflight failure summaries hid the actual missing-env error

Symptom:
- Preflight only showed the first few log lines and buried the real cause.

Why it failed:
- Known-blocker extraction did not recognize missing runtime env failures.

Fix:
- Missing-env lines are now treated as known probe blockers so they surface in failures.

Test gap:
- Missing preflight diagnostics test for missing env var output.

Status:
- Fixed.

### 5. Bench grader hard-required `task.yaml` parsing in the task image

Symptom:
- `benchmark_grader_reachable` failed after topology fixes.
- Direct reproduction inside `bench-v0-workspace-task001:latest` hit:
  `ModuleNotFoundError: No module named 'yaml'`.

Why it failed:
- The exported bench task rows did not include all grading fields.
- The bench grader therefore fell back to loading `task.yaml`.
- The grading import path eagerly loaded the YAML-backed loader at module import time.
- Task images do not include PyYAML, so the grader died before scoring.

Fix:
- Exported bench task rows now include the grading fields needed by the grader:
  - `baseline_injection_patch`
  - `time_limits`
  - `determinism_env`
  - `patch_policy`
- Bench grading now prefers task-payload grading data and only falls back to YAML if required.
- Loader/schema imports were made lazy so the grading path does not require PyYAML when the
  exported task payload is sufficient.
- Regenerated `jesus/.lab/experiments/data/bench_v0.tasks.jsonl`.

Required tests:
- Task-image benchmark smoke that imports and executes the built-in bench grader in an
  actual `bench-v0-workspace-task*` image.
- Exporter test asserting bench JSONL rows include the grading fields the grader needs.

Status:
- Fixed.

## Current Outcome

What now passes:
- Package build
- `agent_bundle_container_compatible`
- `container_ready`
- `agent_runtime_reachable`
- `benchmark_grader_reachable`
- Direct `rex` preflight smoke for:
  - `glm-5` with `jesus/.env`
  - `codex` without unrelated `ZAI_CODER_API_KEY`
- Full package preflight:
  - `/Users/jevinnishioka/Desktop/jesus/.lab/builds/bench_v0_glm5_vs_codex_low_reasoning_pkg_final2`

What still fails:
- No known preflight blockers remain on this experiment package.
