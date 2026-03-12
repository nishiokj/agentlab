# Invariants

Rules that must hold across the lab-runner codebase. Violations are bugs.

---

## 1. No invisible config

If the system injects, rewrites, or defaults a field, the resolved config must be inspectable before execution begins. The user must be able to see exactly what will run.

**Violation found:** `normalize_experiment_authoring()` injects 20+ fields (dataset paths, metrics, grader commands, scheduling strategy, sandbox profiles, timeouts, retry policy) from a one-line `benchmark: bench_v0`. None of this is visible to the user. There is no `--dry-run`, `--show-resolved`, or debug output.

---

## 2. Errors must name what was attempted

Every fallible operation — file open, path resolution, network call — must attach context: what file, what field produced the path, and why it was expected. Bare `?` propagation on IO operations is a bug.

**Violation found:** `load_task_specs_for_build()` does `fs::File::open(path)?` with no context. The user sees `"Error: No such file or directory (os error 2)"` with no indication of which file, which YAML field caused the lookup, or that the path was synthesized by the system.

---

## 3. No hardcoded cross-boundary paths

A path that references a file in the user's project (`.lab/experiments/data/`, `.lab/agents/`) must come from a declared contract — a schema, a config field, or a resolution protocol — not from a hardcoded string in a match arm. Renaming a file in one repo must produce a clear, actionable error in the other, not a silent "file not found."

**Violation found:** `runner.rs` hardcodes `.lab/experiments/data/bench_v0.task_spec.jsonl`. When the Experiments repo changed this filename, the user's project broke with an opaque OS error. No contract existed between the two sides.

---

## 4. Tests must validate contracts, not just internal consistency

If the system constructs a path and later opens it, a test must verify that the constructed path matches an external contract — not just that the test fixture was set up with the same string. Self-consistent fixtures prove the code agrees with itself, not with reality.

**Violation found:** E2E tests use `create_dx_authoring_fixture()` which hardcodes `bench_v0.task_spec.jsonl` — the same string the runner hardcodes. The test passes by construction. No test verifies the path against the actual project layout or catches a rename.

---

## 5. User-authored and system-resolved config are separate objects

The YAML the user writes and the resolved config the system executes must be distinct, traceable objects. Mutating the user's input in place (`set_json_pointer_value` on the parsed YAML) destroys the ability to diff what was authored vs. what was derived. The system should produce a new resolved object, not rewrite the original.

**Violation found:** `normalize_experiment_authoring()` mutates the parsed YAML value in place via `set_json_pointer_value`. By the time an error occurs downstream, there is no way to distinguish user-authored fields from system-injected ones.

---

## 6. The runtime must never re-resolve build-time paths

Once `lab build` produces a sealed package, every file the runtime needs is inside that package. The runtime must never call path-resolution functions against the run directory, the project root, or the original experiment directory. If a file isn't in the package, the build failed — not the run.

**Violation found:** `run_experiment_with_behavior()` (lifecycle.rs:2898) passes `run_dir` as `project_root` to `resolve_variant_runtime_profile()`, which calls `derive_public_path_staging_specs()` (io.rs:981). This function scans the agent command array for relative paths and resolves them against `exp_dir` — but at runtime, `exp_dir` is the run directory (`.lab/runs/run_...`), not the experiment directory or the build package. The build correctly staged `overrides/defaults.bench-lmstudio-headless.json` into the package, but the runtime ignores the package and tries to find the file at `.lab/runs/run_.../overrides/defaults.bench-lmstudio-headless.json`, which doesn't exist. The sealed package is never consulted.
