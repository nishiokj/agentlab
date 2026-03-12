# Patch Spec: Enforce Build/Run Boundary

**Severity: Critical — the runner is architecturally broken.**

The runner re-resolves build-time paths at runtime. This means:
- A sealed package cannot be moved and run from another directory.
- The runtime depends on files that the build already staged but never reads from the package.
- Errors surface as opaque OS errors referencing paths the user never wrote.
- Every field in the YAML that touches a file path is a potential runtime failure.

This is not a single bug. It is a pattern that repeats across `lifecycle.rs`, `io.rs`, and `runner.rs`. Every callsite that passes `run_dir` or `project_root` to a path-resolution function at runtime is a violation of Invariant 6.

---

## Invariants This Patch Enforces

| # | Invariant | Status |
|---|-----------|--------|
| 1 | No invisible config | Violated |
| 2 | Errors must name what was attempted | Violated |
| 3 | No hardcoded cross-boundary paths | Violated |
| 4 | Tests must validate contracts, not internal consistency | Violated |
| 5 | User-authored and system-resolved config are separate objects | Violated |
| 6 | Runtime must never re-resolve build-time paths | Violated |

All six invariants are currently violated. This patch addresses 2, 3, and 6 directly. 1, 4, and 5 require follow-up work.

---

## The Problem

There are two distinct path-resolution phases that should never share code, but currently do:

### Build phase (correct intent, wrong error handling)
- `build_experiment_package()` in lifecycle.rs
- Resolves paths relative to `exp_dir` (the experiment YAML's directory)
- Stages files into the sealed package
- Rewrites paths in the resolved config to be package-relative
- **Problem:** Uses bare `?` propagation on IO — errors have no context (Invariant 2)

### Run phase (fundamentally wrong)
- `run_experiment_with_behavior()` in lifecycle.rs:2898
- `continue_run()` in runner.rs:128
- `prepare_fork_trial()` in runner.rs:426
- All pass `run_dir` or `exp_dir` (derived from run_dir) to `resolve_variant_runtime_profile()`
- This calls `resolve_agent_runtime()` → `derive_public_path_staging_specs()`
- `derive_public_path_staging_specs()` scans the command array for relative paths and resolves them against the run directory
- **Problem:** The run directory is not the experiment directory. The files don't exist there. They exist in the build package. (Invariant 6)

### Why it sometimes works
When `build-run` is used, the run directory is created inside the same project that has the original files. `derive_public_path_staging_specs` resolves `overrides/defaults.bench-lmstudio-headless.json` against whichever directory it's given — and if that directory happens to be somewhere inside the same project tree, the path might resolve to the original file by accident. It's not reading from the package; it's reading from the source. This is fragile, non-portable, and wrong.

---

## The Fix

### Phase 1: Make the sealed package self-contained at runtime

**1a. Build must record resolved staging specs in the package.**

`build_experiment_package()` already calls `rewrite_runtime_paths_for_package()` which stages support files. But the agent command's path references (the things `derive_public_path_staging_specs` finds) are NOT staged during build. They need to be.

In `lifecycle.rs`, after line 2674 (task spec loading), add a step that:
1. Calls `derive_public_path_staging_specs()` against the experiment directory (build-time, where files exist)
2. Copies each resolved file into the package under a `deps/` directory
3. Writes a `staging_manifest.json` into the package that maps each original relative path to its packaged location and container destination

**1b. Runtime must read staging specs from the package, not re-derive them.**

Replace the runtime call chain:
```
resolve_variant_runtime_profile()
  → resolve_agent_runtime()
    → derive_public_path_staging_specs(command, env, exp_dir)  // re-resolves from filesystem
```

With:
```
resolve_variant_runtime_profile()
  → resolve_agent_runtime()
    → load_staging_specs_from_package(package_dir)  // reads staging_manifest.json
```

The runtime function `resolve_agent_runtime()` needs two modes:
- **Build mode:** Takes `exp_dir`, resolves paths, produces staging specs (current behavior, kept for build)
- **Run mode:** Takes `package_dir`, reads pre-computed staging specs from `staging_manifest.json`

The simplest way: add a `PackageContext` enum parameter:
```rust
enum PathResolutionContext {
    Build { exp_dir: PathBuf },
    Run { package_dir: PathBuf },
}
```

**1c. `run_experiment_with_behavior()` must locate and use the package directory.**

The run is always created from a built package. The run already copies `resolved_experiment.json` into the run directory. The package directory is known at run creation time. Either:
- Store the package path in the run's metadata, or
- Copy the `staging_manifest.json` into the run directory alongside `resolved_experiment.json`

The second option is better — it makes the run directory itself self-contained.

### Phase 2: Fix error context (Invariant 2)

Every `fs::File::open(path)?` and `path.exists()` check in the build phase must use `.with_context()`:

```rust
// Before (current)
let file = fs::File::open(path)?;

// After
let file = fs::File::open(path).with_context(|| {
    format!(
        "failed to open dataset file '{}' (resolved from benchmark '{}' via dataset.path)",
        path.display(),
        benchmark_name
    )
})?;
```

Files to audit:
- `config.rs:1556` — `load_task_specs_for_build`
- `lifecycle.rs:2411` — `validate_staged_source_path`
- `io.rs:1003-1008` — `derive_public_path_staging_specs` command resolution
- `io.rs:1034-1040` — `derive_public_path_staging_specs` env resolution
- `io.rs:1081-1087` — `normalize_staged_support_source_path`
- `runner.rs:2534-2538` — `resolve_existing_public_path_reference`
- `runner.rs:2626-2631` — `resolve_staged_source_path`
- `runner.rs:2786-2791` — artifact resolution

### Phase 3: Eliminate hardcoded dataset paths (Invariant 3)

The `normalize_experiment_authoring()` match arm that maps `benchmark: bench_v0` to a hardcoded `.lab/experiments/data/bench_v0.task_spec.jsonl` must be replaced.

Option A (minimal): The benchmark sugar expands to a `dataset.path` field in the resolved config, and the user can override it. The default path is still conventional, but it's visible in the resolved output and the error names it.

Option B (correct): Benchmarks are registered artifacts. The Experiments repo ships benchmark definitions (YAML/JSON files) that declare their dataset path, metrics, grader command, and support files. `benchmark: bench_v0` looks up the registered definition. The consumer project provides the dataset file at the path declared by the benchmark definition. The contract is explicit on both sides.

**Recommendation:** Do Option A now (unblock experiments), plan Option B as the benchmark registry redesign.

---

## Affected Callsites

| File | Line | Function | Issue |
|------|------|----------|-------|
| lifecycle.rs | 2898 | `run_experiment_with_behavior` | Passes `run_dir` as project_root to runtime resolution |
| runner.rs | 128 | `continue_run` | Passes `exp_dir` (derived from run_dir) to runtime resolution |
| runner.rs | 426 | `prepare_fork_trial` | Passes `run_dir` to runtime resolution |
| runner.rs | 3173 | `normalize_experiment_authoring` | Hardcodes dataset paths per benchmark |
| io.rs | 981 | `derive_public_path_staging_specs` | Re-resolves paths at runtime against wrong directory |
| io.rs | 1493 | `resolve_agent_runtime` | Calls derive_public_path_staging_specs with exp_dir |
| config.rs | 1556 | `load_task_specs_for_build` | Bare `?` on File::open |

---

## Validation

The litmus test for this patch (from our earlier discussion):

```bash
# Build the experiment
lab-cli build .lab/experiments/my_experiment.yaml

# Copy the package to a completely unrelated directory
cp -r .lab/builds/my_experiment_20260312_... /tmp/random_dir/

# Run it from there (env vars injected explicitly)
lab-cli run /tmp/random_dir/ --env ZAI_CODER_API_KEY="$ZAI_CODER_API_KEY"
```

If this works, the build/run boundary is enforced. If it fails, every path that broke is a remaining violation.

Write an E2E test that does exactly this. The test must:
1. Build a package from a fixture experiment
2. Move the package to a temp directory with no `.lab/` parent
3. Run the package from the temp directory
4. Assert that the run starts successfully (at least reaches trial execution)
5. Assert that no path in any error or log references the original build location
