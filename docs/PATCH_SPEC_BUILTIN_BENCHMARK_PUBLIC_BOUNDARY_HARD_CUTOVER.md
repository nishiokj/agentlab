# Patch Spec: Builtin Benchmark Public Boundary Hard Cutover

Status: Draft (Hard Cut Required)
Date: 2026-03-11
Owner: `lab-runner`, `lab-cli`, `bench integrations`, `swebench integrations`
Priority: P0 DX/Contract Correctness

## 1. Intent

Hard-cut builtin benchmarks so they obey the same contract stack as external benchmarks:

1. benchmark-native source data
2. runner-owned public task boundary
3. runner-owned internal `TaskDeclaration`
4. prepared runnable task environment

Builtin benchmarks remain first-class only in one sense: the runner ships their native-to-public mapper. They do not bypass the public boundary and they do not author internal `TaskDeclaration` rows directly.

## 2. Problem

Today the builtin benchmark path collapses public input and internal packaged input into the same `*.tasks.jsonl` artifact name and largely the same row shape.

Current live path:

1. `lab-cli build` calls `build_experiment_package()`.
2. `normalize_experiment_authoring()` resolves `benchmark: bench_v0` and `benchmark: swebench_lite_curated`.
3. The builtin registry points directly at checked-in source datasets:
   1. `bench_v0.tasks.jsonl`
   2. `swebench_lite_curated.tasks.jsonl`
4. Build copies that source dataset verbatim into `<package>/tasks/tasks.jsonl`.
5. `load_tasks()` reads those copied rows and immediately parses them as strict internal `TaskDeclaration`.
6. `parse_task_boundary_from_dataset_task()` materializes from `TaskDeclaration`, not from the public boundary.
7. `prepare_task_environment()` writes the `TaskDeclaration` into `prepared_task_environment_v1`.

This means the current provenance of `TaskDeclaration` is:

1. checked-in benchmark dataset row
2. copied into package unchanged
3. parsed as internal declaration

There is no authoritative build-stage compilation from public boundary into internal declaration for builtins.

## 3. Required Architecture

The runner must own exactly two benchmark-facing contracts:

1. public task boundary contract
2. internal `TaskDeclaration`

The runner must also own exactly one compilation step:

1. `public task boundary -> TaskDeclaration`

The public task boundary is the only accepted benchmark output, whether the benchmark is builtin or external.

`TaskDeclaration` is never benchmark-authored. It is a runner-private packaged artifact.

## 4. Non-Negotiable Invariants

1. Builtin and external benchmarks enter the runner through the same public boundary contract.
2. Checked-in benchmark source datasets are never named or treated as packaged runner input.
3. `tasks/tasks.jsonl` is reserved for packaged internal task declarations only.
4. `TaskDeclaration` rows are produced only during build/package time.
5. Run, preflight, and describe consume packaged `TaskDeclaration` rows only.
6. Any drift in the public boundary must fail in benchmark mappers/exporters or build-stage boundary loading, not later in runtime materialization.
7. No builtin benchmark registry entry may point directly at a checked-in `*.tasks.jsonl` source artifact.

## 5. Canonical File Ownership

### 5.1 Checked-in benchmark source artifact

Builtin mapped datasets must use a public-boundary-specific filename:

1. `bench_v0.task_spec.jsonl`
2. `swebench_lite_curated.task_spec.jsonl`

`*.tasks.jsonl` is not permitted as a checked-in benchmark source filename.

### 5.2 Packaged runner input

Packaged internal task rows remain:

1. `<package>/tasks/tasks.jsonl`

Those rows are strict `TaskDeclaration` rows only.

## 6. TaskDeclaration Provenance

### 6.1 Current provenance

Current live provenance is:

1. checked-in `*.tasks.jsonl`
2. copied into package unchanged
3. parsed by `parse_task_declaration()`
4. stored again as `TaskDeclaration`

This is the bug.

### 6.2 Required provenance

Required provenance after this patch is:

1. benchmark-native source
2. builtin or external mapper emits public `task_spec` rows
3. build loads public `task_spec` rows as typed `TaskSpec`
4. build compiles `TaskSpec` into typed `TaskDeclaration`
5. build writes packaged `<package>/tasks/tasks.jsonl`
6. run/preflight/describe load packaged `TaskDeclaration` rows only

This is the only permitted path to `TaskDeclaration`.

## 7. Minimal Patch

### 7.1 Public boundary contract

Use the runner-owned public boundary schema already present in the repo:

1. `schemas/task_spec.jsonschema`

Introduce or finalize a typed Rust struct for that boundary:

1. `TaskSpec`

`TaskSpec` is the benchmark-facing type.

`TaskDeclaration` remains the internal packaged type.

### 7.2 Registry cutover

Change builtin benchmark registry resolution so builtin datasets point at public boundary artifacts:

1. `bench_v0.task_spec.jsonl`
2. `swebench_lite_curated.task_spec.jsonl`

Hard delete any builtin registry path that resolves to checked-in `*.tasks.jsonl`.

### 7.3 Build-stage ownership

`build_experiment_package()` becomes the only place that compiles public benchmark rows into internal declarations.

Required behavior:

1. Resolve public dataset path from builtin registry or external config.
2. Read JSONL rows from that public dataset.
3. Deserialize each row into typed `TaskSpec`.
4. Validate each row against runner-owned public contract.
5. Compile each typed `TaskSpec` into typed `TaskDeclaration`.
6. Write compiled declarations into `<package>/tasks/tasks.jsonl`.

Build must not verbatim-copy the checked-in benchmark source dataset into the packaged internal task location.

### 7.4 `load_tasks()` responsibility

No new public API is required.

The minimal maintainable change is:

1. repurpose `load_tasks()` so it loads public boundary rows and returns compiled `TaskDeclaration` values during build
2. or split it into private helpers if readability demands it

Either way, the architectural rule is the same:

1. `load_tasks()` must not mean both "load checked-in benchmark source rows" and "load packaged internal declarations" unless the caller provides an explicit typed mode

Preferred direction:

1. build-side function loads `TaskSpec` and returns `Vec<TaskDeclaration>`
2. run-side function loads packaged `TaskDeclaration` rows only

If the name `load_tasks()` remains, it must be scoped to one of those jobs only.

### 7.5 Runtime strictness

Keep runtime strict.

Run, preflight, and describe continue to accept only packaged `TaskDeclaration` rows. They do not parse benchmark-native rows and they do not compile the public boundary at runtime.

## 8. Typed Boundary Ownership

### 8.1 Public boundary typing

The public boundary is JSON on disk, but it must still be typed in code.

Required guarantees:

1. benchmark mappers/exporters construct typed `TaskSpec`
2. build-side loaders deserialize JSONL rows into typed `TaskSpec`
3. public boundary drift fails in mapper/exporter/build tests immediately

JSON is not an excuse for untyped boundary handling.

### 8.2 Internal typing

`TaskDeclaration` remains typed and runner-private.

It is valid for `TaskDeclaration` to differ from `TaskSpec` in ways that are useful to the runner, including:

1. explicit internal schema tagging
2. canonicalized defaults
3. runner-owned normalization of workspace/dependency fields

But those differences must be created by the build compiler, never authored directly by benchmarks.

## 9. Hard Deletions

The following behaviors are removed, not deprecated:

1. builtin registry entries resolving to checked-in `*.tasks.jsonl`
2. checked-in builtin benchmark source datasets named `*.tasks.jsonl`
3. verbatim copying of checked-in benchmark source datasets into packaged `tasks/tasks.jsonl`
4. any path where builtin datasets bypass public-boundary deserialization and compilation

## 10. File-Level Change List

### 10.1 Registry

Update:

1. `rust/crates/lab-runner/src/runner.rs`

Required changes:

1. builtin dataset path suffixes become `*.task_spec.jsonl`
2. builtin registry comments/docstrings state that these are public-boundary inputs, not packaged internal rows

### 10.2 Types

Update:

1. `rust/crates/lab-runner/src/types.rs`

Required changes:

1. add or finalize typed `TaskSpec`
2. keep `TaskDeclaration` internal-only
3. document conversion direction `TaskSpec -> TaskDeclaration`

### 10.3 Build/package

Update:

1. `rust/crates/lab-runner/src/lifecycle.rs`
2. `rust/crates/lab-runner/src/config.rs`

Required changes:

1. build loads public dataset rows
2. build compiles them to `TaskDeclaration`
3. package writes compiled declarations into `tasks/tasks.jsonl`
4. no raw benchmark source JSONL is copied into packaged internal task location

### 10.4 Runtime/task materialization

Update:

1. `rust/crates/lab-runner/src/io.rs`

Required changes:

1. task materialization continues to consume `TaskDeclaration`
2. comments and function names must stop implying that runtime ingests public benchmark rows

### 10.5 Builtin exporters and fixtures

Update:

1. builtin benchmark exporters/scripts
2. checked-in `.lab/experiments/data/*`
3. relevant tests and fixtures

Required changes:

1. builtins emit `*.task_spec.jsonl`
2. stale checked-in `*.tasks.jsonl` benchmark source artifacts are deleted

## 11. Test Plan

### 11.1 Build-path correctness

Add integration tests proving:

1. `benchmark: bench_v0` resolves to `bench_v0.task_spec.jsonl`
2. `benchmark: swebench_lite_curated` resolves to `swebench_lite_curated.task_spec.jsonl`
3. build emits packaged `tasks/tasks.jsonl` rows that deserialize as strict `TaskDeclaration`

### 11.2 Drift detection

Add tests proving:

1. invalid builtin mapped rows fail as `TaskSpec` validation failures
2. failures mention the public boundary contract, not `task_declaration_v1`

### 11.3 Runtime isolation

Add tests proving:

1. run/preflight/describe do not accept checked-in public benchmark datasets directly
2. run/preflight/describe only accept packaged `TaskDeclaration` rows

## 12. Acceptance Criteria

This patch is complete only when all of the following are true:

1. builtin benchmarks no longer resolve to checked-in `*.tasks.jsonl`
2. builtin and external benchmarks share the same public task boundary contract
3. `TaskDeclaration` is produced only during build/package time
4. packaged `tasks/tasks.jsonl` is internal-only
5. public boundary drift breaks builtin mappers/exporters or build-stage loading immediately
6. runtime never needs to know whether the originating benchmark was builtin or external

## 13. Outcome

After this patch:

1. internal benchmarks are only "special" because the runner ships their mapper
2. the runner is agnostic to builtin vs external once the public boundary is met
3. changing runner-internal topology can no longer silently require checked-in builtin dataset rewrites unless the public boundary itself changed
4. the path to `TaskDeclaration` is explicit, typed, and build-owned
