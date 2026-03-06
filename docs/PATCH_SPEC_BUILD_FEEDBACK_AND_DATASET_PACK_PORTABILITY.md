# Patch Spec: Build Feedback, Dataset-Pack Portability, and Copy Safety

## Goal

Fix three production issues in the `lab-cli build` path:

1. Builds appear hung because there is almost no progress output.
2. Dataset packs exported with non-default `--dataset-pack-root` are not reliably resolvable at runtime.
3. Directory-copy behavior can recurse through symlinked directories without cycle protection.

---

## Current Problems

### 1) Build appears to hang

- `lab-cli build` prints only:
  - start line (`building package from: ...`)
  - final output paths
- Heavy phases (large artifact digesting, copy, and checksums) emit no intermediate status.
- In `--json` mode, start/end human lines are suppressed from stdout, so users get effectively zero live feedback.

### 2) Dataset-pack root mismatch (functional bug)

- Exporter allows `--dataset-pack-root` and materializes packs there.
- Task rows store only `workspace_seed.dataset_pack_ref` digest.
- Runner resolves packs from fixed `project_root/.lab/dataset_packs/sha256/<digest>`.
- Result: exports can succeed, but run/preflight can fail later when packs are looked up.

### 3) Late failure timing

- Missing dataset packs are discovered when trial workspace materialization runs.
- Build/preflight do not consistently fail early with a direct “digest X missing” diagnosis.

### 4) Copy recursion risk

- `copy_dir_filtered` follows symlinked directories recursively.
- No cycle detection exists for symlink loops.
- Large or cyclic link graphs can look like hangs.

---

## Scope

### In scope

- Improve `build` progress observability.
- Make dataset-pack resolution portable and compatible with non-default pack roots.
- Add early validation for unresolved dataset-pack refs.
- Add recursion safety in copy logic.

### Out of scope

- Redesigning task-boundary schema beyond additive optional fields.
- Changing benchmark task payload schema version.
- Reworking all copy semantics for every runtime path.

---

## Proposed Changes

## A) Build progress instrumentation

### A1. Add build-stage progress logs

- Add `emit_build_log(...)` in `rust/crates/lab-runner/src/lib.rs` using existing progress-log mechanism (stderr).
- Emit clear stage markers in `build_experiment_package(...)`:
  - load + normalize experiment
  - validate required fields
  - stage dataset
  - stage runtime artifacts/files
  - write manifest/resolved spec
  - compute checksums
  - done (duration + counts)

### A2. Add periodic checksum progress

- During checksum walk, emit progress every N files or T seconds:
  - files processed
  - bytes hashed
  - elapsed time
- Keep final output contract unchanged.

### A3. CLI behavior

- Keep JSON payload on stdout unchanged for `--json`.
- Progress continues on stderr (safe for machine-readable stdout).

---

## B) Dataset-pack root portability + resolution correctness

### B1. Add optional root hint to workspace seed

- Extend `WorkspaceSeedSpec` with optional field:
  - `dataset_pack_root: Option<String>`
- Exporter writes the effective absolute root path into:
  - `workspace_seed.dataset_pack_root`

### B2. Vendor referenced packs into built package

- In `build_experiment_package(...)`:
  - Parse dataset rows.
  - Collect all referenced digests from `workspace_seed` and `mount_references`.
  - Resolve source packs using candidate roots:
    1. packaged root if already present
    2. row-level `workspace_seed.dataset_pack_root` (if present)
    3. project default `.lab/dataset_packs/sha256`
  - Copy each resolved digest dir into package-local:
    - `<package_dir>/dataset_packs/sha256/<digest>`

### B3. Runtime lookup search order

- Update resolver logic to check multiple roots in deterministic order:
  1. `<project_root>/dataset_packs/sha256/<digest>` (built package)
  2. explicit row-level `dataset_pack_root` (workspace_seed only)
  3. `<project_root>/.lab/dataset_packs/sha256/<digest>` (legacy/default)
- Apply shared resolver for:
  - `workspace_seed`
  - `mount_references` (without row-level hint, so checks 1 + 3)

### B4. Early preflight check

- Add preflight check:
  - `dataset_pack_refs_resolvable`
- Reports missing digest(s) and dataset line numbers before run starts.

---

## C) Copy recursion safety

### C1. Add symlink-cycle detection in copy path

- Add visited canonical-directory tracking to prevent recursive loops.
- On detected cycle, return explicit error with source path chain.

### C2. Guard runaway copies

- Add max-depth / max-entries guardrails with actionable error messages.
- Log guardrail trips in build progress scope.

---

## File-Level Plan

- `bench/integration/agentlab/export_bench_suite_to_jsonl.py`
  - Emit `workspace_seed.dataset_pack_root` in rows.
- `rust/crates/lab-runner/src/lib.rs`
  - Build progress logging.
  - Multi-root dataset-pack resolver.
  - Package vendoring of dataset packs during build.
  - New preflight check for resolvable dataset-pack refs.
  - Copy recursion/cycle protection.
- `rust/crates/lab-cli/src/main.rs`
  - Keep command output contract; rely on runner stderr progress.
- `bench/integration/agentlab/test_export_bench_suite_to_jsonl.py`
  - Assert `workspace_seed.dataset_pack_root` behavior.
- `rust/crates/lab-runner/src/lib.rs` tests
  - Add/extend tests for resolver order, packaged packs, missing-pack preflight, and symlink cycle handling.

---

## Acceptance Criteria

1. `lab-cli build` emits visible progress every few seconds during long operations.
2. A dataset exported with non-default `--dataset-pack-root` can be built and run without manual copying into `.lab/dataset_packs/sha256`.
3. Running a built package works when only package-local `dataset_packs/sha256` exists.
4. Preflight fails early with clear missing digest diagnostics.
5. Symlink-cycle input no longer causes indefinite recursive copy behavior.

---

## Verification Plan

1. Unit tests:
   - workspace seed parser handles optional `dataset_pack_root`
   - dataset-pack resolver search order
   - copy cycle detection
2. Integration tests:
   - export with custom root -> build package -> run/preflight succeeds via vendored package packs
   - missing digest triggers `dataset_pack_refs_resolvable` preflight failure
3. Manual smoke:
   - build with large artifact shows periodic progress lines and final timing summary

---

## Rollout

1. Ship A (progress logs) first for immediate UX relief.
2. Ship B (resolution + vendoring + preflight) second for correctness.
3. Ship C (copy safety) third, with targeted regression tests for existing workspace copy behavior.
