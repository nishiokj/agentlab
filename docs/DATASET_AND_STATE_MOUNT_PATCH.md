# Patch: Remove `/dataset` mount and dead `agentlabd` state paths

## Context

Audit of the container mount contract found two issues:
1. **`/dataset` mount** — dataset file is copied into each trial dir and mounted `:ro`, but nothing inside the container reads it. Task payload is already in `/agentlab/in/task.json`. Dead infrastructure.
2. **`agentlabd_start_*` paths in `/agentlab/state`** — constants, struct fields, and host path resolution exist for `agentlabd_start_trial.request.json` / `agentlabd_start_trial.response.json`, but the runner never sets the env vars or writes the files. The `agentlab_entrypoint.sh` script that consumes them is also never invoked by the runner. Dead infrastructure.
3. **extra dead runtime host paths** — `entrypoint` and `harness_invocation` are still carried in `RunnerRuntimeHostPaths` but have no active consumers in runner flow. Remove as part of the hard cutover.

The rest of `/agentlab/state` (control plane, events, dependency staging, checkpoints) is legitimate and stays.

---

## Files to modify

1. `rust/crates/lab-core/src/lib.rs` — remove agentlabd constants + dead runtime host fields
2. `rust/crates/lab-runner/src/lib.rs` — remove dataset mount, agentlabd references, update tests
3. `rust/crates/lab-runner/src/agentlab_entrypoint.sh` — delete file
4. `sdk/README.md` — remove `/dataset` mount from contract docs
5. `docs/DOMAIN_MODEL.md` — remove trial-local dataset path from model
6. `docs/host-path-contract.md` — remove replay/fork dependency on trial-local dataset presence

---

## Changes (in order)

### 1. `lab-core/src/lib.rs`

- **Delete** `AGENTLAB_AGENTLABD_START_REQUEST_PATH` and `AGENTLAB_AGENTLABD_START_RESPONSE_PATH` constants (lines 22-25)
- **Delete** `agentlabd_start_request` and `agentlabd_start_response` fields from `RunnerRuntimeHostPaths` struct (lines 64-65)
- **Delete** their initialization in `runner_runtime_host_paths()` (lines 93-94)
- **Delete** dead `entrypoint` and `harness_invocation` fields from `RunnerRuntimeHostPaths` and their initialization

### 2. `lab-runner/src/lib.rs` — agentlabd cleanup

- **Remove** `AGENTLAB_AGENTLABD_START_REQUEST_PATH` and `AGENTLAB_AGENTLABD_START_RESPONSE_PATH` from the `use lab_core` import block (lines 7-8)
- **Remove** `agentlabd_start_request_host` / `agentlabd_start_response_host` from the destructuring tuple in `prepare_io_paths()` (lines ~14498-14499, ~14517-14518, ~14546-14547)
- **Remove** the discard line `let _ = (agentlabd_start_request_host, ...)` and associated `ensure_dir`/`remove_file` calls (~lines 14596-14606)

### 3. `lab-runner/src/lib.rs` — dataset mount removal

- **Remove** `dataset: PathBuf` and `dataset_src: PathBuf` fields from `TrialPaths` struct (lines ~12742, ~12746)
- **Change** `TrialPaths::new()` signature: remove `dataset_src: &Path` parameter
- **Remove** `dataset`/`dataset_src` initialization from `new()`
- **Remove** `ensure_dir(&self.dataset)` and `fs::copy(dataset_src, ...)` from `prepare()`
- **Remove** `-v {}:/dataset:ro` mount arg (line ~13643)
- **Remove** `dataset` entries from manifest metadata JSON arrays (two locations: container-mode and local-mode)
- **Remove** `"dataset"` from materialization policy cleanup lists in `apply_materialization_policy()`
- **Update callers** of `TrialPaths::new()`: `execute_slot` (~5479), `replay_trial` (~2549), `fork_trial` (~2834)
- **Remove** `first_file_in_dir()` function (~lines 2653-2664) — its only callers are the replay/fork dataset_src lines being deleted
- **Remove** `let dataset_src = first_file_in_dir(...)` lines in replay_trial (~2531) and fork_trial (~2807)

### 4. `lab-runner/src/lib.rs` — test updates

- **Update** `create_trial_paths_fixture`: remove dataset_src, update `TrialPaths::new()` call
- **Update** `stage_dependencies_for_trial_copies_into_trial_namespaces`: same
- **Update** `materialize_workspace_files_writes_utf8_and_base64`: same
- **Update** `build_trial_input_uses_run_id_and_limits`: same
- **Update** `seed_parent_trial`: remove `ensure_dir(trial_dir.join("dataset"))`, remove `fs::write(trial_dir.join("dataset")...)`, remove `"dataset"` from runtime.paths JSON fixture
- **Keep** the `/dataset/tasks.jsonl` rejection test — still valid, we still want that path rejected

### 5. Delete `rust/crates/lab-runner/src/agentlab_entrypoint.sh`

Dead file — runner never invokes it.

### 6. Spec/doc alignment

- **Update** `sdk/README.md` trial container mounts list to remove `/dataset`
- **Update** `docs/DOMAIN_MODEL.md` trial layout and `TrialPaths` table to remove `dataset`/`dataset_src`
- **Update** `docs/host-path-contract.md` replay/fork compatibility note to remove dataset dependency

---

## Verification

```bash
cd rust/
cargo check -p lab-core
cargo check -p lab-runner
cargo test -p lab-core
cargo test -p lab-runner
cargo clippy -p lab-core -p lab-runner
```

Key tests to watch:
- `contract_path_mapper_*` tests — still pass (no dataset in contract path mapping)
- `stage_dependencies_for_trial_*` — passes with updated TrialPaths
- `execute_schedule_engine` integration tests — dataset_path still flows for `load_tasks()`, unaffected
