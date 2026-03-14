# Workspace & Grading Overhaul

## 1. Delete swebench adapter shims

Both swebench "grader" adapters are record formatters that don't evaluate anything. Delete them.

### Files to delete

- `adapters/swebench/swebench_task_container_grader.py` — reads agent exit code as verdict
- `adapters/swebench/swebench_official_benchmark_adapter.py` — reads pre-existing `verdict` field from result.json
- `adapters/swebench/_swebench_meta.py` — shared helper, only consumed by the above
- `adapters/swebench/__init__.py` — package init

### References to clean up

- `rust/crates/lab-runner/src/tests.rs` — test fixtures referencing these files, adapter IDs (`swebench_task_container_grader`), and container paths (`/agentlab/deps/swebench/swebench_task_container_grader.py`)
- `rust/crates/lab-runner/src/runner.rs` — runtime asset staging spec referencing the grader script path
- `tests/e2e_cli/test_cli_e2e.py` — e2e assertions on `adapter_id` and grader command path
- `scripts/build_swebench_lite_task_boundary_v3.py` — `DEFAULT_ADAPTER_ID = "swebench_task_container_grader"`
- `scripts/agentlab/tests/test_swebench_meta.py` — imports both adapters

---

## 2. Remove chain state machinery

Chain state workspace persistence is being removed. Each trial gets a fresh workspace; no cross-trial workspace accumulation.

### Types (`types.rs`)

- `ChainRuntimeState` struct — `chain_root_snapshot_ref`, `chain_root_snapshot_manifest`, `latest_snapshot_ref`, `latest_workspace_ref`, `step_index`
- `StatePolicy` enum — `IsolatePerTrial`, `PersistPerTask`, `Accumulate`

### IO (`io.rs`)

- `restore_workspace_from_object_ref()` / `restore_workspace_from_object_ref_with_limit()` — restores workspace bundle from artifact store to host
- `capture_workspace_object_ref()` / `capture_workspace_object_ref_with_limit()` — reads host files, base64-encodes, stores in artifact store

### Lifecycle (`lifecycle.rs`)

- Chain key / step index resolution
- `existing_workspace_ref` resolution from chain state
- Workspace bundle capture after trial
- Chain state update (`chain_states` HashMap insert)
- `chain_root_snapshot_manifest` / `chain_root_snapshot_ref` tracking
- `resolve_chain_label()` if only used for chain state
- Trial result JSON fields: `latest_workspace_ref`, `chain_root_snapshot_ref`

### Anything that consults `StatePolicy` to decide whether to restore a previous workspace or materialize fresh.

---

## 3. Add `ImageProvided` workspace base kind

New `WorkspaceBaseKind` variant for self-contained task images (e.g. swebench eval images) where the repo is baked into the Docker image.

### Types (`types.rs`)

- Add `ImageProvided` to `WorkspaceBaseKind` enum

### Validation (`io.rs`)

- `validate_workspace_base()` — `ImageProvided` rejects `dataset_pack_ref`, `repo`, and `commit` (same constraints as `Empty`)
- `validate_task_boundary_workspace_materialization()` — `ImageProvided` is a valid base for patch-mode tasks (unlike `Empty`)

### Materialization (`io.rs`)

- `materialize_workspace_base()` — `ImageProvided` arm is a no-op; do not create or populate a host workspace directory

### Docker mounts (`io.rs`)

- When workspace base is `ImageProvided`, do **not** bind-mount a host directory to `/workspace` — the image's own filesystem is the workspace
- Continue mounting `/agentlab/in`, `/agentlab/out`, `/agentlab/deps` for I/O

### Snapshotting

- Pre/post snapshots should happen inside the container via `docker exec` rather than host filesystem walks
- For initial implementation, snapshotting can be skipped for `ImageProvided` workspaces
- Diffs can be calculated with `docker exec` before agent runs and after agent exits

### Config / parsing

- Experiment JSON with `workspace.base.kind: "image_provided"` must parse correctly
- Serde deserialization for the new enum variant
