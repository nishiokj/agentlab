# Trial Boundary Spec

The boundary between experiment and trial is the point at which a fully-formed spec is handed off. Everything before that point is experiment-owned. Everything after is trial-owned. This document captures the current state and the target.

## Current Pre-Launch Steps

Every step that occurs between "the schedule engine decides to dispatch a slot" and "execute_trial_runtime is called."

| #  | Step | Current Location | Belongs In | Notes |
|----|------|-----------------|------------|-------|
| 1  | Pick slot from schedule | `experiment/runner.rs` dispatch loop (~718-722) | experiment | Index into `&[TrialSlot]` |
| 2  | Check pruned variants / concurrency limits | `experiment/runner.rs` dispatch loop (~723-737) | experiment | `pruned_variants`, `max_in_flight_per_variant` |
| 3  | Allocate `trial_id`, `trial_dir` | `experiment/runner.rs` dispatch loop (~739-743) | experiment | `format!("trial_{}", index + 1)` |
| 4  | Create `TrialPaths` (scratch dirs, in/out/workspace/state/tmp) | `experiment/runner.rs` (~744) → `trial/prepare.rs:63` | experiment | Only exists to support host-side workspace shadow. Without workspace, trial just needs IO contract paths. |
| 5  | `TrialPaths::prepare()` — `ensure_dir` for in/workspace/state/out/tmp | `experiment/runner.rs` (~745) → `trial/prepare.rs:79` | experiment | Creates host dirs that get bind-mounted into container. |
| 6  | Build `LocalTrialLaunch` — bundle slot + trial_id + paths | `experiment/runner.rs` (~746-751) | experiment | Ownership transfer struct for thread spawn. |
| 7  | `spawn_local_trial` — `thread::spawn(move \|\| ...)` | `experiment/runner.rs` (~752) → `experiment/runner.rs:461` | experiment | Thread boundary. `Arc<Context>` shared, launch moved. |
| 8  | Worker-local bookkeeping — payload dir, `BufferedRunSink`, `ArtifactStore`, local chain states | `experiment/runner.rs:370-391` | experiment | Per-worker isolation. All experiment-owned resources. |
| 9  | Build `ScheduledTrialRequest` — borrows from `Arc<Context>` + local mutables | `experiment/runner.rs:393-415` | experiment | The request struct itself is experiment scaffolding. |
| 10 | Clone variant + task from schedule indices | `trial/schedule.rs:162-167` | experiment | `variants[slot.variant_idx].clone()`, `tasks[task_idx]` |
| 11 | Parse task boundary from packaged task | `trial/schedule.rs:168` → `trial/spec.rs:116` → `trial/spec.rs:64` → `trial/spec.rs:77` | experiment | `parse_task_row` is fine. `materialize_task_row` invents workspace defaults (Scratch/Empty) — that policy decision belongs in experiment config, not the task row parser. |
| 12 | Validate task boundary workspace materialization | `trial/schedule.rs:169` → `trial/spec.rs:170` | experiment | Validates workspace spec constraints. |
| 13 | Extract `task_id` from task payload | `trial/schedule.rs:170-175` | experiment | Fallback to `task_{idx}`. |
| 14 | Check grading enabled vs task-level grading flag | `trial/schedule.rs:176-183` | experiment | Rejects tasks that disable grading when benchmark requires it. |
| 15 | Resolve effective task policy — merge experiment + benchmark + task-level overrides | `trial/schedule.rs:187-191` | experiment | Three-layer policy merge. Pure experiment config logic. |
| 16 | Compute chain label, chain key, chain step index | `trial/schedule.rs:192-202` | experiment | `format!("{}::{}", variant.id, chain_label)`. Reads `chain_states` map. |
| 17 | Increment `trial_index` counter, allocate `trial_id` (again) | `trial/schedule.rs:205-206` | experiment | Redundant with step 3 — trial_id is allocated twice. |
| 18 | Create `trial_dir`, write `trial_state.json` ("running") | `trial/schedule.rs:207-209` → `engine.rs:334` | experiment | Initial trial state. |
| 19 | Create `TrialStateGuard` (RAII) | `trial/schedule.rs:210` → `engine.rs:351` | trial | Guard travels with the trial. Consumed on completion. |
| 20 | Lookup existing workspace ref from chain states | `trial/schedule.rs:212-222` | experiment | Only relevant for `PersistPerTask` / `Accumulate` policies. |
| 21 | **Materialize workspace base on host** — Empty (no-op), DatasetPack (`copy_dir_filtered`), or GitCheckout (clone + checkout) | `trial/prepare.rs:481-488` → `runtime.rs:355,401` | **DELETE** | Host-side shadow filesystem. The container image has a filesystem. Data for the agent belongs in the task image or `/agentlab/in`. |
| 22 | **Materialize workspace overlays on host** — write inline files to host workspace dir | `trial/prepare.rs:488` → `runtime.rs` | **DELETE** | Same. Overlays are data for the agent — deliver via contract, not host shadow. |
| 23 | **Materialize task bundle on host** — for `BaseImageBundle` kind | `trial/prepare.rs:491-497` → `runtime.rs:1906` | **DELETE** | Same pattern. |
| 24 | **Materialize task dependencies on host** | `trial/prepare.rs:504-506` → `runtime.rs` | **DELETE** | Same pattern. |
| 25 | **Stage agent runtime dependencies on host** | `trial/prepare.rs:506` → `runtime.rs` | **DELETE** | Same pattern. |
| 26 | Resolve workspace aux mounts → `Vec<ResolvedMountReference>` | `trial/prepare.rs:507-514` → `runtime.rs` | experiment | Mount planning is experiment config. But the mounts themselves are workspace-adjacent slop. |
| 27 | Build `trial_input` JSON (trial_input_v1) | `trial/prepare.rs:189-248` via `build_trial_input()` | experiment | Pure data assembly from experiment + variant + task. |
| 28 | Serialize trial_input, write to host, prepare IO paths | `trial/prepare.rs:525-526` via `prepare_io_paths()` | experiment | Creates host files at `TrialPaths.in_dir` / `TrialPaths.out`. Maps container contract paths to host paths. |
| 29 | Resolve trial timeout | `trial/prepare.rs:527` | experiment | Reads from trial_input JSON. |
| 30 | Build runtime contract env vars (`AGENTLAB_*`) | `trial/prepare.rs:528-534` via `build_runtime_contract_env()` | experiment | Env var map for the container exec. |
| 31 | Build `TaskSandboxPlan` | `trial/prepare.rs:434-455` via `build_task_sandbox_plan()` | experiment | Image, workdir, IO mount plan, artifact mount, network, timeout. |
| 32 | Build `PreparedTaskEnvironmentManifest`, write to disk | `trial/prepare.rs:535-569` | experiment | Manifest of everything prepared. |
| 33 | Store `trial_input` bytes in artifact store | `trial/schedule.rs:268-269` | experiment | SHA256-referenced content-addressed storage. |
| 34 | Write attempt object to SQLite store | `trial/schedule.rs:270-279` | experiment | Bookkeeping. |
| 35 | Assemble `PreparedScheduledTrial` | `trial/schedule.rs:281-311` | experiment | Aggregation of all the above into one struct. |
| 36 | Write `trial_metadata.json` | `trial/schedule.rs:313` | experiment | Metadata record. |
| 37 | Stage benchmark trial preflight | `trial/schedule.rs:314-324` | experiment | Stages grader-related preflight checks. |
| 38 | Enter retry loop | `experiment/runner.rs:419-440` | experiment | Retry policy is experiment config. |
| 39 | Build `AdapterRunRequest` — all borrowed from `PreparedScheduledTrial` | `trial/schedule.rs:335-358` | **BOUNDARY** | This is where the spec should be fully formed and handed off. |
| 40 | Clear stale output files | `trial/schedule.rs:360-370` | trial | Prep for a clean attempt. |
| 41 | **Call `execute_trial_runtime`** | `trial/schedule.rs:371` → `trial/execution.rs:116` | **TRIAL STARTS** | Ownership of trial lifecycle begins here. |

## Current Post-Trial Steps (finalize_scheduled_trial)

| #  | Step | Current Location | Belongs In | Notes |
|----|------|-----------------|------------|-------|
| 42 | Store pre/post snapshots in artifact store | `trial/schedule.rs:402-415` | experiment | Artifact storage is experiment-owned. |
| 43 | Compute cumulative diffs/patches against chain root | `trial/schedule.rs:417-437` | experiment | Multi-trial chain logic. |
| 44 | Capture workspace bundle to artifact store | `trial/schedule.rs:439-449` | experiment | Chain persistence. Depends on workspace concept — goes away with it. |
| 45 | Update `chain_states` map | `trial/schedule.rs:455-465` | experiment | Experiment-owned mutable shared state. |
| 46 | Store trial_output, stdout, stderr in artifact store | `trial/schedule.rs:467-493` | experiment | Evidence archival. |
| 47 | Build evidence record JSON, validate, append to JSONL | `trial/schedule.rs:496-556` | experiment | Experiment-level aggregation. |
| 48 | Build chain state record, append to JSONL | `trial/schedule.rs:572-608` | experiment | Experiment-level aggregation. |
| 49 | Write state inventory | `trial/schedule.rs:610-623` | experiment | Experiment-level metadata. |
| 50 | Validate hooks | `trial/schedule.rs:626-630` | experiment | Hook validation against manifest. |
| 51 | Determine trial outcome from grader conclusion | `trial/schedule.rs:632-653` | experiment | Maps pass/fail/missing/error → success/failure/missing/error using experiment config. |
| 52 | Classify failure reason | `trial/schedule.rs:654-670` | experiment | grade_error / agent_exit_nonzero / result_parse_error. |
| 53 | Build metric rows, event rows, variant snapshots | `trial/schedule.rs:671-731` | experiment | Row construction from trial output + experiment schema. |
| 54 | Append all rows to `run_sink` | `trial/schedule.rs:732-802` | experiment | Writes to experiment-owned `&mut dyn RunSink`. |
| 55 | Complete `TrialStateGuard` | `trial/schedule.rs:804-810` | trial | Guard consumption. Writes final trial_state.json. |
| 56 | Materialize trial layout (Full/OutputsOnly/MetadataOnly) | `trial/schedule.rs:812-838` | experiment | File layout for human consumption. |
| 57 | Build `TrialExecutionResult` | `trial/schedule.rs:840-858` | experiment | Return struct for scheduler. |

## Types Involved

Current types that participate in this boundary region. Many are redundant or over-abstracted.

| Type | Location | Role | Status |
|------|----------|------|--------|
| `TrialSlot` | `config.rs` | Schedule slot: variant_idx + task_idx + repl_idx | Keep. Experiment type. |
| `LocalTrialLaunch` | `experiment/runner.rs` | Thread dispatch: schedule_idx + trial_id + slot + trial_paths | Keep. Experiment type. |
| `ParallelWorkerExecutionContext` | `experiment/runner.rs` | Immutable shared context cloned into Arc | Keep. Experiment type. |
| `ScheduledTrialRequest<'a>` | `trial/schedule.rs` | Borrow-heavy request threading experiment state into trial functions | **Move to experiment.** Experiment type living in trial module. |
| `PreparedScheduledTrial` | `trial/schedule.rs` | Aggregation of all prep results | **Move to experiment.** Experiment type living in trial module. |
| `TaskBoundaryMaterialization` | `trial/spec.rs` | Parsed task row + invented workspace defaults | **Refactor.** Split parsing (keep) from default invention (delete). |
| `TaskRow` | `trial/spec.rs` | Deserialized task_row_v1 | Keep. Could move to model or experiment. |
| `TaskMaterializationSpec` | `trial/spec.rs` | TaskImage vs BaseImageBundle + bundle ref | Review. Tied to workspace materialization. |
| `TaskMaterializationKind` | `trial/spec.rs` | Enum: TaskImage / BaseImageBundle | Review. Same. |
| `WorkspaceSpec` | `model.rs` | mode + base + overlays + aux_mounts | **DELETE.** Workspace concept. |
| `WorkspaceMode` | `model.rs` | Scratch / Patch | **DELETE.** Workspace concept. |
| `WorkspaceBaseSpec` | `model.rs` | kind + dataset_pack_ref + repo + commit | **DELETE.** Workspace concept. |
| `WorkspaceBaseKind` | `model.rs` | Empty / DatasetPack / GitCheckout | **DELETE.** Workspace concept. |
| `WorkspaceOverlaySpec` | `model.rs` | path + content (utf8 or base64) | **DELETE.** Workspace concept. |
| `WorkspaceAuxMountSpec` | `model.rs` | dataset pack ref + mount path | **DELETE.** Workspace concept. |
| `TrialPaths` | `trial/prepare.rs` | Host paths: trial_dir, scratch_dir, in/workspace/state/out/tmp, runtime | **Refactor.** Remove workspace + scratch. Reduce to IO contract host paths. |
| `PreparedTrialIo` | `model.rs` | Host + container path pairs for IO contract files | Keep. This is the real contract. |
| `PreparedTaskEnvironmentManifest` | `model.rs` | Serialized manifest of everything prepared | **Refactor.** Becomes the trial spec manifest. |
| `PreparedTaskEnvironment` | `trial/prepare.rs` | manifest + trial_paths + io_paths + dynamic_mounts + trial_input | **Refactor.** Most of this becomes experiment output / trial input. |
| `TaskSandboxPlan` | `trial/state.rs` | image + workdir + materialization + io_mounts + artifact_mount + network + timeout | **Refactor.** Core of what becomes TrialSpec. Remove materialization (workspace concept). |
| `IoMountPlan` | `trial/state.rs` | in_dir + out_dir + telemetry_mounts | Keep. Part of sandbox contract. |
| `ArtifactMountPlan` | `trial/state.rs` | host_artifact_path + container_artifact_dir | Keep. Part of sandbox contract. |
| `GradingSandboxPlan` | `trial/state.rs` | strategy + command + io_mounts + output_mode + details | Keep. Part of trial spec. |
| `AdapterRunRequest<'a>` | `engine.rs` | Borrow struct passed to `execute_trial_runtime` | **BOUNDARY TYPE.** This becomes the owned TrialSpec. |
| `TrialRuntimeOutcome` | `trial/execution.rs` | Agent + grading results from a single attempt | Keep. Trial output type. |
| `TrialExecutionResult` | `model.rs` | Deferred records + outcome for scheduler | Keep. Experiment type (post-trial aggregation). |
| `TrialAttemptState` | `trial/state.rs` | Mutable phase-tracked state persisted per attempt | Keep. Trial-owned state machine. |
| `TrialPhase` | `trial/state.rs` | Pending → AgentMaterializing → ... → Committed | Keep. Trial-owned phase enum. |
| `TrialStateGuard` | `engine.rs` | RAII guard for trial_state.json | Keep. Trial-owned. |
| `ResolvedMountReference` | `model.rs` | host_path + mount_path for dynamic mounts | Review. Tied to workspace aux_mounts. |
| `ChainRuntimeState` | `config.rs` | Chain root/latest snapshot refs + workspace ref + step index | Review. Tied to workspace persistence. Goes away or simplifies. |
