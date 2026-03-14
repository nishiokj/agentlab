# Async Docker Cutover Proposal

Status: proposal. This is a major architectural change.

## Summary

Replace the synchronous `std::process::Command`-based Docker invocation layer with an async Bollard + Tokio architecture. Restructure the trial execution hot path around three explicit phases (materialize, execute, grade) with programmatic container lifecycle control. Simultaneously simplify the experiment YAML authoring, task boundary contract, variant model, and output envelope.

This is a full-stack change touching the experiment schema, build stage, runtime executor, grading pipeline, and CLI entry points.

---

## Part 1: Experiment Schema Overhaul

### Current state

Experiments are authored either as YAML directly or via a JavaScript SDK (`ExperimentBuilder`) that emits YAML. The YAML mixes runtime plumbing with user intent. Variants are split into `baseline` + `variant_plan[]` with special semantics. The agent command is specified inline. Workspace materialization details (overlays, aux_mounts, dependency files) leak into the experiment definition.

**Current YAML shape** (`experiment.yaml`):
- `experiment` — id, name, description, workload_type, owner, tags
- `dataset` — suite_id, provider, path, schema_version, split_id, limit
- `design` — sanitization_profile, comparison, replications, random_seed, shuffle_tasks, max_concurrency
- `baseline` — variant_id, bindings
- `variant_plan[]` — variant_id, bindings
- `runtime.agent` — command, image, io (input_arg, output_arg), artifact, image_source, env, env_from_host
- `runtime.policy` — timeout_ms, network, sandbox (mode, root_read_only, hardening, resources)
- `benchmark` — policy (task_model, evaluator_mode, scoring_lifecycle, chain_failure_policy), adapter (command, manifest)
- `validity` — fail_on_state_leak, fail_on_profile_invariant_violation

### New YAML shape

```yaml
experiment:
  id: swebench_eval
  name: SWE-bench Lite Eval
  owner: jevin

dataset:
  path: ./swebench_lite_tasks.jsonl

agent:
  artifact: .lab/agents/nova-current.tar.gz
  # No command. Agent implements CLI contract:
  #   <binary> --input <path> --output <path> [variant args]

variants:
  glm_5:
    bindings: { model_provider: z.ai-coder, model: glm-5 }
    args: [--session-key, "${TRIAL_ID}", --dangerous]
  gpt_5_3:
    bindings: { model_provider: codex, model: gpt-5.3-codex-spark }
    args: [--session-key, "${TRIAL_ID}", --dangerous]

artifact_type: patch_submission

grading:
  strategy: image_baked
  command: [python, /opt/bench/run_tests.py]

design:
  replications: 1
  max_concurrency: 1
  random_seed: 42

limits:
  trial_seconds: 600

network:
  mode: full
```

### Changes

1. **No `command`** — the agent binary implements a CLI contract. The runner constructs the invocation: `<binary> --input <input_path> --output <output_path> [variant args]`. The binary path is derived from the mounted artifact.

2. **Flat variant map** — variants are keyed by ID. No baseline/treatment distinction. That's an analysis-time concern, not a runtime one. Removes `baseline` and `variant_plan[]` top-level keys.

3. **No workspace spec in YAML** — workspace details (base kind, overlays, aux_mounts, dependencies) move to the task JSONL. The experiment YAML doesn't know about workspace materialization.

4. **`artifact_type` declared at experiment level** — declares what the agent produces (e.g. `patch_submission`). Narrow set of types initially.

5. **`grading` with `strategy`** — one of three strategies (see Part 5).

6. **`runtime` section eliminated** — replaced by `agent`, `network`, `limits`. Sandbox hardening becomes implicit (always enabled).

### Files affected

- `rust/crates/lab-runner/src/config.rs` (1710 lines) — experiment YAML parsing, knob validation, runtime resolution. **Major rewrite** of all config parsing functions.
- `rust/crates/lab-runner/src/validations.rs` (1488 lines) — preflight checks reference current schema shape. **Rewrite** preflight validators against new schema.
- `schemas/resolved_experiment.jsonschema` — **Replace** with new schema.
- `scripts/build-curated-swebench-lite.mjs` and all `build_swebench_curated_ab_experiment.mjs` instances — **Delete or rewrite** to emit new YAML shape.

### Types to change (`types.rs`)

- **Delete**: `BenchmarkPolicyConfig.task_model`, `BenchmarkPolicyConfig.scoring_lifecycle`, `BenchmarkPolicyConfig.evaluator_mode`, `BenchmarkPolicyConfig.chain_failure_policy`
- **Delete**: `StatePolicy` enum (`IsolatePerTrial`, `PersistPerTask`, `Accumulate`)
- **Delete**: `TaskModel` enum (`Independent`, `Dependent`)
- **Restructure**: `Variant` — remove `runtime_overrides`, keep `id`, `bindings`, `args`, `env`
- **Add**: `GradingStrategy` enum (`ImageBaked`, `Injected`, `Separate`)
- **Add**: `ArtifactType` enum (initially `PatchSubmission`)
- **Add**: `GradingConfig` struct (`strategy`, `command`, `script_path?`, `image?`)

---

## Part 2: Task JSONL Overhaul

### Current state

Each line in the task JSONL is a full `TaskDeclaration` with `schema_version`, `task` (opaque payload), `environment` (image), `workspace` (mode, base, overlays, aux_mounts), `dependencies`, and `limits`. The runner materializes the workspace on the host from these specs.

### New task JSONL shape

```json
{
  "id": "django__django-12345",
  "image": "swebench/sweb.eval.x86_64.django__django-12345:latest",
  "workdir": "/testbed",
  "prompt": "Fix the issue described in the following problem statement...",
  "workspace": { "kind": "image_provided" },
  "grading": { "enabled": true }
}
```

### Changes

1. **`workdir` is required** — becomes `docker run -w <workdir>`. No more runner-side workspace path resolution.

2. **`workspace.kind: image_provided`** — new variant (per `WORKSPACE_AND_GRADING_OVERHAUL.md`). The image already contains the workspace. No host-side materialization, no git checkout, no dataset pack extraction, no overlay application.

3. **Simplified shape** — `environment` wrapper removed, `image` is top-level. `dependencies` and `limits` removed from per-task (limits are experiment-level). Dependencies that were files-in-JSON move to image build time.

### Types to change (`types.rs`)

- **Add**: `WorkspaceBaseKind::ImageProvided` variant
- **Simplify**: `TaskDeclaration` — remove `environment` wrapper (image is top-level), remove `dependencies`, remove `limits` (experiment-level)
- **Add**: `workdir: String` field
- **Delete**: `TaskSpec` (redundant with simplified `TaskDeclaration`)
- **Delete**: `TaskBoundaryMaterialization` struct in `io.rs` (replaced by simpler per-task config)

### Files affected

- `rust/crates/lab-runner/src/io.rs` — `parse_task_declaration()`, `parse_task_spec()`, `validate_task_declaration()`, `validate_task_spec()`, `materialize_task_boundary()`, `parse_task_boundary_from_packaged_task()`. **All rewritten** to parse new shape.
- `rust/crates/lab-runner/src/types.rs` — type changes listed above.
- `schemas/task_declaration_v1.jsonschema` — **Replace** with new schema.
- `scripts/build_swebench_lite_task_boundary_v3.py` — **Delete** (task boundary v3 concept replaced).

---

## Part 3: Build Stage Simplification

### Current state

`build_experiment_package()` in `lifecycle.rs:2931-3135` does:
- Load YAML with overrides
- Create package directory structure
- Stage tasks and dataset (copy JSONL)
- Stage task workspace dependencies (resolve dataset packs, git checkouts to host paths)
- Rewrite runtime/benchmark asset paths for portability
- Stage command path references and env path references
- Write resolved_experiment.json with all paths rewritten
- Calculate SHA256 checksums of all package files
- Write checksums.json and package.lock

This is ~200 lines of path staging, rewriting, and portability logic that exists because the current design externalizes workspace state from the image.

### New build stage

Build becomes **compile**: validate + seal.

1. Load YAML
2. Validate schema (experiment, dataset, variants, grading, limits)
3. Validate dataset JSONL (all tasks have `id`, `image`, `workdir`)
4. Resolve relative paths (dataset path, agent artifact path)
5. Compute agent artifact digest (SHA256)
6. Write sealed `experiment.json` with resolved paths + digest
7. Write `checksums.json`

No task workspace dependency staging. No runtime asset path rewriting. No dataset pack resolution. No git checkout caching. The sealed JSON is a direct representation of the YAML with paths resolved and the artifact pinned.

### Functions to delete from `lifecycle.rs`

- `stage_task_workspace_dependencies_for_package()` (2287-2367)
- `stage_source_into_package()` (2377-2416)
- `stage_public_runtime_path_reference()` (2417-2451)
- `rewrite_packaged_runtime_asset_entries()` (2453-2490)
- `stage_command_path_refs_for_package()` (2492-2535)
- `stage_runtime_command_env_path_refs_for_package()` (2537-2593)
- `collect_command_staging_entries()` (2595-2629)
- `collect_runtime_command_env_staging_entries()` (2631-2687)
- `lookup_runtime_staging_entry()` (2688-2701)
- `matches_contract_runtime_root()` (2702-2707)
- `collect_packaged_runtime_asset_entries()` (2709-2747)
- `merge_runtime_path_staging_entries()` (2749-2764)
- `write_runtime_staging_manifest()` (2765-2828)
- `rewrite_runtime_paths_for_package()` (2829-2868)
- `rewrite_benchmark_paths_for_package()` (2870-2912)

That's ~630 lines of build-stage path staging deleted.

### Functions to rewrite

- `build_experiment_package()` (2931-3135) — **Rewrite** to the simplified compile flow above.

---

## Part 4: Async Docker Executor (Bollard + Tokio)

### Current state

All Docker interaction is via synchronous `std::process::Command` shell-outs in `io.rs`:

- `build_baked_container_command()` (3471-3487) — constructs `docker run --rm` command
- `append_container_sandbox_args()` (3244-3382) — adds ~140 lines of `-v`, `--network`, `--read-only`, `--security-opt`, `--cap-drop`, `--cpus`, `--memory`, `--tmpfs` args
- `append_container_env_args()` (3383-3407) — adds `-e KEY=VALUE` args
- `append_container_entrypoint()` (3409-3469) — wraps agent+grader in shell script
- `run_external_agent_runtime_trial()` (3089-3192) — orchestrates: validate artifact → pull images → build command → spawn process → optionally run grader sidecar
- `run_adapter_process()` (4180-4273) — spawns child process, captures stdout/stderr to files
- `run_container_sidecar_command()` (3046-3087) — runs grader as separate docker run
- `ensure_container_image_ready()` (3534-3583) — `docker image inspect` + `docker pull`
- `resolve_container_image_digest()` (3585-3623) — `docker image inspect --format`

The adapter pattern (`AgentAdapter` trait in `core.rs:599`) wraps this:
- `BuiltinCommandAdapter.run_trial()` → `run_command_contract_trial()` → `run_external_agent_runtime_trial()`
- `PrebuiltCommandAdapter.run_trial()` → same path with env overrides

### New architecture: `lab-docker` crate

Create a new crate `rust/crates/lab-docker` that owns all Docker interaction via Bollard.

```
lab-docker/
  src/
    lib.rs           — public API
    client.rs        — Bollard client wrapper, image operations
    container.rs     — container lifecycle (create, start, exec, wait, remove)
    mounts.rs        — mount builder (IO contract mounts, agent artifact mount)
    exec.rs          — exec builder (run commands inside containers)
    types.rs         — ContainerConfig, ExecResult, MountSpec, etc.
```

#### Core types

```rust
/// What the trial executor works with — one container's full lifecycle.
pub struct ContainerSession {
    client: Docker,
    container_id: String,
    image: String,
}

/// Mount specification for the IO contract.
pub struct TrialMountSpec {
    pub input_host: PathBuf,      // → /agentlab/in:ro
    pub output_host: PathBuf,     // → /agentlab/out
    pub metrics_host: PathBuf,    // → /agentlab/metrics
    pub agent_artifact: PathBuf,  // → /opt/agent:ro
}

/// Result of an exec call inside a container.
pub struct ExecOutcome {
    pub exit_code: i64,
    pub stdout: String,
    pub stderr: String,
}
```

#### Container lifecycle API

```rust
impl ContainerSession {
    /// Create and start a container from a task image.
    pub async fn create(
        client: &Docker,
        image: &str,
        workdir: &str,
        mounts: &TrialMountSpec,
        env: &[(String, String)],
        network_mode: &str,
    ) -> Result<Self>;

    /// Execute a command inside the running container.
    pub async fn exec(&self, cmd: &[&str], workdir: Option<&str>) -> Result<ExecOutcome>;

    /// Copy files into the container (for Injected grading).
    pub async fn copy_in(&self, archive: &[u8], dest_path: &str) -> Result<()>;

    /// Copy files out of the container (for collecting artifacts).
    pub async fn copy_out(&self, src_path: &str) -> Result<Vec<u8>>;

    /// Wait for the container's main process to exit.
    pub async fn wait(&self) -> Result<i64>;

    /// Remove the container.
    pub async fn remove(self) -> Result<()>;

    /// Pull an image if not present locally.
    pub async fn ensure_image(client: &Docker, image: &str) -> Result<()>;
}
```

#### What this replaces in `io.rs`

| Current function | Lines | Replacement |
|---|---|---|
| `build_baked_container_command()` | 3471-3487 | `ContainerSession::create()` |
| `append_container_sandbox_args()` | 3244-3382 | `ContainerSession::create()` config |
| `append_container_env_args()` | 3383-3407 | `ContainerSession::create()` env param |
| `append_container_entrypoint()` | 3409-3469 | `ContainerSession::exec()` calls |
| `run_external_agent_runtime_trial()` | 3089-3192 | `TrialExecutor` (see Part 6) |
| `run_adapter_process()` | 4180-4273 | `ContainerSession::exec()` + `wait()` |
| `run_container_sidecar_command()` | 3046-3087 | `ContainerSession::exec()` |
| `ensure_container_image_ready()` | 3534-3583 | `ContainerSession::ensure_image()` |
| `resolve_container_image_digest()` | 3585-3623 | Bollard `inspect_image()` |
| `resolve_container_platform()` | 3517-3527 | Bollard platform config |
| `append_container_platform_arg()` | 3528-3531 | Bollard platform config |
| `resolve_local_image_alias()` | 3510-3516 | `ensure_image()` with alias logic |

### Workspace dependencies

Add to `rust/Cargo.toml` workspace dependencies:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "fs", "process", "sync"] }
bollard = "0.18"
futures-util = "0.3"
```

Add `rust/crates/lab-docker/Cargo.toml`:

```toml
[dependencies]
bollard.workspace = true
tokio.workspace = true
futures-util.workspace = true
anyhow.workspace = true
serde.workspace = true
serde_json.workspace = true
lab-core = { path = "../lab-core" }
```

---

## Part 5: Grading Pipeline

### Current state

Grading is conflated with trial execution. The grader runs either:
1. As part of a shell-script wrapper baked into the container entrypoint (`append_container_entrypoint()` in `io.rs:3409-3469`) — agent and grader run sequentially in one `docker run` via `/bin/sh -lc "agent_cmd; grader_cmd"`.
2. As a sidecar `docker run` via `run_container_sidecar_command()` (`io.rs:3046-3087`).

The grader reads `result.json`, computes a prediction, runs evaluation, and writes `benchmark_prediction.json` + `benchmark_score.json` to `/agentlab/out/`.

The runner then loads these files (`lifecycle.rs:354-378`) and persists them to SQLite.

### New grading architecture: three strategies

All three strategies share the same contract:
- **Input**: output envelope from `/agentlab/out/result.json` (with declared `artifact_type`)
- **Output**: score record written to `/agentlab/out/score.json`

```rust
pub enum GradingStrategy {
    /// Grader is baked into the task image. Hidden from agent via
    /// mv/rename during agent phase, unhidden for grading phase.
    ImageBaked {
        command: Vec<String>,
        hidden_path: String,        // e.g. "/opt/bench"
        hidden_rename: String,      // e.g. "/opt/.bench_hidden"
    },

    /// Grader script is copied into the container after agent exits.
    Injected {
        script_host_path: PathBuf,
        script_container_path: String,
        command: Vec<String>,
    },

    /// Grader runs in a separate container with the output mounted in.
    Separate {
        image: String,
        command: Vec<String>,
    },
}
```

#### ImageBaked execution flow

```rust
// 1. Hide grading assets from agent
session.exec(&["mv", "/opt/bench", "/opt/.bench_hidden"], None).await?;

// 2. Run agent
session.exec(&agent_command, Some(workdir)).await?;

// 3. Unhide grading assets
session.exec(&["mv", "/opt/.bench_hidden", "/opt/bench"], None).await?;

// 4. Run grader
session.exec(&grader_command, Some(workdir)).await?;

// 5. Collect score
let score = session.copy_out("/agentlab/out/score.json").await?;
```

This replaces the 60-line shell script wrapper in `append_container_entrypoint()` with explicit, auditable exec calls.

#### Injected execution flow

```rust
// 1. Run agent
session.exec(&agent_command, Some(workdir)).await?;

// 2. Copy grader script into container
let archive = tar_file(&script_host_path)?;
session.copy_in(&archive, "/opt/grader/").await?;

// 3. Run grader
session.exec(&grader_command, Some(workdir)).await?;
```

#### Separate execution flow

```rust
// 1. Run agent in task container
session.exec(&agent_command, Some(workdir)).await?;

// 2. Copy output envelope out
let result = session.copy_out("/agentlab/out/result.json").await?;
session.remove().await?;

// 3. Create grader container, mount result in
let grader_session = ContainerSession::create(
    &client, &grader_image, "/", &grader_mounts, &[], network_mode
).await?;
grader_session.exec(&grader_command, None).await?;
let score = grader_session.copy_out("/agentlab/out/score.json").await?;
grader_session.remove().await?;
```

### Output envelope contract

The agent writes a structured output envelope to `/agentlab/out/result.json`:

```json
{
  "schema_version": "output_envelope_v1",
  "artifact_type": "patch_submission",
  "format": "v1",
  "base_commit": "abc123",
  "patch_format": "unified_diff",
  "patch": "diff --git a/..."
}
```

`artifact_type` matches the experiment-level declaration. Initially supported:
- `patch_submission` — unified diff patch (SWE-bench)

Future types (not implemented now):
- `text_response` — free-text answer
- `structured_json` — schema-validated JSON
- `file_artifact` — reference to a file in the output directory

### Score record contract

The grader writes to `/agentlab/out/score.json`:

```json
{
  "schema_version": "score_record_v1",
  "verdict": "pass",
  "primary_metric_name": "resolved",
  "primary_metric_value": 1.0,
  "metrics": { "tests_passed": 42, "tests_total": 42 },
  "evaluator": { "name": "swebench.test_runner", "version": "v1" }
}
```

### Files affected

- **Delete**: `adapters/swebench/swebench_task_container_grader.py` (per `WORKSPACE_AND_GRADING_OVERHAUL.md`)
- **Delete**: `adapters/swebench/swebench_official_benchmark_adapter.py`
- **Delete**: `adapters/swebench/_swebench_meta.py`
- **Delete**: `adapters/swebench/__init__.py`
- **Replace**: `schemas/benchmark_prediction_record_v1.jsonschema` → `schemas/output_envelope_v1.jsonschema`
- **Replace**: `schemas/benchmark_score_record_v1.jsonschema` → `schemas/score_record_v1.jsonschema`

---

## Part 6: Trial Executor Rewrite

### Current state

The trial execution hot path spans three files via `include!()`:
- `lifecycle.rs` — `TrialExecutor::execute_slot()` (lines 88-769, ~680 lines) orchestrates the trial
- `io.rs` — `prepare_task_environment()` (2308-2401), `run_external_agent_runtime_trial()` (3089-3192), and ~30 helper functions
- `core.rs` — `AdapterRunRequest` (172-186), `AgentAdapter` trait (599-605)

The flow is: `execute_slot()` → `prepare_task_environment()` → `run_external_agent_runtime_trial()` → `build_baked_container_command()` → `run_adapter_process()`. Everything is synchronous. The grader is baked into the entrypoint shell script or run as a sidecar.

### New trial executor

Replace the monolithic `execute_slot()` + adapter pattern with an async three-phase executor.

```rust
pub struct TrialExecutor {
    docker: Docker,
}

impl TrialExecutor {
    /// Execute a single trial: materialize → run agent → grade → collect.
    pub async fn execute_trial(
        &self,
        task: &TaskConfig,
        variant: &Variant,
        trial_id: &str,
        agent_artifact: &Path,
        grading: &GradingConfig,
        limits: &TrialLimits,
        env: &[(String, String)],
        trial_paths: &TrialPaths,
    ) -> Result<TrialExecutionResult> {
        // Phase 1: Materialize
        let mounts = self.build_mounts(trial_paths, agent_artifact);
        ContainerSession::ensure_image(&self.docker, &task.image).await?;
        let session = ContainerSession::create(
            &self.docker,
            &task.image,
            &task.workdir,
            &mounts,
            env,
            &limits.network_mode,
        ).await?;

        // Phase 2: Execute agent
        let agent_cmd = self.build_agent_command(agent_artifact, trial_paths, variant);
        if matches!(grading.strategy, GradingStrategy::ImageBaked { .. }) {
            session.exec(&["mv", &grading.hidden_path, &grading.hidden_rename], None).await?;
        }
        let agent_result = session.exec(&agent_cmd, Some(&task.workdir)).await?;

        // Phase 3: Grade
        let score = self.execute_grading(&session, grading, trial_paths).await?;

        // Collect outputs
        let result_envelope = self.load_output_envelope(trial_paths)?;
        session.remove().await?;

        Ok(TrialExecutionResult { agent_result, score, result_envelope })
    }
}
```

### What gets deleted from `io.rs`

The following function groups are replaced by `lab-docker` + the new `TrialExecutor`:

**Docker command construction** (~250 lines):
- `build_baked_container_command()` (3471-3487)
- `append_container_sandbox_args()` (3244-3382)
- `append_container_env_args()` (3383-3407)
- `append_container_entrypoint()` (3409-3469)
- `resolve_container_platform()` / `append_container_platform_arg()` (3517-3531)
- `resolve_local_image_alias()` (3510-3516)
- `resolve_container_workspace()` (3233-3236)
- `resolve_agent_execution_image()` (3220-3222)
- `resolve_task_sandbox_image()` (3224-3231)

**Trial execution** (~200 lines):
- `run_external_agent_runtime_trial()` (3089-3192)
- `run_command_contract_trial()` (2946-2949)
- `run_container_sidecar_command()` (3046-3087)
- `run_adapter_process()` (4180-4273)

**Image management** (~90 lines):
- `ensure_container_image_ready()` (3534-3583)
- `resolve_container_image_digest()` (3585-3623)
- `repair_agent_artifact_layout()` (3630-3659)
- `resolve_agent_artifact_mount_dir()` (3661-3677)

**Workspace materialization for non-ImageProvided** (~200 lines, retained but gated):
- `materialize_workspace_base()` (532-570) — add `ImageProvided` arm (no-op)
- `materialize_workspace_git_checkout()` (514-530) — retained for `GitCheckout` kind
- `ensure_git_checkout_cache()` (394-434) — retained
- `materialize_workspace_overlays()` (572-608) — retained for non-ImageProvided

**Chain state** (~200 lines, deleted per `WORKSPACE_AND_GRADING_OVERHAUL.md`):
- `capture_workspace_object_ref()` / `capture_workspace_object_ref_with_limit()` (2729-2795)
- `restore_workspace_from_object_ref()` (2797-2847)
- `resolve_chain_label()` (2848-2861)
- All chain state resolution in `lifecycle.rs` execute_slot() (lines 146-156, 296-304, 431-441, 549-583)

**Adapter pattern** (~60 lines, deleted):
- `AgentAdapter` trait (`core.rs:599-605`)
- `BuiltinCommandAdapter` (`io.rs:2993-3005`)
- `PrebuiltCommandAdapter` (`io.rs:3007-3044`)
- `AdapterRunRequest` struct (`core.rs:172-186`)
- `command_contract_capabilities()` (`io.rs:2937-2944`)
- `prebuilt_adapter_profile_value()` (`io.rs:2986-2991`)

**Pause/control protocol** (retained but reimplemented):
- `pause_command_contract_trial()` (2951-2984) — reimplemented via `ContainerSession::exec()`
- `write_adapter_continue_control()` / `write_adapter_control_action()` (4532-4557) — retained

### What stays in `io.rs`

- Task declaration parsing (`parse_task_declaration()`, validation functions) — rewritten for new schema
- IO path preparation (`prepare_io_paths()`) — simplified
- Trial output loading (`load_trial_output_resilient()`) — retained
- Workspace snapshot/diff functions — retained for evidence collection but moved to optional
- Shell quoting utilities — may still be needed for exec commands
- Binding resolution (`project_bindings_to_args()`, `resolve_command_templates()`) — retained but simplified

---

## Part 7: Scheduling & Lifecycle Integration

### Current state

`lifecycle.rs` handles:
- `execute_schedule_engine_parallel()` (1796-2153) — main scheduling loop
- `execute_parallel_worker_trial()` (1716-1793) — worker thread closure
- `DeterministicCommitter` (1193-1339) — ordered slot commitment
- `RunCoordinator::commit_trial_slot()` (996-1191) — persistence

Workers execute trials in `std::thread` via `LocalThreadWorkerBackend`. Results are sent back via channels and committed in order.

### Changes

The scheduling layer stays mostly intact. The key change is the worker execution closure:

**Current** (`execute_parallel_worker_trial()`, lifecycle.rs:1716-1793):
```rust
// Sync closure called in std::thread
let trial_result = executor.execute_slot(dispatch, ...)?;
```

**New**:
```rust
// Async closure called in tokio::task
let trial_result = trial_executor.execute_trial(task, variant, ...).await?;
```

`LocalThreadWorkerBackend` (`core.rs:228-596`) switches from `std::thread::spawn` to `tokio::task::spawn`. The `WorkerBackend` trait (`core.rs:216-226`) becomes async:

```rust
#[async_trait]
trait WorkerBackend {
    async fn submit(&self, dispatch: TrialDispatch) -> Result<WorkerTicket>;
    async fn poll_completions(&self, timeout: Duration) -> Vec<TrialCompletion>;
    async fn request_stop(&self, worker_id: &str, reason: &str) -> Result<()>;
}
```

### CLI entry point change

`lab-cli/src/main.rs` currently calls `lab_runner::run_experiment_with_options()` synchronously. It needs a tokio runtime.

```rust
// Current (main.rs:781)
fn main() -> Result<()> {

// New
#[tokio::main]
async fn main() -> Result<()> {
```

Lab-runner's public API becomes async:

```rust
// Current (core.rs:1432-1453)
pub fn run_experiment(path: &Path) -> Result<RunResult>
pub fn run_experiment_with_options(path: &Path, options: RunExecutionOptions) -> Result<RunResult>

// New
pub async fn run_experiment(path: &Path) -> Result<RunResult>
pub async fn run_experiment_with_options(path: &Path, options: RunExecutionOptions) -> Result<RunResult>
```

### TUI consideration

Lab-cli uses `ratatui` + `crossterm` for TUI rendering. The TUI event loop can run on a dedicated thread (as it does now) while tokio drives the trial execution. No fundamental conflict — the TUI thread communicates with the async runtime via channels, same as today.

---

## Part 8: Chain State & Workspace Evidence Removal

Per `WORKSPACE_AND_GRADING_OVERHAUL.md`, chain state is deleted. Each trial gets a fresh workspace.

### Types to delete (`types.rs`)

- `ChainRuntimeState` struct (line 733)
- `StatePolicy` enum (line 665) — `IsolatePerTrial`, `PersistPerTask`, `Accumulate`

### Functions to delete

**`lifecycle.rs`:**
- Chain key resolution (execute_slot lines 146-156)
- `existing_workspace_ref` resolution from chain state (lines 172-178 in prepare_task_environment call)
- Chain state update after trial (lines 431-441)
- Chain root snapshot tracking (lines 296-304)
- `resolve_chain_label()` calls
- Trial result JSON fields: `latest_workspace_ref`, `chain_root_snapshot_ref`
- Chain state record assembly and persistence (lines 549-583)
- `task_chain_states.jsonl` writing

**`io.rs`:**
- `restore_workspace_from_object_ref()` (2797-2847)
- `restore_workspace_from_object_ref_with_limit()` (called by above)
- `capture_workspace_object_ref()` (2729-2735)
- `capture_workspace_object_ref_with_limit()` (2737-2795)
- `resolve_chain_label()` (2848-2861)

**`config.rs`:**
- `StatePolicy` parsing and validation

### Evidence that stays (optional, per-trial)

Pre/post workspace snapshots and diffs are useful for debugging and can remain as optional evidence collection. For `ImageProvided` workspaces, these happen via `docker exec git diff` inside the container rather than host filesystem walks.

---

## Part 9: Module Restructuring

### Current state

Lab-runner is a single crate with 6 source files stitched together via `include!()` in `lib.rs`:

```
lib.rs (36 lines) — includes everything
├── core.rs (1480 lines) — types, traits, leases, entrypoints
├── lifecycle.rs (3564 lines) — scheduling, packaging, execution
├── io.rs (5057 lines) — workspace, Docker, IO, adapters, preflight
├── runner.rs (3400 lines) — continue/recover/replay/fork/pause/kill
├── validations.rs (1488 lines) — preflight checks
├── config.rs (1710 lines) — experiment config parsing
├── types.rs (1053 lines) — shared types
├── sink.rs (293 lines) — run sink trait
├── persistence/ — SQLite store
└── tests.rs (12298 lines) — test suite
```

Total: ~30,379 lines in one compilation unit (via `include!()`).

### New structure

```
lab-docker/          — NEW CRATE: async Docker client wrapper
  src/
    lib.rs
    client.rs        — Bollard client, image ops
    container.rs     — ContainerSession lifecycle
    mounts.rs        — mount specification builder
    types.rs

lab-runner/
  src/
    lib.rs           — module declarations (no more include!())
    types.rs         — shared types (slimmed)
    config.rs        — experiment YAML parsing (rewritten for new schema)
    build.rs         — NEW: build/compile stage (extracted from lifecycle.rs)
    executor.rs      — NEW: async TrialExecutor (three-phase)
    grading.rs       — NEW: GradingStrategy implementations
    scheduler.rs     — extracted from lifecycle.rs (schedule engine, worker backend)
    coordinator.rs   — extracted from lifecycle.rs (DeterministicCommitter, slot commitment)
    ops.rs           — extracted from runner.rs (continue/recover/pause/kill)
    preflight.rs     — extracted from validations.rs
    persistence/     — SQLite store (unchanged)
    sink.rs          — run sink (unchanged)
    evidence.rs      — workspace snapshots, diffs (extracted from io.rs)
    tests/           — test modules (split from monolithic tests.rs)
```

### Key principle

The `include!()` pattern is eliminated. Each file becomes a proper module with explicit `pub` exports. This enables incremental compilation and makes dependencies between modules visible.

---

## Part 10: Migration Strategy

This is too large for a single PR. Recommended sequence:

### Phase 1: Foundation (non-breaking)

1. **Add `lab-docker` crate** with Bollard wrapper. No callers yet. Write integration tests against Docker daemon.
2. **Add `tokio` and `bollard` to workspace deps**. Add `tokio` to `lab-cli` with `#[tokio::main]`.
3. **Add `WorkspaceBaseKind::ImageProvided`** to types.rs. Add no-op arm to `materialize_workspace_base()`. Add validation. This is per `WORKSPACE_AND_GRADING_OVERHAUL.md`.
4. **Add output envelope schema** (`schemas/output_envelope_v1.jsonschema`) and score record schema (`schemas/score_record_v1.jsonschema`).

### Phase 2: Delete dead code

5. **Delete swebench adapter shims** (per `WORKSPACE_AND_GRADING_OVERHAUL.md`).
6. **Delete chain state machinery** — `ChainRuntimeState`, `StatePolicy`, `capture/restore_workspace_object_ref`, chain resolution in execute_slot.
7. **Delete baseline/treatment variant semantics** — flatten to variant map.

### Phase 3: New executor (parallel path)

8. **Implement async `TrialExecutor`** in `lab-runner/src/executor.rs` using `lab-docker`. Wire it as an alternative to the current sync path behind a feature flag or config switch.
9. **Implement `GradingStrategy`** enum and the three grading flows.
10. **Implement new experiment YAML parser** alongside old one (new schema version discriminator).

### Phase 4: Cutover

11. **Switch default executor** to async path.
12. **Delete old sync Docker path** — `build_baked_container_command()`, `append_container_*()`, `run_external_agent_runtime_trial()`, `run_adapter_process()`, `AgentAdapter` trait, adapter implementations.
13. **Delete old experiment schema parser**.
14. **Module restructuring** — break `include!()` monolith into proper modules.

### Phase 5: Cleanup

15. **Delete `TaskBoundaryMaterialization`**, `TaskSpec` (redundant after simplification).
16. **Delete build-stage path staging functions** from lifecycle.rs.
17. **Rewrite tests** against new types and async executor.
18. **Update preflight** to use `lab-docker` for image checks and smoke tests.

---

## Appendix: Line-count impact estimate

### Deleted (approximate)

| Source | Lines | What |
|---|---|---|
| `io.rs` Docker command construction | ~250 | `build_baked_container_command`, `append_container_*` |
| `io.rs` trial execution | ~200 | `run_external_agent_runtime_trial`, `run_adapter_process` |
| `io.rs` image management | ~90 | `ensure_container_image_ready`, digest resolution |
| `io.rs` chain state | ~200 | `capture/restore_workspace_object_ref` |
| `io.rs` adapter impls | ~60 | `BuiltinCommandAdapter`, `PrebuiltCommandAdapter` |
| `lifecycle.rs` chain state | ~150 | chain resolution, chain records, chain state updates |
| `lifecycle.rs` build staging | ~630 | all `stage_*`, `rewrite_*`, `collect_*` functions |
| `core.rs` adapter pattern | ~50 | `AgentAdapter` trait, `AdapterRunRequest` |
| `config.rs` old schema parsing | ~500 | baseline/treatment, old runtime shape |
| `adapters/swebench/` | ~420 | all four Python files |
| `tests.rs` (affected tests) | ~2000 | tests for deleted functionality |
| **Total deleted** | **~4,550** | |

### Added (approximate)

| Source | Lines | What |
|---|---|---|
| `lab-docker` crate | ~800 | Bollard wrapper, container lifecycle, mounts, types |
| `executor.rs` | ~300 | Async TrialExecutor, three-phase flow |
| `grading.rs` | ~200 | GradingStrategy implementations |
| `build.rs` | ~150 | Simplified build/compile |
| New config parsing | ~400 | New YAML schema parser |
| New schemas | ~100 | output_envelope, score_record JSON schemas |
| New tests | ~1500 | Tests for new executor, grading, config |
| **Total added** | **~3,450** | |

**Net effect**: ~1,100 fewer lines, dramatically simpler architecture, async foundation for future work.
