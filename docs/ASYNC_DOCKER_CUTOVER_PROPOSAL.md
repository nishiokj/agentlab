# Async Docker Cutover Proposal

Status: Proposal  
Date: 2026-03-13

## Executive Summary

This is not just a transport swap from `std::process::Command` shell-outs to Bollard. It is a hard cutover to a cleaner execution model with explicit ownership boundaries:

1. Authoring and benchmark compilers own user intent and benchmark-specific translation.
2. Build owns validation, path resolution, mapper execution, and sealing.
3. The run engine owns durable scheduling, retries, and slot commit semantics.
4. The agent sandbox owns only candidate artifact production.
5. The grader sandbox owns only grading output production.

The main design decision is simple:

1. Agent success is not a benchmark verdict.
2. Grader exit code is not a benchmark verdict.
3. The only benchmark verdict is a validated `trial_conclusion_v1` carried by `mapped_grader_output.json`.

The async Docker cutover is required because the target flow needs explicit container lifecycle control: create, start, exec, copy-in, copy-out, inspect, stream stats, cancel, and remove. Shell-wrapped `docker run` calls are the wrong abstraction for that.

This document is the source of truth for the cutover. Older patch specs should be treated as stale wherever they disagree with this proposal.

---

## Scope

### In scope

1. New authoring and sealed-spec boundary for experiment execution.
2. Build-time benchmark mapping into generic task rows.
3. Fixed agent CLI contract.
4. Mandatory grading contract and normalized conclusion boundary.
5. Durable run-state orchestration around ephemeral per-attempt sandboxes.
6. Async Docker runtime built on Tokio + Bollard.
7. Run-time environment injection semantics.

### Out of scope

1. Full analysis layer redesign.
2. Remote worker protocol redesign.
3. Legacy-shape backward compatibility shims.
4. Durable in-flight process reattachment.
5. Non-container scientific execution modes.

---

## Architectural Split

This cutover only makes sense if the planes are separated cleanly.

| Plane | Owns | Must Not Own |
|---|---|---|
| Authoring | Experiment intent, variants, policy, mapper references | Host paths, mount topology, hidden-test procedures |
| Build / Compile | Validation, relative path resolution, artifact hashing, mapper execution, sealed `experiment.json` | Secret values, run-time state, active trial orchestration |
| Run Orchestrator | Durable schedule, slot attempts, preflight, retries, commit journal | Benchmark verdict logic, benchmark-native task parsing |
| Agent Sandbox | Executing agent binary against `trial_input.json`, writing `result.json`, and optionally emitting declared telemetry | Hidden tests, grader assets, slot commit decisions |
| Grader Sandbox | Evaluating agent output and writing normalized `trial_conclusion_v1` into `mapped_grader_output.json` | Scheduling, retries, mutating authoring or task inputs |
| Persistence | Host path contract, committed trial facts, attestation | Benchmark-specific execution behavior |

Two important consequences fall out of this split:

1. Benchmark-specific semantics must compile into generic run-time boundaries before `lab-cli run` starts.
2. Mounts are not user-facing configuration. Logical IO roots are first-class. Bind mounts are only the local Docker realization of those roots.

---

## Non-Negotiable Invariants

1. There is exactly one scientific execution shape: durable runner orchestrator plus ephemeral containers.
2. The agent never sees hidden tests, oracle data, or grader-only assets during the agent phase.
3. Grading is mandatory for scored experiments. The runner must not infer pass/fail from agent or grader exit code alone.
4. Benchmark-specific translation happens at build time through mappers or built-in compilers, not in the hot path of run execution.
5. Relative paths are authoring-only. The sealed spec contains machine-scoped absolute paths and digests.
6. Secret values are injected at run time only. They are never embedded into the sealed package.
7. Sandboxes are disposable. Durable state exists at the run and committed-slot layers, not in container memory.
8. Slot commit is the exactly-once boundary. Attempts may be retried.
9. Agent output, runner execution records, and grader conclusions are separate records with separate owners.
10. Copies across boundaries are allowed only when isolation requires them. Redundant staging and duplicate ownership are design bugs.

---

## Target User Flow

The intended flow is:

1. User authors experiment YAML with variants, policies, grading strategy, and optional mapper references.
2. Build compiles that YAML into sealed `experiment.json`, resolving relative paths to absolute machine paths and hashing artifacts.
3. `lab-cli run` accepts run-time env injection and creates a run-local execution overlay.
4. The durable run engine expands the schedule and drives slot attempts.
5. Each attempt materializes an agent sandbox, runs the agent contract, materializes grading, runs grading, validates outputs, and commits the slot.

The important separation is:

1. YAML expresses intent.
2. Sealed JSON expresses machine-resolved configuration.
3. Run overlay expresses ephemeral run-time inputs such as secrets.
4. Attempts execute sandboxes.
5. Commit persists facts.

---

## Authoring Contract

### Authoring YAML

The experiment YAML should express only user intent and build-time references. It should not encode container-internal paths or Docker command fragments.

Example:

```yaml
experiment:
  id: swebench_eval
  name: SWE-bench Lite Eval
  owner: jevin

dataset:
  source: ./datasets/swebench_lite.jsonl
  task_mapper: ./mappers/swebench_task_mapper.py

agent:
  artifact: ./.lab/agents/rex-current.tar.gz
  executable: bin/rex
  artifact_type: patch_submission

variants:
  glm_5:
    bindings: { model_provider: z_ai, model: glm-5 }
    args: [--session-key, "${TRIAL_ID}", --dangerous]
  gpt_5_spark:
    bindings: { model_provider: codex, model: gpt-5.3-codex-spark }
    args: [--session-key, "${TRIAL_ID}", --dangerous]

grading:
  strategy: in_task_image
  command: [python, /opt/grader/run.py]
  conclusion:
    mode: mapper
    mapper: ./mappers/swebench_conclusion_mapper.py
  in_task_image:
    hidden_paths: [/testbed/.hidden]
    revealed_paths: [/testbed/.hidden]

design:
  replications: 1
  max_concurrency: 4
  random_seed: 42

limits:
  time_limit_ms: 600000

network:
  mode: full
```

### Build-time mappers

Users may need to provide two different mappers:

1. `task_mapper`
   Translates benchmark-native rows into generic task rows the runner can execute.
2. `conclusion_mapper`
   Translates grader-native output into the normalized conclusion boundary when the grader cannot emit the generic conclusion schema directly.

These are build-time or grading-time boundary adapters. They are not runner topology hooks.

### What authoring must not contain

1. Host absolute paths after build output is sealed.
2. Container mount destinations like `/opt/agent`, `/agentlab/in`, or `/workspace`.
3. Hidden-test file movement procedures.
4. User-authored `docker run` fragments.
5. Secret values.

---

## Sealed Spec and Run Overlay

### Build output

Build becomes compile-and-seal:

1. Validate authoring schema.
2. Resolve relative paths to absolute machine paths.
3. Execute benchmark input mapping into generic task rows.
4. Resolve and hash the agent artifact.
5. Resolve and hash any mapper scripts or grader assets referenced by the experiment.
6. Emit sealed `experiment.json`.
7. Emit `checksums.json`.

The sealed spec is machine-scoped and immutable. It is the only thing the run engine consumes.

### Run-time env injection

Run-time secrets are injected through the run CLI, not embedded into the sealed package.

Example:

```bash
lab-cli run experiments/swebench_eval.yaml \
  --env OPENAI_API_KEY=... \
  --env ZAI_CODER_API_KEY=...
```

Semantics:

1. `lab-cli run` may implicitly build first if the input is authoring YAML.
2. `--env` values are injected into the agent phase only by default.
3. Grader env injection is separate and must be explicit if ever needed.
4. The run records env key names, scopes, and provenance in run metadata, but not raw secret values.
5. `continue` must fail fast if required env keys are missing when execution resumes.

This keeps the sealed package reproducible while still allowing real credentials at execution time.

### Why the sealed spec and run overlay are separate

Without this separation we get one of two bad outcomes:

1. Secrets leak into the sealed artifact.
2. The sealed artifact is no longer the actual source of truth for execution inputs.

The correct model is:

1. sealed experiment for machine-resolved static inputs
2. run overlay for ephemeral secrets and run-scoped overrides

---

## Generic Task Boundary

The task mapper must emit a generic task row the runner can schedule without benchmark-specific parsing.

Suggested shape:

```json
{
  "id": "django__django-12345",
  "image": "swebench/sweb.eval.x86_64.django__django-12345:latest",
  "workdir": "/testbed",
  "time_limit_ms": 600000,
  "task": {
    "problem_statement": "Fix the failing behavior described here...",
    "repo": "django/django",
    "base_commit": "abc123"
  },
  "materialization": {
    "kind": "task_image"
  }
}
```

The runner should only need:

1. `id`
2. `image`
3. `workdir`
4. opaque benchmark task payload
5. `time_limit_ms`, with experiment-level default applied when omitted
6. enough materialization metadata to make the container executable from the declared workdir

Fallback task shape:

```json
{
  "id": "task_123",
  "image": "ubuntu:22.04",
  "workdir": "/workspace/task",
  "materialization": {
    "kind": "base_image_bundle",
    "task_bundle_ref": "tasks/task_bundles/task_123.tar"
  },
  "task": {
    "problem_statement": "..."
  }
}
```

`base_image_bundle` means the task row does not ship a fully baked task image. Instead, build seals a runnable task bundle, and runtime copies that bundle into the declared workdir of the declared base image. Runtime does not install dependencies in this phase.

Anything beyond that is a build-time concern unless it is required for runtime sandbox creation.

### Why this matters

The run engine should never need to understand:

1. how SWE-bench encodes a task
2. how Bench v0 encodes a task
3. how hidden tests are represented upstream
4. how a benchmark-native grader emits its own score format

That translation belongs to build-time compilers and mappers.

---

## Trial Contract

The runner-agent-grader ABI should be explicit and narrow.

### IO contract surface

Required IO roots:

1. `/agentlab/in`
2. `/agentlab/out`

Optional IO roots:

1. `/agentlab/metrics/<declared-id>`

Local Docker may realize these as bind mounts. Another executor may realize them differently. The IO roots are the contract; the mount implementation is not.

The execution workdir is a separate concern from the IO contract. It may come from the task image itself or from a sealed task bundle copied into a declared base image, but it should not be promoted into a fixed public path like `/agentlab/workspace`.

### Primary files

1. `/agentlab/in/trial_input.json`
2. `/agentlab/in/grader_input.json`
3. `/agentlab/out/result.json`
4. optional `/agentlab/out/raw_grader_output.json`
5. `/agentlab/out/mapped_grader_output.json`
6. optional runner-owned auxiliary grading inputs under `/agentlab/in/grader/...`
7. any telemetry files explicitly declared by the experiment under mounted `/agentlab/metrics/<declared-id>/...`

### Ownership

1. The runner owns `trial_input.json`, `grader_input.json`, runner-created auxiliary grading inputs, logs, and execution metadata.
2. The agent owns `result.json` and any declared telemetry files it emits.
3. The grader owns `raw_grader_output.json` when mapper mode is used, and any declared telemetry files it emits.
4. `mapped_grader_output.json` is the canonical grading boundary. It is written either directly by the grader or by the explicit grader-mapping phase.

This is a hard separation. The runner should not fabricate `result.json`, and the agent should not fabricate `mapped_grader_output.json`.

### What is not part of the base contract

1. No mounted `control.json` file in the base design. Durable scheduling and crash retry do not require a per-trial control mount.
2. No generic `/agentlab/deps` mount. Support files belong in the agent artifact, the grader bundle/image, or the workspace materialization procedure.
3. No fixed `/agentlab/workspace` contract root. The working directory is execution context, not IO ABI.

### Optional telemetry contract

Telemetry mounts exist only when the experiment author declares them.

Suggested declaration shape:

```yaml
agent:
  telemetry:
    - id: hook_events
      phase: agent
      rel_path: hooks/events.jsonl
      schema: hook_events_v1
      collect: tail
```

Lifecycle:

1. User declares telemetry in YAML.
2. Build resolves and validates that declaration into the sealed experiment spec.
3. Attempt planning allocates host paths only for declared telemetry entries and splits them by phase.
4. The runner mounts `/agentlab/metrics/<declared-id>` for each declared entry.
5. Experiment-side code writes the declared file or files there.
6. The runner collector reads only those declared paths using the declared schema and collect mode.
7. The runner enriches collected rows with run-owned identity and writes canonical fact rows.
8. DuckDB reads the canonical fact rows, not the raw emitted telemetry files.

If telemetry is not declared:

1. no metrics mount exists
2. nothing is collected from that surface

The canonical write target is still the runner-owned fact sink. The live DuckDB database is a query mirror over those appended facts, not the source of truth.

### Metric provenance

The proposal should separate where metrics come from instead of flattening them into one vague bucket.

1. Runner structural metrics come from runner-owned timers and process observation.
   Examples: phase start/end timestamps, wall-clock duration, exit code, timeout, signal.
2. Agent-reported metrics come from `result.json.metrics`.
   The runner persists them as agent-owned values and does not reinterpret them as benchmark truth.
3. Telemetry-derived metrics exist only when a telemetry surface is declared and the declared schema supports aggregation.
   Example: declared `hook_events_v1` telemetry can be aggregated into token counts, tool-call counts, or model-call latency summaries.
4. Grader-reported metrics come from `trial_conclusion_v1.payload` or optional projection fields.

This matters because "latency" and "tokens" are not magic runner facts. If we want them in the new flow, we need to be able to point to a concrete producer:

1. runner timing for structural duration
2. declared hook telemetry for token and call aggregation
3. agent `result.json.metrics` for agent-owned opaque metrics
4. grader conclusion payload for grading-owned metrics

---

## Agent CLI Contract

The agent runtime must implement a fixed CLI contract.

Suggested invocation:

```text
<artifact-exec> run --input /agentlab/in/trial_input.json --output /agentlab/out/result.json [variant args...]
```

Notes:

1. The runner constructs the invocation.
2. The executable path is derived from the mounted artifact plus `agent.executable`.
3. Variant `args` are appended as literal `argv`.
4. The agent starts in the declared task `workdir`.
5. The runner may also inject stable identity env vars, but input and output are explicit CLI arguments.

This removes the current ambiguity where the runner both exports stable paths and also synthesizes custom IO templates in multiple ways.

### Allowed templating in variant args

Only runner-owned scalar substitutions should be allowed in `variant.args`, for example:

1. `${TRIAL_ID}`
2. `${VARIANT_ID}`
3. `${TASK_ID}`

No templating should expose host paths or mount destinations.

### Trial input shape

`trial_input.json` should remain a runner-owned envelope that combines:

1. trial identity
2. opaque task payload
3. variant bindings
4. limits and timeouts
5. requested network mode
6. execution facts that are safe for the agent to know about

The existing `trial_input_v1` schema is a reasonable starting point. The main cutover requirement is that the agent consumes a stable runner-owned envelope rather than benchmark-native task rows directly.

---

## Agent Result Envelope

The agent produces a candidate artifact envelope, not a benchmark verdict.

Suggested shape:

```json
{
  "schema_version": "artifact_envelope_v1",
  "artifact_type": "patch_submission",
  "artifact": {
    "base_commit": "abc123",
    "patch_format": "unified_diff",
    "patch": "diff --git a/..."
  },
  "metadata": {
    "agent_version": "rex-2026-03-13"
  }
}
```

Initial `artifact_type` values can be narrow:

1. `patch_submission`
2. `text_response`
3. `structured_json`
4. `file_ref`

The agent envelope says what the agent produced. It does not say whether the benchmark considers that correct.

### Candidate artifact extraction

The lifecycle needs one more explicit step: the runner must turn the raw result envelope into a canonical candidate artifact record.

Rules:

1. The sealed experiment declares the expected logical `artifact_type`.
2. The agent writes `result.json`.
3. The runner validates the envelope schema and validates that the declared artifact matches the expected `artifact_type`.
4. The agent may provide the artifact inline in `result.json` or by file reference under `/agentlab/out`.
5. The runner resolves that into a runner-owned `CandidateArtifactRecord` with a concrete validity state.
6. If extraction fails, that becomes contract state. Grading still runs.

The important boundary is:

1. `result.json` is the agent's declaration of its candidate artifact.
2. artifact extraction is the runner's validation and canonicalization step.
3. benchmark verdict still belongs to grading.

### Runner-owned workspace delta

Regardless of `artifact_type` or workspace materialization kind, the runner should also persist an observed workspace delta for every attempt.

That is a separate lifecycle from the agent artifact:

1. immediately before agent execution, the runner captures a pre-agent observation of the execution work state
2. immediately after agent execution, the runner captures a post-agent observation of the same state
3. the runner derives a delta from pre to post and persists it
4. the runner may derive a patch-like text representation when that delta format supports it

Observation depends on workspace realization:

1. `task_image` -> snapshot the execution workdir inside the container
2. `base_image_bundle` -> snapshot the execution workdir inside the container after the sealed task bundle has been copied into place

This delta is runner-owned evidence. It is not the agent artifact. Some benchmarks may grade the declared candidate artifact, some may prefer the observed workspace delta, and some may inspect both. That choice belongs in the grading logic, not in the agent ABI.

---

## Grader Input and Mapped Output

The grader must not infer everything from exit codes or missing files. It should receive a normalized runner-owned grading input envelope.

### Grader input

Suggested shape:

```json
{
  "schema_version": "grader_input_v1",
  "ids": {
    "run_id": "run_...",
    "trial_id": "trial_...",
    "variant_id": "glm_5",
    "task_id": "django__django-12345"
  },
  "task": {
    "problem_statement": "Fix the failing behavior described here..."
  },
  "artifact_type": "patch_submission",
  "agent_phase": {
    "exit_code": 124,
    "timed_out": true,
    "result_present": false,
    "result_schema_valid": false,
    "started_at": "2026-03-13T10:00:00Z",
    "ended_at": "2026-03-13T10:10:00Z"
  },
  "candidate_artifact": {
    "state": "valid",
    "artifact_type": "patch_submission",
    "source": "result.inline",
    "payload": {
      "base_commit": "abc123",
      "patch_format": "unified_diff",
      "patch": "diff --git a/..."
    }
  },
  "workspace_delta": {
    "state": "available",
    "diff_path": "/agentlab/in/grader/workspace_diff.json",
    "patch_path": "/agentlab/in/grader/workspace.patch"
  },
  "paths": {
    "result_path": "/agentlab/out/result.json"
  },
  "workdir": "/testbed"
}
```

This is the key hardening move for edge cases:

1. Grading still runs when the agent timed out.
2. Grading still runs when `result.json` is missing or malformed.
3. The runner does not invent a benchmark verdict on behalf of the grader.

### Mapped grader output

The grader must emit a normalized conclusion boundary that preserves grader-owned output without forcing benchmark-specific fields into the runner ABI.

Suggested shape:

```json
{
  "schema_version": "trial_conclusion_v1",
  "payload": {
    "verdict": "pass",
    "resolved": 1.0,
    "tests_passed": 42,
    "tests_total": 42
  },
  "reported_outcome": "success",
  "primary_metric": {
    "name": "resolved",
    "value": 1.0
  },
  "grader": {
    "name": "swebench",
    "strategy": "in_task_image",
    "version": "v1"
  }
}
```

This `trial_conclusion_v1` payload is stored in the canonical mapped output file at `/agentlab/out/mapped_grader_output.json`.

If grading cannot produce a valid mapped output, the runner must persist a separate runner-owned grading failure record and commit the slot with status `grading_failed`. It must not fabricate a pass/fail conclusion on the grader's behalf.

### Conclusion mapping

Preferred path:

1. grader writes `trial_conclusion_v1` directly to `/agentlab/out/mapped_grader_output.json`

Allowed escape hatch:

1. grader writes benchmark-native output to `/agentlab/out/raw_grader_output.json`
2. an explicit `grader_mapping` phase runs after grading
3. `conclusion_mapper` reads the raw output and writes `trial_conclusion_v1` to `/agentlab/out/mapped_grader_output.json`

Either way, the committed contract is the normalized conclusion record.

### Runner persistence of grading

The runner should persist the grading boundary exactly as it was produced, plus runner-owned contract state.

That means:

1. `mapped_output_state` is runner-owned contract state.
2. `payload` is grader-owned grading output.
3. `reported_outcome` and `primary_metric`, if present, are grader-owned or mapper-owned reporting projections.
4. If those projection fields are absent, the committed trial record just carries the raw payload and the contract state.
5. The runner must not fabricate a benchmark verdict, score, or success bit that the grading boundary did not provide.

---

## Durable Orchestration Model

The experiment runner is a durable state machine, but the dynamic state must be tracked in the right places.

### Static vs dynamic state

1. Variant is static configuration.
2. Schedule slot is the durable unit of planned work.
3. Attempt is the ephemeral execution instance of a slot.

So the answer to "per variant, per trial, are we using traits or state?" is:

1. Variants are not stateful actors.
2. Slots and attempts are where dynamic execution state lives.

### Durable source of truth

The durable source of truth should be:

1. sealed experiment spec
2. schedule progress
3. slot commit journal
4. committed trial artifacts and facts

Attempt-local state may be persisted for observability, but it is not the correctness boundary.

### Recommended slot/attempt phases

Attempts should move through these phases:

1. `pending`
2. `agent_materializing`
3. `agent_running`
4. `agent_finished`
5. `grader_materializing`
6. `grader_running`
7. `grader_mapping`
8. `commit_pending`
9. `committed`
10. `abandoned`

Only `committed` advances durable schedule progress.

### Crash semantics

The durable behavior is:

1. committed slots survive crash
2. active attempts may be abandoned and retried
3. exactly-once applies to slot publication, not to process attempts

---

## Data Model and Procedure Inputs

The right mental model here is not "a big `TrialExecutor` object." The right mental model is:

1. durable trial state records
2. small enums that select procedures
3. low-level functions that consume one state record and produce the next one

Whether that ends up implemented as a module, namespace, or a thin struct wrapper is secondary. The important thing is the data and the transitions.

### Enums that actually drive behavior

These are the enums that should branch the low-level execution procedures.

```rust
pub enum TaskMaterializationKind {
    TaskImage,
    BaseImageBundle,
}

pub enum GradingStrategy {
    InTaskImage,
    Injected,
    Separate,
}

pub enum TrialPhase {
    Pending,
    AgentMaterializing,
    AgentRunning,
    AgentFinished,
    GraderMaterializing,
    GraderRunning,
    GraderMapping,
    CommitPending,
    Committed,
    Abandoned,
}

pub enum ContractFileState {
    Missing,
    PresentInvalid,
    Valid,
}
```

These are upstream policy selectors. They are not incidental implementation details:

1. `TaskMaterializationKind` decides whether runtime uses a task image as-is or copies a sealed task bundle into a declared base image.
2. `GradingStrategy` decides how grader assets become visible and whether the grader reuses the task container.
3. `TrialPhase` is the durable progression for one attempt.
4. `ContractFileState` prevents us from collapsing "file missing" and "file malformed" into the same vague failure bucket.

### Core state records

Suggested state records:

```rust
pub struct TrialSlot {
    pub schedule_idx: u32,
    pub variant_id: String,
    pub task_id: String,
    pub repl_idx: u32,
}

pub struct TrialAttemptKey {
    pub schedule_idx: u32,
    pub attempt: u32,
}

pub struct TrialAttemptState {
    pub key: TrialAttemptKey,
    pub slot: TrialSlot,
    pub phase: TrialPhase,
    pub fs: AttemptFsLayout,
    pub task_sandbox: Option<TaskSandboxState>,
    pub grading_sandbox: Option<GradingSandboxState>,
    pub agent_phase: Option<AgentPhaseRecord>,
    pub grading_phase: Option<GradingPhaseRecord>,
    pub mapping_phase: Option<GraderMappingPhaseRecord>,
    pub candidate_artifact: Option<CandidateArtifactRecord>,
    pub workspace_delta: Option<WorkspaceDeltaRecord>,
}

pub struct AttemptFsLayout {
    pub attempt_dir: String,
    pub in_dir: String,
    pub out_dir: String,
    pub telemetry_mounts: Vec<DeclaredTelemetryMount>,
    pub logs_dir: String,
}

pub struct DeclaredTelemetryMount {
    pub id: String,
    pub phase: TelemetryPhase,
    pub host_dir: String,
    pub container_dir: String,
    pub rel_path: String,
    pub schema: Option<String>,
    pub collect_mode: CollectMode,
}

pub enum TelemetryPhase {
    Agent,
    Grader,
}

pub enum CollectMode {
    Tail,
    AfterPhase,
}

pub struct AgentPhaseRecord {
    pub started_at: String,
    pub ended_at: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub timed_out: bool,
    pub result_state: ContractFileState,
    pub stdout_path: String,
    pub stderr_path: String,
}

pub struct GradingPhaseRecord {
    pub started_at: String,
    pub ended_at: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub timed_out: bool,
    pub raw_output_state: ContractFileState,
    pub stdout_path: String,
    pub stderr_path: String,
}

pub struct GraderMappingPhaseRecord {
    pub started_at: String,
    pub ended_at: String,
    pub mapped_output_state: ContractFileState,
    pub stdout_path: String,
    pub stderr_path: String,
}

pub struct CandidateArtifactRecord {
    pub state: CandidateArtifactState,
    pub artifact_type: String,
    pub source: CandidateArtifactSource,
    pub payload: Option<serde_json::Value>,
}

pub enum CandidateArtifactState {
    Missing,
    Invalid,
    Valid,
}

pub enum CandidateArtifactSource {
    ResultInline,
    ResultFileRef,
    None,
}

pub struct WorkspaceDeltaRecord {
    pub observation_kind: WorkspaceObservationKind,
    pub pre_observation_path: String,
    pub post_observation_path: String,
    pub diff_path: String,
    pub patch_path: Option<String>,
}

pub struct WorkspaceObservationRecord {
    pub observation_kind: WorkspaceObservationKind,
    pub observation_path: String,
}

pub enum WorkspaceObservationKind {
    ContainerTree,
}
```

Two points matter here:

1. `TrialSlot` is schedule-level and durable.
2. `TrialAttemptState` is the evolving record that each low-level step updates.
3. `AttemptFsLayout` is just the runner's local filesystem layout for one attempt. It is allocated by the runner after slot claim, then consumed by planning and execution code. It is not a user-facing contract.

Its lifecycle is:

1. the runner allocates it immediately after claiming an attempt
2. plan derivation reads it to choose concrete host paths for mounts and files
3. execution code writes contract inputs, logs, and optional declared telemetry into it
4. commit code persists references or copies out whatever must survive the attempt
5. the remainder is disposable attempt-local scratch space

### Spec vs plan vs state

These terms should be used consistently:

1. `spec`
   Durable declarative input. Loaded from the sealed experiment, task row, variant config, or grading config. A spec says what should be true, not how this specific attempt will realize it.
2. `plan`
   Attempt-local operational instructions derived from specs plus run overlay plus host allocation. A plan can include host paths, chosen mount realizations, selected images, and exact procedure branches.
3. `state`
   Observed runtime facts after a step has executed. Container ids, exit codes, file validity states, and timestamps are state.

In other words:

1. `TaskRow`, `VariantSpec`, `GradingConfig` are specs.
2. `TaskSandboxPlan` and `GradingSandboxPlan` are plans.
3. `TrialAttemptState`, `TaskSandboxState`, and `GradingPhaseRecord` are state.

### Task sandbox planning data

Task sandbox materialization should be driven by an explicit plan record, not by ad-hoc branching deep in execution code.

```rust
pub struct TaskSandboxPlan {
    pub image: String,
    pub workdir: String,
    pub materialization: TaskMaterializationPlan,
    pub io_mounts: IoMountPlan,
    pub artifact_mount: ArtifactMountPlan,
    pub network_mode: String,
    pub time_limit_ms: u64,
}

pub struct IoMountPlan {
    pub in_dir: String,
    pub out_dir: String,
    pub telemetry_mounts: Vec<DeclaredTelemetryMount>,
}

pub struct ArtifactMountPlan {
    pub host_artifact_path: String,
    pub container_artifact_dir: String,
}

pub enum TaskMaterializationPlan {
    TaskImage,
    BaseImageBundle {
        task_bundle_ref: String,
    },
}

pub struct TaskSandboxState {
    pub container_id: String,
    pub image: String,
    pub workdir: String,
    pub materialization: TaskMaterializationPlan,
}
```

Important consequence:

1. `TaskImage` means runtime does not populate the workdir. The declared image is already executable from that workdir.
2. `BaseImageBundle` means build produced a sealed task bundle, and runtime copies that bundle into the declared workdir of the declared base image.
3. Dataset packs, git checkouts, or any other upstream benchmark source formats are build-time compiler inputs, not runtime materialization branches.

That difference should be obvious in the data shape.

### Grading sandbox planning data

Grading must have its own plan record because it is governed by a different upstream enum and different low-level steps.

```rust
pub struct GradingSandboxPlan {
    pub strategy: GradingStrategy,
    pub command: Vec<String>,
    pub io_mounts: IoMountPlan,
    pub output_mode: GraderOutputMode,
    pub details: GradingSandboxDetails,
}

pub enum GradingSandboxDetails {
    InTaskImage {
        hidden_paths: Vec<String>,
        revealed_paths: Vec<String>,
    },
    Injected {
        bundle_host_path: String,
        copy_dest: String,
    },
    Separate {
        image: String,
        workdir: String,
    },
}

pub struct GradingSandboxState {
    pub container_id: String,
    pub strategy: GradingStrategy,
    pub workdir: String,
}

pub enum GraderOutputMode {
    DirectMapped,
    RawThenMap {
        mapper_ref: String,
    },
}

pub struct GraderMappingPlan {
    pub mapper_ref: String,
}
```

This is the important separation you were pointing at:

1. task sandbox materialization is about workspace and agent execution
2. grading sandbox materialization is about hidden assets and verdict production

They are related, but they are not the same procedure and should not share one overloaded config object.

### Low-level procedure surface

The low-level implementation should look like procedure steps over these records:

```rust
fn derive_task_sandbox_plan(
    slot: &TrialSlot,
    task: &TaskRow,
    variant: &VariantSpec,
    fs: &AttemptFsLayout,
) -> TaskSandboxPlan;
fn materialize_task_sandbox(plan: &TaskSandboxPlan) -> TaskSandboxState;
fn capture_pre_agent_workspace(
    task_sandbox: &TaskSandboxState,
    attempt: &TrialAttemptState,
) -> WorkspaceObservationRecord;
fn run_agent_phase(state: &TaskSandboxState, attempt: &TrialAttemptState) -> AgentPhaseRecord;
fn capture_post_agent_workspace(
    task_sandbox: &TaskSandboxState,
    attempt: &TrialAttemptState,
) -> WorkspaceObservationRecord;
fn derive_workspace_delta(
    attempt: &TrialAttemptState,
) -> WorkspaceDeltaRecord;
fn extract_candidate_artifact(
    attempt: &TrialAttemptState,
    result_path: &str,
) -> CandidateArtifactRecord;

fn derive_grading_sandbox_plan(
    slot: &TrialSlot,
    grading: &GradingConfig,
    task_sandbox: &TaskSandboxState,
    agent_phase: &AgentPhaseRecord,
) -> GradingSandboxPlan;
fn materialize_grading_sandbox(plan: &GradingSandboxPlan) -> GradingSandboxState;
fn run_grading_phase(state: &GradingSandboxState, attempt: &TrialAttemptState) -> GradingPhaseRecord;
fn run_grader_mapping_phase(
    plan: &GraderMappingPlan,
    attempt: &TrialAttemptState,
) -> GraderMappingPhaseRecord;

fn collect_declared_telemetry(
    fs: &AttemptFsLayout,
    phase: TelemetryPhase,
    attempt: &TrialAttemptState,
) -> CollectedTelemetry;

fn build_commit_record(attempt: &TrialAttemptState) -> SlotCommitRecord;
```

The point is not the exact function names. The point is:

1. planning and materialization are separate
2. task and grading each get their own plan/state records
3. workspace observation and candidate-artifact extraction are explicit steps
4. every step consumes explicit data and returns explicit data
5. declared telemetry ingestion is an explicit step, not an accidental side effect

---

## Program Runtime Procedure

This is the concrete flow the program should execute.

### 1. `lab-cli run` entry

1. Parse CLI args.
2. If the input is authoring YAML, build it into sealed `experiment.json`.
3. Read run-time env injection flags and build a run overlay.
4. Create the run directory and initialize durable run metadata.
5. Load or initialize schedule progress.

### 2. Schedule creation

1. Read the sealed experiment spec.
2. Load compiled task rows.
3. Expand variants x tasks x replications into `TrialSlot` records.
4. Persist the schedule and `next_schedule_index`.

At this point the durable inputs are fixed:

1. sealed experiment spec
2. run overlay
3. schedule progress

### 3. Worker takes one slot

For one `TrialSlot`:

1. allocate `TrialAttemptKey { schedule_idx, attempt }`
2. allocate `AttemptFsLayout`
3. write initial `TrialAttemptState { phase = Pending }`
4. advance it to `AgentMaterializing`

### 4. Derive task sandbox plan

Inputs:

1. `TrialSlot`
2. `TaskRow`
3. `VariantSpec`
4. `AttemptFsLayout`
5. run overlay

Outputs:

1. `TaskSandboxPlan`

This step decides:

1. task image
2. execution workdir
3. whether runtime uses `TaskImage` or `BaseImageBundle`
4. IO mount realization for `/agentlab/in`, `/agentlab/out`, plus any declared `phase = agent` telemetry mounts
5. agent artifact mount realization
6. network settings and the resolved `time_limit_ms`

This step does not create containers yet.

### 5. Materialize task sandbox

Input:

1. `TaskSandboxPlan`

Output:

1. `TaskSandboxState`

Procedure:

1. write `trial_input.json` into `AttemptFsLayout.in_dir`
2. create the task container from the declared task image or base image
3. attach `/agentlab/in`, `/agentlab/out`, and any declared agent telemetry mounts
4. attach the agent artifact mount
5. if materialization mode is `BaseImageBundle`, copy the sealed task bundle into the declared workdir
6. if materialization mode is `TaskImage`, do not mutate the workdir contents
7. persist `TaskSandboxState`
8. capture and persist the pre-agent workspace observation for this attempt

### 6. Run agent phase

Input:

1. `TaskSandboxState`
2. `TrialAttemptState`

Output:

1. `AgentPhaseRecord`

Procedure:

1. exec the fixed agent CLI command
2. stream stdout/stderr to host logs
3. for each declared telemetry entry with `phase = agent` and `collect_mode = Tail`, tail the declared file while the agent runs
4. after process exit, read any declared `phase = agent` telemetry entries with `collect_mode = AfterPhase`
5. validate and ingest only the telemetry entries declared in the sealed spec
6. append the resulting runner-enriched fact rows into the run sink
7. incrementally refresh the analysis-owned run-local DuckDB mirror from appended fact rows
8. enforce timeout and resource policy
9. after process exit, inspect `/agentlab/out/result.json`
10. capture and persist the post-agent workspace observation
11. derive and persist the workspace delta from pre-agent to post-agent state
12. extract and persist the canonical candidate artifact according to the declared `artifact_type`
13. classify `result_state` as `Missing`, `PresentInvalid`, or `Valid`
14. persist `AgentPhaseRecord`
15. update `TrialAttemptState.phase = AgentFinished`

### 7. Derive grading sandbox plan

Inputs:

1. `TrialSlot`
2. `GradingConfig`
3. `TaskSandboxState`
4. `AgentPhaseRecord`
5. `AttemptFsLayout`

Output:

1. `GradingSandboxPlan`

This step decides:

1. which `GradingStrategy` branch applies
2. whether grading reuses the task container or creates a new container
3. what hidden paths must be masked or restored
4. what grader bundle or image must be used
5. whether grading writes mapped output directly or raw output for a later mapper phase
6. which IO mounts the grader sees, including only declared `phase = grader` telemetry mounts if any exist

### 8. Materialize grader sandbox

Input:

1. `GradingSandboxPlan`

Output:

1. `GradingSandboxState`

Procedure:

1. if `InTaskImage`, prepare the existing task container for grader visibility rules
2. if `Injected`, copy the grader bundle after the agent phase completes
3. if `Separate`, create the grader container with only declared inputs visible
4. write `grader_input.json` into `AttemptFsLayout.in_dir`, including candidate artifact state and workspace delta refs
5. attach declared grader telemetry mounts if the plan includes them
6. persist `GradingSandboxState`

### 9. Run grading phase

Input:

1. `GradingSandboxState`
2. `TrialAttemptState`

Output:

1. `GradingPhaseRecord`

Procedure:

1. exec the grader command
2. stream grader stdout/stderr to host logs
3. for each declared telemetry entry with `phase = grader` and `collect_mode = Tail`, tail the declared file while grading runs
4. after process exit, read any declared `phase = grader` telemetry entries with `collect_mode = AfterPhase`
5. validate and ingest only the telemetry entries declared in the sealed spec
6. append the resulting runner-enriched fact rows into the run sink
7. incrementally refresh the analysis-owned run-local DuckDB mirror from appended fact rows
8. inspect `/agentlab/out/raw_grader_output.json` only when output mode is `RawThenMap`
9. inspect `/agentlab/out/mapped_grader_output.json` only when output mode is `DirectMapped`
10. classify `raw_output_state` as `Missing`, `PresentInvalid`, or `Valid`
11. persist `GradingPhaseRecord`

### 10. Run grader mapping phase

Input:

1. `GraderMappingPlan`
2. `TrialAttemptState`

Output:

1. `GraderMappingPhaseRecord`

Procedure:

1. run this phase only when grading output mode is `RawThenMap`
2. execute the declared mapper against `/agentlab/out/raw_grader_output.json`
3. require the mapper to write `/agentlab/out/mapped_grader_output.json`
4. classify `mapped_output_state` as `Missing`, `PresentInvalid`, or `Valid`
5. persist `GraderMappingPhaseRecord`

### 11. Commit slot

Input:

1. fully populated `TrialAttemptState`

Output:

1. committed slot publication

Procedure:

1. build the slot commit record from attempt state
2. append intent/commit rows to the slot commit journal
3. persist the final `TrialRecord`, including runner-owned contract state plus raw grading payload and any explicit reporting projection fields
4. ensure the analysis-owned run-local DuckDB mirror has consumed all committed fact rows for this slot
5. advance `next_schedule_index`
6. mark the slot completed exactly once

### 12. Crash behavior

If the program crashes:

1. committed slots remain committed
2. in-flight `TrialAttemptState` records are reconciled as abandoned attempts
3. the slot may be retried with a new `TrialAttemptKey`

That is the full procedure. The durable unit is the slot. The operational unit is the attempt.

---

## Per-Attempt Execution Flow

Each attempt should execute the following phases.

### 1. Preflight and slot claim

1. Resolve required env names and fail early if missing.
2. Ensure required images are available or pullable.
3. Acquire a slot attempt under the durable scheduler.

### 2. Materialize agent sandbox

This phase should branch directly on `TaskMaterializationKind`:

1. `TaskImage`
2. `BaseImageBundle`

#### `TaskImage`

1. Create an ephemeral task container from the task image.
2. Mount `/agentlab/in`, `/agentlab/out`, and only the declared `phase = agent` telemetry roots, if any were declared.
3. Mount the agent artifact read-only.
4. Do not populate the workdir at runtime.
5. Set the container workdir to the task's declared image workdir.
6. Ensure grader-only assets are absent or masked during this phase.

#### `BaseImageBundle`

1. Create the task container from the declared base image.
2. Mount `/agentlab/in`, `/agentlab/out`, and only the declared `phase = agent` telemetry roots, if any were declared.
3. Mount the agent artifact read-only.
4. Copy the sealed task bundle into the declared workdir.
5. Set the container workdir to that declared workdir.
6. Do not install dependencies or run setup steps in this phase.

There should be no separate dataset mount root and no generic deps mount root here. Upstream dataset packs, git checkouts, or overlays must already have been compiled into the task bundle at build time.

The result should be an executable sandbox, not a pile of copied files.

Before the agent starts, the runner captures a pre-agent workspace observation:

1. for `TaskImage`, from the execution workdir inside the container
2. for `BaseImageBundle`, from the execution workdir inside the container after the task bundle copy completes

### 3. Run the agent contract

1. Execute the fixed agent CLI contract.
2. Stream stdout, stderr, and runner-owned structural metrics.
3. Collect only telemetry that was explicitly declared for `phase = agent`.
4. Enforce timeouts, network policy, rootfs policy, and resource limits.
5. Persist a runner-owned agent phase record regardless of exit status.

### 4. Collect agent outputs

1. Validate whether `result.json` exists.
2. Validate whether `result.json` matches the artifact envelope schema.
3. Extract the canonical candidate artifact according to the declared `artifact_type`.
4. Capture the post-agent workspace observation.
5. Derive and persist the workspace delta from pre-agent to post-agent state.
6. Persist canonical copies or references into the trial directory.
7. Build `grader_input.json` from the task row, attempt identity, agent phase record, candidate artifact state, and workspace delta.

### 5. Materialize grader sandbox

This phase should branch directly on `GradingStrategy`.

There are three allowed strategies:

1. `in_task_image`
2. `injected`
3. `separate`

The agent sandbox and grader sandbox may be the same container across phases for `in_task_image`, but they are still distinct phases with a hard state transition.

#### `InTaskImage`

1. Verify the hidden grader paths exist in the task image.
2. Before agent phase, move or mask those paths out of the agent-visible location.
3. After agent phase, restore those paths.
4. Reuse the same task container for grader execution.

#### `Injected`

1. Keep grader assets completely absent during agent phase.
2. After agent exit, copy the grader bundle into the container exactly once.
3. Execute the grader command against `grader_input.json` and `result.json`.

#### `Separate`

1. Create a separate grader container from the declared grader image.
2. Expose only the declared outputs and runner-owned grading input.
3. Do not expose hidden assets or grader image contents to the agent container.
4. Execute grading in the separate container and collect either raw or mapped grading output according to the configured conclusion mode.

### 6. Run grading

1. Execute the declared grader command.
2. Require a valid normalized mapped grading output before the trial can be marked graded.
3. Record grader stdout, stderr, and runner-owned structural execution metadata.
4. Collect only telemetry that was explicitly declared for `phase = grader`.

### 7. Run grader mapping

1. Skip this phase when grading output mode is `DirectMapped`.
2. Execute the declared mapper against `raw_grader_output.json`.
3. Require the mapper to write `mapped_grader_output.json`.
4. Persist a runner-owned mapping phase record regardless of success.

### 8. Commit slot

A slot is committable only when:

1. the attempt has a persisted agent phase record
2. the attempt has either a valid mapped grading output or a separate runner-owned grading failure record with slot status `grading_failed`
3. all trial facts for the slot are durably written

Only then should the scheduler advance `next_schedule_index`.

---

## Grading Strategies

All strategies share the same contract:

1. grader reads `grader_input.json`
2. grader may read `result.json`
3. grader may read runner-owned auxiliary grading inputs referenced by `grader_input.json`, including workspace delta files when present
4. grader writes `mapped_grader_output.json` directly, or writes `raw_grader_output.json` for a later mapper phase

### Conclusion mapping

The canonical grading boundary is always `mapped_grader_output.json`.

1. `direct` mode means the grader writes `trial_conclusion_v1` directly to `mapped_grader_output.json`.
2. `mapper` mode means the grader writes `raw_grader_output.json`, then the explicit `grader_mapping` phase translates that into `trial_conclusion_v1` in `mapped_grader_output.json`.
3. Commit consumes only the mapped output file. Raw output is supporting evidence, not the final grading boundary.

### `in_task_image`

Use when the task image already contains the hidden tests or grader assets.

Flow:

1. hide grader assets before agent phase
2. run agent
3. reveal grader assets after agent phase
4. run grader

Pros:

1. one task image per task
2. no extra image build for grading

Risks:

1. masking and unmasking must be correct
2. a visibility leak here is a hard invariant violation

### `injected`

Use when the grader bundle should be copied into the task container only after the agent exits.

Flow:

1. run agent
2. copy grader bundle into container
3. run grader

Pros:

1. no hidden assets during agent phase
2. simpler than a second container

Risks:

1. copy-in must be explicit and limited
2. grader bundle integrity must be hashed in the sealed spec

### `separate`

Use when grading must happen in a different image or stronger isolation boundary.

Flow:

1. run agent in task container
2. expose only declared artifacts and runner-owned grading input to grader container
3. run grader in separate container

Pros:

1. strongest isolation
2. easiest to reason about hidden assets

Risks:

1. more container lifecycle complexity
2. extra boundary crossing for result exposure

### Default recommendation

1. prefer `in_task_image` when the benchmark already ships per-task images with hidden tests
2. prefer `separate` when the grader environment meaningfully differs from the task image
3. use `injected` only when copy-in is the simplest safe boundary

---

## Environment and Secret Handling

Environment handling needs a tighter policy than we have today.

### Default rule

Run-supplied env applies to the agent phase only.

That prevents accidental leakage of agent credentials into grading.

### Explicit grader env

If a grader truly requires env from the operator, it must be declared separately and explicitly. Do not reuse the agent env channel silently.

### What gets persisted

Persist:

1. env key name
2. scope: `agent` or `grader`
3. provenance: inline, host-env, env-file

Do not persist:

1. raw env value
2. derived secret contents

### Resume behavior

`continue` must:

1. know which env keys are required
2. fail before scheduling if those keys are not provided again

This avoids the current brittle behavior where continuation depends on shell history or `.env` side effects.

---

## Storage and Host Path Rules

This proposal depends on the host path contract being enforced consistently.

### Path rules

1. The runner constructs host paths through typed path structs, not ad-hoc `.join()` chains.
2. Finalized trial outputs are runner-owned durable artifacts.
3. Committed slot publication is the only correctness boundary for progress.

### No-duplicate-copies rule

The system should not create multiple logical owners for the same data.

Allowed:

1. a mount from host path to contract root
2. a single deliberate copy across an isolation boundary when a strategy requires it

Not allowed:

1. staging the same agent artifact into multiple host-owned directories
2. copying grader assets through intermediate scratch directories without ownership meaning
3. duplicating result artifacts just to satisfy an awkward contract

If two components need the same bytes, prefer:

1. one canonical host location
2. one declared read-only mount or deliberate read path

---

## Failure Semantics and Edge Cases

The current design is too soft on edge cases. These behaviors need to be explicit.

| Case | Required behavior |
|---|---|
| Agent exits `0`, but `result.json` is missing | Agent phase is recorded as contract failure. Grading still runs with `result_present = false`. |
| Agent exits non-zero, but writes a valid `result.json` | Grading still runs. The non-zero exit is execution metadata, not a verdict. |
| Agent times out or is OOM-killed | Runner records the failure and still constructs `grader_input.json`. Grader classifies if possible. |
| `result.json` parses, but candidate artifact extraction fails | Runner records `candidate_artifact.state = invalid`. Grading still runs and may still use workspace delta if relevant. |
| Pre/post workspace observation fails | Runner records a workspace-delta observation failure as runner-owned infra state. Grading may continue if it does not require workspace delta. |
| Grader exits non-zero, but writes a valid `mapped_grader_output.json` | Valid mapped output wins. Exit code is recorded as grader anomaly, not treated as the verdict. |
| Grader exits `0`, but required grading output is missing or invalid | Commit the slot with status `grading_failed`. Do not fabricate a scientific verdict. |
| Grader writes valid `raw_grader_output.json`, but mapping fails or `mapped_grader_output.json` is invalid | Commit the slot with status `grading_failed`. Preserve raw output and mapping failure state as evidence. |
| Crash after grading but before commit | Attempt may be retried. Slot publication remains exactly-once. |
| Hidden tests become visible during agent phase | Hard invariant violation. Fail the run or mark the slot infra-invalid. |
| Required env not present on `continue` | Fail preflight before scheduling any slot. |
| `base_image_bundle` task would require runtime dependency installation to become executable | Invalid configuration. Build must have produced a runnable task bundle already. |

One more rule is important:

1. "no benchmark verdict" is a first-class outcome category

That is how we represent grader failures or infrastructure failures without lying to analysis.

---

## Why Async Docker Is Required

The target execution model needs operations that are awkward or brittle as shell-outs:

1. create container without immediately conflating agent and grader phases
2. exec multiple commands into a running container
3. hide and reveal grader assets as separate operations
4. stream logs and resource metrics while the process runs
5. copy assets in after the agent phase
6. read files out deterministically
7. cancel, inspect, and clean up containers without shell-script wrappers

This is why the cutover should introduce a dedicated Docker runtime crate, for example `lab-docker`, that owns:

1. image ensure and inspect
2. container create, start, exec, wait, remove
3. mount specification construction
4. copy-in and copy-out helpers
5. structured log and stats streaming

The runner should depend on that crate, not on handwritten `docker run` command assembly.

---

## Runner Integration

The scheduler and durable commit machinery can remain conceptually similar, but the worker execution model should change.

### Attempt procedure responsibilities

The async low-level attempt procedure should:

1. building the contract roots for one attempt
2. materializing the agent sandbox
3. running the agent contract
4. assembling `grader_input.json`
5. materializing grading
6. running the explicit grader-mapping phase when required
7. validating `mapped_grader_output.json`
8. returning a commit-ready attempt result

### Scheduler responsibilities

The scheduler should continue to own:

1. slot ordering
2. concurrency limits
3. retries
4. pruning or failure policies
5. exactly-once commit publication

### Worker model

The local worker backend should move from blocking threads toward Tokio tasks. The point is not fashion. The point is that container lifecycle is now explicitly async and should not be pushed through a sync shell wrapper API.

---

## Migration Plan

This should be delivered in phases.

### Phase 1: Contract and schema cut

1. Add the new authoring schema shape.
2. Define sealed `experiment.json`.
3. Define `artifact_envelope_v1`, `grader_input_v1`, and `trial_conclusion_v1`.
4. Define the generic task-row shape emitted by mappers.
5. Define the fixed grading file contract: `raw_grader_output.json` and `mapped_grader_output.json`.

### Phase 2: Build pipeline cut

1. Compile relative paths to absolute paths.
2. Hash artifacts and mapper assets.
3. Execute task mappers at build time.
4. Reject legacy runtime staging keys.

### Phase 3: Async executor behind a flag

1. Introduce `lab-docker`.
2. Implement the async attempt procedure plus its state records.
3. Support all three grading strategies.
4. Persist attempt phase records, including `grader_mapping`, and validate slot commit behavior.

### Phase 4: Make grading mandatory

1. Remove any path that treats agent exit code as the benchmark result.
2. Require a normalized mapped grading output for every trial.
3. Add hard failures for hidden-asset leaks and missing mapped output.

### Phase 5: Delete legacy execution paths

1. Remove sync shell-based Docker invocation.
2. Remove runtime-owned workspace patch staging and file staging.
3. Remove dual execution modes and executor-choice branches.
4. Reject old run state that assumes the removed model.

---

## Acceptance Criteria

This cutover is complete only when all of the following are true:

1. `lab-cli run ... --env KEY=value` injects env without embedding values into the sealed package.
2. Build emits only absolute machine-scoped paths and hashed artifacts.
3. Benchmark-native task rows are compiled into generic task rows before run starts.
4. Agent command shape is fixed and runner-constructed.
5. Agent phase never sees hidden tests or grader-only assets.
6. A committed trial record cannot claim a successful or failed scientific outcome without a valid `mapped_grader_output.json` carrying `trial_conclusion_v1`.
7. Missing or malformed `result.json` still produces a grading attempt and a truthful final status.
8. A valid `mapped_grader_output.json` is accepted even when grader exit code is non-zero.
9. Every attempt persists a runner-owned pre/post workspace delta regardless of `artifact_type` or task materialization mode.
10. Crash between grading and commit does not double-publish a slot.
11. Local execution no longer relies on shell-generated `docker run` scripts for normal trial execution.

---

## Final Position

The valuable change here is not "Tokio" or "Bollard" by itself. The valuable change is a system that is easier to reason about:

1. build compiles and seals
2. run orchestrates durably
3. agent produces a candidate artifact
4. grader produces raw or mapped grading output, and optional mapper stage normalizes it
5. commit publishes the slot exactly once

If the implementation preserves those seams, the async Docker cutover will simplify the system. If it only swaps transport libraries while keeping the current mixed ownership model, it will not.
