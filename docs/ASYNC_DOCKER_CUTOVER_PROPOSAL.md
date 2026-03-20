# Async Docker Cutover Proposal

Status: D0 architecture freeze in progress; runtime cutover pieces exist, but the structural extraction in this proposal is not complete  
Date: 2026-03-18

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

## Audit Of Current Tree

This revision is synchronized against the code as of 2026-03-18.

The runtime cutover work is partially landed on the current tree, but the structural extraction described by `D0` through `D10` is not yet landed as the shipped module graph. The current tree is transitional and must not be mistaken for the goal-state ownership model.

### Implemented now

The following work exists and should be treated as landed on the current tree:

1. Build/package flow already materializes generic task rows and `TaskMaterializationKind`-based task boundaries.
2. `artifact_envelope_v1` compatibility and candidate-artifact extraction logic exist.
3. `grader_input_v1` generation exists, including candidate artifact and workspace-delta references.
4. `trial_conclusion_v1` validation and `mapped_grader_output.json` loading exist.
5. Durable schedule progress, slot retry accounting, ordered slot commit machinery, and exactly-once slot publication already exist.
6. `TaskSandboxPlan`, `GradingSandboxPlan`, and durable `trial_runtime_state.json` records are the active runtime contracts, not placeholder types.
7. A narrow Docker runtime now exists under `backend::docker`, and the active production trial path uses it for image ensure, container create/start/exec, output streaming, wait, and cleanup.
8. The production trial path now routes through `trial::execution::execute_trial_runtime(...)` instead of shell-built Docker execution for normal container-backed execution.
9. The primary local schedule engine now launches local trials directly and consumes `TrialExecutionResult` directly instead of routing normal execution through `LocalThreadWorkerBackend` or `TrialDispatch`.
10. `trial_runtime_state.json` now exists as a schema-backed persisted per-trial runtime record, including durable stage transitions through `commit_pending` and reconciliation to `committed` or `abandoned`.
11. Grader and mapper phases are now modeled as first-class persisted run-state records with durable sandbox identity.
12. `pause_run`, `kill_run`, and runtime-backed `resume_trial` consult persisted `trial_runtime_state.json` first, use Docker runtime operations against persisted container ids, and leave adapter-control handling only as a legacy compatibility fallback when durable runtime state is absent.
13. `RunSink`, buffered sink ownership, JSON-row routing helpers, and durable row contracts now live under `persistence/`; the old `sink.rs` owner is deleted.
14. Some production ownership boundaries are already visible under `trial/`, `backend/docker`, `persistence/`, and `run/`, but the target `package/` and `experiment/` module graph is not yet landed.
15. Non-test production code now contains zero `Command::new("docker")` call sites, and executor-choice state is confined to test-only coverage.
16. `run_session_state_v1` is mandatory for continue/recover flows, so runs from the removed execution model are rejected instead of silently rehydrated.
17. Targeted proof coverage now exists for fresh run, continue/recover, persisted-runtime control operations, hidden-asset enforcement, grading failure, mapper failure, and exactly-once commit behavior on the primary path.
18. Contributor-facing documentation is in transition. This proposal is the D0 source of truth for the remaining ownership split, lifecycle vocabulary, grading boundary, and runtime control behavior.

### Not implemented now

The following structural gaps are still open on the current tree:

1. `config.rs` still mixes package-owned concerns with generic file/path helpers instead of landing as `package/`.
2. `run/` is only a transitional orchestration owner; the proposal's target `experiment/` split is not yet landed.
3. `core.rs`, `io.rs`, `runner.rs`, `lifecycle.rs`, `validations.rs`, and `types.rs` are still on the production compile path.
4. `lib.rs` still assembles production code through broad re-exports and `include!(...)` shards.
5. `trial/` and `persistence/` are only partially extracted relative to the target file layout in this proposal.

Those gaps are in scope for this proposal and are tracked by `D0` through `D10`.

### Must not count as complete

The following do not count as completing this cutover:

1. Adding types without routing the execution path through those types.
2. Writing `TaskSandboxPlan` into a manifest without making it the input to actual sandbox materialization.
3. Adding grader-input or conclusion schemas while leaving the old container execution model in place.
4. Replacing one shell-wrapper helper with another shell-wrapper helper.
5. Keeping a monolithic trial executor and only renaming internal blocks to sound stage-oriented.

## Scope

### In scope

1. New authoring and sealed-spec boundary for experiment execution.
2. Build-time benchmark mapping into generic task rows.
3. Fixed agent CLI contract.
4. Mandatory grading contract and normalized conclusion boundary.
5. Durable run-state orchestration around trial executions and their ephemeral sandboxes.
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
| Authoring | Experiment intent, variants, policy, mapper references | Host paths, mount topology, hidden-test handling steps |
| Build / Compile | Validation, relative path resolution, artifact hashing, mapper execution, sealed `experiment.json` | Secret values, run-time state, active trial orchestration |
| Run Orchestrator | Durable schedule, trial launches, retry policy, commit journal | Benchmark verdict logic, benchmark-native task parsing |
| Agent Sandbox | Executing agent binary against `trial_input.json`, writing `result.json`, and optionally emitting declared telemetry | Hidden tests, grader assets, slot commit decisions |
| Grader Sandbox | Evaluating agent output and writing normalized `trial_conclusion_v1` into `mapped_grader_output.json` | Scheduling, retries, mutating authoring or task inputs |
| Persistence | Host path contract, committed trial facts, attestation | Benchmark-specific execution behavior |

Two important consequences fall out of this split:

1. Benchmark-specific semantics must compile into generic run-time boundaries before `lab-cli run` starts.
2. Mounts are not user-facing configuration. Logical IO roots are first-class. Bind mounts are only the local Docker realization of those roots.

---

## Non-Negotiable Invariants

1. There is exactly one scientific execution shape: durable runner orchestrator plus ephemeral containers.
2. The agent never sees hidden tests, oracle data, or grader-only assets during agent execution.
3. Grading is mandatory for scored experiments. The runner must not infer pass/fail from agent or grader exit code alone.
4. Benchmark-specific translation happens at build time through mappers or built-in compilers, not in the hot path of run execution.
5. Relative paths are authoring-only. The sealed spec contains machine-scoped absolute paths and digests.
6. Secret values are injected at run time only. They are never embedded into the sealed package.
7. Sandboxes are disposable. Durable state exists at the run and committed-slot layers, not in container memory.
8. Slot commit is the exactly-once boundary. A trial may be retried by incrementing `attempt_no`.
9. Agent output, runner execution records, and grader conclusions are separate records with separate owners.
10. Copies across boundaries are allowed only when isolation requires them. Redundant staging and duplicate ownership are design bugs.

---

## Target User Flow

The intended flow is:

1. User authors experiment YAML with variants, policies, grading strategy, and optional mapper references.
2. Build compiles that YAML into sealed `experiment.json`, resolving relative paths to absolute machine paths and hashing artifacts.
3. `lab-cli run` accepts run-time env injection and creates a run-local execution overlay.
4. The durable run engine expands the schedule and drives trials for scheduled slots.
5. Each trial execution materializes an agent sandbox, runs the agent contract, materializes grading, runs grading, validates outputs, and commits the slot.

The important separation is:

1. YAML expresses intent.
2. Sealed JSON expresses machine-resolved configuration.
3. Run overlay expresses ephemeral run-time inputs such as secrets.
4. Trials execute sandboxes.
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
3. Hidden-test file movement steps.
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
2. `--env` values are injected into the agent run only by default.
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

`base_image_bundle` means the task row does not ship a fully baked task image. Instead, build seals a runnable task bundle, and runtime copies that bundle into the declared workdir of the declared base image. Runtime does not install dependencies in this step.

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
4. `mapped_grader_output.json` is the canonical grading boundary. It is written either directly by the grader or by the explicit mapper step.

This is a hard separation. The runner should not fabricate `result.json`, and the agent should not fabricate `mapped_grader_output.json`.

### What is not part of the base contract

1. No mounted `control.json` file in the base design. Durable scheduling and crash retry do not require a per-trial control mount.
2. No generic `/agentlab/deps` mount. Support files belong in the agent artifact, the grader bundle/image, or the workspace materialization step.
3. No fixed `/agentlab/workspace` contract root. The working directory is execution context, not IO ABI.

### Optional telemetry contract

Telemetry mounts exist only when the experiment author declares them.

Suggested declaration shape:

```yaml
agent:
  telemetry:
    - id: hook_events
      scope: agent
      rel_path: hooks/events.jsonl
      schema: hook_events_v1
      collect: tail
```

Lifecycle:

1. User declares telemetry in YAML.
2. Build resolves and validates that declaration into the sealed experiment spec.
3. Trial planning allocates host paths only for declared telemetry entries and splits them by `scope` plus `collect_mode`.
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
   Examples: agent/grader/mapper run start and end timestamps, wall-clock duration, exit code, timeout, signal.
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

Regardless of `artifact_type` or workspace materialization kind, the runner should also persist an observed workspace delta for every trial execution.

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
  "agent_run": {
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
2. an explicit mapper step runs after grading
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
3. Trial is the execution lifecycle for one slot.
4. `attempt_no` is retry metadata within that trial.

### Slot vs trial vs attempt

These words should not all compete as top-level lifecycle nouns.

1. `slot`
   The durable scheduled work unit.
   One variant x task x replication position in the experiment schedule.
2. `trial`
   The execution lifecycle for one slot within one run.
   This is the top-level noun for the `trial/` domain and for the state machine that materializes, runs, grades, and commits work for a slot.
3. `attempt`
   Not a peer domain noun.
   Use `attempt_no` only as retry metadata inside `TrialState` and related records.

The design rule is:

1. internal orchestration should reason in terms of `slot` and `trial`
2. `attempt_no` is metadata on a trial, not a separate top-level state-machine concept
3. if compatibility forces `trial_*` names to remain at the ABI boundary, that is fine because `trial` is the execution-layer noun in this design

So the answer to "per variant, per trial, are we using traits or state?" is:

1. Variants are not stateful actors.
2. Slots are the durable schedule units.
3. Trials are the dynamic execution units.

### Durable source of truth

The durable source of truth should be:

1. sealed experiment spec
2. schedule progress
3. slot commit journal
4. committed trial artifacts and facts

In-flight `TrialState` may be persisted for observability and recovery, but it is not the correctness boundary by itself.

### Recommended trial stages

Trials should move through these stages:

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
2. active trials may be abandoned and retried
3. exactly-once applies to slot publication, not to individual retry attempts inside a trial

---

## Data Model and Trial Inputs

The right mental model here is not "a big `TrialExecutor` object." The right mental model is:

1. durable trial state records
2. small enums that select execution branches
3. low-level functions that consume one state record and produce the next one

Whether that ends up implemented as a module, namespace, or a thin struct wrapper is secondary. The important thing is the data and the transitions.

### Enums that actually drive behavior

These are the enums that should branch the low-level execution paths.

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

pub enum TrialStage {
    Pending,
    AgentMaterializing,
    AgentRunning,
    AgentFinished,
    GraderMaterializing,
    GraderRunning,
    MapperRunning,
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
3. `TrialStage` is the durable progression for one trial.
4. `ContractFileState` prevents us from collapsing "file missing" and "file malformed" into the same vague failure bucket.

### Core state records

Suggested state records:

```rust
pub struct ScheduleSlot {
    pub schedule_idx: u32,
    pub variant_id: String,
    pub task_id: String,
    pub repl_idx: u32,
}

pub struct TrialState {
    pub trial_id: String,
    pub slot: ScheduleSlot,
    pub attempt_no: u32,
    pub stage: TrialStage,
    pub fs: TrialFsLayout,
    pub task_sandbox: Option<TaskSandboxState>,
    pub grading_sandbox: Option<GradingSandboxState>,
    pub agent_run: Option<AgentRunRecord>,
    pub grader_run: Option<GraderRunRecord>,
    pub mapper_run: Option<MapperRunRecord>,
    pub candidate_artifact: Option<CandidateArtifactRecord>,
    pub workspace_delta: Option<WorkspaceDeltaRecord>,
}

pub struct TrialFsLayout {
    pub attempt_dir: String,
    pub in_dir: String,
    pub out_dir: String,
    pub telemetry_mounts: Vec<DeclaredTelemetryMount>,
    pub logs_dir: String,
}

pub struct DeclaredTelemetryMount {
    pub id: String,
    pub scope: TelemetryScope,
    pub host_dir: String,
    pub container_dir: String,
    pub rel_path: String,
    pub schema: Option<String>,
    pub collect_mode: CollectMode,
}

pub enum TelemetryScope {
    Agent,
    Grader,
}

pub enum CollectMode {
    Tail,
    AfterPhase,
}

pub struct AgentRunRecord {
    pub started_at: String,
    pub ended_at: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub timed_out: bool,
    pub result_state: ContractFileState,
    pub stdout_path: String,
    pub stderr_path: String,
}

pub struct GraderRunRecord {
    pub started_at: String,
    pub ended_at: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub timed_out: bool,
    pub raw_output_state: ContractFileState,
    pub stdout_path: String,
    pub stderr_path: String,
}

pub struct MapperRunRecord {
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

1. `ScheduleSlot` is schedule-level and durable.
2. `TrialState` is the evolving record that each low-level step updates.
3. `TrialFsLayout` is the runner's local filesystem layout for one trial. It may contain attempt-specific paths, but it is still owned by the trial execution flow rather than exposed as a public contract.

Its lifecycle is:

1. the runner allocates it immediately after starting a trial for a claimed slot
2. plan derivation reads it to choose concrete host paths for mounts and files
3. execution code writes contract inputs, logs, and optional declared telemetry into it
4. commit code persists references or copies out whatever must survive the trial
5. the remainder is disposable trial-local scratch space

### Spec vs plan vs state

These terms should be used consistently:

1. `spec`
   Durable declarative input. Loaded from the sealed experiment, task row, variant config, or grading config. A spec says what should be true, not how this specific trial execution will realize it.
2. `plan`
   Trial-local operational instructions derived from specs plus run overlay plus host allocation. A plan can include host paths, chosen mount realizations, selected images, and exact execution branches.
3. `state`
   Observed runtime facts after a step has executed. Container ids, exit codes, file validity states, and timestamps are state.

In other words:

1. `TaskRow`, `VariantSpec`, `GradingConfig` are specs.
2. `TaskSandboxPlan` and `GradingSandboxPlan` are plans.
3. `TrialState`, `TaskSandboxState`, and `GraderRunRecord` are state.

### Stage vs step vs `attempt_no`

These terms also need strict boundaries:

1. `stage`
   The durable lifecycle marker on `TrialState`.
   This is the recovery boundary and the only place the runtime should use names such as `AgentRunning` or `CommitPending`.
2. `step`
   A prose or function-level unit of work inside `trial/execution.rs`.
   Steps are how the implementation is organized; they do not replace `TrialStage` as the durable field.
3. `attempt_no`
   Retry ordinal metadata on a trial.
   It explains which retry produced a given `TrialState`, but it is not a peer domain noun and not a separate state machine.

The naming rule is:

1. use `TrialStage` for persisted lifecycle progression
2. use `step` for prose sequencing and helper-function boundaries
3. do not introduce `phase` or `procedure` as competing architectural nouns for the same trial runtime path

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

They are related, but they are not the same runtime concern and should not share one overloaded config object.

### Low-level trial execution surface

The low-level implementation should look like trial execution steps over these records:

```rust
fn derive_task_sandbox_plan(
    slot: &ScheduleSlot,
    task: &TaskRow,
    variant: &VariantSpec,
    fs: &TrialFsLayout,
) -> TaskSandboxPlan;
fn materialize_task_sandbox(plan: &TaskSandboxPlan) -> TaskSandboxState;
fn capture_pre_agent_workspace(
    task_sandbox: &TaskSandboxState,
    trial: &TrialState,
) -> WorkspaceObservationRecord;
fn run_agent(state: &TaskSandboxState, trial: &TrialState) -> AgentRunRecord;
fn capture_post_agent_workspace(
    task_sandbox: &TaskSandboxState,
    trial: &TrialState,
) -> WorkspaceObservationRecord;
fn derive_workspace_delta(
    trial: &TrialState,
) -> WorkspaceDeltaRecord;
fn extract_candidate_artifact(
    trial: &TrialState,
    result_path: &str,
) -> CandidateArtifactRecord;

fn derive_grading_sandbox_plan(
    slot: &ScheduleSlot,
    grading: &GradingConfig,
    task_sandbox: &TaskSandboxState,
    agent_run: &AgentRunRecord,
) -> GradingSandboxPlan;
fn materialize_grading_sandbox(plan: &GradingSandboxPlan) -> GradingSandboxState;
fn run_grader(state: &GradingSandboxState, trial: &TrialState) -> GraderRunRecord;
fn run_mapper(
    plan: &GraderMappingPlan,
    trial: &TrialState,
) -> MapperRunRecord;

fn collect_declared_telemetry(
    fs: &TrialFsLayout,
    scope: TelemetryScope,
    trial: &TrialState,
) -> CollectedTelemetry;

fn build_commit_record(trial: &TrialState) -> SlotCommitRecord;
```

The point is not the exact function names. The point is:

1. planning and materialization are separate
2. task and grading each get their own plan/state records
3. workspace observation and candidate-artifact extraction are explicit steps
4. every step consumes explicit data and returns explicit data
5. declared telemetry ingestion is an explicit step, not an accidental side effect

### Required domain-scoped code layout

The current split across `lifecycle.rs`, `io.rs`, `runner.rs`, and `core.rs` is too scattered. The implementation should be organized around domain ownership, not around grab-bag utility files.

D0 freezes the following repo-specific naming decisions before code motion starts:

1. `package/` is the required target owner for authoring, compile, sealed-package, validation, and package-owned staging concerns. The current top-level `config.rs` is transitional and must be treated as a legacy source to be split, not as an accepted final owner.
2. `experiment/` is the required target owner for run/session/control/schedule/commit/lease concerns. The current `run/` module is only a staging boundary on the way there; it is not the accepted final name or final split.
3. `trial/` remains the per-attempt execution boundary only. Any schedule or commit logic currently reachable from `trial/` is legacy leakage unless it is strictly attempt-local.
4. `backend/docker.rs` is the only accepted transport owner.
5. `persistence/` remains the only accepted durable-facts owner, but the current `json_rows.rs`, `run_sink.rs`, and `sqlite_store.rs` split is still transitional relative to the target `rows.rs`, `journal.rs`, and `store.rs` layout.
6. `core.rs`, `io.rs`, `runner.rs`, `lifecycle.rs`, `validations.rs`, `types.rs`, and crate-root re-export glue are explicitly legacy compile-path residents. D0 treats them as extraction inputs, not target owners.

An appropriate target shape looks like:

```text
src/
  main.rs
  cli.rs
  error.rs

  package/
    mod.rs
    authoring.rs
    compile.rs
    sealed.rs
    validate.rs
    staging.rs

  experiment/
    mod.rs
    runner.rs
    state.rs
    control.rs
    commit.rs
    lease.rs

  trial/
    mod.rs
    spec.rs
    state.rs
    prepare.rs
    execution.rs
    env.rs
    grade.rs
    artifacts.rs
    workspace.rs
    preflight.rs
    events.rs

  backend/
    mod.rs
    docker.rs

  persistence/
    mod.rs
    store.rs
    rows.rs
    journal.rs
```

These filenames are the default target for the refactor. Deviating from them should require a concrete repo-specific reason, not taste.

Required ownership:

1. `package/`
   Owns authoring input, static validation, path resolution, benchmark mapping, sealed-package loading and compilation, and package-owned declarative manifests such as packaged runtime staging.
2. `experiment/`
   Owns run-level orchestration: run session/control state, schedule expansion, dispatch, retries, continue/recover/replay/fork behavior, and exactly-once slot commit semantics.
3. `trial/`
   Owns the per-attempt spec, state machine, env resolution, preparation/materialization, execution, grading, artifact handling, workspace observation, and dynamic contract smoke.
4. `backend/`
   Owns container backend operations only. For this cutover that means Docker lifecycle control.
5. `persistence/`
   Owns durable stores, durable row contracts, and append-only journals for both streaming and end-of-trial writes.

The important rule is:

1. `package/` emits immutable package-scoped facts; it does not know active run state, retries, or container ids.
2. `experiment/` may interpret package inputs into a run-specific plan and mutate run-level state; it must not know Docker transport details or parse raw grading payloads directly.
3. `trial/` may mutate per-attempt state and derive sandbox plans; it must not own schedule progression, slot commit publication, or variant pruning policy.
4. `backend/` knows transport only; it does not know schedule semantics or scientific verdict semantics.
5. `persistence/` stores durable facts; it does not materialize containers or branch trial state.
6. no file like the current `io.rs` should survive as a mixed-responsibility dumping ground

### Core boundary contracts

The runner has three primary architectural seams:

1. `package -> experiment`
   Immutable sealed package input crossing into run-level orchestration.
2. `experiment -> trial`
   Run-level orchestration crossing into one per-attempt execution.
3. `trial -> persistence`
   Attempt-produced facts crossing into durable storage.

For the current tree, D0 also freezes how transitional modules are interpreted:

1. `config.rs` may still contain code that eventually belongs to `package/`, but it must not be allowed to define run-truth or attempt-truth boundaries.
2. `run/` is interpreted as a temporary experiment-scoped landing zone. New orchestration code should move toward the `experiment/` split defined here, not deepen the `run/` alias.
3. `trial/schedule.rs` is not a signal that schedule progression belongs to `trial/`. Slot progression and retry policy remain experiment-owned even if some helper code still resides under `trial/` today.
4. `persistence/json_rows.rs` and `persistence/run_sink.rs` are transitional file names, not an alternative ownership model.

#### Package -> Experiment

`package/` emits immutable declarative inputs. `experiment/` interprets those inputs, together with run overlay and behavior, into live run orchestration.

`package/` owns:

1. `SealedExperiment`
   The sealed package payload and its declarative contents.
2. `RuntimeStagingManifest`
   The packaged declaration of variant-scoped runtime staging obligations for packaged files. This corresponds to the current `staging_manifest.json` / `runtime_path_staging_manifest_v1`, which declares packaged source paths, runtime destination paths, and staging flags.
3. Compiled task rows, variant declarations, benchmark declarations, package checksums, and static validation outputs.

`experiment/` consumes from `package/`:

1. `SealedExperiment`
2. compiled task rows
3. variant declarations and baseline selection
4. benchmark grading declarations
5. package-owned runtime staging declarations
6. static scheduling and policy inputs such as replications, scheduling policy, random seed, concurrency policy, and pruning policy

`experiment/` must not receive from `package/`:

1. run id
2. run dir paths
3. schedule progress
4. retry counters
5. pause/kill control state
6. container ids
7. concrete runtime secret values embedded into the sealed package

The important reason this boundary exists is that `package/` is package-scoped and reproducible, while `experiment/` is run-scoped and mutable. `package/` should never know active run truth.

#### Experiment -> Trial

`experiment/` launches a single attempt by handing `trial/` an immutable attempt-scoped execution description plus the run-scoped overlay needed for that attempt.

`experiment/` owns before the handoff:

1. schedule slot selection
2. attempt numbering
3. retry policy
4. continue/recover/replay/fork decisions
5. run control state
6. exactly-once slot commit ordering

`trial/` owns after the handoff:

1. task environment preparation
2. task sandbox and grading sandbox planning
3. `TrialAttemptState`
4. final env and argv resolution for the attempt
5. agent execution
6. grader execution
7. mapper execution
8. candidate artifact extraction
9. workspace observation and delta derivation
10. dynamic contract smoke for the attempt path

`trial/` returns facts to `experiment/`, not policy decisions. The return surface should include:

1. attempt identity
2. terminal attempt phase
3. normalized grading / grading-failure facts
4. candidate artifact facts
5. workspace delta facts
6. durable refs to logs, outputs, and evidence

`trial/` must not return:

1. slot commit publication decisions
2. schedule mutation decisions
3. variant pruning decisions
4. pre-shaped persistence ownership shortcuts that bypass the durable boundary

The important reason this boundary exists is that `trial/` executes one attempt, while `experiment/` decides what to do next with that result.

### Spec / Plan / State terminology

The same three nouns should be used consistently across the runner:

1. `spec`
   Immutable declarative input.
2. `plan`
   Derived execution-shaped intent.
3. `state`
   Mutable runtime truth.

Applied to this architecture:

1. `package/`
   Owns `SealedExperiment` and `RuntimeStagingManifest`.
   These are package-scoped immutable specs.
2. `experiment/`
   Owns `ExperimentPlan` and `ExperimentState`.
   `ExperimentPlan` is derived from `SealedExperiment` plus run overlay/behavior.
   `ExperimentState` is the mutable run truth: session, control, schedule progress, pending completions, commit journal, and leases.
3. `trial/`
   Owns `TrialSpec`, `PreparedTaskEnvironment`, `TaskSandboxPlan`, `GradingSandboxPlan`, and `TrialAttemptState`.
   `TrialSpec` is attempt-scoped immutable input.
   `PreparedTaskEnvironment` and sandbox plans are derived execution plans.
   `TrialAttemptState` is the mutable attempt truth.

The reason to reuse `spec -> plan -> state` across domains is consistency. The reason not to collapse everything into `spec` is that it would blur the line between immutable package inputs, derived orchestration intent, and mutable runtime truth.

### File-level ownership rules

The target tree only helps if each file has a narrow contract. The implementation should follow these rules:

Repo-specific D0 classification of current files:

1. `config.rs`
   Transitional legacy owner. Its package-scoped pieces are D1 input; its generic helper surface must be reassigned or deleted during extraction instead of preserved as a permanent catch-all.
2. `run/mod.rs`
   Transitional orchestration owner. Its run-session and run-control responsibilities are valid experiment-owned concerns, but the module name and one-file shape are not the target state.
3. `trial/schedule.rs`
   Transitional file name only. Any experiment-level schedule progression here must move out under `experiment/`; only genuinely attempt-local scheduling helpers may survive after renaming and narrowing.
4. `persistence/json_rows.rs`, `persistence/run_sink.rs`, and `persistence/sqlite_store.rs`
   Transitional persistence files. Their contents must converge on `rows.rs`, `journal.rs`, and `store.rs` ownership instead of becoming a second accepted layout.
5. `lib.rs`
   Transitional crate assembly only. D0 does not allow `lib.rs` to remain a long-term ownership-smearing façade.

1. `package/authoring.rs`
   Raw authoring input loading and normalization only.
   No run-state mutation and no Docker/runtime behavior.
2. `package/compile.rs`
   Build/package compilation only: path rewriting, task compilation, package staging, and sealed package emission.
   No active run orchestration.
3. `package/sealed.rs`
   Sealed package loading and immutable package-scoped descriptors only.
   No retry policy, no control state, no trial execution.
4. `package/validate.rs`
   Static validation only.
   Dynamic contract smoke must not live here.
5. `package/staging.rs`
   Package-owned runtime staging declarations only, including `RuntimeStagingManifest`.
   No trial-local materialization and no container lifecycle logic.
6. `experiment/state.rs`
   Durable experiment and scheduler records only: run session, run control, schedule progress, commit records, pending completion records, and leases.
   No Docker transport calls, no trial materialization, no grading payload parsing.
7. `experiment/runner.rs`
   Schedule loop, slot claiming, run start/continue/recover/replay/fork, retry decisions, and handoff to commit only.
   It consumes trial facts; it does not speak Docker transport or parse raw grader files directly.
8. `experiment/control.rs`
   Out-of-band operator control only: pause, kill, resume, and run-control updates around those operations.
   It may coordinate with trial state and backend control, but it does not own schedule progression.
9. `experiment/commit.rs`
   Deterministic exactly-once slot publication only.
   It consumes trial facts and writes commit-facing durable facts; it does not run containers.
10. `experiment/lease.rs`
   Run-operation lease and engine-lease ownership only.
   No schedule branching and no trial execution logic.
11. `trial/spec.rs`
   Immutable attempt-scoped execution inputs only: compiled task rows, grading declarations, contract descriptors, and attempt-scoped declarative inputs loaded from the sealed experiment / experiment plan.
   No container ids, host path allocation, or terminal attempt facts.
12. `trial/state.rs`
   Mutable attempt-scoped runtime records only: `TrialAttemptState`, phases, sandbox states, and per-attempt reconciliation helpers.
   No Docker transport calls and no experiment-level retry/commit logic.
13. `trial/prepare.rs`
   Task environment preparation and prepared-task-environment manifests only.
   This is where attempt-scoped preparation is turned into concrete task/grading sandbox plans.
14. `trial/execution.rs`
   Attempt-step coordinator only.
   Owns one attempt execution path and stage transitions, and calls into `prepare`, `env`, `grade`, `artifacts`, `workspace`, `backend::docker`, and `persistence`.
   It must not own retry policy, schedule progression, SQL, or Docker transport implementation details.
15. `trial/env.rs`
   Runtime env resolution and injection semantics only.
   It decides which declared env keys flow to agent or grader for one attempt, but it does not materialize containers or validate grading payloads.
16. `trial/grade.rs`
   Grader-input construction, grading-strategy selection, raw and mapped output validation, and grader-specific contract helpers only.
   It must not decide retries or slot commit publication.
17. `trial/artifacts.rs`
   Candidate-artifact extraction and contract-file/result-envelope handling only.
   No schedule logic and no durable row ownership.
18. `trial/workspace.rs`
   Workspace observation, workspace snapshot, diff, patch, and workspace-bundle capture only.
   If this logic is small it may remain folded into `trial/artifacts.rs`, but it must not contaminate `trial/execution.rs`.
19. `trial/preflight.rs`
   Dynamic contract smoke only.
   The reason it belongs here is that dynamic preflight runs the real trial execution path; static preflight remains package-owned.
20. `trial/events.rs`
   Trial-scoped emitted events and event payload structs only.
   No durability implementation and no state-machine control logic.
21. `backend/docker.rs`
   Docker transport only: image/container/exec/copy/log/wait/remove operations plus transport-level error translation.
   No trial policy, no slot commit semantics, no grading decisions.
22. `persistence/store.rs`
   Durable state and fact storage only.
   No container lifecycle and no trial-stage branching.
23. `persistence/rows.rs`
   Durable row contracts and row-shaping helpers only.
   No schedule mutation logic and no container materialization.
24. `persistence/journal.rs`
   Append-only commit and event journal writes only.
   No schedule mutation logic and no trial execution helpers.

### Mapping from current tree to target tree

The target layout should be justified by the current tree, not invented from scratch.

1. `lib.rs`
   The current `include!("core.rs")`, `include!("runner.rs")`, `include!("lifecycle.rs")`, `include!("validations.rs")`, and `include!("io.rs")` assembly should be deleted.
   `lib.rs` should become real module declarations plus a narrow curated external API only.
2. `config.rs` plus the static validation and build/package helpers now spread across `validations.rs` and `lifecycle.rs`
   This should become `package/`.
   Examples that belong here: authoring input loading, schema/policy validation, path rewriting, task compilation, package staging, sealed-spec emission, sealed package loading, and package-owned runtime staging declarations.
3. `runner.rs` plus the experiment-level state and control types in `types.rs`
   This should become `experiment/`.
   Examples that belong here: run start, continue, recover, replay, fork, pause, kill, `ScheduleProgress`, `RunSessionState`, `RunControlActiveTrial`, lease coordination, and slot-commit coordination.
4. `lifecycle.rs`
   Only the experiment-level schedule and commit machinery should remain in `experiment/`.
   `TrialExecutor::execute_slot` should not remain there at all. It moves conceptually to `trial::execution`.
   Build/package helpers in `lifecycle.rs` should move to `package/`.
5. `io.rs`
   This file is currently the main source of scatter and should be split aggressively:
   `TaskBoundaryMaterialization`, task-row parsing, and task/grading specs go to `trial/spec.rs`.
   `TrialAttemptState`, task/grading sandbox plans and states, and attempt reconciliation live in `trial/state.rs`.
   Task environment materialization and prepared-task-environment manifests go to `trial/prepare.rs`.
   Trial driving logic for one attempt goes to `trial/execution.rs`.
   Runtime env interpolation and env overlay handling go to `trial/env.rs`.
   Candidate-artifact extraction and result-envelope handling go to `trial/artifacts.rs`.
   Grader-input construction, grading-mode selection, and mapped-output validation go to `trial/grade.rs`.
   Workspace observation, workspace delta derivation, and workspace materialization should move to `trial/workspace.rs` unless they remain genuinely small enough to stay inside `trial/artifacts.rs` without mixing responsibilities.
   Dynamic contract smoke goes to `trial/preflight.rs`.
   Docker command construction and container execution logic must not remain under `trial/`; it moves to `backend/docker.rs`.
6. `types.rs`
   This giant shared type file should be split by owning domain.
   Package-owned immutable descriptors stay under `package/sealed.rs` or `package/staging.rs`.
   Experiment-owned types stay under `experiment/state.rs`.
   Trial-owned types stay under `trial/spec.rs`, `trial/prepare.rs`, or `trial/state.rs`.
   Persistence row types and sink-facing row contracts move under `persistence/rows.rs`.
7. `persistence/run_sink.rs` plus `persistence/sqlite_store.rs`
   These are one domain and should live together under `persistence/`.
   `RunSink`, `TrialRecord`, `MetricRow`, `EventRow`, and `VariantSnapshotRow` should not stay top-level while SQLite backing logic lives elsewhere.

The names in the sketch also need discipline:

1. `experiment/runner.rs` advances schedule state forward. It should not absorb out-of-band control behavior or durable row shaping.
2. `experiment/control.rs` handles operator interventions against active work. It should not become a second scheduler.
3. `experiment/state.rs` owns mutable experiment truth. It should not become a dumping ground for helper functions that merely happen to touch a run id.
4. `trial/execution.rs` should only drive stage transitions and call owned helpers. It should not become another giant utility file.
5. `trial/env.rs` should only own runtime env resolution and injection semantics. It should not absorb paths, contracts, or workspace code.
6. `trial/preflight.rs` owns only dynamic contract smoke because it runs the true trial path. Static validation stays in `package/validate.rs`.
7. `backend/docker.rs` may start as one file, but only if it stays purely about Docker lifecycle. If it starts accumulating grading or trial semantics, the split has failed.
8. `persistence/store.rs` should own durable state and facts.
9. `persistence/journal.rs` should own append-only journal writing.
10. If sink abstractions remain, they belong under `persistence/`, not as a top-level crate file.

### Structural extraction execution protocol

This section is normative. It exists to prevent "the code still compiles" from being treated as success while mixed ownership, stale helpers, and dead code remain in production.

Migration principle:

1. The goal of this extraction is not to preserve compilation at every intermediate step.
2. The goal is to migrate precisely the concepts, contracts, and code paths that belong in the target architecture, and to migrate nothing more.
3. The current legacy ownership layout is a poisoned state. A green compile while that state is preserved is evidence that the old ownership model is still being carried forward, not evidence of success.
4. If removing legacy code or legacy import paths causes breakage, that breakage is useful diagnostic signal. It shows where hidden responsibilities, stale assumptions, or missing target-domain concepts still exist.
5. The extraction should therefore prefer truth over temporary smoothness:
   delete what does not belong,
   move only what is justified,
   split what mixes multiple domains,
   and accept temporary breakage when that breakage exposes architectural gaps.
6. Only after the target-domain migration is materially complete should the implementation evaluate what is broken, what gaps remain, and what new owner APIs are actually required.
7. Restoring legacy code or preserving compatibility shims merely to keep the tree compiling is an explicit anti-goal for this refactor.

D0 completion requirements for this document:

1. The proposal must explicitly name the target domains and reject `config.rs` and `run/` as final ownership boundaries.
2. The proposal must classify the current transitional files so later extraction steps can treat them as inputs, not as competing target layouts.
3. The proposal must freeze the import-path direction of travel: toward `package/`, `experiment/`, `trial/`, `backend/`, and `persistence/`, never back toward crate-root re-exports or legacy top-level files.
4. The proposal must be internally consistent about the fact that the structural extraction is still open work.

The required goal-state modules for the runner crate are:

```text
src/
  package/
    mod.rs
    authoring.rs
    compile.rs
    sealed.rs
    validate.rs
    staging.rs

  experiment/
    mod.rs
    runner.rs
    state.rs
    control.rs
    commit.rs
    lease.rs

  trial/
    mod.rs
    spec.rs
    state.rs
    prepare.rs
    execution.rs
    env.rs
    grade.rs
    artifacts.rs
    workspace.rs
    preflight.rs
    events.rs

  backend/
    mod.rs
    docker.rs

  persistence/
    mod.rs
    store.rs
    rows.rs
    journal.rs
```

`run/` is not the default ownership boundary for this refactor. If the implementation wants to keep `run/` instead of `experiment/`, the proposal must be amended first with a concrete reason and an updated ownership map. Silent drift is not acceptable.

Execution process:

1. Inventory every function, struct, enum, type alias, constant, static, and trait currently defined in legacy production files such as `io.rs`, `core.rs`, `runner.rs`, `lifecycle.rs`, `validations.rs`, and `types.rs`.
2. For each symbol, choose exactly one disposition before moving code:
   `move` to a named owning module, `inline` into its only justified caller, or `delete`.
3. A symbol may move only if its responsibility matches the owning module's contract from this section. File motion without ownership cleanup does not count as progress.
4. If moving or deleting a symbol breaks callers, that breakage must be treated as useful signal that the new boundary is incomplete or that hidden responsibilities were previously mixed together.
5. When breakage exposes an unowned concept, either:
   create the smallest justified domain-scoped module for it, or
   amend the proposal before continuing.
   Dumping the concept into an adjacent module for convenience is not allowed.
6. Each extraction step must delete the old definition in the same change. Legacy files are not allowed to survive as compatibility hubs, forwarding layers, or parking lots for "temporary" shims.
7. At the end of the extraction, every surviving symbol must earn its keep by having a clear owner, live production callers, and a responsibility consistent with the domain-scoped layout.

#### Symbol transfer decision protocol

The extraction must be driven by per-symbol triage, not by file motion.

For every symbol in a legacy production file, the implementation must record and answer all of the following before moving it:

1. What domain does this symbol belong to:
   `package`, `experiment`, `trial`, `backend`, or `persistence`?
2. What single responsibility does it own?
   If the honest answer contains "and", the symbol is already suspicious and must usually be split or deleted.
3. Who are its live production callers?
   Test-only callers are not enough to justify migration.
4. What durable contract, runtime contract, or transport contract does it touch?
5. Is the symbol still part of the intended architecture, or is it a compatibility artifact from the removed model?
6. If removed entirely, what explicit behavior would be lost?
7. If moved, what exact file should own it, and why is that file the narrowest correct owner?

Disposition rules:

1. `move`
   Allowed only when the symbol is still architecturally valid, has live production use, has a clear single owner, and can move without dragging unrelated responsibilities with it.
2. `inline`
   Preferred when the symbol is only a thin wrapper, has one justified caller, or exists only because the legacy grab-bag file needed internal indirection.
3. `delete`
   Default choice when the symbol is legacy glue, compatibility scaffolding, stale abstraction, superseded orchestration plumbing, duplicate parsing, duplicate state derivation, or dead code.

Default bias:

1. The burden of proof is on `move`, not on `delete`.
2. "It might be useful later" is not a reason to migrate a symbol.
3. "The code still compiles if we keep it" is not a reason to migrate a symbol.
4. "Tests currently cover it" is not a reason to migrate a symbol if the symbol belongs to a deleted model.

Transfer blockers:

1. A symbol must not move if it mixes two domains, even if both domains are still needed. Split it first or delete the obsolete half.
2. A symbol must not move if its current API is shaped around the legacy architecture rather than the target one.
3. A symbol must not move if preserving it would force wildcard imports, crate-root re-exports, or compatibility wrappers to keep callers building.
4. A symbol must not move if its only callers are themselves slated for deletion.
5. A symbol must not move if its behavior duplicates a surviving owner in the new architecture.

Required patch behavior during extraction:

1. Update imports at the same time the symbol moves so call sites reference the new owner directly.
2. Delete or inline stale wrappers in the same patch rather than layering new wrappers on top of old wrappers.
3. If a move reveals that a caller depends on hidden side effects, patch that caller to use an explicit contract owned by the target module.
4. If a move reveals duplicate logic in two legacy helpers, reconcile to one owner before continuing.
5. If a symbol cannot be classified cleanly, stop and amend the ownership map before migrating more code from that area.

Forbidden migration patterns:

1. Copying a legacy file into a new module and planning to clean it up later.
2. Moving functions wholesale into a new domain file without deleting stale siblings and stale exports.
3. Keeping a legacy file on the compile path as a forwarding module.
4. Preserving a broad public surface just to reduce call-site churn during the refactor.
5. Re-exporting moved symbols back through `lib.rs` or the old legacy module to preserve the previous import shape.
6. Migrating dead code "just in case" because deleting it might expose a missing concept.

Import and export correction rules:

1. The owning module must define the symbol and be the import path used by production callers.
2. `lib.rs` is not an escape hatch for preserving old access paths.
3. Temporary compatibility re-exports are not allowed for production code.
4. If callers become verbose after the move, that is acceptable; hidden ownership is worse than explicit imports.

Hard rules:

1. `lib.rs` must become real module declarations plus a narrow curated external API only. It must not assemble production code through `include!(...)`, wildcard `pub use ...::*`, or crate-wide re-export glue that hides ownership.
2. Production modules must import through their owning module boundaries. `use crate::*;` and similar crate-wide wildcard import patterns are forbidden in production code because they erase ownership and make dumping-ground files cheap.
3. The legacy mixed-responsibility files `io.rs`, `core.rs`, `runner.rs`, `lifecycle.rs`, `validations.rs`, and `types.rs` must be removed from the production compile path. They may be deleted outright or left temporarily on disk during mechanical work, but they must not remain declared as production modules once their owned slices have moved.
4. No new `helpers.rs`, `utils.rs`, `common.rs`, or equivalent catch-all files may be introduced as part of this extraction.
5. No symbol may be migrated just because it exists today. The transfer bar is: the symbol is still needed, the owner is explicit, and the responsibility is narrow enough to preserve the target layout.
6. Trial execution code must not absorb Docker transport details, persistence row-shaping, or experiment-level retry and commit policy.
7. Experiment-level orchestration code must not parse grader payloads, construct Docker transport requests, or own per-trial workspace materialization.
8. Persistence code must not stage workspaces, materialize containers, or own benchmark verdict logic.
9. Backend code must not know schedule semantics, benchmark verdict semantics, or slot commit policy.

Acceptance criteria for the extraction:

1. The production module graph contains the goal-state domain owners from this section, and no production `mod` declarations remain for `io.rs`, `core.rs`, `runner.rs`, `lifecycle.rs`, `validations.rs`, or `types.rs`.
2. `lib.rs` contains only module declarations plus a narrow curated public API and any minimal crate-level constants or statics that are explicitly justified. It does not act as a re-export prelude for the entire crate.
3. Every symbol that survives from the legacy files has been either moved to an owning module, inlined into its single justified caller, or deleted. No symbol remains in a legacy file because its ownership is "unclear."
4. Package-scoped code is split across `authoring`, `compile`, `sealed`, `validate`, and `staging`, and package-owned declarations remain immutable once they cross into `experiment/`.
5. Experiment-scoped scheduling, retries, continue/recover/replay/fork control, leases, and slot commit semantics live under `experiment/` and are not spread across top-level legacy files.
6. Trial-scoped code is split across `spec`, `state`, `prepare`, `execution`, `env`, `grade`, `artifacts`, `workspace`, `preflight`, and `events` without any one file regressing into a new dumping ground.
7. Durable row contracts, sink logic, SQLite storage, and append-only journal writing live under `persistence/` and are not rebuilt under trial or experiment modules.
8. Docker lifecycle, copy, log, wait, and cleanup transport live under `backend/docker.rs` only.
9. The extraction is validated by both behavior and structure:
   behavior means the targeted build and tests still pass,
   structure means the module graph, imports, and call sites match the ownership rules in this proposal.
   A green compile without structural conformance is a failed extraction.
10. The final audit after extraction must identify only real remaining gaps from the target layout, not artifacts preserved from the old layout for convenience.
11. If deleting a legacy symbol causes a behavior regression, the implementation must explain the missed responsibility and place it under the correct owner before closure. Reintroducing the legacy file or restoring a compatibility shim is not an acceptable fix.

### Extraction DAG

The extraction should be executed as a dependency-ordered DAG, not as an open-ended cleanup effort.

| Node | Depends on | Required Inputs | Purpose |
|---|---|---|---|
| D0 | none | `Required domain-scoped code layout`; `Core boundary contracts`; `Spec / Plan / State terminology`; `File-level ownership rules` | Freeze the ownership model, boundary contracts, and `spec -> plan -> state` vocabulary from this proposal before code motion begins |
| D1 | D0 | `Required domain-scoped code layout`; `Core boundary contracts`; `File-level ownership rules`; `Mapping from current tree to target tree` | Extract `package/` so authoring, compile, sealed-package, validation, and package-owned staging declarations are no longer mixed with runtime orchestration |
| D2 | D0 | `Required domain-scoped code layout`; `File-level ownership rules`; `Mapping from current tree to target tree` | Extract `persistence/` so durable rows, store logic, sink logic, and append-only journals have one owner |
| D3 | D0 | `Required domain-scoped code layout`; `File-level ownership rules`; `Required Docker Runtime Surface` | Isolate `backend/docker.rs` as the only Docker transport surface |
| D4 | D0, D1 | `Core boundary contracts`; `Spec / Plan / State terminology`; `File-level ownership rules`; `Mapping from current tree to target tree` | Extract `trial/spec.rs`, `trial/state.rs`, and attempt-scoped immutable/mutable contracts from legacy files |
| D5 | D3, D4 | `Core boundary contracts`; `Spec / Plan / State terminology`; `File-level ownership rules`; `Program Runtime Flow`; `Per-Trial Execution Flow`; `Grading Strategies`; `Environment and Secret Handling` | Extract the trial execution slices: `prepare`, `env`, `grade`, `artifacts`, `workspace`, `preflight`, and the narrowed `execution` coordinator |
| D6 | D0, D1, D2 | `Core boundary contracts`; `Spec / Plan / State terminology`; `File-level ownership rules`; `Program Runtime Flow`; `Failure Semantics and Edge Cases` | Extract `experiment/state.rs`, `experiment/control.rs`, and `experiment/lease.rs` so run truth and operator control stop living in legacy files |
| D7 | D2, D5, D6 | `Core boundary contracts`; `Spec / Plan / State terminology`; `File-level ownership rules`; `Program Runtime Flow`; `Failure Semantics and Edge Cases` | Extract `experiment/runner.rs` and `experiment/commit.rs` so schedule progression, retry policy, and exactly-once slot publication consume trial facts through explicit boundaries |
| D8 | D1, D2, D3, D5, D7 | `Structural extraction execution protocol`; `Symbol transfer decision protocol`; `Acceptance criteria for the extraction` | Remove legacy production files from the compile path: `io.rs`, `core.rs`, `runner.rs`, `lifecycle.rs`, `validations.rs`, and `types.rs` |
| D9 | D8 | `Structural extraction execution protocol`; `Import and export correction rules`; `Acceptance criteria for the extraction` | Collapse `lib.rs` to module declarations plus a narrow curated public API, and correct production imports to use owning-module paths directly |
| D10 | D9 | `Required domain-scoped code layout`; `Core boundary contracts`; `File-level ownership rules`; `Structural extraction execution protocol`; `Acceptance criteria for the extraction` | Run the final structural audit against the ownership rules and extraction acceptance criteria in this proposal |

In plain terms:

1. `D0` defines the rules of the game before code changes start.
2. `D1` and `D2` establish the immutable package boundary and the durable storage boundary first, so later extraction work has stable edges.
3. `D3` prevents Docker transport details from leaking back into trial or experiment code during the move.
4. `D4` and `D5` carve trial execution into attempt-scoped contracts, plans, and state before experiment orchestration is rewritten around them.
5. `D6` and `D7` then move experiment truth, operator control, schedule progression, retry policy, and commit ordering onto the new boundaries.
6. `D8` is the hard delete step. Legacy files leaving the compile path is a required milestone, not optional cleanup.
7. `D9` removes the last ownership-smearing import/export shortcuts.
8. `D10` is the closure gate. If structural conformance fails here, the extraction is not complete even if the build is green.

Required closure conditions for the DAG:

1. `D5` through `D8` are compiler-blackout nodes.
   During these nodes, `cargo check`, `cargo build`, `cargo test`, IDE compile diagnostics, and any equivalent compile-driven feedback loop are forbidden inputs to migration decisions for `lab-runner`.
   The compiler is not the oracle for these nodes; the ownership map is.
2. Compiler-blackout means:
   no symbol may be kept, moved, wrapped, or re-exported because "the compiler wanted it",
   no module may be broadened because "call sites were easier to fix that way",
   and no placeholder owner may be introduced to preserve temporary buildability.
3. `D5` through `D8` must be executed as exhaustive goal-state admission, not as legacy evacuation.
   The implementation must start from the goal-state files and admit only symbols that can prove:
   they are still needed,
   they have one owner,
   and their current API shape matches the target architecture.
4. Before any code motion inside `D5` through `D8`, the implementation must enumerate the full symbol set for the slice being extracted and assign each symbol exactly one disposition:
   `move`, `inline`, or `delete`.
   If the symbol inventory is incomplete, the node is not allowed to start.
5. Any attempt to satisfy a node by creating forwarding modules, re-export façades, one-line placeholder owners, or new catch-all files is an automatic node failure.
   The patch must be rejected, the node remains open, and every symbol touched by that patch returns to "unclassified" status until re-triaged from zero.
6. No node may be called complete if it leaves legacy forwarding layers, compile-path shims, ownership-smearing re-exports, or fake owner files behind.
7. `D8` must not start until the owning replacement modules for that slice are already populated with real definitions and are already used directly by production callers.
8. `D9` is the first node at which compile/test feedback may be consulted again, and even there it is validation only, never migration guidance.
9. `D10` must validate both structure and behavior, as required by the extraction acceptance criteria above.

#### D5. Trial Execution Slice Extraction

Status: blocked until executed under compiler blackout and per-symbol admission discipline.

Depends on: `D3`, `D4`

Objective:

1. Admit only the attempt-local functionality that truly belongs under `trial/`.
2. Refuse to migrate any attempt-local symbol until its exact owner file and single responsibility are explicit.
3. Build the real `trial/` boundary from the goal state inward:
   `prepare`, `env`, `grade`, `artifacts`, `workspace`, `preflight`, then the narrowed `execution` coordinator.

Mandatory working method:

1. Inventory every attempt-local symbol currently living in `io.rs`, `trial/execution.rs`, `runtime.rs`, `experiment/runner.rs`, `experiment/commit.rs`, `trial/schedule.rs`, or crate-root glue that touches:
   task preparation,
   env resolution,
   grading,
   artifact extraction,
   workspace capture/delta,
   dynamic preflight,
   sandbox planning,
   or contract-file handling.
2. For each such symbol, decide exactly one outcome before moving any code:
   `move` to one named `trial/*` owner,
   `inline` into one justified owner,
   or `delete`.
3. A symbol may move into `trial/execution.rs` only if it is stage-coordinator logic.
   If it performs domain work itself, it does not belong there.
4. A new `trial/*` file counts as an owner only if it contains real owning definitions.
   A file that only `pub use`s, forwards, or aliases behavior from `runtime.rs`, `experiment/*`, `lib.rs`, or another broad host does not satisfy this node.
5. If a helper is used by exactly one trial stage after the split, inline it there.
   Shared helper extraction is allowed only when the helper is still genuinely cross-stage and still narrow.

Forbidden behaviors:

1. Creating `trial/prepare.rs`, `trial/env.rs`, `trial/grade.rs`, `trial/artifacts.rs`, `trial/workspace.rs`, or `trial/preflight.rs` as forwarding shells.
2. Parking attempt-local logic in `runtime.rs`, `engine.rs`, `experiment/runner.rs`, `experiment/commit.rs`, `config.rs`, or `lib.rs` because the exact owner was inconvenient.
3. Allowing `trial/execution.rs` to absorb container transport implementation details, workspace diff logic, grader-input shaping, schema validation, or artifact parsing that should live in narrower owners.
4. Leaving retry policy, schedule progression, slot commit publication, or variant pruning under `trial/`.
5. Using compiler errors as justification for broadening `trial/execution.rs` or keeping compatibility glue.

Closure proof required for `D5`:

1. The production module graph contains real owning definitions in `trial/prepare.rs`, `trial/env.rs`, `trial/grade.rs`, `trial/artifacts.rs`, `trial/workspace.rs`, and `trial/preflight.rs`, or an explicitly justified narrower equivalent with one responsibility per file.
2. `trial::execution::execute_trial_runtime(...)` is a coordinator only.
   It may sequence stages and update attempt state.
   It may not become the place where preparation, env logic, grading logic, artifact parsing, workspace capture, or Docker transport internals actually live.
3. No attempt-local production logic for the extracted slice remains in `io.rs`, `runtime.rs`, `experiment/runner.rs`, `experiment/commit.rs`, or crate-root helper spillover.
4. `trial/schedule.rs` contains no experiment-owned retry policy after this node closes.
5. No forwarding wrappers, placeholder owners, or ownership-smearing re-exports remain for the extracted trial slice.

Failure penalty:

1. Any violation of the rules above voids the node.
2. The violating patch must be rejected.
3. `D6`, `D7`, and `D8` are blocked until `D5` is re-started from a fresh symbol inventory rather than patched incrementally around the violation.

#### D6. Experiment State / Control / Lease Extraction

Status: blocked on explicit experiment-truth admission and compiler blackout discipline.

Depends on: `D0`, `D1`, `D2`

Objective:

1. Admit only run-truth, operator-control, and lease functionality that belongs under `experiment/state.rs`, `experiment/control.rs`, and `experiment/lease.rs`.
2. Remove the possibility that experiment truth can hide in transitional catch-alls, crate-root glue, or neighboring orchestration files.

Mandatory working method:

1. Inventory every surviving symbol that owns or mutates:
   run session state,
   run control state,
   active-trial control state,
   schedule progress,
   pending completion records,
   commit records,
   or engine / operation leases.
2. Assign each symbol to exactly one of:
   `experiment/state.rs`,
   `experiment/control.rs`,
   `experiment/lease.rs`,
   `inline`,
   or `delete`.
3. Do not move a symbol into `experiment/state.rs` merely because it touches a run id.
   The symbol must own durable run truth, not convenience behavior.

Forbidden behaviors:

1. Leaving pause / kill / resume branches in `runner.rs`, `trial/schedule.rs`, or other non-control files.
2. Leaving lease logic in broad orchestration files because it was "already nearby".
3. Rebuilding a generic state helper surface that becomes a new catch-all for experiment concepts.

Closure proof required for `D6`:

1. `experiment/state.rs` owns run truth and scheduler records only.
2. `experiment/control.rs` owns operator intervention only.
3. `experiment/lease.rs` owns engine / operation lease behavior only.
4. No other production file continues to own those responsibilities for convenience.

Failure penalty:

1. Any leakage of experiment truth back into broad hosts voids `D6`.
2. `D7` may not begin until the leaked symbols are re-triaged and reassigned or deleted.

#### D7. Experiment Runner And Commit Extraction

Status: blocked until `D5` and `D6` are structurally closed, not merely tree-shaped.

Depends on: `D2`, `D5`, `D6`

Objective:

1. Establish `experiment/runner.rs` as the only owner of experiment-level schedule progression, retry policy, and run-flow orchestration.
2. Establish `experiment/commit.rs` as the only owner of exactly-once slot publication and pending-completion replay.
3. Force experiment orchestration to consume explicit trial facts rather than reaching back into attempt helpers.

Mandatory working method:

1. Inventory every symbol that owns or decides:
   slot claiming,
   schedule progression,
   retry policy,
   continue / recover / replay / fork behavior,
   pending-completion draining,
   commit ordering,
   or slot publication.
2. For each symbol, prove whether it belongs in `experiment/runner.rs`, `experiment/commit.rs`, `inline`, or `delete`.
3. Anything that still materializes trial environments, parses grader payloads, builds workspace evidence, or constructs Docker transport requests fails classification for `experiment/*` and must be split before any move.

Forbidden behaviors:

1. Parking package logic, trial preparation, trial replay/fork materialization details, preflight generation, workspace logic, or grader parsing in `experiment/runner.rs`.
2. Parking verdict logic, row shaping unrelated to commit publication, or trial-runtime helpers in `experiment/commit.rs`.
3. Leaving retry loops or schedule branching in `trial/schedule.rs`.
4. Preserving old access patterns through crate-root re-exports to avoid rewriting callers.

Closure proof required for `D7`:

1. The production module graph contains `experiment/state.rs`, `experiment/control.rs`, `experiment/lease.rs`, `experiment/runner.rs`, and `experiment/commit.rs` with real owning definitions.
2. `experiment/runner.rs` owns schedule progression, retry coordination, run start/continue/recover/replay/fork orchestration, and handoff into trial execution and commit only.
3. `experiment/commit.rs` owns deterministic commit ordering, pending completion replay, slot-publication journaling, and exactly-once durable publication only.
4. `trial/schedule.rs` is either gone from the production path or reduced to strictly attempt-local helpers.
5. Experiment orchestration consumes trial facts only; it does not parse mapped grader payloads, materialize trial environments, or own workspace / artifact logic.

Failure penalty:

1. Any experiment-owned file that absorbs neighboring domain logic for convenience fails this node immediately.
2. If `trial/schedule.rs` still owns experiment semantics, `D7` is incomplete regardless of compile status.

#### D8. Legacy Compile-Path Deletion

Status: hard delete gate; this node is illegal to start before `D5`, `D6`, and `D7` have real structural closure proofs.

Depends on: `D1`, `D2`, `D3`, `D5`, `D7`

Objective:

1. Remove `io.rs`, `core.rs`, `runner.rs`, `lifecycle.rs`, `validations.rs`, and `types.rs` from the production compile path after their surviving responsibilities have been explicitly admitted elsewhere or deleted.
2. Remove every production import shortcut that keeps the old ownership graph alive.
3. End the blackout-era extraction by deleting the old owners, not by memorializing them under new names.

Mandatory working method:

1. Re-run the symbol inventory for the legacy files named above.
   Every surviving symbol must already have a proven owner outside the legacy file or a justified deletion decision.
2. Delete the production `mod` declarations for the legacy files in the same change that removes the final surviving symbol from each file.
3. Rewrite production imports to point at owning modules directly.
   Do not preserve the old path through `lib.rs`, wildcard exports, or compatibility wrappers.

Forbidden behaviors:

1. Deleting the legacy files from the compile path while recreating their mixed responsibilities in `runtime.rs`, `engine.rs`, `lib.rs`, or another broad host.
2. Leaving a legacy file off the compile path but keeping it as a production import trampoline.
3. Preserving wildcard re-exports or crate-root `pub(crate) use ...::*` surfaces to avoid caller churn.
4. Citing compile success as evidence that the node is done.

Closure proof required for `D8`:

1. No production `mod` declarations remain for `io.rs`, `core.rs`, `runner.rs`, `lifecycle.rs`, `validations.rs`, or `types.rs`.
2. No production caller relies on those files, crate-root shims, or compatibility exports for access.
3. Every surviving symbol from those files is either moved to its owning module, inlined into a justified caller, or deleted.
4. No new catch-all owner appears as the price of deleting the legacy files.

Failure penalty:

1. If any legacy responsibility survives by hiding in a broad replacement host, `D8` fails and the delete is invalid.
2. If any compatibility path survives, `D8` fails even if the legacy `mod` declarations are gone.

#### D9. Import / Export Closure

Status: may begin only after `D8` structure is proven without compiler help.

Depends on: `D8`

Objective:

1. Collapse `lib.rs` to module declarations plus a narrow curated external API.
2. Remove the final ownership-smearing access paths.

Allowed validation inputs:

1. `D9` is the first node where targeted compile and test feedback may be consulted again.
2. Compile and test feedback at `D9` may validate the completed structure.
   It may not be used to re-justify symbols, broaden owners, or reintroduce shortcuts that `D5` through `D8` already banned.

Forbidden behaviors:

1. Re-exporting internal production surfaces through `lib.rs` because callers are noisy.
2. Reintroducing wildcard imports or wildcard exports to smooth over the real ownership graph.
3. Using compile breakage discovered at `D9` as an excuse to resurrect deleted glue rather than fixing the caller against the correct owner.

Closure proof required for `D9`:

1. `lib.rs` is no longer a prelude or routing layer for the crate.
2. Production call sites use owning-module paths directly.
3. The remaining public API is deliberate, narrow, and explicitly justified.

Failure penalty:

1. Any compatibility export that preserves the old shape voids `D9`.
2. If `lib.rs` still hides ownership, `D9` is incomplete no matter how green the build is.

#### D10. Final Structural Audit

Status: closure gate only; no unresolved ownership debt may roll past this point.

Depends on: `D9`

Objective:

1. Prove that the runner now matches the goal-state module graph and responsibility map.
2. Prove that every surviving symbol earned its keep under the target architecture.

Required audit method:

1. Walk every production module in `package/`, `experiment/`, `trial/`, `backend/`, and `persistence/`.
2. For each module, list:
   the business responsibility it owns,
   the symbols it defines,
   the neighboring responsibilities it explicitly does not own,
   and any remaining justified exceptions.
3. Verify that no fake owner, forwarding wrapper, crate-root glue path, or catch-all host survived the migration.
4. Only after structural proof is complete may behavior evidence be considered part of closure.

Failure conditions:

1. Any symbol without a single clear owner.
2. Any module whose honest responsibility description contains "and" in a way that crosses domains.
3. Any surviving placeholder owner file.
4. Any leftover reliance on compile-path convenience instead of explicit boundaries.

Closure rule:

1. `D10` passes only if structure and behavior both pass.
2. If behavior is green and structure is not, the extraction fails.
3. If structure is clean and behavior reveals a missed responsibility, that responsibility must be placed under the correct owner.
   Restoring a legacy surface or convenience shim remains forbidden.

---

## Program Runtime Flow

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
3. Expand variants x tasks x replications into `ScheduleSlot` records.
4. Persist the schedule and `next_schedule_index`.

At this point the durable inputs are fixed:

1. sealed experiment spec
2. run overlay
3. schedule progress

### 3. Start one trial for one slot

For one `ScheduleSlot`:

1. allocate `trial_id`
2. allocate `TrialFsLayout`
3. write initial `TrialState { stage = Pending, attempt_no = 1 }`
4. advance it to `AgentMaterializing`

### 4. Derive task sandbox plan

Inputs:

1. `ScheduleSlot`
2. `TaskRow`
3. `VariantSpec`
4. `TrialFsLayout`
5. run overlay

Outputs:

1. `TaskSandboxPlan`

This step decides:

1. task image
2. execution workdir
3. whether runtime uses `TaskImage` or `BaseImageBundle`
4. IO mount realization for `/agentlab/in`, `/agentlab/out`, plus any declared `scope = agent` telemetry mounts
5. agent artifact mount realization
6. network settings and the resolved `time_limit_ms`

This step does not create containers yet.

### 5. Materialize task sandbox

Input:

1. `TaskSandboxPlan`

Output:

1. `TaskSandboxState`

Steps:

1. write `trial_input.json` into `TrialFsLayout.in_dir`
2. create the task container from the declared task image or base image
3. attach `/agentlab/in`, `/agentlab/out`, and any declared agent telemetry mounts
4. attach the agent artifact mount
5. if materialization mode is `BaseImageBundle`, copy the sealed task bundle into the declared workdir
6. if materialization mode is `TaskImage`, do not mutate the workdir contents
7. persist `TaskSandboxState`
8. capture and persist the pre-agent workspace observation for this trial

### 6. Run agent

Input:

1. `TaskSandboxState`
2. `TrialState`

Output:

1. `AgentRunRecord`

Steps:

1. exec the fixed agent CLI command
2. stream stdout/stderr to host logs
3. for each declared telemetry entry with `scope = agent` and `collect_mode = Tail`, tail the declared file while the agent runs
4. after process exit, read any declared `scope = agent` telemetry entries with `collect_mode = AfterPhase`
5. validate and ingest only the telemetry entries declared in the sealed spec
6. append the resulting runner-enriched fact rows into the run sink
7. incrementally refresh the analysis-owned run-local DuckDB mirror from appended fact rows
8. enforce timeout and resource policy
9. after process exit, inspect `/agentlab/out/result.json`
10. capture and persist the post-agent workspace observation
11. derive and persist the workspace delta from pre-agent to post-agent state
12. extract and persist the canonical candidate artifact according to the declared `artifact_type`
13. classify `result_state` as `Missing`, `PresentInvalid`, or `Valid`
14. persist `AgentRunRecord`
15. update `TrialState.stage = AgentFinished`

### 7. Derive grading sandbox plan

Inputs:

1. `ScheduleSlot`
2. `GradingConfig`
3. `TaskSandboxState`
4. `AgentRunRecord`
5. `TrialFsLayout`

Output:

1. `GradingSandboxPlan`

This step decides:

1. which `GradingStrategy` branch applies
2. whether grading reuses the task container or creates a new container
3. what hidden paths must be masked or restored
4. what grader bundle or image must be used
5. whether grading writes mapped output directly or raw output for a later mapper step
6. which IO mounts the grader sees, including only declared `scope = grader` telemetry mounts if any exist

### 8. Materialize grader sandbox

Input:

1. `GradingSandboxPlan`

Output:

1. `GradingSandboxState`

Steps:

1. if `InTaskImage`, prepare the existing task container for grader visibility rules
2. if `Injected`, copy the grader bundle after the agent step completes
3. if `Separate`, create the grader container with only declared inputs visible
4. write `grader_input.json` into `TrialFsLayout.in_dir`, including candidate artifact state and workspace delta refs
5. attach declared grader telemetry mounts if the plan includes them
6. persist `GradingSandboxState`

### 9. Run grader

Input:

1. `GradingSandboxState`
2. `TrialState`

Output:

1. `GraderRunRecord`

Steps:

1. exec the grader command
2. stream grader stdout/stderr to host logs
3. for each declared telemetry entry with `scope = grader` and `collect_mode = Tail`, tail the declared file while grading runs
4. after process exit, read any declared `scope = grader` telemetry entries with `collect_mode = AfterPhase`
5. validate and ingest only the telemetry entries declared in the sealed spec
6. append the resulting runner-enriched fact rows into the run sink
7. incrementally refresh the analysis-owned run-local DuckDB mirror from appended fact rows
8. inspect `/agentlab/out/raw_grader_output.json` only when output mode is `RawThenMap`
9. inspect `/agentlab/out/mapped_grader_output.json` only when output mode is `DirectMapped`
10. classify `raw_output_state` as `Missing`, `PresentInvalid`, or `Valid`
11. persist `GraderRunRecord`

### 10. Run mapper

Input:

1. `GraderMappingPlan`
2. `TrialState`

Output:

1. `MapperRunRecord`

Steps:

1. run this step only when grading output mode is `RawThenMap`
2. execute the declared mapper against `/agentlab/out/raw_grader_output.json`
3. require the mapper to write `/agentlab/out/mapped_grader_output.json`
4. classify `mapped_output_state` as `Missing`, `PresentInvalid`, or `Valid`
5. persist `MapperRunRecord`

### 11. Commit slot

Input:

1. fully populated `TrialState`

Output:

1. committed slot publication

Steps:

1. build the slot commit record from trial state
2. append intent/commit rows to the slot commit journal
3. persist the final `TrialRecord`, including runner-owned contract state plus raw grading payload and any explicit reporting projection fields
4. ensure the analysis-owned run-local DuckDB mirror has consumed all committed fact rows for this slot
5. advance `next_schedule_index`
6. mark the slot completed exactly once

### 12. Crash behavior

If the program crashes:

1. committed slots remain committed
2. in-flight `TrialState` records are reconciled as abandoned trials
3. the trial may retry by incrementing `attempt_no`

That is the full flow. The durable unit is the slot. The operational unit is the trial.

---

## Per-Trial Execution Flow

Each trial should execute the following stages.

### 1. Preflight and slot claim

1. Resolve required env names and fail early if missing.
2. Ensure required images are available or pullable.
3. Acquire a slot under the durable scheduler and start its trial execution.

### 2. Materialize agent sandbox

This step should branch directly on `TaskMaterializationKind`:

1. `TaskImage`
2. `BaseImageBundle`

#### `TaskImage`

1. Create an ephemeral task container from the task image.
2. Mount `/agentlab/in`, `/agentlab/out`, and only the declared `scope = agent` telemetry roots, if any were declared.
3. Mount the agent artifact read-only.
4. Do not populate the workdir at runtime.
5. Set the container workdir to the task's declared image workdir.
6. Ensure grader-only assets are absent or masked during this step.

#### `BaseImageBundle`

1. Create the task container from the declared base image.
2. Mount `/agentlab/in`, `/agentlab/out`, and only the declared `scope = agent` telemetry roots, if any were declared.
3. Mount the agent artifact read-only.
4. Copy the sealed task bundle into the declared workdir.
5. Set the container workdir to that declared workdir.
6. Do not install dependencies or run setup steps in this step.

There should be no separate dataset mount root and no generic deps mount root here. Upstream dataset packs, git checkouts, or overlays must already have been compiled into the task bundle at build time.

The result should be an executable sandbox, not a pile of copied files.

Before the agent starts, the runner captures a pre-agent workspace observation:

1. for `TaskImage`, from the execution workdir inside the container
2. for `BaseImageBundle`, from the execution workdir inside the container after the task bundle copy completes

### 3. Run the agent contract

1. Execute the fixed agent CLI contract.
2. Stream stdout, stderr, and runner-owned structural metrics.
3. Collect only telemetry that was explicitly declared for `scope = agent`.
4. Enforce timeouts, network policy, rootfs policy, and resource limits.
5. Persist a runner-owned `AgentRunRecord` regardless of exit status.

### 4. Collect agent outputs

1. Validate whether `result.json` exists.
2. Validate whether `result.json` matches the artifact envelope schema.
3. Extract the canonical candidate artifact according to the declared `artifact_type`.
4. Capture the post-agent workspace observation.
5. Derive and persist the workspace delta from pre-agent to post-agent state.
6. Persist canonical copies or references into the trial directory.
7. Build `grader_input.json` from the task row, trial identity, agent run record, candidate artifact state, and workspace delta.

### 5. Materialize grader sandbox

This step should branch directly on `GradingStrategy`.

There are three allowed strategies:

1. `in_task_image`
2. `injected`
3. `separate`

The agent sandbox and grader sandbox may be the same container across steps for `in_task_image`, but they are still distinct trial steps with a hard `TrialStage` transition.

#### `InTaskImage`

1. Verify the hidden grader paths exist in the task image.
2. Before the agent step, move or mask those paths out of the agent-visible location.
3. After the agent step, restore those paths.
4. Reuse the same task container for grader execution.

#### `Injected`

1. Keep grader assets completely absent during the agent step.
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
4. Collect only telemetry that was explicitly declared for `scope = grader`.

### 7. Run grader mapping

1. Skip this step when grading output mode is `DirectMapped`.
2. Execute the declared mapper against `raw_grader_output.json`.
3. Require the mapper to write `mapped_grader_output.json`.
4. Persist a runner-owned `MapperRunRecord` regardless of success.

### 8. Commit slot

A slot is committable only when:

1. the trial has a persisted agent run record
2. the trial has either a valid mapped grading output or a separate runner-owned grading failure record with slot status `grading_failed`
3. all trial facts for the slot are durably written

Only then should the scheduler advance `next_schedule_index`.

---

## Grading Strategies

All strategies share the same contract:

1. grader reads `grader_input.json`
2. grader may read `result.json`
3. grader may read runner-owned auxiliary grading inputs referenced by `grader_input.json`, including workspace delta files when present
4. grader writes `mapped_grader_output.json` directly, or writes `raw_grader_output.json` for a later mapper step

### Conclusion mapping

The canonical grading boundary is always `mapped_grader_output.json`.

1. `direct` mode means the grader writes `trial_conclusion_v1` directly to `mapped_grader_output.json`.
2. `mapper` mode means the grader writes `raw_grader_output.json`, then the explicit mapper step translates that into `trial_conclusion_v1` in `mapped_grader_output.json`.
3. Commit consumes only the mapped output file. Raw output is supporting evidence, not the final grading boundary.

### `in_task_image`

Use when the task image already contains the hidden tests or grader assets.

Flow:

1. hide grader assets before the agent step
2. run agent
3. reveal grader assets after the agent step
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

1. no hidden assets during the agent step
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

Run-supplied env applies to the agent run only.

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
| Agent exits `0`, but `result.json` is missing | Agent execution is recorded as a contract failure. Grading still runs with `result_present = false`. |
| Agent exits non-zero, but writes a valid `result.json` | Grading still runs. The non-zero exit is execution metadata, not a verdict. |
| Agent times out or is OOM-killed | Runner records the failure and still constructs `grader_input.json`. Grader classifies if possible. |
| `result.json` parses, but candidate artifact extraction fails | Runner records `candidate_artifact.state = invalid`. Grading still runs and may still use workspace delta if relevant. |
| Pre/post workspace observation fails | Runner records a workspace-delta observation failure as runner-owned infra state. Grading may continue if it does not require workspace delta. |
| Grader exits non-zero, but writes a valid `mapped_grader_output.json` | Valid mapped output wins. Exit code is recorded as grader anomaly, not treated as the verdict. |
| Grader exits `0`, but required grading output is missing or invalid | Commit the slot with status `grading_failed`. Do not fabricate a scientific verdict. |
| Grader writes valid `raw_grader_output.json`, but mapping fails or `mapped_grader_output.json` is invalid | Commit the slot with status `grading_failed`. Preserve raw output and mapping failure state as evidence. |
| Crash after grading but before commit | The trial may be retried with an incremented `attempt_no`. Slot publication remains exactly-once. |
| Hidden tests become visible during the agent step | Hard invariant violation. Fail the run or mark the slot infra-invalid. |
| Required env not present on `continue` | Fail preflight before scheduling any slot. |
| `base_image_bundle` task would require runtime dependency installation to become executable | Invalid configuration. Build must have produced a runnable task bundle already. |

One more rule is important:

1. "no benchmark verdict" is a first-class outcome category

That is how we represent grader failures or infrastructure failures without lying to analysis.

---

## Why Async Docker Is Required

The target execution model needs operations that are awkward or brittle as shell-outs:

1. create container without immediately conflating agent and grader steps
2. exec multiple commands into a running container
3. hide and reveal grader assets as separate operations
4. stream logs and resource metrics while the process runs
5. copy assets in after the agent step
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

The scheduler and durable commit machinery can remain conceptually similar, but the local execution shape should change.

### Trial execution responsibilities

The async low-level trial execution path should:

1. building the contract roots for one trial
2. materializing the agent sandbox
3. running the agent contract
4. assembling `grader_input.json`
5. materializing grading
6. running the explicit mapper step when required
7. validating `mapped_grader_output.json`
8. returning a commit-ready trial result

### Scheduler responsibilities

The scheduler should continue to own:

1. slot ordering
2. concurrency limits
3. retries
4. pruning or failure policies
5. exactly-once commit publication

### Local execution model

The local runtime does not need a generic worker abstraction. The experiment state machine should start trials directly, track durable progress directly, and react to trial events directly.

The important distinction is:

1. async container lifecycle is required
2. a generic worker layer is not required

For the local path:

1. `experiment::runner` starts a trial execution directly
2. `trial::execution` drives the trial state machine directly
3. `backend::docker` provides async container operations
4. `persistence` receives streaming trial events and final committed records

Concretely, the local experiment runner should loop schedule slots directly and invoke a function in the shape of:

1. `trial::execution::execute_trial(...) -> TrialState`
2. `experiment::runner` then decides retry, abandonment, or commit based on that returned trial state

There should not be an intermediate local worker protocol between those two layers.

If remote execution ever exists later, that should be a separate integration boundary. It should not distort the primary local architecture of this cutover.

## Concrete Delete And Replace List

This cutover should name the production functions that must be removed, not just the target abstractions.

### Delete from the production trial path

The following current functions or function groups should be deleted outright from the normal run path and replaced by explicit trial-step functions over `TrialState`:

1. `TrialExecutor::execute_slot`
2. `run_command_contract_trial`
3. `run_external_agent_runtime_trial`
4. `run_container_sidecar_command`
5. `run_benchmark_grading_phase`
6. `run_benchmark_conclusion_mapper_phase`
7. `build_baked_container_command`
8. `append_container_sandbox_args`
9. `append_container_env_args`
10. `append_container_entrypoint`
11. `ensure_container_image_ready`
12. `run_adapter_process` for container-backed trial execution

There should also be a source-level cleanup rule:

1. zero `Command::new("docker")` calls in non-test production execution code
2. zero handwritten `docker run` assembly in non-test production execution code

### Delete the local worker layer from the primary local path

The primary path no longer routes through the local worker layer. The following worker-compatibility pieces still need deletion from the tree:

1. `LocalThreadWorkerBackend`
2. `TrialDispatch`
3. `TrialCompletion`
4. `WorkerTicket`
5. `execute_parallel_worker_trial`
6. `submit_dispatch_with_backpressure`
7. `process_parallel_worker_control_request`

If any of these remain temporarily during migration, they should be transitional compatibility code only and not part of the final architecture.

### Preserve but move

The following current pieces are useful and may survive, but only if they are moved under the correct ownership boundary:

1. schedule/commit machinery such as `DeterministicCommitter`
2. candidate-artifact extraction logic
3. `grader_input_v1` construction logic
4. workspace observation and delta derivation logic
5. typed trial records such as `TrialState`, `TaskSandboxPlan`, and `GradingSandboxPlan`

### Preserve only as temporary transition shims

The following may exist temporarily during migration, but must not be considered part of the end state:

1. `PreparedTaskEnvironmentManifest.task_sandbox_plan`
2. compatibility helpers that read old trial outputs or old run state
3. any sync wrappers that exist only to bridge old tests during the cutover window

## Required Docker Runtime Surface

The Docker API work needs a smaller and more rigid surface than "some Bollard calls somewhere". The runtime crate should expose a narrow interface and nothing outside it should speak Docker transport details.

Required runtime surface:

1. `ensure_image(image_ref) -> ImageMetadata`
2. `create_container(spec) -> ContainerHandle`
3. `start_container(handle)`
4. `exec(handle, exec_spec) -> ExecHandle`
5. `stream_exec_output(exec_handle) -> Stream<Item = LogChunk>`
6. `wait_exec(exec_handle) -> ExitStatus`
7. `copy_to_container(handle, source, dest)`
8. `copy_from_container(handle, source, dest)`
9. `inspect_container(handle) -> ContainerState`
10. `remove_container(handle, force)`

Required runtime rules:

1. the runner persists container ids only through `TaskSandboxState` and `GradingSandboxState`
2. only the Docker runtime crate may construct bind-mount specs or Docker API option objects
3. only the Docker runtime crate may translate Docker errors into structured runtime errors
4. the trial execution path must never parse `docker` CLI stderr text
5. the Docker runtime crate must own cancellation, timeout, and cleanup semantics for partially materialized containers

---

## Migration Plan

The `R1` through `R7` plan below is historical runtime-cutover context. It is retained because those runtime changes influence the extraction, but it is not the active closure plan for the current work.

If any statement in `R1` through `R7` conflicts with `D0` through `D10`, the `D0` through `D10` extraction plan is authoritative.

This should be delivered in milestones.

### Historical status against the runtime-cutover plan

1. Milestone 1 is done.
   Contracts for `artifact_envelope_v1`, `grader_input_v1`, `trial_conclusion_v1`, and mapped-output validation are the shipped contract surface.
2. Milestone 2 is done.
   Build-time task compilation, artifact hashing, runtime asset rewriting, and sealed package loading are all on the primary path.
3. Milestone 3 is done.
   A Docker runtime exists, the primary local scheduler now launches trials directly through the async runtime path, all three grading strategies are wired through that path, and persisted trial runtime state survives commit and recovery.
4. Milestone 4 is done.
   Benchmark verdicts now require validated `mapped_grader_output.json` across runtime, retries, validation, and preflight contract smoke, and unsafe hidden-asset visibility configs fail fast instead of degrading into legacy fallback behavior.
5. Milestone 5 is done.
   Production execution no longer depends on Docker CLI shell-outs, `TrialExecutor::execute_slot` and the proposal's concrete delete-list helpers are removed, executor-choice is test-only, and continue/recover reject persisted run state that predates durable session-state recording.
6. Milestone 6 is done.
   Targeted proof coverage is in place and contributor docs were updated for the runtime cutover, even though the stricter structural closure now moves through `D0` through `D10`.

### What is left right now

The runtime-cutover milestones below are useful historical context, but the structural extraction remains open and is now tracked by `D0` through `D10`.

The historical runtime-cutover closure plan should be treated as a DAG:

| Node | Depends on | Purpose |
|---|---|---|
| R1 | none | Done: container-owned materialization for task and grader sandboxes |
| R2 | none | Done: durable trial state is now the orchestration source of truth |
| R3 | R2 | Done: runtime control now flows through persisted container identity and Docker runtime operations |
| R4 | R2 | Done: sink contracts and durable row ownership now live under `persistence/` |
| R5 | R2, R3, R4 | Done: domain ownership split is landed and residual runtime-control dead code is deleted |
| R6 | R2, R3, R4, R5 | Done: targeted end-to-end and integration coverage plus contributor docs close the cutover |
| R7 | R5, R6 | Done: run-session and run-control ownership now live under `run/` instead of mixed-domain helpers |

In plain terms:

1. `R1` is done. Task/grader materialization now crosses the container boundary through a private materialization mount plus explicit in-container copy, hidden assets stay container-local until grader execution, and the primary runtime path no longer depends on Docker archive upload behavior.
2. `R2` is done. Duplicated lifecycle truth is removed from production orchestration decisions in favor of durable runtime state and committed artifacts.
3. `R3` is done. Runtime control now uses persisted container identity plus Docker runtime operations on the primary production path, and adapter-control fallback remains only as an explicit legacy compatibility path when durable runtime state is absent.
4. `R4` is done. Sink contracts, buffered sink ownership, and JSON-row routing helpers now live under `persistence/`, and the old `sink.rs` owner is deleted.
5. `R5` is done as a runtime-cutover milestone. The remaining production dead code and compatibility branches from the old control surface were reduced substantially, but the stricter ownership closure is now tracked by `D0` through `D10`.
6. `R6` is done. The proof step is satisfied by targeted primary-path coverage and synchronized contributor/operator docs.
7. `R7` is done. Run-session persistence and run-control ownership now live under `run/`, so `core.rs` and `types.rs` no longer own that orchestration boundary.

### Historical Runtime-Cutover DAG

#### R1. Container-Owned Materialization Boundary

Status: done on the current tree.

Depends on: none

Scope:

1. Replace host-workspace-oriented task and grader setup with an explicit copy-in/copy-out boundary for `BaseImageBundle`.
2. Make hidden-asset isolation and grader visibility rules properties of the materialization plan, not ad hoc host staging behavior.
3. Ensure agent and grader materialization follow the same ownership model across supported grading strategies.

Acceptance criteria:

1. `TaskMaterializationKind::BaseImageBundle` is realized by mounting a private runner-owned source path and explicitly copying into the task workdir inside the container; the workdir itself is not a host bind mount.
2. Agent-visible task contents and grader-visible task contents now follow the same container-owned copy boundary for task and separate-grader execution.
3. Hidden assets remain inside the task container and are moved out of the agent-visible tree until grader execution.
4. In-task-image grading reveals hidden assets only for grader execution and does not expose them to the agent step.
5. The old Docker archive upload path for production materialization is deleted rather than left behind as an unused compatibility branch.

Invariants:

1. The agent sandbox never receives hidden tests, oracle data, or grader-only assets.
2. Materialization plans describe logical IO boundaries first; Docker transport details remain owned by `backend/docker`.
3. No production code infers asset visibility from incidental host path layout.
4. Copy operations across boundaries are minimal and justified by isolation, not convenience.

#### R2. Durable Trial State As The Sole Orchestration Truth

Status: done on the current tree.

Depends on: none

Scope:

1. Move commit, retry, recovery, replay, and fork decision logic onto durable `TrialState` plus committed artifacts.
2. Remove orchestration-local reconstruction of lifecycle facts where persisted trial state already exists.
3. Make trial progression derive from persisted runtime records instead of side-channel helper state.

Acceptance criteria:

1. Commit eligibility is derived from durable trial state and committed artifacts, not duplicated helper-local booleans or ad hoc status reconstruction.
2. Retry and recovery decisions read persisted trial runtime facts and attempt metadata directly.
3. Replay and fork decisions use the same durable trial-state vocabulary as run, continue, and recover.
4. Trial lifecycle transitions remain durable and monotonic across crash/restart boundaries.
5. Production control flow no longer contains bespoke compatibility branches that reinterpret the same trial from alternate state sources.

Invariants:

1. `TrialState` is the source of truth for trial progression; helper-local mirrors are caches only.
2. Slot publication remains exactly-once and is never inferred solely from transient in-memory state.
3. `attempt_no` is retry metadata only; it does not become a parallel lifecycle model.
4. Recovery must be deterministic from persisted records alone.

#### R3. Runtime Control Closure

Status: done on the current tree.

Depends on: R2

Scope:

1. Move stop and cancel behavior onto persisted container ids plus Docker runtime operations.
2. Remove remaining adapter-control and compatibility shims from the primary runtime-backed path.
3. Ensure pause, resume, kill, stop, and cancel all speak the same persisted lifecycle vocabulary.

Acceptance criteria:

1. Stop and cancel use persisted runtime state and persisted container identity first, as pause, kill, and runtime-backed resume already do.
2. If live runtime state exists, production control operations do not fall back to worker-style side channels.
3. Control actions persist truthful terminal or interrupted trial state even when Docker operations partially fail.
4. Continue/recover semantics after stop or cancel are derived from the same durable trial-state records used by the other control operations.
5. Remaining compatibility shims, if any, are confined to explicit legacy-rejection or test-only coverage.

Invariants:

1. There is one runtime control plane for active trials.
2. Persisted container identity is runner-owned state, not an optional hint.
3. Control operations never require parsing Docker CLI text or reconstructing state from logs.
4. A failed control action must not fabricate a successful terminal trial state.

#### R4. Persistence Boundary Closure

Status: done on the current tree.

Depends on: R2

Scope:

1. Move `RunSink`, sink row contracts, buffered sink ownership, and related durable record types under `persistence/`.
2. Align row-contract and JSON-row routing ownership with the actual durable store boundary.
3. Remove cross-domain leakage where orchestration types also serve as persistence contracts by accident.

Acceptance criteria:

1. `RunSink`, `BufferedRunSink`, `RunManifestRecord`, `TrialRecord`, `MetricRow`, `EventRow`, and `VariantSnapshotRow` live under `persistence/` with their store implementations.
2. JSON-row table routing and SQLite identity checks also live under `persistence/`, not `io.rs`.
3. `trial/`, `experiment/`, and other production code consume persistence contracts through stable interfaces instead of owning those contracts themselves.
4. The old `sink.rs` owner is deleted; no compatibility alias remains in the old location.

Invariants:

1. Durable row contracts belong to the persistence boundary.
2. Storage schemas, durable row shapes, and JSON-row ingest routing are not owned by scheduler or trial execution modules.
3. Moving the persistence layer must not change persisted semantics or row schemas unless explicitly versioned.

#### R5. Domain Ownership Split And Dead-Code Deletion

Status: done on the current tree.

Depends on: R1, R2, R3, R4

Scope:

1. Finish moving mixed responsibilities out of `core.rs`, `io.rs`, and `types.rs`.
2. Leave production code organized by domain ownership: `package/`, `experiment/`, `trial/`, `persistence/`, and `backend/docker`.
3. Delete residual legacy helpers, compatibility branches, and unused glue once the new owners are in place.

Acceptance criteria:

1. Production module ownership matches the architectural split described in this document.
2. `core.rs`, `io.rs`, and `types.rs` no longer act as mixed-domain dumping grounds for unrelated responsibilities.
3. Legacy helpers made obsolete by `R1` through `R4` are deleted, not left behind unused.
4. No production call sites remain for superseded control paths, superseded materialization helpers, or superseded state-derivation helpers.
5. Contributor-facing code navigation makes the ownership of build, orchestration, trial execution, persistence, and Docker transport obvious from module layout alone.

Invariants:

1. No single production function owns more than one major lifecycle responsibility.
2. No dead production code remains behind compatibility comments or unused helper layers.
3. Docker transport code stays inside `backend/docker`; persistence code stays inside `persistence/`; trial execution code stays inside `trial/`.
4. File movement is not success by itself; ownership and call sites must also be correct.

#### R6. Closure Proof: Coverage And Docs

Status: done on the current tree.

Depends on: R1, R2, R3, R4, R5

Scope:

1. Add targeted tests that prove the remaining closure nodes hold under the primary local path.
2. Update contributor and operator docs so they match the shipped architecture and deleted concepts.
3. Remove stale proposal text once the code no longer matches transitional language.

Acceptance criteria:

1. The repository contains targeted end-to-end or integration coverage for fresh run, continue/recover, stop/cancel, hidden-asset enforcement, mapper failure, grader failure, and exactly-once commit on the primary local path.
2. Documentation explains the shipped ownership model, lifecycle vocabulary, recovery semantics, grading boundary, and control-plane behavior without referring to deleted production concepts as current design.
3. The proposal's "Implemented now", "Not implemented now", and closure DAG remain synchronized with the code at the time the cutover is declared complete.

Invariants:

1. The cutover is not complete until docs and tests describe the same system the code implements.
2. Coverage must exercise the primary production path, not only compatibility or unit-test doubles.
3. Documentation may mention deleted concepts only as historical context or explicit rejection cases.

#### R7. Run Orchestration Boundary Extraction

Status: done on the current tree.

Depends on: R5, R6

Scope:

1. Move run-session persistence and run-control ownership out of `core.rs` and `types.rs`.
2. Make the run-owned boundary visible in the module graph through `run/`.
3. Keep production callers on the same behavior while deleting the old mixed-domain owners.

Acceptance criteria:

1. `run_control_path`, run-session persistence, run-control serialization, and run-control guard ownership live under `run/`.
2. `RunSessionState`, `RunControlActiveTrial`, and `RunControlPauseMetadata` are no longer declared in `types.rs`.
3. Production code imports the run-owned helpers through the `run/` boundary rather than through `core.rs` as a dumping ground.
4. Targeted tests continue to pass for run-control persistence and runtime-backed pause control after the move.

Invariants:

1. Run orchestration state belongs to the run boundary, not to generic utility files.
2. File movement is only valid if the old owners stop defining the moved run-control surface.
3. The extraction must not reintroduce legacy adapter-control or legacy runtime-file mirrors.

### Milestone 1: Contract and schema cut

1. Add the new authoring schema shape.
2. Define sealed `experiment.json`.
3. Define `artifact_envelope_v1`, `grader_input_v1`, and `trial_conclusion_v1`.
4. Define the generic task-row shape emitted by mappers.
5. Define the fixed grading file contract: `raw_grader_output.json` and `mapped_grader_output.json`.

### Milestone 2: Build pipeline cut

1. Compile relative paths to absolute paths.
2. Hash artifacts and mapper assets.
3. Execute task mappers at build time.
4. Reject legacy runtime staging keys.

### Milestone 3: Async executor behind a flag

1. Introduce `lab-docker`.
2. Implement the async trial execution path plus its state records.
3. Support all three grading strategies.
4. Persist trial stage and run records, including mapper execution state, and validate slot commit behavior.

### Milestone 4: Make grading mandatory

1. Remove any path that treats agent exit code as the benchmark result.
2. Require a normalized mapped grading output for every trial.
3. Add hard failures for hidden-asset leaks and missing mapped output.

### Milestone 5: Delete legacy execution paths

1. Remove sync shell-based Docker invocation.
2. Remove runtime-owned workspace patch staging and file staging.
3. Remove dual execution modes and executor-choice branches.
4. Reject old run state that assumes the removed model.
5. Delete the concrete functions listed in `Concrete Delete And Replace List`.

Finishing Milestone 5 is necessary but not sufficient to close the cutover. The bar for calling this proposal complete is architectural closure: one execution model, one lifecycle vocabulary, one durable source of truth, and documentation that matches the shipped design.

---

## Acceptance Criteria

This cutover is complete only when all of the following are true:

1. `lab-cli run ... --env KEY=value` injects env without embedding values into the sealed package.
2. Build emits only absolute machine-scoped paths and hashed artifacts.
3. Benchmark-native task rows are compiled into generic task rows before run starts.
4. Agent command shape is fixed and runner-constructed.
5. Agent execution never sees hidden tests or grader-only assets.
6. A committed trial record cannot claim a successful or failed scientific outcome without a valid `mapped_grader_output.json` carrying `trial_conclusion_v1`.
7. Missing or malformed `result.json` still produces grading input and a truthful final status.
8. A valid `mapped_grader_output.json` is accepted even when grader exit code is non-zero.
9. Every trial execution persists a runner-owned pre/post workspace delta regardless of `artifact_type` or task materialization mode.
10. Crash between grading and commit does not double-publish a slot.
11. Local execution no longer relies on shell-generated `docker run` scripts for normal trial execution.
12. Non-test production execution code contains zero `Command::new("docker")` invocations.
13. No single function performs more than one of these responsibilities: agent materialization, agent execution, grader materialization, grader execution, grader mapping, slot commit.
14. The `TrialStage` plus run records in `TrialState` are not only defined, but persisted and consumed as the source of truth for trial progression.
15. `lib.rs` no longer assembles production code through large `include!(...)` shards.
16. `RunSink` and sink row contracts live under `persistence/`, alongside their durable store implementations.
17. The primary local path does not route trial execution through a local worker protocol.
18. Production module ownership matches the intended domain split: `package/` owns authoring/compile/sealed-package concerns, `experiment/` owns schedule and commit orchestration, `trial/` owns attempt execution stages, `persistence/` owns sinks and durable facts, and `backend/docker` owns container transport only.
19. No legacy top-level execution concepts remain in production control flow: no executor-choice abstraction, no worker-protocol dependency, no shell-wrapper execution model, and no attempt-as-peer-domain model.
20. Run, continue, recover, retry, pause, kill, replay, and fork all pass through the same durable trial-state model rather than bespoke compatibility side channels.
21. Retry decisions, recovery decisions, and slot commit publication are derived from durable `TrialState` plus committed trial artifacts, not from duplicated ad hoc logic spread across orchestration helpers.
22. The primary local path has one end-to-end execution model across all supported grading strategies and task materialization modes; compatibility branches do not bypass the standard trial lifecycle.
23. The design vocabulary is consistent in production code and persisted records: `slot` is the durable schedule unit, `trial` is the execution lifecycle, and `attempt_no` is retry metadata only. Legacy naming may remain only at explicit ABI-compatibility boundaries.
24. The `Concrete Delete And Replace List` is actually exhausted: listed legacy functions and types are removed, or confined to test-only compatibility coverage with no production call sites.
25. Operator and contributor documentation is updated to match the shipped architecture, including module ownership, lifecycle vocabulary, recovery semantics, grading boundaries, and deleted legacy concepts.
26. The repository contains targeted end-to-end coverage for fresh run, continue/recovery, grading failure, mapper failure, hidden-asset invariant violations, and exactly-once commit behavior on the primary local path.

---

## Final Position

The valuable change here is not "Tokio" or "Bollard" by itself. The valuable change is a system that is easier to reason about:

1. build compiles and seals
2. run orchestrates durably
3. agent produces a candidate artifact
4. grader produces raw or mapped grading output, and optional mapper stage normalizes it
5. commit publishes the slot exactly once

If the implementation preserves those seams, the async Docker cutover will simplify the system. If it only swaps transport libraries while keeping the current mixed ownership model, it will not.
