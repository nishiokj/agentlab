# Patch Spec: Runner IO Boundary v1 (Strict) + Optional Stateful Host Control

Status: Draft v1  
Date: 2026-02-13

## Goal

Define and enforce a hard boundary for Runner Core IO and adapter ownership.

- Runner Core remains a deterministic state machine + evidence capture engine.
- Benchmark semantics stay outside Runner Core.
- SDK can optionally provide a stateful host-side control loop without changing core ownership.

This patch is explicitly anti-bloat. No benchmark-specific fields or logic are added to Runner Core.

---

## Hard Boundary

Runner Core return boundary is:

`{R0, R1, R2, R3, R4, R5}`

1. `R0` Run identity: `{run_id, run_dir}`
2. `R1` Run lifecycle state file: `runtime/run_control.json`
3. `R2` Trial lifecycle state files: `trials/<trial_id>/trial_state.json`
4. `R3` Trial artifacts: `trial_input.json`, `result.json`, stdout/stderr, snapshots
5. `R4` Evidence ledgers: `evidence/evidence_records.jsonl`, `evidence/task_chain_states.jsonl`
6. `R5` Adapter artifacts (if adapter configured): `benchmark/{adapter_manifest,predictions,scores,summary}`

Everything above is durable on disk and source-of-truth. stdout is transport only.

---

## Definitions

### Digest-delta metadata (current)

Current `workspace_patch_v1` is not a textual patch. It is structural metadata:

- `format: file_digest_delta`
- `added[]`, `removed[]`, `modified[]`

Source: `rust/crates/lab-runner/src/lib.rs:4597`.

### Adapter ownership

- Runner owns adapter invocation contract and schema validation.
- Adapter implementation is user/framework-owned (outside runner core).
- Adapter maps evidence -> benchmark prediction/score semantics.

---

## Problem

Current runner behavior includes benchmark fallback generation (`runner_passthrough`) when no adapter command is configured. This blurs the boundary by inventing benchmark predictions/scores in core runtime.

Relevant code:

- passthrough branch in `process_benchmark_outputs`: `rust/crates/lab-runner/src/lib.rs:3134`
- `generate_passthrough_benchmark_records(...)`: `rust/crates/lab-runner/src/lib.rs:2985`
- fallback adapter identity defaults (`runner_passthrough`): `rust/crates/lab-runner/src/lib.rs:2704`

This is obsolete for strict boundary-first architecture.

---

## Decision

### D1. Remove runner-owned benchmark passthrough behavior

Runner Core will no longer synthesize benchmark prediction/score artifacts.

- If `benchmark.adapter.command` is configured: invoke adapter, validate artifacts.
- If benchmark adapter is not configured: skip benchmark phase entirely.
- If benchmark section exists but adapter command is missing/empty: fail fast at validation.

### D2. Adapter owns benchmark manifest content

Runner should not fabricate benchmark identity (`adapter_id`, evaluator identity) with `runner_passthrough` defaults.

- Adapter must emit `benchmark/adapter_manifest.json`.
- Runner validates schema only.

### D3. Keep Runner stdout minimal and deterministic

Default CLI behavior remains final JSON envelope only (`--json`).

No per-trial payloads are returned in-memory through stdout by default.

### D4. Add optional generic incremental event stream (non-authoritative mirror)

Add optional `--json-stream` mode for `lab run` and `lab run-dev`:

- emits NDJSON lifecycle events for host observers
- events reference durable files; they do not replace disk artifacts
- final line remains terminal JSON envelope for compatibility

This is a general core primitive for observability/control, not benchmark-specific.

---

## IO Protocol v1

### Authoritative channel (disk)

- all R0..R5 artifacts in run directory
- used for reproducibility, auditing, re-grade, replay/fork

### Transport channel (stdout)

- `--json`: final envelope only
- `--json-stream`: NDJSON event stream + final envelope

#### NDJSON event schema (`runner_event_v1`)

Common fields:

- `schema_version: "runner_event_v1"`
- `ts` (RFC3339)
- `run_id`
- `run_dir`
- `event`

Event types:

1. `run_started`
2. `trial_started` (`trial_id`, `variant_id`, `task_id`, `repl_idx`)
3. `trial_finished` (`trial_id`, `status`, `outcome`, `duration_ms`, `trial_dir`)
4. `run_paused`
5. `run_resumed`
6. `benchmark_started`
7. `benchmark_finished` (`benchmark_dir`)
8. `run_finished` (`status`)

Rule: event payloads may include refs/paths; they must not include benchmark semantics.

---

## Stateful Host Controller (SDK)

### Why this is justified

A stateful host process is justified for adaptive policies and online control (pause/resume/fork/prune). It is not required for static AB template runs.

### Proposed SDK surface

Add `LabClient.runStream(args)`:

- spawns `lab run --json-stream`
- yields typed `RunnerEvent` incrementally
- yields terminal `RunResponse`

Controller pattern:

1. Subscribe to `runStream()` events.
2. Make host decisions.
3. Execute explicit control actions via existing CLI APIs (`pause`, `resume`, `fork`).
4. Persist all decision effects via run artifacts.

This avoids hidden in-memory-only state.

---

## Required Code Changes

### A. Runner core (`rust/crates/lab-runner/src/lib.rs`)

1. **Delete passthrough benchmark synthesis**
- Delete `generate_passthrough_benchmark_records(...)`.
- Remove callsite branch that invokes it.

2. **Make benchmark processing conditional**
- Only call adapter pipeline when adapter command is configured.
- If no adapter configured: no benchmark output generation.

3. **Remove `runner_passthrough` manifest defaults**
- Remove fallback identity/evaluator insertion tied to passthrough behavior.
- Require adapter-provided manifest when adapter pipeline is active.

4. **Emit optional stream events**
- Add internal event emitter hooks at lifecycle points:
  - run start/end
  - trial start/end
  - benchmark start/end
- Event emitter is optional and side-effect free.

### B. CLI (`rust/crates/lab-cli/src/main.rs`)

1. Add `--json-stream` flag to `run` and `run-dev`.
2. Plumb stream mode to runner options.
3. Maintain compatibility:
- final JSON envelope unchanged
- `--json` behavior unchanged

### C. SDK client (`sdk/src/client.ts`, `sdk/src/types.ts`)

1. Add `RunnerEvent` type.
2. Add `runStream(args): AsyncIterable<RunnerEvent | RunResponse>`.
3. Keep existing `run()` unchanged.

---

## Deletions (Obsolete Code)

Delete as obsolete in this patch:

1. Runner benchmark passthrough generation function and related invocation branch.
2. `runner_passthrough` default adapter/evaluator identity fallback paths.
3. Any CLI/SDK assumptions that benchmark outputs always exist when no adapter is configured.

Do not delete:

- Evidence capture and diff/patch evidence generation.
- Existing replay/fork/pause/resume primitives.

---

## Validation Rules

1. If `/benchmark` is present and adapter command is missing -> config error.
2. If adapter command runs but required benchmark artifacts missing -> run error.
3. `apply_score_records_to_trial_summaries` executes only when scores exist.

---

## Backward Compatibility

- Existing `lab run --json` consumers remain compatible.
- Existing run directory contracts remain valid.
- Experiments relying on implicit passthrough benchmark artifacts must migrate to explicit adapter configuration.

---

## Acceptance Criteria

1. Runner core no longer emits synthetic benchmark records.
2. Benchmark semantics are entirely adapter-owned.
3. `lab run --json` still returns final envelope with `run_id/run_dir`.
4. `lab run --json-stream` provides incremental lifecycle events without becoming source-of-truth.
5. SDK can drive a stateful host controller via `runStream` + existing control commands.
6. All critical state transitions remain auditable from run directory artifacts alone.
