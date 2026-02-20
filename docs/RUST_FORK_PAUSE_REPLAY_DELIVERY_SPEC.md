# Rust Fork/Pause/Replay Delivery Spec

Date: 2026-02-11
Status: Draft for implementation
Owner: Runtime

## 1. Scope

Deliver `fork`, `pause`, and `replay` for the Rust runtime with explicit guarantees by integration level (`cli_basic`, `cli_events`, `otel`, `sdk_control`, `sdk_full`).

This spec is delivery-focused. It defines command contracts, runtime state artifacts, implementation milestones, and acceptance tests.

## 2. Current Baseline

As of this draft:
- `lab-cli` has no `replay`, `fork`, or `pause` subcommands.
- Runner initializes control-plane files with `{"action":"continue"}` only.
- Hook schema supports `control_ack` with actions `continue|stop|checkpoint`.
- `trial_output_v1` already supports optional `checkpoints[]`.

Implication: we have control-plane wiring and validation primitives, but no user-facing lifecycle commands yet.

## 3. Goals and Non-Goals

### Goals

- Add stable CLI commands:
  - `lab replay ...`
  - `lab fork ...`
  - `lab pause ...`
- Keep artifact compatibility with existing `.lab/runs/<run_id>/...` layout.
- Fail closed when strict guarantees are requested but evidence is missing.
- Ship incrementally without blocking existing `run`/`run-dev`/`run-experiment` flows.

### Non-Goals

- Perfect deterministic replay for `cli_basic`.
- Preemptive OS-level process suspension as the default pause mechanism.
- New UI work in this phase.

## 4. Definitions

- Replay: execute a trial again from recorded inputs and (when available) recorded boundary evidence/checkpoints.
- Fork: create a child trial derived from a parent trial at a selector (step/event/checkpoint), optionally with binding overrides.
- Pause: cooperative stop-at-boundary flow that preserves a resumable point.

## 5. Guarantee Matrix

| Integration level | Pause | Replay | Fork |
|---|---|---|---|
| `cli_basic` | unsupported (error) | rerun from `trial_input.json` only (`best_effort`) | derive child input from parent input only (`best_effort`) |
| `cli_events` | cooperative boundary pause via control file (`best_effort`) | rerun + step-aware evidence checks (`best_effort`) | fork from nearest checkpoint if declared, else parent input |
| `otel` | same as `cli_events` (if step events present in hooks) | same as `cli_events` | same as `cli_events` |
| `sdk_control` | required, checkpoint+stop contract (`checkpointed`) | replay with checkpoint resume where available | checkpoint-based fork required |
| `sdk_full` | required and strict | strict replay; fail if causal boundaries/evidence missing | strict fork from explicit selector/checkpoint |

Strict mode behavior:
- `--strict` is allowed for `replay` and `fork`.
- If requested strict semantics exceed observed integration evidence, command fails.

## 6. CLI Contract

### 6.1 Replay

```bash
lab replay --run-dir .lab/runs/<run_id> --trial-id <trial_id> [--strict] [--json]
```

Behavior:
- Loads parent trial artifacts (`trial_input.json`, `result.json`, hooks/traces when present).
- Executes a replay trial and writes under:
  - `.lab/runs/<run_id>/replays/<replay_id>/...`
- Emits a result envelope compatible with existing `--json` style.

### 6.2 Fork

```bash
lab fork --run-dir .lab/runs/<run_id> --from-trial <trial_id> --at <selector> --set k=v [--set k=v ...] [--strict] [--json]
```

Selector:
- `checkpoint:<logical_name>`
- `step:<n>`
- `event_seq:<n>`

Behavior:
- Resolves parent trial and selector.
- Produces child trial input with provenance:
  - `ext.fork.parent_run_id`
  - `ext.fork.parent_trial_id`
  - `ext.fork.selector`
  - `ext.fork.source_checkpoint` (if used)
- Executes child trial under:
  - `.lab/runs/<run_id>/forks/<fork_id>/...`

### 6.3 Pause

```bash
lab pause --run-dir .lab/runs/<run_id> [--trial-id <trial_id>] [--label <name>] [--timeout-seconds 60] [--json]
```

Behavior (cooperative):
1. Resolve active trial control-plane file.
2. Write control action `checkpoint` with label.
3. Wait for corresponding `control_ack`.
4. Write control action `stop`.
5. Wait for `control_ack(action_observed=stop)` or timeout.

If timeout is hit:
- command fails with actionable diagnostics.
- run continues unchanged unless explicit `--force-stop` (future extension).

Notes:
- `pause` requires integration level >= `cli_events`.
- For `cli_basic`, command returns `unsupported_for_integration_level`.

### 6.4 Resume

```bash
lab resume --run-dir .lab/runs/<run_id> [--trial-id <trial_id>] [--label <checkpoint_label>] [--set k=v ...] [--strict] [--json]
```

Behavior:
- Requires run status `paused` and target trial status `paused`.
- Resolves resume selector from:
  1. `--label` (explicit checkpoint label),
  2. recorded trial pause label,
  3. latest checkpoint by step.
- Executes resume as a forked continuation from the resolved checkpoint selector.
- Fails if no checkpoint can be resolved.

## 7. Runtime State Artifacts

Add runtime metadata for out-of-process control and provenance.

### 7.1 Run control index

Path:
- `.lab/runs/<run_id>/runtime/run_control.json`

Fields:
- `schema_version: "run_control_v1"`
- `run_id`
- `status: running|completed|failed|paused`
- `active_trial_id: string|null`
- `active_adapter: object|null`
  - `id: string`
  - `version: string`
  - `command_path: string` (host path)
  - `events_path: string|null`
- `updated_at`

Purpose:
- `lab pause` can discover the currently active trial and adapter control channel.

### 7.2 Trial lifecycle state

Path:
- `.lab/runs/<run_id>/trials/<trial_id>/trial_state.json`

Fields:
- `schema_version: "trial_state_v1"`
- `trial_id`
- `status: running|completed|failed|paused`
- `pause_label: string|null`
- `checkpoint_selected: string|null`
- `exit_reason`
- `updated_at`

Purpose:
- Distinguish paused vs failed without changing `trial_output_v1` enum.

### 7.3 Replay/Fork manifests

Paths:
- `.lab/runs/<run_id>/replays/<replay_id>/manifest.json`
- `.lab/runs/<run_id>/forks/<fork_id>/manifest.json`

Fields:
- operation type (`replay` or `fork`)
- parent identifiers
- selector
- strict flag
- effective integration level
- resulting replay grade

## 8. Control-Plane Protocol

Keep current action enum (`continue|stop|checkpoint`) to avoid immediate schema churn.

Control file payload (runner-written):
- `schema_version: "control_plane_v1"`
- `seq: integer` (monotonic per trial)
- `action: continue|checkpoint|stop`
- `label: string|null` (used for pause checkpoint labeling)
- `requested_at`
- `requested_by` (`run_loop` or `lab_pause`)

Harness expectations:
- Observe control file at step boundaries.
- Emit `control_ack` containing:
  - `step_index`
  - `control_version`
  - `action_observed`

Runner expectations:
- For pause, require checkpoint ack before issuing stop.

## 8.1 Safety Invariants (MUST/FAIL)

1. Boundary-only operations (MUST):
- Pause and fork selectors are only actionable at committed boundaries (`agent_step_end` + corresponding `control_ack`, or committed checkpoint artifact).
- If a request is issued mid-flight (tool/model call in progress), runtime queues intent and waits for next committed boundary.

Failure policy:
- If boundary is not reached before timeout, operation fails with `boundary_timeout` and makes no partial state transition.

2. Two-phase pause handshake (MUST):
- Pause executes as `checkpoint` then `stop`.
- Runtime MUST observe `control_ack` for checkpoint request before writing stop request.
- Runtime MUST observe `control_ack(action_observed=stop)` before marking trial paused.

Failure policy:
- Missing/mismatched ack fails with `control_ack_missing` or `control_ack_mismatch`.

3. Committed-source fork (MUST):
- Fork source state MUST come from committed checkpoint/event barrier artifacts, never from live mutable workspace bytes.
- Strict fork requires explicit checkpoint/selector evidence; best-effort fork may degrade to parent input replay only.

Failure policy:
- Strict mode without committed source fails with `strict_source_unavailable`.

4. Atomic writes for control and metadata (MUST):
- Runtime writes control files and lifecycle metadata (`run_control.json`, `trial_state.json`, replay/fork manifests) via temp-file + fsync + rename.
- Readers only treat fully committed files/lines as visible state.

Failure policy:
- Any write failure fails operation and leaves previous committed state intact.

5. Single active control operation per run (MUST):
- Pause/fork/replay mutating operations acquire a run-level lock.
- Concurrent control operations against same run are rejected.

Failure policy:
- Second concurrent operation fails with `operation_in_progress`.

## 9. Implementation Plan

### Milestone A: Plumbing and metadata (no new commands yet)

Changes:
- `lab-runner` writes `runtime/run_control.json` and per-trial `trial_state.json`.
- Keep these files updated during normal `run*` execution.
- Add unit tests for lifecycle transitions.

Acceptance:
- During run, `active_trial_id` and `active_adapter` are correct.
- Trial transitions to `completed` or `failed` are always recorded.

### Milestone B: Replay MVP

Changes:
- Add `Replay` subcommand in `lab-cli`.
- Implement `lab_runner::replay_trial(...)`.
- Replay uses stored `trial_input.json`; writes to `replays/<replay_id>/`.
- Support `--strict` gating by integration level and evidence presence.

Acceptance:
- Replay command works end-to-end on demo harness.
- `--strict` fails for `cli_basic`, succeeds only when criteria met.
- Replay manifest includes parent linkage and grade.

### Milestone C: Fork MVP

Changes:
- Add `Fork` subcommand in `lab-cli`.
- Implement selector parser (`checkpoint`, `step`, `event_seq`).
- Build child input from parent + overrides.
- Execute child as fork run and write manifest.

Acceptance:
- Fork from checkpoint works when checkpoint exists.
- Fork falls back to parent input for best-effort levels unless `--strict`.
- Child trial input contains fork provenance fields.

### Milestone D: Pause MVP (cooperative)

Changes:
- Add `Pause` subcommand in `lab-cli`.
- Implement pause controller:
  - resolve active trial from `run_control.json`
  - write checkpoint action
  - wait for ack
  - write stop action
- Add bounded polling/tailing for hooks file in active trial.

Acceptance:
- For `cli_events` harness that emits `control_ack`, pause completes and trial state becomes `paused`.
- For `cli_basic`, pause returns explicit unsupported error.
- Timeout path is deterministic and leaves run metadata consistent.

### Milestone E: SDK and docs parity

Changes:
- SDK `LabClient` adds `replay`, `fork`, `pause`, `resume`.
- SDK types include new response envelopes.
- README and runtime docs updated with command examples and guarantee matrix.

Acceptance:
- SDK integration tests cover success/failure envelopes for each new command.
- Docs and CLI `--help` are aligned.

## 10. Code Change Map

Primary code paths:
- `rust/crates/lab-cli/src/main.rs`
- `rust/crates/lab-runner/src/lib.rs`
- `rust/crates/lab-hooks/src/lib.rs` (if additional pause-specific invariants are added)
- `sdk/src/client.ts`
- `sdk/src/types.ts`

Likely new modules:
- `rust/crates/lab-runner/src/control.rs`
- `rust/crates/lab-runner/src/replay.rs`
- `rust/crates/lab-runner/src/fork.rs`

Schema additions (new files):
- `schemas/run_control_v1.jsonschema`
- `schemas/trial_state_v1.jsonschema`

## 11. Testing Strategy

Unit tests:
- selector parsing
- strict gating logic
- control file write/read monotonic seq
- run/trial lifecycle transitions

Integration tests:
- replay from completed trial (`cli_basic`, `cli_events`)
- fork from checkpoint with override mutation
- pause happy path with emitted `control_ack`
- pause timeout path
- strict replay failure when required evidence missing

Negative tests:
- unknown trial id
- selector not found
- fork strict mode with no checkpoint
- pause on non-running run

## 12. Rollout and Compatibility

Rollout order:
1. Milestone A (metadata)
2. Milestone B/C (replay/fork)
3. Milestone D (pause)
4. Milestone E (SDK/docs)

Compatibility principles:
- Existing run artifacts remain valid.
- No breaking schema changes to `trial_input_v1` or `trial_output_v1` for MVP.
- New metadata files are additive and optional for old runs.

## 13. Risks and Mitigations

Risk: pause hangs due to missing hook acks.
- Mitigation: hard timeout + clear error + no implicit kill in MVP.

Risk: strict semantics over-claimed.
- Mitigation: explicit gating and fail-closed behavior on missing evidence.

Risk: selector ambiguity (`step` vs `event_seq`).
- Mitigation: typed selector syntax and parse-time validation.

## 14. Open Questions

- Should we add `pause` to hook action enums later (`control_ack.action_observed`) or keep pause as `checkpoint+stop` permanently?
- Should resume remain implemented as checkpoint-based fork under the hood, or graduate to an explicit in-place resume runtime model?
