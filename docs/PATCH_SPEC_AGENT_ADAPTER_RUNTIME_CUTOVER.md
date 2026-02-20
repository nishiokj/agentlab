# Patch Spec: Agent Adapter Runtime Cutover

Status: Implemented in runner core (hard cutover)

## Goal

Make runner/agent integration explicit and stable:

1. Runner owns scheduling, durability, evidence, and analysis.
2. Adapter owns agent-specific invocation and control plumbing.
3. Unknown agent interfaces are not auto-detected or inferred.

This patch removes legacy harness command plumbing from runner core and replaces it with an adapter capability contract.

---

## Problem Statement

Current runner behavior mixes two responsibilities:

1. Experiment orchestration.
2. Agent-specific process/control wiring (`runtime.agent_loop`, control file ack logic, container/local launch internals).

That mix creates hidden seams and fragile behavior when supporting agents with different native interfaces.

---

## New Runtime Model

### Runner-owned

1. Trial scheduling (`run` / `continue` engine).
2. Task boundary parsing and policy enforcement.
3. Trial lifecycle state + durability.
4. Evidence and analysis output.

### Adapter-owned

1. `start_trial` / `run_trial` invocation against a specific agent product.
2. Optional `pause_trial` / `stop_trial`.
3. Optional event stream + control acknowledgements.
4. Normalization to runner result contract (`agent_result_v1` written to canonical `result.json`).

### Capability-driven behavior

Runner gates operations on declared adapter capabilities:

1. `pause`
2. `control_ack`
3. `event_stream`
4. `strict_replay`

No capability implies explicit rejection, not fallback magic.

---

## Patch Plan

## Phase 1: Introduce Adapter Contract + Registry

1. Add `AgentAdapter` trait (or equivalent interface) in runner crate.
2. Add adapter registry keyed by `(adapter_id, adapter_version)`.
3. Extend runtime config to resolve adapter ref from `runtime.agent`.
4. Keep existing runner outputs unchanged (`trial_metadata`, evidence, analysis).

## Phase 2: Wire Runner to Adapter Interface

1. Replace direct `run_agent_loop_*` calls with adapter invocation.
2. Replace pause control-path logic with adapter `pause_trial`/`stop_trial`.
3. Keep continue semantics unchanged; only invocation backend changes.

## Phase 3: Hard Delete Legacy Harness Plumbing

Delete symbols listed in **Delete Matrix**.

## Phase 4: Schema + Docs Cutover

1. Promote `runtime.agent` as primary/required runtime seam.
2. Remove `runtime.agent_loop` from default user path.
3. Document “first-class adapters” + “BYOA adapter implementation” paths.

---

## Delete Matrix (Required)

All delete targets below are in `rust/crates/lab-runner/src/lib.rs` unless noted.

| Delete target | Why delete | Replacement |
|---|---|---|
| `resolve_agent_loop` fallback branch that reads `/runtime/agent_loop` | Legacy manual harness seam | Adapter ref resolution from `runtime.agent` |
| `run_agent_loop_local` | Runner should not own product-specific launch plumbing | `adapter.run_trial(...)` |
| `run_agent_loop_container` | Same as above | `adapter.run_trial(...)` |
| `run_process_with_trial_io` | Process orchestration belongs in adapter runtime | Adapter internal process transport |
| `HarnessControlMode` | Legacy runner-side control transport abstraction | Adapter capability + adapter transport |
| `HarnessControlTransport` | Same | Adapter-owned handle/session state |
| `resolve_control_paths` | Runner-specific control file/socket path mapping | Adapter-managed control channel |
| `wait_for_control_transport` | Runner transport polling | Adapter internals |
| `send_hti_request` | Runner-specific control RPC bridge | Adapter internals |
| `write_control_file` | Runner-authored control command file | Adapter command bridge |
| `wait_for_control_ack` | Runner parsing agent control acks from events | Adapter returns typed ack |
| `has_control_ack` | Same | Adapter returns typed ack |
| `parse_harness_invocation_from_labels` | Image label-based implicit command discovery | Explicit adapter manifest/registry metadata |
| `load_harness_invocation_from_image_file` | Same | Explicit adapter metadata |
| `validate_harness_invocation_metadata` | Same | Adapter manifest validation |
| `resolve_harness_invocation` | Legacy harness command resolver | Adapter runtime invocation plan |
| `resolve_command_local` and `validate_harness_command` usage for agent launch | Local script-path command seam in runner core | Adapter-specific validation |

### Additional required deletions outside function symbols

1. Remove `runtime.agent_loop` checks from required field validation paths.
2. Remove tests that assert `runtime.agent_loop` behavior as primary.
3. Remove docs/examples that teach direct harness command wiring as default.

---

## Compatibility Policy

Hard cutover. No compatibility branch for `runtime.agent_loop` in core runner path.

Allowed migration aid:

1. One explicit migration error message pointing to adapter docs.
2. Optional CLI migration utility that rewrites known `runtime.agent_loop` specs to `runtime.agent.custom_image`.

No silent fallback.

---

## Acceptance Gates

Cutover is complete only when all are true:

1. Runner core contains no direct agent launch functions (`run_agent_loop_local`, `run_agent_loop_container`).
2. Runner core contains no control ack parsing (`wait_for_control_ack`, `has_control_ack`).
3. `runtime.agent_loop` is not used as an execution path in runner.
4. Pause/stop behavior is capability-gated by adapter contract.
5. At least one first-class adapter (for a known agent product) passes run + continue + pause gating tests.
6. BYOA adapter scaffold test passes with explicit capability declarations.

---

## Test Additions Required

1. Adapter contract conformance tests:
   - required methods, result normalization, error semantics.
2. Capability gating tests:
   - pause requested on adapter without `pause` capability returns explicit unsupported error.
3. Run parity tests:
   - same input state produces same result structure across adapter implementations.
4. Continue parity tests:
   - paused/interrupted continue uses identical scheduler semantics regardless of adapter.

---

## Notes on Scope

This patch spec intentionally targets runtime seam cleanup only.

Benchmark scoring adapters remain separate (`docs/BENCHMARK_ADAPTER_PROTOCOL.md`), but both systems should share the same design rule:

1. product-specific logic lives in adapters.
2. runner core remains benchmark- and agent-product-agnostic.
