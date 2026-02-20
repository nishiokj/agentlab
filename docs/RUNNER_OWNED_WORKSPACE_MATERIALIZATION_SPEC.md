# Runner-Owned Workspace Materialization Spec

Status: Proposed  
Date: 2026-02-14  
Owner: Runner/Runtime  
Scope: `rust/crates/lab-runner/src/lib.rs`, task boundary schema + mapper contract

## Problem Statement

Current SWE-bench runs are technically executing, but workspace hydration is wrong:

1. Task mappers emit tiny `workspace_files` payloads (`task/prompt.txt`, metadata) instead of a real repo checkout.
2. Runner materializes exactly what mapper provides, so `/agentlab/workspace` starts with no repository tree.
3. "Success" outputs can be structurally invalid for benchmark intent (creating files from scratch vs patching real checkout).

This violates benchmark realism, patch fidelity, and variant comparability.

## Product Principles

1. Task definitions are semantic; runtime plumbing is runner-owned.
2. Benchmark mappers should not need runner storage internals (`.lab/dataset_packs/...` digests).
3. Trial workspace must start from a deterministic, auditable source state.
4. Variant comparisons must share identical base workspace content per task.

## Current Behavior (As Implemented)

Runner supports two materialization channels in task boundary:

1. `workspace_files`: inline file writes into trial workspace.
2. `mount_references`: container-only mounts via `dataset_pack_ref: sha256:...`.

When `design.policies.task_boundary.require_workspace_materialization` is `true`, runner requires non-empty `workspace_files` or `mount_references` for every task boundary entry. This is policy-driven and benchmark-agnostic.

Result: mapper must currently know mount internals or runs degrade to prompt-only workspaces.

## Target Behavior

For benchmark tasks (starting with SWE-bench), runner owns workspace hydration end-to-end:

1. Mapper emits semantic task data (`repo`, `base_commit`, task payload).
2. Runner resolves or builds immutable checkout pack from semantic source.
3. Runner hydrates writable per-trial workspace from that immutable pack.
4. Runner records source digest + hydration method in trial artifacts.

No mapper-managed `dataset_pack_ref` is required in the happy path.

## Contract Changes

## 1) Task Boundary Schema Extension

Add optional semantic seed field:

```json
{
  "schema_version": "task_boundary_v1",
  "task": { "...": "..." },
  "workspace_seed": {
    "kind": "git_checkout",
    "repo": "astropy/astropy",
    "commit": "a5917978be39d13cd90b517e1de4e7a539ffaa48",
    "subdir": null
  },
  "workspace_files": [],
  "mount_references": [],
  "limits": {}
}
```

Rules:

1. `workspace_seed` is preferred for repo-under-test hydration.
2. `workspace_files` remains supported for small overlays or synthetic tasks.
3. `mount_references` remains supported for read-only auxiliary mounts.
4. For SWE-bench-like tasks, runner may infer `workspace_seed` from `task.swebench.repo` + `task.swebench.input.base_commit` if field is omitted.

## 2) Runner Validation Semantics

Replace current SWE-bench validation:

- Old: require non-empty `workspace_files` or `mount_references`.
- New: require at least one of:
  1. resolvable `workspace_seed`
  2. non-empty `workspace_files`
  3. non-empty `mount_references`

If none are available, fail early with explicit diagnostic.

## Runner Architecture Changes

## 3) Checkout Pack Manager

Introduce runner-owned pack manager with deterministic cache key:

- Key input: `seed.kind + repo + commit + subdir + pack_format_version`
- Store path: `.lab/dataset_packs/sha256/<digest>/`
- Metadata file: `.lab/dataset_packs/sha256/<digest>.json`

Pack manager behavior:

1. Cache hit: reuse existing immutable pack.
2. Cache miss: materialize checkout, normalize, compute digest, persist atomically.
3. Use lock files per digest to avoid duplicate concurrent builds.

## 4) Hydration Into Trial Workspace

For each trial:

1. Resolve pack digest from seed.
2. Hydrate trial workspace from immutable pack using best available method:
   1. reflink (preferred)
   2. hardlink
   3. copy fallback
3. Apply task `workspace_files` as overlays after base hydration.

Do not mount immutable checkout as trial workspace directly; trial workspace must remain writable.

## 5) Observability + Provenance

Add `workspace_seed_resolution.json` per trial with:

1. `seed` (semantic seed resolved)
2. `pack_digest`
3. `cache_hit` boolean
4. `hydration_method`
5. `source_summary` (file count, bytes)

Emit events:

1. `workspace_seed_resolved`
2. `workspace_pack_cache_hit` / `workspace_pack_cache_miss`
3. `workspace_hydrated`

Include this in `evidence_records.jsonl` refs.

## 6) Variant Consistency Guardrail

For a given task id within a run:

1. baseline and treatment must resolve to the same `pack_digest` unless explicitly overridden.
2. if mismatch occurs, fail trial scheduling for that task with deterministic error.

This guarantees control/treatment diffability.

## Migration Plan

## Phase 1 (Compatibility)

1. Add `workspace_seed` parsing and inference.
2. Keep current `workspace_files` and `mount_references` fully working.
3. Add warning when SWE-bench task has no seed and only prompt files.

## Phase 2 (Default Path)

1. Update official SWE-bench mapper to emit semantic seed only.
2. Runner auto-hydrates checkout pack.
3. Deprecate mapper-side direct `dataset_pack_ref` construction in docs.

## Phase 3 (Hardening)

1. Make SWE-bench prompt-only workspace materialization a hard error.
2. Enforce variant pack digest parity by default.

## Non-Goals

1. Replacing existing generic mount system.
2. Introducing remote pack registry in this phase.
3. Solving multi-repo task hydration beyond explicit seed schema.

## Risks

1. Pack build latency on first run.
2. Disk growth from large checkout cache.
3. Cross-platform differences for reflink/hardlink behavior.

Mitigations:

1. Cache + locking + reuse.
2. Add future pack GC command.
3. Implement method fallback with deterministic logging.

## Acceptance Criteria

1. SWE-bench trial pre-snapshot contains repository tree (not only `task/*` files).
2. Trial workspace source is auditable via `workspace_seed_resolution.json`.
3. Control and treatment for same task share `pack_digest`.
4. Unified diffs are generated against real checkout baseline.
5. Mapper can omit `dataset_pack_ref` internals and still produce correct workspace.

## Suggested Initial Implementation Tasks

1. Add schema + parsing for `workspace_seed` in task boundary handling.
2. Implement `resolve_workspace_seed_for_task(...)` with SWE-bench inference.
3. Add `CheckoutPackManager` module in runner.
4. Replace direct prompt-only materialization path for SWE-bench with seed hydration.
5. Write integration test: one SWE-bench task, two variants, assert same pack digest + repo files present pre-trial.
