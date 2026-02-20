# Variant-Executable Runtime Spec

Status: Proposed  
Date: 2026-02-20  
Owner: Experiments Runtime (Runner + CLI + SDK)

## 1. Problem Statement

Variants are currently treated primarily as `bindings` plus optional `runtime_overrides`. That is insufficient for real agent evaluation where a variant is the executable runtime itself (code, dependencies, image, entrypoint, environment, tool implementations).

This blocks core use cases:

1. Testing different agent stacks (for example, Claude CLI vs Codex CLI).
2. Testing runtime/tool implementation changes as first-class variants.
3. Efficiently running many variants/trials with controlled concurrency.
4. Reducing startup overhead with optional keep-warm execution while preserving provenance.

## 2. Product Definition (Source of Truth)

A variant is a runnable executable instance definition.  
`bindings` are task-level inputs, not the full variant identity.

Variant identity must include:

1. Executable source/build/prebuilt reference.
2. Runtime invocation (entrypoint/args/env).
3. Isolation and execution policy (container/local, warm/cold policy, parallelism limits).
4. Optional task-facing bindings.

## 3. Current Gaps (Verified in Code)

1. `design.max_concurrency` is not enforced in runtime execution.
2. Trial execution is currently serial.
3. Container path is cold-start `docker run --rm` per trial.
4. No first-class per-variant build phase in the active runner.
5. SDK/CLI expose variant authoring mostly as `variant_id + bindings`.

## 4. Goals

1. First-class executable-level variants in experiment config.
2. Runner-owned per-variant build phase with deterministic cache/dedup.
3. Parallel execution across variants and across replications of task+variant.
4. Explicit keep-warm mode for low-latency startup when desired.
5. Full provenance of exactly what executable/image digest ran each trial.
6. Backward compatibility for existing baseline/variant-plan bindings flows.

## 5. Non-Goals

1. Remote executor implementation in this phase.
2. Replacing task/dataset boundary contracts.
3. Removing `bindings` support.

## 6. Proposed Experiment Model

## 6.1 Variant Shape

Add first-class variant runtime surface to `baseline` and `variant_plan[]`:

1. `variant_id` (required)
2. `bindings` (optional, task-level inputs)
3. `executable` (new; source/build/prebuilt spec)
4. `runtime_overrides` (still supported for targeted runtime merges)
5. `execution` (new; concurrency/keep-warm policy)

Example (Codex CLI vs Claude CLI):

```yaml
baseline:
  variant_id: codex_cli
  bindings: {}
  executable:
    source:
      kind: docker_build
      context: ./agents/codex
      dockerfile: ./agents/codex/Dockerfile
    runtime:
      entrypoint: ["codex", "exec"]
      env_from_host: ["OPENAI_API_KEY"]
  execution:
    max_parallel_trials: 2
    keep_warm:
      mode: pool
      pool_size: 1
      idle_ttl_seconds: 300

variant_plan:
  - variant_id: claude_cli
    bindings: {}
    executable:
      source:
        kind: docker_build
        context: ./agents/claude
        dockerfile: ./agents/claude/Dockerfile
      runtime:
        entrypoint: ["claude", "exec"]
        env_from_host: ["ANTHROPIC_API_KEY"]
    execution:
      max_parallel_trials: 2
      keep_warm:
        mode: off
```

## 6.2 Backward Compatibility

If a variant does not define `executable`, runner resolves from current `runtime.agent` behavior and treats it as a normalized executable spec.

## 7. Runner Architecture Changes

## 7.1 Variant Build Phase

Before scheduling trials:

1. Resolve each variant executable source.
2. Build or fetch executable artifact (for example container image).
3. Record artifact digest + metadata in run-level manifest.
4. Attach resolved artifact to variant runtime profile.

Build cache requirements:

1. Deterministic fingerprint from context + dockerfile + build args + relevant runtime config.
2. Locking per fingerprint to dedupe concurrent builds.
3. Cache hit/miss telemetry.

## 7.2 Scheduling and Concurrency

Replace serial trial loop with bounded worker execution:

1. Global cap = `design.max_concurrency`.
2. Optional per-variant cap = `variant.execution.max_parallel_trials`.
3. Scheduling policy order (`paired_interleaved`, `variant_sequential`, `randomized`) remains source order for dispatch.
4. Completion order may differ under concurrency.

Must support:

1. Multiple variants running simultaneously.
2. Multiple replications for same task+variant running simultaneously (if policy permits).

## 7.3 Keep-Warm Runtime

Execution policy:

1. `keep_warm.mode: off` (default): strict cold-start per trial.
2. `keep_warm.mode: pool`: maintain N warm workers for variant and run trials via exec.

Keep-warm requirements:

1. Explicitly opt-in per variant.
2. Configurable pool size and idle TTL.
3. Worker lifecycle events and IDs captured in trial evidence.
4. Isolation/provenance flags indicate warm-worker reuse.

## 8. Provenance and Evidence Additions

Per trial, record:

1. `variant_executable_digest`
2. `variant_build_digest`
3. `variant_build_cache_hit`
4. `worker_mode` (`cold` | `warm_pool`)
5. `worker_id` (if pooled)
6. `resolved_entrypoint`

Run-level manifest additions:

1. Variant build manifest entries (source -> artifact digest mapping).
2. Build timings and cache stats.

## 9. CLI and SDK UX

## 9.1 CLI

Add:

1. `lab run --variant <id>` (repeatable) to run subset of variants.
2. Optional run-time overrides for concurrency and keep-warm controls.

## 9.2 SDK

Add first-class variant APIs:

1. `baselineVariant(spec)`
2. `addVariantSpec(spec)`

Keep existing convenience APIs (`baseline(id, bindings)`, `addVariant(id, bindings)`) by lowering to normalized variant specs.

## 10. Schema and Contract Updates

Update:

1. `schemas/resolved_experiment_v0_5.jsonschema`
2. Runner validation for new variant executable fields
3. SDK `ExperimentSpec` types and builder validation
4. CLI help and JSON output payloads

## 11. Rollout Plan

## Phase 1: Schema + Type Surface

1. Add variant executable and execution schema.
2. Add SDK types/builders.
3. Keep old fields fully compatible.

## Phase 2: Build + Cache

1. Implement runner variant build resolver.
2. Add cache + lockfile dedup.
3. Emit build manifests.

## Phase 3: Concurrency

1. Implement worker pool scheduler.
2. Enforce global/per-variant concurrency.
3. Preserve scheduling policy semantics.

## Phase 4: Keep-Warm

1. Implement warm pool runtime.
2. Add lifecycle controls and evidence fields.
3. Update grading/provenance signals.

## Phase 5: UX + Migration

1. CLI variant selection and overrides.
2. SDK ergonomic APIs.
3. Migrate docs/examples to executable-level variants.

## 12. Acceptance Criteria (Definition of Done)

1. Variant configs can define different executable runtimes (for example Claude CLI vs Codex CLI) without hacks.
2. Runner builds/loads variant executables before scheduling and caches deterministically.
3. `design.max_concurrency` is enforced at runtime.
4. Multiple variants can run concurrently in one experiment run.
5. Multiple replications for a single task+variant can run concurrently when configured.
6. Keep-warm pool mode reduces startup overhead and is explicitly auditable.
7. Trial evidence includes executable/build/worker provenance fields.
8. CLI and SDK support first-class variant executable authoring and variant selection.
9. Existing binding-only configs continue to run via compatibility normalization.
10. Integration tests cover:
11. Mixed executable variants in one run.
12. Concurrency caps behavior.
13. Keep-warm worker reuse behavior.

## 13. Risks and Mitigations

1. Build latency spikes:
2. Mitigate with deterministic cache + lock dedup.
3. Warm-pool isolation ambiguity:
4. Mitigate with explicit mode flags and provenance fields.
5. Complexity in scheduler refactor:
6. Mitigate with phased rollout and policy-preserving test fixtures.

## 14. Open Questions

1. Should keep-warm pools be variant-scoped only, or also task-scoped?
2. Should per-variant concurrency default to unlimited (bounded by global) or `1`?
3. Should `lab run --variant` be strict-fail on unknown IDs or warn-and-skip?
4. What minimum provenance fields are required for publish/report compatibility?
