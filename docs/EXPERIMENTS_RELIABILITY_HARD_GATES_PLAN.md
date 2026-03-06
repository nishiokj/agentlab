# Experiments Reliability Hard-Gate Plan

> Status: Reliability workstream reference. Treat as WS1 input to `docs/V1_BENCHMARK_MIGRATION_PLAN.md`.


Date: 2026-03-02  
Scope: `Experiments` package only (runner, adapters, schemas, CI gates).  
Driver: follow-up to `POSTMORTEM_2026-03-02_bench_v0_experiment_runner_incident.md`.

## Objective
Convert incident lessons into blocking, test-enforced invariants so the same failure classes cannot reach runtime.

## Non-Negotiable Invariants

| ID | Invariant | Must Hold Before Merge |
| --- | --- | --- |
| INV-01 | Adapter rows are schema-valid | All benchmark prediction/score outputs validate against JSON schema, including required identity fields (`schedule_idx`, `slot_commit_id`, `attempt`, `row_seq`). |
| INV-02 | Timeout policy is behaviorally enforced | `runtime.policy.timeout_ms` propagates to runtime env and actually constrains trial execution. |
| INV-03 | Disk headroom is a hard gate | Run/preflight fails before scheduling when free disk is below threshold. |
| INV-04 | Container artifact staging/unpack is robust | Per-task artifact unpack works in constrained Docker semantics (`docker cp` + tar flags). |
| INV-05 | Workspace evidence path is bounded | Exclusion logic applies to snapshot and copy paths; regression guard exists for dependency-heavy trees. |
| INV-06 | Recover->continue works from hostile context | Continue path works for interrupted runs under minimal shell/env assumptions. |
| INV-07 | Provider/model wiring is preflight-validated | Bound providers/models are rejected early when capability/env mapping is invalid. |
| INV-08 | CI blocks on all above | No non-blocking path for these checks; trigger coverage includes runner/adapters/scripts/schema changes. |

## Implementation Plan (Concrete)

## PR-1: Adapter Contract Hardening (INV-01)

### Files
- `adapters/harbor/check_harbor_adapter_compat.py`
- `scripts/harbor/tests/test_harbor_benchmark_adapter.py`
- `bench/integration/agentlab/bench_benchmark_adapter.py`
- `bench/integration/agentlab/test_bench_benchmark_adapter.py` (new)
- `bench/taskkit/schema.py` (reuse existing schema helpers, no duplicate validator stack)

### Changes
1. Replace schema-version-only checks in Harbor compat probe with full JSON-schema validation using `bench.taskkit.schema.validate_with_schema_file`.
2. Update existing Harbor adapter tests to assert required identity fields on both prediction and score payloads.
3. Add dedicated bench adapter tests for:
   - schema-valid success payload,
   - schema-valid grader-error payload,
   - explicit failure when required identity fields are absent.
4. Ensure adapter tests validate both `benchmark_prediction_record_v1.jsonschema` and `benchmark_score_record_v1.jsonschema`.

### Required test updates (existing)
1. Update `test_fallback_records_match_schema_shape` to assert:
   - `schedule_idx >= 0`,
   - non-empty `slot_commit_id`,
   - `attempt >= 1`,
   - `row_seq >= 0`,
   - full schema validation pass.
2. Replace `_assert_schema` usage in compat probe output validation with schema validation calls (remove string-only legacy assert path).

### New tests (add)
1. `test_bench_adapter_prediction_record_validates_schema`
2. `test_bench_adapter_score_record_validates_schema`
3. `test_bench_adapter_missing_identity_fields_fails_schema_validation`
4. `test_harbor_compat_probe_rejects_schema_invalid_output`

### Acceptance criteria
1. Removing any of the four required identity fields causes test failure.
2. Compat probe fails with actionable error on schema-invalid adapter output.
3. No test remains that only checks `schema_version` for contract correctness.

## PR-2: Runner Guardrails (INV-02, INV-03, INV-07)

### Files
- `rust/crates/lab-runner/src/lib.rs`
- `rust/crates/lab-cli/src/main.rs` (only if CLI summary/output needs new preflight field exposure)

### Changes
1. Add disk preflight check:
   - Default minimum free bytes: `20 GiB` (configurable via env, e.g. `AGENTLAB_MIN_FREE_BYTES`).
   - Check run root filesystem before trial scheduling.
   - Fail preflight with explicit required/available bytes.
2. Add provider/model preflight check:
   - Read providers from variant bindings (`model_provider`) across baseline + variant plan.
   - Validate each provider has a runtime mapping (`--provider-env provider=ENV`) and env source in config.
   - Fail when binding references unmapped provider or missing required env.
3. Add timeout propagation conformance checks:
   - Verify resolved input policy timeout is passed to `AGENTLAB_TIMEOUT_MS`.
   - Verify template substitution keeps timeout token in resolved command when configured.

### Required test updates (existing)
1. Extend runtime/config tests that currently only assert pointer presence to assert behavioral propagation.
2. Extend preflight test coverage to include hard-fail disk and provider-mapping failures.

### New tests (add)
1. `inv02_timeout_policy_propagates_to_runtime_env`
2. `inv02_timeout_template_in_command_is_rendered`
3. `inv03_preflight_fails_below_min_disk_headroom`
4. `inv03_preflight_passes_at_or_above_min_disk_headroom`
5. `inv07_preflight_fails_for_unmapped_model_provider`
6. `inv07_preflight_fails_when_provider_env_var_missing`
7. `inv07_preflight_passes_for_valid_provider_bindings`

### Acceptance criteria
1. Preflight blocks low disk with deterministic fail message.
2. Provider/model binding mistakes fail before run start.
3. Timeout override affects actual runtime behavior, not just config shape.

## PR-3: Runtime Resilience + Performance Gates (INV-04, INV-05, INV-06)

### Files
- `rust/crates/lab-runner/src/lib.rs`
- `rust/crates/lab-runner/testdata/*` (new fixtures only as needed)

### Changes
1. Add Docker integration test for artifact staging/unpack path (`run_injected_container` path).
2. Add large workspace fixture test to assert exclusions are applied in both:
   - snapshot collection path,
   - copy path.
3. Add recover->continue hostile-context tests:
   - interrupted run fixture,
   - minimal env,
   - relative/absolute path permutations.

### Required test updates (existing)
1. Expand current continue tests beyond status acceptance/persisted behavior checks.
2. Expand workspace exclusion tests from functional-only to regression guard coverage for heavy excluded directories.

### New tests (add)
1. `inv04_artifact_unpack_succeeds_with_container_fs_constraints`
2. `inv05_workspace_evidence_exclusions_apply_to_snapshot_and_copy`
3. `inv05_large_workspace_exclusion_guard_completes_within_budget`
4. `inv06_recover_then_continue_succeeds_with_minimal_env`
5. `inv06_continue_handles_relative_and_absolute_dataset_paths`

### Acceptance criteria
1. Artifact unpack failures from ownership/perms assumptions are reproducibly caught in CI.
2. Evidence traversal regressions are caught before merge.
3. Continue path no longer depends on fragile shell invocation context.

## PR-4: CI Hard Gates + Trigger Coverage (INV-08)

### File
- `.github/workflows/benchmark-v0-gates.yml`

### Changes
1. Expand PR trigger paths to include:
   - `rust/**`
   - `adapters/**`
   - `scripts/**`
   - `bench/integration/**`
   - `schemas/**`
   - `.github/workflows/benchmark-v0-gates.yml`
2. Add blocking jobs:
   - `runner-unit-contracts`:
     - `cargo test --manifest-path rust/Cargo.toml -p lab-runner`
   - `adapter-schema-contracts`:
     - Python tests for Harbor + bench adapter schema conformance.
   - `runner-docker-integration`:
     - Docker-required integration subset for container artifact path and recover/continue smoke.
3. Keep existing schema validation and strict suite validation jobs; do not duplicate equivalent checks.

### Acceptance criteria
1. Any failure in INV-01..INV-07 blocks merge.
2. Changes to runner/adapters/scripts cannot bypass gates due to path filter gaps.
3. No `continue-on-error` for reliability gates.

## Legacy Bloat Elimination Rules (Mandatory)
1. Remove shallow test paths superseded by full schema validation (no dual maintenance).
2. For each invariant, keep one canonical positive and one canonical negative test unless concurrency/state semantics require more.
3. New tests must include invariant ID in name prefix (`inv01_`, `inv02_`, ...).
4. Do not add alternate/legacy adapter contract paths in test harness.
5. Reuse existing schema utility (`bench.taskkit.schema`) instead of adding parallel validators.

## Definition of Done
1. All eight invariants map to implemented tests and passing CI jobs.
2. At least one negative test per invariant fails when the invariant is violated.
3. CI workflow enforces these checks as blocking on PR.
4. Superseded shallow tests are removed.
5. Incident classes RC-1 through RC-7 from the postmortem are each covered by at least one invariant-linked test.

## Execution Order
1. PR-1 adapter contract hardening.
2. PR-2 runner guardrails (timeout/disk/provider).
3. PR-3 runtime resilience/performance/recovery.
4. PR-4 CI hard-gate wiring and trigger expansion.

This order is mandatory to prevent adding CI jobs before the required tests exist.
