# Phase 0 Invariants Ledger

## Phase Metadata

- phase: `phase0`
- owner: `@codex`
- status: `completed`
- commit_sha: `uncommitted`
- ci_run_url: `local`
- updated_at_utc: `2026-03-03T00:00:00Z`

## North Star UX/DX State

1. A user can declare `benchmark: bench_v0` without wiring dataset/adapter/policy internals.
2. A container command can invoke `rex` by name, without `/opt/agent/bin/...`.
3. Authoring uses short artifact/config references, with runner-resolved canonical paths.
4. Resolved manifests are deterministic and include pinned artifact digests.

## Invariant Table

| ID | Status | Test | Evidence | Reviewer | Notes |
|---|---|---|---|---|---|
| P0-I01 | PASS | rust/tests::p0_i01_and_p0_i05_dx_registry_resolution_is_complete_and_deterministic | docs/evidence/dx/phase0/artifacts/p0_i01_registry_resolution.md | @codex | Built-in `bench_v0` registry fields resolve from minimal authoring surface. |
| P0-I02 | PASS | rust/tests::p0_i02_dx_authoring_rejects_legacy_fields | docs/evidence/dx/phase0/artifacts/p0_i02_preflight_errors.md | @codex | Legacy dataset/runtime/design fields are hard-rejected for DX authoring. |
| P0-I03 | PASS | rust/tests::p0_i03_injected_container_env_includes_agent_path | docs/evidence/dx/phase0/artifacts/p0_i03_rex_pathless_invocation.log | @codex | Injected container runs receive PATH including `/opt/agent/bin`. |
| P0-I04 | PASS | rust/tests::p0_i04_artifact_digest_pin_rejects_mutation | docs/evidence/dx/phase0/artifacts/p0_i04_artifact_digest_checks.md | @codex | Pinned digest mismatch fails before execution. |
| P0-I05 | PASS | rust/tests::p0_i01_and_p0_i05_dx_registry_resolution_is_complete_and_deterministic | docs/evidence/dx/phase0/artifacts/p0_i05_resolved_manifest_golden.json | @codex | Deterministic resolved defaults snapshot for minimal bench_v0 contract. |
| P0-I06 | PASS | rust/tests::p0_i06_and_p1_i06_canonical_example_has_no_boundary_leaks_or_cp_hacks | docs/evidence/dx/phase0/artifacts/p0_i06_doc_lint_report.txt | @codex | Canonical fixture avoids internal path leaks and shell copy glue. |
