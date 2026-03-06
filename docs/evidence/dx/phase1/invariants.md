# Phase 1 Invariants Ledger

## Phase Metadata

- phase: `phase1`
- owner: `@codex`
- status: `completed`
- commit_sha: `uncommitted`
- ci_run_url: `local`
- updated_at_utc: `2026-03-03T00:00:00Z`

## North Star UX/DX State

1. Agent commands express intent only; bindings are projected structurally, not templated.
2. Workspace file injection is declarative (`workspace_patches`), no shell `cp` preambles.
3. Preflight catches missing bindings/invalid patch paths before any run starts.
4. Canonical docs show only the minimal opinionated path.

## Invariant Table

| ID | Status | Test | Evidence | Reviewer | Notes |
|---|---|---|---|---|---|
| P1-I01 | PASS | rust/tests::p1_i01_missing_binding_keys_fail_projection | docs/evidence/dx/phase1/artifacts/p1_i01_missing_bindings.md | @codex | Missing binding keys fail arg projection before run execution. |
| P1-I02 | PASS | rust/tests::p1_i02_binding_projection_order_is_deterministic | docs/evidence/dx/phase1/artifacts/p1_i02_argv_order.md | @codex | `bindings_to_args` preserves declaration order exactly. |
| P1-I03 | PASS | rust/tests::p1_i03_binding_projection_preserves_single_token_values | docs/evidence/dx/phase1/artifacts/p1_i03_argv_integrity.md | @codex | Values with spaces are passed as single argv tokens. |
| P1-I04 | PASS | rust/tests::p1_i04_workspace_patch_path_rejection | docs/evidence/dx/phase1/artifacts/p1_i04_patch_path_rejection.md | @codex | Absolute/traversal patch targets are rejected. |
| P1-I05 | PASS | rust/tests::p1_i05_workspace_patch_ordering_overwrites_deterministically | docs/evidence/dx/phase1/artifacts/p1_i05_patch_order_and_overwrite.md | @codex | Workspace patch ordering and overwrite semantics are deterministic. |
| P1-I06 | PASS | rust/tests::p0_i06_and_p1_i06_canonical_example_has_no_boundary_leaks_or_cp_hacks | docs/evidence/dx/phase1/artifacts/p1_i06_no_cp_hacks_report.txt | @codex | Canonical examples exclude shell `cp` glue. |
