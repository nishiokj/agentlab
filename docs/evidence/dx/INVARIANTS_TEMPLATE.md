# DX Invariants Ledger Template

## Phase Metadata

- phase: `phaseX`
- owner: `@owner`
- status: `in_progress`
- commit_sha: `TBD`
- ci_run_url: `TBD`
- updated_at_utc: `YYYY-MM-DDTHH:MM:SSZ`

## North Star UX/DX State

1. `TBD`
2. `TBD`
3. `TBD`
4. `TBD`

## Invariant Table

| ID | Status | Test | Evidence | Reviewer | Notes |
|---|---|---|---|---|---|
| PX-I01 | PENDING | tests/path/to/test | docs/evidence/dx/phaseX/artifacts/px_i01.txt | @reviewer |  |

## Status Vocabulary

- `PENDING`: not complete yet.
- `BLOCKED`: cannot proceed due to external blocker.
- `PASS`: complete and verified by CI + evidence artifact.

## Rules

1. The ledger is add-only. Do not remove historical invariant rows.
2. Every invariant row must include both `Test` and `Evidence`.
3. `PASS` rows require existing evidence files and a non-placeholder reviewer.
4. A phase can only be closed when all required invariant IDs are `PASS`.
