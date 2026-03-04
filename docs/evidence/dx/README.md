# DX Evidence

This directory contains phase-gate evidence for the DX hard-cutover plan.

## Layout

- `INVARIANTS_TEMPLATE.md`: template for phase invariant ledgers.
- `CI_VALIDATION_CONTRACT.md`: machine-checkable contract for CI gating.
- `phase0/invariants.md`: Phase 0 invariant ledger.
- `phase1/invariants.md`: Phase 1 invariant ledger.
- `phase2/invariants.md`: Phase 2 invariant ledger.

## Validation Commands

Lint mode (shape + required rows + field sanity):

```bash
scripts/ci/validate-dx-evidence.sh --phase phase0 --mode lint
scripts/ci/validate-dx-evidence.sh --phase phase1 --mode lint
scripts/ci/validate-dx-evidence.sh --phase phase2 --mode lint
```

Strict mode (all required invariants must be `PASS` and evidence paths must exist):

```bash
scripts/ci/validate-dx-evidence.sh --phase phase0 --mode strict
```

Use strict mode only when a phase is being declared complete.
