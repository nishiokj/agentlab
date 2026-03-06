# DX Evidence CI Validation Contract

## Purpose

Define non-negotiable machine checks for phase-gate completion so invariant sign-off cannot be gamed.

## Scope

The validator enforces phase evidence ledgers at:

- `docs/evidence/dx/phase0/invariants.md`
- `docs/evidence/dx/phase1/invariants.md`
- `docs/evidence/dx/phase2/invariants.md`

Validator entrypoint:

```bash
scripts/ci/validate-dx-evidence.sh
```

## CLI Contract

```bash
scripts/ci/validate-dx-evidence.sh --phase <phase0|phase1|phase2> [--file <path>] [--mode <lint|strict>]
```

- `--phase`: required.
- `--file`: optional override; defaults to `docs/evidence/dx/<phase>/invariants.md`.
- `--mode`: optional; default `lint`.
  - `lint`: enforce table shape, required IDs present, field sanity.
  - `strict`: all `lint` rules plus phase completion gating.

## Invariant Row Format

The validator reads markdown table rows of this shape:

```text
| P0-I01 | PASS | tests/... | docs/evidence/dx/phase0/artifacts/... | @reviewer | notes |
```

Columns:

1. `ID`
2. `Status`
3. `Test`
4. `Evidence`
5. `Reviewer`
6. `Notes` (optional)

## Required IDs by Phase

- `phase0`: `P0-I01..P0-I06`
- `phase1`: `P1-I01..P1-I06`
- `phase2`: `P2-I01..P2-I03`

## Lint Mode Rules

1. Ledger file exists.
2. All required IDs for the phase are present as rows.
3. No duplicate invariant IDs.
4. `Status` is one of: `PENDING`, `BLOCKED`, `PASS`.
5. `Test`, `Evidence`, and `Reviewer` are non-empty.
6. `Evidence` path must be relative (not absolute).
7. For `PASS` rows:
   - `Evidence` file must exist.
   - `Test` and `Reviewer` must not be placeholders (`TBD`, `TODO`, `N/A`, `-`).

## Strict Mode Rules

In addition to lint mode:

1. Every required invariant ID must have `Status=PASS`.
2. Every required invariant ID must have an existing evidence artifact path.
3. Any non-`PASS` required invariant fails the phase gate.

## Exit Codes

- `0`: validation passed.
- `1`: validation failed (missing IDs, bad status, missing evidence, etc.).
- `2`: CLI usage/config error (invalid args, unknown phase).

## CI Integration (Example)

```yaml
- name: Validate DX Evidence (Phase 0 strict gate)
  run: scripts/ci/validate-dx-evidence.sh --phase phase0 --mode strict
```

For non-gating branches, run lint mode for all phases:

```yaml
- name: Validate DX Evidence Shape
  run: |
    scripts/ci/validate-dx-evidence.sh --phase phase0 --mode lint
    scripts/ci/validate-dx-evidence.sh --phase phase1 --mode lint
    scripts/ci/validate-dx-evidence.sh --phase phase2 --mode lint
```
