# Ship Readiness Audit - 2026-03-06

## Outcome

`Experiments` is at a minimum viable ship state for today with the changes in this pass:

1. The active `lab-runner` normalization regression is fixed.
2. DX naming compatibility is tightened around `arg_map` vs `bindings_to_args`.
3. Persistent workspace carry-forward now fails fast before attempting to bundle oversized workspaces into memory.
4. Local Rust, SDK, and Python test entrypoints are green.

## What Ran

### Tests

- `cargo test --manifest-path rust/Cargo.toml --workspace --quiet`
- `cargo test --manifest-path rust/Cargo.toml -p lab-runner --quiet`
- `python3 -m pytest -q --tb=short`
- `npm test` in `sdk/`

All of the above passed on 2026-03-06.

### Security Checks

- `npm audit --json` in `sdk/`: `0` vulnerabilities (`0` low / `0` moderate / `0` high / `0` critical).
- Local first-party code scan for risky process and filesystem patterns:
  - External process launches are done via `Command::new(...)` argv construction rather than shell string concatenation.
  - The only first-party `unsafe` uses found are TTY/ioctl helpers in `lab-cli`, not runner execution paths.
  - Destructive filesystem calls (`remove_dir_all`, permission changes) are limited to runner-owned temp/workspace paths.

### Resource Audit

Highest-risk hot path audited:

- `lab-runner` persistent chain state workspace capture in `capture_workspace_object_ref(...)`.

Issue before this pass:

- The runner base64-packed the full workspace into a `workspace_bundle_v1` JSON blob with no explicit size ceiling.
- Large workspaces could cause runaway memory use during capture.

Change in this pass:

- Added `AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES`.
- Default limit: `268435456` bytes (`256 MiB`).
- The runner now errors before reading further once the workspace bundle crosses that limit.

Operational guidance:

1. For large-data tasks, prefer `isolate_per_trial` unless you explicitly need persistent chain state.
2. Keep generated datasets, caches, and artifacts out of the writable workspace when possible.
3. If a larger carry-forward workspace is intentional, raise `AGENTLAB_MAX_WORKSPACE_BUNDLE_BYTES` explicitly and monitor memory.

## Naming / API Notes

- Canonical DX authoring key is now `arg_map`.
- Compatibility alias remains accepted:
  - `bindings_to_args`
  - row field alias `binding`
- Normalized runner output currently carries both `key` and `binding` for transition safety.

## Residual Risks

1. The runner split refactor still has dead-code warnings. That is not a ship blocker, but it means cleanup work remains.
2. Rust/Python dependency CVE tooling (`cargo-audit`, `pip-audit`) was not installed locally during this pass, so dependency auditing for those ecosystems was limited to code-level review and test validation.
