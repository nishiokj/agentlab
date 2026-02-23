# P7 Review Findings: Parallel Worker Hard Cutover

Date: 2026-02-22  
Scope: `docs/PATCH_SPEC_PARALLEL_WORKER_HARD_CUTOVER.md` (P7 review)  
Focus: dead code, ownership boundaries, potential gaps/bugs

## Status Update (2026-02-23)

- Findings `#3` and `#4` are now **resolved** in `rust/crates/lab-runner/src/lib.rs`.
- Findings `#1`, `#2`, `#5`, `#6`, and `#7` remain open.

## Findings (ordered by severity)

1. **Critical: pause boundary is disconnected from the parallel worker engine**
   - `active_trials` are persisted with `control: None` in the scheduler:
     - `rust/crates/lab-runner/src/lib.rs:5155`
     - `rust/crates/lab-runner/src/lib.rs:5166`
   - `pause_run` requires per-trial `control` and fails with `pause_missing_active_adapter`:
     - `rust/crates/lab-runner/src/lib.rs:3132`
     - `rust/crates/lab-runner/src/lib.rs:3137`
   - On failure, pause writes run status `interrupted`:
     - `rust/crates/lab-runner/src/lib.rs:3301`
   - `WorkerBackend::request_pause/request_stop` exists but is not used by production pause flow:
     - `rust/crates/lab-runner/src/lib.rs:231`

2. **High: `kill_run` is metadata-only; it does not stop active workers**
   - `kill_run` rewrites trial/run state only:
     - `rust/crates/lab-runner/src/lib.rs:3344`
     - `rust/crates/lab-runner/src/lib.rs:3359`
   - No routing through backend stop control.
   - Meanwhile the live engine continues writing `running` updates and can still complete:
     - `rust/crates/lab-runner/src/lib.rs:5499`
     - `rust/crates/lab-runner/src/lib.rs:5877`

3. **High: shared mutable path race in worker execution (isolation breach) — Resolved (2026-02-23)**
   - Chain root seed workspace is now trial-scoped (no shared mutable `chain_root_workspace` path):
     - `rust/crates/lab-runner/src/lib.rs:4758`
     - `rust/crates/lab-runner/src/lib.rs:9084`
   - Added regression test:
     - `rust/crates/lab-runner/src/lib.rs:14004` (`p7_chain_root_workspace_is_trial_scoped`)

4. **High: commit durability ordering can lose sink rows after crash — Resolved (2026-02-23)**
   - Commit ordering now flushes sink rows before writing `schedule_progress`:
     - `rust/crates/lab-runner/src/lib.rs:5197`
     - `rust/crates/lab-runner/src/lib.rs:5218`
   - In-memory progress/pruning/failure counters are only advanced after persistence succeeds.
   - Added regression test:
     - `rust/crates/lab-runner/src/lib.rs:14019` (`p7_commit_trial_slot_does_not_advance_progress_when_flush_fails`)

5. **Medium: continue/recovery is weaker than spec orphan reconciliation**
   - `continue_run` rejects runs still marked `running`:
     - `rust/crates/lab-runner/src/lib.rs:2351`
   - Recovered active trials are immediately converted to `worker_lost` without liveness reattach:
     - `rust/crates/lab-runner/src/lib.rs:5384`
     - `rust/crates/lab-runner/src/lib.rs:5402`

6. **Medium: remote dispatch contract fields are stubbed out**
   - `TrialDispatch` carries `runtime_profile` and `effective_policy`, but scheduler sends `{}`:
     - `rust/crates/lab-runner/src/lib.rs:5474`
     - `rust/crates/lab-runner/src/lib.rs:5476`
   - This is a functional gap for real remote workers.

7. **Low: dead code / unfinished surface**
   - `require_chain_lease` is parsed but not enforced in dispatch gating:
     - parse: `rust/crates/lab-runner/src/lib.rs:6160`
     - dispatch gating checks only per-variant cap: `rust/crates/lab-runner/src/lib.rs:5446`
   - `cargo check` warns dead constants for remote pause/stop schema/path:
     - `rust/crates/lab-runner/src/lib.rs:717`

## Validation Run

- `cargo test -p lab-runner p5a_ -- --nocapture` (pass)
- `cargo test -p lab-runner p5b_ -- --nocapture` (pass)
- `cargo test -p lab-runner p7_ -- --nocapture` (pass)
- `cargo check -p lab-runner` (pass, with dead-code warnings noted above)
