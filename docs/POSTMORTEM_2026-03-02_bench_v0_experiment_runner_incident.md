# Postmortem: Bench v0 Experiment Runner Incident (2026-03-02)

## Incident Metadata
- Date: 2026-03-02
- Primary run id: `run_20260302_160821`
- Final state: `completed` with `40/40` committed slots
- Severity: High (blocked end-to-end experiment execution, repeated operator intervention required)
- Systems involved:
  - Host orchestration script (`scripts/run-bench-experiment.sh`)
  - Experiment config (`.lab/experiments/bench_v0_glm5_vs_codex_spark.yaml`)
  - Rust runner (`crates/lab-runner/src/lib.rs`)
  - Local Docker daemon
  - Benchmark adapter contract output

## Executive Summary
The experiment pipeline did eventually complete end-to-end, but only after multiple failures across both **runner runtime behavior** and **host/experiment wiring**. This was not one bug; it was a stack of failures:

1. Runner/container staging and workspace evidence traversal behaviors created false hangs and brittle execution.
2. Experiment wiring selected unsupported model/account combinations in earlier attempts and initially did not propagate timeout control into the agent command.
3. Disk exhaustion caused late-stage local worker failure (`No space left on device`) and destabilized Docker.
4. Recovery/continue path was fragile to missing env/path context.
5. Benchmark adapter output shape was contract-incomplete, so all committed trials were graded as errors.

The key process failure was that tests did not exercise realistic end-to-end failure modes under production-like conditions (container filesystem semantics, large workspace state, partial failure + recover/continue).

## Customer/Operator Impact
- Time-to-completion was materially extended.
- Multiple manual interventions were required (patches, rebuilds, recover/continue, disk cleanup, Docker restart).
- Observability gap in scoreboard caused delayed diagnosis during failure states.
- Final run completed but quality signal was unusable: all 40 trials were error outcomes due score schema contract mismatch.

## Hard Evidence Snapshot
From run artifacts for `run_20260302_160821`:
- Created: `2026-03-02T16:08:21.247084+00:00` (`manifest.json`)
- Completed: `2026-03-02T16:36:49.960051+00:00` (`runtime/run_control.json`)
- Slot progress: `40/40` committed (`runtime/schedule_progress.json`)
- Recovery event:
  - `recovered_at`: `2026-03-02T16:34:32.821609+00:00`
  - `rewound_to_schedule_idx`: `35`
  - `active_trials_released`: `2`
  - (`runtime/recovery_report.json`)
- Facts rows: `40` (`facts/trials.jsonl`)
- Outcomes: all `error` (`facts/trials.jsonl`)

Representative terminal error near completion:
- `local worker trial execution failed ... No space left on device (os error 28)`

Representative result timeout errors in trial artifacts:
- `Timed out waiting for response for request_id=...` (around `~30.6s` latency in `trials/trial_*/result.json`)

Representative grade/contract failure in facts rows:
- `score_record_invalid ... "schedule_idx" is a required property; "slot_commit_id" is a required property; "attempt" is a required property; "row_seq" is a required property`

## Detailed Timeline (UTC)

### Phase 1: Main run execution
- **16:08:21** Run `run_20260302_160821` created.
- **16:08:30** First slots activated.
- **16:08:58 -> 16:21:37** Run progressed steadily to `35/40` committed.
- **16:21:37** Local worker failure at schedule index 35 due `No space left on device`.

### Phase 2: Recovery and continuation attempts
- Disk headroom discovered critically low.
- Old run artifacts cleaned to reclaim space.
- **16:34:32** `lab-cli recover` rewound to slot 35 and released active trials.
- Continue attempts encountered transient issues:
  - Missing env var context when invoked directly (`ZAI_CODER_API_KEY` not present unless `.env` sourced).
  - Docker daemon became unresponsive after disk pressure and required restart.
- Continue succeeded after Docker restart + recovery + env context.

### Phase 3: Completion
- **16:35:25** Progress resumed (`37/40`).
- **16:36:10** Progress reached `39/40`.
- **16:36:49** Run reached `completed` with `40/40` committed.

## Root Cause Analysis (by layer)

### RC-1 (Rust runner): brittle artifact staging/unpack in task containers
Category: System bug
- The artifact staging/unpack path required hardening for container filesystem semantics and ownership handling.
- Incident patch moved staging to `/opt/agent.tar.gz`, created `/opt/agent`, and unpacked with `--no-same-owner --no-same-permissions`.
- File: `crates/lab-runner/src/lib.rs` around container setup/unpack.

Why tests missed it:
- No integration test against constrained container filesystems/capabilities that reproduces staging/unpack behavior in realistic task images.

### RC-2 (Rust runner): workspace evidence and copy traversal too expensive
Category: System bug
- Evidence snapshot/copy traversed very large trees (including nested dependency trees and AppleDouble files), causing perceived hangs and delayed commits.
- Incident patch introduced stronger exclusions for `node_modules`, caches, `target`, `._*`, etc., and applied exclusions to both snapshot and copy paths.
- File: `crates/lab-runner/src/lib.rs` (`is_workspace_evidence_excluded`, `copy_dir_filtered`).

Why tests missed it:
- No stress test with large dependency-heavy workspace trees and macOS metadata artifacts.
- No performance guardrail assertions for pre/post evidence phases.

### RC-3 (Host/experiment): model binding mismatch in earlier attempts
Category: Configuration/wiring bug
- Earlier runs used model/account combinations that were not supported by the active auth context, producing immediate runtime provider errors.
- Fixed by setting baseline/variant to valid providers/models for this environment.
- File: `.lab/experiments/bench_v0_glm5_vs_codex_spark.yaml`.

Why tests missed it:
- No preflight compatibility check that validates selected models against active auth/account capabilities.

### RC-4 (Host/experiment): timeout policy not initially wired into runtime command
Category: Configuration/wiring bug
- `runtime.policy.timeout_ms` override was set but not forwarded to `rex run` without explicit `--timeout-ms ${AGENTLAB_TIMEOUT_MS}` in command args.
- File: `.lab/experiments/bench_v0_glm5_vs_codex_spark.yaml`.

Why tests missed it:
- No contract test that asserts `runtime.policy.timeout_ms` materially changes observed trial runtime behavior.

### RC-5 (Host adapter): benchmark score schema mismatch
Category: Contract bug
- Standalone adapter emitted score payload missing required schema fields (`schedule_idx`, `slot_commit_id`, `attempt`, `row_seq`), so all trials graded as error.
- File: `.lab/experiments/bench_benchmark_adapter_standalone.py`.

Why tests missed it:
- No schema conformance test for adapter output in CI against benchmark score JSON schema.

### RC-6 (Host operations): disk exhaustion and Docker instability
Category: Environment/operational fault
- Disk reached near full; local worker failed with `os error 28`.
- Docker daemon then became unstable/unresponsive and required restart.

Why tests missed it:
- No pre-run disk headroom gate.
- No chaos test for recoverability under disk pressure / daemon restart.

### RC-7 (Recovery path): continue fragility to invocation context
Category: Resilience/usability bug
- `continue` behavior depended on host env context and dataset path resolution conditions; manual fixes were required in incident flow.
- Recovery itself worked (correctly rewound and released active trials), but continue ergonomics were brittle.

Why tests missed it:
- No end-to-end recover->continue tests that simulate interrupted run with minimal shell environment.

## What Changed During Incident

### Host-side changes
1. Improved preflight and scoreboard observability, plus run overrides.
   - File: `scripts/run-bench-experiment.sh`
   - Added:
     - Docker daemon validation in preflight.
     - `--max-concurrency` and `--timeout-ms` run overrides.
     - Better scoreboard error surfacing from `result.json`/stderr even when trial state is stale.
     - Detection of `active_trials_with_result_files` (stuck finalization signal).

2. Updated experiment runtime and bindings.
   - File: `.lab/experiments/bench_v0_glm5_vs_codex_spark.yaml`
   - Changes included:
     - baseline: `z.ai-coder/glm-5`
     - variant: `codex/gpt-5.3-codex-spark`
     - pass timeout to runtime command with `--timeout-ms ${AGENTLAB_TIMEOUT_MS}`

### Rust-side changes
1. Hardened artifact unpack flow in containers.
2. Excluded heavy/non-actionable files from evidence snapshot and workspace copy traversal.
3. Rebuilt `lab-cli` and reran.

## Why This Was Not Caught Earlier (Test-Suite Gaps)

### Critical missing test classes
1. **Container FS behavior test**
- Validate artifact staging/unpack under sandboxed container filesystems with restricted ownership changes.

2. **Large workspace evidence performance test**
- Enforce upper-bound runtime for pre/post snapshot/copy on dependency-heavy repositories.

3. **Policy propagation test**
- Verify `runtime.policy.timeout_ms` is passed and enforced by runtime command path.

4. **Model capability preflight test**
- Validate selected model/provider against available auth/account capabilities before run starts.

5. **Adapter schema compliance test**
- CI test that emits prediction/score through adapter and validates against JSON schema.

6. **Recover/continue resilience test**
- Simulate interruption at high schedule index and assert continue works in clean shell env.

7. **Disk headroom gate test**
- Enforce minimum free space threshold; abort early if below threshold.

8. **Scoreboard stale-state test**
- Assert error surfacing when `result.json` is terminal but `trial_state` remains stale `running`.

## Corrective and Preventative Actions

### Immediate (P0)
1. Add disk headroom preflight gate to experiment runner script and runner CLI.
2. Add adapter schema conformance CI test; fail build on schema mismatch.
3. Add model capability preflight check (auth-aware).
4. Add runtime command conformance test for timeout propagation.

### Near-term (P1)
1. Add integration test for artifact staging/unpack in realistic constrained container.
2. Add evidence traversal performance benchmark test with thresholds.
3. Add recover/continue smoke suite (with env injection and relative/absolute dataset path permutations).
4. Add Docker health sanity checks before scheduling next slot.

### Mid-term (P2)
1. Formalize incident runbook (disk pressure, daemon restart, recover/continue sequence).
2. Add automatic garbage collection policy for stale run artifacts.
3. Add richer run-progress telemetry for “active but blocked” states.

## Ownership Split (Answer to “Rust packages or host?”)
Both, with different severity and function:
- **Rust package/system bugs** (runner): RC-1, RC-2 were core runtime reliability issues.
- **Host/experiment wiring bugs**: RC-3, RC-4, RC-5 created invalid or low-quality experiment conditions.
- **Operational environment**: RC-6 amplified failures and forced recovery.

This incident should be treated as a **cross-layer reliability failure**, not a single-team defect.

## Current Known Residual Risk
Even though the run reached `40/40`, score records remain contract-invalid in this run due adapter schema omissions. Operationally complete does not mean analytically valid benchmark outcomes.

## Appendix: Key Artifact Paths
- `/Users/jevinnishioka/Desktop/jesus/.lab/runs/run_20260302_160821/manifest.json`
- `/Users/jevinnishioka/Desktop/jesus/.lab/runs/run_20260302_160821/runtime/run_control.json`
- `/Users/jevinnishioka/Desktop/jesus/.lab/runs/run_20260302_160821/runtime/schedule_progress.json`
- `/Users/jevinnishioka/Desktop/jesus/.lab/runs/run_20260302_160821/runtime/recovery_report.json`
- `/Users/jevinnishioka/Desktop/jesus/.lab/runs/run_20260302_160821/facts/trials.jsonl`
- `/Users/jevinnishioka/Desktop/jesus/scripts/run-bench-experiment.sh`
- `/Users/jevinnishioka/Desktop/jesus/.lab/experiments/bench_v0_glm5_vs_codex_spark.yaml`
- `/Users/jevinnishioka/Desktop/Experiments/rust/crates/lab-runner/src/lib.rs`
- `/Users/jevinnishioka/Desktop/jesus/.lab/experiments/bench_benchmark_adapter_standalone.py`
