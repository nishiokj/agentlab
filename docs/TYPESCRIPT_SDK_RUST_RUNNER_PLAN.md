# TypeScript SDK Plan (Rust Runner Backend)

Date: 2026-02-10

## Goal

Build a user-facing TypeScript SDK that uses the Rust runner as the execution engine, and make the CLI a thin wrapper over that SDK.

## Architecture

1. Keep Rust as the execution core (`lab-runner`) and expose a stable machine API contract.
2. Build a TypeScript package (`@agentlab/sdk`) that calls Rust through a JSON-based interface.
3. Make CLI (`@agentlab/cli`) consume SDK methods instead of implementing orchestration logic directly.

## Why This Fits the Current Codebase

1. Rust already exposes reusable library functions in `rust/crates/lab-runner/src/lib.rs`.
2. Current Rust CLI output is human-oriented text in `rust/crates/lab-cli/src/main.rs`; TypeScript needs a stable machine-readable mode.

## Phased Implementation

## 1) Rust Machine Contract (Blocking)

- Add `--format json` (or `--json`) to:
  - `describe`
  - `run`
  - `run-dev`
  - `run-experiment`
  - `publish`
  - `knobs-validate`
- JSON mode emits one stable JSON object and no human text.
- Add typed Rust response structs (`Serialize`) for outputs (`RunResult`, `ExperimentSummary`, etc.).

Acceptance:
- TypeScript can call commands without brittle stdout parsing.

## 2) Rust API Command Set

- Option A: Add `lab api <op>` commands.
- Option B: Keep existing commands + `--json`.
- Standardize JSON error payload:
  - `{ "code": "...", "message": "...", "details": ... }`

Acceptance:
- Every operation has a documented JSON request/response shape.

## 3) TypeScript SDK Core (`sdk`)

- Implement `LabClient`:
  - `describe()`
  - `run()`
  - `runDev()`
  - `runExperiment()`
  - `publish()`
  - `validateHooks()`
  - `validateSchema()`
- Implement ergonomic authoring models:
  - `ExperimentBuilder`
  - `VariantPlan`
  - `AnalysisPlan`
- Emit config compatible with Rust runner (`experiment.yaml`/resolved semantics).

Acceptance:
- SDK can execute a full smoke run against Rust binary end-to-end.

## 4) Progress Streaming

- Add Rust progress mode (e.g., `--progress-jsonl`).
- TS SDK exposes streaming API (`runStream(): AsyncIterable<RunEvent>`).

Acceptance:
- CLI/UI can display progress without scraping text logs.

## 5) CLI Migration

- Build `@agentlab/cli` as a thin SDK wrapper.
- Ensure command handlers call SDK APIs, not ad-hoc process logic.

Acceptance:
- CLI behavior is driven by SDK and covered by SDK integration tests.

## 6) Packaging and Distribution

- Provide Rust binary distribution strategy for npm:
  - bundled per-platform binaries, or
  - install-on-first-run downloader.
- Add runner discovery:
  - `AGENTLAB_RUNNER_BIN`
  - explicit SDK option `runnerBin`.

Acceptance:
- `npm install @agentlab/sdk @agentlab/cli` works on supported macOS/Linux targets in CI.

## 7) Contract + Regression Testing

- Golden tests for Rust JSON outputs.
- TS integration tests with fixture harness repos.
- Contract tests for:
  - `variant_plan`
  - overrides
  - network mode strictness
  - grades/evidence behavior

Acceptance:
- CI blocks machine-contract breaking changes.

## Proposed TypeScript API Shape

```ts
const client = new LabClient({ runnerBin: "lab" });

const exp = Experiment.builder("exp1", "My Experiment")
  .datasetJsonl("tasks.jsonl", { limit: 50 })
  .harnessCli(["node", "./harness.js", "run"], { integrationLevel: "cli_events" })
  .baseline("base", { model: "a" })
  .addVariant("treatment", { model: "b" })
  .build();

await client.validate(exp);
const run = await client.run(exp);
```

## Recommended Execution Order

1. Rust JSON machine mode.
2. TypeScript SDK command wrapper + builders.
3. TypeScript CLI wrapper migration.
4. Progress streaming + UX polish.

## Implementation Status (2026-02-10)

Implemented in this repository:

1. Rust JSON command mode:
   - Added `--json` for `describe`, `run`, `run-dev`, `run-experiment`, `publish`, `knobs-validate`, `schema-validate`, `hooks-validate`.
   - JSON success envelopes include `ok: true`, `command`, and command-specific payload.
   - JSON error envelope is standardized as:
     - `{ "ok": false, "error": { "code": "command_failed", "message": "...", "details": {} } }`

2. TypeScript SDK package:
   - Added `sdk/` (`@agentlab/sdk`) with:
     - `LabClient`
     - `ExperimentBuilder`
     - typed responses and `LabRunnerError`
   - `LabClient` uses Rust CLI JSON mode only (no stdout scraping).
   - Runner discovery order:
     - `runnerBin` option
     - `AGENTLAB_RUNNER_BIN`
     - default `lab`

3. SDK test coverage:
   - Added Node tests for success envelope parsing and typed error handling.

Not implemented yet:

1. `lab api <op>` dedicated API command family.
2. Progress streaming (`--progress-jsonl` + `runStream()`).
3. TypeScript CLI package migration (`@agentlab/cli`) as thin SDK wrapper.
4. CI matrix for npm binary distribution across platforms.
