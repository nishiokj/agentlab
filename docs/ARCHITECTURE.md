# System Architecture

Current runner architecture as of 2026-03-18.

The authoring contract summary lives in [README.md](/Users/jevinnishioka/Desktop/Experiments/README.md). This document is the implementation-side map for contributors and operators.

## Ownership Map

`lab-cli` is the operator surface. It parses commands, loads packages or experiments, and calls into `lab-runner`.

`lab-runner` is split by runtime ownership:

| Area | Current owner | Responsibility |
|---|---|---|
| Config and authoring normalization | `rust/crates/lab-runner/src/config.rs` | Parse experiment state, resolve runtime profiles, validate authoring-time constraints |
| Run entrypoints and control operations | `rust/crates/lab-runner/src/runner.rs` | `run`, `continue`, `recover`, `pause`, `resume`, `kill`, and durable control reconciliation |
| Schedule engine and slot commit | `rust/crates/lab-runner/src/core.rs`, `rust/crates/lab-runner/src/lifecycle.rs` | Expand schedules, coordinate execution, and publish slot facts exactly once |
| Trial orchestration | `rust/crates/lab-runner/src/trial/schedule.rs` | Prepare one scheduled trial, call the runtime, and map runtime outputs into committed trial records |
| Trial runtime | `rust/crates/lab-runner/src/trial/execution.rs` | Materialize task/grader sandboxes, run agent and grader commands, persist runtime state transitions |
| Durable per-trial runtime state | `rust/crates/lab-runner/src/trial/state.rs` | `trial_runtime_state.json`, phase reconciliation, persisted container identity |
| Docker transport | `rust/crates/lab-runner/src/backend/docker.rs` | Image ensure, container create/start/exec/wait/inspect/pause/unpause/remove |
| Durable rows and stores | `rust/crates/lab-runner/src/persistence/` | Row contracts, JSON-row routing, SQLite ingestion, and run-sink implementations |

The important boundary is that production trial execution now flows through `trial::schedule` and `trial::execution`, while Docker transport stays inside `backend/docker` and durable row ownership stays inside `persistence/`.

## Primary Local Path

The shipped local path is:

1. `lab run <package-or-experiment>` resolves the package and creates a run directory.
2. The runner persists `run_control_v2`, `run_session_state_v1`, and `schedule_progress_v2` before scheduling work.
3. The schedule engine dispatches one slot at a time through `trial::schedule::execute_scheduled_trial`.
4. `trial::execution::execute_trial_runtime` creates the task container, copies task contents into the container-owned workdir, runs the agent contract, then runs grading if the benchmark requires it.
5. The runtime persists `trial_runtime_state.json` as the trial advances through materialization, agent, grading, mapping, `commit_pending`, and terminal reconciliation.
6. The committer publishes slot results exactly once into SQLite-backed run facts and advances durable schedule progress.

The primary path does not shell out to `docker`. Production container control is owned by the Docker runtime abstraction in `backend/docker`.

## Lifecycle Vocabulary

Run-level status is persisted in `run_control_v2` and uses:

- `running`
- `paused`
- `interrupted`
- `completed`
- `failed`
- `killed`

Trial-level runtime phase is persisted in `trial_runtime_state.json` and uses:

- `agent_materializing`
- `agent_running`
- `agent_finished`
- `grader_materializing`
- `grader_running`
- `grader_mapping`
- `commit_pending`
- `paused`
- `committed`
- `killed`
- `abandoned`

The runtime phase is the durable source of truth for in-flight control and recovery decisions. `attempt_no` is retry metadata, not a second lifecycle model.

## Recovery And Control Plane

`recover` is the crash-reconciliation operation. It reads persisted run control, schedule progress, committed slot facts, and `trial_runtime_state.json`, then releases or reconciles active trials into a safe continuable state. Recovery writes a report under `runtime/recovery_report.json` and moves the run to `interrupted` unless it was already terminal.

`continue` resumes only from `failed`, `paused`, or `interrupted`. It reloads persisted run behavior and execution options, verifies the schedule matches the stored run state, and resumes from the next durable schedule slot. A still-`running` run must be reconciled with `recover` first.

`pause`, `resume`, and `kill` operate on the same control plane:

- If persisted runtime container ids exist, control uses Docker runtime operations first.
- `pause` pauses the recorded container set and persists trial/run state as paused.
- `resume` unpauses persisted runtime containers when the paused trial still has live runtime state; otherwise it falls back to checkpoint-fork resume semantics.
- `kill` is the terminal cancel operation for a run. It removes persisted runtime containers, marks the affected trials killed, and persists a truthful interrupted-or-killed run state if the operation only partially succeeds.

There is no separate production worker control plane for the primary local Docker path. Legacy adapter-control handling remains only as a compatibility shim when durable runtime state does not exist.

## Grading Boundary

The agent contract produces a candidate artifact. That is not the benchmark verdict.

Benchmark verdicts come only from a validated `trial_conclusion_v1` in `mapped_grader_output.json`:

- Direct grading writes `mapped_grader_output.json` directly.
- Mapper grading writes raw grader output first, then a mapper writes `mapped_grader_output.json`.
- If the mapped output is missing or invalid, the committed trial outcome becomes `grading_failed`.

Hidden grader assets stay outside the agent-visible tree during the agent step. In-task-image grading reveals hidden paths only for grading execution.

## Persistence And Exactly-Once Commit

Run facts are written through `persistence::RunSink` implementations into SQLite-backed JSON rows. The main persisted tables are trial rows, metric rows, event rows, variant snapshot rows, benchmark conclusion rows, and runtime key-value records.

Exactly-once applies to slot publication, not trial attempts:

- a trial may retry with a higher `attempt`
- a schedule slot publishes one committed result set
- pending completions survive restart and drain in schedule order

If a crash happens after grading but before slot publication, the retry/recovery logic uses durable runtime state plus committed artifacts to avoid fabricating duplicate slot commits.

## Contributor Guidance

When changing the runner:

1. Put new Docker operations in `backend/docker`, not in orchestration code.
2. Put new durable row shapes or ingest rules in `persistence/`, not in `io.rs` or schedule code.
3. Put trial-step behavior in `trial/`, and keep `runner.rs` focused on run-level orchestration and operator commands.
4. Treat `mapped_grader_output.json` and committed slot facts as the correctness boundary for benchmark outcomes.
5. Keep recovery and control decisions derivable from persisted records alone.
