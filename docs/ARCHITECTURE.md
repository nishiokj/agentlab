# System Architecture

Reference architecture for the AgentLab experiment runner.
All diagrams reflect the current codebase as of 2026-02-23.

---

## 1. Top-Level Orchestration

```
  experiment.yaml
        │
        ▼
  ┌───────────────────────────────────────────────────────────┐
  │                     Runner (lab-cli)                       │
  │                                                           │
  │  1. Parse YAML → resolved JSON                            │
  │  2. Load dataset (JSONL tasks)                            │
  │  3. Resolve variant plan + runtime profiles               │
  │  4. Preflight checks                                      │
  │  5. Build trial schedule (TrialSlot[])                    │
  │  6. Execute schedule engine                               │
  └───────────┬───────────────────────────────────────────────┘
              │
              ▼
  ┌───────────────────────────────────────────────────────────┐
  │                Schedule Engine (Parallel)                   │
  │                                                           │
  │  Dispatch loop:                                           │
  │    schedule[next_idx] → TrialDispatch → WorkerBackend     │
  │                                                           │
  │  ┌─────────────────┐     ┌──────────────────────┐        │
  │  │ LocalThread      │     │ Remote (HTTP)         │        │
  │  │ WorkerBackend    │     │ WorkerBackend         │        │
  │  │                 │     │                      │        │
  │  │ Thread pool     │     │ Submit → Poll        │        │
  │  │ max_in_flight   │     │ retry + backoff      │        │
  │  └────────┬────────┘     └──────────┬───────────┘        │
  │           │                         │                     │
  │           ▼                         ▼                     │
  │      TrialCompletion          TrialCompletion             │
  │           │                         │                     │
  │           └────────────┬────────────┘                     │
  │                        ▼                                  │
  │              DeterministicCommitter                        │
  │              (ordered fact commit)                         │
  └───────────┬───────────────────────────────────────────────┘
              │
              ▼
  ┌───────────────────────────────────────────────────────────┐
  │                   RunSink (JsonlRunSink)                    │
  │                                                           │
  │   facts/                                                  │
  │   ├── run_manifest.json      (RunManifestRecord)          │
  │   ├── trials.jsonl           (TrialRecord per trial)      │
  │   ├── metrics_long.jsonl     (MetricRow per metric)       │
  │   ├── events.jsonl           (EventRow per event)         │
  │   └── variant_snapshots.jsonl (VariantSnapshotRow)        │
  └───────────┬───────────────────────────────────────────────┘
              │
              ▼
  ┌───────────────────────────────────────────────────────────┐
  │                 Analysis (lab-analysis)                     │
  │                                                           │
  │   DuckDB views over facts/ JSONL:                         │
  │     trials, metrics_long, events, variant_snapshots       │
  │     variant_summary, task_variant_matrix, run_progress    │
  │                                                           │
  │   ViewSet selected by experiment design:                  │
  │     AbTest │ MultiVariant │ ParameterSweep │ Regression   │
  │                                                           │
  │   Project-level cross-run views:                          │
  │     all_trials, all_runs, pass_rate_trend                 │
  └───────────────────────────────────────────────────────────┘
```

---

## 2. System Boundaries

Five boundaries define the contract surfaces between components.

### Boundary 1: Experiment Definition → Runner

```
                  User
                   │
                   │  experiment.yaml (experiment_v1_0 schema)
                   │  + overrides (experiment_overrides_v1)
                   ▼
              ┌──────────┐
              │  Runner   │
              └──────────┘

  Direction: User → Runner (one-shot)

  Contract (experiment_v1_0):
    ├── experiment: { id, name }
    ├── dataset:    { path, limit? }
    ├── design:     { comparison, replications, seed? }
    ├── baseline:   { variant_id, args?, env?, image? }
    ├── variant_plan?: [{ variant_id, args?, image?, env? }]
    └── runtime:    { image, command, agent?, policy?, ... }

  Guarantees:
    - Schema-validated before any trial executes
    - Preflight checks abort before execution on fatal misconfig
    - Overrides merged before validation (experiment_overrides_v1)
```

### Boundary 2: Runner → Agent (Task Contract)

```
  ┌──────────┐                    ┌──────────────────────────┐
  │  Runner   │───────────────────▶│  Agent (inside sandbox)  │
  └──────────┘                    └──────────────────────────┘

  Direction: Runner → Agent (write), Agent → Runner (write result)

  Contract IN (agent_task_v1):
    ├── ids:          { run_id, trial_id, variant_id, task_id, repl_idx }
    ├── task:         { ... }    (benchmark-specific payload)
    ├── bindings:     { ... }    (variant knobs for the agent)
    ├── dependencies: { services? }
    └── policy:       { timeout_ms, network: { mode, allowed_hosts },
                        sandbox: { mode, image?, resources? } }

  Contract OUT (agent_result_v1):
    ├── ids:          (echo back)
    ├── outcome:      "success" | "failure" | "missing" | "error"
    ├── answer?:      raw agent answer
    ├── metrics?:     { metric_name: value }
    ├── objective?:   { name, value, direction? }
    ├── artifacts?:   [{ path, logical_name?, mime_type? }]
    ├── checkpoints?: [{ path, step?, epoch? }]
    └── error?:       { error_type?, message?, stack? }

  Delivery mechanism:
    Container mode:  Files at well-known paths inside /agentlab/contract/
    Local mode:      Files on host filesystem

  Guarantees:
    - Task payload is schema-validated (agent_task_v1)
    - Result is schema-validated (agent_result_v1)
    - Timeout enforced externally by runner
```

### Boundary 3: Trial Execution → Evidence Chain

```
  ┌──────────────────┐              ┌────────────────────┐
  │ Trial Execution   │─────────────▶│  Evidence Store     │
  └──────────────────┘              └────────────────────┘

  Direction: Executor → Evidence (append-only after trial)

  Contract (evidence_record_v1):
    ├── ids:       { run_id, trial_id, variant_id, task_id, repl_idx }
    ├── runtime:   { executor, exit_status, duration_ms? }
    ├── evidence:
    │   ├── trial_input_ref:      artifact://sha256/<hash>
    │   ├── trial_output_ref:     artifact://sha256/<hash>
    │   ├── workspace_pre_ref:    artifact://sha256/<hash>
    │   ├── workspace_post_ref:   artifact://sha256/<hash>
    │   ├── diff_incremental_ref: artifact://sha256/<hash>
    │   ├── diff_cumulative_ref:  artifact://sha256/<hash>
    │   ├── patch_incremental_ref: artifact://sha256/<hash>
    │   ├── patch_cumulative_ref: artifact://sha256/<hash>
    │   ├── stdout_ref?:          artifact://sha256/<hash>
    │   ├── stderr_ref?:          artifact://sha256/<hash>
    │   └── hook_events_ref?:     artifact://sha256/<hash>
    └── paths:     { trial_dir, trial_input, trial_output, ... }

  Guarantees:
    - Every ref is a content-addressed SHA-256 artifact
    - Evidence records are append-only (evidence_records.jsonl)
    - Executor type is recorded: local_docker | local_process | remote
```

### Boundary 4: Runner → Facts (RunSink)

```
  ┌──────────┐              ┌──────────────────┐
  │  Runner   │─────────────▶│  Facts (JSONL)    │
  └──────────┘              └──────────────────┘

  Direction: Runner → Disk (append-only per trial completion)

  Fact types (RunSink trait):
    write_run_manifest()    → facts/run_manifest.json
    append_trial_record()   → facts/trials.jsonl
    append_metric_rows()    → facts/metrics_long.jsonl
    append_event_rows()     → facts/events.jsonl
    append_variant_snapshot()→ facts/variant_snapshots.jsonl

  Guarantees:
    - DeterministicCommitter ensures facts commit in schedule order
    - Completion may arrive out-of-order from parallel workers;
      committer holds completions until all prior slots resolve
    - JSONL format: one JSON object per line, append-only
```

### Boundary 5: Facts → Analysis (DuckDB)

```
  ┌──────────────────┐              ┌────────────────────┐
  │  Facts (JSONL)    │─────────────▶│  DuckDB Analysis    │
  └──────────────────┘              └────────────────────┘

  Direction: Facts → DuckDB (materialized on query)

  Materialization:
    1. Create views over JSONL files (read_json_auto)
    2. Select ViewSet based on experiment design:
       ┌─────────────────────┬──────────────────────────┐
       │ Design               │ ViewSet                   │
       ├─────────────────────┼──────────────────────────┤
       │ paired, ≤2 variants │ AbTest (win_loss_tie)     │
       │ paired, ≥3 variants │ MultiVariant (ranking)    │
       │ unpaired             │ ParameterSweep (best_cfg) │
       │ comparison=none      │ Regression (trend)        │
       └─────────────────────┴──────────────────────────┘
    3. Load opinionated SQL view bundle for the ViewSet
    4. Fallback: in-memory DuckDB on lock contention

  Guarantees:
    - Read-only queries only (validated: no INSERT/UPDATE/DROP)
    - Views are re-materialized from source JSONL on each access
    - Project-level views union across all runs in .lab/runs/
```

---

## 3. Trial Lifecycle

```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                          Schedule Engine                                │
  │                                                                        │
  │  schedule[idx] ──▶ TrialSlot { variant_idx, task_idx, repl_idx }       │
  │                                                                        │
  │       │                                                                │
  │       ▼                                                                │
  │  ┌──────────────────────────────────────────┐                          │
  │  │           Build TrialDispatch             │                          │
  │  │                                          │                          │
  │  │  Resolve: variant, task, bindings,       │                          │
  │  │  policy, runtime profile, task boundary  │                          │
  │  │  (task_image, workspace from dataset)    │                          │
  │  └─────────────────┬────────────────────────┘                          │
  │                    │                                                   │
  │                    ▼                                                   │
  │  ┌──────────────────────────────────────────┐                          │
  │  │        WorkerBackend.submit()             │                          │
  │  │                                          │                          │
  │  │  Returns WorkerTicket                    │                          │
  │  │  { worker_id, ticket_id, trial_id }      │                          │
  │  └─────────────────┬────────────────────────┘                          │
  │                    │                                                   │
  │                    ▼                                                   │
  │  ┌──────────────────────────────────────────┐                          │
  │  │        Trial Execution (in worker)        │                          │
  │  │                                          │                          │
  │  │  1. Stage trial input (trial_input_v1)   │                          │
  │  │  2. Prepare sandbox (container/local)    │                          │
  │  │  3. Mount contract dirs, inject task     │                          │
  │  │  4. Run agent via adapter (command)      │                          │
  │  │  5. Collect result (agent_result_v1)     │                          │
  │  │  6. Snapshot workspace (pre/post)        │                          │
  │  │  7. Compute diffs + patches              │                          │
  │  │  8. Run benchmark grader (if configured) │                          │
  │  │  9. Build evidence record                │                          │
  │  └─────────────────┬────────────────────────┘                          │
  │                    │                                                   │
  │                    ▼                                                   │
  │  ┌──────────────────────────────────────────┐                          │
  │  │         TrialCompletion                   │                          │
  │  │                                          │                          │
  │  │  { ticket, schedule_idx,                 │                          │
  │  │    terminal_status, classification,      │                          │
  │  │    artifacts, metrics, runtime_summary } │                          │
  │  └─────────────────┬────────────────────────┘                          │
  │                    │                                                   │
  │                    ▼                                                   │
  │  ┌──────────────────────────────────────────┐                          │
  │  │      DeterministicCommitter               │                          │
  │  │                                          │                          │
  │  │  Hold out-of-order completions.          │                          │
  │  │  Commit facts only when all prior        │                          │
  │  │  schedule slots are resolved.            │                          │
  │  │                                          │                          │
  │  │  Commit:                                 │                          │
  │  │    → TrialRecord + MetricRows            │                          │
  │  │    → EventRows + VariantSnapshots        │                          │
  │  │    → EvidenceRecord                      │                          │
  │  │    → schedule_progress update            │                          │
  │  │    → run_control update                  │                          │
  │  └─────────────────────────────────────────┘                          │
  └────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Run State Machine

Two files work in tandem to track run state:

```
  runtime/
  ├── run_control.json       (run_control_v2)
  └── schedule_progress.json (schedule_progress_v1)
```

### run_control_v2 — Observable run status

```
  States:
    running ──▶ paused ──▶ running     (pause/resume cycle)
    running ──▶ interrupted             (ctrl-C / signal)
    running ──▶ killed                  (explicit kill)
    running ──▶ completed               (all slots done)
    running ──▶ failed                  (fatal error)

  State diagram:

                  ┌────────────┐
                  │  running    │◀──────────────────────┐
                  └──────┬─────┘                        │
                         │                              │
           ┌─────────────┼─────────────┐                │
           │             │             │                │
           ▼             ▼             ▼                │
    ┌───────────┐ ┌────────────┐ ┌──────────┐   ┌──────┴─────┐
    │ completed  │ │ interrupted │ │  killed   │   │   paused   │
    └───────────┘ └────────────┘ └──────────┘   └────────────┘
                                                (resume → running)

  Fields:
    status:         current state
    active_trials:  map<trial_id → { worker_id, schedule_idx,
                                      variant_id, started_at,
                                      control? }>
    pause:          { label, requested_at, requested_by? } | null
    updated_at:     ISO 8601 timestamp
```

### schedule_progress_v1 — Durable schedule cursor

```
  Fields:
    total_slots:          len(schedule)
    next_schedule_index:  cursor into schedule[] (next to dispatch)
    next_trial_index:     monotonic trial ID counter
    schedule:             [{ variant_idx, task_idx, repl_idx }]
    completed_slots:      [{ schedule_index, trial_id, status }]
                           status: "completed" | "failed" | "skipped_pruned"
    pruned_variants:      [variant_idx, ...]
    consecutive_failures: { variant_idx: count }
    use_container:        bool

  Interaction with run_control_v2:
    1. Dispatch reads next_schedule_index → builds TrialDispatch
    2. run_control writes active_trials for in-flight visibility
    3. On commit: completed_slots appended, next_schedule_index advanced
    4. On continue: schedule_progress is the source of truth for
       resuming from the exact slot where execution stopped
    5. On kill/interrupt: active_trials drained, schedule_progress
       records partial completion

  Progress lifecycle:

    ┌──────────────────────────────────────────────────────────┐
    │  next_schedule_index                                      │
    │       │                                                   │
    │       ▼                                                   │
    │  schedule: [ slot_0, slot_1, slot_2, ... slot_N ]         │
    │             ──────── ──────── ────────                     │
    │             committed in-flight  pending                   │
    │             ────────                                       │
    │             completed_slots[]                              │
    └──────────────────────────────────────────────────────────┘
```

---

## 5. Image Resolution Cascade

The container image used for a trial is resolved through a priority cascade:

```
  experiment.yaml
  ├── runtime.agent.image_source = "global" (default)
  │   │
  │   └──▶ Use runtime.agent.image (or runtime.image for clean contract)
  │        │
  │        │  Variant-level override?
  │        │  baseline.image / variant_plan[].image
  │        │  (variant image overrides global when specified)
  │        │
  │        └──▶ Final image for this variant
  │
  └── runtime.agent.image_source = "per_task"
      │
      └──▶ Each task in the dataset provides its own image:
           task.image (from JSONL row)
           │
           │  Requirements:
           │  ├── runtime.agent.artifact is REQUIRED
           │  │   (agent code injected into per-task image)
           │  ├── Container mode only (local mode rejected)
           │  └── Preflight scans all tasks for valid images
           │
           └──▶ task.workspace? (optional per-task workspace path)

  Resolution at execution time (resolve_container_image):

    ┌──────────────────────────────────────────┐
    │  image_source == PerTask?                 │
    │     YES → use task.image from dataset row │
    │     NO  → use runtime.agent.image         │
    └──────────────────────────────────────────┘

  Workspace resolution (resolve_container_workspace):

    ┌──────────────────────────────────────────────────────┐
    │  image_source == PerTask && task.workspace present?   │
    │     YES → use task.workspace                         │
    │     NO  → default /agentlab/contract/workspace/      │
    │           (or none for clean_contract_v1)            │
    └──────────────────────────────────────────────────────┘
```

---

## 6. Directory Layout (Per Run)

```
  .lab/runs/<run_id>/
  ├── manifest.json                  (manifest_v1)
  ├── resolved_experiment.json       (resolved experiment)
  ├── resolved_experiment.digest     (canonical JSON digest)
  ├── runtime/
  │   ├── run_control.json           (run_control_v2)
  │   └── schedule_progress.json     (schedule_progress_v1)
  ├── trials/
  │   └── <trial_id>/               (per-trial directory)
  │       ├── trial_input.json       (trial_input_v1)
  │       ├── result.json            (agent_result_v1)
  │       ├── stdout.log
  │       ├── stderr.log
  │       └── workspace/             (snapshot)
  ├── evidence/
  │   ├── evidence_records.jsonl     (evidence_record_v1[])
  │   └── task_chain_states.jsonl
  ├── facts/
  │   ├── run_manifest.json          (RunManifestRecord)
  │   ├── trials.jsonl               (TrialRecord[])
  │   ├── metrics_long.jsonl         (MetricRow[])
  │   ├── events.jsonl               (EventRow[])
  │   └── variant_snapshots.jsonl    (VariantSnapshotRow[])
  ├── analysis/
  │   ├── agentlab.duckdb            (materialized views)
  │   └── load_duckdb.sql            (reproducible SQL)
  └── benchmark/                     (if benchmark grading enabled)
      ├── predictions.jsonl
      └── scores.jsonl
```
