# Trial Execution Flow — Function-Level Diagram

Legend:

    ┌──────────┐
    │  MODULE   │  boundary / ownership scope
    └──────────┘
    ──────────►    sequential call
    - - - - - ►   cross-thread / async boundary
    ⟳              retry loop
    ☠              Drop (RAII destructor)
    &              borrow
    ⤳              move (ownership transfer)

    [OWNED]        value is owned
    [&ref]         value is borrowed
    [&mut ref]     value is mutably borrowed
    [Arc]          shared via Arc
    [RAII]         cleaned up on Drop

---

## Phase 1 — Entry & Experiment Loading

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║  lib.rs — Public API                                                           ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                ║
║  ┌───────────────────────────────────────────────────────────────┐              ║
║  │  run_experiment_with_options(path: &Path, opts: RunExecOpts)  │              ║
║  └──────────────────────────────┬────────────────────────────────┘              ║
║                                 │                                              ║
║  ┌───────────────────────────────────────────────────────────────┐              ║
║  │  continue_run_with_options(run_dir: &Path, opts: RunExecOpts) │  (alt entry) ║
║  └──────────────────────────────┬────────────────────────────────┘              ║
║                                 │                                              ║
╚═════════════════════════════════╪══════════════════════════════════════════════╝
                                  │
                                  ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║  experiment/runner.rs — Experiment Loading                                      ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ load_sealed_package_for_run(path: &Path)                │                   ║
║  │ → LoadedExperimentInput {                               │                   ║
║  │     json_value: Value,    [OWNED]                       │                   ║
║  │     exp_dir: PathBuf,     [OWNED]                       │                   ║
║  │     project_root: PathBuf [OWNED]                       │                   ║
║  │   }                                                     │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            ▼                                                   ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ validate_required_fields(json: &Value)                  │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            ▼                                                   ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ experiment_workload_type(json: &Value) → &str           │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            ▼                                                   ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ normalize_execution_options(&RunExecOpts) → RunExecOpts │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            ▼                                                   ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ create_unique_run_dir(&Path)                            │                   ║
║  │ → (run_id: String, run_dir: PathBuf)  [OWNED]          │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            ▼                                                   ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ write_run_control_v2(&Path, &str, "running", &[], None) │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            ▼                                                   ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ write_run_session_state(&Path, &str, &Behavior, &Opts)  │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            ▼                                                   ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ ☠ RunControlGuard::new(&Path, &str)                     │                   ║
║  │   [RAII] auto-writes final status on Drop               │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            ▼                                                   ║
║  ┌─────────────────────────────────────────────────────────┐                   ║
║  │ ☠ start_engine_lease_heartbeat(&Path, &str)             │                   ║
║  │   → LeaseGuard  [RAII] heartbeat stops on Drop          │                   ║
║  └─────────────────────────┬───────────────────────────────┘                   ║
║                            │                                                   ║
╚════════════════════════════╪═══════════════════════════════════════════════════╝
                             │
                             ▼
```

---

## Phase 2 — Resolution & Preflight

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║  experiment/runner.rs — Resolution & Preflight                                  ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                ║
║  ┌──────────────────────────────────────────────────────────────┐              ║
║  │ resolve_dataset_path_in_package(json: &Value, dir: &Path)    │              ║
║  │ → PathBuf [OWNED]                                            │              ║
║  └──────────────────────────────┬───────────────────────────────┘              ║
║                                 ▼                                              ║
║  ┌──────────────────────────────────────────────────────────────┐              ║
║  │ load_tasks(path: &Path, json: &Value)                        │              ║
║  │ → Vec<Value> [OWNED]   (JSONL rows parsed to owned Values)   │              ║
║  └──────────────────────────────┬───────────────────────────────┘              ║
║                                 ▼                                              ║
║  ┌──────────────────────────────────────────────────────────────┐              ║
║  │ resolve_variant_plan(json: &Value)                           │              ║
║  │ → (Vec<Variant>, String baseline_id)  [OWNED]                │              ║
║  └──────────────────────────────┬───────────────────────────────┘              ║
║                                 ▼                                              ║
║  ┌──────────────────────────────────────────────────────────────┐              ║
║  │ write_resolved_variants(&Path, &Value, &str, &[Variant])     │              ║
║  └──────────────────────────────┬───────────────────────────────┘              ║
║                                 ▼                                              ║
║  ┌──────────────────────────────────────────────────────────────┐              ║
║  │ build_trial_schedule(                                        │              ║
║  │   variant_count, task_count, replications,                   │              ║
║  │   policy: SchedulingPolicy, seed: Option<u64>                │              ║
║  │ ) → Vec<TrialSlot { variant_idx, task_idx, repl_idx }> [OWN] │              ║
║  └──────────────────────────────┬───────────────────────────────┘              ║
║                                 ▼                                              ║
║  ┌──────────────────────────────────────────────────────────────┐              ║
║  │ parse_benchmark_config(json: &Value) → BenchmarkConfig [OWN]  │              ║
║  └──────────────────────────────┬───────────────────────────────┘              ║
║                                 ▼                                              ║
║  ┌──────────────────────────────────────────────────────────────┐              ║
║  │ for variant in &variants:                                     │              ║
║  │   resolve_variant_runtime_profile(                            │              ║
║  │     &Value, &Variant, &Path, &RunBehavior, &RunExecOpts       │              ║
║  │   ) → VariantRuntimeProfile [OWNED, pushed to Vec]            │              ║
║  └──────────────────────────────┬───────────────────────────────┘              ║
║                                 ▼                                              ║
║  ┌──────────────────────────────────────────────────────────────┐              ║
║  │ collect_preflight_checks(                                     │              ║
║  │   &Value, &Path, &Path, &Path, &[Value],                     │              ║
║  │   &BenchmarkConfig, &[Variant], &[VariantRuntimeProfile]     │              ║
║  │ ) → Vec<PreflightCheck>                                       │              ║
║  │                                                               │              ║
║  │ PreflightReport { passed: bool, checks }                      │              ║
║  │   gates execution — bail if !passed                           │              ║
║  └──────────────────────────────┬───────────────────────────────┘              ║
║                                 │                                              ║
╚═════════════════════════════════╪══════════════════════════════════════════════╝
                                  │
                                  ▼
```

---

## Phase 3 — Schedule Engine (Dispatcher)

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  experiment/runner.rs — Schedule Engine                                              ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ execute_schedule_engine(                                              │           ║
║  │   ...,                                                                │           ║
║  │   schedule:              &[TrialSlot],             [&ref]             │           ║
║  │   schedule_progress:     &mut ScheduleProgress,    [&mut ref]         │           ║
║  │   trial_index:           &mut usize,               [&mut ref]         │           ║
║  │   consecutive_failures:  &mut BTreeMap<usize,usize>,[&mut ref]        │           ║
║  │   pruned_variants:       &mut HashSet<usize>,      [&mut ref]         │           ║
║  │   run_sink:              &mut dyn RunSink,         [&mut ref]         │           ║
║  │   max_concurrency:       usize                                        │           ║
║  │ )                                                                     │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │  delegates immediately                           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ execute_schedule_engine_local(...)                                     │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ Arc::new(ParallelWorkerExecutionContext {                              │           ║
║  │   run_dir:                  PathBuf,                     [OWNED clone] │           ║
║  │   run_id:                   String,                      [OWNED clone] │           ║
║  │   variants:                 Vec<Variant>,                [OWNED clone] │           ║
║  │   tasks:                    Vec<Value>,                  [OWNED clone] │           ║
║  │   policy_config:            PolicyConfig,                [OWNED clone] │           ║
║  │   benchmark_config:         BenchmarkConfig,             [OWNED clone] │           ║
║  │   variant_runtime_profiles: Vec<VariantRuntimeProfile>,  [OWNED clone] │           ║
║  │   ...                                                                  │           ║
║  │ })                                    ◄── [Arc] shared across threads  │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ mpsc::channel::<LocalTrialCompletion>()                               │           ║
║  │ → (completion_tx: Sender,  completion_rx: Receiver)                   │           ║
║  │     ▲ Sender cloned per worker thread                                 │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ DeterministicCommitter::from_progress(&ScheduleProgress, &[records])  │           ║
║  │   ensures slots are committed in schedule order regardless of         │           ║
║  │   which worker finishes first                                         │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐           ║
║  │  DISPATCH LOOP                                                       │           ║
║  │  while next_commit_idx < schedule.len() || !in_flight.is_empty()     │           ║
║  │                                                                      │           ║
║  │  ┌────────────────────────────────────────────────────────┐          │           ║
║  │  │ INTERRUPTED.load(SeqCst)          [global AtomicBool]  │          │           ║
║  │  │ load_external_schedule_outcome_request(&Path)          │          │           ║
║  │  │ enforce_runtime_disk_headroom(&Path, min_free_bytes)   │          │           ║
║  │  └──────────────────────┬─────────────────────────────────┘          │           ║
║  │                         ▼                                            │           ║
║  │  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐         │           ║
║  │  │  INNER DISPATCH                                         │         │           ║
║  │  │  while slots remain && in_flight.len() < capacity       │         │           ║
║  │  │                                                         │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │         │           ║
║  │  │  │ check: pruned_variants.contains(variant_idx)?  │     │         │           ║
║  │  │  │ check: max_in_flight_per_variant limit?        │     │         │           ║
║  │  │  └──────────────────┬─────────────────────────────┘     │         │           ║
║  │  │                     ▼                                   │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │         │           ║
║  │  │  │ trial_id = format!("trial_{}", index + 1)      │     │         │           ║
║  │  │  │ TrialPaths::new(&trial_dir, &project_root)     │     │         │           ║
║  │  │  │   → TrialPaths  [OWNED]                        │     │         │           ║
║  │  │  └──────────────────┬─────────────────────────────┘     │         │           ║
║  │  │                     ▼                                   │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │         │           ║
║  │  │  │ LocalTrialLaunch {                             │     │         │           ║
║  │  │  │   schedule_idx: usize,                         │     │         │           ║
║  │  │  │   trial_id:     String,      [OWNED]           │     │         │           ║
║  │  │  │   slot:         TrialSlot,   [cloned]          │     │         │           ║
║  │  │  │   trial_paths:  TrialPaths   [OWNED ⤳ moved]   │     │         │           ║
║  │  │  │ }                                              │     │         │           ║
║  │  │  └──────────────────┬─────────────────────────────┘     │         │           ║
║  │  │                     │  ⤳ moved into thread              │         │           ║
║  │  │                     ▼                                   │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │         │           ║
║  │  │  │ spawn_local_trial(                             │     │         │           ║
║  │  │  │   Arc::clone(&context),   [Arc ref]            │     │         │           ║
║  │  │  │   launch,                 [OWNED ⤳ moved]      │     │         │           ║
║  │  │  │   completion_tx.clone()   [cloned Sender]      │     │         │           ║
║  │  │  │ )                                              │     │         │           ║
║  │  │  └──────────────────┬─────────────────────────────┘     │         │           ║
║  │  │                     │                                   │         │           ║
║  │  │                     ▼                                   │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │         │           ║
║  │  │  │ in_flight.insert(trial_id, InFlightDispatch{}) │     │         │           ║
║  │  │  │ in_flight_by_variant[variant_idx] += 1         │     │         │           ║
║  │  │  └────────────────────────────────────────────────┘     │         │           ║
║  │  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘         │           ║
║  │                                                                      │           ║
║  │  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐         │           ║
║  │  │  COMPLETION POLLING                                     │         │           ║
║  │  │                                                         │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │  ◄──────┤           ║
║  │  │  │ poll_local_trial_completions(                  │     │         │           ║
║  │  │  │   &Receiver, Duration                          │     │  ◄ ─ ─ ─mpsc       ║
║  │  │  │ ) → Vec<LocalTrialCompletion>                  │     │         │           ║
║  │  │  └──────────────────┬─────────────────────────────┘     │         │           ║
║  │  │                     ▼                                   │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │         │           ║
║  │  │  │ in_flight.remove(&trial_id)                    │     │         │           ║
║  │  │  │ in_flight_by_variant[variant_idx] -= 1         │     │         │           ║
║  │  │  └──────────────────┬─────────────────────────────┘     │         │           ║
║  │  │                     ▼                                   │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │         │           ║
║  │  │  │ committer.enqueue_trial(idx, result)           │     │         │           ║
║  │  │  │ committer.drain_ready(...)                     │     │         │           ║
║  │  │  │   commits in schedule order to run_sink        │     │         │           ║
║  │  │  └──────────────────┬─────────────────────────────┘     │         │           ║
║  │  │                     ▼                                   │         │           ║
║  │  │  ┌────────────────────────────────────────────────┐     │         │           ║
║  │  │  │ persist_pending_trial_completions(&Path, &recs) │     │         │           ║
║  │  │  │ write_run_control_v2(..., &active_trials, ...)  │     │         │           ║
║  │  │  └────────────────────────────────────────────────┘     │         │           ║
║  │  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘         │           ║
║  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘           ║
║                                                                                     ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                          │
      ┌───────────────────┘
      │ thread::spawn(move || ...)
      │ - - - - - - - - - - - - - - - - - - - ►
      ▼
```

---

## Phase 4 — Worker Thread Entry

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  WORKER THREAD  (experiment/runner.rs)                                               ║
║  one per in-flight trial, isolated from scheduler                                    ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ std::panic::catch_unwind(AssertUnwindSafe(|| {                        │           ║
║  │   execute_local_trial(&context, launch)                               │           ║
║  │ }))                                                                   │           ║
║  │   panics → Err("local trial execution panicked")                      │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ execute_local_trial(                                                  │           ║
║  │   context: &ParallelWorkerExecutionContext,     [&ref from Arc]       │           ║
║  │   launch:  LocalTrialLaunch                     [OWNED ⤳ moved in]   │           ║
║  │ ) → Result<TrialExecutionResult>                                      │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║        thread-local allocations (not shared):                                        ║
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ let mut local_chain_states: BTreeMap<String, ChainRuntimeState>       │           ║
║  │ let mut buffered_sink = BufferedRunSink::default()                    │           ║
║  │ let artifact_store = ArtifactStore::new(run_dir.join("artifacts"))    │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ ScheduledTrialRequest<'a> {                                           │           ║
║  │                                                                       │           ║
║  │   ── borrowed from Arc<Context> ──────────────────────────            │           ║
║  │   run_dir:                  &'a Path,                                 │           ║
║  │   run_id:                   &'a str,                                  │           ║
║  │   variants:                 &'a [Variant],                            │           ║
║  │   tasks:                    &'a [Value],                              │           ║
║  │   policy_config:            &'a PolicyConfig,                         │           ║
║  │   benchmark_config:         &'a BenchmarkConfig,                      │           ║
║  │   variant_runtime_profiles: &'a [VariantRuntimeProfile],              │           ║
║  │                                                                       │           ║
║  │   ── borrowed from launch ────────────────────────────────            │           ║
║  │   slot:                     &'a TrialSlot,                            │           ║
║  │                                                                       │           ║
║  │   ── &mut borrowed from thread locals ────────────────────            │           ║
║  │   trial_index:              &'a mut usize,                            │           ║
║  │   chain_states:             &'a mut BTreeMap<String, ...>,            │           ║
║  │   run_sink:                 &'a mut dyn RunSink,                      │           ║
║  │                                                                       │           ║
║  │   ── &ref to thread local ────────────────────────────────            │           ║
║  │   artifact_store:           &'a ArtifactStore,                        │           ║
║  │ }                                                                     │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │                                                  ║
╚═══════════════════════════════════╪══════════════════════════════════════════════════╝
                                    │
                                    ▼
```

---

## Phase 5 — Trial Preparation

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  trial/schedule.rs — prepare_scheduled_trial                                         ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ prepare_scheduled_trial(                                              │           ║
║  │   request: &mut ScheduledTrialRequest<'_>                             │           ║
║  │ ) → Result<PreparedScheduledTrial>                                    │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ parse_task_boundary_from_packaged_task(task: &Value)                   │           ║
║  │ → TaskBoundaryMaterialization  [OWNED]                                │           ║
║  │   { task_payload, task_image, task_workdir, materialization }          │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ validate_task_boundary_workspace_materialization(&TaskBoundaryMat)     │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ resolve_effective_task_policy(                                         │           ║
║  │   &PolicyConfig, &BenchmarkPolicy, task: &Value                       │           ║
║  │ ) → EffectiveTaskPolicy [OWNED]                                       │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ resolve_chain_label(&Value, &str, StatePolicy) → String               │           ║
║  │ chain_key = format!("{}::{}", variant.id, chain_label)                │           ║
║  │ chain_states.get(&chain_key)          [&mut BTreeMap borrow]          │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ *request.trial_index += 1                                             │           ║
║  │ trial_id = format!("trial_{}", trial_index)                           │           ║
║  │ write_trial_state(&trial_dir, &trial_id, "running", ...)              │           ║
║  │ ☠ TrialStateGuard::new(&trial_dir, &trial_id)                        │           ║
║  │   [RAII] auto-writes status on Drop if not consumed                   │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐           ║
║  │  trial/prepare.rs — Task Environment Preparation                     │           ║
║  │                                                                      │           ║
║  │  ┌──────────────────────────────────────────────────────────┐        │           ║
║  │  │ prepare_task_environment(                                │        │           ║
║  │  │   project_root: &Path,  trial_dir: &Path,               │        │           ║
║  │  │   run_id: &str,  trial_id: &str,                        │        │           ║
║  │  │   experiment: &Value,  variant: &Variant,                │        │           ║
║  │  │   task_idx: usize,  repl: usize,                        │        │           ║
║  │  │   task_boundary: &TaskBoundaryMaterialization,           │        │           ║
║  │  │   agent_runtime: &AgentRuntimeConfig,                    │        │           ║
║  │  │   existing_workspace_ref: Option<&str>                   │        │           ║
║  │  │ ) → PreparedTaskEnvironment                              │        │           ║
║  │  └──────────────────────────┬───────────────────────────────┘        │           ║
║  │                             ▼                                        │           ║
║  │  ┌──────────────────────────────────────────────────────────┐        │           ║
║  │  │ PreparedTaskEnvironment {                                │        │           ║
║  │  │   manifest:       PreparedTaskEnvironmentManifest [OWN]  │        │           ║
║  │  │   trial_paths:    TrialPaths                     [OWN]  │        │           ║
║  │  │   io_paths:       PreparedTrialIo                [OWN]  │        │           ║
║  │  │   dynamic_mounts: Vec<ResolvedMountReference>    [OWN]  │        │           ║
║  │  │   trial_input:    Value                          [OWN]  │        │           ║
║  │  │ }                                                        │        │           ║
║  │  └──────────────────────────────────────────────────────────┘        │           ║
║  │                                                                      │           ║
║  │  Key owned structs:                                                  │           ║
║  │                                                                      │           ║
║  │  ┌──────────────────────────────────────────────────────────┐        │           ║
║  │  │ ☠ TrialPaths {                                           │        │           ║
║  │  │   trial_dir:   PathBuf,                                  │        │           ║
║  │  │   scratch_dir: PathBuf,  ◄── cleaned on Drop             │        │           ║
║  │  │   in_dir:      PathBuf,                                  │        │           ║
║  │  │   workspace:   PathBuf,                                  │        │           ║
║  │  │   state:       PathBuf,                                  │        │           ║
║  │  │   out:         PathBuf,                                  │        │           ║
║  │  │   runtime:     RunnerRuntimeHostPaths                    │        │           ║
║  │  │ }  [RAII] impl Drop → cleanup_scratch()                  │        │           ║
║  │  └──────────────────────────────────────────────────────────┘        │           ║
║  │                                                                      │           ║
║  │  ┌──────────────────────────────────────────────────────────┐        │           ║
║  │  │ PreparedTrialIo {                                        │        │           ║
║  │  │   trial_input_host:  PathBuf,                            │        │           ║
║  │  │   result_host:       PathBuf,                            │        │           ║
║  │  │   trajectory_host:   PathBuf,                            │        │           ║
║  │  │   events_host:       PathBuf,                            │        │           ║
║  │  │   grader_input_host: PathBuf                             │        │           ║
║  │  │ }                                                        │        │           ║
║  │  └──────────────────────────────────────────────────────────┘        │           ║
║  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘           ║
║                                   │                                                  ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ artifact_store.put_bytes(&input_bytes) → String sha256 ref            │           ║
║  │ bootstrap_store.upsert_attempt_object(...)                            │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ write_scheduled_trial_metadata(&request, &prepared)                   │           ║
║  │ stage_benchmark_trial_preflight(&BenchmarkConfig, &Path, ...)         │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │                                                  ║
╚═══════════════════════════════════╪══════════════════════════════════════════════════╝
                                    │
                                    ▼
```

---

## Phase 6 — Attempt Loop & Request Construction

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  trial/schedule.rs — Attempt Loop                                                    ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║          ⟳ for attempt in 0..policy_config.retry_max_attempts                        ║
║          │                                                                           ║
║          ▼                                                                           ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ execute_scheduled_trial_attempt(                                      │           ║
║  │   request:    &ScheduledTrialRequest<'_>,         [&ref]             │           ║
║  │   prepared:   &PreparedScheduledTrial,            [&ref]             │           ║
║  │   attempt_no: u32                                                     │           ║
║  │ ) → Result<TrialRuntimeOutcome>                                       │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │  constructs:                                     ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ AdapterRunRequest<'a> {                                               │           ║
║  │   ALL FIELDS BORROWED — no ownership transfer                         │           ║
║  │                                                                       │           ║
║  │   runtime_experiment:     &'a Value,                                  │           ║
║  │   runtime:                &'a AgentRuntimeConfig,                     │           ║
║  │   variant_args:           &'a [String],                               │           ║
║  │   runtime_env:            &'a BTreeMap<String, String>,               │           ║
║  │   runtime_overrides_env:  &'a BTreeMap<String, String>,               │           ║
║  │   trial_paths:            &'a TrialPaths,                             │           ║
║  │   dynamic_mounts:         &'a [ResolvedMountReference],               │           ║
║  │   io_paths:               &'a PreparedTrialIo,                        │           ║
║  │   network_mode:           &'a str,                                    │           ║
║  │   benchmark_grader:       Option<&'a BenchmarkGraderConfig>,          │           ║
║  │   run_id:                 &'a str,                                    │           ║
║  │   task_image:             &'a str,                                    │           ║
║  │   task_workdir:           &'a str,                                    │           ║
║  │   task_materialization:   TaskMaterializationKind,  [cloned]          │           ║
║  │   agent_artifact:         Option<&'a Path>,                           │           ║
║  │ }                                                                     │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │  calls:                                          ║
║                                   ▼                                                  ║
║                        ┌──────────────────────┐                                      ║
║                        │ execute_trial_runtime │────────► (Phase 7)                   ║
║                        └──────────────────────┘                                      ║
║                                   │                                                  ║
║                                   ▼  on return:                                      ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ benchmark_retry_inputs(                                               │           ║
║  │   grading_enabled, &trial_output, conclusion_row,                     │           ║
║  │   grade_error_reason, &agent_exit_status                              │           ║
║  │ ) → (RetryOutcome, RetryExitStatus)                                   │           ║
║  │                                                                       │           ║
║  │ should_retry_outcome(&outcome, &exit, &retry_on) → bool              │           ║
║  └────────────────────┬──────────────────────┬───────────────────────────┘           ║
║                       │                      │                                       ║
║              retry=true ⟳              retry=false                                   ║
║              (loop back)                     │                                       ║
║                                              ▼                                       ║
║                                    finalize_scheduled_trial ────► (Phase 8)          ║
║                                                                                     ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼
```

---

## Phase 7 — Trial Runtime (Docker Execution)

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  trial/execution.rs — execute_trial_runtime                                          ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ execute_trial_runtime(                                                │           ║
║  │   trial_dir:          &Path,                                          │           ║
║  │   schedule_idx:       usize,                                          │           ║
║  │   attempt_no:         u32,                                            │           ║
║  │   request:            &AdapterRunRequest<'_>,         [&ref]         │           ║
║  │   task_id:            &str,                                           │           ║
║  │   variant_id:         &str,                                           │           ║
║  │   repl_idx:           usize,                                          │           ║
║  │   task_sandbox_plan:  &TaskSandboxPlan                                │           ║
║  │ ) → Result<TrialRuntimeOutcome>                                       │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ validate_benchmark_grading_contract(&AdapterRunRequest)               │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ DockerRuntime::connect()  → DockerRuntime  [OWNED]                    │           ║
║  │   (initializes tokio runtime internally)                              │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ docker.ensure_image(&task_sandbox_plan.image)   [&self]               │           ║
║  │   pulls image if not present locally                                  │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ build_hidden_asset_bindings(&BenchmarkGraderConfig)                   │           ║
║  │ → Vec<HiddenAssetBinding>  [OWNED]                                    │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ let mut attempt_state = new_trial_attempt_state(...)                   │           ║
║  │   [OWNED, mutated throughout execution]                               │           ║
║  │ write_trial_attempt_state(&trial_dir, &attempt_state)                 │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ let mut task_container:    Option<ContainerHandle> = None              │           ║
║  │ let mut grading_container: Option<ContainerHandle> = None             │           ║
║  │   (captured for cleanup on any exit path)                             │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │                                                  ║
║  ┌────────────────────────────────▼──────────────────────────────────────┐           ║
║  │  AGENT PHASE (inner closure → Result<TrialRuntimeOutcome>)            │           ║
║  │                                                                       │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ collect_workspace_snapshot_manifest(&workspace)            │        │           ║
║  │  │ → pre_snapshot_manifest: Value  [OWNED]                    │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ set_trial_attempt_phase(trial_dir, &mut state,            │        │           ║
║  │  │   TrialPhase::AgentMaterializing)                         │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐        │           ║
║  │  │  TASK CONTAINER MATERIALIZATION                           │        │           ║
║  │  │                                                           │        │           ║
║  │  │  materialize_task_sandbox(                                │        │           ║
║  │  │    &docker,           [&self]                             │        │           ║
║  │  │    &request,          [&ref]                              │        │           ║
║  │  │    &sandbox_plan,     [&ref]                              │        │           ║
║  │  │    injected_phase     [Option<&ref>]                      │        │           ║
║  │  │  ) → ContainerHandle  [OWNED]                             │        │           ║
║  │  │           │                                               │        │           ║
║  │  │           ├── build_container_spec(...)  → ContainerSpec   │        │           ║
║  │  │           ├── resolve_container_platform(&image)           │        │           ║
║  │  │           ├── docker.create_container(&spec) → handle     │        │           ║
║  │  │           └── docker.start_container(&handle)             │        │           ║
║  │  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘        │           ║
║  │                            │                                          │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ stash_hidden_assets(                                      │        │           ║
║  │  │   &docker, &task_handle, &trial_dir,                      │        │           ║
║  │  │   &hidden_asset_bindings, timeout_ms                      │        │           ║
║  │  │ )  moves grader files to stash before agent sees them     │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ sync_host_workspace_to_container(                         │        │           ║
║  │  │   &docker, &task_handle, &trial_dir,                      │        │           ║
║  │  │   &workdir, "task_workspace", timeout_ms                  │        │           ║
║  │  │ )                                                         │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ set_trial_attempt_phase(..., TrialPhase::AgentRunning)    │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐        │           ║
║  │  │  AGENT EXECUTION                                          │        │           ║
║  │  │                                                           │        │           ║
║  │  │  resolve_runtime_agent_command(&request) → Vec<String>    │        │           ║
║  │  │           │                                               │        │           ║
║  │  │           ▼                                               │        │           ║
║  │  │  build_exec_env(&request, &workdir, None, true)           │        │           ║
║  │  │  → Vec<(String, String)>                                  │        │           ║
║  │  │  includes: AGENTLAB_* env vars, variant args              │        │           ║
║  │  │           │                                               │        │           ║
║  │  │           ▼                                               │        │           ║
║  │  │  docker.exec(&task_handle, &ExecSpec {                    │        │           ║
║  │  │    command, env, workdir                                  │        │           ║
║  │  │  }) → ExecHandle [OWNED]                                  │        │           ║
║  │  │           │                                               │        │           ║
║  │  │           ▼                                               │        │           ║
║  │  │  docker.stream_exec_output(                               │        │           ║
║  │  │    &exec_handle,                                          │        │           ║
║  │  │    &trial_dir/harness_stdout.log,                         │        │           ║
║  │  │    &trial_dir/harness_stderr.log,                         │        │           ║
║  │  │    Some(Duration::from_millis(time_limit_ms))             │        │           ║
║  │  │  ) → StreamResult { timed_out: bool }                     │        │           ║
║  │  │           │                                               │        │           ║
║  │  │           ▼                                               │        │           ║
║  │  │  docker.wait_exec(&exec_handle)                           │        │           ║
║  │  │  → ExecStatus { exit_code: Option<i64>, running: bool }   │        │           ║
║  │  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘        │           ║
║  │                            │                                          │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ load_trial_output_resilient(&result_host)                 │        │           ║
║  │  │ → (Value, Option<String> parse_error)                     │        │           ║
║  │  │                                                           │        │           ║
║  │  │ classify_contract_file_state(&path, parse_error)          │        │           ║
║  │  │ → ContractFileState                                       │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ sync_container_workspace_to_host(                         │        │           ║
║  │  │   &docker, &task_handle, &trial_dir,                      │        │           ║
║  │  │   &workdir, &workspace_path                               │        │           ║
║  │  │ )                                                         │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ collect_workspace_snapshot_manifest(&workspace)            │        │           ║
║  │  │ → post_snapshot_manifest: Value  [OWNED]                   │        │           ║
║  │  │                                                           │        │           ║
║  │  │ diff_workspace_snapshots(&pre_manifest, &post_manifest)   │        │           ║
║  │  │ → diff_incremental: Value  [OWNED]                        │        │           ║
║  │  │                                                           │        │           ║
║  │  │ derive_patch_from_diff(&diff_incremental)                 │        │           ║
║  │  │ → patch_incremental: Value  [OWNED]                       │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ extract_candidate_artifact_record(&output, artifact_type) │        │           ║
║  │  │ → CandidateArtifactRecord                                 │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ AgentStageOutcome {          [OWNED — assembled here]     │        │           ║
║  │  │   agent_exit_status:         String,                      │        │           ║
║  │  │   trial_output:              Value,                       │        │           ║
║  │  │   result_parse_error:        Option<String>,              │        │           ║
║  │  │   pre_snapshot_manifest:     Value,                       │        │           ║
║  │  │   post_snapshot_manifest:    Value,                       │        │           ║
║  │  │   pre/post_snapshot_path:    PathBuf,                     │        │           ║
║  │  │   diff/patch_incremental:    Value,                       │        │           ║
║  │  │   diff/patch_incr_path:      PathBuf,                    │        │           ║
║  │  │ }                                                         │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            ▼                                          │           ║
║  │  ┌───────────────────────────────────────────────────────────┐        │           ║
║  │  │ set_trial_attempt_phase(..., TrialPhase::AgentFinished)   │        │           ║
║  │  └─────────────────────────┬─────────────────────────────────┘        │           ║
║  │                            │                                          │           ║
║  └────────────────────────────┤──────────────────────────────────────────┘           ║
║                               │                                                      ║
║               ┌───────────────┴───────────────┐                                      ║
║               │                               │                                      ║
║        grading enabled?                  no grading                                  ║
║               │                               │                                      ║
║               ▼                               │                                      ║
║  ┌────────────────────────────────────────┐   │                                      ║
║  │  GRADING PHASE  (see Phase 7a below)   │   │                                      ║
║  └──────────────────┬─────────────────────┘   │                                      ║
║                     │                         │                                      ║
║                     ▼                         ▼                                      ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ finalize_trial_runtime(                                               │           ║
║  │   trial_dir:       &Path,                                             │           ║
║  │   attempt_state:   &mut TrialAttemptState,                            │           ║
║  │   agent_outcome:   AgentStageOutcome,       [OWNED ⤳ moved in]       │           ║
║  │   grading_outcome: GradingStageOutcome      [OWNED ⤳ moved in]       │           ║
║  │ ) → TrialRuntimeOutcome                                               │           ║
║  │   sets phase → CommitPending                                          │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐           ║
║  │  CONTAINER CLEANUP  (runs regardless of success or failure)          │           ║
║  │                                                                      │           ║
║  │  docker.remove_container(&grading_container, force=true)             │           ║
║  │  docker.remove_container(&task_container, force=true)                │           ║
║  │                                                                      │           ║
║  │  if execution.is_err():                                              │           ║
║  │    reconcile_trial_attempt_as_abandoned(&trial_dir)                  │           ║
║  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘           ║
║                                   │                                                  ║
╚═══════════════════════════════════╪══════════════════════════════════════════════════╝
                                    │
                                    ▼  returns TrialRuntimeOutcome [OWNED]
```

---

## Phase 7a — Grading (within execute_trial_runtime)

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  trial/execution.rs — GRADING PHASE  (conditional on benchmark_grading_enabled)      ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ write_grader_input_file(                                              │           ║
║  │   io_paths: &PreparedTrialIo,                                         │           ║
║  │   trial_input: &Value,  trial_output: &Value,                         │           ║
║  │   trial_paths: &TrialPaths,  task_workdir: &str,                      │           ║
║  │   agent_exit_status: &str,  parse_error: Option<&str>,                │           ║
║  │   agent_started_at: &str,  agent_ended_at: &str,                      │           ║
║  │   diff_path: Option<&Path>,  patch_path: Option<&Path>                │           ║
║  │ )                                                                     │           ║
║  │   creates GraderInputV1 JSON with contract IDs, candidate             │           ║
║  │   artifact record, workspace delta, agent phase metadata              │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ resolve_benchmark_grader_command(&request)                            │           ║
║  │ → Option<Vec<String>>                                                 │           ║
║  │   None → early return with grade_error_reason                         │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ resolve_grading_phase(&request, &grader_config, &command)             │           ║
║  │ → ResolvedGradingPhase { image, workdir, command, extra_mounts, ... } │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ build_grading_sandbox_plan(&grader_config, &resolved_phase)           │           ║
║  │ → GradingSandboxPlan                                                  │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ set_trial_attempt_phase(..., TrialPhase::GraderMaterializing)         │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐           ║
║  │  STRATEGY-DEPENDENT MATERIALIZATION                                  │           ║
║  │                                                                      │           ║
║  │  ┌─────────────────────────────────────────────────────────────┐     │           ║
║  │  │ match grader.strategy {                                     │     │           ║
║  │  │                                                             │     │           ║
║  │  │   InTaskImage ──►                                           │     │           ║
║  │  │     reveal_hidden_assets(                                   │     │           ║
║  │  │       &docker, &task_handle, &trial_dir,                    │     │           ║
║  │  │       &hidden_asset_bindings, timeout_ms                    │     │           ║
║  │  │     )                                                       │     │           ║
║  │  │     grading_handle = task_handle.clone()                    │     │           ║
║  │  │                                                             │     │           ║
║  │  │   Injected ──►                                              │     │           ║
║  │  │     materialize_injected_grader_bundle(                     │     │           ║
║  │  │       &docker, &task_handle, &trial_dir,                    │     │           ║
║  │  │       &resolved_phase, timeout_ms                           │     │           ║
║  │  │     )   copies grader bundle into task container            │     │           ║
║  │  │     grading_handle = task_handle.clone()                    │     │           ║
║  │  │                                                             │     │           ║
║  │  │   Separate ──►                                              │     │           ║
║  │  │     materialize_grading_sandbox(                            │     │           ║
║  │  │       &docker, &request, &resolved_phase                    │     │           ║
║  │  │     ) → ContainerHandle [OWNED - new container]             │     │           ║
║  │  │     sync_host_workspace_to_container(...)                   │     │           ║
║  │  │     grading_container = Some(handle.clone())                │     │           ║
║  │  │ }                                                           │     │           ║
║  │  └─────────────────────────────────────────────────────────────┘     │           ║
║  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘           ║
║                                   │                                                  ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ set_trial_attempt_phase(..., TrialPhase::GraderRunning)               │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐           ║
║  │  GRADER EXECUTION                                                    │           ║
║  │                                                                      │           ║
║  │  build_exec_env(&request, &workdir,                                  │           ║
║  │    Some((AGENTLAB_ENV_AGENT_EXIT_STATUS, &exit_status)),             │           ║
║  │    false)                                                            │           ║
║  │           │                                                          │           ║
║  │           ▼                                                          │           ║
║  │  docker.exec(&grading_handle, &ExecSpec {                            │           ║
║  │    command: resolved_phase.command,                                   │           ║
║  │    env, workdir                                                      │           ║
║  │  }) → ExecHandle [OWNED]                                             │           ║
║  │           │                                                          │           ║
║  │           ▼                                                          │           ║
║  │  docker.stream_exec_output(                                          │           ║
║  │    &exec, &grader_stdout.log, &grader_stderr.log,                    │           ║
║  │    Some(Duration)                                                    │           ║
║  │  ) → StreamResult                                                    │           ║
║  │           │                                                          │           ║
║  │           ▼                                                          │           ║
║  │  docker.wait_exec(&exec) → ExecStatus                               │           ║
║  │           │                                                          │           ║
║  │           ▼                                                          │           ║
║  │  classify_contract_file_state(&expected_output_path, None)           │           ║
║  │  → raw_output_state: ContractFileState                               │           ║
║  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘           ║
║                                   │                                                  ║
║               ┌───────────────────┴───────────────┐                                  ║
║               │                                   │                                  ║
║        uses_mapper()?                       direct mode                               ║
║               │                                   │                                  ║
║               ▼                                   │                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐        │                                  ║
║  │  MAPPER PHASE                         │        │                                  ║
║  │                                       │        │                                  ║
║  │  set_trial_attempt_phase(             │        │                                  ║
║  │    ..., GraderMapping)                │        │                                  ║
║  │           │                           │        │                                  ║
║  │           ▼                           │        │                                  ║
║  │  resolve_benchmark_conclusion_        │        │                                  ║
║  │    mapper_command(&request,            │        │                                  ║
║  │    &grader) → Option<Vec<String>>     │        │                                  ║
║  │           │                           │        │                                  ║
║  │           ▼                           │        │                                  ║
║  │  docker.exec(&grading_handle,         │        │                                  ║
║  │    &ExecSpec { mapper_cmd, ... })     │        │                                  ║
║  │  docker.stream_exec_output(...)       │        │                                  ║
║  │  docker.wait_exec(...)                │        │                                  ║
║  │           │                           │        │                                  ║
║  │           ▼                           │        │                                  ║
║  │  validate_json_schema(                │        │                                  ║
║  │    "trial_conclusion_v1",             │        │                                  ║
║  │    &mapped_output_path                │        │                                  ║
║  │  ) → Result<Value>                    │        │                                  ║
║  └ ─ ─ ─ ─ ─ ─ ─┬─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘        │                                  ║
║                   │                               │                                  ║
║                   │        ┌──────────────────────┘                                  ║
║                   │        │                                                         ║
║                   │        ▼                                                         ║
║                   │  ┌────────────────────────────────────┐                          ║
║                   │  │ validate_json_schema(               │                          ║
║                   │  │   "trial_conclusion_v1",            │                          ║
║                   │  │   &mapped_output_path               │                          ║
║                   │  │ ) → Result<Value>                   │                          ║
║                   │  └─────────────┬──────────────────────┘                          ║
║                   │                │                                                  ║
║                   └────────┬───────┘                                                  ║
║                            ▼                                                         ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ GradingStageOutcome {                     [OWNED — assembled here]    │           ║
║  │   trial_conclusion_row:              Option<Value>,                    │           ║
║  │   deferred_trial_conclusion_records: Vec<Value>,                      │           ║
║  │   grade_error_reason:                Option<String>,                   │           ║
║  │ }                                                                     │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │                                                  ║
║                                   ▼  ⤳ moved into finalize_trial_runtime             ║
║                                                                                     ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
```

---

## Phase 8 — Trial Finalization & Persistence

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  trial/schedule.rs — finalize_scheduled_trial                                        ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ finalize_scheduled_trial(                                             │           ║
║  │   request:         &mut ScheduledTrialRequest<'_>,    [&mut ref]     │           ║
║  │   prepared:        &mut PreparedScheduledTrial,       [&mut ref]     │           ║
║  │   runtime_outcome: TrialRuntimeOutcome,               [OWNED ⤳ in]  │           ║
║  │   trial_started_at: Instant                                           │           ║
║  │ ) → Result<TrialExecutionResult>                                      │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐           ║
║  │  ARTIFACT STORAGE  (artifact_store: &ArtifactStore)                  │           ║
║  │                                                                      │           ║
║  │  artifact_store.put_file(&pre_snapshot_path)    → String sha256      │           ║
║  │  artifact_store.put_file(&post_snapshot_path)   → String sha256      │           ║
║  │  artifact_store.put_file(&diff_incremental)     → String sha256      │           ║
║  │  artifact_store.put_file(&diff_cumulative)      → String sha256      │           ║
║  │  artifact_store.put_file(&patch_incremental)    → String sha256      │           ║
║  │  artifact_store.put_file(&patch_cumulative)     → String sha256      │           ║
║  │  capture_workspace_object_ref(&store, &path)    → String sha256      │           ║
║  │    (only if workspace_diff_is_empty() == false)                      │           ║
║  │  artifact_store.put_bytes(&trial_output)        → String sha256      │           ║
║  │  artifact_store.put_file(&stdout/stderr)        → Option<String>     │           ║
║  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ chain_states.insert(chain_key.clone(), ChainRuntimeState {            │           ║
║  │   chain_root_snapshot_ref,                                            │           ║
║  │   chain_root_snapshot_manifest,                                       │           ║
║  │   latest_snapshot_ref,                                                │           ║
║  │   latest_workspace_ref,                                               │           ║
║  │   step_index,                                                         │           ║
║  │ })                                                                    │           ║
║  │   via &mut BTreeMap borrow from request                               │           ║
║  │   (skipped for IsolatePerTrial policy)                                │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ evidence_record = json!({ schema: "evidence_record_v1", ... })        │           ║
║  │   ids:      { run_id, trial_id, variant_id, task_id, repl_idx }       │           ║
║  │   evidence: { *_ref: sha256 strings for all artifacts }               │           ║
║  │                                                                       │           ║
║  │ validate_required_evidence_classes(&record, &required_classes)         │           ║
║  │ append_jsonl(&evidence_records_path, &evidence_record)                │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ chain_state_record = json!({ schema: "task_chain_state_v1", ... })    │           ║
║  │ append_jsonl(&task_chain_states_path, &chain_state_record)            │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ write_state_inventory(...)                                            │           ║
║  │ validate_hooks(&manifest, &events_host, &schema)                      │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐           ║
║  │  OUTCOME CALCULATION                                                 │           ║
║  │                                                                      │           ║
║  │  trial_conclusion_row.pointer("/reported_outcome")                   │           ║
║  │  → "pass" | "fail" | "missing" | "error"                            │           ║
║  │                                                                      │           ║
║  │  mapped outcome:                                                     │           ║
║  │    pass    → "success"                                               │           ║
║  │    fail    → "failure"                                               │           ║
║  │    missing → "missing"                                               │           ║
║  │    error   → "error"                                                 │           ║
║  │                                                                      │           ║
║  │  failure_classification:                                             │           ║
║  │    "grade_error"         if grading failed                           │           ║
║  │    "agent_exit_nonzero"  if exit != 0                                │           ║
║  │    "result_parse_error"  if JSON parse failed                        │           ║
║  │    None                  if successful                               │           ║
║  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘           ║
║                                   ▼                                                  ║
║  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐           ║
║  │  RunSink WRITES  (run_sink: &mut dyn RunSink)                        │           ║
║  │                                                                      │           ║
║  │  run_sink.append_trial_record(TrialRecord { ... })                   │           ║
║  │  run_sink.append_metric_rows(build_metric_rows(...))                 │           ║
║  │  run_sink.append_event_rows(load_event_rows(...))                    │           ║
║  │  run_sink.append_variant_snapshot(build_variant_snapshot_rows(...))   │           ║
║  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ ☠ prepared.trial_guard.complete(status, classification)               │           ║
║  │   writes trial_state.json — RAII guard consumed                       │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ TrialExecutionResult {                          [OWNED — all fields]  │           ║
║  │   trial_id:                          String,                          │           ║
║  │   slot_status:                       String,                          │           ║
║  │   variant_idx:                       Option<usize>,                   │           ║
║  │   failure_classification:            Option<String>,                   │           ║
║  │   deferred_trial_records:            Vec<TrialRecord>,                │           ║
║  │   deferred_metric_rows:              Vec<MetricRow>,                  │           ║
║  │   deferred_event_rows:               Vec<EventRow>,                   │           ║
║  │   deferred_variant_snapshot_rows:    Vec<VariantSnapshotRow>,         │           ║
║  │   deferred_evidence_records:         Vec<Value>,                      │           ║
║  │   deferred_chain_state_records:      Vec<Value>,                      │           ║
║  │   deferred_trial_conclusion_records: Vec<Value>,                      │           ║
║  │ }                                                                     │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │                                                  ║
╚═══════════════════════════════════╪══════════════════════════════════════════════════╝
                                    │
                                    ▼
```

---

## Phase 9 — Worker Return to Scheduler

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  Worker Thread → Scheduler Return                                                    ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ completion_tx.send(LocalTrialCompletion {                             │           ║
║  │   trial_id:     String,                             [OWNED]          │           ║
║  │   schedule_idx: usize,                                                │           ║
║  │   result:       Result<TrialExecutionResult, String> [OWNED ⤳ sent]  │           ║
║  │ })                                                                    │           ║
║  │   ownership transferred across thread boundary via mpsc               │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   │                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ ☠ TrialPaths::Drop                                                    │           ║
║  │   → cleanup_scratch() removes scratch_dir                             │           ║
║  │                                                                       │           ║
║  │ ☠ BufferedRunSink drops (already drained into result)                 │           ║
║  │ ☠ ArtifactStore drops (no persistent state)                           │           ║
║  │ ☠ local_chain_states drops                                            │           ║
║  └───────────────────────────────────────────────────────────────────────┘           ║
║                                   │                                                  ║
║                                   │  - - - mpsc channel - - - ►                      ║
║                                   │                                                  ║
║                                   ▼  received by scheduler in Phase 3                ║
║                                      (poll_local_trial_completions)                  ║
║                                                                                     ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
```

---

## Phase 10 — Run Completion & Attestation

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  experiment/runner.rs — Run Completion                                                ║
║  (after all schedule slots committed via DeterministicCommitter)                      ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                     ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ write_run_control_v2(&run_dir, &run_id, "completed", &[], None)       │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ write_attestation(&run_dir, default_attestation(...))                  │           ║
║  │                                                                       │           ║
║  │ grades: {                                                             │           ║
║  │   integration_level:   run_integration_level,                         │           ║
║  │   replay_grade:        "best_effort",                                 │           ║
║  │   isolation_grade:     resolve_run_isolation_grade(...),               │           ║
║  │   comparability_grade: "unknown",                                     │           ║
║  │   provenance_grade:    "recorded",                                    │           ║
║  │   privacy_grade:       "unknown"                                      │           ║
║  │ }                                                                     │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ ☠ RunControlGuard::Drop                                               │           ║
║  │   auto-commits final run status                                       │           ║
║  │                                                                       │           ║
║  │ ☠ LeaseGuard::Drop                                                    │           ║
║  │   stops engine lease heartbeat                                        │           ║
║  └────────────────────────────────┬──────────────────────────────────────┘           ║
║                                   ▼                                                  ║
║  ┌───────────────────────────────────────────────────────────────────────┐           ║
║  │ → RunResult { run_dir: PathBuf, run_id: String }   [OWNED]           │           ║
║  └───────────────────────────────────────────────────────────────────────┘           ║
║                                                                                     ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
```

---

## Concurrency & Ownership Summary

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  SCHEDULER THREAD (single)                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐              │
│  │ Owns:                                                              │              │
│  │   &mut ScheduleProgress                                           │              │
│  │   &mut BTreeMap<usize, usize> consecutive_failures                │              │
│  │   &mut HashSet<usize> pruned_variants                             │              │
│  │   &mut dyn RunSink                                                │              │
│  │   HashMap<String, InFlightDispatch> in_flight                     │              │
│  │   DeterministicCommitter                                          │              │
│  │   mpsc::Receiver<LocalTrialCompletion>                            │              │
│  │                                                                    │              │
│  │ Shares via Arc:                                                    │              │
│  │   Arc<ParallelWorkerExecutionContext>                              │              │
│  │     (immutable after construction — safe to share)                 │              │
│  └────────────────────────────────────────────────────────────────────┘              │
│       │                                                                             │
│       │  spawn N threads (N = dispatch_capacity)                                    │
│       │                                                                             │
│       ├──── WORKER THREAD 1 ────────────────────────────────────────────┐           │
│       │     │ Receives: Arc<Context> [shared &ref],                     │           │
│       │     │           LocalTrialLaunch [OWNED ⤳ moved],               │           │
│       │     │           Sender [cloned]                                  │           │
│       │     │ Creates:  thread-local {                                   │           │
│       │     │             BufferedRunSink,                               │           │
│       │     │             ArtifactStore,                                 │           │
│       │     │             BTreeMap chain_states,                         │           │
│       │     │             DockerRuntime (per-trial connection)           │           │
│       │     │           }                                                │           │
│       │     │ Borrows:  ScheduledTrialRequest<'a> ties &refs to both    │           │
│       │     │           Arc<Context> fields AND thread-local &mut refs   │           │
│       │     │ Returns:  TrialExecutionResult [OWNED ⤳ via mpsc::send]   │           │
│       │     └───────────────────────────────────────────────────────────┘           │
│       │                                                                             │
│       ├──── WORKER THREAD 2 ──── (same pattern) ───────────────────────┐           │
│       │     └───────────────────────────────────────────────────────────┘           │
│       │                                                                             │
│       └──── WORKER THREAD N ──── (same pattern) ───────────────────────┐           │
│             └───────────────────────────────────────────────────────────┘           │
│                                                                                     │
│  RAII Guards (scope-bound cleanup):                                                 │
│    ☠ RunControlGuard      — run_dir scope, auto-commits run status                 │
│    ☠ LeaseGuard           — run_dir scope, stops heartbeat thread                  │
│    ☠ TrialStateGuard      — trial_dir scope, auto-completes trial status           │
│    ☠ TrialPaths           — trial scope, cleans scratch_dir                        │
│    ☠ ContainerHandle      — explicit cleanup (not Drop, manual remove_container)   │
│                                                                                     │
│  Interior Mutability:                                                               │
│    AtomicBool  INTERRUPTED — global signal, checked each dispatch loop iteration   │
│    AtomicUsize             — scratch dir sequence counter                           │
│                                                                                     │
│  Async Boundary:                                                                    │
│    All async confined to DockerRuntime internals (tokio block_on from sync)         │
│    Schedule engine, preparation, finalization — all synchronous                     │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```
