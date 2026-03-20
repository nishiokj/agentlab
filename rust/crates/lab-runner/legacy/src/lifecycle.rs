use crate::*;

pub(crate) fn load_jsonl_value_rows(path: &Path) -> Result<Vec<Value>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        rows.push(serde_json::from_str::<Value>(trimmed)?);
    }
    Ok(rows)
}

pub(crate) fn read_optional_json_value(path: &Path) -> Result<Option<Value>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path)?;
    if raw.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str::<Value>(&raw)?))
}

pub(crate) fn load_optional_json_record_with_schema(
    schema_name: &str,
    path: &Path,
) -> Result<Option<Value>> {
    let Some(value) = read_optional_json_value(path)? else {
        return Ok(None);
    };
    let schema = compile_schema(schema_name)?;
    if let Err(errors) = schema.validate(&value) {
        let msgs = errors.map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
        return Err(anyhow!(
            "schema validation failed ({}) {}: {}",
            schema_name,
            path.display(),
            msgs
        ));
    }
    Ok(Some(value))
}

pub(crate) fn mapped_grader_output_state(
    trial_conclusion_row: Option<&Value>,
    grade_error_reason: Option<&str>,
) -> Option<&'static str> {
    if trial_conclusion_row.is_some() {
        Some("valid")
    } else if let Some(reason) = grade_error_reason {
        if reason.starts_with("mapped_grader_output_invalid:") {
            Some("present_invalid")
        } else if reason.starts_with("mapped_grader_output_missing:") {
            Some("missing")
        } else {
            Some("missing")
        }
    } else {
        None
    }
}

pub(crate) fn task_grading_enabled(task_payload: &Value) -> bool {
    task_payload
        .pointer("/grading/enabled")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

pub(crate) fn benchmark_retry_inputs(
    benchmark_grading_enabled: bool,
    trial_output: &Value,
    trial_conclusion_row: Option<&Value>,
    grade_error_reason: Option<&str>,
    agent_exit_status: &str,
) -> (String, String) {
    let agent_outcome = trial_output_payload_view(trial_output)
        .get("outcome")
        .and_then(Value::as_str)
        .unwrap_or("error");
    if !benchmark_grading_enabled {
        return (agent_outcome.to_string(), agent_exit_status.to_string());
    }
    if grade_error_reason.is_some() {
        return ("error".to_string(), "0".to_string());
    }
    if let Some(mapped_outcome) = trial_conclusion_row
        .and_then(|row| row.pointer("/reported_outcome"))
        .and_then(Value::as_str)
        .and_then(trial_conclusion_outcome_to_trial_outcome)
    {
        return (mapped_outcome.to_string(), "0".to_string());
    }
    if trial_conclusion_row.is_some() {
        return ("missing".to_string(), "0".to_string());
    }
    ("error".to_string(), "0".to_string())
}

pub(crate) fn trial_output_payload_view<'a>(trial_output: &'a Value) -> &'a Value {
    if trial_output.get("schema_version").and_then(Value::as_str) == Some("artifact_envelope_v1") {
        trial_output.get("artifact").unwrap_or(trial_output)
    } else {
        trial_output
    }
}

pub(crate) fn trial_index_from_trial_id(trial_id: &str) -> Option<usize> {
    trial_id
        .strip_prefix("trial_")
        .and_then(|suffix| suffix.parse::<usize>().ok())
        .filter(|idx| *idx > 0)
}

pub(crate) struct RunCoordinator;

pub(crate) fn slot_commit_payload_digest_for_result(
    schedule_idx: usize,
    trial_result: &TrialExecutionResult,
) -> Result<String> {
    let payload = json!({
        "schedule_idx": schedule_idx,
        "trial_id": trial_result.trial_id.clone(),
        "slot_status": trial_result.slot_status.clone(),
        "trial_rows": trial_result.deferred_trial_records.clone(),
        "metric_rows": trial_result.deferred_metric_rows.clone(),
        "event_rows": trial_result.deferred_event_rows.clone(),
        "variant_snapshot_rows": trial_result.deferred_variant_snapshot_rows.clone(),
        "evidence_rows": trial_result.deferred_evidence_records.clone(),
        "chain_state_rows": trial_result.deferred_chain_state_records.clone(),
        "trial_conclusion_rows": trial_result.deferred_trial_conclusion_records.clone(),
    });
    Ok(canonical_json_digest(&payload))
}

pub(crate) fn annotate_row_identity(
    value: &mut Value,
    run_id: &str,
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
    row_seq: usize,
) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    obj.insert("run_id".to_string(), json!(run_id));
    obj.insert("schedule_idx".to_string(), json!(schedule_idx));
    obj.insert("slot_commit_id".to_string(), json!(slot_commit_id));
    obj.insert("attempt".to_string(), json!(attempt));
    obj.insert("row_seq".to_string(), json!(row_seq));
}

pub(crate) fn annotate_value_rows(
    rows: &[Value],
    run_id: &str,
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<Value> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            annotate_row_identity(
                &mut next,
                run_id,
                schedule_idx,
                slot_commit_id,
                attempt,
                row_seq,
            );
            next
        })
        .collect()
}

pub(crate) fn annotate_trial_rows(
    rows: &[TrialRecord],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<TrialRecord> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            next.schedule_idx = schedule_idx;
            next.slot_commit_id = slot_commit_id.to_string();
            next.attempt = attempt;
            next.row_seq = row_seq;
            next
        })
        .collect()
}

pub(crate) fn annotate_metric_rows(
    rows: &[MetricRow],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<MetricRow> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            next.schedule_idx = schedule_idx;
            next.slot_commit_id = slot_commit_id.to_string();
            next.attempt = attempt;
            next.row_seq = row_seq;
            next
        })
        .collect()
}

pub(crate) fn annotate_event_rows(
    rows: &[EventRow],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<EventRow> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            next.schedule_idx = schedule_idx;
            next.slot_commit_id = slot_commit_id.to_string();
            next.attempt = attempt;
            next.row_seq = row_seq;
            next
        })
        .collect()
}

pub(crate) fn annotate_variant_snapshot_rows(
    rows: &[VariantSnapshotRow],
    schedule_idx: usize,
    slot_commit_id: &str,
    attempt: usize,
) -> Vec<VariantSnapshotRow> {
    rows.iter()
        .enumerate()
        .map(|(row_seq, row)| {
            let mut next = row.clone();
            next.schedule_idx = schedule_idx;
            next.slot_commit_id = slot_commit_id.to_string();
            next.attempt = attempt;
            next.row_seq = row_seq;
            next
        })
        .collect()
}

impl RunCoordinator {
    fn commit_skipped_pruned_slot(
        run_dir: &Path,
        schedule_progress: &mut ScheduleProgress,
        schedule_idx: usize,
        run_sink: &mut dyn RunSink,
        slot_attempts: &mut HashMap<usize, usize>,
    ) -> Result<()> {
        let attempt = slot_attempts.get(&schedule_idx).copied().unwrap_or(0) + 1;
        let payload_digest = canonical_json_digest(&json!({
            "schedule_idx": schedule_idx,
            "status": "skipped_pruned"
        }));
        let slot_commit_id = make_slot_commit_id(
            &schedule_progress.run_id,
            schedule_idx,
            attempt,
            &payload_digest,
        );
        let empty_counts = SlotCommitRowCounts {
            trials: 0,
            metrics: 0,
            events: 0,
            variant_snapshots: 0,
            evidence: 0,
            chain_states: 0,
            conclusions: 0,
            predictions: 0,
            scores: 0,
        };
        append_slot_commit_record(
            run_dir,
            &SlotCommitRecord {
                schema_version: "slot_commit_record_v1".to_string(),
                record_type: "intent".to_string(),
                run_id: schedule_progress.run_id.clone(),
                schedule_idx,
                slot_commit_id: slot_commit_id.clone(),
                trial_id: String::new(),
                slot_status: "skipped_pruned".to_string(),
                attempt,
                recorded_at: Utc::now().to_rfc3339(),
                expected_rows: Some(empty_counts.clone()),
                payload_digest: Some(payload_digest),
                written_rows: None,
                facts_fsync_completed: None,
                runtime_fsync_completed: None,
            },
        )?;
        run_sink.flush()?;
        append_slot_commit_record(
            run_dir,
            &SlotCommitRecord {
                schema_version: "slot_commit_record_v1".to_string(),
                record_type: "commit".to_string(),
                run_id: schedule_progress.run_id.clone(),
                schedule_idx,
                slot_commit_id: slot_commit_id.clone(),
                trial_id: String::new(),
                slot_status: "skipped_pruned".to_string(),
                attempt,
                recorded_at: Utc::now().to_rfc3339(),
                expected_rows: None,
                payload_digest: None,
                written_rows: Some(empty_counts),
                facts_fsync_completed: Some(true),
                runtime_fsync_completed: Some(true),
            },
        )?;

        let mut next_progress = schedule_progress.clone();
        next_progress.completed_slots.push(SlotCompletion {
            schedule_index: schedule_idx,
            trial_id: String::new(),
            status: "skipped_pruned".to_string(),
            slot_commit_id,
            attempt,
        });
        next_progress.next_schedule_index = schedule_idx + 1;
        next_progress.updated_at = Utc::now().to_rfc3339();
        write_schedule_progress(run_dir, &next_progress)?;
        *schedule_progress = next_progress;
        emit_slot_commit_progress(
            &schedule_progress.run_id,
            schedule_progress.next_schedule_index,
            schedule_progress.total_slots,
            schedule_idx,
            "-",
            "skipped_pruned",
        );
        slot_attempts.insert(schedule_idx, attempt);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_trial_slot(
        run_dir: &Path,
        policy_config: &PolicyConfig,
        evidence_records_path: &Path,
        task_chain_states_path: &Path,
        benchmark_conclusions_path: &Path,
        schedule_progress: &mut ScheduleProgress,
        schedule_idx: usize,
        trial_index: usize,
        pruned_variants: &mut HashSet<usize>,
        consecutive_failures: &mut BTreeMap<usize, usize>,
        trial_result: &TrialExecutionResult,
        run_sink: &mut dyn RunSink,
        slot_attempts: &mut HashMap<usize, usize>,
    ) -> Result<()> {
        let attempt = slot_attempts.get(&schedule_idx).copied().unwrap_or(0) + 1;
        let payload_digest = slot_commit_payload_digest_for_result(schedule_idx, trial_result)?;
        let slot_commit_id = make_slot_commit_id(
            &schedule_progress.run_id,
            schedule_idx,
            attempt,
            &payload_digest,
        );
        let expected_rows = SlotCommitRowCounts {
            trials: trial_result.deferred_trial_records.len(),
            metrics: trial_result.deferred_metric_rows.len(),
            events: trial_result.deferred_event_rows.len(),
            variant_snapshots: trial_result.deferred_variant_snapshot_rows.len(),
            evidence: trial_result.deferred_evidence_records.len(),
            chain_states: trial_result.deferred_chain_state_records.len(),
            conclusions: trial_result.deferred_trial_conclusion_records.len(),
            predictions: 0,
            scores: 0,
        };
        append_slot_commit_record(
            run_dir,
            &SlotCommitRecord {
                schema_version: "slot_commit_record_v1".to_string(),
                record_type: "intent".to_string(),
                run_id: schedule_progress.run_id.clone(),
                schedule_idx,
                slot_commit_id: slot_commit_id.clone(),
                trial_id: trial_result.trial_id.clone(),
                slot_status: trial_result.slot_status.clone(),
                attempt,
                recorded_at: Utc::now().to_rfc3339(),
                expected_rows: Some(expected_rows.clone()),
                payload_digest: Some(payload_digest),
                written_rows: None,
                facts_fsync_completed: None,
                runtime_fsync_completed: None,
            },
        )?;

        let evidence_rows = annotate_value_rows(
            &trial_result.deferred_evidence_records,
            &schedule_progress.run_id,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for record in &evidence_rows {
            append_jsonl(evidence_records_path, record)?;
        }
        let chain_rows = annotate_value_rows(
            &trial_result.deferred_chain_state_records,
            &schedule_progress.run_id,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for record in &chain_rows {
            append_jsonl(task_chain_states_path, record)?;
        }
        let conclusion_rows = annotate_value_rows(
            &trial_result.deferred_trial_conclusion_records,
            &schedule_progress.run_id,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for row in &conclusion_rows {
            append_jsonl(benchmark_conclusions_path, row)?;
        }
        let trial_rows = annotate_trial_rows(
            &trial_result.deferred_trial_records,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        for row in &trial_rows {
            run_sink.append_trial_record(row)?;
        }
        let metric_rows = annotate_metric_rows(
            &trial_result.deferred_metric_rows,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        let event_rows = annotate_event_rows(
            &trial_result.deferred_event_rows,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        let snapshot_rows = annotate_variant_snapshot_rows(
            &trial_result.deferred_variant_snapshot_rows,
            schedule_idx,
            &slot_commit_id,
            attempt,
        );
        run_sink.append_metric_rows(&metric_rows)?;
        run_sink.append_event_rows(&event_rows)?;
        run_sink.append_variant_snapshot(&snapshot_rows)?;
        run_sink.flush()?;
        append_slot_commit_record(
            run_dir,
            &SlotCommitRecord {
                schema_version: "slot_commit_record_v1".to_string(),
                record_type: "commit".to_string(),
                run_id: schedule_progress.run_id.clone(),
                schedule_idx,
                slot_commit_id: slot_commit_id.clone(),
                trial_id: trial_result.trial_id.clone(),
                slot_status: trial_result.slot_status.clone(),
                attempt,
                recorded_at: Utc::now().to_rfc3339(),
                expected_rows: None,
                payload_digest: None,
                written_rows: Some(expected_rows),
                facts_fsync_completed: Some(true),
                runtime_fsync_completed: Some(true),
            },
        )?;

        let mut next_consecutive_failures = consecutive_failures.clone();
        let mut next_pruned_variants = pruned_variants.clone();
        if let Some(variant_idx) = trial_result.variant_idx {
            if trial_result.slot_status == "completed" {
                next_consecutive_failures.insert(variant_idx, 0);
            } else {
                *next_consecutive_failures.entry(variant_idx).or_default() += 1;
            }
            if let Some(max_failures) = policy_config.pruning_max_consecutive_failures {
                let count = next_consecutive_failures
                    .get(&variant_idx)
                    .copied()
                    .unwrap_or(0);
                if count >= max_failures {
                    next_pruned_variants.insert(variant_idx);
                }
            }
        }

        let mut next_progress = schedule_progress.clone();
        next_progress.completed_slots.push(SlotCompletion {
            schedule_index: schedule_idx,
            trial_id: trial_result.trial_id.clone(),
            status: trial_result.slot_status.clone(),
            slot_commit_id,
            attempt,
        });
        next_progress.next_schedule_index = schedule_idx + 1;
        next_progress.next_trial_index = trial_index;
        next_progress.pruned_variants = next_pruned_variants.iter().copied().collect();
        next_progress.consecutive_failures = next_consecutive_failures.clone();
        next_progress.updated_at = Utc::now().to_rfc3339();
        write_schedule_progress(run_dir, &next_progress)?;
        let _ = crate::trial::state::reconcile_trial_attempt_as_committed(
            &run_dir.join("trials").join(&trial_result.trial_id),
        );

        *schedule_progress = next_progress;
        emit_slot_commit_progress(
            &schedule_progress.run_id,
            schedule_progress.next_schedule_index,
            schedule_progress.total_slots,
            schedule_idx,
            &trial_result.trial_id,
            &trial_result.slot_status,
        );
        *consecutive_failures = next_consecutive_failures;
        *pruned_variants = next_pruned_variants;
        slot_attempts.insert(schedule_idx, attempt);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PendingSlotCommit {
    SkippedPruned,
    Trial(Box<TrialExecutionResult>),
}

pub(crate) struct DeterministicCommitter {
    pub(crate) next_commit_idx: usize,
    pub(crate) committed_keys: HashSet<String>,
    pub(crate) pending_by_schedule: BTreeMap<usize, PendingSlotCommit>,
    pub(crate) slot_attempts: HashMap<usize, usize>,
}

impl DeterministicCommitter {
    pub(crate) fn from_progress(
        progress: &ScheduleProgress,
        journal_records: &[SlotCommitRecord],
    ) -> Self {
        let mut committed_keys = HashSet::new();
        let mut slot_attempts = highest_attempt_by_schedule(journal_records);
        for slot in &progress.completed_slots {
            committed_keys.insert(Self::commit_key_for_slot_completion(slot));
            let entry = slot_attempts.entry(slot.schedule_index).or_insert(0);
            if slot.attempt > *entry {
                *entry = slot.attempt;
            }
        }
        Self {
            next_commit_idx: progress.next_schedule_index,
            committed_keys,
            pending_by_schedule: BTreeMap::new(),
            slot_attempts,
        }
    }

    pub(crate) fn commit_key_for_slot_completion(slot: &SlotCompletion) -> String {
        format!("{}:{}:{}", slot.schedule_index, slot.trial_id, slot.status)
    }

    fn commit_key_for_pending(schedule_idx: usize, pending: &PendingSlotCommit) -> String {
        match pending {
            PendingSlotCommit::SkippedPruned => {
                format!("{}::skipped_pruned", schedule_idx)
            }
            PendingSlotCommit::Trial(result) => {
                format!(
                    "{}:{}:{}",
                    schedule_idx, result.trial_id, result.slot_status
                )
            }
        }
    }

    pub(crate) fn enqueue_skipped(&mut self, schedule_idx: usize) -> Result<bool> {
        self.enqueue(schedule_idx, PendingSlotCommit::SkippedPruned)
    }

    pub(crate) fn enqueue_trial(
        &mut self,
        schedule_idx: usize,
        result: TrialExecutionResult,
    ) -> Result<bool> {
        self.enqueue(schedule_idx, PendingSlotCommit::Trial(Box::new(result)))
    }

    fn enqueue(&mut self, schedule_idx: usize, pending: PendingSlotCommit) -> Result<bool> {
        let pending_key = Self::commit_key_for_pending(schedule_idx, &pending);
        if self.committed_keys.contains(&pending_key) {
            return Ok(false);
        }
        if schedule_idx < self.next_commit_idx {
            return Err(anyhow!(
                "deterministic committer protocol fault: stale completion schedule_idx {} already committed through {}",
                schedule_idx,
                self.next_commit_idx.saturating_sub(1)
            ));
        }
        if let Some(existing) = self.pending_by_schedule.get(&schedule_idx) {
            let existing_key = Self::commit_key_for_pending(schedule_idx, existing);
            if existing_key == pending_key {
                return Ok(false);
            }
            return Err(anyhow!(
                "deterministic committer protocol fault: conflicting pending completion for schedule_idx {}",
                schedule_idx
            ));
        }
        self.pending_by_schedule.insert(schedule_idx, pending);
        Ok(true)
    }

    pub(crate) fn pending_trial_completion_records(&self) -> Vec<PendingTrialCompletionRecord> {
        let mut out = Vec::new();
        for (schedule_idx, pending) in &self.pending_by_schedule {
            if let PendingSlotCommit::Trial(result) = pending {
                out.push(PendingTrialCompletionRecord {
                    schema_version: "pending_trial_completion_v1".to_string(),
                    schedule_idx: *schedule_idx,
                    trial_result: (**result).clone(),
                });
            }
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn drain_ready(
        &mut self,
        run_dir: &Path,
        policy_config: &PolicyConfig,
        evidence_records_path: &Path,
        task_chain_states_path: &Path,
        benchmark_conclusions_path: &Path,
        schedule_progress: &mut ScheduleProgress,
        trial_index: usize,
        pruned_variants: &mut HashSet<usize>,
        consecutive_failures: &mut BTreeMap<usize, usize>,
        run_sink: &mut dyn RunSink,
    ) -> Result<usize> {
        let mut committed = 0_usize;
        while let Some(pending) = self.pending_by_schedule.remove(&self.next_commit_idx) {
            let schedule_idx = self.next_commit_idx;
            let commit_key = Self::commit_key_for_pending(schedule_idx, &pending);
            match pending {
                PendingSlotCommit::SkippedPruned => {
                    RunCoordinator::commit_skipped_pruned_slot(
                        run_dir,
                        schedule_progress,
                        schedule_idx,
                        run_sink,
                        &mut self.slot_attempts,
                    )?;
                }
                PendingSlotCommit::Trial(result) => {
                    RunCoordinator::commit_trial_slot(
                        run_dir,
                        policy_config,
                        evidence_records_path,
                        task_chain_states_path,
                        benchmark_conclusions_path,
                        schedule_progress,
                        schedule_idx,
                        trial_index,
                        pruned_variants,
                        consecutive_failures,
                        &result,
                        run_sink,
                        &mut self.slot_attempts,
                    )?;
                }
            }
            self.committed_keys.insert(commit_key);
            self.next_commit_idx = schedule_progress.next_schedule_index;
            committed += 1;
        }
        Ok(committed)
    }
}

#[derive(Clone)]
pub(crate) struct ParallelWorkerExecutionContext {
    run_dir: PathBuf,
    run_id: String,
    workload_type: String,
    project_root: PathBuf,
    variants: Vec<Variant>,
    tasks: Vec<Value>,
    policy_config: PolicyConfig,
    benchmark_config: BenchmarkConfig,
    variant_runtime_profiles: Vec<VariantRuntimeProfile>,
    materialize_mode: MaterializationMode,
    trials_dir: PathBuf,
    baseline_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InFlightDispatch {
    schedule_idx: usize,
    trial_id: String,
    variant_idx: usize,
    variant_id: String,
    worker_id: String,
    started_at: String,
}

pub(crate) struct LocalTrialLaunch {
    schedule_idx: usize,
    trial_id: String,
    slot: TrialSlot,
    trial_paths: TrialPaths,
}

#[derive(Debug)]
pub(crate) struct LocalTrialCompletion {
    trial_id: String,
    schedule_idx: usize,
    result: std::result::Result<TrialExecutionResult, String>,
}

pub(crate) fn in_flight_active_trials(
    in_flight: &HashMap<String, InFlightDispatch>,
) -> Vec<RunControlActiveTrial> {
    let mut active: Vec<RunControlActiveTrial> = in_flight
        .values()
        .map(|item| RunControlActiveTrial {
            trial_id: item.trial_id.clone(),
            worker_id: item.worker_id.clone(),
            schedule_idx: Some(item.schedule_idx),
            variant_id: Some(item.variant_id.clone()),
            started_at: Some(item.started_at.clone()),
            #[cfg(test)]
            control: None,
        })
        .collect();
    active.sort_by_key(|entry| entry.schedule_idx.unwrap_or(usize::MAX));
    active
}

pub(crate) fn execute_local_trial(
    context: &ParallelWorkerExecutionContext,
    launch: LocalTrialLaunch,
) -> Result<TrialExecutionResult> {
    let payload_dir = context
        .run_dir
        .join("runtime")
        .join("worker_payload")
        .join(&launch.trial_id);
    if payload_dir.exists() {
        fs::remove_dir_all(&payload_dir)?;
    }
    ensure_dir(&payload_dir)?;
    let payload_evidence = payload_dir.join("evidence_records.jsonl");
    let payload_chain = payload_dir.join("task_chain_states.jsonl");

    let mut local_trial_index = trial_index_from_trial_id(&launch.trial_id)
        .unwrap_or(launch.schedule_idx + 1)
        .saturating_sub(1);
    let mut local_chain_states: BTreeMap<String, ChainRuntimeState> = BTreeMap::new();
    let mut buffered_sink = BufferedRunSink::default();
    let artifact_store = ArtifactStore::new(context.run_dir.join("artifacts"));
    let execution = (|| -> Result<TrialExecutionResult> {
        let mut trial_result = crate::trial::schedule::execute_scheduled_trial(
            crate::trial::schedule::ScheduledTrialRequest {
                run_dir: &context.run_dir,
                run_id: &context.run_id,
                workload_type: &context.workload_type,
                project_root: &context.project_root,
                variants: &context.variants,
                tasks: &context.tasks,
                schedule_idx: launch.schedule_idx,
                slot: &launch.slot,
                policy_config: &context.policy_config,
                benchmark_config: &context.benchmark_config,
                variant_runtime_profiles: &context.variant_runtime_profiles,
                materialize_mode: context.materialize_mode,
                precomputed_trial_paths: Some(launch.trial_paths),
                trials_dir: &context.trials_dir,
                evidence_records_path: &payload_evidence,
                task_chain_states_path: &payload_chain,
                artifact_store: &artifact_store,
                trial_index: &mut local_trial_index,
                chain_states: &mut local_chain_states,
                baseline_id: &context.baseline_id,
                run_sink: &mut buffered_sink,
            },
        )?;
        trial_result.variant_idx = Some(launch.slot.variant_idx);
        trial_result.deferred_trial_records = buffered_sink.trial_records;
        trial_result.deferred_metric_rows = buffered_sink.metric_rows;
        trial_result.deferred_event_rows = buffered_sink.event_rows;
        trial_result.deferred_variant_snapshot_rows = buffered_sink.variant_snapshot_rows;
        trial_result.deferred_evidence_records = load_jsonl_value_rows(&payload_evidence)?;
        trial_result.deferred_chain_state_records = load_jsonl_value_rows(&payload_chain)?;
        Ok(trial_result)
    })();

    let _ = fs::remove_dir_all(&payload_dir);
    execution
}

pub(crate) fn spawn_local_trial(
    context: Arc<ParallelWorkerExecutionContext>,
    launch: LocalTrialLaunch,
    completion_tx: mpsc::Sender<LocalTrialCompletion>,
) -> Result<()> {
    let thread_name = format!("agentlab-{}", launch.trial_id);
    let completion_trial_id = launch.trial_id.clone();
    let completion_schedule_idx = launch.schedule_idx;
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_local_trial(context.as_ref(), launch)
            })) {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(err)) => Err(err.to_string()),
                Err(_) => Err("local trial execution panicked".to_string()),
            };
            let _ = completion_tx.send(LocalTrialCompletion {
                trial_id: completion_trial_id,
                schedule_idx: completion_schedule_idx,
                result,
            });
        })
        .map(|_| ())
        .map_err(|err| anyhow!("failed to spawn local trial thread: {}", err))
}

pub(crate) fn poll_local_trial_completions(
    completion_rx: &mpsc::Receiver<LocalTrialCompletion>,
    timeout: Duration,
) -> Result<Vec<LocalTrialCompletion>> {
    let first = if timeout.is_zero() {
        match completion_rx.try_recv() {
            Ok(completion) => Some(completion),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err(anyhow!("local scheduler completion channel disconnected"));
            }
        }
    } else {
        match completion_rx.recv_timeout(timeout) {
            Ok(completion) => Some(completion),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(anyhow!("local scheduler completion channel disconnected"));
            }
        }
    };

    let Some(first) = first else {
        return Ok(Vec::new());
    };

    let mut completions = vec![first];
    loop {
        match completion_rx.try_recv() {
            Ok(completion) => completions.push(completion),
            Err(mpsc::TryRecvError::Empty) => break,
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err(anyhow!("local scheduler completion channel disconnected"));
            }
        }
    }
    Ok(completions)
}

pub(crate) fn load_external_schedule_outcome_request(
    run_dir: &Path,
) -> Result<Option<ScheduleEngineOutcome>> {
    let run_control = load_json_file(&run_control_path(run_dir))?;
    let status = run_control
        .pointer("/status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    Ok(match status {
        "paused" => Some(ScheduleEngineOutcome::Paused),
        "killed" => Some(ScheduleEngineOutcome::Killed),
        _ => None,
    })
}

pub(crate) fn schedule_engine_status(
    requested_outcome: Option<ScheduleEngineOutcome>,
) -> &'static str {
    match requested_outcome {
        Some(ScheduleEngineOutcome::Paused) => "paused",
        Some(ScheduleEngineOutcome::Killed) => "killed",
        Some(ScheduleEngineOutcome::Interrupted) => "interrupted",
        _ => "running",
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_schedule_engine_local(
    mode: ScheduleEngineMode,
    run_dir: &Path,
    run_id: &str,
    workload_type: &str,
    project_root: &Path,
    dataset_path: &Path,
    variants: &[Variant],
    tasks: &[Value],
    schedule: &[TrialSlot],
    policy_config: &PolicyConfig,
    benchmark_config: &BenchmarkConfig,
    variant_runtime_profiles: &[VariantRuntimeProfile],
    behavior: &RunBehavior,
    materialize_mode: MaterializationMode,
    task_boundary_policy: &TaskBoundaryPolicy,
    trials_dir: &Path,
    evidence_dir: &Path,
    evidence_records_path: &Path,
    task_chain_states_path: &Path,
    schedule_progress: &mut ScheduleProgress,
    trial_index: &mut usize,
    consecutive_failures: &mut BTreeMap<usize, usize>,
    pruned_variants: &mut HashSet<usize>,
    recovered_active_trials: &[RunControlActiveTrial],
    baseline_id: &str,
    run_sink: &mut dyn RunSink,
    max_concurrency: usize,
) -> Result<ScheduleEngineOutcome> {
    let benchmark_dir = run_dir.join("benchmark");
    let benchmark_conclusions_path = benchmark_dir.join("conclusions.jsonl");

    let requested_dispatch_capacity = max_concurrency.max(1);
    let configured_ceiling = parse_local_worker_capacity_ceiling_from_env()?;
    let (dispatch_capacity, capacity_warning) =
        resolve_local_worker_max_in_flight(requested_dispatch_capacity, configured_ceiling);
    if let Some(warning) = capacity_warning {
        eprintln!("{}", warning);
    }

    let execution_context = Arc::new(ParallelWorkerExecutionContext {
        run_dir: run_dir.to_path_buf(),
        run_id: run_id.to_string(),
        workload_type: workload_type.to_string(),
        project_root: project_root.to_path_buf(),
        variants: variants.to_vec(),
        tasks: tasks.to_vec(),
        policy_config: policy_config.clone(),
        benchmark_config: benchmark_config.clone(),
        variant_runtime_profiles: variant_runtime_profiles.to_vec(),
        materialize_mode,
        trials_dir: trials_dir.to_path_buf(),
        baseline_id: baseline_id.to_string(),
    });
    let (completion_tx, completion_rx) = mpsc::channel::<LocalTrialCompletion>();
    let min_free_bytes = resolve_min_free_bytes()?;
    let max_run_bytes = parse_max_run_bytes_from_env()?;
    let disk_check_interval = Duration::from_secs(RUNTIME_DISK_HEADROOM_CHECK_INTERVAL_SECONDS);
    let run_size_check_interval = Duration::from_secs(RUNTIME_RUN_SIZE_CHECK_INTERVAL_SECONDS);
    let mut last_disk_check = Instant::now() - disk_check_interval;
    let mut last_run_size_check = Instant::now() - run_size_check_interval;

    let journal_records = load_slot_commit_records(run_dir)?;
    let mut committer = DeterministicCommitter::from_progress(schedule_progress, &journal_records);
    let persisted_pending = load_pending_trial_completion_records(run_dir)?;
    for (schedule_idx, result) in &persisted_pending {
        if *schedule_idx < schedule_progress.next_schedule_index || *schedule_idx >= schedule.len()
        {
            continue;
        }
        committer.enqueue_trial(*schedule_idx, result.clone())?;
    }
    if !recovered_active_trials.is_empty() {
        let mut variant_idx_by_id: HashMap<String, usize> = HashMap::new();
        for (idx, variant) in variants.iter().enumerate() {
            variant_idx_by_id.insert(variant.id.clone(), idx);
        }
        for recovered in recovered_active_trials {
            let Some(schedule_idx) = recovered.schedule_idx else {
                continue;
            };
            if schedule_idx < schedule_progress.next_schedule_index
                || schedule_idx >= schedule.len()
            {
                continue;
            }
            // If we already persisted a completion for this slot, prefer it over recovered worker_lost.
            if persisted_pending.contains_key(&schedule_idx) {
                continue;
            }
            let variant_idx = recovered
                .variant_id
                .as_ref()
                .and_then(|id| variant_idx_by_id.get(id).copied());
            let result = TrialExecutionResult::worker_lost(
                recovered.trial_id.clone(),
                variant_idx,
                Some("worker_lost".to_string()),
            );
            let _ = crate::trial::state::reconcile_trial_attempt_as_abandoned(
                &run_dir.join("trials").join(&recovered.trial_id),
            );
            committer.enqueue_trial(schedule_idx, result)?;
        }
    }
    persist_pending_trial_completions(run_dir, &committer)?;

    let mut next_dispatch_idx = schedule_progress.next_schedule_index;
    let mut in_flight: HashMap<String, InFlightDispatch> = HashMap::new();
    let mut in_flight_by_variant: BTreeMap<usize, usize> = BTreeMap::new();

    committer.drain_ready(
        run_dir,
        policy_config,
        evidence_records_path,
        task_chain_states_path,
        &benchmark_conclusions_path,
        schedule_progress,
        *trial_index,
        pruned_variants,
        consecutive_failures,
        run_sink,
    )?;
    persist_pending_trial_completions(run_dir, &committer)?;
    write_run_control_v2(
        run_dir,
        run_id,
        "running",
        &in_flight_active_trials(&in_flight),
        None,
    )?;
    let mut requested_outcome: Option<ScheduleEngineOutcome> = None;

    while committer.next_commit_idx < schedule.len() || !in_flight.is_empty() {
        if INTERRUPTED.load(Ordering::SeqCst) {
            emit_run_log(
                run_id,
                "received interrupt signal, shutting down gracefully",
            );
            write_run_control_v2(
                run_dir,
                run_id,
                "interrupted",
                &in_flight_active_trials(&in_flight),
                None,
            )?;
            return Ok(ScheduleEngineOutcome::Interrupted);
        }
        if let Some(external_outcome) = load_external_schedule_outcome_request(run_dir)? {
            requested_outcome = Some(external_outcome);
        }

        if last_disk_check.elapsed() >= disk_check_interval {
            enforce_runtime_disk_headroom(run_dir, min_free_bytes)?;
            last_disk_check = Instant::now();
        }
        if let Some(max_bytes) = max_run_bytes {
            if last_run_size_check.elapsed() >= run_size_check_interval {
                enforce_runtime_run_size_budget(run_dir, max_bytes)?;
                last_run_size_check = Instant::now();
            }
        }

        let mut made_progress = false;

        while requested_outcome.is_none()
            && next_dispatch_idx < schedule.len()
            && in_flight.len() < dispatch_capacity
        {
            let slot = &schedule[next_dispatch_idx];
            if pruned_variants.contains(&slot.variant_idx) {
                committer.enqueue_skipped(next_dispatch_idx)?;
                next_dispatch_idx += 1;
                made_progress = true;
                continue;
            }
            if let Some(limit) = policy_config.concurrency.max_in_flight_per_variant {
                let variant_in_flight = in_flight_by_variant
                    .get(&slot.variant_idx)
                    .copied()
                    .unwrap_or(0);
                if variant_in_flight >= limit {
                    break;
                }
            }

            let proposed_trial_index = trial_index.saturating_add(1);
            let trial_id = format!("trial_{}", proposed_trial_index);
            let variant = &variants[slot.variant_idx];
            let trial_dir = trials_dir.join(&trial_id);
            ensure_dir(&trial_dir)?;
            let trial_paths = TrialPaths::new(&trial_dir, project_root)?;
            trial_paths.prepare(false)?;
            let launch = LocalTrialLaunch {
                schedule_idx: next_dispatch_idx,
                trial_id: trial_id.clone(),
                slot: slot.clone(),
                trial_paths,
            };
            spawn_local_trial(execution_context.clone(), launch, completion_tx.clone())?;
            *trial_index = proposed_trial_index;
            let started_at = Utc::now().to_rfc3339();
            in_flight.insert(
                trial_id.clone(),
                InFlightDispatch {
                    schedule_idx: next_dispatch_idx,
                    trial_id: trial_id.clone(),
                    variant_idx: slot.variant_idx,
                    variant_id: variant.id.clone(),
                    worker_id: RUN_CONTROL_UNKNOWN_WORKER_ID.to_string(),
                    started_at,
                },
            );
            *in_flight_by_variant.entry(slot.variant_idx).or_default() += 1;
            next_dispatch_idx += 1;
            made_progress = true;
            write_run_control_v2(
                run_dir,
                run_id,
                schedule_engine_status(requested_outcome),
                &in_flight_active_trials(&in_flight),
                None,
            )?;
        }

        let committed = committer.drain_ready(
            run_dir,
            policy_config,
            evidence_records_path,
            task_chain_states_path,
            &benchmark_conclusions_path,
            schedule_progress,
            *trial_index,
            pruned_variants,
            consecutive_failures,
            run_sink,
        )?;
        persist_pending_trial_completions(run_dir, &committer)?;
        if committed > 0 {
            made_progress = true;
        }

        if committer.next_commit_idx >= schedule.len() && in_flight.is_empty() {
            break;
        }
        if let Some(outcome) = requested_outcome {
            if in_flight.is_empty() {
                return Ok(outcome);
            }
        }

        let poll_timeout = if made_progress {
            Duration::from_millis(0)
        } else {
            Duration::from_millis(50)
        };
        let completions = poll_local_trial_completions(&completion_rx, poll_timeout)?;
        if completions.is_empty() {
            continue;
        }

        for completion in completions {
            let in_flight_entry =
                in_flight
                    .remove(completion.trial_id.as_str())
                    .ok_or_else(|| {
                        anyhow!(
                            "local scheduler protocol fault: completion for unknown trial {}",
                            completion.trial_id
                        )
                    })?;
            if completion.schedule_idx != in_flight_entry.schedule_idx {
                return Err(anyhow!(
                    "local scheduler protocol fault: completion schedule_idx {} did not match dispatched schedule_idx {}",
                    completion.schedule_idx,
                    in_flight_entry.schedule_idx
                ));
            }
            if let Some(count) = in_flight_by_variant.get_mut(&in_flight_entry.variant_idx) {
                if *count > 0 {
                    *count -= 1;
                }
                if *count == 0 {
                    in_flight_by_variant.remove(&in_flight_entry.variant_idx);
                }
            }
            let mut trial_result = match completion.result {
                Ok(result) => result,
                Err(detail) => {
                    return Err(anyhow!(
                        "local trial execution failed (trial_id={}, schedule_idx={}): {}",
                        in_flight_entry.trial_id,
                        in_flight_entry.schedule_idx,
                        detail
                    ));
                }
            };
            if trial_result.trial_id != in_flight_entry.trial_id {
                return Err(anyhow!(
                    "local scheduler protocol fault: completion trial_id mismatch: expected {}, got {}",
                    in_flight_entry.trial_id,
                    trial_result.trial_id
                ));
            }
            if trial_result.variant_idx.is_none() {
                trial_result.variant_idx = Some(in_flight_entry.variant_idx);
            }
            committer.enqueue_trial(in_flight_entry.schedule_idx, trial_result)?;
        }
        persist_pending_trial_completions(run_dir, &committer)?;

        write_run_control_v2(
            run_dir,
            run_id,
            schedule_engine_status(requested_outcome),
            &in_flight_active_trials(&in_flight),
            None,
        )?;
        committer.drain_ready(
            run_dir,
            policy_config,
            evidence_records_path,
            task_chain_states_path,
            &benchmark_conclusions_path,
            schedule_progress,
            *trial_index,
            pruned_variants,
            consecutive_failures,
            run_sink,
        )?;
        persist_pending_trial_completions(run_dir, &committer)?;
        if let Some(outcome) = requested_outcome {
            if in_flight.is_empty() {
                return Ok(outcome);
            }
        }
    }

    committer.drain_ready(
        run_dir,
        policy_config,
        evidence_records_path,
        task_chain_states_path,
        &benchmark_conclusions_path,
        schedule_progress,
        *trial_index,
        pruned_variants,
        consecutive_failures,
        run_sink,
    )?;
    persist_pending_trial_completions(run_dir, &committer)?;
    write_run_control_v2(
        run_dir,
        run_id,
        schedule_engine_status(requested_outcome),
        &in_flight_active_trials(&in_flight),
        None,
    )?;
    Ok(requested_outcome.unwrap_or(ScheduleEngineOutcome::Completed))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_schedule_engine(
    mode: ScheduleEngineMode,
    run_dir: &Path,
    run_id: &str,
    workload_type: &str,
    project_root: &Path,
    dataset_path: &Path,
    variants: &[Variant],
    tasks: &[Value],
    schedule: &[TrialSlot],
    policy_config: &PolicyConfig,
    benchmark_config: &BenchmarkConfig,
    variant_runtime_profiles: &[VariantRuntimeProfile],
    behavior: &RunBehavior,
    materialize_mode: MaterializationMode,
    task_boundary_policy: &TaskBoundaryPolicy,
    trials_dir: &Path,
    evidence_dir: &Path,
    evidence_records_path: &Path,
    task_chain_states_path: &Path,
    schedule_progress: &mut ScheduleProgress,
    trial_index: &mut usize,
    consecutive_failures: &mut BTreeMap<usize, usize>,
    pruned_variants: &mut HashSet<usize>,
    recovered_active_trials: &[RunControlActiveTrial],
    baseline_id: &str,
    run_sink: &mut dyn RunSink,
    max_concurrency: usize,
) -> Result<ScheduleEngineOutcome> {
    if !matches!(policy_config.state, StatePolicy::IsolatePerTrial) {
        return Err(anyhow!(
            "local async docker path supports only isolate_per_trial state policy; got {:?}",
            policy_config.state
        ));
    }
    execute_schedule_engine_local(
        mode,
        run_dir,
        run_id,
        workload_type,
        project_root,
        dataset_path,
        variants,
        tasks,
        schedule,
        policy_config,
        benchmark_config,
        variant_runtime_profiles,
        behavior,
        materialize_mode,
        task_boundary_policy,
        trials_dir,
        evidence_dir,
        evidence_records_path,
        task_chain_states_path,
        schedule_progress,
        trial_index,
        consecutive_failures,
        pruned_variants,
        recovered_active_trials,
        baseline_id,
        run_sink,
        max_concurrency,
    )
}
pub(crate) fn load_authoring_input_for_build(
    path: &Path,
    overrides_path: Option<&Path>,
) -> Result<LoadedExperimentInput> {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if canonical.is_dir() {
        return Err(anyhow!(
            "build_input_invalid_kind: expected authoring spec file, got directory '{}'",
            canonical.display()
        ));
    }

    if canonical
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "manifest.json")
    {
        return Err(anyhow!(
            "build_input_invalid_kind: expected authoring spec file, got sealed package manifest"
        ));
    }

    let exp_dir = canonical
        .parent()
        .unwrap_or(Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));
    let project_root = find_project_root(&exp_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&exp_dir));
    let raw_yaml = fs::read_to_string(&canonical)?;
    let yaml_value: serde_yaml::Value = serde_yaml::from_str(&raw_yaml)?;
    let mut json_value: Value = serde_json::to_value(yaml_value)?;
    if let Some(overrides_path) = overrides_path {
        json_value = apply_experiment_overrides(json_value, overrides_path, &project_root)?;
    }
    json_value = normalize_experiment_authoring(json_value, &exp_dir, &project_root)?;
    Ok(LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root,
    })
}

pub(crate) fn as_portable_rel(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

pub(crate) fn copy_path_into_package(source: &Path, destination: &Path) -> Result<()> {
    if source.is_dir() {
        ensure_dir(destination)?;
        return copy_dir_preserve_all(source, destination, &[]);
    }
    if source.is_file() {
        if let Some(parent) = destination.parent() {
            ensure_dir(parent)?;
        }
        fs::copy(source, destination)?;
        return Ok(());
    }
    Err(anyhow!(
        "package build expected file or directory source, got: {}",
        source.display()
    ))
}

pub(crate) fn packaged_task_bundle_rel_path(
    task_id: &str,
    task_idx: usize,
    source: Option<&Path>,
) -> PathBuf {
    let stem = format!("{}_{}", sanitize_for_fs(task_id), task_idx + 1);
    let base = PathBuf::from("tasks").join("task_bundles");
    let Some(source) = source else {
        return base.join(stem);
    };
    let Some(name) = source.file_name().and_then(|value| value.to_str()) else {
        return base.join(stem);
    };
    if source.is_dir() {
        return base.join(stem);
    }
    base.join(format!("{}_{}", stem, name))
}

pub(crate) fn resolve_task_bundle_source_for_package(
    raw: &str,
    dataset_dir: &Path,
    exp_dir: &Path,
) -> Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("task bundle ref cannot be empty"));
    }
    let source = Path::new(trimmed);
    if source.is_absolute() {
        return Ok(source.to_path_buf());
    }
    let dataset_candidate = dataset_dir.join(source);
    if dataset_candidate.exists() {
        return Ok(dataset_candidate);
    }
    let exp_candidate = exp_dir.join(source);
    if exp_candidate.exists() {
        return Ok(exp_candidate);
    }
    Err(anyhow!(
        "task bundle ref '{}' could not be resolved relative to dataset or experiment directory",
        raw
    ))
}

pub(crate) fn stage_task_row_bundle_for_package(
    task_row: &TaskRow,
    task_idx: usize,
    dataset_dir: &Path,
    exp_dir: &Path,
    package_dir: &Path,
) -> Result<TaskRow> {
    let mut staged = task_row.clone();
    if !matches!(
        staged.materialization.kind,
        TaskMaterializationKind::BaseImageBundle
    ) {
        return Ok(staged);
    }
    let raw_bundle_ref = staged
        .materialization
        .task_bundle_ref
        .as_deref()
        .ok_or_else(|| {
            anyhow!(
                "task '{}' is missing materialization.task_bundle_ref for base_image_bundle",
                staged.id
            )
        })?;
    let source = resolve_task_bundle_source_for_package(raw_bundle_ref, dataset_dir, exp_dir)?;
    let bundle_rel = packaged_task_bundle_rel_path(&staged.id, task_idx, Some(&source));
    copy_path_into_package(&source, &package_dir.join(&bundle_rel))?;
    staged.materialization.task_bundle_ref = Some(as_portable_rel(&bundle_rel));
    Ok(staged)
}

pub(crate) fn compile_tasks_for_package(
    tasks: &[Value],
    _project_root: &Path,
    exp_dir: &Path,
    dataset_path: &Path,
    package_dir: &Path,
) -> Result<Vec<Value>> {
    let dataset_dir = dataset_path.parent().unwrap_or(exp_dir);
    let mut compiled = Vec::with_capacity(tasks.len());
    for (idx, task) in tasks.iter().enumerate() {
        let task_row = parse_task_row(task).with_context(|| {
            format!("package build task {} is not a valid task_row_v1", idx + 1)
        })?;
        let row =
            stage_task_row_bundle_for_package(&task_row, idx, dataset_dir, exp_dir, package_dir)?;
        compiled.push(serde_json::to_value(row)?);
    }
    Ok(compiled)
}

pub(crate) fn write_packaged_tasks(path: &Path, tasks: &[Value]) -> Result<()> {
    let mut bytes = Vec::new();
    for task in tasks {
        serde_json::to_writer(&mut bytes, task)?;
        bytes.push(b'\n');
    }
    atomic_write_bytes(path, &bytes)
}

pub(crate) fn stage_source_into_package(
    raw_source: &str,
    exp_dir: &Path,
    package_dir: &Path,
    subdir: &str,
    prefix: &str,
    copies: &mut BTreeMap<String, String>,
    counter: &mut usize,
) -> Result<String> {
    let raw_path = PathBuf::from(raw_source);
    let resolved = if raw_path.is_absolute() {
        normalize_path(&raw_path)
    } else {
        normalize_path(&exp_dir.join(raw_path))
    };
    let key = resolved.to_string_lossy().to_string();
    if let Some(existing) = copies.get(&key) {
        return Ok(existing.clone());
    }
    fs::metadata(&resolved).with_context(|| {
        format!(
            "package build failed to read staged source '{}' resolved from '{}'",
            resolved.display(),
            raw_source
        )
    })?;
    let name = resolved
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}_{}", prefix, counter));
    let rel_path = PathBuf::from(subdir).join(format!("{:03}_{}", *counter, name));
    let destination = package_dir.join(&rel_path);
    copy_path_into_package(&resolved, &destination)?;
    *counter += 1;
    let rel_portable = as_portable_rel(&rel_path);
    copies.insert(key, rel_portable.clone());
    Ok(rel_portable)
}

pub(crate) fn stage_public_runtime_path_reference(
    rel: &Path,
    exp_dir: &Path,
    package_dir: &Path,
    copies: &mut BTreeMap<String, String>,
    manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
    field_name: &str,
) -> Result<String> {
    let rel_portable = as_portable_rel(rel);
    let resolved = normalize_path(&exp_dir.join(&rel));
    fs::metadata(&resolved).with_context(|| {
        format!(
            "package build failed to read {} public path reference '{}' resolved to '{}'",
            field_name,
            rel_portable,
            resolved.display()
        )
    })?;
    if copies.contains_key(&rel_portable) {
        return Ok(task_workdir_support_destination_path(&rel_portable));
    }
    let packaged_rel = PathBuf::from(PACKAGED_RUNTIME_ASSETS_DIR).join(rel);
    let packaged_rel_portable = as_portable_rel(&packaged_rel);
    let destination = package_dir.join(&packaged_rel);
    copy_path_into_package(&resolved, &destination)?;
    copies.insert(rel_portable.clone(), packaged_rel_portable.clone());
    manifest_entries.push(RuntimePathStagingManifestEntry {
        original_relative_path: rel_portable.clone(),
        packaged_path: packaged_rel_portable,
        runtime_path: task_workdir_support_destination_path(&rel_portable),
        required: true,
        read_only: true,
    });
    Ok(task_workdir_support_destination_path(&rel_portable))
}

pub(crate) fn is_runner_staged_destination_path(raw: &str) -> bool {
    strip_task_workdir_support_destination_path(raw).is_some()
        || raw == AGENTLAB_CONTRACT_RUNTIME_AUX_DIR
        || raw.starts_with(&format!("{}/", AGENTLAB_CONTRACT_RUNTIME_AUX_DIR))
}

pub(crate) fn rewrite_packaged_runtime_asset_entries(
    entries: Option<&mut Value>,
    field_name: &str,
    exp_dir: &Path,
    package_dir: &Path,
    file_copies: &mut BTreeMap<String, String>,
    file_counter: &mut usize,
) -> Result<()> {
    let Some(items) = entries.and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for (idx, item) in items.iter_mut().enumerate() {
        let raw = item
            .get("build_source_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].build_source_path is required", field_name, idx))?;
        let rel = stage_source_into_package(
            raw,
            exp_dir,
            package_dir,
            PACKAGED_RUNTIME_ASSETS_DIR,
            "dep",
            file_copies,
            file_counter,
        )
        .with_context(|| {
            format!(
                "failed to stage {}[{}].build_source_path '{}' into sealed package",
                field_name, idx, raw
            )
        })?;
        if let Some(obj) = item.as_object_mut() {
            obj.remove("build_source_path");
        }
        set_json_pointer_value(item, "/packaged_path", json!(rel))?;
    }
    Ok(())
}

pub(crate) fn rewrite_optional_package_source_path(
    value: Option<&mut Value>,
    field_name: &str,
    exp_dir: &Path,
    package_dir: &Path,
    subdir: &str,
    prefix: &str,
    file_copies: &mut BTreeMap<String, String>,
    file_counter: &mut usize,
) -> Result<()> {
    let Some(item) = value else {
        return Ok(());
    };
    let Some(raw) = item
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let rel = stage_source_into_package(
        raw,
        exp_dir,
        package_dir,
        subdir,
        prefix,
        file_copies,
        file_counter,
    )
    .with_context(|| {
        format!(
            "failed to stage {} '{}' into sealed package",
            field_name, raw
        )
    })?;
    *item = Value::String(rel);
    Ok(())
}

pub(crate) fn stage_optional_public_runtime_path_for_package(
    value: Option<&mut Value>,
    field_name: &str,
    exp_dir: &Path,
    package_dir: &Path,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    let Some(item) = value else {
        return Ok(());
    };
    let Some(raw) = item
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    if is_runner_staged_destination_path(raw) {
        return Ok(());
    }
    let Some(rel) = resolve_existing_public_path_reference(raw, exp_dir, field_name)? else {
        return Ok(());
    };
    let contract_path = stage_public_runtime_path_reference(
        &rel,
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
        field_name,
    )?;
    *item = Value::String(contract_path);
    Ok(())
}

pub(crate) fn stage_command_path_refs_for_package(
    command_root: Option<&mut Value>,
    field_name: &str,
    exp_dir: &Path,
    package_dir: &Path,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    let Some(items) = command_root.and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for idx in 0..items.len() {
        let token = items[idx]
            .as_str()
            .ok_or_else(|| anyhow!("{}[{}] must be a string", field_name, idx))?;
        if contains_removed_runtime_template(token) {
            return Err(anyhow!(
                "{}[{}] uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                field_name,
                idx
            ));
        }
        if idx == 0 {
            continue;
        }
        if is_runner_staged_destination_path(token) {
            continue;
        }
        let Some(rel) = resolve_existing_public_path_reference(
            token,
            exp_dir,
            &format!("{}[{}]", field_name, idx),
        )?
        else {
            continue;
        };
        let contract_path = stage_public_runtime_path_reference(
            &rel,
            exp_dir,
            package_dir,
            public_path_copies,
            staging_manifest_entries,
            &format!("{}[{}]", field_name, idx),
        )?;
        items[idx] = Value::String(contract_path);
    }
    Ok(())
}

pub(crate) fn stage_runtime_command_env_path_refs_for_package(
    runtime_root: &mut Value,
    exp_dir: &Path,
    package_dir: &Path,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    stage_command_path_refs_for_package(
        runtime_root.pointer_mut("/agent_runtime/command"),
        "runtime.agent_runtime.command",
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    if let Some(items) = runtime_root
        .pointer_mut("/agent_runtime/env")
        .and_then(Value::as_object_mut)
    {
        let keys = items.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let raw = items
                .get(&key)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("runtime.agent_runtime.env.{} must be a string", key))?;
            if contains_removed_runtime_template(raw) {
                return Err(anyhow!(
                    "runtime.agent_runtime.env.{} uses removed '${{...}}' syntax; use $NAME runtime bindings instead",
                    key
                ));
            }
            if raw.trim().starts_with("/agentlab/") {
                return Err(anyhow!(
                    "runtime.agent_runtime.env.{} leaks runner topology; remove internal /agentlab paths from public authoring",
                    key
                ));
            }
            if is_runner_staged_destination_path(raw) {
                continue;
            }
            let Some(rel) = resolve_existing_public_path_reference(
                raw,
                exp_dir,
                &format!("runtime.agent_runtime.env.{}", key),
            )?
            else {
                continue;
            };
            let contract_path = stage_public_runtime_path_reference(
                &rel,
                exp_dir,
                package_dir,
                public_path_copies,
                staging_manifest_entries,
                &format!("runtime.agent_runtime.env.{}", key),
            )?;
            items.insert(key, Value::String(contract_path));
        }
    }
    Ok(())
}

pub(crate) fn collect_command_staging_entries(
    command_root: Option<&Value>,
    field_name: &str,
    catalog: &BTreeMap<String, RuntimePathStagingManifestEntry>,
    seen: &mut HashSet<String>,
    entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    let Some(items) = command_root.and_then(Value::as_array) else {
        return Ok(());
    };
    for (idx, item) in items.iter().enumerate() {
        if idx == 0 {
            continue;
        }
        let Some(runtime_path) = item.as_str().map(str::trim) else {
            return Err(anyhow!("{}[{}] must be a string", field_name, idx));
        };
        if strip_task_workdir_support_destination_path(runtime_path).is_none() {
            continue;
        }
        if !seen.insert(runtime_path.to_string()) {
            continue;
        }
        let entry = lookup_runtime_staging_entry(catalog, runtime_path).ok_or_else(|| {
            anyhow!(
                "{}[{}] references packaged dependency '{}' with no staging manifest entry",
                field_name,
                idx,
                runtime_path
            )
        })?;
        entries.push(entry);
    }
    Ok(())
}

pub(crate) fn collect_runtime_command_env_staging_entries(
    experiment: &Value,
    catalog: &BTreeMap<String, RuntimePathStagingManifestEntry>,
) -> Result<Vec<RuntimePathStagingManifestEntry>> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();

    collect_command_staging_entries(
        experiment.pointer("/runtime/agent_runtime/command"),
        "runtime.agent_runtime.command",
        catalog,
        &mut seen,
        &mut entries,
    )?;
    collect_command_staging_entries(
        experiment.pointer("/benchmark/grader/command"),
        "benchmark.grader.command",
        catalog,
        &mut seen,
        &mut entries,
    )?;
    collect_command_staging_entries(
        experiment.pointer("/benchmark/adapter/command"),
        "benchmark.adapter.command",
        catalog,
        &mut seen,
        &mut entries,
    )?;

    if let Some(items) = experiment
        .pointer("/runtime/agent_runtime/env")
        .and_then(Value::as_object)
    {
        for (key, value) in items {
            let Some(runtime_path) = value.as_str().map(str::trim) else {
                return Err(anyhow!(
                    "runtime.agent_runtime.env.{} must be a string",
                    key
                ));
            };
            if strip_task_workdir_support_destination_path(runtime_path).is_none() {
                continue;
            }
            if !seen.insert(runtime_path.to_string()) {
                continue;
            }
            let entry = lookup_runtime_staging_entry(catalog, runtime_path).ok_or_else(|| {
                anyhow!(
                    "runtime.agent_runtime.env.{} references packaged dependency '{}' with no staging manifest entry",
                    key,
                    runtime_path
                )
            })?;
            entries.push(entry);
        }
    }

    Ok(entries)
}

pub(crate) fn lookup_runtime_staging_entry(
    catalog: &BTreeMap<String, RuntimePathStagingManifestEntry>,
    runtime_path: &str,
) -> Option<RuntimePathStagingManifestEntry> {
    if let Some(entry) = catalog.get(runtime_path) {
        return Some(entry.clone());
    }
    catalog
        .values()
        .filter(|entry| matches_contract_runtime_root(runtime_path, &entry.runtime_path))
        .max_by_key(|entry| entry.runtime_path.len())
        .cloned()
}

pub(crate) fn matches_contract_runtime_root(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn collect_packaged_runtime_asset_entries(
    value: Option<&Value>,
    field_name: &str,
) -> Result<Vec<RuntimePathStagingManifestEntry>> {
    let Some(items) = value else {
        return Ok(Vec::new());
    };
    let arr = items
        .as_array()
        .ok_or_else(|| anyhow!("{} must be an array", field_name))?;
    let mut entries = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| anyhow!("{}[{}] must be an object", field_name, idx))?;
        let packaged_path = obj
            .get("packaged_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].packaged_path is required", field_name, idx))?;
        let runtime_path = obj
            .get("runtime_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{}[{}].runtime_path is required", field_name, idx))?;
        entries.push(RuntimePathStagingManifestEntry {
            original_relative_path: packaged_path.to_string(),
            packaged_path: validate_dx_support_file_relpath(
                packaged_path,
                &format!("{}[{}].packaged_path", field_name, idx),
            )?,
            runtime_path: validate_runner_staged_destination_path(
                runtime_path,
                &format!("{}[{}].runtime_path", field_name, idx),
            )?,
            required: obj.get("required").and_then(Value::as_bool).unwrap_or(true),
            read_only: obj
                .get("read_only")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        });
    }
    Ok(entries)
}

pub(crate) fn merge_runtime_path_staging_entries(
    base: &mut Vec<RuntimePathStagingManifestEntry>,
    extra: Vec<RuntimePathStagingManifestEntry>,
) {
    for next in extra {
        if let Some(existing) = base
            .iter_mut()
            .find(|entry| entry.runtime_path == next.runtime_path)
        {
            *existing = next;
        } else {
            base.push(next);
        }
    }
}

pub(crate) fn write_runtime_staging_manifest(
    package_dir: &Path,
    experiment: &Value,
    entries: &[RuntimePathStagingManifestEntry],
) -> Result<()> {
    let (variants, _) = resolve_variant_plan(experiment)?;
    let mut variants_manifest: BTreeMap<String, Vec<RuntimePathStagingManifestEntry>> =
        BTreeMap::new();
    for variant in &variants {
        let variant_experiment = resolve_runtime_for_variant(experiment, variant)?;
        let mut variant_catalog_entries = entries.to_vec();
        merge_runtime_path_staging_entries(
            &mut variant_catalog_entries,
            collect_packaged_runtime_asset_entries(
                variant_experiment.pointer("/benchmark/grader/_runtime_assets"),
                "benchmark.grader._runtime_assets",
            )?,
        );
        merge_runtime_path_staging_entries(
            &mut variant_catalog_entries,
            collect_packaged_runtime_asset_entries(
                variant_experiment.pointer("/benchmark/adapter/_runtime_assets"),
                "benchmark.adapter._runtime_assets",
            )?,
        );
        let variant_catalog = variant_catalog_entries
            .iter()
            .cloned()
            .map(|entry| (entry.runtime_path.clone(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut variant_entries =
            collect_runtime_command_env_staging_entries(&variant_experiment, &variant_catalog)?;
        merge_runtime_path_staging_entries(&mut variant_entries, variant_catalog_entries);
        variant_entries.sort_by(|left, right| {
            left.runtime_path
                .cmp(&right.runtime_path)
                .then(left.packaged_path.cmp(&right.packaged_path))
        });
        for (idx, entry) in variant_entries.iter().enumerate() {
            let packaged_source = resolve_package_path_under_root(
                package_dir,
                &entry.packaged_path,
                &format!(
                    "staging_manifest.variants.{}[{}].packaged_path",
                    variant.id, idx
                ),
            )?;
            fs::metadata(&packaged_source).with_context(|| {
                format!(
                    "failed to read packaged runtime staging source '{}' for variant '{}'",
                    packaged_source.display(),
                    variant.id
                )
            })?;
        }
        variants_manifest.insert(variant.id.clone(), variant_entries);
    }
    let manifest_value = serde_json::to_value(RuntimePathStagingManifest {
        schema_version: STAGING_MANIFEST_SCHEMA_VERSION.to_string(),
        variants: variants_manifest,
    })?;
    atomic_write_json_pretty(&package_dir.join(STAGING_MANIFEST_FILE), &manifest_value)
}

pub(crate) fn rewrite_runtime_paths_for_package(
    runtime_root: &mut Value,
    exp_dir: &Path,
    package_dir: &Path,
    artifact_copies: &mut BTreeMap<String, String>,
    _file_copies: &mut BTreeMap<String, String>,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
    artifact_counter: &mut usize,
    _file_counter: &mut usize,
) -> Result<()> {
    if let Some(raw) = runtime_root
        .pointer("/agent_runtime/artifact")
        .and_then(Value::as_str)
    {
        let rel = stage_source_into_package(
            raw,
            exp_dir,
            package_dir,
            "agent_builds",
            "build",
            artifact_copies,
            artifact_counter,
        )?;
        set_json_pointer_value(runtime_root, "/agent_runtime/artifact", json!(rel.clone()))?;
        set_json_pointer_value(
            runtime_root,
            "/agent_runtime/artifact_resolved_path",
            json!(rel),
        )?;
    }
    stage_runtime_command_env_path_refs_for_package(
        runtime_root,
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    Ok(())
}

pub(crate) fn rewrite_benchmark_paths_for_package(
    benchmark_root: &mut Value,
    exp_dir: &Path,
    package_dir: &Path,
    file_copies: &mut BTreeMap<String, String>,
    file_counter: &mut usize,
    public_path_copies: &mut BTreeMap<String, String>,
    staging_manifest_entries: &mut Vec<RuntimePathStagingManifestEntry>,
) -> Result<()> {
    rewrite_packaged_runtime_asset_entries(
        benchmark_root.pointer_mut("/grader/_runtime_assets"),
        "benchmark.grader._runtime_assets",
        exp_dir,
        package_dir,
        file_copies,
        file_counter,
    )?;
    rewrite_packaged_runtime_asset_entries(
        benchmark_root.pointer_mut("/adapter/_runtime_assets"),
        "benchmark.adapter._runtime_assets",
        exp_dir,
        package_dir,
        file_copies,
        file_counter,
    )?;
    stage_command_path_refs_for_package(
        benchmark_root.pointer_mut("/grader/command"),
        "benchmark.grader.command",
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    stage_optional_public_runtime_path_for_package(
        benchmark_root.pointer_mut("/grader/conclusion/mapper"),
        "benchmark.grader.conclusion.mapper",
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    rewrite_optional_package_source_path(
        benchmark_root.pointer_mut("/grader/injected/bundle"),
        "benchmark.grader.injected.bundle",
        exp_dir,
        package_dir,
        "files",
        "grader_bundle",
        file_copies,
        file_counter,
    )?;
    stage_command_path_refs_for_package(
        benchmark_root.pointer_mut("/adapter/command"),
        "benchmark.adapter.command",
        exp_dir,
        package_dir,
        public_path_copies,
        staging_manifest_entries,
    )?;
    Ok(())
}

pub(crate) fn sanitize_name_for_path(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "experiment".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn build_experiment_package(
    path: &Path,
    overrides_path: Option<&Path>,
    out_dir: Option<&Path>,
) -> Result<BuildResult> {
    let loaded = load_authoring_input_for_build(path, overrides_path)?;
    let mut json_value = loaded.json_value.clone();
    validate_required_fields(&json_value)?;

    let experiment_id = json_value
        .pointer("/experiment/id")
        .and_then(Value::as_str)
        .unwrap_or("experiment");
    let package_dir = if let Some(out_dir) = out_dir {
        out_dir.to_path_buf()
    } else {
        let ts = Utc::now().format("%Y%m%d_%H%M%S_%6f");
        loaded
            .project_root
            .join(".lab")
            .join("builds")
            .join(format!("{}_{}", sanitize_name_for_path(experiment_id), ts))
    };
    if package_dir.exists() {
        if !package_dir.is_dir() {
            return Err(anyhow!(
                "build output path exists and is not a directory: {}",
                package_dir.display()
            ));
        }
        let mut entries = fs::read_dir(&package_dir)?;
        if entries.next().is_some() {
            return Err(anyhow!(
                "build output directory must be empty: {}",
                package_dir.display()
            ));
        }
    } else {
        ensure_dir(&package_dir)?;
    }

    ensure_dir(&package_dir.join("agent_builds"))?;
    ensure_dir(&package_dir.join("tasks"))?;
    ensure_dir(&package_dir.join("files"))?;
    ensure_dir(&package_dir.join(PACKAGED_RUNTIME_ASSETS_DIR))?;

    let dataset_path = resolve_dataset_path(&json_value, &loaded.exp_dir)?;
    let dataset_target = package_dir.join("tasks").join("tasks.jsonl");
    let raw_tasks = load_task_rows_for_build(&dataset_path, &json_value)?;
    let packaged_tasks = compile_tasks_for_package(
        &raw_tasks,
        &loaded.project_root,
        &loaded.exp_dir,
        &dataset_path,
        &package_dir,
    )?;
    write_packaged_tasks(&dataset_target, &packaged_tasks)?;
    let dataset_rel = PathBuf::from("tasks").join("tasks.jsonl");
    set_json_pointer_value(
        &mut json_value,
        "/dataset/path",
        json!(as_portable_rel(&dataset_rel)),
    )?;

    let mut artifact_copies: BTreeMap<String, String> = BTreeMap::new();
    let mut file_copies: BTreeMap<String, String> = BTreeMap::new();
    let mut public_path_copies: BTreeMap<String, String> = BTreeMap::new();
    let mut staging_manifest_entries = Vec::new();
    let mut artifact_counter = 0usize;
    let mut file_counter = 0usize;

    if let Some(runtime) = json_value.pointer_mut("/runtime") {
        rewrite_runtime_paths_for_package(
            runtime,
            &loaded.exp_dir,
            &package_dir,
            &mut artifact_copies,
            &mut file_copies,
            &mut public_path_copies,
            &mut staging_manifest_entries,
            &mut artifact_counter,
            &mut file_counter,
        )?;
    }
    if let Some(runtime_overrides) = json_value.pointer_mut("/baseline/runtime_overrides") {
        rewrite_runtime_paths_for_package(
            runtime_overrides,
            &loaded.exp_dir,
            &package_dir,
            &mut artifact_copies,
            &mut file_copies,
            &mut public_path_copies,
            &mut staging_manifest_entries,
            &mut artifact_counter,
            &mut file_counter,
        )?;
    }
    if let Some(variant_plan) = json_value
        .pointer_mut("/variant_plan")
        .and_then(Value::as_array_mut)
    {
        for variant in variant_plan.iter_mut() {
            if let Some(runtime_overrides) = variant.get_mut("runtime_overrides") {
                rewrite_runtime_paths_for_package(
                    runtime_overrides,
                    &loaded.exp_dir,
                    &package_dir,
                    &mut artifact_copies,
                    &mut file_copies,
                    &mut public_path_copies,
                    &mut staging_manifest_entries,
                    &mut artifact_counter,
                    &mut file_counter,
                )?;
            }
        }
    }
    if let Some(variants) = json_value
        .pointer_mut("/variants")
        .and_then(Value::as_array_mut)
    {
        for variant in variants.iter_mut() {
            if let Some(runtime_overrides) = variant.get_mut("runtime_overrides") {
                rewrite_runtime_paths_for_package(
                    runtime_overrides,
                    &loaded.exp_dir,
                    &package_dir,
                    &mut artifact_copies,
                    &mut file_copies,
                    &mut public_path_copies,
                    &mut staging_manifest_entries,
                    &mut artifact_counter,
                    &mut file_counter,
                )?;
            }
        }
    }
    if let Some(benchmark) = json_value.pointer_mut("/benchmark") {
        rewrite_benchmark_paths_for_package(
            benchmark,
            &loaded.exp_dir,
            &package_dir,
            &mut file_copies,
            &mut file_counter,
            &mut public_path_copies,
            &mut staging_manifest_entries,
        )?;
    }

    validate_packaged_runtime_artifacts(&package_dir, &json_value)?;
    write_runtime_staging_manifest(&package_dir, &json_value, &staging_manifest_entries)?;

    let resolved_for_manifest = json_value.clone();
    atomic_write_json_pretty(
        &package_dir.join("resolved_experiment.json"),
        &resolved_for_manifest,
    )?;

    let manifest_path = package_dir.join("manifest.json");
    let checksums_path = package_dir.join("checksums.json");
    let lock_path = package_dir.join("package.lock");
    let mut checksums: BTreeMap<String, String> = BTreeMap::new();
    for entry in walkdir::WalkDir::new(&package_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
    {
        let path = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }
        if path == checksums_path || path == manifest_path || path == lock_path {
            continue;
        }
        let rel = path
            .strip_prefix(&package_dir)
            .map(as_portable_rel)
            .unwrap_or_else(|_| path.display().to_string());
        checksums.insert(rel, sha256_file(path)?);
    }
    let checksums_value = json!({
        "schema_version": "sealed_package_checksums_v2",
        "files": checksums,
    });
    atomic_write_json_pretty(&checksums_path, &checksums_value)?;
    let package_digest = canonical_json_digest(
        checksums_value
            .pointer("/files")
            .ok_or_else(|| anyhow!("build failed to materialize checksums files map"))?,
    );
    atomic_write_json_pretty(
        &lock_path,
        &json!({
            "schema_version": "sealed_package_lock_v1",
            "package_digest": package_digest.clone(),
        }),
    )?;
    let package_manifest = json!({
        "schema_version": "sealed_run_package_v2",
        "created_at": Utc::now().to_rfc3339(),
        "resolved_experiment": resolved_for_manifest,
        "checksums_ref": "checksums.json",
        "package_digest": package_digest,
    });
    atomic_write_json_pretty(&manifest_path, &package_manifest)?;

    Ok(BuildResult {
        package_dir,
        manifest_path,
        checksums_path,
    })
}

pub(crate) fn run_experiment_with_behavior(
    path: &Path,
    behavior: RunBehavior,
    execution: RunExecutionOptions,
) -> Result<RunResult> {
    let LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root,
    } = load_sealed_package_for_run(path)?;
    validate_required_fields(&json_value)?;
    let workload_type = experiment_workload_type(&json_value)?;

    let execution = normalize_execution_options(&execution);
    let materialize_mode = execution.materialize.unwrap_or(MaterializationMode::Full);

    let (run_id, run_dir) = create_unique_run_dir(&project_root)?;
    emit_run_log(
        &run_id,
        format!("created run directory {}", run_dir.display()),
    );
    write_run_control_v2(&run_dir, &run_id, "running", &[], None)?;
    write_run_session_state(&run_dir, &run_id, &behavior, &execution)?;
    let _engine_lease_guard = start_engine_lease_heartbeat(&run_dir, &run_id)?;
    let mut run_guard = RunControlGuard::new(&run_dir, &run_id);

    for subdir in [
        "tasks",
        "files",
        "agent_builds",
        PACKAGED_RUNTIME_ASSETS_DIR,
    ] {
        let source = exp_dir.join(subdir);
        if source.exists() {
            copy_path_into_package(&source, &run_dir.join(subdir))?;
        }
    }
    let staging_manifest_source = exp_dir.join(STAGING_MANIFEST_FILE);
    if !staging_manifest_source.is_file() {
        return Err(anyhow!(
            "sealed package missing runtime staging manifest: {}",
            staging_manifest_source.display()
        ));
    }
    copy_path_into_package(
        &staging_manifest_source,
        &run_dir.join(STAGING_MANIFEST_FILE),
    )
    .with_context(|| {
        format!(
            "failed to copy runtime staging manifest from sealed package {} into run directory {}",
            staging_manifest_source.display(),
            run_dir.display()
        )
    })?;

    let resolved_path = run_dir.join("resolved_experiment.json");
    atomic_write_json_pretty(&resolved_path, &json_value)?;
    let resolved_digest = canonical_json_digest(&json_value);
    atomic_write_bytes(
        &run_dir.join("resolved_experiment.digest"),
        resolved_digest.as_bytes(),
    )?;

    let manifest = json!({
        "schema_version": "manifest_v1",
        "run_id": run_id,
        "runner_version": "rust-0.3.0",
        "created_at": Utc::now().to_rfc3339(),
    });
    atomic_write_json_pretty(&run_dir.join("manifest.json"), &manifest)?;

    let dataset_path = resolve_dataset_path_in_package(&json_value, &run_dir)?;
    let tasks = load_tasks(&dataset_path, &json_value)?;

    let (variants, baseline_id) = resolve_variant_plan(&json_value)?;
    write_resolved_variants(&run_dir, &json_value, &baseline_id, &variants)?;
    let replications = json_value
        .pointer("/design/replications")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing /design/replications"))? as usize;
    emit_run_log(
        &run_id,
        format!(
            "resolved experiment: tasks={} variants={} replications={} total_trials={}",
            tasks.len(),
            variants.len(),
            replications,
            tasks.len() * variants.len() * replications
        ),
    );

    let trials_dir = run_dir.join("trials");
    ensure_dir(&trials_dir)?;

    let evidence_dir = run_dir.join("runtime").join("sqlite_ingest");
    let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
    let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
    let benchmark_config = parse_benchmark_config(&json_value);
    let mut variant_runtime_profiles = Vec::with_capacity(variants.len());
    for variant in &variants {
        let profile =
            resolve_variant_runtime_profile(&json_value, variant, &run_dir, &behavior, &execution)?;
        ensure_required_runtime_env_present(&profile.agent_runtime, &profile.agent_runtime_env)?;
        variant_runtime_profiles.push(profile);
    }
    let run_integration_level = variant_runtime_profiles
        .first()
        .map(|profile| profile.agent_runtime.integration_level.clone())
        .unwrap_or_else(|| "cli_basic".to_string());
    let isolation_grade = resolve_run_isolation_grade(&variant_runtime_profiles, &behavior);

    // Preflight checks — abort before trial execution if anything is fatally wrong
    {
        emit_run_log(
            &run_id,
            "starting preflight checks (Docker probes can take a while for per-task images)",
        );
        let preflight_started = Instant::now();
        let checks = collect_preflight_checks(
            &json_value,
            &run_dir,
            &run_dir,
            &project_root,
            &tasks,
            &benchmark_config,
            &variants,
            &variant_runtime_profiles,
        );

        let preflight = PreflightReport {
            passed: checks
                .iter()
                .all(|c| c.passed || matches!(c.severity, PreflightSeverity::Warning)),
            checks,
        };

        let mut passed_count = 0usize;
        let mut warning_count = 0usize;
        let mut failed_count = 0usize;
        for check in &preflight.checks {
            let status = if check.passed {
                passed_count += 1;
                "PASS"
            } else {
                match check.severity {
                    PreflightSeverity::Error => {
                        failed_count += 1;
                        "FAIL"
                    }
                    PreflightSeverity::Warning => {
                        warning_count += 1;
                        "WARN"
                    }
                }
            };
            emit_preflight_log(format!("[{}] {}: {}", status, check.name, check.message));
        }
        emit_run_log(
            &run_id,
            format!(
                "preflight finished in {:.1}s (passed={}, warnings={}, failed={})",
                preflight_started.elapsed().as_secs_f32(),
                passed_count,
                warning_count,
                failed_count
            ),
        );

        if !preflight.passed {
            // Clean up the partial run directory before aborting
            run_guard.complete("preflight_failed")?;
            return Err(anyhow!("preflight failed:\n{}", preflight));
        }
    }

    let mut run_sink = SqliteRunStore::new(&run_dir)?;
    run_sink.write_run_manifest(&RunManifestRecord {
        schema_version: "run_manifest_v1".to_string(),
        run_id: run_id.clone(),
        created_at: Utc::now().to_rfc3339(),
        workload_type: workload_type.clone(),
        baseline_id: baseline_id.clone(),
        variant_ids: variants.iter().map(|variant| variant.id.clone()).collect(),
    })?;

    let policy_config = parse_policies(&json_value);
    let max_concurrency = experiment_max_concurrency(&json_value);
    let random_seed = experiment_random_seed(&json_value);
    let schedule = build_trial_schedule(
        variants.len(),
        tasks.len(),
        replications,
        policy_config.scheduling,
        random_seed,
    );
    write_resolved_schedule(&run_dir, &schedule)?;
    emit_run_log(
        &run_id,
        format!(
            "starting schedule execution: slots={} max_concurrency={}",
            schedule.len(),
            max_concurrency.max(1)
        ),
    );

    // Per-variant consecutive failure tracking (for pruning)
    let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
    let mut pruned_variants: HashSet<usize> = HashSet::new();

    let mut schedule_progress = ScheduleProgress {
        schema_version: "schedule_progress_v2".to_string(),
        run_id: run_id.clone(),
        total_slots: schedule.len(),
        next_schedule_index: 0,
        next_trial_index: 0,
        schedule: schedule.clone(),
        completed_slots: Vec::new(),
        pruned_variants: Vec::new(),
        consecutive_failures: BTreeMap::new(),
        updated_at: Utc::now().to_rfc3339(),
    };
    write_schedule_progress(&run_dir, &schedule_progress)?;

    let mut trial_index: usize = 0;
    let schedule_outcome = execute_schedule_engine(
        ScheduleEngineMode::FreshRun,
        &run_dir,
        &run_id,
        &workload_type,
        &project_root,
        &dataset_path,
        &variants,
        &tasks,
        &schedule,
        &policy_config,
        &benchmark_config,
        &variant_runtime_profiles,
        &behavior,
        materialize_mode,
        &policy_config.task_boundary,
        &trials_dir,
        &evidence_dir,
        &evidence_records_path,
        &task_chain_states_path,
        &mut schedule_progress,
        &mut trial_index,
        &mut consecutive_failures,
        &mut pruned_variants,
        &[],
        &baseline_id,
        &mut run_sink,
        max_concurrency,
    )?;
    run_sink.flush()?;
    if schedule_outcome != ScheduleEngineOutcome::Completed {
        emit_run_log(
            &run_id,
            format!("schedule execution halted with {:?}", schedule_outcome),
        );
        match schedule_outcome {
            ScheduleEngineOutcome::Interrupted => {
                run_guard.complete("interrupted")?;
            }
            _ => {
                // Paused/Killed: handler already wrote correct status
                run_guard.disarm();
            }
        }
        return Ok(RunResult { run_dir, run_id });
    }
    let _ = (
        &project_root,
        &benchmark_config,
        &evidence_records_path,
        &task_chain_states_path,
    );

    if isolation_grade != "hermetic" {
        run_guard.complete("invalid_isolation")?;
        return Err(anyhow!(
            "scientific run completed without hermetic isolation (got {})",
            isolation_grade
        ));
    }

    let grades = json!({
        "schema_version": "grades_v1",
        "integration_level": run_integration_level,
        "replay_grade": "best_effort",
        "isolation_grade": isolation_grade,
        "comparability_grade": "unknown",
        "provenance_grade": "recorded",
        "privacy_grade": "unknown"
    });

    let att = default_attestation(
        &resolved_digest,
        None,
        grades.clone(),
        vec![],
        json!({"name": "unknown"}),
        "hooks",
    );
    write_attestation(&run_dir, att)?;
    run_guard.complete("completed")?;
    emit_run_log(&run_id, "run completed");

    Ok(RunResult { run_dir, run_id })
}

pub fn describe_experiment(path: &Path) -> Result<ExperimentSummary> {
    describe_experiment_with_options(path, &RunExecutionOptions::default())
}

pub fn describe_experiment_with_options(
    path: &Path,
    execution: &RunExecutionOptions,
) -> Result<ExperimentSummary> {
    let LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root: _,
    } = load_sealed_package_for_run(path)?;
    validate_required_fields(&json_value)?;
    let execution = normalize_execution_options(execution);

    let dataset_path = resolve_dataset_path_in_package(&json_value, &exp_dir)?;
    let task_count = count_tasks(&dataset_path, &json_value)?;
    let (variants, _) = resolve_variant_plan(&json_value)?;
    let replications = json_value
        .pointer("/design/replications")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing /design/replications"))? as usize;
    let variant_count = variants.len();
    let total_trials = task_count * replications * variant_count;

    let baseline_variant = variants
        .first()
        .ok_or_else(|| anyhow!("no variants available in experiment"))?;
    let runtime_profile = resolve_variant_runtime_profile(
        &json_value,
        baseline_variant,
        &exp_dir,
        &RunBehavior::default(),
        &execution,
    )?;
    let preflight_runtime_profiles = vec![runtime_profile.clone()];
    let VariantRuntimeProfile {
        agent_runtime: runtime_agent,
        configured_network_mode: network_mode,
        ..
    } = runtime_profile;
    let image = Some(runtime_agent.image.clone());

    let exp_id = json_value
        .pointer("/experiment/id")
        .and_then(|v| v.as_str())
        .unwrap_or("exp")
        .to_string();
    let workload_type = experiment_workload_type(&json_value)?;

    let policy_config = parse_policies(&json_value);
    let comparison = json_value
        .pointer("/design/comparison")
        .and_then(|v| v.as_str())
        .unwrap_or("paired")
        .to_string();

    // Collect preflight warnings (lightweight: dataset + config checks only, no Docker)
    let benchmark_config = parse_benchmark_config(&json_value);
    let tasks_for_preflight = load_tasks(&dataset_path, &json_value).unwrap_or_default();
    let mut preflight_warnings = Vec::new();
    for check in check_dataset_task_ids(
        &tasks_for_preflight,
        &benchmark_config,
        &preflight_runtime_profiles,
    ) {
        if matches!(check.severity, PreflightSeverity::Warning) || !check.passed {
            preflight_warnings.push(format!("[{}] {}", check.name, check.message));
        }
    }
    {
        let grader_check = check_benchmark_grader_reachable(
            &benchmark_config,
            &resolve_variant_runtime_profile(
                &json_value,
                baseline_variant,
                &exp_dir,
                &RunBehavior::default(),
                &execution,
            )?,
            baseline_variant,
            &tasks_for_preflight,
            &exp_dir,
        );
        if matches!(grader_check.severity, PreflightSeverity::Warning)
            && !grader_check.message.contains("no benchmark")
        {
            preflight_warnings.push(format!("[{}] {}", grader_check.name, grader_check.message));
        }
    }
    for check in check_dependency_files_exist(&json_value, &exp_dir) {
        if !check.passed {
            preflight_warnings.push(format!("[{}] {}", check.name, check.message));
        }
    }

    Ok(ExperimentSummary {
        exp_id,
        workload_type,
        dataset_path,
        task_count,
        replications,
        variant_count,
        total_trials,
        agent_runtime_command: runtime_agent.command_raw,
        image,
        network_mode,
        trajectory_path: runtime_agent.trajectory_path,
        causal_extraction: runtime_agent.causal_extraction,
        scheduling: match policy_config.scheduling {
            SchedulingPolicy::PairedInterleaved => "paired_interleaved".to_string(),
            SchedulingPolicy::VariantSequential => "variant_sequential".to_string(),
            SchedulingPolicy::Randomized => "randomized".to_string(),
        },
        state_policy: match policy_config.state {
            StatePolicy::IsolatePerTrial => "isolate_per_trial".to_string(),
            StatePolicy::PersistPerTask => "persist_per_task".to_string(),
            StatePolicy::Accumulate => "accumulate".to_string(),
        },
        comparison,
        retry_max_attempts: policy_config.retry_max_attempts,
        preflight_warnings,
    })
}
