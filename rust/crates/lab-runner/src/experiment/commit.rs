use anyhow::{anyhow, Result};
use chrono::Utc;
use lab_core::{canonical_json_digest, sha256_bytes};
use lab_schemas::compile_schema;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::experiment::runner::emit_slot_commit_progress;
use crate::experiment::state::*;
use crate::model::*;
use crate::persistence::journal::append_jsonl;
use crate::persistence::journal::*;
use crate::persistence::rows::*;

pub(crate) fn make_slot_commit_id(
    run_id: &str,
    schedule_idx: usize,
    attempt: usize,
    payload_digest: &str,
) -> String {
    let raw = format!("{}:{}:{}:{}", run_id, schedule_idx, attempt, payload_digest);
    let digest = sha256_bytes(raw.as_bytes());
    format!("slot_{}", &digest[..24])
}

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
