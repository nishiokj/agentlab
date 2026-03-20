use crate::persistence::sqlite_store::JsonRowTable;
use crate::{run_sqlite_path, Path, PathBuf, Value};

pub(crate) fn infer_run_dir_from_path(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        if run_sqlite_path(ancestor).exists() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

pub(crate) fn json_row_table_from_path(path: &Path) -> Option<JsonRowTable> {
    let name = path.file_name()?.to_string_lossy().to_ascii_lowercase();
    if name.contains("evidence") {
        return Some(JsonRowTable::Evidence);
    }
    if name.contains("task_chain") || name.contains("chain_state") {
        return Some(JsonRowTable::ChainState);
    }
    if name.contains("conclusion") {
        return Some(JsonRowTable::BenchmarkConclusion);
    }
    None
}

pub(crate) fn row_has_sqlite_identity_fields(row: &Value) -> bool {
    row.pointer("/run_id")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
        && row
            .pointer("/schedule_idx")
            .and_then(Value::as_u64)
            .is_some()
        && row.pointer("/attempt").and_then(Value::as_u64).is_some()
        && row.pointer("/row_seq").and_then(Value::as_u64).is_some()
        && row
            .pointer("/slot_commit_id")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
}

pub(crate) fn path_uses_sqlite_json_row_ingest(run_dir: &Path, path: &Path) -> bool {
    !path.starts_with(run_dir.join("runtime").join("worker_payload"))
}
