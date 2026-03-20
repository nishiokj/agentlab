use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::Path;

use crate::model::{WorkspaceBaseKind, WorkspaceBaseSpec, WorkspaceMode, WorkspaceSpec};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskMaterializationKind {
    TaskImage,
    BaseImageBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskMaterializationSpec {
    pub(crate) kind: TaskMaterializationKind,
    #[serde(default)]
    pub(crate) task_bundle_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskRow {
    pub(crate) schema_version: String,
    pub(crate) id: String,
    pub(crate) image: String,
    pub(crate) workdir: String,
    #[serde(default)]
    pub(crate) time_limit_ms: Option<u64>,
    pub(crate) task: Value,
    pub(crate) materialization: TaskMaterializationSpec,
}

impl TaskRow {
    pub(crate) fn task_id(&self, task_idx: usize) -> String {
        let trimmed = self.id.trim();
        if trimmed.is_empty() {
            format!("task_{}", task_idx)
        } else {
            trimmed.to_string()
        }
    }

    pub(crate) fn task_image(&self) -> &str {
        self.image.as_str()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TaskBoundaryMaterialization {
    pub(crate) declaration: Value,
    pub(crate) task_payload: Value,
    pub(crate) workspace: WorkspaceSpec,
    pub(crate) dependencies: Value,
    pub(crate) materialization: TaskMaterializationSpec,
    pub(crate) task_id: String,
    pub(crate) task_image: String,
    pub(crate) task_workdir: String,
    pub(crate) time_limit_ms: Option<u64>,
}

pub(crate) fn parse_task_row(task: &Value) -> Result<TaskRow> {
    let obj = task
        .as_object()
        .ok_or_else(|| anyhow!("task row must be an object"))?;
    if obj.get("schema_version").and_then(Value::as_str) != Some("task_row_v1") {
        return Err(anyhow!("task row schema_version must be 'task_row_v1'"));
    }
    let task_row: TaskRow =
        serde_json::from_value(task.clone()).map_err(|err| anyhow!("invalid task row: {}", err))?;
    validate_task_row(&task_row)?;
    Ok(task_row)
}

pub(crate) fn materialize_task_row(task_row: TaskRow) -> TaskBoundaryMaterialization {
    TaskBoundaryMaterialization {
        declaration: serde_json::to_value(&task_row).unwrap_or_else(|_| json!({})),
        task_payload: task_row.task.clone(),
        workspace: WorkspaceSpec {
            mode: WorkspaceMode::Scratch,
            base: WorkspaceBaseSpec {
                kind: WorkspaceBaseKind::Empty,
                dataset_pack_ref: None,
                repo: None,
                commit: None,
            },
            overlays: Vec::new(),
            aux_mounts: Vec::new(),
        },
        dependencies: json!({}),
        materialization: task_row.materialization.clone(),
        task_id: task_row.task_id(0),
        task_image: task_row.image.clone(),
        task_workdir: task_row.workdir.clone(),
        time_limit_ms: task_row.time_limit_ms,
    }
}

pub(crate) fn materialize_packaged_task_boundary(
    task: &Value,
) -> Result<TaskBoundaryMaterialization> {
    match task.get("schema_version").and_then(Value::as_str) {
        Some("task_row_v1") => Ok(materialize_task_row(parse_task_row(task)?)),
        Some(other) => Err(anyhow!(
            "packaged task schema_version '{}' is not supported at runtime; expected 'task_row_v1'",
            other
        )),
        None => Err(anyhow!(
            "packaged task row missing schema_version; expected 'task_row_v1'"
        )),
    }
}

pub(crate) fn parse_task_boundary_from_packaged_task(
    task: &Value,
) -> Result<TaskBoundaryMaterialization> {
    materialize_packaged_task_boundary(task)
}

pub(crate) fn validate_task_row(task_row: &TaskRow) -> Result<()> {
    if task_row.id.trim().is_empty() {
        return Err(anyhow!("task row field 'id' must be a non-empty string"));
    }
    if task_row.image.trim().is_empty() {
        return Err(anyhow!("task row field 'image' must be a non-empty string"));
    }
    if task_row.workdir.trim().is_empty() {
        return Err(anyhow!(
            "task row field 'workdir' must be a non-empty string"
        ));
    }
    if !Path::new(task_row.workdir.trim()).is_absolute() {
        return Err(anyhow!("task row field 'workdir' must be an absolute path"));
    }
    if !task_row.task.is_object() {
        return Err(anyhow!("task row field 'task' must be an object"));
    }
    if task_row.time_limit_ms == Some(0) {
        return Err(anyhow!(
            "task row field 'time_limit_ms' must be > 0 when provided"
        ));
    }
    match task_row.materialization.kind {
        TaskMaterializationKind::TaskImage => {
            if task_row.materialization.task_bundle_ref.is_some() {
                return Err(anyhow!(
                    "task row materialization.kind='task_image' does not allow task_bundle_ref"
                ));
            }
        }
        TaskMaterializationKind::BaseImageBundle => {
            let _task_bundle_ref = task_row
                .materialization
                .task_bundle_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "task row materialization.task_bundle_ref is required for base_image_bundle"
                    )
                })?;
        }
    }
    Ok(())
}

pub(crate) fn validate_task_boundary_workspace_materialization(
    task_boundary: &TaskBoundaryMaterialization,
) -> Result<()> {
    if task_boundary.workspace.mode != WorkspaceMode::Patch {
        return Ok(());
    }
    if task_boundary.workspace.base.kind != WorkspaceBaseKind::Empty {
        return Ok(());
    }
    Err(anyhow!(
        "task '{}' uses workspace.mode='patch' but workspace.base.kind='empty'; patch tasks require a real base (dataset_pack or git_checkout)",
        task_boundary.task_id
    ))
}
