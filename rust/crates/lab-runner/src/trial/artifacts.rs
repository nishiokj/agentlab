use anyhow::Result;
use lab_core::AGENTLAB_CONTRACT_OUT_DIR;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;

use crate::model::*;

fn trial_output_error_payload(code: &str, message: &str) -> Value {
    json!({
        "schema_version": "agent_result_v1",
        "outcome": "error",
        "error": {
            "code": code,
            "message": message,
        },
    })
}

pub(crate) fn load_trial_output_resilient(path: &Path) -> Result<(Value, Option<String>)> {
    if !path.exists() {
        return Ok((
            trial_output_error_payload("result_missing", "agent did not write a result payload"),
            None,
        ));
    }

    let bytes = fs::read(path)?;
    match serde_json::from_slice(&bytes) {
        Ok(value) => Ok((value, None)),
        Err(err) => {
            let detail = format!(
                "failed to parse agent result JSON at {}: {}",
                path.display(),
                err
            );
            Ok((
                trial_output_error_payload("result_parse_error", &detail),
                Some(detail),
            ))
        }
    }
}

pub(crate) fn trial_output_payload_view<'a>(trial_output: &'a Value) -> &'a Value {
    if trial_output.get("schema_version").and_then(Value::as_str) == Some("artifact_envelope_v1") {
        trial_output.get("artifact").unwrap_or(trial_output)
    } else {
        trial_output
    }
}

fn result_file_ref_path(result_value: &Value) -> Option<&str> {
    result_value
        .get("artifact")
        .and_then(Value::as_str)
        .or_else(|| {
            result_value
                .pointer("/artifact/path")
                .and_then(Value::as_str)
        })
}

pub(crate) fn artifact_type_from_trial_input(trial_input: &Value) -> ArtifactType {
    match trial_input
        .pointer("/artifact_type")
        .and_then(Value::as_str)
        .unwrap_or("structured_json")
    {
        "patch_submission" => ArtifactType::PatchSubmission,
        "text_response" => ArtifactType::TextResponse,
        "file_ref" => ArtifactType::FileRef,
        _ => ArtifactType::StructuredJson,
    }
}

pub(crate) fn artifact_type_from_trial_input_path(path: &Path) -> Result<ArtifactType> {
    let trial_input: Value = serde_json::from_slice(&fs::read(path)?)?;
    Ok(artifact_type_from_trial_input(&trial_input))
}

pub(crate) fn extract_candidate_artifact_record(
    result_value: &Value,
    expected_artifact_type: ArtifactType,
) -> CandidateArtifactRecord {
    if result_value
        .pointer("/error/code")
        .and_then(Value::as_str)
        .is_some_and(|code| code == "result_missing")
    {
        return CandidateArtifactRecord {
            state: CandidateArtifactState::Missing,
            artifact_type: expected_artifact_type,
            source: CandidateArtifactSource::None,
            payload: None,
        };
    }

    match serde_json::from_value::<ArtifactEnvelopeV1>(result_value.clone()) {
        Ok(envelope) if envelope.artifact_type == expected_artifact_type => {
            let source = if matches!(envelope.artifact_type, ArtifactType::FileRef) {
                CandidateArtifactSource::ResultFileRef
            } else {
                CandidateArtifactSource::ResultInline
            };
            let state = if matches!(envelope.artifact_type, ArtifactType::FileRef) {
                match result_file_ref_path(result_value) {
                    Some(path)
                        if path == DEFAULT_CONTAINER_RESULT_PATH
                            || path.starts_with(&format!("{}/", AGENTLAB_CONTRACT_OUT_DIR)) =>
                    {
                        CandidateArtifactState::Valid
                    }
                    _ => CandidateArtifactState::Invalid,
                }
            } else {
                CandidateArtifactState::Valid
            };
            CandidateArtifactRecord {
                state,
                artifact_type: envelope.artifact_type,
                source,
                payload: Some(envelope.artifact),
            }
        }
        Ok(envelope) => CandidateArtifactRecord {
            state: CandidateArtifactState::Invalid,
            artifact_type: envelope.artifact_type,
            source: CandidateArtifactSource::None,
            payload: Some(envelope.artifact),
        },
        Err(_) => CandidateArtifactRecord {
            state: CandidateArtifactState::Invalid,
            artifact_type: expected_artifact_type,
            source: CandidateArtifactSource::None,
            payload: Some(result_value.clone()),
        },
    }
}
