import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from agentlab_core.artifact_store import ArtifactStore
from agentlab_core.hashing import sha256_bytes


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _hashchain_heads(trials_root: str) -> List[Dict[str, str]]:
    heads = []
    if not os.path.isdir(trials_root):
        return heads
    for trial_id in os.listdir(trials_root):
        tdir = os.path.join(trials_root, trial_id)
        head_path = os.path.join(tdir, "events.head")
        if os.path.exists(head_path):
            with open(head_path, "r", encoding="utf-8") as f:
                head = f.read().strip()
            heads.append({"trial_id": trial_id, "head": head})
    return heads


def compute_artifact_store_root_digest(artifact_root: str) -> Optional[str]:
    if not os.path.isdir(artifact_root):
        return None
    hashes = []
    for root, _, files in os.walk(artifact_root):
        for name in files:
            path = os.path.join(root, name)
            with open(path, "rb") as f:
                hashes.append(sha256_bytes(f.read()))
    if not hashes:
        return None
    canonical = json.dumps(sorted(hashes), separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return sha256_bytes(canonical)


def write_attestation(
    run_dir: str,
    artifact_store: ArtifactStore,
    grades: Dict[str, Any],
    harness_identity: Optional[Dict[str, Any]] = None,
    hooks_schema_version: Optional[str] = None,
    trace_ingestion: Optional[Dict[str, Any]] = None,
    sbom_artifact_ref: Optional[str] = None,
) -> str:
    resolved_digest_path = os.path.join(run_dir, "resolved_experiment.digest")
    with open(resolved_digest_path, "r", encoding="utf-8") as f:
        resolved_digest = f.read().strip()

    attestation = {
        "schema_version": "attestation_v1",
        "resolved_experiment_digest": resolved_digest,
        "events_hashchain": _hashchain_heads(os.path.join(run_dir, "trials")),
        "grades_summary": grades,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    if sbom_artifact_ref:
        attestation["sbom"] = {"format": "spdx", "artifact_ref": sbom_artifact_ref}

    if trace_ingestion:
        attestation["trace_ingestion"] = trace_ingestion

    if hooks_schema_version:
        attestation["hooks_schema_version"] = hooks_schema_version

    if harness_identity:
        attestation["harness_identity"] = harness_identity

    root_digest = compute_artifact_store_root_digest(os.path.join(run_dir, "artifacts"))
    if root_digest:
        attestation["artifact_store_root"] = root_digest

    out_path = os.path.join(run_dir, "attestation.json")
    _write_json(out_path, attestation)
    return out_path


def capture_sbom_stub(run_dir: str, artifact_store: ArtifactStore) -> Optional[str]:
    # Stub: if an SBOM already exists in run_dir/sbom/image.spdx.json, store it.
    sbom_path = os.path.join(run_dir, "sbom", "image.spdx.json")
    if not os.path.exists(sbom_path):
        return None
    return artifact_store.put_file(sbom_path)
