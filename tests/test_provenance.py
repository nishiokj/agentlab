"""Strong test suite for provenance module."""
import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from agentlab_core import ArtifactStore
from agentlab_runner.provenance import (
    _hashchain_heads,
    capture_sbom_stub,
    compute_artifact_store_root_digest,
    write_attestation,
)


class TestHashchainHeads:
    """Tests for _hashchain_heads function."""

    def test_hashchain_heads_empty_dir(self):
        """Empty trials directory returns empty list."""
        with tempfile.TemporaryDirectory() as tmp:
            result = _hashchain_heads(os.path.join(tmp, "trials"))
            assert result == []

    def test_hashchain_heads_nonexistent_dir(self):
        """Nonexistent directory returns empty list."""
        result = _hashchain_heads("/nonexistent/path/trials")
        assert result == []

    def test_hashchain_heads_multiple_trials(self):
        """Multiple trial directories with valid head files."""
        with tempfile.TemporaryDirectory() as tmp:
            trials_root = os.path.join(tmp, "trials")
            trial1 = os.path.join(trials_root, "trial_001")
            trial2 = os.path.join(trials_root, "trial_002")
            os.makedirs(trial1)
            os.makedirs(trial2)

            hash1 = "sha256:a" * 16
            hash2 = "sha256:b" * 16

            with open(os.path.join(trial1, "events.head"), "w") as f:
                f.write(hash1)
            with open(os.path.join(trial2, "events.head"), "w") as f:
                f.write(hash2)

            result = _hashchain_heads(trials_root)

            assert len(result) == 2
            trial_ids = {r["trial_id"] for r in result}
            assert trial_ids == {"trial_001", "trial_002"}

            for r in result:
                if r["trial_id"] == "trial_001":
                    assert r["head"] == hash1
                else:
                    assert r["head"] == hash2

    def test_hashchain_heads_ignores_missing_head(self):
        """Trials without events.head are ignored."""
        with tempfile.TemporaryDirectory() as tmp:
            trials_root = os.path.join(tmp, "trials")
            trial1 = os.path.join(trials_root, "trial_001")
            trial2 = os.path.join(trials_root, "trial_002")
            os.makedirs(trial1)
            os.makedirs(trial2)

            # Only one has events.head
            with open(os.path.join(trial1, "events.head"), "w") as f:
                f.write("sha256:abc")

            result = _hashchain_heads(trials_root)
            assert len(result) == 1
            assert result[0]["trial_id"] == "trial_001"

    def test_hashchain_heads_trims_whitespace(self):
        """Head file whitespace is trimmed."""
        with tempfile.TemporaryDirectory() as tmp:
            trial_dir = os.path.join(tmp, "trials", "trial_001")
            os.makedirs(trial_dir)

            hash_val = "sha256:abc123"
            with open(os.path.join(trial_dir, "events.head"), "w") as f:
                f.write(f"  {hash_val}  \n")

            result = _hashchain_heads(os.path.join(tmp, "trials"))
            assert len(result) == 1
            assert result[0]["head"] == hash_val


class TestArtifactStoreRootDigest:
    """Tests for compute_artifact_store_root_digest function."""

    def test_root_digest_nonexistent_dir(self):
        """Nonexistent directory returns None."""
        result = compute_artifact_store_root_digest("/nonexistent")
        assert result is None

    def test_root_digest_empty_dir(self):
        """Empty directory returns None."""
        with tempfile.TemporaryDirectory() as tmp:
            result = compute_artifact_store_root_digest(tmp)
            assert result is None

    def test_root_digest_single_file(self):
        """Single file digest is deterministic."""
        with tempfile.TemporaryDirectory() as tmp:
            file_path = os.path.join(tmp, "test.txt")
            with open(file_path, "wb") as f:
                f.write(b"hello world")

            digest1 = compute_artifact_store_root_digest(tmp)
            digest2 = compute_artifact_store_root_digest(tmp)

            assert digest1 is not None
            assert digest1.startswith("sha256:")
            assert digest1 == digest2  # Deterministic

    def test_root_digest_order_independence(self):
        """Digest is independent of file traversal order."""
        with tempfile.TemporaryDirectory() as tmp:
            # Create multiple files
            for i in range(5):
                with open(os.path.join(tmp, f"file{i}.txt"), "wb") as f:
                    f.write(f"content{i}".encode())

            digest1 = compute_artifact_store_root_digest(tmp)
            # Create new temp dir with same content
            with tempfile.TemporaryDirectory() as tmp2:
                for i in range(5):
                    with open(os.path.join(tmp2, f"file{i}.txt"), "wb") as f:
                        f.write(f"content{i}".encode())
                digest2 = compute_artifact_store_root_digest(tmp2)

            assert digest1 == digest2

    def test_root_digest_content_change_affects_digest(self):
        """Content change changes the digest."""
        with tempfile.TemporaryDirectory() as tmp:
            file_path = os.path.join(tmp, "test.txt")
            with open(file_path, "wb") as f:
                f.write(b"original")

            digest1 = compute_artifact_store_root_digest(tmp)

            with open(file_path, "wb") as f:
                f.write(b"modified")

            digest2 = compute_artifact_store_root_digest(tmp)

            assert digest1 != digest2

    def test_root_digest_nested_directories(self):
        """Handles nested directory structures."""
        with tempfile.TemporaryDirectory() as tmp:
            nested = os.path.join(tmp, "a", "b", "c")
            os.makedirs(nested)

            with open(os.path.join(nested, "deep.txt"), "wb") as f:
                f.write(b"deep content")

            with open(os.path.join(tmp, "shallow.txt"), "wb") as f:
                f.write(b"shallow content")

            digest = compute_artifact_store_root_digest(tmp)
            assert digest is not None
            assert digest.startswith("sha256:")


class TestWriteAttestation:
    """Tests for write_attestation function."""

    def test_write_attestation_basic(self):
        """Basic attestation writing with minimal parameters."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            os.makedirs(run_dir)

            # Write required resolved_experiment.digest
            digest_path = os.path.join(run_dir, "resolved_experiment.digest")
            with open(digest_path, "w") as f:
                f.write("sha256:expdigest")

            # Create trials dir with events.head
            trials_root = os.path.join(run_dir, "trials")
            trial_dir = os.path.join(trials_root, "trial_001")
            os.makedirs(trial_dir)
            with open(os.path.join(trial_dir, "events.head"), "w") as f:
                f.write("sha256:eventhash")

            # Create artifacts dir
            artifacts_root = os.path.join(run_dir, "artifacts")
            os.makedirs(artifacts_root)
            with open(os.path.join(artifacts_root, "artifact.txt"), "wb") as f:
                f.write(b"test artifact")

            artifact_store = ArtifactStore(artifacts_root)
            grades = {"overall": "pass", "replay": "strict"}

            attestation_path = write_attestation(
                run_dir=run_dir,
                artifact_store=artifact_store,
                grades=grades,
            )

            assert attestation_path == os.path.join(run_dir, "attestation.json")
            assert os.path.exists(attestation_path)

            with open(attestation_path) as f:
                attestation = json.load(f)

            assert attestation["schema_version"] == "attestation_v1"
            assert attestation["resolved_experiment_digest"] == "sha256:expdigest"
            assert attestation["grades_summary"] == grades
            assert attestation["created_at"] is not None
            assert len(attestation["events_hashchain"]) == 1
            assert "artifact_store_root" in attestation

    def test_write_attestation_with_optional_fields(self):
        """Attestation with all optional fields."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            os.makedirs(run_dir)

            digest_path = os.path.join(run_dir, "resolved_experiment.digest")
            with open(digest_path, "w") as f:
                f.write("sha256:expdigest")

            os.makedirs(os.path.join(run_dir, "trials"))
            os.makedirs(os.path.join(run_dir, "artifacts"))

            artifact_store = ArtifactStore(os.path.join(run_dir, "artifacts"))
            grades = {"score": 0.95}

            harness_identity = {
                "type": "docker",
                "image": "test:latest",
                "digest": "sha256:imgdigest",
            }
            hooks_schema_version = "hook_events_v1"
            trace_ingestion = {"source": "otlp", "received_at": "2024-01-01"}
            sbom_ref = artifact_store.put_bytes(b"sbom content")

            attestation_path = write_attestation(
                run_dir=run_dir,
                artifact_store=artifact_store,
                grades=grades,
                harness_identity=harness_identity,
                hooks_schema_version=hooks_schema_version,
                trace_ingestion=trace_ingestion,
                sbom_artifact_ref=sbom_ref,
            )

            with open(attestation_path) as f:
                attestation = json.load(f)

            assert attestation["harness_identity"] == harness_identity
            assert attestation["hooks_schema_version"] == hooks_schema_version
            assert attestation["trace_ingestion"] == trace_ingestion
            assert attestation["sbom"]["format"] == "spdx"
            assert attestation["sbom"]["artifact_ref"] == sbom_ref

    def test_write_attestation_no_artifacts(self):
        """Attestation without artifact store works."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            os.makedirs(run_dir)

            digest_path = os.path.join(run_dir, "resolved_experiment.digest")
            with open(digest_path, "w") as f:
                f.write("sha256:expdigest")

            os.makedirs(os.path.join(run_dir, "trials"))
            os.makedirs(os.path.join(run_dir, "artifacts"))  # Empty

            artifact_store = ArtifactStore(os.path.join(run_dir, "artifacts"))
            grades = {"status": "complete"}

            attestation_path = write_attestation(
                run_dir=run_dir,
                artifact_store=artifact_store,
                grades=grades,
            )

            with open(attestation_path) as f:
                attestation = json.load(f)

            # artifact_store_root should be missing for empty store
            assert "artifact_store_root" not in attestation


class TestCaptureSbomStub:
    """Tests for capture_sbom_stub function."""

    def test_capture_sbom_stub_exists(self):
        """Captures SBOM when file exists."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            sbom_dir = os.path.join(run_dir, "sbom")
            os.makedirs(sbom_dir)

            sbom_content = {"spdxVersion": "SPDX-2.3", "name": "test"}
            sbom_path = os.path.join(sbom_dir, "image.spdx.json")
            with open(sbom_path, "w") as f:
                json.dump(sbom_content, f)

            artifacts_root = os.path.join(tmp, "artifacts")
            artifact_store = ArtifactStore(artifacts_root)

            ref = capture_sbom_stub(run_dir, artifact_store)

            assert ref is not None
            assert ref.startswith("artifact://sha256/")

            # Verify content is retrievable
            content = artifact_store.get_bytes(ref)
            assert json.loads(content) == sbom_content

    def test_capture_sbom_stub_missing(self):
        """Returns None when SBOM doesn't exist."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            os.makedirs(run_dir)

            artifacts_root = os.path.join(tmp, "artifacts")
            artifact_store = ArtifactStore(artifacts_root)

            ref = capture_sbom_stub(run_dir, artifact_store)
            assert ref is None

    def test_capture_sbom_stub_expected_path(self):
        """Only checks the specific image.spdx.json path."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            os.makedirs(run_dir)

            # Create a different SBOM file
            sbom_path = os.path.join(run_dir, "other.spdx.json")
            with open(sbom_path, "w") as f:
                f.write("{}")

            artifacts_root = os.path.join(tmp, "artifacts")
            artifact_store = ArtifactStore(artifacts_root)

            ref = capture_sbom_stub(run_dir, artifact_store)
            assert ref is None  # Should not find the different file
