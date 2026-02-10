"""Strong test suite for trace_ingest module."""
import json
import os
import tempfile
from unittest.mock import MagicMock

import pytest

from agentlab_runner.schemas import SchemaRegistry
from agentlab_runner.trace_ingest import TraceIngestor


class TestTraceIngestorInit:
    """Tests for TraceIngestor initialization."""

    def test_init_with_registry(self):
        """Initialize with schema registry."""
        registry = MagicMock(spec=SchemaRegistry)
        ingestor = TraceIngestor(registry)
        assert ingestor.registry == registry


class TestTraceIngestorIngestManifest:
    """Tests for ingest_manifest method."""

    def test_ingest_manifest_basic(self):
        """Ingest basic trace manifest."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "version": "trace_manifest_v1",
                "trace_id": "abc123",
                "spans": [{"name": "test", "duration": 100}],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            result = ingestor.ingest_manifest(manifest_path)

            assert result == manifest_data

    def test_ingest_manifest_validates(self):
        """Validates manifest against schema."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {"version": "trace_manifest_v1", "trace_id": "abc123"}
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            ingestor.ingest_manifest(manifest_path)

            registry.validate.assert_called_once_with(
                "trace_manifest_v1.jsonschema", manifest_data
            )

    def test_ingest_manifest_nonexistent_file(self):
        """Raises FileNotFoundError for missing file."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "nonexistent.json")

            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            with pytest.raises(FileNotFoundError):
                ingestor.ingest_manifest(manifest_path)

    def test_ingest_manifest_invalid_json(self):
        """Raises JSONDecodeError for invalid JSON."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            with open(manifest_path, "w") as f:
                f.write("not valid json")

            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            with pytest.raises(json.JSONDecodeError):
                ingestor.ingest_manifest(manifest_path)

    def test_ingest_manifest_complex_structure(self):
        """Handles complex nested manifest structure."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "version": "trace_manifest_v1",
                "trace_id": "trace_001",
                "metadata": {
                    "start_time": "2024-01-01T00:00:00Z",
                    "end_time": "2024-01-01T00:01:00Z",
                    "tags": ["tag1", "tag2"],
                },
                "spans": [
                    {
                        "span_id": "span_001",
                        "parent_id": None,
                        "name": "root",
                        "start": 0,
                        "duration": 60000,
                        "attributes": {"key": "value"},
                    },
                    {
                        "span_id": "span_002",
                        "parent_id": "span_001",
                        "name": "child",
                        "start": 1000,
                        "duration": 50000,
                        "attributes": {},
                    },
                ],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            result = ingestor.ingest_manifest(manifest_path)

            assert result == manifest_data
            assert len(result["spans"]) == 2

    def test_ingest_manifest_empty_file(self):
        """Handles empty JSON object."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            with open(manifest_path, "w") as f:
                f.write("{}")

            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            result = ingestor.ingest_manifest(manifest_path)

            assert result == {}

    def test_ingest_manifest_large_file(self):
        """Handles large manifest files."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            # Create manifest with many spans
            manifest_data = {
                "version": "trace_manifest_v1",
                "trace_id": "trace_001",
                "spans": [{"span_id": f"span_{i}", "name": f"operation_{i}"} for i in range(1000)],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            result = ingestor.ingest_manifest(manifest_path)

            assert len(result["spans"]) == 1000
            assert result["spans"][0]["span_id"] == "span_0"
            assert result["spans"][999]["span_id"] == "span_999"

    def test_ingest_manifest_with_special_characters(self):
        """Handles special characters in values."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "version": "trace_manifest_v1",
                "trace_id": "abc123",
                "message": "Hello\nWorld\tTest",
                "unicode": "Hello 世界 🌍",
            }
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            result = ingestor.ingest_manifest(manifest_path)

            assert result["message"] == "Hello\nWorld\tTest"
            assert result["unicode"] == "Hello 世界 🌍"


class TestTraceIngestorIngestOtlp:
    """Tests for ingest_otlp method."""

    def test_ingest_otlp_not_implemented(self):
        """OTLP ingestion is not implemented."""
        registry = MagicMock(spec=SchemaRegistry)
        ingestor = TraceIngestor(registry)

        with pytest.raises(NotImplementedError, match="OTLP receiver is not implemented"):
            ingestor.ingest_otlp()


class TestTraceIngestorIntegration:
    """Integration tests for TraceIngestor."""

    def test_multiple_manifests(self):
        """Ingest multiple manifests sequentially."""
        with tempfile.TemporaryDirectory() as tmp:
            registry = MagicMock(spec=SchemaRegistry)
            ingestor = TraceIngestor(registry)

            manifests = []
            for i in range(3):
                manifest_path = os.path.join(tmp, f"manifest_{i}.json")
                manifest_data = {"version": "trace_manifest_v1", "trace_id": f"trace_{i}"}
                with open(manifest_path, "w") as f:
                    json.dump(manifest_data, f)
                manifests.append((manifest_path, manifest_data))

            for manifest_path, expected_data in manifests:
                result = ingestor.ingest_manifest(manifest_path)
                assert result == expected_data

    def test_manifest_with_validation_error(self):
        """Handles validation errors from registry."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {"version": "trace_manifest_v1", "trace_id": "abc123"}
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            registry.validate.side_effect = ValueError("Invalid schema")

            ingestor = TraceIngestor(registry)

            with pytest.raises(ValueError, match="Invalid schema"):
                ingestor.ingest_manifest(manifest_path)
