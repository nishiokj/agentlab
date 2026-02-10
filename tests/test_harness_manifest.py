"""Strong test suite for harness_manifest module."""
import json
import os
import tempfile
from unittest.mock import MagicMock

import pytest

from agentlab_runner.schemas import SchemaRegistry
from agentlab_runner.harness_manifest import HarnessManifest


class TestHarnessManifestInit:
    """Tests for HarnessManifest initialization."""

    def test_init_with_data(self):
        """Initialize with manifest data."""
        data = {
            "integration_level": "cli_events",
            "step": {"semantics": "exact"},
            "hooks": {"schema_version": "hook_events_v1"},
        }
        manifest = HarnessManifest(data)
        assert manifest.data == data

    def test_init_with_empty_data(self):
        """Initialize with empty data."""
        manifest = HarnessManifest({})
        assert manifest.data == {}


class TestHarnessManifestProperties:
    """Tests for HarnessManifest properties."""

    def test_integration_level_default(self):
        """Returns 'cli_basic' when not specified."""
        manifest = HarnessManifest({})
        assert manifest.integration_level == "cli_basic"

    def test_integration_level_explicit(self):
        """Returns specified integration level."""
        manifest = HarnessManifest({"integration_level": "sdk_control"})
        assert manifest.integration_level == "sdk_control"

    def test_step_semantics_default(self):
        """Returns empty string when step.semantics not specified."""
        manifest = HarnessManifest({})
        assert manifest.step_semantics == ""

    def test_step_semantics_explicit(self):
        """Returns specified step semantics."""
        manifest = HarnessManifest({"step": {"semantics": "exact"}})
        assert manifest.step_semantics == "exact"

    def test_step_semantics_with_empty_step(self):
        """Returns empty string when step is empty dict."""
        manifest = HarnessManifest({"step": {}})
        assert manifest.step_semantics == ""

    def test_step_semantics_with_step_without_semantics(self):
        """Returns empty string when step exists but has no semantics."""
        manifest = HarnessManifest({"step": {"other_key": "value"}})
        assert manifest.step_semantics == ""

    def test_hooks_schema_version_default(self):
        """Returns empty string when hooks.schema_version not specified."""
        manifest = HarnessManifest({})
        assert manifest.hooks_schema_version == ""

    def test_hooks_schema_version_explicit(self):
        """Returns specified hooks schema version."""
        manifest = HarnessManifest({"hooks": {"schema_version": "hook_events_v1"}})
        assert manifest.hooks_schema_version == "hook_events_v1"

    def test_hooks_schema_version_with_empty_hooks(self):
        """Returns empty string when hooks is empty dict."""
        manifest = HarnessManifest({"hooks": {}})
        assert manifest.hooks_schema_version == ""

    def test_hooks_schema_version_with_hooks_without_version(self):
        """Returns empty string when hooks exists but has no schema_version."""
        manifest = HarnessManifest({"hooks": {"other_key": "value"}})
        assert manifest.hooks_schema_version == ""


class TestHarnessManifestLoad:
    """Tests for HarnessManifest.load static method."""

    def test_load_basic_manifest(self):
        """Load a basic manifest file."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "integration_level": "cli_events",
                "step": {"semantics": "exact"},
                "hooks": {"schema_version": "hook_events_v1"},
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            manifest = HarnessManifest.load(manifest_path, registry)

            assert manifest.data == manifest_data
            registry.validate.assert_called_once_with(
                "harness_manifest_v1.jsonschema", manifest_data
            )

    def test_load_validates_against_schema(self):
        """Validates manifest against schema."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {"integration_level": "cli_events"}
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            HarnessManifest.load(manifest_path, registry)

            registry.validate.assert_called_once()

    def test_load_nonexistent_file(self):
        """Raises FileNotFoundError for missing file."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "nonexistent.json")

            registry = MagicMock(spec=SchemaRegistry)
            with pytest.raises(FileNotFoundError):
                HarnessManifest.load(manifest_path, registry)

    def test_load_invalid_json(self):
        """Raises JSONDecodeError for invalid JSON."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            with open(manifest_path, "w") as f:
                f.write("not valid json")

            registry = MagicMock(spec=SchemaRegistry)
            with pytest.raises(json.JSONDecodeError):
                HarnessManifest.load(manifest_path, registry)

    def test_load_complex_manifest(self):
        """Load complex manifest with nested data."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "integration_level": "sdk_control",
                "step": {
                    "semantics": "fuzzy",
                    "timeout": 30.0,
                    "retries": 3,
                },
                "hooks": {
                    "schema_version": "hook_events_v2",
                    "required": ["before_step", "after_step"],
                },
                "control_plane": {
                    "mode": "grpc",
                    "address": "localhost:50051",
                },
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            manifest = HarnessManifest.load(manifest_path, registry)

            assert manifest.integration_level == "sdk_control"
            assert manifest.step_semantics == "fuzzy"
            assert manifest.hooks_schema_version == "hook_events_v2"

    def test_load_minimal_manifest(self):
        """Load minimal manifest with only required fields."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {"integration_level": "cli_basic"}
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            manifest = HarnessManifest.load(manifest_path, registry)

            assert manifest.integration_level == "cli_basic"
            assert manifest.step_semantics == ""
            assert manifest.hooks_schema_version == ""

    def test_load_preserves_original_data(self):
        """Original data is preserved exactly as loaded."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "integration_level": "cli_events",
                "custom_field": "custom_value",
                "nested": {"a": 1, "b": 2},
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            manifest = HarnessManifest.load(manifest_path, registry)

            assert manifest.data["custom_field"] == "custom_value"
            assert manifest.data["nested"]["a"] == 1

    def test_load_with_validation_error(self):
        """Handles validation errors from registry."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {"integration_level": "cli_events"}
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            registry.validate.side_effect = ValueError("Invalid schema")

            with pytest.raises(ValueError, match="Invalid schema"):
                HarnessManifest.load(manifest_path, registry)

    def test_load_with_special_characters(self):
        """Handles special characters in values."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "integration_level": "cli_events",
                "description": "Test with\nnewline and\ttab",
                "unicode": "Hello 世界 🌍",
            }
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            manifest = HarnessManifest.load(manifest_path, registry)

            assert manifest.data["description"] == "Test with\nnewline and\ttab"
            assert manifest.data["unicode"] == "Hello 世界 🌍"

    def test_load_with_large_manifest(self):
        """Handles large manifest files."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "integration_level": "cli_events",
                "steps": [{"name": f"step_{i}", "duration": i} for i in range(1000)],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            manifest = HarnessManifest.load(manifest_path, registry)

            assert len(manifest.data["steps"]) == 1000
            assert manifest.data["steps"][0]["name"] == "step_0"
            assert manifest.data["steps"][999]["name"] == "step_999"


class TestHarnessManifestIntegration:
    """Integration tests for HarnessManifest."""

    def test_load_and_access_properties(self):
        """Load manifest and access all properties."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, "manifest.json")
            manifest_data = {
                "integration_level": "sdk_full",
                "step": {"semantics": "exact"},
                "hooks": {"schema_version": "hook_events_v3"},
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            registry = MagicMock(spec=SchemaRegistry)
            manifest = HarnessManifest.load(manifest_path, registry)

            assert manifest.integration_level == "sdk_full"
            assert manifest.step_semantics == "exact"
            assert manifest.hooks_schema_version == "hook_events_v3"

    def test_multiple_manifests(self):
        """Load multiple manifests independently."""
        with tempfile.TemporaryDirectory() as tmp:
            registry = MagicMock(spec=SchemaRegistry)

            manifests = []
            for i in range(3):
                manifest_path = os.path.join(tmp, f"manifest_{i}.json")
                manifest_data = {
                    "integration_level": "cli_events",
                    "index": i,
                }
                with open(manifest_path, "w") as f:
                    json.dump(manifest_data, f)

                manifest = HarnessManifest.load(manifest_path, registry)
                manifests.append(manifest)

            assert manifests[0].data["index"] == 0
            assert manifests[1].data["index"] == 1
            assert manifests[2].data["index"] == 2
