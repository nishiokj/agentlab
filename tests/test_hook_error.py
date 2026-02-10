import json
import os
import tempfile

from agentlab_runner import HarnessManifest, HookCollector, HookValidationError, SchemaRegistry


def test_hook_validation_error_includes_line_num():
    with tempfile.TemporaryDirectory() as tmp:
        registry = SchemaRegistry(os.path.join(os.getcwd(), "schemas"))
        manifest_path = os.path.join(tmp, "harness_manifest.json")
        manifest = {
            "schema_version": "harness_manifest_v1",
            "created_at": "2026-02-04T00:00:00Z",
            "integration_level": "cli_events",
            "step": {"semantics": "decision_cycle"},
            "hooks": {"schema_version": "hook_events_v1", "events_path": "/out/harness_events.jsonl"},
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f)
        m = HarnessManifest.load(manifest_path, registry)

        events_path = os.path.join(tmp, "harness_events.jsonl")
        with open(events_path, "w", encoding="utf-8") as f:
            f.write("{not-json}\n")

        collector = HookCollector(registry)
        try:
            collector.collect(events_path, m)
        except HookValidationError as e:
            assert e.line_num == 1
            assert "Invalid JSON" in str(e)
        else:
            raise AssertionError("Expected HookValidationError")
