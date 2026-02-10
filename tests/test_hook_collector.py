import json
import os
import tempfile

from agentlab_runner import HarnessManifest, HookCollector, SchemaRegistry


def _write_manifest(path: str) -> None:
    manifest = {
        "schema_version": "harness_manifest_v1",
        "created_at": "2026-02-04T00:00:00Z",
        "integration_level": "cli_events",
        "step": {"semantics": "decision_cycle"},
        "hooks": {"schema_version": "hook_events_v1", "events_path": "/out/harness_events.jsonl"},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f)


def _event(event_type: str, seq: int, step_index=None):
    return {
        "hooks_schema_version": "hook_events_v1",
        "event_type": event_type,
        "ts": "2026-02-04T00:00:00Z",
        "seq": seq,
        "ids": {
            "run_id": "run_1",
            "trial_id": "trial_1",
            "variant_id": "v",
            "task_id": "t",
            "repl_idx": 0,
        },
        "step_index": step_index,
    }


def _model_call_end(seq: int, step_index: int):
    e = _event("model_call_end", seq, step_index)
    e.update(
        {
            "call_id": "call_1",
            "outcome": {"status": "ok"},
            "usage": {"tokens_in": 0, "tokens_out": 0},
            "timing": {"duration_ms": 1},
        }
    )
    return e


def test_seq_monotonic_enforced():
    with tempfile.TemporaryDirectory() as tmp:
        registry = SchemaRegistry(os.path.join(os.getcwd(), "schemas"))
        manifest_path = os.path.join(tmp, "harness_manifest.json")
        _write_manifest(manifest_path)
        manifest = HarnessManifest.load(manifest_path, registry)

        events_path = os.path.join(tmp, "harness_events.jsonl")
        events = [
            _event("agent_step_start", 2, 0),
            _event("agent_step_end", 1, 0),
        ]
        with open(events_path, "w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

        collector = HookCollector(registry)
        try:
            collector.collect(events_path, manifest)
        except ValueError as e:
            assert "monotonically increasing" in str(e)
        else:
            raise AssertionError("Expected seq monotonicity error")


def test_control_ack_required():
    with tempfile.TemporaryDirectory() as tmp:
        registry = SchemaRegistry(os.path.join(os.getcwd(), "schemas"))
        manifest_path = os.path.join(tmp, "harness_manifest.json")
        _write_manifest(manifest_path)
        manifest = HarnessManifest.load(manifest_path, registry)

        events_path = os.path.join(tmp, "harness_events.jsonl")
        events = [
            _event("agent_step_start", 1, 0),
            _model_call_end(2, 0),
            _event("agent_step_end", 3, 0),
            _event("agent_step_start", 4, 1),
        ]
        with open(events_path, "w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

        collector = HookCollector(registry)
        try:
            collector.collect(events_path, manifest)
        except ValueError as e:
            assert "Missing control_ack" in str(e)
        else:
            raise AssertionError("Expected missing control_ack error")
