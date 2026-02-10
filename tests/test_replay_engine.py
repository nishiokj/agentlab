"""Strong test suite for replay_engine module."""
import json
import os
import tempfile

import pytest

from agentlab_core import ArtifactStore
from agentlab_runner.replay_engine import EventIndex, EventReplayer


class TestEventIndex:
    """Tests for EventIndex class."""

    def test_event_index_init_empty_file(self):
        """Initialize with empty events file."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            with open(events_path, "w") as f:
                f.write("")

            index = EventIndex(events_path)
            assert index.by_seq == {}

    def test_event_index_single_event(self):
        """Index a single event with sequence number."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            event = {"seq": 1, "type": "test", "data": "value"}
            with open(events_path, "w") as f:
                f.write(json.dumps(event))

            index = EventIndex(events_path)
            assert 1 in index.by_seq
            assert index.by_seq[1] == event

    def test_event_index_multiple_events(self):
        """Index multiple events."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            events = [
                {"seq": 0, "type": "start"},
                {"seq": 1, "type": "step"},
                {"seq": 2, "type": "end"},
            ]
            with open(events_path, "w") as f:
                for event in events:
                    f.write(json.dumps(event) + "\n")

            index = EventIndex(events_path)
            assert len(index.by_seq) == 3
            for event in events:
                assert index.by_seq[event["seq"]] == event

    def test_event_index_ignores_blank_lines(self):
        """Blank lines are ignored."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            with open(events_path, "w") as f:
                f.write('{"seq": 1, "type": "test"}\n\n\n')
                f.write('{"seq": 2, "type": "test2"}\n')

            index = EventIndex(events_path)
            assert len(index.by_seq) == 2
            assert 1 in index.by_seq
            assert 2 in index.by_seq

    def test_event_index_trims_whitespace(self):
        """Lines are stripped of leading/trailing whitespace."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            with open(events_path, "w") as f:
                f.write('  {"seq": 1, "type": "test"}  \n')

            index = EventIndex(events_path)
            assert 1 in index.by_seq
            assert index.by_seq[1]["type"] == "test"

    def test_event_index_no_seq_field(self):
        """Events without seq field are not indexed."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            with open(events_path, "w") as f:
                f.write('{"type": "no_seq"}\n')
                f.write('{"seq": 1, "type": "has_seq"}\n')

            index = EventIndex(events_path)
            assert len(index.by_seq) == 1
            assert 1 in index.by_seq
            assert 0 not in index.by_seq

    def test_event_index_str_seq_converted(self):
        """String seq values are converted to int."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            # Note: seq as string
            with open(events_path, "w") as f:
                f.write('{"seq": "1", "type": "test"}')

            index = EventIndex(events_path)
            assert 1 in index.by_seq
            assert "1" not in index.by_seq

    def test_event_index_negative_seq(self):
        """Negative sequence numbers work."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            with open(events_path, "w") as f:
                f.write('{"seq": -1, "type": "test"}')

            index = EventIndex(events_path)
            assert -1 in index.by_seq

    def test_event_index_large_seq(self):
        """Large sequence numbers work."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            large_seq = 999999
            with open(events_path, "w") as f:
                f.write(f'{{"seq": {large_seq}, "type": "test"}}')

            index = EventIndex(events_path)
            assert large_seq in index.by_seq

    def test_get_by_seq_exists(self):
        """Retrieve event by sequence number."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            event = {"seq": 5, "type": "test"}
            with open(events_path, "w") as f:
                f.write(json.dumps(event))

            index = EventIndex(events_path)
            retrieved = index.get_by_seq(5)
            assert retrieved == event

    def test_get_by_seq_not_exists(self):
        """Returns None for missing sequence number."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            with open(events_path, "w") as f:
                f.write('{"seq": 1, "type": "test"}')

            index = EventIndex(events_path)
            result = index.get_by_seq(999)
            assert result is None

    def test_event_index_duplicate_seq(self):
        """Later events with same seq overwrite earlier ones."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            with open(events_path, "w") as f:
                f.write('{"seq": 1, "type": "first"}\n')
                f.write('{"seq": 1, "type": "second"}\n')

            index = EventIndex(events_path)
            assert index.by_seq[1]["type"] == "second"


class TestEventReplayer:
    """Tests for EventReplayer class."""

    def test_event_replayer_init(self):
        """Initialize replayer with events and artifact store."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            artifacts_root = os.path.join(tmp, "artifacts")
            os.makedirs(artifacts_root)

            with open(events_path, "w") as f:
                f.write('{"seq": 1, "type": "test"}')

            store = ArtifactStore(artifacts_root)
            replayer = EventReplayer(events_path, store)

            assert replayer.index is not None
            assert replayer.artifact_store == store

    def test_get_event_exists(self):
        """Retrieve event by sequence number."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            artifacts_root = os.path.join(tmp, "artifacts")
            os.makedirs(artifacts_root)

            event = {"seq": 10, "type": "step", "action": "click"}
            with open(events_path, "w") as f:
                f.write(json.dumps(event))

            store = ArtifactStore(artifacts_root)
            replayer = EventReplayer(events_path, store)

            retrieved = replayer.get_event(10)
            assert retrieved == event

    def test_get_event_not_found(self):
        """Raises KeyError for missing event."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            artifacts_root = os.path.join(tmp, "artifacts")
            os.makedirs(artifacts_root)

            with open(events_path, "w") as f:
                f.write('{"seq": 1, "type": "test"}')

            store = ArtifactStore(artifacts_root)
            replayer = EventReplayer(events_path, store)

            with pytest.raises(KeyError, match="No event with seq 999"):
                replayer.get_event(999)

    def test_get_payload_no_ref(self):
        """Returns None when event has no payload_ref."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            artifacts_root = os.path.join(tmp, "artifacts")
            os.makedirs(artifacts_root)

            event = {"seq": 1, "type": "test"}
            store = ArtifactStore(artifacts_root)
            replayer = EventReplayer(events_path, store)

            payload = replayer.get_payload(event)
            assert payload is None

    def test_get_payload_with_ref(self):
        """Retrieves payload bytes from artifact store."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            artifacts_root = os.path.join(tmp, "artifacts")
            os.makedirs(artifacts_root)

            original_data = b"binary payload content"
            store = ArtifactStore(artifacts_root)
            ref = store.put_bytes(original_data)

            event = {"seq": 1, "payload_ref": ref}
            replayer = EventReplayer(events_path, store)

            retrieved_data = replayer.get_payload(event)
            assert retrieved_data == original_data

    def test_get_payload_large_data(self):
        """Retrieves large payloads."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            artifacts_root = os.path.join(tmp, "artifacts")
            os.makedirs(artifacts_root)

            # Create 10MB payload
            large_data = b"x" * (10 * 1024 * 1024)
            store = ArtifactStore(artifacts_root)
            ref = store.put_bytes(large_data)

            event = {"seq": 1, "payload_ref": ref}
            replayer = EventReplayer(events_path, store)

            retrieved_data = replayer.get_payload(event)
            assert len(retrieved_data) == len(large_data)
            assert retrieved_data == large_data

    def test_get_payload_binary_data(self):
        """Handles binary payloads correctly."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            artifacts_root = os.path.join(tmp, "artifacts")
            os.makedirs(artifacts_root)

            # Binary data with null bytes and other special bytes
            binary_data = b"\x00\x01\x02\xff\xfe\xfd"
            store = ArtifactStore(artifacts_root)
            ref = store.put_bytes(binary_data)

            event = {"seq": 1, "payload_ref": ref}
            replayer = EventReplayer(events_path, store)

            retrieved_data = replayer.get_payload(event)
            assert retrieved_data == binary_data

    def test_get_event_and_payload_integration(self):
        """Integration: get event then get its payload."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "events.jsonl")
            artifacts_root = os.path.join(tmp, "artifacts")
            os.makedirs(artifacts_root)

            store = ArtifactStore(artifacts_root)
            payload_data = b"screenshot image data"
            ref = store.put_bytes(payload_data)

            event = {
                "seq": 5,
                "type": "screenshot",
                "timestamp": "2024-01-01T00:00:00Z",
                "payload_ref": ref,
            }

            with open(events_path, "w") as f:
                f.write(json.dumps(event))

            replayer = EventReplayer(events_path, store)

            retrieved_event = replayer.get_event(5)
            assert retrieved_event == event

            retrieved_payload = replayer.get_payload(retrieved_event)
            assert retrieved_payload == payload_data

    def test_event_index_nonexistent_file(self):
        """Raises FileNotFoundError for missing events file."""
        with tempfile.TemporaryDirectory() as tmp:
            events_path = os.path.join(tmp, "nonexistent.jsonl")

            with pytest.raises(FileNotFoundError):
                EventIndex(events_path)
