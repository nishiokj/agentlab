import json
import os
import tempfile

from agentlab_core import ArtifactStore
from agentlab_runner.event_recorder import EventRecorder, GENESIS_HASH


def test_event_recorder_hashchain():
    with tempfile.TemporaryDirectory() as tmp:
        artifacts_root = os.path.join(tmp, "artifacts")
        events_path = os.path.join(tmp, "events.jsonl")
        store = ArtifactStore(artifacts_root)
        recorder = EventRecorder(events_path, store)

        e1 = {"event_type": "test", "seq": 1}
        e2 = {"event_type": "test", "seq": 2}

        r1 = recorder.record(e1)
        r2 = recorder.record(e2)
        recorder.close()

        assert r1["hashchain"]["prev"] == GENESIS_HASH
        assert r2["hashchain"]["prev"] == r1["hashchain"]["self"]

        with open(events_path, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        assert len(lines) == 2
