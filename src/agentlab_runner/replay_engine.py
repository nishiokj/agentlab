import json
from typing import Any, Dict, Optional

from agentlab_core.artifact_store import ArtifactStore


class EventIndex:
    def __init__(self, events_path: str) -> None:
        self.by_seq: Dict[int, Dict[str, Any]] = {}
        with open(events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                seq = obj.get("seq")
                if seq is not None:
                    self.by_seq[int(seq)] = obj

    def get_by_seq(self, seq: int) -> Optional[Dict[str, Any]]:
        return self.by_seq.get(seq)


class EventReplayer:
    def __init__(self, events_path: str, artifact_store: ArtifactStore) -> None:
        self.index = EventIndex(events_path)
        self.artifact_store = artifact_store

    def get_event(self, seq: int) -> Dict[str, Any]:
        event = self.index.get_by_seq(seq)
        if event is None:
            raise KeyError(f"No event with seq {seq}")
        return event

    def get_payload(self, event: Dict[str, Any]) -> Optional[bytes]:
        ref = event.get("payload_ref")
        if not ref:
            return None
        return self.artifact_store.get_bytes(ref)
