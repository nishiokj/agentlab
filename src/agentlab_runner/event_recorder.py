import json
import os
from typing import Any, Dict, Optional

from agentlab_core.canonical_json import canonical_dumps
from agentlab_core.hashing import sha256_bytes
from agentlab_core.artifact_store import ArtifactStore


GENESIS_HASH = "sha256:" + "0" * 64


def _hash_event_without_self(event: Dict[str, Any]) -> str:
    # Hash canonical JSON with hashchain.self omitted for determinism.
    copy = json.loads(json.dumps(event))
    if "hashchain" in copy and isinstance(copy["hashchain"], dict):
        copy["hashchain"].pop("self", None)
    return sha256_bytes(canonical_dumps(copy).encode("utf-8"))


class EventRecorder:
    """Append-only recorder for framework events.

    Hashing rule: hashchain.self is computed from canonical JSON with hashchain.self omitted.
    hashchain.prev is the previous event's self hash (or GENESIS_HASH for the first event).
    """

    def __init__(
        self,
        events_path: str,
        artifact_store: ArtifactStore,
        schema_registry: Optional[object] = None,
    ) -> None:
        self.events_path = events_path
        self.artifact_store = artifact_store
        self.schema_registry = schema_registry
        self.prev_hash: str = GENESIS_HASH
        os.makedirs(os.path.dirname(events_path), exist_ok=True)
        self._fh = open(events_path, "a", encoding="utf-8")

    def record(
        self,
        event: Dict[str, Any],
        payload_bytes: Optional[bytes] = None,
        redaction: Optional[Dict[str, Any]] = None,
        validate_schema: bool = False,
    ) -> Dict[str, Any]:
        obj = json.loads(json.dumps(event))

        if payload_bytes is not None:
            obj["payload_ref"] = self.artifact_store.put_bytes(payload_bytes)

        if "redaction" not in obj:
            obj["redaction"] = redaction or {"applied": False, "mode": "store"}

        obj["hashchain"] = {"prev": self.prev_hash, "self": ""}
        self_hash = _hash_event_without_self(obj)
        obj["hashchain"]["self"] = self_hash

        if validate_schema and self.schema_registry is not None:
            self.schema_registry.validate("event_envelope_v1.jsonschema", obj)

        line = canonical_dumps(obj)
        self._fh.write(line + "\n")
        self._fh.flush()
        self.prev_hash = self_hash
        return obj

    def finalize(self, head_path: str) -> None:
        os.makedirs(os.path.dirname(head_path), exist_ok=True)
        with open(head_path, "w", encoding="utf-8") as f:
            f.write(self.prev_hash)

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()
