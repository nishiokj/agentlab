import json
from typing import Any, Dict

from .schemas import SchemaRegistry


class TraceIngestor:
    def __init__(self, schema_registry: SchemaRegistry) -> None:
        self.registry = schema_registry

    def ingest_manifest(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.registry.validate("trace_manifest_v1.jsonschema", data)
        return data

    def ingest_otlp(self) -> None:
        raise NotImplementedError("OTLP receiver is not implemented in Phase 1")
