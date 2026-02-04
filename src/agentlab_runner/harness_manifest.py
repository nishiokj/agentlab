import json
from typing import Any, Dict

from .schemas import SchemaRegistry


class HarnessManifest:
    def __init__(self, data: Dict[str, Any]) -> None:
        self.data = data

    @property
    def integration_level(self) -> str:
        return self.data.get("integration_level", "cli_basic")

    @property
    def step_semantics(self) -> str:
        step = self.data.get("step") or {}
        return step.get("semantics", "")

    @property
    def hooks_schema_version(self) -> str:
        hooks = self.data.get("hooks") or {}
        return hooks.get("schema_version", "")

    @staticmethod
    def load(path: str, registry: SchemaRegistry) -> "HarnessManifest":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        registry.validate("harness_manifest_v1.jsonschema", data)
        return HarnessManifest(data)
