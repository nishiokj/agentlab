import json
import os
from typing import Any, Dict

from jsonschema import Draft202012Validator


class SchemaRegistry:
    def __init__(self, schema_dir: str) -> None:
        self.schema_dir = schema_dir
        self._cache: Dict[str, dict] = {}

    def load(self, filename: str) -> dict:
        if filename not in self._cache:
            path = os.path.join(self.schema_dir, filename)
            with open(path, "r", encoding="utf-8") as f:
                self._cache[filename] = json.load(f)
        return self._cache[filename]

    def validate(self, filename: str, data: Any) -> None:
        schema = self.load(filename)
        validator = Draft202012Validator(schema)
        errors = sorted(validator.iter_errors(data), key=lambda e: e.path)
        if errors:
            msg = "; ".join([e.message for e in errors[:5]])
            raise ValueError(f"Schema validation failed: {msg}")
