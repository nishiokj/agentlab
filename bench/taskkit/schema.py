"""Schema validation utilities for task, trace, and score."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import yaml


def load_schema(schema_path: Path) -> dict[str, Any]:
    """Load a JSON Schema file."""
    return json.loads(schema_path.read_text())


def validate_json(data: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    """Validate data against a JSON schema. Returns list of error messages."""
    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
    return [
        f"{'.'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in errors
    ]


def validate_task_yaml(task_yaml_path: Path, schema_path: Path) -> list[str]:
    """Validate a task.yaml file against the task schema."""
    with open(task_yaml_path) as f:
        data = yaml.safe_load(f)
    schema = load_schema(schema_path)
    return validate_json(data, schema)


def validate_trace_record(record: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    """Validate a single trace record against the trace schema."""
    return validate_json(record, schema)


def validate_score(score: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    """Validate a score.json against the score schema."""
    return validate_json(score, schema)


def validate_all_schemas(schemas_dir: Path) -> list[str]:
    """Validate that all benchmark schemas are well-formed JSON Schema documents.

    Returns a list of error messages (empty if all valid).
    """
    errors: list[str] = []
    expected = ["task.schema.json", "trace.schema.json", "score.schema.json"]
    for name in expected:
        path = schemas_dir / name
        if not path.exists():
            errors.append(f"Missing schema file: {path}")
            continue
        try:
            schema = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON in {name}: {e}")
            continue
        # Check that the schema itself is a valid JSON Schema
        meta_schema_uri = schema.get("$schema", "")
        try:
            jsonschema.Draft202012Validator.check_schema(schema)
        except jsonschema.SchemaError as e:
            errors.append(f"Invalid schema {name}: {e.message}")
    return errors


def load_task_yaml(task_dir: Path) -> dict[str, Any]:
    """Load and return task.yaml data from a task directory."""
    task_yaml = task_dir / "task.yaml"
    if not task_yaml.exists():
        raise FileNotFoundError(f"task.yaml not found in {task_dir}")
    with open(task_yaml) as f:
        return yaml.safe_load(f)
