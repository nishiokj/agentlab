#!/usr/bin/env python3
"""Shared SWE-bench metadata extraction helpers.

This module keeps fallback order centralized so adapter/grader behavior
does not drift.
"""

from __future__ import annotations

from typing import Any


_SENTINEL = object()


def _coerce_non_empty_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    return trimmed if trimmed else None


def _read_path(payload: Any, path: tuple[str, ...]) -> Any:
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return _SENTINEL
        current = current.get(key, _SENTINEL)
        if current is _SENTINEL:
            return _SENTINEL
    return current


def _contextual_payloads(payload: Any) -> list[Any]:
    contexts: list[Any] = [payload]
    if isinstance(payload, dict):
        task_value = payload.get("task")
        if isinstance(task_value, dict):
            contexts.append(task_value)
    return contexts


def _first_string(payload: Any, candidates: list[tuple[str, ...]]) -> str | None:
    for context in _contextual_payloads(payload):
        for candidate in candidates:
            value = _read_path(context, candidate)
            if value is _SENTINEL:
                continue
            coerced = _coerce_non_empty_string(value)
            if coerced is not None:
                return coerced
    return None


def extract_swebench_meta(payload: Any) -> dict[str, str | None]:
    """Extract SWE-bench metadata from known payload shapes.

    Supported shapes:
    - trial-input shape: task.swebench.input.*
    - task-boundary shape: swebench.input.*
    - instance_id fallbacks: task.input.instance_id, input.instance_id
    """

    base_paths = {
        "repo": [
            ("task", "swebench", "input", "repo"),
            ("swebench", "input", "repo"),
        ],
        "base_commit": [
            ("task", "swebench", "input", "base_commit"),
            ("swebench", "input", "base_commit"),
        ],
        "instance_id": [
            ("task", "swebench", "input", "instance_id"),
            ("swebench", "input", "instance_id"),
            ("task", "input", "instance_id"),
            ("input", "instance_id"),
        ],
        "problem_statement": [
            ("task", "swebench", "input", "problem_statement"),
            ("swebench", "input", "problem_statement"),
            ("task", "input", "problem_statement"),
            ("input", "problem_statement"),
        ],
    }
    return {
        key: _first_string(payload, candidates) for key, candidates in base_paths.items()
    }
