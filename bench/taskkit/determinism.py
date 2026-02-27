"""Determinism utilities for reproducible benchmark execution.

Provides:
- Seeded RNG management
- Stable sorting/serialization
- Deterministic environment enforcement
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from typing import Any

from bench.config import BenchConfig

# Fixed defaults
DEFAULT_SEED = 42
DETERMINISM_ENV = {
    "PYTHONHASHSEED": "0",
    "TZ": "UTC",
    "LC_ALL": "C.UTF-8",
    "LANG": "C.UTF-8",
    "SOURCE_DATE_EPOCH": "1700000000",
    "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
}


def get_seeded_rng(seed: int = DEFAULT_SEED) -> random.Random:
    """Return a seeded Random instance for deterministic generation."""
    return random.Random(seed)


def stable_json(obj: Any) -> str:
    """Serialize to JSON with deterministic key ordering and consistent formatting.

    - Keys are sorted
    - No trailing whitespace
    - Consistent float representation
    - UTF-8, no ASCII escaping
    """
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=None, separators=(",", ":"))


def stable_json_pretty(obj: Any) -> str:
    """Like stable_json but with indentation for human readability."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=2)


def stable_hash(data: str | bytes) -> str:
    """Compute a stable SHA256 hex digest."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def hash_jsonl_file(path: str | os.PathLike) -> str:
    """Compute a deterministic hash of a JSONL file (order-sensitive)."""
    with open(path) as f:
        lines = f.readlines()
    # Re-serialize each line with stable_json to normalize
    normalized = []
    for line in lines:
        line = line.strip()
        if line:
            obj = json.loads(line)
            normalized.append(stable_json(obj))
    combined = "\n".join(normalized)
    return stable_hash(combined)


def enforce_determinism_env(env: dict[str, str] | None = None) -> dict[str, str]:
    """Return a complete environment dict with determinism settings applied."""
    result = os.environ.copy()
    result.update(DETERMINISM_ENV)
    if env:
        result.update(env)
    return result


def verify_determinism(hash1: str, hash2: str, label: str = "") -> bool:
    """Compare two hashes and return True if identical."""
    if hash1 != hash2:
        return False
    return True
