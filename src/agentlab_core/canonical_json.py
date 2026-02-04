import json
from typing import Any


def canonical_dumps(obj: Any) -> str:
    """Serialize to a canonical JSON string for stable hashing.

    Rules:
    - UTF-8, ASCII-only output
    - Sorted object keys
    - No insignificant whitespace
    """
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def canonical_bytes(obj: Any) -> bytes:
    return canonical_dumps(obj).encode("utf-8")
