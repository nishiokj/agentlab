import os
from typing import Dict

from agentlab_core.canonical_json import canonical_dumps
from agentlab_core.hashing import sha256_bytes


def write_control_action(path: str, action: Dict) -> str:
    """Write control action JSON and return control_version (sha256 of file bytes)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = canonical_dumps(action).encode("utf-8")
    tmp_path = path + ".tmp"
    with open(tmp_path, "wb") as f:
        f.write(data)
    os.replace(tmp_path, path)
    return sha256_bytes(data)
