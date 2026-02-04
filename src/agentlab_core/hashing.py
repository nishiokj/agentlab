import hashlib
from typing import Optional


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


class HashChain:
    """Minimal hash chain for JSONL event logs.

    Each event stores:
    - prev: hash of previous event line bytes
    - self: hash of current event line bytes
    """

    def __init__(self, prev_hash: Optional[str] = None) -> None:
        self.prev_hash = prev_hash

    def current_prev(self) -> Optional[str]:
        return self.prev_hash

    def hash_line(self, line_bytes: bytes) -> str:
        self_hash = sha256_bytes(line_bytes)
        self.prev_hash = self_hash
        return self_hash
