import os
import shutil
from typing import Optional

from .hashing import sha256_bytes, sha256_file


class ArtifactStore:
    """Content-addressed artifact store under <root>/sha256/<hash>."""

    def __init__(self, root: str) -> None:
        self.root = root

    def _path_for_hash(self, hex_hash: str) -> str:
        return os.path.join(self.root, "sha256", hex_hash)

    def put_bytes(
        self,
        data: bytes,
        content_type: Optional[str] = None,
        redaction_meta: Optional[dict] = None,
    ) -> str:
        digest = sha256_bytes(data)
        hex_hash = digest.split(":", 1)[1]
        path = self._path_for_hash(hex_hash)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if not os.path.exists(path):
            # Write atomically when possible.
            tmp_path = path + ".tmp"
            with open(tmp_path, "wb") as f:
                f.write(data)
            os.replace(tmp_path, path)

        return f"artifact://sha256/{hex_hash}"

    def put_file(
        self,
        path: str,
        content_type: Optional[str] = None,
        redaction_meta: Optional[dict] = None,
    ) -> str:
        digest = sha256_file(path)
        hex_hash = digest.split(":", 1)[1]
        dest_path = self._path_for_hash(hex_hash)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        if not os.path.exists(dest_path):
            shutil.copyfile(path, dest_path)
        return f"artifact://sha256/{hex_hash}"

    def get_bytes(self, ref: str) -> bytes:
        if not ref.startswith("artifact://sha256/"):
            raise ValueError("Invalid artifact ref")
        hex_hash = ref.split("/")[-1]
        path = self._path_for_hash(hex_hash)
        with open(path, "rb") as f:
            return f.read()

    def has(self, ref: str) -> bool:
        if not ref.startswith("artifact://sha256/"):
            return False
        hex_hash = ref.split("/")[-1]
        path = self._path_for_hash(hex_hash)
        return os.path.exists(path)
