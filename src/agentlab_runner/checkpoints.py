import io
import json
import os
import tarfile
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from agentlab_core.artifact_store import ArtifactStore


class CheckpointManager:
    def __init__(self, checkpoints_dir: str, artifact_store: ArtifactStore) -> None:
        self.checkpoints_dir = checkpoints_dir
        self.artifact_store = artifact_store
        os.makedirs(self.checkpoints_dir, exist_ok=True)

    def _tar_path(self, source_path: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".tar.gz")
        os.close(fd)
        with tarfile.open(path, "w:gz") as tar:
            # Store contents relative to root to avoid nesting on restore.
            tar.add(source_path, arcname=".")
        return path

    def save(
        self,
        label: str,
        surfaces: Dict[str, str],
        runtime_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        checkpoint = {
            "label": label,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "surfaces": {},
            "runtime_state": runtime_state or {},
        }

        for name, path in surfaces.items():
            if not os.path.exists(path):
                raise FileNotFoundError(f"Surface path not found: {path}")
            tar_path = self._tar_path(path)
            try:
                ref = self.artifact_store.put_file(tar_path)
            finally:
                os.remove(tar_path)
            checkpoint["surfaces"][name] = {
                "path": path,
                "artifact_ref": ref,
            }

        out_path = os.path.join(self.checkpoints_dir, f"checkpoint_{label}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f)

        return checkpoint

    def _safe_extract(self, tar: tarfile.TarFile, path: str) -> None:
        for member in tar.getmembers():
            member_path = os.path.join(path, member.name)
            if not os.path.realpath(member_path).startswith(os.path.realpath(path)):
                raise ValueError("Unsafe path in tar archive")
        tar.extractall(path)

    def restore(self, checkpoint_path: str) -> Dict[str, Any]:
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)

        for surface in checkpoint.get("surfaces", {}).values():
            dest_path = surface["path"]
            ref = surface["artifact_ref"]
            data = self.artifact_store.get_bytes(ref)
            with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
                tmp.write(data)
                tmp_path = tmp.name
            try:
                with tarfile.open(tmp_path, "r:gz") as tar:
                    os.makedirs(dest_path, exist_ok=True)
                    self._safe_extract(tar, dest_path)
            finally:
                os.remove(tmp_path)

        return checkpoint
