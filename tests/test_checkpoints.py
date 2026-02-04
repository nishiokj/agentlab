import os
import tempfile

from agentlab_core import ArtifactStore
from agentlab_runner import CheckpointManager


def test_checkpoint_save_restore():
    with tempfile.TemporaryDirectory() as tmp:
        artifacts_root = os.path.join(tmp, "artifacts")
        checkpoints_dir = os.path.join(tmp, "checkpoints")
        surface = os.path.join(tmp, "workspace")
        os.makedirs(surface, exist_ok=True)

        file_path = os.path.join(surface, "file.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("hello")

        store = ArtifactStore(artifacts_root)
        manager = CheckpointManager(checkpoints_dir, store)
        ckpt = manager.save("step0", {"workspace": surface})

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("changed")

        manager.restore(os.path.join(checkpoints_dir, "checkpoint_step0.json"))
        with open(file_path, "r", encoding="utf-8") as f:
            assert f.read() == "hello"
