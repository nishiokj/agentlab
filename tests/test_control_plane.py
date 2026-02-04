import os
import tempfile

from agentlab_runner import write_control_action


def test_control_plane_hash():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "lab_control.json")
        digest = write_control_action(path, {"action": "continue"})
        assert digest.startswith("sha256:")
        assert os.path.exists(path)
