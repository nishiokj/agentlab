import os
import tempfile
import zipfile

from agentlab_runner.publish import publish_run


def test_publish_creates_bundle():
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = os.path.join(tmp, "run")
        os.makedirs(run_dir, exist_ok=True)
        bundle = publish_run(run_dir)
        assert os.path.exists(bundle)


def test_publish_full_bundle_includes_trials():
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = os.path.join(tmp, "run")
        os.makedirs(run_dir, exist_ok=True)
        os.makedirs(os.path.join(run_dir, "trials", "trial_1"), exist_ok=True)
        with open(os.path.join(run_dir, "manifest.json"), "w", encoding="utf-8") as f:
            f.write("{}")
        with open(
            os.path.join(run_dir, "trials", "trial_1", "trial_output.json"),
            "w",
            encoding="utf-8",
        ) as f:
            f.write("{}")

        bundle = publish_run(run_dir)
        assert bundle.endswith(".full.zip")
        assert os.path.exists(bundle)
        with zipfile.ZipFile(bundle, "r") as zf:
            entries = set(zf.namelist())
        assert "trials/trial_1/trial_output.json" in entries
