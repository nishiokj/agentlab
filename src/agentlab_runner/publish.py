import os
import shutil
import zipfile
from typing import Optional

from agentlab_runner.debug_bundle import build_debug_bundle


def _append_tree(bundle_path: str, run_dir: str, rel_dir: str) -> None:
    abs_dir = os.path.join(run_dir, rel_dir)
    if not os.path.isdir(abs_dir):
        return
    with zipfile.ZipFile(bundle_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(abs_dir):
            for name in sorted(files):
                abs_path = os.path.join(root, name)
                arcname = os.path.relpath(abs_path, run_dir)
                zf.write(abs_path, arcname=arcname)


def publish_run(run_dir: str, out_path: Optional[str] = None) -> str:
    if out_path is None:
        out_path = os.path.join(run_dir, "publish", "bundle.zip")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Use debug bundle as the base, then add key directories if present.
    build_debug_bundle(run_dir, out_path)

    # If additional artifacts exist, create a separate full bundle.
    full_bundle = out_path
    if os.path.isdir(os.path.join(run_dir, "trials")):
        # make a full copy alongside
        full_bundle = out_path[:-4] + ".full.zip" if out_path.endswith(".zip") else out_path + ".full.zip"
        if full_bundle != out_path:
            shutil.copyfile(out_path, full_bundle)
        _append_tree(full_bundle, run_dir, "trials")
        _append_tree(full_bundle, run_dir, "artifacts")

    return full_bundle
