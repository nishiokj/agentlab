import os
import zipfile
from typing import List, Optional


def build_debug_bundle(run_dir: str, out_path: str, extra_paths: Optional[List[str]] = None) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    include = [
        "manifest.json",
        "resolved_experiment.json",
        "resolved_experiment.digest",
        "attestation.json",
        "analysis/summary.json",
        "analysis/comparisons.json",
        "grades.json",
    ]
    if extra_paths:
        include.extend(extra_paths)

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel in include:
            abs_path = os.path.join(run_dir, rel)
            if os.path.exists(abs_path):
                zf.write(abs_path, arcname=rel)

    return out_path
