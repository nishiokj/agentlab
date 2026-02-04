import os
import tempfile

from agentlab_report import build_report


def test_report_builder():
    with tempfile.TemporaryDirectory() as tmp:
        analysis_dir = os.path.join(tmp, "analysis")
        os.makedirs(analysis_dir, exist_ok=True)
        with open(os.path.join(analysis_dir, "comparisons.json"), "w", encoding="utf-8") as f:
            f.write('{"comparisons": []}')
        out_dir = os.path.join(tmp, "report")
        path = build_report(tmp, out_dir)
        assert os.path.exists(path)
