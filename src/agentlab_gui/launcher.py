from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


def main() -> int:
    streamlit = shutil.which("streamlit")
    if not streamlit:
        print("streamlit is not installed. Install GUI deps: pip install 'agentlab[gui]'", file=sys.stderr)
        return 1

    app_path = Path(__file__).with_name("app.py")
    cmd = [streamlit, "run", str(app_path)]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())

