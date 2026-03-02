"""Thin wrapper for importing custom benchmark tasks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from bench.config import BenchConfig
from bench.taskkit.importer import import_suite


def main() -> None:
    parser = argparse.ArgumentParser(description="Import custom benchmark suite")
    parser.add_argument("--source", required=True)
    parser.add_argument("--suite", required=True)
    parser.add_argument("--repo-map", required=True, help="JSON mapping")
    args = parser.parse_args()

    cfg = BenchConfig.from_root(Path(__file__).resolve().parent.parent)
    mapping = json.loads(args.repo_map)
    result = import_suite(Path(args.source).resolve(), args.suite, mapping, cfg)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
