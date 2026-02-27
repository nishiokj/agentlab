# Repository Snapshots

This directory contains pinned repository snapshots for the benchmark.

## Structure

Each repository has:
- `src.tar.zst`: Compressed source snapshot
- `baseline_commit.txt`: Git commit hash of the snapshot
- `deps/requirements.lock`: Pinned pip dependencies
- `deps/constraints.txt`: Pip constraints for reproducibility
- `injections/README.md`: Documentation of available injection patches
- `LICENSES/`: License files for the repository

## v0 Repositories

| Repo | License | Description |
|------|---------|-------------|
| click | BSD-3-Clause | CLI toolkit |
| rich | MIT | Terminal formatting |
| jinja2 | BSD-3-Clause | Template engine |

## Smoke Test

```bash
python -m bench.cli repo-smoke --repo click
python -m bench.cli repo-smoke --repo rich
python -m bench.cli repo-smoke --repo jinja2
```
