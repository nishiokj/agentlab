"""Benchmark configuration and path resolution."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class BenchConfig:
    """Immutable benchmark configuration resolved from the repo root."""

    root: Path
    bench_dir: Path = field(init=False)
    schemas_dir: Path = field(init=False)
    repos_dir: Path = field(init=False)
    tasks_dir: Path = field(init=False)
    runs_dir: Path = field(init=False)
    reports_dir: Path = field(init=False)
    scripts_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "bench_dir", self.root / "bench")
        object.__setattr__(self, "schemas_dir", self.root / "schemas")
        object.__setattr__(self, "repos_dir", self.root / "repos")
        object.__setattr__(self, "tasks_dir", self.root / "tasks")
        object.__setattr__(self, "runs_dir", self.root / "runs")
        object.__setattr__(self, "reports_dir", self.root / "reports")
        object.__setattr__(self, "scripts_dir", self.root / "scripts")

    @classmethod
    def from_root(cls, root: Path) -> BenchConfig:
        return cls(root=root.resolve())

    # Default determinism environment variables
    DETERMINISM_ENV: dict[str, str] = field(default_factory=lambda: {
        "PYTHONHASHSEED": "0",
        "TZ": "UTC",
        "LC_ALL": "C.UTF-8",
        "LANG": "C.UTF-8",
        "SOURCE_DATE_EPOCH": "1700000000",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
    })

    # Default time limits (seconds)
    DEFAULT_AGENT_TIMEOUT: int = 1200  # 20 min
    DEFAULT_GRADE_TIMEOUT: int = 300   # 5 min
    DEFAULT_HIDDEN_TIMEOUT: int = 60   # 1 min
    DEFAULT_PUBLIC_TIMEOUT: int = 30   # 30 sec
