"""Strong test suite for experiment_resolver module."""
import os
import tempfile
import textwrap
from pathlib import Path

import pytest
import yaml

from agentlab_runner.experiment_resolver import ExperimentResolver


class TestExperimentResolverLoad:
    """Tests for ExperimentResolver.load method."""

    def test_load_basic_experiment(self):
        """Load a basic valid experiment YAML."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                name: test_experiment
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            exp = resolver.load()

            assert exp["version"] == "0.3"
            assert exp["name"] == "test_experiment"
            assert exp["dataset"]["path"] == "data.jsonl"

    def test_load_invalid_format(self):
        """Raises ValueError for non-dict YAML."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            with open(exp_path, "w") as f:
                f.write("- item1\n- item2")

            resolver = ExperimentResolver(exp_path)
            with pytest.raises(ValueError, match="must be a mapping"):
                resolver.load()

    def test_load_nonexistent_file(self):
        """Raises FileNotFoundError for missing file."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "nonexistent.yaml")
            resolver = ExperimentResolver(exp_path)
            with pytest.raises(FileNotFoundError):
                resolver.load()


class TestExperimentResolverResolve:
    """Tests for ExperimentResolver.resolve method."""

    def test_resolve_basic(self):
        """Resolve basic experiment with defaults."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            dataset_path = os.path.join(tmp, "data.jsonl")
            with open(dataset_path, "w") as f:
                f.write("{}")

            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert resolved["version"] == "0.3"
            assert resolved["dataset"]["path"] == dataset_path
            assert "content_hash" in resolved["dataset"]
            assert "registered_at" in resolved
            assert resolved["runtime"]["harness"]["integration_level"] == "cli_basic"
            assert resolved["runtime"]["harness"]["control_plane"]["mode"] == "file"

    def test_resolve_version_check(self):
        """Only version 0.3 is supported."""
        with tempfile.TemporaryDirectory() as tmp:
            for version in ["0.1", "0.2", "0.4", "1.0"]:
                exp_path = os.path.join(tmp, f"exp_v{version.replace('.', '_')}.yaml")
                content = f'version: "{version}"\ndataset:\n  path: data.jsonl'
                with open(exp_path, "w") as f:
                    f.write(content)

                resolver = ExperimentResolver(exp_path)
                with pytest.raises(ValueError, match="Only version 0.3"):
                    resolver.resolve()

    def test_resolve_dataset_path_required(self):
        """dataset.path is required."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset: {}
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            with pytest.raises(ValueError, match="dataset.path is required"):
                resolver.resolve()

    def test_resolve_relative_dataset_path(self):
        """Relative dataset paths are resolved relative to experiment dir."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            data_path = os.path.join(tmp, "subdir", "data.jsonl")
            os.makedirs(os.path.dirname(data_path))
            with open(data_path, "w") as f:
                f.write("{}")

            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: subdir/data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert resolved["dataset"]["path"] == data_path
            assert os.path.isabs(resolved["dataset"]["path"])

    def test_resolve_absolute_dataset_path(self):
        """Absolute dataset paths are preserved."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            abs_data_path = os.path.join(tmp, "data.jsonl")
            with open(abs_data_path, "w") as f:
                f.write("{}")

            content = textwrap.dedent(f"""
                version: "0.3"
                dataset:
                  path: {abs_data_path}
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert resolved["dataset"]["path"] == abs_data_path

    def test_resolve_content_hash_computed(self):
        """Content hash is computed for dataset."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            data_path = os.path.join(tmp, "data.jsonl")
            with open(data_path, "w") as f:
                f.write('{"test": "data"}')

            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert "content_hash" in resolved["dataset"]
            assert resolved["dataset"]["content_hash"].startswith("sha256:")

    def test_resolve_existing_content_hash_preserved(self):
        """Existing content hash is preserved."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            data_path = os.path.join(tmp, "data.jsonl")
            with open(data_path, "w") as f:
                f.write("{}")

            existing_hash = "sha256:customhash123456"
            content = textwrap.dedent(f"""
                version: "0.3"
                dataset:
                  path: data.jsonl
                  content_hash: {existing_hash}
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert resolved["dataset"]["content_hash"] == existing_hash

    def test_resolve_harness_mode_check(self):
        """Only cli mode is supported."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: sdk
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            with pytest.raises(ValueError, match="mode must be 'cli'"):
                resolver.resolve()

    def test_resolve_harness_command_required(self):
        """harness.command is required."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            with pytest.raises(ValueError, match="command is required"):
                resolver.resolve()

    def test_resolve_relative_file_args(self):
        """Relative file paths in command are resolved if they exist."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write("#!/usr/bin/env python\npass")

            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["./harness.py", "--config", "config.yaml"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            cmd = resolved["runtime"]["harness"]["command"]
            assert cmd[0] == script_path  # Resolved to absolute
            assert cmd[2] == "config.yaml"  # Not resolved (doesn't exist)

    def test_resolve_nonpath_args_preserved(self):
        """Command arguments that aren't paths are preserved."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "-m", "module", "--flag", "value"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert resolved["runtime"]["harness"]["command"] == [
                "python", "-m", "module", "--flag", "value"
            ]

    def test_resolve_default_integration_level(self):
        """Defaults to cli_basic when not specified."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert resolved["runtime"]["harness"]["integration_level"] == "cli_basic"

    def test_resolve_custom_integration_level(self):
        """Custom integration level is preserved."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
                    integration_level: cli_events
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert resolved["runtime"]["harness"]["integration_level"] == "cli_events"

    def test_resolve_default_control_plane(self):
        """Defaults to file mode control plane when not specified."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            control_plane = resolved["runtime"]["harness"]["control_plane"]
            assert control_plane["mode"] == "file"
            assert control_plane["path"] == "/state/lab_control.json"

    def test_resolve_custom_control_plane(self):
        """Custom control plane is preserved."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
                    control_plane:
                      mode: grpc
                      address: localhost:50051
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            control_plane = resolved["runtime"]["harness"]["control_plane"]
            assert control_plane["mode"] == "grpc"
            assert control_plane["address"] == "localhost:50051"


class TestExperimentResolverDigest:
    """Tests for ExperimentResolver.digest method."""

    def test_digest_format(self):
        """Digest returns sha256 prefixed hash."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()
            digest = resolver.digest(resolved)

            assert digest.startswith("sha256:")
            assert len(digest.split(":")[1]) == 64  # 64 hex chars

    def test_digest_deterministic(self):
        """Digest is deterministic for same input."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved1 = resolver.resolve()
            digest1 = resolver.digest(resolved1)
            digest2 = resolver.digest(resolved1)

            assert digest1 == digest2

    def test_digest_content_sensitive(self):
        """Digest changes with content changes."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved1 = resolver.resolve()
            digest1 = resolver.digest(resolved1)

            resolved1["name"] = "modified"
            digest2 = resolver.digest(resolved1)

            assert digest1 != digest2

    def test_digest_order_independence(self):
        """Digest is independent of key order in input."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "experiment.yaml")
            with open(exp_path, "w") as f:
                f.write("version: 0.3\nruntime:\n  harness:\n    mode: cli\n    command: [python]\ndataset:\n  path: d.jsonl")

            resolver = ExperimentResolver(exp_path)
            digest1 = resolver.digest(resolver.resolve())

            # Different YAML but same structure
            exp_path2 = os.path.join(tmp, "experiment2.yaml")
            with open(exp_path2, "w") as f:
                f.write("dataset:\n  path: d.jsonl\nruntime:\n  harness:\n    command: [python]\n    mode: cli\nversion: 0.3")

            resolver2 = ExperimentResolver(exp_path2)
            digest2 = resolver2.digest(resolver2.resolve())

            # Digests should be the same after canonicalization
            assert digest1 == digest2


class TestExperimentResolverBaseDir:
    """Tests for base_dir handling."""

    def test_base_dir_absolute(self):
        """Base dir is absolute path."""
        with tempfile.TemporaryDirectory() as tmp:
            exp_path = os.path.join(tmp, "subdir", "experiment.yaml")
            os.makedirs(os.path.dirname(exp_path))
            with open(exp_path, "w") as f:
                f.write('version: "0.3"\ndataset:\n  path: data.jsonl')

            resolver = ExperimentResolver(exp_path)
            assert os.path.isabs(resolver.base_dir)
            assert resolver.base_dir == os.path.dirname(os.path.abspath(exp_path))

    def test_base_dir_used_for_relative_paths(self):
        """Base dir is used for relative path resolution."""
        with tempfile.TemporaryDirectory() as tmp:
            subdir = os.path.join(tmp, "subdir")
            os.makedirs(subdir)
            data_path = os.path.join(subdir, "data.jsonl")
            with open(data_path, "w") as f:
                f.write("{}")

            exp_path = os.path.join(subdir, "experiment.yaml")
            content = textwrap.dedent("""
                version: "0.3"
                dataset:
                  path: data.jsonl
                runtime:
                  harness:
                    mode: cli
                    command: ["python", "harness.py"]
            """)
            with open(exp_path, "w") as f:
                f.write(content)

            resolver = ExperimentResolver(exp_path)
            resolved = resolver.resolve()

            assert resolved["dataset"]["path"] == data_path
