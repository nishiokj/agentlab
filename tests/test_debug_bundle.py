"""Strong test suite for debug_bundle module."""
import json
import os
import tempfile
import zipfile

from agentlab_runner.debug_bundle import build_debug_bundle


class TestBuildDebugBundle:
    """Tests for build_debug_bundle function."""

    def test_build_basic_bundle(self):
        """Creates a zip bundle with standard files."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            # Create required files
            os.makedirs(run_dir)
            for filename in [
                "manifest.json",
                "resolved_experiment.json",
                "resolved_experiment.digest",
                "attestation.json",
                "grades.json",
            ]:
                with open(os.path.join(run_dir, filename), "w") as f:
                    f.write(json.dumps({"file": filename}))

            os.makedirs(os.path.join(run_dir, "analysis"))
            for filename in ["summary.json", "comparisons.json"]:
                with open(os.path.join(run_dir, "analysis", filename), "w") as f:
                    f.write(json.dumps({"file": filename}))

            result = build_debug_bundle(run_dir, out_path)

            assert result == out_path
            assert os.path.exists(out_path)

            # Verify zip contents
            with zipfile.ZipFile(out_path, "r") as zf:
                names = zf.namelist()
                assert "manifest.json" in names
                assert "attestation.json" in names
                assert "analysis/summary.json" in names

    def test_build_creates_output_dir(self):
        """Creates output directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_dir = os.path.join(tmp, "output", "nested")
            out_path = os.path.join(out_dir, "debug.zip")

            os.makedirs(run_dir)
            for filename in ["manifest.json", "attestation.json", "grades.json"]:
                with open(os.path.join(run_dir, filename), "w") as f:
                    f.write("{}")

            result = build_debug_bundle(run_dir, out_path)

            assert result == out_path
            assert os.path.exists(out_path)
            assert os.path.exists(out_dir)

    def test_build_missing_files_skipped(self):
        """Missing files are silently skipped."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            # Only create some files
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write('{"exists": true}')

            result = build_debug_bundle(run_dir, out_path)

            assert result == out_path
            with zipfile.ZipFile(out_path, "r") as zf:
                assert "manifest.json" in zf.namelist()
                assert "attestation.json" not in zf.namelist()

    def test_build_extra_paths(self):
        """Additional files can be included via extra_paths."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write("{}")

            extra_file = os.path.join(run_dir, "custom.log")
            with open(extra_file, "w") as f:
                f.write("custom log content\n")

            os.makedirs(os.path.join(run_dir, "trials", "trial_001"))
            trial_file = os.path.join(run_dir, "trials", "trial_001", "error.log")
            with open(trial_file, "w") as f:
                f.write("error details\n")

            result = build_debug_bundle(
                run_dir,
                out_path,
                extra_paths=["custom.log", "trials/trial_001/error.log"],
            )

            assert result == out_path
            with zipfile.ZipFile(out_path, "r") as zf:
                assert "manifest.json" in zf.namelist()
                assert "custom.log" in zf.namelist()
                assert "trials/trial_001/error.log" not in zf.namelist()
                # Files are stored at the root with their rel path from run_dir
                assert "error.log" in zf.namelist()

            # Verify content
            with zipfile.ZipFile(out_path, "r") as zf:
                with zf.open("custom.log") as f:
                    assert f.read() == b"custom log content\n"

    def test_build_preserves_file_content(self):
        """File content is preserved correctly."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            content = {"key": "value", "nested": {"data": 123}}
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                json.dump(content, f)

            build_debug_bundle(run_dir, out_path)

            with zipfile.ZipFile(out_path, "r") as zf:
                with zf.open("manifest.json") as f:
                    extracted = json.load(f)
                    assert extracted == content

    def test_build_empty_run_dir(self):
        """Handles empty run directory."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)

            result = build_debug_bundle(run_dir, out_path)

            assert result == out_path
            with zipfile.ZipFile(out_path, "r") as zf:
                # Should be empty or contain no standard files
                assert len(zf.namelist()) == 0

    def test_build_overwrites_existing(self):
        """Overwrites existing output file."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write("{}")

            # Create existing zip with different content
            with zipfile.ZipFile(out_path, "w") as zf:
                zf.writestr("old.txt", "old content")

            build_debug_bundle(run_dir, out_path)

            with zipfile.ZipFile(out_path, "r") as zf:
                assert "old.txt" not in zf.namelist()
                assert "manifest.json" in zf.namelist()

    def test_build_compression(self):
        """Uses DEFLATE compression."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            large_content = "x" * 10000
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write('{"large": "' + large_content + '"}')

            build_debug_bundle(run_dir, out_path)

            # Verify compression is used
            with zipfile.ZipFile(out_path, "r") as zf:
                info = zf.getinfo("manifest.json")
                assert info.compress_type == zipfile.ZIP_DEFLATED

    def test_build_analysis_directory(self):
        """Files from analysis directory are included."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write("{}")

            analysis_dir = os.path.join(run_dir, "analysis")
            os.makedirs(analysis_dir)

            summary = {"summary": "results"}
            with open(os.path.join(analysis_dir, "summary.json"), "w") as f:
                json.dump(summary, f)

            comparisons = [{"test": 1}, {"test": 2}]
            with open(os.path.join(analysis_dir, "comparisons.json"), "w") as f:
                json.dump(comparisons, f)

            build_debug_bundle(run_dir, out_path)

            with zipfile.ZipFile(out_path, "r") as zf:
                with zf.open("analysis/summary.json") as f:
                    assert json.load(f) == summary
                with zf.open("analysis/comparisons.json") as f:
                    assert json.load(f) == comparisons

    def test_build_binary_file(self):
        """Handles binary files correctly."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write("{}")

            # Create a binary file
            binary_file = os.path.join(run_dir, "binary.bin")
            with open(binary_file, "wb") as f:
                f.write(b"\x00\x01\x02\xff\xfe\xfd")

            build_debug_bundle(run_dir, out_path, extra_paths=["binary.bin"])

            with zipfile.ZipFile(out_path, "r") as zf:
                with zf.open("binary.bin") as f:
                    assert f.read() == b"\x00\x01\x02\xff\xfe\xfd"

    def test_build_multiple_extra_paths(self):
        """Multiple extra paths are included."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write("{}")

            files = []
            for i in range(3):
                filename = f"file{i}.log"
                filepath = os.path.join(run_dir, filename)
                with open(filepath, "w") as f:
                    f.write(f"content {i}\n")
                files.append(filename)

            build_debug_bundle(run_dir, out_path, extra_paths=files)

            with zipfile.ZipFile(out_path, "r") as zf:
                for i in range(3):
                    assert f"file{i}.log" in zf.namelist()

    def test_build_returns_path(self):
        """Returns the output path."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write("{}")

            result = build_debug_bundle(run_dir, out_path)

            assert result == out_path
            assert isinstance(result, str)

    def test_build_grades_json(self):
        """grades.json is included in bundle."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write("{}")

            grades = {"overall": "pass", "replay": "strict"}
            with open(os.path.join(run_dir, "grades.json"), "w") as f:
                json.dump(grades, f)

            build_debug_bundle(run_dir, out_path)

            with zipfile.ZipFile(out_path, "r") as zf:
                with zf.open("grades.json") as f:
                    extracted = json.load(f)
                    assert extracted == grades

    def test_build_attestation_json(self):
        """attestation.json is included in bundle."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, "run")
            out_path = os.path.join(tmp, "debug.zip")

            os.makedirs(run_dir)
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                f.write("{}")

            attestation = {"schema_version": "attestation_v1", "digest": "abc123"}
            with open(os.path.join(run_dir, "attestation.json"), "w") as f:
                json.dump(attestation, f)

            build_debug_bundle(run_dir, out_path)

            with zipfile.ZipFile(out_path, "r") as zf:
                with zf.open("attestation.json") as f:
                    extracted = json.load(f)
                    assert extracted == attestation
