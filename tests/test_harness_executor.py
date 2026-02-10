"""Strong test suite for harness_executor module."""
import json
import os
import subprocess
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from agentlab_runner.schemas import SchemaRegistry
from agentlab_runner.harness_executor import HarnessExecutor


class TestHarnessExecutorInit:
    """Tests for HarnessExecutor initialization."""

    def test_init_with_registry(self):
        """Initialize with schema registry."""
        registry = MagicMock(spec=SchemaRegistry)
        executor = HarnessExecutor(registry)
        assert executor.registry == registry


class TestHarnessExecutorRun:
    """Tests for HarnessExecutor.run method."""

    def test_run_creates_directories(self):
        """Creates input and output directories if they don't exist."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = os.path.join(tmp, "tmp")
            input_path = os.path.join(tmp_dir, "input", "trial_input.json")
            output_path = os.path.join(tmp_dir, "output", "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            executor = HarnessExecutor(registry)

            # Create a simple Python script that just writes output
            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write('import sys, json; json.dump({"status": "ok"}, open(sys.argv[1], "w"))')

            trial_input = {"task": "test"}

            try:
                executor.run(
                    command=[sys.executable, script_path, output_path],
                    trial_input=trial_input,
                    input_path=input_path,
                    output_path=output_path,
                )
            except RuntimeError:
                pass  # May fail if validation doesn't pass

            assert os.path.exists(os.path.dirname(input_path))
            assert os.path.exists(os.path.dirname(output_path))

    def test_run_writes_input_file(self):
        """Writes trial_input.json with correct content."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            executor = HarnessExecutor(registry)

            trial_input = {"task_id": "123", "data": "test"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(
                    "import sys, json; "
                    "input_data = json.load(open(sys.argv[1])); "
                    "json.dump(input_data, open(sys.argv[2], 'w'))"
                )

            executor.run(
                command=[sys.executable, script_path, input_path, output_path],
                trial_input=trial_input,
                input_path=input_path,
                output_path=output_path,
            )

            with open(input_path) as f:
                written_input = json.load(f)
                assert written_input == trial_input

    def test_run_validates_input(self):
        """Validates trial input against schema."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(f'import json; json.dump({{"status": "ok"}}, open("{output_path}", "w"))')

            executor.run(
                command=[sys.executable, script_path],
                trial_input=trial_input,
                input_path=input_path,
                output_path=output_path,
            )

            registry.validate.assert_called_once()

    def test_run_validates_output(self):
        """Validates trial output against schema."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(f'import json; json.dump({{"status": "ok"}}, open("{output_path}", "w"))')

            executor.run(
                command=[sys.executable, script_path],
                trial_input=trial_input,
                input_path=input_path,
                output_path=output_path,
            )

            # Called twice: once for input, once for output
            assert registry.validate.call_count == 2

    def test_run_returns_output(self):
        """Returns the parsed trial output."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            expected_output = {"status": "success", "result": 42}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(f'import json; json.dump({json.dumps(expected_output)}, open("{output_path}", "w"))')

            result = executor = HarnessExecutor(registry)
            result = executor.run(
                command=[sys.executable, script_path],
                trial_input=trial_input,
                input_path=input_path,
                output_path=output_path,
            )

            assert result == expected_output

    def test_run_nonzero_returncode_raises_error(self):
        """Raises RuntimeError when harness exits with non-zero code."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write("import sys; sys.exit(1)")

            executor = HarnessExecutor(registry)

            with pytest.raises(RuntimeError, match="exited non-zero"):
                executor.run(
                    command=[sys.executable, script_path],
                    trial_input=trial_input,
                    input_path=input_path,
                    output_path=output_path,
                )

    def test_run_missing_output_raises_error(self):
        """Raises FileNotFoundError when harness doesn't write output."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write("pass")  # Does nothing

            executor = HarnessExecutor(registry)

            with pytest.raises(FileNotFoundError, match="did not write"):
                executor.run(
                    command=[sys.executable, script_path],
                    trial_input=trial_input,
                    input_path=input_path,
                    output_path=output_path,
                )

    def test_run_with_custom_cwd(self):
        """Runs harness from custom working directory."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")
            cwd = os.path.join(tmp, "custom_cwd")
            os.makedirs(cwd)

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            script_path = os.path.join(cwd, "harness.py")
            with open(script_path, "w") as f:
                f.write(
                    f'import json, os; '
                    f'json.dump({{"cwd": os.getcwd()}}, open("{output_path}", "w"))'
                )

            executor = HarnessExecutor(registry)
            result = executor.run(
                command=[sys.executable, "harness.py"],
                trial_input=trial_input,
                input_path=input_path,
                output_path=output_path,
                cwd=cwd,
            )

            assert os.path.isabs(result["cwd"])
            assert result["cwd"].replace("\\", "/") == cwd.replace("\\", "/")

    def test_run_with_custom_env(self):
        """Runs harness with custom environment variables."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            custom_env = {"CUSTOM_VAR": "custom_value"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(
                    f'import json, os; '
                    f'json.dump({{"var": os.environ.get("CUSTOM_VAR")}}, open("{output_path}", "w"))'
                )

            executor = HarnessExecutor(registry)
            result = executor.run(
                command=[sys.executable, script_path],
                trial_input=trial_input,
                input_path=input_path,
                output_path=output_path,
                env=custom_env,
            )

            assert result["var"] == "custom_value"

    def test_run_with_timeout(self):
        """Times out execution after specified seconds."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write("import time; time.sleep(10)")

            executor = HarnessExecutor(registry)

            with pytest.raises(subprocess.TimeoutExpired):
                executor.run(
                    command=[sys.executable, script_path],
                    trial_input=trial_input,
                    input_path=input_path,
                    output_path=output_path,
                    timeout=1,
                )

    def test_run_complex_command(self):
        """Handles complex commands with multiple arguments."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(
                    f'import json, sys; '
                    f'json.dump({{"args": sys.argv}}, open("{output_path}", "w"))'
                )

            command = [
                sys.executable,
                script_path,
                "--flag",
                "value",
                "--another-flag",
                "another-value",
            ]

            executor = HarnessExecutor(registry)
            result = executor.run(
                command=command,
                trial_input=trial_input,
                input_path=input_path,
                output_path=output_path,
            )

            assert "--flag" in result["args"]
            assert "value" in result["args"]
            assert "--another-flag" in result["args"]
            assert "another-value" in result["args"]

    def test_run_large_input_output(self):
        """Handles large input and output data."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)

            # Create large input
            large_input = {"task": "test", "data": list(range(10000))}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(
                    "import json, sys; "
                    "input_data = json.load(open(sys.argv[1])); "
                    "json.dump(input_data, open(sys.argv[2], 'w'))"
                )

            executor = HarnessExecutor(registry)
            result = executor.run(
                command=[sys.executable, script_path, input_path, output_path],
                trial_input=large_input,
                input_path=input_path,
                output_path=output_path,
            )

            assert result == large_input

    def test_run_preserves_output_structure(self):
        """Output JSON structure is preserved."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            complex_output = {
                "nested": {"data": {"key": "value"}},
                "array": [1, 2, {"item": 3}],
                "null": None,
                "bool": True,
            }

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(f'import json; json.dump({json.dumps(complex_output)}, open("{output_path}", "w"))')

            executor = HarnessExecutor(registry)
            result = executor.run(
                command=[sys.executable, script_path],
                trial_input=trial_input,
                input_path=input_path,
                output_path=output_path,
            )

            assert result == complex_output

    def test_run_invalid_json_output(self):
        """Handles invalid JSON in output."""
        with tempfile.TemporaryDirectory() as tmp:
            input_path = os.path.join(tmp, "trial_input.json")
            output_path = os.path.join(tmp, "trial_output.json")

            registry = MagicMock(spec=SchemaRegistry)
            trial_input = {"task": "test"}

            script_path = os.path.join(tmp, "harness.py")
            with open(script_path, "w") as f:
                f.write(f'open("{output_path}", "w").write("not valid json")')

            executor = HarnessExecutor(registry)

            with pytest.raises(json.JSONDecodeError):
                executor.run(
                    command=[sys.executable, script_path],
                    trial_input=trial_input,
                    input_path=input_path,
                    output_path=output_path,
                )
