"""Tests for the tool server and filesystem tools."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from bench.tools import fs
from bench.tools.exec import run_command
from bench.tools.patch import parse_patch_files, check_patch_escapes_workspace, apply_patch
from bench.tools.protocol import truncate_output


@pytest.fixture
def workspace(tmp_path):
    """Create a temporary workspace with test files."""
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text(
        "def hello():\n    return 'hello'\n\ndef add(a, b):\n    return a + b\n"
    )
    (tmp_path / "src" / "utils.py").write_text(
        "def helper():\n    pass\n"
    )
    (tmp_path / "README.md").write_text("# Test Project\n")
    return tmp_path


class TestSearch:
    def test_search_finds_pattern(self, workspace):
        result = fs.search(workspace, "def hello")
        assert len(result.matches) >= 1
        assert result.matches[0].path == "src/main.py"
        assert result.matches[0].line_number == 1

    def test_search_deterministic_ordering(self, workspace):
        r1 = fs.search(workspace, "def ")
        r2 = fs.search(workspace, "def ")
        assert [m.path for m in r1.matches] == [m.path for m in r2.matches]
        assert [m.line_number for m in r1.matches] == [m.line_number for m in r2.matches]

    def test_search_max_results(self, workspace):
        result = fs.search(workspace, "def ", max_results=1)
        assert len(result.matches) <= 1


class TestListDir:
    def test_list_dir_root(self, workspace):
        result = fs.list_dir(workspace)
        names = {e.name for e in result.entries}
        assert "src" in names
        assert "README.md" in names

    def test_list_dir_recursive(self, workspace):
        result = fs.list_dir(workspace, recursive=True)
        names = {e.name for e in result.entries}
        assert any("main.py" in n for n in names)


class TestReadFile:
    def test_read_file(self, workspace):
        result = fs.read_file(workspace, "src/main.py")
        assert "def hello" in result.content
        assert not result.truncated

    def test_read_file_with_max_bytes(self, workspace):
        result = fs.read_file(workspace, "src/main.py", max_bytes=10)
        assert result.truncated
        assert len(result.content) <= 50  # 10 bytes + truncation marker

    def test_read_file_not_found(self, workspace):
        with pytest.raises(FileNotFoundError):
            fs.read_file(workspace, "nonexistent.py")

    def test_read_file_workspace_escape(self, workspace):
        with pytest.raises(PermissionError):
            fs.read_file(workspace, "../../etc/passwd")


class TestRunCommand:
    def test_run_simple(self, workspace):
        result = run_command(workspace, ["python", "-c", "print('hello')"])
        assert result.exit_code == 0
        assert "hello" in result.stdout

    def test_run_timeout(self, workspace):
        result = run_command(workspace, ["python", "-c", "import time; time.sleep(10)"], timeout=1)
        assert result.timed_out
        assert result.exit_code == -1

    def test_run_allowlist(self, workspace):
        result = run_command(workspace, ["cat", "/etc/passwd"], allowed_commands=["python"])
        assert result.exit_code == -1
        assert "not allowed" in result.stderr

    def test_run_cwd_escape(self, workspace):
        result = run_command(workspace, ["ls"], cwd="../../..")
        assert result.exit_code == -1
        assert "escape" in result.stderr.lower()


class TestPatch:
    def test_parse_patch_files(self):
        patch = """diff --git a/src/main.py b/src/main.py
--- a/src/main.py
+++ b/src/main.py
@@ -1,2 +1,2 @@
-def hello():
+def hello_world():
"""
        files = parse_patch_files(patch)
        assert "src/main.py" in files

    def test_workspace_escape_detection(self):
        patch = """--- a/../../../etc/passwd
+++ b/../../../etc/passwd
"""
        violations = check_patch_escapes_workspace(patch)
        assert len(violations) > 0

    def test_empty_patch_rejected(self, workspace):
        result = apply_patch(workspace, "")
        assert not result.success


class TestTruncation:
    def test_short_text_not_truncated(self):
        text, truncated = truncate_output("hello")
        assert text == "hello"
        assert not truncated

    def test_long_text_truncated(self):
        text, truncated = truncate_output("x" * 10000, max_len=100)
        assert len(text) <= 100
        assert truncated
        assert "TRUNCATED" in text
