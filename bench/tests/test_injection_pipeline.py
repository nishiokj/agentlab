"""Tests for injection pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.taskkit.inject import (
    validate_injection_patch,
    get_injection_manifest,
    compute_workspace_tree_hash,
)


class TestInjectionValidation:
    def test_valid_patch(self, tmp_path):
        patch = tmp_path / "inject.patch"
        patch.write_text("""diff --git a/src/main.py b/src/main.py
--- a/src/main.py
+++ b/src/main.py
@@ -1 +1 @@
-def foo(): pass
+def foo(): return None
""")
        errors = validate_injection_patch(patch)
        assert errors == []

    def test_missing_patch(self, tmp_path):
        errors = validate_injection_patch(tmp_path / "nonexistent.patch")
        assert len(errors) > 0

    def test_empty_patch(self, tmp_path):
        patch = tmp_path / "empty.patch"
        patch.write_text("")
        errors = validate_injection_patch(patch)
        assert len(errors) > 0

    def test_denylisted_file(self, tmp_path):
        patch = tmp_path / "bad.patch"
        patch.write_text("""diff --git a/conftest.py b/conftest.py
--- a/conftest.py
+++ b/conftest.py
@@ -1 +1 @@
-# old
+# new
""")
        errors = validate_injection_patch(patch, deny_patterns=["conftest.py"])
        assert len(errors) > 0


class TestInjectionManifest:
    def test_manifest_includes_files(self, tmp_path):
        patch = tmp_path / "inject.patch"
        patch.write_text("""diff --git a/src/a.py b/src/a.py
--- a/src/a.py
+++ b/src/a.py
@@ -1 +1 @@
-old
+new
""")
        manifest = get_injection_manifest(patch)
        assert "src/a.py" in manifest["files_changed"]
        assert "patch_hash" in manifest


class TestWorkspaceHash:
    def test_identical_workspaces_same_hash(self, tmp_path):
        ws1 = tmp_path / "ws1"
        ws2 = tmp_path / "ws2"
        for ws in [ws1, ws2]:
            ws.mkdir()
            (ws / "a.py").write_text("hello")
            (ws / "b.py").write_text("world")

        h1 = compute_workspace_tree_hash(ws1)
        h2 = compute_workspace_tree_hash(ws2)
        assert h1 == h2

    def test_different_content_different_hash(self, tmp_path):
        ws1 = tmp_path / "ws1"
        ws2 = tmp_path / "ws2"
        ws1.mkdir()
        ws2.mkdir()
        (ws1 / "a.py").write_text("hello")
        (ws2 / "a.py").write_text("world")

        h1 = compute_workspace_tree_hash(ws1)
        h2 = compute_workspace_tree_hash(ws2)
        assert h1 != h2
