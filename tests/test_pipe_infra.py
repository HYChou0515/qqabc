"""Tests for qqabc.pipe infrastructure (issue #30) — 3.10+ 側。

此檔案只在 Python 3.10+ 上跑，驗證 pipe 能正常 import。
Python < 3.10 的 ImportError 分支由 test_pipe_infra_oldpy.py 覆蓋。
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import textwrap

import pytest


class TestPipeImport310Plus:
    """驗證 qqabc.pipe 在 Python 3.10+ 上的行為。"""

    @pytest.fixture(autouse=True)
    def _require_310(self) -> None:
        if sys.version_info < (3, 10):
            pytest.skip("需要 Python 3.10+")

    def test_pipe_importable(self) -> None:
        """Import qqabc.pipe 應正常運作。"""
        import qqabc.pipe  # noqa: F401

    def test_pipe_reimport_after_clear(self) -> None:
        """清除後重新 import pipe 應該正常。"""
        mods_to_remove = [k for k in sys.modules if k.startswith("qqabc.pipe")]
        for mod in mods_to_remove:
            del sys.modules[mod]

        import qqabc.pipe  # noqa: F401

        assert "qqabc.pipe" in sys.modules

    def test_pipe_import_in_subprocess(self) -> None:
        """在乾淨的子程序中 import qqabc.pipe 應成功。"""
        result = subprocess.run(
            [sys.executable, "-c", "import qqabc.pipe; print('ok')"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0
        assert "ok" in result.stdout

    def test_pipe_extra_exists_in_metadata(self) -> None:
        """qqabc[pipe] 應該是合法的 optional dependency group。"""
        from importlib.metadata import metadata

        meta = metadata("qqabc")
        extras = [v for k, v in meta.items() if k == "Provides-Extra"]
        assert "pipe" in extras, f"pipe not in extras: {extras}"


class TestPipeInitContent:
    """驗證 __init__.py 原始碼含有版本守衛。"""

    def test_pipe_init_has_version_guard(self) -> None:
        """__init__.py 含有 sys.version_info 檢查。"""
        import pathlib

        init_path = (
            pathlib.Path(__file__).resolve().parent.parent
            / "src"
            / "qqabc"
            / "pipe"
            / "__init__.py"
        )
        content = init_path.read_text()
        assert "sys.version_info" in content
        assert "(3, 10)" in content


class TestQqabcNotPullingPipe:
    """驗證 import qqabc 不會連帶載入 pipe。"""

    def test_qqabc_import_does_not_pull_pipe(self) -> None:
        """Import qqabc 不應自動載入 pipe 子模組。"""
        mods_to_remove = [k for k in sys.modules if k.startswith("qqabc.pipe")]
        for mod in mods_to_remove:
            del sys.modules[mod]

        if "qqabc" in sys.modules:
            importlib.reload(sys.modules["qqabc"])
        else:
            import qqabc  # noqa: F401

        assert "qqabc.pipe" not in sys.modules

    def test_qqabc_import_in_subprocess_no_pipe(self) -> None:
        """在子程序中 import qqabc 不應拉入 pipe。"""
        code = textwrap.dedent("""\
            import qqabc
            import sys
            assert "qqabc.pipe" not in sys.modules, "pipe was auto-imported!"
            print("ok")
        """)
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert "ok" in result.stdout
