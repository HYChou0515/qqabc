"""Tests for qqabc.pipe infrastructure (issue #30) — Python < 3.10 側。

此檔案只在 Python < 3.10 上跑（透過 uv run --python 3.9），
驗證 import qqabc.pipe 會正確 raise ImportError。
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import textwrap

import pytest


class TestPipeImportErrorOldPython:
    """驗證 qqabc.pipe 在 Python < 3.10 上的 ImportError 行為。"""

    @pytest.fixture(autouse=True)
    def _require_old_python(self) -> None:
        if sys.version_info >= (3, 10):
            pytest.skip("此測試僅在 Python < 3.10 上執行")

    def test_import_raises_import_error(self) -> None:
        """Import qqabc.pipe 應 raise ImportError。"""
        with pytest.raises(ImportError):
            import qqabc.pipe  # noqa: F401

    def test_error_message_mentions_310(self) -> None:
        """ImportError 訊息應提到 3.10。"""
        # 確保 module 未被快取
        mods_to_remove = [k for k in sys.modules if k.startswith("qqabc.pipe")]
        for mod in mods_to_remove:
            del sys.modules[mod]

        with pytest.raises(ImportError, match=r"3\.10"):
            importlib.import_module("qqabc.pipe")

    def test_error_message_contains_current_version(self) -> None:
        """ImportError 訊息應包含當前 Python 版本。"""
        mods_to_remove = [k for k in sys.modules if k.startswith("qqabc.pipe")]
        for mod in mods_to_remove:
            del sys.modules[mod]

        current = f"{sys.version_info[0]}.{sys.version_info[1]}"
        with pytest.raises(ImportError, match=r"requires Python 3\.10") as exc_info:
            importlib.import_module("qqabc.pipe")
        assert current in str(exc_info.value)

    def test_error_message_format(self) -> None:
        """ImportError 訊息格式完整。"""
        mods_to_remove = [k for k in sys.modules if k.startswith("qqabc.pipe")]
        for mod in mods_to_remove:
            del sys.modules[mod]

        with pytest.raises(ImportError) as exc_info:
            importlib.import_module("qqabc.pipe")

        msg = str(exc_info.value)
        assert "requires Python 3.10 or later" in msg
        assert "Current version:" in msg

    def test_qqabc_base_still_works(self) -> None:
        """即使 pipe 不能用，qqabc 本身仍可正常 import。"""
        import qqabc

        assert hasattr(qqabc, "__version__")

    def test_subprocess_confirms_import_error(self) -> None:
        """在子程序中也能確認 ImportError。"""
        code = textwrap.dedent("""\
            import sys
            try:
                import qqabc.pipe
                sys.exit(1)
            except ImportError as e:
                assert "3.10" in str(e)
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
