from __future__ import annotations

import os
import shutil
import tempfile

import pytest


@pytest.fixture
def fx_workdir():
    """建立臨時資料夾, 並切換到該資料夾"""
    d = tempfile.mkdtemp()
    os.chdir(d)
    try:
        yield d
    finally:
        os.chdir(os.path.dirname(__file__))
        shutil.rmtree(d)
