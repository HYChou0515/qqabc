from __future__ import annotations

import os
import tempfile

import pytest



@pytest.fixture
def fx_workdir():
    """建立臨時資料夾, 並切換到該資料夾"""
    with tempfile.TemporaryDirectory() as d:
        os.chdir(d)
        yield d
