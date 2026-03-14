"""Example: 用 async stage 做批次資料轉換。

情境：
    使用者有一批數字，想透過 async 函式做轉換，
    然後收集所有結果。

用法展示：
    - pipe() 一行式 API 搭配 async stage
    - Stage 自動偵測 async function
    - 結果是完整且正確的
"""

from __future__ import annotations

import asyncio
import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


def test_async_stage_transforms_numbers():
    """
    Given: 一組數字 [1, 2, 3, 4, 5]
    When:  經過一個 async stage，每個數字乘以 2
    Then:  得到 [2, 4, 6, 8, 10]
    """
    from qqabc.pipe import Stage, pipe

    async def double(x: int) -> int:
        await asyncio.sleep(0.001)
        return x * 2

    numbers = [1, 2, 3, 4, 5]
    results = sorted(pipe([Stage(fn=double)], input=numbers))

    assert results == [2, 4, 6, 8, 10]


def test_async_stage_with_string_data():
    """
    Given: 一組英文單字 ["hello", "world", "async"]
    When:  經過 async stage，每個單字轉大寫
    Then:  得到 ["ASYNC", "HELLO", "WORLD"]
    """
    from qqabc.pipe import Stage, pipe

    async def to_upper(s: str) -> str:
        return s.upper()

    words = ["hello", "world", "async"]
    results = sorted(pipe([Stage(fn=to_upper)], input=words))

    assert results == ["ASYNC", "HELLO", "WORLD"]


def test_async_stage_with_empty_input():
    """
    Given: 空的輸入 []
    When:  經過 async stage
    Then:  結果也是空的 []
    """
    from qqabc.pipe import Stage, pipe

    async def noop(x: int) -> int:
        return x

    results = list(pipe([Stage(fn=noop)], input=[]))

    assert results == []
