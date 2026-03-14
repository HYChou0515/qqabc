"""Example: Async Stage 的錯誤處理。

情境：
    使用者的 async 處理函式可能對某些 input 拋出例外，
    pipeline 應能繼續處理其餘 item 而不 deadlock。

用法展示：
    - 部分 item 失敗時，其餘 item 仍正常處理
    - 全部 item 失敗時，回傳空結果
    - pipeline 不會因 exception 而卡死
"""

from __future__ import annotations

import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


def test_partial_failure_continues():
    """
    Given: 數字 0~9
    When:  async stage 對 x==5 拋出 ValueError
    Then:  5 被丟棄，其餘 [0,1,2,3,4,6,7,8,9] 正常回傳
    """
    from qqabc.pipe import Stage, pipe

    async def maybe_fail(x: int) -> int:
        if x == 5:
            msg = "skip this one"
            raise ValueError(msg)
        return x

    results = sorted(pipe([Stage(fn=maybe_fail)], input=range(10)))

    assert results == [0, 1, 2, 3, 4, 6, 7, 8, 9]


def test_all_failures_return_empty():
    """
    Given: 5 筆資料
    When:  async stage 對每筆都拋出例外
    Then:  結果為空 []，pipeline 正常結束不 deadlock
    """
    from qqabc.pipe import Stage, pipe

    async def always_fail(x: int) -> int:
        msg = "always fail"
        raise ValueError(msg)

    results = list(pipe([Stage(fn=always_fail)], input=range(5)))

    assert results == []


def test_odd_numbers_filtered_by_error():
    """
    Given: 數字 0~9
    When:  async stage 對奇數拋出例外
    Then:  只剩偶數 [0, 2, 4, 6, 8]
    """
    from qqabc.pipe import Stage, pipe

    async def even_only(x: int) -> int:
        if x % 2 == 1:
            msg = f"odd: {x}"
            raise ValueError(msg)
        return x

    results = sorted(pipe([Stage(fn=even_only)], input=range(10)))

    assert results == [0, 2, 4, 6, 8]
