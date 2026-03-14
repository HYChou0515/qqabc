"""Example: Thread 與 Async Stage 混合使用。

情境：
    使用者有一個多步驟的資料處理流程：
    1. 先用同步函式做前處理（thread stage）
    2. 再用 async 函式做 IO 密集操作（async stage）
    3. 最後用同步函式做後處理（thread stage）

用法展示：
    - 混合 thread / async executor
    - | 運算子串接 stage
    - Pipeline context manager 用法
"""

from __future__ import annotations

import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


def test_thread_then_async_pipeline():
    """
    Given: 數字 [1, 2, 3]
    When:  thread stage +1 → async stage *2
    Then:  得到 [4, 6, 8]
    """
    from qqabc.pipe import Stage, pipe

    async def async_double(x: int) -> int:
        return x * 2

    results = sorted(
        pipe(
            [
                Stage(fn=lambda x: x + 1, executor="thread"),
                Stage(fn=async_double),
            ],
            input=[1, 2, 3],
        )
    )

    assert results == [4, 6, 8]


def test_async_then_thread_pipeline():
    """
    Given: 數字 [0, 1, 2]
    When:  async stage +1 → thread stage *10
    Then:  得到 [10, 20, 30]
    """
    from qqabc.pipe import Stage, pipe

    async def async_inc(x: int) -> int:
        return x + 1

    results = sorted(
        pipe(
            [
                Stage(fn=async_inc),
                Stage(fn=lambda x: x * 10, executor="thread"),
            ],
            input=[0, 1, 2],
        )
    )

    assert results == [10, 20, 30]


def test_three_stage_mixed_pipeline():
    """
    Given: 數字 [0, 5, 10]
    When:  thread +1 → async *2 → thread -1
    Then:  (0+1)*2-1=1, (5+1)*2-1=11, (10+1)*2-1=21
    """
    from qqabc.pipe import Stage, pipe

    async def async_mul2(x: int) -> int:
        return x * 2

    results = sorted(
        pipe(
            [
                Stage(fn=lambda x: x + 1, executor="thread"),
                Stage(fn=async_mul2),
                Stage(fn=lambda x: x - 1, executor="thread"),
            ],
            input=[0, 5, 10],
        )
    )

    assert results == [1, 11, 21]


def test_pipe_operator_two_stages():
    """
    Given: 兩個 stage 用 | 串接
    When:  放入 Pipeline context manager 使用
    Then:  資料正確流經兩段 stage
    """
    from qqabc.pipe import Pipeline, Stage

    async def async_double(x: int) -> int:
        return x * 2

    stages = Stage(fn=lambda x: x + 1, name="inc") | Stage(
        fn=async_double, name="double"
    )

    with Pipeline(stages) as p:
        p.submit_many([10, 20, 30])

    results = sorted(p.results())

    assert results == [22, 42, 62]


def test_pipe_operator_three_stages():
    """
    Given: 三個 stage 用 | 連續串接
    When:  inc | double | dec 傳入 pipe() 一行式 API
    Then:  (x+1)*2 - 1 的結果正確
    """
    from qqabc.pipe import Stage, pipe

    async def async_double(x: int) -> int:
        return x * 2

    stages = (
        Stage(fn=lambda x: x + 1, name="inc")
        | Stage(fn=async_double, name="double")
        | Stage(fn=lambda x: x - 1, name="dec")
    )

    results = sorted(pipe(stages, input=[0, 5, 10]))

    # (0+1)*2-1=1, (5+1)*2-1=11, (10+1)*2-1=21
    assert results == [1, 11, 21]


def test_pipe_operator_result_is_list():
    """
    Given: 兩個 stage
    When:  用 | 串接
    Then:  回傳值是 list[IStage]，可直接傳給 Pipeline 或 pipe()
    """
    from qqabc.pipe import Stage

    a = Stage(fn=lambda x: x, name="a")
    b = Stage(fn=lambda x: x, name="b")

    result = a | b

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0].name == "a"
    assert result[1].name == "b"


def test_pipe_operator_stage_or_list():
    """
    Given: 一個 stage a 和一個 list [b, c]
    When:  用 a | [b, c] 串接
    Then:  回傳 [a, b, c]，順序正確
    """
    from qqabc.pipe import Stage, pipe

    async def async_double(x: int) -> int:
        return x * 2

    a = Stage(fn=lambda x: x + 1, name="a")
    b = Stage(fn=async_double, name="b")
    c = Stage(fn=lambda x: x - 1, name="c")

    stages = a | [b, c]

    assert isinstance(stages, list)
    assert len(stages) == 3
    assert [s.name for s in stages] == ["a", "b", "c"]

    # 實際跑 pipeline 驗證行為正確：(x+1)*2 - 1
    results = sorted(pipe(stages, input=[0, 5, 10]))
    assert results == [1, 11, 21]


def test_pipe_operator_list_or_stage():
    """
    Given: 一個 list [a, b] 和一個 stage c
    When:  用 [a, b] | c 串接（觸發 __ror__）
    Then:  回傳 [a, b, c]，順序正確
    """
    from qqabc.pipe import Stage, pipe

    async def async_double(x: int) -> int:
        return x * 2

    a = Stage(fn=lambda x: x + 1, name="a")
    b = Stage(fn=async_double, name="b")
    c = Stage(fn=lambda x: x - 1, name="c")

    stages = [a, b] | c

    assert isinstance(stages, list)
    assert len(stages) == 3
    assert [s.name for s in stages] == ["a", "b", "c"]

    results = sorted(pipe(stages, input=[0, 5, 10]))
    assert results == [1, 11, 21]
