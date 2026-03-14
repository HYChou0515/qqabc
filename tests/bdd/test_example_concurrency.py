"""Example: Async Stage 的 Concurrency 控制。

情境：
    使用者想要同時發起大量 async IO 操作（如 HTTP 請求），
    但需要限制同時進行的數量以避免壓垮下游服務。

用法展示：
    - concurrency 參數控制同時執行的 async task 數量
    - 高併發場景 (concurrency=50) 的運作
    - 背壓 (backpressure) 搭配 async stage
"""

from __future__ import annotations

import asyncio
import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


def test_concurrency_limits_parallel_tasks():
    """
    Given: 30 筆資料，async stage concurrency=5
    When:  每個 task 耗時 30ms
    Then:  同時執行中的 task 不超過 5 個，且全部資料都被處理
    """
    from qqabc.pipe import Stage, pipe

    peak_concurrent = [0]
    current = [0]

    async def slow_task(x: int) -> int:
        current[0] += 1
        peak_concurrent[0] = max(peak_concurrent[0], current[0])
        await asyncio.sleep(0.03)
        current[0] -= 1
        return x

    results = sorted(pipe([Stage(fn=slow_task, concurrency=5)], input=range(30)))

    assert results == list(range(30))  # 全部處理完成
    assert peak_concurrent[0] <= 5  # 沒有超過 concurrency 限制


def test_high_concurrency_50_tasks():
    """
    Given: 100 筆資料，async stage concurrency=50
    When:  每個 task 耗時 150ms
    Then:  同時確實有接近 50 個 task 在跑
    """
    from qqabc.pipe import Stage, pipe

    peak_concurrent = [0]
    current = [0]

    async def slow_task(x: int) -> int:
        current[0] += 1
        peak_concurrent[0] = max(peak_concurrent[0], current[0])
        await asyncio.sleep(0.15)
        current[0] -= 1
        return x

    results = sorted(pipe([Stage(fn=slow_task, concurrency=50)], input=range(100)))

    assert results == list(range(100))
    assert peak_concurrent[0] >= 40  # 容許些許 timing 波動


def test_backpressure_with_async_stage():
    """
    Given: 30 筆資料，backpressure=2，async concurrency=4
    When:  pipeline 限制 queue 大小為 2
    Then:  不會 deadlock，所有資料仍完整送達
    """
    from qqabc.pipe import Stage, pipe

    async def process(x: int) -> int:
        await asyncio.sleep(0.005)
        return x * 2

    results = sorted(
        pipe(
            [
                Stage(fn=lambda x: x + 1, executor="thread", concurrency=2),
                Stage(fn=process, concurrency=4),
            ],
            input=range(30),
            backpressure=2,
        )
    )

    expected = [(x + 1) * 2 for x in range(30)]
    assert results == expected
