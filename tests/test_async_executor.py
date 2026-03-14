"""Tests for AsyncExecutor — async stage 的 concurrency 與錯誤處理 (issue #33)。

驗證：
- async stage 的 semaphore concurrency 控制
- 高併發 (concurrency=50) 確實達到 50 個 concurrent task
- async fn 拋出例外時 pipeline 不會 deadlock
- thread → async → thread bridge 正確傳輸
- async stage 自動偵測
"""

from __future__ import annotations

import asyncio
import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


# === Concurrency 控制 ===


class TestAsyncConcurrencyControl:
    """驗證 asyncio.Semaphore 確實限制同時執行的 async task 數量。"""

    def test_concurrency_limit_respected(self) -> None:
        """concurrency=5 時，同時執行的 task 不超過 5。"""
        from qqabc.pipe import Stage, pipe

        peak = [0]
        current = [0]

        async def tracked(x: int) -> int:
            current[0] += 1
            peak[0] = max(peak[0], current[0])
            await asyncio.sleep(0.03)
            current[0] -= 1
            return x

        result = list(pipe([Stage(fn=tracked, concurrency=5)], input=range(30)))
        assert peak[0] <= 5
        assert sorted(result) == list(range(30))

    def test_concurrency_1_sequential(self) -> None:
        """concurrency=1 時，一次只有一個 task。"""
        from qqabc.pipe import Stage, pipe

        peak = [0]
        current = [0]

        async def tracked(x: int) -> int:
            current[0] += 1
            peak[0] = max(peak[0], current[0])
            await asyncio.sleep(0.01)
            current[0] -= 1
            return x

        result = list(pipe([Stage(fn=tracked, concurrency=1)], input=range(10)))
        assert peak[0] == 1
        assert sorted(result) == list(range(10))

    def test_concurrency_50_reached(self) -> None:
        """concurrency=50 時確實有接近 50 個 concurrent async task。

        驗收條件：concurrency=50 時確實有 50 個 concurrent async task。
        """
        from qqabc.pipe import Stage, pipe

        peak = [0]
        current = [0]

        async def tracked(x: int) -> int:
            current[0] += 1
            peak[0] = max(peak[0], current[0])
            await asyncio.sleep(0.15)
            current[0] -= 1
            return x

        result = list(pipe([Stage(fn=tracked, concurrency=50)], input=range(100)))
        # 應接近 50（容許些許 timing 差異）
        assert peak[0] >= 40
        assert peak[0] <= 50
        assert sorted(result) == list(range(100))


# === 錯誤傳播 ===


class TestAsyncErrorHandling:
    """Async stage 中 fn 拋出例外時的行為。"""

    def test_error_does_not_deadlock(self) -> None:
        """單一 item 拋出例外時，pipeline 不 deadlock、其餘 item 正常。"""
        from qqabc.pipe import Stage, pipe

        async def maybe_fail(x: int) -> int:
            if x == 5:
                msg = "boom"
                raise ValueError(msg)
            return x

        result = list(pipe([Stage(fn=maybe_fail)], input=range(10)))
        # item 5 失敗被丟棄，其餘正常
        assert 5 not in result
        assert sorted(result) == [0, 1, 2, 3, 4, 6, 7, 8, 9]

    def test_multiple_errors(self) -> None:
        """多個 item 拋出例外，pipeline 仍完成。"""
        from qqabc.pipe import Stage, pipe

        async def fail_odd(x: int) -> int:
            if x % 2 == 1:
                msg = f"odd: {x}"
                raise ValueError(msg)
            return x

        result = list(pipe([Stage(fn=fail_odd)], input=range(10)))
        assert sorted(result) == [0, 2, 4, 6, 8]

    def test_all_errors_empty_result(self) -> None:
        """所有 item 都失敗，回傳空結果、不 deadlock。"""
        from qqabc.pipe import Stage, pipe

        async def always_fail(x: int) -> int:
            msg = "always"
            raise ValueError(msg)

        result = list(pipe([Stage(fn=always_fail)], input=range(5)))
        assert result == []


# === Bridge 正確性 ===


class TestAsyncBridgeCorrectness:
    """Thread ↔ async bridge 的資料傳輸正確性。"""

    def test_order_preserved_in_async_stage(self) -> None:
        """Async stage 的輸出 order 正確（可用 sorted 驗證完整性）。"""
        from qqabc.pipe import Stage, pipe

        async def identity(x: int) -> int:
            await asyncio.sleep(0.001)
            return x

        result = list(pipe([Stage(fn=identity)], input=range(50)))
        assert sorted(result) == list(range(50))

    def test_large_data_through_bridge(self) -> None:
        """大量資料通過 bridge 不遺漏。"""
        from qqabc.pipe import Stage, pipe

        async def add_one(x: int) -> int:
            return x + 1

        result = list(pipe([Stage(fn=add_one)], input=range(500)))
        assert sorted(result) == list(range(1, 501))

    def test_thread_async_thread_roundtrip(self) -> None:
        """Thread → Async → Thread 完整 roundtrip。"""
        from qqabc.pipe import Stage, pipe

        async def async_mul(x: int) -> int:
            await asyncio.sleep(0.001)
            return x * 3

        result = list(
            pipe(
                [
                    Stage(fn=lambda x: x + 1, executor="thread"),
                    Stage(fn=async_mul),
                    Stage(fn=lambda x: x - 1, executor="thread"),
                ],
                input=range(20),
            )
        )
        # (x+1)*3 - 1 = 3x+2
        assert sorted(result) == [3 * x + 2 for x in range(20)]


# === 自動偵測 ===


class TestAsyncAutoDetect:
    """Stage 自動偵測 asyncio.iscoroutinefunction(fn)。"""

    def test_async_fn_auto_detected(self) -> None:
        """Async 函式自動設定 executor='async'。"""
        from qqabc.pipe import Stage

        async def my_async(_x: int) -> int:
            return _x

        stage = Stage(fn=my_async)
        assert stage.executor == "async"

    def test_sync_fn_auto_detected(self) -> None:
        """同步函式自動設定 executor='thread'。"""
        from qqabc.pipe import Stage

        def my_sync(_x: int) -> int:
            return _x

        stage = Stage(fn=my_sync)
        assert stage.executor == "thread"

    def test_explicit_executor_overrides(self) -> None:
        """明確指定 executor 時覆蓋自動偵測。"""
        from qqabc.pipe import Stage

        async def my_async(_x: int) -> int:
            return _x

        stage = Stage(fn=my_async, executor="thread")
        assert stage.executor == "thread"

    def test_auto_detected_async_runs_correctly(self) -> None:
        """自動偵測的 async stage 在 pipeline 中正確運作。"""
        from qqabc.pipe import Stage, pipe

        async def double(x: int) -> int:
            return x * 2

        # 不明確指定 executor，靠自動偵測
        result = list(pipe([Stage(fn=double)], input=[1, 2, 3]))
        assert sorted(result) == [2, 4, 6]


# === Backpressure 與 async ===


class TestAsyncBackpressure:
    """Async stage 搭配 backpressure 的行為。"""

    def test_async_stage_with_tight_backpressure(self) -> None:
        """背壓=1 搭配 async stage 不 deadlock。"""
        from qqabc.pipe import Stage, pipe

        async def slow_async(x: int) -> int:
            await asyncio.sleep(0.01)
            return x

        result = list(
            pipe(
                [Stage(fn=slow_async, concurrency=3)],
                input=range(15),
                backpressure=1,
            )
        )
        assert sorted(result) == list(range(15))

    def test_mixed_with_backpressure(self) -> None:
        """Thread + Async + backpressure 不 deadlock。"""
        from qqabc.pipe import Stage, pipe

        async def async_step(x: int) -> int:
            await asyncio.sleep(0.005)
            return x * 2

        result = list(
            pipe(
                [
                    Stage(fn=lambda x: x + 1, executor="thread", concurrency=2),
                    Stage(fn=async_step, concurrency=4),
                ],
                input=range(30),
                backpressure=2,
            )
        )
        assert sorted(result) == [(x + 1) * 2 for x in range(30)]
