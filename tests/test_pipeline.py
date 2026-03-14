"""Tests for qqabc.pipe.pipeline — Pipeline 與 pipe() (issue #34 + #38)。

驗證：
- pipe() 一行式 API
- Pipeline context manager
- 單 stage / 多 stage
- 背壓行為
- async stage
- 混合 executor（thread + async）
"""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


# === pipe() 一行式 API ===


class TestPipeFunction:
    """pipe() 便捷函式。"""

    def test_single_stage_identity(self) -> None:
        """單 stage，identity 函式。"""
        from qqabc.pipe import Stage, pipe

        result = list(pipe([Stage(fn=lambda x: x)], input=[1, 2, 3]))
        assert sorted(result) == [1, 2, 3]

    def test_single_stage_transform(self) -> None:
        """單 stage，x * 2。"""
        from qqabc.pipe import Stage, pipe

        result = list(pipe([Stage(fn=lambda x: x * 2)], input=[1, 2, 3]))
        assert sorted(result) == [2, 4, 6]

    def test_two_stages(self) -> None:
        """兩個 stage 串接。"""
        from qqabc.pipe import Stage, pipe

        result = list(
            pipe(
                [Stage(fn=lambda x: x + 1), Stage(fn=lambda x: x * 10)],
                input=[1, 2, 3],
            )
        )
        assert sorted(result) == [20, 30, 40]

    def test_three_stages(self) -> None:
        """三個 stage 串接。"""
        from qqabc.pipe import Stage, pipe

        result = list(
            pipe(
                [
                    Stage(fn=lambda x: x + 1),
                    Stage(fn=lambda x: x * 2),
                    Stage(fn=lambda x: x - 1),
                ],
                input=[0, 1, 2],
            )
        )
        assert sorted(result) == [1, 3, 5]

    def test_single_stage_instance(self) -> None:
        """傳入單一 Stage（非 list）。"""
        from qqabc.pipe import Stage, pipe

        result = list(pipe(Stage(fn=lambda x: x + 10), input=[1, 2]))
        assert sorted(result) == [11, 12]

    def test_empty_input(self) -> None:
        """空 input 回傳空結果。"""
        from qqabc.pipe import Stage, pipe

        result = list(pipe([Stage(fn=lambda x: x)], input=[]))
        assert result == []

    def test_no_input_returns_pipeline(self) -> None:
        """不提供 input 回傳 Pipeline 物件。"""
        from qqabc.pipe import Pipeline, Stage, pipe

        p = pipe([Stage(fn=lambda x: x)])
        assert isinstance(p, Pipeline)
        # 手動操作
        p.submit(42)
        result = list(p.results())
        assert result == [42]

    def test_with_backpressure(self) -> None:
        """背壓參數正常運作。"""
        from qqabc.pipe import Stage, pipe

        result = list(
            pipe([Stage(fn=lambda x: x * 3)], input=range(20), backpressure=2)
        )
        assert sorted(result) == [i * 3 for i in range(20)]

    def test_concurrency_respected(self) -> None:
        """不同 concurrency 都能正常完成。"""
        from qqabc.pipe import Stage, pipe

        result = list(
            pipe(
                [Stage(fn=lambda x: x, concurrency=1)],
                input=range(10),
            )
        )
        assert sorted(result) == list(range(10))

    def test_high_concurrency(self) -> None:
        """高 concurrency 也正常。"""
        from qqabc.pipe import Stage, pipe

        result = list(
            pipe(
                [Stage(fn=lambda x: x, concurrency=8)],
                input=range(50),
            )
        )
        assert sorted(result) == list(range(50))


# === Pipeline context manager ===


class TestPipelineContextManager:
    """Pipeline 作為 context manager。"""

    def test_basic_context_manager(self) -> None:
        """With Pipeline(...) as p 基本用法。"""
        from qqabc.pipe import Pipeline, Stage

        with Pipeline([Stage(fn=lambda x: x + 1)]) as p:
            p.submit(1)
            p.submit(2)
        result = list(p.results())
        assert sorted(result) == [2, 3]

    def test_submit_many(self) -> None:
        """submit_many 批次提交。"""
        from qqabc.pipe import Pipeline, Stage

        with Pipeline([Stage(fn=lambda x: x * 2)]) as p:
            p.submit_many([10, 20, 30])
        result = sorted(p.results())
        assert result == [20, 40, 60]

    def test_from_or_operator(self) -> None:
        """用 | 建構的 stage list 傳給 Pipeline。"""
        from qqabc.pipe import Pipeline, Stage

        stages = Stage(fn=lambda x: x + 1, name="a") | Stage(
            fn=lambda x: x * 2, name="b"
        )
        with Pipeline(stages) as p:
            p.submit_many([0, 1, 2])
        result = sorted(p.results())
        assert result == [2, 4, 6]

    def test_empty_stages_raises(self) -> None:
        """空 stages 應 raise。"""
        from qqabc.pipe import Pipeline

        with pytest.raises(ValueError, match="至少需要一個"):
            Pipeline([])


# === 背壓驗證 ===


class TestPipelineBackpressure:
    """驗證背壓行為：下游慢時上游不會無限堆積。"""

    def test_slow_consumer_no_oom(self) -> None:
        """下游慢，資料仍完整傳輸。"""
        from qqabc.pipe import Stage, pipe

        def slow_fn(x: int) -> int:
            time.sleep(0.01)
            return x

        result = list(
            pipe(
                [Stage(fn=lambda x: x), Stage(fn=slow_fn, concurrency=1)],
                input=range(15),
                backpressure=2,
            )
        )
        assert sorted(result) == list(range(15))


# === Async stage ===


class TestPipelineAsync:
    """Async stage 在 pipeline 中的行為。"""

    def test_single_async_stage(self) -> None:
        """單一 async stage。"""
        from qqabc.pipe import Stage, pipe

        async def double(x: int) -> int:
            await asyncio.sleep(0.001)
            return x * 2

        result = list(pipe([Stage(fn=double)], input=[1, 2, 3, 4, 5]))
        assert sorted(result) == [2, 4, 6, 8, 10]

    def test_async_with_backpressure(self) -> None:
        """Async stage + 背壓。"""
        from qqabc.pipe import Stage, pipe

        async def add_ten(x: int) -> int:
            await asyncio.sleep(0.001)
            return x + 10

        result = list(pipe([Stage(fn=add_ten)], input=range(10), backpressure=3))
        assert sorted(result) == list(range(10, 20))


# === 混合 executor ===


class TestPipelineMixed:
    """Thread + Async 混合 pipeline。"""

    def test_thread_then_async(self) -> None:
        """Thread stage → Async stage。"""
        from qqabc.pipe import Stage, pipe

        async def async_double(x: int) -> int:
            return x * 2

        result = list(
            pipe(
                [
                    Stage(fn=lambda x: x + 1, executor="thread"),
                    Stage(fn=async_double),
                ],
                input=[1, 2, 3],
            )
        )
        assert sorted(result) == [4, 6, 8]

    def test_async_then_thread(self) -> None:
        """Async stage → Thread stage。"""
        from qqabc.pipe import Stage, pipe

        async def async_inc(x: int) -> int:
            return x + 1

        result = list(
            pipe(
                [
                    Stage(fn=async_inc),
                    Stage(fn=lambda x: x * 10, executor="thread"),
                ],
                input=[0, 1, 2],
            )
        )
        assert sorted(result) == [10, 20, 30]

    def test_three_mixed_stages(self) -> None:
        """Thread → Async → Thread 三段混合。"""
        from qqabc.pipe import Stage, pipe

        async def async_step(x: int) -> int:
            return x * 2

        result = list(
            pipe(
                [
                    Stage(fn=lambda x: x + 1, executor="thread"),
                    Stage(fn=async_step),
                    Stage(fn=lambda x: x - 1, executor="thread"),
                ],
                input=[0, 5, 10],
                backpressure=5,
            )
        )
        assert sorted(result) == [1, 11, 21]

    def test_mixed_with_different_concurrency(self) -> None:
        """各 stage 不同 concurrency 也正常。"""
        from qqabc.pipe import Stage, pipe

        async def async_fn(x: int) -> int:
            await asyncio.sleep(0.001)
            return x

        result = list(
            pipe(
                [
                    Stage(fn=lambda x: x, executor="thread", concurrency=2),
                    Stage(fn=async_fn, concurrency=8),
                    Stage(fn=lambda x: x, executor="thread", concurrency=1),
                ],
                input=range(20),
                backpressure=3,
            )
        )
        assert sorted(result) == list(range(20))


# === 邊界情況 ===


class TestPipelineEdgeCases:
    """邊界與特殊情況。"""

    def test_large_input(self) -> None:
        """較大 input 也正常。"""
        from qqabc.pipe import Stage, pipe

        result = list(pipe([Stage(fn=lambda x: x)], input=range(500), backpressure=10))
        assert sorted(result) == list(range(500))

    def test_string_data(self) -> None:
        """字串資料。"""
        from qqabc.pipe import Stage, pipe

        result = list(
            pipe(
                [Stage(fn=lambda s: s.upper())],
                input=["hello", "world"],
            )
        )
        assert sorted(result) == ["HELLO", "WORLD"]

    def test_dict_data(self) -> None:
        """Dict 資料。"""
        from qqabc.pipe import Stage, pipe

        result = list(
            pipe(
                [Stage(fn=lambda d: {**d, "done": True})],
                input=[{"id": 1}, {"id": 2}],
            )
        )
        result.sort(key=lambda d: d["id"])
        assert result == [{"id": 1, "done": True}, {"id": 2, "done": True}]

    def test_pipeline_close_idempotent(self) -> None:
        """多次 close 不報錯。"""
        from qqabc.pipe import Pipeline, Stage

        p = Pipeline([Stage(fn=lambda x: x)])
        p.submit(1)
        p.close()
        p.close()  # idempotent
        result = list(p.results())
        assert result == [1]

    def test_results_auto_closes(self) -> None:
        """呼叫 results() 時若未 close，自動 close。"""
        from qqabc.pipe import Pipeline, Stage

        p = Pipeline([Stage(fn=lambda x: x)])
        p.submit(1)
        result = list(p.results())
        assert result == [1]
