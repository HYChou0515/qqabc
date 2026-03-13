"""Tests for qqabc.pipe.channel — Bounded Queue 與背壓機制 (issue #32)。

驗證 BoundedQ / AsyncBoundedQ / bridge 的行為，包含：
- BoundedQ 基本功能與繼承的 Q 操作
- 背壓（backpressure）行為
- AsyncBoundedQ 基本功能
- thread ↔ async bridge 正確性
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


# === BoundedQ 基本功能 ===


class TestBoundedQBasic:
    """BoundedQ 基本建立與屬性。"""

    def test_thread_mode(self) -> None:
        """Thread 模式建立 BoundedQ。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[str] = BoundedQ(kind="thread", maxsize=5)
        assert q.maxsize == 5

    def test_process_mode(self) -> None:
        """Process 模式建立 BoundedQ。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[str] = BoundedQ(kind="process", maxsize=3)
        assert q.maxsize == 3

    def test_default_kind_is_thread(self) -> None:
        """預設 kind 為 thread。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[int] = BoundedQ(maxsize=2)
        assert q.maxsize == 2

    def test_maxsize_zero_unbounded(self) -> None:
        """maxsize=0 為無界 queue，可放入大量 items。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[int] = BoundedQ(maxsize=0)
        assert q.maxsize == 0
        for i in range(100):
            q.put(i, order=i)
        q.end()
        assert len(list(q)) == 100

    def test_invalid_kind_raises(self) -> None:
        """非法 kind 應 raise ValueError。"""
        from qqabc.pipe.channel import BoundedQ

        with pytest.raises(ValueError, match="Unknown queue type"):
            BoundedQ(kind="invalid")  # type: ignore[arg-type]

    def test_is_subclass_of_q(self) -> None:
        """BoundedQ 是 Q 的子類。"""
        from qqabc.pipe.channel import BoundedQ
        from qqabc.qq import Q

        assert issubclass(BoundedQ, Q)


class TestBoundedQOperations:
    """BoundedQ 繼承的 Q 操作。"""

    def test_put_and_iter(self) -> None:
        """Put + iter 正常運作。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[str] = BoundedQ(maxsize=10)
        q.put("a", order=0)
        q.put("b", order=1)
        q.end()
        assert [m.data for m in q] == ["a", "b"]

    def test_put_msg_directly(self) -> None:
        """直接 put Msg 物件。"""
        from qqabc.pipe.channel import BoundedQ
        from qqabc.qq import Msg

        q: BoundedQ[str] = BoundedQ(maxsize=10)
        q.put(Msg(data="hello", order=0))
        q.end()
        assert [m.data for m in q] == ["hello"]

    def test_sorted_iteration(self) -> None:
        """sorted() 按 order 排序。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[str] = BoundedQ(maxsize=10)
        q.put("c", order=2)
        q.put("a", order=0)
        q.put("b", order=1)
        q.end()
        assert [m.data for m in q.sorted()] == ["a", "b", "c"]

    def test_items_cached(self) -> None:
        """items(cache=True) 可重複讀取。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[int] = BoundedQ(maxsize=10)
        q.put(1, order=0)
        q.put(2, order=1)
        first = [m.data for m in q.items(cache=True)]
        second = [m.data for m in q.items(cache=True)]
        assert first == second == [1, 2]

    def test_qsize_and_empty(self) -> None:
        """qsize() 和 empty() 正確回報。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[int] = BoundedQ(maxsize=10)
        assert q.empty()
        q.put(42)
        assert q.qsize() >= 1
        assert not q.empty()

    def test_stop_workers(self) -> None:
        """stop() 正確結束 workers。"""
        from qqabc.pipe.channel import BoundedQ
        from qqabc.qq import Worker

        results: list[int] = []

        def worker_fn(q: BoundedQ[int]) -> None:
            results.extend(msg.data for msg in q)

        q: BoundedQ[int] = BoundedQ(maxsize=10)
        w = Worker.thread(worker_fn, q)
        q.put(1)
        q.put(2)
        q.stop([w])
        assert sorted(results) == [1, 2]

    def test_process_put_and_iter(self) -> None:
        """Process 模式 put + iter。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[str] = BoundedQ(kind="process", maxsize=10)
        q.put("x", order=0)
        q.put("y", order=1)
        q.end()
        assert [m.data for m in q] == ["x", "y"]


# === BoundedQ 背壓 ===


class TestBoundedQBackpressure:
    """BoundedQ 背壓行為。"""

    def test_thread_put_blocks_when_full(self) -> None:
        """Thread 模式 queue 滿時 put() 阻塞。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[str] = BoundedQ(kind="thread", maxsize=2)
        q.put("a")
        q.put("b")

        put_completed = threading.Event()

        def producer() -> None:
            q.put("c")
            put_completed.set()

        t = threading.Thread(target=producer)
        t.start()
        time.sleep(0.15)
        assert not put_completed.is_set(), "put() 應被阻塞"

        # 消費一個 item 讓 producer 解除阻塞（透過 Q.__getattr__ 委派）
        q.get()

        t.join(timeout=2)
        assert put_completed.is_set(), "put() 應已完成"

    def test_backpressure_preserves_data(self) -> None:
        """背壓情境下資料不遺失。"""
        from qqabc.pipe.channel import BoundedQ

        q: BoundedQ[int] = BoundedQ(kind="thread", maxsize=2)
        n = 20

        def producer() -> None:
            for i in range(n):
                q.put(i, order=i)
            q.end()

        t = threading.Thread(target=producer)
        t.start()

        results = []
        for msg in q.sorted():
            results.append(msg.data)
            time.sleep(0.005)  # 模擬慢消費

        t.join(timeout=10)
        assert results == list(range(n))


# === AsyncBoundedQ 基本功能 ===


class TestAsyncBoundedQBasic:
    """AsyncBoundedQ 基本建立與屬性。"""

    @pytest.mark.asyncio
    async def test_init_and_maxsize(self) -> None:
        """建立 AsyncBoundedQ 並檢查 maxsize。"""
        from qqabc.pipe.channel import AsyncBoundedQ

        q: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=5)
        assert q.maxsize == 5

    @pytest.mark.asyncio
    async def test_unbounded(self) -> None:
        """maxsize=0 為無界。"""
        from qqabc.pipe.channel import AsyncBoundedQ

        q: AsyncBoundedQ[int] = AsyncBoundedQ(maxsize=0)
        assert q.maxsize == 0
        for i in range(50):
            await q.put(i)
        assert q.qsize() == 50

    @pytest.mark.asyncio
    async def test_qsize_empty_full(self) -> None:
        """qsize、empty、full 正確回報。"""
        from qqabc.pipe.channel import AsyncBoundedQ

        q: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=2)
        assert q.empty()
        assert not q.full()
        assert q.qsize() == 0

        await q.put("a")
        assert q.qsize() == 1
        assert not q.empty()
        assert not q.full()

        await q.put("b")
        assert q.qsize() == 2
        assert q.full()

    @pytest.mark.asyncio
    async def test_put_and_get(self) -> None:
        """Put + get 基本功能。"""
        from qqabc.pipe.channel import AsyncBoundedQ

        q: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)
        await q.put("hello", order=1)
        msg = await q.get()
        assert msg.data == "hello"
        assert msg.order == 1

    @pytest.mark.asyncio
    async def test_put_msg_directly(self) -> None:
        """直接 put Msg 物件。"""
        from qqabc.pipe.channel import AsyncBoundedQ
        from qqabc.qq import Msg

        q: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)
        await q.put(Msg(data="world", kind="test", order=5))
        msg = await q.get()
        assert msg.data == "world"
        assert msg.kind == "test"
        assert msg.order == 5

    @pytest.mark.asyncio
    async def test_end_and_aiter(self) -> None:
        """end() + __aiter__ 正常迭代至 END_MSG。"""
        from qqabc.pipe.channel import AsyncBoundedQ

        q: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)
        await q.put("a", order=0)
        await q.put("b", order=1)
        await q.end()

        msgs = [msg.data async for msg in q]
        assert msgs == ["a", "b"]


# === AsyncBoundedQ 背壓 ===


class TestAsyncBoundedQBackpressure:
    """AsyncBoundedQ 背壓行為。"""

    @pytest.mark.asyncio
    async def test_put_blocks_when_full(self) -> None:
        """Queue 滿時 put() await 阻塞。"""
        from qqabc.pipe.channel import AsyncBoundedQ

        q: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=1)
        await q.put("a")

        put_done = asyncio.Event()

        async def producer() -> None:
            await q.put("b")
            put_done.set()

        task = asyncio.create_task(producer())
        await asyncio.sleep(0.1)
        assert not put_done.is_set(), "put() 應被阻塞"

        # 消費一個 item 解除阻塞
        _ = await q.get()
        await asyncio.sleep(0.05)
        assert put_done.is_set(), "put() 應已完成"
        await task

    @pytest.mark.asyncio
    async def test_backpressure_preserves_order(self) -> None:
        """背壓情境下資料順序與完整性。"""
        from qqabc.pipe.channel import AsyncBoundedQ

        q: AsyncBoundedQ[int] = AsyncBoundedQ(maxsize=2)
        n = 15

        async def producer() -> None:
            for i in range(n):
                await q.put(i, order=i)
            await q.end()

        task = asyncio.create_task(producer())

        results = []
        async for msg in q:
            results.append(msg.data)
            await asyncio.sleep(0.005)

        await task
        assert results == list(range(n))


# === Bridge: Thread → Async ===


class TestBridgeThreadToAsync:
    """Thread → Async bridge。"""

    @pytest.mark.asyncio
    async def test_basic_transfer(self) -> None:
        """基本資料轉移。"""
        from qqabc.pipe.channel import AsyncBoundedQ, BoundedQ, bridge_thread_to_async

        source: BoundedQ[str] = BoundedQ(kind="thread", maxsize=10)
        dest: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)

        source.put("a", order=0)
        source.put("b", order=1)
        source.end()

        await bridge_thread_to_async(source, dest)

        msgs = [msg.data async for msg in dest]
        assert msgs == ["a", "b"]

    @pytest.mark.asyncio
    async def test_with_regular_q(self) -> None:
        """使用一般 Q 作為 source。"""
        from qqabc.pipe.channel import AsyncBoundedQ, bridge_thread_to_async
        from qqabc.qq import Q

        source: Q[str] = Q(kind="thread")
        dest: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)

        source.put("hello", order=0)
        source.end()

        await bridge_thread_to_async(source, dest)

        msgs = [msg.data async for msg in dest]
        assert msgs == ["hello"]

    @pytest.mark.asyncio
    async def test_empty_source(self) -> None:
        """空 source 只傳遞 END_MSG。"""
        from qqabc.pipe.channel import AsyncBoundedQ, BoundedQ, bridge_thread_to_async

        source: BoundedQ[str] = BoundedQ(kind="thread", maxsize=10)
        dest: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)

        source.end()
        await bridge_thread_to_async(source, dest)

        msgs = [msg.data async for msg in dest]
        assert msgs == []

    @pytest.mark.asyncio
    async def test_with_backpressure(self) -> None:
        """Dest queue 小時仍正確傳輸（背壓）。"""
        from qqabc.pipe.channel import AsyncBoundedQ, BoundedQ, bridge_thread_to_async

        source: BoundedQ[str] = BoundedQ(kind="thread", maxsize=10)
        dest: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=1)

        source.put("a", order=0)
        source.put("b", order=1)
        source.put("c", order=2)
        source.end()

        bridge_task = asyncio.create_task(bridge_thread_to_async(source, dest))

        msgs = [msg.data async for msg in dest]

        await bridge_task
        assert msgs == ["a", "b", "c"]


# === Bridge: Async → Thread ===


class TestBridgeAsyncToThread:
    """Async → Thread bridge。"""

    @pytest.mark.asyncio
    async def test_basic_transfer(self) -> None:
        """基本資料轉移。"""
        from qqabc.pipe.channel import AsyncBoundedQ, BoundedQ, bridge_async_to_thread

        source: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)
        dest: BoundedQ[str] = BoundedQ(kind="thread", maxsize=10)

        await source.put("x", order=0)
        await source.put("y", order=1)
        await source.end()

        await bridge_async_to_thread(source, dest)

        msgs = list(dest)
        assert [m.data for m in msgs] == ["x", "y"]

    @pytest.mark.asyncio
    async def test_with_regular_q_dest(self) -> None:
        """使用一般 Q 作為 dest。"""
        from qqabc.pipe.channel import AsyncBoundedQ, bridge_async_to_thread
        from qqabc.qq import Q

        source: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)
        dest: Q[str] = Q(kind="thread")

        await source.put("data", order=0)
        await source.end()

        await bridge_async_to_thread(source, dest)

        msgs = list(dest)
        assert [m.data for m in msgs] == ["data"]

    @pytest.mark.asyncio
    async def test_empty_source(self) -> None:
        """空 source 只傳遞 END_MSG。"""
        from qqabc.pipe.channel import AsyncBoundedQ, BoundedQ, bridge_async_to_thread

        source: AsyncBoundedQ[str] = AsyncBoundedQ(maxsize=10)
        dest: BoundedQ[str] = BoundedQ(kind="thread", maxsize=10)

        await source.end()
        await bridge_async_to_thread(source, dest)

        msgs = list(dest)
        assert msgs == []

    @pytest.mark.asyncio
    async def test_with_backpressure(self) -> None:
        """Dest queue 小時仍正確傳輸（背壓）。"""
        from qqabc.pipe.channel import AsyncBoundedQ, BoundedQ, bridge_async_to_thread

        source: AsyncBoundedQ[int] = AsyncBoundedQ(maxsize=10)
        dest: BoundedQ[int] = BoundedQ(kind="thread", maxsize=1)

        await source.put(1, order=0)
        await source.put(2, order=1)
        await source.put(3, order=2)
        await source.end()

        # consumer thread 先啟動，否則 bridge 的 put 可能因 dest 已滿而卡住
        results: list[int] = []
        consumer_done = threading.Event()

        def consumer() -> None:
            results.extend(msg.data for msg in dest)
            consumer_done.set()

        t = threading.Thread(target=consumer)
        t.start()

        await bridge_async_to_thread(source, dest)

        t.join(timeout=5)
        assert consumer_done.is_set()
        assert results == [1, 2, 3]
