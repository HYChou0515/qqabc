"""Channel — Bounded queue 與背壓機制。

提供 ``BoundedQ``（有界 thread/process queue）、``AsyncBoundedQ``（asyncio queue
包裝）、以及 thread ↔ async bridge 用於跨執行模型的資料傳輸。
"""

from __future__ import annotations

import asyncio
from queue import Queue as ThreadSafeQueue
from typing import TYPE_CHECKING, Generic, TypeVar

from multiprocess import Queue  # type: ignore[reportAttributeAccessIssue]

from qqabc.qq import END_MSG, Msg, Q

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from qqabc.qq import ContextName

T = TypeVar("T")

__all__ = [
    "AsyncBoundedQ",
    "BoundedQ",
    "bridge_async_to_thread",
    "bridge_thread_to_async",
]


class BoundedQ(Q[T]):
    """有界 queue，put() 在 queue 滿時阻塞。

    繼承 ``Q`` 的所有功能，額外支援 ``maxsize`` 參數控制背壓。
    ``maxsize=0`` 時行為與 ``Q`` 完全相同（無界）。

    Args:
        kind: 執行模型，預設為 ``"thread"``。
        maxsize: queue 容量上限，0 表示無界。
    """

    def __init__(self, kind: ContextName = "thread", maxsize: int = 0) -> None:
        if kind == "process":
            self._q = Queue(maxsize=maxsize)
        elif kind == "thread":
            self._q = ThreadSafeQueue(maxsize=maxsize)
        else:
            msg = f"Unknown queue type: {kind}"
            raise ValueError(msg)
        self._maxsize = maxsize

    @property
    def maxsize(self) -> int:
        """Queue 容量上限，0 表示無界。"""
        return self._maxsize


class AsyncBoundedQ(Generic[T]):
    """asyncio.Queue 包裝，用於 async stage 之間。

    提供與 ``Q`` 類似的介面，但使用 ``async``/``await``。
    ``maxsize=0`` 時為無界。

    Args:
        maxsize: queue 容量上限，0 表示無界。
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._q: asyncio.Queue[Msg[T]] = asyncio.Queue(maxsize=maxsize)
        self._maxsize = maxsize

    @property
    def maxsize(self) -> int:
        """Queue 容量上限，0 表示無界。"""
        return self._maxsize

    def qsize(self) -> int:
        """回傳 queue 中的近似項目數。"""
        return self._q.qsize()

    def empty(self) -> bool:
        """Queue 為空時回傳 ``True``。"""
        return self._q.empty()

    def full(self) -> bool:
        """Queue 已滿時回傳 ``True``。"""
        return self._q.full()

    async def put(self, data: T | Msg[T], *, kind: str = "", order: int = 0) -> None:
        """放入訊息，queue 滿時 await 阻塞（背壓）。

        Args:
            data: 訊息資料或已包裝的 ``Msg``。
            kind: 訊息種類標記。
            order: 訊息排序編號。
        """
        if isinstance(data, Msg):
            await self._q.put(data)
        else:
            await self._q.put(Msg(data=data, kind=kind, order=order))

    async def get(self) -> Msg[T]:
        """取出訊息，queue 空時 await 阻塞。"""
        return await self._q.get()

    async def end(self) -> None:
        """放入 ``END_MSG`` 表示不再有後續訊息。"""
        await self._q.put(END_MSG)

    async def __aiter__(self) -> AsyncIterator[Msg[T]]:
        """非同步迭代至收到 ``END_MSG``。"""
        while True:
            msg = await self._q.get()
            if msg.kind == END_MSG.kind:
                break
            yield msg


async def bridge_thread_to_async(
    source: Q[T],
    dest: AsyncBoundedQ[T],
) -> None:
    """從 thread/process queue 讀取並放入 async queue。

    在 executor 中執行阻塞式 ``get()``，再 await 放入 async queue。
    遇到 ``END_MSG`` 時結束並在 dest 放入 ``END_MSG``。

    Args:
        source: 來源 thread/process queue（``Q`` 或 ``BoundedQ``）。
        dest: 目標 async queue。
    """
    loop = asyncio.get_running_loop()
    # Q.__getattr__ 將 get 委派給底層 queue
    _get = source.get
    while True:
        msg: Msg[T] = await loop.run_in_executor(None, _get)
        if msg.kind == END_MSG.kind:
            await dest.end()
            break
        await dest.put(msg)


async def bridge_async_to_thread(
    source: AsyncBoundedQ[T],
    dest: Q[T],
) -> None:
    """從 async queue 讀取並放入 thread/process queue。

    以 ``run_in_executor`` 包裝阻塞式 ``put()``，避免阻塞事件迴圈。
    遇到 ``END_MSG`` 時結束並在 dest 放入 ``END_MSG``。

    Args:
        source: 來源 async queue。
        dest: 目標 thread/process queue（``Q`` 或 ``BoundedQ``）。
    """
    loop = asyncio.get_running_loop()
    async for msg in source:
        await loop.run_in_executor(None, dest.put, msg)
    await loop.run_in_executor(None, dest.end)
