"""Channel — Bounded Queue 與 async bridge。

提供 ``BoundedQ``（有界 queue）和 ``AsyncBoundedQ``（asyncio queue），
以及 thread ↔ async 的 bridge 函式，讓 thread-based 與 async-based
的 stage 能正確溝通。
"""

from __future__ import annotations

import asyncio
from queue import Queue as ThreadSafeQueue
from typing import TYPE_CHECKING, Any, Generic, TypeVar

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
    """有界 queue，``put()`` 在 queue 滿時阻塞（backpressure）。

    繼承 ``Q`` 的所有迭代與 sentinel 機制，
    但底層 queue 透過 ``maxsize`` 限制容量。

    Args:
        kind: 執行模式，``"thread"`` 或 ``"process"``。
        maxsize: 最大容量，0 = 無界（向後相容）。
    """

    def __init__(
        self,
        *,
        kind: ContextName = "thread",
        maxsize: int = 0,
    ) -> None:
        # 不呼叫 super().__init__()，直接建立有 maxsize 的 queue
        if kind == "process":
            self._q: Any = Queue(maxsize=maxsize)
        elif kind == "thread":
            self._q = ThreadSafeQueue(maxsize=maxsize)
        else:
            msg = f"Unknown queue type: {kind}"
            raise ValueError(msg)
        self._cache: list[Msg[T]] | None = None


class AsyncBoundedQ(Generic[T]):
    """``asyncio.Queue`` 包裝，用於 async stage 內部通訊。

    提供 ``async for`` 迭代（到 ``END_MSG`` 為止）、
    以及 ``put`` / ``get`` / ``end`` 方法。

    Args:
        maxsize: 最大容量，0 = 無界。
    """

    def __init__(self, *, maxsize: int = 0) -> None:
        self._q: asyncio.Queue[Msg[T]] = asyncio.Queue(maxsize=maxsize)

    async def put(self, data: T, *, order: int = 0) -> None:
        """Put a data item wrapped in ``Msg``."""
        await self._q.put(Msg(data=data, order=order))

    async def put_msg(self, msg: Msg[T]) -> None:
        """Put a raw ``Msg`` directly."""
        await self._q.put(msg)

    async def get(self) -> Msg[T]:
        """Get the next ``Msg``."""
        return await self._q.get()

    async def end(self) -> None:
        """Send ``END_MSG`` sentinel."""
        await self._q.put(END_MSG)

    def qsize(self) -> int:
        """Approximate queue size."""
        return self._q.qsize()

    def empty(self) -> bool:
        """Whether the queue is empty."""
        return self._q.empty()

    def full(self) -> bool:
        """Whether the queue is full."""
        return self._q.full()

    def __aiter__(self) -> AsyncIterator[Msg[T]]:
        """Iterate until ``END_MSG`` is received."""
        return self._aiter_impl()

    async def _aiter_impl(self) -> AsyncIterator[Msg[T]]:  # type: ignore[misc]
        """內部 async iterator 實作。"""
        while True:
            msg = await self._q.get()
            if msg.kind == END_MSG.kind:
                break
            yield msg


async def bridge_thread_to_async(
    thread_q: BoundedQ[T],
    async_q: AsyncBoundedQ[T],
) -> None:
    """從 thread-safe queue 讀取並轉發到 async queue。

    在 event loop 中用 ``run_in_executor`` 執行阻塞的 ``get()``，
    讀到 ``END_MSG`` 時結束並送 ``END_MSG`` 到 ``async_q``。
    """
    loop = asyncio.get_running_loop()
    while True:
        msg: Msg[T] = await loop.run_in_executor(None, thread_q._q.get)  # noqa: SLF001
        if msg.kind == END_MSG.kind:
            await async_q.end()
            break
        await async_q.put_msg(msg)


async def bridge_async_to_thread(
    async_q: AsyncBoundedQ[T],
    thread_q: BoundedQ[T],
) -> None:
    """從 async queue 讀取並轉發到 thread-safe queue。

    讀到 ``END_MSG`` 時結束並送 ``END_MSG`` 到 ``thread_q``。
    用 ``run_in_executor`` 避免阻塞 event loop。
    """
    loop = asyncio.get_running_loop()
    async for msg in async_q:
        await loop.run_in_executor(None, thread_q._q.put, msg)  # noqa: SLF001
    await loop.run_in_executor(None, thread_q._q.put, END_MSG)  # noqa: SLF001
