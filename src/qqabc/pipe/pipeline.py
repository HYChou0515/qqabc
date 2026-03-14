"""Pipeline — 線性 pipeline 編排器。

將多個 Stage 串接成 ``A → B → C`` 流水線，自動建立 BoundedQ、
啟動 worker、處理 END_MSG 傳播與背壓。

使用者只需：

.. code-block:: python

    from qqabc.pipe import pipe, Stage

    results = list(
        pipe(
            [Stage(fn=download), Stage(fn=parse)],
            input=urls,
            backpressure=100,
        )
    )

或使用 ``|`` 運算子搭配 context manager：

.. code-block:: python

    with Pipeline(download_stage | parse_stage, backpressure=50) as p:
        p.submit_many(urls)
        for r in p.results():
            print(r)
"""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

from qqabc.pipe.channel import (
    AsyncBoundedQ,
    BoundedQ,
    bridge_async_to_thread,
    bridge_thread_to_async,
)
from qqabc.pipe.stage import IStage

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from types import TracebackType

    from typing_extensions import Self

T = TypeVar("T")
R = TypeVar("R")

__all__ = ["Pipeline", "pipe"]


def _counted_worker(
    fn: Any,
    in_q: BoundedQ[Any],
    out_q: BoundedQ[Any],
    remaining: list[int],
    lock: threading.Lock,
) -> None:
    """Worker 附帶計數：最後一個完成的 worker 發送 END_MSG。

    避免 dispatcher join 導致的 deadlock（worker 可能被 out_q.put 阻塞）。
    """
    for msg in in_q:
        result = fn(msg.data)
        out_q.put(result, order=msg.order)
    with lock:
        remaining[0] -= 1
        if remaining[0] == 0:
            out_q.end()


def _async_runner(
    fn: Any,
    concurrency: int,
    in_q: BoundedQ[Any],
    out_q: BoundedQ[Any],
) -> None:
    """在專屬 thread 中啟動 asyncio event loop 執行 async stage。

    接收一個 END_MSG 即結束（由 feeder / 上一階段送出）。
    """
    asyncio.run(_async_main(fn, concurrency, in_q, out_q))


async def _async_main(
    fn: Any,
    concurrency: int,
    in_q: BoundedQ[Any],
    out_q: BoundedQ[Any],
) -> None:
    """Async executor 核心邏輯。

    1. 透過 bridge 將 thread queue 轉為 async queue
    2. 用 ``asyncio.Semaphore`` 控制並行度
    3. 每個 item 以 ``asyncio.create_task`` 執行 ``fn``
    4. 結果透過 bridge 轉回 thread queue
    """
    sem = asyncio.Semaphore(concurrency)
    pending: set[asyncio.Task[None]] = set()

    # thread → async bridge（input 方向）
    async_in: AsyncBoundedQ[Any] = AsyncBoundedQ(maxsize=0)
    bridge_task = asyncio.create_task(bridge_thread_to_async(in_q, async_in))

    # async → thread bridge（output 方向）
    async_out: AsyncBoundedQ[Any] = AsyncBoundedQ(maxsize=0)

    async def _process(data: Any, order: int) -> None:
        try:
            result = await fn(data)
            await async_out.put(result, order=order)
        finally:
            sem.release()

    async def _consumer() -> None:
        try:
            async for msg in async_in:
                await sem.acquire()
                task = asyncio.create_task(_process(msg.data, msg.order))
                pending.add(task)
                task.add_done_callback(pending.discard)
            # 等待尚在處理的 tasks（return_exceptions=True 防止 deadlock）
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        finally:
            await async_out.end()

    consumer_task = asyncio.create_task(_consumer())

    # bridge output back to thread queue
    await bridge_async_to_thread(async_out, out_q)
    await bridge_task
    await consumer_task


class Pipeline(Generic[T, R]):
    """線性 pipeline，串接多個 Stage。

    自動建立 BoundedQ 連接各 stage、啟動 worker，
    提供 ``submit`` / ``results`` 介面。

    Args:
        stages: Stage 列表，可由 ``stage_a | stage_b`` 建構。
        backpressure: stage 之間 queue 的 maxsize，0 = 無界。
    """

    def __init__(
        self,
        stages: list[IStage[Any, Any]] | IStage[Any, Any],
        *,
        backpressure: int = 0,
    ) -> None:
        if isinstance(stages, IStage):
            stages = [stages]
        if not stages:
            msg = "Pipeline 至少需要一個 Stage"
            raise ValueError(msg)

        self._stages = stages
        self._backpressure = backpressure
        self._started = False
        self._closed = False
        self._order = 0

        # queues: len(stages) + 1 個 queue（入口 → [stage0] → [stage1] → ... → 出口）
        self._queues: list[BoundedQ[Any]] = [
            BoundedQ(kind="thread", maxsize=backpressure)
            for _ in range(len(stages) + 1)
        ]
        self._workers: list[threading.Thread] = []

    def _start(self) -> None:
        if self._started:
            return
        self._started = True

        for i, stage in enumerate(self._stages):
            in_q = self._queues[i]
            out_q = self._queues[i + 1]

            if stage.executor == "async":
                # async stage：一個 thread 跑 asyncio event loop，
                # 內部用 semaphore 控制 concurrency
                t = threading.Thread(
                    target=_async_runner,
                    args=(stage.fn, stage.concurrency, in_q, out_q),
                    daemon=True,
                )
                t.start()
                self._workers.append(t)
            else:
                # thread / process stage：啟動 N 個 counted worker，
                # 最後一個完成的 worker 自行發送 END_MSG 給 out_q，
                # 不需要 dispatcher join，避免 deadlock。
                worker_in_q = BoundedQ[Any](kind="thread", maxsize=self._backpressure)
                remaining = [stage.concurrency]
                lock = threading.Lock()

                for _ in range(stage.concurrency):
                    t = threading.Thread(
                        target=_counted_worker,
                        args=(stage.fn, worker_in_q, out_q, remaining, lock),
                        daemon=True,
                    )
                    t.start()
                    self._workers.append(t)

                # feeder: 從 in_q 讀取、fan-out 到 worker_in_q，
                # 收到 END_MSG 後送 N 個 END_MSG
                def _feeder(
                    _in: BoundedQ[Any],
                    _fan: BoundedQ[Any],
                    _n: int,
                ) -> None:
                    for msg in _in:
                        _fan.put(msg)
                    for _ in range(_n):
                        _fan.end()

                ft = threading.Thread(
                    target=_feeder,
                    args=(in_q, worker_in_q, stage.concurrency),
                    daemon=True,
                )
                ft.start()
                self._workers.append(ft)

    def submit(self, item: T) -> None:
        """提交一個 item 到 pipeline 入口。"""
        self._start()
        self._queues[0].put(item, order=self._order)
        self._order += 1

    def submit_many(self, items: Iterable[T]) -> None:
        """批次提交 items。"""
        for item in items:
            self.submit(item)

    def results(self) -> Iterator[R]:
        """迭代 pipeline 出口的結果（按完成順序）。

        呼叫此方法前需先呼叫 ``close()`` 或在 context manager 結束時自動 close。
        也可以先 close 再呼叫，或在 close 之前呼叫（此時會自動 close）。
        """
        if not self._closed:
            self.close()
        return (msg.data for msg in self._queues[-1])

    def run(self, items: Iterable[T]) -> Iterator[R]:
        """同時餵資料與取結果，避免背壓導致的 deadlock。

        在背景 thread 中 submit 所有 items 並 close，
        主 thread 可立即開始消費結果。

        Args:
            items: 輸入資料。

        Returns:
            結果 iterator。
        """
        self._start()

        def _feed() -> None:
            self.submit_many(items)
            self.close()

        feeder = threading.Thread(target=_feed, daemon=True)
        feeder.start()
        return (msg.data for msg in self._queues[-1])

    def close(self) -> None:
        """關閉 pipeline 入口，觸發 END_MSG 逐級傳播。"""
        if self._closed:
            return
        self._closed = True
        self._start()
        self._queues[0].end()

    def __enter__(self) -> Self:
        self._start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def __iter__(self) -> Iterator[R]:
        """讓 Pipeline 可被直接迭代（等同呼叫 ``results()``）。"""
        return self.results()

    def __del__(self) -> None:
        if getattr(self, "_started", False) and not self._closed:
            self.close()


@overload
def pipe(
    stages: list[IStage[Any, Any]] | IStage[Any, Any],
    *,
    input: Iterable[Any],
    backpressure: int = 0,
) -> Iterator[Any]: ...


@overload
def pipe(
    stages: list[IStage[Any, Any]] | IStage[Any, Any],
    *,
    input: None = None,
    backpressure: int = 0,
) -> Pipeline[Any, Any]: ...


def pipe(
    stages: list[IStage[Any, Any]] | IStage[Any, Any],
    *,
    input: Iterable[Any] | None = None,  # noqa: A002
    backpressure: int = 0,
) -> Iterator[Any] | Pipeline[Any, Any]:
    """一行建構並執行 pipeline。

    若提供 ``input``，自動 submit 全部資料並回傳 results iterator。
    若不提供 ``input``，回傳 ``Pipeline`` context manager 供手動操作。

    Args:
        stages: Stage 列表或單一 Stage。
        input: 輸入資料，若提供則自動 submit。
        backpressure: stage 之間 queue 的 maxsize，0 = 無界。

    Returns:
        若有 input：結果 iterator。
        若無 input：Pipeline 物件（可作為 context manager）。

    Examples:
        >>> from qqabc.pipe import pipe, Stage
        >>> list(pipe([Stage(fn=lambda x: x * 2)], input=[1, 2, 3]))
        [2, 4, 6]
    """
    p = Pipeline(stages, backpressure=backpressure)
    if input is not None:
        return p.run(input)
    return p
