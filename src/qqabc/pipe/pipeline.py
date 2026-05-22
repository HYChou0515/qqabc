"""Pipeline — 線性 pipeline 編排器。

將多個 Stage 串接成 ``A → B → C`` 流水線，自動建立 BoundedQ、
啟動 worker、處理 END_MSG 傳播與背壓。

支援 stage 級別的錯誤處理：

- **retry** — 失敗 item 重新排入該 stage 的 worker queue
- **on_error** — 重試耗盡後的策略（raise / skip / dead_letter）
- **error_handler** — 自訂錯誤處理（可修改 item 或 skip）
- **worker_chance** — per-worker 連續失敗上限（health budget）

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
import logging
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

from qqabc.pipe.channel import (
    AsyncBoundedQ,
    BoundedQ,
    bridge_async_to_thread,
    bridge_thread_to_async,
)
from qqabc.pipe.stage import IStage

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from types import TracebackType

    from typing_extensions import Self

T = TypeVar("T")
R = TypeVar("R")

__all__ = ["Pipeline", "pipe"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers for retry-enabled stages
# ---------------------------------------------------------------------------


@dataclass
class _RetryEnvelope:
    """在 stage 內部攜帶重試元資料的包裝。

    Attributes:
        data: 當前 item 資料（可能被 error_handler 修改）。
        retries_left: 剩餘重試次數。
        original_data: 原始 item（供 dead_letter 報告用）。
    """

    data: Any
    retries_left: int
    original_data: Any = field(repr=False)


class _StageState:
    """Drain-aware 終止控制器。

    管理 re-enqueue 場景下的 END_MSG 發送時機：
    只有當 feeder 完成且所有 in-flight item 都到達終態（成功 / exhausted）時，
    才發送 N 個 END_MSG 給 worker，讓 stage 正確結束。
    """

    def __init__(self, num_workers: int) -> None:
        self._lock = threading.Lock()
        self._in_flight = 0
        self._feeder_done = False
        self._workers_alive = num_workers
        self._end_sent = False
        self.stop_event = threading.Event()

    def item_sent(self) -> None:
        """Feeder 送出一個 item 到 worker_in_q 時呼叫。"""
        with self._lock:
            self._in_flight += 1

    def item_completed(self, worker_in_q: BoundedQ[Any]) -> None:
        """Item 到達終態（成功或 exhausted）時呼叫。"""
        with self._lock:
            self._in_flight -= 1
            n = self._maybe_end_count()
        for _ in range(n):
            worker_in_q.end()

    def mark_feeder_done(self, worker_in_q: BoundedQ[Any]) -> None:
        """Feeder 讀完 in_q 所有資料後呼叫。"""
        with self._lock:
            self._feeder_done = True
            n = self._maybe_end_count()
        for _ in range(n):
            worker_in_q.end()

    def force_shutdown(self, worker_in_q: BoundedQ[Any]) -> None:
        """強制終止所有 worker（用於 ``on_error='raise'``）。"""
        self.stop_event.set()
        with self._lock:
            if self._end_sent:
                return
            self._end_sent = True
            n = self._workers_alive
        for _ in range(n):
            worker_in_q.end()

    def worker_exiting(self, out_q: BoundedQ[Any]) -> None:
        """Worker 退出時呼叫。最後一個退出的 worker 發送 END_MSG 到 out_q。"""
        with self._lock:
            self._workers_alive -= 1
            should_end = self._workers_alive == 0
        if should_end:
            out_q.end()

    def _maybe_end_count(self) -> int:
        """計算需要發送的 END_MSG 數量。需持有 ``_lock``。"""
        if self._feeder_done and self._in_flight == 0 and not self._end_sent:
            self._end_sent = True
            return self._workers_alive
        return 0


# ---------------------------------------------------------------------------
# Worker functions
# ---------------------------------------------------------------------------


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


def _worker_health_exhausted(
    consecutive_failures: int, worker_chance: int, stage_name: str
) -> bool:
    """檢查 per-worker health budget 是否耗盡。"""
    if worker_chance > 0 and consecutive_failures >= worker_chance:
        logger.warning("Worker health exhausted for stage %r", stage_name)
        return True
    return False


def _apply_on_error(
    exc: Exception,
    envelope: _RetryEnvelope,
    msg_order: int,
    in_q: BoundedQ[Any],
    state: _StageState,
    *,
    stage_name: str,
    on_error: str,
    dead_letters: list[tuple[str, Exception, Any]],
    dl_lock: threading.Lock,
    raise_error: list[BaseException | None],
) -> bool:
    """Retry 耗盡 + handler 沒有修復值時, 套用 on_error 策略.

    Returns:
        True 表示 worker 應該 break (raise 路徑); False 表示繼續下個 msg。
    """
    del msg_order  # 目前 on_error 路徑用不到 order
    if on_error == "skip":
        logger.warning(
            "Stage %r: item skipped after retries exhausted: %s", stage_name, exc
        )
        state.item_completed(in_q)
        return False
    if on_error == "dead_letter":
        with dl_lock:
            dead_letters.append((stage_name, exc, envelope.original_data))
        state.item_completed(in_q)
        return False
    # raise
    raise_error[0] = exc
    state.item_completed(in_q)
    state.force_shutdown(in_q)
    return True


def _retry_worker(  # noqa: PLR0912
    fn: Any,
    in_q: BoundedQ[Any],
    out_q: BoundedQ[Any],
    state: _StageState,
    *,
    stage_name: str,
    on_error: str,
    error_handler: Callable[[Exception, Any], Any] | None,
    worker_chance: int,
    dead_letters: list[tuple[str, Exception, Any]],
    dl_lock: threading.Lock,
    raise_error: list[BaseException | None],
) -> None:
    """帶重試邏輯的 worker。

    流程：fn 處理 item → 成功則輸出 / 失敗則檢查 error_handler →
    有 retry 則 re-enqueue → retry 耗盡則走 on_error 策略。
    """
    consecutive_failures = 0
    try:
        for msg in in_q:
            if state.stop_event.is_set():
                break

            envelope: _RetryEnvelope = msg.data

            try:
                result = fn(envelope.data)
            except Exception as exc:
                consecutive_failures += 1

                # 1) error_handler: None => skip; non-None => "已修好的值"
                handler_repaired, repaired_value = False, None
                if error_handler is not None:
                    handler_result = error_handler(exc, envelope.data)
                    if handler_result is None:
                        state.item_completed(in_q)
                        if _worker_health_exhausted(
                            consecutive_failures, worker_chance, stage_name
                        ):
                            break
                        continue
                    handler_repaired, repaired_value = True, handler_result

                # 2) 有重試機會 → re-enqueue
                if envelope.retries_left > 0:
                    next_data = repaired_value if handler_repaired else envelope.data
                    in_q.put(
                        _RetryEnvelope(
                            data=next_data,
                            retries_left=envelope.retries_left - 1,
                            original_data=envelope.original_data,
                        ),
                        order=msg.order,
                    )
                    if _worker_health_exhausted(
                        consecutive_failures, worker_chance, stage_name
                    ):
                        break
                    continue

                # 3) 重試耗盡, handler 修好了 → 直接 emit
                if handler_repaired:
                    out_q.put(repaired_value, order=msg.order)
                    state.item_completed(in_q)
                    consecutive_failures = 0
                    continue

                # 4) 否則走 on_error 策略
                if _apply_on_error(
                    exc,
                    envelope,
                    msg.order,
                    in_q,
                    state,
                    stage_name=stage_name,
                    on_error=on_error,
                    dead_letters=dead_letters,
                    dl_lock=dl_lock,
                    raise_error=raise_error,
                ):
                    break

                # 5) worker health check
                if _worker_health_exhausted(
                    consecutive_failures, worker_chance, stage_name
                ):
                    break
            else:
                # 成功
                out_q.put(result, order=msg.order)
                state.item_completed(in_q)
                consecutive_failures = 0
    finally:
        state.worker_exiting(out_q)


def _retry_feeder(
    in_q: BoundedQ[Any],
    fan_q: BoundedQ[Any],
    state: _StageState,
    retry_count: int,
) -> None:
    """從 in_q 讀取、包裝成 ``_RetryEnvelope``、fan-out 到 fan_q。"""
    for msg in in_q:
        envelope = _RetryEnvelope(
            data=msg.data,
            retries_left=retry_count,
            original_data=msg.data,
        )
        fan_q.put(envelope, order=msg.order)
        state.item_sent()
    state.mark_feeder_done(fan_q)


# ---------------------------------------------------------------------------
# Async runner
# ---------------------------------------------------------------------------


def _async_retry_runner(
    fn: Any,
    concurrency: int,
    in_q: BoundedQ[Any],
    out_q: BoundedQ[Any],
    *,
    retry: int,
    on_error: str,
    error_handler: Callable[[Exception, Any], Any] | None,
    stage_name: str,
    dead_letters: list[tuple[str, Exception, Any]],
    dl_lock: threading.Lock,
    raise_error: list[BaseException | None],
) -> None:
    """在專屬 thread 中啟動 asyncio event loop 執行帶錯誤處理的 async stage。"""
    asyncio.run(
        _async_retry_main(
            fn,
            concurrency,
            in_q,
            out_q,
            retry=retry,
            on_error=on_error,
            error_handler=error_handler,
            stage_name=stage_name,
            dead_letters=dead_letters,
            dl_lock=dl_lock,
            raise_error=raise_error,
        ),
    )


def _apply_async_on_error(
    exc: Exception,
    original_data: Any,
    *,
    stage_name: str,
    on_error: str,
    dead_letters: list[tuple[str, Exception, Any]],
    dl_lock: threading.Lock,
    raise_error: list[BaseException | None],
) -> None:
    """Async stage 在 retry 耗盡且無 handler 修復值時的 on_error 分派。"""
    if on_error == "skip":
        logger.warning("Stage %r: async item skipped: %s", stage_name, exc)
    elif on_error == "dead_letter":
        with dl_lock:
            dead_letters.append((stage_name, exc, original_data))
    else:  # raise
        raise_error[0] = exc


async def _async_retry_main(
    fn: Any,
    concurrency: int,
    in_q: BoundedQ[Any],
    out_q: BoundedQ[Any],
    *,
    retry: int,
    on_error: str,
    error_handler: Callable[[Exception, Any], Any] | None,
    stage_name: str,
    dead_letters: list[tuple[str, Exception, Any]],
    dl_lock: threading.Lock,
    raise_error: list[BaseException | None],
) -> None:
    """Async executor 核心邏輯，支援 retry / on_error / error_handler。

    Async stage 使用 local retry（在同一 coroutine 內重試），
    因為所有 task 共享同一 event loop，re-enqueue 在架構上不可行。
    """
    sem = asyncio.Semaphore(concurrency)
    pending: set[asyncio.Task[None]] = set()

    async_in: AsyncBoundedQ[Any] = AsyncBoundedQ(maxsize=0)
    bridge_task = asyncio.create_task(bridge_thread_to_async(in_q, async_in))

    async_out: AsyncBoundedQ[Any] = AsyncBoundedQ(maxsize=0)

    async def _process(data: Any, order: int) -> None:
        retries_left = retry
        current = data
        try:
            while True:
                try:
                    result = await fn(current)
                except Exception as exc:  # noqa: PERF203
                    # error_handler: None => skip; non-None => "已修好的值"
                    handler_repaired, repaired_value = False, None
                    if error_handler is not None:
                        handler_result = error_handler(exc, current)
                        if handler_result is None:
                            return  # skip
                        handler_repaired, repaired_value = True, handler_result

                    if retries_left > 0:
                        retries_left -= 1
                        current = repaired_value if handler_repaired else current
                        continue

                    if handler_repaired:
                        await async_out.put(repaired_value, order=order)
                        return

                    _apply_async_on_error(
                        exc,
                        data,
                        stage_name=stage_name,
                        on_error=on_error,
                        dead_letters=dead_letters,
                        dl_lock=dl_lock,
                        raise_error=raise_error,
                    )
                    return
                else:
                    await async_out.put(result, order=order)
                    return
        finally:
            sem.release()

    async def _consumer() -> None:
        try:
            async for msg in async_in:
                await sem.acquire()
                task = asyncio.create_task(_process(msg.data, msg.order))
                pending.add(task)
                task.add_done_callback(pending.discard)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        finally:
            await async_out.end()

    consumer_task = asyncio.create_task(_consumer())

    await bridge_async_to_thread(async_out, out_q)
    await bridge_task
    await consumer_task


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline(Generic[T, R]):
    """線性 pipeline，串接多個 Stage。

    自動建立 BoundedQ 連接各 stage、啟動 worker，
    提供 ``submit`` / ``results`` 介面。

    支援 stage 級別的錯誤處理：失敗 item 可重試、跳過、或收集到
    ``dead_letters`` 供事後檢查。

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
        self._drained = False
        self._order = 0

        # queues: len(stages) + 1 個 queue（入口 → [stage0] → [stage1] → ... → 出口）
        self._queues: list[BoundedQ[Any]] = [
            BoundedQ(kind="thread", maxsize=backpressure)
            for _ in range(len(stages) + 1)
        ]
        self._workers: list[threading.Thread] = []

        # 錯誤處理共用狀態
        self._dead_letters: list[tuple[str, Exception, Any]] = []
        self._dead_letters_lock = threading.Lock()
        self._raise_error: list[BaseException | None] = [None]

    @property
    def dead_letters(self) -> list[tuple[str, Exception, Any]]:
        """取得所有 dead letter 項目。

        Returns:
            ``(stage_name, exception, original_item)`` 的 list。
        """
        with self._dead_letters_lock:
            return list(self._dead_letters)

    def _start(self) -> None:
        if self._started:
            return
        self._started = True

        for i, stage in enumerate(self._stages):
            in_q = self._queues[i]
            out_q = self._queues[i + 1]

            if stage.executor == "async":
                # 所有 async stage 統一走錯誤處理路徑，預設 on_error='raise'
                # 與 thread / process stage 行為一致, 避免例外被靜默吞掉。
                t = threading.Thread(
                    target=_async_retry_runner,
                    args=(stage.fn, stage.concurrency, in_q, out_q),
                    kwargs={
                        "retry": stage.retry,
                        "on_error": stage.on_error,
                        "error_handler": stage.error_handler,
                        "stage_name": stage.name,
                        "dead_letters": self._dead_letters,
                        "dl_lock": self._dead_letters_lock,
                        "raise_error": self._raise_error,
                    },
                    daemon=True,
                )
                t.start()
                self._workers.append(t)
            else:
                # thread / process stage：統一使用 _retry_worker，
                # 即使無錯誤處理參數也能正確處理例外（不會像 _counted_worker hang）。
                worker_in_q = BoundedQ[Any](
                    kind="thread",
                    maxsize=self._backpressure,
                )
                state = _StageState(stage.concurrency)

                for _ in range(stage.concurrency):
                    t = threading.Thread(
                        target=_retry_worker,
                        args=(stage.fn, worker_in_q, out_q, state),
                        kwargs={
                            "stage_name": stage.name,
                            "on_error": stage.on_error,
                            "error_handler": stage.error_handler,
                            "worker_chance": stage.worker_chance,
                            "dead_letters": self._dead_letters,
                            "dl_lock": self._dead_letters_lock,
                            "raise_error": self._raise_error,
                        },
                        daemon=True,
                    )
                    t.start()
                    self._workers.append(t)

                ft = threading.Thread(
                    target=_retry_feeder,
                    args=(in_q, worker_in_q, state, stage.retry),
                    daemon=True,
                )
                ft.start()
                self._workers.append(ft)

    def submit(self, item: T) -> None:
        """提交一個 item 到 pipeline 入口。

        Raises:
            RuntimeError: 若 pipeline 已 closed (END_MSG 已送出)。
                在 close 之後 put 的 item 會被第一個 stage 的 iterator
                越過 END_MSG 之後丟棄，造成靜默資料遺失，因此直接 raise。
        """
        if self._closed:
            msg = "Pipeline is closed; cannot submit new items."
            raise RuntimeError(msg)
        self._start()
        self._queues[0].put(item, order=self._order)
        self._order += 1

    def submit_many(self, items: Iterable[T]) -> None:
        """批次提交 items。"""
        for item in items:
            self.submit(item)

    def _iter_results(self) -> Iterator[R]:
        """產生結果，結束後檢查是否有需要向上傳播的例外。"""
        try:
            yield from (msg.data for msg in self._queues[-1])
        finally:
            self._drained = True
        if self._raise_error[0] is not None:
            raise self._raise_error[0]

    def results(self) -> Iterator[R]:
        """迭代 pipeline 出口的結果（按完成順序）。

        呼叫此方法前需先呼叫 ``close()`` 或在 context manager 結束時自動 close。
        也可以先 close 再呼叫，或在 close 之前呼叫（此時會自動 close）。

        Raises:
            RuntimeError: 若 ``results()`` 已被消費過。 Pipeline 是 single-shot,
                第二次呼叫過去會 hang (END_MSG 已被取走), 現在改為 raise。
        """
        if self._drained:
            msg = "Pipeline results have already been consumed."
            raise RuntimeError(msg)
        if not self._closed:
            self.close()
        return self._iter_results()

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
        return self._iter_results()

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
