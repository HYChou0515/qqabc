"""Stage 抽象與 Executor 模型。

定義 Pipeline 中處理階段的核心抽象，支援 thread、process 與 async 三種執行模式。
支援 per-item 重試、錯誤策略（raise / skip / dead_letter）、自訂 error_handler，
以及 per-worker health budget（worker_chance）。
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

T = TypeVar("T")
R = TypeVar("R")

ExecutorType = Literal["thread", "process", "async"]
OnErrorType = Literal["raise", "skip", "dead_letter"]

_DEFAULT_CONCURRENCY = 4

__all__ = ["ExecutorType", "IStage", "OnErrorType", "Stage"]


class IStage(ABC, Generic[T, R]):
    """Stage 的抽象介面，供進階使用者自訂。

    類似現有的 IWorker，提供 Stage 的最小抽象。
    進階使用者可實作此介面來自訂 Stage 行為。
    """

    @property
    @abstractmethod
    def fn(self) -> Callable[[T], R] | Callable[[T], Awaitable[R]]:
        """處理函式，可以是同步或非同步 callable。"""

    @property
    @abstractmethod
    def executor(self) -> ExecutorType:
        """執行方式：thread、process 或 async。"""

    @property
    @abstractmethod
    def concurrency(self) -> int:
        """並行 worker 數量。"""

    @property
    @abstractmethod
    def name(self) -> str:
        """此 stage 的名稱，用於監控與除錯。"""

    # --- 錯誤處理相關 properties（concrete 預設值，不強制子類實作） ---

    @property
    def retry(self) -> int:
        """單一 item 的重試次數，預設 0（不重試）。"""
        return 0

    @property
    def on_error(self) -> OnErrorType:
        """重試耗盡後的錯誤策略，預設 ``"raise"``。"""
        return "raise"

    @property
    def error_handler(self) -> Callable[[Exception, Any], Any] | None:
        """自訂錯誤處理函式，預設 None。"""
        return None

    @property
    def worker_chance(self) -> int:
        """Per-worker 連續失敗上限，0 = 不啟用 health check。"""
        return 0

    def __or__(
        self, other: IStage[Any, Any] | list[IStage[Any, Any]]
    ) -> list[IStage[Any, Any]]:
        """串接 stage，回傳 stage 列表供後續 Pipeline builder 使用。"""
        if isinstance(other, list):
            tail = cast("list[IStage[Any, Any]]", other)
            return [self, *tail]
        return [self, other]

    def __ror__(
        self, other: IStage[Any, Any] | list[IStage[Any, Any]]
    ) -> list[IStage[Any, Any]]:
        """當左側為 list 時的反向串接。"""
        if isinstance(other, list):
            head = cast("list[IStage[Any, Any]]", other)
            return [*head, self]
        return [other, self]


class Stage(IStage[T, R]):
    """Pipeline 中的一個處理階段。

    一個可配置執行方式的處理單元，支援 thread、process 與 async 三種模式。
    若 fn 為 coroutine function 且未明確指定 executor，將自動使用 ``"async"``。

    Args:
        fn: 處理函式，可以是同步函式或 async 函式。
        executor: 執行方式。若未指定，coroutine function 預設為 ``"async"``，
            否則為 ``"thread"``。
        concurrency: 並行 worker 數量，預設為 4。
        name: 此 stage 的名稱，若未提供則使用 ``fn.__name__``。
        retry: 單一 item 的重試次數，預設 0（不重試）。
        on_error: 重試耗盡後的錯誤策略。``"raise"``（預設）、``"skip"``、``"dead_letter"``。
        error_handler: 自訂錯誤處理函式 ``(exception, item) -> new_item | None``。
            回傳 non-None 時以新 item 重試；回傳 None 時視為 skip。
        worker_chance: Per-worker 連續失敗上限。0 = 不啟用。
            當 worker 連續失敗達到此值時，該 worker 自動終止。
    """

    def __init__(
        self,
        fn: Callable[[T], R] | Callable[[T], Awaitable[R]],
        *,
        executor: ExecutorType | None = None,
        concurrency: int = _DEFAULT_CONCURRENCY,
        name: str = "",
        retry: int = 0,
        on_error: OnErrorType = "raise",
        error_handler: Callable[[Exception, Any], Any] | None = None,
        worker_chance: int = 0,
    ) -> None:
        if concurrency < 1:
            msg = f"concurrency must be >= 1, got {concurrency}"
            raise ValueError(msg)
        self._fn = fn
        self._executor: ExecutorType = (
            executor
            if executor is not None
            else ("async" if inspect.iscoroutinefunction(fn) else "thread")
        )
        self._concurrency = concurrency
        self._name = name or getattr(fn, "__name__", "")
        self._retry = retry
        self._on_error: OnErrorType = on_error
        self._error_handler = error_handler
        self._worker_chance = worker_chance

    @property
    def fn(self) -> Callable[[T], R] | Callable[[T], Awaitable[R]]:
        """處理函式。"""
        return self._fn

    @property
    def executor(self) -> ExecutorType:
        """執行方式。"""
        return self._executor

    @property
    def concurrency(self) -> int:
        """並行 worker 數量。"""
        return self._concurrency

    @property
    def name(self) -> str:
        """此 stage 的名稱。"""
        return self._name

    @property
    def retry(self) -> int:  # type: ignore[override]
        """單一 item 的重試次數。"""
        return self._retry

    @property
    def on_error(self) -> OnErrorType:  # type: ignore[override]
        """重試耗盡後的錯誤策略。"""
        return self._on_error

    @property
    def error_handler(self) -> Callable[[Exception, Any], Any] | None:  # type: ignore[override]
        """自訂錯誤處理函式。"""
        return self._error_handler

    @property
    def worker_chance(self) -> int:  # type: ignore[override]
        """Per-worker 連續失敗上限。"""
        return self._worker_chance

    def __repr__(self) -> str:
        parts = [
            f"Stage(name={self._name!r}",
            f"executor={self._executor!r}",
            f"concurrency={self._concurrency}",
        ]
        if self._retry:
            parts.append(f"retry={self._retry}")
        if self._on_error != "raise":
            parts.append(f"on_error={self._on_error!r}")
        if self._worker_chance:
            parts.append(f"worker_chance={self._worker_chance}")
        return ", ".join(parts) + ")"
