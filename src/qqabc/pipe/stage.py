"""Stage 抽象與 Executor 模型。

定義 Pipeline 中處理階段的核心抽象，支援 thread、process 與 async 三種執行模式。
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

T = TypeVar("T")
R = TypeVar("R")

ExecutorType = Literal["thread", "process", "async"]

_DEFAULT_CONCURRENCY = 4

__all__ = ["ExecutorType", "IStage", "Stage"]


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

    def __or__(
        self, other: IStage[Any, Any] | list[IStage[Any, Any]]
    ) -> list[IStage[Any, Any]]:
        """串接 stage，回傳 stage 列表供後續 Pipeline builder 使用。"""
        if isinstance(other, list):
            return [self, *other]
        return [self, other]

    def __ror__(
        self, other: IStage[Any, Any] | list[IStage[Any, Any]]
    ) -> list[IStage[Any, Any]]:
        """當左側為 list 時的反向串接。"""
        if isinstance(other, list):
            return [*other, self]
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
    """

    def __init__(
        self,
        fn: Callable[[T], R] | Callable[[T], Awaitable[R]],
        *,
        executor: ExecutorType | None = None,
        concurrency: int = _DEFAULT_CONCURRENCY,
        name: str = "",
    ) -> None:
        self._fn = fn
        self._executor: ExecutorType = (
            executor
            if executor is not None
            else ("async" if inspect.iscoroutinefunction(fn) else "thread")
        )
        self._concurrency = concurrency
        self._name = name or getattr(fn, "__name__", "")

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

    def __repr__(self) -> str:
        return (
            f"Stage(name={self._name!r}, executor={self._executor!r}, "
            f"concurrency={self._concurrency})"
        )
