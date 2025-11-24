from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import INFO
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    import datetime as dt
    from contextlib import AbstractContextManager
    from io import BytesIO


@dataclass
class LogData:
    task_id: int | None
    worker_id: int
    msg: str
    time: dt.datetime
    must: bool
    level: int = INFO


@dataclass
class InData:
    task_id: int
    url: str
    fpath: str | None = None


@dataclass
class OutData:
    task_id: int
    data: BytesIO


class IWorker(ABC):
    @abstractmethod
    def start(self, worker_id: int) -> AbstractContextManager[IWorker]:
        pass

    @abstractmethod
    def resolve(self, indata: InData) -> OutData:
        pass

    @property
    def input_timeout(self) -> float | None:
        return None


class IUrlGrammar(ABC):
    """URL語法規則介面

    定義用於解析檔案中URL的語法規則。
    實作此介面的類別應該提供方法來檢查檔案內容是否符合語法規則,
    並從檔案中解析出URL。
    """

    @abstractmethod
    def parse_url(self, fp: IO[bytes]) -> str | None:
        """從檔案物件中解析出URL。

        Args:
            fp: 檔案物件, 以二進位模式開啟。

        Returns:
            成功解析出URL時回傳URL字串, 否則回傳None。
        """


class DataDeletedError(KeyError):
    def __init__(self, task_id: int):
        super().__init__(f"Output data for task_id {task_id} has been deleted.")


class WorkersDiedOutError(RuntimeError):
    def __init__(self):
        super().__init__("All workers have stopped unexpectedly.")


class QQBugError(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)


class InvalidTaskError(ValueError):
    def __init__(self, task_id: int):
        super().__init__(f"Invalid task_id: {task_id}")


class IStorage(ABC):
    @abstractmethod
    def register(self, indata: InData):
        pass

    @abstractmethod
    def save(self, task_id: int, outdata: OutData):
        pass

    @abstractmethod
    def load(self, task_id: int) -> OutData:
        """Load the output data associated with the given task ID.

        Raises ValueError if the data has been deleted.
        """

    @abstractmethod
    def delete(self, task_id: int) -> None:
        pass

    @abstractmethod
    def delete_all(self) -> None:
        pass

    @abstractmethod
    def has(self, task_id: int) -> bool:
        pass
