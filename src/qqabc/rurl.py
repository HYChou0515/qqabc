from __future__ import annotations

import datetime as dt
import io
import shutil
import tempfile
import traceback
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from functools import partial
from io import BytesIO
from logging import ERROR, INFO, getLogger
from queue import Empty
from typing import IO, TYPE_CHECKING, Generator, Literal, overload

from typing_extensions import Self

import qqabc.qq

if TYPE_CHECKING:
    from collections.abc import Callable

logger = getLogger(__name__)


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


class Storage(IStorage):
    def __init__(self, cached_size: int):
        self.cached_size = cached_size
        self.indata_storage: dict[int, InData] = {}
        self.outdata_storage: dict[int, OutData] = {}
        self.size = 0
        self.saved: set[int] = set()

    def register(self, indata: InData):
        self.indata_storage[indata.task_id] = indata

    def save(self, task_id: int, outdata: OutData):
        if task_id in self.saved:
            raise ValueError(
                f"Output data for task_id {task_id} has already been saved."
            )
        this_size = outdata.data.getbuffer().nbytes
        while self.size + this_size > self.cached_size and self.outdata_storage:
            oldest_task_id = min(self.outdata_storage)
            self.delete(oldest_task_id)
        self.saved.add(task_id)
        if self.size + this_size > self.cached_size:
            indata = self.indata_storage.pop(task_id)
            self._save_to_disk(indata, outdata)
        else:
            self.size += this_size
            self.outdata_storage[task_id] = outdata

    def load(self, task_id: int) -> OutData:
        if task_id not in self.outdata_storage and task_id in self.saved:
            raise DataDeletedError(task_id)
        return self.outdata_storage[task_id]

    def _save_to_disk(self, indata: InData, outdata: OutData):
        if indata.fpath is not None:
            with tempfile.NamedTemporaryFile(delete=False) as tmpf:
                tmpf.write(outdata.data.getbuffer())
            shutil.move(tmpf.name, indata.fpath)
            self.size -= outdata.data.getbuffer().nbytes

    def delete(self, task_id: int) -> None:
        outdata = self.outdata_storage.pop(task_id)
        indata = self.indata_storage.pop(task_id)
        self._save_to_disk(indata, outdata)

    def delete_all(self) -> None:
        for task_id in list(self.outdata_storage.keys()):
            self.delete(task_id)

    def has(self, task_id: int) -> bool:
        return task_id in self.saved


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


class DefaultWorker(IWorker):
    @contextmanager
    def start(self, worker_id: int):
        self.worker_id = worker_id
        import httpx  # noqa: PLC0415

        with httpx.Client(follow_redirects=True) as client:
            self.client = client
            yield self

    def resolve(self, indata: InData) -> OutData:
        resp = self.client.get(indata.url)
        resp.raise_for_status()
        b = BytesIO(resp.content)
        return OutData(task_id=indata.task_id, data=b)


def _make_log(
    msg: str,
    *,
    must: bool = False,
    worker_id: int,
    task_id: int | None,
    level: int = INFO,
) -> LogData:
    return LogData(
        task_id=task_id,
        worker_id=worker_id,
        msg=msg,
        time=_getnow(),
        must=must,
        level=level,
    )


def _worker_download(  # noqa: PLR0913
    input_q: qqabc.qq.Q[InData],
    output_q: qqabc.qq.Q[int],
    log_q: qqabc.qq.Q[LogData],
    worker: IWorker,
    worker_id: int,
    storage: IStorage,
):
    log_func = partial(_make_log, worker_id=worker_id, task_id=None)
    try:
        with worker.start(worker_id):
            log_q.put(log_func("Worker started", must=True))
            for msg in input_q.iter(worker.input_timeout):
                ind = msg.data
                log_task_func = partial(
                    _make_log, worker_id=worker_id, task_id=ind.task_id
                )
                try:
                    log_q.put(log_task_func("Start resolving"))
                    outd = worker.resolve(ind)
                    storage.save(ind.task_id, outd)
                    output_q.put(ind.task_id)
                    log_q.put(log_task_func("Finished"))
                except Exception as e:
                    log_q.put(log_task_func(f"Error: {e}", must=True, level=ERROR))
                    input_q.put(msg)
                    raise
    except Empty:
        log_q.put(
            log_func(
                f"No new jobs before timeout={worker.input_timeout} reached",
                must=True,
                level=ERROR,
            )
        )
    except Exception:
        log_q.put(log_func(traceback.format_exc(), must=True, level=ERROR))


def _getnow():
    return dt.datetime.now(tz=dt.timezone.utc)


def _worker_print(log_q: qqabc.qq.Q[LogData], min_interval: float = 0.1):
    last_print = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    for msg in log_q:
        log = msg.data
        timestamp = log.time.strftime("%Y-%m-%d %H:%M:%S")
        if log.task_id is not None:
            prefix = f"[Worker {log.worker_id} | Task {log.task_id} | {timestamp}]"
        else:
            prefix = f"[Worker {log.worker_id} | {timestamp}]"
        if log.must or (_getnow() - last_print).total_seconds() >= min_interval:
            logger.log(log.level, "{prefix} - {log.msg}", prefix=prefix, log=log)
            last_print = _getnow()


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


class BasicUrlGrammar(IUrlGrammar):
    """基本的URL語法規則
    提供基本的URL解析功能。

    提供兩個好用的util方法:
    - sanity_check: 用於快速檢查檔案內容是否可能包含URL。
    - parse_url: 用於從檔案中解析出URL。

    一般來說, 使用者可以繼承此類別並覆寫main_rule方法來實作自訂的URL解析規則。
    """

    def __init__(self):
        self.url_min = 5
        self.url_max = 512

    def sanity_check(self, fp: IO[bytes]) -> bool:
        """快速檢查檔案內容是否可能包含URL。

        我們相信一個有效的URL應該符合以下條件:
        1. 檔案大小介於url_min與url_max之間。
        2. 檔案前10個位元組中包含"://"
        """
        sz = fp.seek(0, 2)
        if sz < self.url_min or sz > self.url_max:
            return False
        fp.seek(0)
        if b"://" not in fp.read(10):
            return False
        fp.seek(0)
        return True

    def main_rule(self, content: str) -> str | None:
        """從字串中解析出URL的主要規則。
        預設實作為檢查字串是否以"http://"或"https://"開頭。
        """
        if content.startswith(("http://", "https://")):
            return content.strip()
        return None

    def parse_url(self, fp: IO[bytes]) -> str | None:
        """從檔案物件中解析出URL。"""
        if not self.sanity_check(fp):
            return None
        try:
            fp.seek(0)
            content = fp.read(self.url_max).decode("utf-8")
            url = self.main_rule(content)
            if url is not None:
                return url
        except UnicodeDecodeError:
            return None


class IResolver(ABC):
    """URL解析器介面

    定義用於解析URL的解析器介面。
    實作此介面的類別應該提供方法來新增URL解析任務,
    等待任務完成, 以及打開可能包含URL的檔案。
    """

    @abstractmethod
    def __enter__(self) -> Self:
        pass

    @abstractmethod
    def add(self, url: str, fname: str | None = None) -> int:
        """Add a URL to be resolved and return its task ID."""

    @abstractmethod
    def wait(self, task_id: int) -> OutData:
        """Wait for the completion of a task and return its output data."""

    @abstractmethod
    def iter_and_close(self) -> Generator[OutData]:
        """Iterator that yields completed tasks as they finish, then closes the resolver."""

    @abstractmethod
    def completed(self, timeout: float = 0) -> Generator[OutData]:
        """Generator that yields completed tasks as they finish.

        The generator will yield completed tasks
        until no more tasks are available within the timeout period.
        """

    @overload
    def open(
        self, filepath: str, mode: Literal["rb"]
    ) -> AbstractContextManager[IO[bytes]]: ...
    @overload
    def open(
        self, filepath: str, mode: Literal["r"]
    ) -> AbstractContextManager[IO[str]]: ...

    @abstractmethod
    def open(
        self, filepath: str, mode: Literal["r", "rb"] = "r"
    ) -> AbstractContextManager[IO]:
        """Open a file that may contain a URL, resolving it if necessary.

        If the file contains a URL, it will be resolved and the resulting data
        will be returned as a file-like object. If the file does not contain a URL,
        the original file will be opened and returned.

        Args:
            filepath: The path to the file to open.
            mode: The mode in which to open the file (e.g., 'r', 'rb').
        """

    @abstractmethod
    def add_wait(self, url, fname: str | None = None) -> OutData:
        """Adds a URL to be resolved and waits for its completion."""


class Resolver(IResolver):
    def __init__(
        self,
        num_workers: int,
        *,
        storage: IStorage,
        worker_factory: Callable[[], IWorker],
        grammars: list[IUrlGrammar],
    ):
        input_q = qqabc.qq.Q[InData]("thread")
        output_q = qqabc.qq.Q[int]("thread")
        log_q = qqabc.qq.Q[LogData]("thread")
        self.input_q = input_q
        self.output_q = output_q
        self.log_q = log_q
        self.storage = storage
        self.grammars = grammars
        self.workers = [
            qqabc.qq.run_thread(
                _worker_download,
                input_q,
                output_q,
                self.log_q,
                worker_factory(),
                w,
                self.storage,
            )
            for w in range(num_workers)
        ]
        self.printer = qqabc.qq.run_thread(_worker_print, self.log_q)
        self.task_cnt = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        self.storage.delete_all()

    def _get_task_id(self):
        self.task_cnt += 1
        return self.task_cnt

    @contextmanager
    def open(self, filepath: str, mode: Literal["r", "rb"] = "r") -> Generator[IO]:
        outd = None
        with open(filepath, "rb") as f:
            try:
                for grammar in self.grammars:
                    f.seek(0)
                    url = grammar.parse_url(f)
                    if url is not None:
                        outd = self.add_wait(url, fname=filepath)
                        break
            except DataDeletedError:
                pass
        if outd is None:
            with open(filepath, mode) as f:
                yield f
        else:
            outd.data.seek(0)
            if "b" in mode:
                yield outd.data
            else:
                yield io.StringIO(outd.data.read().decode("utf-8"))

    def add_wait(self, url, fname: str | None = None):
        task_id = self.add(url, fname=fname)
        return self.wait(task_id)

    def add(self, url: str, fname: str | None = None) -> int:
        task_id = self._get_task_id()
        indata = InData(task_id=task_id, url=url, fpath=fname)
        self.storage.register(indata)
        self.input_q.put(indata)
        return task_id

    def _get_result(self, task_id: int):
        return self.storage.load(task_id)

    def iter_and_close(self):
        self.close()
        for msg in self.output_q:
            task_id = msg.data
            yield self._get_result(task_id)

    def close(self):
        self.input_q.stop(self.workers)
        self.output_q.end()
        self.log_q.stop(self.printer)

    def completed(self, timeout: float = 0):
        for msg in self._iter(timeout=timeout, empty_ok=True):
            task_id = msg.data
            yield self._get_result(task_id)

    def _iter(self, timeout: float = 0.05, *, empty_ok: bool):
        """Generator that yields completed tasks as they finish.

        If timeout is set to a float value, the generator will yield
        completed tasks until no more tasks are available within the timeout period.
        """
        while True:
            try:
                yield from self.output_q.iter(timeout=timeout)
            except Empty:  # noqa: PERF203
                if all(not worker.is_alive() for worker in self.workers):
                    raise WorkersDiedOutError from None
                if empty_ok:
                    break

    def wait(self, task_id: int):
        if not (0 < task_id <= self.task_cnt):
            raise InvalidTaskError(task_id)
        if self.storage.has(task_id):
            return self._get_result(task_id)
        for msg in self._iter(timeout=0.05, empty_ok=False):
            completed_task_id = msg.data
            if completed_task_id == task_id:
                return self._get_result(task_id)
        raise QQBugError("Unreachable code reached.")


def resolve(
    *,
    num_workers: int = 4,
    cache_size: int = 1 << 20,
    worker: type[IWorker] | Callable[[], IWorker] | None = None,
    grammars: list[IUrlGrammar] | None = None,
) -> IResolver:
    """建立一個Resolver物件來下載URL資源。

    Args:
        num_workers: 啟動的Worker數量。
        cache_size: 用於快取下載資料的記憶體大小(單位: byte)。
        worker: 用於下載URL的Worker類別或工廠函式。
        grammars: 用於解析檔案中URL的語法規則列表。

    cache_size預設為1 MiB, 意味著Resolver會嘗試將下載的資料保存在記憶體中,
    直到快取大小達到1 MiB為止。超過此大小的資料會被存回硬碟以節省記憶體使用。

    worker預設為DefaultWorker, 使用httpx庫來下載URL資源。
    可以自訂worker以使用不同的下載實作。

    grammars預設為BasicUrlGrammar, 提供基本的URL解析功能。
    可以提供自訂的語法規則來解析不同格式的URL檔案。
    傳入為list[IUrlGrammar], 將會依序嘗試每個語法規則來解析檔案中的URL
    並使用第一個成功解析的URL進行下載。
    若無法解析出URL, 將認為該檔案不是URL，會直接打開原始檔案。
    """
    storage: IStorage = Storage(cached_size=cache_size)
    grammars = grammars if grammars is not None else [BasicUrlGrammar()]

    return Resolver(
        num_workers,
        storage=storage,
        worker_factory=worker if worker is not None else DefaultWorker,
        grammars=grammars,
    )
