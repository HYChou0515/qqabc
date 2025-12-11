from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import io
import os
import re
import sys
import traceback
import urllib.request
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager, contextmanager, suppress
from dataclasses import dataclass
from functools import partial
from inspect import signature
from logging import ERROR, INFO, getLogger
from pathlib import Path
from queue import Empty
from typing import IO, TYPE_CHECKING, Generator, Literal, TypedDict, overload

import qqabc.qq
from qqabc.rurl.basic import BasicUrlGrammar, DefaultWorker, Storage
from qqabc.types import (
    DataDeletedError,
    InData,
    InvalidTaskError,
    InvalidUrlError,
    IStorage,
    IUrlGrammar,
    IWorker,
    LogData,
    OutData,
    QQBugError,
    WorkersDiedOutError,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from typing_extensions import Self, Unpack
else:
    try:
        from typing import Self, Unpack
    except ImportError:
        from typing_extensions import Self, Unpack


logger = getLogger(__name__)


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


def _worker_download(
    input_q: qqabc.qq.Q[InData],
    output_q: qqabc.qq.Q[int],
    log_q: qqabc.qq.Q[LogData],
    worker: IWorker,
    worker_id: int,
    storage: IStorage,
    worker_chance: int,
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
                    ind.job_chance -= 1
                    worker_chance -= 1
                    log_q.put(
                        log_task_func(
                            f"Error: {e}, job_chance={ind.job_chance}, {worker_chance=}",
                            must=True,
                            level=ERROR,
                        )
                    )
                    if ind.job_chance > 0:
                        input_q.put(msg)
                    else:
                        outd = OutData(task_id=ind.task_id, data=io.BytesIO(), err=e)
                        storage.save(ind.task_id, outd)
                        output_q.put(ind.task_id)
                        log_q.put(
                            log_task_func(
                                "Failed, no more retries left", must=True, level=ERROR
                            )
                        )
                    if worker_chance <= 0:
                        log_q.put(
                            log_func(
                                "Worker chance exhausted, stopping worker",
                                must=True,
                                level=ERROR,
                            )
                        )
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
            logger.log(log.level, "%s - %s", prefix, log.msg)
            last_print = _getnow()


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
    def add(self, url: str | None = None, fname: str | None = None) -> int:
        """Add a URL to be resolved and return its task ID."""

    @abstractmethod
    def solve_url(self, url: str | IO[bytes]) -> str | None:
        """Solve the URL from a string or file-like object using the registered grammars."""

    @abstractmethod
    def wait(self, task_id: int) -> OutData:
        """Wait for the completion of a task and return its output data."""

    @abstractmethod
    def iter_and_close(self) -> Generator[OutData]:
        """Iterator that yields completed tasks as they finish, then closes the resolver."""

    @abstractmethod
    def completed(self) -> Generator[OutData]:
        """Generator that yields completed tasks as they finish.

        The generator will yield completed tasks
        until no more tasks to be done.
        """

    @abstractmethod
    def iter_completed_tasks(self) -> Generator[int]:
        """Generator that yields completed task IDs as they finish."""

    @overload
    def iter_open(self, mode: Literal["rb"]) -> Generator[IO[bytes]]: ...
    @overload
    def iter_open(self, mode: Literal["r"] = "r") -> Generator[IO[str]]: ...

    @abstractmethod
    def iter_open(
        self,
        mode: Literal["r", "rb"] = "r",
    ) -> Generator[IO]:
        """Iterator that opens resolved URLs as file-like objects.

        Args:
            mode: The mode in which to open the file-like objects (e.g., 'r', 'rb').

        Yields:
            File-like objects containing the resolved URL data.
        """

    @overload
    def open(
        self, filepath: str | Path, mode: Literal["rb"]
    ) -> AbstractContextManager[IO[bytes]]: ...
    @overload
    def open(
        self, filepath: str | Path, mode: Literal["r"] = "r"
    ) -> AbstractContextManager[IO[str]]: ...

    @abstractmethod
    def open(
        self, filepath: str | Path, mode: Literal["r", "rb"] = "r"
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
    def add_wait(self, url: str | None = None, fname: str | None = None):
        """Adds a URL to be resolved and waits for its completion."""


class Resolver(IResolver):
    def __init__(
        self,
        num_workers: int,
        *,
        storage: IStorage,
        worker_factory: Callable[[], IWorker],
        grammars: list[IUrlGrammar],
        job_chance: int,
        worker_chance: int,
        reraise: bool = True,
    ) -> None:
        self._num_workers = num_workers

        input_q = qqabc.qq.Q[InData]("thread")
        output_q = qqabc.qq.Q[int]("thread")
        log_q = qqabc.qq.Q[LogData]("thread")
        self.input_q = input_q
        self.output_q = output_q
        self.log_q = log_q
        self.storage = storage
        self.grammars = grammars
        self.job_chance = job_chance
        self.reraise = reraise
        self.workers = [
            qqabc.qq.run_thread(
                _worker_download,
                input_q,
                output_q,
                self.log_q,
                worker_factory(),
                w,
                self.storage,
                worker_chance=worker_chance,
            )
            for w in range(num_workers)
        ]
        self.printer = qqabc.qq.run_thread(_worker_print, self.log_q)
        self.task_cnt = 0
        self.done_cnt = 0
        self.saved_task_id = {}

    def __enter__(self) -> Self:
        self.storage.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        self.storage.__exit__(exc_type, exc_value, traceback)

    def _get_task_id(self):
        self.task_cnt += 1
        return self.task_cnt

    def solve_url(self, url: str | IO[bytes]) -> str | None:
        bio = io.BytesIO(url.encode("utf-8")) if isinstance(url, str) else url
        for grammar in self.grammars:
            bio.seek(0)
            url_ = grammar.parse_url(bio)
            if url_ is not None:
                return url_
        return None

    @contextmanager
    def open(
        self, filepath: str | Path, mode: Literal["r", "rb"] = "r"
    ) -> Generator[IO]:
        filepath = str(filepath)
        outd = None
        with suppress(DataDeletedError, InvalidUrlError):
            outd = self.add_wait(fname=filepath)
        if outd is None:
            with open(filepath, mode) as f:
                yield f
        else:
            outd.data.seek(0)
            if "b" in mode:
                yield outd.data
            else:
                yield io.StringIO(outd.data.read().decode("utf-8"))

    def iter_open(
        self,
        mode: Literal["r", "rb"] = "r",
    ) -> Generator[IO]:
        for msg in self._iter():
            outd = self._get_result(msg.data)
            outd.data.seek(0)
            if "b" in mode:
                yield outd.data
            else:
                yield io.StringIO(outd.data.read().decode("utf-8"))

    def add_wait(self, url: str | None = None, fname: str | None = None):
        if not (task_id := self.saved_task_id.get((url, str(fname)))):
            task_id = self.add(url, fname=fname)
        return self.wait(task_id)

    def add(
        self,
        url: str | None = None,
        fname: str | None = None,
        *,
        on_err: Literal["raise", "none"] = "raise",
    ) -> int | None:
        if url is None:
            with open(fname, "rb") as f:
                surl = self.solve_url(f)
        else:
            surl = self.solve_url(url)
        surl = surl or url
        if surl is None:
            if on_err == "raise":
                raise InvalidUrlError("Either url or fname must be provided.")
            return None
        task_id = self._get_task_id()
        self.saved_task_id[(url, str(fname))] = task_id
        indata = InData(
            task_id=task_id, url=surl, fpath=fname, job_chance=self.job_chance
        )
        self.storage.register(indata)
        self.input_q.put(indata)
        return task_id

    def _get_result(self, task_id: int):
        outd = self.storage.load(task_id)
        if outd.err:
            if self.reraise:
                raise outd.err
            logger.error("Task %d failed with error: %s", task_id, outd.err)
        return outd

    def iter_and_close(self):
        self.close()
        for msg in self.output_q:
            task_id = msg.data
            yield self._get_result(task_id)

    def close(self):
        self.input_q.stop(self.workers)
        self.output_q.end()
        self.log_q.stop(self.printer)

    def completed(self):
        for msg in self._iter():
            task_id = msg.data
            yield self._get_result(task_id)

    def iter_completed_tasks(self) -> Generator[int]:
        for msg in self._iter():
            yield msg.data

    def _iter(self, timeout: float = 0.05):
        """Generator that yields completed tasks as they finish.

        If timeout is set to a float value, the generator will yield
        completed tasks until no more tasks are available within the timeout period.
        """
        while True:
            try:
                for od in self.output_q.iter(timeout=timeout):
                    self.done_cnt += 1
                    yield od
            except Empty:  # noqa: PERF203
                if all(not worker.is_alive() for worker in self.workers):
                    raise WorkersDiedOutError from None
                if self.done_cnt >= self.task_cnt:
                    return

    def wait(self, task_id: int):
        if not (0 < task_id <= self.task_cnt):
            raise InvalidTaskError(task_id)
        if self.storage.has(task_id):
            return self._get_result(task_id)
        for msg in self._iter():
            completed_task_id = msg.data
            if completed_task_id == task_id:
                return self._get_result(task_id)
        raise QQBugError("Unreachable code reached.")


class PluginOptions(TypedDict, total=False):
    cache_dir: Path | None
    httpx_options: dict | None
    rm_cache: bool
    context: dict | None
    download_fn: Callable[[str, Path], bytes] | None


@dataclass
class Plugin:
    url: str
    cache_dir: Path | None = None
    httpx_options: dict | None = None
    rm_cache: bool = False
    context: dict | None = None
    download_fn: Callable[[str, Path], bytes] | None = None


class ResolverConfig(TypedDict, total=False):
    num_workers: int
    cache_size: int
    worker: type[IWorker] | Callable[[], IWorker] | None
    job_chance: int
    worker_chance: int
    grammars: list[IUrlGrammar] | None
    plugins: list[Plugin] | list[str] | None
    plugin_options: PluginOptions | None


def get_grammar_cache_dir(*, cache_dir: Path | None = None) -> Path:
    """返回緩存目錄，遵守 XDG config standard"""
    if cache_dir is None:
        xdg_config_home = os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")
        cache_dir = Path(xdg_config_home) / "qqabc" / "grammar_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _download_plugin_file(
    url: str,
    local_path: Path,
    httpx_options: dict | None = None,
    download_fn: Callable[[str, Path], bytes] | None = None,
):
    """下載 plugin Python 檔案到指定路徑"""
    if download_fn is not None:
        download_fn(url, local_path)
        return
    try:
        import httpx  # noqa: PLC0415

        client_args = httpx_options if httpx_options is not None else {}
        with httpx.Client(**client_args) as client:
            response = client.get(url)
            response.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(response.content)
    except ImportError:
        logger.info("httpx not available, falling back to urllib")
        if httpx_options:
            logger.warning(
                "httpx_options provided but httpx is not installed; ignoring options."
            )
        urllib.request.urlretrieve(url, local_path)  # noqa: S310


def load_remote_plugin(
    plugin: Plugin,
) -> tuple[Callable[[], IWorker] | None, list[IUrlGrammar]]:
    """
    下載遠端 plugin Python 檔案到緩存目錄，並返回其中定義的 Grammar 列表與可選 Worker 工廠。

    Plugin 模組規範：
        - 可選函式 `get_grammars() -> list[IUrlGrammar]`。
        - 可選函式 `get_worker_factory_func() -> Callable[[], IWorker]`。
        - 建議搭配內部緩存避免重複初始化：
            ```python
            _cached_grammars: list[IUrlGrammar] | None = None


            def get_grammars() -> list[IUrlGrammar]:
                global _cached_grammars
                if _cached_grammars is None:
                    _cached_grammars = [BasicUrlGrammar(), AdvancedGrammar()]
                return _cached_grammars
            ```
          這樣可以：
            - 延遲初始化 Grammar 實例，避免 import 時執行副作用
            - 每次呼叫返回同一份實例，避免重複生成
            - 支援依據參數或環境動態初始化 Grammar

    函式行為：
        1. 檢查本地緩存目錄（由 `get_grammar_cache_dir()` 決定）是否已有對應檔案。
        2. 若不存在，從指定 URL 下載並存到緩存。
           - 優先使用 `httpx`（可透過 `httpx_options` 傳入如 `verify`, `follow_redirects`）。
           - 若 `httpx` 不可用，fallback 至 `urllib.request.urlretrieve`。
        3. 使用安全化檔名 + URL hash 生成唯一本地檔案名，避免非法字元與 module 衝突。
        4. 動態載入 Python module。
        5. 呼叫 `get_grammars()`（若存在）取得 Grammar 列表，呼叫 `get_worker_factory_func()`（若存在）取得 Worker 工廠。
        6. 如果對應函式不存在，會返回空列表或 None，但不會拋出例外。

    Args:
        url (str): 遠端 plugin Python 檔案的 URL，例如：
            "https://myserver.com/url_grammars/basic_url.py"
        httpx_options (dict | None): 傳入給 httpx.Client 的額外參數，若使用 urllib fallback 則忽略。

    Returns:
        tuple[Callable[[], IWorker] | None, list[IUrlGrammar]]:
            - 第一個元素：`get_worker_factory_func()` 返回的 Worker 工廠，若不存在則為 None。
            - 第二個元素：`get_grammars()` 返回的 Grammar 列表，若不存在或函式返回 None，則為空列表。

    Cache:
        - Plugin 會被下載到本地緩存目錄（`~/.config/qqabc/grammar_cache`）。
        - 如果 cache 中已有同名檔案，會直接使用本地檔案，而不重新下載。
        - 檔案名稱由 URL 最後一段經過非法字元替換與 hash 處理決定。

    Example:
        >>> worker_factory, grammars = load_remote_plugin(
        ...     "https://myserver.com/url_grammars/basic_url.py"
        ... )
        >>> for g in grammars:
        ...     logger.info(g.parse_url(some_file))
        >>> if worker_factory is not None:
        ...     worker = worker_factory()
    """
    url = plugin.url
    httpx_options = plugin.httpx_options
    cache_dir = get_grammar_cache_dir(cache_dir=plugin.cache_dir)
    orig_filename = url.split("/")[-1]
    safe_filename = re.sub(r"[^a-zA-Z0-9_\-\.]", "_", orig_filename)  # 非法字元換成 "_"
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]  # noqa: S324
    local_filename = f"{safe_filename}_{url_hash}.py"

    local_path = cache_dir / local_filename
    if plugin.rm_cache and local_path.exists():
        logger.info("Removing cached plugin: %s", local_path)
        local_path.unlink()

    # 如果本地不存在，下載
    if not local_path.exists():
        _download_plugin_file(
            url, local_path, httpx_options=httpx_options, download_fn=plugin.download_fn
        )
    else:
        logger.info("Using cached plugin: %s", local_path)

    # 動態 import，使用唯一 module 名稱
    module_name = f"plugin_{url_hash}"
    spec = importlib.util.spec_from_file_location(module_name, local_path)
    if spec.loader is None:
        logger.warning("Cannot load plugin module from %s", local_path)
        return None, []
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    # 呼叫 get_grammars()
    get_grammars_func = getattr(module, "get_grammars", None)
    if get_grammars_func is None:
        logger.warning("Plugin module %s must define get_grammars()", orig_filename)
        grammars = []
    else:
        sig = signature(get_grammars_func)
        if len(sig.parameters) == 0:
            grammars = get_grammars_func()
        else:
            grammars = get_grammars_func(plugin.context or {})

    # 呼叫 worker_factory()
    get_worker_factory_func = getattr(module, "get_worker_factory_func", None)
    if get_worker_factory_func is None:
        logger.warning(
            "Plugin module %s must define get_worker_factory_func()", orig_filename
        )
        worker_factory = None
    else:
        sig = signature(get_worker_factory_func)
        if len(sig.parameters) == 0:
            worker_factory = get_worker_factory_func()
        else:
            worker_factory = get_worker_factory_func(plugin.context or {})

    return worker_factory, grammars or []


_DEFAULT_RESOLVER_CONFIG: ResolverConfig = {
    "num_workers": 4,
    "cache_size": 1 << 20,
    "worker": None,
    "grammars": None,
    "plugins": None,
    "plugin_options": None,
}


class ResolverFactory:
    def __init__(
        self,
        **kwargs: Unpack[ResolverConfig],
    ):
        self.config = _DEFAULT_RESOLVER_CONFIG | kwargs

    def __call__(
        self,
        **kwargs: Unpack[ResolverConfig],
    ) -> IResolver:
        config = self.config | kwargs
        num_workers = config.get("num_workers")
        cache_size = config.get("cache_size")
        worker = config.get("worker")
        grammars = config.get("grammars")
        plugins = config.get("plugins")
        plugin_options = config.get("plugin_options") or {}
        job_chance = config.get("job_chance", 10)
        worker_chance = config.get("worker_chance", 10)

        storage: IStorage = Storage(cached_size=cache_size)
        grammars: list[IUrlGrammar] = grammars or []
        _plugins = []
        if plugins is not None:
            for p in plugins:
                if isinstance(p, str):
                    _plugins.append(Plugin(url=p, **plugin_options))
                else:
                    for k, v in plugin_options.items():
                        if getattr(p, k) is None:
                            setattr(p, k, v)
                    _plugins.append(p)
        for p in _plugins:
            worker_factory, remote_grammars = load_remote_plugin(p)
            grammars.extend(remote_grammars)
            if worker is None and worker_factory is not None:
                # 使用第一個合法的worker_factory
                worker = worker_factory

        return Resolver(
            num_workers,
            storage=storage,
            worker_factory=worker if worker is not None else DefaultWorker,
            grammars=grammars if grammars else [BasicUrlGrammar()],
            job_chance=job_chance,
            worker_chance=worker_chance,
        )


def resolve(
    **kwargs: ResolverConfig,
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
    return ResolverFactory(**kwargs)()
