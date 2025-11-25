from __future__ import annotations

import shutil
import tempfile
from contextlib import contextmanager
from io import BytesIO
from typing import IO

from qqabc.types import (
    DataDeletedError,
    InData,
    IStorage,
    IUrlGrammar,
    IWorker,
    OutData,
)


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
        fp.seek(0)
        if sz < self.url_min or sz > self.url_max:
            return False
        if b"://" not in fp.read(10):
            fp.seek(0)
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
