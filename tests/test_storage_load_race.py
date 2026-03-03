"""測試 Storage.load 的競態條件修正。

當 task_id 已在 self.saved 中但檔案尚未寫入完成時，
load() 應該等待檔案出現，而非直接拋出 FileNotFoundError。
"""

from __future__ import annotations

import threading
import time
from io import BytesIO
from pathlib import Path

from qqabc.rurl.basic import Storage
from qqabc.types import InData, OutData


def _make_indata(task_id: int, fpath: str) -> InData:
    return InData(
        task_id=task_id, url=f"https://example.com/{task_id}", job_chance=1, fpath=fpath
    )


def _make_outdata(task_id: int, content: bytes = b"hello world") -> OutData:
    return OutData(task_id=task_id, data=BytesIO(content))


def test_load_waits_for_file_to_appear():
    """load() 應在檔案尚不存在時等待，而非立即拋出 FileNotFoundError。

    模擬競態條件：task_id 已在 saved 中，但檔案尚未寫入磁碟。
    另一個執行緒會在短暫延遲後建立檔案。load() 應該等待並成功讀取。
    """
    with Storage(cached_size=0) as storage:
        fpath = str(Path(storage.tmpdir.name) / "task_99.dat")
        indata = _make_indata(99, fpath)
        storage.register(indata)

        # 手動將 task 標記為已儲存，但不實際寫入檔案（模擬競態）
        storage.saved.add(99)

        content = b"race condition test data"

        # 另一個執行緒在短暫延遲後寫入檔案
        def delayed_write():
            time.sleep(0.15)
            Path(fpath).write_bytes(content)

        writer = threading.Thread(target=delayed_write)
        writer.start()

        # load() 應該等待檔案出現，而非拋出 FileNotFoundError
        result = storage.load(99)
        writer.join()

        assert result.task_id == 99
        result.data.seek(0)
        assert result.data.read() == content


def test_load_existing_file_no_wait():
    """當檔案已存在時，load() 應立即返回，不需等待。"""
    with Storage(cached_size=0) as storage:
        fpath = str(Path(storage.tmpdir.name) / "task_42.dat")
        indata = _make_indata(42, fpath)
        storage.register(indata)

        content = b"already exists"
        Path(fpath).write_bytes(content)
        storage.saved.add(42)

        result = storage.load(42)
        assert result.task_id == 42
        result.data.seek(0)
        assert result.data.read() == content


def test_load_from_memory_cache():
    """從記憶體快取載入時不受影響。"""
    with Storage(cached_size=1024) as storage:
        indata = _make_indata(1, str(Path(storage.tmpdir.name) / "task_1.dat"))
        storage.register(indata)

        outdata = _make_outdata(1, b"cached in memory")
        storage.save(1, outdata)

        result = storage.load(1)
        assert result.task_id == 1
        result.data.seek(0)
        assert result.data.read() == b"cached in memory"
