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


def test_save_to_disk_creates_tempfile_in_target_dir():
    """_save_to_disk 應在目標檔案的同目錄建立暫存檔，確保 shutil.move 使用 os.rename（原子操作）。

    若 NamedTemporaryFile 建在 /tmp 而目標在其他檔案系統上，
    shutil.move 會 fallback 為 copy+delete（非原子），
    導致競態條件下 load() 讀到不完整的檔案。
    """
    import tempfile
    from unittest.mock import patch

    created_tmp_dirs: list[str] = []
    original_ntf = tempfile.NamedTemporaryFile

    def tracking_ntf(*args, **kwargs):
        """追蹤 NamedTemporaryFile 的 dir 參數。"""
        created_tmp_dirs.append(kwargs.get("dir"))
        return original_ntf(*args, **kwargs)

    with Storage(cached_size=0) as storage:
        fpath = str(Path(storage.tmpdir.name) / "task_1.dat")
        indata = _make_indata(1, fpath)
        storage.register(indata)

        outdata = _make_outdata(1, b"atomic write test")

        with patch("tempfile.NamedTemporaryFile", side_effect=tracking_ntf):
            storage.save(1, outdata)

        # 確認暫存檔建在與目標相同的目錄
        assert len(created_tmp_dirs) == 1
        assert created_tmp_dirs[0] == str(Path(fpath).parent)


def test_save_to_disk_atomic_on_eviction():
    """Eviction 時 _save_to_disk 也應在目標同目錄建立暫存檔。"""
    import tempfile
    from unittest.mock import patch

    created_tmp_dirs: list[str] = []
    original_ntf = tempfile.NamedTemporaryFile

    def tracking_ntf(*args, **kwargs):
        created_tmp_dirs.append(kwargs.get("dir"))
        return original_ntf(*args, **kwargs)

    # cache_size=20: 第一個 task 剛好裝得下，第二個會觸發 eviction
    with Storage(cached_size=20) as storage:
        fpath1 = str(Path(storage.tmpdir.name) / "task_1.dat")
        fpath2 = str(Path(storage.tmpdir.name) / "task_2.dat")
        indata1 = _make_indata(1, fpath1)
        indata2 = _make_indata(2, fpath2)
        storage.register(indata1)
        storage.register(indata2)

        # 第一個 task 存入記憶體
        storage.save(1, _make_outdata(1, b"data1_that_is_big"))

        with patch("tempfile.NamedTemporaryFile", side_effect=tracking_ntf):
            # 第二個 task 觸發 eviction of task 1
            storage.save(2, _make_outdata(2, b"data2_that_is_big"))

        # eviction 的暫存檔應建在目標同目錄
        for tmp_dir in created_tmp_dirs:
            assert tmp_dir == storage.tmpdir.name


def test_saved_add_after_disk_write():
    """saved.add(task_id) 應在資料實際可供讀取後才執行。

    若 saved.add 在 _save_to_disk 前執行，會產生時間窗口：
    load() 看到 task_id in saved 但檔案尚未寫入。
    """
    with Storage(cached_size=0) as storage:
        fpath = str(Path(storage.tmpdir.name) / "task_1.dat")
        indata = _make_indata(1, fpath)
        storage.register(indata)

        # 在 save 完成前，task 不應在 saved 中
        assert 1 not in storage.saved

        storage.save(1, _make_outdata(1, b"test data"))

        # save 完成後，task 應在 saved 中，且檔案已存在
        assert 1 in storage.saved
        assert Path(fpath).exists()
