"""Tests for add() skipping download when fname already exists on disk."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_httpx import HTTPXMock


def test_add_fname_exists_wait(tmp_path: Path):
    """Fname 指向已存在的檔案時, add() 回傳 task_id, wait() 回傳 OutData.

    不需要任何 worker 實際下載, 應直接從磁碟讀取既有內容.
    """
    from qqabc.rurl import resolve

    content = b"hello existing file"
    existing = tmp_path / "existing.dat"
    existing.write_bytes(content)

    with resolve() as resolver:
        task_id = resolver.add(fname=str(existing))
        assert isinstance(task_id, int)
        od = resolver.wait(task_id)
        assert od.err is None
        od.data.seek(0)
        assert od.data.read() == content


def test_add_fname_exists_with_url(tmp_path: Path, httpx_mock: HTTPXMock):
    """同時給 url 和已存在的 fname 時, 不應觸發下載.

    httpx_mock 不註冊任何 URL, 若有下載會報錯.
    """
    from qqabc.rurl import resolve

    content = b"pre-existing data"
    existing = tmp_path / "existing.dat"
    existing.write_bytes(content)

    with resolve() as resolver:
        task_id = resolver.add(
            url="https://should.not.be.called/resource", fname=str(existing)
        )
        assert isinstance(task_id, int)
        od = resolver.wait(task_id)
        assert od.err is None
        od.data.seek(0)
        assert od.data.read() == content


def test_add_fname_exists_dedup(tmp_path: Path):
    """連續 add 同一個已存在的 fname 兩次, 回傳同一個 task_id."""
    from qqabc.rurl import resolve

    existing = tmp_path / "existing.dat"
    existing.write_bytes(b"data")

    with resolve() as resolver:
        tid1 = resolver.add(fname=str(existing))
        tid2 = resolver.add(fname=str(existing))
        assert tid1 == tid2


def test_add_fname_exists_completed(tmp_path: Path, httpx_mock: HTTPXMock):
    """混合 pre-done 和正常 task, completed() 應產出全部 OutData."""
    from qqabc.rurl import ResolverFactory
    from qqabc.rurl.basic import BasicUrlGrammar

    httpx_mock.add_response(
        url="https://example.com/download",
        content=b"downloaded content",
    )

    existing = tmp_path / "existing.dat"
    existing.write_bytes(b"local content")

    resolve = ResolverFactory(grammars=[BasicUrlGrammar()])
    with resolve() as resolver:
        tid_exist = resolver.add(fname=str(existing))
        tid_download = resolver.add(url="https://example.com/download")
        assert isinstance(tid_exist, int)
        assert isinstance(tid_download, int)

        results = {}
        for od in resolver.completed():
            results[od.task_id] = od.data.read()

        assert tid_exist in results
        assert tid_download in results
        assert results[tid_exist] == b"local content"
        assert results[tid_download] == b"downloaded content"


def test_add_fname_exists_iter_open(tmp_path: Path):
    """pre-done task 透過 iter_open('rb') 正確讀取."""
    from qqabc.rurl import resolve

    content = b"binary content for iter_open"
    existing = tmp_path / "existing.dat"
    existing.write_bytes(content)

    with resolve() as resolver:
        resolver.add(fname=str(existing))
        results = [fp.read() for fp in resolver.iter_open("rb")]
        assert len(results) == 1
        assert results[0] == content


def test_add_fname_exists_iter_open_text(tmp_path: Path):
    """pre-done task 透過 iter_open('r') 正確讀取文字."""
    from qqabc.rurl import resolve

    content = "hello text content"
    existing = tmp_path / "existing.txt"
    existing.write_text(content)

    with resolve() as resolver:
        resolver.add(fname=str(existing))
        results = [fp.read() for fp in resolver.iter_open("r")]
        assert len(results) == 1
        assert results[0] == content


def test_add_fname_not_exists_normal_flow(tmp_path: Path, httpx_mock: HTTPXMock):
    """Fname 不存在時, 走正常 worker resolve 流程, 不觸發新邏輯."""
    from qqabc.rurl import ResolverFactory
    from qqabc.rurl.basic import BasicUrlGrammar

    httpx_mock.add_response(
        url="https://example.com/resource",
        content=b"resolved data",
    )

    target = tmp_path / "not_yet_here.dat"
    assert not target.exists()

    resolve = ResolverFactory(grammars=[BasicUrlGrammar()])
    with resolve() as resolver:
        task_id = resolver.add(url="https://example.com/resource", fname=str(target))
        assert isinstance(task_id, int)
        od = resolver.wait(task_id)
        od.data.seek(0)
        assert od.data.read() == b"resolved data"


def test_add_fname_exists_iter_and_close(tmp_path: Path):
    """pre-done task 透過 iter_and_close() 正確取得."""
    from qqabc.rurl import resolve

    content = b"content for iter_and_close"
    existing = tmp_path / "existing.dat"
    existing.write_bytes(content)

    with resolve() as resolver:
        tid = resolver.add(fname=str(existing))
        results = list(resolver.iter_and_close())
        assert len(results) == 1
        assert results[0].task_id == tid
        results[0].data.seek(0)
        assert results[0].data.read() == content


def test_add_fname_exists_add_wait(tmp_path: Path):
    """已存在的 fname 透過 add_wait() 一步到位."""
    from qqabc.rurl import resolve

    content = b"add_wait content"
    existing = tmp_path / "existing.dat"
    existing.write_bytes(content)

    with resolve() as resolver:
        od = resolver.add_wait(fname=str(existing))
        assert od is not None
        assert od.err is None
        od.data.seek(0)
        assert od.data.read() == content
