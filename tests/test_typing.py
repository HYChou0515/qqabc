"""Tests for typing improvements.

驗證型別標註改進後的 runtime 行為不變,
以及公開 API 的型別簽名正確性。
"""

from __future__ import annotations

import inspect
from io import BytesIO
from typing import TYPE_CHECKING, Any

from qqabc.qq import Q, Worker
from qqabc.rurl.basic import BasicUrlGrammar, DefaultWorker, Storage, _ensure_fpath
from qqabc.rurl.rurl import (
    IResolver,
    Plugin,
    PluginOptions,
    Resolver,
    ResolverFactory,
    _getnow,
    resolve,
)
from qqabc.types import InData, IStorage, OutData

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock


# ---------------------------------------------------------------------------
# Step 1: resolve() 使用 Unpack[ResolverConfig] 簽名
# ---------------------------------------------------------------------------


class TestResolveSignature:
    """驗證 resolve() 和 ResolverFactory 接受 ResolverConfig 中定義的 kwargs。"""

    def test_resolve_accepts_num_workers(self, httpx_mock: HTTPXMock) -> None:
        """resolve() 應該接受 num_workers kwarg。"""
        httpx_mock.add_response(url="https://example.com/a.txt", content=b"hello")
        r = resolve(num_workers=1)
        with r:
            r.add(url="https://example.com/a.txt")
            for od in r.completed():
                assert od.data.read() == b"hello"

    def test_resolve_accepts_cache_size(self, httpx_mock: HTTPXMock) -> None:
        """resolve() 應該接受 cache_size kwarg。"""
        httpx_mock.add_response(url="https://example.com/b.txt", content=b"world")
        r = resolve(num_workers=1, cache_size=1024)
        with r:
            r.add(url="https://example.com/b.txt")
            for od in r.completed():
                assert od.data.read() == b"world"

    def test_resolver_factory_accepts_kwargs(self, httpx_mock: HTTPXMock) -> None:
        """ResolverFactory 應正確接受 typed kwargs。"""
        httpx_mock.add_response(url="https://example.com/c.txt", content=b"data")
        factory = ResolverFactory(num_workers=1, cache_size=2048)
        r = factory()
        with r:
            r.add(url="https://example.com/c.txt")
            for od in r.completed():
                assert od.data.read() == b"data"


# ---------------------------------------------------------------------------
# Step 2: Resolver 公開方法回傳型別驗證
# ---------------------------------------------------------------------------


class TestResolverReturnTypes:
    """驗證 Resolver 各方法回傳正確型別的物件。"""

    def test_wait_returns_outdata(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url="https://example.com/w.txt", content=b"wait_data")
        with resolve(num_workers=1) as r:
            tid = r.add(url="https://example.com/w.txt")
            result = r.wait(tid)
            assert isinstance(result, OutData)
            assert result.data.read() == b"wait_data"

    def test_add_wait_returns_outdata_or_none(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url="https://example.com/aw.txt", content=b"aw_data")
        with resolve(num_workers=1) as r:
            result = r.add_wait(url="https://example.com/aw.txt")
            assert isinstance(result, OutData)
            assert result.data.read() == b"aw_data"

    def test_add_wait_returns_outdata_for_existing_non_url_file(
        self, tmp_path: Any
    ) -> None:
        fpath = tmp_path / "not_a_url.txt"
        fpath.write_text("this is not a url content")
        with resolve(num_workers=1) as r:
            # fname 不包含 URL 但檔案存在 → 視為已完成, 回傳 OutData
            result = r.add_wait(fname=str(fpath))
            assert isinstance(result, OutData)
            assert result.err is None
            result.data.seek(0)
            assert result.data.read() == b"this is not a url content"

    def test_completed_yields_outdata(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url="https://example.com/cmp.txt", content=b"cmp")
        with resolve(num_workers=1) as r:
            r.add(url="https://example.com/cmp.txt")
            results = list(r.completed())
            assert len(results) == 1
            assert isinstance(results[0], OutData)

    def test_iter_and_close_yields_outdata(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url="https://example.com/ic.txt", content=b"ic")
        r = resolve(num_workers=1)
        r.__enter__()
        r.add(url="https://example.com/ic.txt")
        results = list(r.iter_and_close())
        assert len(results) == 1
        assert isinstance(results[0], OutData)
        r.__exit__(None, None, None)

    def test_close_returns_none(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url="https://example.com/cl.txt", content=b"cl")
        r = resolve(num_workers=1)
        r.__enter__()
        r.add(url="https://example.com/cl.txt")
        result = r.close()
        assert result is None
        r.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# Step 3: saved_task_id 型別正確
# ---------------------------------------------------------------------------


class TestResolverSavedTaskId:
    def test_saved_task_id_is_typed_dict(
        self, httpx_mock: HTTPXMock, tmp_path: Any
    ) -> None:
        httpx_mock.add_response(url="https://example.com/st.txt", content=b"st")
        fpath = str(tmp_path / "st.txt")
        with resolve(num_workers=1) as r:
            r.add(url="https://example.com/st.txt", fname=fpath)
            assert isinstance(r.saved_task_id, dict)
            for key, val in r.saved_task_id.items():
                assert isinstance(key, tuple)
                assert isinstance(val, int)
            list(r.completed())


# ---------------------------------------------------------------------------
# Step 4: Q.put/end/stop 回傳 Self (chain calls)
# ---------------------------------------------------------------------------


class TestQSelfReturn:
    def test_put_returns_self(self) -> None:
        q: Q[int] = Q("thread")
        result = q.put(42)
        assert result is q

    def test_end_returns_self(self) -> None:
        q: Q[int] = Q("thread")
        result = q.end()
        assert result is q

    def test_stop_returns_self(self) -> None:
        q: Q[int] = Q("thread")

        def noop(_q: Q[int]) -> None:
            for _ in _q:
                pass

        w = Worker.thread(noop, q)
        result = q.stop(w)
        assert result is q

    def test_chaining(self) -> None:
        """Put 和 end 應該可以鏈式呼叫。"""
        q: Q[int] = Q("thread")
        result = q.put(1, order=0).put(2, order=1).put(3, order=2).end()
        assert result is q
        items = [m.data for m in q.sorted()]
        assert items == [1, 2, 3]


# ---------------------------------------------------------------------------
# Step 5: Worker/Q 明確方法簽名
# ---------------------------------------------------------------------------


class TestWorkerExplicitMethods:
    def test_worker_join(self) -> None:
        q: Q[int] = Q("thread")
        q.end()

        def noop(_q: Q[int]) -> None:
            for _ in _q:
                pass

        w = Worker.thread(noop, q)
        w.join()
        assert not w.is_alive()

    def test_worker_is_alive(self) -> None:
        import time

        q: Q[int] = Q("thread")

        def slow_worker(_q: Q[int]) -> None:
            for _ in _q:
                time.sleep(0.1)

        w = Worker.thread(slow_worker, q)
        assert w.is_alive()
        q.end()
        w.join()
        assert not w.is_alive()


class TestQExplicitMethods:
    def test_qsize(self) -> None:
        q: Q[int] = Q("thread")
        assert q.qsize() == 0
        q.put(1)
        assert q.qsize() == 1

    def test_empty(self) -> None:
        q: Q[int] = Q("thread")
        assert q.empty()
        q.put(1)
        assert not q.empty()


# ---------------------------------------------------------------------------
# Step 6: IStorage 方法回傳 None
# ---------------------------------------------------------------------------


class TestIStorageReturnTypes:
    def test_register_returns_none(self) -> None:
        storage = Storage(cached_size=1024 * 1024)
        with storage:
            indata = InData(task_id=1, url="https://example.com", job_chance=10)
            result = storage.register(indata)
            assert result is None

    def test_save_returns_none(self) -> None:
        storage = Storage(cached_size=1024 * 1024)
        with storage:
            indata = InData(task_id=1, url="https://example.com", job_chance=10)
            storage.register(indata)
            outdata = OutData(task_id=1, data=BytesIO(b"data"))
            result = storage.save(1, outdata)
            assert result is None


# ---------------------------------------------------------------------------
# Step 7: dict 參數化驗證
# ---------------------------------------------------------------------------


class TestDictParameterization:
    def test_plugin_options_context_accepts_dict_str_any(self) -> None:
        """PluginOptions 的 context 和 httpx_options 應接受 dict[str, Any]。"""
        opts: PluginOptions = {
            "httpx_options": {"timeout": 30},
            "context": {"key": "value"},
        }
        assert opts["httpx_options"] == {"timeout": 30}
        assert opts["context"] == {"key": "value"}

    def test_plugin_dataclass_dict_str_any(self) -> None:
        """Plugin dataclass 應接受 dict[str, Any] 參數。"""
        p = Plugin(
            url="https://example.com/plugin.py",
            httpx_options={"timeout": 30},
            context={"key": "value"},
        )
        assert p.httpx_options == {"timeout": 30}
        assert p.context == {"key": "value"}

    def test_basic_url_grammar_context(self) -> None:
        """BasicUrlGrammar 應接受 dict[str, Any] context。"""
        g = BasicUrlGrammar(context={"custom_key": 123})
        assert g.context == {"custom_key": 123}


# ---------------------------------------------------------------------------
# Step 8: DefaultWorker.start() 回傳型別
# ---------------------------------------------------------------------------


class TestDefaultWorkerStart:
    def test_start_is_context_manager(self) -> None:
        """DefaultWorker.start() 應回傳 context manager。"""
        worker = DefaultWorker()
        # start() 透過 @contextmanager 裝飾,應回傳 context manager
        cm = worker.start(worker_id=0)
        assert hasattr(cm, "__enter__")
        assert hasattr(cm, "__exit__")


# ---------------------------------------------------------------------------
# Step 9: open/iter_open IO 參數化
# ---------------------------------------------------------------------------


class TestIOParameterization:
    def test_open_rb_yields_bytes_io(
        self, httpx_mock: HTTPXMock, tmp_path: Any
    ) -> None:
        httpx_mock.add_response(url="https://example.com/io.txt", content=b"iodata")
        fpath = tmp_path / "test_url.txt"
        fpath.write_text("https://example.com/io.txt")
        with resolve(num_workers=1) as r:
            with r.open(fpath, "rb") as f:
                data = f.read()
                assert isinstance(data, bytes)
                assert data == b"iodata"

    def test_open_r_yields_str_io(self, httpx_mock: HTTPXMock, tmp_path: Any) -> None:
        httpx_mock.add_response(url="https://example.com/io2.txt", content=b"iodata2")
        fpath = tmp_path / "test_url2.txt"
        fpath.write_text("https://example.com/io2.txt")
        with resolve(num_workers=1) as r:
            with r.open(fpath, "r") as f:
                data = f.read()
                assert isinstance(data, str)
                assert data == "iodata2"

    def test_iter_open_rb_yields_bytes(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url="https://example.com/it.txt", content=b"itdata")
        with resolve(num_workers=1) as r:
            r.add(url="https://example.com/it.txt")
            for f in r.iter_open("rb"):
                data = f.read()
                assert isinstance(data, bytes)

    def test_iter_open_r_yields_str(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url="https://example.com/it2.txt", content=b"itdata2")
        with resolve(num_workers=1) as r:
            r.add(url="https://example.com/it2.txt")
            for f in r.iter_open("r"):
                data = f.read()
                assert isinstance(data, str)


# ---------------------------------------------------------------------------
# Step 10: _getnow 回傳型別
# ---------------------------------------------------------------------------


class TestGetnow:
    def test_getnow_returns_datetime(self) -> None:
        import datetime as dt

        result = _getnow()
        assert isinstance(result, dt.datetime)
        assert result.tzinfo is not None


# ---------------------------------------------------------------------------
# 綜合: 型別簽名檢查 (inspect 驗證)
# ---------------------------------------------------------------------------


class TestTypeAnnotationsPresent:
    """檢查各方法是否有型別標註 (非 Any)。"""

    def test_q_put_returns_self(self) -> None:
        annotations = Q.put.__annotations__
        assert "return" in annotations
        # 回傳應該是 Self (在 runtime 可能解析為不同名稱)

    def test_q_end_returns_self(self) -> None:
        annotations = Q.end.__annotations__
        assert "return" in annotations

    def test_q_stop_returns_self(self) -> None:
        annotations = Q.stop.__annotations__
        assert "return" in annotations

    def test_resolver_wait_has_return(self) -> None:
        sig = inspect.signature(Resolver.wait)
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_resolver_add_wait_has_return(self) -> None:
        sig = inspect.signature(Resolver.add_wait)
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_resolver_completed_has_return(self) -> None:
        sig = inspect.signature(Resolver.completed)
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_resolver_close_has_return(self) -> None:
        sig = inspect.signature(Resolver.close)
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_resolver_iter_and_close_has_return(self) -> None:
        sig = inspect.signature(Resolver.iter_and_close)
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_worker_join_exists(self) -> None:
        """Worker 應有明確的 join 方法 (非 __getattr__ 委派)。"""
        assert "join" in Worker.__dict__

    def test_worker_is_alive_exists(self) -> None:
        """Worker 應有明確的 is_alive 方法。"""
        assert "is_alive" in Worker.__dict__

    def test_q_qsize_exists(self) -> None:
        """Q 應有明確的 qsize 方法。"""
        assert "qsize" in Q.__dict__

    def test_q_empty_exists(self) -> None:
        """Q 應有明確的 empty 方法。"""
        assert "empty" in Q.__dict__

    def test_getnow_has_return(self) -> None:
        sig = inspect.signature(_getnow)
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_istorage_register_returns_none(self) -> None:
        sig = inspect.signature(IStorage.register)
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_istorage_save_returns_none(self) -> None:
        sig = inspect.signature(IStorage.save)
        assert sig.return_annotation is not inspect.Parameter.empty

    def test_iresolver_add_wait_has_return(self) -> None:
        sig = inspect.signature(IResolver.add_wait)
        assert sig.return_annotation is not inspect.Parameter.empty


class TestEnsureFpath:
    """測試 _ensure_fpath 工具函式。"""

    def test_returns_path_from_str(self) -> None:
        from pathlib import Path

        result = _ensure_fpath("/tmp/test.dat", task_id=1)
        assert isinstance(result, Path)
        assert result == Path("/tmp/test.dat")

    def test_returns_path_preserves_value(self) -> None:
        from pathlib import Path

        result = _ensure_fpath("relative/path/file.txt", task_id=42)
        assert result == Path("relative/path/file.txt")

    def test_raises_value_error_on_none(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="fpath for task_id 99 is None"):
            _ensure_fpath(None, task_id=99)
