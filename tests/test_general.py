from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Literal

import pytest
from httpx import HTTPStatusError

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock
    from typing_extensions import Literal
else:
    try:
        from typing import Literal
    except ImportError:
        from typing_extensions import Literal


def get_url(read_mode: Literal["r", "rb"]) -> str:
    if read_mode == "rb":
        return "https://picsum.photos/200"
    if read_mode == "r":
        return "https://www.lipsum.com/feed/html/"
    raise NotImplementedError


def test_import_version():
    from qqabc import __version__

    assert re.match(r"^v?\d+\.\d+\.\d+([.-]\w+)?$", __version__)


def test_usage1():
    """測試add_wait基本功能

    測試使用add_wait方法來加入一個URL下載任務
    完成後需要成功取得下載的資料。
    """
    from qqabc.rurl import resolve

    url = get_url("rb")
    with resolve() as resolver:
        od = resolver.add_wait(url)
        assert od
        b = od.data
    assert b.seek(0, 2) > 1024


def test_usage11():
    """即便cache size設為0,
    仍能使用add_wait方法下載資料
    """
    from qqabc.rurl import resolve

    url = get_url("rb")
    with resolve(cache_size=0) as resolver:
        od = resolver.add_wait(url)
        assert od
        b = od.data
    assert b.seek(0, 2) > 1024


def test_usage2():
    """測試add + completed的使用情境

    測試加入多個任務，然後使用completed方法來取得所有完成的任務結果。
    """
    from qqabc.rurl import resolve

    url = get_url("rb")
    tasks = set()
    with resolve() as resolver:
        for _ in range(2):
            tasks.add(resolver.add(url))
        for task in resolver.iter_and_close():
            b = task.data
            tasks.remove(task.task_id)
            assert b.seek(0, 2) > 1024
        assert len(tasks) == 0


def test_usage21():
    """測試add + completed的使用情境

    測試加入多個任務，然後使用completed方法來取得所有完成的任務結果。
    這次加上邊跑邊加任務的情境。
    """
    from qqabc.rurl import resolve

    url = get_url("rb")
    tasks = set()
    with resolve() as resolver:
        todos = set(range(4))
        for _ in range(2):
            todos.pop()
            tasks.add(resolver.add(url))
        for task in resolver.completed():
            if todos:
                todos.pop()
                tasks.add(resolver.add(url))
            b = task.data
            tasks.remove(task.task_id)
            assert b.seek(0, 2) > 1024
        assert len(tasks) == 0


def test_usage22():
    from qqabc.rurl import resolve

    url = get_url("rb")
    dones = set()
    with resolve() as resolver:
        todos = set(range(4))
        for _ in range(2):
            todos.pop()
            resolver.add(url)
        for task_id in resolver.iter_completed_tasks():
            dones.add(task_id)
            if todos:
                todos.pop()
                resolver.add(url)
    assert len(dones) == 4


def test_usage3():
    """測試add + wait的使用情境

    測試加入多個任務，然後使用wait方法來等待每個任務完成並取得結果。
    """
    from qqabc.rurl import resolve

    url = get_url("rb")
    tasks = set()
    with resolve() as resolver:
        for _ in range(2):
            tasks.add(resolver.add(url))
        for task_id in tasks:
            od = resolver.wait(task_id)
            assert od
            b = od.data
            assert b.seek(0, 2) > 1024


@pytest.mark.parametrize("read_mode", ["rb", "r"])
def test_usage4(tmpdir: Path, read_mode: Literal["rb", "r"]):
    """測試open方法的使用情境

    測試使用open方法來打開一個包含URL的檔案, 會回傳IO物件來讀取下載的資料。
    預設cache size足夠大到裝的下測試的檔案, 此時不會馬上把下載的資料存到硬碟。
    resolver關閉後, 資料會被存回硬碟, 此時打開會讀到下載過後的檔案而非URL。
    """
    url = get_url(read_mode)
    with open(tmpdir / "urls.txt", "w") as f:
        f.write(url)
    from qqabc.rurl import resolve

    with resolve() as resolver:
        with resolver.open(tmpdir / "urls.txt", read_mode) as fp:
            assert isinstance(fp.read(1), bytes if read_mode == "rb" else str)
            assert fp.seek(0, 2) > 1024
        with open(tmpdir / "urls.txt") as fp:
            assert fp.read() == url
    with open(tmpdir / "urls.txt", read_mode) as fp:
        assert fp.seek(0, 2) > 1024


@pytest.mark.parametrize("read_mode", ["rb", "r"])
def test_usage5(tmpdir: Path, read_mode: Literal["rb", "r"]):
    """測試open方法的使用情境, cache size設為0

    測試使用open方法來打開一個包含URL的檔案, 會回傳IO物件來讀取下載的資料。
    這次cache size設為0, 代表不會把下載的資料存在記憶體中, 會直接存到硬碟。
    resolver關閉後, 資料已經存在硬碟中, 此時
    打開會讀到下載過後的檔案而非URL。
    """
    url = get_url(read_mode)
    with open(tmpdir / "urls.txt", "w") as f:
        f.write(url + "\n")
    from qqabc.rurl import resolve

    with resolve(cache_size=0) as resolver:
        with resolver.open(tmpdir / "urls.txt", read_mode) as fp:
            assert fp.seek(0, 2) > 1024

    with open(tmpdir / "urls.txt", read_mode) as fp:
        assert fp.seek(0, 2) > 1024


def test_usage6():
    """測試Worker啟動失敗的情境

    測試使用一個會在啟動時拋出例外的Worker類別,
    來確保Resolver能正確處理Worker啟動失敗的情況,會退出process並拋出WorkersDiedOutError例外。
    """
    from qqabc.rurl import InData, IWorker, OutData, resolve
    from qqabc.types import WorkersDiedOutError

    class BadWorker(IWorker):
        def start(self, worker_id: int):
            raise RuntimeError("Failed to start worker")

        def resolve(self, indata: InData) -> OutData:
            pass

    url = get_url("rb")
    with resolve(worker=BadWorker) as resolver, pytest.raises(WorkersDiedOutError):
        resolver.add_wait(url)


def test_usage61():
    """測試Worker啟動失敗的情境

    測試使用一個會在啟動時拋出例外的Worker類別,
    來確保Resolver能正確處理Worker啟動失敗的情況。
    這裡只要還有一個Worker啟動成功, 就能正常下載資料。
    """
    from qqabc.rurl import resolve
    from qqabc.rurl.basic import DefaultWorker

    class Worker(DefaultWorker):
        def start(self, worker_id: int):
            if worker_id == 3:
                import time

                time.sleep(1)
            return super().start(worker_id)

        def resolve(self, indata):
            if self.worker_id != 3:
                raise RuntimeError("Failed to download")
            return super().resolve(indata)

        @property
        def input_timeout(self) -> float | None:
            return 4.0

    url = get_url("rb")
    with resolve(
        job_chance=4, worker_chance=1, num_workers=4, worker=Worker
    ) as resolver:
        resolver.add_wait(url)


def test_usage7(tmpdir: Path):
    """測試open方法的使用情境

    測試使用open方法來打開多個包含URL的檔案, 會回傳IO物件來讀取下載的資料。
    使用cache size能裝一個但裝不下兩個的情境。
    在第二個檔案打開時, 會把第一個檔案的下載資料存回硬碟。
    resolver關閉後, 資料會被存回硬碟, 此時打開會讀到下載過後的檔案而非URL。

    這次使用自訂的Worker來限制下載內容的大小, 以確保測試的穩定性。
    下載的內容會是4500 bytes, 因此cache size設為5000 bytes能裝下一個但裝不下兩個。
    """
    from qqabc.rurl import InData, OutData, resolve
    from qqabc.rurl.basic import DefaultWorker

    class Worker(DefaultWorker):
        def resolve(self, indata: InData) -> OutData:
            resp = self.client.get(indata.url)
            resp.raise_for_status()
            content = resp.content[:4500]
            assert len(content) == 4500
            b = BytesIO(content)
            return OutData(task_id=indata.task_id, data=b)

    url = get_url("rb")
    with open(tmpdir / "urls1.txt", "w") as f:
        f.write(url)
    with open(tmpdir / "urls2.txt", "w") as f:
        f.write(url)

    with resolve(cache_size=5000, worker=Worker) as resolver:
        with resolver.open(tmpdir / "urls1.txt", "rb") as fp:
            assert isinstance(fp.read(1), bytes)
            assert fp.seek(0, 2) > 1024
        with open(tmpdir / "urls1.txt") as fp:
            assert fp.read() == url
        with resolver.open(tmpdir / "urls2.txt", "rb") as fp:
            assert isinstance(fp.read(1), bytes)
            assert fp.seek(0, 2) > 1024
        with open(tmpdir / "urls1.txt", "rb") as fp:
            assert fp.seek(0, 2) > 1024
        with open(tmpdir / "urls2.txt") as fp:
            assert fp.read() == url
    with open(tmpdir / "urls2.txt", "rb") as fp:
        assert fp.seek(0, 2) > 1024


def test_usage8(tmpdir: Path):
    """測試open方法的使用情境, 檔案內容不是URL

    測試使用open方法來打開一個不包含URL的檔案, 會回傳IO物件來讀取原始的檔案內容。
    """
    content = "This is not a URL.\nJust some text content."
    with open(tmpdir / "not_a_url.txt", "w") as f:
        f.write(content)
    from qqabc.rurl import resolve

    with resolve() as resolver:
        with resolver.open(tmpdir / "not_a_url.txt", "r") as fp:
            assert isinstance(fp.read(1), str)
            fp.seek(0)
            assert fp.read() == content
    with open(tmpdir / "not_a_url.txt") as fp:
        assert fp.read() == content


def test_usage9(tmpdir: Path):
    """測試自訂UrlGrammar的使用情境

    測試使用自訂的UrlGrammar來解析URL。
    """
    from qqabc.rurl import resolve
    from qqabc.rurl.basic import BasicUrlGrammar

    class CustomUrlGrammar(BasicUrlGrammar):
        def main_rule(self, content: str) -> str | None:
            if content.startswith("custom://"):
                return "https://picsum.photos/300"
            return None

    url = "custom://example/resource"

    with open(tmpdir / "url.txt", "w") as f:
        f.write(url)

    with resolve(grammars=[CustomUrlGrammar()]) as resolver:
        with resolver.open(tmpdir / "url.txt", "rb") as fp:
            assert isinstance(fp.read(1), bytes)
            assert fp.seek(0, 2) > 1024


def test_plugins(tmpdir: Path, httpx_mock: HTTPXMock) -> None:
    """Test that plugins are correctly imported."""
    from qqabc.rurl import Plugin, resolve

    tmpdir = Path(tmpdir)

    content = dedent("""
        from qqabc.rurl.basic import BasicUrlGrammar
        class TestUrlGrammar(BasicUrlGrammar):
            def main_rule(self, content: str) -> str | None:
                if content.startswith("test://"):
                    return "https://picsum.photos/200"
                return None

        def get_grammars():
            return [TestUrlGrammar()]
        """)
    httpx_mock.add_response(
        url="https://test_url.py",
        content=content,
    )
    httpx_mock.add_response(  # should be called twice
        url="https://test_url.py",
        content=content,
    )
    with resolve(
        plugins=[Plugin("https://test_url.py", cache_dir=tmpdir, rm_cache=True)]
    ) as _:
        pass
    with resolve(
        plugins=[Plugin("https://test_url.py", cache_dir=tmpdir, rm_cache=True)]
    ) as _:
        pass
    assert tmpdir.exists()
    assert any(tmpdir.iterdir())


@pytest.mark.parametrize("use_pic", [True, False])
def test_plugins2(tmpdir: Path, httpx_mock: HTTPXMock, *, use_pic: bool) -> None:
    """Test that plugins are correctly imported."""
    from qqabc.rurl import Plugin, resolve

    tmpdir = Path(tmpdir)

    content = dedent("""
        from qqabc.rurl.basic import BasicUrlGrammar
        class TestUrlGrammar(BasicUrlGrammar):
            def main_rule(self, content: str) -> str | None:
                if content.startswith("test://"):
                    if self.context.get("use_pic"):
                        return "https://picsum.photos/200"
                    else:
                        return "https://www.lipsum.com/feed/html"
                return None

        def get_grammars(context):
            return [TestUrlGrammar(context)]
        """)
    httpx_mock.add_response(
        url="https://test_url.py",
        content=content,
    )
    if use_pic:
        httpx_mock.add_response(
            url="https://picsum.photos/200",
        )
    else:
        httpx_mock.add_response(
            url="https://www.lipsum.com/feed/html",
        )
    with resolve(
        plugins=[
            Plugin(
                "https://test_url.py",
                cache_dir=tmpdir,
                rm_cache=True,
                context={"use_pic": use_pic},
            )
        ],
    ) as resolver:
        resolver.add_wait("test://example")


def test_plugins3(tmpdir: Path, httpx_mock: HTTPXMock) -> None:
    """Test that plugins are correctly imported."""
    import httpx

    from qqabc.rurl import Plugin, resolve

    tmpdir = Path(tmpdir)

    content = dedent("""
        from qqabc.rurl.basic import BasicUrlGrammar
        class TestUrlGrammar(BasicUrlGrammar):
            def main_rule(self, content: str) -> str | None:
                if content.startswith("test://"):
                    return "https://picsum.photos/200"
                return None

        def get_grammars(context):
            return [TestUrlGrammar(context)]
        """)

    def download_fn(url: str, local_path: Path) -> None:
        with open(local_path, "wb") as f:
            resp = httpx.get("https://hoo.py")
            f.write(resp.content)

    httpx_mock.add_response(
        url="https://hoo.py",
        content=content,
    )
    with resolve(
        plugins=[
            Plugin(
                "https://test_url.py",
                cache_dir=tmpdir,
                rm_cache=True,
                download_fn=download_fn,
            )
        ],
    ) as _:
        pass


def test_plugins4(tmpdir: Path, httpx_mock: HTTPXMock) -> None:
    """Test that plugins are correctly imported."""
    from qqabc.rurl import Plugin, resolve

    tmpdir = Path(tmpdir)

    content = dedent("""
        from qqabc.rurl.basic import BasicUrlGrammar
        class TestUrlGrammar(BasicUrlGrammar):
            def main_rule(self, content: str) -> str | None:
                if content.startswith("test://"):
                    return "https://picsum.photos/200"
                return None

        def get_grammars(context):
            return [TestUrlGrammar(context)]
        """)

    httpx_mock.add_response(
        url="https://test_url.py",
        content=content,
    )
    with resolve(
        plugins=[
            Plugin(
                "https://test_url.py",
                cache_dir=tmpdir,
            )
        ],
        plugin_options={"rm_cache": True},
    ) as _:
        pass
    assert tmpdir.exists()
    assert any(tmpdir.iterdir())


def test_resolver_factory():
    """Test the resolve factory function."""
    from qqabc.rurl import ResolverFactory

    resolve = ResolverFactory(
        num_workers=2,
        cache_size=1024 * 1024,
    )
    with resolve() as resolver:
        assert resolver._num_workers == 2  # noqa: SLF001
        assert len(resolver.grammars) == 1

    resolve = ResolverFactory()
    with resolve() as resolver:
        assert resolver._num_workers == 4  # noqa: SLF001


def test_add_should_resolve(tmpdir: Path):
    from qqabc.rurl import ResolverFactory
    from qqabc.rurl.basic import BasicUrlGrammar

    tmpdir = Path(tmpdir)

    class CustomUrlGrammar(BasicUrlGrammar):
        def main_rule(self, content: str) -> str | None:
            if content.startswith("custom://"):
                return "https://picsum.photos/300"
            return None

    resolve = ResolverFactory(
        grammars=[CustomUrlGrammar()],
    )
    with resolve() as resolver:
        data = resolver.add_wait("custom://example/resource").data
        assert data.seek(0, 2) > 1024


def test_retry1(httpx_mock: HTTPXMock):
    from qqabc.rurl import ResolverFactory
    from qqabc.rurl.basic import BasicUrlGrammar

    class CustomUrlGrammar(BasicUrlGrammar):
        def main_rule(self, content: str) -> str | None:
            return "https://example.com/resource"

    httpx_mock.add_response(
        url="https://example.com/resource",
        status_code=500,
    )
    httpx_mock.add_response(
        url="https://example.com/resource",
        content=bytes("x" * 1500, "utf-8"),
    )

    resolve = ResolverFactory(
        grammars=[CustomUrlGrammar()],
    )
    with resolve(job_chance=2) as resolver:
        data = resolver.add_wait("custom://example/resource").data
        assert data.seek(0, 2) == 1500


def test_retry2(httpx_mock: HTTPXMock):
    from qqabc.rurl import ResolverFactory
    from qqabc.rurl.basic import BasicUrlGrammar

    class CustomUrlGrammar(BasicUrlGrammar):
        def main_rule(self, content: str) -> str | None:
            return "https://example.com/resource"

    httpx_mock.add_response(
        url="https://example.com/resource",
        status_code=500,
    )
    httpx_mock.add_response(
        url="https://example.com/resource",
        status_code=500,
    )

    resolve = ResolverFactory(
        grammars=[CustomUrlGrammar()],
    )
    with pytest.raises(HTTPStatusError):
        with resolve(job_chance=2) as resolver:
            resolver.add_wait("custom://example/resource")


def test_usage10(tmpdir: Path, httpx_mock: HTTPXMock):
    from qqabc.rurl import ResolverFactory
    from qqabc.rurl.basic import BasicUrlGrammar

    class CustomUrlGrammar(BasicUrlGrammar):
        def main_rule(self, content: str) -> str | None:
            if content.startswith("custom://"):
                return f"https://{content.removeprefix('custom://')}"
            return None

    resolve = ResolverFactory(
        grammars=[CustomUrlGrammar()],
    )
    tmpdir = Path(tmpdir)

    def add_response(foo: str):
        httpx_mock.add_response(
            url=f"https://example.com/{foo}",
            content=bytes(foo * 1500, "utf-8"),
        )
        with open(tmpdir / f"{foo}.txt", "w") as f:
            f.write(f"custom://example.com/{foo}")

    add_response("a")
    add_response("b")

    with resolve() as resolver:
        for path in tmpdir.glob("*.txt"):
            resolver.add(fname=path)
        for path in tmpdir.glob("*.txt"):
            with resolver.open(path, "rb") as fp:
                assert fp.seek(0, 2) == 1500

    add_response("c")
    add_response("d")

    with resolve() as resolver:
        for path in tmpdir.glob("*.txt"):
            resolver.add(fname=path, on_err="none")
        for fp in resolver.iter_open():
            assert fp.seek(0, 2) == 1500

    add_response("e")
    add_response("f")
    import time

    with resolve(cache_size=1) as resolver:
        for path in tmpdir.glob("*.txt"):
            resolver.add(fname=path, on_err="none")
        time.sleep(1)  # wait for all downloads to finish
        #  issue #11 will fail in this case
        for fp in resolver.iter_open():
            assert fp.seek(0, 2) == 1500
