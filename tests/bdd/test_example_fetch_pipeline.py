"""Example: 模擬真實場景 — async HTTP fetch pipeline。

情境：
    使用者有一批 URL 需要下載，下載後解析出標題。
    這類似真實的 web scraping pipeline：

    URLs → async fetch (模擬) → sync parse → 結果

用法展示：
    - 模擬 async IO (fetch)
    - 搭配 sync 後處理 (parse)
    - pipe() 一行式 + backpressure
    - Pipeline context manager + submit/results
"""

from __future__ import annotations

import asyncio
import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


# 模擬的 "HTTP response"
_FAKE_RESPONSES = {
    "https://example.com/a": "<html><title>Page A</title></html>",
    "https://example.com/b": "<html><title>Page B</title></html>",
    "https://example.com/c": "<html><title>Page C</title></html>",
}


async def fake_fetch(url: str) -> dict:
    """模擬 async HTTP GET，回傳 {url, body}。"""
    await asyncio.sleep(0.01)  # 模擬網路延遲
    body = _FAKE_RESPONSES.get(url, "<html><title>404</title></html>")
    return {"url": url, "body": body}


def parse_title(response: dict) -> dict:
    """同步解析 HTML title（簡易版）。"""
    body: str = response["body"]
    start = body.index("<title>") + len("<title>")
    end = body.index("</title>")
    return {"url": response["url"], "title": body[start:end]}


def test_fetch_and_parse_with_pipe():
    """
    Given: 三個 URL
    When:  async fetch → sync parse
    Then:  得到三個 {url, title} 結果
    """
    from qqabc.pipe import Stage, pipe

    urls = list(_FAKE_RESPONSES.keys())

    results = list(
        pipe(
            [
                Stage(fn=fake_fetch, concurrency=10),
                Stage(fn=parse_title, executor="thread"),
            ],
            input=urls,
            backpressure=5,
        )
    )

    results.sort(key=lambda r: r["url"])
    assert results == [
        {"url": "https://example.com/a", "title": "Page A"},
        {"url": "https://example.com/b", "title": "Page B"},
        {"url": "https://example.com/c", "title": "Page C"},
    ]


def test_fetch_pipeline_with_context_manager():
    """
    Given: 三個 URL，用 Pipeline context manager 提交
    When:  submit_many → results
    Then:  得到相同的正確結果
    """
    from qqabc.pipe import Pipeline, Stage

    stages = Stage(fn=fake_fetch, concurrency=5, name="fetch") | Stage(
        fn=parse_title, executor="thread", name="parse"
    )

    with Pipeline(stages, backpressure=5) as p:
        p.submit_many(_FAKE_RESPONSES.keys())

    results = sorted(p.results(), key=lambda r: r["url"])

    assert len(results) == 3
    assert results[0]["title"] == "Page A"
    assert results[1]["title"] == "Page B"
    assert results[2]["title"] == "Page C"


def test_fetch_unknown_url_gets_404():
    """
    Given: 一個不存在的 URL
    When:  經過 fetch → parse pipeline
    Then:  title 為 "404"
    """
    from qqabc.pipe import Stage, pipe

    results = list(
        pipe(
            [
                Stage(fn=fake_fetch),
                Stage(fn=parse_title, executor="thread"),
            ],
            input=["https://unknown.example.com"],
        )
    )

    assert len(results) == 1
    assert results[0]["title"] == "404"
