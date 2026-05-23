# QQabc

URL 資源下載與 Pipeline 抽象 — 一個簡單的 Python 工具庫。

`qqabc` 包含兩個彼此獨立的核心模組:

<div class="grid cards" markdown>

-   :material-download: **`qqabc.rurl`**

    ---

    高效的 URL 資源下載與解析工具, 支援多工、快取、檔案自動判斷與自訂解析規則。

    [:octicons-arrow-right-24: 進入 rurl 章節](rurl/index.md)

-   :material-pipe: **`qqabc.pipe`**

    ---

    `Stage` 抽象, 用於定義 pipeline 中的處理階段, 支援 thread / process / async 三種執行模式。

    [:octicons-arrow-right-24: 進入 pipe 章節](pipe/index.md)

</div>

## 安裝

```bash
pip install qqabc[httpx]
```

不想要安裝 `httpx` 也可以使用:

```bash
pip install qqabc
```

!!! note
    沒有 `[httpx]` extra 時, 預設的 `DefaultWorker` 會在啟動時 `import httpx`
    失敗 → 所有 worker 立刻死光, `wait()` / `completed()` 會 raise
    [`WorkersDiedOutError`][qqabc.types.WorkersDiedOutError]。此情況下必須自己
    提供 `resolve(worker=...)` (見 [§自訂 Worker](rurl/index.md#自訂-worker)),
    例如改用 `requests` / `urllib`。

## 快速開始

### 下載一個 URL

```python
from qqabc.rurl import resolve

with resolve() as resolver:
    od = resolver.add_wait("https://picsum.photos/200")
    data = od.data.read()
    # data 為 url 的下載結果 binary
```

### 跑一條 pipeline

```python
from qqabc.pipe import pipe, Stage

for result in pipe(
    [Stage(fn=lambda x: x + 1), Stage(fn=lambda x: x * 2)],
    input=[1, 2, 3],
):
    print(result)
# 4, 6, 8 (順序可能不同 — pipeline 是 streaming, 依完成順序輸出)
```

## 下一步

- [URL Resolver 完整指南](rurl/index.md)
- [URL Resolver 使用範例](rurl/examples.md)
- [Pipeline 完整指南](pipe/index.md)
- [支援的 Python 版本](compatibility.md)
