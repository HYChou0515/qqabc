# Pipeline (`qqabc.pipe`)

!!! warning "Python 版本"
    `qqabc.pipe` **需要 Python 3.10+** (import 時會強制檢查)。
    其他模組則只需 Python 3.9+。

`qqabc.pipe` 提供 `Stage` 抽象, 用於定義 pipeline 中的處理階段。
每個 Stage 可獨立選擇執行模式 (thread / process / async),
適合區分 CPU-bound 與 IO-bound 任務。

!!! note
    Pipeline 隨 base 套件一起出貨, 不需要額外的 extra; 只要 Python ≥ 3.10
    就能 `from qqabc.pipe import pipe, Stage`。

## 基本用法

```python
from qqabc.pipe import Stage

# 建立一個 thread-based stage
stage = Stage(fn=lambda x: x + 1, executor="thread", concurrency=4, name="adder")

stage.fn(10)       # 11
stage.executor     # "thread"
stage.concurrency  # 4
stage.name         # "adder"
```

## Executor 類型

Stage 支援三種執行模式:

| Executor | 說明 | 適用場景 |
|---|---|---|
| `"thread"` | 執行緒 (預設) | IO-bound 任務, 如網路請求、檔案讀寫 |
| `"process"` | 行程 | CPU-bound 任務, 如資料運算、影像處理 |
| `"async"` | asyncio | 大量並行 IO 任務, 如批次 API 呼叫 |

```python
from qqabc.pipe import Stage

io_stage = Stage(fn=download, executor="thread", concurrency=8)
cpu_stage = Stage(fn=transform, executor="process", concurrency=4)
async_stage = Stage(fn=fetch, executor="async", concurrency=16)
```

## Async 自動偵測

傳入 async 函式時, `executor` 會自動設為 `"async"`, 無需手動指定:

```python
from qqabc.pipe import Stage

async def fetch(url: str) -> bytes:
    ...

stage = Stage(fn=fetch)
stage.executor  # "async" — 自動偵測
```

若需要覆蓋自動偵測, 可明確指定 `executor`:

```python
stage = Stage(fn=fetch, executor="thread")  # 強制使用 thread
```

## 使用 `|` 串接 Stage

透過 `|` 運算子將多個 Stage 串成 pipeline chain, 為後續 Pipeline builder 做準備:

```python
from qqabc.pipe import Stage

download = Stage(fn=download_fn, executor="thread", concurrency=8, name="download")
parse = Stage(fn=parse_fn, executor="process", concurrency=4, name="parse")
save = Stage(fn=save_fn, executor="thread", concurrency=2, name="save")

pipeline = download | parse | save
# pipeline 為 [download, parse, save]
```

## 自訂 Stage (`IStage`)

進階使用者可實作 `IStage` 介面來自訂 Stage 行為:

```python
from collections.abc import Callable
from qqabc.pipe import IStage, ExecutorType

class MyStage(IStage[int, int]):
    @property
    def fn(self) -> Callable[[int], int]:
        return lambda x: x * 2

    @property
    def executor(self) -> ExecutorType:
        return "process"

    @property
    def concurrency(self) -> int:
        return 8

    @property
    def name(self) -> str:
        return "my_custom_stage"

custom = MyStage()
pipeline = custom | Stage(fn=save_fn, name="save")
```

## Pipeline — 一行建構流水線

`pipe()` 是最簡單的使用方式: 傳入 Stage 列表與 input 資料,
自動串接、執行、回傳結果。

!!! note "輸出順序"
    Pipeline 是 **streaming**, 輸出順序為 *完成順序* 而非送入順序。
    例如 `concurrency=4` 配合長短不一的工作, 早完成的 item 會先出來。
    需要保持順序時, 自行在每個 item 攜帶 index 並在收集端 sort。

!!! note "Dead-letter 取用"
    `pipe(stages, input=...)` 回傳的是 iterator (不是 `Pipeline` 物件),
    因此無法存取 `dead_letters`。要用 `on_error="dead_letter"` 收集失敗 item 時,
    改用 `Pipeline` context manager 並 keep reference (見 [Context Manager 用法](#context-manager-用法))。

```python
from qqabc.pipe import pipe, Stage

# 一行搞定: download → parse → save, 所有 stage 同時運行
for result in pipe(
    [
        Stage(fn=download, executor="thread", concurrency=8),
        Stage(fn=parse, executor="thread", concurrency=4),
        Stage(fn=save, executor="thread", concurrency=2),
    ],
    input=urls,
    backpressure=100,  # 背壓: stage 之間 queue 最多 100 筆
):
    print(result)
```

### 比較 stdlib

=== "qqabc.pipe (streaming)"

    ```python
    from qqabc.pipe import pipe, Stage

    # pipeline, 一筆完成一筆出來
    for r in pipe(
        [
            Stage(fn=download, concurrency=8),
            Stage(fn=parse, concurrency=4),
            Stage(fn=save, concurrency=2),
        ],
        input=urls,
        backpressure=100,
    ):
        print(r)
    ```

=== "stdlib (batched)"

    ```python
    from concurrent.futures import ThreadPoolExecutor

    # stdlib 做法, 要等全部完成才能開始下一步
    with ThreadPoolExecutor(8) as tp:
        raw = list(tp.map(download, urls))  # 要等全部下載完
    with ThreadPoolExecutor(4) as tp:
        parsed = list(tp.map(parse, raw))   # 要等全部 parse 完
    with ThreadPoolExecutor(2) as tp:
        list(tp.map(save, parsed))
    ```

### 混合 Thread + Async

```python
from qqabc.pipe import pipe, Stage

async def fetch(url: str) -> bytes:
    ...  # aiohttp / httpx async

for result in pipe(
    [
        Stage(fn=fetch, concurrency=50),                          # async (自動偵測)
        Stage(fn=parse_html, executor="thread", concurrency=4),   # thread
    ],
    input=url_list,
    backpressure=50,
):
    print(result)
```

### Context Manager 用法

需要更細粒度的控制時, 使用 `Pipeline` 物件:

```python
from qqabc.pipe import Pipeline, Stage

with Pipeline(
    Stage(fn=lambda x: x + 1) | Stage(fn=lambda x: x * 2),
    backpressure=10,
) as p:
    p.submit(1)
    p.submit(2)
    p.submit_many(range(3, 6))

    for result in p.results():
        print(result)
```

## 錯誤處理

Stage 支援三種錯誤策略, 透過 `on_error` 參數設定:

| 策略 | 說明 |
|---|---|
| `"raise"` (預設) | 重試耗盡後丟出例外, 終止 pipeline。 |
| `"skip"` | 重試耗盡後 silently 跳過該 item。 |
| `"dead_letter"` | 重試耗盡後收集到 `Pipeline.dead_letters` (`(stage_name, exception, item)`)。 |

```python
from qqabc.pipe import Pipeline, Stage

def maybe_fail(x):
    if x == 3:
        raise ValueError(f"failure on {x}")
    return x * 2

with Pipeline(
    [Stage(fn=maybe_fail, on_error="dead_letter")],
    backpressure=10,
) as p:
    p.submit_many([1, 2, 3, 4])

results = list(p.results())              # [2, 4, 8]
print(p.dead_letters)
# [('maybe_fail', ValueError('failure on 3'), 3)]
```

其他相關參數: `retry` (單 item 重試次數)、`error_handler`
(自訂修復函式)、`worker_chance` (per-worker 連續失敗上限)。

## Bounded Queue 與背壓 (Backpressure)

`pipe()` / `Pipeline` 的 `backpressure` 參數控制 stage 之間的 queue 大小。
當下游慢時, 上游的 `put()` 自動阻塞, 防止記憶體無限成長。

若需要手動操作底層 queue, 可直接使用 `BoundedQ` (繼承自 `Q`, 完全向後相容):

```python
from qqabc.pipe import BoundedQ

q = BoundedQ(kind="thread", maxsize=100)  # maxsize=0 為無界
q.put("data", order=0)
q.end()
for msg in q:
    print(msg.data)
```

## AsyncBoundedQ

用於 async stage 之間的 asyncio queue 包裝, 同樣支援背壓:

```python
import asyncio
from qqabc.pipe import AsyncBoundedQ

async def main():
    q = AsyncBoundedQ(maxsize=10)
    await q.put("hello", order=0)
    await q.put("world", order=1)
    await q.end()

    async for msg in q:
        print(msg.data)

asyncio.run(main())
```

## Thread ↔ Async Bridge

在混合 pipeline 中跨執行模型傳遞資料 (通常 `Pipeline` 會自動處理, 不需要手動使用):

| 函式 | 方向 | 說明 |
|---|---|---|
| `bridge_thread_to_async` | thread/process → async | 將阻塞式 `Q` 的訊息轉入 `AsyncBoundedQ` |
| `bridge_async_to_thread` | async → thread/process | 將 `AsyncBoundedQ` 的訊息轉入阻塞式 `Q` |
