# URL Resolver (`qqabc.rurl`)

`qqabc.rurl` 提供高效的 URL 資源下載與解析工具, 支援多工、快取、檔案自動判斷
與自訂解析規則。核心類別為 `Resolver`, 可透過 `resolve()` 工廠方法建立。

## 任務管理

| 方法 | 說明 |
|---|---|
| `add(url)` | 加入下載任務, 回傳 `task_id`。 |
| `add_wait(url)` | 加入下載任務並等待完成, 回傳下載結果。 |
| `wait(task_id)` | 等待指定任務完成。 |
| `completed()` | 取得所有已完成任務 (已知任務全部完成後 generator 結束)。 |
| `iter_completed_tasks()` | 同上, 但只 yield `task_id`。 |
| `iter_and_close()` | 迭代所有完成任務並關閉解析器 (呼叫後不可再 `add`)。 |

## 檔案自動判斷與打開

`open(filepath, mode)` 可自動判斷檔案內容是否為 URL, 若是則下載並回傳資料流,
否則回傳原始檔案內容。

```python
# url.txt 內容為 URL
# https://picsum.photos/200
with resolve() as resolver:
    with resolver.open("url.txt", "rb") as fp:
        data = fp.read()
        # data 為 url 的下載結果 binary
```

## 快取與硬碟儲存

- `cache_size`: 設定記憶體快取大小, 超過則自動存回硬碟。
- 關閉解析器時, 所有未存回硬碟的資料會自動儲存。

## 多工下載

- `num_workers`: 設定同時下載的 worker 數量, 預設 4。

## 自訂 Worker

可自訂 Worker 類別以擴充下載邏輯, 以下演示使用 `requests` 作為下載工具:

```python
from contextlib import contextmanager

from qqabc.rurl import DefaultWorker, resolve

class RequestWorker(DefaultWorker):
    @contextmanager
    def start(self, worker_id: int):
        self.worker_id = worker_id
        import requests  # noqa: PLC0415

        with requests.Session() as client:
            self.client = client
            yield self

with resolve(worker=RequestWorker) as resolver:
    ...
```

## 自訂 URL 語法解析

可自訂 `IUrlGrammar` 來解析特殊格式的 URL:

```python
from qqabc.rurl import BasicUrlGrammar, resolve

class CustomGrammar(BasicUrlGrammar):
    def main_rule(self, content: str) -> str | None:
        if content.startswith("custom://"):
            return f"https://picsum.photos/{content.replace('custom://', '')}"
        return None

with resolve(grammars=[CustomGrammar()]) as resolver:
    ...
```

## 例外處理

| 例外 | 說明 |
|---|---|
| `WorkersDiedOutError` | 所有 worker 異常終止時拋出。 |
| `DataDeletedError` | 資料已被刪除時拋出 (`KeyError` 的子類)。 |
| `InvalidTaskError` | 無效 `task_id` 時拋出。 |
| `InvalidUrlError` | 無法解析出 URL 時拋出 (僅 URL 偵測階段)。 |

## 下一步

- [使用範例: 4 種常見場景](examples.md)
