# QQabc

## 1. 基本介紹

`qqabc.rurl` 提供高效的 URL 資源下載與解析工具，支援多工、快取、檔案自動判斷與自訂解析規則。核心類別為 `Resolver`，可透過 `resolve()` 工廠方法建立。

## 2. 快速開始

```python
from qqabc.rurl import resolve

with resolve() as resolver:
    od = resolver.add_wait("https://picsum.photos/200")
    data = od.data.read()
    # data 為url的下載結果binary
```

**安裝**
```
pip install qqabc[httpx]
```

不想要安裝httpx可使用

```
pip install qqabc
```

## 3. 主要功能

### 3.1 任務管理

- `add(url)`: 加入下載任務，回傳 task_id。
- `add_wait(url)`: 加入下載任務並等待完成，回傳下載結果。
- `wait(task_id)`: 等待指定任務完成。
- `completed(timeout)`: 取得所有已完成任務（可設定超時）。
- `iter_and_close()`: 迭代所有完成任務並關閉解析器。

### 3.2 檔案自動判斷與打開

`open(filepath, mode)` 可自動判斷檔案內容是否為 URL，若是則下載並回傳資料流，否則回傳原始檔案內容。

```python
# url.txt內容為URL
# https://picsum.photos/200
with resolve() as resolver:
    with resolver.open("url.txt", "rb") as fp:
        data = fp.read()
        # data 為url的下載結果binary
```

### 3.3 快取與硬碟儲存

- `cache_size`：設定記憶體快取大小，超過則自動存回硬碟。
- 關閉解析器時，所有未存回硬碟的資料會自動儲存。

### 3.4 多工下載

- `num_workers`：設定同時下載的 worker 數量，預設 4。

### 3.5 自訂 Worker

可自訂 Worker 類別以擴充下載邏輯，以下演示使用requests作為下載工具

```python
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

### 3.6 自訂 URL 語法解析

可自訂 `IUrlGrammar` 來解析特殊格式的 URL：

```python
from qqabc.rurl import BasicUrlGrammar, resolve

class CustomGrammar(BasicUrlGrammar):
    def main_rule(self, content: str) -> str | None:
        if content.startswith("custom://"):
            return "https://picsum.photos/{content.replace('custom://', '')}"
        return None

with resolve(grammars=[CustomGrammar()]) as resolver:
    ...
```

## 4. 例外處理

- `WorkersDiedOutError`：所有 worker 異常終止時拋出。
- `DataDeletedError`：資料已被刪除時拋出。
- `InvalidTaskError`：無效 task_id 時拋出。

## 5. Example Usages

### 1. 一次給齊所有工作, 依結束順序處理結果

**應用場景**

- 批次下載大量資源，例如圖片、檔案、API資料等。
- 任務清單已知且固定，適合一次性處理所有任務。
- 需要依照任務完成順序即時處理結果（如即時儲存、分析、轉換等）。
- 適合高併發、批次任務、資料蒐集等場景。

> **_NOTE:_**  開始`iter_and_close`之後就無法再次添加新任務

```python
from qqabc.rurl import resolve

with resolve() as resolver:
    for i in range(100, 200):
        resolver.add(f"https://picsum.photos/{i}")
    for task in resolver.iter_and_close():
        # 會依照task結束順序給出結果
        b = task.data
        # b 為下載的二進位內容可繼續下游任務
    # 所有任務皆已完成
```

### 2. 邊跑邊加任務

**應用場景**

- 動態任務生成：根據前一批任務的結果，決定是否要再加入新任務。例如爬蟲、批次下載、API輪詢等。
- 資源分批處理：有些任務需要分批執行，根據已完成任務的狀態，持續補充新任務，確保資源利用率最大化。
- 即時任務調度：在任務執行過程中，根據外部事件或條件，隨時加入新下載或處理任務。
- 長時間監控/輪詢：持續監控某些資源，根據回應結果決定是否要再發起新請求。

> **_NOTE:_**  開始`completed`之前需要至少有一個任務

```python
from qqabc.rurl import resolve

url = "https://picsum.photos/200"
with resolve() as resolver:
    i = 100
    resolver.add(f"https://picsum.photos/{i}")
    for task in resolver.completed(timeout=5): # 超過timeout無新任務將認為全部做完並跳出迴圈
        b = task.data
        # b 為下載的二進位內容
        # 動態添加新任務
        if i < 1000 and b.seek(0, 2) > 100: # 如果size > 100
            resolver.add(f"https://picsum.photos/{i}")
            i += 2
```

### 3. 多任務 + wait

**應用場景**

- 需要精確控制每個任務的完成時機：例如每個下載任務完成後要立即做後續處理（如解析、轉存、通知等）。
- 任務之間有依賴或順序要求：例如先下載 A，再下載 B，或每個任務完成後要根據結果決定下一步。
- 小量任務、同步流程：適合任務數量不多，或希望逐一確認每個任務結果的情境。
- 簡單批次處理：例如批次下載幾個檔案，並逐一取得結果

```python
from qqabc.rurl import resolve

tasks = set()
with resolve() as resolver:
    for i in range(100, 200):
        tasks.add(resolver.add(f"https://picsum.photos/{i}"))
    for task_id in tasks:
        od = resolver.wait(task_id)
        b = od.data
        # b 為下載的二進位內容
```

### 4. open 方法自動判斷 URL

**應用場景**

- 自動判斷檔案內容是否為 URL：讓你用同一個 API 開啟本地檔案或遠端資源，無需手動判斷。
- 資料前處理/轉存：可直接取得下載內容進行分析、轉存或後續處理。
- 快取測試與效能優化：適合需要在記憶體中快取下載結果、避免重複下載的場景。
- 混合型檔案處理：同時處理本地檔案與 URL 清單，程式碼更簡潔一致。

```python
from qqabc.rurl import resolve

# url.txt內容為URL
# https://picsum.photos/200
with resolve() as resolver:
    with resolver.open("url.txt", "rb") as fp:
        data = fp.read()
        # data 為下載的二進位內容
    # 不會馬上存進disk (看cache_size決定)
    with open("url.txt") as fp:
        text = fp.read() # text 為原始 URL 字串

# 出去之後就會寫入原檔案
with open("url.txt", "rb") as fp:
    data = fp.read() # data 為下載的二進位內容
```

##### Cache size用法

設定cache_size=0, 則所有東西都會馬上寫回硬碟

```python
from qqabc.rurl import resolve

# url.txt內容為URL
# https://picsum.photos/200
with resolve(cache_size=0) as resolver:
    with resolver.open("url.txt", "rb") as fp:
        data = fp.read() # data 為下載的二進位內容
```

以下演示cache size以及寫入硬碟的時機

```python
from qqabc.rurl import DefaultWorker, InData, OutData, resolve

class Worker(DefaultWorker):
    def resolve(self, indata: InData) -> OutData:
        resp = self.client.get(indata.url)
        resp.raise_for_status()
        content = resp.content[:4500]
        b = BytesIO(content)
        return OutData(task_id=indata.task_id, data=b)

url = "https://picsum.photos/200"
with open("urls1.txt", "w") as f:
    f.write(url)
with open("urls2.txt", "w") as f:
    f.write(url)

with resolve(cache_size=5000, worker=Worker) as resolver:
    with resolver.open("urls1.txt", "rb") as fp:
        data1 = fp.read()
        # data1 為 4500 bytes 的下載內容
    with open("urls1.txt") as fp:
        text1 = fp.read()
        # text1 為原始 URL 字串
    with resolver.open("urls2.txt", "rb") as fp:
        data2 = fp.read()
        # data2 為 4500 bytes 的下載內容
    with open("urls1.txt", "rb") as fp:
        data1_disk = fp.read()
        # data1_disk 為 4500 bytes 的下載內容
    with open("urls2.txt") as fp:
        text2 = fp.read()
        # text2 為原始 URL 字串
with open("urls2.txt", "rb") as fp:
    data2_disk = fp.read()
    # data2_disk 為 4500 bytes 的下載內容
```
