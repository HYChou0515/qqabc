# QQabc

## 📚 文檔

# 使用手冊
## 1. 基本介紹

`qqabc.rurl` 提供高效的 URL 資源下載與解析工具，支援多工、快取、檔案自動判斷與自訂解析規則。核心類別為 `Resolver`，可透過 `resolve()` 工廠方法建立。

## 2. 快速開始

```python
from qqabc.rurl import resolve

url = "https://picsum.photos/200"
with resolve() as resolver:
    od = resolver.add_wait(url)
    data = od.data.read()
    # data 為下載的二進位內容
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
with resolve() as resolver:
    with resolver.open("urls.txt", "rb") as fp:
        data = fp.read()
        # data 為下載的二進位內容
```

### 3.3 快取與硬碟儲存

- `cache_size`：設定記憶體快取大小，超過則自動存回硬碟。
- 關閉解析器時，所有未存回硬碟的資料會自動儲存。

### 3.4 多工下載

- `num_workers`：設定同時下載的 worker 數量，預設 4。

### 3.5 自訂 Worker

可自訂 Worker 類別以擴充下載邏輯：

```python
from qqabc.rurl import DefaultWorker, resolve

class MyWorker(DefaultWorker):
    def resolve(self, indata):
        # 自訂下載邏輯
        return super().resolve(indata)

with resolve(worker=MyWorker) as resolver:
    ...
```

### 3.6 自訂 URL 語法解析

可自訂 `IUrlGrammar` 來解析特殊格式的 URL：

```python
from qqabc.rurl import BasicUrlGrammar, resolve

class CustomGrammar(BasicUrlGrammar):
    def main_rule(self, content: str) -> str | None:
        if content.startswith("custom://"):
            return "https://picsum.photos/300"
        return None

with resolve(grammars=[CustomGrammar()]) as resolver:
    ...
```

## 4. 例外處理

- `WorkersDiedOutError`：所有 worker 異常終止時拋出。
- `DataDeletedError`：資料已被刪除時拋出。
- `InvalidTaskError`：無效 task_id 時拋出。

## 5. Example Usages

### 1. 多任務下載與 completed

```python
from qqabc.rurl import resolve

url = "https://picsum.photos/200"
tasks = set()
with resolve() as resolver:
    for _ in range(2):
        tasks.add(resolver.add(url))
    for task in resolver.iter_and_close():
        b = task.data
        # b 為下載的二進位內容
        tasks.remove(task.task_id)
    # 所有任務皆已完成
```

### 2. 邊跑邊加任務

```python
from qqabc.rurl import resolve

url = "https://picsum.photos/200"
tasks = set()
with resolve() as resolver:
    todos = set(range(4))
    for _ in range(2):
        todos.pop()
        tasks.add(resolver.add(url))
    for task in resolver.completed(timeout=5):
        if todos:
            todos.pop()
            tasks.add(resolver.add(url))
        b = task.data
        # b 為下載的二進位內容
        tasks.remove(task.task_id)
    # 所有任務皆已完成
```

### 3. 多任務 + wait

```python
from qqabc.rurl import resolve

url = "https://picsum.photos/200"
tasks = set()
with resolve() as resolver:
    for _ in range(2):
        tasks.add(resolver.add(url))
    for task_id in tasks:
        od = resolver.wait(task_id)
        b = od.data
        # b 為下載的二進位內容
```

### 4. open 方法自動判斷 URL（cache in memory）

```python
from qqabc.rurl import resolve

url = "https://picsum.photos/200"
with open("urls.txt", "w") as f:
    f.write(url)
with resolve() as resolver:
    with resolver.open("urls.txt", "rb") as fp:
        data = fp.read()
        # data 為下載的二進位內容
    with open("urls.txt") as fp:
        text = fp.read()
        # text 為原始 URL 字串
with open("urls.txt", "rb") as fp:
    data = fp.read()
    # data 為下載的二進位內容
```

### 5. open 方法自動判斷 URL（cache to disk）

```python
from qqabc.rurl import resolve

url = "https://picsum.photos/200"
with open("urls.txt", "w") as f:
    f.write(url + "\n")
with resolve(cache_size=0) as resolver:
    with resolver.open("urls.txt", "rb") as fp:
        data = fp.read()
        # data 為下載的二進位內容
with open("urls.txt", "rb") as fp:
    data = fp.read()
    # data 為下載的二進位內容
```

### 6. 多檔案 open + cache size 限制

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

---

如需更詳細 API 說明，請參考 `src/qqabc/rurl.py` 內的 docstring。