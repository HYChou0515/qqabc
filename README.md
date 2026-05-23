# QQabc

URL 資源下載與 Pipeline 抽象 — 一個簡單的 Python 工具庫。

完整文件: <https://hychou0515.github.io/qqabc/>

## 安裝

```bash
pip install qqabc[httpx]
```

## 快速開始

```python
from qqabc.rurl import resolve

with resolve() as resolver:
    od = resolver.add_wait("https://picsum.photos/200")
    data = od.data.read()
```

```python
from qqabc.pipe import pipe, Stage

for result in pipe(
    [Stage(fn=lambda x: x + 1), Stage(fn=lambda x: x * 2)],
    input=[1, 2, 3],
):
    print(result)
```

## 模組

- **`qqabc.rurl`** — 高效的 URL 資源下載與解析工具, 支援多工、快取、檔案自動判斷與自訂解析規則。
- **`qqabc.pipe`** — `Stage` 抽象, 用於定義 pipeline 中的處理階段, 支援 thread / process / async 三種執行模式 (需要 Python 3.10+)。

詳細指南、API、範例請見 [完整文件](https://hychou0515.github.io/qqabc/)。

## 開發

```bash
make dev-install     # 安裝開發依賴
make test            # 執行測試
make style           # 格式化 + lint --fix
make docs-serve      # 本地預覽文件 (http://127.0.0.1:8000)
```

## License

[MIT](LICENSE) — 部分檔案 (`src/qqabc/qq.py`) 取自第三方專案, 詳見 [NOTICE](NOTICE)。
