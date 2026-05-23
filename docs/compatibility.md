# 支援的 Python 版本

`qqabc` 在 CI 上測試的版本如下:

| Python | 支援等級 | 說明 |
|---|---|---|
| **3.13** | 完整支援 | CI matrix, 全部模組可用。 |
| **3.12** | 完整支援 | CI matrix。 |
| **3.11** | 完整支援 | CI matrix。 |
| **3.10** | 完整支援 | CI matrix, `qqabc.pipe` 的最低需求。 |
| **3.9**  | rurl/qq 支援 | CI matrix。`qqabc.pipe` import 時 raise `ImportError`。 |
| **3.8**  | **best-effort** | EOL, 不在 CI matrix 內。`pyproject.toml` 仍宣告 `requires-python = ">=3.8"`, source-level 相容, 但出問題不保證會修。建議升級到 3.9 以上。 |

## 模組與版本對照

| 模組 | 最低 Python |
|---|---|
| `qqabc.qq` | 3.9 |
| `qqabc.rurl` | 3.9 |
| `qqabc.pipe` | **3.10** (import 時強制檢查) |

## 為何 `qqabc.pipe` 需要 3.10+

`qqabc.pipe` 使用了 `match` 語句、`X | Y` 型別語法的 runtime 用法,
以及 `asyncio` 在 3.10+ 才穩定的某些 API。在 3.9 以下 import 會直接 raise:

```python
>>> import sys; sys.version_info[:2]
(3, 9)
>>> from qqabc.pipe import Stage
ImportError: qqabc.pipe requires Python 3.10 or later. Current version: 3.9
```

其他模組 (`qqabc.rurl`、`qqabc.qq`) 不受影響, 可在 3.9 上正常使用。
