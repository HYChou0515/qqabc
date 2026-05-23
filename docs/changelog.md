# 變更紀錄

詳細變更請參考 [GitHub releases](https://github.com/HYChou0515/qqabc/releases)
與 [commit history](https://github.com/HYChou0515/qqabc/commits/master)。

本頁面僅列出對使用者有感的重大變更, 細節以 commit 為準。

## Unreleased

- **文件**: 將 README 由單頁 Sphinx 改為 MkDocs Material 多頁文件 (rurl /
  pipe / examples / compatibility), 之後部署至 GitHub Pages。
- **修復**: README §3.5 / §3.6 / §4 / §5 中四個無法 copy-paste 的範例
  (缺 import、缺 `f` 前綴、用了不存在的 `completed(timeout=...)` 參數)。
- **修復**: `qqabc.pipe` 新增 `bridge_thread_to_async`、`bridge_async_to_thread`、
  `OnErrorType` 為 public re-export (README 一直視其為 public)。
- **修復**: `Storage.load()` 對未知 `task_id` 改 raise `DataDeletedError`
  (原本為裸 `KeyError`, 導致 `Resolver.open()` 內的 `suppress(DataDeletedError, ...)` 無效)。

## 0.2.4

- 加入 async executor 與相關 pipeline 抽象 (`qqabc.pipe`)。
- 加入 BoundedQ 與背壓控制。
- 各種錯誤處理改善。
