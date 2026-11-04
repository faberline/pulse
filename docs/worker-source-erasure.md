# 地端工作來源清除候選

這是 D06 的前置工作。模型記憶仍關閉。
Swift 只處理介面與輸入。
來源核對、清除、傳輸與取消均由 Rust 處理。
其他介面可以使用同一套核心。

資料庫 schema 為 19。
remote workflow 為 `pulse-remote-22`。
policy 為 `approved-effects-memory-fences-9`。

## Server 與地端資料

來源被移除後，server 清除原 read 要求與帶來源的操作 intent。
操作識別、批准狀態、未知結果與預算紀錄仍保留。
原派工訊息改為沒有正文的標頭。
標頭保留原序號、識別與摘要。
地端不能把這個標頭當作執行要求。

每次連線都須由 local executor 宣告支援清除協定。
舊地端不會收到帶來源的派工正文。
已確認收件的派工另有新的清除通知。
通知可重送，且仍核對原裝置、來源與任務修訂。

地端先將刪除限制保存到 SQLite 以外的限制檔。
限制檔只保存識別與任務修訂，沒有來源正文。
再清除工作要求、待送結果與圖片分塊。
待送資料改為原標頭，以便在不重送正文的情況下完成確認。
SQL 或實體清除失敗時，地端拒絕送出舊正文。
取出工作與保存結果也須先重查磁碟上的刪除限制。
重新開啟時，Rust 先重做清除。
只還原較舊的 SQLite 檔案，不能越過目前的刪除限制。
這項測試不代表已完成正式備份還原。
schema 15 與 16 的升級測試也涵蓋已撤銷的來源。

## 執行中的工作

清除限制會阻止相同任務修訂的新工作。
Rust 會撤銷快取的瀏覽器 session、電腦觀察與選擇權限。
執行監視器發現清除狀態後，會取消工作 future。
既有子程序清理會終止其程序群組。
晚到的結果與圖片不會重新保存。

受控測試已確認：測試 helper 開始操作一次後，來源清除能終止它。
測試也核對原選擇權限失效，且沒有再次操作。
這不是實際 macOS 擷取或操作驗收。
Rust 另已接上子程序退出核對與 Chrome profile 清理候選，見 [程序生命週期](owned-process-lifecycle.md)。

## 回歸檢查

`cargo test -p pulse-link --lib worker_erasure` 核對雙向確認、角色、序號、SQL 故障、舊 journal 還原、圖片分塊、真正 WebSocket 重連與測試子程序取消。
`cargo test -p pulse-link --test task_memory_sources` 核對 server 的來源清除與舊 schema 升級。
這兩組測試不會呼叫付費模型，也不會操作真實 macOS 視窗。

`cargo test -p pulse-link --lib worker_cleanup` 另核對停止回報、實際測試子程序退出、圖片讀取、佇列滿載、SQL 故障、資料庫還原及真正 WebSocket／HTTP 狀態查詢。
隔離 Chrome 的回報測試須另設 `PULSE_TEST_BROWSER_DRIVER`，再執行 `cargo test -p pulse-link --lib worker_cleanup_real_chrome_receipt -- --ignored`。
它核對 A 的 profile 移除後才送回報，並保留 B 的 session 與 profile。
這不是 macOS 視窗擷取、模型對話或完整日常使用驗收。

## 確認的意義與剩餘工作

傳輸確認只表示該訊息已完成本機持久處理。
它不證明操作成功、未執行，或實機程序已完全停止。
server 的未知結果與已保留預算仍保留。

Rust 另有獨立的 `WorkerCleanup` 回報。
它綁定原來源、任務修訂、裝置及已接受的清除通知標頭。
地端核對目前持有的程序群組退出、圖片讀取結束與自有快取清除後，才保存回報。
回報先進入最多 1,000 筆的獨立持久佇列，再排入一般傳輸佇列。
一般佇列滿載時，已核對的回報仍會保留。
重連與確認遺失會重送同一封回報，不重做原操作。

server 以 WebSocket 升級回覆的 `x-pulse-worker-cleanup: 1` 宣告支援。
新版地端確認支援後，才宣告版本 1 的回報能力。
舊版連線不會新增未知 JSON 欄位或訊息。
已配對的介面可查詢 `GET /v1/worker-cleanup/{operation_id}`。
結果為 `owned_runtime_cleanup: verified` 或 `unverified`，沒有來源正文。
這只表示自有 runtime 的清除核對，不表示原操作已成功或全部副本已清除。

runtime 的開啟及正常關閉紀錄保存在 SQLite 以外。
強制退出、舊版 journal、未知舊檔案或缺少正常關閉紀錄時，保持未核對。
後續新 process 的空清單不能清除這項不確定狀態。
只還原 SQLite 也不能將它改為已核對。
已在先前完成核對並保存的回報仍可重送。

## 地端身分撤銷候選

Rust local executor 現在也處理設定 server 的明確憑證拒絕。
它先在 SQLite 以外保存格式 7 的禁止重送紀錄，再移除目前 journal 的工作正文、待送結果、圖片分塊及快取投影。
停止按鈕、舊 SQLite 還原或新的空 runtime 都不能清除這項限制。
舊版核心不能讀取這份新限制檔；它必須拒絕啟動。
原操作的未知結果及費用保留不變。

local process 會經過正常收尾後退出。
Rust 會停止目前持有的 helper 與程序群組，等待圖片讀取結束，再移除自有快取。
讀取尚未結束時，仍保留 device lock。
無法清除未知檔案時，保留檔案並回報清除失敗。
這不是 server 的全副本清除證明。

只有明確的憑證拒絕才觸發這個流程。
一般斷線、沒有拒絕標記的 401，以及 503 都不觸發清除。
SQL 失敗後，外部限制仍禁止重送及顯示舊正文；修復後可重做本機清除。
這不代表失敗當下已完成實體清除。

`cargo test -p pulse-link --lib worker_denial` 核對角色與來源綁定、SQL 失敗、舊 SQLite 還原、停止按鈕、真正 server 撤銷、程序退出、讀取保護與清除失敗。
隔離 Chrome 的同流程測試須設定 `PULSE_TEST_BROWSER_DRIVER`，再執行 `cargo test -p pulse-link --lib worker_denial_real_chrome -- --ignored`。
一般測試使用假的憑證儲存與 native helper，不要求實際 macOS 權限。

目前 process 持有的原生 PNG 已加入分任務清除與到期容量回收候選。舊共用 PNG、舊 profile 與硬退出後的孤兒程序仍須完整處理。
provider 已加入送出前與等待核對，模型工具派工也已加入來源限制鎖；見 [任務來源核對](task-memory-sources.md)。server 的獨立自有 runtime 清除回報已有候選。其他 adapter 的完整在途取消、舊 runtime 與孤兒程序處理仍須完成。
子任務明確採用父來源已有候選。自動規劃的完整來源繼承、關閉副本與正式還原仍未完成。
這個候選不代表 D06、D08 或 K15 已接受。
