# core 通知與對話的可靠交接

工作單：[#24](https://github.com/faberline/pulse/issues/24)。原問題是在正常逐次確認訊息後，core outbox 仍持續累積，第 1001 則「目前進度」被拒絕。

## 交接

`Hub::snapshot` 現在於同一 SQLite 交易讀取 task／chat／local 狀態，保存有界 snapshot 到 chat outbox，再確認該交易視圖中的 core 狀態更新。水位保存在 `settings.chat_core_outbox_cursor`。之後才 commit；任一步失敗，snapshot、core ACK 與水位一起回滾。

只有未撤銷、有有效 lease 的 chat 裝置可以消費 core 通知。local executor 或失效 chat 不能消費。Human journal 重播可以保留原 epoch；交接依據是目前有效的 chat 身分與 lease，不會修改既有 envelope 身分。

這是可合併的 domain 狀態更新交接，不是使用者已讀、逐則提醒或任務成功。core 記錄仍保留，只更新確認旗標。面向使用者的對話、結果及後續排程提醒仍需自己的持久內容。原 snapshot 大小／裁剪與兩層 outbox 的 1000 項容量限制保留。

device ACK 仍負責 chat outbox 的最後一段傳輸。即使交接後 server 重啟、device ACK 遺失，原 snapshot ID、sequence 與內容仍能重播。後於交易視圖的 domain 更新留給下一次交接。

## 背壓與工作恢復

core 容量不足現在有明確的 `Capacity` 錯誤。Human 事件保持 pending；server 不把暫時背壓保存成永久拒絕或回覆已完成。容量恢復後，原來源事件與 journal envelope 以既有 ID 重播。

remote 在開始新工作前檢查通知容量，並先套用已持久保存的 pending 來源事件。因此已收到的取消不會排在恢復的新工作後面。套用仍經原有 Origin、revision 與命令檢查；模型文字不能變成 Human。

若容量在領取工作後才耗盡，只重試本機預留／狀態交易。每次都重新核對 run token 與期限；不重試 HTTP 或供應商呼叫。這避免已知結果因通知塞車而卡在 Planning，也避免重複計費。

執行版本為 `pulse-remote-7`。舊 run 保持原版本限制，不會自動換版本續跑。DB schema 仍為 4；未改寫 migration 或 wire 格式。

## 2026-11-01 驗證

- 修改前的 1001 次實際 Hub／Store／device ACK 回歸在第 1001 次失敗，core pending=1000。日誌：`/private/tmp/pulse-outbox-repro-red.log`。
- 修正後 1001 次詢問全部通過，core pending=0，原 1001 筆 core 記錄保留，零新 task／模型呼叫。
- 12 項通知測試另覆蓋角色／lease、滿載、錯誤水位、交接 commit 失敗回滾、重開 DB、ACK 遺失、原 epoch journal、並行投影一致性、滿載後重播、領取競態及延後取消優先。
- fmt、workspace clippy 與 107 個正常 Rust 測試通過。既有 155 個 hard-exit 子程序樣本保持通過。

上述是後端交接證據。完整 UI 壓力、中文輸入、一般 browser／computer 操作、排程及 8 小時 soak 仍須依日常助理驗收完成。
