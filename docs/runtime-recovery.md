# Local executor 強制退出恢復候選

Swift 仍只處理介面。Rust local executor 負責連線、授權及工具。
官方 `pulse-executor` 現在要求同目錄的 Rust `pulse-guardian`。
app 建置會一併複製並簽署它。
guardian 只擁有一次工具子程序。它不連 server，不呼叫模型，也不排程任務。
嵌入式 Rust client 須明確使用 `with_runtime_recovery()`；舊的未受管 client 不能沿用這項恢復證據。

## 程序停止

工具啟動前，Rust 保存私有的操作目錄、原目錄身分與啟動要求。
guardian 先取得該操作的獨占鎖，再保存「開始啟動」狀態。
同一次操作不能再次啟動工具。
executor 與 guardian 使用私有 Unix socket，也就是只存在這兩個程序間的連線。
工具另有自己的 stdin、stdout 與 stderr，不能繼承控制連線。

executor 強制退出後，控制連線會結束。
guardian 仍持有原工具子程序，因此可以停止及回收它。
它只向尚未回收的自有子程序群組發出停止訊號。
重啟流程不依舊 PID 發出訊號。

Chrome session 建立前，guardian 須確認已接手 Driver 的本機關閉端點。
退出時先要求該 Driver 關閉所有 session。
成功回覆、Driver 退出及程序群組消失均須核對。
只有成功回覆不能通過。
無法證明 Chrome 已停止時，profile 會保留，還原也會拒絕假報完成。

## 檔案所有權

runtime 紀錄位於本機資料目錄的 `runtime/`，權限為 `0700`。
操作與資源各有獨立的 UUID 紀錄。檔案權限為 `0600`。
runtime 及操作目錄都核對原裝置與 inode；inode 是檔案系統中的物件識別。
只複製舊紀錄或替換目錄，不能取得原目錄的清除資格。

PNG 目錄與 Chrome profile 在交給寫入者之前，先保存原目錄身分。
新資源使用格式 2。建立目錄前，先保存隨機的歸屬值。
交給寫入者之前，將同一個值寫入目錄內的私有 `.pulse-owner` 檔，並保存到磁碟。
清理時須同時核對原裝置、inode 與歸屬值。
即使檔案系統重用 inode，缺少原標記或標記不同的目錄也不能清除正文。
標記的符號連結、硬連結、公開權限與無效內容均會被拒絕。
清理保留標記直到其他內容都已移除；中途退出後可以繼續。
標記已移除的目錄只能以非遞迴方式移除空目錄。
舊格式 1 沒有歸屬標記；若還有內容，保留資料與未知狀態。
若在建立空目錄與保存 inode 之間中斷，只能移除仍為空的目錄。
若裡面已有無法核對的內容，保留資料並回報失敗。
PNG 清理沿用正常 runtime 的檢查：只接受受管 PNG 或 UUID 暫存檔，並核對權限、擁有者、檔案連結與容量。
未知檔案、符號連結、硬連結或被替換的目錄不能通過。
profile 則以原本獨占的整個私有目錄作為所有權範圍。

## 重啟順序

新 executor 先取得原本的本機資料鎖。
它等待舊 guardian 的結果與鎖釋放，再讀取原始停止紀錄。
鎖已釋放，仍不等於工具已停止。
只有尚未開始啟動的要求，或已保存完整停止結果的要求，才可收尾。
整機重新開機另可提供停止證據。
新操作與 runtime 紀錄會保存核心提供的開機 UUID。
Linux 讀取 `/proc/sys/kernel/random/boot_id`；macOS 讀取 `kern.bootsessionuuid`。
介面來源見 [Linux 核心文件](https://www.kernel.org/doc/html/latest/admin-guide/sysctl/kernel.html#random) 與 [Apple XNU 的唯讀 sysctl](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_sysctl.c)。
這是每次開機的識別，不使用時間、環境變數或 PID 推測。
目前開機、原操作與原 runtime 均須有可核對的識別。
只有同平台的開機 UUID 已改變，且原操作與原 runtime 相符，才可將未知啟動收尾。
收尾前再次讀取核心確認。遲到的 guardian 也必須核對原開機，才能啟動工具。
缺少舊識別、不同平台、紀錄不符或系統拒絕讀取時，仍保留未知狀態。
Mac 的工具沙箱會拒絕這項讀取；一般主機環境已確認可讀。
清理所有已核對的工具及資源後，才可結束舊 runtime 並開始新的 runtime。
關閉 Core 之前會先保存收尾狀態；再次中止後仍可續接同一個 runtime。
這項狀態也會寫入 Core 原有的外部 worker 限制，不能只回退 SQLite 清除它。

恢復不會重新執行舊操作，也不會產生新的成功 receipt 或退回未知費用。
來源撤銷仍會封鎖原任務修訂。取消 A 不會停止 B 的工具或清理 B 的圖片。
新工作仍須通過原本的裝置、來源、修訂、scope、批准與預算檢查。

## 目前證據與限制

受控程序測試涵蓋真正 SIGKILL、重啟後的 Core runtime 核對、PNG／profile 清理、雙任務隔離、原始 stdin／stdout、目錄替換、複製的操作目錄、未知檔案及部分準備寫入。
原生 helper 的受控測試另核對原 IPC、輸出限制與圖片來源。
它們不是實際 macOS 視窗操作。

明確啟用的 Chrome 測試使用兩個自有 loopback 頁面 session 與獨立 profile。
測試強制終止 executor，核對兩個 Driver 端點關閉，再以新 executor 讀回同一個隨機標記。
它不使用個人 profile、Keychain、AX 或付費 API。
2026-11-04 的主機驗證使用 Chrome 與 ChromeDriver `157.0.8103.38`，結果通過。
預設工具沙箱內的 Chrome 觀察失敗；相同隔離測試在一般主機環境通過。

```sh
cargo build --locked -p pulse-link --bin pulse-guardian
cargo test --locked -p pulse-link --lib process_guardian
cargo test --locked -p pulse-link --lib process_guardian::tests::reboot::verified_new_kernel_boot_retires_original_operation_and_reopens_core_runtime -- --exact --include-ignored
PULSE_TEST_BROWSER_DRIVER=/ABSOLUTE/PATH/chromedriver cargo test --locked -p pulse-link --lib process_guardian::tests::real_chrome_executor_kill_stops_both_drivers_and_allows_a_fresh_browser_read -- --exact --ignored
```

開機切換測試使用模擬的舊開機紀錄與真正核心提供的目前識別。
它核對原資源清理、Core runtime 重新接入及無關檔案保留。
這項測試沒有重新啟動電腦，不能代替實際斷電／重新開機驗收。
guardian 自身在同次開機內被強制終止、缺少原始紀錄、未知舊 profile 或舊 PNG，仍不能自動推定已清理。
這些情況會保留資料與未知結果，仍須完成後續恢復工作。
完整來源清除、保留期限、原生桌面、8 小時運行與 D01–D10／K15 尚未接受。
模型記憶仍關閉。
