# Rust 子程序停止與 profile 清理候選

這是完整日常助理目標的前置工作。
Swift 仍只處理介面與輸入。
Rust 管理 ChromeDriver、原生 helper 與 Git 子程序。
模型記憶仍關閉。

## 停止順序

Rust 以任務與修訂追蹤子程序。
來源被撤銷後，相同任務修訂不能再啟動子程序。
另一個任務仍可繼續。
程序紀錄最多 64 筆，撤銷範圍最多 1000 筆。

原生 helper 與 Git 收到停止要求後，Rust 等待子程序退出。
Rust 也核對原程序群組已消失。
發出訊號不代表已停止。
回收子程序與發出訊號共用同一個鎖。
已回收的 PID 不會再被拿來發出停止訊號。

ChromeDriver 另有明確關閉流程。
Rust 使用本次自有 Driver 的 loopback `shutdown` 端點。
Rust 要求有效回覆、成功退出與程序群組消失。
這個端點會處理所有 session，見 [Chromium 實作](https://chromium.googlesource.com/chromium/src/+/main/chrome/test/chromedriver/server/http_handler.cc)。
單獨收到關閉回覆，仍不能通過停止檢查。

## 私有檔案

新的 Chrome profile 明確以 `0700` 建立，只供目前帳號存取。
Rust 保留 profile，直到停止檢查完成。
清除前，Rust 核對目錄的身分、擁有者與權限。
目錄被更換時，Rust 不刪除替換進來的檔案，也不回報完成。

工作 future 被取消時，Rust 把子程序與 profile 交給回收工作。
回收工作仍須完成停止與清除檢查。
若回收工作本身被中止，profile 會保留，不能自動當成已清除。
沒有有效停止證據時，Rust 保留失敗狀態。

## 驗證

`cargo test -p pulse-link --lib owned_process` 核對程序退出、跨任務隔離、目錄更換與假關閉確認。
假 Driver 回覆成功、但子程序仍存活時，profile 必須保留，停止檢查必須失敗。

真正 Chrome 測試另須明確指定已安裝的 Driver：

```sh
PULSE_TEST_BROWSER_DRIVER=/ABSOLUTE/PATH/chromedriver cargo test -p pulse-link --lib real_chrome_source_stop_reaps_and_clears_only_its_owned_profile --locked -- --ignored
```

測試使用自有 loopback 頁面與獨立 profile。
它核對撤銷 A 後 B 仍可讀頁，以及載入中取消後的停止與清除。
它也重查 profile 沒有立刻再次出現，且其他檔案未被移除。
測試不呼叫 provider，也不擷取真實 macOS 視窗。
既有雙任務點擊與輸入範例也須通過。

## 剩餘工作

這只涵蓋目前 Rust process 所持有的子程序與 profile。
硬退出後的孤兒程序、舊 profile、PNG 快取與正式還原仍須完整處理。
server 仍沒有獨立的已停止回報與地端工作容量核對。
來源清除的傳輸 ACK 仍不能作為執行或全域停止證據。
未知效果與已保留預算不變。

provider 送出核對、在途取消、來源繼承、關閉副本與完整實機驗收仍未完成。
這個候選不代表 D04、D05、D06、D08 或 K15 已接受。
