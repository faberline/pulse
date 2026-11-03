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

## 原生 PNG

每個任務與修訂使用獨立的私有截圖目錄。
同一張圖片用在 A、B 兩個任務時，也分別保存，避免清除 A 時破壞 B 的原圖證據。
目錄權限為 `0700`，圖片為 `0600`。本機路徑仍不離開 Rust adapter。

來源撤銷先禁止該修訂再啟動 helper，並移除本機選擇與觀察。
Rust 先核對自有 helper 與程序群組已退出，再等待同來源的圖片讀取結束。
圖片讀取的保護會交給真正的 blocking 執行緒。
取消外層 future 不會提早解除這個保護。
停止或讀取結束無法確認時，清除失敗，不能回報已完成。
其他任務仍使用自己的目錄與讀取保護。

清除前會核對原目錄的裝置、inode、擁有者與權限。
只允許受管理的 PNG 及 UUID 暫存片段。
遇到目錄替換、符號連結、硬連結、錯誤權限或不認得的檔案時，保留資料並回報未確認。
清除失敗會保留目錄所有權紀錄，供同一 runtime 再核對。
成功清除後釋放目錄控制代碼；相同已撤銷修訂不能重新建立快取。

原有 128 張圖片上限仍套用於整個 cache，不是每個任務各 128 張。
操作前另保留兩張的空間，涵蓋 native preflight 與結果擷取。
最多 128 個目錄及 1,000 筆來源紀錄。

這只清除目前 runtime 持有且已核對的目錄。
舊共用 PNG 或同來源的未知舊目錄不會被猜測刪除，也不會被當成已清除。
runtime 結束時，沒有完整停止證據的目錄會保留。
舊資源復原、硬退出孤兒程序與到期快取回收仍待完成。

## 驗證

`cargo test -p pulse-link --lib owned_process` 核對程序退出、跨任務隔離、目錄更換與假關閉確認。
`cargo test -p pulse-link --lib computer_ --locked` 另核對分任務 PNG、取消中的讀取、寫圖 helper 退出、未知資源保留與全域容量。這些 helper 是受控 fixture，不是實際 AX 擷取或操作。
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
硬退出後的孤兒程序、舊 profile、舊 PNG、到期快取回收與正式還原仍須完整處理。
server 仍沒有獨立的已停止回報與地端工作容量核對。
來源清除的傳輸 ACK 仍不能作為執行或全域停止證據。
未知效果與已保留預算不變。

provider 送出與等待核對、明確父來源採用及部分工具／快照入口已有候選。其他 adapter 在途取消、自動規劃的完整來源繼承、關閉副本與完整實機驗收仍未完成。
這個候選不代表 D04、D05、D06、D08 或 K15 已接受。
