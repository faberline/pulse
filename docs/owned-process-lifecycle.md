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

## 正常關閉

Rust 地端收到關閉命令或輸入結束後，先結束連線、工作與本機控制通道。
資料目錄的鎖會保留到收尾結束，避免新的地端程序提早使用同一份資料。
憑證讀取或初始化失敗時，也會執行收尾。
控制通道使用建立 Device 時已選定的私有目錄。

收尾先禁止所有 adapter 接受新工作，再等待任何一個 adapter。
Git 與原生 helper 會收到停止要求，並核對子程序回收與程序群組消失。
瀏覽器會關閉目前持有的 session，再要求各自的 Driver 關閉。
已關閉的 executor 及其分身不能再次啟動工作；新的地端程序使用新的 runtime。

原生 helper 全部確認停止後，Rust 等待真正的圖片讀取結束。
之後立即清理目前持有且已核對的 PNG 目錄，不必等待到期回收。
遇到未知檔案、目錄替換、程序停止失敗或讀取尚未結束時，保留資料並回傳錯誤。
整個 executor 收尾有 25 秒上限。逾時不能當成已停止或已清除。
操作帳本與未知結果不會因此改寫，也不會自動重做效果。

直接中止 `Device::run` future 或強制終止程序，不會完成這個正常關閉流程。
瀏覽器 runtime 已停止新增獨立成果 PNG；原圖仍交給 journal 與 server 保存，見 [瀏覽器交接](browser-chat.md#boundaries)。
舊的瀏覽器成果 PNG、server／chat 副本、舊 runtime 的檔案、孤兒程序與正式恢復仍須另行處理。
這個結果也不是 server 的獨立停止回報，不能代替完整來源清除或實機接受。

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

Rust 的地端維護迴圈另會回收到期快取。
同來源最後一個圖片讀取保護釋放滿兩分鐘，且沒有仍可批准的觀察時，才可回收。
這個保護涵蓋 helper 呼叫與圖片讀取。
等待取得讀取保護的工作也算使用中。
回收還須核對該來源沒有待收尾或停止失敗的程序。
即使呼叫被取消而先釋放保護，回收仍須等待真正的程序收尾。
因此，已發停止訊號但尚未回收的程序不能通過。
時間間隔使用不會隨系統時鐘調整的計時器。
仍有效的批准來源另有保護，避免時鐘倒退時提早刪除原圖。

到期回收會釋放圖片、目錄與未撤銷來源紀錄，讓後續工作取得空間。
它不撤銷任務，也不移除來源撤銷紀錄。
回收後，相同未撤銷任務可重新觀察。
不認得的檔案仍保留；維護失敗會回傳錯誤，不能當成清除成功。
這不清除 server 或 journal 的成果副本，也不改動未知操作帳本。

這只清除目前 runtime 持有且已核對的目錄。
舊共用 PNG 或同來源的未知舊目錄不會被猜測刪除，也不會被當成已清除。
runtime 結束時，沒有完整停止證據的目錄會保留。
舊資源復原與硬退出孤兒程序仍待完成。

## 驗證

`cargo test -p pulse-link --lib owned_process` 核對程序退出、跨任務隔離、目錄更換與假關閉確認。
`cargo test -p pulse-link --lib computer_ --locked` 另核對分任務 PNG、取消中的讀取、寫圖 helper 退出、未知資源保留與全域容量。這些 helper 是受控 fixture，不是實際 AX 擷取或操作。
到期測試使用固定時間，涵蓋 130 個任務的容量回收、最後一次讀取、待取鎖工作、仍有效的批准、撤銷紀錄與程序回收。這不是 8 小時實機驗收。
假 Driver 回覆成功、但子程序仍存活時，profile 必須保留，停止檢查必須失敗。
關閉測試另核對有／無任務來源的程序、在途寫圖、正在讀取的 PNG、輸入結束、初始化失敗、資料鎖及錯誤回傳。

真正 Chrome 測試另須明確指定已安裝的 Driver：

```sh
PULSE_TEST_BROWSER_DRIVER=/ABSOLUTE/PATH/chromedriver cargo test -p pulse-link --lib real_chrome_source_stop_reaps_and_clears_only_its_owned_profile --locked -- --ignored
PULSE_TEST_BROWSER_DRIVER=/ABSOLUTE/PATH/chromedriver cargo test -p pulse-link --lib real_chrome_executor_shutdown_closes_both_owned_sessions_and_rejects_new_work --locked -- --ignored
```

測試使用自有 loopback 頁面與獨立 profile。
它核對撤銷 A 後 B 仍可讀頁，以及載入中取消後的停止與清除。
它也重查 profile 沒有立刻再次出現，且其他檔案未被移除。
測試不呼叫 provider，也不擷取真實 macOS 視窗。
既有雙任務點擊與輸入範例也須通過。

2026-11-04 的正常關閉元件驗證使用 Chrome 與 ChromeDriver `156.0.8078.80`。
兩個自有頁面回傳同一個隨機標記；關閉後兩份 profile 消失，原 KEEP 檔案保留。
再次關閉及關閉後的新讀取也已核對。
同一實機檢查另核對新 runtime 不建立額外 PNG 目錄；舊目錄已有 128 份檔案時仍能擷取，原圖可交接，舊檔的內容與權限保持原樣。
預設工具沙箱內建立 Chrome session 失敗；相同測試在正常主機環境通過。
這是程序與 profile 清理證據，不代表完整桌面、provider、強制終止復原或 8 小時驗收通過。

## 剩餘工作

這只涵蓋目前 Rust process 所持有的子程序與 profile。
硬退出後的孤兒程序、舊 profile、舊 PNG 與正式還原仍須完整處理。
server 仍沒有獨立的已停止回報與地端工作容量核對。
來源清除的傳輸 ACK 仍不能作為執行或全域停止證據。
未知效果與已保留預算不變。

provider 送出與等待核對、明確父來源採用及部分工具／快照入口已有候選。其他 adapter 在途取消、自動規劃的完整來源繼承、關閉副本與完整實機驗收仍未完成。
這個候選不代表 D04、D05、D06、D08 或 K15 已接受。
