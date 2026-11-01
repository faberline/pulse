# 專用瀏覽器觀察基礎

工作單：[#19](https://github.com/faberline/pulse/issues/19)。對應 [日常助理 D03](dot-parity.md)。這是開發用 adapter，尚未接進 server、模型或 SwiftUI 工具。

`FixtureBrowser` 經本機 WebDriver 建立新 Chrome session。沒有 attach 既有 profile 或接受任意 JS 的介面。只發送固定命令：建立／刪除 session、導覽、取得目前 URL、標題、body 文字與 PNG 截圖。session 與 element ID 先核對，再組成固定路徑。

driver 與 fixture 的啟動參數僅接受帶明確非零 port 的 `http://127.0.0.1/` root。不能帶帳密、query、fragment 或額外 path。導覽目標須符合指定 fixture origin。這不是 browser 的網路隔離：頁面 redirect、子資源、DNS、下載及一般網站的權限控制仍是後續整合門檻。產品與模型不會呼叫此 adapter。

driver HTTP 禁止 proxy 與 redirect。每次命令最多 10 秒，整份觀察最多 15 秒。每份回應最多 4 MiB；文字最多 32 KiB，截斷保留 UTF-8 邊界並標示。截圖須符合 PNG header 與有界尺寸。觀察前後核對 URL，失敗或逾時後停止該 session 的後續觀察／導覽；仍可明確嘗試清理。清理失敗不能當作已關閉。

## 重現原生測試

需要另外準備與已安裝 Chrome 相容的官方 ChromeDriver。程式不自行下載或安裝它。指定絕對路徑：

```sh
cargo run -p pulse-link --example browser_fixture --locked -- /ABSOLUTE/PATH/chromedriver
```

示例啟動自己的 loopback 頁面、driver 程序群組及 headless Chrome。成功時保存 private 暫存資料夾：

- `observation.json`：URL、標題、文字、時間、瀏覽器版本與截圖 digest。
- `screenshot.png`：真正 Chrome 截圖。
- `cleanup.json`：已確認的 session 關閉、driver PID 與已回收狀態。

示例在返回前關閉 session，終止自身建立的程序群組並等待 driver 退出。它不讀個人登入資料，不呼叫模型，也不提交表單。

## 2026-11-01 證據

macOS 上實際使用 Chrome `156.0.8078.79`、官方已驗簽 ChromeDriver `156.0.8078.80`。取得中文文字與 PNG，已檢視截圖。最終 fixture 位於 `/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-browser-fixture-LJBjNB`。另以作業系統程序清單核對自有程序群組 `98328` 已無殘留。

`cargo fmt`、workspace clippy 與 75 個正常 Rust 測試通過。新增六項 browser 測試涵蓋固定命令、session／element 路徑注入、非 loopback、越範圍導覽、外部頁面、driver redirect、未知錯誤、超大回應、錯誤 PNG、清理失敗與真實 10 秒逾時。CI 使用 mock driver；原生證據另外保存，不能混稱為一般 CI 的 browser 實測。

上述暫存路徑與 PID 是本機歷史證據，不是可攜的 CI artifact。一般網站、對話中的 browser 工具、畫面新鮮度、批准、取消／重啟核對、下載與網路權限尚未完成，不能宣稱 D03 已接受。

協定依據：[W3C WebDriver 工作草案](https://www.w3.org/TR/webdriver2/)、[ChromeDriver 啟動](https://developer.chrome.com/docs/chromedriver/get-started)、[ChromeOptions 與新暫存 profile](https://developer.chrome.com/docs/chromedriver/capabilities)。
