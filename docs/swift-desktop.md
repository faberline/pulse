# SwiftUI 桌面

2026-10-31，使用者確認介面可改用 Swift。這取代原 iced 介面選型。server、持久任務核心、chat 傳輸與 local executor 保留 Rust。remote 仍由 Pulse 自行實作，不接 AW。

## 元件邊界

`apps/pulse-desktop` 是 Swift Package，沒有第三方 Swift 相依套件。SwiftUI 呈現對話、設定與進度。AppKit 的 `NSTextView` 提供原生文字編輯、選取、復原與輸入法介面。

SwiftUI 只啟動自己的 `pulse-chat`。兩者透過私有 stdin/stdout 傳送有界 JSON Lines。這是對話框元件內的傳輸程序，不是 local executor。它沿用 Rust 的 Keychain、journal、角色檢查、重送與 WSS。密鑰不進入 SwiftUI 訊息或偏好設定。

`pulse-executor` 仍是獨立 local process。對話框不啟動、代理或終止它。關閉對話框只會結束 `pulse-chat`；父程序關閉 pipe 後，chat 也有有界退出測試。

## 畫面與輸入

主畫面只有一個可換行的對話框。Enter 換行，Command+Enter 傳送。草稿收到相同來源 ID 的保存回報後才清空。等待期間新增的編輯會保留。重連使用相同訊息及 task ID；切換 server 或 journal 不會自動重送舊輸入。

設定與進度預設收起。設定頁只有 server、配對與進階資料目錄。本機 repo 範圍仍由 executor 的啟動參數決定。SwiftUI 不接受 repo 參數，也不執行 repo 工具。

Markdown 文字、code 與有界表格使用原生元件。HTML 不執行。外部連結只接受沒有登入資料的 HTTPS。圖片只從人類選取的 PNG/JPEG 解碼，不從遠端訊息載入。讀取最多 5 MiB，尺寸最多 4096×4096，解碼首張影像並用實際尺寸建立預覽。圖片僅供本機預覽，尚未上傳。

非秘密連線設定使用 `pulse.swift.connection` 偏好鍵。舊 Rust 桌面設定檔不會被清除；首次改用 SwiftUI 可在設定重新輸入 server，並選回原 chat journal。現有任務與 journal 不會因介面遷移刪除。舊版混合身分的升級要求仍見 [v0.3](v0.3-desktop.md)。

## 建置與驗證

需要 macOS 14 或更新版本、可用的 Xcode／Swift 6 工具鏈，以及固定的 Rust 1.96.1。本機驗證工具鏈是 Swift 6.3.3。

```sh
swift test --package-path apps/pulse-desktop --scratch-path target/swift-desktop
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
sh scripts/build-macos-app.sh --debug
```

輸出為 `target/debug/Pulse.app`。app 包含 Swift `pulse-desktop`、Rust `pulse-chat` 與獨立 Rust `pulse-executor`。本機 ad hoc 簽章不等於 Developer ID 或 notarization。

連線及分開配對步驟沿用 [v0.3](v0.3-desktop.md)。開發時可將一次性配對碼保存在 0600 私有檔案，再加上 `--pair-code-file /PRIVATE/CODE_FILE` 啟動 app。此參數只使用一次，不保存到偏好，也不將碼放入 argv。重新啟動時移除此參數。

## 證據與缺口

Swift 與 Rust 的測試及 coverage 分開記錄。Rust coverage 已不含被移除的 iced crate，不能與舊候選直接比較。Swift 的 UI、圖片與 model 仍有未覆蓋部分。人工操作不會提高 headless coverage。

本次 49 個 Rust 測試與 11 個 Swift 測試通過。另有 140 次真正子程序中止樣本。Rust fmt/clippy 及 Swift format lint 通過，macOS app 的建置與 ad hoc 簽章驗證通過。

Rust 產品 source LCOV 為 2,977/3,424 行（86.95%）、281/367 functions（76.57%），無 branch instrumentation。Swift LLVM coverage 只加總 `Sources/PulseDesktop`，排除 tests 與測試 runner：306/1,424 行（21.49%）、58/197 functions（29.44%）。SwiftUI 畫面、文字元件及 `ChatModel` 仍為 0%；不能把 model/helper 測試說成完整 UI 接受。兩套工具的計數方式不同，不合併百分比。

本機已核對原生控制項、中文多行及 code 輸入、保存後清空、重啟後待送保留、設定返回與 PNG 選取／預覽。SQLite 中的換行原樣保存。修正短 pipe 訊息等待滿緩衝區的問題，並以保持運行的子程序驗證即時交付。

本次真實配對已在 server 兌換，但 Keychain 保存未完成，兩個身分沒有建立 lease。SwiftUI 的 authenticated 端到端、進度／取消／只讀操作及 Keychain 重啟讀回保持未接受。沒有改 Keychain 權限，也沒有明文 fallback。Rust 的 MemoryVault 整合測試不能代替此項。

完整中文 IME、全部滑鼠／鍵盤、正式遠端 WSS、效能／soak、provider 與 remote 執行閉環仍未接受。GitHub Actions 也須核對當前 SHA。既有帳務阻擋不會被本機通過取代。

原生元件依據：[NSViewRepresentable](https://developer.apple.com/documentation/SwiftUI/NSViewRepresentable)。套件與測試依據：[Swift Package targets](https://docs.swift.org/latest/documentation/packagedescription/target/)。這些文件不是本案驗收證據。
