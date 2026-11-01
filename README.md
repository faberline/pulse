# Pulse

Pulse 是單人使用的事件驅動助理。它有三個元件：server、對話框、executor。executor 分成 remote 系統與 local process。

## 目前狀態

目前提供 v0.2 任務核心，以及 v0.3 原生桌面與地端連線候選。server 保存任務與對話。SwiftUI 對話框透過自己的 Rust chat 傳輸程序連 server。`pulse-executor` 是獨立的 local process，也直接連 server。關閉對話框不會關閉 executor。

remote executor 由 Pulse 自己實作。它沿用現有 `pulse-core` 的持久任務、派工、checkpoint 與恢復機制。它不需要對應一台 VM，也不依賴 AW。實際模型與工具執行迴圈接續 v0.4。

v0.4 候選已接上自有 remote、Luna／Haiku 原生工具提議及 local 只讀證據迴圈。預設使用 Luna，可用 `PULSE_PROVIDER=anthropic` 選 Haiku。回覆及用量持久保存，模型不能自行驗收任務。兩家 API 的原生桌面 A/B 交錯、有效決策修訂、查詢、歧義澄清與重啟已實測。晚到結果已加入人類核對與安全續推，並通過真正斷線／server 重啟實測，見 [對話候選](docs/v0.4-conversation.md) 與 [核對候選](docs/v0.4-recovery.md)。詳見 [v0.4 remote](docs/v0.4-remote.md)。

主畫面只有一個可換行的對話輸入框。連線與配對在「設定」；任務與工具操作在可收起的「進度」。圖片以系統選取視窗開啟，不必輸入路徑。

本機 WebSocket 與恢復測試已驗證。三元件版本的實機證據見 v0.3 文件。正式遠端 WSS、完整畫面互動與效能驗收仍待驗證。真實模型已接入；雲端部署未接入。

Anthropic API 已通過小額文字與只讀工具實測。相關研究 issue 仍須獨立接受。

## 執行本機示例

需要 Rust 1.96.1。從 repository 根目錄執行：

```sh
pulse_demo_dir=$(mktemp -d)
cargo run --locked --example durable_demo -- "$pulse_demo_dir/pulse.db"
cargo run --locked --example durable_demo -- "$pulse_demo_dir/pulse.db"
```

第一次示例建立 A/B 任務、保存模擬操作、重開 DB，再核對結果。第二次重播相同來源，顯示原任務，不重複建立或執行。A 完成，B 等待裝置。這是替身證據，不會讀 repo、呼叫模型或執行外部工具。

DB 使用 private directory 與 0600 檔案。拒絕 symlink、其他使用者可寫的目錄及共享讀取權限。

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

[核心使用方式與驗證邊界](docs/v0.2-core.md) 列出對應契約、測試、coverage 命令與未實作項目。

桌面試用方式、配對與撤銷命令見 [v0.3 桌面與連線](docs/v0.3-desktop.md)。macOS 可用 `sh scripts/build-macos-app.sh --debug` 建立本機 app。SwiftUI 移植、建置與目前證據見 [Swift 桌面](docs/swift-desktop.md)。

## 規格入口

| 文件 | 內容 |
|---|---|
| [架構](docs/architecture.md) | 使用情境、Rust 選型、程序與資料邊界 |
| [契約](docs/contracts.md) | 任務、權限、傳輸、恢復、費用與資料保留 |
| [驗收](docs/acceptance.md) | 需求對照、測試矩陣、證據與目前缺口 |
| [v0.2 核心](docs/v0.2-core.md) | 可執行的 library、替身示例與當前測試界線 |
| [Swift 桌面](docs/swift-desktop.md) | SwiftUI、Rust chat 傳輸與原生實測 |
| [v0.3 桌面](docs/v0.3-desktop.md) | 三元件、分開配對與連線恢復 |

研究工作：[架構 #1](https://github.com/faberline/pulse/issues/1)、[契約 #2](https://github.com/faberline/pulse/issues/2)、[驗收與 API #3](https://github.com/faberline/pulse/issues/3)。

本次實作工作：[#4](https://github.com/faberline/pulse/issues/4)–[#7](https://github.com/faberline/pulse/issues/7)。使用者於 2026-10-30 明確要求開始實作；這不代表 v0.1 的 API 缺口已通過，也不關閉原有 gate。

v0.3 工作：[#9](https://github.com/faberline/pulse/issues/9)–[#12](https://github.com/faberline/pulse/issues/12)。候選堆疊在 v0.2 分支上。兩階段都須通過各自的獨立接受，才可合併。

## 交付順序

2026-11-01 已啟動日常桌面助理目標，包含瀏覽器與電腦操作。能力差距、實作順序與完成條件見 [日常助理驗收](docs/dot-parity.md)。目前只讀候選不代表這些新增能力已完成。

| 階段 | 交付 |
|---|---|
| [v0.1](https://github.com/faberline/pulse/milestone/1) | 需求、架構、行為契約與驗收計畫 |
| [v0.2](https://github.com/faberline/pulse/milestone/2) | 可恢復任務核心與替身測試 |
| [v0.3](https://github.com/faberline/pulse/milestone/3) | 桌面、可靠連線與受限地端執行器 |
| [v0.4](https://github.com/faberline/pulse/milestone/4) | Anthropic、自有 remote executor 與雙任務只讀閉環 |
| [v0.5](https://github.com/faberline/pulse/milestone/5) | 授權操作、結果核對與主動排程 |
| [v0.6](https://github.com/faberline/pulse/milestone/6) | 首版整合、故障驗收與運維交付 |

實作前須有對應 issue、依賴與驗收條件。`agy:done` 表示工作已提交，issue 仍保持開啟，等待 Codex 獨立接受。未執行、失敗或僅替身通過的檢查，均不是完整產品驗收。
