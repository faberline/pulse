# Pulse

Pulse 是單人使用的事件驅動助理。雲端保存任務狀態。預定以 Rust 桌面 app 提供同一對話、地端連線與受限工具。

## 目前狀態

目前提供 v0.2 Rust 任務核心候選、SQLite 保存、替身示例與故障測試。完整桌面、HTTP/WSS、真實模型與部署仍未實作。此 library 只供可信的同程序程式呼叫，尚無網路認證入口。

Anthropic API 實測尚未執行。研究環境沒有設定 `ANTHROPIC_API_KEY`。v0.1 與相關研究 issue 保持開啟。

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

## 規格入口

| 文件 | 內容 |
|---|---|
| [架構](docs/architecture.md) | 使用情境、Rust 選型、程序與資料邊界 |
| [契約](docs/contracts.md) | 任務、權限、傳輸、恢復、費用與資料保留 |
| [驗收](docs/acceptance.md) | 需求對照、測試矩陣、證據與目前缺口 |
| [v0.2 核心](docs/v0.2-core.md) | 可執行的 library、替身示例與當前測試界線 |

研究工作：[架構 #1](https://github.com/faberline/pulse/issues/1)、[契約 #2](https://github.com/faberline/pulse/issues/2)、[驗收與 API #3](https://github.com/faberline/pulse/issues/3)。

本次實作工作：[#4](https://github.com/faberline/pulse/issues/4)–[#7](https://github.com/faberline/pulse/issues/7)。使用者於 2026-10-30 明確要求開始實作；這不代表 v0.1 的 API 缺口已通過，也不關閉原有 gate。

## 交付順序

| 階段 | 交付 |
|---|---|
| [v0.1](https://github.com/faberline/pulse/milestone/1) | 需求、架構、行為契約與驗收計畫 |
| [v0.2](https://github.com/faberline/pulse/milestone/2) | 可恢復任務核心與替身測試 |
| [v0.3](https://github.com/faberline/pulse/milestone/3) | 桌面、可靠連線與受限地端執行器 |
| [v0.4](https://github.com/faberline/pulse/milestone/4) | Anthropic、AW 與雙任務只讀閉環 |
| [v0.5](https://github.com/faberline/pulse/milestone/5) | 授權操作、結果核對與主動排程 |
| [v0.6](https://github.com/faberline/pulse/milestone/6) | 首版整合、故障驗收與運維交付 |

實作前須有對應 issue、依賴與驗收條件。`agy:done` 表示工作已提交，issue 仍保持開啟，等待 Codex 獨立接受。未執行、失敗或僅替身通過的檢查，均不是完整產品驗收。
