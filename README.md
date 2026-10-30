# Pulse

Pulse 是單人使用的事件驅動助理。雲端保存任務狀態。Rust 桌面 app 提供同一對話、地端連線與受限工具。

## 目前狀態

目前只有 v0.1 架構規格，沒有產品程式、可執行檔或部署。技術選型尚未經整合編譯或實機驗證。

Anthropic API 實測尚未執行。研究環境沒有設定 `ANTHROPIC_API_KEY`。v0.1 與相關研究 issue 保持開啟。

## 規格入口

| 文件 | 內容 |
|---|---|
| [架構](docs/architecture.md) | 使用情境、Rust 選型、程序與資料邊界 |
| [契約](docs/contracts.md) | 任務、權限、傳輸、恢復、費用與資料保留 |
| [驗收](docs/acceptance.md) | 需求對照、測試矩陣、證據與目前缺口 |

研究工作：[架構 #1](https://github.com/faberline/pulse/issues/1)、[契約 #2](https://github.com/faberline/pulse/issues/2)、[驗收與 API #3](https://github.com/faberline/pulse/issues/3)。

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
