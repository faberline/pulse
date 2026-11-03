# Rust server 與 CLI

2026-11-03，使用者要求先完成 server，允許以 CLI 傳送訊息。
本階段以 Rust server／remote／獨立 local process 驗證。
Swift 仍是可替換的介面。macOS 授權與畫面驗收留待之後。
沿用 #9、#14、#16 的傳輸、文字執行與只讀工具範圍。

## 啟動

從 Pulse repository 根目錄執行。先建立自己的私有資料目錄。
以下 `/PRIVATE/pulse` 代表你建立的絕對路徑。

```sh
cargo build --locked -p pulse-link --bins
mkdir -m 700 /PRIVATE/pulse
target/debug/pulse-server serve /PRIVATE/pulse/server.db 8721
```

server 讀取既有 private `.env`。預設使用 OpenAI。
使用 Anthropic 時，在啟動 server 前設定 `PULSE_PROVIDER=anthropic`。
模型、費用上限與未知結果規則沿用原設定。

另開終端產生 chat 配對檔案，再啟動 CLI：

```sh
target/debug/pulse-server pair-chat /PRIVATE/pulse/server.db /PRIVATE/pulse/chat.pair
target/debug/pulse-cli --server http://127.0.0.1:8721 --dev-loopback --data-dir /PRIVATE/pulse/chat --pair-code-file /PRIVATE/pulse/chat.pair
```

每行是一則訊息。任務、查詢、修訂與取消沿用 server 的自然語句。
輸入 `:quit` 或送出 EOF 結束。CLI 先等本機保存，再等 server 確認待送佇列。
管線也可使用相同入口：

```sh
printf '%s\n' 'A：請讀 README.md，列出三個元件。' | target/debug/pulse-cli --server http://127.0.0.1:8721 --dev-loopback --data-dir /PRIVATE/pulse/chat
```

`本機已保存` 只表示 journal 已提交。
`server 已收件` 表示待送訊息已確認，不表示模型分析或任務完成。
EOF 最多等 30 秒。未確認時以非零碼退出，不自動建立新任務重送。
使用原資料目錄重新連線，原 outbox 會沿用相同識別補送。

只看保存狀態與後續回覆：

```sh
target/debug/pulse-cli --watch --server http://127.0.0.1:8721 --dev-loopback --data-dir /PRIVATE/pulse/chat
```

`--watch` 不讀 stdin，也不建立新的 Human 任務。以 Ctrl-C 結束。
它仍會補送原 outbox，並核對既有結果。
chat 資料目錄只能由一個程序使用。CLI 與桌面不能同時開啟同一個目錄。
重新開啟時不再提供配對檔案；憑證沿用 NativeVault，目前 macOS 使用 Keychain。
沒有明文憑證 fallback。

需要讀取 Pulse repo 時，另開獨立 local process：

```sh
target/debug/pulse-server pair-local /PRIVATE/pulse/server.db /PRIVATE/pulse/local.pair
target/debug/pulse-executor --server http://127.0.0.1:8721 --dev-loopback --repo /ABSOLUTE/PATH/pulse --data-dir /PRIVATE/pulse/local --pair-code-file /PRIVATE/pulse/local.pair
```

關閉 CLI 不會關閉 server 或 local executor。
後續啟動 local 時也不再提供配對檔案。

## 邊界

CLI 只把文字交給共用 Rust chat。它不持有模型金鑰，不執行 shell，也不自動批准工具效果。
每行最多 4096 UTF-8 bytes。半行資料保留在有界 buffer，不因狀態更新而丟失。
輸出會移除終端控制字元。只保留目前投影的訊息識別與摘要，用來避免重複顯示。
沒有增加任務資料庫、server wire 版本或另一套 remote 排程。
新增的 `NativeState.input_receipt` 只回報目前程序的本機保存結果，不是 server 或任務收據。

終端或人類重新導向的輸出仍可能形成副本。這不證明完整資料清除或 K15 通過。
模型記憶保持關閉。原有預算、權限、來源及所有副本清除限制不變。
正式 WSS、部署、macOS 操作與完整 D01–D10 驗收仍待完成。

## 有界實機檢查

不使用模型時，先執行：

```sh
python3 scripts/check-server-cli.py --offline
```

這個模式清除子程序的模型環境變數，並指定不存在的設定檔。
它只用 localhost，核對真正程序的管線輸入、server 收件、Keychain 角色憑證及資料庫重開。
缺少 provider 的任務會保持 blocked，不會假裝已有模型成果。

已核准 API 測試後，可以執行：

```sh
python3 scripts/check-server-cli.py --live
```

這會使用既有 `.env`，產生兩家 provider 的付費用量。
每家在新的私有測試目錄建立 A/B 任務。
local 只連到一個隔離目錄，裡面只有 Pulse README.md 和 Cargo.toml 的固定副本。
真正 CLI、server 與 local process 使用這兩份副本。
它核對讀取內容摘要、模型回覆、用量及 CLI 收件。
它也核對原憑證與資料庫重開後，沒有新增模型呼叫或讀取派工紀錄。
本機費用是保守用量估算，不是 provider 帳單。
它不修改 key、模型或費用限制，也不自動重試未確認的模型呼叫。
一般 CI 不執行這個命令。通過此範圍仍不是完整故障矩陣或發布接受。

## 本機候選證據

2026-11-03 的不使用 API 實測已通過。
兩個任務經 CLI 管線與 EOF 送到真正 server。重開 server、local 與 CLI 後，任務識別未變。
server 仍只有一個 chat 與一個 local 身分，兩者 epoch 均為 3。
兩個任務都明確保持 `provider_not_configured`；budget 與 read 派工均為 0。
本機報告是 `/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-server-cli-live-341o5g02/report.json`。
報告保存執行檔摘要與產品來源摘要。這不是 CI artifact，也不是模型回覆的驗證。

本輪真正 OpenAI／Anthropic API 驗證未執行。
目前等待本次目的地、檔案內容與費用的明確授權。
較早候選的 API 證據不能取代目前候選驗證。
