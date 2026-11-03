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

## 明確附上文字檔

在 CLI 輸入以下兩行。路徑須為絕對路徑；含空白時不用加引號。

```text
:file /PRIVATE/pulse/report.md
任務 A：摘要這份檔案，列出來源。
```

`:file` 只在本機選取固定副本，不上傳，也不建立任務。
CLI 等 Rust 核對檔案後，顯示檔名、大小與摘要雜湊。
下一則訊息才會附上這份副本。原檔之後變更，不影響副本。
成功保存訊息後，CLI 移除未送出的選擇；後續訊息恢復一般輸入。
已保存的待送訊息保留原副本，直到既有傳輸流程確認。

送出前可輸入 `:clear` 移除選擇。它不撤回已送出的訊息。
只選檔便結束時，不會送出檔案。重新開啟 CLI 也不會自動附上舊選擇。
選檔或保存失敗時，CLI 以非零碼退出，不把原要求改成一般任務。
不支援的冒號命令會被拒絕，不會成為模型任務。
退出時只顯示固定的錯誤分類。系統憑證、啟動逾時、選檔與 journal 存取問題分別顯示。
錯誤中的任務文字、秘密值與檔案路徑不會直接印到終端。

檔案上限、私密路徑拒絕、固定來源及禁止檔案工具的規則，沿用 [文字輸入](text-input.md)。
絕對路徑不會進入 server 任務或模型內容。
這個入口沒有新增資料授權。操作前仍須確認檔案可以送給目前 provider。
沒有開啟模型記憶，也沒有變更模型、金鑰或預算。

2026-11-03 的 CLI 回歸測試使用真正 loopback server、Rust chat 與私有 journal。
測試核對選檔零任務、原檔改動後仍送原副本、下一則一般任務沒有附件、清除、重開與拒絕輸入。
這些測試沒有呼叫付費 API，不代表兩家模型的檔案輸入或完整 D09 已驗收。
真正執行檔的額外檢查仍未完成。新隔離場景在配對前退出，任務、裝置與費用紀錄均為零。
取得系統憑證存取的執行權限後仍未通過；原因尚未確定。
兩次 incomplete 報告保留在本機：
`/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-cli-file-native-xqkpbyle/report.json`、
`/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-cli-file-native-9_lpjswl/report.json`。
第二次檢查已核對所持有的 server 與 CLI 程序均退出。

## 配對失敗回報候選

Rust 現在會用本機 `NativeState.pairing_failure` 回報配對失敗。
它只包含「系統憑證」、「server 明確拒絕」或「未確認」三種分類。
原始錯誤、憑證、配對碼與 server 回應正文不會進入這個欄位。
系統憑證失敗另保留一個非機密的數字代碼。它不包含服務、帳號或憑證內容。
此欄位不改變 server wire、裝置身分或配對權限。

尚未配對的 CLI 收到失敗分類後，顯示原因並以非零碼退出。
它不再等到 30 秒啟動期限，才顯示通用的逾時訊息。
下一次明確配對及成功配對會清除舊分類。
已有有效身分時，失敗的新配對不會使 CLI 自動放棄原身分。

失敗測試先在假的憑證儲存點停住，再注入錯誤。
初始化沿用 30 秒啟動期限。注入錯誤後，仍須在 5 秒內退出。
儲存點放行前，server 必須沒有收到任何請求。
測試也保留未保存憑證不送出配對、沒有任務，以及錯誤不洩漏機密的檢查。

失敗回報不刪除待送工作、不重送模型，也不以明文檔案取代系統憑證。
真正執行檔的三次原始失敗紀錄仍保留。
2026-11-04 的同資料重開取樣只有等待狀態，無法證明原配對失敗的原因。
待確認的配對仍不能當作檔案傳送驗收。
新執行檔沿用原隔離資料後，已回報「系統憑證存取失敗」。當時仍是零任務、零裝置、零費用紀錄。
只讀的儲存庫狀態查詢顯示已解鎖、可讀與可寫；這不能證明個別憑證操作可成功。
修正後的觀察報告是 `/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-cli-pair-observe-kfapyx9a/report-corrected.json`。
原觀察報告誤讀舊日誌名稱。修正報告改讀本次日誌，沒有重跑程序，也沒有刪除原報告。
保留數字代碼後，真正 CLI 回報 `-25293`。
目前 macOS SDK 的 `SecBase.h` 將它定義為 `errSecAuthFailed`，即認證／授權失敗。
同一執行檔的嚴格簽章檢查通過，但不能據此宣稱 Keychain 已接受它的存取。
原配對檔與資料庫仍保留；該次仍是零任務、零裝置、零費用紀錄，所持有程序均已退出。
報告是 `/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-cli-pair-code-sgan9t68/report.json`。
這項實機配對限制尚未解除，未改動系統存取設定。

## 獨立 local process

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

## 停止與重開 server

在 server 所在終端按 Ctrl-C，或向該程序送出 SIGINT。
Rust 停止輪詢工作迴圈，並停止接受新 HTTP 連線。
既有 HTTP 請求最多有 2 秒收尾時間。用戶端沒有送完正文時，server 仍會退出。
這個期限不會清除已保留的模型費用，也不會把未確認的操作改為成功。
停止 server 不代表獨立 local process 已退出或工具效果已核對。

以原資料庫與原連線位址重新啟動，再用原裝置憑證連線。
裝置的連線世代會更新。原訊息因收件確認遺失而重播時，仍沿用原識別，不建立第二個任務。
用戶端仍須保留自己的原 journal；server 的收件不能取代用戶端保存或工作驗收。

```sh
cargo test -p pulse-link --test server_lifecycle --locked
```

2026-11-04 的真正 server 子程序測試已通過兩個案例。
未完成 HTTP 正文的案例先收到 `100 Continue`，證明 server 正在等待正文，再驗證 5 秒內退出及連線關閉。
另一個案例保留 chat／local 兩條認證連線，核對正常停止、原位址重開、兩個原身分重連及原訊息重播。
最後仍只有一個合成任務、兩個裝置，模型費用與地端讀取紀錄均為零。
測試憑證只存在測試程序的記憶體內。Keychain、實際 local executor、在途付費請求及完整故障矩陣仍須另外驗收。

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

## 兩家 API 與程序重開證據

2026-11-03，使用者明確核准本次兩家 API 的目的地、資料與費用範圍。
真正的 Rust server、CLI 與獨立 local process 已通過下列案例。
產品來源與執行檔摘要對應 `15cd938ad4b25c7fb62cd13d50867ce9944e102d` 的 Rust 程式。

| Provider／模型 | 任務 | 模型呼叫 | 完成讀取 | 本機費用估算 |
|---|---:|---:|---:|---:|
| OpenAI／`gpt-6-luna` | 2 | 4 | 2 | 1007 micro USD |
| Anthropic／`claude-haiku-5-5` | 2 | 4 | 2 | 2071 micro USD |

兩家都回覆正確的三個元件與 workspace 成員。
local 讀取摘要符合隔離副本。模型使用有界證據，沒有宣稱讀完被截斷的內容。
CLI 顯示回覆後正常結束。三個程序重開時沒有提供新的配對碼。
原回覆、任務識別、用量與讀取派工紀錄保持一致；每個角色仍只有一個身分，epoch 為 3。
新案例沒有未知呼叫。任務保持 `blocked / model_reply_ready`，等待人類核對，不自動宣告完成。

本機報告是 `/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-server-cli-live-jhqj1nes/report.json`。
這是本輪 CLI／API 功能證據，不是完整故障矩陣、正式部署或整體 Dot 接受。

## 中斷案例與未確認費用

第一次付費檢查的腳本未處理 null checkpoint，因此在首輪中止。
修正後加入不使用 API 的回歸測試，並接入一般 CI。
原資料庫重開後，該任務保持 `previous_call_not_retried`。
同一 call 的預留與識別未變，沒有重送或清除未確認費用。
另一個原已排隊的任務完成，保守用量估算為 655 micro USD。

該中斷呼叫仍保留 US$0.02 預留，不能視為已知實付費用。
包含上述成功案例，本輪已知用量估算合計為 US$0.003733；未確認預留另計。
故障資料庫與原 incomplete 報告保留。
重開核對紀錄是 `/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-server-cli-live-8pdabsa9/openai/recovery-check.json`。
