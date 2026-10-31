# v0.1 需求對照、驗收與目前證據

日期：2026-10-30，Asia/Taipei。研究 issue：[#3](https://github.com/faberline/pulse/issues/3)，依賴 [#1](https://github.com/faberline/pulse/issues/1)、[#2](https://github.com/faberline/pulse/issues/2)。

來源是 [v0.1 milestone](https://github.com/faberline/pulse/milestone/1) 在規格工作開始前的需求。以下 ID 固定對應原文順序，不以後續新增文字重編。`Kxx` 指 [契約文件](contracts.md) 的章節。本文保留 v0.1 的證據快照；後續核心實作與測試子集見 [v0.2 核心](v0.2-core.md)，不能將子集通過提升成整張矩陣通過。

## 1. v0.1 的接受邊界

v0.1 交付設計與可行性證據。v0.2–v0.6 交付及驗證產品。文件完整、工具已安裝或官方文件存在，均不能證明產品功能通過。

| Gate | 完成需要的證據 | 目前狀態 |
|---|---|---|
| G01 架構 | Rust 選型、替代方案、官方來源、程序/資料/憑證邊界 | 文件已撰寫，等待獨立接受 |
| G02 契約 | 對話/任務、版本、授權、ACK、未知結果、費用與刪除規則 | 文件已撰寫，等待獨立接受 |
| G03 覆蓋 | 原核心 9 項、可靠性 16 項、必要行為 7 項與研究 5 類皆有對照 | 對照已建立；靜態檢查結果另列 |
| G04 Anthropic 可行性 | 帳號認證、精確模型/API version、完整工具提議、usage 與有界費用 smoke | **blocked / unrun：沒有 ANTHROPIC_API_KEY** |
| G05 v0.1 接受 | G01–G04 適用證據齊全，Codex 獨立接受，沒有未解必要 blocker | **未接受** |

API 認證只檢查環境變數是否存在，不讀取其他應用的私有認證。本次檢查結果為 false，未送出付費模型請求。金鑰須由私密設定注入，不寫入對話、repo 或證據檔。

## 2. 原始核心情境對照

下表 C01–C09 對應原 milestone「核心使用情境」九個 checkbox。負責階段須完成該行適用能力；v0.6 再對同一候選版本完整整合驗收。

| ID | 原始要求 | 契約 | 負責階段 | 測試 |
|---|---|---|---|---|
| C01 | Rust 桌面整合交付、rich content、背景不阻塞、renderer 不授權 | K05、K06、K12 | v0.3 | T10、T11 |
| C02 | 同一對話交錯 A/B，多任務關聯與獨立 context | K01–K04 | v0.2、v0.4 | T01 |
| C03 | 修改/取消 A 不誤改 B；歧義先問，不假裝回滾 | K02、K03、K08 | v0.2、v0.4、v0.5 | T01、T02 |
| C04 | 狀態查詢不建立或重跑背景工作 | K02、K06、K10 | v0.2、v0.4 | T01、T09 |
| C05 | 重啟恢復任務 current context、checkpoint 與等待 | K01、K03、K04、K07 | v0.2、v0.4 | T03 |
| C06 | 地端斷線不阻止雲端工作；恢復不重做效果 | K07、K08、K12 | v0.3、v0.4 | T05、T06 |
| C07 | 新決策取代舊方案，歷史仍可追溯 | K03、K04 | v0.2、v0.4 | T02 |
| C08 | 工具、第三方、摘要與推測不能創造批准 | K04、K05、K13 | v0.2、v0.4、v0.5 | T08 |
| C09 | 沒有 idle LLM loop，重複事件與輸出雜訊先過濾 | K07、K09、K10、K12 | v0.2、v0.4、v0.5 | T09、T12 |

## 3. 原始可靠性與邊界對照

R01–R16 對應原 milestone「支援以上情境的可靠性與邊界檢查」十六個 checkbox，順序不變。

| ID | 原始要求 | 契約 | 負責階段 | 測試 |
|---|---|---|---|---|
| R01 | 修訂/取消與舊 run 晚到結果競態，不復活舊決策 | K03、K04、K08 | v0.2、v0.4、v0.5 | T02、T07 |
| R02 | 無關歷史、過期決策、缺證據及 context 不足 | K04 | v0.4 | T04 |
| R03 | 離線 UI 誠實呈現快取、pending 及同步時間 | K06、K12 | v0.3 | T05 |
| R04 | 長輸出/重播時 UI 可用，executor 失敗可見且不重啟既有 run | K06、K07、K12 | v0.3 | T10、T11 |
| R05 | 重播去重，同 key 不同內容隔離 | K02、K06 | v0.2、v0.3 | T03、T06 |
| R06 | ingest commit 前後 crash 不遺失已確認事件/狀態 | K02、K07 | v0.2 | T03 |
| R07 | 裝置離線時雲端接訊、查詢、完成 cloud-only 任務，零等待喚醒 | K06–K10 | v0.3、v0.4 | T05、T09 |
| R08 | ACK 前後 crash、丟 ACK、缺序、重播、outbox 滿載 | K06、K07 | v0.3 | T06、T12 |
| R09 | 單一 lease、拒絕 stale epoch、未知寫入不重派、模擬效果核對 | K07、K08 | v0.2、v0.3、v0.5 | T07 |
| R10 | 人類控制不受背景阻塞，取消未知不誤報停止，量測延遲 | K03、K06、K08、K12 | v0.3、v0.5、v0.6 | T07、T10 |
| R11 | 拒絕、到期、digest/scope、錯裝置、撤銷、注入及離線限制 | K05、K08、K12、K13 | v0.3、v0.4、v0.5 | T08 |
| R12 | 有界 loop、費用/時間耗盡留進度、provider down/429 有限重試 | K09、K10 | v0.2、v0.4、v0.5 | T09、T13 |
| R13 | 真實 provider/Pulse remote/桌面 smoke，fixture 與實機、收件與成果分開 | K09、K13、K16 | v0.4 | T14 |
| R14 | 雲地重啟、網路、lease、provider、取消/撤銷、批准及備份故障矩陣/soak | K07–K11、K15 | v0.2–v0.6 | T03、T05–T08、T13、T15、T16、T18 |
| R15 | 終態證據、可恢復通知、不洩密與無模型健康查詢 | K14–K16 | v0.2、v0.5、v0.6 | T17、T19 |
| R16 | 當前候選的 deterministic CI/tests/coverage；依角色工作，不以別人自報代驗收 | K11、K16 | v0.2–v0.6 | T20 |

## 4. 其他必要行為與研究項

N01–N07 對應原「第一版必要行為」。它們不是額外增加的產品 scope。

| ID | 要求 | 已定義的位置 | 實作/證據歸屬 |
|---|---|---|---|
| N01 | 持久收件、事件零到多任務、去重及衝突 | K01、K02 | v0.2；T01、T03 |
| N02 | 進度、補充、批准、拒絕、取消及結果；人類控制優先 | K03、K05、K06、K12 | v0.2、v0.3、v0.5；T01、T07、T08、T10 |
| N03 | goal/scope/授權/優先/預算/revision/run/checkpoint/evidence | K01、K03、K10、K14 | v0.2；T02、T03、T17 |
| N04 | 只處理授權事件/排程，有界工作 | K05、K10 | v0.5；T09、T12 |
| N05 | 雲地重啟恢復；provider/資料不足原因與下一步 | K03、K07–K09、K16 | v0.2–v0.4；T03、T05、T13 |
| N06 | polling、重複事件與進度雜訊先確定性過濾，證據不丟 | K07、K10、K12、K14 | v0.2、v0.4；T09、T12、T17 |
| N07 | rich content、安全 renderer、正常互動 | K12 | v0.3；T10、T11 |

| 原研究問題 | 本次決定/文件 | 仍需的證據 |
|---|---|---|
| Q1 產品方式與首批工作 | 架構 U01/U05、D1/D5；Pulse A/B 情境；同一對話 | v0.3 UI 與 v0.4 真實閉環 |
| Q2 自主/資料/預算/保留 | 架構 U06–U08；K05、K10、K14、K15 | policy、費用、刪除及恢復測試 |
| Q3 provider/auth 可行性 | Anthropic Messages、明確模型與版本、雲端私密 API 金鑰 | **G04 尚未跑**；v0.4 provider 端到端 |
| Q4 Rust/UI/DB/transport/auth/hosting/context | 架構 A01–A09、D1–D5；K01–K13 | 編譯、相容性、實機與部署測試；選型不代替證據 |
| Q5 驗收/交付/coverage/soak | 本文 T01–T20 與證據格式 | 依當前候選逐階段產生；v0.6 完整接受 |

原始非目標維持：不建企業/多租戶平台、多 session UI、fleet、多裝置、graph/vector DB、任意自主工作、無限自我改善、任意遠端 shell、自動 merge/deploy 或 24/7 SLA。不複製 AW/CI controller，也不修改其他專案 rollout。

## 5. 後續產品測試矩陣

所有測試都有固定 seed 或明確故障位置。替身使用可控制時鐘、provider、transport 及可計數的模擬效果。實機測試另列平台、服務版本與真實認證路線。以下各列在 v0.1 建立時均 **unrun**；v0.2 僅驗證其 core 子集，完整情境仍需相應階段接受。

| ID | 輸入/注入 | 成功判準 |
|---|---|---|
| T01 雙任務 | A→B→A；一次關聯 A/B；status；含糊「取消它」 | 獨立 context；status 不開工；歧義不執行；只改指定任務 |
| T02 修訂 | A 執行中改決策/取消；舊模型或工具結果晚到 | 舊結果只作證據；有效決策不倒退；B 不受影響 |
| T03 收件/恢復 | commit 前後 crash；丟收件回覆；重送/衝突；重啟 | 已確認不丟；不重建 task；衝突可見；checkpoint 可恢復 |
| T04 context | 無關長歷史、過期方案、缺證據、超出輸入預算 | 只取有效必要來源；不能壓掉限制；不足明確 blocked |
| T05 離線 | 派送前斷線；全離線輸入；已快取 A、需本機 B；重連 | A 僅用有效雲端資料；B 等待；UI pending/cache 誠實；無重複操作 |
| T06 傳輸 | 接受/結果 commit 與 ACK 前後逐點 crash；丟 ACK、缺序、衝突 | 只 ACK durable 資料；原 ID 重送；補齊缺序；不丟未確認結果 |
| T07 未知/取消 | 效果前後 crash；lease 到期；新 epoch；取消/完成競態 | 模擬效果計數不增加；先核對未知；取消不冒充已停止；晚到不倒退 |
| T08 權限 | 錯 device/task/digest、scope 改變、拒絕/到期、注入、越界、撤銷、惡意 rich content | 無未授權效果；本機 kill switch 有效；離線撤銷限制如實呈現 |
| T09 loop/預算 | idle/等待；模型/工具/時間上限；兩任務同時預留；跨日/重啟/未知費用 | idle 模型請求為零；上限不因重啟重置；預留原子化；未知額度不釋放 |
| T10 UI/control | 長輸出、10 筆突發事件、重播、local executor crash；輸入/捲動/status/取消 | 控制不淹沒；UI 可用；不假報停止；延遲符合本節測量目標 |
| T11 rich content | 中文輸入、文字、code、table、image、artifact link、惡意 URL/HTML | 正常呈現；不能執行 script/命令；圖片不越權或自動外送 |
| T12 容量/排程 | outbox 接近/超限、磁碟滿、queue 滿、重複 timer、輸出風暴 | 預留結果空間；停領新工作；不丟 receipts；授權排程不重複開工 |
| T13 provider | 429/retry-after、5xx、延遲、斷流、拒絕、未知 stop reason、partial tool JSON | 有限請求；費用計入；未完整提議不執行；必要時 blocked |
| T14 真實閉環 | 真實桌面、Anthropic、Pulse 自有 remote executor，完成 A/B 情境 | 有 repo/HEAD/artifact/限制；收件不作成功；不靠 fixture 冒充 |
| T15 升級/rollback | 相容/不相容 schema、prompt/tool manifest 缺失、舊 run、舊 DB | 不靜默換版本；不重做效果；不相容明確停止或走驗證過的遷移 |
| T16 備份/刪除 | 在線備份、還原、缺 WAL、不完整 snapshot、已刪資料/撤銷身份 | 可驗一致性；拒絕壞快照；套用刪除/撤銷；不能復活授權或已刪正文 |
| T17 證據 | 成功/失敗/取消/unknown；trace 跨雲地/工具；刻意缺檢查 | 每終態原因可查；成功有適用證據；unrun 不變 passed；不洩 secrets |
| T18 soak | 8 小時週期 workload，穿插雲地重啟、網路分割、provider 失敗 | 完成全故障樣本；無遺失已接受資料、無重複模擬效果、queue/outbox 有界 |
| T19 通知/健康 | 通知送前後 crash、重連、重複通知；裝置離線與恢復 | 有界補送；桌面去重；授權入口；查健康不呼叫模型 |
| T20 CI/current head | 同一候選 SHA 跑 deterministic tests、fmt、clippy、coverage | 當前 SHA 證據齊；失敗/未跑不能接受；角色審查不取代 CI |

### 負載、延遲與樣本

首版測量採 20 個未結任務，雲端同時最多 2 個執行中任務，地端 1 操作。十筆突發事件可包含 status/control，不要求十個效果並行。每個故障切點至少執行 20 次固定 seed 樣本。記錄 seed、樣本數及故障切點，不以一個成功樣本代替矩陣。

macOS Apple Silicon 實機測 UI。記錄實際機型、OS、畫面設定、任務量與輸出速率。在每個 UI 壓力情境收集至少 200 次互動：從輸入/點擊送出至畫面反應，p95 目標小於 100 ms。雲端收件/status 的 p95 目標小於 1 s；在單機測試網路測至少 200 次，包含持久 commit，不包含模型回答時間。

另記 trigger-to-start、queue age、實際模型/工具延遲、最大 outbox 大小、task 成功率及 token/費用。目標是測試環境的接受門檻，不是公開 SLA。8 小時 soak 以一系列有界任務持續運行；每個 task 仍遵守 15 分鐘 deadline。

### CI 與 coverage

v0.2 起固定 Rust toolchain 與 Cargo.lock。在當前候選 SHA 上執行 `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`，並保存 cargo-llvm-cov 報告與實際版本。桌面依賴另在 macOS 驗證，不能只用 Linux core test 宣稱 UI 可用。

coverage 報告列出檔案/行/分支分母、排除項與命令。僅排除第三方/generated code，不把 domain/policy/recovery 排除來提高數字。核心故障行為必須有測試。v0.1 沒有產品碼，coverage 為 not applicable，不記 0% 或 100%；本次不新增未確認的 99% gate。

Codex 負責規劃與獨立接受。開發依 issue 走 AGY 生命週期；Claude 若參與，只檢查 exact issue 的缺漏/多做。任何角色的報告都不是 current-head CI 證據。v0.1 文件由本次工作提交，仍保持 issues 開啟待接受。

## 6. Anthropic v0.1 可行性 smoke

這是 G04 的一次小額研究呼叫，不是 T14 產品閉環，也不啟動真實 repo 工具。

1. 從受保護環境讀 `ANTHROPIC_API_KEY`，只記是否存在。缺少就停止，不搜尋或借用 CLI 登入資料。
2. 固定官方 endpoint、model `claude-sonnet-5-5`、API version `2026-10-21` 與一個無副作用的 `probe_echo` 工具宣告。只送合成文字，不送 repo 內容。使用 tool_choice auto，不依賴強制工具呼叫。
3. 請求模型提出 `probe_echo`，輸出上限 256 tokens，輸入估算上限 1,000 tokens，逾時 30 秒。不自動重試。設定及工具 shape 依已查閱的官方 API 契約，遇不相容回報失敗，不自動換模型或版本。
4. 先核對費率與最壞輸出估算，預留最多 US$0.01，計入當日/研究任務額度。無法確認上界就不送出。回覆含額外思考/計費項時一併核對；不以公開單價估算冒充實際帳單。
5. 只檢查完整回覆的模型、stop reason、工具 ID/name/參數及 usage。不得執行工具提議。模型沒有提議工具或 usage 缺失，G04 不通過。
6. 保存去敏的時間、API/model/rate version、request ID、合成 request/response、usage、預留/實算額及結果。不保存 API key、Authorization header 或真實私人來源。

認證、帳號 entitlement、費率或模型不可用時，G04 保持 blocked。實作/部署時可行性仍需重新確認，不能永久依賴一次研究成功。

## 7. 證據格式與目前執行紀錄

每份實作證據須包含：candidate commit/branch、dirty state、相關 issues、測試 ID、命令與 exit、環境/依賴版本、樣本/seed、開始結束時間、實際結果、去敏 artifact、限制與未跑項目。多階段實作成果不得混用不相同 SHA 的 passing 結果來宣稱整體接受。下表是 v0.1 規格提交時的歷史快照，不是 v0.2 的當前執行結果。

| 本次檢查 | 結果與限制 |
|---|---|
| repository 起始狀態 | main 沒有 commit；origin 為 faberline/pulse；沒有既有程式/PR/issues |
| Rust 工具 | rustc/cargo 1.96.1 存在；Darwin arm64；不是編譯結果 |
| 官方來源 | 已核對 Axum/SQLx/iced、SQLite WAL/backup、Anthropic Messages/model/versioning/streaming/token counting、JCS |
| 本次文件靜態檢查 | Python 標準庫一次性檢查通過：4 份文件的 UTF-8/換行、尾端空白、fences、全部相對連結；C01–C09、R01–R16、N01–N07、Q1–Q5、T01–T20、K01–K16、U01–U08、A01–A09、D1–D5 的唯一性/完整性及 gate blocker；不是產品測試 |
| Anthropic 環境 | 2026-10-30 檢查 ANTHROPIC_API_KEY presence=false；G04 unrun；無付費模型請求 |
| Cargo tests / coverage | unrun / not applicable，尚無 Cargo workspace 或產品程式 |
| UI/transport/provider/remote 產品閉環 | unrun；本次 AW 協作登記不是 Pulse 的 T14 證據 |
| 故障矩陣、效能、8 小時 soak、部署 | unrun，須後續產品實作與當前候選環境 |

G04 未完成時，issue #3 與 milestone v0.1 保持開啟。#1/#2 文件工作可報提交完成，但 `agy:done` 仍不代表獨立接受。不要勾選 G05 或宣稱已完成首版。

## 8. 後續拆 issue 的邊界

| 階段 | 下一批有界實作責任 | 前置條件 |
|---|---|---|
| v0.2 | source ingest/dedupe；task/revision/context 投影；queue/checkpoint；version/budget/outbox 與替身故障 | v0.1 契約接受；各 issue 指定 Kxx、Txx |
| v0.3 | SwiftUI 對話框／Rust chat 傳輸與獨立 local process；角色配對/WSS；ACK/outbox/lease；只讀工具與實機恢復 | v0.2 相應契約/核心證據；不可等 v0.5 才加 policy |
| v0.4 | Anthropic adapter/context；Pulse 自有 remote executor；A/B 真實只讀閉環 | G04、v0.2 核心與 v0.3 桌面路徑 |
| v0.5 | 完整批准/取消/撤銷；模擬效果核對；授權 schedule；預算/通知 | v0.4 已通；真實效果仍須明確另授權 |
| v0.6 | current-head 全矩陣、版本/備份/刪除、效能/soak 與可交付證據 | 前階段適用實作與證據，無未解必要 blocker |

本次只建立 #1–#3 研究 issues。不自動開工 v0.2、不建立雲端資源、不安裝付費服務，也不對其他 repo 開工。

2026-10-31：使用者將桌面介面改選 SwiftUI。原生移植的測試、實測與未接受項目另見 [Swift 桌面](swift-desktop.md)。舊 iced coverage 只屬於歷史候選。
