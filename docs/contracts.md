# v0.1 行為與介面契約

日期：2026-10-30。研究 issue：[#2](https://github.com/faberline/pulse/issues/2)，依賴 [#1](https://github.com/faberline/pulse/issues/1)。本文定義後續 Rust 實作，不表示介面已存在。

## K01：共通型別與資料權威

所有 ID 是 opaque 字串。時間保存為 UTC；每日費用依 `Asia/Taipei` 分桶。金額以整數 micro-USD 保存，不使用浮點累加。wire 上的 revision、sequence、epoch 與金額使用十進位字串，避免 JSON 數字精度差異。

| 型別 | 必要內容 | 權威與限制 |
|---|---|---|
| SourceEvent | event/source ID、actor、source_kind、時間、payload_ref/digest、因果與 task links | 來源內容不可由摘要回寫；修正用新事件；刪除見 K15 |
| Task | task_id、goal、scope、authorization_refs、revision、priority、status、reason、budget、parent/dependency links | 雲端 DB 唯一權威 |
| Decision | decision_id、task_id、來源、內容、supersedes、有效狀態 | 只有人類或既有授權能改執行決策 |
| Run | run_id、task_revision、版本組合、checkpoint、provider request IDs、limits、執行證據 | 與 provider session 不同；後者不能取代 run |
| Approval | approval_id、task/revision、operation、target、payload_digest、owner、expiry、decision、consumption | 操作綁定；不能用對話摘要替代 |
| Operation | operation_id、run、target、intent_digest、idempotency_key、epoch、狀態、receipt_refs | 外部效果核對單位 |
| Receipt | operation_id、裝置、實際結果、時間、artifact refs、證據來源、核對結論 | 收件、開始與完成是不同 receipt |
| Device | owner/device ID、capabilities、credential status、last_seen、active epoch、resume cursors | 身份不授予任務權限 |
| OutboxItem | message_id、destination、sequence、payload/digest、attempt、next_retry、ACK 狀態 | commit 後才能送；對端 commit ACK 後才能清理 |
| BudgetLedger | task/day/call ID、rate version、reserved/settled/unknown micro-USD、usage、狀態 | 費用未知時不釋放預留 |
| Artifact / KnowledgeRef | ID、來源、時間、scope、有效性、TTL、刪除狀態 | 知識不是批准；成果不是成功的唯一判準 |

資料可在同一 SQLite DB 中分表。SourceEvent 與任務用多對多 links，父子關係與依賴用關聯表，不採 graph DB。地端 journal 不複製雲端完整 task truth。

## K02：收件與對話關聯

1. 先核對身份、資料大小與語法。未通過者不進入模型。actor/source_kind 由可信入口核定，不接受 payload 自報「人類」或「已批准」。裝置工具結果不能經由改寫欄位變成人類控制事件。
2. 以 `(owner, source, source_event_id)` 去重。相同 key 與 digest 回傳原 receipt；不同內容回報衝突並隔離，不覆蓋原事件。
3. 在交易中保存來源事件與處理狀態，再回覆 accepted receipt。只保存事件時不虛構 task ID。
4. 後續 routing 以明確 task ID、回覆事件及已知 links 優先。問候與查詢可關聯零任務，一則事件可關聯多任務。
5. 推測關聯記為 proposed/uncertain。若歧義影響修改、取消、寫入或 scope，先提出澄清問題，不執行候選動作。
6. 修改多個任務須明示各自變更。任一 revision 不符，整筆控制變更不套用；來源事件仍保留並回覆衝突。模型不能靜默選擇部分任務。

DB commit 前 crash，來源可重送。commit 後回覆遺失，重送取得原 receipt。routing crash 從已保存事件續做，不建立重複任務。

## K03：任務狀態與修訂

主狀態只有一個。等待或錯誤細節放在 `reason`，不靠文字猜狀態。

| 目前狀態 | 允許的下一步與條件 |
|---|---|
| queued | 取得工作名額與當前授權後進 planning；已知缺裝置可進 waiting_device |
| planning | 驗完整模型結果後進 ready；需批准進 waiting_approval；資料/預算缺口進 blocked；可重試錯誤進 retry_wait |
| ready | 派送交易成功後進 dispatched；裝置未連線進 waiting_device；執行前需批准則進 waiting_approval |
| dispatched | 地端持久接受後進 running；送出後失聯且不能證明未接受，進 outcome_unknown |
| running | 有證據後進 succeeded/failed；需更多推理回 planning；未知效果進 outcome_unknown；需人類決策進 waiting_approval/blocked |
| waiting_approval | 批准且 revision/digest 有效後進 queued；拒絕或到期進 blocked，不自動重提 |
| waiting_device | 有可達且授權仍有效的裝置後進 queued；不因 heartbeat 自行啟動 LLM |
| retry_wait | 到期且仍可安全重試後進 queued；錯誤已不可重試則 blocked/failed |
| outcome_unknown | 保存未知原因後進 reconciling；不進 queued |
| reconciling | 證明已完成後進 succeeded/failed；證明未開始且授權仍有效後進 queued；無法核對則 blocked，保留未知 operation |
| blocked | 只有新的明確證據或人類決策解除原因後進 queued；仍有未知 operation 時先進 reconciling |
| cancel_requested | 無活躍操作或已核對停止後進 cancelled；效果不明進 outcome_unknown；晚到完成證據可如實進 succeeded/failed |
| succeeded / failed / cancelled | 不自動重開；後續工作建立新 task 或明確的新任務關係 |

所有非終態都可收到取消。收到取消先記 `cancel_requested`，阻止新派送。對「尚無任何已派送操作」可在同一交易中提供未開始證據並轉 cancelled。

任務每次控制變更增加 revision。以 `expected_revision` 做條件更新。新 revision 使舊 run 的下一步無效，但不刪除舊 run 的證據。晚到結果只能追加歷史或核對既有操作，不倒退終態或恢復舊決策。

v0.4 的只讀接續細化：改需求或取消會增加 revision。查詢、核對同一需求的既有 receipt，以及在已核對後續推，不改 goal/scope，保留 task revision。重新領取時更換 run epoch，拒絕原執行的下一步。這不重置費用、次數或 deadline。

模型只讀提議可在保存後由 planning 進 waiting_device；local 可用時以同一交易保存 intent/outbox 並進 dispatched。已知 receipt 只有在當前 run/revision/epoch 仍有效時，才可排入同一 run 的下一模型 round。其他晚到結果只作證據，停在 blocked/outcome_unknown。人類「核對」在單一 DB 交易內經 reconciling 驗證憑證，通過後停在 blocked；失敗回滾並保留未知。之後還需人類「繼續」，才可領取下一 round。此路徑不宣告任務成功。

## K04：context 與決策

每次推理只取該任務的目標、scope、有效決策、授權、必要工具契約與證據引用。保留各內容的 actor、來源時間與信任層級。

摘要是可重建的投影。它不能創造批准、略過限制或復活已取代決策。其他任務的資料須經明確 cross-link 與 scope 檢查才可引用。

預設每次模型輸入上限 16,000 estimated tokens，輸出上限 2,048 tokens，均包含協定所計費的適用內容。先移除無關或過期來源，再按需取必要證據；仍不足時保存缺口並 blocked。不得截掉限制來塞進 context。

使用者更新 task 後，下次動作必須重取 current state。模型回覆附原 task revision；不符時保存為舊 run 證據，不直接執行工具。

## K05：授權與本機工具

執行需要同時通過 owner/device 身份、當前 task scope、模型外 policy、本機 allowlist、task revision、版本、lease 與預算。任何一項未知則不執行。

首批工具如下。`repo_id` 只對應 Pulse 的管理設定，不接受雲端傳入任意絕對路徑。

| 操作 | 輸入與範圍 | 結果 |
|---|---|---|
| repo.list_files | repo_id、repo-relative path、有限 limit/cursor | 有界清單、截斷標記與 cursor |
| repo.read_text | repo_id、相對路徑、byte range、預期檔案版本 | UTF-8 內容、實際 digest/版本與截斷標記 |
| repo.git_status | repo_id | 固定格式的狀態、實際 HEAD 或 unborn 標示 |
| repo.git_diff | repo_id、明確 base/target 或 working-tree 選項 | 有界 diff、HEAD/來源版本與截斷標記 |

local executor 解析路徑並在開檔時檢查實際檔案與允許根目錄。拒絕 traversal、symlink 越界與讀取競態；不能只用字串 prefix 判斷。首版拒絕 symlink 檔案。允許的普通文字為 Markdown、Rust、TOML、JSON、YAML 與純文字；私密路徑拒絕規則優先。其他格式先等待明確擴充。

拒絕 `.env*`、`.git/**` 的直接讀取、`.aws/**`、`.ssh/**`、`.codex/**`、`.agents/**`、憑證/金鑰檔、忽略檔、build/dependency 目錄與其他 repo。內容去敏是第二道檢查，不能保證辨識所有 secrets。Git 子命令可內部讀自身 metadata，但不回傳原始私密 config，並停用 hooks、fsmonitor、外部 diff/textconv、pager 與互動認證。不得接收任意 command 字串或外部 fetch。

批准綁定 task/revision、operation、target、intent digest 與到期時間。範圍或內容改變就失效。拒絕及到期只保存結果，不自動再問。批准只可重送同一 operation 的傳輸訊息，不能授權另一個操作。

v0.5 以前，真實寫入 adapter 不存在或停用。v0.5 必測模擬寫入；真實寫入仍須另外授權。native client 的批准交給其原生介面，不代按或從 tool output 推論已批准。

## K06：HTTP、WSS 與桌面程序介面

以下是 v1 語意介面，尚未實作。

| 介面 | 行為 |
|---|---|
| POST /v1/events | 認證後提交來源事件或人類控制；回傳持久收件 receipt，不承諾任務完成 |
| GET /v1/tasks、GET /v1/tasks/{id} | 游標分頁或單項目前狀態、revision、證據與等待原因；不啟動新工作 |
| GET /v1/timeline | 按游標讀對話與任務投影，標明最後同步位置 |
| POST /v1/device-pairings/redeem | 兌換單次配對碼；不可用此介面產生新配對碼 |
| GET /v1/devices/connect | 經認證升級為 WSS；握手交換 schema version、device ID、capabilities 與 resume cursors |
| GET /health/live、GET /health/ready | 無模型檢查；公開回應不含任務或身份；詳細健康資料須認證 |

批准、拒絕、取消、刪除與修改均是具來源的人類控制事件，經 POST /v1/events 處理；不另設可略過 event/revision 檢查的寫入入口。配對管理只透過受保護的本機管理程序，不公開 owner 管理 API。

HTTP 回應區分 accepted、duplicate、conflict、rejected 與 temporarily_unavailable。相同 key 不同內容、revision 衝突或不相容協定不能自動重試。原事件已接受但後續任務 blocked，仍回覆真實的原 receipt。

對話框與 local executor 各自使用 WSS JSON envelope：`schema_version / kind / message_id / device_id / sequence / task_id? / run_id? / task_revision? / epoch? / payload / payload_digest`。ACK 使用獨立 kind，引用被確認的 message ID 與連續 cursor。ACK、heartbeat 不再要求對端 ACK，避免確認迴圈。

payload digest 為 SHA-256，輸入是按 [RFC 8785 JCS](https://www.rfc-editor.org/rfc/rfc8785) 正規化後的 payload。重複 JSON keys、非有限數字與不相容型別直接拒絕。operation intent digest 另包含 task/revision、operation、target、內容與授權綁定，不只 hash 顯示文字。

每方向 sequence 都在本地交易中配置。只 ACK 已連續 commit 的資料。發現缺序時要求從 cursor 補送，不越過缺口派送效果操作；同 ID/sequence 不同內容隔離並告警。重送保持原 message ID、sequence 與 digest。傳輸重送不產生新 operation ID。

UI 與 local executor 分別連 server，各自保存 journal 與 credential。UI 不代理工具連線，不負責 executor 的程序生命週期。UI 顯示 `pending_local / accepted_cloud / cached_offline`，不能把本機保存當作雲端收件。控制與資料使用分開的有界邏輯 queue；傳送端優先控制，且每次寫入有界，不能在巨大 frame 後無限等待取消。

## K07：commit、outbox、重連與容量

地端收到操作時，先保存 message 去重紀錄、operation 接受紀錄與 outbox，再 ACK。每次工具動作前重新核對 K05。工具結束先保存 receipt 與結果 outbox，再傳回雲端。雲端在交易中保存結果、任務條件更新與通知 outbox，commit 後才 ACK 結果。地端收到雲端 ACK 才可刪除對應 outbox payload。

重連先交換 cursor、補送舊資料、核對 active/unknown operations，再接受新操作。UI 重啟不直接重啟 run。缺少原去重資料時要求重新同步，不能把未知訊息當新工作。

| 項目 | 首版預設 |
|---|---|
| 單一 JSON frame/HTTP event | 最多 256 KiB；更大輸入拒絕並提示分段 |
| 單次 read_text/diff 結果 | 最多 64 KiB；須標示截斷，不能假稱完整 |
| 地端未確認 outbox | 最多 1,000 筆或 64 MiB，以先達者為準 |
| local executor 工具輸出暫存 | 最多 1 MiB/操作；達限即截斷或停止，不吃滿記憶體 |
| 雲端待處理任務 | 最多 100 個；滿載拒絕建立新工作，控制現有任務仍可用 |
| 重連/傳送退避 | full jitter，基準 1 秒，最大 60 秒；每組最多 8 次後 cooldown 5 分鐘 |

裝置在線時，cooldown 到期可由確定性 transport 再嘗試。斷線期間不啟動模型。未確認資料保持 durable，不因達重試上限刪除。

先為已接受操作預留結果空間，再領新工作。outbox 滿載即停領新工作並回報容量不足。清理只能移除已確認且符合保留規則的資料。DB/磁碟錯誤時停止新增效果操作；容量限制不能成為丟棄既有 receipt 的理由。

## K08：lease、取消與未知效果

雲端為每個裝置工作配置單調 epoch。lease 預設 60 秒，裝置每 15 秒續租。使用本機單調計時估計剩餘有效期；本機重啟視為沒有有效 lease。每次開始動作前重新驗證。失去續租能力時不開始下一動作。

fencing 只可阻止接受新指令或寫入新狀態，不能撤回已送出的外部 request。若已派送後失聯，保存 outcome_unknown。未確認原操作停止前，不將同一效果派給新 worker。

收到取消或撤銷時，先保存事件並阻止新動作。local executor 嘗試停止目前操作，回報實際結果；無法證明停止時不可標 cancelled。已完成的操作不會因取消而消失。離線撤銷最晚要等裝置重連或本機 lease 失效，不能宣稱立即控制離線程序。

未來 write adapter 必須提供 intent、冪等鍵、receipt 與結果查詢方式。核對證明已做就保存結果；證明未做且授權仍有效才可新嘗試；零個或多個不明候選保持 blocked。即使 adapter 宣稱冪等，仍核對目標、內容及實際結果。沒有查詢/核對契約的 adapter 停用。

## K09：Anthropic 模型邊界

首版使用 `POST https://api.anthropic.com/v1/messages`、明確 model ID `claude-sonnet-5-5`、API version `2026-10-21`，不使用 `latest`。送出完整且有界的必要 Messages context。Anthropic Messages 的回覆不是 Pulse 的任務真相。

Rust provider 接口接收 task/run/revision、version set、messages、工具宣告、輸出/時間/費用上限與取消訊號；輸出只有文字、完整工具提議、usage、provider request ID 或型別化錯誤。它不直接執行工具。

本版可串流顯示文字，但完整 message、stop reason 與工具參數完成驗證並持久化後才可提議執行。中斷的 JSON/工具 delta 不可執行。模型的 thinking/internal blocks 不作人類可見推理或授權；按 provider 契約必要時保存並原樣續傳，仍受資料保留與刪除限制。

模型拒絕、不完整輸出、長度耗盡、未知 stop reason 或 schema 不符，保存原因並等待/失敗，不宣稱成功。模型不接受特定設定時回報相容性問題，不偷偷移除設定、改模型或改 API version。

429/明確暫時性服務錯誤最多 3 個 API 嘗試，遵守 retry-after 與總時間/費用限制。每次嘗試都記錄並預留費用，計入每任務模型呼叫數。送出後連線中斷可能已計費；無法核對的呼叫保留 unknown 預留，不以一般 retry 規則盲目重送。工具效果未知時必先走 K08。

provider 取消只表示停止等待或要求停止傳輸，不保證供應商停止計費，也不回滾已發生的工具效果。

## K10：預算與排程

| 上限 | 預設 |
|---|---|
| API 日額 | US$1 = 1,000,000 micro-USD，Asia/Taipei 日界 |
| 每任務 API 總額 | US$0.25 = 250,000 micro-USD，跨 run/重啟累計 |
| 任務 wall time | 15 分鐘/任務，自建立起跨 run/重啟累計，包含等待；到期保存 checkpoint 並 blocked，不重跑未知操作 |
| 模型請求 | 最多 8 次/任務，包含重試及必要對話推理 |
| 工具提議執行 | 最多 16 次/任務，傳輸重送不重算操作 |
| 同時執行 | 雲端最多 2 任務；地端最多 1 操作 |

人類、裝置與重試等待不占執行名額，但仍受上述 deadline 約束。到期後要有明確新指示才能延長 deadline，模型不能自行重置時間或費用額度。

所有模型呼叫都要有預算 bucket。未關聯任務的對話推理使用單事件 bucket，上限 US$0.25，並計入日額；不為計費而強制建立 task。純狀態查詢可直接使用權威投影，不必呼叫模型。

預留與 task/day 檢查在同一 DB 交易完成。以模型輸入估算、最大輸出及已核對費率計算保守預留，向上取整；不假設 cache 折扣。費率或 token 上界無法確定時停止該呼叫。實際 usage 回來後結算差額。計數估算可能不同於帳單，須揭露差異，不能把應用額度當供應商硬性帳單保證。

日額切換不釋放舊日未知費用；跨日進行中的請求歸原 reservation 日。後續呼叫使用新日額，task 總額仍累計。預算耗盡進 blocked，保存 checkpoint；不得自動購買額度、換帳號或改用更貴模型。

只允許明確授權的 timer/事件訂閱。保存 schedule ID、授權、scope、時區、下一到期時間及有限工作。到期先由規則去重/合併，才喚醒模型。停用排程不抹除既有任務；已有任務仍依其狀態與授權處理。

## K11：版本與相容性

每個 run 保存 `workflow_version / schema_version / prompt_version / tool_contract_version / model_id / provider_api_version / model_policy_version`，與 task revision 分開。版本內容由不可變 manifest/digest 引用。新版本只啟動新 run；舊 run 使用相容版本或等待明確遷移。

wire major 不相容時握手拒絕；未知操作 kind 不執行。migration 以單一操作者、備份與版本檢查執行。不相容版本、缺 prompt/tool manifest 或缺 provider 必要歷史時 blocked，不用新的設定硬續跑。

rollback 先停止新 dispatch，再核對活躍與未知 operation。可相容的舊 run 保留原版本；無法相容則停止接續。不能以還原 DB 撤銷真實效果、重新啟用已撤銷身份或遺忘刪除事件。

## K12：UI、rich content 與控制優先

UI 使用同一對話 timeline，呈現涉及任務、最後進展、證據、等待原因與最後同步時間。它可以顯示候選關聯，但不建立「所有訊息永久屬於目前任務」的隱藏模式。

格式文字、程式碼、表格、圖片及成果連結正常呈現。HTML/script 不執行。圖片僅顯示經驗證的 Pulse artifact 或使用者已選的本機來源；不自動讀任意 file URL 或載入追蹤遠端圖片。外部連結須人類點擊，拒絕 javascript/custom execution schemes。

background output 可合併，批准、取消、最終結果及同步狀態不可被丟棄。local executor 錯誤顯示為執行器不可用，不當作任務成功。local executor crash 後 UI 保留快取與輸入能力，重啟 local executor 後依 K07 恢復。關閉 UI 不會關閉 executor。

桌面只在雲端 commit 後顯示已接受。完全離線時可保存有界 pending input；不得代雲端批准、執行新動作或聲稱看到即時雲端狀態。

## K13：Pulse 執行系統邊界

remote executor 由 Pulse 自行實作，不使用 AW runtime、mailbox、admission 或 skills 系統。沿用 `pulse-core` 的任務、run token、checkpoint、重試與取消。faberline/core 只在能減少重複程式時使用。

server 決定授權與派工。executor 只執行已核對的工作。工具與模型回覆保持資料來源，不得轉成人類批准。收件、進度或工具結果都不代表任務目標已完成。

## K14：證據、通知與健康

每個進度事件包含 task/run/trigger/device/tool call/policy decision 的適用 ID，以及 trace ID 或等效關聯鍵。不適用欄位可空，不虛構 ID。成功結果保存 artifact/receipt、適用 repo/HEAD、來源版本、檢查命令與 exit、時間及限制。未跑檢查明記 unrun。

通知只送授權的 Pulse 桌面入口。以持久 outbox、穩定 notification ID、去重與節流發送。裝置離線只形成一則狀態變更通知並彙整任務，恢復後補報。不具通道去重能力時標示可能重複，不能聲稱 exactly-once。

健康資料包括 queue age、running/waiting/blocked 數、last_seen、lease/ACK lag、outbox backlog、最近 provider 成功、restart reason、模型 token/費用與無效喚醒數。健康查詢不需要模型。logs 不保存完整 prompt、秘密、原始 repo 內容或多餘私人路徑。

## K15：保留、刪除與備份

完成任務與適用來源內容自 terminal_at 起保留 90 天；除錯 logs 自寫入起 7 天；一致性備份自建立起最多 30 天。活躍任務不因 90 天自動失去恢復資料。共用來源在最後一個引用任務完成後起算，已刪除者除外。可重用知識預設最多 90 天，續留需新的有效來源或人類決策。

明確刪除事件先阻止相關任務新動作並撤銷執行 context，再移除 payload、artifact、摘要、檢索及地端 cache 的適用副本。若仍有未知效果，保留最少 opaque operation/tombstone metadata 並阻止自動重跑，不保留已要求刪除的正文或可識別內容摘要。正文不可變規則不阻止資料刪除；刪除留下事件及 payload 不可用標示。

離線地端刪除在重連後同步，不假裝即時完成。尚未到期的加密備份可殘留已刪除資料至 30 天上限，不能承諾立即從所有備份消失。還原前須套用最新刪除/撤銷 manifest；此 manifest 是恢復限制的證據，不是競爭的 task DB。拿不到最新限制就不得開放 dispatch。

備份每天建立並加密。恢復需驗 integrity、task/checkpoint、批准/撤銷與刪除狀態，以及未知 operation 核對。Pulse 的保留設定只控制 Pulse 副本；不能由此推論模型供應商的保留政策或宣稱零保存。

## K16：錯誤分類與完成判定

| 分類 | 處置 |
|---|---|
| 語法/大小/身份/policy 不合 | 拒絕並回報可用原因；不啟動模型 |
| revision/digest/sequence 衝突 | 保存衝突，要求核對；不覆寫 |
| 暫時不可用且未接受 | 有界退避後可重送同一訊息 |
| 裝置未派送即離線 | waiting_device |
| 已派送或效果後失聯 | outcome_unknown → reconciling |
| 資料/context/費用不足 | blocked，保存缺口與下一步 |
| 已證明的工具/業務失敗 | failed，保存實際 evidence |
| 未知相容性或 provider 能力 | blocked，不繞過限制 |

只有證據支持任務目標已達成，才可標 succeeded。收件、工具文字、傳送成功或模型說「完成」均不足以單獨成立。本文規定的行為要在 [驗收矩陣](acceptance.md) 對應的實作階段證明。

## K16：remote / local executor 邊界

server 保存任務、批准、修訂與結果。對話框只接收人類輸入並顯示同步資料。executor 不擁有人類授權。

local executor 是一個獨立 process。它使用本機啟動參數選定工具範圍，並主動連 server。server 不能改寫此範圍。

電腦視窗可另由本機人類在對話框選擇。只在本機開放的選擇通道把特定 task、revision、程式啟動實例與視窗交給 local executor。這不是 server 可呼叫的介面。原有操作員限制仍有效。範圍有限期，local 重啟後須重新選擇；持久任務與結果仍只由既有 server／journal 保存。

remote executor 是 Pulse 自己的系統。它沿用既有任務 ID、run token 與 operation ID，不另加外部工作系統或 DB。它不要求 VM、SSH 或 PID。收件只表示收件；取消要求也不表示已停止。無法核對的結果保持 unknown。

server 派工前仍須檢查來源、scope、revision、批准、deadline 與預算。工具回覆只提供證據，不能轉為 Human 來源。v0.4 須接上模型與工具迴圈；只有現有持久任務核心不能通過閉環驗收。
