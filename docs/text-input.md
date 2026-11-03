# 明確選入文字檔案

這是 D09 的功能候選。它沿用單一對話框、Rust chat、server 與 remote executor。
Swift 只開啟系統選檔視窗、送出命令及顯示狀態。
Rust 負責讀取、核對、保存、傳輸、任務與模型內容。
其他介面可共用同一份 JSON Lines 命令與狀態。

## 使用流程

1. 在「附加」選「文字檔案」。Rust 讀取一份固定副本。
2. 確認檔名與大小。在同一對話框輸入要求。
3. 按「傳送」。Rust 先保存訊息，再經 chat 身分送給 server。

選檔本身不會上傳，也不會建立任務。
Swift 收到的狀態只有識別、檔名、摘要雜湊、大小及選取時間，沒有正文或原路徑。
送出命令只帶副本識別。Rust 取出已核對的副本。
原檔之後變更，不會改變副本。
圖片按鈕仍只有本機預覽，不會把圖片交給模型。

## 輸入範圍

每次可選一份非空 UTF-8 文字檔案，最多 32 KiB。
支援 txt、md、rs、toml、json、yaml、yml、csv，以及 Cargo.lock、README、LICENSE、NOTICE。
Rust 拒絕隱藏檔名、私密目錄、最後一段為符號連結的檔案、硬連結及特殊檔案。
父目錄可使用正常別名，例如 macOS 的 `/tmp`。
Rust 解析父目錄後，再核對私密路徑，並以不跟隨連結的方式逐段開啟。
讀取前後須有相同的檔案識別、大小與修改時間。
檔案內容仍是不可信任的資料。內容中的指令不能取消任務、建立記憶或取得工具權限。

未送出的副本保存在既有私有 chat journal，最多保留 24 小時。
重啟後只可由同一有效 chat 身分取用。
移除、到期、損壞或身分撤銷會清除未送出的副本，並截斷 WAL。
選檔取消須等待 Rust 的同一要求識別確認。晚到的選取結果不能留下新副本。
已按傳送的訊息屬於既有待送佇列。移除附件選擇不會撤回這則訊息。
它可在離線後重送；server 不會因未送出選擇的 24 小時期限而拒絕它。
這不是記憶功能，也不宣稱完成所有備份或副本的清除。

## 任務與模型

server 把副本放在原 Human 來源事件中，不另建任務資料庫。
新分析任務使用 `input:<sha256>` 範圍。它不需要 local executor 在線。
每次組裝模型內容，Rust 都重查原 Human 事件、Create 命令、任務識別與摘要雜湊。
來源缺失或不符時停止，不改讀其他檔案或任務。
一般追問保留原副本、來源、期限與費用紀錄。
A/B 任務各用自己的來源。進度查詢與取消沿用原控制規則，不因附件而呼叫模型。
重送使用原來源識別與命令，不重新規劃任務。

檔案任務不提供 repo、瀏覽器或電腦工具。
兩家 provider 都只收到固定文字副本及任務內容。
檔案任務的編碼訊息上限為 48 KiB；其他任務維持 32 KiB。
特殊字元編碼可能增加大小。本機大小核對失敗時不送出，並保留草稿。
provider 仍會重查完整內容上限；超過時不呼叫模型。
既有模型、每日 US$1、每任務 US$0.25、每次 20,000 microUSD 保留及未知費用規則不變。
回覆附檔名與選取時間，不自動傳送本機絕對路徑。

執行版本為 `pulse-remote-22`、`effective-read-7`。
schema 仍為 17，tools 仍為 `pulse-tools-6`，policy 仍為 `approved-effects-memory-fences-9`。
舊 checkpoint 不會自動採用新版本。模型記憶保持關閉。

## 驗證與缺口

受控測試涵蓋真正 Rust chat 迴圈、WebSocket、選定檔案、固定副本、取消、離線 journal 重開、來源隔離、重送、追問及偽造工具拒絕。
兩家 provider 的請求格式使用假金鑰核對，沒有送出付費呼叫。
Swift 測試核對顯示資料與只帶副本識別的送出命令。
這些測試不代表實際桌面、兩家模型回覆或硬體 I/O 故障驗收已完成。
PDF、圖片模型輸入、成果匯出及受限寫入仍未完成。D09 與完整 D01–D10 目標保持未接受。

文字請求依據：[OpenAI 文字輸入](https://developers.openai.com/api/docs/guides/text)、[OpenAI 工具設定](https://developers.openai.com/api/docs/guides/tools)、[Anthropic 工具設定](https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools)。
