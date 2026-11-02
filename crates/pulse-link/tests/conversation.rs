use pulse_core::{State, Store};
use pulse_link::{protocol::*, remote, server::Hub};
use serde_json::Value;
use sqlx::Row;

async fn setup() -> (tempfile::TempDir, Hub, Credential, u64) {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    let code = hub
        .pairing(ClientRole::Chat, pulse_link::now())
        .await
        .unwrap();
    let cred = hub
        .redeem(
            PairRequest {
                code,
                role: ClientRole::Chat,
                device: uuid::Uuid::new_v4().to_string(),
            },
            pulse_link::now(),
        )
        .await
        .unwrap();
    let epoch = hub.begin(&cred.device, 0, pulse_link::now()).await.unwrap();
    (dir, hub, cred, epoch)
}
async fn say(hub: &Hub, cred: &Credential, epoch: u64, source: &str, text: &str) {
    let cursor: i64 = sqlx::query_scalar("SELECT in_cursor FROM link_device WHERE id=?")
        .bind(&cred.device)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    let envelope = Envelope::new(
        &cred.device,
        cursor as u64 + 1,
        epoch,
        DevicePayload::Human {
            source_id: source.into(),
            action: HumanAction::Submit {
                id: format!("task-{source}"),
                goal: text.into(),
            },
        },
    )
    .unwrap();
    hub.receive(&cred.device, &envelope, pulse_link::now())
        .await
        .unwrap();
}
async fn reply(hub: &Hub, source: &str) -> String {
    sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
        .bind(format!("reply:{source}"))
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn unselected_computer_requests_ask_for_a_window_instead_of_reading_the_repo() {
    let (_dir, hub, credential, epoch) = setup().await;
    for (index, text) in ["請讀取視窗並摘要", "幫我讀這個視窗", "觀察程式", "操作電腦"]
        .iter()
        .enumerate()
    {
        let source = format!("window-{index}");
        say(&hub, &credential, epoch, &source, text).await;
        assert!(reply(&hub, &source).await.contains("程式視窗"));
    }
    assert!(hub.core.list_tasks(100).await.unwrap().is_empty());
    assert!(!remote::step(&hub, pulse_link::now()).await.unwrap());
    let budget: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(budget, 0);
    say(
        &hub,
        &credential,
        epoch,
        "source-code",
        "讀取程式碼 README.md 並摘要",
    )
    .await;
    assert_eq!(
        hub.core.task("task-source-code").await.unwrap().scope,
        "pulse:read"
    );
}

#[tokio::test]
async fn everyday_progress_questions_are_read_only_and_do_not_start_model_work() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(
        &hub,
        &cred,
        epoch,
        "create",
        "任務 A：讀 README.md\n任務 B：讀 Cargo.toml",
    )
    .await;
    let before = serde_json::to_value(hub.core.list_tasks(100).await.unwrap()).unwrap();
    for (index, text) in [
        "現在進度如何？",
        "麻煩你看看 A 的進度",
        "請先幫我看一下 A 進度如何？",
        "A 現在怎麼樣？",
        "B 完成了嗎？",
        "A 和 B 的進度呢？",
        "目前做到哪了？",
    ]
    .iter()
    .enumerate()
    {
        let source = format!("question-{index}");
        say(&hub, &cred, epoch, &source, text).await;
        assert!(reply(&hub, &source).await.contains("等待處理"), "{text}");
        assert_eq!(
            serde_json::to_value(hub.core.list_tasks(100).await.unwrap()).unwrap(),
            before
        );
    }
    say(&hub, &cred, epoch, "unknown-query", "C 完成了嗎？").await;
    assert!(reply(&hub, "unknown-query").await.contains("請指明"));
    assert_eq!(
        serde_json::to_value(hub.core.list_tasks(100).await.unwrap()).unwrap(),
        before
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn polite_controls_keep_explicit_targets_and_atomic_batch_validation() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(
        &hub,
        &cred,
        epoch,
        "create",
        "任務 A：讀 README.md\n任務 B：讀 Cargo.toml",
    )
    .await;
    let tasks = hub.core.list_tasks(100).await.unwrap();
    let a = tasks
        .iter()
        .find(|t| t.goal.starts_with("任務 A："))
        .unwrap();
    let b = tasks
        .iter()
        .find(|t| t.goal.starts_with("任務 B："))
        .unwrap();
    say(&hub, &cred, epoch, "ambiguous", "麻煩你先幫我取消它").await;
    assert!(reply(&hub, "ambiguous").await.contains("請指明"));
    say(
        &hub,
        &cred,
        epoch,
        "conflicting",
        "請幫我修改 A：新需求\n麻煩取消 A",
    )
    .await;
    assert!(reply(&hub, "conflicting").await.contains("請指明"));
    assert_eq!(hub.core.task(&a.id).await.unwrap().revision, 1);
    assert_eq!(hub.core.task(&b.id).await.unwrap().revision, 1);
    say(
        &hub,
        &cred,
        epoch,
        "controls",
        "請幫我修改 A：只讀 Cargo.toml\n麻煩你取消 B",
    )
    .await;
    let revised = hub.core.task(&a.id).await.unwrap();
    assert_eq!(revised.revision, 2);
    assert_eq!(revised.goal, "任務 A：只讀 Cargo.toml");
    assert_eq!(hub.core.task(&b.id).await.unwrap().state, State::Cancelled);
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
    say(
        &hub,
        &cred,
        epoch,
        "controls",
        "請幫我修改 A：只讀 Cargo.toml\n麻煩你取消 B",
    )
    .await;
    assert_eq!(hub.core.task(&a.id).await.unwrap().revision, 2);
}

#[tokio::test]
async fn single_chat_routes_multiple_tasks_and_never_creates_from_query_or_ambiguity() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(
        &hub,
        &cred,
        epoch,
        "both",
        "任務 A：讀 README.md\n任務 B：讀 Cargo.toml",
    )
    .await;
    let tasks = hub.core.list_tasks(100).await.unwrap();
    assert_eq!(tasks.len(), 2);
    let a = tasks
        .iter()
        .find(|t| t.goal.starts_with("任務 A："))
        .unwrap();
    let b = tasks
        .iter()
        .find(|t| t.goal.starts_with("任務 B："))
        .unwrap();
    let a_id = a.id.clone();
    let b_id = b.id.clone();
    for (source, text) in [
        ("query", "查詢 A 和 B"),
        ("query-suffix", "A 的狀態"),
        ("all", "目前進度"),
        ("ambiguous", "先取消它"),
        ("unknown", "修改 C：新目標"),
        ("conflict", "修改 A：新目標\n取消 A"),
    ] {
        say(&hub, &cred, epoch, source, text).await;
        assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
        assert_eq!(hub.core.task(&a_id).await.unwrap().revision, 1);
        assert_eq!(hub.core.task(&b_id).await.unwrap().revision, 1);
    }
    assert!(reply(&hub, "query").await.contains("A · 等待處理"));
    assert!(reply(&hub, "query").await.contains("B · 等待處理"));
    assert!(reply(&hub, "ambiguous").await.contains("請指明"));
    say(
        &hub,
        &cred,
        epoch,
        "revise",
        "修改 A：只讀 src/model.rs，不要讀 README.md",
    )
    .await;
    let a = hub.core.task(&a_id).await.unwrap();
    assert_eq!(a.revision, 2);
    assert_eq!(a.decisions.len(), 2);
    assert_eq!(a.decisions.last().unwrap().text, a.goal);
    assert_eq!(
        a.decisions.last().unwrap().supersedes.as_deref(),
        Some(a.decisions[0].event_id.as_str())
    );
    assert_eq!(hub.core.task(&b_id).await.unwrap().revision, 1);
    say(
        &hub,
        &cred,
        epoch,
        "revise",
        "修改 A：只讀 src/model.rs，不要讀 README.md",
    )
    .await;
    assert_eq!(hub.core.task(&a_id).await.unwrap().revision, 2);
    say(&hub, &cred, epoch, "cancel", "取消 B").await;
    assert_eq!(hub.core.task(&b_id).await.unwrap().state, State::Cancelled);
    let link=sqlx::query("SELECT task_id FROM event_tasks WHERE event_id=(SELECT id FROM events WHERE source_id='query') ORDER BY task_id").fetch_all(&hub.core.application_pool()).await.unwrap();
    assert_eq!(link.len(), 2);
    assert!(link.iter().any(|r| r.get::<String, _>("task_id") == a_id));
}

#[tokio::test]
async fn resume_requires_a_safe_state_and_conflicting_batches_do_not_mutate() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "create", "任務 A：讀 README.md").await;
    let a = hub.core.list_tasks(100).await.unwrap().pop().unwrap();
    say(
        &hub,
        &cred,
        epoch,
        "bad-batch",
        "修改 A：另一個需求\n繼續 A",
    )
    .await;
    assert_eq!(hub.core.task(&a.id).await.unwrap().revision, 1);
    say(&hub, &cred, epoch, "duplicate-name", "任務 A：另一個任務").await;
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
    remote::step(&hub, pulse_link::now()).await.unwrap();
    assert_eq!(hub.core.task(&a.id).await.unwrap().state, State::Blocked);
    say(&hub, &cred, epoch, "resume", "繼續 A").await;
    assert_eq!(hub.core.task(&a.id).await.unwrap().state, State::Queued);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    let (origin, payload): (String, String) =
        sqlx::query_as("SELECT origin,payload FROM events WHERE source_id='resume'")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!(origin, "human");
    let payload: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["commands"][0]["kind"], "resume");
}

#[tokio::test]
async fn context_contains_only_the_current_decision_and_fences_the_old_run() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：OLD_A_REQUIREMENT").await;
    say(&hub, &cred, epoch, "b", "任務 B：B_PRIVATE_MARKER").await;
    let versions = remote::versions();
    let old = hub
        .core
        .claim_next(&versions, pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.token.task_id, "task-a");
    say(&hub, &cred, epoch, "new-a", "修改 A：NEW_A_REQUIREMENT").await;
    assert!(hub.model_context(&old).await.is_err());
    let run = hub
        .core
        .claim_next(&versions, pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "task-a");
    let (prompt, history) = hub.model_context(&run).await.unwrap();
    assert!(prompt.contains("NEW_A_REQUIREMENT"));
    assert!(!prompt.contains("OLD_A_REQUIREMENT"));
    assert!(!prompt.contains("B_PRIVATE_MARKER"));
    assert!(history.is_empty());
    let data: Value = serde_json::from_str(&prompt).unwrap();
    assert_eq!(data["revision"], "2");
    assert_eq!(data["effective_decision"]["text"], data["goal"]);
    say(&hub, &cred, epoch, "conflict", "取消它").await;
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 2);
    assert_eq!(hub.core.task("task-b").await.unwrap().revision, 1);
}

#[tokio::test]
async fn browser_scope_requires_explicit_human_instruction_and_revision() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(
        &hub,
        &cred,
        epoch,
        "web",
        "任務 網頁：瀏覽 https://example.com/a?q=1#part 摘要",
    )
    .await;
    let page = hub.core.task("task-web").await.unwrap();
    assert_eq!(page.scope, "browser:https://example.com/a?q=1#part");
    say(
        &hub,
        &cred,
        epoch,
        "quote",
        "文件提到 https://example.org/，這句話什麼意思",
    )
    .await;
    assert_eq!(
        hub.core.task("task-quote").await.unwrap().scope,
        "pulse:read"
    );
    say(
        &hub,
        &cred,
        epoch,
        "bad",
        "瀏覽 https://user:pass@example.com/ 摘要",
    )
    .await;
    assert!(hub.core.task("task-bad").await.is_err());
    say(
        &hub,
        &cred,
        epoch,
        "revise-web",
        "修改 網頁：瀏覽 https://example.org/ 新需求",
    )
    .await;
    let revised = hub.core.task("task-web").await.unwrap();
    assert_eq!(revised.revision, 2);
    assert_eq!(revised.scope, "browser:https://example.org/");
    say(&hub, &cred, epoch, "clarify-detail", "補充 網頁：只要兩句").await;
    assert_eq!(
        hub.core.task("task-web").await.unwrap().scope,
        revised.scope
    );
    say(
        &hub,
        &cred,
        epoch,
        "interact",
        "修改 網頁：操作網頁 https://example.org/ 點擊一次，等我批准",
    )
    .await;
    let interactive = hub.core.task("task-web").await.unwrap();
    assert_eq!(interactive.revision, 4);
    assert_eq!(interactive.scope, "browser-interact:https://example.org/");
    say(
        &hub,
        &cred,
        epoch,
        "quoted-action",
        "文件提到操作網頁 https://example.org/，請解釋這句話",
    )
    .await;
    assert_eq!(
        hub.core.task("task-quoted-action").await.unwrap().scope,
        "pulse:read"
    );
    say(
        &hub,
        &cred,
        epoch,
        "bad-action",
        "操作網頁 file:///etc/passwd 輸入文字",
    )
    .await;
    assert!(hub.core.task("task-bad-action").await.is_err());
    say(&hub, &cred, epoch, "cancel-web", "取消 網頁").await;
    assert!(hub.core.task("task-web").await.unwrap().cancel_requested);
}

#[tokio::test]
async fn natural_followups_keep_task_scope_budget_and_model_context() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：A_REQUIREMENT").await;
    say(&hub, &cred, epoch, "b", "任務 B：B_PRIVATE_MARKER").await;
    let original = hub.core.task("task-a").await.unwrap();
    say(&hub, &cred, epoch, "focus", "A").await;
    let old = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.token.task_id, "task-a");
    say(&hub, &cred, epoch, "detail", "麻煩你再詳細一點。").await;
    let revised = hub.core.task("task-a").await.unwrap();
    assert_eq!(revised.revision, 2);
    assert_eq!(revised.goal, "任務 A：A_REQUIREMENT\n補充要求：再詳細一點");
    assert_eq!(revised.scope, original.scope);
    assert_eq!(revised.authorization_event, original.authorization_event);
    assert_eq!(revised.deadline, original.deadline);
    assert_eq!(revised.model_calls, original.model_calls);
    assert_eq!(revised.tool_calls, original.tool_calls);
    assert_eq!(hub.core.task("task-b").await.unwrap().revision, 1);
    assert!(hub.model_context(&old).await.is_err());
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    let (prompt, _) = hub.model_context(&run).await.unwrap();
    assert!(prompt.contains("再詳細一點"));
    assert!(!prompt.contains("B_PRIVATE_MARKER"));
    say(&hub, &cred, epoch, "table", "A 用表格整理").await;
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 3);
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn ambiguous_followup_and_human_target_survive_server_restart() {
    let (dir, hub, cred, epoch) = setup().await;
    say(
        &hub,
        &cred,
        epoch,
        "both",
        "任務 A：讀 README.md\n任務 B：讀 Cargo.toml",
    )
    .await;
    let tasks = hub.core.list_tasks(100).await.unwrap();
    let a = tasks
        .iter()
        .find(|task| task.goal.starts_with("任務 A："))
        .unwrap()
        .id
        .clone();
    let b = tasks
        .iter()
        .find(|task| task.goal.starts_with("任務 B："))
        .unwrap()
        .id
        .clone();
    say(&hub, &cred, epoch, "style", "請用條列整理").await;
    assert!(reply(&hub, "style").await.contains("哪個任務"));
    assert_eq!(hub.core.task(&a).await.unwrap().revision, 1);
    assert_eq!(hub.core.task(&b).await.unwrap().revision, 1);
    let metadata: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='chat_conversation_context_v1'")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert!(!metadata.contains("用條列整理"));
    assert!(!metadata.contains("Cargo.toml"));
    drop(hub);
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    let epoch = hub.begin(&cred.device, 0, pulse_link::now()).await.unwrap();
    say(&hub, &cred, epoch, "answer", "B").await;
    let revised = hub.core.task(&b).await.unwrap();
    assert_eq!(revised.revision, 2);
    assert!(revised.goal.ends_with("補充要求：用條列整理"));
    assert_eq!(hub.core.task(&a).await.unwrap().revision, 1);
    // The same Human source is immutable. Replaying it cannot apply the refinement twice.
    say(&hub, &cred, epoch, "answer", "B").await;
    assert_eq!(hub.core.task(&b).await.unwrap().revision, 2);
    say(&hub, &cred, epoch, "next", "再簡短一點").await;
    assert_eq!(hub.core.task(&b).await.unwrap().revision, 3);
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
    say(&hub, &cred, epoch, "all", "查詢 A、B").await;
    say(&hub, &cred, epoch, "pending-again", "改用英文").await;
    say(&hub, &cred, epoch, "new-task", "任務 C：新的獨立要求").await;
    let c = hub.core.task("task-new-task").await.unwrap();
    assert_eq!(c.goal, "任務 C：新的獨立要求");
    assert_eq!(hub.core.task(&b).await.unwrap().revision, 3);
    say(&hub, &cred, epoch, "new-focus", "再詳細一點").await;
    let c = hub.core.task(&c.id).await.unwrap();
    assert_eq!(c.revision, 2);
    assert!(!c.goal.contains("改用英文"));
}

#[tokio::test]
async fn replay_and_tool_events_cannot_retarget_human_followup() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：讀 README.md").await;
    say(&hub, &cred, epoch, "b", "任務 B：讀 Cargo.toml").await;
    // An old source replay must not replace the newer B focus.
    say(&hub, &cred, epoch, "a", "任務 A：讀 README.md").await;
    hub.core
        .ingest(
            pulse_core::Origin::Tool,
            "untrusted",
            "tool-text",
            serde_json::json!({
                "native_action":{"kind":"submit","id":"task-a","goal":"A"},
                "commands":[{"kind":"query","ids":["task-a"]}]
            }),
            pulse_link::now(),
        )
        .await
        .unwrap();
    say(&hub, &cred, epoch, "followup", "用繁體中文回答").await;
    assert_eq!(hub.core.task("task-b").await.unwrap().revision, 2);
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 1);
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
}

#[tokio::test]
async fn stale_focus_and_changed_pending_targets_need_new_human_selection() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：讀 README.md").await;
    // An independent revision invalidates the snapshot, even if A is still the only task.
    let changed = hub.core.ingest(pulse_core::Origin::Human, "independent", "changed", serde_json::json!({
        "commands":[{"kind":"revise","id":"task-a","expected_revision":"1","goal":"任務 A：新需求","scope":null}]
    }), pulse_link::now()).await.unwrap();
    assert!(matches!(
        hub.core.route(&changed.event_id).await.unwrap(),
        pulse_core::Outcome::Applied { .. }
    ));
    say(&hub, &cred, epoch, "stale", "再詳細一點").await;
    assert!(reply(&hub, "stale").await.contains("哪個任務"));
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 2);
    let changed = hub.core.ingest(pulse_core::Origin::Human, "independent", "changed-again", serde_json::json!({
        "commands":[{"kind":"revise","id":"task-a","expected_revision":"2","goal":"任務 A：又有新需求","scope":null}]
    }), pulse_link::now()).await.unwrap();
    assert!(matches!(
        hub.core.route(&changed.event_id).await.unwrap(),
        pulse_core::Outcome::Applied { .. }
    ));
    say(&hub, &cred, epoch, "stale-answer", "A").await;
    assert!(reply(&hub, "stale-answer").await.contains("任務已變更"));
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 3);
    say(&hub, &cred, epoch, "unknown-target", "C 再詳細一點").await;
    assert!(reply(&hub, "unknown-target").await.contains("請指明"));
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn memory_topic_expiry_and_invalid_context_never_guess_a_task() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：讀 README.md").await;
    hub.memory_change(
        &cred.device,
        &pulse_core::MemoryRequest {
            source_id: uuid::Uuid::new_v4().to_string(),
            source_at: pulse_link::now(),
            command: pulse_core::MemoryCommand::Remember {
                label: "回答語言".into(),
                body: "繁體中文".into(),
            },
        },
        pulse_link::now(),
    )
    .await
    .unwrap();
    say(&hub, &cred, epoch, "after-memory", "再詳細一點").await;
    assert!(reply(&hub, "after-memory").await.contains("哪個任務"));
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 1);
    for (index, mutation) in [
        serde_json::json!({"recorded_at":0}),
        serde_json::json!({"device":"different-chat"}),
        serde_json::json!({"event_id":"missing-event"}),
        serde_json::json!({"version":999}),
    ]
    .into_iter()
    .enumerate()
    {
        say(&hub, &cred, epoch, &format!("focus-{index}"), "查詢 A").await;
        let db = hub.core.application_pool();
        let data: String = sqlx::query_scalar(
            "SELECT value FROM settings WHERE key='chat_conversation_context_v1'",
        )
        .fetch_one(&db)
        .await
        .unwrap();
        let mut value: Value = serde_json::from_str(&data).unwrap();
        for (key, changed) in mutation.as_object().unwrap() {
            value[key] = changed.clone();
        }
        sqlx::query("UPDATE settings SET value=? WHERE key='chat_conversation_context_v1'")
            .bind(value.to_string())
            .execute(&db)
            .await
            .unwrap();
        let source = format!("invalid-{index}");
        say(&hub, &cred, epoch, &source, "用表格整理").await;
        assert!(reply(&hub, &source).await.contains("哪個任務"));
        assert_eq!(hub.core.task("task-a").await.unwrap().revision, 1);
    }
    say(&hub, &cred, epoch, "bad-answer", "不存在的名稱").await;
    assert!(reply(&hub, "bad-answer").await.contains("任務名稱"));
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
    say(&hub, &cred, epoch, "corrected-answer", "A").await;
    let revised = hub.core.task("task-a").await.unwrap();
    assert_eq!(revised.revision, 2);
    assert!(revised.goal.ends_with("補充要求：用表格整理"));
}

#[tokio::test]
async fn natural_batch_is_atomic_and_does_not_grant_browser_permissions() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(
        &hub,
        &cred,
        epoch,
        "a",
        "任務 A：瀏覽 https://example.com/ 摘要",
    )
    .await;
    say(&hub, &cred, epoch, "b", "任務 B：讀 Cargo.toml").await;
    let original = hub.core.task("task-a").await.unwrap();
    say(&hub, &cred, epoch, "conflict", "A再詳細一點\n取消 A").await;
    assert!(reply(&hub, "conflict").await.contains("請指明"));
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 1);
    say(&hub, &cred, epoch, "both", "A再詳細一點\nB用表格整理").await;
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 2);
    assert_eq!(hub.core.task("task-b").await.unwrap().revision, 2);
    let revised = hub.core.task("task-a").await.unwrap();
    assert_eq!(revised.scope, original.scope);
    assert_eq!(revised.authorization_event, original.authorization_event);
    say(&hub, &cred, epoch, "cancel", "取消 A").await;
    say(&hub, &cred, epoch, "terminal", "A再簡短一點").await;
    assert_eq!(
        hub.core.task("task-a").await.unwrap().state,
        State::Cancelled
    );
    assert!(reply(&hub, "terminal").await.contains("請指明"));
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
}

#[tokio::test]
async fn followup_preserves_unknown_model_cost_and_existing_call_count() {
    let (_dir, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：讀 README.md").await;
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert!(
        hub.core
            .reserve(
                &run.token,
                "unknown-call",
                20000,
                "fixture",
                pulse_link::now()
            )
            .await
            .unwrap()
    );
    hub.core.cost_unknown("unknown-call").await.unwrap();
    let original = hub.core.task("task-a").await.unwrap();
    assert_eq!(original.model_calls, 1);
    let before: (String, i64, i64) =
        sqlx::query_as("SELECT state,reserved,charged FROM budget WHERE call_id='unknown-call'")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    // Unknown usage keeps the full conservative charge, not a zero-cost fixture.
    assert_eq!(before, ("unknown".into(), 20000, 20000));
    say(&hub, &cred, epoch, "followup", "再詳細一點").await;
    let revised = hub.core.task("task-a").await.unwrap();
    assert_eq!(revised.revision, 2);
    assert_eq!(revised.model_calls, original.model_calls);
    assert_eq!(revised.deadline, original.deadline);
    let budget: (String, i64, i64) =
        sqlx::query_as("SELECT state,reserved,charged FROM budget WHERE call_id='unknown-call'")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!(budget, before);
    assert!(hub.model_context(&run).await.is_err());
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
}
