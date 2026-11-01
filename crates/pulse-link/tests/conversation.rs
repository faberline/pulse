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
