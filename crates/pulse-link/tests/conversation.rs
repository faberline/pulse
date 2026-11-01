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
