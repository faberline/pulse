//! Authenticated Human result review. All model replies are owned fixtures.
use pulse_core::{Command, Origin, Run, State, Store};
use pulse_link::{protocol::*, provider::Reply, remote, server::Hub};
use serde_json::json;

async fn pair(hub: &Hub, role: ClientRole) -> Credential {
    let code = hub.pairing(role, pulse_link::now()).await.unwrap();
    hub.redeem(
        PairRequest {
            code,
            role,
            device: uuid::Uuid::new_v4().to_string(),
        },
        pulse_link::now(),
    )
    .await
    .unwrap()
}
async fn action(
    hub: &Hub,
    c: &Credential,
    source: &str,
    action: HumanAction,
) -> pulse_link::Result<()> {
    let cursor: i64 = sqlx::query_scalar("SELECT in_cursor FROM link_device WHERE id=?")
        .bind(&c.device)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    let frame = Envelope::new(
        &c.device,
        cursor as u64 + 1,
        0,
        DevicePayload::Human {
            source_id: source.into(),
            action,
        },
    )
    .unwrap();
    hub.receive(&c.device, &frame, pulse_link::now())
        .await
        .map(|_| ())
}
async fn result(hub: &Hub, c: &Credential, id: &str) -> (Run, HumanAction) {
    let event = hub.core.ingest(Origin::Human, &format!("device:{}", c.device), id, json!({"commands":[Command::Create {id:id.into(),goal:format!("任務 {id}：owned result"),scope:"pulse:read".into(),priority:0,parent:None,dependencies:vec![],requires_device:false}]}), pulse_link::now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    hub.core
        .reserve(
            &run.token,
            &format!("call-{id}"),
            20000,
            "fixture",
            pulse_link::now(),
        )
        .await
        .unwrap();
    hub.core
        .settle(&format!("call-{id}"), 7, "owned known usage")
        .await
        .unwrap();
    hub.model_reply(
        &run.token,
        &Reply {
            text: format!("OWNED_{id}_RESULT"),
            tool: None,
            usage: json!({}),
            cost: 7,
        },
        "fixture",
        pulse_link::now(),
    )
    .await
    .unwrap();
    let message = format!("model:{}:1:0", run.token.run_id);
    let body: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
        .bind(&message)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    (
        run,
        HumanAction::AcceptResult {
            task_id: id.into(),
            revision: "1".into(),
            message_id: message,
            chat_digest: hash_bytes(body.as_bytes()),
            text: format!("接受成果 {id}"),
        },
    )
}
async fn setup() -> (tempfile::TempDir, Hub, Credential) {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(root.path().join("db"), "owner").await.unwrap());
    let c = pair(&hub, ClientRole::Chat).await;
    hub.begin(&c.device, 0, pulse_link::now()).await.unwrap();
    (root, hub, c)
}

#[tokio::test]
async fn human_accepts_only_the_saved_result_and_replay_preserves_export_and_cost() {
    let (root, hub, c) = setup().await;
    let (run, review) = result(&hub, &c, "A").await;
    let (_, other) = result(&hub, &c, "B").await;
    let before = hub.core.task("A").await.unwrap();
    let manifest = hub
        .text_result_manifest(&c.device, &run.token.run_id, pulse_link::now())
        .await
        .unwrap();
    let bytes = hub
        .text_result_bytes(
            &c.device,
            &run.token.run_id,
            &manifest.reference.digest,
            pulse_link::now(),
        )
        .await
        .unwrap();
    action(&hub, &c, "accept-A", review.clone()).await.unwrap();
    let accepted = hub.core.task("A").await.unwrap();
    assert_eq!(accepted.state, State::Succeeded);
    assert_eq!(accepted.reason.as_deref(), Some("human_accepted_result"));
    assert_eq!(
        (
            accepted.model_calls,
            accepted.tool_calls,
            accepted.retry_count
        ),
        (before.model_calls, before.tool_calls, before.retry_count)
    );
    assert_eq!(accepted.authorization_event, before.authorization_event);
    assert_eq!(hub.core.task("B").await.unwrap().state, State::Blocked);
    action(&hub, &c, "accept-A", review.clone()).await.unwrap();
    action(&hub, &c, "accept-A-again", review).await.unwrap();
    assert_eq!(
        serde_json::to_value(hub.core.task("A").await.unwrap()).unwrap(),
        serde_json::to_value(&accepted).unwrap()
    );
    assert_eq!(
        hub.text_result_bytes(
            &c.device,
            &run.token.run_id,
            &manifest.reference.digest,
            pulse_link::now()
        )
        .await
        .unwrap(),
        bytes
    );
    let spend: i64 = sqlx::query_scalar("SELECT sum(charged) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(spend, 14);
    hub.core.clone().close().await;
    drop(hub);
    let reopened = Hub::new(Store::open(root.path().join("db"), "owner").await.unwrap());
    reopened
        .begin(&c.device, 0, pulse_link::now())
        .await
        .unwrap();
    assert_eq!(
        reopened.core.task("A").await.unwrap().state,
        State::Succeeded
    );
    action(&reopened, &c, "accept-B", other).await.unwrap();
    assert_eq!(
        reopened.core.task("B").await.unwrap().state,
        State::Succeeded
    );
}

#[tokio::test]
async fn stale_or_worker_reviews_are_not_completion_and_raw_text_cannot_create_work() {
    let (_root, hub, c) = setup().await;
    let (_, review) = result(&hub, &c, "A").await;
    let worker = pair(&hub, ClientRole::LocalExecutor).await;
    assert!(
        action(&hub, &worker, "worker-review", review.clone())
            .await
            .is_err()
    );
    assert_eq!(hub.core.task("A").await.unwrap().state, State::Blocked);
    let mut stale = review.clone();
    if let HumanAction::AcceptResult { chat_digest, .. } = &mut stale {
        *chat_digest = "0".repeat(64);
    }
    action(&hub, &c, "stale", stale).await.unwrap();
    let outcome: String =
        sqlx::query_scalar("SELECT outcome FROM link_inbox WHERE device=? AND sequence=1")
            .bind(&c.device)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!(outcome, "rejected");
    assert_eq!(hub.core.task("A").await.unwrap().state, State::Blocked);
    // Rejected input is durably consumed; it must not stall later Human input.
    let cursor: i64 = sqlx::query_scalar("SELECT in_cursor FROM link_device WHERE id=?")
        .bind(&c.device)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(cursor, 1);
    action(
        &hub,
        &c,
        "raw",
        HumanAction::Submit {
            id: "never-create".into(),
            goal: "接受成果 A".into(),
        },
    )
    .await
    .unwrap();
    assert!(hub.core.task("never-create").await.is_err());
    assert_eq!(hub.core.task("A").await.unwrap().state, State::Blocked);
    action(
        &hub,
        &c,
        "cancel",
        HumanAction::Submit {
            id: "never-cancel-task".into(),
            goal: "取消 A".into(),
        },
    )
    .await
    .unwrap();
    action(&hub, &c, "late-review", review).await.unwrap();
    let outcome: String = sqlx::query_scalar(
        "SELECT outcome FROM link_inbox WHERE device=? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&c.device)
    .fetch_one(&hub.core.application_pool())
    .await
    .unwrap();
    assert_eq!(outcome, "rejected");
    assert_ne!(hub.core.task("A").await.unwrap().state, State::Succeeded);
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
}
