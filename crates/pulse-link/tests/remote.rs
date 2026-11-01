use pulse_core::{Origin, State, Store};
use pulse_link::{remote, server::Hub};
use serde_json::json;

async fn task(hub: &Hub, id: &str) {
    let event=hub.core.ingest(Origin::Human,"test",id,json!({"commands":[{"kind":"create","id":id,"goal":"Reply OK only.","scope":"pulse:read","priority":0,"requires_device":false}]}),pulse_link::now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
}

#[tokio::test]
async fn late_model_reply_cannot_override_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    task(&hub, "cancel").await;
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    let event = hub
        .core
        .ingest(
            Origin::Human,
            "test",
            "cancel-control",
            json!({"commands":[{"kind":"cancel","id":"cancel","expected_revision":"1"}]}),
            pulse_link::now(),
        )
        .await
        .unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let reply = pulse_link::provider::Reply {
        text: "late".into(),
        usage: json!({}),
        cost: 1,
    };
    assert!(
        hub.model_reply(&run.token, &reply, "test", pulse_link::now())
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_chat")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "explicit paid smoke with private local API keys"]
async fn real_providers_save_reply_and_settle_budget() {
    for name in ["openai", "anthropic"] {
        let provider = pulse_link::provider::Provider::load(name)
            .unwrap()
            .expect("key missing");
        let dir = tempfile::tempdir().unwrap();
        let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
        task(&hub, name).await;
        remote::step_with(&hub, Some(&provider), pulse_link::now())
            .await
            .unwrap();
        let task = hub.core.task(name).await.unwrap();
        assert_eq!(
            task.reason.as_deref(),
            Some("model_reply_ready"),
            "{name} failed"
        );
        assert_eq!(task.state, State::Blocked);
        let (state, cost): (String, i64) = sqlx::query_as("SELECT state,charged FROM budget")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(state, "settled");
        assert!((0..=20000).contains(&cost));
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_chat")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert!(
            !remote::step_with(&hub, Some(&provider), pulse_link::now())
                .await
                .unwrap()
        );
        println!("{name}: reply persisted; budget settled ({cost} micro USD)");
    }
}

#[tokio::test]
async fn remote_scheduling_is_durable_and_never_claims_missing_provider_succeeded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server.db");
    let store = Store::open(&path, "owner").await.unwrap();
    let hub = Hub::new(store.clone());
    let now = pulse_link::now();
    let event = store
        .ingest(
            Origin::Human,
            "test",
            "source",
            json!({"commands":[{
                "kind":"create","id":"remote-task","goal":"analyse Pulse","scope":"pulse:read",
                "priority":0,"requires_device":false
            }]}),
            now,
        )
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap();
    assert!(remote::step(&hub, now + 1).await.unwrap());
    let task = store.task("remote-task").await.unwrap();
    assert_eq!(task.state, State::Blocked);
    assert_eq!(task.reason.as_deref(), Some("provider_not_configured"));
    assert_eq!(task.model_calls, 0);
    assert!(!remote::step(&hub, now + 2).await.unwrap());
    drop(hub);
    store.close().await;
    let reopened = Store::open(path, "owner").await.unwrap();
    reopened
        .recover(&remote::versions(), now + 3)
        .await
        .unwrap();
    assert_eq!(
        reopened.task("remote-task").await.unwrap().state,
        State::Blocked
    );
}
