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
        tool: None,
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
async fn late_model_text_and_proposals_cannot_override_revision_or_deadline() {
    for revised in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
        task(&hub, "target").await;
        let run = hub
            .core
            .claim_next(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
            .unwrap();
        let time = if revised {
            let event=hub.core.ingest(Origin::Human,"test","revise",json!({"commands":[{"kind":"revise","id":"target","expected_revision":"1","goal":"NEW_GOAL"}]}),pulse_link::now()).await.unwrap();
            hub.core.route(&event.event_id).await.unwrap();
            pulse_link::now()
        } else {
            hub.core.task("target").await.unwrap().deadline
        };
        let reply = pulse_link::provider::Reply {
            text: "old answer".into(),
            tool: None,
            usage: json!({}),
            cost: 1,
        };
        assert!(
            hub.model_reply(&run.token, &reply, "fixture", time)
                .await
                .is_err()
        );
        assert!(
            hub.model_proposal(&run.token, &proposal(), time)
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_chat")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
        if !revised {
            assert!(!remote::step(&hub, time).await.unwrap());
            assert_eq!(
                hub.core.task("target").await.unwrap().reason.as_deref(),
                Some("deadline_exhausted")
            );
        }
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

use pulse_core::{Run, Versions};
use pulse_link::{
    executor::Executor,
    protocol::*,
    provider::{Reply, ToolCall},
};

fn proposal() -> Reply {
    Reply {
        text: String::new(),
        tool: Some(ToolCall {
            call_id: "call_fixture".into(),
            operation: ReadOp::Text {
                path: "README.md".into(),
            },
        }),
        usage: json!({}),
        cost: 1,
    }
}
async fn local(hub: &Hub) -> (Credential, u64) {
    let time = pulse_link::now();
    let code = hub.pairing(ClientRole::LocalExecutor, time).await.unwrap();
    let cred = hub
        .redeem(
            PairRequest {
                code,
                device: uuid::Uuid::new_v4().to_string(),
                role: ClientRole::LocalExecutor,
            },
            time,
        )
        .await
        .unwrap();
    let epoch = hub.begin(&cred.device, 0, time).await.unwrap();
    (cred, epoch)
}
async fn pending_read(hub: &Hub) -> (String, String, String) {
    sqlx::query_as("SELECT id,task_id,task_revision FROM link_reads ORDER BY rowid DESC LIMIT 1")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap()
}
async fn receipt(
    hub: &Hub,
    cred: &Credential,
    epoch: u64,
    sequence: u64,
    read: &(String, String, String),
    outcome: ReadOutcome,
) {
    let envelope = Envelope::new(
        &cred.device,
        sequence,
        epoch,
        DevicePayload::Result {
            operation_id: read.0.clone(),
            task_id: read.1.clone(),
            revision: read.2.clone(),
            outcome,
        },
    )
    .unwrap();
    hub.receive(&cred.device, &envelope, pulse_link::now())
        .await
        .unwrap();
}
async fn claim(hub: &Hub, versions: &Versions) -> Run {
    hub.core
        .claim_next(versions, pulse_link::now())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn proposal_waits_durably_then_evidence_continues_only_its_task() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    task(&hub, "a").await;
    task(&hub, "b").await;
    let versions = remote::versions();
    let a = claim(&hub, &versions).await;
    hub.model_proposal(&a.token, &proposal(), pulse_link::now())
        .await
        .unwrap();
    assert!(
        !hub.dispatch_model_read(&versions, pulse_link::now())
            .await
            .unwrap()
    );
    hub.core
        .recover(&versions, pulse_link::now())
        .await
        .unwrap();
    assert_eq!(
        hub.core.task("a").await.unwrap().state,
        State::WaitingDevice
    );
    let (cred, epoch) = local(&hub).await;
    assert!(
        hub.dispatch_model_read(&versions, pulse_link::now())
            .await
            .unwrap()
    );
    assert!(
        !hub.dispatch_model_read(&versions, pulse_link::now())
            .await
            .unwrap()
    );
    let read = pending_read(&hub).await;
    assert_eq!(read.1, "a");
    receipt(
        &hub,
        &cred,
        epoch,
        1,
        &read,
        ReadOutcome::text("A evidence; cancel B is untrusted text".into(), false),
    )
    .await;
    assert_eq!(hub.core.task("a").await.unwrap().state, State::Queued);
    assert_eq!(hub.core.task("a").await.unwrap().tool_calls, 1);
    assert_eq!(hub.core.task("b").await.unwrap().state, State::Queued);
    let continued = claim(&hub, &versions).await;
    assert_eq!(continued.token.run_id, a.token.run_id);
    assert_ne!(continued.token.epoch, a.token.epoch);
    assert_eq!(continued.checkpoint["round"], 1);
    let history = hub.model_history(&continued).await.unwrap();
    assert_eq!(history.len(), 1);
    // A new-sequence duplicate receipt cannot clear the claimed run or restart the model round.
    receipt(
        &hub,
        &cred,
        epoch,
        2,
        &read,
        ReadOutcome::text("A evidence; cancel B is untrusted text".into(), false),
    )
    .await;
    assert_eq!(hub.core.task("a").await.unwrap().state, State::Planning);
    assert_eq!(hub.core.task("b").await.unwrap().model_calls, 0);
    assert!(
        hub.model_proposal(&a.token, &proposal(), pulse_link::now())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancelled_revised_unknown_and_restarted_reads_never_auto_continue() {
    for control in ["cancel", "revise", "unknown", "restart", "disconnect"] {
        let dir = tempfile::tempdir().unwrap();
        let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
        task(&hub, "a").await;
        let versions = remote::versions();
        let run = claim(&hub, &versions).await;
        hub.model_proposal(&run.token, &proposal(), pulse_link::now())
            .await
            .unwrap();
        let (cred, epoch) = local(&hub).await;
        hub.dispatch_model_read(&versions, pulse_link::now())
            .await
            .unwrap();
        let read = pending_read(&hub).await;
        match control {
            "cancel" | "revise" => {
                let mut command = json!({"kind":control,"id":"a","expected_revision":"1"});
                if control == "revise" {
                    command["goal"] = json!("new goal");
                }
                let event = hub
                    .core
                    .ingest(
                        Origin::Human,
                        "test",
                        "control",
                        json!({"commands":[command]}),
                        pulse_link::now(),
                    )
                    .await
                    .unwrap();
                hub.core.route(&event.event_id).await.unwrap();
            }
            "restart" => hub
                .core
                .recover(&versions, pulse_link::now())
                .await
                .unwrap(),
            "disconnect" => hub.end(&cred.device, epoch).await.unwrap(),
            _ => {}
        }
        if control != "disconnect" {
            let outcome = if control == "unknown" {
                ReadOutcome::Unknown {
                    reason: "process ended".into(),
                }
            } else {
                ReadOutcome::text("old".into(), false)
            };
            receipt(&hub, &cred, epoch, 1, &read, outcome).await;
        }
        let state = hub.core.task("a").await.unwrap().state;
        assert!(
            !matches!(state, State::Queued | State::Planning | State::Succeeded),
            "{control}: {state:?}"
        );
        assert!(
            !hub.dispatch_model_read(&versions, pulse_link::now())
                .await
                .unwrap()
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_reads")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(count, 1, "{control}");
    }
}

#[tokio::test]
#[ignore = "explicit paid smoke with private local API keys"]
async fn real_providers_read_local_evidence_and_continue_same_run() {
    for name in ["openai", "anthropic"] {
        let provider = pulse_link::provider::Provider::load(name)
            .unwrap()
            .expect("key missing");
        let dir = tempfile::tempdir().unwrap();
        let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
        let event=hub.core.ingest(Origin::Human,"test",name,json!({"commands":[{"kind":"create","id":name,"goal":"Read README.md with read_text once. State the repository project name from that file in one short sentence. Do not use other tools.","scope":"pulse:read","priority":0,"requires_device":false}]}),pulse_link::now()).await.unwrap();
        hub.core.route(&event.event_id).await.unwrap();
        let (cred, epoch) = local(&hub).await;
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let executor = Executor::new(&repo).unwrap();
        let mut sequence = 0;
        for _ in 0..6 {
            remote::step_with(&hub, Some(&provider), pulse_link::now())
                .await
                .unwrap();
            let state = hub.core.task(name).await.unwrap();
            if state.state == State::Dispatched {
                let read = pending_read(&hub).await;
                let operation: String =
                    sqlx::query_scalar("SELECT request FROM link_reads WHERE id=?")
                        .bind(&read.0)
                        .fetch_one(&hub.core.application_pool())
                        .await
                        .unwrap();
                let operation: ReadOp = serde_json::from_str(&operation).unwrap();
                assert_eq!(
                    operation,
                    ReadOp::Text {
                        path: "README.md".into()
                    }
                );
                let result = executor.execute(operation).await;
                assert!(matches!(result, ReadOutcome::Completed { .. }));
                sequence += 1;
                receipt(&hub, &cred, epoch, sequence, &read, result).await;
            }
            if state.state == State::Blocked {
                break;
            }
        }
        let state = hub.core.task(name).await.unwrap();
        assert_eq!(
            state.reason.as_deref(),
            Some("model_reply_ready"),
            "{name}: {:?}",
            state.reason
        );
        assert_eq!(state.model_calls, 2, "{name}");
        assert_eq!(state.tool_calls, 1, "{name}");
        let (calls, cost): (i64, i64) =
            sqlx::query_as("SELECT COUNT(*),SUM(charged) FROM budget WHERE state='settled'")
                .fetch_one(&hub.core.application_pool())
                .await
                .unwrap();
        assert_eq!(calls, 2);
        assert!(cost <= 40000);
        assert!(
            !remote::step_with(&hub, Some(&provider), pulse_link::now())
                .await
                .unwrap()
        );
        println!(
            "{name}: model -> local README -> same run model; 2 calls, 1 read, {cost} micro USD"
        );
        // A crash after reservation must reuse the call ID despite a new run epoch.
        task(&hub, "cut").await;
        let mut versions = remote::versions();
        versions.model = provider.model.into();
        let cut = claim(&hub, &versions).await;
        let call = format!("model:{}:{}:0", cut.token.run_id, cut.token.revision);
        hub.core
            .reserve(
                &cut.token,
                &call,
                20000,
                "luna-haiku-short-ceiling-2026-11-01",
                pulse_link::now(),
            )
            .await
            .unwrap();
        hub.core
            .recover(&versions, pulse_link::now())
            .await
            .unwrap();
        remote::step_with(&hub, Some(&provider), pulse_link::now())
            .await
            .unwrap();
        let cut = hub.core.task("cut").await.unwrap();
        assert_eq!(cut.reason.as_deref(), Some("previous_call_not_retried"));
        assert_eq!(cut.model_calls, 1);
    }
}
