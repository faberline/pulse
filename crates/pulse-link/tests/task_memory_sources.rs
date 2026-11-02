use pulse_core::*;
use pulse_link::{protocol::*, provider::Reply, remote, server::Hub};
use serde_json::json;
const BODY: &str = "private source body remains outside provider prompts";
async fn setup() -> (tempfile::TempDir, Hub, Memory) {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(root.path().join("db"), "owner").await.unwrap());
    let now = pulse_link::now();
    hub.core
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                command: MemoryCommand::Remember {
                    label: "偏好".into(),
                    body: BODY.into(),
                },
            },
            now,
        )
        .await
        .unwrap();
    let source = hub.core.list_memories(now).await.unwrap().remove(0);
    let e=hub.core.ingest(Origin::Human,"fixture","task",json!({"commands":[Command::Create{id:"task".into(),goal:"分析 Pulse".into(),scope:"pulse:read".into(),priority:0,requires_device:false,parent:None,dependencies:vec![]}]}),now).await.unwrap();
    hub.core.route(&e.event_id).await.unwrap();
    hub.core
        .bind_task_memories(
            Origin::Human,
            &TaskMemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                task_id: "task".into(),
                task_revision: 1,
                sources: vec![MemoryRef {
                    id: source.id.clone(),
                    revision: 1,
                }],
            },
            now,
        )
        .await
        .unwrap();
    (root, hub, source)
}
async fn forget(hub: &Hub, source: &Memory) {
    let now = pulse_link::now();
    hub.core
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                command: MemoryCommand::Forget {
                    id: source.id.clone(),
                    revision: 1,
                },
            },
            now,
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn binding_metadata_does_not_enable_prompt_memory_and_withdrawal_rejects_late_model_content()
{
    let (_root, hub, source) = setup().await;
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    let (prompt, history) = hub.model_context(&run).await.unwrap();
    assert!(!prompt.contains(BODY));
    assert!(history.is_empty());
    forget(&hub, &source).await;
    assert!(hub.model_context(&run).await.is_err());
    assert!(
        hub.model_reply(
            &run.token,
            &Reply {
                text: "late private derived reply".into(),
                tool: None,
                usage: json!({}),
                cost: 0
            },
            "fixture",
            pulse_link::now()
        )
        .await
        .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM link_chat")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(remote::step(&hub, pulse_link::now()).await.unwrap()); // Coalesced durable task change, no provider call.
    assert!(!remote::step(&hub, pulse_link::now()).await.unwrap());
    assert_eq!(hub.core.task("task").await.unwrap().state, State::Blocked);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}
async fn pair(hub: &Hub, role: ClientRole) -> (String, u64) {
    let now = pulse_link::now();
    let code = hub.pairing(role, now).await.unwrap();
    let device = uuid::Uuid::new_v4().to_string();
    hub.redeem(
        PairRequest {
            code,
            device: device.clone(),
            role,
        },
        now,
    )
    .await
    .unwrap();
    let epoch = hub.begin(&device, 0, now).await.unwrap();
    (device, epoch)
}
#[tokio::test]
async fn a_real_persisted_read_frame_loses_delivery_permission_when_its_memory_source_is_removed() {
    let (_root, hub, source) = setup().await;
    let (chat, chat_epoch) = pair(&hub, ClientRole::Chat).await;
    let (worker, worker_epoch) = pair(&hub, ClientRole::LocalExecutor).await;
    let input = Envelope::new(
        &chat,
        1,
        chat_epoch,
        DevicePayload::Human {
            source_id: uuid::Uuid::new_v4().to_string(),
            action: HumanAction::Read {
                task_id: "task".into(),
                revision: "1".into(),
                operation: ReadOp::Text {
                    path: "README.md".into(),
                },
            },
        },
    )
    .unwrap();
    hub.receive(&chat, &input, pulse_link::now()).await.unwrap();
    let output = hub
        .outgoing(&worker)
        .await
        .unwrap()
        .into_iter()
        .find(|e| matches!(e.payload, ServerPayload::Read { .. }))
        .expect("read was not committed to the actual transport queue");
    assert!(
        hub.permitted(&output, worker_epoch, pulse_link::now())
            .await
            .unwrap()
    );
    forget(&hub, &source).await;
    assert!(
        !hub.permitted(&output, worker_epoch, pulse_link::now())
            .await
            .unwrap()
    );
    assert!(
        hub.core
            .task_memory_sources("task", 1)
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 1); // Keep the original dispatch accounting.
}
