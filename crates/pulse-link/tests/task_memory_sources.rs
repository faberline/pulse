use pulse_core::*;
use pulse_link::{protocol::*, provider::Reply, remote, server::Hub};
use serde_json::json;
const BODY: &str = "private source body remains outside provider prompts";
#[path = "task_memory_sources/tool_erasure.rs"]
mod tool_erasure;
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
    if role == ClientRole::LocalExecutor {
        hub.worker_erasure_ready(&device, epoch, now).await.unwrap();
    }
    (device, epoch)
}

const DERIVED: &str = "PRIVATE_DERIVED_SERVER_REPLY";
async fn generated(hub: &Hub) -> String {
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    hub.model_reply(
        &run.token,
        &Reply {
            text: DERIVED.into(),
            tool: None,
            usage: json!({}),
            cost: 0,
        },
        "fixture",
        pulse_link::now(),
    )
    .await
    .unwrap();
    format!("model:{}:1:0", run.token.run_id)
}
#[tokio::test]
async fn withdrawal_scrubs_acked_and_pending_projections_without_changing_ack_identity() {
    let (root, hub, source) = setup().await;
    let message = generated(&hub).await;
    let (device, epoch) = pair(&hub, ClientRole::Chat).await;
    hub.projection_erasure_ready(&device, epoch, pulse_link::now())
        .await
        .unwrap();
    hub.snapshot(&device, epoch, pulse_link::now())
        .await
        .unwrap();
    let first = hub.outgoing(&device).await.unwrap().remove(0);
    assert!(matches!(
        first.payload,
        ServerPayload::DerivedSnapshot { .. }
    ));
    assert!(serde_json::to_string(&first).unwrap().contains(DERIVED));
    hub.ack(&device, number(&first.sequence).unwrap(), &first.id)
        .await
        .unwrap();
    hub.snapshot(&device, epoch, pulse_link::now())
        .await
        .unwrap();
    let second = hub.outgoing(&device).await.unwrap().remove(0);
    forget(&hub, &source).await;
    let row: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
        .bind(&message)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(!row.contains(DERIVED));
    for original in [&first, &second] {
        let erased = hub
            .erased_projection(&device, &original.sequence)
            .await
            .unwrap()
            .unwrap();
        assert!(erased.valid());
        assert_eq!(erased.id, original.id);
        assert_eq!(erased.binding_digest, original.binding_digest);
        let data: String =
            sqlx::query_scalar("SELECT data FROM link_outbox WHERE device=? AND sequence=?")
                .bind(&device)
                .bind(number(&original.sequence).unwrap() as i64)
                .fetch_one(&hub.core.application_pool())
                .await
                .unwrap();
        assert_eq!(data, "null");
    }
    let deliveries = hub.deliveries(&device).await.unwrap();
    assert!(matches!(&deliveries[0],pulse_link::server::Delivery::Erased(h) if h.id==second.id));
    // An ACK based on the original accepted projection still succeeds.
    hub.ack(&device, number(&second.sequence).unwrap(), &second.id)
        .await
        .unwrap();
    assert!(hub.deliveries(&device).await.unwrap().is_empty());
    hub.core.close().await;
    let bytes = std::fs::read(root.path().join("db")).unwrap();
    assert!(
        !bytes
            .windows(DERIVED.len())
            .any(|s| s == DERIVED.as_bytes())
    );
}
#[tokio::test]
async fn projection_support_is_chat_only_and_must_be_reannounced_each_epoch() {
    let (_root, hub, _source) = setup().await;
    generated(&hub).await;
    let (chat, epoch) = pair(&hub, ClientRole::Chat).await;
    hub.snapshot(&chat, epoch, pulse_link::now()).await.unwrap();
    let old = hub.outgoing(&chat).await.unwrap().remove(0);
    assert!(matches!(old.payload, ServerPayload::Snapshot { .. }));
    assert!(!serde_json::to_string(&old).unwrap().contains(DERIVED));
    hub.ack(&chat, number(&old.sequence).unwrap(), &old.id)
        .await
        .unwrap();
    assert!(
        hub.projection_erasure_ready(&chat, epoch + 1, pulse_link::now())
            .await
            .is_err()
    );
    hub.projection_erasure_ready(&chat, epoch, pulse_link::now())
        .await
        .unwrap();
    hub.snapshot(&chat, epoch, pulse_link::now()).await.unwrap();
    let sourced = hub.outgoing(&chat).await.unwrap().remove(0);
    assert!(matches!(
        sourced.payload,
        ServerPayload::DerivedSnapshot { .. }
    ));
    let new_epoch = hub
        .begin(&chat, number(&old.sequence).unwrap(), pulse_link::now())
        .await
        .unwrap();
    assert!(hub.deliveries(&chat).await.unwrap().is_empty());
    assert!(
        hub.projection_erasure_ready(&chat, epoch, pulse_link::now())
            .await
            .is_err()
    );
    hub.projection_erasure_ready(&chat, new_epoch, pulse_link::now())
        .await
        .unwrap();
    assert!(!hub.deliveries(&chat).await.unwrap().is_empty());
    let (worker, worker_epoch) = pair(&hub, ClientRole::LocalExecutor).await;
    assert!(
        hub.projection_erasure_ready(&worker, worker_epoch, pulse_link::now())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn generated_message_and_lineage_roll_back_together_on_provenance_failure() {
    let (_root, hub, _source) = setup().await;
    sqlx::query("CREATE TRIGGER fail_lineage BEFORE INSERT ON memory_derived_messages BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&hub.core.application_pool()).await.unwrap();
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert!(
        hub.model_reply(
            &run.token,
            &Reply {
                text: DERIVED.into(),
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
    assert_eq!(hub.core.task("task").await.unwrap().state, State::Planning);
}

#[tokio::test]
async fn derived_message_lineage_is_immutable_and_cannot_be_downgraded_by_replay() {
    let (_root, hub, source) = setup().await;
    let id = generated(&hub).await;
    let mut db = hub.core.application_pool().acquire().await.unwrap();
    Store::track_memory_message_in(&mut db, "task", 1, &id, pulse_link::now())
        .await
        .unwrap();
    assert!(
        Store::track_memory_message_in(&mut db, "unbound-task", 1, &id, pulse_link::now())
            .await
            .is_err()
    );
    assert!(
        Store::track_memory_message_in(&mut db, "task", 2, &id, pulse_link::now())
            .await
            .is_err()
    );
    drop(db);
    let data: String = sqlx::query_scalar(
        "SELECT task_id||task_revision||redacted FROM memory_derived_messages WHERE id=?",
    )
    .bind(&id)
    .fetch_one(&hub.core.application_pool())
    .await
    .unwrap();
    assert_eq!(data, "task10");
    assert!(!data.contains(BODY));
    assert!(!data.contains(DERIVED));
    sqlx::query("INSERT INTO link_chat VALUES('independent','KEEP_INDEPENDENT_HUMAN_INPUT',0)")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    forget(&hub, &source).await;
    let mut db = hub.core.application_pool().acquire().await.unwrap();
    assert!(
        Store::track_memory_message_in(&mut db, "task", 1, &id, pulse_link::now())
            .await
            .is_err()
    );
    let body: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id='independent'")
        .fetch_one(&mut *db)
        .await
        .unwrap();
    assert_eq!(body, "KEEP_INDEPENDENT_HUMAN_INPUT");
}

#[tokio::test]
async fn erasure_can_advance_past_a_snapshot_whose_derived_messages_were_size_trimmed() {
    let (_root, hub, source) = setup().await;
    let (device, epoch) = pair(&hub, ClientRole::Chat).await;
    hub.projection_erasure_ready(&device, epoch, pulse_link::now())
        .await
        .unwrap();
    // Snapshot size trimming can remove its last derived message. The source
    // references still require erasure so that replay does not stop at this row.
    let frame = Envelope::new(
        &device,
        1,
        epoch,
        ServerPayload::DerivedSnapshot {
            tasks: vec![],
            messages: vec![],
            synced_at: pulse_link::now(),
            local: LocalStatus::default(),
            sources: vec![TaskMemorySource {
                memory_id: source.id.clone(),
                revision: source.revision,
                source_id: source.source_id.clone(),
                expires_at: source.expires_at,
            }],
        },
    )
    .unwrap();
    sqlx::query("INSERT INTO link_outbox(device,sequence,id,data) VALUES(?,?,?,?)")
        .bind(&device)
        .bind(1)
        .bind(&frame.id)
        .bind(serde_json::to_string(&frame).unwrap())
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    forget(&hub, &source).await;
    let deliveries = hub.deliveries(&device).await.unwrap();
    assert!(
        matches!(&deliveries[0],pulse_link::server::Delivery::Erased(h) if h.id==frame.id && h.binding_digest==frame.binding_digest && h.valid())
    );
    hub.ack(&device, 1, &frame.id).await.unwrap();
    hub.snapshot(&device, epoch, pulse_link::now())
        .await
        .unwrap();
    let next = hub.outgoing(&device).await.unwrap().remove(0);
    assert_eq!(next.sequence, "2");
}

#[tokio::test]
async fn correction_revocation_and_expiry_erase_generated_bodies_without_rebinding() {
    for change in 0..3 {
        let (_root, hub, source) = setup().await;
        let id = generated(&hub).await;
        match change {
            0 | 1 => {
                let now = pulse_link::now();
                let command = if change == 0 {
                    MemoryCommand::Correct {
                        id: source.id.clone(),
                        revision: 1,
                        label: "偏好".into(),
                        body: "新版來源".into(),
                    }
                } else {
                    MemoryCommand::Revoke {
                        sources: vec![source.source_id.clone()],
                        expected: Some(MemoryRef {
                            id: source.id.clone(),
                            revision: 1,
                        }),
                    }
                };
                hub.core
                    .change_memory(
                        Origin::Human,
                        &MemoryRequest {
                            source_id: uuid::Uuid::new_v4().to_string(),
                            source_at: now,
                            command,
                        },
                        now,
                    )
                    .await
                    .unwrap();
            }
            _ => {
                hub.core.list_memories(source.expires_at).await.unwrap();
            }
        }
        let body: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
            .bind(id)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert!(!body.contains(DERIVED));
        assert!(
            hub.core
                .task_memory_sources("task", 1)
                .await
                .unwrap()
                .unwrap()
                .invalidated
        );
        assert!(
            hub.core
                .lease_projection_sources(
                    &[TaskMemorySource {
                        memory_id: source.id,
                        revision: 1,
                        source_id: source.source_id,
                        expires_at: source.expires_at
                    }],
                    source.expires_at
                )
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn schema14_upgrade_backfills_model_provenance_before_replaying_withdrawal() {
    use sqlx::{Connection, Executor};
    for invalidated in [false, true] {
        let (root, hub, source) = setup().await;
        let id = generated(&hub).await;
        if invalidated {
            forget(&hub, &source).await;
        }
        hub.core.close().await;
        let mut db = sqlx::SqliteConnection::connect(&format!(
            "sqlite:{}",
            root.path().join("db").display()
        ))
        .await
        .unwrap();
        db.execute("DROP TABLE link_worker_fences; DROP TABLE link_erased_work; ALTER TABLE link_device DROP COLUMN worker_erasure; DROP TABLE memory_erased_reads; DROP TABLE memory_derived_messages; DROP TABLE link_erased_frames; ALTER TABLE link_device DROP COLUMN projection_erasure; DELETE FROM _sqlx_migrations WHERE version IN (15,16,17); PRAGMA user_version=14").await.unwrap();
        // Reconstruct a schema14 body which had no derived-content purge.
        sqlx::query("UPDATE link_chat SET body=? WHERE id=?")
            .bind(DERIVED)
            .bind(&id)
            .execute(&mut db)
            .await
            .unwrap();
        db.close().await.unwrap();
        let hub = Hub::new(Store::open(root.path().join("db"), "owner").await.unwrap());
        let lineage: (String, String, i64) = sqlx::query_as(
            "SELECT task_id,task_revision,redacted FROM memory_derived_messages WHERE id=?",
        )
        .bind(&id)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
        assert_eq!(lineage, ("task".into(), "1".into(), i64::from(invalidated)));
        if !invalidated {
            forget(&hub, &source).await;
        }
        let body: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
            .bind(id)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert!(!body.contains(DERIVED));
    }
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
