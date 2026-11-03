use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use pulse_link::artifact::{ArtifactRef, CHUNK};
const PRIVATE: &str = "PRIVATE_TOOL_RESULT_AND_IMAGE";
struct Read {
    chat: String,
    chat_epoch: u64,
    worker: String,
    epoch: u64,
    id: String,
    revision: String,
}
async fn read(hub: &Hub) -> Read {
    let (chat, chat_epoch) = pair(hub, ClientRole::Chat).await;
    let (worker, epoch) = pair(hub, ClientRole::LocalExecutor).await;
    let source = uuid::Uuid::new_v4().to_string();
    let frame = Envelope::new(
        &chat,
        1,
        chat_epoch,
        DevicePayload::Human {
            source_id: source.clone(),
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
    hub.receive(&chat, &frame, pulse_link::now()).await.unwrap();
    let id = format!("read:{source}");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM link_reads WHERE id=?")
            .bind(&id)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap(),
        1
    );
    Read {
        chat,
        chat_epoch,
        worker,
        epoch,
        id,
        revision: "1".into(),
    }
}
fn packet(read: &Read, sequence: u64, outcome: ReadOutcome) -> Envelope<DevicePayload> {
    Envelope::new(
        &read.worker,
        sequence,
        read.epoch,
        DevicePayload::Result {
            operation_id: read.id.clone(),
            task_id: "task".into(),
            revision: read.revision.clone(),
            outcome,
        },
    )
    .unwrap()
}
async fn send(hub: &Hub, read: &Read, sequence: u64, outcome: ReadOutcome) {
    hub.receive(
        &read.worker,
        &packet(read, sequence, outcome),
        pulse_link::now(),
    )
    .await
    .unwrap();
}
async fn saved(hub: &Hub, read: &Read) -> (String, Option<String>) {
    sqlx::query_as("SELECT state,result FROM link_reads WHERE id=?")
        .bind(&read.id)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap()
}
async fn count(hub: &Hub, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn tool_source_erasure_removes_result_event_chat_and_transport_without_ack_conflict() {
    let (root, hub, memory) = setup().await;
    let read = read(&hub).await;
    let frame = packet(&read, 1, ReadOutcome::text(PRIVATE.into(), false));
    hub.receive(&read.worker, &frame, pulse_link::now())
        .await
        .unwrap();
    assert!(saved(&hub, &read).await.1.unwrap().contains(PRIVATE));
    hub.projection_erasure_ready(&read.chat, read.chat_epoch, pulse_link::now())
        .await
        .unwrap();
    hub.snapshot(&read.chat, read.chat_epoch, pulse_link::now())
        .await
        .unwrap();
    let before = hub
        .outgoing(&read.chat)
        .await
        .unwrap()
        .into_iter()
        .find(|f| matches!(f.payload, ServerPayload::DerivedSnapshot { .. }))
        .unwrap();
    let ServerPayload::DerivedSnapshot {
        messages, sources, ..
    } = &before.payload
    else {
        panic!()
    };
    let message = messages.iter().find(|m| m.id == read.id).unwrap();
    assert_eq!(message.memory_sources.as_ref(), Some(sources));
    hub.ack(&read.chat, number(&before.sequence).unwrap(), &before.id)
        .await
        .unwrap();
    forget(&hub, &memory).await;
    assert_eq!(saved(&hub, &read).await, ("discarded".into(), None));
    let payload: String = sqlx::query_scalar(
        "SELECT payload FROM events WHERE source_id=? AND source LIKE 'device-result:%'",
    )
    .bind(&read.id)
    .fetch_one(&hub.core.application_pool())
    .await
    .unwrap();
    assert!(!payload.contains(PRIVATE));
    assert!(payload.contains("source_erased"));
    let erased = hub
        .erased_projection(&read.chat, &before.sequence)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(erased.binding_digest, before.binding_digest);
    hub.receive(&read.worker, &frame, pulse_link::now())
        .await
        .unwrap();
    send(
        &hub,
        &read,
        2,
        ReadOutcome::text(format!("{PRIVATE}_LATE"), false),
    )
    .await;
    assert_eq!(saved(&hub, &read).await, ("discarded".into(), None));
    assert_eq!(count(&hub, "SELECT count(*) FROM conflicts").await, 0);
    hub.core.close().await;
    let bytes = std::fs::read(root.path().join("db")).unwrap();
    assert!(
        !bytes
            .windows(PRIVATE.len())
            .any(|s| s == PRIVATE.as_bytes())
    );
}

#[tokio::test]
async fn tool_source_late_unknown_stays_unknown_and_discarded_read_never_resumes_the_task() {
    let (_root, hub, memory) = setup().await;
    let read = read(&hub).await;
    forget(&hub, &memory).await;
    send(
        &hub,
        &read,
        1,
        ReadOutcome::Unknown {
            reason: PRIVATE.into(),
        },
    )
    .await;
    assert_eq!(saved(&hub, &read).await, ("unknown".into(), None));
    assert_eq!(
        hub.core.task("task").await.unwrap().state,
        State::OutcomeUnknown
    );
    send(&hub, &read, 2, ReadOutcome::text(PRIVATE.into(), false)).await;
    assert_eq!(saved(&hub, &read).await, ("discarded".into(), None));
    let task = hub.core.task("task").await.unwrap();
    assert_eq!(task.state, State::Blocked);
    assert!(task.active_run.is_none());
    assert_eq!(
        count(
            &hub,
            "SELECT count(*) FROM events WHERE source LIKE 'device-result:%'"
        )
        .await,
        0
    );
    assert_eq!(count(&hub, "SELECT count(*) FROM runs").await, 0);
    assert_eq!(count(&hub, "SELECT count(*) FROM budget").await, 0);
}

#[tokio::test]
async fn tool_source_chat_and_provenance_failure_roll_back_result_and_orphan_event_is_erasable() {
    let (_root, hub, memory) = setup().await;
    let read = read(&hub).await;
    sqlx::query("CREATE TRIGGER fail_result_tag BEFORE INSERT ON memory_derived_messages BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&hub.core.application_pool()).await.unwrap();
    let failed = packet(&read, 1, ReadOutcome::text(PRIVATE.into(), false));
    assert!(
        hub.receive(&read.worker, &failed, pulse_link::now())
            .await
            .is_err()
    );
    assert!(saved(&hub, &read).await.1.is_none());
    assert_eq!(count(&hub, "SELECT count(*) FROM link_chat").await, 0);
    assert_eq!(
        count(
            &hub,
            "SELECT in_cursor FROM link_device WHERE role='local_executor'"
        )
        .await,
        0
    );
    sqlx::query("DROP TRIGGER fail_result_tag")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    forget(&hub, &memory).await;
    let payload: String = sqlx::query_scalar(
        "SELECT payload FROM events WHERE source_id=? AND source LIKE 'device-result:%'",
    )
    .bind(&read.id)
    .fetch_one(&hub.core.application_pool())
    .await
    .unwrap();
    assert!(!payload.contains(PRIVATE));
    hub.receive(&read.worker, &failed, pulse_link::now())
        .await
        .unwrap();
    assert_eq!(saved(&hub, &read).await, ("discarded".into(), None));
}

async fn browser(hub: &Hub, memory: &Memory) -> (Read, ArtifactRef, Vec<u8>) {
    let now = pulse_link::now();
    let event=hub.core.ingest(Origin::Human,"fixture","revise",json!({"commands":[Command::Revise{id:"task".into(),expected_revision:1,goal:"讀取測試頁".into(),scope:Some("browser:https://example.com/".into())}]}),now).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    hub.core
        .bind_task_memories(
            Origin::Human,
            &TaskMemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                task_id: "task".into(),
                task_revision: 2,
                sources: vec![MemoryRef {
                    id: memory.id.clone(),
                    revision: 1,
                }],
            },
            now,
        )
        .await
        .unwrap();
    let (chat, chat_epoch) = pair(hub, ClientRole::Chat).await;
    let (worker, epoch) = pair(hub, ClientRole::LocalExecutor).await;
    let run = hub
        .core
        .claim_next(&remote::versions(), now)
        .await
        .unwrap()
        .unwrap();
    hub.model_proposal(
        &run.token,
        &Reply {
            text: String::new(),
            tool: Some(pulse_link::provider::ToolCall {
                call_id: "image".into(),
                operation: ReadOp::Browser {
                    url: "https://example.com/".into(),
                },
            }),
            usage: json!({}),
            cost: 0,
        },
        now,
    )
    .await
    .unwrap();
    hub.browser_ready(&worker, epoch, None, pulse_link::now())
        .await
        .unwrap();
    assert!(
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    let frame = hub
        .outgoing(&worker)
        .await
        .unwrap()
        .into_iter()
        .find(|e| matches!(e.payload, ServerPayload::Read { .. }))
        .unwrap();
    let ServerPayload::Read { operation_id, .. } = frame.payload else {
        panic!()
    };
    let mut png=STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=").unwrap();
    png.extend_from_slice(PRIVATE.as_bytes());
    let artifact = ArtifactRef {
        task_id: "task".into(),
        revision: "2".into(),
        digest: hash_bytes(&png),
        length: png.len() as u32,
        url: "https://example.com/".into(),
        computer: None,
        title: PRIVATE.into(),
        observed_at: now,
        width: 1,
        height: 1,
    };
    (
        Read {
            chat,
            chat_epoch,
            worker,
            epoch,
            id: operation_id,
            revision: "2".into(),
        },
        artifact,
        png,
    )
}
fn chunk(
    read: &Read,
    artifact: &ArtifactRef,
    bytes: &[u8],
    sequence: u64,
) -> Envelope<DevicePayload> {
    assert!(bytes.len() < CHUNK);
    Envelope::new(
        &read.worker,
        sequence,
        read.epoch,
        DevicePayload::ArtifactChunk {
            operation_id: read.id.clone(),
            artifact: artifact.clone(),
            index: 0,
            data: STANDARD.encode(bytes),
        },
    )
    .unwrap()
}

#[tokio::test]
async fn tool_source_erasure_removes_original_png_and_refuses_late_repopulation_and_download() {
    let (root, hub, memory) = setup().await;
    let (read, artifact, png) = browser(&hub, &memory).await;
    hub.receive(
        &read.worker,
        &chunk(&read, &artifact, &png, 1),
        pulse_link::now(),
    )
    .await
    .unwrap();
    send(&hub, &read, 2, ReadOutcome::text(PRIVATE.into(), false)).await;
    assert_eq!(
        hub.artifact_png(&read.chat, &read.id, &artifact.digest, pulse_link::now())
            .await
            .unwrap(),
        png
    );
    forget(&hub, &memory).await;
    assert!(
        hub.artifact_png(&read.chat, &read.id, &artifact.digest, pulse_link::now())
            .await
            .is_err()
    );
    hub.receive(
        &read.worker,
        &chunk(&read, &artifact, &png, 3),
        pulse_link::now(),
    )
    .await
    .unwrap();
    send(&hub, &read, 4, ReadOutcome::text(PRIVATE.into(), false)).await;
    assert_eq!(count(&hub, "SELECT count(*) FROM link_artifacts").await, 0);
    assert_eq!(
        count(&hub, "SELECT count(*) FROM link_artifact_parts").await,
        0
    );
    hub.core.close().await;
    let bytes = std::fs::read(root.path().join("db")).unwrap();
    assert!(
        !bytes
            .windows(PRIVATE.len())
            .any(|s| s == PRIVATE.as_bytes())
    );
}

#[tokio::test]
async fn tool_source_failed_sql_erasure_denies_download_and_new_result_until_reconciled() {
    let (_root, hub, memory) = setup().await;
    let (read, artifact, png) = browser(&hub, &memory).await;
    hub.receive(
        &read.worker,
        &chunk(&read, &artifact, &png, 1),
        pulse_link::now(),
    )
    .await
    .unwrap();
    send(&hub, &read, 2, ReadOutcome::text(PRIVATE.into(), false)).await;
    sqlx::query("CREATE TRIGGER fail_png_erasure BEFORE DELETE ON link_artifacts BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&hub.core.application_pool()).await.unwrap();
    let now = pulse_link::now();
    assert!(
        hub.core
            .change_memory(
                Origin::Human,
                &MemoryRequest {
                    source_id: uuid::Uuid::new_v4().to_string(),
                    source_at: now,
                    command: MemoryCommand::Forget {
                        id: memory.id,
                        revision: 1
                    }
                },
                now
            )
            .await
            .is_err()
    );
    assert!(
        hub.artifact_png(&read.chat, &read.id, &artifact.digest, pulse_link::now())
            .await
            .is_err()
    );
    let late = packet(
        &read,
        3,
        ReadOutcome::text(format!("{PRIVATE}_LATE"), false),
    );
    assert!(
        hub.receive(&read.worker, &late, pulse_link::now())
            .await
            .is_err()
    );
    assert_eq!(
        count(
            &hub,
            "SELECT in_cursor FROM link_device WHERE role='local_executor'"
        )
        .await,
        2
    );
    sqlx::query("DROP TRIGGER fail_png_erasure")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    hub.receive(&read.worker, &late, pulse_link::now())
        .await
        .unwrap();
    assert_eq!(saved(&hub, &read).await, ("discarded".into(), None));
    assert_eq!(count(&hub, "SELECT count(*) FROM link_artifacts").await, 0);
}

#[tokio::test]
async fn tool_source_schema15_and16_restore_backfills_then_scrubs_evidence() {
    use sqlx::{Connection, Executor};
    for (schema, invalidated) in [(15, false), (15, true), (16, false), (16, true)] {
        let (root, hub, memory) = setup().await;
        let read = read(&hub).await;
        // Both previous schemas retained command bodies and original frames.
        let request: String = sqlx::query_scalar("SELECT request FROM link_reads WHERE id=?")
            .bind(&read.id)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        let frames: Vec<(i64, String)> = sqlx::query_as("SELECT sequence,data FROM link_outbox WHERE device=? AND json_extract(data,'$.payload.operation_id')=?")
            .bind(&read.worker)
            .bind(&read.id)
            .fetch_all(&hub.core.application_pool())
            .await
            .unwrap();
        let encoded = serde_json::to_string(&ReadOutcome::text(PRIVATE.into(), false)).unwrap();
        send(&hub, &read, 1, ReadOutcome::text(PRIVATE.into(), false)).await;
        if invalidated {
            forget(&hub, &memory).await;
        }
        hub.core.close().await;
        let mut db = sqlx::SqliteConnection::connect(&format!(
            "sqlite:{}",
            root.path().join("db").display()
        ))
        .await
        .unwrap();
        db.execute("DROP TABLE link_worker_cleanup; ALTER TABLE link_device DROP COLUMN worker_cleanup; DROP TABLE link_worker_fences; DROP TABLE link_erased_work; ALTER TABLE link_device DROP COLUMN worker_erasure").await.unwrap();
        if schema == 15 {
            db.execute("DROP TABLE memory_erased_reads; DELETE FROM _sqlx_migrations WHERE version IN (16,17,18); PRAGMA user_version=15").await.unwrap();
        } else {
            db.execute("ALTER TABLE memory_erased_reads DROP COLUMN context; DELETE FROM _sqlx_migrations WHERE version IN (17,18); PRAGMA user_version=16").await.unwrap();
        }
        if schema == 15 {
            sqlx::query("DELETE FROM memory_derived_messages WHERE id=?")
                .bind(&read.id)
                .execute(&mut db)
                .await
                .unwrap();
            sqlx::query("UPDATE link_reads SET state='completed',result=? WHERE id=?")
                .bind(encoded)
                .bind(&read.id)
                .execute(&mut db)
                .await
                .unwrap();
            sqlx::query("UPDATE link_chat SET body=? WHERE id=?")
                .bind(PRIVATE)
                .bind(&read.id)
                .execute(&mut db)
                .await
                .unwrap();
        }
        sqlx::query("UPDATE link_reads SET request=? WHERE id=?")
            .bind(request)
            .bind(&read.id)
            .execute(&mut db)
            .await
            .unwrap();
        for (sequence, data) in frames {
            sqlx::query("UPDATE link_outbox SET data=? WHERE device=? AND sequence=?")
                .bind(data)
                .bind(&read.worker)
                .bind(sequence)
                .execute(&mut db)
                .await
                .unwrap();
        }
        db.close().await.unwrap();
        let hub = Hub::new(Store::open(root.path().join("db"), "owner").await.unwrap());
        assert_eq!(
            count(&hub, "SELECT count(*) FROM memory_derived_messages").await,
            1
        );
        if !invalidated {
            forget(&hub, &memory).await;
        }
        assert_eq!(saved(&hub, &read).await, ("discarded".into(), None));
        let context: String =
            sqlx::query_scalar("SELECT context FROM memory_erased_reads WHERE id=?")
                .bind(&read.id)
                .fetch_one(&hub.core.application_pool())
                .await
                .unwrap();
        let context: pulse_core::WorkerErasure = serde_json::from_str(&context).unwrap();
        assert!(!context.is_effect);
        assert!(context.read_header.is_some());
        hub.worker_erasure_ready(&read.worker, read.epoch, pulse_link::now())
            .await
            .unwrap();
        assert!(matches!(
            hub.deliveries(&read.worker).await.unwrap()[0],
            pulse_link::server::Delivery::ErasedWork(_)
        ));
        let body: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
            .bind(&read.id)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert!(!body.contains(PRIVATE));
    }
}

#[tokio::test]
async fn tool_source_orphan_proposal_event_is_scrubbed_even_before_request_commit() {
    let (_root, hub, memory) = setup().await;
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    let payload = json!({"commands":[Command::Query{ids:vec!["task".into()]}],"operation_proposal":OperationProposal{id:"orphan".into(),token:run.token.clone(),target:"browser:https://example.com/".into(),expires_at:pulse_link::now()+1000,intent:json!({"text":PRIVATE})}});
    let event = hub
        .core
        .ingest(
            Origin::Tool,
            "browser-proposal",
            "orphan",
            payload.clone(),
            pulse_link::now(),
        )
        .await
        .unwrap();
    let human = hub
        .core
        .ingest(
            Origin::Human,
            "fixture",
            "independent-human",
            payload,
            pulse_link::now(),
        )
        .await
        .unwrap();
    forget(&hub, &memory).await;
    let erased: String = sqlx::query_scalar("SELECT payload FROM events WHERE id=?")
        .bind(&event.event_id)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(!erased.contains(PRIVATE));
    let retained: String = sqlx::query_scalar("SELECT payload FROM events WHERE id=?")
        .bind(&human.event_id)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(retained.contains(PRIVATE));
    assert!(
        matches!(hub.core.route(&event.event_id).await.unwrap(),Outcome::Applied{task_ids} if task_ids.is_empty())
    );
    assert!(hub.core.task("task").await.unwrap().active_run.is_none());
}

#[tokio::test]
async fn tool_source_erased_old_revision_does_not_prevent_review_of_fresh_independent_evidence() {
    let (_root, hub, memory) = setup().await;
    let mut read = read(&hub).await;
    send(&hub, &read, 1, ReadOutcome::text(PRIVATE.into(), false)).await;
    let old = read.id.clone();
    forget(&hub, &memory).await;
    let now = pulse_link::now();
    let event=hub.core.ingest(Origin::Human,"fixture","independent",json!({"commands":[Command::Revise{id:"task".into(),expected_revision:1,goal:"新的獨立工作".into(),scope:None}]}),now).await.unwrap();
    assert!(matches!(
        hub.core.route(&event.event_id).await.unwrap(),
        Outcome::Applied { .. }
    ));
    let source = uuid::Uuid::new_v4().to_string();
    let input = Envelope::new(
        &read.chat,
        2,
        read.chat_epoch,
        DevicePayload::Human {
            source_id: source.clone(),
            action: HumanAction::Read {
                task_id: "task".into(),
                revision: "2".into(),
                operation: ReadOp::Text {
                    path: "src/lib.rs".into(),
                },
            },
        },
    )
    .unwrap();
    hub.receive(&read.chat, &input, pulse_link::now())
        .await
        .unwrap();
    read.id = format!("read:{source}");
    read.revision = "2".into();
    send(
        &hub,
        &read,
        2,
        ReadOutcome::text("NEW_INDEPENDENT_EVIDENCE".into(), false),
    )
    .await;
    let event = hub
        .core
        .ingest(
            Origin::Human,
            "fixture",
            "review-fresh",
            json!({"commands":[Command::ReconcileRead{id:"task".into(),expected_revision:2}]}),
            pulse_link::now(),
        )
        .await
        .unwrap();
    assert!(matches!(
        hub.core.route(&event.event_id).await.unwrap(),
        Outcome::Applied { .. }
    ));
    let task = hub.core.task("task").await.unwrap();
    assert_eq!(task.reason.as_deref(), Some("read_receipts_verified"));
    assert_ne!(task.state, State::Succeeded);
    assert!(
        task.evidence
            .iter()
            .any(|e| e.contains(&format!("verified-read:{}:", read.id)))
    );
    assert!(
        !task
            .evidence
            .iter()
            .any(|e| e.contains(&format!("verified-read:{old}:")))
    );
}
