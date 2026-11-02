use super::*;
use crate::{
    journal::DeviceLock,
    server::{Delivery, Hub},
};
use pulse_core::{
    Command, MemoryCommand, MemoryRef, MemoryRequest, Origin, State, Store, TaskMemoryRequest,
};
use serde_json::json;
const BODY: &str = "WORKER_SOURCE_BODY";
struct Fixture {
    _root: tempfile::TempDir,
    hub: Hub,
    journal: Journal,
    _lock: DeviceLock,
    read: Envelope<ServerPayload>,
    chat: String,
    chat_epoch: u64,
    epoch: u64,
    memory: pulse_core::Memory,
    credential: Credential,
}
async fn pair(hub: &Hub, role: ClientRole, device: String) -> (Credential, u64) {
    let now = crate::now();
    let credential = hub
        .redeem(
            PairRequest {
                code: hub.pairing(role, now).await.unwrap(),
                device: device.clone(),
                role,
            },
            now,
        )
        .await
        .unwrap();
    let epoch = hub.begin(&device, 0, now).await.unwrap();
    (credential, epoch)
}
async fn setup() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let (journal, lock) = Journal::open(
        &root.path().join("worker"),
        "https://pulse.example/",
        root.path(),
    )
    .await
    .unwrap();
    let chat = uuid::Uuid::new_v4().to_string();
    let (_, chat_epoch) = pair(&hub, ClientRole::Chat, chat.clone()).await;
    let (credential, epoch) = pair(&hub, ClientRole::LocalExecutor, journal.device.clone()).await;
    let now = crate::now();
    hub.core
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                command: MemoryCommand::Remember {
                    label: "偏好".into(),
                    body: "繁體中文".into(),
                },
            },
            now,
        )
        .await
        .unwrap();
    let memory = hub.core.list_memories(now).await.unwrap().remove(0);
    let event=hub.core.ingest(Origin::Human,"fixture","task",json!({"commands":[Command::Create{id:"task".into(),goal:"分析測試檔".into(),scope:"pulse:read".into(),priority:0,requires_device:false,parent:None,dependencies:vec![]}]}),now).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    hub.core
        .bind_task_memories(
            Origin::Human,
            &TaskMemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                task_id: "task".into(),
                task_revision: 1,
                sources: vec![MemoryRef {
                    id: memory.id.clone(),
                    revision: 1,
                }],
            },
            now,
        )
        .await
        .unwrap();
    let source = uuid::Uuid::new_v4().to_string();
    let input = Envelope::new(
        &chat,
        1,
        chat_epoch,
        DevicePayload::Human {
            source_id: source,
            action: HumanAction::Read {
                task_id: "task".into(),
                revision: "1".into(),
                operation: ReadOp::Text {
                    path: format!("{BODY}.md"),
                },
            },
        },
    )
    .unwrap();
    hub.receive(&chat, &input, crate::now()).await.unwrap();
    // A legacy worker must not receive a source-bound command, even as denied data.
    assert!(hub.deliveries(&journal.device).await.unwrap().is_empty());
    hub.worker_erasure_ready(&journal.device, epoch, crate::now())
        .await
        .unwrap();
    let read = hub
        .outgoing(&journal.device)
        .await
        .unwrap()
        .into_iter()
        .find(|e| matches!(e.payload, ServerPayload::Read { .. }))
        .unwrap();
    Fixture {
        _root: root,
        hub,
        journal,
        _lock: lock,
        read,
        chat,
        chat_epoch,
        epoch,
        memory,
        credential,
    }
}
async fn forget(f: &Fixture) -> WorkerErasure {
    f.hub
        .core
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: crate::now(),
                command: MemoryCommand::Forget {
                    id: f.memory.id.clone(),
                    revision: 1,
                },
            },
            crate::now(),
        )
        .await
        .unwrap();
    let ServerPayload::Read { operation_id, .. } = &f.read.payload else {
        panic!()
    };
    serde_json::from_str(
        &sqlx::query_scalar::<_, String>("SELECT context FROM memory_erased_reads WHERE id=?")
            .bind(operation_id)
            .fetch_one(&f.hub.core.application_pool())
            .await
            .unwrap(),
    )
    .unwrap()
}
async fn accept(f: &Fixture, ack: bool) -> Envelope<ServerPayload> {
    f.journal
        .accept(&f.read, true, f.epoch, true)
        .await
        .unwrap();
    if ack {
        f.hub
            .ack(
                &f.journal.device,
                number(&f.read.sequence).unwrap(),
                &f.read.id,
            )
            .await
            .unwrap();
    }
    f.journal.take_work().await.unwrap().unwrap()
}
async fn send_erased(f: &Fixture) {
    for item in f.journal.deliveries().await.unwrap() {
        match item {
            DeviceDelivery::Data(data) => f
                .hub
                .receive(&f.journal.device, &data, crate::now())
                .await
                .unwrap(),
            DeviceDelivery::Erased(data) => {
                f.hub
                    .receive_erased_worker(
                        &f.journal.device,
                        &data.header,
                        &data.operation_id,
                        &data.source_id,
                    )
                    .await
                    .unwrap();
                f.journal
                    .ack(number(&data.header.sequence).unwrap(), &data.header.id)
                    .await
                    .unwrap();
            }
        }
    }
}
#[tokio::test]
async fn worker_erasure_real_socket_reconnect_uses_bodyless_retry_and_negative_ack_only() {
    use futures_util::{SinkExt, StreamExt};
    use std::{future::IntoFuture, time::Duration};
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
    let f = setup().await;
    let work = accept(&f, false).await;
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    forget(&f).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server =
        tokio::spawn(axum::serve(listener, crate::server::router(f.hub.clone())).into_future());
    let mut request = format!("ws://{address}/v1/devices/connect")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", f.credential.token).parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            serde_json::to_string(&ClientWire::Hello {
                device: f.journal.device.clone(),
                cursor: "0".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    let mut original_erased = false;
    let mut fresh_fence = false;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !original_erased || !fresh_fence {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            assert!(!text.contains(BODY));
            match parse::<ServerWire>(text.as_bytes()).unwrap() {
                ServerWire::Welcome { .. } => {
                    socket
                        .send(Message::Text(
                            serde_json::to_string(&ClientWire::WorkerErasureReady)
                                .unwrap()
                                .into(),
                        ))
                        .await
                        .unwrap();
                }
                ServerWire::ErasedWork { header, context } => {
                    assert_eq!(header.id, f.read.id);
                    assert_eq!(header.binding_digest, f.read.binding_digest);
                    f.journal
                        .accept_worker_erasure(&header, &context)
                        .await
                        .unwrap();
                    original_erased = true;
                    socket
                        .send(Message::Text(
                            serde_json::to_string(&ClientWire::Ack {
                                sequence: header.sequence,
                                id: header.id,
                            })
                            .unwrap()
                            .into(),
                        ))
                        .await
                        .unwrap();
                }
                ServerWire::Data {
                    envelope,
                    permitted,
                } => {
                    let epoch = number(&envelope.epoch).unwrap();
                    f.journal
                        .accept(&envelope, permitted, epoch, true)
                        .await
                        .unwrap();
                    fresh_fence |= matches!(envelope.payload, ServerPayload::EraseWork { .. });
                    socket
                        .send(Message::Text(
                            serde_json::to_string(&ClientWire::Ack {
                                sequence: envelope.sequence,
                                id: envelope.id,
                            })
                            .unwrap()
                            .into(),
                        ))
                        .await
                        .unwrap();
                }
                other => panic!("unexpected server frame: {other:?}"),
            }
        }
        let DeviceDelivery::Erased(output) = f.journal.deliveries().await.unwrap().remove(0) else {
            panic!()
        };
        let wire = ClientWire::ErasedData {
            header: output.header.clone(),
            operation_id: output.operation_id,
            source_id: output.source_id,
        };
        // Repeat after dropping the first acknowledgement, with the same header.
        for _ in 0..2 {
            socket
                .send(Message::Text(serde_json::to_string(&wire).unwrap().into()))
                .await
                .unwrap();
            loop {
                let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                    continue;
                };
                assert!(!text.contains(BODY));
                match parse::<ServerWire>(text.as_bytes()).unwrap() {
                    ServerWire::Ack { sequence, id } => {
                        assert_eq!(sequence, output.header.sequence);
                        assert_eq!(id, output.header.id);
                        break;
                    }
                    ServerWire::Data {
                        envelope,
                        permitted,
                    } => {
                        f.journal
                            .accept(&envelope, permitted, number(&envelope.epoch).unwrap(), true)
                            .await
                            .unwrap();
                    }
                    ServerWire::ErasedWork { header, context } => {
                        f.journal
                            .accept_worker_erasure(&header, &context)
                            .await
                            .unwrap();
                    }
                    other => panic!("unexpected server frame: {other:?}"),
                }
            }
        }
        f.journal
            .ack(number(&output.header.sequence).unwrap(), &output.header.id)
            .await
            .unwrap();
    })
    .await
    .unwrap();
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    assert_eq!(
        f.hub.core.task("task").await.unwrap().state,
        State::OutcomeUnknown
    );
    socket.close(None).await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn worker_erasure_removes_multipart_image_bodies_and_keeps_each_original_ack() {
    use crate::artifact::{ArtifactRef, CHUNK};
    use base64::{Engine, engine::general_purpose::STANDARD};
    let f = setup().await;
    let work = accept(&f, true).await;
    let ServerPayload::Read { operation_id, .. } = &work.payload else {
        panic!()
    };
    let mut png = STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=").unwrap();
    png.resize(CHUNK + 100, 0);
    let encoded = STANDARD.encode(&png[..CHUNK])[..64].to_owned();
    let artifact = ArtifactRef {
        task_id: "task".into(),
        revision: "1".into(),
        digest: hash_bytes(&png),
        length: png.len() as u32,
        url: "https://example.com/".into(),
        computer: None,
        title: BODY.into(),
        observed_at: crate::now(),
        width: 1,
        height: 1,
    };
    artifact.verify(&png).unwrap();
    let mut tx = f.journal.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    for (index, bytes) in png.chunks(CHUNK).enumerate() {
        f.journal
            .enqueue_in(
                &mut tx,
                DevicePayload::ArtifactChunk {
                    operation_id: operation_id.clone(),
                    artifact: artifact.clone(),
                    index: index as u32,
                    data: STANDARD.encode(bytes),
                },
                f.epoch,
            )
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    let originals = f.journal.outgoing().await.unwrap();
    assert_eq!(originals.len(), 3);
    f.journal.flush_memory().await.unwrap();
    let before = std::fs::read(f._root.path().join("worker/device.db")).unwrap();
    assert!(before.windows(BODY.len()).any(|v| v == BODY.as_bytes()));
    assert!(
        before
            .windows(encoded.len())
            .any(|v| v == encoded.as_bytes())
    );
    let context = forget(&f).await;
    f.journal
        .accept_worker_erasure(context.read_header.as_ref().unwrap(), &context)
        .await
        .unwrap();
    let erased = f.journal.deliveries().await.unwrap();
    assert_eq!(erased.len(), 3);
    for (original, item) in originals.iter().zip(&erased) {
        let DeviceDelivery::Erased(item) = item else {
            panic!()
        };
        assert_eq!(item.header.id, original.id);
        assert_eq!(item.header.binding_digest, original.binding_digest);
    }
    let bytes = std::fs::read(f._root.path().join("worker/device.db")).unwrap();
    assert!(!bytes.windows(BODY.len()).any(|v| v == BODY.as_bytes()));
    assert!(
        !bytes
            .windows(encoded.len())
            .any(|v| v == encoded.as_bytes())
    );
    send_erased(&f).await;
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    assert_eq!(
        f.hub.core.task("task").await.unwrap().state,
        State::OutcomeUnknown
    );
}

#[tokio::test]
async fn worker_erasure_removes_unacked_commands_and_cannot_be_execution_proof() {
    let f = setup().await;
    let work = accept(&f, false).await;
    let context = forget(&f).await;
    let delivery = f.hub.deliveries(&f.journal.device).await.unwrap();
    let Delivery::ErasedWork(ctx) = &delivery[0] else {
        panic!()
    };
    assert_eq!(**ctx, context);
    let header = context.read_header.as_ref().unwrap();
    assert!(
        !f.journal
            .accept_worker_erasure(header, &context)
            .await
            .unwrap()
    );
    assert!(f.journal.work_erased(&context.operation_id).await.unwrap());
    f.journal
        .finish_work(&work, ReadOutcome::text(format!("{BODY}_LATE"), false))
        .await
        .unwrap();
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    let value: String = sqlx::query_scalar("SELECT envelope FROM local_work")
        .fetch_one(&f.journal.db)
        .await
        .unwrap();
    assert_eq!(value, "null");
    f.hub
        .ack(
            &f.journal.device,
            number(&header.sequence).unwrap(),
            &header.id,
        )
        .await
        .unwrap();
    assert_eq!(
        f.hub.core.task("task").await.unwrap().state,
        State::OutcomeUnknown
    );
    assert!(
        !f.journal
            .accept_worker_erasure(header, &context)
            .await
            .unwrap()
    );
}
#[tokio::test]
async fn worker_erasure_after_lost_result_ack_preserves_both_direction_header_identity() {
    let f = setup().await;
    let work = accept(&f, true).await;
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    let packet = f.journal.outgoing().await.unwrap().remove(0);
    f.hub
        .receive(&f.journal.device, &packet, crate::now())
        .await
        .unwrap();
    let context = forget(&f).await;
    // This worker had already acknowledged its command; it still needs a new fence.
    f.hub
        .flush_worker_erasures(&f.journal.device, f.epoch)
        .await
        .unwrap();
    for item in f.hub.deliveries(&f.journal.device).await.unwrap() {
        if let Delivery::Data(envelope) = item {
            f.journal
                .accept(&envelope, true, f.epoch, true)
                .await
                .unwrap();
            f.hub
                .ack(
                    &f.journal.device,
                    number(&envelope.sequence).unwrap(),
                    &envelope.id,
                )
                .await
                .unwrap();
        }
    }
    let packets = f.journal.deliveries().await.unwrap();
    let DeviceDelivery::Erased(erased) = &packets[0] else {
        panic!()
    };
    assert_eq!(erased.header.id, packet.id);
    assert_eq!(erased.header.binding_digest, packet.binding_digest);
    f.hub
        .receive_erased_worker(
            &f.journal.device,
            &erased.header,
            &erased.operation_id,
            &erased.source_id,
        )
        .await
        .unwrap();
    send_erased(&f).await;
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    f.journal.flush_memory().await.unwrap();
    let bytes = std::fs::read(f._root.path().join("worker/device.db")).unwrap();
    assert!(!bytes.windows(BODY.len()).any(|v| v == BODY.as_bytes()));
    let request: String = sqlx::query_scalar("SELECT request FROM link_reads WHERE id=?")
        .bind(&context.operation_id)
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(request, "null");
}
#[tokio::test]
async fn worker_erasure_negative_keeper_replays_over_a_pre_erasure_journal_restore() {
    let f = setup().await;
    let work = accept(&f, true).await;
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    f.journal.flush_memory().await.unwrap();
    let db = f._root.path().join("worker/device.db");
    let backup = f._root.path().join("snapshot");
    std::fs::copy(&db, &backup).unwrap();
    let context = forget(&f).await;
    f.journal
        .core
        .record_worker_erasure(&context, &hash(&f.journal.server).unwrap())
        .await
        .unwrap();
    f.journal.core.close().await;
    drop(f._lock);
    std::fs::copy(backup, &db).unwrap();
    let (journal, _lock) = Journal::open(
        &f._root.path().join("worker"),
        "https://pulse.example/",
        f._root.path(),
    )
    .await
    .unwrap();
    assert!(journal.take_work().await.unwrap().is_none());
    assert!(matches!(
        &journal.deliveries().await.unwrap()[0],
        DeviceDelivery::Erased(_)
    ));
    let bytes = std::fs::read(&db).unwrap();
    assert!(!bytes.windows(BODY.len()).any(|v| v == BODY.as_bytes()));
}
#[tokio::test]
async fn worker_erasure_roles_gaps_conflicts_and_capability_epochs_are_fenced() {
    let f = setup().await;
    let context = forget(&f).await;
    let header = context.read_header.as_ref().unwrap();
    let (chat, _lock) = Journal::open_role(
        &f._root.path().join("chat"),
        "https://pulse.example/",
        f._root.path(),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    assert!(chat.accept_worker_erasure(header, &context).await.is_err());
    assert!(
        f.hub
            .worker_erasure_ready(&f.chat, f.chat_epoch, crate::now())
            .await
            .is_err()
    );
    assert!(
        f.hub
            .worker_erasure_ready(&f.journal.device, f.epoch + 1, crate::now())
            .await
            .is_err()
    );
    let epoch = f
        .hub
        .begin(&f.journal.device, 0, crate::now())
        .await
        .unwrap();
    assert!(
        f.hub
            .deliveries(&f.journal.device)
            .await
            .unwrap()
            .is_empty()
    );
    f.hub
        .worker_erasure_ready(&f.journal.device, epoch, crate::now())
        .await
        .unwrap();
    assert!(
        !f.hub
            .deliveries(&f.journal.device)
            .await
            .unwrap()
            .is_empty()
    );
    let mut gap = header.clone();
    gap.sequence = "2".into();
    gap.binding_digest = hash(&(
        gap.version,
        &gap.id,
        &gap.device,
        &gap.sequence,
        &gap.epoch,
        &gap.digest,
    ))
    .unwrap();
    let mut wrong = context.clone();
    wrong.read_header = Some(gap.clone());
    assert!(f.journal.accept_worker_erasure(&gap, &wrong).await.is_err());
    assert_eq!(f.journal.cursor().await.unwrap(), 0);
    assert!(
        f.journal
            .accept_worker_erasure(header, &context)
            .await
            .unwrap()
    );
    let mut conflict = header.clone();
    conflict.id = "different".into();
    conflict.binding_digest = hash(&(
        conflict.version,
        &conflict.id,
        &conflict.device,
        &conflict.sequence,
        &conflict.epoch,
        &conflict.digest,
    ))
    .unwrap();
    let mut wrong = context.clone();
    wrong.read_header = Some(conflict.clone());
    assert!(
        f.journal
            .accept_worker_erasure(&conflict, &wrong)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn worker_erasure_sql_failure_blocks_execution_and_late_result_persistence() {
    let f = setup().await;
    f.journal
        .accept(&f.read, true, f.epoch, true)
        .await
        .unwrap();
    let context = forget(&f).await;
    sqlx::query("CREATE TRIGGER fail_worker_redaction BEFORE UPDATE ON local_work BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&f.journal.db).await.unwrap();
    assert!(
        f.journal
            .accept_worker_erasure(context.read_header.as_ref().unwrap(), &context)
            .await
            .is_err()
    );
    assert!(f.journal.work_erased(&context.operation_id).await.unwrap());
    assert!(f.journal.take_work().await.is_err());
    assert!(
        f.journal
            .finish_work(&f.read, ReadOutcome::text(format!("{BODY}_LATE"), false))
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM local_outbox")
            .fetch_one(&f.journal.db)
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_worker_redaction")
        .execute(&f.journal.db)
        .await
        .unwrap();
    assert!(f.journal.take_work().await.unwrap().is_none());
    f.journal
        .finish_work(&f.read, ReadOutcome::text(format!("{BODY}_LATE"), false))
        .await
        .unwrap();
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    let bytes = std::fs::read(f._root.path().join("worker/device.db")).unwrap();
    assert!(!bytes.windows(BODY.len()).any(|v| v == BODY.as_bytes()));
}

#[tokio::test]
async fn worker_erasure_sql_failure_keeps_a_durable_negative_rule_and_cannot_send_old_bodies() {
    let f = setup().await;
    let work = accept(&f, true).await;
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    let context = forget(&f).await;
    sqlx::query("CREATE TRIGGER fail_worker_redaction BEFORE UPDATE ON local_work BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&f.journal.db).await.unwrap();
    assert!(
        f.journal
            .core
            .record_worker_erasure(&context, &hash(&f.journal.server).unwrap())
            .await
            .is_err()
    );
    assert!(f.journal.deliveries().await.is_err());
    sqlx::query("DROP TRIGGER fail_worker_redaction")
        .execute(&f.journal.db)
        .await
        .unwrap();
    assert!(matches!(
        &f.journal.deliveries().await.unwrap()[0],
        DeviceDelivery::Erased(_)
    ));
    assert!(
        f.journal
            .core
            .worker_task_erased(
                &f.journal.device,
                &hash(&f.journal.server).unwrap(),
                "task",
                "1"
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn worker_erasure_blocks_future_work_of_the_same_revision_without_saving_arguments() {
    let f = setup().await;
    let context = forget(&f).await;
    let header = context.read_header.as_ref().unwrap();
    f.journal
        .accept_worker_erasure(header, &context)
        .await
        .unwrap();
    let frame = Envelope::new(
        &f.journal.device,
        2,
        f.epoch,
        ServerPayload::Read {
            operation_id: "retry".into(),
            task_id: "task".into(),
            revision: "1".into(),
            operation: ReadOp::Text {
                path: format!("{BODY}_FUTURE.md"),
            },
        },
    )
    .unwrap();
    f.journal.accept(&frame, true, f.epoch, true).await.unwrap();
    assert!(f.journal.take_work().await.unwrap().is_none());
    let rows: Vec<String> = sqlx::query_scalar("SELECT data FROM local_outbox")
        .fetch_all(&f.journal.db)
        .await
        .unwrap();
    assert!(rows.iter().all(|s| !s.contains(BODY)));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM local_work")
            .fetch_one(&f.journal.db)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn worker_erasure_cannot_ack_live_work_or_another_role_and_retries_physical_checkpoint() {
    use sqlx::{Connection, Executor};
    let f = setup().await;
    let work = accept(&f, true).await;
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    let packet = f.journal.outgoing().await.unwrap().remove(0);
    let header = ErasedProjection::from_value(&serde_json::to_value(&packet).unwrap()).unwrap();
    let ServerPayload::Read { operation_id, .. } = &f.read.payload else {
        panic!()
    };
    assert!(
        f.hub
            .receive_erased_worker(
                &f.journal.device,
                &header,
                operation_id,
                &uuid::Uuid::new_v4().to_string()
            )
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT in_cursor FROM link_device WHERE role='local_executor'"
        )
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap(),
        0
    );
    let context = forget(&f).await;
    f.journal.flush_memory().await.unwrap();
    let mut reader = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        f._root.path().join("worker/device.db").display()
    ))
    .await
    .unwrap();
    reader
        .execute("BEGIN; SELECT data FROM local_outbox")
        .await
        .unwrap();
    assert!(
        f.journal
            .accept_worker_erasure(context.read_header.as_ref().unwrap(), &context)
            .await
            .is_err()
    );
    reader.execute("ROLLBACK").await.unwrap();
    reader.close().await.unwrap();
    assert!(
        !f.journal
            .accept_worker_erasure(context.read_header.as_ref().unwrap(), &context)
            .await
            .unwrap()
    );
    let bytes = std::fs::read(f._root.path().join("worker/device.db")).unwrap();
    assert!(!bytes.windows(BODY.len()).any(|v| v == BODY.as_bytes()));
    let mut other = header;
    other.device = f.chat.clone();
    other.binding_digest = hash(&(
        other.version,
        &other.id,
        &other.device,
        &other.sequence,
        &other.epoch,
        &other.digest,
    ))
    .unwrap();
    assert!(
        f.hub
            .receive_erased_worker(&f.chat, &other, operation_id, &context.source_id)
            .await
            .is_err()
    );
}
