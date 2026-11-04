use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

const PRIVATE: &str = "RETENTION_OWNED_WEBSOCKET_PRIVATE_REPLY";

async fn cancelled_task(f: &Fixture, old: i64) -> pulse_core::Task {
    let event = f.hub.core.ingest(Origin::Human, "fixture", "retention-task", json!({"commands":[{
        "kind":"create", "id":"retained", "goal":PRIVATE, "scope":"pulse:read", "priority":0, "requires_device":false
    }]}), old).await.unwrap();
    f.hub.core.route(&event.event_id).await.unwrap();
    let event = f
        .hub
        .core
        .ingest(
            Origin::Human,
            "fixture",
            "cancel-retention-task",
            json!({"commands":[{
                "kind":"cancel", "id":"retained", "expected_revision":"1"
            }]}),
            old + 1,
        )
        .await
        .unwrap();
    f.hub.core.route(&event.event_id).await.unwrap();
    f.hub.core.task("retained").await.unwrap()
}

#[tokio::test]
async fn busy_retention_guard_never_queues_an_unchecked_task_snapshot() {
    let f = setup().await;
    let now = crate::now();
    cancelled_task(&f, now - pulse_core::MEMORY_RETENTION_MS - 1000).await;
    let epoch = f.hub.begin(&f.journal.device, 0, now).await.unwrap();
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM link_outbox")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(f._root.path().join("server.db.memory-lock"))
        .unwrap();
    lock.lock().unwrap();
    f.hub.snapshot(&f.journal.device, epoch, now).await.unwrap();
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM link_outbox")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(
        after, queued,
        "a busy guard cannot authorize an ordinary snapshot"
    );
    drop(lock);
    f.hub.core.maintain(now).await.unwrap();
    f.hub.snapshot(&f.journal.device, epoch, now).await.unwrap();
    let frames = f.hub.outgoing(&f.journal.device).await.unwrap();
    assert!(!frames.is_empty());
    assert!(!serde_json::to_string(&frames).unwrap().contains(PRIVATE));
}

#[tokio::test]
async fn copied_snapshots_recheck_expiry_before_send_even_with_an_unrelated_live_memory() {
    for derived in [false, true] {
        let f = setup().await;
        let now = crate::now();
        let task = cancelled_task(&f, now - pulse_core::MEMORY_RETENTION_MS - 1000).await;
        let epoch = f.hub.begin(&f.journal.device, 0, now).await.unwrap();
        let mut payload = ServerPayload::Snapshot {
            tasks: vec![task.clone()],
            messages: vec![],
            synced_at: now,
            local: LocalStatus::default(),
        };
        if derived {
            f.hub
                .core
                .change_memory(
                    Origin::Human,
                    &MemoryRequest {
                        source_id: uuid::Uuid::new_v4().to_string(),
                        source_at: now,
                        command: pulse_core::MemoryCommand::Remember {
                            label: "unrelated".into(),
                            body: "LIVE_UNRELATED_MEMORY".into(),
                        },
                    },
                    now,
                )
                .await
                .unwrap();
            let memory = f.hub.core.list_memories(now).await.unwrap().remove(0);
            payload = ServerPayload::DerivedSnapshot {
                tasks: vec![task],
                messages: vec![],
                synced_at: now,
                local: LocalStatus::default(),
                sources: vec![pulse_core::TaskMemorySource {
                    memory_id: memory.id,
                    revision: memory.revision,
                    source_id: memory.source_id,
                    expires_at: memory.expires_at,
                }],
            };
            f.hub
                .projection_erasure_ready(&f.journal.device, epoch, now)
                .await
                .unwrap();
        }
        let mut tx = f.hub.db.begin().await.unwrap();
        Hub::enqueue(&mut tx, &f.journal.device, epoch, payload)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let copied = f.hub.outgoing(&f.journal.device).await.unwrap().remove(0);
        assert!(serde_json::to_string(&copied).unwrap().contains(PRIVATE));
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(f._root.path().join("server.db.memory-lock"))
            .unwrap();
        lock.lock().unwrap();
        assert!(matches!(
            f.hub.projection_delivery(&copied, now).await,
            Err(crate::Error::Core(pulse_core::Error::Busy))
        ));
        drop(lock);
        sqlx::raw_sql("CREATE TRIGGER fail_retention BEFORE UPDATE OF data ON tasks WHEN NEW.id='retained' AND json_extract(NEW.data,'$.reason')='task_retention_expired' BEGIN SELECT RAISE(ABORT,'owned fault'); END;")
            .execute(&f.hub.db).await.unwrap();
        assert!(f.hub.core.maintain(now).await.is_err());
        assert!(f.hub.projection_delivery(&copied, now).await.is_err());
        sqlx::query("DROP TRIGGER fail_retention")
            .execute(&f.hub.db)
            .await
            .unwrap();
        let (lease, header) = f.hub.projection_delivery(&copied, now).await.unwrap();
        let header =
            header.expect("the copied payload must be replaced by its original erased header");
        assert_eq!(header.id, copied.id);
        assert_eq!(header.sequence, copied.sequence);
        assert_eq!(header.binding_digest, copied.binding_digest);
        drop(lease);
        if derived {
            assert_eq!(
                f.hub.core.list_memories(now).await.unwrap()[0].body,
                "LIVE_UNRELATED_MEMORY"
            );
        }
    }
}

#[tokio::test]
async fn memory_bound_task_keeps_its_expiry_marker_in_the_chat_snapshot() {
    let f = setup().await;
    let now = crate::now();
    let old = now - pulse_core::MEMORY_RETENTION_MS - 1000;
    f.hub
        .core
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: old,
                command: pulse_core::MemoryCommand::Remember {
                    label: "owned retention fixture".into(),
                    body: "owned source".into(),
                },
            },
            old,
        )
        .await
        .unwrap();
    let memory = f.hub.core.list_memories(old).await.unwrap().remove(0);
    let event = f.hub.core.ingest(Origin::Human, "fixture", "bound-task", json!({"commands":[{
        "kind":"create", "id":"bound", "goal":"owned retention fixture", "scope":"pulse:read", "priority":0, "requires_device":false
    }]}), old).await.unwrap();
    f.hub.core.route(&event.event_id).await.unwrap();
    f.hub
        .core
        .bind_task_memories(
            Origin::Human,
            &pulse_core::TaskMemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: old,
                task_id: "bound".into(),
                task_revision: 1,
                sources: vec![pulse_core::MemoryRef {
                    id: memory.id,
                    revision: memory.revision,
                }],
            },
            old,
        )
        .await
        .unwrap();
    let event = f
        .hub
        .core
        .ingest(
            Origin::Human,
            "fixture",
            "cancel-bound",
            json!({"commands":[{
                "kind":"cancel", "id":"bound", "expected_revision":"1"
            }]}),
            old + 1,
        )
        .await
        .unwrap();
    f.hub.core.route(&event.event_id).await.unwrap();
    f.hub.core.maintain(now).await.unwrap();
    assert_eq!(
        f.hub.core.task("bound").await.unwrap().reason.as_deref(),
        Some("task_retention_expired")
    );
    let epoch = f.hub.begin(&f.journal.device, 0, now).await.unwrap();
    f.hub.snapshot(&f.journal.device, epoch, now).await.unwrap();
    let frames = f.hub.outgoing(&f.journal.device).await.unwrap();
    let task = frames
        .iter()
        .find_map(|frame| match &frame.payload {
            ServerPayload::Snapshot { tasks, .. }
            | ServerPayload::DerivedSnapshot { tasks, .. } => {
                tasks.iter().find(|task| task.id == "bound")
            }
            _ => None,
        })
        .expect("expired task in the server-owned snapshot");
    assert_eq!(task.reason.as_deref(), Some("task_retention_expired"));
    assert!(task.cancel_requested && task.active_run.is_none());
}

async fn cache(f: &Fixture) -> String {
    sqlx::query_scalar::<_, String>("SELECT value FROM local_meta WHERE key='cache'")
        .fetch_optional(&f.journal.db)
        .await
        .unwrap()
        .unwrap_or_default()
}

#[tokio::test]
async fn real_websocket_expiry_clears_acked_and_unacked_non_memory_replies_without_model_calls() {
    for acknowledge in [false, true] {
        let f = setup().await;
        crate::memory_chat::client::sync(&f.config, &f.journal, &f.credential)
            .await
            .unwrap();
        let now = crate::now();
        let old = now - pulse_core::MEMORY_RETENTION_MS - 1000;
        let event=f.hub.core.ingest(Origin::Human,"fixture","task",json!({"commands":[{"kind":"create","id":"retained","goal":"write an owned fixture reply","scope":"pulse:read","priority":0,"requires_device":false}]}),old).await.unwrap();
        f.hub.core.route(&event.event_id).await.unwrap();
        let run = f
            .hub
            .core
            .claim_next(&crate::remote::versions(), old + 1)
            .await
            .unwrap()
            .unwrap();
        f.hub
            .model_reply(
                &run.token,
                &crate::provider::Reply {
                    text: PRIVATE.into(),
                    tool: None,
                    usage: json!({}),
                    cost: 0,
                },
                "fixture",
                old + 2,
            )
            .await
            .unwrap();
        let event = f
            .hub
            .core
            .ingest(
                Origin::Human,
                "fixture",
                "cancel",
                json!({"commands":[{"kind":"cancel","id":"retained","expected_revision":"1"}]}),
                old + 3,
            )
            .await
            .unwrap();
        f.hub.core.route(&event.event_id).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let mut url = f.config.server.join("v1/devices/connect").unwrap();
            url.set_scheme("ws").unwrap();
            let mut request = url.as_str().into_client_request().unwrap();
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
            let accepted = loop {
                let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                    continue;
                };
                match crate::protocol::parse::<ServerWire>(text.as_bytes()).unwrap() {
                    ServerWire::Welcome { .. } => socket
                        .send(Message::Text(
                            serde_json::to_string(&ClientWire::ProjectionErasureReady)
                                .unwrap()
                                .into(),
                        ))
                        .await
                        .unwrap(),
                    ServerWire::Data {
                        envelope,
                        permitted,
                    } => {
                        f.journal
                            .accept(&envelope, permitted, 1, true)
                            .await
                            .unwrap();
                        if cache(&f).await.contains(PRIVATE) {
                            break envelope;
                        }
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
                    _ => panic!("unexpected initial retention frame"),
                }
            };
            if acknowledge {
                socket
                    .send(Message::Text(
                        serde_json::to_string(&ClientWire::Ack {
                            sequence: accepted.sequence.clone(),
                            id: accepted.id.clone(),
                        })
                        .unwrap()
                        .into(),
                    ))
                    .await
                    .unwrap();
            }
            f.hub.core.maintain(now).await.unwrap();
            // Normal remote polling publishes the durable bodyless change.
            assert!(crate::remote::step(&f.hub, now).await.unwrap());
            let mut original_erased = false;
            loop {
                let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                    continue;
                };
                match crate::protocol::parse::<ServerWire>(text.as_bytes()).unwrap() {
                    ServerWire::ErasedProjection { header } => {
                        if header.id == accepted.id {
                            assert_eq!(header.binding_digest, accepted.binding_digest);
                            original_erased = true;
                        }
                        f.journal.accept_erased_projection(&header).await.unwrap();
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
                        f.journal
                            .accept(&envelope, permitted, 1, true)
                            .await
                            .unwrap();
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
                    _ => panic!("unexpected retention cleanup frame"),
                }
                if !cache(&f).await.contains(PRIVATE) && (acknowledge || original_erased) {
                    break;
                }
            }
            assert!(
                !std::fs::read(f.config.data_dir.join("device.db"))
                    .unwrap()
                    .windows(PRIVATE.len())
                    .any(|v| v == PRIVATE.as_bytes())
            );
            let calls: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
                .fetch_one(&f.hub.db)
                .await
                .unwrap();
            assert_eq!(calls, 0);
            socket.close(None).await.unwrap();
        })
        .await
        .expect("bounded loopback task expiry");
    }
}

#[tokio::test]
async fn terminal_expiry_registers_non_memory_worker_cleanup_and_discards_late_body_after_clock_rollback()
 {
    let f = setup().await;
    let now = crate::now();
    let old = now - pulse_core::MEMORY_RETENTION_MS - 1000;
    let chat_epoch = f.hub.begin(&f.journal.device, 0, old).await.unwrap();
    let worker = uuid::Uuid::new_v4().to_string();
    let code = f.hub.pairing(ClientRole::LocalExecutor, old).await.unwrap();
    f.hub
        .redeem(
            PairRequest {
                code,
                device: worker.clone(),
                role: ClientRole::LocalExecutor,
            },
            old,
        )
        .await
        .unwrap();
    let epoch = f.hub.begin(&worker, 0, old).await.unwrap();
    let e=f.hub.core.ingest(Origin::Human,"fixture","worker-task",json!({"commands":[{"kind":"create","id":"retained-worker","goal":"owned worker result","scope":"pulse:read","priority":0,"requires_device":false}]}),old).await.unwrap();
    f.hub.core.route(&e.event_id).await.unwrap();
    let input = Envelope::new(
        &f.journal.device,
        1,
        chat_epoch,
        DevicePayload::Human {
            source_id: uuid::Uuid::new_v4().to_string(),
            action: HumanAction::Read {
                task_id: "retained-worker".into(),
                revision: "1".into(),
                operation: ReadOp::Text {
                    path: "OWNED_RETAINED_PATH.md".into(),
                },
            },
        },
    )
    .unwrap();
    f.hub
        .receive(&f.journal.device, &input, old + 1)
        .await
        .unwrap();
    let read = f
        .hub
        .outgoing(&worker)
        .await
        .unwrap()
        .into_iter()
        .find(|f| matches!(f.payload, ServerPayload::Read { .. }))
        .unwrap();
    let ServerPayload::Read {
        operation_id,
        task_id,
        revision,
        ..
    } = &read.payload
    else {
        unreachable!()
    };
    let original = Envelope::new(
        &worker,
        1,
        epoch,
        DevicePayload::Result {
            operation_id: operation_id.clone(),
            task_id: task_id.clone(),
            revision: revision.clone(),
            outcome: ReadOutcome::text("OWNED_RETAINED_WORKER_BODY".into(), false),
        },
    )
    .unwrap();
    f.hub.receive(&worker, &original, old + 2).await.unwrap();
    let e = f
        .hub
        .core
        .ingest(
            Origin::Human,
            "fixture",
            "cancel-worker",
            json!({"commands":[{"kind":"cancel","id":task_id,"expected_revision":"1"}]}),
            old + 3,
        )
        .await
        .unwrap();
    f.hub.core.route(&e.event_id).await.unwrap();
    f.hub.core.maintain(now).await.unwrap();
    let context: String = sqlx::query_scalar("SELECT context FROM memory_erased_reads WHERE id=?")
        .bind(operation_id)
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    let context: pulse_core::WorkerErasure = serde_json::from_str(&context).unwrap();
    assert!(context.valid() && !context.is_effect);
    assert_eq!(context.read_header.as_ref().unwrap().id, read.id);
    let late = Envelope::new(
        &worker,
        2,
        epoch,
        DevicePayload::Result {
            operation_id: operation_id.clone(),
            task_id: task_id.clone(),
            revision: revision.clone(),
            outcome: ReadOutcome::text("LATE_RETAINED_WORKER_BODY".into(), false),
        },
    )
    .unwrap();
    f.hub.receive(&worker, &late, old + 4).await.unwrap();
    let row: (String, String, Option<String>) =
        sqlx::query_as("SELECT state,request,result FROM link_reads WHERE id=?")
            .bind(operation_id)
            .fetch_one(&f.hub.db)
            .await
            .unwrap();
    assert_eq!(row, ("discarded".into(), "null".into(), None));
    let texts: Vec<String> =
        sqlx::query_scalar("SELECT body FROM link_chat UNION ALL SELECT payload FROM events")
            .fetch_all(&f.hub.db)
            .await
            .unwrap();
    assert!(!texts.join("").contains("OWNED_RETAINED_PATH"));
    assert!(!texts.join("").contains("RETAINED_WORKER_BODY"));
    assert_eq!(
        f.hub.core.task(task_id).await.unwrap().reason.as_deref(),
        Some("task_retention_expired")
    );
}
