use super::*;
use std::time::Duration;

async fn body(hub: &Hub, chat: &str) -> String {
    serde_json::to_string(&hub.outgoing(chat).await.unwrap()).unwrap()
}
async fn ready(hub: &Hub) -> (String, u64) {
    let (chat, epoch) = pair(hub, ClientRole::Chat).await;
    hub.projection_erasure_ready(&chat, epoch, pulse_link::now())
        .await
        .unwrap();
    (chat, epoch)
}
#[tokio::test]
async fn snapshot_reconciles_negative_keeper_and_cannot_save_a_stale_body_after_sql_repair() {
    let (_root, hub, memory) = setup().await;
    generated(&hub).await;
    let (chat, epoch) = ready(&hub).await;
    sqlx::query("CREATE TRIGGER block_projection_redaction BEFORE DELETE ON memories BEGIN SELECT RAISE(ABORT,'controlled projection failure'); END")
        .execute(&hub.core.application_pool()).await.unwrap();
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
                        revision: memory.revision
                    },
                },
                now
            )
            .await
            .is_err()
    );
    assert!(hub.snapshot(&chat, epoch, now).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM link_outbox")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER block_projection_redaction")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    hub.snapshot(&chat, epoch, pulse_link::now()).await.unwrap();
    assert!(!body(&hub, &chat).await.contains(DERIVED));
    assert!(
        hub.core
            .list_memories(pulse_link::now())
            .await
            .unwrap()
            .is_empty()
    );
}
#[tokio::test]
async fn invalid_adoption_proof_hides_derived_content_and_keeps_independent_human_messages() {
    let (_root, hub, _memory) = setup().await;
    generated(&hub).await;
    let binding = hub
        .core
        .task_memory_sources("task", 1)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE events SET digest='tampered' WHERE id=?")
        .bind(binding.authorization_event)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO link_chat VALUES('human-independent','KEEP_HUMAN_MESSAGE',?)")
        .bind(pulse_link::now())
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let (chat, epoch) = ready(&hub).await;
    hub.snapshot(&chat, epoch, pulse_link::now()).await.unwrap();
    let data = body(&hub, &chat).await;
    assert!(!data.contains(DERIVED));
    assert!(data.contains("KEEP_HUMAN_MESSAGE"));
}
#[tokio::test]
async fn a_redacted_message_never_renders_a_restored_old_body_or_approval() {
    let (_root, hub, memory) = setup().await;
    let id = generated(&hub).await;
    forget(&hub, &memory).await;
    sqlx::query("UPDATE link_chat SET body=? WHERE id=?")
        .bind(DERIVED)
        .bind(id)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let (chat, epoch) = ready(&hub).await;
    hub.snapshot(&chat, epoch, pulse_link::now()).await.unwrap();
    let data = body(&hub, &chat).await;
    assert!(!data.contains(DERIVED));
    assert!(data.contains("回覆內容已清除"));
}
#[tokio::test]
async fn source_busy_snapshot_keeps_human_content_and_does_not_ack_failed_queue_writes() {
    let (root, hub, _memory) = setup().await;
    generated(&hub).await;
    sqlx::query("INSERT INTO link_chat VALUES('human-independent','KEEP_HUMAN_MESSAGE',?)")
        .bind(pulse_link::now())
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let (chat, epoch) = ready(&hub).await;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.path().join("db.memory-lock"))
        .unwrap();
    lock.lock().unwrap();
    hub.snapshot(&chat, epoch, pulse_link::now()).await.unwrap();
    let data = body(&hub, &chat).await;
    assert!(!data.contains(DERIVED));
    assert!(data.contains("KEEP_HUMAN_MESSAGE"));
    lock.unlock().unwrap();
    let before: Option<String> =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='chat_core_outbox_cursor'")
            .fetch_optional(&hub.core.application_pool())
            .await
            .unwrap();
    sqlx::query("CREATE TRIGGER block_snapshot_queue BEFORE INSERT ON link_outbox BEGIN SELECT RAISE(ABORT,'controlled enqueue failure'); END")
        .execute(&hub.core.application_pool()).await.unwrap();
    assert!(hub.snapshot(&chat, epoch, pulse_link::now()).await.is_err());
    let after: Option<String> =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='chat_core_outbox_cursor'")
            .fetch_optional(&hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!(after, before);
    sqlx::query("DROP TRIGGER block_snapshot_queue")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    hub.snapshot(&chat, epoch, pulse_link::now()).await.unwrap();
    assert!(body(&hub, &chat).await.contains(DERIVED));
}

#[tokio::test]
async fn websocket_retries_a_busy_projection_without_new_human_input_or_model_work() {
    use futures_util::{SinkExt, StreamExt};
    use std::future::IntoFuture;
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
    let (root, hub, _memory) = setup().await;
    generated(&hub).await;
    sqlx::query("INSERT INTO link_chat VALUES('human-independent','KEEP_HUMAN_MESSAGE',?)")
        .bind(pulse_link::now())
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let cred = hub
        .redeem(
            PairRequest {
                code: hub
                    .pairing(ClientRole::Chat, pulse_link::now())
                    .await
                    .unwrap(),
                device: uuid::Uuid::new_v4().to_string(),
                role: ClientRole::Chat,
            },
            pulse_link::now(),
        )
        .await
        .unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.path().join("db.memory-lock"))
        .unwrap();
    lock.lock().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server =
        tokio::spawn(axum::serve(listener, pulse_link::server::router(hub.clone())).into_future());
    let mut request = format!("ws://{address}/v1/devices/connect")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", cred.token).parse().unwrap(),
    );
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(request),
    )
    .await
    .unwrap()
    .unwrap();
    socket
        .send(Message::Text(
            serde_json::to_string(&ClientWire::Hello {
                device: cred.device.clone(),
                cursor: "0".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    let welcome = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(welcome) = welcome else {
        panic!()
    };
    let ServerWire::Welcome { epoch, .. } = parse(welcome.as_bytes()).unwrap() else {
        panic!()
    };
    socket
        .send(Message::Text(
            serde_json::to_string(&ClientWire::ProjectionErasureReady)
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            if let ServerWire::Data {
                envelope,
                permitted,
            } = parse::<ServerWire>(text.as_bytes()).unwrap()
            {
                let data = serde_json::to_string(&envelope.payload).unwrap();
                assert!(!data.contains(DERIVED));
                socket
                    .send(Message::Text(
                        serde_json::to_string(&ClientWire::Ack {
                            sequence: envelope.sequence.clone(),
                            id: envelope.id.clone(),
                        })
                        .unwrap()
                        .into(),
                    ))
                    .await
                    .unwrap();
                if number(&envelope.sequence).unwrap() >= 2 && data.contains("KEEP_HUMAN_MESSAGE") {
                    assert!(permitted);
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    // No new Human input, notify_change, task or model response after releasing the lock.
    lock.unlock().unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            if let ServerWire::Data {
                envelope,
                permitted,
            } = parse::<ServerWire>(text.as_bytes()).unwrap()
            {
                let found = serde_json::to_string(&envelope.payload)
                    .unwrap()
                    .contains(DERIVED);
                socket
                    .send(Message::Text(
                        serde_json::to_string(&ClientWire::Ack {
                            sequence: envelope.sequence.clone(),
                            id: envelope.id.clone(),
                        })
                        .unwrap()
                        .into(),
                    ))
                    .await
                    .unwrap();
                if found {
                    assert!(permitted);
                    assert!(matches!(
                        envelope.payload,
                        ServerPayload::DerivedSnapshot { .. }
                    ));
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(hub.core.task("task").await.unwrap().model_calls, 0);
    let budget: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(budget, 0);
    socket.close(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while hub
            .live(&cred.device, number(&epoch).unwrap(), pulse_link::now())
            .await
            .unwrap()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
