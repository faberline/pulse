use futures_util::{SinkExt, StreamExt};
use pulse_core::{Command, Origin, State, Store};
use pulse_link::{
    protocol::*,
    provider::{Reply, ToolCall},
    remote,
    server::{Hub, router},
};
use serde_json::json;
use std::{future::IntoFuture, time::Duration};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

async fn hub(dir: &std::path::Path) -> Hub {
    Hub::new(Store::open(dir.join("db"), "owner").await.unwrap())
}
async fn task(hub: &Hub, requires_device: bool) {
    let event=hub.core.ingest(Origin::Human,"fixture","task",json!({"commands":[{"kind":"create","id":"task","goal":"Use the saved evidence","scope":"pulse:read","priority":0,"requires_device":requires_device}]}),pulse_link::now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
}
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

#[tokio::test]
async fn completed_read_identity_rejects_aliases_in_both_new_and_legacy_requests() {
    for (saved, proposed) in [
        ("docs/contracts.md", "docs//contracts.md"),
        ("docs//contracts.md", "docs/contracts.md"),
        ("docs/./contracts.md", "docs/contracts.md"),
        ("docs/contracts.md/", "docs/contracts.md"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let hub = hub(dir.path()).await;
        let repo = tempfile::tempdir().unwrap();
        std::fs::create_dir(repo.path().join("docs")).unwrap();
        std::fs::write(repo.path().join("docs/contracts.md"), "saved evidence").unwrap();
        task(&hub, false).await;
        let chat = pair(&hub, ClientRole::Chat).await;
        let worker = pair(&hub, ClientRole::LocalExecutor).await;
        let epoch = hub
            .begin(&worker.device, 0, pulse_link::now())
            .await
            .unwrap();
        let message = Envelope::new(
            &chat.device,
            1,
            0,
            DevicePayload::Human {
                source_id: "read".into(),
                action: HumanAction::Read {
                    task_id: "task".into(),
                    revision: "1".into(),
                    operation: ReadOp::Text { path: saved.into() },
                },
            },
        )
        .unwrap();
        hub.receive(&chat.device, &message, pulse_link::now())
            .await
            .unwrap();
        let outcome = pulse_link::executor::Executor::new(repo.path())
            .unwrap()
            .read_text(saved)
            .unwrap();
        let result = Envelope::new(
            &worker.device,
            1,
            epoch,
            DevicePayload::Result {
                operation_id: "read:read".into(),
                task_id: "task".into(),
                revision: "1".into(),
                outcome,
            },
        )
        .unwrap();
        hub.receive(&worker.device, &result, pulse_link::now())
            .await
            .unwrap();
        for (source, command) in [
            (
                "review",
                Command::ReconcileRead {
                    id: "task".into(),
                    expected_revision: 1,
                },
            ),
            (
                "resume",
                Command::Resume {
                    id: "task".into(),
                    expected_revision: 1,
                },
            ),
        ] {
            let event = hub
                .core
                .ingest(
                    Origin::Human,
                    "fixture",
                    source,
                    json!({"commands":[command]}),
                    pulse_link::now(),
                )
                .await
                .unwrap();
            assert!(matches!(
                hub.core.route(&event.event_id).await.unwrap(),
                pulse_core::Outcome::Applied { .. }
            ));
        }
        let run = hub
            .core
            .claim_next(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
            .unwrap();
        let reply = |path: &str| Reply {
            text: String::new(),
            tool: Some(ToolCall {
                call_id: "next_call".into(),
                operation: ReadOp::Text { path: path.into() },
            }),
            usage: json!({}),
            cost: 0,
        };
        assert!(
            hub.model_proposal(&run.token, &reply(proposed), pulse_link::now())
                .await
                .is_err(),
            "repeated {saved} as {proposed}"
        );
        assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 1);
        let reads: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_reads")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(reads, 1);
        // Rejecting an alias must not prevent a genuinely different read.
        hub.model_proposal(&run.token, &reply("docs/other.md"), pulse_link::now())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn connected_chat_receives_expiry_without_query_or_reconnect() {
    let dir = tempfile::tempdir().unwrap();
    let hub = hub(dir.path()).await;
    task(&hub, true).await;
    let chat = pair(&hub, ClientRole::Chat).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    let mut request = format!("ws://{address}/v1/devices/connect")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", chat.token).parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            serde_json::to_string(&ClientWire::Hello {
                device: chat.device,
                cursor: "0".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    async fn wait(
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        state: State,
    ) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                    continue;
                };
                if let ServerWire::Data { envelope, .. } =
                    parse::<ServerWire>(text.as_bytes()).unwrap()
                {
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
                    if let ServerPayload::Snapshot { tasks, .. } = envelope.payload
                        && tasks.iter().any(|t| t.id == "task" && t.state == state)
                    {
                        break;
                    }
                }
            }
        })
        .await
        .expect("chat did not receive the saved task transition");
    }
    wait(&mut socket, State::WaitingDevice).await;
    let deadline = hub.core.task("task").await.unwrap().deadline;
    assert!(!remote::step(&hub, deadline).await.unwrap());
    wait(&mut socket, State::Blocked).await;
    assert_eq!(
        hub.core.task("task").await.unwrap().reason.as_deref(),
        Some("deadline_exhausted")
    );
    assert_eq!(hub.core.task("task").await.unwrap().model_calls, 0);
    let (run, changed) = hub
        .core
        .claim_next_with_changes(&remote::versions(), deadline + 1)
        .await
        .unwrap();
    assert!(run.is_none());
    assert!(
        !changed,
        "an unchanged expired task must not notify each tick"
    );
    server.abort();
    let _ = server.await;
}
