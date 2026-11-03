use super::*;
use crate::provider::Reply;
use pulse_core::{MemoryCommand, MemoryRef, MemoryRequest, Origin, Run, Store, TaskMemoryRequest};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Fixture {
    _root: tempfile::TempDir,
    hub: Hub,
    run: Run,
    call: String,
    source: Option<pulse_core::Memory>,
}
impl Fixture {
    async fn new(inherited: bool) -> Self {
        Self::new_at(inherited, now()).await
    }
    async fn new_at(inherited: bool, memory_at: i64) -> Self {
        let root = tempfile::tempdir().unwrap();
        let hub = Hub::new(Store::open(root.path().join("db"), "owner").await.unwrap());
        let time = now();
        let source = if inherited {
            hub.core
                .change_memory(
                    Origin::Human,
                    &MemoryRequest {
                        source_id: uuid::Uuid::new_v4().to_string(),
                        source_at: memory_at,
                        command: MemoryCommand::Remember {
                            label: "fixture".into(),
                            body: "never sent".into(),
                        },
                    },
                    time,
                )
                .await
                .unwrap();
            Some(hub.core.list_memories(time).await.unwrap().remove(0))
        } else {
            None
        };
        let mut commands = vec![
            json!({"kind":"create","id":"parent","goal":"reply","scope":"pulse:read","priority":0,"requires_device":false}),
        ];
        if inherited {
            commands.push(json!({"kind":"create","id":"child","goal":"reply","scope":"pulse:read","priority":100,"requires_device":false,"parent":"parent"}));
        }
        let event = hub
            .core
            .ingest(
                Origin::Human,
                "fixture",
                "create",
                json!({"commands":commands}),
                time,
            )
            .await
            .unwrap();
        hub.core.route(&event.event_id).await.unwrap();
        if let Some(source) = &source {
            for id in ["parent", "child"] {
                let request = TaskMemoryRequest {
                    source_id: uuid::Uuid::new_v4().to_string(),
                    source_at: time,
                    task_id: id.into(),
                    task_revision: 1,
                    sources: vec![MemoryRef {
                        id: source.id.clone(),
                        revision: source.revision,
                    }],
                };
                if id == "parent" {
                    hub.core
                        .bind_task_memories(Origin::Human, &request, time)
                        .await
                        .unwrap();
                } else {
                    hub.core
                        .inherit_task_memories(Origin::Human, &request, 1, time)
                        .await
                        .unwrap();
                }
            }
        }
        let run = hub
            .core
            .claim_next(&versions(), time)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            run.token.task_id,
            if inherited { "child" } else { "parent" }
        );
        let call = format!("model:{}:{}:0", run.token.run_id, run.token.revision);
        assert!(
            hub.core
                .reserve(&run.token, &call, 20_000, "fixture", time)
                .await
                .unwrap()
        );
        Self {
            _root: root,
            hub,
            run,
            call,
            source,
        }
    }
    async fn invalidate(&self, kind: &str) {
        match kind {
            "recover" => self.hub.core.recover(&versions(), now()).await.unwrap(),
            "unknown" => self.hub.core.cost_unknown(&self.call).await.unwrap(),
            "settled" => self
                .hub
                .core
                .settle(&self.call, 7, "known usage")
                .await
                .unwrap(),
            "forget" | "external-forget" => {
                let source = self.source.as_ref().unwrap();
                let external;
                let core = if kind == "external-forget" {
                    external = Store::open(self._root.path().join("db"), "owner")
                        .await
                        .unwrap();
                    &external
                } else {
                    &self.hub.core
                };
                core.change_memory(
                    Origin::Human,
                    &MemoryRequest {
                        source_id: uuid::Uuid::new_v4().to_string(),
                        source_at: now(),
                        command: MemoryCommand::Forget {
                            id: source.id.clone(),
                            revision: source.revision,
                        },
                    },
                    now(),
                )
                .await
                .unwrap();
            }
            "parent-proof" => {
                sqlx::query("UPDATE events SET digest='tampered' WHERE id=(SELECT json_extract(data,'$.authorization_event') FROM task_memory_sources WHERE task_id='parent')")
                    .execute(&self.hub.core.application_pool()).await.unwrap();
            }
            "charge" => {
                sqlx::query("UPDATE budget SET charged=0 WHERE call_id=?")
                    .bind(&self.call)
                    .execute(&self.hub.core.application_pool())
                    .await
                    .unwrap();
            }
            "missing" => {
                sqlx::query("DELETE FROM budget WHERE call_id=?")
                    .bind(&self.call)
                    .execute(&self.hub.core.application_pool())
                    .await
                    .unwrap();
            }
            kind => {
                let mut command =
                    json!({"kind":kind,"id":self.run.token.task_id,"expected_revision":"1"});
                if kind == "revise" {
                    command["goal"] = json!("new goal");
                }
                let e = self
                    .hub
                    .core
                    .ingest(
                        Origin::Human,
                        "fixture",
                        "control",
                        json!({"commands":[command]}),
                        now(),
                    )
                    .await
                    .unwrap();
                self.hub.core.route(&e.event_id).await.unwrap();
            }
        }
    }
    async fn budget(&self) -> (String, i64) {
        sqlx::query_as("SELECT state,charged FROM budget WHERE call_id=?")
            .bind(&self.call)
            .fetch_one(&self.hub.core.application_pool())
            .await
            .unwrap()
    }
}
fn reply() -> Reply {
    Reply {
        text: "controlled response".into(),
        tool: None,
        usage: json!({"fixture":true}),
        cost: 7,
    }
}

#[tokio::test]
async fn invalid_preflight_never_polls_provider_io() {
    for kind in [
        "cancel",
        "revise",
        "recover",
        "unknown",
        "settled",
        "forget",
        "parent-proof",
        "charge",
        "missing",
        "wrong-call",
    ] {
        let fixture = Fixture::new(matches!(
            kind,
            "forget" | "parent-proof" | "external-forget"
        ))
        .await;
        if kind != "wrong-call" {
            fixture.invalidate(kind).await;
        }
        let polls = AtomicUsize::new(0);
        let call = if kind == "wrong-call" {
            "wrong"
        } else {
            &fixture.call
        };
        let result = send_fenced(&fixture.hub, &fixture.run.token, call, async {
            polls.fetch_add(1, Ordering::SeqCst);
            Ok(reply())
        })
        .await;
        if matches!(kind, "missing" | "wrong-call") {
            assert!(result.is_err(), "{kind}");
        } else {
            assert!(matches!(result, Ok(None)), "{kind}");
        }
        if kind == "settled" {
            assert_eq!(fixture.budget().await, ("settled".into(), 7));
        }
        assert_eq!(polls.load(Ordering::SeqCst), 0, "{kind}");
        assert_eq!(
            fixture
                .hub
                .core
                .task(&fixture.run.token.task_id)
                .await
                .unwrap()
                .model_calls,
            1
        );
        if !matches!(kind, "settled" | "charge" | "missing") {
            assert_eq!(fixture.budget().await.1, 20_000, "{kind}");
        }
    }
}

#[tokio::test]
async fn exact_round_and_deadline_are_checked_without_new_reservations() {
    let fixture = Fixture::new(true).await;
    let until = fixture
        .hub
        .core
        .validate_model_call(&fixture.run.token, &fixture.call, now())
        .await
        .unwrap();
    let task = fixture.hub.core.task("child").await.unwrap();
    assert_eq!(
        until,
        task.deadline
            .min(fixture.source.as_ref().unwrap().expires_at)
    );
    assert!(
        fixture
            .hub
            .core
            .validate_model_call(&fixture.run.token, &fixture.call, until)
            .await
            .is_err()
    );
    // A reservation from a different round is not an authority to send this round.
    let wrong_round = format!(
        "model:{}:{}:1",
        fixture.run.token.run_id, fixture.run.token.revision
    );
    fixture
        .hub
        .core
        .reserve(&fixture.run.token, &wrong_round, 20_000, "fixture", now())
        .await
        .unwrap();
    assert!(
        fixture
            .hub
            .core
            .validate_model_call(&fixture.run.token, &wrong_round, now())
            .await
            .is_err()
    );
    assert_eq!(fixture.budget().await, ("reserved".into(), 20_000));
}

struct DropMark(Arc<AtomicUsize>);
impl Drop for DropMark {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn in_flight_http_is_dropped_after_cancel_revision_recovery_or_parent_withdrawal() {
    for kind in [
        "cancel",
        "revise",
        "recover",
        "forget",
        "parent-proof",
        "external-forget",
    ] {
        let fixture = Arc::new(
            Fixture::new(matches!(
                kind,
                "forget" | "parent-proof" | "external-forget"
            ))
            .await,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (started, received) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = requests.clone();
        // Only the test future uses this address. Production endpoints stay fixed.
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            counted.fetch_add(1, Ordering::SeqCst);
            started.send(()).unwrap();
            let _ = wait.await;
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
                .await;
        });
        let dropped = Arc::new(AtomicUsize::new(0));
        let mark = DropMark(dropped.clone());
        let target = fixture.clone();
        let send = tokio::spawn(async move {
            send_fenced(&target.hub, &target.run.token, &target.call, async move {
                let _mark = mark;
                reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .unwrap()
                    .get(format!("http://{address}/fixture"))
                    .send()
                    .await
                    .map_err(|_| crate::Error::Rejected("fixture transport".into()))?
                    .bytes()
                    .await
                    .map_err(|_| crate::Error::Rejected("fixture body".into()))?;
                Ok(reply())
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), received)
            .await
            .unwrap()
            .unwrap();
        // No source restriction lock may be retained by the HTTP wait.
        tokio::time::timeout(Duration::from_secs(5), fixture.invalidate(kind))
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), send)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Ok(None)), "{kind}");
        assert_eq!(dropped.load(Ordering::SeqCst), 1, "{kind}");
        assert_eq!(requests.load(Ordering::SeqCst), 1, "{kind}");
        // The production helper records uncertainty, without a refund or retry.
        assert_eq!(fixture.budget().await, ("unknown".into(), 20_000));
        assert!(
            fixture
                .hub
                .model_reply(&fixture.run.token, &reply(), "fixture", now())
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM link_chat")
            .fetch_one(&fixture.hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
        let _ = release.send(());
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn valid_reply_keeps_known_usage_and_unrelated_changes_do_not_cancel_it() {
    let fixture = Fixture::new(true).await;
    let (response, wait) = tokio::sync::oneshot::channel();
    let (start, started) = tokio::sync::oneshot::channel();
    let work = send_fenced(&fixture.hub, &fixture.run.token, &fixture.call, async {
        start.send(()).unwrap();
        Ok(wait.await.unwrap())
    });
    let control = async {
        started.await.unwrap();
        let e = fixture.hub.core.ingest(Origin::Human, "fixture", "other", json!({"commands":[{"kind":"create","id":"unrelated","goal":"other","scope":"pulse:read","priority":0,"requires_device":false}]}), now()).await.unwrap();
        fixture.hub.core.route(&e.event_id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        response
            .send(reply())
            .unwrap_or_else(|_| panic!("reply receiver dropped"));
    };
    let (result, ()) = tokio::join!(work, control);
    let result = result.unwrap().unwrap();
    fixture
        .hub
        .core
        .settle(
            &fixture.call,
            result.cost,
            &serde_json::to_string(&result.usage).unwrap(),
        )
        .await
        .unwrap();
    fixture
        .hub
        .model_reply(&fixture.run.token, &result, "fixture", now())
        .await
        .unwrap();
    assert_eq!(fixture.budget().await, ("settled".into(), 7));
    assert_eq!(
        fixture
            .hub
            .core
            .task("child")
            .await
            .unwrap()
            .reason
            .as_deref(),
        Some("model_reply_ready")
    );
    assert_eq!(
        fixture
            .hub
            .core
            .task("unrelated")
            .await
            .unwrap()
            .model_calls,
        0
    );
}

#[tokio::test]
async fn deadline_stops_waiting_and_expiry_reconciles_sources() {
    let fixture = Fixture::new(false).await;
    let mut task = fixture.hub.core.task("parent").await.unwrap();
    task.deadline = now() + 250;
    sqlx::query("UPDATE tasks SET data=? WHERE id=?")
        .bind(serde_json::to_string(&task).unwrap())
        .bind(&task.id)
        .execute(&fixture.hub.core.application_pool())
        .await
        .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mark = DropMark(dropped.clone());
    let polls = AtomicUsize::new(0);
    let polled = &polls;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        send_fenced(
            &fixture.hub,
            &fixture.run.token,
            &fixture.call,
            async move {
                let _mark = mark;
                polled.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<Reply>>().await
            },
        ),
    )
    .await
    .unwrap();
    assert!(result.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.budget().await, ("unknown".into(), 20_000));

    // Public memory creation can adopt an older source. Its earlier expiry must
    // bound the wake-up hint even when the task has much more time remaining.
    let fixture = Fixture::new_at(true, now() - pulse_core::MEMORY_RETENTION_MS + 60_000).await;
    let source = fixture.source.as_ref().unwrap();
    let until = fixture
        .hub
        .core
        .validate_model_call(&fixture.run.token, &fixture.call, now())
        .await
        .unwrap();
    assert_eq!(until, source.expires_at);
    assert!(until < fixture.hub.core.task("child").await.unwrap().deadline);
    assert!(
        fixture
            .hub
            .core
            .validate_model_call(&fixture.run.token, &fixture.call, until)
            .await
            .is_err()
    );
    assert!(
        fixture
            .hub
            .core
            .task_memory_sources("child", 1)
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
    assert_eq!(fixture.budget().await, ("reserved".into(), 20_000));
}

#[tokio::test]
async fn transport_failure_is_not_retried_and_late_known_usage_is_preserved() {
    let fixture = Fixture::new(false).await;
    let polls = AtomicUsize::new(0);
    assert!(
        send_fenced(&fixture.hub, &fixture.run.token, &fixture.call, async {
            polls.fetch_add(1, Ordering::SeqCst);
            Err(crate::Error::Rejected(
                "controlled transport failure".into(),
            ))
        })
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(fixture.budget().await, ("unknown".into(), 20_000));
    assert!(
        send_fenced(&fixture.hub, &fixture.run.token, &fixture.call, async {
            polls.fetch_add(1, Ordering::SeqCst);
            Ok(reply())
        })
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    fixture
        .hub
        .core
        .settle(&fixture.call, 7, "late known usage")
        .await
        .unwrap();
    fixture.hub.core.cost_unknown(&fixture.call).await.unwrap();
    assert_eq!(fixture.budget().await, ("settled".into(), 7));
    assert!(fixture.hub.core.cost_unknown("missing").await.is_err());
}

#[tokio::test]
async fn response_racing_with_withdrawal_settles_known_usage_but_cannot_apply_content() {
    let fixture = Fixture::new(true).await;
    let result = send_fenced(&fixture.hub, &fixture.run.token, &fixture.call, async {
        fixture.invalidate("forget").await;
        Ok(reply())
    })
    .await
    .unwrap()
    .unwrap();
    fixture
        .hub
        .core
        .settle(
            &fixture.call,
            result.cost,
            &serde_json::to_string(&result.usage).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        fixture
            .hub
            .model_reply(&fixture.run.token, &result, "fixture", now())
            .await
            .is_err()
    );
    assert_eq!(fixture.budget().await, ("settled".into(), 7));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM link_chat")
        .fetch_one(&fixture.hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn failed_uncertainty_write_reaches_controller_and_keeps_reservation() {
    let fixture = Fixture::new(false).await;
    sqlx::query("CREATE TRIGGER fixture_deny_uncertainty BEFORE UPDATE ON budget BEGIN SELECT RAISE(FAIL,'controlled ledger failure'); END")
        .execute(&fixture.hub.core.application_pool()).await.unwrap();
    let polls = AtomicUsize::new(0);
    assert!(
        send_fenced(&fixture.hub, &fixture.run.token, &fixture.call, async {
            polls.fetch_add(1, Ordering::SeqCst);
            Err(crate::Error::Rejected(
                "controlled transport failure".into(),
            ))
        })
        .await
        .is_err()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.budget().await, ("reserved".into(), 20_000));
}
