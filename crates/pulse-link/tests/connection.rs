use pulse_core::{State, Store};
use pulse_link::{
    client::{self, Config, Device, Vault},
    executor::Executor,
    journal::{Journal, private_write},
    protocol::*,
    server::Hub,
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, watch};

async fn core(dir: &Path) -> Store {
    Store::open(dir.join("cloud.db"), "owner").await.unwrap()
}
async fn paired(hub: &Hub, time: i64) -> Credential {
    let code = hub.pairing(time).await.unwrap();
    hub.redeem(
        PairRequest {
            code,
            device: uuid::Uuid::new_v4().to_string(),
        },
        time,
    )
    .await
    .unwrap()
}
fn submit(device: &str, sequence: u64, id: &str) -> Envelope<DevicePayload> {
    Envelope::new(
        device,
        sequence,
        0,
        DevicePayload::Human {
            source_id: format!("source-{id}"),
            action: HumanAction::Submit {
                id: id.into(),
                goal: "查看 Pulse 文件".into(),
            },
        },
    )
    .unwrap()
}
fn read(device: &str, sequence: u64, epoch: u64) -> Envelope<ServerPayload> {
    Envelope::new(
        device,
        sequence,
        epoch,
        ServerPayload::Read {
            operation_id: "op-1".into(),
            task_id: "task-1".into(),
            revision: "1".into(),
            operation: ReadOp::Text {
                path: "README.md".into(),
            },
        },
    )
    .unwrap()
}

#[test]
fn strict_wire_and_origin_policy() {
    assert!(parse::<ClientWire>(br#"{"kind":"heartbeat","epoch":"1","epoch":"2"}"#).is_err());
    assert!(parse::<ClientWire>(br#"{"kind":"heartbeat","epoch":"1","extra":true}"#).is_err());
    assert!(parse::<ClientWire>(br#"{"kind":"shell","command":"rm"}"#).is_err());
    assert!(parse::<ClientWire>(&vec![b' '; MAX_FRAME + 1]).is_err());
    for n in [
        "",
        "01",
        "-1",
        "18446744073709551615",
        "9223372036854775808",
    ] {
        assert!(number(n).is_err());
    }
    let mut e = read("device", 1, 1);
    assert!(e.validate().is_ok());
    e.epoch = "2".into();
    assert!(e.validate().is_err());
    for value in [
        "http://example.com",
        "http://localhost:1234",
        "https://user:pass@example.com",
        "https://example.com/path",
        "https://example.com/?token=x",
        "https://example.com/#x",
    ] {
        assert!(client::validate_server(value, true).is_err());
    }
    assert!(client::validate_server("http://127.0.0.1:8721", false).is_err());
    assert!(client::validate_server("http://127.0.0.1:8721", true).is_ok());
    assert!(client::validate_server("https://pulse.example", false).is_ok());
}

#[tokio::test]
async fn pairing_is_one_use_and_revocation_fences_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(core(dir.path()).await);
    let time = pulse_link::now();
    let code = hub.pairing(time).await.unwrap();
    let request = PairRequest {
        code: code.clone(),
        device: uuid::Uuid::new_v4().to_string(),
    };
    assert!(hub.redeem(request.clone(), time + 600_000).await.is_err());
    let cred = hub.redeem(request.clone(), time).await.unwrap();
    assert_eq!(hub.authenticate(&cred.token).await.unwrap(), cred.device);
    assert!(hub.redeem(request.clone(), time).await.is_err());
    let first = hub.begin(&cred.device, 0, time).await.unwrap();
    let second = hub.begin(&cred.device, 0, time).await.unwrap();
    assert!(!hub.live(&cred.device, first, time).await.unwrap());
    assert!(hub.live(&cred.device, second, time).await.unwrap());
    assert!(
        !hub.live(&cred.device, second, time + LEASE_MS)
            .await
            .unwrap()
    );
    assert!(
        hub.renew(&cred.device, second, time + LEASE_MS)
            .await
            .is_err()
    );
    hub.revoke(&cred.device).await.unwrap();
    assert!(hub.authenticate(&cred.token).await.is_err());
    assert!(hub.begin(&cred.device, 0, time).await.is_err());
    assert!(hub.redeem(request, time).await.is_err());
    let token_digest: String = sqlx::query_scalar("SELECT token_digest FROM link_device")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_ne!(token_digest, cred.token);
}

#[tokio::test]
async fn journal_commits_before_ack_and_deduplicates_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let (journal, lock) = Journal::open(
        &dir.path().join("private"),
        "https://pulse.example/",
        repo.path(),
    )
    .await
    .unwrap();
    assert!(
        Journal::open(
            &dir.path().join("private"),
            "https://pulse.example/",
            repo.path()
        )
        .await
        .is_err()
    );
    journal
        .action(
            "human-1",
            HumanAction::Submit {
                id: "t".into(),
                goal: "離線輸入".into(),
            },
        )
        .await
        .unwrap();
    let outgoing = journal.outgoing().await.unwrap();
    assert_eq!(outgoing.len(), 1);
    assert_eq!(journal.view().await.unwrap().messages.len(), 1);
    assert!(journal.ack(1, "wrong").await.is_err());
    assert_eq!(journal.pending().await.unwrap(), 1);
    journal.ack(1, &outgoing[0].id).await.unwrap();
    assert_eq!(journal.pending().await.unwrap(), 0);
    let snapshot = Envelope::new(
        &journal.device,
        1,
        2,
        ServerPayload::Snapshot {
            tasks: vec![],
            messages: vec![ChatMessage {
                id: "human-1".into(),
                body: "雲端已保存".into(),
            }],
            synced_at: 123,
        },
    )
    .unwrap();
    assert!(journal.accept(&snapshot, true, 2, true).await.unwrap());
    assert!(!journal.accept(&snapshot, true, 2, true).await.unwrap());
    let conflict = Envelope::new(&journal.device, 1, 2, snapshot.payload.clone()).unwrap();
    assert!(journal.accept(&conflict, true, 2, true).await.is_err());
    let gap = read(&journal.device, 3, 2);
    assert!(journal.accept(&gap, true, 2, true).await.is_err());
    assert_eq!(journal.cursor().await.unwrap(), 1);
    drop(lock);
    drop(journal);
    let (journal, lock) = Journal::open(
        &dir.path().join("private"),
        "https://pulse.example/",
        repo.path(),
    )
    .await
    .unwrap();
    assert_eq!(journal.cursor().await.unwrap(), 1);
    assert_eq!(journal.view().await.unwrap().synced_at, Some(123));
    drop(lock);
    drop(journal);
    assert!(
        Journal::open(
            &dir.path().join("private"),
            "https://other.example/",
            repo.path()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn crash_after_accept_or_start_never_reexecutes() {
    // Twenty deterministic samples at each durable acceptance boundary.
    for started in [false, true] {
        for _sample in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let repo = tempfile::tempdir().unwrap();
            let (journal, lock) = Journal::open(
                &dir.path().join("private"),
                "https://pulse.example/",
                repo.path(),
            )
            .await
            .unwrap();
            let envelope = read(&journal.device, 1, 7);
            journal.accept(&envelope, true, 7, true).await.unwrap();
            if started {
                assert!(journal.take_work().await.unwrap().is_some());
            }
            drop(lock);
            drop(journal);
            let (journal, _lock) = Journal::open(
                &dir.path().join("private"),
                "https://pulse.example/",
                repo.path(),
            )
            .await
            .unwrap();
            journal.recover().await.unwrap();
            journal.recover().await.unwrap();
            assert!(journal.take_work().await.unwrap().is_none());
            assert!(!journal.accept(&envelope, true, 8, true).await.unwrap());
            let out = journal.outgoing().await.unwrap();
            assert_eq!(out.len(), 1);
            assert!(matches!(
                &out[0].payload,
                DevicePayload::Result {
                    outcome: ReadOutcome::Unknown { .. },
                    ..
                }
            ));
            assert_eq!(out[0].epoch, "7");
        }
    }
}

#[tokio::test]
async fn result_survives_lost_ack_and_stale_dispatch_is_not_run() {
    let dir = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let (journal, lock) = Journal::open(
        &dir.path().join("private"),
        "https://pulse.example/",
        repo.path(),
    )
    .await
    .unwrap();
    let e = read(&journal.device, 1, 7);
    journal.accept(&e, true, 7, true).await.unwrap();
    let work = journal.take_work().await.unwrap().unwrap();
    journal
        .finish_work(&work, ReadOutcome::text("proof".into(), false))
        .await
        .unwrap();
    let out = journal.outgoing().await.unwrap();
    let serialized = serde_json::to_string(&out[0]).unwrap();
    drop(lock);
    drop(journal);
    let (journal, _lock) = Journal::open(
        &dir.path().join("private"),
        "https://pulse.example/",
        repo.path(),
    )
    .await
    .unwrap();
    journal.recover().await.unwrap();
    assert_eq!(
        serde_json::to_string(&journal.outgoing().await.unwrap()[0]).unwrap(),
        serialized
    );
    assert!(!journal.accept(&e, true, 8, true).await.unwrap());
    journal.ack(1, &out[0].id).await.unwrap();
    assert_eq!(journal.pending().await.unwrap(), 0);
    let mut stale = read(&journal.device, 2, 7);
    stale.payload = ServerPayload::Read {
        operation_id: "op-2".into(),
        task_id: "task-1".into(),
        revision: "1".into(),
        operation: ReadOp::GitStatus,
    };
    let stale = Envelope::new(&journal.device, 2, 7, stale.payload).unwrap();
    journal.accept(&stale, false, 8, true).await.unwrap();
    assert!(journal.take_work().await.unwrap().is_none());
    assert!(matches!(
        &journal.outgoing().await.unwrap()[0].payload,
        DevicePayload::Result {
            outcome: ReadOutcome::Unknown { .. },
            ..
        }
    ));
}

#[tokio::test]
async fn capacity_refuses_work_before_advancing_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let (journal, _lock) = Journal::open(
        &dir.path().join("private"),
        "https://pulse.example/",
        repo.path(),
    )
    .await
    .unwrap();
    // Use the public SQLite extension only in the fixture to hit the exact bound.
    let options =
        sqlx::sqlite::SqliteConnectOptions::new().filename(dir.path().join("private/device.db"));
    let db = sqlx::SqlitePool::connect_with(options).await.unwrap();
    let mut tx = db.begin().await.unwrap();
    for i in 1..=998 {
        sqlx::query("INSERT INTO local_outbox VALUES(?,?,?)")
            .bind(i)
            .bind(format!("fixture-{i}"))
            .bind("{}")
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    assert!(
        journal
            .accept(&read(&journal.device, 1, 7), true, 7, true)
            .await
            .is_err()
    );
    assert_eq!(journal.cursor().await.unwrap(), 0);
    assert!(journal.take_work().await.unwrap().is_none());
}

#[tokio::test]
async fn cloud_ingest_replay_cancel_and_late_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(core(dir.path()).await);
    let time = pulse_link::now();
    let credential = paired(&hub, time).await;
    let device = &credential.device;
    let epoch = hub.begin(device, 0, time).await.unwrap();
    let event = submit(device, 1, "task-1");
    hub.receive(device, &event, time).await.unwrap();
    hub.receive(device, &event, time).await.unwrap();
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
    assert!(
        hub.receive(device, &submit(device, 1, "task-conflict"), time)
            .await
            .is_err()
    );
    let task = hub.core.task("task-1").await.unwrap();
    let request = Envelope::new(
        device,
        2,
        0,
        DevicePayload::Human {
            source_id: "read-1".into(),
            action: HumanAction::Read {
                task_id: task.id.clone(),
                revision: task.revision.to_string(),
                operation: ReadOp::GitStatus,
            },
        },
    )
    .unwrap();
    hub.receive(device, &request, time).await.unwrap();
    let dispatch = hub
        .outgoing(device)
        .await
        .unwrap()
        .into_iter()
        .find(|e| matches!(e.payload, ServerPayload::Read { .. }))
        .unwrap();
    assert!(hub.permitted(&dispatch, epoch, time).await.unwrap());
    let cancel = Envelope::new(
        device,
        3,
        0,
        DevicePayload::Human {
            source_id: "cancel-1".into(),
            action: HumanAction::Cancel {
                task_id: task.id.clone(),
                revision: task.revision.to_string(),
            },
        },
    )
    .unwrap();
    hub.receive(device, &cancel, time).await.unwrap();
    assert!(!hub.permitted(&dispatch, epoch, time).await.unwrap());
    assert_eq!(
        hub.core.task(&task.id).await.unwrap().state,
        State::OutcomeUnknown
    );
    let ServerPayload::Read {
        operation_id,
        task_id,
        revision,
        ..
    } = dispatch.payload
    else {
        panic!()
    };
    let result = Envelope::new(
        device,
        4,
        epoch,
        DevicePayload::Result {
            operation_id,
            task_id,
            revision,
            outcome: ReadOutcome::text("late evidence".into(), false),
        },
    )
    .unwrap();
    hub.receive(device, &result, time).await.unwrap();
    hub.receive(device, &result, time).await.unwrap();
    let final_task = hub.core.task(&task.id).await.unwrap();
    assert_eq!(final_task.state, State::Cancelled);
    assert!(!final_task.evidence.is_empty());
}

#[tokio::test]
async fn read_policy_rejects_escape_and_executes_no_shell() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("README.md"), "hello 中文").unwrap();
    std::fs::write(outside.path().join("outside.md"), "private").unwrap();
    symlink(outside.path(), dir.path().join("escape")).unwrap();
    symlink(
        outside.path().join("outside.md"),
        dir.path().join("link.md"),
    )
    .unwrap();
    std::fs::create_dir(dir.path().join(".private")).unwrap();
    std::fs::write(dir.path().join(".private/x.md"), "private").unwrap();
    let executor = Executor::new(dir.path()).unwrap();
    for path in [
        "../outside.md",
        "/etc/passwd",
        "link.md",
        "escape/outside.md",
        ".private/x.md",
        "credentials.json",
        "foo.pem",
        "target/log.txt",
    ] {
        assert!(executor.read_text(path).is_err(), "{path}");
    }
    assert!(matches!(
        executor.read_text("README.md").unwrap(),
        ReadOutcome::Completed { .. }
    ));
    let listing = executor.list("").unwrap();
    let ReadOutcome::Completed { text, .. } = listing else {
        panic!()
    };
    assert!(!text.contains("escape") && !text.contains(".private"));
    assert!(parse::<ReadOp>(br#"{"kind":"shell","command":"touch /tmp/unsafe"}"#).is_err());
    private_write(&dir.path().join("private-code"), b"x").unwrap();
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(dir.path().join("private-code"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[derive(Default)]
struct MemoryVault(Mutex<Option<Credential>>);
impl Vault for MemoryVault {
    fn load(&self, _: &str) -> pulse_link::Result<Option<Credential>> {
        Ok(self.0.lock().unwrap().clone())
    }
    fn save(&self, _: &str, credential: &Credential) -> pulse_link::Result<()> {
        *self.0.lock().unwrap() = Some(credential.clone());
        Ok(())
    }
}
async fn until(
    states: &mut watch::Receiver<NativeState>,
    predicate: impl Fn(&NativeState) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if predicate(&states.borrow()) {
                break;
            }
            states.changed().await.unwrap();
        }
    })
    .await
    .expect("state transition timeout");
}
#[tokio::test]
async fn real_websocket_pair_offline_submit_read_stop_and_revoke() {
    let cloud = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(
        repo.path().join("README.md"),
        "# Real local evidence\n\nTool output only.",
    )
    .unwrap();
    let hub = Hub::new(core(cloud.path()).await);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server =
        tokio::spawn(axum::serve(listener, pulse_link::server::router(hub.clone())).into_future());
    let config = Config::parse([
        "--server".into(),
        origin,
        "--repo".into(),
        repo.path().to_str().unwrap().into(),
        "--data-dir".into(),
        local.path().join("private").to_str().unwrap().into(),
        "--dev-loopback".into(),
    ])
    .unwrap();
    let device = Device::open(config, Arc::new(MemoryVault::default()))
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(32);
    let (state, mut states) = watch::channel(NativeState::default());
    let helper = tokio::spawn(device.run(rx, state));
    tx.send(NativeCommand::Action {
        source_id: "offline-input".into(),
        action: HumanAction::Submit {
            id: "native-task".into(),
            goal: "Read the local README".into(),
        },
    })
    .await
    .unwrap();
    until(&mut states, |s| s.pending == 1 && !s.connected).await;
    tx.send(NativeCommand::Pair {
        code: hub.pairing(pulse_link::now()).await.unwrap(),
    })
    .await
    .unwrap();
    until(&mut states, |s| {
        s.connected && s.tasks.iter().any(|t| t.id == "native-task") && s.pending == 0
    })
    .await;
    let task = hub.core.task("native-task").await.unwrap();
    tx.send(NativeCommand::Action {
        source_id: "native-read".into(),
        action: HumanAction::Read {
            task_id: task.id.clone(),
            revision: task.revision.to_string(),
            operation: ReadOp::Text {
                path: "README.md".into(),
            },
        },
    })
    .await
    .unwrap();
    until(&mut states, |s| {
        s.messages
            .iter()
            .any(|m| m.body.contains("Real local evidence"))
    })
    .await;
    assert_eq!(hub.core.task(&task.id).await.unwrap().state, State::Blocked);
    tx.send(NativeCommand::KillSwitch { stopped: true })
        .await
        .unwrap();
    until(&mut states, |s| s.stopped).await;
    let device_id: String = sqlx::query_scalar("SELECT id FROM link_device")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    hub.revoke(&device_id).await.unwrap();
    until(&mut states, |s| !s.connected && s.stopped).await;
    tx.send(NativeCommand::Pair {
        code: hub.pairing(pulse_link::now()).await.unwrap(),
    })
    .await
    .unwrap();
    until(&mut states, |s| s.connected && s.stopped).await;
    tx.send(NativeCommand::Shutdown).await.unwrap();
    helper.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn device_limits_and_old_receipts_cannot_clobber_new_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(core(dir.path()).await);
    let time = pulse_link::now();
    let credential = paired(&hub, time).await;
    let device = &credential.device;
    let epoch = hub.begin(device, 0, time).await.unwrap();
    hub.receive(device, &submit(device, 1, "bounded-task"), time)
        .await
        .unwrap();
    let task = hub.core.task("bounded-task").await.unwrap();
    let mut sequence = 2;
    let mut first_result = None;
    for i in 0..16 {
        let source_id = format!("bounded-{i}");
        let request = Envelope::new(
            device,
            sequence,
            0,
            DevicePayload::Human {
                source_id: source_id.clone(),
                action: HumanAction::Read {
                    task_id: task.id.clone(),
                    revision: task.revision.to_string(),
                    operation: ReadOp::GitStatus,
                },
            },
        )
        .unwrap();
        hub.receive(device, &request, time).await.unwrap();
        sequence += 1;
        assert_eq!(
            hub.core.task(&task.id).await.unwrap().state,
            State::Dispatched
        );
        if i == 1 {
            let payload = first_result.clone().unwrap();
            let replay = Envelope::new(device, sequence, epoch, payload).unwrap();
            hub.receive(device, &replay, time).await.unwrap();
            sequence += 1;
            assert_eq!(
                hub.core.task(&task.id).await.unwrap().state,
                State::Dispatched
            );
        }
        let payload = DevicePayload::Result {
            operation_id: format!("read:{source_id}"),
            task_id: task.id.clone(),
            revision: task.revision.to_string(),
            outcome: ReadOutcome::text("proof".into(), false),
        };
        if i == 0 {
            first_result = Some(payload.clone());
        }
        let result = Envelope::new(device, sequence, epoch, payload).unwrap();
        hub.receive(device, &result, time).await.unwrap();
        sequence += 1;
    }
    let request = Envelope::new(
        device,
        sequence,
        0,
        DevicePayload::Human {
            source_id: "over-limit".into(),
            action: HumanAction::Read {
                task_id: task.id.clone(),
                revision: task.revision.to_string(),
                operation: ReadOp::GitStatus,
            },
        },
    )
    .unwrap();
    hub.receive(device, &request, time).await.unwrap();
    assert_eq!(hub.core.task(&task.id).await.unwrap().tool_calls, 16);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_reads")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 16);
}

#[tokio::test]
async fn server_restart_fences_pending_work_and_late_result_is_only_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(core(dir.path()).await);
    let time = pulse_link::now();
    let cred = paired(&hub, time).await;
    let epoch = hub.begin(&cred.device, 0, time).await.unwrap();
    hub.receive(&cred.device, &submit(&cred.device, 1, "restart-task"), time)
        .await
        .unwrap();
    let task = hub.core.task("restart-task").await.unwrap();
    let read = Envelope::new(
        &cred.device,
        2,
        0,
        DevicePayload::Human {
            source_id: "restart-read".into(),
            action: HumanAction::Read {
                task_id: task.id.clone(),
                revision: task.revision.to_string(),
                operation: ReadOp::GitStatus,
            },
        },
    )
    .unwrap();
    hub.receive(&cred.device, &read, time).await.unwrap();
    hub.recover().await.unwrap();
    assert!(!hub.live(&cred.device, epoch, time).await.unwrap());
    assert_eq!(
        hub.core.task(&task.id).await.unwrap().state,
        State::OutcomeUnknown
    );
    let new_epoch = hub.begin(&cred.device, 0, time).await.unwrap();
    let dispatch = hub
        .outgoing(&cred.device)
        .await
        .unwrap()
        .into_iter()
        .find(|e| matches!(e.payload, ServerPayload::Read { .. }))
        .unwrap();
    assert!(!hub.permitted(&dispatch, new_epoch, time).await.unwrap());
    hub.end(&cred.device, epoch).await.unwrap();
    assert!(hub.live(&cred.device, new_epoch, time).await.unwrap());
    let result = Envelope::new(
        &cred.device,
        3,
        epoch,
        DevicePayload::Result {
            operation_id: "read:restart-read".into(),
            task_id: task.id.clone(),
            revision: task.revision.to_string(),
            outcome: ReadOutcome::text("saved before restart".into(), false),
        },
    )
    .unwrap();
    hub.receive(&cred.device, &result, time).await.unwrap();
    assert_eq!(hub.core.task(&task.id).await.unwrap().state, State::Blocked);
}

#[tokio::test]
async fn pairing_timeout_does_not_block_native_stop() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().route(
        "/v1/device-pairings/redeem",
        axum::routing::post(|| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            axum::http::StatusCode::FORBIDDEN
        }),
    );
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let local = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let config = Config::parse([
        "--server".into(),
        origin,
        "--repo".into(),
        repo.path().to_str().unwrap().into(),
        "--data-dir".into(),
        local.path().join("private").to_str().unwrap().into(),
        "--dev-loopback".into(),
    ])
    .unwrap();
    let device = Device::open(config, Arc::new(MemoryVault::default()))
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(32);
    let (states, mut state) = watch::channel(NativeState::default());
    let helper = tokio::spawn(device.run(rx, states));
    tx.send(NativeCommand::Pair {
        code: "a".repeat(64),
    })
    .await
    .unwrap();
    tx.send(NativeCommand::KillSwitch { stopped: true })
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        until(&mut state, |s| s.stopped && !s.paired),
    )
    .await
    .unwrap();
    tx.send(NativeCommand::Shutdown).await.unwrap();
    helper.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn git_disables_external_helpers_and_unicode_truncation_stays_valid() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let git = |args: &[&str]| {
        assert!(
            std::process::Command::new("/usr/bin/git")
                .current_dir(root)
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .args(args)
                .output()
                .unwrap()
                .status
                .success()
        );
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    std::fs::write(root.join("README.md"), "before\n").unwrap();
    std::fs::write(root.join("credentials.json"), "private fixture\n").unwrap();
    git(&["add", "--", "README.md", "credentials.json"]);
    git(&["-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture"]);
    let helper = root.join("external-helper");
    let sentinel = root.join("unexpected-effect");
    std::fs::write(
        &helper,
        format!("#!/bin/sh\nprintf unsafe > '{}'\n", sentinel.display()),
    )
    .unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
    git(&["config", "diff.external", helper.to_str().unwrap()]);
    git(&["config", "core.fsmonitor", helper.to_str().unwrap()]);
    std::fs::write(root.join("README.md"), "after\n").unwrap();
    std::fs::write(root.join("credentials.json"), "changed private fixture\n").unwrap();
    let executor = Executor::new(root).unwrap();
    let ReadOutcome::Completed { text, .. } = executor.execute(ReadOp::GitStatus).await else {
        panic!("status failed")
    };
    assert!(text.contains("README.md") && !text.contains("credentials"));
    let ReadOutcome::Completed { text, .. } = executor
        .execute(ReadOp::GitDiff {
            path: "README.md".into(),
        })
        .await
    else {
        panic!("diff failed")
    };
    assert!(text.contains("+after") && !sentinel.exists());
    std::fs::write(root.join("large.md"), "中".repeat(MAX_RESULT)).unwrap();
    let ReadOutcome::Completed {
        text, truncated, ..
    } = executor.read_text("large.md").unwrap()
    else {
        panic!()
    };
    assert!(truncated && text.len() <= MAX_RESULT && text.ends_with('中'));
}

#[tokio::test]
async fn pending_preview_remains_bounded_without_losing_saved_input() {
    let dir = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let (journal, _lock) = Journal::open(
        &dir.path().join("private"),
        "https://pulse.example/",
        repo.path(),
    )
    .await
    .unwrap();
    let goal = format!("X{}", "\0".repeat(4095));
    for i in 0..100 {
        journal
            .action(
                &format!("pending-{i}"),
                HumanAction::Submit {
                    id: format!("task-{i}"),
                    goal: goal.clone(),
                },
            )
            .await
            .unwrap();
    }
    let view = journal.view().await.unwrap();
    assert_eq!(view.messages.len(), 100);
    assert!(view.messages[0].body.contains("預覽已截斷"));
    assert!(serde_json::to_vec(&view).unwrap().len() < MAX_IPC);
    let outgoing = journal.outgoing().await.unwrap();
    assert_eq!(outgoing.len(), 100);
    let DevicePayload::Human {
        action: HumanAction::Submit { goal: saved, .. },
        ..
    } = &outgoing[0].payload
    else {
        panic!()
    };
    assert_eq!(saved, &goal);
}
