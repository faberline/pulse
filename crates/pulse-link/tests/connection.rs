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
    paired_role(hub, ClientRole::Chat, time).await
}
async fn paired_role(hub: &Hub, role: ClientRole, time: i64) -> Credential {
    let code = hub.pairing(role, time).await.unwrap();
    hub.redeem(
        PairRequest {
            code,
            role,
            device: uuid::Uuid::new_v4().to_string(),
        },
        time,
    )
    .await
    .unwrap()
}
async fn result(
    hub: &Hub,
    worker: &str,
    epoch: u64,
    payload: DevicePayload,
    time: i64,
) -> Envelope<DevicePayload> {
    let cursor: i64 = sqlx::query_scalar("SELECT in_cursor FROM link_device WHERE id=?")
        .bind(worker)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    let envelope = Envelope::new(worker, cursor as u64 + 1, epoch, payload).unwrap();
    hub.receive(worker, &envelope, time).await.unwrap();
    envelope
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

#[test]
fn pairing_file_stays_private_and_preserves_client_role() {
    let dir = tempfile::tempdir().unwrap();
    let code = dir.path().join("code");
    private_write(
        &code,
        b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap();
    let args = vec![
        "--server".into(),
        "https://pulse.example".into(),
        "--pair-code-file".into(),
        code.to_str().unwrap().into(),
    ];
    let (config, pair) = Config::parse_with_pairing(args.clone(), ClientRole::Chat).unwrap();
    assert!(config.repo.as_os_str().is_empty());
    assert_eq!(pair.unwrap().len(), 64);
    assert!(Config::parse_with_pairing(args.clone(), ClientRole::LocalExecutor).is_err());
    let mut duplicate = args;
    duplicate.extend(["--pair-code-file".into(), code.to_str().unwrap().into()]);
    assert!(Config::parse_with_pairing(duplicate, ClientRole::Chat).is_err());
}

#[tokio::test]
async fn pairing_is_one_use_and_revocation_fences_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(core(dir.path()).await);
    let time = pulse_link::now();
    let code = hub.pairing(ClientRole::Chat, time).await.unwrap();
    let request = PairRequest {
        code: code.clone(),
        role: ClientRole::Chat,
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
async fn roles_are_bound_by_server_and_cannot_exchange_human_and_tool_authority() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(core(dir.path()).await);
    let time = pulse_link::now();
    let code = hub.pairing(ClientRole::LocalExecutor, time).await.unwrap();
    let mut request = PairRequest {
        code,
        device: uuid::Uuid::new_v4().to_string(),
        role: ClientRole::Chat,
    };
    assert!(hub.redeem(request.clone(), time).await.is_err());
    request.role = ClientRole::LocalExecutor;
    let local = hub.redeem(request, time).await.unwrap();
    let chat = paired(&hub, time).await;
    assert!(
        hub.receive(
            &local.device,
            &submit(&local.device, 1, "forged-human"),
            time
        )
        .await
        .is_err()
    );
    assert!(hub.core.task("forged-human").await.is_err());
    let forged = Envelope::new(
        &chat.device,
        1,
        0,
        DevicePayload::Result {
            operation_id: "fake".into(),
            task_id: "fake".into(),
            revision: "1".into(),
            outcome: ReadOutcome::text("untrusted".into(), false),
        },
    )
    .unwrap();
    assert!(hub.receive(&chat.device, &forged, time).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_inbox")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    // A read request without a live local executor cannot dispatch to chat.
    hub.receive(&chat.device, &submit(&chat.device, 1, "no-worker"), time)
        .await
        .unwrap();
    let task = hub.core.task("no-worker").await.unwrap();
    let read = Envelope::new(
        &chat.device,
        2,
        0,
        DevicePayload::Human {
            source_id: "no-worker-read".into(),
            action: HumanAction::Read {
                task_id: task.id.clone(),
                revision: task.revision.to_string(),
                operation: ReadOp::GitStatus,
            },
        },
    )
    .unwrap();
    hub.receive(&chat.device, &read, time).await.unwrap();
    assert_eq!(hub.core.task(&task.id).await.unwrap().tool_calls, 0);
    assert!(
        hub.outgoing(&chat.device)
            .await
            .unwrap()
            .iter()
            .all(|e| !matches!(e.payload, ServerPayload::Read { .. }))
    );
}

#[tokio::test]
async fn chat_journal_cannot_be_reused_as_executor_or_accept_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chat");
    let (journal, lock) = Journal::open_role(
        &path,
        "https://pulse.example/",
        Path::new(""),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    assert!(
        journal
            .accept(&read(&journal.device, 1, 1), true, 1, true)
            .await
            .is_err()
    );
    assert_eq!(journal.cursor().await.unwrap(), 0);
    drop(journal);
    drop(lock);
    assert!(
        Journal::open(&path, "https://pulse.example/", Path::new(""))
            .await
            .is_err()
    );
    assert!(Config::parse_chat(["--server".into(), "https://pulse.example".into()]).is_ok());
    assert!(
        Config::parse_chat([
            "--server".into(),
            "https://pulse.example".into(),
            "--repo".into(),
            "/tmp".into()
        ])
        .is_err()
    );
}

#[tokio::test]
async fn upgrade_preserves_history_and_revokes_combined_helper_credentials() {
    use sqlx::{Connection, migrate::Migrate};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cloud.db");
    private_write(&path, b"").unwrap();
    let mut db = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(&path),
    )
    .await
    .unwrap();
    db.ensure_migrations_table("_sqlx_migrations")
        .await
        .unwrap();
    for migration in sqlx::migrate!("../../migrations")
        .iter()
        .filter(|m| m.version <= 2)
    {
        db.apply("_sqlx_migrations", migration).await.unwrap();
    }
    sqlx::query("INSERT INTO link_device(id,token_digest,epoch,lease_until) VALUES('old-helper','old-digest',7,9223372036854775807)").execute(&mut db).await.unwrap();
    sqlx::query("INSERT INTO link_pairings(digest,expires) VALUES('old-code',9223372036854775807)")
        .execute(&mut db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO link_chat VALUES('history','keep this history',1)")
        .execute(&mut db)
        .await
        .unwrap();
    db.close().await.unwrap();
    let hub = Hub::new(core(dir.path()).await);
    let (revoked, epoch, lease): (i64, i64, i64) =
        sqlx::query_as("SELECT revoked,epoch,lease_until FROM link_device WHERE id='old-helper'")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!((revoked, epoch, lease), (1, 8, 0));
    let used: i64 = sqlx::query_scalar("SELECT used FROM link_pairings WHERE digest='old-code'")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(used, 1);
    let body: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id='history'")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(body, "keep this history");
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
            local: LocalStatus::default(),
            tasks: vec![],
            messages: vec![ChatMessage {
                artifact_operation_id: None,
                approval: None,
                artifact: None,
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
    let worker = paired_role(&hub, ClientRole::LocalExecutor, time).await;
    let epoch = hub.begin(&worker.device, 0, time).await.unwrap();
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
        .outgoing(&worker.device)
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
        &worker.device,
        1,
        epoch,
        DevicePayload::Result {
            operation_id,
            task_id,
            revision,
            outcome: ReadOutcome::text("late evidence".into(), false),
        },
    )
    .unwrap();
    hub.receive(&worker.device, &result, time).await.unwrap();
    hub.receive(&worker.device, &result, time).await.unwrap();
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
struct MemoryVault(Mutex<Option<Credential>>, Mutex<Option<PairClaim>>);
impl Vault for MemoryVault {
    fn load(&self, _: &str) -> pulse_link::Result<Option<Credential>> {
        Ok(self.0.lock().unwrap().clone())
    }
    fn save(&self, _: &str, credential: &Credential) -> pulse_link::Result<()> {
        *self.0.lock().unwrap() = Some(credential.clone());
        Ok(())
    }
    fn pending(&self, _: &str) -> pulse_link::Result<Option<PairClaim>> {
        Ok(self.1.lock().unwrap().clone())
    }
    fn save_pending(&self, _: &str, claim: &PairClaim) -> pulse_link::Result<()> {
        *self.1.lock().unwrap() = Some(claim.clone());
        Ok(())
    }
    fn clear_pending(&self, _: &str) -> pulse_link::Result<()> {
        *self.1.lock().unwrap() = None;
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
    let chat = tempfile::tempdir().unwrap();
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
    let executor_config = Config::parse([
        "--server".into(),
        origin.clone(),
        "--repo".into(),
        repo.path().to_str().unwrap().into(),
        "--data-dir".into(),
        local.path().join("private").to_str().unwrap().into(),
        "--dev-loopback".into(),
    ])
    .unwrap();
    let executor = Device::open(executor_config, Arc::new(MemoryVault::default()))
        .await
        .unwrap();
    let (local_tx, local_rx) = mpsc::channel(32);
    let (local_state, mut local_states) = watch::channel(NativeState::default());
    let worker = tokio::spawn(executor.run(local_rx, local_state));
    local_tx
        .send(NativeCommand::Pair {
            code: hub
                .pairing(ClientRole::LocalExecutor, pulse_link::now())
                .await
                .unwrap(),
        })
        .await
        .unwrap();
    until(&mut local_states, |s| s.connected).await;
    let config = Config::parse_chat([
        "--server".into(),
        origin,
        "--data-dir".into(),
        chat.path().join("private").to_str().unwrap().into(),
        "--dev-loopback".into(),
    ])
    .unwrap();
    let vault = Arc::new(MemoryVault::default());
    let conversation = Device::open_chat(config.clone(), vault.clone())
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(32);
    let (state, mut states) = watch::channel(NativeState::default());
    let conversation = tokio::spawn(conversation.run(rx, state));
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
        code: hub
            .pairing(ClientRole::Chat, pulse_link::now())
            .await
            .unwrap(),
    })
    .await
    .unwrap();
    until(&mut states, |s| {
        s.connected
            && s.local_connected
            && s.tasks.iter().any(|t| t.id == "native-task")
            && s.pending == 0
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
    // Closing chat has no effect on the independent executor's lease or process.
    tx.send(NativeCommand::Shutdown).await.unwrap();
    conversation.await.unwrap().unwrap();
    assert!(!worker.is_finished());
    let local_id: String =
        sqlx::query_scalar("SELECT id FROM link_device WHERE role='local_executor'")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    let epoch: i64 = sqlx::query_scalar("SELECT epoch FROM link_device WHERE id=?")
        .bind(&local_id)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        hub.live(&local_id, epoch as u64, pulse_link::now())
            .await
            .unwrap()
    );
    let conversation = Device::open_chat(config, vault).await.unwrap();
    let (tx, rx) = mpsc::channel(32);
    let (state, mut states) = watch::channel(NativeState::default());
    let conversation = tokio::spawn(conversation.run(rx, state));
    until(&mut states, |s| s.connected).await;
    tx.send(NativeCommand::KillSwitch { stopped: true })
        .await
        .unwrap();
    until(&mut states, |s| s.stopped).await;
    until(&mut local_states, |s| s.stopped).await;
    hub.revoke(&local_id).await.unwrap();
    until(&mut local_states, |s| !s.connected && s.stopped).await;
    until(&mut states, |s| s.connected && !s.local_connected).await;
    local_tx
        .send(NativeCommand::Pair {
            code: hub
                .pairing(ClientRole::LocalExecutor, pulse_link::now())
                .await
                .unwrap(),
        })
        .await
        .unwrap();
    until(&mut local_states, |s| s.connected && s.stopped).await;
    tx.send(NativeCommand::Shutdown).await.unwrap();
    conversation.await.unwrap().unwrap();
    local_tx.send(NativeCommand::Shutdown).await.unwrap();
    worker.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn device_limits_and_old_receipts_cannot_clobber_new_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(core(dir.path()).await);
    let time = pulse_link::now();
    let credential = paired(&hub, time).await;
    let device = &credential.device;
    let worker = paired_role(&hub, ClientRole::LocalExecutor, time).await;
    let epoch = hub.begin(&worker.device, 0, time).await.unwrap();
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
            result(&hub, &worker.device, epoch, payload, time).await;
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
        result(&hub, &worker.device, epoch, payload, time).await;
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
    let worker = paired_role(&hub, ClientRole::LocalExecutor, time).await;
    let epoch = hub.begin(&worker.device, 0, time).await.unwrap();
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
    assert!(!hub.live(&worker.device, epoch, time).await.unwrap());
    assert_eq!(
        hub.core.task(&task.id).await.unwrap().state,
        State::OutcomeUnknown
    );
    let new_epoch = hub.begin(&worker.device, 0, time).await.unwrap();
    let dispatch = hub
        .outgoing(&worker.device)
        .await
        .unwrap()
        .into_iter()
        .find(|e| matches!(e.payload, ServerPayload::Read { .. }))
        .unwrap();
    assert!(!hub.permitted(&dispatch, new_epoch, time).await.unwrap());
    hub.end(&worker.device, epoch).await.unwrap();
    assert!(hub.live(&worker.device, new_epoch, time).await.unwrap());
    let result = Envelope::new(
        &worker.device,
        1,
        epoch,
        DevicePayload::Result {
            operation_id: "read:restart-read".into(),
            task_id: task.id.clone(),
            revision: task.revision.to_string(),
            outcome: ReadOutcome::text("saved before restart".into(), false),
        },
    )
    .unwrap();
    hub.receive(&worker.device, &result, time).await.unwrap();
    assert_eq!(hub.core.task(&task.id).await.unwrap().state, State::Blocked);
}

#[tokio::test]
async fn pairing_timeout_does_not_block_native_stop() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let app = axum::Router::new().route(
        "/v1/device-pairings/identity",
        axum::routing::get(move || {
            let signal = signal.clone();
            async move {
                signal.notify_one();
                tokio::time::sleep(Duration::from_secs(30)).await;
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            }
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
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("pairing request must be pending before the stop command");
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
