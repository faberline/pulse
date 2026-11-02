use super::*;
use crate::protocol::hash;

async fn local(f: &Fixture, name: &str) -> (PathBuf, Journal, DeviceLock, Credential) {
    let path = f._root.path().join(name);
    let (journal, lock) = Journal::open_role(
        &path,
        f.config.server.as_str(),
        std::path::Path::new(""),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    f.hub.revoke(&f.credential.device).await.unwrap();
    let code = f.hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    let value = f
        .hub
        .redeem(
            PairRequest {
                code,
                device: journal.device.clone(),
                role: ClientRole::Chat,
            },
            crate::now(),
        )
        .await
        .unwrap();
    assert!(journal.bind_memory_credential(&value).await.unwrap());
    (path, journal, lock, value)
}
async fn control(journal: &Journal, text: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    journal
        .memory_control(&id, text, crate::now())
        .await
        .unwrap();
    id
}
async fn snapshot(journal: &Journal, path: &std::path::Path) {
    sqlx::query("VACUUM INTO ?")
        .bind(path.to_str().unwrap())
        .execute(&journal.db)
        .await
        .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
async fn reopen(f: &Fixture, path: &std::path::Path) -> (Journal, DeviceLock) {
    Journal::open_role(
        path,
        f.config.server.as_str(),
        std::path::Path::new(""),
        ClientRole::Chat,
    )
    .await
    .unwrap()
}
fn manifest(path: &std::path::Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path.join("device.db.memory-restrictions.json")).unwrap())
        .unwrap()
}
async fn stored_without_ack(f: &Fixture, journal: &Journal) -> MemoryRequest {
    control(journal, "記住 偏好：PRIVATE_WITHDRAWAL_RESTORE_BODY").await;
    let source = journal.memory_requests().await.unwrap().remove(0);
    f.hub
        .memory_change(&journal.device, &source, crate::now())
        .await
        .unwrap();
    source
}
async fn restore(
    f: &Fixture,
    path: &std::path::Path,
    journal: Journal,
    lock: DeviceLock,
    snapshot: &std::path::Path,
) -> (Journal, DeviceLock) {
    journal.db.close().await;
    drop(journal);
    drop(lock);
    std::fs::copy(snapshot, path.join("device.db")).unwrap();
    reopen(f, path).await
}

#[tokio::test]
async fn database_before_cancel_recovers_exact_withdrawal_before_any_source_replay() {
    let f = setup().await;
    let (path, journal, lock, credential) = local(&f, "restore-pending").await;
    let old = stored_without_ack(&f, &journal).await;
    let late = f.hub.core.memory_view(crate::now()).await.unwrap();
    let backup = f._root.path().join("pre-cancel.db");
    snapshot(&journal, &backup).await;
    let cancel = control(&journal, "取消待送記憶 偏好").await;
    let request = journal.memory_requests().await.unwrap().remove(0);
    assert_eq!(request.source_id, cancel);
    let keeper = serde_json::to_string(&manifest(&path)).unwrap();
    assert!(!keeper.contains("PRIVATE_WITHDRAWAL_RESTORE_BODY"));
    assert!(!keeper.contains("偏好"));
    assert!(!keeper.contains(&credential.token));
    let (journal, _lock) = restore(&f, &path, journal, lock, &backup).await;
    assert!(journal.bind_memory_credential(&credential).await.unwrap());
    let recovered = journal.memory_requests().await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(hash(&recovered[0]).unwrap(), hash(&request).unwrap());
    assert!(!journal.memory_request_active(&old).await.unwrap());
    journal
        .apply_memory_view(&late, crate::now())
        .await
        .unwrap();
    assert!(journal.view().await.unwrap().memories.is_empty());
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(journal.memory_requests().await.unwrap().is_empty());
    assert_eq!(f.writes.load(Ordering::SeqCst), 1);
    super::identity_tests::assert_erased(&path, &["PRIVATE_WITHDRAWAL_RESTORE_BODY"]);
}

#[tokio::test]
async fn acknowledged_withdrawal_survives_pre_cancel_restore_without_resend_and_preserves_new_sources()
 {
    let f = setup().await;
    let (path, journal, lock, credential) = local(&f, "restore-ack").await;
    let old = stored_without_ack(&f, &journal).await;
    let late = f.hub.core.memory_view(crate::now()).await.unwrap();
    let backup = f._root.path().join("pre-ack.db");
    snapshot(&journal, &backup).await;
    control(&journal, "取消待送記憶 偏好").await;
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert_eq!(f.writes.load(Ordering::SeqCst), 1);
    let (journal, _lock) = restore(&f, &path, journal, lock, &backup).await;
    assert!(journal.bind_memory_credential(&credential).await.unwrap());
    assert!(journal.memory_requests().await.unwrap().is_empty());
    journal
        .apply_memory_view(&late, crate::now())
        .await
        .unwrap();
    assert!(journal.view().await.unwrap().memories.is_empty());
    assert!(
        journal
            .memory_control(
                &old.source_id.to_uppercase(),
                "記住 偏好：FORBIDDEN_REPLAY",
                crate::now()
            )
            .await
            .is_err()
    );
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert_eq!(f.writes.load(Ordering::SeqCst), 1);
    control(&journal, "記住 偏好：NEW_INDEPENDENT_HUMAN_SOURCE").await;
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert_eq!(
        f.hub.core.list_memories(crate::now()).await.unwrap()[0].body,
        "NEW_INDEPENDENT_HUMAN_SOURCE"
    );
    assert_eq!(
        journal.view().await.unwrap().memories[0].body,
        "NEW_INDEPENDENT_HUMAN_SOURCE"
    );
}

#[tokio::test]
async fn failed_sqlite_after_durable_withdrawal_blocks_the_running_copy_until_recovery() {
    let f = setup().await;
    let (path, journal, _lock, credential) = local(&f, "failed-local-commit").await;
    let old = stored_without_ack(&f, &journal).await;
    sqlx::query("CREATE TRIGGER fail_withdrawal BEFORE UPDATE ON local_memory_queue WHEN NEW.phase='redacted' BEGIN SELECT RAISE(ABORT,'fixture rollback'); END").execute(&journal.db).await.unwrap();
    let cancel = uuid::Uuid::new_v4().to_string();
    assert!(
        journal
            .memory_control(&cancel, "取消待送記憶 偏好", crate::now())
            .await
            .is_err()
    );
    assert!(manifest(&path)["withdrawals"].get(&cancel).is_some());
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') IN ('remember','correct')")
            .fetch_one(&journal.db)
            .await
            .unwrap();
    assert_eq!(pending, 1); // The SQL transaction really rolled back.
    assert!(!journal.memory_request_active(&old).await.unwrap());
    assert!(journal.view().await.unwrap().memories.is_empty());
    sqlx::query("DROP TRIGGER fail_withdrawal")
        .execute(&journal.db)
        .await
        .unwrap();
    assert!(journal.bind_memory_credential(&credential).await.unwrap());
    assert_eq!(
        journal.memory_requests().await.unwrap()[0].source_id,
        cancel
    );
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    super::identity_tests::assert_erased(&path, &["PRIVATE_WITHDRAWAL_RESTORE_BODY"]);
}

#[tokio::test]
async fn withdrawal_rejects_tool_origin_wrong_scope_changed_source_and_stale_credential() {
    let f = setup().await;
    let (path, journal, _lock, credential) = local(&f, "withdrawal-authority").await;
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Revoke {
            sources: vec![uuid::Uuid::new_v4().to_string()],
            expected: None,
        },
    };
    let server = hash(&journal.server).unwrap();
    let before = manifest(&path);
    assert!(
        journal
            .core
            .queue_memory_withdrawal(
                Origin::Tool,
                &journal.device,
                &server,
                &request,
                crate::now()
            )
            .await
            .is_err()
    );
    assert!(
        journal
            .core
            .queue_memory_withdrawal(
                Origin::Human,
                &journal.device,
                &hash(&"https://foreign.example/").unwrap(),
                &request,
                crate::now()
            )
            .await
            .is_err()
    );
    let mut body = request.clone();
    body.command = MemoryCommand::Remember {
        label: "LEAK".into(),
        body: "PRIVATE_FORGED_LEDGER_BODY".into(),
    };
    assert!(
        journal
            .core
            .queue_memory_withdrawal(Origin::Human, &journal.device, &server, &body, crate::now())
            .await
            .is_err()
    );
    assert_eq!(manifest(&path), before);
    journal
        .core
        .queue_memory_withdrawal(
            Origin::Human,
            &journal.device,
            &server,
            &request,
            crate::now(),
        )
        .await
        .unwrap();
    let before = manifest(&path);
    let mut changed = request.clone();
    changed.source_at -= 1;
    assert!(
        journal
            .core
            .queue_memory_withdrawal(
                Origin::Human,
                &journal.device,
                &server,
                &changed,
                crate::now()
            )
            .await
            .is_err()
    );
    assert!(
        journal
            .core
            .finish_memory_withdrawal(
                &journal.device,
                &server,
                &"0".repeat(64),
                &request.source_id,
                "accepted"
            )
            .await
            .is_err()
    );
    assert_eq!(manifest(&path), before);
    assert!(
        journal
            .core
            .finish_memory_withdrawal(
                &journal.device,
                &server,
                &hash(&credential).unwrap(),
                &request.source_id,
                "accepted"
            )
            .await
            .unwrap()
    );
    assert!(journal.memory_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn format_three_pending_withdrawal_is_imported_before_cutover() {
    let f = setup().await;
    let (path, journal, lock, credential) = local(&f, "legacy-withdrawal").await;
    stored_without_ack(&f, &journal).await;
    control(&journal, "取消待送記憶 偏好").await;
    let request = journal.memory_requests().await.unwrap().remove(0);
    let mut value = manifest(&path);
    value["format"] = 3.into();
    value.as_object_mut().unwrap().remove("withdrawals");
    journal.db.close().await;
    drop(journal);
    drop(lock);
    std::fs::write(
        path.join("device.db.memory-restrictions.json"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
    let (journal, _lock) = reopen(&f, &path).await;
    assert!(journal.bind_memory_credential(&credential).await.unwrap());
    assert_eq!(manifest(&path)["format"], 4);
    assert_eq!(
        hash(&journal.memory_requests().await.unwrap()[0]).unwrap(),
        hash(&request).unwrap()
    );
    assert!(
        manifest(&path)["withdrawals"]
            .get(&request.source_id)
            .is_some()
    );
}

#[tokio::test]
async fn a_stale_keeper_cannot_be_freshened_by_importing_a_pending_withdrawal() {
    let f = setup().await;
    let (path, journal, lock, credential) = local(&f, "stale-withdrawal-keeper").await;
    stored_without_ack(&f, &journal).await;
    let file = path.join("device.db.memory-restrictions.json");
    let old = std::fs::read(&file).unwrap();
    control(&journal, "取消待送記憶 偏好").await;
    let current = std::fs::read(&file).unwrap();
    journal.db.close().await;
    drop(journal);
    drop(lock);
    std::fs::write(&file, &old).unwrap();
    assert!(
        Journal::open_role(
            &path,
            f.config.server.as_str(),
            std::path::Path::new(""),
            ClientRole::Chat
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read(&file).unwrap(), old);
    std::fs::write(&file, current).unwrap();
    let (journal, _lock) = reopen(&f, &path).await;
    assert!(journal.bind_memory_credential(&credential).await.unwrap());
    assert_eq!(journal.memory_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn forget_receipt_after_restore_preserves_a_newer_independent_revision() {
    let f = setup().await;
    let (path, journal, lock, credential) = local(&f, "forget-newer").await;
    stored_without_ack(&f, &journal).await;
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    let old = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    let backup = f._root.path().join("pre-forget.db");
    snapshot(&journal, &backup).await;
    control(&journal, "忘記記憶 偏好").await;
    let correction = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Correct {
            id: old.id,
            revision: old.revision,
            label: old.label,
            body: "NEWER_SOURCE_MUST_STAY".into(),
        },
    };
    f.hub
        .memory_change(&journal.device, &correction, crate::now())
        .await
        .unwrap();
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert_eq!(
        journal.view().await.unwrap().memories[0].body,
        "NEWER_SOURCE_MUST_STAY"
    );
    let (journal, _lock) = restore(&f, &path, journal, lock, &backup).await;
    assert!(journal.bind_memory_credential(&credential).await.unwrap());
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert_eq!(
        journal.view().await.unwrap().memories[0].body,
        "NEWER_SOURCE_MUST_STAY"
    );
    assert!(journal.memory_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn withdrawal_capacity_is_bounded_and_ack_frees_only_pending_space() {
    let f = setup().await;
    let (path, journal, _lock, credential) = local(&f, "withdrawal-capacity").await;
    sqlx::query("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<1000) INSERT INTO local_memory_queue(source_id,request,phase,saved_at) SELECT printf('00000000-0000-4000-8000-%012x',n),NULL,'queried',n FROM seq").execute(&journal.db).await.unwrap();
    let server = hash(&journal.server).unwrap();
    let mut requests = Vec::new();
    for _ in 0..101 {
        requests.push(MemoryRequest {
            source_id: uuid::Uuid::new_v4().to_string(),
            source_at: crate::now(),
            command: MemoryCommand::Revoke {
                sources: vec![uuid::Uuid::new_v4().to_string()],
                expected: None,
            },
        });
    }
    for request in &requests[..100] {
        journal
            .core
            .queue_memory_withdrawal(
                Origin::Human,
                &journal.device,
                &server,
                request,
                crate::now(),
            )
            .await
            .unwrap();
    }
    let before = manifest(&path);
    assert!(
        journal
            .core
            .queue_memory_withdrawal(
                Origin::Human,
                &journal.device,
                &server,
                &requests[100],
                crate::now()
            )
            .await
            .is_err()
    );
    assert_eq!(manifest(&path), before);
    assert_eq!(journal.memory_requests().await.unwrap().len(), 100);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM local_memory_queue")
        .fetch_one(&journal.db)
        .await
        .unwrap();
    assert_eq!(count, 1000);
    journal
        .core
        .finish_memory_withdrawal(
            &journal.device,
            &server,
            &hash(&credential).unwrap(),
            &requests[0].source_id,
            "accepted",
        )
        .await
        .unwrap();
    journal
        .core
        .queue_memory_withdrawal(
            Origin::Human,
            &journal.device,
            &server,
            &requests[100],
            crate::now(),
        )
        .await
        .unwrap();
    assert_eq!(journal.memory_requests().await.unwrap().len(), 100);
    assert_eq!(
        manifest(&path)["withdrawals"].as_object().unwrap().len(),
        101
    );
}

#[tokio::test]
async fn full_unknown_withdrawal_history_keeps_space_for_a_known_memory_deletion() {
    let f = setup().await;
    let (path, journal, _lock, credential) = local(&f, "withdrawal-delete-reserve").await;
    stored_without_ack(&f, &journal).await;
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    let mut value = manifest(&path);
    let target = uuid::Uuid::new_v4().to_string();
    let mut entries = serde_json::Map::new();
    for _ in 0..10_000 {
        let id = uuid::Uuid::new_v4().to_string();
        entries.insert(id.clone(),serde_json::json!({"request":{"source_id":id,"source_at":crate::now(),"command":{"kind":"revoke","sources":[target],"expected":null}},"phase":"accepted"}));
    }
    value["withdrawals"] = entries.into();
    value["copy"]["serial"] = (value["copy"]["serial"].as_u64().unwrap() + 1).into();
    std::fs::write(
        path.join("device.db.memory-restrictions.json"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
    let rejected = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Revoke {
            sources: vec![target],
            expected: None,
        },
    };
    assert!(
        journal
            .core
            .queue_memory_withdrawal(
                Origin::Human,
                &journal.device,
                &hash(&journal.server).unwrap(),
                &rejected,
                crate::now()
            )
            .await
            .is_err()
    );
    assert_eq!(manifest(&path), value);
    control(&journal, "忘記記憶 偏好").await;
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        manifest(&path)["withdrawals"].as_object().unwrap().len(),
        10_001
    );
}

#[tokio::test]
async fn an_unrelated_success_body_cannot_consume_a_pending_withdrawal() {
    let f = setup().await;
    let (path, journal, _lock, credential) = local(&f, "wrong-receipt").await;
    stored_without_ack(&f, &journal).await;
    let cancel = control(&journal, "取消待送記憶 偏好").await;
    f.bad_ack.store(true, Ordering::SeqCst);
    assert!(
        matches!(client::sync(&f.config,&journal,&credential).await,Err(crate::Error::Rejected(reason)) if reason=="withdrawal acknowledgement mismatch")
    );
    assert_eq!(manifest(&path)["withdrawals"][&cancel]["phase"], "queued");
    assert_eq!(
        journal.memory_requests().await.unwrap()[0].source_id,
        cancel
    );
    assert_eq!(
        f.hub.core.list_memories(crate::now()).await.unwrap().len(),
        1
    );
    f.bad_ack.store(false, Ordering::SeqCst);
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "child harness; parent requires exit73 after durable evidence and failed SQL"]
async fn withdrawal_boundary_child() {
    let Ok(path) = std::env::var("PULSE_WITHDRAWAL_CHILD_DIR") else {
        return;
    };
    let path = PathBuf::from(path);
    let server = std::env::var("PULSE_WITHDRAWAL_CHILD_SERVER").unwrap();
    let (journal, _lock) =
        Journal::open_role(&path, &server, std::path::Path::new(""), ClientRole::Chat)
            .await
            .unwrap();
    let request: MemoryRequest =
        serde_json::from_slice(&std::fs::read(path.join("child-request.json")).unwrap()).unwrap();
    assert!(
        journal
            .core
            .queue_memory_withdrawal(
                Origin::Human,
                &journal.device,
                &hash(&server).unwrap(),
                &request,
                crate::now()
            )
            .await
            .is_err()
    );
    assert!(
        manifest(&path)["withdrawals"]
            .get(&request.source_id)
            .is_some()
    );
    std::process::exit(73);
}

#[tokio::test]
async fn hard_exit_after_keeper_write_recovers_the_exact_cancel_before_reopening_content() {
    use sqlx::Connection;
    let f = setup().await;
    let (path, journal, lock, credential) = local(&f, "withdrawal-hard-exit").await;
    let old = stored_without_ack(&f, &journal).await;
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Revoke {
            sources: vec![old.source_id],
            expected: None,
        },
    };
    crate::journal::private_write(
        &path.join("child-request.json"),
        &serde_json::to_vec(&request).unwrap(),
    )
    .unwrap();
    sqlx::query("CREATE TRIGGER fail_withdrawal BEFORE UPDATE ON local_memory_queue WHEN NEW.phase='redacted' BEGIN SELECT RAISE(ABORT,'fixture rollback'); END").execute(&journal.db).await.unwrap();
    journal.db.close().await;
    drop(journal);
    drop(lock);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "memory_chat::tests::withdrawal_tests::withdrawal_boundary_child",
            "--ignored",
            "--nocapture",
        ])
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("PULSE_WITHDRAWAL_CHILD_DIR", &path)
        .env("PULSE_WITHDRAWAL_CHILD_SERVER", f.config.server.as_str())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut db = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(path.join("device.db")),
    )
    .await
    .unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') IN ('remember','correct')")
            .fetch_one(&mut db)
            .await
            .unwrap();
    assert_eq!(count, 1);
    sqlx::query("DROP TRIGGER fail_withdrawal")
        .execute(&mut db)
        .await
        .unwrap();
    db.close().await.unwrap();
    let (journal, _lock) = reopen(&f, &path).await;
    assert!(journal.bind_memory_credential(&credential).await.unwrap());
    assert_eq!(
        hash(&journal.memory_requests().await.unwrap()[0]).unwrap(),
        hash(&request).unwrap()
    );
    client::sync(&f.config, &journal, &credential)
        .await
        .unwrap();
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    super::identity_tests::assert_erased(&path, &["PRIVATE_WITHDRAWAL_RESTORE_BODY"]);
}
