use super::*;

async fn journal_at(f: &Fixture, name: &str) -> (PathBuf, Journal, DeviceLock) {
    let path = f._root.path().join(name);
    let (journal, lock) = Journal::open_role(
        &path,
        f.config.server.as_str(),
        std::path::Path::new(""),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    (path, journal, lock)
}
fn credential(journal: &Journal, character: char) -> Credential {
    Credential {
        device: journal.device.clone(),
        role: ClientRole::Chat,
        token: character.to_string().repeat(64),
    }
}
async fn remember(journal: &Journal, body: &str) -> MemoryRequest {
    journal
        .memory_control(
            &uuid::Uuid::new_v4().to_string(),
            &format!("記住 偏好：{body}"),
            crate::now(),
        )
        .await
        .unwrap();
    journal.memory_requests().await.unwrap().pop().unwrap()
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

#[tokio::test]
async fn old_database_cannot_restore_queued_sources_after_revocation_and_repair() {
    let f = setup().await;
    let (path, journal, lock) = journal_at(&f, "restored").await;
    let old = credential(&journal, 'a');
    let new = credential(&journal, 'b');
    assert!(journal.bind_memory_credential(&old).await.unwrap());
    let request = remember(&journal, "PRIVATE_OLD_COPY_BODY").await;
    let snapshot = f._root.path().join("old-copy.db");
    sqlx::query("VACUUM INTO ?")
        .bind(snapshot.to_str().unwrap())
        .execute(&journal.db)
        .await
        .unwrap();
    journal.deny_memory_access().await.unwrap();
    journal.prepare_memory_pairing(&new).await.unwrap();
    journal.activate_memory_pairing(&new).await.unwrap();
    let current = std::fs::read(path.join("device.db.memory-restrictions.json")).unwrap();
    assert!(!String::from_utf8_lossy(&current).contains(&old.token));
    assert!(!String::from_utf8_lossy(&current).contains(&new.token));
    journal.db.close().await;
    drop(journal);
    drop(lock);
    std::fs::copy(snapshot, path.join("device.db")).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        path.join("device.db"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let (journal, _lock) = reopen(&f, &path).await;
    assert!(journal.bind_memory_credential(&new).await.unwrap());
    assert!(journal.memory_requests().await.unwrap().is_empty());
    assert!(!journal.memory_request_active(&request).await.unwrap());
    assert!(journal.view().await.unwrap().memories.is_empty());
    super::identity_tests::assert_erased(&path, &["PRIVATE_OLD_COPY_BODY"]);
    assert_eq!(f.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stale_or_missing_copy_keeper_is_rejected_without_reopening_memory() {
    let f = setup().await;
    let (path, journal, lock) = journal_at(&f, "stale-keeper").await;
    let value = credential(&journal, 'a');
    assert!(journal.bind_memory_credential(&value).await.unwrap());
    let manifest = path.join("device.db.memory-restrictions.json");
    let old = std::fs::read(&manifest).unwrap();
    journal.deny_memory_access().await.unwrap();
    let current = std::fs::read(&manifest).unwrap();
    journal.db.close().await;
    drop(journal);
    drop(lock);
    std::fs::write(&manifest, old).unwrap();
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
    std::fs::write(&manifest, current).unwrap();
    std::fs::remove_file(&manifest).unwrap();
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
}

#[tokio::test]
async fn prepared_first_pair_keeps_human_queue_but_rejects_the_wrong_credential() {
    let f = setup().await;
    let (path, journal, lock) = journal_at(&f, "bootstrap").await;
    let request = remember(&journal, "PRIVATE_BOOTSTRAP_BODY").await;
    let new = credential(&journal, 'b');
    let wrong = credential(&journal, 'c');
    journal.prepare_memory_pairing(&new).await.unwrap();
    assert!(!journal.bind_memory_credential(&wrong).await.unwrap());
    // An uncompleted first-pair retry still owns no old active identity.
    journal.prepare_memory_pairing(&wrong).await.unwrap();
    journal.prepare_memory_pairing(&new).await.unwrap();
    assert!(
        journal
            .memory_control(
                &uuid::Uuid::new_v4().to_string(),
                "記住 偏好：BLOCKED",
                crate::now()
            )
            .await
            .is_err()
    );
    journal.db.close().await;
    drop(journal);
    drop(lock);
    let (journal, _lock) = reopen(&f, &path).await;
    assert!(journal.bind_memory_credential(&new).await.unwrap());
    let replay = journal.memory_requests().await.unwrap().remove(0);
    assert_eq!(
        serde_json::to_value(replay).unwrap(),
        serde_json::to_value(&request).unwrap()
    );
    let value: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.join("device.db.memory-restrictions.json")).unwrap(),
    )
    .unwrap();
    assert!(value["copy"]["pending"].is_null());
    assert_eq!(value["copy"]["generation"], 0);
    // Repeating the completed cutover must preserve newer unacknowledged input.
    let before = std::fs::read(path.join("device.db.memory-restrictions.json")).unwrap();
    journal.prepare_memory_pairing(&new).await.unwrap();
    journal.activate_memory_pairing(&new).await.unwrap();
    assert_eq!(
        journal.memory_requests().await.unwrap()[0].source_id,
        request.source_id
    );
    assert_eq!(
        std::fs::read(path.join("device.db.memory-restrictions.json")).unwrap(),
        before
    );
}

#[tokio::test]
async fn same_generation_pair_state_cannot_be_rolled_back_or_removed() {
    let f = setup().await;
    let (path, journal, lock) = journal_at(&f, "serial").await;
    let file = path.join("device.db.memory-restrictions.json");
    let before = std::fs::read(&file).unwrap();
    let value = credential(&journal, 'b');
    journal.prepare_memory_pairing(&value).await.unwrap();
    journal.activate_memory_pairing(&value).await.unwrap();
    let current = std::fs::read(&file).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&current).unwrap();
    assert_eq!(doc["copy"]["generation"], 0);
    assert!(doc["copy"]["serial"].as_u64().unwrap() > 1);
    journal.db.close().await;
    drop(journal);
    drop(lock);
    std::fs::write(&file, before).unwrap();
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
    let mut missing: serde_json::Value = serde_json::from_slice(&current).unwrap();
    missing.as_object_mut().unwrap().remove("copy");
    std::fs::write(&file, serde_json::to_vec(&missing).unwrap()).unwrap();
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
}

#[tokio::test]
async fn copy_fence_cannot_be_rebound_to_another_device_or_origin() {
    let f = setup().await;
    let (path, journal, _lock) = journal_at(&f, "binding").await;
    let value = credential(&journal, 'a');
    assert!(journal.bind_memory_credential(&value).await.unwrap());
    let file = path.join("device.db.memory-restrictions.json");
    let before = std::fs::read(&file).unwrap();
    let server = crate::protocol::hash(&journal.server).unwrap();
    assert!(
        journal
            .core
            .memory_copy(
                &uuid::Uuid::new_v4().to_string(),
                &server,
                pulse_core::MemoryCopyChange::Deny
            )
            .await
            .is_err()
    );
    assert!(
        journal
            .core
            .memory_copy(
                &journal.device,
                &crate::protocol::hash(&"https://foreign.example/").unwrap(),
                pulse_core::MemoryCopyChange::Deny
            )
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(file).unwrap(), before);
    assert!(journal.memory_access_allowed().await.unwrap());
}

#[tokio::test]
async fn failed_sqlite_fence_still_blocks_the_running_copy_until_cleanup_succeeds() {
    let f = setup().await;
    say(&f, "記住 快取：PRIVATE_FAILED_COPY_CACHE").await;
    sync(&f).await;
    let source = say(&f, "記住 待送：PRIVATE_FAILED_COPY_PENDING").await;
    let captured = f.journal.memory_requests().await.unwrap().remove(0);
    assert_eq!(captured.source_id, source);
    sqlx::query("CREATE TRIGGER fail_copy BEFORE UPDATE ON local_memory_access BEGIN SELECT RAISE(ABORT,'fixture rollback'); END").execute(&f.journal.db).await.unwrap();
    assert!(f.journal.deny_memory_access().await.is_err());
    assert_eq!(count(&f, "SELECT denied FROM local_memory_access").await, 0); // Transaction really rolled back.
    assert_eq!(
        count(&f, "SELECT count(*) FROM local_memory_cache").await,
        1
    );
    assert!(!f.journal.memory_access_allowed().await.unwrap());
    assert!(!f.journal.memory_request_active(&captured).await.unwrap());
    assert!(f.journal.memory_requests().await.unwrap().is_empty());
    assert!(f.journal.view().await.unwrap().memories.is_empty());
    assert!(
        f.journal
            .memory_control(
                &uuid::Uuid::new_v4().to_string(),
                "記住 拒絕：BLOCKED",
                crate::now()
            )
            .await
            .is_err()
    );
    sqlx::query("DROP TRIGGER fail_copy")
        .execute(&f.journal.db)
        .await
        .unwrap();
    f.journal.deny_memory_access().await.unwrap();
    super::identity_tests::assert_erased(
        &f.config.data_dir,
        &["PRIVATE_FAILED_COPY_CACHE", "PRIVATE_FAILED_COPY_PENDING"],
    );
}

async fn repair(f: &Fixture) -> Credential {
    f.journal.deny_memory_access().await.unwrap();
    f.hub.revoke(&f.credential.device).await.unwrap();
    let code = f.hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    let value = f
        .hub
        .redeem(
            PairRequest {
                code,
                device: f.journal.device.clone(),
                role: ClientRole::Chat,
            },
            crate::now(),
        )
        .await
        .unwrap();
    f.journal.prepare_memory_pairing(&value).await.unwrap();
    f.journal.activate_memory_pairing(&value).await.unwrap();
    value
}

#[tokio::test]
async fn bodyless_human_withdrawal_survives_identity_rotation_and_reaches_the_server() {
    let f = setup().await;
    say(&f, "記住 偏好：PRIVATE_WITHDRAWAL_BODY").await;
    sync(&f).await;
    say(&f, "忘記記憶 偏好").await;
    let withdrawal = f.journal.memory_requests().await.unwrap().remove(0);
    assert!(matches!(withdrawal.command, MemoryCommand::Revoke { .. }));
    let value = repair(&f).await;
    assert_eq!(
        f.journal.memory_requests().await.unwrap()[0].source_id,
        withdrawal.source_id
    );
    client::sync(&f.config, &f.journal, &value).await.unwrap();
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(f.journal.memory_requests().await.unwrap().is_empty());
    assert_eq!(f.writes.load(Ordering::SeqCst), 2);
    super::identity_tests::assert_erased(&f.config.data_dir, &["PRIVATE_WITHDRAWAL_BODY"]);
}

#[tokio::test]
async fn old_credential_cannot_clear_the_new_copy_complete_its_queue_or_apply_a_late_view() {
    let f = setup().await;
    say(&f, "記住 舊來源：OLD_AUTHORIZED_SOURCE").await;
    sync(&f).await;
    let old_view = f.hub.core.memory_view(crate::now()).await.unwrap();
    let value = repair(&f).await;
    say(&f, "記住 新來源：NEW_AUTHORIZED_SOURCE").await;
    client::sync(&f.config, &f.journal, &value).await.unwrap();
    say(&f, "記住 新生效待送：NEW_PENDING_MUST_STAY").await;
    let request = f.journal.memory_requests().await.unwrap().remove(0);
    assert!(
        f.journal
            .apply_memory_view_for(&old_view, &f.credential, crate::now())
            .await
            .is_err()
    );
    assert!(
        f.journal
            .finish_memory_for(&request.source_id, "accepted", &f.credential)
            .await
            .is_err()
    );
    assert!(
        !f.journal
            .deny_memory_credential(&f.credential)
            .await
            .unwrap()
    );
    assert!(
        matches!(client::sync(&f.config,&f.journal,&f.credential).await,Err(crate::Error::Rejected(reason)) if reason=="memory identity changed")
    );
    assert!(f.journal.memory_access_allowed().await.unwrap());
    assert_eq!(
        f.journal.memory_requests().await.unwrap()[0].source_id,
        request.source_id
    );
    assert!(
        f.journal
            .view()
            .await
            .unwrap()
            .memories
            .iter()
            .any(|m| m.body == "NEW_AUTHORIZED_SOURCE")
    );
    assert_eq!(f.writes.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn native_restart_finishes_a_pair_whose_credential_was_already_saved() {
    use tokio::sync::{mpsc, watch};
    let f = setup().await;
    let (path, journal, lock) = journal_at(&f, "pair-recovery").await;
    let old = credential(&journal, 'a');
    assert!(journal.bind_memory_credential(&old).await.unwrap());
    remember(&journal, "PRIVATE_OLD_PAIR_BODY").await;
    journal.deny_memory_access().await.unwrap();
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
    journal.prepare_memory_pairing(&value).await.unwrap();
    // Match the real vault-save -> process-exit boundary. Activation is omitted.
    let vault = Arc::new(MemoryVault(std::sync::Mutex::new(Some(value))));
    journal.db.close().await;
    drop(journal);
    drop(lock);
    let device = crate::client::Device::open_chat(
        Config {
            server: f.config.server.clone(),
            repo: PathBuf::new(),
            data_dir: path.clone(),
        },
        vault,
    )
    .await
    .unwrap();
    let (commands, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let worker = tokio::spawn(device.run(receiver, states));
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.wait_for(|s| s.connected && s.paired),
    )
    .await
    .unwrap()
    .unwrap();
    commands
        .send(NativeCommand::Action {
            source_id: uuid::Uuid::new_v4().to_string(),
            action: HumanAction::Submit {
                id: "not-a-task".into(),
                goal: "記住 新偏好：RECOVERED_PAIR_BODY".into(),
            },
        })
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.wait_for(|s| s.memories.iter().any(|m| m.body == "RECOVERED_PAIR_BODY")),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        !f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .iter()
            .any(|m| m.body == "PRIVATE_OLD_PAIR_BODY")
    );
    assert_eq!(f.writes.load(Ordering::SeqCst), 1);
    assert!(f.hub.core.list_tasks(100).await.unwrap().is_empty());
    super::identity_tests::assert_erased(&path, &["PRIVATE_OLD_PAIR_BODY"]);
    commands.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "child harness for copy_fence_survives_exit_before_sqlite_commit"]
async fn copy_fence_exit_child() {
    let path = PathBuf::from(std::env::var_os("PULSE_COPY_EXIT_PATH").expect("fixture path"));
    let server = std::env::var("PULSE_COPY_EXIT_SERVER").unwrap();
    let (journal, _lock) =
        Journal::open_role(&path, &server, std::path::Path::new(""), ClientRole::Chat)
            .await
            .unwrap();
    sqlx::query("CREATE TRIGGER fail_copy BEFORE UPDATE ON local_memory_access BEGIN SELECT RAISE(ABORT,'fixture rollback'); END").execute(&journal.db).await.unwrap();
    assert!(journal.deny_memory_access().await.is_err());
    std::process::exit(73);
}

#[tokio::test]
async fn copy_fence_survives_exit_before_sqlite_commit() {
    use sqlx::Connection;
    let f = setup().await;
    let (path, journal, lock) = journal_at(&f, "hard-exit").await;
    assert!(
        journal
            .bind_memory_credential(&credential(&journal, 'a'))
            .await
            .unwrap()
    );
    remember(&journal, "PRIVATE_COPY_EXIT_BODY").await;
    journal.db.close().await;
    drop(journal);
    drop(lock);
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "memory_chat::tests::generation_tests::copy_fence_exit_child",
                "--ignored",
                "--nocapture",
            ])
            .env("PULSE_COPY_EXIT_PATH", &path)
            .env("PULSE_COPY_EXIT_SERVER", f.config.server.as_str())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
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
    let generation: i64 = sqlx::query_scalar("SELECT generation FROM local_memory_access")
        .fetch_one(&mut db)
        .await
        .unwrap();
    assert_eq!(generation, 0);
    sqlx::query("DROP TRIGGER fail_copy")
        .execute(&mut db)
        .await
        .unwrap();
    db.close().await.unwrap();
    let (journal, _lock) = reopen(&f, &path).await;
    assert!(!journal.memory_access_allowed().await.unwrap());
    assert!(journal.memory_requests().await.unwrap().is_empty());
    super::identity_tests::assert_erased(&path, &["PRIVATE_COPY_EXIT_BODY"]);
}
