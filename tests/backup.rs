use pulse_core::*;
use serde_json::json;
use sqlx::{
    Connection,
    sqlite::{SqliteConnectOptions, SqliteConnection},
};
use std::{fs, path::Path};
use tempfile::TempDir;

const NOW: i64 = 1_800_000_000_000;
const DAY: i64 = 86_400_000;

async fn setup() -> (TempDir, Store) {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    (dir, store)
}

async fn memory(store: &Store, at: i64) -> String {
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: at,
        command: MemoryCommand::Remember {
            label: "回答用語".into(),
            body: "PRIVATE-KNOWLEDGE-MARKER-繁體中文".into(),
        },
    };
    let MemoryOutcome::Stored { id, .. } = store
        .change_memory(Origin::Human, &request, at)
        .await
        .unwrap()
    else {
        panic!("not stored")
    };
    id
}

async fn raw(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(path))
        .await
        .unwrap()
}

#[tokio::test]
async fn encrypted_daily_backup_is_durable_private_and_validates_latest_memory_deletion() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, store) = setup().await;
    let id = memory(&store, NOW).await;
    let first = store.maintain(NOW).await.unwrap();
    let backup = first.backup.unwrap();
    assert_eq!(first.next_at, (NOW / DAY + 1) * DAY);
    let encrypted = fs::read(&backup).unwrap();
    assert!(encrypted.starts_with(b"PULSEBK1"));
    assert!(
        !encrypted
            .windows(b"PRIVATE-KNOWLEDGE-MARKER".len())
            .any(|w| w == b"PRIVATE-KNOWLEDGE-MARKER")
    );
    for path in [&backup, &dir.path().join("db.backup-key")] {
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert_eq!(
        fs::metadata(dir.path().join("db.backups"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::read_dir(dir.path().join("db.backups")).unwrap().count(),
        1
    );
    assert!(store.maintain(NOW + 1).await.unwrap().backup.is_none());
    let forget = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: NOW + 2,
        command: MemoryCommand::Forget { id, revision: 1 },
    };
    store
        .change_memory(Origin::Human, &forget, NOW + 2)
        .await
        .unwrap();
    store.close().await;
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert!(store.maintain(NOW + 3).await.unwrap().backup.is_none());
    let check = store
        .check_backup(
            &backup,
            &dir.path().join("check"),
            &Versions::fixture(),
            NOW + 3,
        )
        .await
        .unwrap();
    assert_eq!(check.memories, 0);
    assert!(Store::open(&check.candidate, "owner").await.is_err());
    let mut db = raw(&check.candidate).await;
    // This binding was committed after maintain's WAL truncation and before
    // serialization. A raw copy of the main file would lose this WAL change.
    let restored_key: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='backup_key_fingerprint'")
            .fetch_one(&mut db)
            .await
            .unwrap();
    let live_key: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='backup_key_fingerprint'")
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
    assert_eq!(restored_key, live_key);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&mut db)
            .await
            .unwrap(),
        2_147_483_647
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM memories")
            .fetch_one(&mut db)
            .await
            .unwrap(),
        0
    );
    db.close().await.unwrap();
    let next = store.maintain(NOW + DAY).await.unwrap().backup.unwrap();
    assert_ne!(fs::read(next).unwrap(), encrypted);
}

#[tokio::test]
async fn maintenance_expires_without_queries_and_schedules_the_earliest_expiry() {
    let (dir, store) = setup().await;
    let source = NOW - MEMORY_RETENTION_MS + 500;
    memory(&store, source).await;
    let first = store.maintain(NOW).await.unwrap();
    assert_eq!(first.next_at, NOW + 500);
    assert!(store.maintain(NOW + 500).await.unwrap().backup.is_none());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM memories")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    let checked = store
        .check_backup(
            &first.backup.unwrap(),
            &dir.path().join("expired-source"),
            &Versions::fixture(),
            NOW + 500,
        )
        .await
        .unwrap();
    assert_eq!(checked.memories, 0);
    // No generic task or model budget entry is created by local maintenance.
    assert!(store.list_tasks(100).await.unwrap().is_empty());
    let charged: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(charged, 0);
}

#[tokio::test]
async fn thirty_day_cleanup_removes_only_owned_managed_backup_files() {
    let (dir, store) = setup().await;
    let old = store.maintain(NOW).await.unwrap().backup.unwrap();
    assert!(
        store
            .maintain(NOW + 29 * DAY)
            .await
            .unwrap()
            .backup
            .is_some()
    );
    assert!(old.exists());
    let state = store.maintain(NOW + 30 * DAY).await.unwrap();
    assert_eq!(state.removed_backups, 1);
    assert!(!old.exists());
    let unknown = dir.path().join("db.backups/keep-me");
    fs::write(&unknown, b"unrelated").unwrap();
    assert!(store.maintain(NOW + 30 * DAY + 1).await.is_err());
    assert_eq!(fs::read(unknown).unwrap(), b"unrelated");
}

#[tokio::test]
async fn lost_wrong_or_public_key_cannot_be_regenerated_or_used() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, store) = setup().await;
    let backup = store.maintain(NOW).await.unwrap().backup.unwrap();
    let key = dir.path().join("db.backup-key");
    let original = fs::read(&key).unwrap();
    fs::remove_file(&key).unwrap();
    assert!(store.maintain(NOW + DAY).await.is_err());
    assert!(!key.exists());
    assert!(
        store
            .check_backup(
                &backup,
                &dir.path().join("missing"),
                &Versions::fixture(),
                NOW + 1
            )
            .await
            .is_err()
    );
    fs::write(&key, &original).unwrap();
    fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(store.maintain(NOW + DAY).await.is_err());
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    let mut changed: serde_json::Value = serde_json::from_slice(&original).unwrap();
    changed["key"][0] = json!(255 - changed["key"][0].as_u64().unwrap());
    fs::write(&key, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert!(store.maintain(NOW + DAY).await.is_err());
    assert!(
        store
            .check_backup(
                &backup,
                &dir.path().join("wrong"),
                &Versions::fixture(),
                NOW + 1
            )
            .await
            .is_err()
    );
    fs::write(&key, original).unwrap();
    assert!(store.maintain(NOW + DAY).await.is_ok());
}

#[tokio::test]
async fn missing_restrictions_corruption_expiry_or_wrong_keeper_never_yields_candidate() {
    let (dir, store) = setup().await;
    memory(&store, NOW).await;
    let backup = store.maintain(NOW).await.unwrap().backup.unwrap();
    assert!(
        store
            .check_backup(
                &backup,
                &dir.path().join("future"),
                &Versions::fixture(),
                NOW - 1
            )
            .await
            .is_err()
    );
    assert!(
        store
            .check_backup(
                &backup,
                &dir.path().join("expired"),
                &Versions::fixture(),
                NOW + 30 * DAY
            )
            .await
            .is_err()
    );
    let (_other_dir, other) = setup().await;
    other.maintain(NOW).await.unwrap();
    assert!(
        other
            .check_backup(
                &backup,
                &dir.path().join("other"),
                &Versions::fixture(),
                NOW
            )
            .await
            .is_err()
    );
    let original = fs::read(&backup).unwrap();
    let mut corrupt = original.clone();
    let end = corrupt.len() - 1;
    corrupt[end] ^= 1;
    fs::write(&backup, corrupt).unwrap();
    let rejected = dir.path().join("corrupt");
    assert!(
        store
            .check_backup(&backup, &rejected, &Versions::fixture(), NOW)
            .await
            .is_err()
    );
    assert!(!rejected.exists());
    // Same-day corruption causes a fresh encrypted publication, not a silent skip.
    assert!(store.maintain(NOW + 1).await.unwrap().backup.is_some());
    fs::write(&backup, original).unwrap();
    fs::remove_file(dir.path().join("db.memory-restrictions.json")).unwrap();
    assert!(
        store
            .check_backup(
                &backup,
                &dir.path().join("missing"),
                &Versions::fixture(),
                NOW
            )
            .await
            .is_err()
    );
    assert!(!dir.path().join("missing").exists());
}

#[tokio::test]
async fn interrupted_encryption_is_cleaned_and_restore_never_overwrites_a_directory() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, store) = setup().await;
    let backup = store.maintain(NOW).await.unwrap().backup.unwrap();
    let partial = dir
        .path()
        .join(format!("db.backups/{}.partial", uuid::Uuid::new_v4()));
    fs::write(&partial, b"incomplete encrypted record").unwrap();
    fs::set_permissions(&partial, fs::Permissions::from_mode(0o600)).unwrap();
    store.maintain(NOW + 1).await.unwrap();
    assert!(!partial.exists());
    let existing = dir.path().join("existing");
    fs::create_dir(&existing).unwrap();
    let preserved = existing.join("preserve");
    fs::write(&preserved, b"human file").unwrap();
    assert!(
        store
            .check_backup(&backup, &existing, &Versions::fixture(), NOW + 1)
            .await
            .is_err()
    );
    assert_eq!(fs::read(preserved).unwrap(), b"human file");
}

#[tokio::test]
async fn snapshot_contains_committed_tasks_and_unknown_operation_stays_unknown() {
    let (dir, store) = setup().await;
    let event = store.ingest(Origin::Human,"ui","task", json!({"commands":[{"kind":"create","id":"task","goal":"owned fixture","scope":"pulse:read","priority":0,"requires_device":false}]}),NOW).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            json!({"step":1}),
            "fixture",
            None,
            NOW + 2,
        )
        .await
        .unwrap();
    let operation = store
        .prepare_operation(&run.token, "fixture:counter", json!({"delta":1}), NOW + 3)
        .await
        .unwrap();
    let backup = store.maintain(NOW + 4).await.unwrap().backup.unwrap();
    let checked = store
        .check_backup(
            &backup,
            &dir.path().join("checked"),
            &Versions::fixture(),
            NOW + 5,
        )
        .await
        .unwrap();
    assert_eq!(checked.tasks, 1);
    assert_eq!(checked.unknown_tasks, 1);
    let mut db = raw(&checked.candidate).await;
    let data: String = sqlx::query_scalar("SELECT data FROM operations WHERE id=?")
        .bind(&operation.id)
        .fetch_one(&mut db)
        .await
        .unwrap();
    let restored: Operation = serde_json::from_str(&data).unwrap();
    assert_eq!(restored.state, OperationState::Unknown);
    assert!(store.pending_operations("task").await.unwrap().len() == 1);
}

#[tokio::test]
async fn restore_check_revokes_old_approved_but_undispatched_grants() {
    let (dir, store) = setup().await;
    let event = store.ingest(Origin::Human,"ui","create",json!({"commands":[{"kind":"create","id":"task","goal":"owned approval fixture","scope":"pulse:read","priority":0,"requires_device":false}]}),NOW).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    let proposal = OperationProposal {
        id: "approval".into(),
        token: run.token.clone(),
        target: "fixture:counter".into(),
        intent: json!({"delta":1}),
        expires_at: NOW + 60_000,
    };
    let event = store
        .ingest(
            Origin::Tool,
            "planner",
            "propose",
            json!({"commands":[{"kind":"query","ids":["task"]}],"operation_proposal":proposal}),
            NOW + 2,
        )
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap();
    let approval = store
        .propose_operation(&event.event_id, proposal, NOW + 2)
        .await
        .unwrap();
    let event = store.ingest(Origin::Human,"ui","approve",json!({"commands":[{"kind":"approve_operation","binding":{"task_id":"task","expected_revision":"1","request_id":"approval","intent_digest":approval.intent_digest}}]}),NOW+3).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    assert_eq!(
        store.operation_request("approval").await.unwrap().state,
        ApprovalState::Approved
    );
    let backup = store.maintain(NOW + 4).await.unwrap().backup.unwrap();
    let checked = store
        .check_backup(
            &backup,
            &dir.path().join("checked"),
            &Versions::fixture(),
            NOW + 5,
        )
        .await
        .unwrap();
    let mut db = raw(&checked.candidate).await;
    let data: String =
        sqlx::query_scalar("SELECT data FROM operation_requests WHERE id='approval'")
            .fetch_one(&mut db)
            .await
            .unwrap();
    let restored: OperationRequest = serde_json::from_str(&data).unwrap();
    assert_eq!(restored.state, ApprovalState::Invalidated);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operations")
            .fetch_one(&mut db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store.operation_request("approval").await.unwrap().state,
        ApprovalState::Approved
    );
}

#[tokio::test]
async fn migration_validation_failure_leaves_an_unrunnable_quarantined_candidate() {
    let (dir, store) = setup().await;
    sqlx::query("UPDATE _sqlx_migrations SET checksum=X'00' WHERE version=6")
        .execute(&store.application_pool())
        .await
        .unwrap();
    let backup = store.maintain(NOW).await.unwrap().backup.unwrap();
    let destination = dir.path().join("bad-migration");
    assert!(
        store
            .check_backup(&backup, &destination, &Versions::fixture(), NOW)
            .await
            .is_err()
    );
    let mut db = raw(&destination.join("candidate.db")).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&mut db)
            .await
            .unwrap(),
        2_147_483_647
    );
    assert!(
        Store::open(destination.join("candidate.db"), "owner")
            .await
            .is_err()
    );
}
