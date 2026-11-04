use pulse_core::*;
use sqlx::{Connection, sqlite::SqliteConnectOptions};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const NOW: i64 = 1_800_000_000_000;
const MARKER: &str = "OWNED-OFFLINE-BACKUP-PRIVATE-PREFERENCE-繁體中文";

fn suffix(database: &Path, ending: &str) -> PathBuf {
    let mut name = database.as_os_str().to_owned();
    name.push(ending);
    name.into()
}

async fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf, Store, String) {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = dir.path().join("db");
    let store = Store::open(&database, "owner").await.unwrap();
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: NOW,
        command: MemoryCommand::Remember {
            label: "owned preference".into(),
            body: MARKER.into(),
        },
    };
    let MemoryOutcome::Stored { id, .. } = store
        .change_memory(Origin::Human, &request, NOW)
        .await
        .unwrap()
    else {
        panic!("not stored")
    };
    let backup = store.maintain(NOW).await.unwrap().backup.unwrap();
    (dir, database, backup, store, id)
}

async fn admission(database: &Path) -> DatabaseMaintenance {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match DatabaseMaintenance::try_acquire(database) {
            Ok(value) => return value,
            Err(error) if Instant::now() >= deadline => panic!("admission not released: {error}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

async fn forget(store: &Store, id: &str) {
    store
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: NOW + 1,
                command: MemoryCommand::Forget {
                    id: id.into(),
                    revision: 1,
                },
            },
            NOW + 1,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn lost_database_check_uses_original_key_and_latest_deletion_without_recreating_source() {
    let (dir, database, backup, store, id) = fixture().await;
    forget(&store, &id).await;
    store.close().await;
    let admission = admission(&database).await;
    fs::remove_file(&database).unwrap();
    let key = suffix(&database, ".backup-key");
    let restrictions = suffix(&database, ".memory-restrictions.json");
    let key_before = fs::read(&key).unwrap();
    let rules_before = fs::read(&restrictions).unwrap();
    let check = admission
        .check_backup(
            &backup,
            &dir.path().join("checked"),
            "owner",
            &Versions::fixture(),
            NOW + 2,
        )
        .await
        .unwrap();
    assert!(!check.source_identity_verified);
    assert_eq!(check.backup.memories, 0);
    assert!(!database.exists());
    assert_eq!(fs::read(&key).unwrap(), key_before);
    assert_eq!(fs::read(&restrictions).unwrap(), rules_before);
    let candidate = check.backup.candidate;
    assert!(Store::open(&candidate, "owner").await.is_err());
    assert!(
        !fs::read(&candidate)
            .unwrap()
            .windows(MARKER.len())
            .any(|bytes| bytes == MARKER.as_bytes())
    );
}

#[tokio::test]
async fn damaged_database_is_left_unchanged_and_does_not_claim_verified_identity() {
    let (dir, database, backup, store, _) = fixture().await;
    store.close().await;
    let admission = admission(&database).await;
    fs::write(&database, b"owned damaged database fixture").unwrap();
    let before = fs::read(&database).unwrap();
    let checked = admission
        .check_backup(
            &backup,
            &dir.path().join("checked"),
            "owner",
            &Versions::fixture(),
            NOW + 1,
        )
        .await
        .unwrap();
    assert!(!checked.source_identity_verified);
    assert_eq!(checked.backup.memories, 1);
    assert_eq!(fs::read(&database).unwrap(), before);
    assert!(
        Store::open(&checked.backup.candidate, "owner")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn readable_source_identity_is_checked_without_migration_or_source_replacement() {
    let (dir, database, backup, store, _) = fixture().await;
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    let exported = store.application_pool();
    store.close().await;
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    drop(exported);
    let admission = admission(&database).await;
    let before = fs::read(&database).unwrap();
    let checked = admission
        .check_backup(
            &backup,
            &dir.path().join("checked"),
            "owner",
            &Versions::fixture(),
            NOW + 1,
        )
        .await
        .unwrap();
    assert!(checked.source_identity_verified);
    assert_eq!(fs::read(&database).unwrap(), before);
    assert_eq!(checked.backup.memories, 1);
    let wrong = dir.path().join("wrong-owner");
    assert!(
        admission
            .check_backup(
                &backup,
                &wrong,
                "other-owner",
                &Versions::fixture(),
                NOW + 1
            )
            .await
            .is_err()
    );
    assert!(!wrong.exists());
    assert_eq!(fs::read(&database).unwrap(), before);
    // A genuine schema-17 source must stay schema 17, even though the backup
    // candidate itself is current. Opening it through Store would migrate it.
    let mut old =
        sqlx::SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&database))
            .await
            .unwrap();
    sqlx::raw_sql("DROP TABLE link_worker_cleanup; ALTER TABLE link_device DROP COLUMN worker_cleanup; DELETE FROM _sqlx_migrations WHERE version=18; PRAGMA user_version=17;")
        .execute(&mut old).await.unwrap();
    old.close().await.unwrap();
    let older_before = fs::read(&database).unwrap();
    let checked = admission
        .check_backup(
            &backup,
            &dir.path().join("older-source"),
            "owner",
            &Versions::fixture(),
            NOW + 1,
        )
        .await
        .unwrap();
    assert!(!checked.source_identity_verified);
    assert_eq!(fs::read(&database).unwrap(), older_before);
}

#[tokio::test]
async fn absent_or_wrong_recovery_material_never_creates_a_new_source_or_key() {
    for fault in [
        "missing-key",
        "wrong-key",
        "wrong-keeper",
        "public-key",
        "missing-restrictions",
    ] {
        let (dir, database, backup, store, _) = fixture().await;
        store.close().await;
        let admission = admission(&database).await;
        fs::remove_file(&database).unwrap();
        let key = suffix(&database, ".backup-key");
        let restrictions = suffix(&database, ".memory-restrictions.json");
        match fault {
            "missing-key" => fs::remove_file(&key).unwrap(),
            "missing-restrictions" => fs::remove_file(&restrictions).unwrap(),
            "public-key" => fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap(),
            _ => {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&fs::read(&key).unwrap()).unwrap();
                if fault == "wrong-key" {
                    value["key"] = serde_json::json!(vec![0_u8; 32]);
                } else {
                    value["keeper"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
                }
                fs::write(&key, serde_json::to_vec(&value).unwrap()).unwrap();
            }
        }
        let before = fs::read(&key).ok();
        let destination = dir.path().join("rejected");
        assert!(
            admission
                .check_backup(
                    &backup,
                    &destination,
                    "owner",
                    &Versions::fixture(),
                    NOW + 1
                )
                .await
                .is_err(),
            "{fault}"
        );
        assert!(!database.exists(), "{fault}");
        assert!(!destination.exists(), "{fault}");
        assert_eq!(fs::read(&key).ok(), before, "{fault}");
    }
}

#[tokio::test]
async fn pending_restore_fence_blocks_open_before_a_missing_database_can_be_created() {
    let (dir, database, backup, store, _) = fixture().await;
    store.close().await;
    let lease = admission(&database).await;
    fs::remove_file(&database).unwrap();
    let restrictions = suffix(&database, ".memory-restrictions.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&restrictions).unwrap()).unwrap();
    value["format"] = serde_json::json!(8);
    value["restore"] =
        serde_json::json!({"generation":uuid::Uuid::new_v4().to_string(),"committed":false});
    fs::write(&restrictions, serde_json::to_vec(&value).unwrap()).unwrap();
    drop(lease);
    let error = Store::open(&database, "owner")
        .await
        .err()
        .expect("pending restore must stop open");
    assert!(matches!(error,Error::Rejected(reason) if reason.contains("incomplete")));
    assert!(!database.exists());
    let lease = admission(&database).await;
    let destination = dir.path().join("not-ready");
    assert!(
        lease
            .check_backup(
                &backup,
                &destination,
                "owner",
                &Versions::fixture(),
                NOW + 1
            )
            .await
            .is_err()
    );
    assert!(!database.exists());
    assert!(!destination.exists());
}

#[tokio::test]
async fn committed_generation_survives_forget_and_old_sqlite_cannot_bypass_it() {
    let (dir, database, backup, store, id) = fixture().await;
    store.close().await;
    let lease = admission(&database).await;
    let older = dir.path().join("older.db");
    fs::copy(&database, &older).unwrap();
    let generation = uuid::Uuid::new_v4().to_string();
    let mut db =
        sqlx::SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&database))
            .await
            .unwrap();
    sqlx::query("INSERT INTO settings VALUES('restore_generation',?)")
        .bind(&generation)
        .execute(&mut db)
        .await
        .unwrap();
    db.close().await.unwrap();
    let restrictions = suffix(&database, ".memory-restrictions.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&restrictions).unwrap()).unwrap();
    value["format"] = serde_json::json!(8);
    value["restore"] = serde_json::json!({"generation":generation,"committed":true});
    fs::write(&restrictions, serde_json::to_vec(&value).unwrap()).unwrap();
    drop(lease);
    let store = Store::open(&database, "owner").await.unwrap();
    forget(&store, &id).await;
    let checked = store
        .check_backup(
            &backup,
            &dir.path().join("checked"),
            &Versions::fixture(),
            NOW + 2,
        )
        .await
        .unwrap();
    assert_eq!(checked.memories, 0);
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(&restrictions).unwrap()).unwrap();
    assert_eq!(value["format"], 8);
    assert_eq!(value["restore"]["generation"], generation);
    store.close().await;
    let lease = admission(&database).await;
    fs::copy(&older, &database).unwrap();
    let before = fs::read(&database).unwrap();
    drop(lease);
    let error = Store::open(&database, "owner")
        .await
        .err()
        .expect("old image must stop open");
    assert!(matches!(error,Error::Rejected(reason) if reason.contains("predates")));
    assert_eq!(fs::read(&database).unwrap(), before);
}
