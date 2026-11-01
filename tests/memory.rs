use pulse_core::*;
use serde_json::{Value, json};
use sqlx::Executor;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tempfile::TempDir;

const NOW: i64 = 1_800_000_000_000;

async fn setup() -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    (dir, store)
}

fn request(command: MemoryCommand, at: i64) -> MemoryRequest {
    MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: at,
        command,
    }
}

fn remember(label: &str, body: &str, at: i64) -> MemoryRequest {
    request(
        MemoryCommand::Remember {
            label: label.into(),
            body: body.into(),
        },
        at,
    )
}

async fn add(store: &Store) -> (MemoryRequest, String) {
    let req = remember("回答用語", "請使用繁體中文。", NOW);
    let result = store.change_memory(Origin::Human, &req, NOW).await.unwrap();
    let MemoryOutcome::Stored { id, revision: 1 } = result else {
        panic!("not stored")
    };
    (req, id)
}

fn manifest(dir: &Path) -> PathBuf {
    dir.join("db.memory-restrictions.json")
}

async fn snapshot(store: &Store, dir: &Path) -> PathBuf {
    let file = dir.join("old-snapshot");
    sqlx::query("VACUUM INTO ?")
        .bind(file.to_str().unwrap())
        .execute(&store.application_pool())
        .await
        .unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    file
}

#[tokio::test]
async fn human_sources_are_bounded_durable_and_replay_cannot_renew() {
    let (dir, store) = setup().await;
    let (req, id) = add(&store).await;
    let records = store.list_memories(NOW).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, id);
    assert_eq!(records[0].source_id, req.source_id);
    assert_eq!(records[0].source_at, NOW);
    assert_eq!(records[0].expires_at, NOW + MEMORY_RETENTION_MS);
    for origin in [Origin::Tool, Origin::External] {
        assert!(store.change_memory(origin, &req, NOW).await.is_err());
    }
    let mut changed = req.clone();
    changed.source_at += 1;
    assert!(
        store
            .change_memory(Origin::Human, &changed, NOW + 1)
            .await
            .is_err()
    );
    changed = req.clone();
    changed.command = MemoryCommand::Remember {
        label: "回答用語".into(),
        body: "different".into(),
    };
    assert!(
        store
            .change_memory(Origin::Human, &changed, NOW + 1)
            .await
            .is_err()
    );
    assert!(
        store
            .change_memory(
                Origin::Human,
                &remember("回答用語", "duplicate label", NOW),
                NOW
            )
            .await
            .is_err()
    );
    assert!(
        store
            .change_memory(Origin::Human, &remember("x", &"文".repeat(342), NOW), NOW)
            .await
            .is_err()
    );
    assert!(
        store
            .change_memory(Origin::Human, &remember("x", "body", NOW + 1), NOW)
            .await
            .is_err()
    );
    assert!(
        store
            .change_memory(Origin::Human, &remember("x", "body", 0), NOW)
            .await
            .is_err()
    );
    store.close().await;
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert!(matches!(
        store
            .change_memory(Origin::Human, &req, NOW + 30)
            .await
            .unwrap(),
        MemoryOutcome::Stored { revision: 1, .. }
    ));
    assert_eq!(
        store.list_memories(NOW + 30).await.unwrap()[0].expires_at,
        NOW + MEMORY_RETENTION_MS
    );
    assert!(store.list_tasks(100).await.unwrap().is_empty());
}

#[tokio::test]
async fn correction_requires_current_revision_and_removes_old_source_evidence() {
    let (dir, store) = setup().await;
    let (old, id) = add(&store).await;
    let correction = request(
        MemoryCommand::Correct {
            id: id.clone(),
            revision: 1,
            label: "回答用語".into(),
            body: "用簡短繁體中文。".into(),
        },
        NOW + 10,
    );
    assert_eq!(
        store
            .change_memory(Origin::Human, &correction, NOW + 10)
            .await
            .unwrap(),
        MemoryOutcome::Stored {
            id: id.clone(),
            revision: 2
        }
    );
    let records = store.list_memories(NOW + 10).await.unwrap();
    assert_eq!(records[0].source_id, correction.source_id);
    assert_eq!(records[0].expires_at, NOW + 10 + MEMORY_RETENTION_MS);
    assert_eq!(records[0].body, "用簡短繁體中文。");
    assert!(matches!(
        store
            .change_memory(Origin::Human, &old, NOW + 11)
            .await
            .unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
    let stale = request(
        MemoryCommand::Forget {
            id: id.clone(),
            revision: 1,
        },
        NOW + 11,
    );
    assert!(
        store
            .change_memory(Origin::Human, &stale, NOW + 11)
            .await
            .is_err()
    );
    let private = fs::read_to_string(manifest(dir.path())).unwrap();
    assert!(!private.contains("回答用語"));
    assert!(!private.contains("繁體"));
    let fingerprint: Option<String> =
        sqlx::query_scalar("SELECT fingerprint FROM memory_events WHERE source_id=?")
            .bind(&old.source_id)
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
    assert!(fingerprint.is_none());
    let payloads = fs::read(dir.path().join("db")).unwrap();
    assert!(
        !payloads
            .windows("請使用繁體中文。".len())
            .any(|w| w == "請使用繁體中文。".as_bytes())
    );
    let wal = fs::metadata(dir.path().join("db-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert_eq!(wal, 0);
}

#[tokio::test]
async fn forget_survives_db_restore_and_old_request_replay() {
    let (dir, store) = setup().await;
    let (old, id) = add(&store).await;
    let backup = snapshot(&store, dir.path()).await;
    let forget = request(
        MemoryCommand::Forget {
            id: id.clone(),
            revision: 1,
        },
        NOW + 1,
    );
    assert_eq!(
        store
            .change_memory(Origin::Human, &forget, NOW + 1)
            .await
            .unwrap(),
        MemoryOutcome::Forgotten { id, revision: 1 }
    );
    assert!(store.list_memories(NOW + 1).await.unwrap().is_empty());
    store.close().await;
    fs::copy(backup, dir.path().join("db")).unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert!(store.list_memories(NOW + 2).await.unwrap().is_empty());
    assert!(matches!(
        store
            .change_memory(Origin::Human, &old, NOW + 2)
            .await
            .unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
    assert!(store.list_memories(NOW + 2).await.unwrap().is_empty());
}

#[tokio::test]
async fn expiry_is_ninety_days_from_source_and_cannot_be_reversed_by_clock_or_restore() {
    let (dir, store) = setup().await;
    let (old, _) = add(&store).await;
    let backup = snapshot(&store, dir.path()).await;
    assert_eq!(
        store
            .list_memories(NOW + MEMORY_RETENTION_MS - 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .list_memories(NOW + MEMORY_RETENTION_MS)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.list_memories(NOW).await.unwrap().is_empty());
    store.close().await;
    fs::copy(backup, dir.path().join("db")).unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert!(matches!(
        store.change_memory(Origin::Human, &old, NOW).await.unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
    assert!(store.list_memories(NOW).await.unwrap().is_empty());
}

#[tokio::test]
async fn durable_restriction_precedes_failed_database_commit() {
    let (dir, store) = setup().await;
    let (old, id) = add(&store).await;
    store.application_pool().execute("CREATE TRIGGER refuse_memory_event BEFORE INSERT ON memory_events BEGIN SELECT RAISE(ABORT,'fixture failure'); END").await.unwrap();
    let correction = request(
        MemoryCommand::Correct {
            id,
            revision: 1,
            label: "new".into(),
            body: "new body".into(),
        },
        NOW + 1,
    );
    assert!(
        store
            .change_memory(Origin::Human, &correction, NOW + 1)
            .await
            .is_err()
    );
    store.close().await;
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert!(store.list_memories(NOW + 2).await.unwrap().is_empty());
    assert!(matches!(
        store
            .change_memory(Origin::Human, &old, NOW + 2)
            .await
            .unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
}

#[tokio::test]
async fn restrictions_missing_stale_wrong_or_private_file_violation_fail_closed() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let (dir, store) = setup().await;
    let initial = fs::read(manifest(dir.path())).unwrap();
    let (_, id) = add(&store).await;
    let forget = request(MemoryCommand::Forget { id, revision: 1 }, NOW);
    store
        .change_memory(Origin::Human, &forget, NOW)
        .await
        .unwrap();
    let latest = fs::read(manifest(dir.path())).unwrap();
    fs::write(manifest(dir.path()), initial).unwrap();
    assert!(store.list_memories(NOW).await.is_err());
    store.close().await;
    assert!(Store::open(dir.path().join("db"), "owner").await.is_err());
    fs::remove_file(manifest(dir.path())).unwrap();
    assert!(Store::open(dir.path().join("db"), "owner").await.is_err());
    fs::write(dir.path().join("evidence"), &latest).unwrap();
    symlink(dir.path().join("evidence"), manifest(dir.path())).unwrap();
    assert!(Store::open(dir.path().join("db"), "owner").await.is_err());
    fs::remove_file(manifest(dir.path())).unwrap();
    fs::write(manifest(dir.path()), &latest).unwrap();
    fs::set_permissions(manifest(dir.path()), fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Store::open(dir.path().join("db"), "owner").await.is_err());
    fs::set_permissions(manifest(dir.path()), fs::Permissions::from_mode(0o600)).unwrap();
    let mut wrong: Value = serde_json::from_slice(&latest).unwrap();
    wrong["keeper"] = json!(uuid::Uuid::new_v4().to_string());
    fs::write(manifest(dir.path()), serde_json::to_vec(&wrong).unwrap()).unwrap();
    assert!(Store::open(dir.path().join("db"), "owner").await.is_err());
    fs::write(manifest(dir.path()), latest).unwrap();
    assert!(Store::open(dir.path().join("db"), "owner").await.is_ok());
}

#[tokio::test]
async fn live_store_checks_restrictions_and_other_store_cannot_lose_updates() {
    let (dir, store) = setup().await;
    let (_, id) = add(&store).await;
    let other = Store::open(dir.path().join("db"), "owner").await.unwrap();
    let correction = request(
        MemoryCommand::Correct {
            id: id.clone(),
            revision: 1,
            label: "label".into(),
            body: "body".into(),
        },
        NOW,
    );
    let forget = request(MemoryCommand::Forget { id, revision: 1 }, NOW);
    let (a, b) = tokio::join!(
        store.change_memory(Origin::Human, &correction, NOW),
        other.change_memory(Origin::Human, &forget, NOW)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    fs::remove_file(manifest(dir.path())).unwrap();
    assert!(
        store
            .change_memory(Origin::Human, &remember("x", "y", NOW), NOW)
            .await
            .is_err()
    );
    assert!(other.list_memories(NOW).await.is_err());
}

#[tokio::test]
async fn busy_file_lock_is_not_stolen_and_release_allows_a_fresh_check() {
    let (dir, store) = setup().await;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.path().join("db.memory-lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let started = std::time::Instant::now();
    assert!(store.list_memories(NOW).await.is_err());
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
    let other = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.path().join("db.memory-lock"))
        .unwrap();
    assert!(other.try_lock().is_err());
    drop(lock);
    assert!(store.list_memories(NOW).await.unwrap().is_empty());
}

#[tokio::test]
async fn capacity_limits_new_knowledge_but_never_prevents_deletion() {
    let (_dir, store) = setup().await;
    let mut first = None;
    for n in 0..100 {
        let result = store
            .change_memory(
                Origin::Human,
                &remember(&format!("label{n}"), "body", NOW),
                NOW,
            )
            .await
            .unwrap();
        if n == 0 {
            first = Some(result);
        }
    }
    assert!(
        store
            .change_memory(Origin::Human, &remember("overflow", "body", NOW), NOW)
            .await
            .is_err()
    );
    // Fill opaque replay evidence without thousands of fsyncs in the fixture.
    for n in 100..10_000 {
        sqlx::query("INSERT INTO memory_events(source_id,memory_id,revision,outcome) VALUES(?,?,1,'redacted')")
            .bind(format!("fixture-{n}")).bind(uuid::Uuid::new_v4().to_string()).execute(&store.application_pool()).await.unwrap();
    }
    let MemoryOutcome::Stored { id, revision } = first.unwrap() else {
        panic!("not stored")
    };
    let forget = request(MemoryCommand::Forget { id, revision }, NOW);
    assert!(matches!(
        store
            .change_memory(Origin::Human, &forget, NOW)
            .await
            .unwrap(),
        MemoryOutcome::Forgotten { .. }
    ));
    assert_eq!(store.list_memories(NOW).await.unwrap().len(), 99);
    assert!(
        store
            .change_memory(Origin::Human, &remember("overflow", "body", NOW), NOW)
            .await
            .is_err()
    );
}

#[test]
#[ignore = "child harness; parent checks each durable boundary"]
fn memory_boundary_child() {
    let database = PathBuf::from(std::env::var_os("PULSE_MEMORY_CRASH_DB").unwrap());
    let mode = std::env::var("PULSE_MEMORY_CRASH_MODE").unwrap();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let store = Store::open(&database, "owner").await.unwrap();
        let (_, id) = add(&store).await;
        if mode == "restriction" {
            store.application_pool().execute("CREATE TRIGGER refuse_memory_event BEFORE INSERT ON memory_events BEGIN SELECT RAISE(ABORT,'fixture failure'); END").await.unwrap();
        }
        let forget = request(MemoryCommand::Forget { id, revision: 1 }, NOW);
        let result = store.change_memory(Origin::Human, &forget, NOW).await;
        assert_eq!(result.is_ok(), mode == "committed");
        // No Store::close, destructor, or orderly WAL flush in this process.
        std::process::exit(73);
    });
}

#[tokio::test]
async fn hard_exit_after_restriction_or_commit_never_restores_source_text() {
    for mode in ["restriction", "committed"] {
        for _ in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "memory_boundary_child", "--ignored"])
                .env("PULSE_MEMORY_CRASH_DB", &path)
                .env("PULSE_MEMORY_CRASH_MODE", mode)
                .output()
                .unwrap();
            assert_eq!(
                child.status.code(),
                Some(73),
                "{mode}: {}",
                String::from_utf8_lossy(&child.stderr)
            );
            let store = Store::open(&path, "owner").await.unwrap();
            assert!(store.list_memories(NOW).await.unwrap().is_empty());
            let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
                .fetch_one(&store.application_pool())
                .await
                .unwrap();
            assert_eq!(integrity, "ok");
        }
    }
}
