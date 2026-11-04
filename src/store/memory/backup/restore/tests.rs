use super::*;
use std::{
    process::Stdio,
    time::{Duration, Instant},
};

const NOW: i64 = 1_800_000_000_000;

async fn admit(database: &Path) -> crate::DatabaseMaintenance {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match crate::DatabaseMaintenance::try_acquire(database) {
            Ok(lease) => return lease,
            Err(error) if Instant::now() >= deadline => panic!("{error}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}
async fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
    fixture_at(NOW).await
}
async fn fixture_at(source_at: i64) -> (tempfile::TempDir, PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = dir.path().join("db");
    let store = Store::open(&database, "owner").await.unwrap();
    store
        .change_memory(
            crate::Origin::Human,
            &crate::MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at,
                command: crate::MemoryCommand::Remember {
                    label: "owned crash fixture".into(),
                    body: "OWNED_RESTORE_CRASH_SOURCE".into(),
                },
            },
            NOW,
        )
        .await
        .unwrap();
    let backup = store.maintain(NOW).await.unwrap().backup.unwrap();
    store.close().await;
    drop(admit(&database).await);
    (dir, database, backup)
}
async fn crash(root: &Path, backup: &Path, stage: &str) {
    crash_action(root, backup, stage, NOW + 1, false).await;
}
async fn crash_action(root: &Path, backup: &Path, stage: &str, now: i64, resume: bool) {
    struct OwnedChild(std::process::Child);
    impl Drop for OwnedChild {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "store::memory::backup::restore::tests::restore_child",
            "--ignored",
            "--nocapture",
        ])
        .env("PULSE_OWNED_RESTORE_DIR", root)
        .env("PULSE_OWNED_RESTORE_BACKUP", backup)
        .env("PULSE_OWNED_RESTORE_CRASH", stage)
        .env("PULSE_OWNED_RESTORE_NOW", now.to_string())
        .env("PULSE_OWNED_RESTORE_RESUME", if resume { "1" } else { "0" })
        .env_remove("OPENAI_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let mut child = OwnedChild(child.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "owned restore child timed out: {stage}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(
        status.code(),
        Some(if stage == "live-wal" { 74 } else { 73 }),
        "{stage}"
    );
}

#[tokio::test]
#[ignore = "owned child invoked by restore process crash tests"]
async fn restore_child() {
    let root = PathBuf::from(std::env::var_os("PULSE_OWNED_RESTORE_DIR").unwrap());
    let backup = PathBuf::from(std::env::var_os("PULSE_OWNED_RESTORE_BACKUP").unwrap());
    if std::env::var("PULSE_OWNED_RESTORE_CRASH").as_deref() == Ok("live-wal") {
        let store = Store::open(root.join("db"), "owner").await.unwrap();
        let event = store.ingest(crate::Origin::Human, "owned-wal", "owned-wal-event", serde_json::json!({"commands":[{"kind":"create","id":"wal-only-task","goal":"OWNED_COMMITTED_WAL_ONLY_TASK","scope":"pulse:read","priority":0,"requires_device":false}]}), NOW).await.unwrap();
        assert!(matches!(
            store.route(&event.event_id).await.unwrap(),
            crate::Outcome::Applied { .. }
        ));
        assert!(
            std::fs::metadata(suffix(&root.join("db"), "-wal"))
                .unwrap()
                .len()
                > 32
        );
        std::process::exit(74);
    }
    let lease = admit(&root.join("db")).await;
    let now = std::env::var("PULSE_OWNED_RESTORE_NOW")
        .unwrap()
        .parse()
        .unwrap();
    let result = if std::env::var("PULSE_OWNED_RESTORE_RESUME").as_deref() == Ok("1") {
        lease
            .resume_restore("owner", &Versions::fixture(), now)
            .await
    } else {
        lease
            .restore_backup(&backup, "owner", &Versions::fixture(), now)
            .await
    };
    panic!("expected owned crash, got {result:?}");
}

#[tokio::test]
async fn committed_original_wal_is_imported_and_never_attaches_to_the_new_database() {
    for stage in ["removed-main", "removed-originals", "installed"] {
        let (root, database, backup) = fixture().await;
        crash(root.path(), &backup, "live-wal").await;
        assert!(std::fs::metadata(suffix(&database, "-wal")).unwrap().len() > 32);
        crash(root.path(), &backup, stage).await;
        let lease = admit(&database).await;
        let restored = lease
            .resume_restore("owner", &Versions::fixture(), NOW + 2)
            .await
            .unwrap();
        assert!(restored.current_records_imported);
        drop(lease);
        let store = Store::open(&database, "owner").await.unwrap();
        let task = store.task("wal-only-task").await.unwrap();
        assert_eq!(task.goal, "OWNED_COMMITTED_WAL_ONLY_TASK");
        assert_eq!(task.state, crate::State::Blocked);
        assert!(
            store
                .claim_next(&Versions::fixture(), NOW + 3)
                .await
                .unwrap()
                .is_none()
        );
        store.close().await;
    }
}

#[tokio::test]
async fn paused_restore_expires_memory_before_publication_even_across_refresh_crashes() {
    for installed in ["fenced", "installed", "torn-activation", "activated"] {
        for point in ["refresh-restrictions", "refresh-prepared"] {
            let (root, database, backup) = fixture_at(NOW - crate::MEMORY_RETENTION_MS + 100).await;
            crash(root.path(), &backup, installed).await;
            crash_action(root.path(), &backup, point, NOW + 101, true).await;
            let guard = MemoryGuard::target(&database).unwrap();
            assert!(!guard.read().unwrap().unwrap().removed.is_empty());
            assert!(Store::open(&database, "owner").await.is_err());
            let lease = admit(&database).await;
            lease
                .resume_restore("owner", &Versions::fixture(), NOW + 102)
                .await
                .unwrap();
            drop(lease);
            let store = Store::open(&database, "owner").await.unwrap();
            assert!(store.list_memories(NOW + 102).await.unwrap().is_empty());
            store.close().await;
            let generation = guard.read().unwrap().unwrap().restore.unwrap().generation;
            let dir = files::root(&guard).join(generation);
            for slot in ["candidate", "current", "refresh"] {
                assert!(!dir.join(slot).exists());
            }
        }
    }
}

#[tokio::test]
async fn long_paused_restore_refreshes_day_hold_and_retains_no_expired_plaintext() {
    let (root, database, backup) = fixture_at(NOW - crate::MEMORY_RETENTION_MS + 100).await;
    crash(root.path(), &backup, "installed").await;
    let lease = admit(&database).await;
    let later = NOW + RETENTION + DAY;
    let restored = lease
        .resume_restore("owner", &Versions::fixture(), later)
        .await
        .unwrap();
    assert_eq!(restored.budget_held_through, evidence::day(later).unwrap());
    drop(lease);
    let store = Store::open(&database, "owner").await.unwrap();
    assert!(store.list_memories(later).await.unwrap().is_empty());
    store.maintain(later).await.unwrap();
    store.close().await;
    let guard = MemoryGuard::target(&database).unwrap();
    let dir = files::root(&guard).join(restored.generation);
    let remaining: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(remaining, vec![std::ffi::OsString::from("progress.json")]);
    let lease = admit(&database).await;
    assert!(
        lease
            .resume_restore("owner", &Versions::fixture(), later + 1)
            .await
            .unwrap()
            .already_committed
    );
}

#[tokio::test]
async fn current_only_expired_memory_adds_a_durable_negative_restriction() {
    let (_root, database, backup) = fixture().await;
    let store = Store::open(&database, "owner").await.unwrap();
    let outcome = store
        .change_memory(
            crate::Origin::Human,
            &crate::MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: NOW - crate::MEMORY_RETENTION_MS + 100,
                command: crate::MemoryCommand::Remember {
                    label: "current only expiring".into(),
                    body: "OWNED_CURRENT_ONLY_EXPIRING".into(),
                },
            },
            NOW,
        )
        .await
        .unwrap();
    let crate::MemoryOutcome::Stored { id, .. } = outcome else {
        panic!("memory not stored")
    };
    store.close().await;
    let lease = admit(&database).await;
    let restored = lease
        .restore_backup(&backup, "owner", &Versions::fixture(), NOW + 101)
        .await
        .unwrap();
    assert!(restored.current_records_imported);
    let guard = MemoryGuard::target(&database).unwrap();
    assert_eq!(guard.read().unwrap().unwrap().removed.get(&id), Some(&1));
    drop(lease);
    let store = Store::open(&database, "owner").await.unwrap();
    assert_eq!(store.list_memories(NOW + 101).await.unwrap().len(), 1);
    store.close().await;
}

#[tokio::test]
async fn actual_process_exit_at_each_cutover_boundary_resumes_without_blank_data_or_republication()
{
    for stage in [
        "prepared",
        "fenced",
        "removed-main",
        "removed-originals",
        "installed",
        "torn-activation",
        "activated",
        "published",
    ] {
        let (root, database, backup) = fixture().await;
        crash(root.path(), &backup, stage).await;
        if !matches!(stage, "prepared" | "published") {
            assert!(Store::open(&database, "owner").await.is_err(), "{stage}");
        }
        let lease = admit(&database).await;
        let result = lease
            .resume_restore("owner", &Versions::fixture(), NOW + 2)
            .await
            .unwrap_or_else(|error| panic!("{stage}: {error}"));
        assert_eq!(result.already_committed, stage == "published");
        let repeated = lease
            .resume_restore("owner", &Versions::fixture(), NOW + 3)
            .await
            .unwrap();
        assert!(repeated.already_committed);
        assert_eq!(result.generation, repeated.generation);
        drop(lease);
        let store = Store::open(&database, "owner").await.unwrap();
        let memories = store.list_memories(NOW + 3).await.unwrap();
        assert_eq!(memories.len(), 1, "{stage}");
        assert_eq!(memories[0].body, "OWNED_RESTORE_CRASH_SOURCE");
        assert!(store.list_tasks(100).await.unwrap().is_empty());
        let charged: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(charged, 0);
        store.close().await;
    }
}

#[tokio::test]
async fn changed_candidate_progress_archive_and_unexpected_original_are_preserved_and_refused() {
    for kind in ["candidate", "progress", "archive", "original", "sidecar"] {
        let (root, database, backup) = fixture().await;
        crash(root.path(), &backup, "fenced").await;
        let lease = admit(&database).await;
        let guard = MemoryGuard::target(&database).unwrap();
        let manifest = guard.read().unwrap().unwrap();
        let generation = &manifest.restore.unwrap().generation;
        let dir = files::root(&guard).join(generation);
        let path = match kind {
            "candidate" => dir.join("candidate/candidate.db"),
            "progress" => dir.join("progress.json"),
            "archive" => dir.join("main.evidence"),
            "original" => database.clone(),
            "sidecar" => suffix(&database, "-wal"),
            _ => unreachable!(),
        };
        let mut bytes = if files::exists(&path).unwrap() {
            std::fs::read(&path).unwrap()
        } else {
            Vec::new()
        };
        bytes.extend_from_slice(b"OWNED_UNEXPECTED_CONTENT");
        std::fs::write(&path, &bytes).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let original = std::fs::read(&database).unwrap();
        assert!(
            lease
                .resume_restore("owner", &Versions::fixture(), NOW + 2)
                .await
                .is_err(),
            "{kind}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(std::fs::read(&database).unwrap(), original);
        drop(lease);
        assert!(Store::open(&database, "owner").await.is_err());
    }
}
