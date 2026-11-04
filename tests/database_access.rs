#![cfg(unix)]

use pulse_core::{DatabaseMaintenance, Store};
use sqlx::Connection;
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

fn directory() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

async fn maintenance_after_close(database: &Path) -> DatabaseMaintenance {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match DatabaseMaintenance::try_acquire(database) {
            Ok(lease) => return lease,
            Err(error) if Instant::now() >= deadline => panic!("lease not released: {error}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

#[tokio::test]
async fn maintenance_blocks_open_before_creation_and_pool_handles_keep_admission() {
    let dir = directory();
    let database = dir.path().join("db");
    let lease = DatabaseMaintenance::try_acquire(&database).unwrap();
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    assert!(Store::open(&database, "owner").await.is_err());
    assert!(!database.exists());
    drop(lease);

    let first = Store::open(&database, "owner").await.unwrap();
    let second = Store::open(&database, "owner").await.unwrap();
    let exported = second.application_pool();
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    first.close().await;
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    second.close().await;
    // Even a closed exported pool retains its own cooperative admission.
    assert!(exported.is_closed());
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    drop(exported);
    let lease = maintenance_after_close(&database).await;
    assert!(Store::open(&database, "owner").await.is_err());
    drop(lease);
    Store::open(&database, "owner").await.unwrap().close().await;
}

#[tokio::test]
async fn detached_connection_cannot_outlive_database_admission() {
    let dir = directory();
    let database = dir.path().join("db");
    let store = Store::open(&database, "owner").await.unwrap();
    let pool = store.application_pool();
    let mut connection = pool.acquire().await.unwrap().detach();
    store.close().await;
    drop(pool);
    let value: i64 = sqlx::query_scalar("SELECT 42")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, 42);
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    connection.close().await.unwrap();
    drop(maintenance_after_close(&database).await);
}

#[tokio::test]
async fn parent_aliases_share_admission_and_linked_database_files_are_rejected() {
    let dir = directory();
    let parent = dir.path().join("private");
    fs::create_dir(&parent).unwrap();
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
    let alias = dir.path().join("alias");
    symlink(&parent, &alias).unwrap();
    let database = parent.join("db");
    let lease = DatabaseMaintenance::try_acquire(alias.join("../private/db")).unwrap();
    assert_eq!(lease.database(), parent.canonicalize().unwrap().join("db"));
    assert!(Store::open(alias.join("db"), "owner").await.is_err());
    drop(lease);
    let store = Store::open(alias.join("db"), "owner").await.unwrap();
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    store.close().await;
    drop(maintenance_after_close(&database).await);

    let bytes = fs::read(&database).unwrap();
    let soft = parent.join("soft");
    symlink(&database, &soft).unwrap();
    assert!(DatabaseMaintenance::try_acquire(&soft).is_err());
    assert!(Store::open(&soft, "owner").await.is_err());
    let hard = parent.join("hard");
    fs::hard_link(&database, &hard).unwrap();
    for path in [&hard, &database] {
        assert!(DatabaseMaintenance::try_acquire(path).is_err());
        assert!(Store::open(path, "owner").await.is_err());
    }
    assert_eq!(fs::read(&database).unwrap(), bytes);
}

#[tokio::test]
async fn unsafe_admission_files_and_writable_parent_do_not_create_database() {
    for kind in ["symlink", "hardlink", "public", "nonempty", "directory"] {
        let dir = directory();
        let database = dir.path().join("db");
        let lock = dir.path().join("db.use-lock");
        let other = dir.path().join("other");
        fs::write(&other, b"preserve").unwrap();
        fs::set_permissions(&other, fs::Permissions::from_mode(0o600)).unwrap();
        match kind {
            "symlink" => symlink(&other, &lock).unwrap(),
            "hardlink" => fs::hard_link(&other, &lock).unwrap(),
            "directory" => fs::create_dir(&lock).unwrap(),
            _ => {
                fs::write(&lock, if kind == "nonempty" { "x" } else { "" }).unwrap();
                fs::set_permissions(
                    &lock,
                    fs::Permissions::from_mode(if kind == "public" { 0o644 } else { 0o600 }),
                )
                .unwrap();
            }
        }
        assert!(
            DatabaseMaintenance::try_acquire(&database).is_err(),
            "{kind}"
        );
        assert!(Store::open(&database, "owner").await.is_err(), "{kind}");
        assert!(!database.exists(), "{kind}");
        assert_eq!(fs::read(&other).unwrap(), b"preserve", "{kind}");
    }
    let dir = directory();
    let database = dir.path().join("db");
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o777)).unwrap();
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    assert!(Store::open(&database, "owner").await.is_err());
    assert!(!database.exists());
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn killed_process_releases_admission_without_stale_lock_recovery() {
    let dir = directory();
    let database = dir.path().join("db");
    let ready = dir.path().join("ready");
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "database_use_child", "--ignored", "--nocapture"])
            .env("PULSE_DATABASE_USE_CHILD_DIR", dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before ready"
        );
        assert!(Instant::now() < deadline, "child did not become ready");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    // Normal users may still share a live database, for example pair/revoke CLI.
    Store::open(&database, "owner").await.unwrap().close().await;
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    let lease = maintenance_after_close(&database).await;
    assert!(Store::open(&database, "owner").await.is_err());
    drop(lease);
    Store::open(&database, "owner").await.unwrap().close().await;
}

#[tokio::test]
#[ignore = "child fixture invoked by killed_process_releases_admission_without_stale_lock_recovery"]
async fn database_use_child() {
    let dir = std::path::PathBuf::from(std::env::var_os("PULSE_DATABASE_USE_CHILD_DIR").unwrap());
    let store = Store::open(dir.join("db"), "owner").await.unwrap();
    let pool = store.application_pool();
    drop(store);
    fs::write(dir.join("ready"), b"ready").unwrap();
    std::future::pending::<()>().await;
    drop(pool);
}
