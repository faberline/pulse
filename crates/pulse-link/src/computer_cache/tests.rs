use super::*;
use std::os::unix::fs::symlink;
use std::time::Duration;
fn write(path: &Path, name: &str) -> PathBuf {
    let file = path.join(name);
    std::fs::write(&file, b"owned capture bytes").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    file
}
fn private_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    root
}
fn png_name(n: usize) -> String {
    format!("{n:064x}.png")
}
#[tokio::test]
async fn equal_images_are_scope_isolated_and_erasure_does_not_regrow_or_remove_another_task() {
    let root = private_root();
    let caches = ImageCaches::default();
    let a = caches.acquire(root.path(), "A", "1").await.unwrap();
    let b = caches.acquire(root.path(), "B", "1").await.unwrap();
    assert_ne!(a.path, b.path);
    let path_a = a.path.clone();
    let path_b = b.path.clone();
    let file_a = write(&path_a, &png_name(1));
    let file_b = write(&path_b, &png_name(1));
    write(&path_a, &format!(".partial-{}", uuid::Uuid::new_v4()));
    let unrelated = root.path().join("keep.txt");
    std::fs::write(&unrelated, b"KEEP").unwrap();
    drop(a);
    caches.fence("A", "1").unwrap();
    caches.clear(root.path(), "A", "1").await.unwrap();
    assert!(!file_a.exists() && !path_a.exists());
    assert!(file_b.exists());
    b.valid().unwrap();
    assert!(caches.acquire(root.path(), "A", "1").await.is_err());
    caches.clear(root.path(), "A", "1").await.unwrap();
    assert!(!path_a.exists());
    assert_eq!(std::fs::read(unrelated).unwrap(), b"KEEP");
    drop(b);
    caches.fence("B", "1").unwrap();
    caches.clear(root.path(), "B", "1").await.unwrap();
    assert!(!path_b.exists());
}
#[tokio::test]
async fn cancellation_of_the_parent_does_not_release_an_active_blocking_image_reader() {
    let root = private_root();
    let caches = ImageCaches::default();
    let lease = caches.acquire(root.path(), "A", "1").await.unwrap();
    let file = write(&lease.path, &png_name(1));
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let (done, finished) = tokio::sync::oneshot::channel();
    let parent = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let guard = lease;
            started.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            drop(guard);
            let _ = done.send(());
        })
        .await
        .unwrap();
    });
    ready.await.unwrap();
    parent.abort();
    assert!(parent.await.unwrap_err().is_cancelled());
    caches.fence("A", "1").unwrap();
    let clear = caches.clear(root.path(), "A", "1");
    tokio::pin!(clear);
    tokio::select! {
        _=&mut clear=>panic!("cleanup returned before image reader stopped"),
        _=tokio::time::sleep(Duration::from_millis(60))=>{},
    }
    assert!(file.exists());
    release.send(()).unwrap();
    finished.await.unwrap();
    clear.await.unwrap();
    assert!(!file.exists());
}
#[tokio::test]
async fn replacing_a_directory_preserves_both_the_replacement_and_unverified_original() {
    let root = private_root();
    let caches = ImageCaches::default();
    let lease = caches.acquire(root.path(), "A", "1").await.unwrap();
    let path = lease.path.clone();
    write(&path, &png_name(1));
    drop(lease);
    let moved = root.path().join("moved-owned-directory");
    std::fs::rename(&path, &moved).unwrap();
    private_dir(&path).unwrap();
    std::fs::write(path.join("keep.txt"), b"KEEP_REPLACEMENT").unwrap();
    caches.fence("A", "1").unwrap();
    assert!(caches.clear(root.path(), "A", "1").await.is_err());
    assert_eq!(
        std::fs::read(path.join("keep.txt")).unwrap(),
        b"KEEP_REPLACEMENT"
    );
    assert!(moved.join(png_name(1)).exists());
    assert!(caches.clear(root.path(), "A", "1").await.is_err());
}
#[tokio::test]
async fn legacy_files_and_unknown_old_scopes_are_kept_without_claiming_erasure() {
    let root = private_root();
    let caches = ImageCaches::default();
    let lease = caches.acquire(root.path(), "A", "1").await.unwrap();
    let path = lease.path.clone();
    write(&path, &png_name(1));
    drop(lease);
    let artifacts = root.path().join("artifacts");
    let legacy = write(&artifacts, &png_name(2));
    let old = artifacts.join(format!("{}old-runtime", prefix("A", "1").unwrap()));
    private_dir(&old).unwrap();
    write(&old, &png_name(3));
    caches.fence("A", "1").unwrap();
    assert!(caches.clear(root.path(), "A", "1").await.is_err());
    assert!(legacy.exists() && path.exists() && old.exists());
    std::fs::remove_file(&legacy).unwrap();
    assert!(caches.clear(root.path(), "A", "1").await.is_err());
    assert!(old.join(png_name(3)).exists());
}
#[tokio::test]
async fn the_128_file_capacity_remains_global_across_scopes() {
    let root = private_root();
    let caches = ImageCaches::default();
    let a = caches.acquire(root.path(), "A", "1").await.unwrap();
    let b = caches.acquire(root.path(), "B", "1").await.unwrap();
    for n in 0..64 {
        write(&a.path, &png_name(n));
        write(&b.path, &png_name(n));
    }
    assert!(caches.capacity(root.path(), 1).is_err());
    std::fs::remove_file(a.path.join(png_name(1))).unwrap();
    caches.capacity(root.path(), 1).unwrap();
    assert!(caches.capacity(root.path(), 2).is_err());
}
#[tokio::test]
async fn unverified_entries_are_not_removed_or_followed() {
    for kind in ["symlink", "hardlink", "mode", "unknown"] {
        let root = private_root();
        let caches = ImageCaches::default();
        let lease = caches.acquire(root.path(), "A", "1").await.unwrap();
        let path = lease.path.clone();
        let outside = root.path().join("KEEP_OUTSIDE");
        std::fs::write(&outside, b"KEEP").unwrap();
        let file = path.join(png_name(1));
        match kind {
            "symlink" => symlink(&outside, &file).unwrap(),
            "hardlink" => {
                write(&path, &png_name(1));
                std::fs::hard_link(&file, root.path().join("ALIAS")).unwrap();
            }
            "mode" => {
                write(&path, &png_name(1));
                std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
            }
            _ => {
                std::fs::write(path.join("unrelated.txt"), b"KEEP").unwrap();
            }
        }
        drop(lease);
        caches.fence("A", "1").unwrap();
        assert!(caches.clear(root.path(), "A", "1").await.is_err(), "{kind}");
        assert!(path.exists());
        assert_eq!(std::fs::read(outside).unwrap(), b"KEEP");
    }
}

#[tokio::test]
async fn shutdown_waits_for_readers_and_requires_global_process_stop_before_removing_owned_images()
{
    let root = private_root();
    let caches = ImageCaches::default();
    let processes = crate::owned_process::ProcessTracker::default();
    let lease = caches.acquire(root.path(), "A", "1").await.unwrap();
    let file = write(&lease.path, &png_name(1));
    caches.begin_shutdown().unwrap();
    assert!(lease.valid().is_err());
    assert!(caches.acquire(root.path(), "new", "1").await.is_err());
    assert!(caches.existing("A", "1").await.is_err());
    assert!(caches.finish_shutdown(&processes).await.is_err());
    assert!(file.exists());
    processes.shutdown().await.unwrap();
    let clear = caches.finish_shutdown(&processes);
    tokio::pin!(clear);
    tokio::select! {
        _=&mut clear=>panic!("shutdown cleared an active image reader"),
        _=tokio::time::sleep(Duration::from_millis(60))=>{},
    }
    assert!(file.exists());
    drop(lease);
    clear.await.unwrap();
    assert!(!file.exists());
    caches.finish_shutdown(&processes).await.unwrap();
}

#[tokio::test]
async fn shutdown_keeps_unverified_files_and_repeats_the_failure_without_losing_ownership() {
    let root = private_root();
    let caches = ImageCaches::default();
    let processes = crate::owned_process::ProcessTracker::default();
    let lease = caches.acquire(root.path(), "A", "1").await.unwrap();
    let unknown = lease.path.join("KEEP");
    std::fs::write(&unknown, b"KEEP").unwrap();
    drop(lease);
    caches.begin_shutdown().unwrap();
    processes.shutdown().await.unwrap();
    for _ in 0..2 {
        assert!(caches.finish_shutdown(&processes).await.is_err());
        assert_eq!(std::fs::read(&unknown).unwrap(), b"KEEP");
    }
}
