use super::*;
use crate::owned_process::ProcessTracker;
use std::process::Stdio;

fn root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn image(path: &Path) -> PathBuf {
    let file = path.join(format!("{}.png", "a".repeat(64)));
    std::fs::write(&file, b"owned image").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    file
}
fn age(caches: &ImageCaches, task: &str) {
    let entries = caches.0.lock().unwrap();
    *entries[&SourceScope::new(task, "1").unwrap()]
        .last_used
        .lock()
        .unwrap() = Instant::now() - Duration::from_secs(121);
}

#[tokio::test]
async fn idle_collection_reclaims_capacity_across_more_than_128_tasks_and_allows_fresh_work() {
    let root = root();
    let caches = ImageCaches::default();
    let processes = ProcessTracker::default();
    for n in 0..130 {
        let task = n.to_string();
        let lease = caches.acquire(root.path(), &task, "1").await.unwrap();
        let file = image(&lease.path);
        drop(lease);
        if n == 0 {
            assert_eq!(
                caches
                    .collect_idle(
                        &processes,
                        &BTreeSet::new(),
                        Instant::now() + Duration::from_secs(119)
                    )
                    .unwrap(),
                0
            );
            assert!(file.exists());
        }
        age(&caches, &task);
        assert_eq!(
            caches
                .collect_idle(&processes, &BTreeSet::new(), Instant::now())
                .unwrap(),
            1
        );
        assert!(!file.exists());
    }
    assert!(caches.0.lock().unwrap().is_empty());
    assert_eq!(
        std::fs::read_dir(root.path().join("artifacts"))
            .unwrap()
            .count(),
        0
    );
    let fresh = caches.acquire(root.path(), "0", "1").await.unwrap();
    fresh.valid().unwrap();
    caches.capacity(root.path(), 2).unwrap();
}

#[tokio::test]
async fn live_reads_pending_readers_and_the_last_release_delay_collection() {
    let root = root();
    let caches = ImageCaches::default();
    let processes = ProcessTracker::default();
    let lease = caches.acquire(root.path(), "A", "1").await.unwrap();
    let file = image(&lease.path);
    age(&caches, "A");
    assert_eq!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .unwrap(),
        0
    );
    assert!(file.exists());
    drop(lease);
    assert_eq!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .unwrap(),
        0
    );

    let cache = caches.0.lock().unwrap()[&SourceScope::new("A", "1").unwrap()].clone();
    let exclusive = cache.io.clone().write_owned().await;
    // Clone before polling lease(), as acquire/existing do before waiting.
    let waiting_cache = cache.clone();
    let waiting = tokio::spawn(async move { ImageCaches::lease(waiting_cache).await.unwrap() });
    drop(cache);
    age(&caches, "A");
    assert_eq!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .unwrap(),
        0
    );
    assert!(file.exists());
    drop(exclusive);
    let reader = waiting.await.unwrap();
    reader.valid().unwrap();
    drop(reader);
    age(&caches, "A");
    assert_eq!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .unwrap(),
        1
    );
    assert!(!file.exists());
}

#[tokio::test]
async fn live_approval_sources_survive_even_when_the_monotonic_idle_deadline_passes() {
    let root = root();
    let caches = ImageCaches::default();
    let processes = ProcessTracker::default();
    let a = caches.acquire(root.path(), "A", "1").await.unwrap();
    let file_a = image(&a.path);
    drop(a);
    let b = caches.acquire(root.path(), "B", "1").await.unwrap();
    let file_b = image(&b.path);
    drop(b);
    age(&caches, "A");
    age(&caches, "B");
    let protected = BTreeSet::from([SourceScope::new("A", "1").unwrap()]);
    assert_eq!(
        caches
            .collect_idle(&processes, &protected, Instant::now())
            .unwrap(),
        1
    );
    assert!(file_a.exists() && !file_b.exists());
    assert_eq!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .unwrap(),
        1
    );
    assert!(!file_a.exists());
}

#[tokio::test]
async fn expiration_never_removes_a_withdrawal_tombstone_or_unknown_resources() {
    let root = root();
    let caches = ImageCaches::default();
    let processes = ProcessTracker::default();
    let a = caches.acquire(root.path(), "A", "1").await.unwrap();
    let file = image(&a.path);
    drop(a);
    age(&caches, "A");
    caches.fence("A", "1").unwrap();
    assert_eq!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .unwrap(),
        0
    );
    assert!(file.exists());
    caches.clear(root.path(), "A", "1").await.unwrap();
    caches
        .collect_idle(&processes, &BTreeSet::new(), Instant::now())
        .unwrap();
    assert!(caches.acquire(root.path(), "A", "1").await.is_err());

    let b = caches.acquire(root.path(), "B", "1").await.unwrap();
    let unknown = b.path.join("keep.txt");
    std::fs::write(&unknown, b"KEEP").unwrap();
    drop(b);
    age(&caches, "B");
    let c = caches.acquire(root.path(), "C", "1").await.unwrap();
    let removable = image(&c.path);
    drop(c);
    age(&caches, "C");
    let legacy = root.path().join("artifacts").join("legacy.png");
    std::fs::write(&legacy, b"KEEP_LEGACY").unwrap();
    assert!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .is_err()
    );
    assert_eq!(std::fs::read(&unknown).unwrap(), b"KEEP");
    assert_eq!(std::fs::read(&legacy).unwrap(), b"KEEP_LEGACY");
    assert!(!removable.exists());
    // A different source's failed cleanup must not leave C bound to its removed path.
    caches
        .acquire(root.path(), "C", "1")
        .await
        .unwrap()
        .valid()
        .unwrap();
}

#[tokio::test]
async fn a_signalled_but_unreaped_source_keeps_its_images_until_verified_stop() {
    let root = root();
    let caches = ImageCaches::default();
    let processes = ProcessTracker::default();
    let scope = SourceScope::new("A", "1").unwrap();
    let lease = caches.acquire(root.path(), "A", "1").await.unwrap();
    let file = image(&lease.path);
    drop(lease);
    age(&caches, "A");
    let mut command = tokio::process::Command::new("/bin/sh");
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .args(["-c", "exec sleep 60"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = processes
        .spawn(&mut command, Some(scope.clone()), None)
        .unwrap();
    child.terminate().unwrap();
    assert!(!processes.source_idle(&scope).unwrap());
    assert_eq!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .unwrap(),
        0
    );
    assert!(file.exists());
    child.wait().await.unwrap();
    assert!(processes.source_idle(&scope).unwrap());
    assert_eq!(
        caches
            .collect_idle(&processes, &BTreeSet::new(), Instant::now())
            .unwrap(),
        1
    );
    assert!(!file.exists());
}
