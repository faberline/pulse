use super::*;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};

fn private_tempdir(parent: Option<&std::path::Path>) -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let mut builder = tempfile::Builder::new();
    builder.permissions(std::fs::Permissions::from_mode(0o700));
    match parent {
        Some(parent) => builder.tempdir_in(parent).unwrap(),
        None => builder.tempdir().unwrap(),
    }
}

async fn hanging(
    tracker: &ProcessTracker,
    source: &SourceScope,
    resource: Option<tempfile::TempDir>,
) -> OwnedProcess {
    let mut command = Command::new("/bin/sh");
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .args(["-c", "echo ready; exec sleep 60"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = tracker
        .spawn(&mut command, Some(source.clone()), resource)
        .unwrap();
    let out = child.child().stdout.take().unwrap();
    let line = tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(out).lines().next_line(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(line.as_deref(), Some("ready"));
    child
}
#[tokio::test]
async fn owned_process_drop_reaps_before_profile_cleanup_and_source_completion() {
    let tracker = ProcessTracker::default();
    let scope = SourceScope::new("task", "1").unwrap();
    let directory = private_tempdir(None);
    let path = directory.path().to_path_buf();
    std::fs::write(path.join("private-profile"), b"source bytes").unwrap();
    let child = hanging(&tracker, &scope, Some(directory)).await;
    let pid = child.entry.group;
    assert!(path.exists());
    tracker.fence(&scope).unwrap();
    // A successful signal is insufficient while the owner has not reaped.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), tracker.wait_source(&scope))
            .await
            .is_err()
    );
    assert!(path.exists());
    drop(child);
    tracker.wait_source(&scope).await.unwrap();
    assert!(!path.exists());
    assert_eq!(test_kill_process_group(pid), Err(rustix::io::Errno::SRCH));
}
#[tokio::test]
async fn owned_process_fence_keeps_another_task_running_and_blocks_future_spawns() {
    let tracker = ProcessTracker::default();
    let a = SourceScope::new("A", "1").unwrap();
    let b = SourceScope::new("B", "1").unwrap();
    let first = hanging(&tracker, &a, None).await;
    let second = hanging(&tracker, &b, None).await;
    let second_pid = second.entry.group;
    tracker.fence(&a).unwrap();
    drop(first);
    tracker.wait_source(&a).await.unwrap();
    assert!(test_kill_process_group(second_pid).is_ok());
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("must-not-run");
    let mut command = Command::new("/usr/bin/touch");
    command.arg(&marker);
    assert!(tracker.spawn(&mut command, Some(a), None).is_err());
    assert!(!marker.exists());
    tracker.fence(&b).unwrap();
    drop(second);
    tracker.wait_source(&b).await.unwrap();
}
#[tokio::test]
async fn owned_process_normal_exit_releases_profile_and_an_empty_fenced_scope_can_finish() {
    let tracker = ProcessTracker::default();
    let source = SourceScope::new("task", "2").unwrap();
    let directory = private_tempdir(None);
    let path = directory.path().to_path_buf();
    let mut command = Command::new("/usr/bin/true");
    let mut child = tracker
        .spawn(&mut command, Some(source.clone()), Some(directory))
        .unwrap();
    assert!(child.wait().await.unwrap().success());
    assert!(!path.exists());
    tracker.fence(&source).unwrap();
    tracker.wait_source(&source).await.unwrap();
}
#[tokio::test]
async fn owned_process_failed_cleanup_is_never_a_stopped_result() {
    let tracker = ProcessTracker::default();
    let scope = SourceScope::new("task", "1").unwrap();
    let parent = tempfile::tempdir().unwrap();
    let directory = private_tempdir(Some(parent.path()));
    let original = directory.path().to_path_buf();
    std::fs::write(original.join("private-profile"), b"owned source").unwrap();
    let child = hanging(&tracker, &scope, Some(directory)).await;
    let entry = child.entry.clone();
    assert!(!tracker.source_idle(&scope).unwrap());
    // A changed directory identity must preserve the unrelated replacement.
    let moved = parent.path().join("moved-profile");
    std::fs::rename(&original, &moved).unwrap();
    crate::journal::private_dir(&original).unwrap();
    std::fs::write(original.join("human-file"), b"must keep").unwrap();
    tracker.fence(&scope).unwrap();
    drop(child);
    assert!(tracker.wait_source(&scope).await.is_err());
    assert!(!tracker.source_idle(&scope).unwrap());
    assert_eq!(
        std::fs::read(original.join("human-file")).unwrap(),
        b"must keep"
    );
    assert_eq!(
        std::fs::read(moved.join("private-profile")).unwrap(),
        b"owned source"
    );
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if test_kill_process_group(entry.group) == Err(rustix::io::Errno::SRCH) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!tracker.source_idle(&scope).unwrap());
}
#[test]
fn owned_process_source_requires_a_bounded_canonical_revision() {
    for rev in ["0", "01", "-1", "9223372036854775808"] {
        assert!(SourceScope::new("task", rev).is_err());
    }
    assert!(SourceScope::new("", "1").is_err());
    assert!(SourceScope::new(&"x".repeat(129), "1").is_err());
}

#[tokio::test]
async fn owned_process_shutdown_ack_without_exit_cannot_erase_profile_or_prove_stop() {
    use std::future::IntoFuture;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint: url::Url = format!("http://{}/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let app = axum::Router::new().route(
        "/shutdown",
        axum::routing::get(|| async { axum::Json(serde_json::json!({"value":null})) }),
    );
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let tracker = ProcessTracker::default();
    let scope = SourceScope::new("task", "1").unwrap();
    let parent = tempfile::tempdir().unwrap();
    let directory = private_tempdir(Some(parent.path()));
    let path = directory.path().to_path_buf();
    std::fs::write(path.join("private-profile"), b"owned source").unwrap();
    let mut child = hanging(&tracker, &scope, Some(directory)).await;
    let pid = child.entry.group;
    child.driver_shutdown(&endpoint).unwrap();
    assert!(child.shutdown().await.is_err());
    assert!(path.exists());
    assert!(test_kill_process_group(pid).is_ok());
    child.terminate().unwrap();
    assert!(child.wait().await.is_err());
    assert!(path.exists());
    tracker.block(&scope).unwrap();
    assert!(tracker.wait_source(&scope).await.is_err());
    assert_eq!(test_kill_process_group(pid), Err(rustix::io::Errno::SRCH));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn owned_process_aborted_reaper_keeps_profile_and_reports_no_stop_proof() {
    let tracker = ProcessTracker::default();
    let scope = SourceScope::new("task", "1").unwrap();
    let parent = tempfile::tempdir().unwrap();
    let directory = private_tempdir(Some(parent.path()));
    let path = directory.path().to_path_buf();
    std::fs::write(path.join("private-profile"), b"unverified cleanup").unwrap();
    let mut child = hanging(&tracker, &scope, Some(directory)).await;
    let pid = child.entry.group;
    let owner = ReapOwner {
        child: child.child.take().unwrap(),
        entry: child.entry.clone(),
        tracker: tracker.clone(),
        id: child.id,
        resource: std::mem::take(&mut child.resource),
        finished: false,
    };
    child.finished = true;
    drop(child);
    // Abort before polling the reaper, as can happen during runtime shutdown.
    let task = tokio::spawn(async move {
        let _owner = owner;
        std::future::pending::<()>().await
    });
    task.abort();
    assert!(matches!(task.await,Err(error) if error.is_cancelled()));
    tracker.block(&scope).unwrap();
    assert!(tracker.wait_source(&scope).await.is_err());
    assert_eq!(
        std::fs::read(path.join("private-profile")).unwrap(),
        b"unverified cleanup"
    );
    // Drive Tokio's orphan reaper; this is not a positive resource receipt.
    assert!(
        Command::new("/usr/bin/true")
            .status()
            .await
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while test_kill_process_group(pid) != Err(rustix::io::Errno::SRCH) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(path.exists());
}
