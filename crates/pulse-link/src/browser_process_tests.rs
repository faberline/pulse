//! Explicit real Chrome process/profile acceptance on an owned loopback page.
use super::*;
use crate::browser_network::NetworkPolicy;
use std::{future::IntoFuture, path::Path};

fn profiles(root: &Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("session-")
        })
        .collect()
}
#[tokio::test]
#[ignore = "explicit real Chrome fixture; set PULSE_TEST_BROWSER_DRIVER; no provider calls"]
async fn real_chrome_source_stop_reaps_and_clears_only_its_owned_profile() {
    use axum::{Router, response::Html, routing::get};
    let driver =
        std::env::var_os("PULSE_TEST_BROWSER_DRIVER").expect("explicit installed driver required");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let marker = uuid::Uuid::new_v4().to_string();
    let value = marker.clone();
    let loading = std::sync::Arc::new(tokio::sync::Notify::new());
    let signal = loading.clone();
    let app = Router::new()
        .route(
            "/",
            get(move || {
                let value = value.clone();
                async move {
                    Html(format!(
                        "<!doctype html><title>Owned process fixture</title><p>{value}</p>"
                    ))
                }
            }),
        )
        .route(
            "/slow",
            get(move || {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    std::future::pending::<Html<String>>().await
                }
            }),
        );
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let root = tempfile::tempdir().unwrap();
    let runtime = BrowserRuntime::new(
        driver.into(),
        NetworkPolicy::new(Some(&origin)).unwrap(),
        root.path().join("browser"),
    )
    .unwrap()
    .with_interactions(true);
    let unrelated = root.path().join("browser/keep-human-file.txt");
    std::fs::write(&unrelated, b"not a browser-owned profile").unwrap();
    let binding = |task: &str| WorkBinding {
        operation_id: format!("observe-{task}"),
        task_id: task.into(),
        revision: "1".into(),
        device: uuid::Uuid::new_v4().to_string(),
        epoch: 1,
    };
    let url: Url = origin.parse().unwrap();
    let first = runtime.inspect(&url, &binding("A")).await.unwrap();
    let second = runtime.inspect(&url, &binding("B")).await.unwrap();
    assert!(matches!(first.outcome, ReadOutcome::Completed { .. }));
    let ReadOutcome::Completed {
        observation: Some(second),
        ..
    } = second.outcome
    else {
        panic!("real observation required")
    };
    assert_eq!(profiles(&root.path().join("browser")).len(), 2);
    runtime.invalidate_source("A", "1").await.unwrap();
    assert_eq!(profiles(&root.path().join("browser")).len(), 1);
    {
        let mut sessions = runtime.sessions.lock().await;
        assert_eq!(sessions.len(), 1);
        let session = sessions.get_mut(&second.session_id).unwrap();
        assert_eq!(session.task_id, "B");
        let fresh = session.browser.browser.observe().await.unwrap();
        assert!(fresh.text.contains(&marker));
    }
    assert!(runtime.inspect(&url, &binding("A")).await.is_err());
    runtime.maintain(None, &[]).await.unwrap();
    assert!(profiles(&root.path().join("browser")).is_empty());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(profiles(&root.path().join("browser")).is_empty());
    assert_eq!(
        std::fs::read(&unrelated).unwrap(),
        b"not a browser-owned profile"
    );
    let active = runtime.clone();
    let current = binding("C");
    let slow = url.join("slow").unwrap();
    let read = tokio::spawn(async move { active.read_bound(&slow, &current).await });
    tokio::time::timeout(Duration::from_secs(5), loading.notified())
        .await
        .unwrap();
    assert_eq!(profiles(&root.path().join("browser")).len(), 1);
    read.abort();
    assert!(matches!(read.await,Err(error) if error.is_cancelled()));
    runtime.invalidate_source("C", "1").await.unwrap();
    assert!(profiles(&root.path().join("browser")).is_empty());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(profiles(&root.path().join("browser")).is_empty());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "explicit real Chrome shutdown fixture; set PULSE_TEST_BROWSER_DRIVER; no provider calls"]
async fn real_chrome_executor_shutdown_closes_both_owned_sessions_and_rejects_new_work() {
    use axum::{Router, response::Html, routing::get};
    use std::os::unix::fs::PermissionsExt;
    let driver =
        std::env::var_os("PULSE_TEST_BROWSER_DRIVER").expect("explicit installed driver required");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let marker = uuid::Uuid::new_v4().to_string();
    let value = marker.clone();
    let app = Router::new().route(
        "/",
        get(move || {
            let value = value.clone();
            async move {
                Html(format!(
                    "<!doctype html><title>Owned shutdown fixture</title><p>{value}</p>"
                ))
            }
        }),
    );
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let root = tempfile::tempdir().unwrap();
    let url: Url = origin.parse().unwrap();
    let fresh_dir = root.path().join("fresh-browser");
    let fresh = BrowserRuntime::new(
        driver.clone().into(),
        NetworkPolicy::new(Some(&origin)).unwrap(),
        fresh_dir.clone(),
    )
    .unwrap();
    let snapshot = fresh.read(&url).await.unwrap();
    assert!(snapshot.text.contains(&marker));
    assert_eq!(
        crate::protocol::hash_bytes(&snapshot.png),
        snapshot.screenshot_digest
    );
    assert!(!fresh_dir.join("artifacts").exists());
    fresh.begin_shutdown().unwrap();
    fresh.finish_shutdown().await.unwrap();

    // Old unbound files cannot block fresh evidence or become deletion targets.
    let legacy = root.path().join("browser/artifacts");
    crate::journal::private_dir(&root.path().join("browser")).unwrap();
    crate::journal::private_dir(&legacy).unwrap();
    for n in 0..128 {
        let file = legacy.join(format!("{n:064x}.png"));
        std::fs::write(&file, b"KEEP_LEGACY").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let runtime = BrowserRuntime::new(
        driver.into(),
        NetworkPolicy::new(Some(&origin)).unwrap(),
        root.path().join("browser"),
    )
    .unwrap()
    .with_interactions(true);
    let unrelated = root.path().join("browser/KEEP");
    std::fs::write(&unrelated, b"KEEP").unwrap();
    let binding = |task: &str| WorkBinding {
        operation_id: format!("observe-{task}"),
        task_id: task.into(),
        revision: "1".into(),
        device: uuid::Uuid::new_v4().to_string(),
        epoch: 1,
    };
    for task in ["A", "B"] {
        let read = runtime.inspect(&url, &binding(task)).await.unwrap();
        let image = read
            .image
            .as_ref()
            .expect("original image still handed to journal caller");
        image.artifact.verify(&image.png).unwrap();
        assert!(
            matches!(read.outcome, ReadOutcome::Completed { text, .. } if text.contains(&marker))
        );
    }
    assert_eq!(profiles(&root.path().join("browser")).len(), 2);
    let executor = crate::executor::Executor::new(root.path())
        .unwrap()
        .with_browser(Some(runtime.clone()));
    executor.shutdown().await.unwrap();
    assert!(runtime.processes.stopped());
    assert!(runtime.sessions.lock().await.is_empty());
    assert!(profiles(&root.path().join("browser")).is_empty());
    assert!(runtime.inspect(&url, &binding("C")).await.is_err());
    assert!(runtime.read(&url).await.is_err());
    assert!(matches!(
        executor.execute(ReadOp::Browser { url: origin }).await,
        ReadOutcome::Rejected { .. }
    ));
    executor.shutdown().await.unwrap();
    assert_eq!(std::fs::read(&unrelated).unwrap(), b"KEEP");
    assert_eq!(std::fs::read_dir(&legacy).unwrap().count(), 128);
    for n in 0..128 {
        let file = legacy.join(format!("{n:064x}.png"));
        assert_eq!(std::fs::read(&file).unwrap(), b"KEEP_LEGACY");
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(profiles(&root.path().join("browser")).is_empty());
    server.abort();
    let _ = server.await;
}
