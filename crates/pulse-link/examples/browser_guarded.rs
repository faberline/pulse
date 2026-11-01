//! Explicit native evidence. Not run by normal CI. Uses only operator-owned fixtures.
use axum::{
    Router,
    response::{Html, Redirect},
    routing::get,
};
use pulse_link::{Result, browser_network::NetworkPolicy, browser_runtime::BrowserRuntime};
use serde_json::json;
use std::{
    future::IntoFuture,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[tokio::main]
async fn main() -> Result<()> {
    let driver = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .expect("absolute ChromeDriver path"),
    );
    let dir = tempfile::Builder::new()
        .prefix("pulse-browser-guarded-")
        .tempdir()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let forbidden = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}/", listener.local_addr()?);
    let denied = format!("http://{}/", forbidden.local_addr()?);
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    let sink = tokio::spawn(
        axum::serve(
            forbidden,
            Router::new().fallback(get(move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    "FORBIDDEN"
                }
            })),
        )
        .into_future(),
    );
    let marker = format!("pulse-{}", uuid::Uuid::new_v4());
    let html = format!(
        "<!doctype html><meta charset=utf-8><title>Pulse guarded fixture</title><h1>{marker}</h1><p>Static page evidence.</p><img src='{denied}subresource'><iframe src='{denied}frame'></iframe><script>document.body.textContent='SCRIPT RAN';fetch('{denied}script');new RTCPeerConnection({{iceServers:[{{urls:'stun:127.0.0.1:9'}}]}})</script>"
    );
    let redirect = denied.clone();
    let slow_started = Arc::new(tokio::sync::Notify::new());
    let started = slow_started.clone();
    let app = Router::new()
        .route(
            "/slow",
            get(move || {
                let started = started.clone();
                async move {
                    started.notify_one();
                    std::future::pending::<Html<String>>().await
                }
            }),
        )
        .route(
            "/",
            get(move || {
                let html = html.clone();
                async move { Html(html) }
            }),
        )
        .route(
            "/redirect",
            get(move || {
                let url = redirect.clone();
                async move { Redirect::temporary(&url) }
            }),
        )
        .route(
            "/download",
            get(|| async {
                (
                    [
                        ("content-type", "application/octet-stream"),
                        (
                            "content-disposition",
                            "attachment; filename=forbidden-download.txt",
                        ),
                    ],
                    "DO NOT SAVE",
                )
            }),
        );
    let fixture = tokio::spawn(axum::serve(listener, app).into_future());
    let runtime = BrowserRuntime::new(
        driver.clone(),
        NetworkPolicy::new(Some(&origin))?,
        dir.path().join("fixture"),
    )?;
    let snapshot = runtime.read(&origin.parse()?).await?;
    assert!(snapshot.text.contains(&marker));
    assert!(!snapshot.text.contains("SCRIPT RAN"));
    assert_eq!(hits.load(Ordering::SeqCst), 0, "subresource bypassed proxy");
    assert!(
        runtime
            .read(&format!("{origin}redirect").parse()?)
            .await
            .is_err()
    );
    let slow_runtime = runtime.clone();
    let slow_url = format!("{origin}slow").parse()?;
    let read = tokio::spawn(async move { slow_runtime.read(&slow_url).await });
    tokio::time::timeout(std::time::Duration::from_secs(15), slow_started.notified())
        .await
        .expect("slow navigation never started");
    read.abort();
    assert!(matches!(read.await, Err(error) if error.is_cancelled()));
    assert_eq!(hits.load(Ordering::SeqCst), 0, "redirect bypassed proxy");
    assert!(
        runtime
            .read(&format!("{origin}download").parse()?)
            .await
            .is_err()
    );
    assert!(
        std::fs::read_dir(dir.path().join("fixture"))?.all(|e| !e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("session-")),
        "profile was not removed"
    );
    let public = BrowserRuntime::new(driver, NetworkPolicy::new(None)?, dir.path().join("public"))?;
    let page = public.read(&"https://example.com/".parse()?).await?;
    assert_eq!(page.title, "Example Domain");
    assert!(page.text.contains("documentation examples"));
    assert!(public.read(&origin.parse()?).await.is_err());
    std::fs::write(
        dir.path().join("evidence.json"),
        serde_json::to_vec_pretty(&json!({
            "fixture":{"url":snapshot.url,"title":snapshot.title,"text":snapshot.text,"observed_at":snapshot.observed_at,"png_digest":snapshot.screenshot_digest,"browser_version":snapshot.browser_version},
            "public":{"url":page.url,"title":page.title,"text":page.text,"observed_at":page.observed_at,"png_digest":page.screenshot_digest,"browser_version":page.browser_version},
            "forbidden_hits":hits.load(Ordering::SeqCst),"private_redirect_blocked":true,"private_subresources_blocked":true,"page_scripts_disabled":true,"download_rejected":true,"profiles_removed":true,"cancelled_navigation_cleaned":true
        }))?,
    )?;
    fixture.abort();
    sink.abort();
    println!(
        "Native guarded browser passed. Evidence: {}",
        dir.keep().display()
    );
    Ok(())
}
