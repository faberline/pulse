//! Credential denial clears the live local copy. Offline/restored copies remain
//! a separate acceptance gate; these tests never claim physical offline erasure.
use super::*;

pub(super) fn assert_erased(path: &std::path::Path, markers: &[&str]) {
    for name in ["device.db", "device.db-wal"] {
        let bytes = match std::fs::read(path.join(name)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => panic!("{name}: {error}"),
        };
        for marker in markers {
            assert!(!bytes.windows(marker.len()).any(|b| b == marker.as_bytes()));
        }
    }
}

#[tokio::test]
async fn denied_memory_get_erases_cache_pending_body_and_blocks_late_views_and_new_input() {
    let f = setup().await;
    say(&f, "記住 已確認：DENIED_CACHE_BODY").await;
    sync(&f).await;
    let old = f.hub.core.memory_view(crate::now()).await.unwrap();
    say(&f, "記住 待送：DENIED_PENDING_BODY").await;
    let pending = f.journal.memory_requests().await.unwrap().remove(0);
    f.hub.revoke(&f.credential.device).await.unwrap();
    assert!(matches!(
        client::sync(&f.config, &f.journal, &f.credential).await,
        Err(crate::Error::CredentialRejected)
    ));
    assert!(!f.journal.memory_access_allowed().await.unwrap());
    assert!(!f.journal.memory_request_active(&pending).await.unwrap());
    assert!(f.journal.memory_requests().await.unwrap().is_empty());
    assert!(f.journal.view().await.unwrap().memories.is_empty());
    assert!(
        f.journal
            .apply_memory_view(&old, crate::now())
            .await
            .is_err()
    );
    assert!(
        f.journal
            .memory_control(
                &uuid::Uuid::new_v4().to_string(),
                "記住 新來源：DENIED_NEW_BODY",
                crate::now()
            )
            .await
            .is_err()
    );
    assert_erased(
        &f.config.data_dir,
        &[
            "DENIED_CACHE_BODY",
            "DENIED_PENDING_BODY",
            "DENIED_NEW_BODY",
        ],
    );
    assert_eq!(f.writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.hub.core.list_memories(crate::now()).await.unwrap().len(),
        1
    );
    assert!(f.hub.core.list_tasks(100).await.unwrap().is_empty());

    // Test a real reopen, with a separate private journal owned by this fixture.
    let path = f._root.path().join("denied-reopen");
    let (journal, lock) = Journal::open_role(
        &path,
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::Chat,
    )
    .await
    .unwrap();
    journal.apply_memory_view(&old, crate::now()).await.unwrap();
    journal
        .memory_control(
            &uuid::Uuid::new_v4().to_string(),
            "記住 待送：DENIED_REOPEN_BODY",
            crate::now(),
        )
        .await
        .unwrap();
    journal.deny_memory_access().await.unwrap();
    journal.deny_memory_access().await.unwrap();
    journal.db.close().await;
    drop(journal);
    drop(lock);
    let (journal, _lock) = Journal::open_role(
        &path,
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::Chat,
    )
    .await
    .unwrap();
    assert!(!journal.memory_access_allowed().await.unwrap());
    assert!(journal.memory_requests().await.unwrap().is_empty());
    assert!(journal.apply_memory_view(&old, crate::now()).await.is_err());
    assert_erased(&path, &["DENIED_CACHE_BODY", "DENIED_REOPEN_BODY"]);
}

#[tokio::test]
async fn denial_between_view_and_post_clears_the_newly_received_copy_without_sending_again() {
    let mut f = setup().await;
    say(&f, "記住 已確認：DENIED_POST_CACHE").await;
    sync(&f).await;
    say(&f, "記住 待送：DENIED_POST_PENDING").await;
    let hub = f.hub.clone();
    let device = f.credential.device.clone();
    let writes = f.writes.clone();
    let app = router(hub.clone()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let hub = hub.clone();
            let device = device.clone();
            let is_view = request.method() == axum::http::Method::GET
                && request.uri().path() == "/v1/memories";
            if request.method() == axum::http::Method::POST {
                writes.fetch_add(1, Ordering::SeqCst);
            }
            async move {
                let response = next.run(request).await;
                if is_view && response.status().is_success() {
                    hub.revoke(&device).await.unwrap();
                }
                response
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    f.config.server =
        url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    f.server.abort();
    f.server = tokio::spawn(axum::serve(listener, app).into_future());
    assert!(matches!(
        client::sync(&f.config, &f.journal, &f.credential).await,
        Err(crate::Error::CredentialRejected)
    ));
    assert!(f.journal.view().await.unwrap().memories.is_empty());
    assert!(f.journal.memory_requests().await.unwrap().is_empty());
    assert!(!f.journal.memory_access_allowed().await.unwrap());
    assert!(matches!(
        client::sync(&f.config, &f.journal, &f.credential).await,
        Err(crate::Error::CredentialRejected)
    ));
    assert_eq!(f.writes.load(Ordering::SeqCst), 2); // Initial save and denied POST only.
    assert_eq!(
        f.hub.core.list_memories(crate::now()).await.unwrap().len(),
        1
    );
    assert_erased(
        &f.config.data_dir,
        &["DENIED_POST_CACHE", "DENIED_POST_PENDING"],
    );
}

#[tokio::test]
async fn authentication_storage_failure_is_503_and_preserves_pending_human_content() {
    use tokio_tungstenite::tungstenite::{Error, client::IntoClientRequest};
    let f = setup().await;
    say(&f, "記住 已確認：PRESERVE_CACHE_503").await;
    sync(&f).await;
    say(&f, "記住 待送：PRESERVE_PENDING_503").await;
    f.hub.core.application_pool().close().await;
    let http = reqwest::Client::new();
    let url = f.config.server.join("v1/memories").unwrap();
    for response in [
        http.get(url.clone())
            .bearer_auth(&f.credential.token)
            .send()
            .await
            .unwrap(),
        http.post(url)
            .bearer_auth(&f.credential.token)
            .json(&f.journal.memory_requests().await.unwrap()[0])
            .send()
            .await
            .unwrap(),
    ] {
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    let mut url = f.config.server.join("v1/devices/connect").unwrap();
    url.set_scheme("ws").unwrap();
    let mut request = url.as_str().into_client_request().unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", f.credential.token).parse().unwrap(),
    );
    assert!(
        matches!(tokio_tungstenite::connect_async(request).await, Err(Error::Http(response)) if response.status().as_u16()==503)
    );
    assert!(matches!(
        client::sync(&f.config, &f.journal, &f.credential).await,
        Err(crate::Error::Rejected(reason)) if reason == "memory sync denied"
    ));
    assert!(f.journal.memory_access_allowed().await.unwrap());
    assert_eq!(
        f.journal.view().await.unwrap().memories[0].body,
        "PRESERVE_CACHE_503"
    );
    assert!(
        matches!(&f.journal.memory_requests().await.unwrap()[0].command, MemoryCommand::Remember{body,..} if body=="PRESERVE_PENDING_503")
    );
}

#[tokio::test]
async fn denied_identity_stays_fenced_when_local_checkpoint_is_busy_then_erases_on_retry() {
    use sqlx::Connection;
    let f = setup().await;
    say(&f, "記住 快取：DENIED_BUSY_CACHE").await;
    sync(&f).await;
    say(&f, "記住 待送：DENIED_BUSY_PENDING").await;
    let mut reader = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(f.config.data_dir.join("device.db")),
    )
    .await
    .unwrap();
    sqlx::query("BEGIN").execute(&mut reader).await.unwrap();
    sqlx::query("SELECT * FROM local_memory_cache")
        .fetch_all(&mut reader)
        .await
        .unwrap();
    f.hub.revoke(&f.credential.device).await.unwrap();
    let start = std::time::Instant::now();
    let error = client::sync(&f.config, &f.journal, &f.credential)
        .await
        .unwrap_err();
    assert!(
        matches!(error, crate::Error::CredentialCleanup(cause) if matches!(*cause, crate::Error::Core(pulse_core::Error::Busy)))
    );
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
    assert!(!f.journal.memory_access_allowed().await.unwrap());
    assert!(f.journal.memory_requests().await.unwrap().is_empty());
    assert!(
        f.journal
            .memory_control(
                &uuid::Uuid::new_v4().to_string(),
                "記住 拒絕：DENIED_BUSY_NEW",
                crate::now()
            )
            .await
            .is_err()
    );
    let timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&f.journal.db)
        .await
        .unwrap();
    assert_eq!(timeout, 5000);
    sqlx::query("ROLLBACK").execute(&mut reader).await.unwrap();
    reader.close().await.unwrap();
    f.journal.deny_memory_access().await.unwrap();
    assert_erased(
        &f.config.data_dir,
        &[
            "DENIED_BUSY_CACHE",
            "DENIED_BUSY_PENDING",
            "DENIED_BUSY_NEW",
        ],
    );
}

#[tokio::test]
async fn ambiguous_denial_or_a_misplaced_marker_never_erases_human_content() {
    let mut f = setup().await;
    say(&f, "記住 快取：PRESERVE_AMBIGUOUS_CACHE").await;
    sync(&f).await;
    say(&f, "記住 待送：PRESERVE_AMBIGUOUS_PENDING").await;
    let cases = [(401, None), (403, None), (403, Some("0")), (503, Some("1"))];
    let selected = Arc::new(AtomicUsize::new(0));
    let next = selected.clone();
    let app = axum::Router::new().route(
        "/v1/memories",
        axum::routing::get(move || {
            let (status, marker) = cases[next.load(Ordering::SeqCst)];
            async move {
                use axum::response::IntoResponse;
                let mut response = axum::http::StatusCode::from_u16(status)
                    .unwrap()
                    .into_response();
                if let Some(marker) = marker {
                    response
                        .headers_mut()
                        .insert("x-pulse-credential-denied", marker.parse().unwrap());
                }
                response
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    f.config.server =
        url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    f.server.abort();
    f.server = tokio::spawn(axum::serve(listener, app).into_future());
    for index in 0..cases.len() {
        selected.store(index, Ordering::SeqCst);
        assert!(
            matches!(client::sync(&f.config,&f.journal,&f.credential).await,Err(crate::Error::Rejected(reason)) if reason=="memory sync denied")
        );
        assert!(f.journal.memory_access_allowed().await.unwrap());
        assert_eq!(
            f.journal.view().await.unwrap().memories[0].body,
            "PRESERVE_AMBIGUOUS_CACHE"
        );
        assert!(
            matches!(&f.journal.memory_requests().await.unwrap()[0].command,MemoryCommand::Remember{body,..} if body=="PRESERVE_AMBIGUOUS_PENDING")
        );
    }
    assert_eq!(f.writes.load(Ordering::SeqCst), 1);
}
