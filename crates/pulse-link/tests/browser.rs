use axum::{
    body::Bytes,
    extract::State,
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
};
use pulse_link::browser::FixtureBrowser;
use serde_json::{Value, json};
use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
};
use url::Url;

const ORIGIN: &str = "http://127.0.0.1:43210/";
const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=";
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Normal,
    BadSession,
    BadVersion,
    FailedNavigation,
    ForeignPage,
    LargeResponse,
    Redirect,
    BadElement,
    BadPng,
    DriverError,
    Slow,
    FailedClose,
}
#[derive(Clone)]
struct Driver {
    mode: Mode,
    calls: Arc<Mutex<Vec<(Method, String, Value)>>>,
    current: Arc<Mutex<String>>,
}
async fn command(State(driver): State<Driver>, method: Method, uri: Uri, bytes: Bytes) -> Response {
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    driver
        .calls
        .lock()
        .unwrap()
        .push((method.clone(), uri.path().into(), body.clone()));
    let value = match (method, uri.path()) {
        (Method::POST, "/session") => {
            if driver.mode == Mode::Slow {
                return std::future::pending().await;
            }
            if driver.mode == Mode::Redirect {
                return (StatusCode::FOUND, [("location", "/unwanted")]).into_response();
            }
            json!({"sessionId":if driver.mode==Mode::BadSession {"../other"} else {"owned-session"}, "capabilities":{"browserVersion":if driver.mode==Mode::BadVersion {""} else {"fixture-1"}}})
        }
        (Method::POST, "/session/owned-session/url") => {
            if driver.mode == Mode::FailedNavigation {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            *driver.current.lock().unwrap() = body["url"].as_str().unwrap().into();
            Value::Null
        }
        (Method::GET, "/session/owned-session/url") => json!(if driver.mode == Mode::ForeignPage {
            "https://example.com/".into()
        } else {
            driver.current.lock().unwrap().clone()
        }),
        (Method::GET, "/session/owned-session/title") => json!("Test page"),
        (Method::POST, "/session/owned-session/element") => {
            assert_eq!(body, json!({"using":"css selector","value":"body"}));
            json!({"element-6066-11e4-a52e-4f735466cecf":if driver.mode==Mode::BadElement {"../url"} else {"body-1"}})
        }
        (Method::GET, "/session/owned-session/element/body-1/text") => {
            json!(if driver.mode == Mode::LargeResponse {
                "x".repeat(4 * 1024 * 1024 + 1)
            } else {
                "證據".repeat(10_000)
            })
        }
        (Method::GET, "/session/owned-session/screenshot") => {
            if driver.mode == Mode::DriverError {
                json!({"error":"unknown error","message":"untrusted diagnostic must not enter a receipt"})
            } else {
                json!(if driver.mode == Mode::BadPng {
                    "eA=="
                } else {
                    PNG
                })
            }
        }
        (Method::DELETE, "/session/owned-session") => {
            if driver.mode == Mode::FailedClose {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            Value::Null
        }
        _ => panic!("unexpected browser command: {uri}"),
    };
    axum::Json(json!({"value":value})).into_response()
}
async fn setup(mode: Mode) -> (Driver, Url, tokio::task::JoinHandle<std::io::Result<()>>) {
    let driver = Driver {
        mode,
        calls: Default::default(),
        current: Arc::new(Mutex::new(format!("{ORIGIN}page"))),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let router = axum::Router::new()
        .route("/{*path}", any(command))
        .with_state(driver.clone());
    let task = tokio::spawn(axum::serve(listener, router).into_future());
    (driver, url, task)
}

#[tokio::test]
async fn owned_session_uses_fixed_commands_and_bounds_untrusted_evidence() {
    let (driver, endpoint, server) = setup(Mode::Normal).await;
    let mut browser = FixtureBrowser::open(endpoint, ORIGIN.parse().unwrap())
        .await
        .unwrap();
    let caps = driver.calls.lock().unwrap()[0].2.clone();
    assert_eq!(caps["capabilities"]["alwaysMatch"]["browserName"], "chrome");
    let options = &caps["capabilities"]["alwaysMatch"]["goog:chromeOptions"];
    assert!(options.get("debuggerAddress").is_none());
    assert!(!options.to_string().contains("user-data-dir"));
    let count = driver.calls.lock().unwrap().len();
    assert!(
        browser
            .navigate(&"http://127.0.0.1:43211/outside".parse().unwrap())
            .await
            .is_err()
    );
    assert_eq!(driver.calls.lock().unwrap().len(), count);
    browser
        .navigate(&format!("{ORIGIN}page").parse().unwrap())
        .await
        .unwrap();
    let snapshot = browser.observe().await.unwrap();
    assert_eq!(snapshot.url, format!("{ORIGIN}page"));
    assert_eq!(snapshot.title, "Test page");
    assert_eq!(snapshot.browser_version, "fixture-1");
    assert!(snapshot.text.len() <= 32 * 1024 && snapshot.truncated);
    assert!(snapshot.text.starts_with("證據"));
    assert_eq!(
        snapshot.screenshot_digest,
        pulse_link::protocol::hash_bytes(&snapshot.png)
    );
    assert!(snapshot.observed_at > 0);
    browser.close().await.unwrap();
    let count = driver.calls.lock().unwrap().len();
    browser.close().await.unwrap();
    assert!(browser.observe().await.is_err());
    assert_eq!(driver.calls.lock().unwrap().len(), count);
    server.abort();
}

#[tokio::test]
async fn invalid_endpoints_and_session_ids_never_become_driver_paths() {
    for bad in [
        "https://example.com/",
        "http://localhost:8723/",
        "http://127.0.0.1:0/",
        "http://127.0.0.1:8723/session/",
        "http://secret@127.0.0.1:8723/",
        "http://127.0.0.1:8723/?key=value",
        "http://127.0.0.1:8723/#fragment",
    ] {
        assert!(
            FixtureBrowser::open(bad.parse().unwrap(), ORIGIN.parse().unwrap())
                .await
                .is_err(),
            "{bad}"
        );
        assert!(
            FixtureBrowser::open(ORIGIN.parse().unwrap(), bad.parse().unwrap())
                .await
                .is_err(),
            "{bad}"
        );
    }
    let (driver, endpoint, server) = setup(Mode::BadSession).await;
    assert!(
        FixtureBrowser::open(endpoint, ORIGIN.parse().unwrap())
            .await
            .is_err()
    );
    assert_eq!(driver.calls.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn protocol_failures_stop_the_session_and_allow_only_cleanup() {
    for mode in [
        Mode::FailedNavigation,
        Mode::ForeignPage,
        Mode::LargeResponse,
        Mode::BadElement,
        Mode::BadPng,
        Mode::DriverError,
    ] {
        let (driver, endpoint, server) = setup(mode).await;
        let mut browser = FixtureBrowser::open(endpoint, ORIGIN.parse().unwrap())
            .await
            .unwrap();
        if mode == Mode::FailedNavigation {
            assert!(
                browser
                    .navigate(&format!("{ORIGIN}page").parse().unwrap())
                    .await
                    .is_err()
            );
        } else {
            assert!(browser.observe().await.is_err());
        }
        let count = driver.calls.lock().unwrap().len();
        assert!(browser.observe().await.is_err());
        assert!(
            browser
                .navigate(&format!("{ORIGIN}page").parse().unwrap())
                .await
                .is_err()
        );
        assert_eq!(driver.calls.lock().unwrap().len(), count);
        browser.close().await.unwrap();
        server.abort();
    }
}

#[tokio::test]
async fn cleanup_failure_is_not_reported_as_a_confirmed_close() {
    let (driver, endpoint, server) = setup(Mode::FailedClose).await;
    let mut browser = FixtureBrowser::open(endpoint, ORIGIN.parse().unwrap())
        .await
        .unwrap();
    assert!(browser.close().await.is_err());
    let count = driver.calls.lock().unwrap().len();
    assert!(browser.observe().await.is_err());
    assert_eq!(driver.calls.lock().unwrap().len(), count);
    assert!(browser.close().await.is_err());
    assert_eq!(driver.calls.lock().unwrap().len(), count + 1);
    server.abort();
}

#[tokio::test]
async fn a_silent_driver_has_a_bounded_request_timeout() {
    let (_driver, endpoint, server) = setup(Mode::Slow).await;
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        FixtureBrowser::open(endpoint, ORIGIN.parse().unwrap()),
    )
    .await;
    assert!(result.expect("driver request ignored its timeout").is_err());
    assert!(start.elapsed() < std::time::Duration::from_secs(20));
    server.abort();
}

#[tokio::test]
async fn redirects_are_not_followed_and_missing_versions_close_the_owned_session() {
    for mode in [Mode::Redirect, Mode::BadVersion] {
        let (driver, endpoint, server) = setup(mode).await;
        assert!(
            FixtureBrowser::open(endpoint, ORIGIN.parse().unwrap())
                .await
                .is_err()
        );
        let calls = driver.calls.lock().unwrap();
        assert_eq!(calls.len(), if mode == Mode::BadVersion { 2 } else { 1 });
        if mode == Mode::BadVersion {
            assert_eq!(calls[1].0, Method::DELETE);
        }
        server.abort();
    }
}
