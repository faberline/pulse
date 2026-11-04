//! Real connection denial and owned shutdown. Vault and native helper are fixtures.
use super::*;
use crate::{
    computer_runtime::tests::{fixture, image_reader},
    server::{Hub, router},
};
use std::future::IntoFuture;

struct FixedVault(Credential);
impl Vault for FixedVault {
    fn load(&self, _: &str) -> Result<Option<Credential>> {
        Ok(Some(self.0.clone()))
    }
    fn pending(&self, _: &str) -> Result<Option<PairClaim>> {
        Ok(None)
    }
    fn save(&self, _: &str, _: &Credential) -> Result<()> {
        panic!("unexpected credential save")
    }
    fn save_pending(&self, _: &str, _: &PairClaim) -> Result<()> {
        panic!("unexpected pairing")
    }
    fn clear_pending(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "explicit isolated Chrome revocation; set PULSE_TEST_BROWSER_DRIVER; no provider calls"]
async fn worker_denial_real_chrome_stops_owned_profile_and_fences_reopen() {
    let driver =
        std::env::var_os("PULSE_TEST_BROWSER_DRIVER").expect("explicit installed driver required");
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let hub = Hub::new(
        pulse_core::Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let now = crate::now();
    let event=hub.core.ingest(pulse_core::Origin::Human,"fixture","browser-task",serde_json::json!({"commands":[pulse_core::Command::Create {id:"browser-task".into(),goal:"Owned revocation fixture".into(),scope:format!("browser-interact:{origin}"),priority:0,requires_device:true,parent:None,dependencies:vec![]}]}),now).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let config = Config {
        server: origin.parse().unwrap(),
        repo: root.path().to_path_buf(),
        data_dir: root.path().join("local"),
    };
    let (journal, lock) = Journal::open(&config.data_dir, &origin, &config.repo)
        .await
        .unwrap();
    let credential = hub
        .redeem(
            PairRequest {
                device: journal.device.clone(),
                role: ClientRole::LocalExecutor,
                code: hub
                    .pairing(ClientRole::LocalExecutor, crate::now())
                    .await
                    .unwrap(),
            },
            crate::now(),
        )
        .await
        .unwrap();
    journal.core.clone().close().await;
    drop(journal);
    drop(lock);
    let directory = config.data_dir.join("browser");
    let runtime = crate::browser_runtime::BrowserRuntime::new(
        driver.into(),
        crate::browser_network::NetworkPolicy::new(Some(&origin)).unwrap(),
        directory.clone(),
    )
    .unwrap()
    .with_interactions(true);
    let mut device = Device::open(config.clone(), Arc::new(FixedVault(credential)))
        .await
        .unwrap();
    device.executor = Some(
        Executor::new(root.path())
            .unwrap()
            .with_browser(Some(runtime.clone())),
    );
    let journal = device.journal.clone();
    let page=router(hub.clone()).route("/",axum::routing::get(||async {axum::response::Html("<!doctype html><title>Owned revocation fixture</title><p>Only owned fixture content</p>")}));
    let server = tokio::spawn(axum::serve(listener, page).into_future());
    let (_commands, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let running = tokio::spawn(device.run(receiver, states));
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let ready = {
                let value = state.borrow();
                value.connected && value.tasks.iter().any(|t| t.id == "browser-task")
            };
            if ready {
                break;
            }
            state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let epoch: i64 = sqlx::query_scalar("SELECT epoch FROM link_device WHERE id=?")
        .bind(&journal.device)
        .fetch_one(&hub.db)
        .await
        .unwrap();
    let binding = crate::browser_session::WorkBinding {
        operation_id: "observe-browser".into(),
        task_id: "browser-task".into(),
        revision: "1".into(),
        device: journal.device.clone(),
        epoch: epoch as u64,
    };
    let observation = runtime
        .inspect(&origin.parse().unwrap(), &binding)
        .await
        .unwrap();
    assert!(matches!(observation.outcome, ReadOutcome::Completed { .. }));
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
    hub.revoke(&journal.device).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(30), running)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(crate::Error::CredentialRejected)));
    assert!(
        journal
            .core
            .worker_access_denied(&journal.device, &hash(&origin).unwrap())
            .await
            .unwrap()
    );
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
    assert!(
        runtime
            .inspect(&origin.parse().unwrap(), &binding)
            .await
            .is_err()
    );
    let (reopened, _lock) = Journal::open(&config.data_dir, &origin, &config.repo)
        .await
        .unwrap();
    assert!(reopened.deliveries().await.is_err());
    let executor = Executor::new(root.path()).unwrap();
    assert!(
        reopened
            .begin_worker_runtime(&executor, &config.data_dir)
            .await
            .is_err()
    );
    let calls: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.db)
        .await
        .unwrap();
    assert_eq!(calls, 0);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn worker_denial_from_the_real_server_waits_for_png_readers_and_reports_failed_owned_cleanup()
{
    for unknown_file in [false, true] {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let hub = Hub::new(
            pulse_core::Store::open(root.path().join("server.db"), "owner")
                .await
                .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let config = Config {
            server: origin.parse().unwrap(),
            repo: root.path().to_path_buf(),
            data_dir: root.path().join("local"),
        };
        let (journal, lock) = Journal::open(&config.data_dir, &origin, &config.repo)
            .await
            .unwrap();
        let credential = hub
            .redeem(
                PairRequest {
                    device: journal.device.clone(),
                    role: ClientRole::LocalExecutor,
                    code: hub
                        .pairing(ClientRole::LocalExecutor, crate::now())
                        .await
                        .unwrap(),
                },
                crate::now(),
            )
            .await
            .unwrap();
        journal.core.clone().close().await;
        drop(journal);
        drop(lock);
        let (helper, runtime) = fixture("cache-hang");
        let mut device = Device::open(config.clone(), Arc::new(FixedVault(credential)))
            .await
            .unwrap();
        device.control_directory = root.path().join("control");
        device.executor = Some(
            Executor::new(root.path())
                .unwrap()
                .with_computer(Some(runtime.clone())),
        );
        let journal = device.journal.clone();
        let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
        let (_commands, receiver) = mpsc::channel(8);
        let (states, mut state) = watch::channel(NativeState::default());
        let running = tokio::spawn(device.run(receiver, states));
        tokio::time::timeout(Duration::from_secs(30), async {
            while !state.borrow().connected {
                state.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        runtime.set_live(true);
        let epoch: i64 = sqlx::query_scalar("SELECT epoch FROM link_device WHERE id=?")
            .bind(&journal.device)
            .fetch_one(&hub.db)
            .await
            .unwrap();
        let binding = pulse_core::ComputerBinding {
            operation_id: "denial-writer".into(),
            task_id: "task".into(),
            revision: 1,
            device: journal.device.clone(),
            epoch: epoch as u64,
        };
        let worker = runtime.clone();
        let writing = tokio::spawn(async move {
            loop {
                match worker.observe(&binding).await {
                    Err(crate::Error::Capacity) => {
                        tokio::time::sleep(Duration::from_millis(5)).await
                    }
                    result => return result,
                }
            }
        });
        let directory = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(path) = std::fs::read_to_string(helper.path().join("cache-hang.cache")) {
                    break std::path::PathBuf::from(path);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let pid: i32 = std::fs::read_to_string(helper.path().join("cache-hang.pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let reader = image_reader(&runtime, "task").await;
        let unknown = directory.join("KEEP");
        if unknown_file {
            std::fs::write(&unknown, b"unknown owned fixture file").unwrap();
        }
        hub.revoke(&journal.device).await.unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let denied = journal
                    .core
                    .worker_access_denied(&journal.device, &hash(&origin).unwrap())
                    .await
                    .unwrap();
                if denied && reader.valid().is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(directory.exists());
        assert!(!running.is_finished());
        assert!(journal.deliveries().await.is_err());
        assert!(journal.view().await.unwrap().messages.is_empty());
        assert!(writing.await.unwrap().is_err());
        let pid = rustix::process::Pid::from_raw(pid).unwrap();
        assert_eq!(
            rustix::process::test_kill_process(pid),
            Err(rustix::io::Errno::SRCH)
        );
        assert_eq!(
            rustix::process::test_kill_process_group(pid),
            Err(rustix::io::Errno::SRCH)
        );
        assert!(
            Journal::open(&config.data_dir, &origin, &config.repo)
                .await
                .is_err()
        );
        drop(reader);
        let result = tokio::time::timeout(Duration::from_secs(30), running)
            .await
            .unwrap()
            .unwrap();
        if unknown_file {
            assert!(matches!(result, Err(crate::Error::CredentialCleanup(_))));
            assert_eq!(
                std::fs::read(&unknown).unwrap(),
                b"unknown owned fixture file"
            );
        } else {
            assert!(matches!(result, Err(crate::Error::CredentialRejected)));
            assert!(!directory.exists());
        }
        assert!(runtime.permissions().await.is_err());
        let (reopened, _lock) = Journal::open(&config.data_dir, &origin, &config.repo)
            .await
            .unwrap();
        assert!(reopened.deliveries().await.is_err());
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn worker_denial_is_not_inferred_from_an_ambiguous_http_error() {
    for status in [
        axum::http::StatusCode::UNAUTHORIZED,
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let config = Config {
            server: origin.parse().unwrap(),
            repo: root.path().to_path_buf(),
            data_dir: root.path().join("local"),
        };
        let (journal, lock) = Journal::open(&config.data_dir, &origin, &config.repo)
            .await
            .unwrap();
        let read = Envelope::new(
            &journal.device,
            1,
            1,
            ServerPayload::Read {
                operation_id: "offline".into(),
                task_id: "task".into(),
                revision: "1".into(),
                operation: ReadOp::Text {
                    path: "README.md".into(),
                },
            },
        )
        .unwrap();
        journal.accept(&read, true, 1, true).await.unwrap();
        let work = journal.take_work().await.unwrap().unwrap();
        journal
            .finish_work(
                &work,
                ReadOutcome::text("KEEP_OFFLINE_RESULT".into(), false),
            )
            .await
            .unwrap();
        let credential = Credential {
            device: journal.device.clone(),
            token: "f".repeat(64),
            role: ClientRole::LocalExecutor,
        };
        journal.core.clone().close().await;
        drop(journal);
        drop(lock);
        let seen = Arc::new(tokio::sync::Notify::new());
        let signal = seen.clone();
        let app = axum::Router::new().route(
            "/v1/devices/connect",
            axum::routing::get(move || {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    status
                }
            }),
        );
        let server = tokio::spawn(axum::serve(listener, app).into_future());
        let device = Device::open(config.clone(), Arc::new(FixedVault(credential)))
            .await
            .unwrap();
        let journal = device.journal.clone();
        let (commands, receiver) = mpsc::channel(8);
        let (states, _state) = watch::channel(NativeState::default());
        let running = tokio::spawn(device.run(receiver, states));
        tokio::time::timeout(Duration::from_secs(30), seen.notified())
            .await
            .unwrap();
        assert!(
            !journal
                .core
                .worker_access_denied(&journal.device, &hash(&origin).unwrap())
                .await
                .unwrap()
        );
        assert!(journal.deliveries().await.unwrap().iter().any(|d|matches!(d,crate::worker_erasure::DeviceDelivery::Data(e) if matches!(&e.payload,DevicePayload::Result{outcome:ReadOutcome::Completed{text,..},..} if text=="KEEP_OFFLINE_RESULT"))));
        commands.send(NativeCommand::Shutdown).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.abort();
        let _ = server.await;
    }
}
