use super::*;
use crate::server::{Hub, router};
use axum::{body::Body, http::StatusCode, response::IntoResponse};
use std::{
    future::IntoFuture,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

static FIXTURES: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
#[path = "pairing_restore_tests.rs"]
mod restore;
#[derive(Default)]
struct TestVault {
    active: Mutex<Option<Credential>>,
    pending: Mutex<Option<PairClaim>>,
    fail_pending: AtomicBool,
    fail_save: AtomicBool,
    fail_clear: AtomicBool,
    pending_saves: AtomicUsize,
    fail_pending_at: AtomicUsize,
}
impl Vault for TestVault {
    fn load(&self, _: &str) -> Result<Option<Credential>> {
        Ok(self.active.lock().unwrap().clone())
    }
    fn pending(&self, _: &str) -> Result<Option<PairClaim>> {
        Ok(self.pending.lock().unwrap().clone())
    }
    fn save(&self, _: &str, value: &Credential) -> Result<()> {
        ensure(
            !self.fail_save.load(Ordering::SeqCst),
            "fixture vault save failed",
        )?;
        *self.active.lock().unwrap() = Some(value.clone());
        Ok(())
    }
    fn save_pending(&self, _: &str, value: &PairClaim) -> Result<()> {
        let save = self.pending_saves.fetch_add(1, Ordering::SeqCst) + 1;
        ensure(
            !self.fail_pending.load(Ordering::SeqCst)
                && self.fail_pending_at.load(Ordering::SeqCst) != save,
            "fixture pending save failed",
        )?;
        *self.pending.lock().unwrap() = Some(value.clone());
        Ok(())
    }
    fn clear_pending(&self, _: &str) -> Result<()> {
        ensure(
            !self.fail_clear.load(Ordering::SeqCst),
            "fixture pending clear failed",
        )?;
        *self.pending.lock().unwrap() = None;
        Ok(())
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    hub: Hub,
    config: Config,
    vault: Arc<TestVault>,
    mode: Arc<AtomicUsize>,
    gets: Arc<AtomicUsize>,
    posts: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
    _permit: tokio::sync::SemaphorePermit<'static>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        let permit = FIXTURES.acquire().await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let hub = Hub::new(
            pulse_core::Store::open(root.path().join("server.db"), "owner")
                .await
                .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            server: Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap(),
            repo: PathBuf::new(),
            data_dir: root.path().join("chat"),
        };
        let mode = Arc::new(AtomicUsize::new(0));
        let gets = Arc::new(AtomicUsize::new(0));
        let posts = Arc::new(AtomicUsize::new(0));
        let (m, g, p) = (mode.clone(), gets.clone(), posts.clone());
        let app=router(hub.clone()).layer(axum::middleware::from_fn(move |request: axum::extract::Request,next:axum::middleware::Next| {
            let (m,g,p)=(m.clone(),g.clone(),p.clone());
            async move {
                let identity=request.uri().path()=="/v1/device-pairings/identity";
                let claim=request.uri().path()=="/v1/device-pairings/claim";
                if identity {g.fetch_add(1,Ordering::SeqCst);}
                if claim {p.fetch_add(1,Ordering::SeqCst);}
                if claim && m.load(Ordering::SeqCst)==5 {return StatusCode::FORBIDDEN.into_response();}
                if identity {
                    match m.load(Ordering::SeqCst) {
                        2=>return StatusCode::SERVICE_UNAVAILABLE.into_response(),
                        3=>return StatusCode::UNAUTHORIZED.into_response(),
                        4=>return axum::Json(serde_json::json!({"device":uuid::Uuid::new_v4().to_string(),"role":"chat"})).into_response(),
                        _=>{}
                    }
                }
                let response=next.run(request).await;
                if claim && response.status()==StatusCode::OK && m.load(Ordering::SeqCst)==1 {
                    // The real server commits. Its response body is then lost.
                    let (parts,_)=response.into_parts();
                    return axum::response::Response::from_parts(parts,Body::from("{"));
                }
                response
            }
        }));
        let server = tokio::spawn(axum::serve(listener, app).into_future());
        Self {
            _root: root,
            hub,
            config,
            vault: Arc::new(TestVault::default()),
            mode,
            gets,
            posts,
            server,
            _permit: permit,
        }
    }
    async fn open(&self) -> Device {
        Device::open_chat(self.config.clone(), self.vault.clone())
            .await
            .unwrap()
    }
    async fn code(&self) -> String {
        self.hub
            .pairing(ClientRole::Chat, crate::now())
            .await
            .unwrap()
    }
    async fn begin(&self, device: &Device, code: String) -> Result<Credential> {
        pairing::begin(
            self.config.clone(),
            device.journal.clone(),
            self.vault.clone(),
            device.account.clone(),
            code,
        )
        .await
    }
    async fn remember(&self, device: &Device) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        device
            .journal
            .memory_control(&id, "記住 偏好：PAIR_OFFLINE_BODY", crate::now())
            .await
            .unwrap();
        id
    }
    async fn count(&self, query: &'static str) -> i64 {
        sqlx::query_scalar(query)
            .fetch_one(&self.hub.core.application_pool())
            .await
            .unwrap()
    }
}
async fn wait_connected(state: &mut watch::Receiver<NativeState>) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if state.borrow().connected {
                return;
            }
            state.changed().await.unwrap();
        }
    })
    .await
    .expect("Rust connector did not recover pairing");
}

#[tokio::test]
async fn failed_pending_vault_write_makes_zero_http_requests_and_preserves_human_input() {
    let f = Fixture::new().await;
    let device = f.open().await;
    let source = f.remember(&device).await;
    f.vault.fail_pending.store(true, Ordering::SeqCst);
    assert!(f.begin(&device, f.code().await).await.is_err());
    assert_eq!(f.gets.load(Ordering::SeqCst), 0);
    assert_eq!(f.posts.load(Ordering::SeqCst), 0);
    assert!(f.vault.pending("").unwrap().is_none());
    assert_eq!(
        device.journal.memory_requests().await.unwrap()[0].source_id,
        source
    );
}

#[tokio::test]
async fn lost_response_recovers_on_connector_restart_after_code_expiry_without_redeeming_again() {
    let f = Fixture::new().await;
    let device = f.open().await;
    f.remember(&device).await;
    f.mode.store(1, Ordering::SeqCst);
    assert!(f.begin(&device, f.code().await).await.is_err());
    let claim = f.vault.pending("").unwrap().unwrap();
    assert!(f.vault.load("").unwrap().is_none());
    assert_eq!(f.count("SELECT count(*) FROM link_device").await, 1);
    sqlx::query("UPDATE link_pairings SET expires=0")
        .execute(&f.hub.core.application_pool())
        .await
        .unwrap();
    device.journal.db.close().await;
    drop(device);
    f.mode.store(0, Ordering::SeqCst);
    let device = f.open().await;
    let (tx, rx) = mpsc::channel(8);
    let (state, mut view) = watch::channel(NativeState::default());
    let running = tokio::spawn(device.run(rx, state));
    wait_connected(&mut view).await;
    assert!(
        hash(&f.vault.load("").unwrap().unwrap()).unwrap() == hash(&claim.credential()).unwrap()
    );
    assert!(f.vault.pending("").unwrap().is_none());
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
    assert_eq!(f.count("SELECT count(*) FROM link_device").await, 1);
    assert_eq!(f.count("SELECT epoch FROM link_device").await, 1); // One actual WebSocket connection.
    assert_eq!(f.count("SELECT expires FROM link_pairings").await, 0);
    let serialized = serde_json::to_string(&*view.borrow()).unwrap();
    assert!(!serialized.contains(&claim.token));
    assert!(!serialized.contains(&claim.code));
    tx.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn vault_save_and_clear_failures_preserve_same_attempt_and_can_be_recovered() {
    let f = Fixture::new().await;
    let device = f.open().await;
    let source = f.remember(&device).await;
    f.vault.fail_save.store(true, Ordering::SeqCst);
    assert!(f.begin(&device, f.code().await).await.is_err());
    let claim = f.vault.pending("").unwrap().unwrap();
    assert!(f.vault.load("").unwrap().is_none());
    f.vault.fail_save.store(false, Ordering::SeqCst);
    f.vault.fail_clear.store(true, Ordering::SeqCst);
    assert!(f.begin(&device, claim.code.clone()).await.is_err());
    assert!(
        hash(&f.vault.load("").unwrap().unwrap()).unwrap() == hash(&claim.credential()).unwrap()
    );
    assert!(f.vault.pending("").unwrap().is_some());
    device.journal.db.close().await;
    drop(device);
    f.vault.fail_clear.store(false, Ordering::SeqCst);
    let device = f.open().await;
    let value = pairing::recover(
        f.config.clone(),
        device.journal.clone(),
        f.vault.clone(),
        device.account.clone(),
        claim,
    )
    .await
    .unwrap();
    device
        .journal
        .activate_memory_pairing(&value)
        .await
        .unwrap();
    assert!(f.vault.pending("").unwrap().is_none());
    assert_eq!(
        device.journal.memory_requests().await.unwrap()[0].source_id,
        source
    );
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
    assert_eq!(f.count("SELECT epoch FROM link_device").await, 0);
}

#[tokio::test]
async fn startup_finishes_an_active_and_pending_vault_record_without_rotating_identity() {
    let f = Fixture::new().await;
    let device = f.open().await;
    f.vault.fail_clear.store(true, Ordering::SeqCst);
    assert!(f.begin(&device, f.code().await).await.is_err());
    let claim = f.vault.pending("").unwrap().unwrap();
    assert!(f.vault.load("").unwrap().is_some());
    device.journal.db.close().await;
    drop(device);
    f.vault.fail_clear.store(false, Ordering::SeqCst);
    let device = f.open().await;
    let (tx, rx) = mpsc::channel(8);
    let (state, mut view) = watch::channel(NativeState::default());
    let running = tokio::spawn(device.run(rx, state));
    wait_connected(&mut view).await;
    assert!(f.vault.pending("").unwrap().is_none());
    assert!(
        hash(&f.vault.load("").unwrap().unwrap()).unwrap() == hash(&claim.credential()).unwrap()
    );
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
    assert_eq!(f.count("SELECT epoch FROM link_device").await, 1);
    tx.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn ambiguous_identity_errors_preserve_pending_and_queue_without_claim_fallback() {
    let f = Fixture::new().await;
    let device = f.open().await;
    let source = f.remember(&device).await;
    let code = f.code().await;
    let mut fingerprint = None;
    for mode in [2, 3, 4, 5] {
        f.mode.store(mode, Ordering::SeqCst);
        assert!(f.begin(&device, code.clone()).await.is_err());
        let current = hash(&f.vault.pending("").unwrap().unwrap()).unwrap();
        if let Some(old) = &fingerprint {
            assert_eq!(&current, old);
        } else {
            fingerprint = Some(current);
        }
        assert_eq!(
            device.journal.memory_requests().await.unwrap()[0].source_id,
            source
        );
        assert_eq!(f.posts.load(Ordering::SeqCst), usize::from(mode == 5));
    }
    assert!(f.vault.load("").unwrap().is_none());
}

#[tokio::test]
async fn rejected_code_clears_only_pending_claim_and_keeps_usable_active_identity() {
    let f = Fixture::new().await;
    let device = f.open().await;
    let old = f.begin(&device, f.code().await).await.unwrap();
    device.journal.activate_memory_pairing(&old).await.unwrap();
    let source = f.remember(&device).await;
    assert!(matches!(
        f.begin(&device, "0".repeat(64)).await,
        Err(crate::Error::PairingRejected)
    ));
    assert!(f.vault.pending("").unwrap().is_none());
    assert!(hash(&f.vault.load("").unwrap().unwrap()).unwrap() == hash(&old).unwrap());
    assert!(device.journal.bind_memory_credential(&old).await.unwrap());
    assert_eq!(
        device.journal.memory_requests().await.unwrap()[0].source_id,
        source
    );
    assert_eq!(f.hub.authenticate(&old.token).await.unwrap(), old.device);
}

#[tokio::test]
async fn revoked_pending_claim_never_restores_identity_or_installs_credential() {
    let f = Fixture::new().await;
    let device = f.open().await;
    f.mode.store(1, Ordering::SeqCst);
    assert!(f.begin(&device, f.code().await).await.is_err());
    let claim = f.vault.pending("").unwrap().unwrap();
    f.hub.revoke(&claim.device).await.unwrap();
    f.mode.store(0, Ordering::SeqCst);
    assert!(matches!(
        f.begin(&device, claim.code).await,
        Err(crate::Error::PairingRejected)
    ));
    assert!(f.vault.load("").unwrap().is_none());
    assert!(f.vault.pending("").unwrap().is_none());
    assert_eq!(f.count("SELECT revoked FROM link_device").await, 1);
}

#[tokio::test]
async fn pairing_endpoints_fail_closed_on_storage_errors_and_disable_caching() {
    let f = Fixture::new().await;
    let device = f.open().await;
    let value = f.begin(&device, f.code().await).await.unwrap();
    let client = reqwest::Client::new();
    let url = f.config.server.join("v1/device-pairings/identity").unwrap();
    let response = client
        .get(url.clone())
        .bearer_auth(&value.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    f.hub.core.application_pool().close().await;
    let response = client
        .get(url)
        .bearer_auth(&value.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        response
            .headers()
            .get("x-pulse-credential-denied")
            .is_none()
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    let claim = PairClaim {
        code: "a".repeat(64),
        token: "b".repeat(64),
        device: value.device,
        role: value.role,
        attempt_id: uuid::Uuid::new_v4().to_string(),
        server_generation: None,
    };
    let response = client
        .post(f.config.server.join("v1/device-pairings/claim").unwrap())
        .json(&claim)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["cache-control"], "no-store");
}
