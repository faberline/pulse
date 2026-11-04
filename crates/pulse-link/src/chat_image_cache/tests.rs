use super::*;
mod retention;
use crate::{client::Config, journal::DeviceLock};
use axum::{Router, response::IntoResponse, routing::get};
use base64::{Engine, engine::general_purpose::STANDARD};
use pulse_core::{Memory, MemoryView, TaskMemorySource};
use std::{
    future::IntoFuture,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

static FIXTURES: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
struct Fixture {
    _permit: tokio::sync::SemaphorePermit<'static>,
    root: tempfile::TempDir,
    journal: Journal,
    lock: Option<DeviceLock>,
    config: Config,
    credential: Credential,
    memories: Arc<std::sync::Mutex<Vec<Memory>>>,
    keeper: String,
    gate: Arc<tokio::sync::Semaphore>,
    entered: Arc<tokio::sync::Notify>,
    gets: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
    sequence: u64,
    artifacts: std::collections::BTreeMap<String, ArtifactRef>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
fn png() -> Vec<u8> {
    STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=").unwrap()
}
fn memory(label: &str) -> Memory {
    let now = crate::now();
    Memory {
        id: uuid::Uuid::new_v4().to_string(),
        revision: 1,
        label: label.into(),
        body: format!("PRIVATE_SOURCE_{label}"),
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: now,
        expires_at: now + pulse_core::MEMORY_RETENTION_MS,
    }
}
fn source(m: &Memory) -> TaskMemorySource {
    TaskMemorySource {
        memory_id: m.id.clone(),
        revision: m.revision,
        source_id: m.source_id.clone(),
        expires_at: m.expires_at,
    }
}
fn picture(operation: &str) -> CapturedImage {
    let png = png();
    CapturedImage {
        artifact: ArtifactRef {
            task_id: operation.into(),
            revision: "1".into(),
            digest: hash_bytes(&png),
            length: png.len() as u32,
            url: "https://example.com/".into(),
            computer: None,
            title: format!("PRIVATE_IMAGE_{operation}"),
            observed_at: crate::now(),
            width: 1,
            height: 1,
        },
        png,
    }
}
impl Fixture {
    async fn new(hold: bool) -> Self {
        let permit = FIXTURES.acquire().await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            server: format!("http://{}/", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
            repo: root.path().into(),
            data_dir: root.path().join("chat"),
        };
        let (journal, lock) = Journal::open_role(
            &config.data_dir,
            config.server.as_str(),
            &config.repo,
            ClientRole::Chat,
        )
        .await
        .unwrap();
        let credential = Credential {
            device: journal.device.clone(),
            token: uuid::Uuid::new_v4().to_string(),
            role: ClientRole::Chat,
        };
        assert!(journal.bind_memory_credential(&credential).await.unwrap());
        let memories = Arc::new(std::sync::Mutex::new(vec![memory("A"), memory("B")]));
        let keeper = uuid::Uuid::new_v4().to_string();
        let gate = Arc::new(tokio::sync::Semaphore::new(usize::from(!hold)));
        let entered = Arc::new(tokio::sync::Notify::new());
        let gets = Arc::new(AtomicUsize::new(0));
        let (g, e, c) = (gate.clone(), entered.clone(), gets.clone());
        let (m, k) = (memories.clone(), keeper.clone());
        let app = Router::new()
            .route(
                "/v1/artifacts/{operation}/{digest}",
                get(move || {
                    let (g, e, c) = (g.clone(), e.clone(), c.clone());
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        e.notify_one();
                        let _permit = g.acquire().await.unwrap();
                        ([("content-type", "image/png")], png()).into_response()
                    }
                }),
            )
            .route(
                "/v1/memories",
                get(move || {
                    let (m, k) = (m.clone(), k.clone());
                    async move {
                        axum::Json(MemoryView {
                            keeper: k,
                            epoch: 2,
                            memories: m.lock().unwrap().clone(),
                            redacted_sources: vec![],
                        })
                    }
                }),
            );
        let server = tokio::spawn(axum::serve(listener, app).into_future());
        let f = Self {
            _permit: permit,
            root,
            journal,
            lock: Some(lock),
            config,
            credential,
            memories,
            keeper,
            gate,
            entered,
            gets,
            server,
            sequence: 0,
            artifacts: Default::default(),
        };
        f.refresh().await;
        f
    }
    async fn refresh(&self) {
        let view = MemoryView {
            keeper: self.keeper.clone(),
            epoch: 2,
            memories: self.memories.lock().unwrap().clone(),
            redacted_sources: vec![],
        };
        self.journal
            .apply_memory_view_for(&view, &self.credential, crate::now())
            .await
            .unwrap();
    }
    async fn snapshot(&mut self, operation: &str, index: usize) -> Envelope<ServerPayload> {
        self.sequence += 1;
        let artifact = self
            .artifacts
            .entry(operation.into())
            .or_insert_with(|| picture(operation).artifact)
            .clone();
        let m = self.memories.lock().unwrap()[index].clone();
        let frame = Envelope::new(
            &self.journal.device,
            self.sequence,
            1,
            ServerPayload::DerivedSnapshot {
                tasks: vec![],
                messages: vec![ChatMessage {
                    memory_sources: Some(vec![source(&m)]),
                    id: operation.into(),
                    body: "Image fixture".into(),
                    approval: None,
                    artifact: Some(artifact),
                    artifact_operation_id: None,
                }],
                synced_at: crate::now(),
                local: LocalStatus::default(),
                sources: vec![source(&m)],
            },
        )
        .unwrap();
        self.journal.accept(&frame, true, 1, true).await.unwrap();
        frame
    }
    async fn load(&self, operation: &str) -> LocalArtifact {
        self.journal
            .load_artifact(
                &self.config,
                &self.credential,
                operation,
                &hash_bytes(&png()),
            )
            .await
            .unwrap()
    }
    async fn forget(&self, label: &str) -> crate::Result<bool> {
        self.journal
            .memory_control(
                &uuid::Uuid::new_v4().to_string(),
                &format!("忘記記憶 {label}"),
                crate::now(),
            )
            .await
    }
    async fn images(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM local_meta WHERE key GLOB 'image:*'")
            .fetch_one(&self.journal.db)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn withdrawal_removes_owned_png_and_metadata_without_erasing_equal_pixel_b() {
    let mut f = Fixture::new(false).await;
    f.snapshot("A", 0).await;
    let a = f.load("A").await;
    f.snapshot("B", 1).await;
    let b = f.load("B").await;
    assert_eq!(a.path, b.path);
    assert_eq!(f.gets.load(Ordering::SeqCst), 1);
    f.forget("A").await.unwrap();
    assert_eq!(std::fs::read(&b.path).unwrap(), png());
    assert_eq!(f.images().await, 1);
    let all: Vec<String> =
        sqlx::query_scalar("SELECT value FROM local_meta WHERE key GLOB 'image:*'")
            .fetch_all(&f.journal.db)
            .await
            .unwrap();
    assert!(!all.join("").contains("PRIVATE_IMAGE_A"));
    let database = std::fs::read(f.config.data_dir.join("device.db")).unwrap();
    assert!(
        !database
            .windows(b"PRIVATE_IMAGE_A".len())
            .any(|w| w == b"PRIVATE_IMAGE_A")
    );
    if let Ok(wal) = std::fs::read(f.config.data_dir.join("device.db-wal")) {
        assert!(
            !wal.windows(b"PRIVATE_IMAGE_A".len())
                .any(|w| w == b"PRIVATE_IMAGE_A")
        );
    }
    assert_eq!(
        f.journal.view().await.unwrap().artifacts[0].operation_id,
        "B"
    );
    f.forget("B").await.unwrap();
    assert!(!std::path::Path::new(&b.path).exists());
    assert_eq!(f.images().await, 0);
}

#[tokio::test]
async fn full_reference_capacity_clears_one_source_and_keeps_the_other_shared_file() {
    let mut f = Fixture::new(false).await;
    f.snapshot("A", 0).await;
    f.load("A").await;
    f.snapshot("B", 1).await;
    let image = f.load("B").await;
    // The bounded journal can retain many completed operations with equal pixels.
    sqlx::query("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<499) INSERT INTO local_meta SELECT 'image:bulk-'||substr(m.key,7)||'-'||x,json_set(m.value,'$.operation_id','bulk-'||substr(m.key,7)||'-'||x) FROM local_meta m,n WHERE m.key IN ('image:A','image:B')")
        .execute(&f.journal.db).await.unwrap();
    assert_eq!(f.images().await, 1000);
    f.forget("A").await.unwrap();
    assert_eq!(f.images().await, 500);
    assert_eq!(std::fs::read(&image.path).unwrap(), png());
    f.forget("B").await.unwrap();
    assert_eq!(f.images().await, 0);
    assert!(!std::path::Path::new(&image.path).exists());
}

#[tokio::test]
async fn late_download_after_withdrawal_or_identity_denial_never_writes_png() {
    for deny in [false, true] {
        let mut f = Fixture::new(true).await;
        f.snapshot("A", 0).await;
        let (j, c, cred) = (f.journal.clone(), f.config.clone(), f.credential.clone());
        let loading =
            tokio::spawn(async move { j.load_artifact(&c, &cred, "A", &hash_bytes(&png())).await });
        f.entered.notified().await;
        if deny {
            assert!(
                f.journal
                    .deny_memory_credential(&f.credential)
                    .await
                    .unwrap()
            );
        } else {
            f.forget("A").await.unwrap();
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), loading)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        f.gate.add_permits(1);
        assert_eq!(f.images().await, 0);
        assert_eq!(
            std::fs::read_dir(f.config.data_dir.join("artifacts"))
                .unwrap()
                .count(),
            0
        );
    }
}

#[tokio::test]
async fn server_erasure_waits_for_fresh_sources_and_keeps_independent_images() {
    let mut f = Fixture::new(false).await;
    f.snapshot("A", 0).await;
    let a = f.load("A").await;
    f.snapshot("B", 1).await;
    f.load("B").await;
    // A's file belongs to an earlier projection than this bodyless erasure.
    let current = f.snapshot("A", 0).await;
    let header =
        pulse_core::ErasedProjection::from_value(&serde_json::to_value(current).unwrap()).unwrap();
    f.memories.lock().unwrap().remove(0);
    assert!(f.journal.accept_erased_projection(&header).await.is_err());
    crate::memory_chat::client::sync(&f.config, &f.journal, &f.credential)
        .await
        .unwrap();
    assert!(!f.journal.accept_erased_projection(&header).await.unwrap());
    assert_eq!(f.images().await, 1);
    assert_eq!(std::fs::read(&a.path).unwrap(), png());
    f.snapshot("B", 0).await;
    assert_eq!(
        f.journal.view().await.unwrap().artifacts[0].operation_id,
        "B"
    );
    assert!(f.journal.accept_erased_projection(&header).await.is_err());
    crate::memory_chat::client::sync(&f.config, &f.journal, &f.credential)
        .await
        .unwrap();
    assert!(!f.journal.accept_erased_projection(&header).await.unwrap());
    assert_eq!(f.images().await, 1);
    // The same immutable header can later concern another source in that projection.
    f.memories.lock().unwrap().clear();
    assert!(f.journal.accept_erased_projection(&header).await.is_err());
    crate::memory_chat::client::sync(&f.config, &f.journal, &f.credential)
        .await
        .unwrap();
    assert!(!f.journal.accept_erased_projection(&header).await.unwrap());
    assert_eq!(f.images().await, 0);
    assert!(!std::path::Path::new(&a.path).exists());
}

#[tokio::test]
async fn modified_or_replaced_files_keep_the_erasure_receipt_for_a_safe_retry() {
    for replacement in ["symlink", "hardlink", "bytes", "directory"] {
        let mut f = Fixture::new(false).await;
        f.snapshot("A", 0).await;
        let a = f.load("A").await;
        let path = std::path::PathBuf::from(&a.path);
        let keep = f.root.path().join("KEEP");
        std::fs::write(&keep, "KEEP_USER_FILE").unwrap();
        let original = f.root.path().join("original");
        match replacement {
            "symlink" => {
                std::fs::rename(&path, &original).unwrap();
                std::os::unix::fs::symlink(&keep, &path).unwrap();
            }
            "hardlink" => std::fs::hard_link(&path, &original).unwrap(),
            "bytes" => std::fs::write(&path, "CHANGED").unwrap(),
            _ => {
                std::fs::rename(path.parent().unwrap(), &original).unwrap();
                crate::journal::private_dir(path.parent().unwrap()).unwrap();
                std::fs::write(&path, "CHANGED").unwrap();
            }
        }
        assert!(f.forget("A").await.is_err(), "{replacement}");
        assert_eq!(f.images().await, 1);
        assert_eq!(std::fs::read(&keep).unwrap(), b"KEEP_USER_FILE");
        let receipt: String =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='image:A'")
                .fetch_one(&f.journal.db)
                .await
                .unwrap();
        assert!(!receipt.contains("PRIVATE_IMAGE"));
        assert!(receipt.contains("erased"));
        match replacement {
            "symlink" => {
                std::fs::remove_file(&path).unwrap();
                std::fs::rename(&original, &path).unwrap();
            }
            "hardlink" => std::fs::remove_file(&original).unwrap(),
            "bytes" => std::fs::write(&path, png()).unwrap(),
            _ => {
                std::fs::remove_file(&path).unwrap();
                std::fs::remove_dir(path.parent().unwrap()).unwrap();
                std::fs::rename(&original, path.parent().unwrap()).unwrap();
            }
        }
        f.journal.flush_memory().await.unwrap();
        assert!(!path.exists());
        assert_eq!(f.images().await, 0);
    }
}

#[tokio::test]
async fn abandoned_private_write_is_removed_after_reopen_before_any_preview() {
    let mut f = Fixture::new(false).await;
    f.snapshot("A", 0).await;
    let image = picture("A");
    let directory = Directory::open(&f.journal).unwrap();
    let mut file = directory.open_file(&image.artifact.digest, true).unwrap();
    let claim = directory.claim(&file, &image.artifact).unwrap();
    let path = directory.path(&image.artifact.digest);
    let entry = Entry {
        image: LocalArtifact {
            operation_id: "A".into(),
            artifact: image.artifact,
            path: path.to_str().unwrap().into(),
        },
        memory_sources: vec![],
        sequence: "1".into(),
        file: claim,
        writing: true,
    };
    sqlx::query("INSERT INTO local_meta VALUES('image:A',?)")
        .bind(serde_json::to_string(&entry).unwrap())
        .execute(&f.journal.db)
        .await
        .unwrap();
    file.write_all(&image.png[..20]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    drop(directory);
    assert!(f.journal.view().await.unwrap().artifacts.is_empty());
    f.journal.core.application_pool().close().await;
    drop(f.lock.take());
    let (reopened, _lock) = Journal::open_role(
        &f.config.data_dir,
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::Chat,
    )
    .await
    .unwrap();
    assert!(!path.exists());
    assert!(reopened.view().await.unwrap().artifacts.is_empty());
}

#[tokio::test]
async fn combined_snapshot_binds_each_image_only_to_its_own_sources() {
    let mut f = Fixture::new(false).await;
    let a = f.snapshot("A", 0).await;
    let b = f.snapshot("B", 1).await;
    let ServerPayload::DerivedSnapshot { messages: am, .. } = a.payload else {
        panic!()
    };
    let ServerPayload::DerivedSnapshot { messages: bm, .. } = b.payload else {
        panic!()
    };
    f.sequence += 1;
    let sources = f.memories.lock().unwrap().iter().map(source).collect();
    let both = Envelope::new(
        &f.journal.device,
        f.sequence,
        1,
        ServerPayload::DerivedSnapshot {
            tasks: vec![],
            messages: vec![am[0].clone(), bm[0].clone()],
            synced_at: crate::now(),
            local: LocalStatus::default(),
            sources,
        },
    )
    .unwrap();
    f.journal.accept(&both, true, 1, true).await.unwrap();
    f.load("A").await;
    let image = f.load("B").await;
    f.forget("A").await.unwrap();
    assert_eq!(f.images().await, 1);
    assert_eq!(std::fs::read(&image.path).unwrap(), png());
    f.snapshot("B", 1).await;
    assert_eq!(
        f.journal.view().await.unwrap().artifacts[0].operation_id,
        "B"
    );
    // A plain snapshot cannot smuggle in source-dependent image metadata.
    f.sequence += 1;
    let forged = Envelope::new(
        &f.journal.device,
        f.sequence,
        1,
        ServerPayload::Snapshot {
            tasks: vec![],
            messages: bm,
            synced_at: crate::now(),
            local: LocalStatus::default(),
        },
    )
    .unwrap();
    assert!(f.journal.accept(&forged, true, 1, true).await.is_err());
}

#[tokio::test]
async fn correction_expiry_and_reopen_clear_sourced_images_before_reuse() {
    for change in ["correct", "expire", "reopen"] {
        let mut f = Fixture::new(false).await;
        f.snapshot("A", 0).await;
        let image = f.load("A").await;
        match change {
            "correct" => {
                f.journal
                    .memory_control(
                        &uuid::Uuid::new_v4().to_string(),
                        "更正記憶 A：NEW_SOURCE",
                        crate::now(),
                    )
                    .await
                    .unwrap();
            }
            "expire" => {
                let sources = f.memories.lock().unwrap().clone();
                let view = MemoryView {
                    keeper: f.keeper.clone(),
                    epoch: 3,
                    memories: vec![],
                    redacted_sources: vec![],
                };
                f.journal
                    .apply_memory_view_for(&view, &f.credential, sources[0].expires_at)
                    .await
                    .unwrap();
            }
            _ => {
                f.journal.core.application_pool().close().await;
                drop(f.lock.take());
                let (_reopened, _lock) = Journal::open_role(
                    &f.config.data_dir,
                    f.config.server.as_str(),
                    &f.config.repo,
                    ClientRole::Chat,
                )
                .await
                .unwrap();
            }
        }
        assert!(!std::path::Path::new(&image.path).exists(), "{change}");
    }
}

#[tokio::test]
async fn failed_final_commit_and_unknown_files_never_publish_or_erase_user_bytes() {
    let mut f = Fixture::new(false).await;
    f.snapshot("A", 0).await;
    sqlx::query("CREATE TRIGGER fail_image_ready BEFORE UPDATE ON local_meta WHEN NEW.key='image:A' AND json_extract(NEW.value,'$.writing')=0 BEGIN SELECT RAISE(ABORT,'controlled image commit failure'); END")
        .execute(&f.journal.db).await.unwrap();
    assert!(
        f.journal
            .load_artifact(&f.config, &f.credential, "A", &hash_bytes(&png()))
            .await
            .is_err()
    );
    assert_eq!(f.images().await, 0);
    let path = f
        .config
        .data_dir
        .join("artifacts")
        .join(format!("{}.png", hash_bytes(&png())));
    assert!(!path.exists());
    std::fs::write(&path, png()).unwrap();
    assert!(
        f.journal
            .load_artifact(&f.config, &f.credential, "A", &hash_bytes(&png()))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), png());
    assert_eq!(f.images().await, 0);
}
