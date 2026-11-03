//! Real Rust device shutdown with a private fake vault and controlled helper.
use super::*;
use crate::computer_runtime::tests::{fixture, image_reader};

struct EmptyVault {
    fail: bool,
}
impl Vault for EmptyVault {
    fn load(&self, _: &str) -> Result<Option<Credential>> {
        ensure(!self.fail, "fixture vault load failed")?;
        Ok(None)
    }
    fn pending(&self, _: &str) -> Result<Option<PairClaim>> {
        Ok(None)
    }
    fn save(&self, _: &str, _: &Credential) -> Result<()> {
        Err(crate::Error::Rejected("unexpected fixture save".into()))
    }
    fn save_pending(&self, _: &str, _: &PairClaim) -> Result<()> {
        Err(crate::Error::Rejected("unexpected fixture claim".into()))
    }
    fn clear_pending(&self, _: &str) -> Result<()> {
        Ok(())
    }
}
async fn device(
    root: &std::path::Path,
    runtime: crate::computer_runtime::ComputerRuntime,
    fail: bool,
) -> Device {
    let config = Config {
        server: Url::parse("https://pulse.example/").unwrap(),
        repo: root.to_path_buf(),
        data_dir: root.join("local"),
    };
    let (journal, lock) = Journal::open_role(
        &config.data_dir,
        config.server.as_str(),
        &config.repo,
        ClientRole::LocalExecutor,
    )
    .await
    .unwrap();
    journal.recover().await.unwrap();
    Device {
        control_directory: root.join("control"),
        executor: Some(Executor::new(root).unwrap().with_computer(Some(runtime))),
        config,
        journal,
        _lock: lock,
        vault: Arc::new(EmptyVault { fail }),
        account: "fixture".into(),
    }
}
fn binding() -> pulse_core::ComputerBinding {
    pulse_core::ComputerBinding {
        operation_id: "image".into(),
        task_id: "task".into(),
        revision: 1,
        device: uuid::Uuid::new_v4().to_string(),
        epoch: 1,
    }
}

#[tokio::test]
async fn shutdown_eof_and_startup_error_keep_the_device_lock_until_owned_image_readers_finish() {
    for mode in ["shutdown", "eof", "startup-error"] {
        let (_helper, runtime) = fixture("good");
        runtime.observe(&binding()).await.unwrap();
        let lease = image_reader(&runtime, "task").await;
        let image_dir = lease.path.clone();
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let device = device(root.path(), runtime.clone(), mode == "startup-error").await;
        let config = device.config.clone();
        let (sender, receiver) = mpsc::channel(8);
        let (states, _state) = watch::channel(NativeState::default());
        if mode == "shutdown" {
            sender.send(NativeCommand::Shutdown).await.unwrap();
        }
        if mode == "eof" {
            drop(sender);
        }
        let running = tokio::spawn(device.run(receiver, states));
        tokio::time::timeout(Duration::from_secs(5), async {
            while lease.valid().is_ok() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!running.is_finished());
        assert!(image_dir.exists());
        assert!(
            Journal::open_role(
                &config.data_dir,
                config.server.as_str(),
                &config.repo,
                ClientRole::LocalExecutor
            )
            .await
            .is_err()
        );
        drop(lease);
        let result = running.await.unwrap();
        if mode == "startup-error" {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
        assert!(!image_dir.exists());
        assert_eq!(
            std::fs::read_dir(root.path().join("control"))
                .unwrap()
                .count(),
            0
        );
        assert!(runtime.permissions().await.is_err());
        let (_journal, _lock) = Journal::open_role(
            &config.data_dir,
            config.server.as_str(),
            &config.repo,
            ClientRole::LocalExecutor,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn a_failed_owned_cache_cleanup_is_an_error_at_the_device_boundary_and_keeps_the_file() {
    let (_helper, runtime) = fixture("good");
    runtime.observe(&binding()).await.unwrap();
    let lease = image_reader(&runtime, "task").await;
    let unknown = lease.path.join("KEEP");
    std::fs::write(&unknown, b"KEEP").unwrap();
    drop(lease);
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let device = device(root.path(), runtime, false).await;
    let (sender, receiver) = mpsc::channel(8);
    sender.send(NativeCommand::Shutdown).await.unwrap();
    let (states, _state) = watch::channel(NativeState::default());
    assert!(device.run(receiver, states).await.is_err());
    assert_eq!(std::fs::read(unknown).unwrap(), b"KEEP");
}
