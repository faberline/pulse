//! Real Rust command loop and private Unix channel, with a fake helper and vault.
use super::*;
use crate::computer_control::PickerChannel;
use crate::computer_runtime::tests::fixture;

struct EmptyVault;
impl Vault for EmptyVault {
    fn load(&self, _: &str) -> Result<Option<Credential>> {
        Ok(None)
    }
    fn pending(&self, _: &str) -> Result<Option<PairClaim>> {
        Ok(None)
    }
    fn save(&self, _: &str, _: &Credential) -> Result<()> {
        Err(crate::Error::Rejected(
            "unexpected fixture credential write".into(),
        ))
    }
    fn save_pending(&self, _: &str, _: &PairClaim) -> Result<()> {
        Err(crate::Error::Rejected("unexpected fixture claim".into()))
    }
    fn clear_pending(&self, _: &str) -> Result<()> {
        Ok(())
    }
}
async fn wait(
    state: &mut watch::Receiver<NativeState>,
    predicate: impl Fn(&NativeState) -> bool,
) -> NativeState {
    tokio::time::timeout(Duration::from_secs(5), state.wait_for(predicate))
        .await
        .unwrap()
        .unwrap()
        .clone()
}
#[tokio::test]
async fn permission_commands_stay_local_quiet_until_human_request_and_never_create_work() {
    let (helper, runtime) = fixture("permission-pending");
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let directory = root.path().join("control");
    let server = "https://pulse.example/";
    let channel = PickerChannel::bind_at(&directory, server).await.unwrap();
    let backend = tokio::spawn(channel.serve(runtime.clone()));
    let config = Config {
        server: Url::parse(server).unwrap(),
        repo: PathBuf::new(),
        data_dir: root.path().join("chat"),
    };
    let mut device = Device::open_chat(config, Arc::new(EmptyVault))
        .await
        .unwrap();
    device.control_directory = directory.clone();
    let journal = device.journal.clone();
    let (sender, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let client = tokio::spawn(device.run(receiver, states));
    let list = uuid::Uuid::new_v4().to_string();
    sender
        .send(NativeCommand::ListComputerWindows {
            request_id: list.clone(),
        })
        .await
        .unwrap();
    let ready = wait(&mut state, |s| {
        s.computer_windows
            .as_ref()
            .is_some_and(|w| w.request_id == list && !w.loading)
    })
    .await;
    assert!(!ready.computer_windows.unwrap().failed);
    assert!(
        !ready
            .computer_permissions
            .unwrap()
            .permissions
            .unwrap()
            .can_observe()
    );
    assert!(!helper.path().join("permission-pending.requests").exists());
    let prompt = uuid::Uuid::new_v4().to_string();
    sender
        .send(NativeCommand::ComputerPermissions {
            request_id: prompt.clone(),
            request: Some(pulse_core::ComputerPermission::Accessibility),
        })
        .await
        .unwrap();
    let pending = wait(&mut state, |s| {
        s.computer_permissions
            .as_ref()
            .is_some_and(|p| p.request_id == prompt && !p.loading)
    })
    .await;
    assert!(
        !pending
            .computer_permissions
            .unwrap()
            .permissions
            .unwrap()
            .accessibility
    );
    assert_eq!(
        std::fs::read_to_string(helper.path().join("permission-pending.requests")).unwrap(),
        "1"
    );
    sender
        .send(NativeCommand::CancelComputerPermissions {
            request_id: prompt.clone(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| s.computer_permissions.is_none()).await;
    let list = uuid::Uuid::new_v4().to_string();
    sender
        .send(NativeCommand::ListComputerWindows {
            request_id: list.clone(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.computer_windows
            .as_ref()
            .is_some_and(|w| w.request_id == list && !w.loading)
    })
    .await;
    // An old UUID after a newer request is denied by the local backend too.
    sender
        .send(NativeCommand::ComputerPermissions {
            request_id: prompt,
            request: Some(pulse_core::ComputerPermission::Accessibility),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.computer_permissions
            .as_ref()
            .is_some_and(|p| p.failed && !p.loading)
    })
    .await;
    assert_eq!(
        std::fs::read_to_string(helper.path().join("permission-pending.requests")).unwrap(),
        "1"
    );
    assert!(journal.outgoing().await.unwrap().is_empty());
    assert!(journal.view().await.unwrap().tasks.is_empty());
    assert!(runtime.selections().is_empty());
    sender.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), client)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // The independent local channel stays alive after the chat process stops.
    assert!(!backend.is_finished());
    backend.abort();
    let _ = backend.await;
}
