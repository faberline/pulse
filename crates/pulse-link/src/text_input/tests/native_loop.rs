use super::*;
use crate::client::{Config, Device, Vault};
use crate::server::{Hub, router};
use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, watch};

#[derive(Default)]
struct MemoryVault(Mutex<(Option<Credential>, Option<PairClaim>)>);
impl Vault for MemoryVault {
    fn load(&self, _: &str) -> Result<Option<Credential>> {
        Ok(self.0.lock().unwrap().0.clone())
    }
    fn save(&self, _: &str, c: &Credential) -> Result<()> {
        self.0.lock().unwrap().0 = Some(c.clone());
        Ok(())
    }
    fn pending(&self, _: &str) -> Result<Option<PairClaim>> {
        Ok(self.0.lock().unwrap().1.clone())
    }
    fn save_pending(&self, _: &str, c: &PairClaim) -> Result<()> {
        self.0.lock().unwrap().1 = Some(c.clone());
        Ok(())
    }
    fn clear_pending(&self, _: &str) -> Result<()> {
        self.0.lock().unwrap().1 = None;
        Ok(())
    }
}
async fn wait(
    state: &mut watch::Receiver<NativeState>,
    condition: impl Fn(&NativeState) -> bool,
) -> NativeState {
    tokio::time::timeout(Duration::from_secs(5), state.wait_for(condition))
        .await
        .unwrap()
        .unwrap()
        .clone()
}
#[tokio::test]
async fn real_rust_file_selection_stays_local_until_submit_and_cancellation_removes_late_selection()
{
    let (root, path, _) = owned_file("ORIGINAL_NATIVE_FILE_SENTINEL");
    let hub = Hub::new(
        pulse_core::Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    let config = Config {
        server: url::Url::parse(&origin).unwrap(),
        repo: PathBuf::new(),
        data_dir: root.path().join("native-chat"),
    };
    let device = Device::open_chat(config, Arc::new(MemoryVault::default()))
        .await
        .unwrap();
    let (sender, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let worker = tokio::spawn(device.run(receiver, states));
    let unpaired = uuid::Uuid::new_v4().to_string();
    sender
        .send(NativeCommand::SelectTextFile {
            request_id: unpaired.clone(),
            path: path.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_input_request
            .as_ref()
            .is_some_and(|r| r.request_id == unpaired && r.failed)
    })
    .await;
    assert!(state.borrow().text_input.is_none());
    let code = hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    sender.send(NativeCommand::Pair { code }).await.unwrap();
    wait(&mut state, |s| s.connected && s.paired).await;
    let request = uuid::Uuid::new_v4().to_string();
    sender
        .send(NativeCommand::SelectTextFile {
            request_id: request.clone(),
            path: path.to_string_lossy().into(),
        })
        .await
        .unwrap();
    let selected = wait(&mut state, |s| {
        s.text_input_request
            .as_ref()
            .is_some_and(|r| r.request_id == request && !r.loading && !r.failed)
    })
    .await;
    let input = selected.text_input.unwrap();
    assert!(hub.core.list_tasks(100).await.unwrap().is_empty());
    let uploaded:i64=sqlx::query_scalar("SELECT count(*) FROM events WHERE json_extract(payload,'$.native_action.kind')='submit_text_file'").fetch_one(&hub.core.application_pool()).await.unwrap();
    assert_eq!(uploaded, 0);
    std::fs::write(&path, "CHANGED_AFTER_SELECTION").unwrap();
    let source = uuid::Uuid::new_v4().to_string();
    for _ in 0..2 {
        sender
            .send(NativeCommand::SubmitTextFile {
                source_id: source.clone(),
                id: "file-native-task".into(),
                goal: "摘要這份檔案".into(),
                input_id: input.id.clone(),
            })
            .await
            .unwrap();
    }
    wait(&mut state, |s| {
        s.tasks.iter().any(|t| t.id == "file-native-task")
    })
    .await;
    let run = hub
        .core
        .claim_next(&crate::remote::versions(), crate::now())
        .await
        .unwrap()
        .unwrap();
    let (prompt, history) = hub.model_context(&run).await.unwrap();
    assert!(history.is_empty());
    let prompt: serde_json::Value = serde_json::from_str(&prompt).unwrap();
    assert_eq!(
        prompt["selected_file_data"]["text"],
        "ORIGINAL_NATIVE_FILE_SENTINEL"
    );
    assert!(!prompt.to_string().contains("CHANGED_AFTER_SELECTION"));
    assert!(!prompt.to_string().contains(path.to_str().unwrap()));
    sender
        .send(NativeCommand::ClearTextFile { input_id: input.id })
        .await
        .unwrap();
    wait(&mut state, |s| s.text_input.is_none()).await;
    let request = uuid::Uuid::new_v4().to_string();
    sender
        .send(NativeCommand::SelectTextFile {
            request_id: request.clone(),
            path: path.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_input_request
            .as_ref()
            .is_some_and(|r| r.request_id == request && !r.loading && !r.failed)
    })
    .await;
    sender
        .send(NativeCommand::CancelTextFileSelection {
            request_id: request.clone(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_input.is_none()
            && s.text_input_request
                .as_ref()
                .is_some_and(|r| r.request_id == request && r.cancelled)
    })
    .await;
    let request = uuid::Uuid::new_v4().to_string();
    sender
        .send(NativeCommand::SelectTextFile {
            request_id: request.clone(),
            path: path.to_string_lossy().into(),
        })
        .await
        .unwrap();
    sender
        .send(NativeCommand::CancelTextFileSelection {
            request_id: request.clone(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_input.is_none()
            && s.text_input_request
                .as_ref()
                .is_some_and(|r| r.request_id == request && r.cancelled)
    })
    .await;
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
    let budget: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(budget, 0);
    sender.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
    let _ = server.await;
}
