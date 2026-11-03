use super::*;
use crate::{
    client::{Device, Vault},
    server::{Hub, router},
};
use std::{future::IntoFuture, path::PathBuf, sync::Mutex, time::Duration};
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
    f: impl Fn(&NativeState) -> bool,
) -> NativeState {
    tokio::time::timeout(Duration::from_secs(6), state.wait_for(f))
        .await
        .unwrap()
        .unwrap()
        .clone()
}
async fn wait_for(mut f: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(6), async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn rust_composer_exports_original_file_result_and_cancels_download_without_model_calls() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().canonicalize().unwrap();
    let input = base.join("source.md");
    std::fs::write(&input, "ORIGINAL_FILE_INPUT").unwrap();
    let hub = Hub::new(
        pulse_core::Store::open(base.join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let gate = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hold = gate.clone();
    let visits = entered.clone();
    let app = router(hub.clone()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let hold = hold.clone();
            let visits = visits.clone();
            async move {
                if request.uri().path().starts_with("/v1/text-results/")
                    && hold.load(Ordering::Acquire)
                {
                    visits.fetch_add(1, Ordering::Release);
                    while hold.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                next.run(request).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let vault = Arc::new(MemoryVault::default());
    let device = Device::open_chat(
        Config {
            server: url::Url::parse(&server_url).unwrap(),
            repo: PathBuf::new(),
            data_dir: base.join("chat"),
        },
        vault.clone(),
    )
    .await
    .unwrap();
    let (commands, receiver) = mpsc::channel(16);
    let (states, mut state) = watch::channel(NativeState::default());
    let process = tokio::spawn(device.run(receiver, states));
    let code = hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    commands.send(NativeCommand::Pair { code }).await.unwrap();
    wait(&mut state, |s| s.connected && s.paired).await;
    let selected = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::SelectTextFile {
            request_id: selected.clone(),
            path: input.to_string_lossy().into(),
        })
        .await
        .unwrap();
    let value = wait(&mut state, |s| {
        s.text_input_request
            .as_ref()
            .is_some_and(|r| r.request_id == selected && !r.loading && !r.failed)
    })
    .await;
    let original = value.text_input.unwrap();
    commands
        .send(NativeCommand::SubmitTextFile {
            source_id: "file-source".into(),
            id: "file-task".into(),
            goal: "摘要這份資料".into(),
            input_id: original.id,
        })
        .await
        .unwrap();
    wait(&mut state, |s| s.tasks.iter().any(|t| t.id == "file-task")).await;
    let run = hub
        .core
        .claim_next(&crate::remote::versions(), crate::now())
        .await
        .unwrap()
        .unwrap();
    hub.model_reply(
        &run.token,
        &crate::provider::Reply {
            text: "EXPORT_RESULT_SENTINEL".into(),
            tool: None,
            usage: serde_json::json!({}),
            cost: 0,
        },
        "fixture",
        crate::now(),
    )
    .await
    .unwrap();
    let ready = wait(&mut state, |s| {
        s.messages.iter().any(|m| m.id.starts_with("model:"))
    })
    .await;
    let message = ready
        .messages
        .iter()
        .find(|m| m.id.starts_with("model:"))
        .unwrap()
        .clone();
    let credential = vault.0.lock().unwrap().0.clone().unwrap();
    let manifest = hub
        .text_result_manifest(&credential.device, &run.token.run_id, crate::now())
        .await
        .unwrap();
    assert_eq!(manifest.message_id, message.id);
    assert_eq!(manifest.chat_digest, hash_bytes(message.body.as_bytes()));
    let expected = hub
        .text_result_bytes(
            &credential.device,
            &run.token.run_id,
            &manifest.reference.digest,
            crate::now(),
        )
        .await
        .unwrap();
    let document = String::from_utf8(expected.clone()).unwrap();
    assert!(
        document.contains("EXPORT_RESULT_SENTINEL")
            && document.contains("source\\.md")
            && document.contains(&original.digest)
    );
    assert!(!document.contains(base.to_str().unwrap()));
    std::fs::write(&input, "CHANGED_AFTER_SELECTION").unwrap();
    let saved = base.join("saved.md");
    let request = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::ExportTextResult {
            request_id: request.clone(),
            message_id: message.id.clone(),
            path: saved.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_result_export
            .as_ref()
            .is_some_and(|r| r.request_id == request && r.status == ExportStatus::Saved)
    })
    .await;
    assert_eq!(std::fs::read(&saved).unwrap(), expected);
    let conflicting = base.join("conflicting-replay.md");
    commands
        .send(NativeCommand::ExportTextResult {
            request_id: request.clone(),
            message_id: message.id.clone(),
            path: conflicting.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_result_export
            .as_ref()
            .is_some_and(|r| r.request_id == request && r.status == ExportStatus::NotSaved)
    })
    .await;
    assert!(!conflicting.exists());
    commands
        .send(NativeCommand::ExportTextResult {
            request_id: request.clone(),
            message_id: message.id.clone(),
            path: saved.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_result_export
            .as_ref()
            .is_some_and(|r| r.request_id == request && r.status == ExportStatus::Saved)
    })
    .await;
    assert_eq!(std::fs::read(&saved).unwrap(), expected);
    let request = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::ExportTextResult {
            request_id: request.clone(),
            message_id: message.id.clone(),
            path: saved.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_result_export
            .as_ref()
            .is_some_and(|r| r.request_id == request && r.status == ExportStatus::NotSaved)
    })
    .await;
    assert_eq!(std::fs::read(&saved).unwrap(), expected);
    gate.store(true, Ordering::Release);
    let cancelled = base.join("cancelled.md");
    let request = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::ExportTextResult {
            request_id: request.clone(),
            message_id: message.id.clone(),
            path: cancelled.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait_for(|| entered.load(Ordering::Acquire) > 0).await;
    commands
        .send(NativeCommand::CancelTextResultExport {
            request_id: request.clone(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_result_export
            .as_ref()
            .is_some_and(|r| r.request_id == request && r.status == ExportStatus::NotSaved)
    })
    .await;
    assert!(!cancelled.exists());
    gate.store(false, Ordering::Release);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    assert_eq!(
        client
            .get(format!("{server_url}v1/text-results/{}", run.token.run_id))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert!(
        hub.text_result_bytes(
            &credential.device,
            &run.token.run_id,
            &"0".repeat(64),
            crate::now()
        )
        .await
        .is_err()
    );
    let code = hub
        .pairing(ClientRole::LocalExecutor, crate::now())
        .await
        .unwrap();
    let local = hub
        .redeem(
            PairRequest {
                code,
                role: ClientRole::LocalExecutor,
                device: uuid::Uuid::new_v4().to_string(),
            },
            crate::now(),
        )
        .await
        .unwrap();
    hub.begin(&local.device, 0, crate::now()).await.unwrap();
    assert!(
        hub.text_result_manifest(&local.device, &run.token.run_id, crate::now())
            .await
            .is_err()
    );
    let journal = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        base.join("chat/device.db").display()
    ))
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER suppress_export_status BEFORE UPDATE OF value ON local_meta WHEN OLD.key GLOB 'text_result_export_v1:*' BEGIN SELECT RAISE(IGNORE); END").execute(&journal).await.unwrap();
    let uncertain = base.join("receipt-unknown.md");
    let uncertain_id = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::ExportTextResult {
            request_id: uncertain_id.clone(),
            message_id: message.id.clone(),
            path: uncertain.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_result_export
            .as_ref()
            .is_some_and(|r| r.request_id == uncertain_id && r.status == ExportStatus::Unknown)
    })
    .await;
    assert_eq!(std::fs::read(&uncertain).unwrap(), expected);
    // A lost durable result must not automatically recreate a Human-removed file.
    std::fs::remove_file(&uncertain).unwrap();
    commands
        .send(NativeCommand::ExportTextResult {
            request_id: uncertain_id.clone(),
            message_id: message.id.clone(),
            path: base.join("different-unknown.md").to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_result_export
            .as_ref()
            .is_some_and(|r| r.request_id == uncertain_id && r.status == ExportStatus::NotSaved)
    })
    .await;
    commands
        .send(NativeCommand::ExportTextResult {
            request_id: uncertain_id.clone(),
            message_id: message.id.clone(),
            path: uncertain.to_string_lossy().into(),
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.text_result_export
            .as_ref()
            .is_some_and(|r| r.request_id == uncertain_id && r.status == ExportStatus::Unknown)
    })
    .await;
    assert!(!uncertain.exists());
    assert!(!base.join("different-unknown.md").exists());
    let receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM local_meta WHERE key GLOB 'text_result_export_v1:*'",
    )
    .fetch_one(&journal)
    .await
    .unwrap();
    assert_eq!(receipts, 2);
    sqlx::query("DROP TRIGGER suppress_export_status")
        .execute(&journal)
        .await
        .unwrap();
    journal.close().await;
    commands
        .send(NativeCommand::Action {
            source_id: "file-followup".into(),
            action: HumanAction::Submit {
                id: "no-new-task".into(),
                goal: "再詳細一點".into(),
            },
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.tasks
            .iter()
            .any(|t| t.id == "file-task" && t.revision == 2)
    })
    .await;
    assert_eq!(
        hub.text_result_bytes(
            &credential.device,
            &run.token.run_id,
            &manifest.reference.digest,
            crate::now()
        )
        .await
        .unwrap(),
        expected
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    // Changing the original source invalidates download even if an old UI kept its reference.
    let task = hub.core.task("file-task").await.unwrap();
    sqlx::query("UPDATE events SET payload='{}' WHERE id=?")
        .bind(task.authorization_event)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        hub.text_result_manifest(&credential.device, &run.token.run_id, crate::now())
            .await
            .is_err()
    );
    commands.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(6), process)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let restarted = Device::open_chat(
        Config {
            server: url::Url::parse(&server_url).unwrap(),
            repo: PathBuf::new(),
            data_dir: base.join("chat"),
        },
        vault,
    )
    .await
    .unwrap();
    let (next_commands, next_receiver) = mpsc::channel(8);
    let (next_states, mut restored) = watch::channel(NativeState::default());
    let next_process = tokio::spawn(restarted.run(next_receiver, next_states));
    let recovered = wait(&mut restored, |s| {
        s.connected
            && s.text_result_export
                .as_ref()
                .is_some_and(|r| r.request_id == uncertain_id && r.status == ExportStatus::Unknown)
    })
    .await;
    assert_eq!(
        recovered.text_result_export.unwrap().file_name.as_deref(),
        Some("receipt-unknown.md")
    );
    assert!(!uncertain.exists());
    next_commands.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(6), next_process)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
}
