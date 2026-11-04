use super::*;
use crate::{
    client::{Config, Device, Vault},
    journal::Journal,
    provider::Reply,
    server::{Hub, router},
};
use pulse_core::{Command, Origin, State, Store};
use serde_json::json;
use std::{
    future::IntoFuture,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, watch};

fn submit(text: &str) -> HumanAction {
    HumanAction::Submit {
        id: "must-not-create".into(),
        goal: text.into(),
    }
}
async fn seed(hub: &Hub, id: &str) -> (pulse_core::Task, ChatMessage) {
    let at = crate::now();
    let event=hub.core.ingest(Origin::Human,"fixture",id,json!({"commands":[Command::Create{id:id.into(),goal:format!("任務 {id}：owned fixture"),scope:"pulse:read".into(),priority:0,parent:None,dependencies:vec![],requires_device:false}]}),at).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let run = hub
        .core
        .claim_next(&crate::remote::versions(), at)
        .await
        .unwrap()
        .unwrap();
    hub.model_reply(
        &run.token,
        &Reply {
            text: format!("OWNED_RESULT_{id}"),
            tool: None,
            usage: json!({}),
            cost: 0,
        },
        "fixture",
        at,
    )
    .await
    .unwrap();
    let message = format!("model:{}:1:0", run.token.run_id);
    let body = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
        .bind(&message)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    (
        hub.core.task(id).await.unwrap(),
        ChatMessage {
            id: message,
            body,
            memory_sources: Some(vec![]),
            artifact: None,
            artifact_operation_id: None,
            approval: None,
        },
    )
}
async fn cache(journal: &Journal, tasks: Vec<pulse_core::Task>, messages: Vec<ChatMessage>) {
    let state = NativeState {
        tasks,
        messages,
        ..Default::default()
    };
    sqlx::query("INSERT INTO local_meta VALUES('cache',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(serde_json::to_string(&state).unwrap()).execute(&journal.db).await.unwrap();
}

#[test]
fn review_phrases_require_explicit_review_words() {
    for text in [
        "接受成果 A",
        "確認成果A。",
        "接受 A 的成果",
        "確認A的成果！",
    ] {
        assert_eq!(target(text), Some("A"));
    }
    assert_eq!(target("接受成果"), Some(""));
    for text in ["好", "可以", "批准 A", "確認 A", "幫我接受任務", "成果 A"] {
        assert_eq!(target(text), None);
    }
}

#[tokio::test]
async fn offline_journal_freezes_the_original_display_and_reopens_without_retargeting() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let (a, message) = seed(&hub, "A").await;
    let dir = root.path().join("chat");
    let (journal, lock) =
        Journal::open_role(&dir, "http://127.0.0.1:9/", Path::new(""), ClientRole::Chat)
            .await
            .unwrap();
    cache(&journal, vec![a.clone()], vec![message.clone()]).await;
    journal
        .action("review", submit("接受成果 A"))
        .await
        .unwrap();
    let original = journal
        .outgoing()
        .await
        .unwrap()
        .into_iter()
        .find(|e| matches!(&e.payload,DevicePayload::Human{source_id,..} if source_id=="review"))
        .unwrap();
    let DevicePayload::Human {
        action:
            HumanAction::AcceptResult {
                task_id,
                revision,
                message_id,
                chat_digest,
                ..
            },
        ..
    } = &original.payload
    else {
        panic!("frozen review required")
    };
    assert_eq!(
        (task_id.as_str(), revision.as_str(), message_id.as_str()),
        ("A", "1", message.id.as_str())
    );
    assert_eq!(*chat_digest, hash_bytes(message.body.as_bytes()));
    let mut changed = message;
    changed.body = "DIFFERENT_DISPLAY".into();
    cache(&journal, vec![a], vec![changed]).await;
    journal.core.close().await;
    drop(lock);
    let (journal, _lock) =
        Journal::open_role(&dir, "http://127.0.0.1:9/", Path::new(""), ClientRole::Chat)
            .await
            .unwrap();
    journal
        .action("review", submit("接受成果 A"))
        .await
        .unwrap();
    let reviews = journal
        .outgoing()
        .await
        .unwrap()
        .into_iter()
        .filter(|e| matches!(&e.payload,DevicePayload::Human{source_id,..} if source_id=="review"))
        .collect::<Vec<_>>();
    assert_eq!(reviews.len(), 2);
    assert!(
        reviews
            .iter()
            .all(|e| serde_json::to_value(&e.payload).unwrap()
                == serde_json::to_value(&original.payload).unwrap())
    );
    assert!(
        journal
            .action("review", submit("確認成果 A"))
            .await
            .is_err()
    );
    assert!(hub.core.task("must-not-create").await.is_err());
}

#[tokio::test]
async fn ambiguity_or_missing_display_preserves_input_and_queues_no_partial_review() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let (a, ma) = seed(&hub, "A").await;
    let (b, mb) = seed(&hub, "B").await;
    let (journal, _lock) = Journal::open_role(
        &root.path().join("chat"),
        "http://127.0.0.1:9/",
        Path::new(""),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    cache(&journal, vec![a.clone(), b], vec![ma.clone(), mb]).await;
    for text in ["接受成果", "接受成果 A B", "接受成果 C"] {
        assert!(journal.action(text, submit(text)).await.is_err(), "{text}");
    }
    assert!(journal.outgoing().await.unwrap().is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM local_pending")
            .fetch_one(&journal.db)
            .await
            .unwrap(),
        0
    );
    cache(&journal, vec![a.clone()], vec![]).await;
    assert!(
        journal
            .action("missing", submit("接受成果 A"))
            .await
            .is_err()
    );
    cache(&journal, vec![a], vec![ma]).await;
    journal.action("one", submit("接受成果")).await.unwrap();
    let (worker, _lock) = Journal::open(
        &root.path().join("worker"),
        "http://127.0.0.1:9/",
        Path::new(""),
    )
    .await
    .unwrap();
    assert!(worker.action("worker", submit("接受成果 A")).await.is_err());
    assert!(worker.outgoing().await.unwrap().is_empty());
}

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
    test: impl Fn(&NativeState) -> bool,
) -> NativeState {
    tokio::time::timeout(Duration::from_secs(6), state.wait_for(test))
        .await
        .unwrap()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn rust_composer_confirms_over_real_websocket_and_recovers_terminal_state() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let (_, ma) = seed(&hub, "A").await;
    let (_, mb) = seed(&hub, "B").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    let vault = Arc::new(MemoryVault::default());
    let config = Config {
        server: url::Url::parse(&origin).unwrap(),
        repo: Default::default(),
        data_dir: root.path().join("chat"),
    };
    let device = Device::open_chat(config.clone(), vault.clone())
        .await
        .unwrap();
    let (commands, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let process = tokio::spawn(device.run(receiver, states));
    let code = hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    commands.send(NativeCommand::Pair { code }).await.unwrap();
    wait(&mut state, |s| {
        s.connected
            && s.messages.iter().any(|m| m.id == ma.id)
            && s.messages.iter().any(|m| m.id == mb.id)
    })
    .await;
    commands
        .send(NativeCommand::Action {
            source_id: "ambiguous".into(),
            action: submit("接受成果"),
        })
        .await
        .unwrap();
    let rejected = wait(&mut state, |s| {
        s.input_receipt
            .as_ref()
            .is_some_and(|r| r.source_id == "ambiguous" && !r.saved)
    })
    .await;
    assert_eq!(rejected.input_failure.as_deref(), Some("ambiguous"));
    assert_eq!(hub.core.task("A").await.unwrap().state, State::Blocked);
    commands
        .send(NativeCommand::Action {
            source_id: "accept-A".into(),
            action: submit("接受成果 A"),
        })
        .await
        .unwrap();
    let accepted = wait(&mut state, |s| {
        s.tasks
            .iter()
            .any(|t| t.id == "A" && t.state == State::Succeeded)
    })
    .await;
    assert!(
        accepted
            .tasks
            .iter()
            .any(|t| t.id == "B" && t.state == State::Blocked)
    );
    assert!(
        accepted
            .messages
            .iter()
            .any(|m| m.id == "reply:accept-A" && m.body.contains("任務完成"))
    );
    assert!(hub.core.task("must-not-create").await.is_err());
    commands.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(6), process)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let device = Device::open_chat(config, vault).await.unwrap();
    let (commands, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let process = tokio::spawn(device.run(receiver, states));
    wait(&mut state, |s| {
        s.connected
            && s.tasks
                .iter()
                .any(|t| t.id == "A" && t.state == State::Succeeded)
    })
    .await;
    commands.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(6), process)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
}
