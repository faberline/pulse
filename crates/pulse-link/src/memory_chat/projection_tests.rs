use super::*;
use pulse_core::{ErasedProjection, Memory, TaskMemorySource};

const DERIVED: &str = "PRIVATE_DERIVED_CHAT_COPY";

async fn source(f: &Fixture) -> Memory {
    say(f, "記住 偏好：使用繁體中文").await;
    sync(f).await;
    f.hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0)
}
fn frame(f: &Fixture, memory: &Memory, sequence: u64) -> Envelope<ServerPayload> {
    Envelope::new(
        &f.journal.device,
        sequence,
        1,
        ServerPayload::DerivedSnapshot {
            tasks: vec![],
            messages: vec![ChatMessage {
                id: "generated".into(),
                body: DERIVED.into(),
                approval: None,
                artifact: None,
                artifact_operation_id: None,
            }],
            synced_at: crate::now(),
            local: LocalStatus::default(),
            sources: vec![TaskMemorySource {
                memory_id: memory.id.clone(),
                revision: memory.revision,
                source_id: memory.source_id.clone(),
                expires_at: memory.expires_at,
            }],
        },
    )
    .unwrap()
}
fn erasure(frame: &Envelope<ServerPayload>) -> ErasedProjection {
    ErasedProjection {
        version: frame.version as u32,
        id: frame.id.clone(),
        device: frame.device.clone(),
        sequence: frame.sequence.clone(),
        epoch: frame.epoch.clone(),
        digest: frame.digest.clone(),
        binding_digest: frame.binding_digest.clone(),
    }
}
async fn cache(f: &Fixture) -> Option<String> {
    sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cache'")
        .fetch_optional(&f.journal.db)
        .await
        .unwrap()
}

#[tokio::test]
async fn erasure_capability_requires_an_active_fresh_memory_sync() {
    let f = setup().await;
    assert!(f.journal.projection_token().await.unwrap().is_none());
    let memory = source(&f).await;
    assert!(f.journal.projection_token().await.unwrap().is_some());
    f.journal.deny_memory_access().await.unwrap();
    assert!(f.journal.projection_token().await.unwrap().is_none());
    let worker = f._root.path().join("worker");
    let (journal, _lock) = Journal::open_role(
        &worker,
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::LocalExecutor,
    )
    .await
    .unwrap();
    assert!(journal.projection_token().await.unwrap().is_none());
    let header = erasure(&frame(&f, &memory, 1));
    assert!(journal.accept_erased_projection(&header).await.is_err());
}
#[tokio::test]
async fn projection_erasure_replays_original_identity_without_erasing_newer_cache() {
    let f = setup().await;
    let m = source(&f).await;
    let first = frame(&f, &m, 1);
    assert!(f.journal.accept(&first, true, 1, true).await.unwrap());
    assert!(cache(&f).await.unwrap().contains(DERIVED));
    let erased = erasure(&first);
    assert!(erased.valid());
    assert!(!f.journal.accept_erased_projection(&erased).await.unwrap());
    assert!(cache(&f).await.is_none());
    let second = frame(&f, &m, 2);
    f.journal.accept(&second, true, 1, true).await.unwrap();
    assert!(!f.journal.accept_erased_projection(&erased).await.unwrap());
    assert!(cache(&f).await.unwrap().contains(DERIVED));
    let mut forged = erased.clone();
    forged.id = "changed".into();
    assert!(f.journal.accept_erased_projection(&forged).await.is_err());
    let gap = erasure(&frame(&f, &m, 4));
    assert!(f.journal.accept_erased_projection(&gap).await.is_err());
    let mut conflict = erased;
    conflict.id = "conflicting-retry".into();
    conflict.binding_digest = hash(&(
        conflict.version,
        &conflict.id,
        &conflict.device,
        &conflict.sequence,
        &conflict.epoch,
        &conflict.digest,
    ))
    .unwrap();
    assert!(conflict.valid());
    assert!(f.journal.accept_erased_projection(&conflict).await.is_err());
    assert_eq!(f.journal.cursor().await.unwrap(), 2);
}
#[tokio::test]
async fn local_withdrawal_erases_derived_copy_and_late_body_is_discarded_before_ack() {
    let f = setup().await;
    let m = source(&f).await;
    let frame = frame(&f, &m, 1);
    f.journal.accept(&frame, true, 1, true).await.unwrap();
    f.journal
        .memory_control(
            &uuid::Uuid::new_v4().to_string(),
            "忘記記憶 偏好",
            crate::now(),
        )
        .await
        .unwrap();
    assert!(cache(&f).await.is_none());
    // This is the exact original payload whose ACK was lost.
    assert!(!f.journal.accept(&frame, true, 1, true).await.unwrap());
    assert!(cache(&f).await.is_none());
    let late = self::frame(&f, &m, 2);
    assert!(f.journal.accept(&late, true, 1, true).await.unwrap());
    assert_eq!(f.journal.cursor().await.unwrap(), 2);
    assert!(cache(&f).await.is_none());
    assert!(
        !serde_json::to_string(&f.journal.view().await.unwrap())
            .unwrap()
            .contains(DERIVED)
    );
    let bytes = std::fs::read(f.config.data_dir.join("device.db")).unwrap();
    assert!(
        !bytes
            .windows(DERIVED.len())
            .any(|s| s == DERIVED.as_bytes())
    );
}
#[tokio::test]
async fn refreshed_source_revision_prunes_cache_and_does_not_hide_human_drafts() {
    let f = setup().await;
    let m = source(&f).await;
    f.journal
        .accept(&frame(&f, &m, 1), true, 1, true)
        .await
        .unwrap();
    let mut current = MemoryView {
        keeper: sqlx::query_scalar("SELECT value FROM local_meta WHERE key='memory_keeper'")
            .fetch_one(&f.journal.db)
            .await
            .unwrap(),
        epoch: 2,
        memories: vec![m.clone()],
        redacted_sources: vec![],
    };
    current.memories[0].revision += 1;
    f.journal
        .apply_memory_view(&current, crate::now())
        .await
        .unwrap();
    assert!(cache(&f).await.is_none());
    say(&f, "分析架構，這是尚未送出的草稿").await;
    // A denied derived projection must not remove independent Human input.
    f.journal
        .accept(&frame(&f, &m, 2), true, 1, true)
        .await
        .unwrap();
    assert!(
        f.journal
            .view()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|m| m.body.contains("尚未送出的草稿"))
    );
}

#[tokio::test]
async fn offline_correction_and_credential_denial_remove_derived_projection_immediately() {
    for deny in [false, true] {
        let f = setup().await;
        let m = source(&f).await;
        f.journal
            .accept(&frame(&f, &m, 1), true, 1, true)
            .await
            .unwrap();
        if deny {
            f.journal.deny_memory_access().await.unwrap();
        } else {
            assert!(
                f.journal
                    .memory_control(
                        &uuid::Uuid::new_v4().to_string(),
                        "更正記憶 偏好：改用英文",
                        crate::now()
                    )
                    .await
                    .unwrap()
            );
        }
        assert!(cache(&f).await.is_none());
        f.journal
            .accept(&frame(&f, &m, 2), true, 1, true)
            .await
            .unwrap();
        assert!(cache(&f).await.is_none());
        assert!(
            !serde_json::to_string(&f.journal.view().await.unwrap())
                .unwrap()
                .contains(DERIVED)
        );
    }
}

#[tokio::test]
async fn fresh_erasure_supersedes_older_sourced_cache_but_retains_human_input() {
    let f = setup().await;
    let m = source(&f).await;
    f.journal
        .accept(&frame(&f, &m, 1), true, 1, true)
        .await
        .unwrap();
    say(&f, "分析尚未送出的任務").await;
    assert!(
        f.journal
            .accept_erased_projection(&erasure(&frame(&f, &m, 2)))
            .await
            .unwrap()
    );
    assert!(cache(&f).await.is_none());
    assert!(
        f.journal
            .view()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|m| m.body.contains("尚未送出的任務"))
    );
}
#[tokio::test]
async fn restarting_clears_sourced_cache_before_any_frontend_can_read_it() {
    let f = setup().await;
    let m = source(&f).await;
    f.journal
        .accept(&frame(&f, &m, 1), true, 1, true)
        .await
        .unwrap();
    say(&f, "分析 Pulse 的 Rust 核心").await;
    // Copy the committed database as a pre-erasure restore fixture.
    f.journal.flush_memory().await.unwrap();
    let restore = f._root.path().join("restore");
    std::fs::create_dir(&restore).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&restore, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::copy(
        f.config.data_dir.join("device.db"),
        restore.join("device.db"),
    )
    .unwrap();
    std::fs::copy(
        f.config.data_dir.join("device.db.memory-restrictions.json"),
        restore.join("device.db.memory-restrictions.json"),
    )
    .unwrap();
    let (journal, _lock) = Journal::open_role(
        &restore,
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::Chat,
    )
    .await
    .unwrap();
    let view = journal.view().await.unwrap();
    assert!(!serde_json::to_string(&view).unwrap().contains(DERIVED));
    assert!(
        view.messages
            .iter()
            .any(|m| m.body.contains("分析 Pulse 的 Rust 核心"))
    );
    assert_eq!(journal.cursor().await.unwrap(), 1);
}
#[tokio::test]
async fn erasure_checkpoint_failure_is_not_acknowledged_and_retry_cleans_physical_bytes() {
    let f = setup().await;
    let m = source(&f).await;
    let frame = frame(&f, &m, 1);
    f.journal.accept(&frame, true, 1, true).await.unwrap();
    f.journal.flush_memory().await.unwrap();
    use sqlx::{Connection, Executor};
    let mut reader = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        f.config.data_dir.join("device.db").display()
    ))
    .await
    .unwrap();
    reader
        .execute("BEGIN; SELECT value FROM local_meta WHERE key='cache'")
        .await
        .unwrap();
    assert!(
        f.journal
            .accept_erased_projection(&erasure(&frame))
            .await
            .is_err()
    );
    assert_eq!(f.journal.cursor().await.unwrap(), 1);
    reader.execute("ROLLBACK").await.unwrap();
    reader.close().await.unwrap();
    assert!(
        !f.journal
            .accept_erased_projection(&erasure(&frame))
            .await
            .unwrap()
    );
    assert!(cache(&f).await.is_none());
    let bytes = std::fs::read(f.config.data_dir.join("device.db")).unwrap();
    assert!(
        !bytes
            .windows(DERIVED.len())
            .any(|s| s == DERIVED.as_bytes())
    );
}

#[tokio::test]
async fn live_websocket_erases_a_sourced_reply_even_when_original_ack_was_lost() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
    // Waiting for another fixture's semaphore is outside the wire deadline.
    let f = setup().await;
    tokio::time::timeout(std::time::Duration::from_secs(15),async {
        let m=source(&f).await;let now=crate::now();
        let event=f.hub.core.ingest(Origin::Human,"fixture","task",serde_json::json!({"commands":[pulse_core::Command::Create{id:"task".into(),goal:"分析 Pulse".into(),scope:"pulse:read".into(),priority:0,requires_device:false,parent:None,dependencies:vec![]}]}),now).await.unwrap();
        f.hub.core.route(&event.event_id).await.unwrap();
        f.hub.core.bind_task_memories(Origin::Human,&pulse_core::TaskMemoryRequest{source_id:uuid::Uuid::new_v4().to_string(),source_at:now,task_id:"task".into(),task_revision:1,sources:vec![pulse_core::MemoryRef{id:m.id.clone(),revision:1}]},now).await.unwrap();
        let run=f.hub.core.claim_next(&crate::remote::versions(),crate::now()).await.unwrap().unwrap();
        f.hub.model_reply(&run.token,&crate::provider::Reply{text:DERIVED.into(),tool:None,usage:serde_json::json!({}),cost:0},"fixture",crate::now()).await.unwrap();
        let mut url=f.config.server.join("v1/devices/connect").unwrap();url.set_scheme("ws").unwrap();
        let mut request=url.as_str().into_client_request().unwrap();
        request.headers_mut().insert("authorization",format!("Bearer {}",f.credential.token).parse().unwrap());
        let (mut socket,_)=tokio_tungstenite::connect_async(request).await.unwrap();
        socket.send(Message::Text(serde_json::to_string(&ClientWire::Hello{device:f.journal.device.clone(),cursor:"0".into()}).unwrap().into())).await.unwrap();
        let accepted=loop {
            let Message::Text(text)=socket.next().await.unwrap().unwrap() else {continue};
            match crate::protocol::parse::<ServerWire>(text.as_bytes()).unwrap() {
                ServerWire::Welcome{..}=>{socket.send(Message::Text(serde_json::to_string(&ClientWire::ProjectionErasureReady).unwrap().into())).await.unwrap();},
                ServerWire::Data{envelope,permitted}=>{
                    f.journal.accept(&envelope,permitted,1,true).await.unwrap();
                    if matches!(envelope.payload,ServerPayload::DerivedSnapshot{..}) {break envelope;}
                    socket.send(Message::Text(serde_json::to_string(&ClientWire::Ack{sequence:envelope.sequence,id:envelope.id}).unwrap().into())).await.unwrap();
                },
                _=>panic!("unexpected server response"),
            }
        };
        assert!(cache(&f).await.unwrap().contains(DERIVED));
        // Keep the exact derived frame pending at the server, then withdraw.
        f.hub.core.change_memory(Origin::Human,&MemoryRequest{source_id:uuid::Uuid::new_v4().to_string(),source_at:crate::now(),command:pulse_core::MemoryCommand::Forget{id:m.id,revision:1}},crate::now()).await.unwrap();
        loop {
            let Message::Text(text)=socket.next().await.unwrap().unwrap() else {continue};
            match crate::protocol::parse::<ServerWire>(text.as_bytes()).unwrap() {
                ServerWire::ErasedProjection{header}=>{
                    assert_eq!(header.id,accepted.id);assert_eq!(header.binding_digest,accepted.binding_digest);
                    assert!(!f.journal.accept_erased_projection(&header).await.unwrap());break;
                },
                ServerWire::Data{envelope,permitted}=>{f.journal.accept(&envelope,permitted,1,true).await.unwrap();},
                _=>panic!("unexpected erasure response"),
            }
        }
        assert!(cache(&f).await.is_none());
        socket.close(None).await.unwrap();
    }).await.expect("bounded loopback erasure");
}
