use super::*;

// Direct journal/server fixtures declare the current Rust chat contract.
// The real command-loop test opens a new epoch and must declare it on the wire.
async fn setup() -> Fixture {
    let f = super::setup().await;
    let epoch = f
        .hub
        .begin(&f.journal.device, 0, crate::now())
        .await
        .unwrap();
    f.hub
        .conversation_ready(&f.journal.device, epoch, 1, crate::now())
        .await
        .unwrap();
    f
}

async fn deliver(f: &Fixture) {
    for frame in f.journal.outgoing().await.unwrap() {
        f.hub
            .receive(&f.journal.device, &frame, crate::now())
            .await
            .unwrap();
        f.journal
            .ack(number(&frame.sequence).unwrap(), &frame.id)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn memory_topic_query_breaks_implicit_task_focus_before_followup() {
    let f = setup().await;
    say(&f, "任務 A：讀 README.md").await;
    deliver(&f).await;
    let original = f.hub.core.list_tasks(100).await.unwrap().remove(0);
    say(&f, "查詢記憶").await;
    assert!(f.journal.outgoing().await.unwrap().is_empty());
    let source = say(&f, "再詳細一點").await;
    deliver(&f).await;
    assert_eq!(f.hub.core.task(&original.id).await.unwrap().revision, 1);
    assert_eq!(f.hub.core.list_tasks(100).await.unwrap().len(), 1);
    let body: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
        .bind(format!("reply:{source}"))
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert!(body.contains("哪個任務"));
    say(&f, "A").await;
    deliver(&f).await;
    assert_eq!(f.hub.core.task(&original.id).await.unwrap().revision, 2);
}

#[tokio::test]
async fn memory_topic_marker_is_bodyless_coalesced_and_old_replay_cannot_reset_new_focus() {
    let f = setup().await;
    say(&f, "任務 A：讀 README.md").await;
    deliver(&f).await;
    let a = f.hub.core.list_tasks(100).await.unwrap().remove(0);
    say(&f, "記住 私人偏好：PRIVATE_TOPIC_BODY").await;
    let query_source = say(&f, "查詢記憶").await;
    say(&f, "記住").await; // Invalid syntax is still a Human topic change.
    assert!(f.journal.outgoing().await.unwrap().is_empty());
    let source = say(&f, "再詳細一點").await;
    let frames = f.journal.outgoing().await.unwrap();
    assert_eq!(frames.len(), 2);
    assert!(
        !serde_json::to_string(&frames)
            .unwrap()
            .contains("PRIVATE_TOPIC_BODY")
    );
    let DevicePayload::Human { action, .. } = &frames[0].payload else {
        panic!("missing topic boundary")
    };
    assert_eq!(
        serde_json::to_value(action).unwrap(),
        serde_json::json!({"kind":"conversation_boundary"})
    );
    assert!(
        serde_json::from_value::<HumanAction>(
            serde_json::json!({"kind":"conversation_boundary","text":"PRIVATE_FORGED_BODY"})
        )
        .is_err()
    );
    assert_eq!(
        number(&frames[1].sequence).unwrap(),
        number(&frames[0].sequence).unwrap() + 1
    );
    deliver(&f).await;
    assert_eq!(f.hub.core.task(&a.id).await.unwrap().revision, 1);
    say(&f, "A").await;
    deliver(&f).await;
    assert_eq!(f.hub.core.task(&a.id).await.unwrap().revision, 2);
    let DevicePayload::Human {
        source_id: boundary_source,
        ..
    } = &frames[0].payload
    else {
        unreachable!()
    };
    f.journal
        .action(boundary_source, HumanAction::ConversationBoundary {})
        .await
        .unwrap();
    deliver(&f).await; // A new transport frame carrying an old source is also a replay.
    let code = f
        .hub
        .pairing(ClientRole::LocalExecutor, crate::now())
        .await
        .unwrap();
    let worker = f
        .hub
        .redeem(
            PairRequest {
                code,
                device: uuid::Uuid::new_v4().to_string(),
                role: ClientRole::LocalExecutor,
            },
            crate::now(),
        )
        .await
        .unwrap();
    let forged = Envelope::new(
        &worker.device,
        1,
        0,
        DevicePayload::Human {
            source_id: uuid::Uuid::new_v4().to_string(),
            action: HumanAction::ConversationBoundary {},
        },
    )
    .unwrap();
    assert!(
        f.hub
            .receive(&worker.device, &forged, crate::now())
            .await
            .is_err()
    );
    // An already accepted local memory source must not clear a newer task focus.
    f.journal
        .action(
            &query_source,
            HumanAction::Submit {
                id: uuid::Uuid::new_v4().to_string(),
                goal: "查詢記憶".into(),
            },
        )
        .await
        .unwrap();
    assert!(f.journal.outgoing().await.unwrap().is_empty());
    say(&f, "用表格整理").await;
    assert_eq!(f.journal.outgoing().await.unwrap().len(), 1);
    deliver(&f).await;
    assert_eq!(f.hub.core.task(&a.id).await.unwrap().revision, 3);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    let event: String = sqlx::query_scalar("SELECT payload FROM events WHERE source_id=?")
        .bind(source)
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert!(!event.contains("PRIVATE_TOPIC_BODY"));
}

#[tokio::test]
async fn memory_topic_boundary_survives_chat_journal_restart() {
    let _permit = MEMORY_FIXTURES.acquire().await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("chat");
    let server = "https://pulse.example/";
    let (journal, lock) = Journal::open_role(&path, server, root.path(), ClientRole::Chat)
        .await
        .unwrap();
    let credential = Credential {
        device: journal.device.clone(),
        token: "a".repeat(64),
        role: ClientRole::Chat,
    };
    journal.bind_memory_credential(&credential).await.unwrap();
    let source = uuid::Uuid::new_v4().to_string();
    journal
        .action(
            &source,
            HumanAction::Submit {
                id: "unused-query-id".into(),
                goal: "我的記憶".into(),
            },
        )
        .await
        .unwrap();
    assert!(journal.outgoing().await.unwrap().is_empty());
    drop(journal);
    drop(lock);
    let (journal, _lock) = Journal::open_role(&path, server, root.path(), ClientRole::Chat)
        .await
        .unwrap();
    journal.bind_memory_credential(&credential).await.unwrap();
    journal
        .action(
            &uuid::Uuid::new_v4().to_string(),
            HumanAction::Submit {
                id: "followup".into(),
                goal: "再詳細一點".into(),
            },
        )
        .await
        .unwrap();
    let frames = journal.outgoing().await.unwrap();
    assert_eq!(frames.len(), 2);
    assert!(
        matches!(&frames[0].payload,DevicePayload::Human {source_id,action:HumanAction::ConversationBoundary {}} if source_id==&format!("topic:{source}"))
    );
    assert!(
        matches!(&frames[1].payload,DevicePayload::Human {action:HumanAction::Submit {id,..},..} if id=="followup")
    );
}

async fn fill_transport(f: &Fixture, first: u64, last: u64) {
    let mut tx = f.journal.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    for sequence in first..=last {
        let frame = Envelope::new(
            &f.journal.device,
            sequence,
            0,
            DevicePayload::Human {
                source_id: format!("filler-{sequence}"),
                action: HumanAction::Status,
            },
        )
        .unwrap();
        sqlx::query("INSERT INTO local_outbox VALUES(?,?,?)")
            .bind(sequence as i64)
            .bind(&frame.id)
            .bind(serde_json::to_string(&frame).unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE local_meta SET value=? WHERE key='next'")
        .bind(last.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn memory_topic_capacity_rolls_back_input_and_never_blocks_source_deletion() {
    let f = setup().await;
    say(&f, "記住 偏好：PRIVATE_CAPACITY_BODY").await;
    fill_transport(&f, 1, 999).await;
    let input = uuid::Uuid::new_v4().to_string();
    let action = HumanAction::Submit {
        id: "not-yet-queued".into(),
        goal: "再詳細一點".into(),
    };
    assert!(matches!(
        f.journal.action(&input, action.clone()).await,
        Err(crate::Error::Capacity)
    ));
    assert_eq!(count(&f, "SELECT count(*) FROM local_outbox").await, 999);
    assert_eq!(count(&f, "SELECT count(*) FROM local_pending").await, 0);
    fill_transport(&f, 1000, 1000).await;
    let cancel = say(&f, "取消待送記憶 偏好").await;
    assert_eq!(count(&f, "SELECT count(*) FROM local_outbox").await, 1000);
    let requests = f.journal.memory_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(matches!(
        requests[0].command,
        pulse_core::MemoryCommand::Revoke { .. }
    ));
    assert!(
        !serde_json::to_string(&requests)
            .unwrap()
            .contains("PRIVATE_CAPACITY_BODY")
    );
    sqlx::query("DELETE FROM local_outbox")
        .execute(&f.journal.db)
        .await
        .unwrap();
    f.journal.action(&input, action).await.unwrap();
    let frames = f.journal.outgoing().await.unwrap();
    assert_eq!(frames.len(), 2);
    assert!(
        matches!(&frames[0].payload,DevicePayload::Human {source_id,action:HumanAction::ConversationBoundary {}} if source_id==&format!("topic:{cancel}"))
    );
    assert_eq!(number(&frames[0].sequence).unwrap(), 1001);
}

#[tokio::test]
async fn memory_topic_real_rust_command_loop_clarifies_then_revises_only_selected_task() {
    use tokio::sync::{mpsc, watch};
    async fn wait(
        state: &mut watch::Receiver<NativeState>,
        condition: impl Fn(&NativeState) -> bool,
    ) -> NativeState {
        tokio::time::timeout(std::time::Duration::from_secs(5), state.wait_for(condition))
            .await
            .unwrap()
            .unwrap()
            .clone()
    }
    let f = setup().await;
    let config = Config {
        server: f.config.server.clone(),
        repo: PathBuf::new(),
        data_dir: f._root.path().join("topic-native-chat"),
    };
    let device = crate::client::Device::open_chat(config, Arc::new(MemoryVault::default()))
        .await
        .unwrap();
    let (commands, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let worker = tokio::spawn(device.run(receiver, states));
    f.hub.revoke(&f.credential.device).await.unwrap();
    let code = f.hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    commands.send(NativeCommand::Pair { code }).await.unwrap();
    wait(&mut state, |s| s.connected).await;
    commands
        .send(NativeCommand::Action {
            source_id: uuid::Uuid::new_v4().to_string(),
            action: HumanAction::Submit {
                id: "wire-task".into(),
                goal: "任務 A：讀 README.md".into(),
            },
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.tasks
            .iter()
            .any(|t| t.id == "wire-task" && t.revision == 1)
    })
    .await;
    let declared: i64 = sqlx::query_scalar("SELECT count(*) FROM settings s JOIN link_device d ON s.key='chat-context:' || d.id WHERE d.role='chat' AND d.revoked=0 AND json_extract(s.value,'$.version')=1 AND json_extract(s.value,'$.epoch')=d.epoch")
        .fetch_one(&f.hub.core.application_pool()).await.unwrap();
    assert_eq!(declared, 1);
    let query = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::Action {
            source_id: query.clone(),
            action: HumanAction::Submit {
                id: "not-a-memory-task".into(),
                goal: "我的記憶".into(),
            },
        })
        .await
        .unwrap();
    wait(&mut state, |s| s.messages.iter().any(|m| m.id == query)).await;
    let source = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::Action {
            source_id: source.clone(),
            action: HumanAction::Submit {
                id: "not-a-followup-task".into(),
                goal: "再詳細一點".into(),
            },
        })
        .await
        .unwrap();
    let clarified = wait(&mut state, |s| {
        s.messages
            .iter()
            .any(|m| m.id == format!("reply:{source}") && m.body.contains("哪個任務"))
    })
    .await;
    assert!(
        clarified
            .tasks
            .iter()
            .any(|t| t.id == "wire-task" && t.revision == 1)
    );
    commands
        .send(NativeCommand::Action {
            source_id: uuid::Uuid::new_v4().to_string(),
            action: HumanAction::Submit {
                id: "not-a-selection-task".into(),
                goal: "A".into(),
            },
        })
        .await
        .unwrap();
    wait(&mut state, |s| {
        s.tasks
            .iter()
            .any(|t| t.id == "wire-task" && t.revision == 2)
    })
    .await;
    assert_eq!(f.hub.core.list_tasks(100).await.unwrap().len(), 1);
    assert!(
        f.hub
            .core
            .task("wire-task")
            .await
            .unwrap()
            .goal
            .ends_with("補充要求：再詳細一點")
    );
    let budget: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(budget, 0);
    commands.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
