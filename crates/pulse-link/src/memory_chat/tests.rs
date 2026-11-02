use super::parse;
use super::*;
use crate::{
    client::Config,
    journal::{DeviceLock, Journal},
    protocol::*,
    server::{Hub, router},
};
use pulse_core::{MemoryRequest, MemoryView, Origin, Store};
use std::{
    future::IntoFuture,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

// Each fixture fsyncs independent server and chat databases. Bound this test
// workload so it cannot starve unrelated native process deadline checks.
static MEMORY_FIXTURES: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

struct Fixture {
    _root: tempfile::TempDir,
    hub: Hub,
    journal: Journal,
    _lock: DeviceLock,
    credential: Credential,
    config: Config,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
    writes: Arc<AtomicUsize>,
    _permit: tokio::sync::SemaphorePermit<'static>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn setup() -> Fixture {
    let permit = MEMORY_FIXTURES.acquire().await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let config = Config {
        server: url,
        repo: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf(),
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
    let code = hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    let credential = hub
        .redeem(
            PairRequest {
                code,
                device: journal.device.clone(),
                role: ClientRole::Chat,
            },
            crate::now(),
        )
        .await
        .unwrap();
    let writes = Arc::new(AtomicUsize::new(0));
    let counter = writes.clone();
    let app = router(hub.clone()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            if request.method() == axum::http::Method::POST
                && request.uri().path() == "/v1/memories"
            {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            async move { next.run(request).await }
        },
    ));
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    Fixture {
        _root: root,
        hub,
        journal,
        _lock: lock,
        credential,
        config,
        server,
        writes,
        _permit: permit,
    }
}
async fn say(f: &Fixture, text: &str) -> String {
    let source = uuid::Uuid::new_v4().to_string();
    f.journal
        .action(
            &source,
            HumanAction::Submit {
                id: uuid::Uuid::new_v4().to_string(),
                goal: text.into(),
            },
        )
        .await
        .unwrap();
    source
}
async fn sync(f: &Fixture) {
    client::sync(&f.config, &f.journal, &f.credential)
        .await
        .unwrap();
}
async fn count(f: &Fixture, query: &'static str) -> i64 {
    sqlx::query_scalar(query)
        .fetch_one(&f.journal.db)
        .await
        .unwrap()
}

#[tokio::test]
async fn cancelling_an_offline_source_erases_payload_and_fences_late_delivery() {
    let f = setup().await;
    let source = say(&f, "記住 離線偏好：PRIVATE_CANCELLED_PENDING").await;
    let captured = f.journal.memory_requests().await.unwrap().remove(0);
    let cancel = say(&f, "取消待送記憶 離線偏好").await;
    assert!(!f.journal.memory_request_active(&captured).await.unwrap());
    assert!(f.journal.pending_memory_revocation().await.unwrap());
    let requests = f.journal.memory_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].source_id, cancel);
    assert!(
        matches!(&requests[0].command, pulse_core::MemoryCommand::Revoke { sources, expected: None } if sources == &vec![source])
    );
    let payload: String =
        sqlx::query_scalar("SELECT group_concat(coalesce(request,'')) FROM local_memory_queue")
            .fetch_one(&f.journal.db)
            .await
            .unwrap();
    assert!(!payload.contains("PRIVATE_CANCELLED_PENDING"));
    assert!(!payload.contains("離線偏好"));
    sync(&f).await;
    assert_eq!(f.writes.load(Ordering::SeqCst), 1); // Only the bodyless revocation.
    let response = reqwest::Client::new()
        .post(f.config.server.join("v1/memories").unwrap())
        .bearer_auth(&f.credential.token)
        .json(&captured)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(matches!(
        response.json::<pulse_core::MemoryOutcome>().await.unwrap(),
        pulse_core::MemoryOutcome::Redacted { .. }
    ));
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(f.journal.outgoing().await.unwrap().is_empty());
}

#[tokio::test]
async fn forget_cancels_an_accepted_source_when_the_ack_was_lost() {
    let f = setup().await;
    say(&f, "記住 尚未確認：PRIVATE_LOST_ACK").await;
    let captured = f.journal.memory_requests().await.unwrap().remove(0);
    f.hub
        .core
        .change_memory(Origin::Human, &captured, crate::now())
        .await
        .unwrap();
    // No ACK and no cache: deletion must still address this pending source.
    assert!(f.journal.view().await.unwrap().memories.is_empty());
    say(&f, "忘記記憶 尚未確認").await;
    sync(&f).await;
    assert_eq!(f.writes.load(Ordering::SeqCst), 1);
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        count(
            &f,
            "SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn cancelling_a_pending_correction_preserves_old_data_only_if_it_never_committed() {
    for committed in [false, true] {
        let f = setup().await;
        say(&f, "記住 修正偏好：old").await;
        sync(&f).await;
        say(&f, "更正記憶 修正偏好：PRIVATE_PENDING_NEW").await;
        let captured = f.journal.memory_requests().await.unwrap().remove(0);
        if committed {
            f.hub
                .core
                .change_memory(Origin::Human, &captured, crate::now())
                .await
                .unwrap();
        }
        say(&f, "取消待送記憶 修正偏好").await;
        sync(&f).await;
        let memories = f.hub.core.list_memories(crate::now()).await.unwrap();
        if committed {
            assert!(memories.is_empty());
        } else {
            assert_eq!(memories.len(), 1);
            assert_eq!(memories[0].body, "old");
        }
        assert!(matches!(
            f.hub
                .core
                .change_memory(Origin::Human, &captured, crate::now())
                .await
                .unwrap(),
            pulse_core::MemoryOutcome::Redacted { .. }
        ));
    }
}

#[tokio::test]
async fn forgetting_during_an_unacknowledged_correction_removes_both_possible_versions() {
    for committed in [false, true] {
        let f = setup().await;
        say(&f, "記住 刪除偏好：old").await;
        sync(&f).await;
        say(&f, "更正記憶 刪除偏好：new").await;
        let captured = f.journal.memory_requests().await.unwrap().remove(0);
        if committed {
            f.hub
                .core
                .change_memory(Origin::Human, &captured, crate::now())
                .await
                .unwrap();
        }
        say(&f, "忘記記憶 刪除偏好").await;
        sync(&f).await;
        assert!(
            f.hub
                .core
                .list_memories(crate::now())
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn full_write_queue_still_accepts_query_and_prioritizes_deletion() {
    let f = setup().await;
    say(&f, "記住 現有：old").await;
    sync(&f).await;
    for n in 0..100 {
        say(&f, &format!("記住 待送{n}：body")).await;
    }
    assert!(
        f.journal
            .memory_control(
                &uuid::Uuid::new_v4().to_string(),
                "記住 超額：body",
                crate::now()
            )
            .await
            .is_err()
    );
    say(&f, "查詢記憶").await;
    say(&f, "忘記記憶 現有").await;
    let requests = f.journal.memory_requests().await.unwrap();
    assert_eq!(requests.len(), 101);
    assert!(matches!(
        requests[0].command,
        pulse_core::MemoryCommand::Revoke { .. }
    ));
    sync(&f).await;
    let memories = f.hub.core.list_memories(crate::now()).await.unwrap();
    assert_eq!(memories.len(), 100);
    assert!(memories.iter().all(|m| m.label != "現有"));
    assert_eq!(
        count(
            &f,
            "SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn local_source_aliases_keep_the_original_ui_identity_and_ack_the_same_request() {
    let f = setup().await;
    let id = uuid::Uuid::new_v4();
    let source = id.to_string().to_uppercase();
    f.journal
        .memory_control(&source, "記住 身分：body", crate::now())
        .await
        .unwrap();
    f.journal
        .memory_control(
            &id.simple().to_string(),
            "記住 不可替換：other",
            crate::now(),
        )
        .await
        .unwrap();
    let requests = f.journal.memory_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        f.journal
            .view()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|m| m.id == source)
    );
    f.journal
        .finish_memory(&id.braced().to_string(), "accepted")
        .await
        .unwrap();
    assert!(f.journal.memory_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn one_composer_remembers_corrects_queries_and_forgets_without_task_history() {
    let f = setup().await;
    let source = say(&f, "記住 回答語言：PRIVATE_OLD_繁體中文").await;
    assert!(f.journal.outgoing().await.unwrap().is_empty());
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    let view = f.journal.view().await.unwrap();
    assert_eq!(view.pending, 1);
    assert!(
        view.messages
            .iter()
            .any(|m| m.id == source && m.body.contains("待送"))
    );
    assert!(
        !serde_json::to_string(&view)
            .unwrap()
            .contains("PRIVATE_OLD")
    );
    sync(&f).await;
    let old = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    assert_eq!(old.source_id, source);
    assert_eq!(
        count(
            &f,
            "SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL"
        )
        .await,
        0
    );
    assert_eq!(
        f.journal.view().await.unwrap().memories[0].body,
        "PRIVATE_OLD_繁體中文"
    );
    say(&f, "更正記憶 回答語言：PRIVATE_NEW_簡短繁體中文").await;
    assert!(f.journal.view().await.unwrap().memories.is_empty());
    sync(&f).await;
    let current = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    assert_eq!(current.id, old.id);
    assert_eq!(current.revision, 2);
    assert_eq!(current.body, "PRIVATE_NEW_簡短繁體中文");
    let query = say(&f, "查詢記憶").await;
    sync(&f).await;
    assert!(
        f.journal
            .view()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|m| m.id == query && m.body.contains("已核對"))
    );
    say(&f, "忘記記憶 回答語言").await;
    assert!(f.journal.view().await.unwrap().memories.is_empty());
    assert_eq!(
        f.hub.core.list_memories(crate::now()).await.unwrap().len(),
        1
    ); // Offline local clearing is not server confirmation.
    sync(&f).await;
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        count(&f, "SELECT count(*) FROM local_memory_cache").await,
        0
    );
    for query in [
        "SELECT count(*) FROM events",
        "SELECT count(*) FROM tasks",
        "SELECT count(*) FROM budget",
        "SELECT count(*) FROM link_chat",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(query)
                .fetch_one(&f.hub.core.application_pool())
                .await
                .unwrap(),
            0
        );
    }
    let bytes = std::fs::read(f.config.data_dir.join("device.db")).unwrap();
    for marker in [b"PRIVATE_OLD".as_slice(), b"PRIVATE_NEW".as_slice()] {
        assert!(!bytes.windows(marker.len()).any(|b| b == marker));
    }
}

#[tokio::test]
async fn lost_ack_then_server_deletion_purges_pending_source_before_any_replay() {
    let f = setup().await;
    say(&f, "記住 偏好：PRIVATE_PENDING_BODY").await;
    let request = f.journal.memory_requests().await.unwrap().remove(0);
    f.hub
        .memory_change(&f.credential.device, &request, crate::now())
        .await
        .unwrap(); // Accepted, but no local acknowledgement.
    let old = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    let deletion = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Forget {
            id: old.id,
            revision: old.revision,
        },
    };
    f.hub
        .core
        .change_memory(Origin::Human, &deletion, crate::now())
        .await
        .unwrap();
    sync(&f).await;
    assert_eq!(f.writes.load(Ordering::SeqCst), 0);
    assert!(f.journal.memory_requests().await.unwrap().is_empty());
    assert!(f.journal.view().await.unwrap().memories.is_empty());
    let bytes = std::fs::read(f.config.data_dir.join("device.db")).unwrap();
    assert!(
        !bytes
            .windows(b"PRIVATE_PENDING_BODY".len())
            .any(|b| b == b"PRIVATE_PENDING_BODY")
    );
}

#[tokio::test]
async fn stale_offline_correction_keeps_its_original_revision_and_never_overwrites_newer_source() {
    let f = setup().await;
    say(&f, "記住 偏好：old").await;
    sync(&f).await;
    let old = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    say(&f, "更正記憶 偏好：stale local edit").await;
    let original = f.journal.memory_requests().await.unwrap().remove(0);
    let other = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Correct {
            id: old.id,
            revision: 1,
            label: "偏好".into(),
            body: "new Human source".into(),
        },
    };
    f.hub
        .memory_change(&f.credential.device, &other, crate::now())
        .await
        .unwrap();
    let frozen = f.journal.memory_requests().await.unwrap().remove(0);
    assert_eq!(frozen.source_id, original.source_id);
    assert_eq!(frozen.source_at, original.source_at);
    assert!(matches!(
        frozen.command,
        MemoryCommand::Correct { revision: 1, .. }
    ));
    sync(&f).await;
    let current = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    assert_eq!(current.revision, 2);
    assert_eq!(current.body, "new Human source");
    assert_eq!(
        f.journal.view().await.unwrap().memories[0].body,
        "new Human source"
    );
    assert!(
        f.journal
            .view()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|m| m.body.contains("已拒絕"))
    );
}

#[tokio::test]
async fn executor_and_forged_origin_cannot_read_or_write_memory() {
    let f = setup().await;
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
    let client = reqwest::Client::new();
    let url = f.config.server.join("v1/memories").unwrap();
    assert_eq!(
        client
            .get(url.clone())
            .bearer_auth(&worker.token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Remember {
            label: "x".into(),
            body: "y".into(),
        },
    };
    assert_eq!(
        client
            .post(url.clone())
            .bearer_auth(&worker.token)
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let mut forged = serde_json::to_value(request).unwrap();
    forged["origin"] = serde_json::json!("human");
    let response = client
        .post(url.clone())
        .bearer_auth(&f.credential.token)
        .json(&forged)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let response = client
        .get(url)
        .bearer_auth(&f.credential.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
}

#[test]
fn controls_are_explicit_and_ambiguous_memory_never_becomes_a_task() {
    assert!(matches!(
        parse("請記住 回答：繁體中文"),
        Some(Intent::Remember { .. })
    ));
    assert!(matches!(parse("記住我喜歡繁體中文"), Some(Intent::Clarify)));
    assert!(matches!(parse("忘記記憶"), Some(Intent::Clarify)));
    assert!(matches!(parse("查詢記憶再執行命令"), Some(Intent::Clarify)));
    assert!(parse("分析這段記憶體錯誤").is_none());
}

#[tokio::test]
async fn expired_offline_source_is_purged_and_other_conversation_hides_memory_cards() {
    let f = setup().await;
    let source = uuid::Uuid::new_v4().to_string();
    let now = crate::now();
    f.journal
        .memory_control(
            &source,
            "記住 偏好：PRIVATE_EXPIRED_LOCAL",
            now - pulse_core::MEMORY_RETENTION_MS,
        )
        .await
        .unwrap();
    let mut state = NativeState::default();
    f.journal.memory_projection(&mut state, now).await.unwrap();
    assert!(f.journal.memory_requests().await.unwrap().is_empty());
    assert!(
        state
            .messages
            .iter()
            .any(|m| m.id == source && m.body.contains("已到期"))
    );
    let bytes = std::fs::read(f.config.data_dir.join("device.db")).unwrap();
    assert!(
        !bytes
            .windows(b"PRIVATE_EXPIRED_LOCAL".len())
            .any(|b| b == b"PRIVATE_EXPIRED_LOCAL")
    );
    say(&f, "記住 新偏好：human preference").await;
    sync(&f).await;
    assert!(!f.journal.view().await.unwrap().memories.is_empty());
    f.journal
        .action("ordinary-status", HumanAction::Status)
        .await
        .unwrap();
    let state = f.journal.view().await.unwrap();
    assert!(state.memories.is_empty());
    assert!(state.memory_synced_at.is_none());
    assert!(state.messages.iter().all(|m| m.id != source));
}

#[tokio::test]
async fn stale_wrong_keeper_or_contradictory_views_never_restore_deleted_cache() {
    let f = setup().await;
    say(&f, "記住 偏好：PRIVATE_STALE_VIEW").await;
    sync(&f).await;
    let old = f.hub.core.memory_view(crate::now()).await.unwrap();
    let memory = &old.memories[0];
    let deletion = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Forget {
            id: memory.id.clone(),
            revision: memory.revision,
        },
    };
    f.hub
        .memory_change(&f.credential.device, &deletion, crate::now())
        .await
        .unwrap();
    let current = f.hub.core.memory_view(crate::now()).await.unwrap();
    f.journal
        .apply_memory_view(&current, crate::now())
        .await
        .unwrap();
    assert!(
        f.journal
            .apply_memory_view(&old, crate::now())
            .await
            .is_err()
    );
    let wrong = MemoryView {
        keeper: uuid::Uuid::new_v4().to_string(),
        epoch: current.epoch,
        memories: vec![],
        redacted_sources: vec![],
    };
    assert!(
        f.journal
            .apply_memory_view(&wrong, crate::now())
            .await
            .is_err()
    );
    let contradictory = MemoryView {
        keeper: current.keeper.clone(),
        epoch: current.epoch,
        memories: old.memories,
        redacted_sources: current.redacted_sources,
    };
    assert!(
        f.journal
            .apply_memory_view(&contradictory, crate::now())
            .await
            .is_err()
    );
    assert_eq!(
        count(&f, "SELECT count(*) FROM local_memory_cache").await,
        0
    );
}

#[tokio::test]
async fn journal_reopen_clears_unverified_cache_but_keeps_pending_source_and_time() {
    let f = setup().await;
    say(&f, "記住 偏好：PRIVATE_CACHED_ON_REOPEN").await;
    sync(&f).await;
    let path = f._root.path().join("restarted-chat");
    let (journal, lock) = Journal::open_role(
        &path,
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::Chat,
    )
    .await
    .unwrap();
    journal
        .apply_memory_view(
            &f.hub.core.memory_view(crate::now()).await.unwrap(),
            crate::now(),
        )
        .await
        .unwrap();
    let source = uuid::Uuid::new_v4().to_string();
    journal
        .action(
            &source,
            HumanAction::Submit {
                id: "unused-task".into(),
                goal: "記住 新偏好：KEEP_PENDING_SOURCE".into(),
            },
        )
        .await
        .unwrap();
    let pending = journal.memory_requests().await.unwrap().remove(0);
    drop(journal);
    drop(lock);
    let (journal, _lock) = Journal::open_role(
        &path,
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::Chat,
    )
    .await
    .unwrap();
    let replay = journal.memory_requests().await.unwrap().remove(0);
    assert_eq!(replay.source_id, pending.source_id);
    assert_eq!(replay.source_at, pending.source_at);
    assert!(journal.view().await.unwrap().memories.is_empty());
    let bytes = std::fs::read(path.join("device.db")).unwrap();
    assert!(
        !bytes
            .windows(b"PRIVATE_CACHED_ON_REOPEN".len())
            .any(|b| b == b"PRIVATE_CACHED_ON_REOPEN")
    );
}

#[tokio::test]
async fn legacy_memory_text_never_falls_through_to_a_task_or_echoed_history() {
    let f = setup().await;
    let epoch = f
        .hub
        .begin(&f.credential.device, 0, crate::now())
        .await
        .unwrap();
    let envelope = Envelope::new(
        &f.credential.device,
        1,
        epoch,
        DevicePayload::Human {
            source_id: uuid::Uuid::new_v4().to_string(),
            action: HumanAction::Submit {
                id: "unsafe-fallback".into(),
                goal: "記住 偏好：PRIVATE_LEGACY_BODY".into(),
            },
        },
    )
    .unwrap();
    f.hub
        .receive(&f.credential.device, &envelope, crate::now())
        .await
        .unwrap();
    assert!(f.hub.core.list_tasks(100).await.unwrap().is_empty());
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(events, 0);
    let chat: Vec<String> = sqlx::query_scalar("SELECT body FROM link_chat")
        .fetch_all(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        chat.iter()
            .all(|body| !body.contains("PRIVATE_LEGACY_BODY"))
    );
}

#[derive(Default)]
struct MemoryVault(std::sync::Mutex<Option<Credential>>);
impl crate::client::Vault for MemoryVault {
    fn load(&self, _: &str) -> crate::Result<Option<Credential>> {
        Ok(self.0.lock().unwrap().clone())
    }
    fn save(&self, _: &str, value: &Credential) -> crate::Result<()> {
        *self.0.lock().unwrap() = Some(value.clone());
        Ok(())
    }
}

#[tokio::test]
async fn native_command_loop_syncs_memory_and_reports_unsaved_input_without_model_work() {
    use tokio::sync::{mpsc, watch};
    async fn wait(
        label: &str,
        state: &mut watch::Receiver<NativeState>,
        condition: impl Fn(&NativeState) -> bool,
    ) -> NativeState {
        match tokio::time::timeout(std::time::Duration::from_secs(5), state.wait_for(condition))
            .await
        {
            Ok(Ok(value)) => value.clone(),
            result => {
                drop(result);
                panic!("{label}: {:?}", state.borrow());
            }
        }
    }
    let f = setup().await;
    let config = Config {
        server: f.config.server.clone(),
        repo: PathBuf::new(),
        data_dir: f._root.path().join("native-chat"),
    };
    let device = crate::client::Device::open_chat(config, Arc::new(MemoryVault::default()))
        .await
        .unwrap();
    let (commands, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let worker = tokio::spawn(device.run(receiver, states));
    // This disposable server permits one active chat. Release the fixture's
    // direct-HTTP identity before pairing the real command-loop client.
    f.hub.revoke(&f.credential.device).await.unwrap();
    let code = f.hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    commands.send(NativeCommand::Pair { code }).await.unwrap();
    wait("connect", &mut state, |s| s.connected).await;
    let source = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::Action {
            source_id: source.clone(),
            action: HumanAction::Submit {
                id: "not-a-task".into(),
                goal: "記住 回答：NATIVE_OWNED_MEMORY".into(),
            },
        })
        .await
        .unwrap();
    let saved = wait("remember", &mut state, |s| {
        s.memories.iter().any(|m| m.body == "NATIVE_OWNED_MEMORY")
    })
    .await;
    assert!(
        saved
            .messages
            .iter()
            .all(|m| !m.body.contains("NATIVE_OWNED_MEMORY"))
    );
    let deletion = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::Action {
            source_id: deletion.clone(),
            action: HumanAction::Submit {
                id: "also-not-a-task".into(),
                goal: "忘記記憶 回答".into(),
            },
        })
        .await
        .unwrap();
    wait("forget", &mut state, |s| {
        s.memories.is_empty()
            && s.messages
                .iter()
                .any(|m| m.id == deletion && m.body.contains("server 確認"))
    })
    .await;
    let bad = uuid::Uuid::new_v4().to_string();
    commands
        .send(NativeCommand::Action {
            source_id: bad.clone(),
            action: HumanAction::Submit {
                id: "never-a-task".into(),
                goal: "忘記記憶 不存在".into(),
            },
        })
        .await
        .unwrap();
    let failed = wait("input_failure", &mut state, |s| {
        s.input_failure.as_deref() == Some(&bad)
    })
    .await;
    assert!(!failed.messages.iter().any(|m| m.id == bad));
    assert_eq!(f.writes.load(Ordering::SeqCst), 2);
    assert!(f.hub.core.list_tasks(100).await.unwrap().is_empty());
    assert!(
        f.hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .is_empty()
    );
    commands.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn post_commit_checkpoint_busy_is_retryable_and_same_source_never_applies_twice() {
    use sqlx::Connection;
    let f = setup().await;
    say(&f, "記住 偏好：before").await;
    sync(&f).await;
    let old = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    let mut reader = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(f._root.path().join("server.db")),
    )
    .await
    .unwrap();
    sqlx::query("BEGIN").execute(&mut reader).await.unwrap();
    sqlx::query("SELECT * FROM memories")
        .fetch_all(&mut reader)
        .await
        .unwrap();
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: crate::now(),
        command: MemoryCommand::Correct {
            id: old.id,
            revision: 1,
            label: "偏好".into(),
            body: "after".into(),
        },
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .unwrap();
    let url = f.config.server.join("v1/memories").unwrap();
    let started = std::time::Instant::now();
    let response = client
        .post(url.clone())
        .bearer_auth(&f.credential.token)
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert!(started.elapsed() < std::time::Duration::from_secs(3));
    let writer_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(writer_timeout, 5000);
    sqlx::query("ROLLBACK").execute(&mut reader).await.unwrap();
    reader.close().await.unwrap();
    let response = client
        .post(url)
        .bearer_auth(&f.credential.token)
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let current = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    assert_eq!(current.revision, 2);
    assert_eq!(current.body, "after");
}

#[tokio::test]
async fn quoted_card_names_cannot_correct_or_forget_a_different_colon_prefix() {
    let f = setup().await;
    for label in ["回答", "回答:語言\"選擇"] {
        let request = MemoryRequest {
            source_id: uuid::Uuid::new_v4().to_string(),
            source_at: crate::now(),
            command: MemoryCommand::Remember {
                label: label.into(),
                body: "unchanged".into(),
            },
        };
        f.hub
            .memory_change(&f.credential.device, &request, crate::now())
            .await
            .unwrap();
    }
    say(&f, "查詢記憶").await;
    sync(&f).await;
    let quoted = serde_json::to_string("回答:語言\"選擇").unwrap();
    say(&f, &format!("更正記憶 {quoted}：changed")).await;
    sync(&f).await;
    let memories = f.hub.core.list_memories(crate::now()).await.unwrap();
    assert!(
        memories
            .iter()
            .any(|m| m.label == "回答" && m.body == "unchanged" && m.revision == 1)
    );
    assert!(
        memories
            .iter()
            .any(|m| m.label == "回答:語言\"選擇" && m.body == "changed" && m.revision == 2)
    );
    say(&f, &format!("忘記記憶 {quoted}")).await;
    sync(&f).await;
    let memories = f.hub.core.list_memories(crate::now()).await.unwrap();
    assert_eq!(memories.len(), 1);
    assert_eq!(memories[0].label, "回答");
}
