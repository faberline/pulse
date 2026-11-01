use pulse_core::Store;
use pulse_link::{protocol::*, server::Hub};

async fn setup(role: ClientRole) -> (tempfile::TempDir, Hub, Credential, u64, i64) {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    let now = pulse_link::now();
    let (cred, epoch) = pair(&hub, role, now).await;
    (dir, hub, cred, epoch, now)
}
async fn pair(hub: &Hub, role: ClientRole, now: i64) -> (Credential, u64) {
    let code = hub.pairing(role, now).await.unwrap();
    let cred = hub
        .redeem(
            PairRequest {
                code,
                role,
                device: uuid::Uuid::new_v4().to_string(),
            },
            now,
        )
        .await
        .unwrap();
    let epoch = hub.begin(&cred.device, 0, now).await.unwrap();
    (cred, epoch)
}
async fn create(hub: &Hub, now: i64) {
    let event=hub.core.ingest(pulse_core::Origin::Human,"fixture","create",serde_json::json!({"commands":[{"kind":"create","id":"task","goal":"fixture task","scope":"pulse:read","priority":0,"requires_device":false}]}),now).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
}
async fn cursor(hub: &Hub) -> i64 {
    sqlx::query_scalar::<_, String>(
        "SELECT value FROM settings WHERE key='chat_core_outbox_cursor'",
    )
    .fetch_optional(&hub.core.application_pool())
    .await
    .unwrap()
    .map(|s| s.parse().unwrap())
    .unwrap_or(0)
}
async fn acknowledge(hub: &Hub, device: &str) {
    loop {
        let batch = hub.outgoing(device).await.unwrap();
        if batch.is_empty() {
            break;
        }
        for envelope in batch {
            hub.ack(device, envelope.sequence.parse().unwrap(), &envelope.id)
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn acknowledged_chat_queries_do_not_fill_the_domain_notification_queue() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    let now = pulse_link::now();
    let code = hub.pairing(ClientRole::Chat, now).await.unwrap();
    let cred = hub
        .redeem(
            PairRequest {
                code,
                role: ClientRole::Chat,
                device: uuid::Uuid::new_v4().to_string(),
            },
            now,
        )
        .await
        .unwrap();
    let epoch = hub.begin(&cred.device, 0, now).await.unwrap();
    for n in 0..1001u64 {
        let envelope = Envelope::new(
            &cred.device,
            n + 1,
            epoch,
            DevicePayload::Human {
                source_id: format!("query-{n}"),
                action: HumanAction::Submit {
                    id: format!("unused-{n}"),
                    goal: "目前進度".into(),
                },
            },
        )
        .unwrap();
        hub.receive(&cred.device, &envelope, now).await.unwrap();
        for outbound in hub.outgoing(&cred.device).await.unwrap() {
            hub.ack(
                &cred.device,
                outbound.sequence.parse().unwrap(),
                &outbound.id,
            )
            .await
            .unwrap();
        }
        let rejected: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM link_chat WHERE body LIKE '要求已拒絕%'")
                .fetch_one(&hub.core.application_pool())
                .await
                .unwrap();
        assert_eq!(
            rejected,
            0,
            "acknowledged progress query {} was rejected; core notifications: {}",
            n + 1,
            hub.core.health(now).await.unwrap().pending_notifications
        );
    }
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 0);
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
    let notices: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(
        notices, 1001,
        "domain records must be retained, not deleted"
    );
    assert_eq!(cursor(&hub).await, 1001);
    let calls: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(calls, 0);
}

#[tokio::test]
async fn local_executor_and_inactive_chat_do_not_consume_domain_notices() {
    let (_dir, hub, local, epoch, now) = setup(ClientRole::LocalExecutor).await;
    create(&hub, now).await;
    hub.snapshot(&local.device, epoch, now).await.unwrap();
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 1);
    assert_eq!(cursor(&hub).await, 0);
    let (chat, chat_epoch) = pair(&hub, ClientRole::Chat, now).await;
    hub.snapshot(&chat.device, chat_epoch, now + LEASE_MS + 1)
        .await
        .unwrap();
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 1);
    let next = hub.begin(&chat.device, 0, now).await.unwrap();
    hub.snapshot(&chat.device, next, now).await.unwrap();
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
    // A new notice remains pending when that chat identity is revoked.
    let event = hub
        .core
        .ingest(
            pulse_core::Origin::Human,
            "fixture",
            "query",
            serde_json::json!({"commands":[{"kind":"query","ids":["task"]}]}),
            now,
        )
        .await
        .unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    hub.revoke(&chat.device).await.unwrap();
    hub.snapshot(&chat.device, next, now).await.unwrap();
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 1);
}

#[tokio::test]
async fn a_full_transport_queue_does_not_ack_domain_work_or_advance_its_cursor() {
    let (_dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    for _ in 0..1000 {
        hub.snapshot(&chat.device, epoch, now).await.unwrap();
    }
    create(&hub, now).await;
    assert!(matches!(
        hub.snapshot(&chat.device, epoch, now).await,
        Err(pulse_link::Error::Capacity)
    ));
    assert_eq!(cursor(&hub).await, 0);
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 1);
    acknowledge(&hub, &chat.device).await;
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
    let frames = hub.outgoing(&chat.device).await.unwrap();
    assert_eq!(frames.len(), 1);
    assert!(
        matches!(&frames[0].payload,ServerPayload::Snapshot{tasks,..} if tasks.iter().any(|t|t.id=="task"))
    );
}

#[tokio::test]
async fn malformed_or_future_cursors_roll_back_snapshot_and_domain_ack_together() {
    let (_dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    create(&hub, now).await;
    for bad in ["invalid", "-1", "99999"] {
        sqlx::query("INSERT INTO settings(key,value) VALUES('chat_core_outbox_cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(bad).execute(&hub.core.application_pool()).await.unwrap();
        assert!(hub.snapshot(&chat.device, epoch, now).await.is_err());
        assert!(hub.outgoing(&chat.device).await.unwrap().is_empty());
        assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 1);
    }
    sqlx::query("UPDATE settings SET value='0' WHERE key='chat_core_outbox_cursor'")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
}

#[tokio::test]
async fn a_committed_handoff_survives_reopen_before_transport_ack() {
    let (dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    create(&hub, now).await;
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    let original = hub.outgoing(&chat.device).await.unwrap().remove(0);
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
    hub.core.clone().close().await;
    drop(hub);
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    hub.recover().await.unwrap();
    let next = hub.begin(&chat.device, 0, now).await.unwrap();
    assert_ne!(next, epoch);
    let replay = hub.outgoing(&chat.device).await.unwrap();
    assert_eq!(
        serde_json::to_value(&replay[0]).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    hub.ack(
        &chat.device,
        original.sequence.parse().unwrap(),
        &original.id,
    )
    .await
    .unwrap();
    assert!(hub.outgoing(&chat.device).await.unwrap().is_empty());
    hub.snapshot(&chat.device, next, now).await.unwrap();
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
    assert_eq!(cursor(&hub).await, 1);
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
}

#[tokio::test]
async fn human_journal_replays_keep_their_original_epoch_without_losing_handoff() {
    let (_dir, hub, chat, old, now) = setup(ClientRole::Chat).await;
    let next = hub.begin(&chat.device, 0, now).await.unwrap();
    assert_ne!(old, next);
    let envelope = Envelope::new(
        &chat.device,
        1,
        old,
        DevicePayload::Human {
            source_id: "old-journal".into(),
            action: HumanAction::Submit {
                id: "unused".into(),
                goal: "目前進度".into(),
            },
        },
    )
    .unwrap();
    hub.receive(&chat.device, &envelope, now).await.unwrap();
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
    assert_eq!(cursor(&hub).await, 1);
    hub.receive(&chat.device, &envelope, now).await.unwrap();
    assert_eq!(cursor(&hub).await, 1);
    assert!(hub.core.list_tasks(100).await.unwrap().is_empty());
}

#[tokio::test]
async fn concurrent_state_updates_match_the_snapshot_handoff_watermark() {
    let (_dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    create(&hub, now).await;
    let core = hub.core.clone();
    let producer = tokio::spawn(async move {
        for revision in 2..=51 {
            let event=core.ingest(pulse_core::Origin::Human,"fixture",&format!("revise-{revision}"),serde_json::json!({"commands":[{"kind":"revise","id":"task","expected_revision":(revision-1).to_string(),"goal":format!("revision {revision}")}]}),now).await.unwrap();
            assert!(matches!(
                core.route(&event.event_id).await.unwrap(),
                pulse_core::Outcome::Applied { .. }
            ));
            tokio::task::yield_now().await;
        }
    });
    for _ in 0..50 {
        hub.snapshot(&chat.device, epoch, now).await.unwrap();
        let watermark = cursor(&hub).await;
        let frame = hub.outgoing(&chat.device).await.unwrap().remove(0);
        let ServerPayload::Snapshot { tasks, .. } = frame.payload else {
            panic!("snapshot required")
        };
        assert_eq!(tasks[0].revision, watermark as u64);
        acknowledge(&hub, &chat.device).await;
    }
    producer.await.unwrap();
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    assert_eq!(cursor(&hub).await, 51);
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
}

#[tokio::test]
async fn failed_cursor_commit_rolls_back_both_durable_queues() {
    let (_dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    create(&hub, now).await;
    sqlx::query("CREATE TRIGGER refuse_handoff BEFORE INSERT ON settings WHEN NEW.key='chat_core_outbox_cursor' BEGIN SELECT RAISE(ABORT,'fixture cursor commit failed'); END").execute(&hub.core.application_pool()).await.unwrap();
    assert!(hub.snapshot(&chat.device, epoch, now).await.is_err());
    assert!(hub.outgoing(&chat.device).await.unwrap().is_empty());
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 1);
    assert_eq!(cursor(&hub).await, 0);
    sqlx::query("DROP TRIGGER refuse_handoff")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    assert_eq!(cursor(&hub).await, 1);
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
}

async fn fill_domain(hub: &Hub, amount: i64) {
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO outbox(dedupe_key,payload) SELECT 'fixture-full:'||i,'{}' FROM n").bind(amount).execute(&hub.core.application_pool()).await.unwrap();
}

#[tokio::test]
async fn full_domain_capacity_keeps_human_delivery_pending_until_safe_replay() {
    let (_dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    fill_domain(&hub, 1000).await;
    let envelope = Envelope::new(
        &chat.device,
        1,
        epoch,
        DevicePayload::Human {
            source_id: "retry-input".into(),
            action: HumanAction::Submit {
                id: "task".into(),
                goal: "讀 README.md".into(),
            },
        },
    )
    .unwrap();
    assert!(matches!(
        hub.receive(&chat.device, &envelope, now).await,
        Err(pulse_link::Error::Core(pulse_core::Error::Capacity))
    ));
    assert!(hub.core.list_tasks(100).await.unwrap().is_empty());
    let outcome: Option<String> =
        sqlx::query_scalar("SELECT outcome FROM link_inbox WHERE device=? AND sequence=1")
            .bind(&chat.device)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert!(outcome.is_none());
    let rejected: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM link_chat WHERE body LIKE '要求已拒絕%'")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!(rejected, 0);
    // A reconnect/refresh transfers the backlog before replaying the original journal entry.
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    // Durable ingress may finish before the device repeats its unacknowledged envelope.
    assert!(pulse_link::remote::step(&hub, now).await.unwrap());
    assert_eq!(
        hub.core.task("task").await.unwrap().state,
        pulse_core::State::Queued
    );
    hub.receive(&chat.device, &envelope, now).await.unwrap();
    hub.receive(&chat.device, &envelope, now).await.unwrap();
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
    assert_eq!(hub.core.health(now).await.unwrap().pending_notifications, 0);
}

#[tokio::test]
async fn a_full_domain_queue_does_not_claim_work_or_start_model_calls() {
    let (_dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    create(&hub, now).await;
    fill_domain(&hub, 999).await;
    assert!(!pulse_link::remote::step(&hub, now).await.unwrap());
    let task = hub.core.task("task").await.unwrap();
    assert_eq!(task.state, pulse_core::State::Queued);
    assert!(task.active_run.is_none());
    assert_eq!(task.model_calls, 0);
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    assert!(pulse_link::remote::step(&hub, now).await.unwrap());
    assert_eq!(
        hub.core.task("task").await.unwrap().state,
        pulse_core::State::Blocked
    );
}

#[tokio::test]
async fn capacity_racing_a_claim_does_not_orphan_its_local_completion() {
    let (_dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    create(&hub, now).await;
    fill_domain(&hub, 998).await;
    // Fill the last slot after the scheduler's capacity check, inside its claim commit.
    sqlx::query("CREATE TRIGGER capacity_race AFTER UPDATE ON tasks WHEN json_extract(NEW.data,'$.state')='planning' AND (SELECT COUNT(*) FROM outbox WHERE acknowledged=0)=999 BEGIN INSERT INTO outbox(dedupe_key,payload) VALUES('race:last','{}'); END").execute(&hub.core.application_pool()).await.unwrap();
    let runner = hub.clone();
    let worker = tokio::spawn(async move { pulse_link::remote::step(&runner, now).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if hub.core.health(now).await.unwrap().pending_notifications == 1000 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    );
    let task = hub.core.task("task").await.unwrap();
    assert_eq!(task.state, pulse_core::State::Blocked);
    assert_eq!(task.reason.as_deref(), Some("provider_not_configured"));
    assert_eq!(task.model_calls, 0);
    let calls: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(calls, 0);
}

#[tokio::test]
async fn deferred_cancellation_is_routed_before_recovered_work_can_start() {
    let (_dir, hub, chat, epoch, now) = setup(ClientRole::Chat).await;
    create(&hub, now).await;
    fill_domain(&hub, 999).await;
    let event = hub
        .core
        .ingest(
            pulse_core::Origin::Human,
            "fixture",
            "deferred-cancel",
            serde_json::json!({"commands":[{"kind":"cancel","id":"task","expected_revision":"1"}]}),
            now,
        )
        .await
        .unwrap();
    assert!(!pulse_link::remote::step(&hub, now).await.unwrap());
    assert_eq!(
        hub.core.task("task").await.unwrap().state,
        pulse_core::State::Queued
    );
    hub.snapshot(&chat.device, epoch, now).await.unwrap();
    assert!(pulse_link::remote::step(&hub, now).await.unwrap());
    let task = hub.core.task("task").await.unwrap();
    assert_eq!(task.state, pulse_core::State::Cancelled);
    assert_eq!(task.model_calls, 0);
    assert!(matches!(
        hub.core.route(&event.event_id).await.unwrap(),
        pulse_core::Outcome::Applied { .. }
    ));
    assert!(hub.core.pending_events(100).await.unwrap().is_empty());
}
