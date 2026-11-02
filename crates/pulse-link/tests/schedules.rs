use pulse_core::{Command, ScheduleRequest, ScheduledAction, Store};
use pulse_link::{
    client::{Config, Device, Vault},
    journal::{DeviceLock, Journal},
    protocol::*,
    server::{Hub, router},
};
use std::{
    future::IntoFuture,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, watch};
static FIXTURES: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
struct Server(tokio::task::JoinHandle<std::io::Result<()>>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[derive(Default)]
struct TestVault(Mutex<Option<Credential>>, Mutex<Option<PairClaim>>);
impl Vault for TestVault {
    fn load(&self, _: &str) -> pulse_link::Result<Option<Credential>> {
        Ok(self.0.lock().unwrap().clone())
    }
    fn save(&self, _: &str, value: &Credential) -> pulse_link::Result<()> {
        *self.0.lock().unwrap() = Some(value.clone());
        Ok(())
    }
    fn pending(&self, _: &str) -> pulse_link::Result<Option<PairClaim>> {
        Ok(self.1.lock().unwrap().clone())
    }
    fn save_pending(&self, _: &str, value: &PairClaim) -> pulse_link::Result<()> {
        *self.1.lock().unwrap() = Some(value.clone());
        Ok(())
    }
    fn clear_pending(&self, _: &str) -> pulse_link::Result<()> {
        *self.1.lock().unwrap() = None;
        Ok(())
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    hub: Hub,
    config: Config,
    journal: Option<Journal>,
    lock: Option<DeviceLock>,
    credential: Credential,
    _server: Server,
    _permit: tokio::sync::SemaphorePermit<'static>,
}
impl Fixture {
    async fn new() -> Self {
        let permit = FIXTURES.acquire().await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let hub = Hub::new(
            Store::open(root.path().join("server.db"), "owner")
                .await
                .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            server: url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap()))
                .unwrap(),
            repo: PathBuf::new(),
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
        let code = hub
            .pairing(ClientRole::Chat, pulse_link::now())
            .await
            .unwrap();
        let credential = hub
            .redeem(
                PairRequest {
                    code,
                    device: journal.device.clone(),
                    role: ClientRole::Chat,
                },
                pulse_link::now(),
            )
            .await
            .unwrap();
        let server = Server(tokio::spawn(
            axum::serve(listener, router(hub.clone())).into_future(),
        ));
        Self {
            _root: root,
            hub,
            config,
            journal: Some(journal),
            lock: Some(lock),
            credential,
            _server: server,
            _permit: permit,
        }
    }
    fn journal(&self) -> &Journal {
        self.journal.as_ref().unwrap()
    }
    async fn count(&self, sql: &'static str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&self.hub.core.application_pool())
            .await
            .unwrap()
    }
    async fn arm(&self, label: &str, delay: i64, action: ScheduledAction) -> ScheduleRequest {
        let now = pulse_link::now();
        let request = ScheduleRequest {
            id: uuid::Uuid::new_v4().to_string(),
            label: label.into(),
            source_at: now,
            due_at: now + delay,
            timezone: "Asia/Taipei".into(),
            action,
        };
        let event = self
            .hub
            .core
            .ingest(
                pulse_core::Origin::Human,
                "fixture",
                &request.id,
                serde_json::json!({"commands":[Command::Schedule {request:request.clone()}]}),
                now,
            )
            .await
            .unwrap();
        assert!(matches!(
            self.hub.core.route(&event.event_id).await.unwrap(),
            pulse_core::Outcome::Schedules { .. }
        ));
        request
    }
}

#[tokio::test]
async fn composer_freezes_relative_time_and_reuses_it_after_retry_reopen_and_late_delivery() {
    let mut f = Fixture::new().await;
    let source = uuid::Uuid::new_v4().to_string();
    let id = uuid::Uuid::new_v4().to_string();
    let action = HumanAction::Submit {
        id,
        goal: "一分鐘後提醒我喝水".into(),
    };
    f.journal().action(&source, action.clone()).await.unwrap();
    let original = f.journal().outgoing().await.unwrap().remove(0);
    let DevicePayload::Human {
        action: HumanAction::Schedule { request, .. },
        ..
    } = &original.payload
    else {
        panic!("schedule was not frozen in Rust");
    };
    let expected = request.clone();
    let journal = f.journal.take().unwrap();
    drop(journal);
    drop(f.lock.take());
    let (journal, lock) = Journal::open_role(
        &f.config.data_dir,
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::Chat,
    )
    .await
    .unwrap();
    f.journal = Some(journal);
    f.lock = Some(lock);
    f.journal().action(&source, action).await.unwrap();
    let replay = f.journal().outgoing().await.unwrap().remove(1);
    assert_eq!(
        hash(&original.payload).unwrap(),
        hash(&replay.payload).unwrap()
    );
    f.hub
        .receive(&f.credential.device, &original, expected.due_at + 5000)
        .await
        .unwrap();
    f.hub
        .receive(&f.credential.device, &replay, expected.due_at + 6000)
        .await
        .unwrap();
    let saved = f.hub.core.schedule(&expected.id).await.unwrap();
    assert_eq!(saved.request, expected);
    assert!(
        f.hub
            .core
            .fire_schedule(expected.due_at + 6000)
            .await
            .unwrap()
            .fired
    );
    assert_eq!(f.count("SELECT count(*) FROM schedules").await, 1);
    assert_eq!(f.count("SELECT count(*) FROM tasks").await, 0);
    assert_eq!(
        f.count("SELECT count(*) FROM outbox WHERE json_extract(payload,'$.kind')='schedule_due'")
            .await,
        1
    );
}

#[tokio::test]
async fn notice_handoff_is_opted_in_durable_noncoalescible_and_replay_safe() {
    let f = Fixture::new().await;
    let request = f
        .arm(
            "water",
            1000,
            ScheduledAction::Remind {
                message: "喝水".into(),
            },
        )
        .await;
    f.hub.core.fire_schedule(request.due_at).await.unwrap();
    let now = request.due_at + 1;
    let epoch = f.hub.begin(&f.credential.device, 0, now).await.unwrap();
    f.hub
        .snapshot(&f.credential.device, epoch, now)
        .await
        .unwrap(); // Legacy chat must not consume the notice.
    assert_eq!(f.count("SELECT count(*) FROM outbox WHERE acknowledged=0 AND json_extract(payload,'$.kind')='schedule_due'").await,1);
    f.hub
        .schedule_ready(&f.credential.device, epoch, now)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_snapshot BEFORE INSERT ON link_outbox WHEN json_extract(NEW.data,'$.payload.kind')='snapshot' BEGIN SELECT RAISE(ABORT,'fixture rollback'); END").execute(&f.hub.core.application_pool()).await.unwrap();
    assert!(
        f.hub
            .snapshot(&f.credential.device, epoch, now)
            .await
            .is_err()
    );
    assert_eq!(f.count("SELECT count(*) FROM link_outbox WHERE json_extract(data,'$.payload.kind')='schedule_notice'").await,0);
    assert_eq!(f.count("SELECT count(*) FROM outbox WHERE acknowledged=0 AND json_extract(payload,'$.kind')='schedule_due'").await,1);
    sqlx::query("DROP TRIGGER fail_snapshot")
        .execute(&f.hub.core.application_pool())
        .await
        .unwrap();
    f.hub
        .snapshot(&f.credential.device, epoch, now)
        .await
        .unwrap();
    f.hub
        .snapshot(&f.credential.device, epoch, now)
        .await
        .unwrap();
    let outgoing = f.hub.outgoing(&f.credential.device).await.unwrap();
    let notices: Vec<_> = outgoing
        .iter()
        .filter(|e| matches!(e.payload, ServerPayload::ScheduleNotice { .. }))
        .collect();
    assert_eq!(notices.len(), 1);
    for envelope in &outgoing {
        f.journal()
            .accept(envelope, false, epoch, false)
            .await
            .unwrap();
    }
    assert!(
        !f.journal()
            .accept(notices[0], false, epoch, false)
            .await
            .unwrap()
    );
    let state = f.journal().view().await.unwrap();
    assert_eq!(state.schedule_notices.len(), 1);
    assert!(state.messages.iter().any(|m| m.body.contains("喝水")));
    assert_eq!(f.count("SELECT count(*) FROM outbox WHERE acknowledged=0 AND json_extract(payload,'$.kind')='schedule_due'").await,0);
    let ServerPayload::ScheduleNotice { notice } = &notices[0].payload else {
        unreachable!();
    };
    let mut changed = notice.clone();
    changed.message = "changed receipt".into();
    let invalid = Envelope::new(
        &f.credential.device,
        f.journal().cursor().await.unwrap() + 1,
        epoch,
        ServerPayload::ScheduleNotice { notice: changed },
    )
    .unwrap();
    assert!(
        f.journal()
            .accept(&invalid, false, epoch, false)
            .await
            .is_err()
    );
    let (worker, _lock) = Journal::open_role(
        &f._root.path().join("worker"),
        f.config.server.as_str(),
        &f.config.repo,
        ClientRole::LocalExecutor,
    )
    .await
    .unwrap();
    let invalid = Envelope::new(
        &worker.device,
        1,
        0,
        ServerPayload::ScheduleNotice {
            notice: notice.clone(),
        },
    )
    .unwrap();
    assert!(worker.accept(&invalid, false, 0, false).await.is_err());
}

#[tokio::test]
async fn actual_rust_socket_and_timer_deliver_once_without_models_and_survive_chat_restart() {
    let mut f = Fixture::new().await;
    drop(f.journal.take());
    drop(f.lock.take());
    let vault = Arc::new(TestVault(
        Mutex::new(Some(f.credential.clone())),
        Mutex::new(None),
    ));
    let driver = tokio::spawn(pulse_link::schedules::run(f.hub.clone()));
    async fn wait(view: &mut watch::Receiver<NativeState>, notice: bool) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if view.borrow().connected && (!notice || view.borrow().schedule_notices.len() == 1)
                {
                    break;
                }
                view.changed().await.unwrap();
            }
        })
        .await
        .expect("schedule transport did not progress");
    }
    let device = Device::open_chat(f.config.clone(), vault.clone())
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(8);
    let (state, mut view) = watch::channel(NativeState::default());
    let running = tokio::spawn(device.run(rx, state));
    wait(&mut view, false).await;
    tx.send(NativeCommand::Action {
        source_id: uuid::Uuid::new_v4().to_string(),
        action: HumanAction::Submit {
            id: uuid::Uuid::new_v4().to_string(),
            goal: "1秒後提醒我喝水".into(),
        },
    })
    .await
    .unwrap();
    wait(&mut view, true).await;
    assert_eq!(view.borrow().schedule_notices[0].message, "喝水");
    tx.send(NativeCommand::Shutdown).await.unwrap();
    running.await.unwrap().unwrap();
    let device = Device::open_chat(f.config.clone(), vault).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    let (state, mut view) = watch::channel(NativeState::default());
    let running = tokio::spawn(device.run(rx, state));
    wait(&mut view, true).await;
    assert_eq!(view.borrow().schedule_notices.len(), 1);
    assert_eq!(f.count("SELECT count(*) FROM tasks").await, 0);
    assert_eq!(f.count("SELECT count(*) FROM budget").await, 0);
    assert_eq!(
        f.count("SELECT count(*) FROM outbox WHERE json_extract(payload,'$.kind')='schedule_due'")
            .await,
        1
    );
    tx.send(NativeCommand::Shutdown).await.unwrap();
    running.await.unwrap().unwrap();
    driver.abort();
    let _ = driver.await;
}

#[tokio::test]
async fn a_quiet_chat_drains_more_than_one_notice_batch_without_new_input() {
    let mut f = Fixture::new().await;
    for n in 0..49 {
        let request = f
            .arm(
                &format!("batch-{n}"),
                1,
                ScheduledAction::Remind {
                    message: format!("reminder {n}"),
                },
            )
            .await;
        assert!(
            f.hub
                .core
                .fire_schedule(request.due_at)
                .await
                .unwrap()
                .fired
        );
    }
    drop(f.journal.take());
    drop(f.lock.take());
    let vault = Arc::new(TestVault(
        Mutex::new(Some(f.credential.clone())),
        Mutex::new(None),
    ));
    let device = Device::open_chat(f.config.clone(), vault).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    let (state, mut view) = watch::channel(NativeState::default());
    let running = tokio::spawn(device.run(rx, state));
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if view.borrow().connected && view.borrow().schedule_notices.len() == 20
                && f.count("SELECT count(*) FROM outbox WHERE acknowledged=0 AND json_extract(payload,'$.kind')='schedule_due'").await == 0
                && f.count("SELECT count(*) FROM link_outbox WHERE acknowledged=0").await == 0 {
                break;
            }
            tokio::select! {
                result=view.changed()=>result.unwrap(),
                ()=tokio::time::sleep(Duration::from_millis(20))=>{},
            }
        }
    }).await.expect("a quiet chat left notices behind after a merged wakeup");
    tx.send(NativeCommand::Shutdown).await.unwrap();
    running.await.unwrap().unwrap();
    let db = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(f.config.data_dir.join("device.db")),
    )
    .await
    .unwrap();
    let saved: i64 = sqlx::query_scalar("SELECT count(*) FROM local_schedule_notices")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(saved, 49);
    assert_eq!(f.count("SELECT count(*) FROM tasks").await, 0);
    assert_eq!(f.count("SELECT count(*) FROM budget").await, 0);
    db.close().await;
}

#[tokio::test]
async fn timed_work_uses_existing_remote_executor_and_unconfigured_provider_cannot_claim_success() {
    let f = Fixture::new().await;
    let source = uuid::Uuid::new_v4().to_string();
    f.journal()
        .action(
            &source,
            HumanAction::Submit {
                id: uuid::Uuid::new_v4().to_string(),
                goal: "排程 A：一分鐘後：執行 分析 Pulse".into(),
            },
        )
        .await
        .unwrap();
    let envelope = f.journal().outgoing().await.unwrap().remove(0);
    f.hub
        .receive(&f.credential.device, &envelope, pulse_link::now())
        .await
        .unwrap();
    let schedule = f.hub.core.schedules(100).await.unwrap().remove(0);
    assert!(
        !pulse_link::remote::step(&f.hub, pulse_link::now())
            .await
            .unwrap()
    );
    assert_eq!(f.count("SELECT count(*) FROM tasks").await, 0);
    f.hub
        .core
        .fire_schedule(schedule.request.due_at)
        .await
        .unwrap();
    assert!(
        pulse_link::remote::step(&f.hub, schedule.request.due_at)
            .await
            .unwrap()
    );
    let task = f
        .hub
        .core
        .task(
            f.hub
                .core
                .schedule(&schedule.request.id)
                .await
                .unwrap()
                .task_id
                .as_ref()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(task.state, pulse_core::State::Blocked);
    assert_eq!(task.reason.as_deref(), Some("provider_not_configured"));
    assert_eq!(task.model_calls, 0);
}

#[tokio::test]
async fn reserved_invalid_schedule_does_not_fall_through_to_a_task_and_forged_time_is_rejected() {
    let f = Fixture::new().await;
    f.journal()
        .action(
            &uuid::Uuid::new_v4().to_string(),
            HumanAction::Submit {
                id: uuid::Uuid::new_v4().to_string(),
                goal: "提醒我有空時處理".into(),
            },
        )
        .await
        .unwrap();
    let envelope = f.journal().outgoing().await.unwrap().remove(0);
    f.hub
        .receive(&f.credential.device, &envelope, pulse_link::now())
        .await
        .unwrap();
    assert_eq!(f.count("SELECT count(*) FROM tasks").await, 0);
    assert!(f.hub.core.schedules(100).await.unwrap().is_empty());
    let now = pulse_link::now();
    let request = ScheduleRequest {
        id: uuid::Uuid::new_v4().to_string(),
        label: "喝水".into(),
        source_at: now,
        due_at: now + 1,
        timezone: "Asia/Taipei".into(),
        action: ScheduledAction::Remind {
            message: "喝水".into(),
        },
    };
    let forged = Envelope::new(
        &f.credential.device,
        2,
        0,
        DevicePayload::Human {
            source_id: uuid::Uuid::new_v4().to_string(),
            action: HumanAction::Schedule {
                request,
                text: "一分鐘後提醒我喝水".into(),
            },
        },
    )
    .unwrap();
    f.hub
        .receive(&f.credential.device, &forged, now)
        .await
        .unwrap();
    assert_eq!(f.count("SELECT count(*) FROM schedules").await, 0);
    assert_eq!(
        f.count("SELECT count(*) FROM link_inbox WHERE outcome='rejected'")
            .await,
        1
    );
    assert!(
        f.hub
            .schedule_ready(&f.credential.device, 999, now)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn an_earlier_timer_wakes_the_driver_and_cancelled_timers_stay_quiet() {
    let f = Fixture::new().await;
    f.arm(
        "later",
        60_000,
        ScheduledAction::Remind {
            message: "later".into(),
        },
    )
    .await;
    let cancelled = f
        .arm(
            "cancelled",
            100,
            ScheduledAction::Remind {
                message: "must not deliver".into(),
            },
        )
        .await;
    let event=f.hub.core.ingest(pulse_core::Origin::Human,"fixture","cancel-timer",serde_json::json!({"commands":[Command::CancelSchedule {id:cancelled.id,expected_revision:1}]}),pulse_link::now()).await.unwrap();
    f.hub.core.route(&event.event_id).await.unwrap();
    let driver = tokio::spawn(pulse_link::schedules::run(f.hub.clone()));
    let earlier = f
        .arm(
            "earlier",
            200,
            ScheduledAction::Remind {
                message: "earlier".into(),
            },
        )
        .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if f.hub.core.schedule(&earlier.id).await.unwrap().state
                == pulse_core::ScheduleState::Fired
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        f.count("SELECT count(*) FROM outbox WHERE json_extract(payload,'$.kind')='schedule_due'")
            .await,
        1
    );
    assert_eq!(f.count("SELECT count(*) FROM budget").await, 0);
    driver.abort();
    let _ = driver.await;
}
