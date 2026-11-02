use pulse_core::*;
use serde_json::json;
const NOW: i64 = 1_800_000_000_000;
async fn setup() -> (tempfile::TempDir, Store) {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    (root, store)
}
fn reminder(id: &str) -> ScheduleRequest {
    ScheduleRequest {
        id: id.into(),
        label: id.into(),
        source_at: NOW,
        due_at: NOW + 10_000,
        timezone: "Asia/Taipei".into(),
        action: ScheduledAction::Remind {
            message: "喝水".into(),
        },
    }
}
async fn send(store: &Store, source: &str, commands: Vec<Command>, now: i64) -> Outcome {
    let event = store
        .ingest(
            Origin::Human,
            "test",
            source,
            json!({"commands":commands}),
            now,
        )
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap()
}
async fn create(store: &Store, request: ScheduleRequest) {
    let key = request.id.clone();
    let outcome = send(store, &key, vec![Command::Schedule { request }], NOW).await;
    assert!(matches!(outcome, Outcome::Schedules { .. }), "{outcome:?}");
}
async fn count(store: &Store, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(&store.application_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn reminder_waits_then_fires_once_across_concurrent_ticks_replay_and_restart() {
    let (root, store) = setup().await;
    let request = reminder("water");
    create(&store, request.clone()).await;
    assert_eq!(count(&store, "SELECT count(*) FROM tasks").await, 0);
    assert_eq!(
        store.fire_schedule(NOW).await.unwrap().next_at,
        Some(request.due_at)
    );
    let (a, b) = tokio::join!(
        store.fire_schedule(request.due_at),
        store.fire_schedule(request.due_at)
    );
    assert_eq!(
        usize::from(a.unwrap().fired) + usize::from(b.unwrap().fired),
        1
    );
    assert_eq!(
        count(
            &store,
            "SELECT count(*) FROM outbox WHERE json_extract(payload,'$.kind')='schedule_due'"
        )
        .await,
        1
    );
    assert!(matches!(
        send(
            &store,
            "water",
            vec![Command::Schedule {
                request: request.clone()
            }],
            NOW + 20_000
        )
        .await,
        Outcome::Schedules { .. }
    ));
    store.close().await;
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    store
        .recover(&Versions::fixture(), NOW + 20_000)
        .await
        .unwrap();
    assert!(!store.fire_schedule(NOW + 30_000).await.unwrap().fired);
    assert_eq!(count(&store, "SELECT count(*) FROM budget").await, 0);
    assert_eq!(
        store.schedule("water").await.unwrap().state,
        ScheduleState::Fired
    );
}

#[tokio::test]
async fn scheduled_work_enters_the_real_task_queue_with_original_scope_source_and_deadline() {
    let (_root, store) = setup().await;
    let mut request = reminder("analysis");
    request.action = ScheduledAction::Task {
        goal: "分析 Pulse".into(),
        scope: "pulse:read".into(),
        requires_device: false,
    };
    create(&store, request.clone()).await;
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 1)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .fire_schedule(request.due_at + 5_000)
            .await
            .unwrap()
            .fired
    );
    let schedule = store.schedule(&request.id).await.unwrap();
    let task = store
        .task(schedule.task_id.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(task.authorization_event, schedule.authorization_event);
    assert_eq!(task.scope, "pulse:read");
    assert_eq!(task.goal, "分析 Pulse");
    assert_eq!(task.deadline, request.due_at + 15 * 60 * 1000);
    assert_eq!(task.model_calls, 0);
    let run = store
        .claim_next(&Versions::fixture(), request.due_at + 5_001)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, task.id);
    send(
        &store,
        "stop-schedule",
        vec![Command::CancelSchedule {
            id: request.id,
            expected_revision: 1,
        }],
        request.due_at + 5_002,
    )
    .await;
    assert_eq!(store.task(&task.id).await.unwrap().state, State::Planning); // Stopping the schedule does not cancel its existing task.
}

#[tokio::test]
async fn pending_cancel_is_routed_before_timer_and_tool_origin_cannot_create_work() {
    let (_root, store) = setup().await;
    create(&store, reminder("cancel")).await;
    let event = store
        .ingest(
            Origin::Human,
            "test",
            "cancel-source",
            json!({"commands":[Command::CancelSchedule {id:"cancel".into(),expected_revision:1}]}),
            NOW + 1,
        )
        .await
        .unwrap();
    assert!(store.fire_schedule(NOW + 20_000).await.unwrap().blocked);
    store.route(&event.event_id).await.unwrap();
    assert!(!store.fire_schedule(NOW + 20_000).await.unwrap().fired);
    assert_eq!(
        store.schedule("cancel").await.unwrap().state,
        ScheduleState::Cancelled
    );
    let event = store
        .ingest(
            Origin::Tool,
            "test",
            "forged",
            json!({"commands":[Command::Schedule {request:reminder("forged")}]}),
            NOW,
        )
        .await
        .unwrap();
    assert!(matches!(
        store.route(&event.event_id).await.unwrap(),
        Outcome::Rejected { .. }
    ));
    assert!(store.schedule("forged").await.is_err());
}

#[tokio::test]
async fn offline_late_creation_and_long_restart_never_run_expired_tasks() {
    let (root, store) = setup().await;
    let mut request = reminder("late");
    request.action = ScheduledAction::Task {
        goal: "分析 Pulse".into(),
        scope: "pulse:read".into(),
        requires_device: false,
    };
    assert!(matches!(
        send(
            &store,
            "late",
            vec![Command::Schedule {
                request: request.clone()
            }],
            request.due_at + 1
        )
        .await,
        Outcome::Schedules { .. }
    ));
    assert_eq!(
        store.schedule("late").await.unwrap().state,
        ScheduleState::Missed
    );
    request.id = "restart".into();
    request.label = "restart".into();
    create(&store, request.clone()).await;
    store.close().await;
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    store
        .recover(&Versions::fixture(), NOW + 1_000_000)
        .await
        .unwrap();
    assert!(store.fire_schedule(NOW + 1_000_000).await.unwrap().fired);
    assert_eq!(
        store.schedule("restart").await.unwrap().state,
        ScheduleState::Missed
    );
    assert_eq!(count(&store, "SELECT count(*) FROM tasks").await, 0);
}

#[tokio::test]
async fn full_notice_queue_rolls_back_firing_and_changed_authorization_never_spawns_a_task() {
    let (_root, store) = setup().await;
    create(&store, reminder("full")).await;
    sqlx::query("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<1000) INSERT OR IGNORE INTO outbox(dedupe_key,payload) SELECT 'load:'||n,'{}' FROM seq").execute(&store.application_pool()).await.unwrap();
    assert!(store.fire_schedule(NOW + 20_000).await.unwrap().blocked);
    assert_eq!(
        store.schedule("full").await.unwrap().state,
        ScheduleState::Armed
    );
    sqlx::query("UPDATE outbox SET acknowledged=1")
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(store.fire_schedule(NOW + 20_000).await.unwrap().fired);
    let mut request = reminder("altered");
    request.action = ScheduledAction::Task {
        goal: "original".into(),
        scope: "pulse:read".into(),
        requires_device: false,
    };
    create(&store, request).await;
    sqlx::query("UPDATE schedules SET data=json_set(data,'$.request.action.goal','forged instructions') WHERE id='altered'").execute(&store.application_pool()).await.unwrap();
    assert!(store.fire_schedule(NOW + 20_000).await.unwrap().fired);
    assert_eq!(
        store.schedule("altered").await.unwrap().state,
        ScheduleState::Missed
    );
    assert_eq!(count(&store, "SELECT count(*) FROM tasks").await, 0);
}

#[tokio::test]
async fn batch_conflict_is_atomic_and_queries_do_not_create_tasks_or_model_work() {
    let (_root, store) = setup().await;
    let request = reminder("atomic");
    let outcome = send(
        &store,
        "batch",
        vec![
            Command::Schedule { request },
            Command::CancelSchedule {
                id: "missing".into(),
                expected_revision: 1,
            },
        ],
        NOW,
    )
    .await;
    assert!(matches!(outcome, Outcome::Rejected { .. }));
    assert!(store.schedules(100).await.unwrap().is_empty());
    create(&store, reminder("active")).await;
    assert!(matches!(
        send(
            &store,
            "stale",
            vec![Command::CancelSchedule {
                id: "active".into(),
                expected_revision: 2
            }],
            NOW
        )
        .await,
        Outcome::Rejected { .. }
    ));
    assert!(matches!(
        send(
            &store,
            "query",
            vec![Command::QuerySchedules { ids: vec![] }],
            NOW
        )
        .await,
        Outcome::Schedules { .. }
    ));
    assert_eq!(
        store.schedule("active").await.unwrap().state,
        ScheduleState::Armed
    );
    assert_eq!(count(&store, "SELECT count(*) FROM tasks").await, 0);
    assert_eq!(count(&store, "SELECT count(*) FROM budget").await, 0);
}

#[test]
fn local_times_offsets_relative_chinese_and_dst_ambiguity_are_explicit() {
    let source = chrono::DateTime::parse_from_rfc3339("2026-10-31T12:00:00+08:00")
        .unwrap()
        .timestamp_millis();
    assert_eq!(
        resolve_schedule_time("十分鐘後", source).unwrap().0,
        source + 600_000
    );
    assert_eq!(
        resolve_schedule_time("半小時後", source).unwrap().0,
        source + 1_800_000
    );
    assert_eq!(
        resolve_schedule_time("明天上午9點半", source).unwrap().0,
        chrono::DateTime::parse_from_rfc3339("2026-11-01T09:30:00+08:00")
            .unwrap()
            .timestamp_millis()
    );
    assert!(resolve_schedule_time("2026-11-01 01:30 America/New_York", source).is_err());
    assert!(resolve_schedule_time("2026-11-01T01:30:00-04:00", source).is_ok());
    assert!(resolve_schedule_time("0分鐘後", source).is_err());
    assert!(resolve_schedule_time("找時間", source).is_err());
    assert!(resolve_schedule_time("400天後", source).is_err());
}

#[tokio::test]
async fn active_and_history_capacity_keep_cancel_and_query_available() {
    let (_root, store) = setup().await;
    for n in 0..100 {
        create(&store, reminder(&format!("schedule-{n}"))).await;
    }
    assert!(matches!(
        send(
            &store,
            "overflow",
            vec![Command::Schedule {
                request: reminder("overflow")
            }],
            NOW
        )
        .await,
        Outcome::Rejected { .. }
    ));
    assert!(matches!(
        send(
            &store,
            "free",
            vec![Command::CancelSchedule {
                id: "schedule-0".into(),
                expected_revision: 1
            }],
            NOW
        )
        .await,
        Outcome::Schedules { .. }
    ));
    create(&store, reminder("replacement")).await;
    let template = serde_json::to_string(&store.schedule("schedule-0").await.unwrap()).unwrap();
    sqlx::query("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<899) INSERT INTO schedules(id,data) SELECT 'history-'||n,json_set(?,'$.request.id','history-'||n,'$.state','cancelled') FROM seq").bind(template).execute(&store.application_pool()).await.unwrap();
    assert_eq!(count(&store, "SELECT count(*) FROM schedules").await, 1000);
    assert!(matches!(
        send(
            &store,
            "history-full",
            vec![Command::Schedule {
                request: reminder("history-full")
            }],
            NOW
        )
        .await,
        Outcome::Rejected { .. }
    ));
    assert!(matches!(
        send(
            &store,
            "cancel-at-full",
            vec![Command::CancelSchedule {
                id: "schedule-1".into(),
                expected_revision: 1
            }],
            NOW
        )
        .await,
        Outcome::Schedules { .. }
    ));
    assert!(matches!(
        send(
            &store,
            "query-at-full",
            vec![Command::QuerySchedules { ids: vec![] }],
            NOW
        )
        .await,
        Outcome::Schedules { .. }
    ));
}

#[tokio::test]
async fn full_task_queue_defers_work_without_extending_the_due_deadline() {
    let (_root, store) = setup().await;
    let mut request = reminder("backpressure");
    request.action = ScheduledAction::Task {
        goal: "分析 Pulse".into(),
        scope: "pulse:read".into(),
        requires_device: false,
    };
    create(&store, request.clone()).await;
    send(
        &store,
        "task",
        vec![Command::Create {
            id: "base".into(),
            goal: "fixture".into(),
            scope: "pulse:read".into(),
            priority: 0,
            requires_device: false,
            parent: None,
            dependencies: vec![],
        }],
        NOW,
    )
    .await;
    let template = serde_json::to_string(&store.task("base").await.unwrap()).unwrap();
    sqlx::query("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<99) INSERT INTO tasks(id,data) SELECT 'load-'||n,json_set(?,'$.id','load-'||n) FROM seq").bind(template).execute(&store.application_pool()).await.unwrap();
    assert!(store.fire_schedule(request.due_at).await.unwrap().blocked);
    assert_eq!(
        store.schedule(&request.id).await.unwrap().state,
        ScheduleState::Armed
    );
    assert!(
        store
            .fire_schedule(request.due_at + 15 * 60 * 1000)
            .await
            .unwrap()
            .fired
    );
    assert_eq!(
        store.schedule(&request.id).await.unwrap().state,
        ScheduleState::Missed
    );
    assert_eq!(count(&store, "SELECT count(*) FROM tasks").await, 100);
}
