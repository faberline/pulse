use pulse_core::*;
use serde_json::{Value, json};
use sqlx::{
    Connection, Executor,
    sqlite::{SqliteConnectOptions, SqliteConnection},
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

const NOW: i64 = 1_800_000_000_000;

async fn setup() -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("core.db"), "owner")
        .await
        .unwrap();
    (dir, store)
}
fn create(id: &str, device: bool) -> Value {
    json!({"kind":"create","id":id,"goal":format!("analyze {id}"),"scope":"pulse:read","priority":0,"requires_device":device})
}
async fn send(store: &Store, origin: Origin, key: &str, commands: Value, now: i64) -> Outcome {
    let event = store
        .ingest(origin, "desktop", key, json!({"commands":commands}), now)
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap()
}
async fn add(store: &Store, id: &str, device: bool) {
    assert!(matches!(
        send(store, Origin::Human, id, json!([create(id, device)]), NOW).await,
        Outcome::Applied { .. }
    ));
}
async fn claim(store: &Store) -> Run {
    store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap()
}
async fn ready(store: &Store, run: &Run) {
    store
        .advance(
            &run.token,
            State::Ready,
            json!({"step":1}),
            "planned",
            None,
            NOW + 2,
        )
        .await
        .unwrap();
}
async fn raw(dir: &TempDir) -> SqliteConnection {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(dir.path().join("core.db")),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn durable_receipt_replay_conflict_and_routing_restart() {
    let (dir, store) = setup().await;
    let payload = json!({"commands":[create("a",false)],"note":{"b":2,"a":1}});
    let first = store
        .ingest(Origin::Human, "ui", "one", payload.clone(), NOW)
        .await
        .unwrap();
    assert!(!first.duplicate);
    store.close().await;
    let store = Store::open(dir.path().join("core.db"), "owner")
        .await
        .unwrap();
    assert_eq!(
        store.pending_events(100).await.unwrap(),
        vec![first.event_id.clone()]
    );
    let replay = store
        .ingest(Origin::Human, "ui", "one", payload, NOW + 5)
        .await
        .unwrap();
    assert!(replay.duplicate);
    assert_eq!(first.event_id, replay.event_id);
    assert!(
        store
            .ingest(
                Origin::Human,
                "ui",
                "one",
                json!({"different":true}),
                NOW + 6
            )
            .await
            .is_err()
    );
    let outcome = store.route(&first.event_id).await.unwrap();
    assert_eq!(outcome, store.route(&first.event_id).await.unwrap());
    assert_eq!(store.task("a").await.unwrap().revision, 1);
    assert_eq!(store.outbox(100).await.unwrap().len(), 1);
    let conflicts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conflicts")
        .fetch_one(&mut raw(&dir).await)
        .await
        .unwrap();
    assert_eq!(conflicts, 1);
}

#[tokio::test]
async fn source_origin_is_part_of_dedup_identity_and_not_payload_authority() {
    let (_dir, store) = setup().await;
    let payload =
        json!({"source_kind":"human","approved":true,"commands":[create("forged",false)]});
    let event = store
        .ingest(Origin::Tool, "tool", "x", payload.clone(), NOW)
        .await
        .unwrap();
    assert!(matches!(
        store.route(&event.event_id).await.unwrap(),
        Outcome::Rejected { .. }
    ));
    assert!(store.task("forged").await.is_err());
    assert!(
        store
            .ingest(Origin::Human, "tool", "x", payload, NOW)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn multi_task_changes_are_atomic_and_status_is_read_only() {
    let (_dir, store) = setup().await;
    send(
        &store,
        Origin::Human,
        "create",
        json!([create("a", false), create("b", false)]),
        NOW,
    )
    .await;
    let before = store.task("a").await.unwrap();
    let bad = send(
        &store,
        Origin::Human,
        "bad",
        json!([
            {"kind":"revise","id":"a","expected_revision":"1","goal":"new a"},
            {"kind":"cancel","id":"b","expected_revision":"999"}
        ]),
        NOW + 1,
    )
    .await;
    assert!(matches!(bad, Outcome::Rejected { .. }));
    assert_eq!(store.task("a").await.unwrap().goal, before.goal);
    send(
        &store,
        Origin::Human,
        "revise",
        json!([{"kind":"revise","id":"a","expected_revision":"1","goal":"new a"}]),
        NOW + 2,
    )
    .await;
    send(
        &store,
        Origin::Human,
        "status",
        json!([{"kind":"query","ids":["a","b"]}]),
        NOW + 3,
    )
    .await;
    send(
        &store,
        Origin::Human,
        "cancel",
        json!([{"kind":"cancel","id":"a","expected_revision":"2"}]),
        NOW + 4,
    )
    .await;
    let a = store.task("a").await.unwrap();
    let b = store.task("b").await.unwrap();
    assert_eq!(a.state, State::Cancelled);
    assert_eq!(b.state, State::Queued);
    assert_eq!(b.revision, 1);
    assert_eq!(a.decisions.len(), 2);
    assert_eq!(
        a.decisions[1].supersedes,
        Some(a.decisions[0].event_id.clone())
    );
    assert_eq!(
        store
            .health(NOW + 5)
            .await
            .unwrap()
            .states
            .values()
            .sum::<u64>(),
        2
    );
}

#[tokio::test]
async fn clarification_never_applies_other_commands() {
    let (_dir, store) = setup().await;
    let out = send(
        &store,
        Origin::Human,
        "mixed",
        json!([create("a",false),{"kind":"clarify","question":"which task?"}]),
        NOW,
    )
    .await;
    assert!(matches!(out, Outcome::Rejected { .. }));
    assert!(store.task("a").await.is_err());
    let out = send(
        &store,
        Origin::Human,
        "question",
        json!([{"kind":"clarify","question":"which task?"}]),
        NOW,
    )
    .await;
    assert!(matches!(out, Outcome::Clarification { .. }));
    assert!(store.health(NOW).await.unwrap().states.is_empty());
}

#[tokio::test]
async fn routing_commit_failure_does_not_lose_receipt_or_partially_create_task() {
    let (dir, store) = setup().await;
    let event = store
        .ingest(
            Origin::Human,
            "ui",
            "a",
            json!({"commands":[create("a",false)]}),
            NOW,
        )
        .await
        .unwrap();
    let mut db = raw(&dir).await;
    db.execute("CREATE TRIGGER fail_notification BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'injected commit boundary'); END").await.unwrap();
    assert!(store.route(&event.event_id).await.is_err());
    assert!(store.task("a").await.is_err());
    assert_eq!(
        store.pending_events(100).await.unwrap(),
        vec![event.event_id.clone()]
    );
    db.execute("DROP TRIGGER fail_notification").await.unwrap();
    assert!(matches!(
        store.route(&event.event_id).await.unwrap(),
        Outcome::Applied { .. }
    ));
}

#[tokio::test]
async fn queue_is_bounded_but_cancel_and_query_remain_available() {
    let (_dir, store) = setup().await;
    for i in 0..100 {
        add(&store, &format!("task-{i:03}"), false).await;
    }
    assert!(matches!(
        send(
            &store,
            Origin::Human,
            "overflow",
            json!([create("overflow", false)]),
            NOW
        )
        .await,
        Outcome::Rejected { .. }
    ));
    assert!(matches!(
        send(
            &store,
            Origin::Human,
            "control",
            json!([{"kind":"cancel","id":"task-000","expected_revision":"1"}]),
            NOW + 1
        )
        .await,
        Outcome::Applied { .. }
    ));
    add(&store, "replacement", false).await;
    assert!(store.task("overflow").await.is_err());
}

#[tokio::test]
async fn scheduler_priority_concurrency_waits_and_deadline() {
    let (_dir, store) = setup().await;
    add(&store, "device", true).await;
    for (id, priority) in [("low", 0), ("high", 10), ("middle", 5)] {
        let mut command = create(id, false);
        command["priority"] = json!(priority);
        send(&store, Origin::Human, id, json!([command]), NOW).await;
    }
    assert_eq!(claim(&store).await.token.task_id, "high");
    assert_eq!(claim(&store).await.token.task_id, "middle");
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 2)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.task("device").await.unwrap().state,
        State::WaitingDevice
    );
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 900_000)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.task("low").await.unwrap().reason.as_deref(),
        Some("deadline_exhausted")
    );
}

#[tokio::test]
async fn restart_keeps_checkpoint_and_fences_original_worker() {
    let (dir, store) = setup().await;
    add(&store, "a", false).await;
    let old = claim(&store).await;
    ready(&store, &old).await;
    store.close().await;
    let store = Store::open(dir.path().join("core.db"), "owner")
        .await
        .unwrap();
    store.recover(&Versions::fixture(), NOW + 3).await.unwrap();
    let resumed = claim(&store).await;
    assert_eq!(resumed.token.run_id, old.token.run_id);
    assert!(resumed.token.epoch > old.token.epoch);
    assert_eq!(resumed.checkpoint, json!({"step":1}));
    assert!(
        store
            .advance(
                &old.token,
                State::Ready,
                Value::Null,
                "stale",
                None,
                NOW + 4
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn incompatible_versions_do_not_resume_old_run() {
    let (_dir, store) = setup().await;
    add(&store, "a", false).await;
    claim(&store).await;
    let mut version = Versions::fixture();
    version.prompt = "new-prompt".into();
    store.recover(&version, NOW + 2).await.unwrap();
    assert!(store.claim_next(&version, NOW + 3).await.unwrap().is_none());
    assert_eq!(
        store.task("a").await.unwrap().reason.as_deref(),
        Some("version_incompatible")
    );
    let out = send(
        &store,
        Origin::Human,
        "resume",
        json!([{"kind":"resume","id":"a","expected_revision":"1"}]),
        NOW + 4,
    )
    .await;
    assert!(matches!(out, Outcome::Rejected { .. }));
}

#[tokio::test]
async fn retry_wakes_only_when_due_and_is_finite() {
    let (_dir, store) = setup().await;
    add(&store, "a", false).await;
    let mut run = claim(&store).await;
    for n in 0..3 {
        let wake = NOW + 100 + n * 100;
        store
            .advance(
                &run.token,
                State::RetryWait,
                json!({"retry":n}),
                "temporary",
                Some(wake),
                NOW + 2,
            )
            .await
            .unwrap();
        assert!(
            store
                .claim_next(&Versions::fixture(), wake - 1)
                .await
                .unwrap()
                .is_none()
        );
        run = store
            .claim_next(&Versions::fixture(), wake)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(
        store
            .advance(
                &run.token,
                State::RetryWait,
                Value::Null,
                "again",
                Some(NOW + 500),
                NOW + 400
            )
            .await
            .is_err()
    );
    assert_eq!(store.task("a").await.unwrap().retry_count, 3);
    assert_eq!(store.task("a").await.unwrap().state, State::Blocked);
}

#[tokio::test]
async fn waiting_and_idle_ticks_make_no_model_calls() {
    let (_dir, store) = setup().await;
    add(&store, "device", true).await;
    let calls = AtomicUsize::new(0);
    for _ in 0..20 {
        if store
            .claim_next(&Versions::fixture(), NOW + 1)
            .await
            .unwrap()
            .is_some()
        {
            calls.fetch_add(1, Ordering::SeqCst);
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    add(&store, "a", false).await;
    let run = claim(&store).await;
    calls.fetch_add(1, Ordering::SeqCst);
    store
        .advance(
            &run.token,
            State::WaitingApproval,
            Value::Null,
            "needs human",
            None,
            NOW + 2,
        )
        .await
        .unwrap();
    for _ in 0..20 {
        assert!(
            store
                .claim_next(&Versions::fixture(), NOW + 3)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.health(NOW).await.unwrap().states["waiting_approval"],
        1
    );
}

#[tokio::test]
async fn budget_is_idempotent_unknown_is_held_and_task_limit_survives_restart() {
    let (dir, store) = setup().await;
    add(&store, "a", false).await;
    let run = claim(&store).await;
    assert!(
        store
            .reserve(&run.token, "call", 200_000, "rates-1", NOW + 2)
            .await
            .unwrap()
    );
    assert!(
        !store
            .reserve(&run.token, "call", 200_000, "rates-1", NOW + 2)
            .await
            .unwrap()
    );
    assert!(
        store
            .reserve(&run.token, "call", 1, "rates-1", NOW + 2)
            .await
            .is_err()
    );
    store.cost_unknown("call").await.unwrap();
    store.close().await;
    let store = Store::open(dir.path().join("core.db"), "owner")
        .await
        .unwrap();
    store.recover(&Versions::fixture(), NOW + 3).await.unwrap();
    let run = claim(&store).await;
    assert!(
        store
            .reserve(&run.token, "second", 60_000, "rates-1", NOW + 4)
            .await
            .is_err()
    );
    assert_eq!(store.task("a").await.unwrap().state, State::Blocked);
    store
        .settle("call", 10_000, "provider usage receipt")
        .await
        .unwrap();
    store
        .settle("call", 10_000, "provider usage receipt")
        .await
        .unwrap();
    assert!(store.settle("call", 1, "different").await.is_err());
    assert_eq!(store.task("a").await.unwrap().model_calls, 1);
}

#[tokio::test]
async fn daily_budget_serializes_across_connections_and_rolls_at_taipei_midnight() {
    let (dir, store) = setup().await;
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-29T15:59:59Z")
        .unwrap()
        .timestamp_millis();
    let mut tokens = Vec::new();
    for i in 0..5 {
        let id = format!("a{i}");
        send(&store, Origin::Human, &id, json!([create(&id, false)]), now).await;
        let run = store
            .claim_next(&Versions::fixture(), now)
            .await
            .unwrap()
            .unwrap();
        if i < 3 {
            store
                .reserve(&run.token, &id, 250_000, "rates", now)
                .await
                .unwrap();
            store
                .advance(
                    &run.token,
                    State::WaitingApproval,
                    Value::Null,
                    "wait",
                    None,
                    now,
                )
                .await
                .unwrap();
        } else {
            tokens.push(run.token);
        }
    }
    let peer = Store::open(dir.path().join("core.db"), "owner")
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        store.reserve(&tokens[0], "race-a", 200_000, "rates", now),
        peer.reserve(&tokens[1], "race-b", 200_000, "rates", now)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let blocked = if a.is_err() { &tokens[0] } else { &tokens[1] };
    send(
        &store,
        Origin::Human,
        "resume",
        json!([{"kind":"resume","id":blocked.task_id,"expected_revision":"1"}]),
        now + 2_000,
    )
    .await;
    let run = store
        .claim_next(&Versions::fixture(), now + 2_000)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .reserve(&run.token, "new-day", 200_000, "rates", now + 2_000)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn model_calls_are_bounded_even_when_each_request_is_cheap() {
    let (_dir, store) = setup().await;
    add(&store, "a", false).await;
    let run = claim(&store).await;
    for i in 0..8 {
        store
            .reserve(&run.token, &i.to_string(), 1, "rates", NOW + 2)
            .await
            .unwrap();
    }
    assert!(
        store
            .reserve(&run.token, "ninth", 1, "rates", NOW + 2)
            .await
            .is_err()
    );
    assert_eq!(store.task("a").await.unwrap().model_calls, 8);
    assert_eq!(
        store.task("a").await.unwrap().reason.as_deref(),
        Some("model_calls_exhausted")
    );
}

#[tokio::test]
async fn effect_after_crash_is_reconciled_without_reexecution() {
    let (dir, store) = setup().await;
    add(&store, "a", false).await;
    let run = claim(&store).await;
    ready(&store, &run).await;
    let op = store
        .prepare_operation(&run.token, "fixture:write", json!({"data":"x"}), NOW + 3)
        .await
        .unwrap();
    let effects = AtomicUsize::new(0);
    effects.fetch_add(1, Ordering::SeqCst);
    store.close().await;
    let store = Store::open(dir.path().join("core.db"), "owner")
        .await
        .unwrap();
    store.recover(&Versions::fixture(), NOW + 4).await.unwrap();
    assert_eq!(store.task("a").await.unwrap().state, State::OutcomeUnknown);
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 5)
            .await
            .unwrap()
            .is_none()
    );
    let receipt = Receipt {
        operation_id: op.id.clone(),
        token: op.token,
        success: true,
        evidence: "verified fixture object x".into(),
    };
    store
        .reconcile(&op.id, Resolution::Executed(receipt.clone()), NOW + 6)
        .await
        .unwrap();
    let resumed = store
        .claim_next(&Versions::fixture(), NOW + 6)
        .await
        .unwrap()
        .unwrap();
    store
        .finish(&resumed.token, true, "verified task artifact x", NOW + 6)
        .await
        .unwrap();
    assert_eq!(store.task("a").await.unwrap().state, State::Succeeded);
    assert_eq!(
        store.record_receipt(&receipt).await.unwrap(),
        ReceiptEffect::Duplicate
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 7)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn ambiguous_effect_blocks_and_proven_not_executed_can_resume() {
    let (_dir, store) = setup().await;
    add(&store, "a", false).await;
    let run = claim(&store).await;
    ready(&store, &run).await;
    let op = store
        .prepare_operation(&run.token, "fixture:write", json!({}), NOW + 3)
        .await
        .unwrap();
    store
        .reconcile(
            &op.id,
            Resolution::Ambiguous {
                evidence: "two candidates".into(),
            },
            NOW + 4,
        )
        .await
        .unwrap();
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 5)
            .await
            .unwrap()
            .is_none()
    );
    let outcome = send(
        &store,
        Origin::Human,
        "unsafe-resume",
        json!([{"kind":"resume","id":"a","expected_revision":"1"}]),
        NOW + 5,
    )
    .await;
    assert!(matches!(outcome, Outcome::Rejected { .. }));
    store
        .reconcile(
            &op.id,
            Resolution::NotExecuted {
                evidence: "authoritative zero-effect receipt".into(),
            },
            NOW + 6,
        )
        .await
        .unwrap();
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 7)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn late_receipt_cannot_complete_a_revised_goal() {
    let (_dir, store) = setup().await;
    add(&store, "a", false).await;
    add(&store, "b", true).await;
    let run = claim(&store).await;
    ready(&store, &run).await;
    let op = store
        .prepare_operation(&run.token, "fixture:write", json!({}), NOW + 3)
        .await
        .unwrap();
    send(
        &store,
        Origin::Human,
        "revision",
        json!([{"kind":"revise","id":"a","expected_revision":"1","goal":"new goal"}]),
        NOW + 4,
    )
    .await;
    let receipt = Receipt {
        operation_id: op.id,
        token: op.token,
        success: true,
        evidence: "old goal result".into(),
    };
    assert_eq!(
        store.record_receipt(&receipt).await.unwrap(),
        ReceiptEffect::Late
    );
    assert_eq!(store.task("a").await.unwrap().goal, "new goal");
    assert_eq!(store.task("a").await.unwrap().state, State::Blocked);
    assert_eq!(store.task("b").await.unwrap().state, State::WaitingDevice);
    assert!(
        store
            .advance(
                &run.token,
                State::Running,
                Value::Null,
                "stale",
                None,
                NOW + 5
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancel_unknown_is_not_reported_stopped_until_evidence_arrives() {
    let (_dir, store) = setup().await;
    add(&store, "a", false).await;
    let run = claim(&store).await;
    ready(&store, &run).await;
    let op = store
        .prepare_operation(&run.token, "fixture:write", json!({}), NOW + 3)
        .await
        .unwrap();
    send(
        &store,
        Origin::Human,
        "cancel",
        json!([{"kind":"cancel","id":"a","expected_revision":"1"}]),
        NOW + 4,
    )
    .await;
    assert_eq!(store.task("a").await.unwrap().state, State::OutcomeUnknown);
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 5)
            .await
            .unwrap()
            .is_none()
    );
    let receipt = Receipt {
        operation_id: op.id,
        token: op.token,
        success: true,
        evidence: "effect finished before cancel".into(),
    };
    assert_eq!(
        store.record_receipt(&receipt).await.unwrap(),
        ReceiptEffect::Late
    );
    let task = store.task("a").await.unwrap();
    assert_eq!(task.state, State::Cancelled);
    assert!(
        task.evidence
            .iter()
            .any(|v| v == "effect finished before cancel")
    );
}

#[tokio::test]
async fn receipt_conflict_and_unsafe_targets_are_rejected() {
    let (_dir, store) = setup().await;
    add(&store, "a", false).await;
    let run = claim(&store).await;
    ready(&store, &run).await;
    assert!(
        store
            .prepare_operation(&run.token, "shell:rm", json!({}), NOW + 3)
            .await
            .is_err()
    );
    let op = store
        .prepare_operation(&run.token, "fixture:read", json!({}), NOW + 3)
        .await
        .unwrap();
    let mut receipt = Receipt {
        operation_id: op.id,
        token: op.token,
        success: true,
        evidence: "proof".into(),
    };
    assert_eq!(
        store.record_receipt(&receipt).await.unwrap(),
        ReceiptEffect::Applied
    );
    assert_eq!(store.task("a").await.unwrap().state, State::Planning);
    store
        .finish(&run.token, true, "goal artifact verified", NOW + 4)
        .await
        .unwrap();
    receipt.success = false;
    assert!(store.record_receipt(&receipt).await.is_err());
    assert_eq!(store.task("a").await.unwrap().state, State::Succeeded);
}

#[tokio::test]
async fn outbox_survives_restart_and_ack_requires_exact_identity() {
    let (dir, store) = setup().await;
    add(&store, "a", false).await;
    let item = store.outbox(1).await.unwrap().remove(0);
    store.close().await;
    let store = Store::open(dir.path().join("core.db"), "owner")
        .await
        .unwrap();
    assert_eq!(store.outbox(1).await.unwrap()[0].key, item.key);
    assert!(store.acknowledge(item.sequence, "wrong").await.is_err());
    store.acknowledge(item.sequence, &item.key).await.unwrap();
    store.acknowledge(item.sequence, &item.key).await.unwrap();
    assert!(store.outbox(100).await.unwrap().is_empty());
}

#[tokio::test]
async fn full_outbox_applies_backpressure_without_losing_source_or_task_atomicity() {
    let (dir, store) = setup().await;
    raw(&dir).await.execute("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<1000) INSERT INTO outbox(dedupe_key,payload) SELECT 'fill-'||x,'{}' FROM n").await.unwrap();
    let event = store
        .ingest(
            Origin::Human,
            "ui",
            "a",
            json!({"commands":[create("a",false)]}),
            NOW,
        )
        .await
        .unwrap();
    assert!(store.route(&event.event_id).await.is_err());
    assert!(store.task("a").await.is_err());
    assert_eq!(store.pending_events(100).await.unwrap().len(), 1);
    let item = store.outbox(1).await.unwrap().remove(0);
    store.acknowledge(item.sequence, &item.key).await.unwrap();
    assert!(matches!(
        store.route(&event.event_id).await.unwrap(),
        Outcome::Applied { .. }
    ));
}

#[tokio::test]
async fn wrong_owner_future_schema_invalid_manifest_and_large_event_fail_closed() {
    let (dir, store) = setup().await;
    assert!(
        Store::open(dir.path().join("core.db"), "other")
            .await
            .is_err()
    );
    let mut version = Versions::fixture();
    version.schema = "999".into();
    assert!(store.claim_next(&version, NOW).await.is_err());
    assert!(
        store
            .ingest(
                Origin::Human,
                "ui",
                "large",
                json!({"text":"x".repeat(256*1024)}),
                NOW
            )
            .await
            .is_err()
    );
    assert!(store.pending_events(0).await.is_err());
    assert!(store.outbox(101).await.is_err());
    raw(&dir)
        .await
        .execute("PRAGMA user_version=99")
        .await
        .unwrap();
    store.close().await;
    assert!(
        Store::open(dir.path().join("core.db"), "owner")
            .await
            .is_err()
    );
}

#[test]
fn wire_counters_are_decimal_strings_and_unknown_commands_fail() {
    let token = Token {
        run_id: "r".into(),
        task_id: "t".into(),
        revision: u64::MAX,
        epoch: 9,
    };
    let value = serde_json::to_value(&token).unwrap();
    assert_eq!(value["revision"], u64::MAX.to_string());
    assert_eq!(serde_json::from_value::<Token>(value).unwrap(), token);
    assert!(
        serde_json::from_value::<Token>(
            json!({"run_id":"r","task_id":"t","revision":1,"epoch":"01"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<Command>(json!({"kind":"shell","command":"anything"})).is_err()
    );
}
