//! Core contract for an explicit Human review of a saved result.
use pulse_core::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const NOW: i64 = 1_800_000_000_000;
const BODY: &str = "OWNED_REVIEW_RESULT_PRIVATE_BODY";

struct Fixture {
    root: tempfile::TempDir,
    store: Store,
    run: Run,
    message: String,
    digest: String,
}

async fn ingest(store: &Store, origin: Origin, source: &str, commands: Value, at: i64) -> String {
    store
        .ingest(
            origin,
            "review-fixture",
            source,
            json!({"commands":commands}),
            at,
        )
        .await
        .unwrap()
        .event_id
}
async fn create(store: &Store, id: &str) {
    let event=ingest(store,Origin::Human,id,json!([{"kind":"create","id":id,"goal":format!("owned goal {id}"),"scope":"pulse:read","priority":0,"requires_device":false}]),NOW).await;
    assert!(matches!(
        store.route(&event).await.unwrap(),
        Outcome::Applied { .. }
    ));
}
fn command(f: &Fixture) -> Value {
    json!({"kind":"accept_result","id":"A","expected_revision":"1","message_id":f.message,"chat_digest":f.digest})
}
async fn submit(f: &Fixture, source: &str, command: Value, at: i64) -> Outcome {
    let event = ingest(&f.store, Origin::Human, source, json!([command]), at).await;
    f.store.route_at(&event, at).await.unwrap()
}
async fn fixture() -> Fixture {
    fixture_with_memory(false).await
}
async fn fixture_with_memory(bound: bool) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    create(&store, "A").await;
    if bound {
        store
            .change_memory(
                Origin::Human,
                &MemoryRequest {
                    source_id: uuid::Uuid::new_v4().to_string(),
                    source_at: NOW,
                    command: MemoryCommand::Remember {
                        label: "owned preference".into(),
                        body: "owned source".into(),
                    },
                },
                NOW,
            )
            .await
            .unwrap();
        let memory = store.list_memories(NOW).await.unwrap().remove(0);
        store
            .bind_task_memories(
                Origin::Human,
                &TaskMemoryRequest {
                    source_id: uuid::Uuid::new_v4().to_string(),
                    source_at: NOW,
                    task_id: "A".into(),
                    task_revision: 1,
                    sources: vec![MemoryRef {
                        id: memory.id,
                        revision: memory.revision,
                    }],
                },
                NOW,
            )
            .await
            .unwrap();
    }
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .reserve(&run.token, "held-call", 20_000, "fixture", NOW + 1)
        .await
        .unwrap();
    let message = format!("model:{}:1:0", run.token.run_id);
    let digest = format!("{:x}", Sha256::digest(BODY.as_bytes()));
    // Simulate the trusted model adapter. Server tests use Hub::model_reply.
    store
        .advance(
            &run.token,
            State::Blocked,
            json!({"phase":"reply_saved","reply":BODY,
        "result_review":{"version":1,"message_id":message,"chat_digest":digest,"saved_at":NOW+2}}),
            &message,
            None,
            NOW + 2,
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO link_chat(id,body,created_at) VALUES(?,?,?)")
        .bind(&message)
        .bind(BODY)
        .bind(NOW + 2)
        .execute(&store.application_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET data=json_set(data,'$.reason','model_reply_ready') WHERE id='A'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    Fixture {
        root,
        store,
        run,
        message,
        digest,
    }
}
async fn terminal_time(store: &Store) -> Option<i64> {
    sqlx::query_scalar("SELECT terminal_at FROM task_retention WHERE task_id='A'")
        .fetch_optional(&store.application_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn human_acceptance_is_atomic_replay_safe_and_keeps_costs_scope_and_other_tasks() {
    let f = fixture().await;
    create(&f.store, "B").await;
    let before = f.store.task("A").await.unwrap();
    assert!(
        f.store
            .finish(&f.run.token, true, "receipt alone", NOW + 3)
            .await
            .is_err()
    );
    let request = command(&f);
    let event = ingest(&f.store, Origin::Human, "accept", json!([request]), NOW + 3).await;
    let processed = NOW + 86_400_000;
    let outcome = f.store.route_at(&event, processed).await.unwrap();
    assert!(matches!(outcome, Outcome::Applied { .. }));
    let task = f.store.task("A").await.unwrap();
    assert_eq!(task.state, State::Succeeded);
    assert_eq!(task.reason.as_deref(), Some("human_accepted_result"));
    assert_eq!(
        (
            task.goal.clone(),
            task.scope.clone(),
            task.authorization_event.clone(),
            task.revision,
            task.model_calls,
            task.tool_calls,
            task.deadline
        ),
        (
            before.goal,
            before.scope,
            before.authorization_event,
            before.revision,
            before.model_calls,
            before.tool_calls,
            before.deadline
        )
    );
    assert_eq!(terminal_time(&f.store).await, Some(processed));
    assert_eq!(f.store.task("B").await.unwrap().state, State::Queued);
    let budget: (i64, String) =
        sqlx::query_as("SELECT charged,state FROM budget WHERE call_id='held-call'")
            .fetch_one(&f.store.application_pool())
            .await
            .unwrap();
    assert_eq!(budget, (20_000, "reserved".into()));
    let evidence = task.evidence.clone();
    assert!(matches!(
        f.store.route_at(&event, processed + 1).await.unwrap(),
        Outcome::Applied { .. }
    ));
    assert!(matches!(
        submit(&f, "accept-again", command(&f), processed + 2).await,
        Outcome::Applied { .. }
    ));
    assert_eq!(terminal_time(&f.store).await, Some(processed));
    assert_eq!(f.store.task("A").await.unwrap().evidence, evidence);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox WHERE dedupe_key='terminal:A:1'")
            .fetch_one(&f.store.application_pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn tool_and_external_sources_cannot_accept_a_result() {
    let f = fixture().await;
    for (origin, source) in [(Origin::Tool, "tool"), (Origin::External, "external")] {
        let event = ingest(&f.store, origin, source, json!([command(&f)]), NOW + 3).await;
        assert!(matches!(
            f.store.route(&event).await.unwrap(),
            Outcome::Rejected { .. }
        ));
    }
    assert_eq!(f.store.task("A").await.unwrap().state, State::Blocked);
    assert_eq!(terminal_time(&f.store).await, None);
}

#[tokio::test]
async fn stale_revision_body_or_saved_review_proof_cannot_be_confirmed() {
    for fault in [
        "revision",
        "message",
        "digest",
        "body",
        "recomputed-body",
        "proof",
        "missing-proof",
        "future-proof",
        "evidence",
        "scope",
    ] {
        let f = fixture().await;
        let mut request = command(&f);
        match fault {
            "revision" => request["expected_revision"] = json!("2"),
            "message" => request["message_id"] = json!("model:another-run:1:0"),
            "digest" => request["chat_digest"] = json!("0".repeat(64)),
            "body" | "recomputed-body" => {
                sqlx::query("UPDATE link_chat SET body='changed' WHERE id=?")
                    .bind(&f.message)
                    .execute(&f.store.application_pool())
                    .await
                    .unwrap();
                if fault == "recomputed-body" {
                    request["chat_digest"] = json!(format!("{:x}", Sha256::digest(b"changed")));
                }
            }
            "proof" | "missing-proof" | "future-proof" => {
                let sql = match fault {
                    "proof" => {
                        "UPDATE runs SET data=json_set(data,'$.checkpoint.result_review.chat_digest','changed')"
                    }
                    "missing-proof" => {
                        "UPDATE runs SET data=json_remove(data,'$.checkpoint.result_review')"
                    }
                    _ => {
                        "UPDATE runs SET data=json_set(data,'$.checkpoint.result_review.saved_at',1800000000100)"
                    }
                };
                sqlx::query(sql)
                    .execute(&f.store.application_pool())
                    .await
                    .unwrap();
            }
            "evidence" => {
                sqlx::query(
                    "UPDATE tasks SET data=json_set(data,'$.evidence',json('[]')) WHERE id='A'",
                )
                .execute(&f.store.application_pool())
                .await
                .unwrap();
            }
            _ => {
                sqlx::query(
                    "UPDATE tasks SET data=json_set(data,'$.scope','fixture:write') WHERE id='A'",
                )
                .execute(&f.store.application_pool())
                .await
                .unwrap();
            }
        }
        assert!(
            matches!(
                submit(&f, "accept", request, NOW + 3).await,
                Outcome::Rejected { .. }
            ),
            "{fault}"
        );
        assert_eq!(terminal_time(&f.store).await, None, "{fault}");
        assert_eq!(
            f.store.task("A").await.unwrap().state,
            State::Blocked,
            "{fault}"
        );
    }
}

#[tokio::test]
async fn cancellation_and_revision_stop_old_review_without_changing_the_old_answer() {
    for kind in ["cancel", "revise"] {
        let f = fixture().await;
        let control = if kind == "cancel" {
            json!({"kind":"cancel","id":"A","expected_revision":"1"})
        } else {
            json!({"kind":"revise","id":"A","expected_revision":"1","goal":"new Human requirement"})
        };
        submit(&f, "change", control, NOW + 3).await;
        let before = serde_json::to_value(f.store.task("A").await.unwrap()).unwrap();
        assert!(matches!(
            submit(&f, "old-review", command(&f), NOW + 4).await,
            Outcome::Rejected { .. }
        ));
        assert_eq!(
            serde_json::to_value(f.store.task("A").await.unwrap()).unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn an_unknown_effect_cannot_be_reported_stopped_by_accepting_a_model_answer() {
    let f = fixture().await;
    let operation = Operation {
        id: "owned-unknown".into(),
        idempotency_key: "owned-unknown".into(),
        token: f.run.token.clone(),
        target: "fixture:write".into(),
        intent: json!({"owned":true}),
        intent_digest: "owned-proof".into(),
        state: OperationState::Unknown,
    };
    // An imported unknown ledger entry must remain unknown even if an adapter
    // previously saved a final-looking answer. No effect is executed here.
    sqlx::query("INSERT INTO operations(id,data) VALUES(?,?)")
        .bind(&operation.id)
        .bind(serde_json::to_string(&operation).unwrap())
        .execute(&f.store.application_pool())
        .await
        .unwrap();
    assert!(matches!(
        submit(&f, "accept", command(&f), NOW + 3).await,
        Outcome::Rejected { .. }
    ));
    assert_eq!(
        f.store.pending_operations("A").await.unwrap()[0].state,
        OperationState::Unknown
    );
    assert_eq!(terminal_time(&f.store).await, None);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM receipts")
        .fetch_one(&f.store.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn final_notification_backpressure_rolls_back_completion_and_its_retention_clock() {
    let f = fixture().await;
    let current: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE acknowledged=0")
        .fetch_one(&f.store.application_pool())
        .await
        .unwrap();
    let extra = 999 - current;
    assert!(extra > 0);
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO outbox(dedupe_key,payload) SELECT 'review-filler:'||i,'{}' FROM n")
        .bind(extra).execute(&f.store.application_pool()).await.unwrap();
    let event = ingest(
        &f.store,
        Origin::Human,
        "accept",
        json!([command(&f)]),
        NOW + 3,
    )
    .await;
    assert!(matches!(
        f.store.route_at(&event, NOW + 3).await,
        Err(Error::Capacity)
    ));
    assert_eq!(f.store.task("A").await.unwrap().state, State::Blocked);
    assert_eq!(terminal_time(&f.store).await, None);
    let outcome: Option<String> = sqlx::query_scalar("SELECT outcome FROM events WHERE id=?")
        .bind(&event)
        .fetch_one(&f.store.application_pool())
        .await
        .unwrap();
    assert_eq!(outcome, None);
    sqlx::query("DELETE FROM outbox WHERE dedupe_key GLOB 'review-filler:*'")
        .execute(&f.store.application_pool())
        .await
        .unwrap();
    assert!(matches!(
        f.store.route_at(&event, NOW + 4).await.unwrap(),
        Outcome::Applied { .. }
    ));
    assert_eq!(terminal_time(&f.store).await, Some(NOW + 4));
}

#[tokio::test]
async fn accepted_and_stale_review_fingerprints_follow_the_same_task_retention() {
    let f = fixture().await;
    let mut stale = command(&f);
    stale["expected_revision"] = json!("2");
    assert!(matches!(
        submit(&f, "stale", stale, NOW + 3).await,
        Outcome::Rejected { .. }
    ));
    assert!(matches!(
        submit(&f, "accept", command(&f), NOW + 4).await,
        Outcome::Applied { .. }
    ));
    f.store
        .maintain(NOW + 4 + MEMORY_RETENTION_MS)
        .await
        .unwrap();
    assert_eq!(
        f.store.task("A").await.unwrap().reason.as_deref(),
        Some("task_retention_expired")
    );
    let values: Vec<String> = sqlx::query_scalar("SELECT payload FROM events")
        .fetch_all(&f.store.application_pool())
        .await
        .unwrap();
    assert!(!values.join("").contains(&f.digest));
    let bytes = std::fs::read(f.root.path().join("db")).unwrap();
    assert!(!bytes.windows(BODY.len()).any(|v| v == BODY.as_bytes()));
    assert!(
        !bytes
            .windows(f.digest.len())
            .any(|v| v == f.digest.as_bytes())
    );
}

#[tokio::test]
async fn review_is_standalone_and_cannot_be_combined_with_other_task_mutations() {
    let f = fixture().await;
    let event = ingest(
        &f.store,
        Origin::Human,
        "mixed",
        json!([command(&f),{"kind":"cancel","id":"A","expected_revision":"1"}]),
        NOW + 3,
    )
    .await;
    assert!(matches!(
        f.store.route(&event).await.unwrap(),
        Outcome::Rejected { .. }
    ));
    assert_eq!(f.store.task("A").await.unwrap().state, State::Blocked);
    assert_eq!(terminal_time(&f.store).await, None);
    let linked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM event_tasks WHERE event_id=? AND task_id='A'")
            .bind(event)
            .fetch_one(&f.store.application_pool())
            .await
            .unwrap();
    assert_eq!(linked, 1);
    let malformed = ingest(
        &f.store,
        Origin::Human,
        "malformed",
        json!(["not a command", null]),
        NOW + 4,
    )
    .await;
    assert!(matches!(
        f.store.route(&malformed).await.unwrap(),
        Outcome::Rejected { .. }
    ));
}

#[tokio::test]
async fn memory_bound_results_stay_unaccepted_until_the_original_memory_gates_are_met() {
    let f = fixture_with_memory(true).await;
    assert!(matches!(submit(&f,"accept",command(&f),NOW+3).await,
        Outcome::Rejected { reason } if reason=="memory-derived result review is not enabled"));
    assert_eq!(f.store.task("A").await.unwrap().state, State::Blocked);
    assert_eq!(terminal_time(&f.store).await, None);
}
