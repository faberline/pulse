use pulse_core::*;
use serde_json::{Value, json};
const NOW: i64 = 1_800_000_000_000;
const BODY: &str = "private memory body which must not enter provenance";

async fn setup() -> (tempfile::TempDir, Store, Memory) {
    setup_at(NOW).await
}
async fn setup_at(memory_at: i64) -> (tempfile::TempDir, Store, Memory) {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    store
        .change_memory(
            Origin::Human,
            &request(
                MemoryCommand::Remember {
                    label: "偏好".into(),
                    body: BODY.into(),
                },
                memory_at,
            ),
            NOW,
        )
        .await
        .unwrap();
    let memory = store.list_memories(NOW).await.unwrap().remove(0);
    create(&store, "a", NOW).await;
    (root, store, memory)
}
fn request(command: MemoryCommand, at: i64) -> MemoryRequest {
    MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: at,
        command,
    }
}
async fn route(store: &Store, commands: Vec<Command>, now: i64) -> Outcome {
    let e = store
        .ingest(
            Origin::Human,
            "fixture",
            &uuid::Uuid::new_v4().to_string(),
            json!({"commands":commands}),
            now,
        )
        .await
        .unwrap();
    store.route(&e.event_id).await.unwrap()
}
async fn create(store: &Store, id: &str, now: i64) {
    assert!(matches!(
        route(
            store,
            vec![Command::Create {
                id: id.into(),
                goal: "分析 Pulse".into(),
                scope: "pulse:read".into(),
                priority: 0,
                requires_device: false,
                parent: None,
                dependencies: vec![]
            }],
            now
        )
        .await,
        Outcome::Applied { .. }
    ));
}
fn adoption(memory: &Memory, task: &str, revision: u64, now: i64) -> TaskMemoryRequest {
    TaskMemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: now,
        task_id: task.into(),
        task_revision: revision,
        sources: vec![MemoryRef {
            id: memory.id.clone(),
            revision: memory.revision,
        }],
    }
}
async fn bind(store: &Store, memory: &Memory) -> TaskMemorySources {
    store
        .bind_task_memories(Origin::Human, &adoption(memory, "a", 1, NOW), NOW)
        .await
        .unwrap()
}
async fn forget(store: &Store, memory: &Memory, now: i64) -> Result<MemoryOutcome> {
    store
        .change_memory(
            Origin::Human,
            &request(
                MemoryCommand::Forget {
                    id: memory.id.clone(),
                    revision: memory.revision,
                },
                now,
            ),
            now,
        )
        .await
}
async fn count(store: &Store, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(&store.application_pool())
        .await
        .unwrap()
}
async fn checkpoint(store: &Store, id: &str) -> Value {
    let data: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=?")
        .bind(id)
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    serde_json::from_str::<Run>(&data).unwrap().checkpoint
}

#[tokio::test]
async fn adoption_is_human_immutable_bounded_and_bodyless_without_authority_expansion() {
    let (_root, store, memory) = setup().await;
    let req = adoption(&memory, "a", 1, NOW);
    assert!(
        store
            .bind_task_memories(Origin::Tool, &req, NOW)
            .await
            .is_err()
    );
    let mut bad = req.clone();
    bad.sources.push(bad.sources[0].clone());
    assert!(
        store
            .bind_task_memories(Origin::Human, &bad, NOW)
            .await
            .is_err()
    );
    bad = req.clone();
    bad.sources[0].revision = 2;
    assert!(
        store
            .bind_task_memories(Origin::Human, &bad, NOW)
            .await
            .is_err()
    );
    let binding = store
        .bind_task_memories(Origin::Human, &req, NOW)
        .await
        .unwrap();
    assert_eq!(binding.request, req);
    assert_eq!(binding.sources[0].source_id, memory.source_id);
    assert_eq!(
        store
            .bind_task_memories(Origin::Human, &req, NOW + 1)
            .await
            .unwrap(),
        binding
    );
    bad = req.clone();
    bad.source_at += 1;
    assert!(
        store
            .bind_task_memories(Origin::Human, &bad, NOW + 1)
            .await
            .is_err()
    );
    assert!(
        store
            .bind_task_memories(Origin::Human, &adoption(&memory, "a", 1, NOW), NOW)
            .await
            .is_err()
    );
    let data: String = sqlx::query_scalar("SELECT data FROM task_memory_sources")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert!(!data.contains(BODY));
    assert!(!data.contains("偏好"));
    let payload: String = sqlx::query_scalar("SELECT payload FROM events WHERE id=?")
        .bind(&binding.authorization_event)
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert!(!payload.contains(BODY));
    assert!(
        !serde_json::to_string(&store.outbox(100).await.unwrap())
            .unwrap()
            .contains(BODY)
    );
    let task = store.task("a").await.unwrap();
    assert_eq!(task.scope, "pulse:read");
    assert_eq!(task.goal, "分析 Pulse");
    assert_eq!(task.model_calls, 0);
    assert_eq!(task.tool_calls, 0);
    assert_eq!(
        count(&store, "SELECT count(*) FROM task_memory_sources").await,
        1
    );
    assert_eq!(
        count(
            &store,
            "SELECT count(*) FROM events WHERE source='task-memory'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn correction_stops_the_old_run_without_refunding_cost_and_needs_fresh_human_adoption() {
    let (_root, store, memory) = setup().await;
    let binding = bind(&store, &memory).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    store
        .reserve(&run.token, "reserved", 20_000, "fixture", NOW)
        .await
        .unwrap();
    store.cost_unknown("reserved").await.unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            json!({"derived":BODY}),
            BODY,
            None,
            NOW,
        )
        .await
        .unwrap();
    store
        .change_memory(
            Origin::Human,
            &request(
                MemoryCommand::Correct {
                    id: memory.id.clone(),
                    revision: 1,
                    label: "偏好".into(),
                    body: "新的偏好".into(),
                },
                NOW + 1,
            ),
            NOW + 1,
        )
        .await
        .unwrap();
    let task = store.task("a").await.unwrap();
    assert_eq!(task.state, State::Blocked);
    assert!(task.active_run.is_none());
    assert!(task.evidence.is_empty());
    assert_eq!(task.reason.as_deref(), Some("memory_source_withdrawn"));
    assert!(
        !checkpoint(&store, &run.token.run_id)
            .await
            .to_string()
            .contains(BODY)
    );
    assert!(
        store
            .reserve(&run.token, "new-call", 20_000, "fixture", NOW + 1)
            .await
            .is_err()
    );
    assert!(
        store
            .advance(&run.token, State::Running, json!({}), "late", None, NOW + 1)
            .await
            .is_err()
    );
    assert!(
        store
            .finish(&run.token, true, "late", NOW + 1)
            .await
            .is_err()
    );
    assert!(
        store
            .task_memory_sources("a", 1)
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
    assert!(
        store
            .bind_task_memories(Origin::Human, &binding.request, NOW + 1)
            .await
            .unwrap()
            .invalidated
    );
    assert_eq!(
        count(&store, "SELECT count(*) FROM budget WHERE state='unknown'").await,
        1
    );
    store
        .settle("reserved", 500, "verified usage only")
        .await
        .unwrap();
    assert_eq!(
        count(&store, "SELECT count(*) FROM budget WHERE state='settled'").await,
        1
    );
    assert!(matches!(
        route(
            &store,
            vec![Command::Revise {
                id: "a".into(),
                expected_revision: 1,
                goal: "使用新偏好重新分析".into(),
                scope: None
            }],
            NOW + 2
        )
        .await,
        Outcome::Applied { .. }
    ));
    let revised = store.list_memories(NOW + 2).await.unwrap().remove(0);
    store
        .bind_task_memories(Origin::Human, &adoption(&revised, "a", 2, NOW + 2), NOW + 2)
        .await
        .unwrap();
    let next = store
        .claim_next(&Versions::fixture(), NOW + 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.token.revision, 2);
    assert!(
        store
            .task_memory_sources("a", 1)
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
}

#[tokio::test]
async fn forgetting_with_an_unknown_operation_preserves_uncertainty_and_late_evidence_never_resumes()
 {
    let (_root, store, memory) = setup().await;
    bind(&store, &memory).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            json!({}),
            "approved fixture",
            None,
            NOW,
        )
        .await
        .unwrap();
    let op = store
        .prepare_operation(
            &run.token,
            "fixture:uncertain",
            json!({"intent":"owned fixture"}),
            NOW,
        )
        .await
        .unwrap();
    forget(&store, &memory, NOW + 1).await.unwrap();
    assert_eq!(store.task("a").await.unwrap().state, State::OutcomeUnknown);
    assert_eq!(store.pending_operations("a").await.unwrap()[0].id, op.id);
    let effect = store
        .record_receipt(&Receipt {
            operation_id: op.id,
            token: run.token.clone(),
            success: true,
            evidence: "late known result".into(),
        })
        .await
        .unwrap();
    assert_eq!(effect, ReceiptEffect::Late);
    assert_eq!(store.task("a").await.unwrap().state, State::Blocked);
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 2)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .prepare_operation(&run.token, "fixture:new", json!({}), NOW + 2)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn source_revocation_and_expiry_both_fence_claims_and_direct_device_dispatch() {
    for expiry in [false, true] {
        let (_root, store, memory) = setup_at(if expiry {
            NOW + 10 - MEMORY_RETENTION_MS
        } else {
            NOW
        })
        .await;
        let source = store.list_memories(NOW).await.unwrap().remove(0);
        bind(&store, &source).await;
        let at = if expiry { NOW + 11 } else { NOW + 1 };
        if expiry {
            assert!(store.list_memories(at).await.unwrap().is_empty());
        } else {
            store
                .change_memory(
                    Origin::Human,
                    &request(
                        MemoryCommand::Revoke {
                            sources: vec![memory.source_id],
                            expected: None,
                        },
                        at,
                    ),
                    at,
                )
                .await
                .unwrap();
        }
        assert!(
            store
                .claim_next(&Versions::fixture(), at)
                .await
                .unwrap()
                .is_none()
        );
        let mut tx = store.application_pool().begin().await.unwrap();
        assert!(
            Store::authorize_device_read(&mut tx, "a", 1, "no-dispatch", at)
                .await
                .is_err()
        );
        assert!(
            Store::validate_task_memory_in(&mut tx, "a", 1, at)
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        assert_eq!(store.task("a").await.unwrap().state, State::Blocked);
    }
}

#[tokio::test]
async fn a_failed_sql_redaction_denies_new_local_commits_until_the_keeper_reconciles() {
    let (_root, store, memory) = setup().await;
    bind(&store, &memory).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("CREATE TRIGGER block_task_redaction BEFORE UPDATE ON tasks BEGIN SELECT RAISE(ABORT,'fixture write failure'); END").execute(&store.application_pool()).await.unwrap();
    assert!(forget(&store, &memory, NOW + 1).await.is_err());
    assert_eq!(count(&store, "SELECT count(*) FROM memories").await, 1); // SQL rollback; negative keeper is already durable.
    assert!(
        store
            .reserve(&run.token, "must-not-send", 20_000, "fixture", NOW + 1)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "SELECT count(*) FROM budget").await, 0);
    sqlx::query("DROP TRIGGER block_task_redaction")
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(
        store
            .advance(&run.token, State::Running, json!({}), "late", None, NOW + 2)
            .await
            .is_err()
    );
    assert!(store.list_memories(NOW + 2).await.unwrap().is_empty());
    assert_eq!(store.task("a").await.unwrap().state, State::Blocked);
    assert!(
        store
            .task_memory_sources("a", 1)
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
}

#[tokio::test]
async fn a_pre_forget_snapshot_replays_source_fences_and_cannot_reclaim_the_old_run() {
    let (root, store, memory) = setup().await;
    bind(&store, &memory).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    let snapshot = root.path().join("snapshot");
    sqlx::query("VACUUM INTO ?")
        .bind(snapshot.to_str().unwrap())
        .execute(&store.application_pool())
        .await
        .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o600)).unwrap();
    forget(&store, &memory, NOW + 1).await.unwrap();
    store.close().await;
    std::fs::copy(snapshot, root.path().join("db")).unwrap();
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    store.recover(&Versions::fixture(), NOW + 2).await.unwrap();
    assert!(store.list_memories(NOW + 2).await.unwrap().is_empty());
    assert_eq!(store.task("a").await.unwrap().state, State::Blocked);
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 2)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .reserve(&run.token, "late", 20_000, "fixture", NOW + 2)
            .await
            .is_err()
    );
    assert!(
        store
            .task_memory_sources("a", 1)
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
}

#[tokio::test]
async fn a_full_outbox_refuses_adoption_atomically_but_cannot_block_erasure_or_its_later_handoff() {
    let (_root, store, memory) = setup().await;
    bind(&store, &memory).await;
    create(&store, "b", NOW).await;
    sqlx::query("UPDATE outbox SET acknowledged=1")
        .execute(&store.application_pool())
        .await
        .unwrap();
    sqlx::query("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<1000) INSERT INTO outbox(dedupe_key,payload) SELECT 'fill-'||n,'{}' FROM seq").execute(&store.application_pool()).await.unwrap();
    let events = count(&store, "SELECT count(*) FROM events").await;
    assert!(
        store
            .bind_task_memories(Origin::Human, &adoption(&memory, "b", 1, NOW), NOW)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "SELECT count(*) FROM events").await, events);
    assert!(store.task_memory_sources("b", 1).await.unwrap().is_none());
    forget(&store, &memory, NOW + 1).await.unwrap();
    assert_eq!(store.task("a").await.unwrap().state, State::Blocked);
    assert!(store.flush_memory_task_changes().await.is_err());
    sqlx::query("UPDATE outbox SET acknowledged=1 WHERE dedupe_key='fill-1'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(store.flush_memory_task_changes().await.unwrap());
    assert!(!store.flush_memory_task_changes().await.unwrap());
    assert_eq!(
        count(&store, "SELECT count(*) FROM outbox WHERE acknowledged=0").await,
        1000
    );
}

#[tokio::test]
async fn withdrawing_an_old_revision_clears_its_checkpoint_and_keeps_a_new_independent_run() {
    let (_root, store, memory) = setup().await;
    bind(&store, &memory).await;
    let old = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    store
        .advance(
            &old.token,
            State::Ready,
            json!({"derived":BODY}),
            "old context",
            None,
            NOW,
        )
        .await
        .unwrap();
    route(
        &store,
        vec![Command::Revise {
            id: "a".into(),
            expected_revision: 1,
            goal: "新的獨立要求".into(),
            scope: None,
        }],
        NOW + 1,
    )
    .await;
    let current = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .advance(
            &current.token,
            State::Ready,
            json!({"independent":"new context"}),
            "new context",
            None,
            NOW + 1,
        )
        .await
        .unwrap();
    forget(&store, &memory, NOW + 2).await.unwrap();
    assert!(
        !checkpoint(&store, &old.token.run_id)
            .await
            .to_string()
            .contains(BODY)
    );
    assert_eq!(
        checkpoint(&store, &current.token.run_id).await,
        json!({"independent":"new context"})
    );
    let task = store.task("a").await.unwrap();
    assert_eq!(task.state, State::Ready);
    assert_eq!(
        task.active_run.as_deref(),
        Some(current.token.run_id.as_str())
    );
    assert!(
        store
            .task_memory_sources("a", 1)
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
    assert!(store.task_memory_sources("a", 2).await.unwrap().is_none());
}

#[tokio::test]
async fn changed_source_authority_is_denied_and_missing_memory_keeper_does_not_grant_or_block_unrelated_work()
 {
    let (root, store, memory) = setup().await;
    let binding = bind(&store, &memory).await;
    sqlx::query("UPDATE events SET origin='tool' WHERE id=?")
        .bind(binding.authorization_event)
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.task("a").await.unwrap().reason.as_deref(),
        Some("memory_source_unavailable")
    );
    create(&store, "b", NOW).await;
    std::fs::remove_file(root.path().join("db.memory-restrictions.json")).unwrap();
    assert!(store.lease_task_memory("a", NOW).await.is_err());
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "b");
    assert!(
        store
            .reserve(&run.token, "unrelated", 20_000, "fixture", NOW)
            .await
            .unwrap()
    );
}
