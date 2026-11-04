use pulse_core::*;
use serde_json::{Value, json};
use std::path::Path;

const NOW: i64 = 1_800_000_000_000;
const RETENTION: i64 = 90 * 24 * 60 * 60 * 1_000;

async fn open(root: &Path) -> Store {
    Store::open(root.join("db"), "owner").await.unwrap()
}
fn create(id: &str, goal: &str) -> Value {
    json!({"kind":"create","id":id,"goal":goal,"scope":"pulse:read","priority":0,"requires_device":false})
}
async fn send(store: &Store, source: &str, commands: Value, now: i64) -> Outcome {
    let event = store
        .ingest(
            Origin::Human,
            "fixture",
            source,
            json!({"commands":commands}),
            now,
        )
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap()
}
async fn add(store: &Store, id: &str, marker: &str) {
    assert!(matches!(
        send(store, id, json!([create(id, marker)]), NOW).await,
        Outcome::Applied { .. }
    ));
}
async fn cancel(store: &Store, id: &str, now: i64) {
    assert!(matches!(
        send(
            store,
            &format!("cancel-{id}"),
            json!([{"kind":"cancel","id":id,"expected_revision":"1"}]),
            now
        )
        .await,
        Outcome::Applied { .. }
    ));
}
async fn terminal_time(store: &Store, id: &str) -> Option<i64> {
    sqlx::query_scalar("SELECT terminal_at FROM task_retention WHERE task_id=?")
        .bind(id)
        .fetch_optional(&store.application_pool())
        .await
        .unwrap()
}
async fn snapshot(store: &Store, root: &Path) -> std::path::PathBuf {
    let path = root.join("snapshot");
    sqlx::query("VACUUM INTO ?")
        .bind(path.to_str().unwrap())
        .execute(&store.application_pool())
        .await
        .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    path
}
fn restrictions(root: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(root.join("db.memory-restrictions.json")).unwrap())
        .unwrap()
}

#[tokio::test]
async fn retention_uses_first_terminal_time_and_leaves_active_tasks_alone() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "active", "LIVE_TASK_SOURCE").await;
    add(&store, "finished", "EXPIRED_TASK_SOURCE").await;
    cancel(&store, "finished", NOW + 1000).await;
    assert_eq!(terminal_time(&store, "active").await, None);
    assert_eq!(terminal_time(&store, "finished").await, Some(NOW + 1000));
    send(
        &store,
        "late-query",
        json!([{"kind":"query","ids":["finished"]}]),
        NOW + 10_000,
    )
    .await;
    assert_eq!(terminal_time(&store, "finished").await, Some(NOW + 1000));
    let due = NOW + 1000 + RETENTION;
    let before = store.maintain(due - 1).await.unwrap();
    assert_eq!(before.next_at, due);
    assert_eq!(
        store.task("finished").await.unwrap().goal,
        "EXPIRED_TASK_SOURCE"
    );
    store.maintain(due).await.unwrap();
    assert_eq!(store.task("active").await.unwrap().goal, "LIVE_TASK_SOURCE");
    let task = store.task("finished").await.unwrap();
    assert_eq!(task.state, State::Cancelled);
    assert_eq!(task.reason.as_deref(), Some("task_retention_expired"));
    assert!(task.decisions.is_empty() && task.evidence.is_empty() && task.scope.is_empty());
    assert!(
        !serde_json::to_string(&task)
            .unwrap()
            .contains("EXPIRED_TASK_SOURCE")
    );
    assert!(restrictions(root.path())["expired_tasks"]["finished"].is_object());
    assert!(store.maintain(NOW).await.is_err());
    assert_eq!(
        store.task("finished").await.unwrap().reason.as_deref(),
        Some("task_retention_expired")
    );
    let bytes = std::fs::read(root.path().join("db")).unwrap();
    assert!(
        !bytes
            .windows(b"EXPIRED_TASK_SOURCE".len())
            .any(|v| v == b"EXPIRED_TASK_SOURCE")
    );
    store.close().await;
}

#[tokio::test]
async fn shared_human_source_waits_for_the_last_referring_task() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    let marker = "SHARED_HUMAN_SOURCE";
    for id in ["shared", "reply:shared"] {
        sqlx::query("INSERT INTO link_chat(id,body,created_at) VALUES(?,?,?)")
            .bind(id)
            .bind(marker)
            .bind(NOW)
            .execute(&store.application_pool())
            .await
            .unwrap();
    }
    assert!(matches!(
        send(
            &store,
            "shared",
            json!([create("A", marker), create("B", marker)]),
            NOW
        )
        .await,
        Outcome::Applied { .. }
    ));
    cancel(&store, "A", NOW + 1).await;
    store.maintain(NOW + 1 + RETENTION).await.unwrap();
    let source: String = sqlx::query_scalar("SELECT payload FROM events WHERE source_id='shared'")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert!(source.contains(marker));
    assert_eq!(store.task("B").await.unwrap().goal, marker);
    cancel(&store, "B", NOW + RETENTION + 2).await;
    store.maintain(NOW + 2 * RETENTION + 2).await.unwrap();
    let source: String = sqlx::query_scalar("SELECT payload FROM events WHERE source_id='shared'")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert!(!source.contains(marker));
    let bodies: Vec<String> = sqlx::query_scalar("SELECT body FROM link_chat")
        .fetch_all(&store.application_pool())
        .await
        .unwrap();
    assert!(!bodies.join("").contains(marker));
    assert!(
        store
            .ingest(
                Origin::Human,
                "fixture",
                "shared",
                json!({"commands":[create("A",marker),create("B",marker)]}),
                NOW + 2 * RETENTION + 3
            )
            .await
            .is_err()
    );
    let conflicts: i64 = sqlx::query_scalar("SELECT count(*) FROM conflicts")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(conflicts, 0);
    store.close().await;
}

#[tokio::test]
async fn failed_sql_scrub_blocks_reads_and_an_old_database_replays_the_external_restriction() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "ROLLBACK_PRIVATE_SOURCE").await;
    cancel(&store, "A", NOW + 1).await;
    let old = snapshot(&store, root.path()).await;
    sqlx::raw_sql("CREATE TRIGGER fail_retention BEFORE UPDATE OF data ON tasks WHEN NEW.id='A' AND json_extract(NEW.data,'$.reason')='task_retention_expired' BEGIN SELECT RAISE(ABORT,'owned fault'); END;")
        .execute(&store.application_pool()).await.unwrap();
    assert!(store.maintain(NOW + 1 + RETENTION).await.is_err());
    assert!(restrictions(root.path())["expired_tasks"]["A"].is_object());
    assert!(store.task("A").await.is_err());
    assert!(store.list_tasks(20).await.is_err());
    store.close().await;
    std::fs::copy(old, root.path().join("db")).unwrap();
    let store = open(root.path()).await;
    assert_eq!(
        store.task("A").await.unwrap().reason.as_deref(),
        Some("task_retention_expired")
    );
    assert!(
        !std::fs::read(root.path().join("db"))
            .unwrap()
            .windows(b"ROLLBACK_PRIVATE_SOURCE".len())
            .any(|v| v == b"ROLLBACK_PRIVATE_SOURCE")
    );
    store.close().await;
}

#[tokio::test]
async fn a_snapshot_before_task_creation_cannot_reuse_an_expired_task_id() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    let old = snapshot(&store, root.path()).await;
    add(&store, "A", "RETIRED_SOURCE").await;
    cancel(&store, "A", NOW + 1).await;
    store.maintain(NOW + 1 + RETENTION).await.unwrap();
    store.close().await;
    std::fs::copy(old, root.path().join("db")).unwrap();
    let store = open(root.path()).await;
    assert!(matches!(
        send(
            &store,
            "reuse",
            json!([create("A", "new content")]),
            NOW + 2 + RETENTION
        )
        .await,
        Outcome::Rejected { .. }
    ));
    assert!(store.task("A").await.is_err());
    store.close().await;
}

#[tokio::test]
async fn unknown_effect_has_no_terminal_clock_and_expiry_never_replays_it_or_refunds_cost() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "UNKNOWN_SOURCE").await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .reserve(&run.token, "call", 20_000, "fixture", NOW + 2)
        .await
        .unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            json!({"phase":"ready"}),
            "ready",
            None,
            NOW + 3,
        )
        .await
        .unwrap();
    let operation = store
        .prepare_operation(
            &run.token,
            "fixture:write",
            json!({"text":"UNKNOWN_SOURCE"}),
            NOW + 4,
        )
        .await
        .unwrap();
    cancel(&store, "A", NOW + 5).await;
    assert_eq!(terminal_time(&store, "A").await, None);
    store.maintain(NOW + RETENTION + 6).await.unwrap();
    assert_eq!(store.task("A").await.unwrap().state, State::OutcomeUnknown);
    assert_eq!(store.task("A").await.unwrap().goal, "UNKNOWN_SOURCE");
    let amount: i64 = sqlx::query_scalar("SELECT charged FROM budget WHERE call_id='call'")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(amount, 20_000);
    store
        .reconcile(
            &operation.id,
            Resolution::NotExecuted {
                evidence: "owned adapter proof".into(),
            },
            NOW + RETENTION + 7,
        )
        .await
        .unwrap();
    assert_eq!(terminal_time(&store, "A").await, Some(NOW + RETENTION + 7));
    store.maintain(NOW + 2 * RETENTION + 7).await.unwrap();
    assert_eq!(store.task("A").await.unwrap().tool_calls, 1);
    let amount: i64 = sqlx::query_scalar("SELECT charged FROM budget WHERE call_id='call'")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(amount, 20_000);
    store
        .settle("call", 1234, "LATE_PRIVATE_USAGE")
        .await
        .unwrap();
    store
        .settle("call", 1234, "OTHER_PRIVATE_USAGE")
        .await
        .unwrap();
    assert!(store.settle("call", 1, "wrong amount").await.is_err());
    let row: (i64, Option<String>) =
        sqlx::query_as("SELECT charged,evidence FROM budget WHERE call_id='call'")
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
    assert_eq!(row, (1234, None));
    store.close().await;
}

#[tokio::test]
async fn legacy_terminal_without_original_timestamp_is_not_assigned_a_query_time() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "LEGACY_CONTENT").await;
    cancel(&store, "A", NOW + 1).await;
    sqlx::query("DELETE FROM task_retention WHERE task_id='A'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    send(
        &store,
        "read-legacy",
        json!([{"kind":"query","ids":["A"]}]),
        NOW + RETENTION,
    )
    .await;
    store.maintain(NOW + 2 * RETENTION).await.unwrap();
    assert_eq!(terminal_time(&store, "A").await, None);
    assert_eq!(store.task("A").await.unwrap().goal, "LEGACY_CONTENT");
    store.close().await;
}

#[tokio::test]
async fn completed_receipts_keep_outcome_and_counters_without_reintroducing_expired_text() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "COMPLETE_PRIVATE_TEXT").await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            json!({"phase":"ready","body":"COMPLETE_PRIVATE_TEXT"}),
            "proof",
            None,
            NOW + 2,
        )
        .await
        .unwrap();
    let op = store
        .prepare_operation(
            &run.token,
            "fixture:write",
            json!({"text":"COMPLETE_PRIVATE_TEXT"}),
            NOW + 3,
        )
        .await
        .unwrap();
    let receipt = Receipt {
        operation_id: op.id.clone(),
        token: op.token.clone(),
        success: true,
        evidence: "COMPLETE_PRIVATE_TEXT".into(),
    };
    store.record_receipt_at(&receipt, NOW + 4).await.unwrap();
    store
        .finish(&run.token, true, "COMPLETE_PRIVATE_TEXT", NOW + 5)
        .await
        .unwrap();
    assert_eq!(terminal_time(&store, "A").await, Some(NOW + 5));
    store.maintain(NOW + RETENTION + 5).await.unwrap();
    assert_eq!(
        store
            .record_receipt_at(&receipt, NOW + RETENTION + 6)
            .await
            .unwrap(),
        ReceiptEffect::Late
    );
    assert!(store.receipts("A").await.unwrap()[0].success);
    assert_eq!(store.task("A").await.unwrap().tool_calls, 1);
    let operation: String = sqlx::query_scalar("SELECT data FROM operations WHERE id=?")
        .bind(&op.id)
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    let operation: Operation = serde_json::from_str(&operation).unwrap();
    assert_eq!(operation.state, OperationState::Completed);
    assert!(operation.target.is_empty() && operation.intent_digest.is_empty());
    let bytes = std::fs::read(root.path().join("db")).unwrap();
    assert!(
        !bytes
            .windows(b"COMPLETE_PRIVATE_TEXT".len())
            .any(|b| b == b"COMPLETE_PRIVATE_TEXT")
    );
    store.close().await;
}

#[tokio::test]
async fn offline_restore_of_a_recent_backup_keeps_expiry_and_original_known_financial_outcome() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "RESTORE_RETENTION_PRIVATE_SOURCE").await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .reserve(&run.token, "original-call", 20_000, "fixture", NOW + 2)
        .await
        .unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            json!({"phase":"ready"}),
            "proof",
            None,
            NOW + 3,
        )
        .await
        .unwrap();
    let op = store
        .prepare_operation(
            &run.token,
            "fixture:write",
            json!({"body":"RESTORE_RETENTION_PRIVATE_SOURCE"}),
            NOW + 4,
        )
        .await
        .unwrap();
    let receipt = Receipt {
        operation_id: op.id,
        token: op.token,
        success: true,
        evidence: "RESTORE_RETENTION_PRIVATE_SOURCE".into(),
    };
    store.record_receipt_at(&receipt, NOW + 5).await.unwrap();
    store
        .finish(
            &run.token,
            true,
            "RESTORE_RETENTION_PRIVATE_SOURCE",
            NOW + 6,
        )
        .await
        .unwrap();
    let due = NOW + 6 + RETENTION;
    let backup = store.maintain(due - 1).await.unwrap().backup.unwrap();
    store
        .settle("original-call", 7777, "PRIVATE_PROVIDER_USAGE")
        .await
        .unwrap();
    store.maintain(due).await.unwrap();
    store.close().await;
    let database = root.path().join("db");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let admission = loop {
        match DatabaseMaintenance::try_acquire(&database) {
            Ok(a) => break a,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await
            }
            Err(e) => panic!("{e}"),
        }
    };
    admission
        .restore_backup(&backup, "owner", &Versions::fixture(), due + 5)
        .await
        .unwrap();
    drop(admission);
    let store = open(root.path()).await;
    let task = store.task("A").await.unwrap();
    assert_eq!(task.state, State::Succeeded);
    assert!(!task.goal.contains("PRIVATE_SOURCE"));
    let receipts = store.receipts("A").await.unwrap();
    assert_eq!(receipts.len(), 1);
    assert!(receipts[0].success && receipts[0].evidence.is_empty());
    let fee: (i64, String, Option<String>) =
        sqlx::query_as("SELECT charged,state,evidence FROM budget WHERE call_id='original-call'")
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
    assert_eq!(fee, (7777, "settled".into(), None));
    let bytes = std::fs::read(&database).unwrap();
    assert!(
        !bytes
            .windows(b"RESTORE_RETENTION_PRIVATE_SOURCE".len())
            .any(|v| v == b"RESTORE_RETENTION_PRIVATE_SOURCE")
    );
    store.close().await;
}

#[tokio::test]
async fn invalid_terminal_time_and_removed_external_expiry_proof_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "SOURCE").await;
    cancel(&store, "A", NOW + 1).await;
    sqlx::query("UPDATE task_retention SET terminal_at=0 WHERE task_id='A'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(store.maintain(NOW + RETENTION + 1).await.is_err());
    assert!(restrictions(root.path()).get("expired_tasks").is_none());
    sqlx::query("UPDATE task_retention SET terminal_at=? WHERE task_id='A'")
        .bind(NOW + 1)
        .execute(&store.application_pool())
        .await
        .unwrap();
    store.maintain(NOW + RETENTION + 1).await.unwrap();
    store.close().await;
    let mut manifest = restrictions(root.path());
    manifest.as_object_mut().unwrap().remove("expired_tasks");
    std::fs::write(
        root.path().join("db.memory-restrictions.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(Store::open(root.path().join("db"), "owner").await.is_err());
}

#[tokio::test]
async fn verified_reconciliation_of_retired_unknown_metadata_keeps_private_proof_out_of_storage() {
    for conclusion in ["executed", "not-executed", "ambiguous"] {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path()).await;
        add(&store, "A", "OLD_RECONCILIATION_BODY").await;
        let run = store
            .claim_next(&Versions::fixture(), NOW + 1)
            .await
            .unwrap()
            .unwrap();
        store
            .advance(
                &run.token,
                State::Ready,
                json!({"phase":"ready"}),
                "ready",
                None,
                NOW + 2,
            )
            .await
            .unwrap();
        let op = store
            .prepare_operation(
                &run.token,
                "fixture:write",
                json!({"text":"OLD_RECONCILIATION_BODY"}),
                NOW + 3,
            )
            .await
            .unwrap();
        let old = snapshot(&store, root.path()).await;
        let receipt = Receipt {
            operation_id: op.id.clone(),
            token: op.token.clone(),
            success: true,
            evidence: "OLD_RECONCILIATION_BODY".into(),
        };
        store.record_receipt_at(&receipt, NOW + 4).await.unwrap();
        store
            .finish(&run.token, true, "verified", NOW + 5)
            .await
            .unwrap();
        store.maintain(NOW + RETENTION + 5).await.unwrap();
        store.close().await;
        std::fs::copy(old, root.path().join("db")).unwrap();
        let store = open(root.path()).await;
        assert_eq!(store.task("A").await.unwrap().state, State::OutcomeUnknown);
        assert_eq!(
            store
                .record_receipt_at(&receipt, NOW + RETENTION + 6)
                .await
                .unwrap(),
            ReceiptEffect::Late
        );
        assert_eq!(store.pending_operations("A").await.unwrap().len(), 1);
        let resolution = match conclusion {
            "executed" => Resolution::Executed(Receipt {
                evidence: "NEW_PRIVATE_PROOF".into(),
                ..receipt
            }),
            "not-executed" => Resolution::NotExecuted {
                evidence: "NEW_PRIVATE_PROOF".into(),
            },
            _ => Resolution::Ambiguous {
                evidence: "NEW_PRIVATE_PROOF".into(),
            },
        };
        store
            .reconcile(&op.id, resolution, NOW + RETENTION + 7)
            .await
            .unwrap();
        let task = store.task("A").await.unwrap();
        assert_eq!(task.reason.as_deref(), Some("task_retention_expired"));
        assert!(task.evidence.is_empty());
        assert_eq!(
            task.state,
            if conclusion == "ambiguous" {
                State::Blocked
            } else {
                State::Cancelled
            }
        );
        let values:Vec<String>=sqlx::query_scalar("SELECT data FROM tasks UNION ALL SELECT data FROM receipts UNION ALL SELECT data FROM operations").fetch_all(&store.application_pool()).await.unwrap();
        assert!(!values.join("").contains("NEW_PRIVATE_PROOF"));
        if conclusion == "executed" {
            assert!(store.receipts("A").await.unwrap()[0].success);
        }
        if conclusion == "ambiguous" {
            store
                .reconcile(
                    &op.id,
                    Resolution::NotExecuted {
                        evidence: "RECHECK_PRIVATE_PROOF".into(),
                    },
                    NOW + RETENTION + 8,
                )
                .await
                .unwrap();
            assert_eq!(store.task("A").await.unwrap().state, State::Cancelled);
            assert_eq!(terminal_time(&store, "A").await, None);
            store.maintain(NOW + RETENTION + 9).await.unwrap();
        }
        store.close().await;
    }
}

#[tokio::test]
async fn restore_retirement_keeps_review_evidence_and_is_not_a_data_expiry() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "RECENT_RESTORE_SOURCE").await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .reserve(&run.token, "restore-call", 20_000, "fixture", NOW + 2)
        .await
        .unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            json!({"phase":"ready"}),
            "proof",
            None,
            NOW + 3,
        )
        .await
        .unwrap();
    let op = store
        .prepare_operation(
            &run.token,
            "fixture:write",
            json!({"body":"RECENT_RESTORE_SOURCE"}),
            NOW + 4,
        )
        .await
        .unwrap();
    let backup = store.maintain(NOW + 5).await.unwrap().backup.unwrap();
    store.close().await;
    let database = root.path().join("db");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let admission = loop {
        match DatabaseMaintenance::try_acquire(&database) {
            Ok(a) => break a,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await
            }
            Err(e) => panic!("{e}"),
        }
    };
    admission
        .restore_backup(&backup, "owner", &Versions::fixture(), NOW + 6)
        .await
        .unwrap();
    drop(admission);
    let store = open(root.path()).await;
    store
        .reconcile(
            &op.id,
            Resolution::Executed(Receipt {
                operation_id: op.id.clone(),
                token: op.token,
                success: true,
                evidence: "RECENT_VERIFIED_RESULT".into(),
            }),
            NOW + 7,
        )
        .await
        .unwrap();
    assert_eq!(store.task("A").await.unwrap().goal, "RECENT_RESTORE_SOURCE");
    assert_ne!(
        store.task("A").await.unwrap().reason.as_deref(),
        Some("task_retention_expired")
    );
    assert_eq!(
        store.receipts("A").await.unwrap()[0].evidence,
        "RECENT_VERIFIED_RESULT"
    );
    store
        .settle("restore-call", 7777, "RECENT_VERIFIED_USAGE")
        .await
        .unwrap();
    let evidence: Option<String> =
        sqlx::query_scalar("SELECT evidence FROM budget WHERE call_id='restore-call'")
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
    assert_eq!(evidence.as_deref(), Some("RECENT_VERIFIED_USAGE"));
    assert!(restrictions(root.path()).get("expired_tasks").is_none());
    store.close().await;
}

#[tokio::test]
async fn failed_completion_transaction_never_starts_the_retention_clock() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "LIVE_SOURCE").await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000) INSERT INTO outbox(dedupe_key,payload) SELECT 'retention-backpressure:'||i,'{}' FROM n").execute(&store.application_pool()).await.unwrap();
    assert!(matches!(
        store.finish(&run.token, true, "verified", NOW + 2).await,
        Err(Error::Capacity)
    ));
    assert_eq!(terminal_time(&store, "A").await, None);
    assert_eq!(store.task("A").await.unwrap().state, State::Planning);
    sqlx::query("DELETE FROM outbox WHERE dedupe_key LIKE 'retention-backpressure:%'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    store
        .finish(&run.token, true, "verified", NOW + 3)
        .await
        .unwrap();
    assert_eq!(terminal_time(&store, "A").await, Some(NOW + 3));
    store.close().await;
}

#[tokio::test]
async fn delayed_cancel_uses_processing_time_without_extending_original_task_deadline() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    let created = store
        .ingest(
            Origin::Human,
            "fixture",
            "A",
            json!({"commands":[create("A","DELAYED_PRIVATE_SOURCE")]}),
            NOW,
        )
        .await
        .unwrap();
    store.route_at(&created.event_id, NOW + 1000).await.unwrap();
    let original = store.task("A").await.unwrap();
    assert_eq!(original.created_at, NOW);
    let event = store
        .ingest(
            Origin::Human,
            "fixture",
            "cancel-A",
            json!({"commands":[{"kind":"cancel","id":"A","expected_revision":"1"}]}),
            NOW + 1001,
        )
        .await
        .unwrap();
    let processed = NOW + RETENTION + 2000;
    store.route_at(&event.event_id, processed).await.unwrap();
    assert_eq!(terminal_time(&store, "A").await, Some(processed));
    assert_eq!(store.task("A").await.unwrap().deadline, original.deadline);
    store
        .route_at(&event.event_id, processed + 1000)
        .await
        .unwrap();
    assert_eq!(terminal_time(&store, "A").await, Some(processed));
    store.maintain(processed + RETENTION - 1).await.unwrap();
    assert_eq!(
        store.task("A").await.unwrap().goal,
        "DELAYED_PRIVATE_SOURCE"
    );
    store.maintain(processed + RETENTION).await.unwrap();
    assert_eq!(
        store.task("A").await.unwrap().reason.as_deref(),
        Some("task_retention_expired")
    );
    store.close().await;
}

#[tokio::test]
async fn maintenance_finishes_all_due_batches_before_returning_success() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    for n in 0..70 {
        let id = format!("batch-{n:03}");
        add(&store, &id, "BATCH_PRIVATE_SOURCE").await;
        cancel(&store, &id, NOW + 1 + n).await;
    }
    let now = NOW + RETENTION + 100;
    let result = store.maintain(now).await.unwrap();
    assert!(result.next_at > now);
    let bodies = store.list_tasks(100).await.unwrap();
    assert_eq!(bodies.len(), 70);
    assert!(
        bodies
            .iter()
            .all(|task| task.reason.as_deref() == Some("task_retention_expired"))
    );
    assert_eq!(
        restrictions(root.path())["expired_tasks"]
            .as_object()
            .unwrap()
            .len(),
        70
    );
    store.close().await;
}

#[tokio::test]
async fn untrusted_payload_expiry_label_neither_claims_authority_nor_skips_erasure() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    let payload = json!({"commands":[create("A","LABEL_PRIVATE_SOURCE")],"retention_expired":true});
    let event = store
        .ingest(Origin::Human, "fixture", "A", payload.clone(), NOW)
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap();
    assert!(
        store
            .ingest(Origin::Human, "fixture", "A", payload.clone(), NOW + 1)
            .await
            .unwrap()
            .duplicate
    );
    cancel(&store, "A", NOW + 2).await;
    store.maintain(NOW + RETENTION + 2).await.unwrap();
    let body: String = sqlx::query_scalar("SELECT payload FROM events WHERE id=?")
        .bind(event.event_id)
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert!(!body.contains("LABEL_PRIVATE_SOURCE"));
    assert!(
        store
            .ingest(Origin::Human, "fixture", "A", payload, NOW + RETENTION + 3)
            .await
            .is_err()
    );
    store.close().await;
}

#[tokio::test]
async fn full_history_stops_new_work_before_it_can_consume_retirement_space() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    add(&store, "A", "LAST_ACCEPTED_PRIVATE_SOURCE").await;
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<9999) INSERT INTO tasks(id,data) SELECT 'legacy-'||i,json_set(t.data,'$.id','legacy-'||i,'$.state','succeeded') FROM n CROSS JOIN tasks t WHERE t.id='A'").execute(&store.application_pool()).await.unwrap();
    let outcome = send(
        &store,
        "too-many",
        json!([create("B", "not accepted")]),
        NOW + 1,
    )
    .await;
    assert!(
        matches!(outcome,Outcome::Rejected{ref reason} if reason=="task history capacity; retirement space unavailable")
    );
    assert!(store.task("B").await.is_err());
    cancel(&store, "A", NOW + 2).await;
    store.maintain(NOW + RETENTION + 2).await.unwrap();
    assert_eq!(
        store.task("A").await.unwrap().reason.as_deref(),
        Some("task_retention_expired")
    );
    assert_eq!(
        restrictions(root.path())["expired_tasks"]
            .as_object()
            .unwrap()
            .len(),
        1
    );
    store.close().await;
}

#[tokio::test]
async fn unrepresentable_task_identity_cannot_poison_the_external_restriction_file() {
    let root = tempfile::tempdir().unwrap();
    let store = open(root.path()).await;
    assert!(matches!(
        send(
            &store,
            "oversized",
            json!([create(&"a".repeat(129), "source")]),
            NOW
        )
        .await,
        Outcome::Rejected { .. }
    ));
    add(&store, "A", "SOURCE").await;
    cancel(&store, "A", NOW + 1).await;
    let before = std::fs::read(root.path().join("db.memory-restrictions.json")).unwrap();
    sqlx::query("UPDATE tasks SET data=json_set(data,'$.revision',?) WHERE id='A'")
        .bind(u64::MAX.to_string())
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(store.maintain(NOW + RETENTION + 1).await.is_err());
    assert_eq!(
        std::fs::read(root.path().join("db.memory-restrictions.json")).unwrap(),
        before
    );
    store.close().await;
}
