use pulse_core::*;
use serde_json::json;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const NOW: i64 = 1_800_000_000_000;
const DAY: i64 = 86_400_000;
const DELETED: &str = "OWNED_RESTORE_FORGOTTEN_SOURCE_繁體中文";

async fn command(store: &Store, id: &str, value: serde_json::Value, now: i64) -> Outcome {
    let event = store
        .ingest(
            Origin::Human,
            "owned-restore-test",
            id,
            json!({"commands":[value]}),
            now,
        )
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap()
}
async fn create(store: &Store, id: &str, now: i64) {
    let outcome = command(store, id, json!({"kind":"create","id":id,"goal":"owned restore task","scope":"pulse:read","priority":0,"requires_device":false}), now).await;
    assert!(matches!(outcome, Outcome::Applied { .. }));
}
async fn admission(database: &Path) -> DatabaseMaintenance {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match DatabaseMaintenance::try_acquire(database) {
            Ok(value) => return value,
            Err(error) if Instant::now() >= until => panic!("{error}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}
async fn remember(store: &Store, label: &str, body: &str, now: i64) -> String {
    let outcome = store
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                command: MemoryCommand::Remember {
                    label: label.into(),
                    body: body.into(),
                },
            },
            now,
        )
        .await
        .unwrap();
    match outcome {
        MemoryOutcome::Stored { id, .. } => id,
        _ => panic!("not stored"),
    }
}
async fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf, Store, Receipt) {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let db = dir.path().join("db");
    let store = Store::open(&db, "owner").await.unwrap();
    let memory = remember(&store, "forgotten", DELETED, NOW).await;
    create(&store, "A", NOW).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .reserve(
            &run.token,
            "held-original-call",
            20_000,
            "fixture-rate",
            NOW + 1,
        )
        .await
        .unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            json!({}),
            "owned checkpoint",
            None,
            NOW + 2,
        )
        .await
        .unwrap();
    store
        .prepare_operation(
            &run.token,
            "fixture:original-unknown",
            json!({"delta":1}),
            NOW + 3,
        )
        .await
        .unwrap();
    let backup = store.maintain(NOW + 4).await.unwrap().backup.unwrap();
    store
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: NOW + 5,
                command: MemoryCommand::Forget {
                    id: memory,
                    revision: 1,
                },
            },
            NOW + 5,
        )
        .await
        .unwrap();
    remember(
        &store,
        "current only",
        "OWNED_CURRENT_POST_BACKUP_MEMORY",
        NOW + 5,
    )
    .await;
    command(
        &store,
        "revise-A",
        json!({"kind":"revise","id":"A","expected_revision":"1","goal":"newer effective decision"}),
        NOW + 5,
    )
    .await;
    create(&store, "B", NOW + 6).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "B");
    store
        .advance(
            &run.token,
            State::Ready,
            json!({}),
            "owned checkpoint",
            None,
            NOW + 7,
        )
        .await
        .unwrap();
    let operation = store
        .prepare_operation(&run.token, "fixture:known", json!({"delta":1}), NOW + 7)
        .await
        .unwrap();
    let receipt = Receipt {
        operation_id: operation.id,
        token: run.token.clone(),
        success: true,
        evidence: "OWNED_ORIGINAL_RESULT".into(),
    };
    store.record_receipt(&receipt).await.unwrap();
    store
        .finish(&run.token, true, "owned goal verification", NOW + 8)
        .await
        .unwrap();
    (dir, db, backup, store, receipt)
}

fn no_plaintext(path: &Path) {
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            no_plaintext(&entry.path());
        } else {
            let bytes = fs::read(entry.path()).unwrap();
            assert!(
                !bytes
                    .windows(DELETED.len())
                    .any(|part| part == DELETED.as_bytes()),
                "{}",
                entry.path().display()
            );
        }
    }
}

#[tokio::test]
async fn latest_settlement_epoch_and_post_restore_activity_survive_repeat_resume() {
    let (_dir, db, backup, store, _receipt) = fixture().await;
    store
        .settle("held-original-call", 7_000, "OWNED_ORIGINAL_PROVIDER_USAGE")
        .await
        .unwrap();
    sqlx::query("UPDATE settings SET value='1000000' WHERE key='epoch'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    store.close().await;
    let lease = admission(&db).await;
    let first = lease
        .restore_backup(&backup, "owner", &Versions::fixture(), NOW + 10)
        .await
        .unwrap();
    drop(lease);
    let store = Store::open(&db, "owner").await.unwrap();
    let cost: (String, i64, String) = sqlx::query_as(
        "SELECT state,charged,evidence FROM budget WHERE call_id='held-original-call'",
    )
    .fetch_one(&store.application_pool())
    .await
    .unwrap();
    assert_eq!(
        cost,
        (
            "settled".into(),
            7_000,
            "OWNED_ORIGINAL_PROVIDER_USAGE".into()
        )
    );
    let later = NOW + DAY;
    create(&store, "new-after-restore", later).await;
    let run = store
        .claim_next(&Versions::fixture(), later + 1)
        .await
        .unwrap()
        .unwrap();
    assert!(run.token.epoch > 1_000_000);
    assert!(
        store
            .reserve(
                &run.token,
                "new-after-restore-call",
                20_000,
                "fixture-rate",
                later + 1
            )
            .await
            .unwrap()
    );
    remember(
        &store,
        "new after restore",
        "OWNED_NEW_POST_RESTORE_MEMORY",
        later + 1,
    )
    .await;
    store.close().await;
    let lease = admission(&db).await;
    let resumed = lease
        .resume_restore("owner", &Versions::fixture(), later + 2)
        .await
        .unwrap();
    assert!(resumed.already_committed);
    assert_eq!(resumed.generation, first.generation);
    drop(lease);
    let store = Store::open(&db, "owner").await.unwrap();
    assert_eq!(
        store
            .task("new-after-restore")
            .await
            .unwrap()
            .active_run
            .as_deref(),
        Some(run.token.run_id.as_str())
    );
    assert!(
        store
            .list_memories(later + 2)
            .await
            .unwrap()
            .iter()
            .any(|m| m.body == "OWNED_NEW_POST_RESTORE_MEMORY")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT charged FROM budget WHERE call_id='new-after-restore-call'"
        )
        .fetch_one(&store.application_pool())
        .await
        .unwrap(),
        20_000
    );
    store.close().await;
}

#[tokio::test]
async fn contradictory_original_cost_evidence_refuses_cutover_without_changing_original_files() {
    let (_dir, db, backup, store, _receipt) = fixture().await;
    sqlx::query("UPDATE budget SET reserved=21000 WHERE call_id='held-original-call'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    store.close().await;
    let lease = admission(&db).await;
    let mut rules = db.as_os_str().to_owned();
    rules.push(".memory-restrictions.json");
    let rules = PathBuf::from(rules);
    let original = fs::read(&db).unwrap();
    let restrictions = fs::read(&rules).unwrap();
    assert!(
        lease
            .restore_backup(&backup, "owner", &Versions::fixture(), NOW + 10)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&db).unwrap(), original);
    assert_eq!(fs::read(rules).unwrap(), restrictions);
    drop(lease);
    let store = Store::open(&db, "owner").await.unwrap();
    assert_eq!(store.task("B").await.unwrap().state, State::Succeeded);
    store.close().await;
}

#[tokio::test]
async fn retired_audit_tasks_do_not_consume_the_new_work_queue() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let store = Store::open(&db, "owner").await.unwrap();
    for i in 0..100 {
        create(&store, &format!("old-{i}"), NOW).await;
    }
    let backup = store.maintain(NOW).await.unwrap().backup.unwrap();
    store.close().await;
    let lease = admission(&db).await;
    lease
        .restore_backup(&backup, "owner", &Versions::fixture(), NOW + 1)
        .await
        .unwrap();
    drop(lease);
    let store = Store::open(&db, "owner").await.unwrap();
    let resume = command(
        &store,
        "old-resume",
        json!({"kind":"resume","id":"old-0","expected_revision":"1"}),
        NOW + DAY,
    )
    .await;
    assert!(!matches!(resume, Outcome::Applied { .. }));
    create(&store, "new-work", NOW + DAY).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + DAY + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "new-work");
    assert!(
        store
            .reserve(
                &run.token,
                "new-work-call",
                20_000,
                "fixture-rate",
                NOW + DAY + 1
            )
            .await
            .unwrap()
    );
    store.close().await;
}

#[tokio::test]
async fn original_approved_operation_receipt_is_preserved_without_reusing_approval() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("db");
    let store = Store::open(&db, "owner").await.unwrap();
    create(&store, "approved-task", NOW).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    let proposal = OperationProposal {
        id: "owned-request".into(),
        token: run.token.clone(),
        target: "fixture:approved-effect".into(),
        intent: json!({"delta":1}),
        expires_at: NOW + 60_000,
    };
    let source = store.ingest(Origin::Tool, "owned-planner", "owned-proposal", json!({"commands":[{"kind":"query","ids":["approved-task"]}],"operation_proposal":proposal}), NOW + 2).await.unwrap();
    store.route(&source.event_id).await.unwrap();
    let request = store
        .propose_operation(&source.event_id, proposal, NOW + 2)
        .await
        .unwrap();
    let binding = OperationBinding {
        task_id: "approved-task".into(),
        expected_revision: 1,
        request_id: request.proposal.id.clone(),
        intent_digest: request.intent_digest.clone(),
    };
    assert!(matches!(
        command(
            &store,
            "owned-approve",
            json!({"kind":"approve_operation","binding":binding}),
            NOW + 3
        )
        .await,
        Outcome::Applied { .. }
    ));
    let operation = store
        .prepare_approved_operation(&run.token, &request.proposal.id, NOW + 4)
        .await
        .unwrap();
    let backup = store.maintain(NOW + 5).await.unwrap().backup.unwrap();
    let receipt = Receipt {
        operation_id: operation.id,
        token: run.token.clone(),
        success: true,
        evidence: "OWNED_APPROVED_ORIGINAL_RESULT".into(),
    };
    store.record_receipt(&receipt).await.unwrap();
    store.close().await;
    let lease = admission(&db).await;
    assert!(
        lease
            .restore_backup(&backup, "owner", &Versions::fixture(), NOW + 6)
            .await
            .unwrap()
            .current_records_imported
    );
    drop(lease);
    let store = Store::open(&db, "owner").await.unwrap();
    assert_eq!(
        serde_json::to_value(store.receipts("approved-task").await.unwrap()).unwrap(),
        json!([receipt])
    );
    assert!(
        store
            .prepare_approved_operation(&run.token, &request.proposal.id, NOW + 7)
            .await
            .is_err()
    );
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 7)
            .await
            .unwrap()
            .is_none()
    );
    store.close().await;
}

#[tokio::test]
async fn real_restore_handles_readable_missing_and_corrupt_originals_without_releasing_truth_or_budget()
 {
    for mode in ["readable", "missing", "corrupt"] {
        let (dir, db, backup, store, receipt) = fixture().await;
        assert!(DatabaseMaintenance::try_acquire(&db).is_err());
        store.close().await;
        let lease = admission(&db).await;
        if mode == "missing" {
            fs::remove_file(&db).unwrap();
        }
        if mode == "corrupt" {
            fs::write(&db, b"owned corrupt original").unwrap();
        }
        let restored = lease
            .restore_backup(&backup, "owner", &Versions::fixture(), NOW + 10)
            .await
            .unwrap();
        assert_eq!(restored.current_records_imported, mode == "readable");
        assert!(!restored.already_committed);
        let generation = restored.generation.clone();
        let repeated = lease
            .resume_restore("owner", &Versions::fixture(), NOW + 11)
            .await
            .unwrap();
        assert!(repeated.already_committed);
        assert_eq!(repeated.generation, generation);
        drop(lease);
        let store = Store::open(&db, "owner").await.unwrap();
        let stored_generation: String =
            sqlx::query_scalar("SELECT value FROM settings WHERE key='restore_generation'")
                .fetch_one(&store.application_pool())
                .await
                .unwrap();
        assert_eq!(stored_generation, generation);
        let memories = store.list_memories(NOW + 12).await.unwrap();
        assert_eq!(memories.len(), usize::from(mode == "readable"));
        assert!(memories.iter().all(|m| m.body != DELETED));
        let task = store.task("A").await.unwrap();
        assert_eq!(task.state, State::OutcomeUnknown);
        assert_eq!(task.revision, if mode == "readable" { 2 } else { 1 });
        assert!(
            store
                .claim_next(&Versions::fixture(), NOW + 12)
                .await
                .unwrap()
                .is_none()
        );
        let cost: (String, i64, i64) = sqlx::query_as(
            "SELECT state,reserved,charged FROM budget WHERE call_id='held-original-call'",
        )
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
        assert_eq!(cost, ("unknown".into(), 20_000, 20_000));
        if mode == "readable" {
            assert_eq!(store.task("B").await.unwrap().state, State::Succeeded);
            assert_eq!(
                serde_json::to_value(store.receipts("B").await.unwrap()).unwrap(),
                json!([receipt])
            );
        }
        create(&store, "new-today", NOW + 12).await;
        let run = store
            .claim_next(&Versions::fixture(), NOW + 13)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.token.task_id, "new-today");
        assert!(
            store
                .reserve(
                    &run.token,
                    "refused-today",
                    20_000,
                    "fixture-rate",
                    NOW + 13
                )
                .await
                .is_err()
        );
        assert_eq!(
            store.task("new-today").await.unwrap().reason.as_deref(),
            Some("restore_budget_hold")
        );
        create(&store, "new-tomorrow", NOW + DAY).await;
        let run = store
            .claim_next(&Versions::fixture(), NOW + DAY + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.token.task_id, "new-tomorrow");
        assert!(
            store
                .reserve(
                    &run.token,
                    "new-authorized-call",
                    20_000,
                    "fixture-rate",
                    NOW + DAY + 1
                )
                .await
                .unwrap()
        );
        store.close().await;
        no_plaintext(&dir.path().join("db.restores"));
        let bytes = fs::read(&db).unwrap();
        assert!(
            !bytes
                .windows(DELETED.len())
                .any(|part| part == DELETED.as_bytes())
        );
    }
}

#[tokio::test]
async fn bad_backup_and_wrong_owner_leave_the_original_usable_and_unchanged() {
    let (_dir, db, backup, store, _) = fixture().await;
    store.close().await;
    let lease = admission(&db).await;
    let before = fs::read(&db).unwrap();
    assert!(
        lease
            .restore_backup(&backup, "other-owner", &Versions::fixture(), NOW + 10)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&db).unwrap(), before);
    let mut bytes = fs::read(&backup).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    fs::write(&backup, bytes).unwrap();
    assert!(
        lease
            .restore_backup(&backup, "owner", &Versions::fixture(), NOW + 10)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&db).unwrap(), before);
    drop(lease);
    Store::open(&db, "owner").await.unwrap().close().await;
}
