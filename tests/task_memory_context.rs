use pulse_core::*;
use serde_json::json;

const NOW: i64 = 1_800_000_000_000;
const BODY: &str = "OWNED_PRIVATE_PREFERENCE_BODY";

struct Fixture {
    root: tempfile::TempDir,
    store: Store,
    memory: Memory,
    run: Run,
}

async fn command(store: &Store, commands: serde_json::Value, now: i64) -> Outcome {
    let event = store
        .ingest(
            Origin::Human,
            "fixture",
            &uuid::Uuid::new_v4().to_string(),
            json!({"commands": commands}),
            now,
        )
        .await
        .unwrap();
    store.route_at(&event.event_id, now).await.unwrap()
}

async fn create(store: &Store, id: &str, parent: Option<&str>, priority: i32) {
    assert!(matches!(command(store, json!([{
        "kind":"create", "id":id, "goal":"owned task", "scope":"pulse:read", "priority":priority,
        "requires_device":false, "parent":parent
    }]), NOW).await, Outcome::Applied { .. }));
}

async fn remember(store: &Store, label: &str, body: &str, source_at: i64) -> Memory {
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at,
        command: MemoryCommand::Remember {
            label: label.into(),
            body: body.into(),
        },
    };
    let MemoryOutcome::Stored { id, .. } = store
        .change_memory(Origin::Human, &request, NOW)
        .await
        .unwrap()
    else {
        panic!()
    };
    store
        .list_memories(NOW)
        .await
        .unwrap()
        .into_iter()
        .find(|m| m.id == id)
        .unwrap()
}

fn adoption(task: &str, memories: &[Memory]) -> TaskMemoryRequest {
    TaskMemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: NOW,
        task_id: task.into(),
        task_revision: 1,
        sources: memories
            .iter()
            .map(|m| MemoryRef {
                id: m.id.clone(),
                revision: m.revision,
            })
            .collect(),
    }
}

async fn fixture(source_at: i64) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    let memory = remember(&store, "PRIVATE_PREFERENCE_LABEL", BODY, source_at).await;
    remember(&store, "unrelated", "UNADOPTED_PRIVATE_BODY", NOW).await;
    create(&store, "A", None, 1).await;
    store
        .bind_task_memories(
            Origin::Human,
            &adoption("A", std::slice::from_ref(&memory)),
            NOW,
        )
        .await
        .unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    Fixture {
        root,
        store,
        memory,
        run,
    }
}

async fn read(f: &Fixture, now: i64) -> Result<Option<TaskMemoryContext>> {
    f.store
        .task_memory_context(&f.run.token, TASK_MEMORY_CONTEXT_BYTES, now)
        .await
}

#[tokio::test]
async fn only_adopted_sources_are_returned_with_original_proof_and_no_new_work() {
    let f = fixture(NOW).await;
    let before = serde_json::to_value(f.store.task("A").await.unwrap()).unwrap();
    let context = read(&f, NOW + 1).await.unwrap().unwrap();
    assert_eq!(context.token, f.run.token);
    assert_eq!(context.memories, vec![f.memory.clone()]);
    assert_eq!(context.binding.request.task_id, "A");
    assert_eq!(context.checked_at, NOW + 1);
    assert_eq!(
        context.valid_until,
        f.store.task("A").await.unwrap().deadline
    );
    let serialized = serde_json::to_string(&context).unwrap();
    assert!(serialized.contains(BODY));
    assert!(!serialized.contains("UNADOPTED_PRIVATE_BODY"));
    let diagnostic = format!("{context:?}");
    assert!(!diagnostic.contains(BODY));
    assert!(!diagnostic.contains("PRIVATE_PREFERENCE_LABEL"));
    assert_eq!(
        serde_json::to_value(f.store.task("A").await.unwrap()).unwrap(),
        before
    );
    let counts: (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM budget),(SELECT count(*) FROM operations)")
            .fetch_one(&f.store.application_pool())
            .await
            .unwrap();
    assert_eq!(counts, (0, 0));
}

#[tokio::test]
async fn an_unbound_task_cannot_borrow_another_tasks_memory() {
    let f = fixture(NOW).await;
    create(&f.store, "B", Some("A"), 2).await;
    let other = f
        .store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(other.token.task_id, "B");
    assert!(
        f.store
            .task_memory_context(&other.token, TASK_MEMORY_CONTEXT_BYTES, NOW + 1)
            .await
            .unwrap()
            .is_none()
    );
    let mut forged = f.run.token.clone();
    forged.task_id = "B".into();
    assert!(
        f.store
            .task_memory_context(&forged, TASK_MEMORY_CONTEXT_BYTES, NOW + 1)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn bytes_are_measured_after_serialization_without_partial_or_truncated_sources() {
    let f = fixture(NOW).await;
    let context = read(&f, NOW).await.unwrap().unwrap();
    let size = serde_json::to_vec(&context).unwrap().len();
    assert!(
        f.store
            .task_memory_context(&f.run.token, size, NOW)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        f.store
            .task_memory_context(&f.run.token, size - 1, NOW)
            .await
            .is_err()
    );
    for maximum in [0, TASK_MEMORY_CONTEXT_BYTES + 1, usize::MAX] {
        assert!(
            f.store
                .task_memory_context(&f.run.token, maximum, NOW)
                .await
                .is_err()
        );
    }
    assert!(read(&f, -1).await.is_err());

    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    let mut memories = vec![];
    for i in 0..8 {
        memories.push(remember(&store, &format!("quote-{i}"), &"\"".repeat(1024), NOW).await);
    }
    create(&store, "quoted", None, 0).await;
    store
        .bind_task_memories(Origin::Human, &adoption("quoted", &memories), NOW)
        .await
        .unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.task_memory_context(&run.token, TASK_MEMORY_CONTEXT_BYTES, NOW).await,
        Err(Error::Rejected(reason)) if reason == "task memory context capacity; no partial sources returned")
    );
    assert_eq!(store.list_memories(NOW).await.unwrap().len(), 8);
}

#[tokio::test]
async fn cjk_source_is_preserved_as_utf8_and_counted_in_bytes() {
    let f = fixture(NOW).await;
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: NOW + 1,
        command: MemoryCommand::Correct {
            id: f.memory.id.clone(),
            revision: 1,
            label: "語言".into(),
            body: "請使用繁體中文。".repeat(20),
        },
    };
    f.store
        .change_memory(Origin::Human, &request, NOW + 1)
        .await
        .unwrap();
    command(
        &f.store,
        json!([{"kind":"revise","id":"A","expected_revision":"1","goal":"fresh reviewed task"}]),
        NOW + 2,
    )
    .await;
    let memory = f
        .store
        .list_memories(NOW + 2)
        .await
        .unwrap()
        .into_iter()
        .find(|m| m.id == f.memory.id)
        .unwrap();
    let mut request = adoption("A", std::slice::from_ref(&memory));
    request.task_revision = 2;
    request.source_at = NOW + 2;
    f.store
        .bind_task_memories(Origin::Human, &request, NOW + 2)
        .await
        .unwrap();
    let run = f
        .store
        .claim_next(&Versions::fixture(), NOW + 2)
        .await
        .unwrap()
        .unwrap();
    let context = f
        .store
        .task_memory_context(&run.token, TASK_MEMORY_CONTEXT_BYTES, NOW + 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.memories, vec![memory]);
    let size = serde_json::to_vec(&context).unwrap().len();
    assert!(
        f.store
            .task_memory_context(&run.token, size - 1, NOW + 2)
            .await
            .is_err()
    );
    assert!(
        f.store
            .task_memory_context(&run.token, size, NOW + 2)
            .await
            .unwrap()
            .is_some()
    );
    assert!(read(&f, NOW + 2).await.is_err());
}

#[tokio::test]
async fn original_human_content_and_adoption_proofs_must_both_match() {
    for fault in [
        "body",
        "oversized-body",
        "label",
        "source-time",
        "fingerprint",
        "missing-event",
        "adoption",
    ] {
        let f = fixture(NOW).await;
        let sql = match fault {
            "body" => {
                "UPDATE memories SET body='UNAUTHORIZED_REPLACEMENT' WHERE label='PRIVATE_PREFERENCE_LABEL'"
            }
            "oversized-body" => {
                "UPDATE memories SET body=printf('%2048s','unverified') WHERE label='PRIVATE_PREFERENCE_LABEL'"
            }
            "label" => {
                "UPDATE memories SET label='UNAUTHORIZED_REPLACEMENT' WHERE label='PRIVATE_PREFERENCE_LABEL'"
            }
            "source-time" => {
                "UPDATE memories SET source_at=source_at+1 WHERE label='PRIVATE_PREFERENCE_LABEL'"
            }
            "fingerprint" => {
                "UPDATE memory_events SET fingerprint='unverified' WHERE memory_id=(SELECT id FROM memories WHERE label='PRIVATE_PREFERENCE_LABEL')"
            }
            "missing-event" => {
                "DELETE FROM memory_events WHERE memory_id=(SELECT id FROM memories WHERE label='PRIVATE_PREFERENCE_LABEL')"
            }
            _ => "UPDATE events SET origin='tool' WHERE source='task-memory'",
        };
        sqlx::query(sql)
            .execute(&f.store.application_pool())
            .await
            .unwrap();
        assert!(read(&f, NOW + 1).await.is_err(), "{fault}");
    }
}

#[tokio::test]
async fn expiry_is_committed_even_when_the_context_request_is_refused() {
    let f = fixture(NOW - MEMORY_RETENTION_MS + 10).await;
    assert_eq!(
        read(&f, NOW + 9).await.unwrap().unwrap().valid_until,
        NOW + 10
    );
    assert!(read(&f, NOW + 10).await.is_err());
    assert_eq!(
        f.store.task("A").await.unwrap().reason.as_deref(),
        Some("memory_source_withdrawn")
    );
    assert!(
        !f.store
            .list_memories(NOW + 10)
            .await
            .unwrap()
            .iter()
            .any(|m| m.id == f.memory.id)
    );
    let bytes = std::fs::read(f.root.path().join("db")).unwrap();
    assert!(!bytes.windows(BODY.len()).any(|v| v == BODY.as_bytes()));
}

#[tokio::test]
async fn current_run_scope_state_deadline_and_cancellation_are_required() {
    let f = fixture(NOW).await;
    for field in ["epoch", "revision", "run"] {
        let mut token = f.run.token.clone();
        match field {
            "epoch" => token.epoch += 1,
            "revision" => token.revision += 1,
            _ => token.run_id = "other-run".into(),
        }
        assert!(
            f.store
                .task_memory_context(&token, TASK_MEMORY_CONTEXT_BYTES, NOW)
                .await
                .is_err(),
            "{field}"
        );
    }
    sqlx::query("UPDATE tasks SET data=json_set(data,'$.scope','fixture:write') WHERE id='A'")
        .execute(&f.store.application_pool())
        .await
        .unwrap();
    assert!(read(&f, NOW).await.is_err());
    sqlx::query("UPDATE tasks SET data=json_set(data,'$.scope','pulse:read') WHERE id='A'")
        .execute(&f.store.application_pool())
        .await
        .unwrap();
    let deadline = f.store.task("A").await.unwrap().deadline;
    assert!(read(&f, deadline).await.is_err());
    f.store
        .advance(
            &f.run.token,
            State::Ready,
            json!({"phase":"ready"}),
            "proof",
            None,
            NOW + 1,
        )
        .await
        .unwrap();
    assert!(read(&f, NOW + 2).await.is_err());
    command(
        &f.store,
        json!([{"kind":"cancel","id":"A","expected_revision":"1"}]),
        NOW + 3,
    )
    .await;
    assert!(read(&f, NOW + 4).await.is_err());
}

#[tokio::test]
async fn keeper_busy_refuses_bound_data_without_blocking_an_unbound_task() {
    let f = fixture(NOW).await;
    create(&f.store, "B", None, 2).await;
    let run = f
        .store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "B");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(f.root.path().join("db.memory-lock"))
        .unwrap();
    lock.lock().unwrap();
    assert!(matches!(read(&f, NOW + 1).await, Err(Error::Busy)));
    assert!(
        f.store
            .task_memory_context(&run.token, TASK_MEMORY_CONTEXT_BYTES, NOW + 1)
            .await
            .unwrap()
            .is_none()
    );
    drop(lock);
    assert!(read(&f, NOW + 1).await.unwrap().is_some());
}

#[tokio::test]
async fn failed_sql_withdrawal_and_database_rollback_cannot_return_an_old_body() {
    let f = fixture(NOW).await;
    let old = f.root.path().join("old.db");
    sqlx::query("VACUUM INTO ?")
        .bind(old.to_str().unwrap())
        .execute(&f.store.application_pool())
        .await
        .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&old, std::fs::Permissions::from_mode(0o600)).unwrap();
    let call = format!("model:{}:1:0", f.run.token.run_id);
    f.store
        .reserve(&f.run.token, &call, 20_000, "fixture", NOW + 1)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER refuse_forget BEFORE DELETE ON memories BEGIN SELECT RAISE(ABORT,'owned fault'); END;")
        .execute(&f.store.application_pool()).await.unwrap();
    let request = MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: NOW + 2,
        command: MemoryCommand::Forget {
            id: f.memory.id.clone(),
            revision: 1,
        },
    };
    assert!(
        f.store
            .change_memory(Origin::Human, &request, NOW + 2)
            .await
            .is_err()
    );
    assert!(read(&f, NOW + 3).await.is_err());
    sqlx::query("DROP TRIGGER refuse_forget")
        .execute(&f.store.application_pool())
        .await
        .unwrap();
    assert!(read(&f, NOW + 3).await.is_err());
    assert!(
        f.store
            .validate_model_call(&f.run.token, &call, NOW + 3)
            .await
            .is_err()
    );
    let charge: i64 = sqlx::query_scalar("SELECT charged FROM budget WHERE call_id=?")
        .bind(&call)
        .fetch_one(&f.store.application_pool())
        .await
        .unwrap();
    assert_eq!(charge, 20_000);
    f.store.close().await;
    std::fs::copy(&old, f.root.path().join("db")).unwrap();
    let reopened = Store::open(f.root.path().join("db"), "owner")
        .await
        .unwrap();
    assert!(
        reopened
            .task_memory_context(&f.run.token, TASK_MEMORY_CONTEXT_BYTES, NOW + 4)
            .await
            .is_err()
    );
    assert!(
        !reopened
            .list_memories(NOW + 4)
            .await
            .unwrap()
            .iter()
            .any(|m| m.id == f.memory.id)
    );
    reopened.close().await;
}

#[tokio::test]
async fn inherited_context_rechecks_the_original_parent_adoption() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    let memory = remember(&store, "owned", BODY, NOW).await;
    create(&store, "parent", None, 0).await;
    let parent = store
        .bind_task_memories(
            Origin::Human,
            &adoption("parent", std::slice::from_ref(&memory)),
            NOW,
        )
        .await
        .unwrap();
    create(&store, "child", Some("parent"), 10).await;
    store
        .inherit_task_memories(
            Origin::Human,
            &adoption("child", std::slice::from_ref(&memory)),
            1,
            NOW,
        )
        .await
        .unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "child");
    let context = store
        .task_memory_context(&run.token, TASK_MEMORY_CONTEXT_BYTES, NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.memories, vec![memory]);
    assert_eq!(context.binding.request.task_id, "child");
    sqlx::query("UPDATE events SET digest='unverified' WHERE id=?")
        .bind(parent.authorization_event)
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(
        store
            .task_memory_context(&run.token, TASK_MEMORY_CONTEXT_BYTES, NOW + 1)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn original_legacy_source_spelling_is_verified_without_changing_canonical_identity() {
    use sha2::{Digest, Sha256};
    let f = fixture(NOW).await;
    // A historical source may predate UUID normalization. The original ledger
    // fingerprint must use that spelling; lineage still uses its canonical UUID.
    let spelling = f.memory.source_id.to_uppercase();
    let original = MemoryRequest {
        source_id: spelling.clone(),
        source_at: NOW,
        command: MemoryCommand::Remember {
            label: f.memory.label.clone(),
            body: f.memory.body.clone(),
        },
    };
    let fingerprint = format!(
        "{:x}",
        Sha256::digest(serde_jcs::to_vec(&original).unwrap())
    );
    let mut tx = f.store.application_pool().begin().await.unwrap();
    sqlx::query("UPDATE memories SET source_id=? WHERE id=?")
        .bind(&spelling)
        .bind(&f.memory.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE memory_events SET source_id=?,fingerprint=? WHERE memory_id=?")
        .bind(&spelling)
        .bind(fingerprint)
        .bind(&f.memory.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        read(&f, NOW + 1).await.unwrap().unwrap().memories,
        vec![f.memory]
    );
}
