use super::*;

async fn child(store: &Store, id: &str, parent: &str) {
    assert!(matches!(
        route(
            store,
            vec![Command::Create {
                id: id.into(),
                goal: "Child analysis".into(),
                scope: "pulse:read".into(),
                priority: 100,
                requires_device: false,
                parent: Some(parent.into()),
                dependencies: vec![],
            }],
            NOW
        )
        .await,
        Outcome::Applied { .. }
    ));
}
async fn inherited(store: &Store, memory: &Memory, id: &str) -> TaskMemorySources {
    store
        .inherit_task_memories(Origin::Human, &adoption(memory, id, 1, NOW), 1, NOW)
        .await
        .unwrap()
}

#[tokio::test]
async fn explicit_inheritance_freezes_bodyless_parent_proof_and_survives_reopen() {
    let (root, store, memory) = setup().await;
    let parent = bind(&store, &memory).await;
    child(&store, "b", "a").await;
    let before = store.task("b").await.unwrap();
    let req = adoption(&memory, "b", 1, NOW);
    let bound = store
        .inherit_task_memories(Origin::Human, &req, 1, NOW)
        .await
        .unwrap();
    assert_eq!(bound.sources, parent.sources);
    let after = store.task("b").await.unwrap();
    assert_eq!(
        (
            after.scope,
            after.deadline,
            after.model_calls,
            after.tool_calls
        ),
        (before.scope, before.deadline, 0, 0)
    );
    let payload: String = sqlx::query_scalar("SELECT payload FROM events WHERE id=?")
        .bind(&bound.authorization_event)
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    let proof: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        proof["task_memory_parent"]["authorization_event"],
        parent.authorization_event
    );
    assert!(!payload.contains(BODY) && !payload.contains("偏好"));
    assert!(
        store
            .bind_task_memories(Origin::Human, &req, NOW)
            .await
            .is_err(),
        "same identity cannot drop inheritance"
    );
    assert!(
        store
            .inherit_task_memories(Origin::Human, &req, 2, NOW)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .inherit_task_memories(Origin::Human, &req, 1, NOW)
            .await
            .unwrap(),
        bound
    );
    // Parent revisions do not silently replace the child's frozen adoption.
    assert!(matches!(
        route(
            &store,
            vec![Command::Revise {
                id: "a".into(),
                expected_revision: 1,
                goal: "New parent task".into(),
                scope: None
            }],
            NOW + 1
        )
        .await,
        Outcome::Applied { .. }
    ));
    assert!(
        store
            .lease_task_memory("b", NOW + 1)
            .await
            .unwrap()
            .is_some()
    );
    store.close().await;
    let store = Store::open(root.path().join("db"), "owner").await.unwrap();
    assert_eq!(
        store.task_memory_sources("b", 1).await.unwrap().unwrap(),
        bound
    );
    assert!(
        store
            .lease_task_memory("b", NOW + 2)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn inheritance_requires_human_actual_parent_current_revision_and_unstarted_child() {
    let (_root, store, memory) = setup().await;
    bind(&store, &memory).await;
    child(&store, "b", "a").await;
    let req = adoption(&memory, "b", 1, NOW);
    for origin in [Origin::Tool, Origin::External] {
        assert!(
            store
                .inherit_task_memories(origin, &req, 1, NOW)
                .await
                .is_err()
        );
    }
    assert!(
        store
            .inherit_task_memories(Origin::Human, &req, 2, NOW)
            .await
            .is_err()
    );
    assert!(
        store
            .inherit_task_memories(Origin::Human, &adoption(&memory, "a", 1, NOW), 1, NOW)
            .await
            .is_err()
    );
    create(&store, "unbound", NOW).await;
    child(&store, "c", "unbound").await;
    assert!(
        store
            .inherit_task_memories(Origin::Human, &adoption(&memory, "c", 1, NOW), 1, NOW)
            .await
            .is_err()
    );
    let mut changed = req.clone();
    changed.sources[0].revision += 1;
    assert!(
        store
            .inherit_task_memories(Origin::Human, &changed, 1, NOW)
            .await
            .is_err()
    );
    assert!(store.task_memory_sources("b", 1).await.unwrap().is_none());
    let run = store
        .claim_next(&Versions::fixture(), NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "b");
    assert!(
        store
            .inherit_task_memories(Origin::Human, &req, 1, NOW)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn forgetting_correcting_expiring_and_old_snapshot_restore_fence_all_adopted_descendants() {
    for mode in ["forget", "correct", "expiry"] {
        let (root, store, memory) = setup_at(if mode == "expiry" {
            NOW + 10 - MEMORY_RETENTION_MS
        } else {
            NOW
        })
        .await;
        bind(&store, &memory).await;
        child(&store, "b", "a").await;
        inherited(&store, &memory, "b").await;
        child(&store, "c", "b").await;
        let leaf = inherited(&store, &memory, "c").await;
        let first = store
            .claim_next(&Versions::fixture(), NOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.token.task_id, "b");
        store
            .advance(
                &first.token,
                State::Blocked,
                json!({}),
                "fixture pause",
                None,
                NOW,
            )
            .await
            .unwrap();
        let run = store
            .claim_next(&Versions::fixture(), NOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.token.task_id, "c");
        store
            .reserve(&run.token, "child-call", 20_000, "fixture", NOW)
            .await
            .unwrap();
        store.cost_unknown("child-call").await.unwrap();
        let snapshot = root.path().join("snapshot");
        sqlx::query("VACUUM INTO ?")
            .bind(snapshot.to_str().unwrap())
            .execute(&store.application_pool())
            .await
            .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o600)).unwrap();
        let at = if mode == "expiry" { NOW + 11 } else { NOW + 1 };
        match mode {
            "forget" => {
                forget(&store, &memory, at).await.unwrap();
            }
            "correct" => {
                store
                    .change_memory(
                        Origin::Human,
                        &request(
                            MemoryCommand::Correct {
                                id: memory.id.clone(),
                                revision: memory.revision,
                                label: "偏好".into(),
                                body: "replacement".into(),
                            },
                            at,
                        ),
                        at,
                    )
                    .await
                    .unwrap();
            }
            _ => {
                assert!(store.list_memories(at).await.unwrap().is_empty());
            }
        }
        for id in ["a", "b", "c"] {
            assert!(
                store
                    .task_memory_sources(id, 1)
                    .await
                    .unwrap()
                    .unwrap()
                    .invalidated
            );
            assert!(store.lease_task_memory(id, at).await.is_err());
            assert_eq!(
                store.task(id).await.unwrap().reason.as_deref(),
                Some("memory_source_withdrawn")
            );
        }
        assert!(
            store
                .inherit_task_memories(Origin::Human, &leaf.request, 1, at)
                .await
                .unwrap()
                .invalidated
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM budget WHERE state='unknown' AND reserved=20000"
            )
            .await,
            1
        );
        assert!(
            store
                .reserve(&run.token, "no-new-call", 20_000, "fixture", at)
                .await
                .is_err()
        );
        store.close().await;
        std::fs::copy(snapshot, root.path().join("db")).unwrap();
        let store = Store::open(root.path().join("db"), "owner").await.unwrap();
        store.recover(&Versions::fixture(), at + 1).await.unwrap();
        for id in ["a", "b", "c"] {
            assert!(
                store
                    .task_memory_sources(id, 1)
                    .await
                    .unwrap()
                    .unwrap()
                    .invalidated
            );
        }
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM budget WHERE state='unknown' AND reserved=20000"
            )
            .await,
            1
        );
        assert!(
            store
                .claim_next(&Versions::fixture(), at + 1)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn parent_proof_loss_and_tampering_deny_child_even_when_memory_rows_are_live() {
    for mode in ["missing", "auth", "cycle"] {
        let (_root, store, memory) = setup().await;
        let parent = bind(&store, &memory).await;
        child(&store, "b", "a").await;
        inherited(&store, &memory, "b").await;
        child(&store, "c", "b").await;
        let leaf = inherited(&store, &memory, "c").await;
        assert!(store.lease_task_memory("c", NOW).await.unwrap().is_some());
        if mode == "missing" {
            sqlx::query("DELETE FROM task_memory_sources WHERE task_id='b'")
                .execute(&store.application_pool())
                .await
                .unwrap();
        } else if mode == "auth" {
            sqlx::query("UPDATE events SET payload=json_set(payload,'$.task_memory_binding.task_id','forged') WHERE id=?")
                .bind(parent.authorization_event).execute(&store.application_pool()).await.unwrap();
        } else {
            // Valid checksums cannot make a cyclic proof usable.
            use sha2::{Digest, Sha256};
            let data: String = sqlx::query_scalar("SELECT payload FROM events WHERE id=?")
                .bind(&leaf.authorization_event)
                .fetch_one(&store.application_pool())
                .await
                .unwrap();
            let mut payload: Value = serde_json::from_str(&data).unwrap();
            payload["task_memory_parent"] = json!({"task_id":"c","revision":"1","authorization_event":leaf.authorization_event});
            let digest = format!(
                "{:x}",
                Sha256::digest(
                    serde_jcs::to_vec(&json!({"origin":"human","payload":payload})).unwrap()
                )
            );
            sqlx::query("UPDATE events SET payload=?,digest=? WHERE id=?")
                .bind(payload.to_string())
                .bind(digest)
                .bind(&leaf.authorization_event)
                .execute(&store.application_pool())
                .await
                .unwrap();
            sqlx::query("UPDATE tasks SET data=json_set(data,'$.parent','c') WHERE id='c'")
                .execute(&store.application_pool())
                .await
                .unwrap();
        }
        assert_eq!(store.list_memories(NOW).await.unwrap().len(), 1);
        assert!(store.lease_task_memory("c", NOW).await.is_err());
        let mut tx = store.application_pool().begin().await.unwrap();
        assert!(
            Store::authorize_device_read(&mut tx, "c", 1, "forbidden", NOW)
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        assert_eq!(store.task("c").await.unwrap().tool_calls, 0);
    }
}

#[tokio::test]
async fn inherited_history_is_bounded_and_full_notification_capacity_rolls_back_adoption() {
    let (_root, store, memory) = setup().await;
    bind(&store, &memory).await;
    let mut parent = "a".to_string();
    for index in 1..32 {
        let id = format!("child-{index}");
        child(&store, &id, &parent).await;
        inherited(&store, &memory, &id).await;
        parent = id;
    }
    assert!(
        store
            .lease_task_memory(&parent, NOW)
            .await
            .unwrap()
            .is_some()
    );
    child(&store, "too-deep", &parent).await;
    let events = count(&store, "SELECT count(*) FROM events").await;
    assert!(
        store
            .inherit_task_memories(
                Origin::Human,
                &adoption(&memory, "too-deep", 1, NOW),
                1,
                NOW
            )
            .await
            .is_err()
    );
    assert_eq!(count(&store, "SELECT count(*) FROM events").await, events);
    assert!(
        store
            .task_memory_sources("too-deep", 1)
            .await
            .unwrap()
            .is_none()
    );

    child(&store, "full", "a").await;
    sqlx::query("UPDATE outbox SET acknowledged=1")
        .execute(&store.application_pool())
        .await
        .unwrap();
    sqlx::query("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<1000) INSERT INTO outbox(dedupe_key,payload) SELECT 'inherit-fill-'||n,'{}' FROM seq").execute(&store.application_pool()).await.unwrap();
    let events = count(&store, "SELECT count(*) FROM events").await;
    assert!(
        store
            .inherit_task_memories(Origin::Human, &adoption(&memory, "full", 1, NOW), 1, NOW)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "SELECT count(*) FROM events").await, events);
    assert!(
        store
            .task_memory_sources("full", 1)
            .await
            .unwrap()
            .is_none()
    );
    forget(&store, &memory, NOW + 1).await.unwrap();
    assert!(
        store
            .task_memory_sources(&parent, 1)
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
}
