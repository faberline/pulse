use pulse_core::*;
use sqlx::Executor;
use std::fs;

const NOW: i64 = 1_800_000_000_000;
fn request(command: MemoryCommand) -> MemoryRequest {
    MemoryRequest {
        source_id: uuid::Uuid::new_v4().to_string(),
        source_at: NOW,
        command,
    }
}
async fn setup() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    (dir, store)
}
fn remember() -> MemoryRequest {
    request(MemoryCommand::Remember {
        label: "偏好".into(),
        body: "PRIVATE_REVOKED_SOURCE".into(),
    })
}
async fn snapshot(store: &Store, dir: &std::path::Path) {
    sqlx::query("VACUUM INTO ?")
        .bind(dir.join("snapshot").to_str().unwrap())
        .execute(&store.application_pool())
        .await
        .unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir.join("snapshot"), fs::Permissions::from_mode(0o600)).unwrap();
}

#[tokio::test]
async fn unknown_source_cancel_survives_pre_source_restore_and_late_case_alias_replay() {
    let (dir, store) = setup().await;
    snapshot(&store, dir.path()).await;
    let mut old = remember();
    old.source_id = old.source_id.to_uppercase();
    let cancel = request(MemoryCommand::Revoke {
        sources: vec![old.source_id.clone()],
        expected: None,
    });
    assert_eq!(
        store
            .change_memory(Origin::Human, &cancel, NOW)
            .await
            .unwrap(),
        MemoryOutcome::Revoked {
            current_changed: false
        }
    );
    assert!(matches!(
        store.change_memory(Origin::Human, &old, NOW).await.unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
    store.close().await;
    fs::copy(dir.path().join("snapshot"), dir.path().join("db")).unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    old.source_id = old.source_id.to_lowercase();
    assert!(matches!(
        store.change_memory(Origin::Human, &old, NOW).await.unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
    assert_eq!(
        store
            .change_memory(Origin::Human, &cancel, NOW)
            .await
            .unwrap(),
        MemoryOutcome::Revoked {
            current_changed: false
        }
    );
    assert!(store.list_memories(NOW).await.unwrap().is_empty());
}

#[tokio::test]
async fn ordinary_forget_and_expiry_also_fence_a_backup_before_the_source_existed() {
    for expiry in [false, true] {
        let (dir, store) = setup().await;
        snapshot(&store, dir.path()).await;
        let source = remember();
        store
            .change_memory(Origin::Human, &source, NOW)
            .await
            .unwrap();
        let memory = store.list_memories(NOW).await.unwrap().remove(0);
        if expiry {
            assert!(
                store
                    .list_memories(NOW + MEMORY_RETENTION_MS)
                    .await
                    .unwrap()
                    .is_empty()
            );
        } else {
            store
                .change_memory(
                    Origin::Human,
                    &request(MemoryCommand::Forget {
                        id: memory.id,
                        revision: 1,
                    }),
                    NOW,
                )
                .await
                .unwrap();
        }
        store.close().await;
        fs::copy(dir.path().join("snapshot"), dir.path().join("db")).unwrap();
        let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
        assert!(matches!(
            store
                .change_memory(Origin::Human, &source, NOW)
                .await
                .unwrap(),
            MemoryOutcome::Redacted { .. }
        ));
        assert!(store.list_memories(NOW).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancelling_an_accepted_but_unacknowledged_source_removes_it_without_erasing_newer_human_data()
 {
    let (_dir, store) = setup().await;
    let old = remember();
    store.change_memory(Origin::Human, &old, NOW).await.unwrap();
    let memory = store.list_memories(NOW).await.unwrap().remove(0);
    let newer = request(MemoryCommand::Correct {
        id: memory.id.clone(),
        revision: 1,
        label: "偏好".into(),
        body: "new Human data".into(),
    });
    store
        .change_memory(Origin::Human, &newer, NOW)
        .await
        .unwrap();
    let cancel = request(MemoryCommand::Revoke {
        sources: vec![old.source_id],
        expected: Some(MemoryRef {
            id: memory.id.clone(),
            revision: 1,
        }),
    });
    assert_eq!(
        store
            .change_memory(Origin::Human, &cancel, NOW)
            .await
            .unwrap(),
        MemoryOutcome::Revoked {
            current_changed: true
        }
    );
    assert_eq!(
        store.list_memories(NOW).await.unwrap()[0].body,
        "new Human data"
    );
    let cancel = request(MemoryCommand::Revoke {
        sources: vec![newer.source_id.clone()],
        expected: None,
    });
    store
        .change_memory(Origin::Human, &cancel, NOW)
        .await
        .unwrap();
    assert!(store.list_memories(NOW).await.unwrap().is_empty());
    assert!(matches!(
        store
            .change_memory(Origin::Human, &newer, NOW)
            .await
            .unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
}

#[tokio::test]
async fn unknown_source_id_is_not_authority_to_delete_an_unrelated_memory_with_the_same_id() {
    let (_dir, store) = setup().await;
    let MemoryOutcome::Stored { id, .. } = store
        .change_memory(Origin::Human, &remember(), NOW)
        .await
        .unwrap()
    else {
        panic!("not stored")
    };
    store
        .change_memory(
            Origin::Human,
            &request(MemoryCommand::Revoke {
                sources: vec![id.clone()],
                expected: None,
            }),
            NOW,
        )
        .await
        .unwrap();
    let memories = store.list_memories(NOW).await.unwrap();
    assert_eq!(memories.len(), 1);
    assert_eq!(memories[0].id, id);
}

#[tokio::test]
async fn durable_source_restriction_is_applied_after_sql_rollback_and_operator_repair() {
    let (dir, store) = setup().await;
    let old = remember();
    store.application_pool().execute("CREATE TRIGGER fail_revocation BEFORE INSERT ON memory_events BEGIN SELECT RAISE(ABORT,'fixture failure'); END").await.unwrap();
    let cancel = request(MemoryCommand::Revoke {
        sources: vec![old.source_id.clone()],
        expected: None,
    });
    assert!(
        store
            .change_memory(Origin::Human, &cancel, NOW)
            .await
            .is_err()
    );
    // Repair only the injected fixture fault. The restriction must already exist.
    store
        .application_pool()
        .execute("DROP TRIGGER fail_revocation")
        .await
        .unwrap();
    store.close().await;
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert!(matches!(
        store.change_memory(Origin::Human, &old, NOW).await.unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
    let metadata = fs::read_to_string(dir.path().join("db.memory-restrictions.json")).unwrap();
    assert!(!metadata.contains("PRIVATE_REVOKED_SOURCE"));
    assert!(!metadata.contains("偏好"));
}

#[tokio::test]
async fn schema_seven_uppercase_fingerprints_replay_but_uuid_case_cannot_create_a_second_source() {
    use sha2::{Digest, Sha256};
    let (_dir, store) = setup().await;
    let source = remember();
    store
        .change_memory(Origin::Human, &source, NOW)
        .await
        .unwrap();
    let mut legacy = source.clone();
    legacy.source_id = source.source_id.to_uppercase();
    let fingerprint = format!("{:x}", Sha256::digest(serde_jcs::to_vec(&legacy).unwrap()));
    sqlx::query("UPDATE memory_events SET source_id=?,fingerprint=? WHERE source_id=?")
        .bind(&legacy.source_id)
        .bind(fingerprint)
        .bind(&source.source_id)
        .execute(&store.application_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE memories SET source_id=? WHERE source_id=?")
        .bind(&legacy.source_id)
        .bind(&source.source_id)
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(matches!(
        store
            .change_memory(Origin::Human, &source, NOW)
            .await
            .unwrap(),
        MemoryOutcome::Stored { revision: 1, .. }
    ));
    assert_eq!(store.list_memories(NOW).await.unwrap().len(), 1);
    store
        .change_memory(
            Origin::Human,
            &request(MemoryCommand::Revoke {
                sources: vec![legacy.source_id],
                expected: None,
            }),
            NOW,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .change_memory(Origin::Human, &source, NOW)
            .await
            .unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
}

#[tokio::test]
async fn every_legacy_uuid_spelling_has_one_fingerprint_and_one_revocation_identity() {
    use sha2::{Digest, Sha256};
    for spelling in 0..4 {
        let (_dir, store) = setup().await;
        let source = remember();
        store
            .change_memory(Origin::Human, &source, NOW)
            .await
            .unwrap();
        let id = uuid::Uuid::parse_str(&source.source_id).unwrap();
        let mut legacy = source.clone();
        legacy.source_id = match spelling {
            0 => id.simple().to_string(),
            1 => id.braced().to_string(),
            2 => id.urn().to_string(),
            _ => id.to_string().to_uppercase(),
        };
        let fingerprint = format!("{:x}", Sha256::digest(serde_jcs::to_vec(&legacy).unwrap()));
        sqlx::query("UPDATE memory_events SET source_id=?,fingerprint=? WHERE source_id=?")
            .bind(&legacy.source_id)
            .bind(fingerprint)
            .bind(&source.source_id)
            .execute(&store.application_pool())
            .await
            .unwrap();
        sqlx::query("UPDATE memories SET source_id=? WHERE source_id=?")
            .bind(&legacy.source_id)
            .bind(&source.source_id)
            .execute(&store.application_pool())
            .await
            .unwrap();
        assert!(matches!(
            store
                .change_memory(Origin::Human, &source, NOW)
                .await
                .unwrap(),
            MemoryOutcome::Stored { revision: 1, .. }
        ));
        assert_eq!(
            store.list_memories(NOW).await.unwrap()[0].source_id,
            source.source_id
        );
        store
            .change_memory(
                Origin::Human,
                &request(MemoryCommand::Revoke {
                    sources: vec![legacy.source_id],
                    expected: None,
                }),
                NOW,
            )
            .await
            .unwrap();
        assert!(matches!(
            store
                .change_memory(Origin::Human, &source, NOW)
                .await
                .unwrap(),
            MemoryOutcome::Redacted { .. }
        ));
    }
}

async fn legacy_manifest(dir: &std::path::Path) {
    use sqlx::{
        Connection,
        sqlite::{SqliteConnectOptions, SqliteConnection},
    };
    let mut db =
        SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(dir.join("db")))
            .await
            .unwrap();
    db.execute("DROP TABLE local_memory_access; DROP INDEX memory_event_uuid; DROP INDEX memory_live_uuid; DROP INDEX local_memory_uuid; ALTER TABLE memory_events DROP COLUMN source_uuid; ALTER TABLE memories DROP COLUMN source_uuid; ALTER TABLE local_memory_queue DROP COLUMN source_uuid; DELETE FROM _sqlx_migrations WHERE version IN (8,9,10); PRAGMA user_version=7;").await.unwrap();
    db.close().await.unwrap();
    let path = dir.join("db.memory-restrictions.json");
    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    value["format"] = 1.into();
    value.as_object_mut().unwrap().remove("sources");
    value.as_object_mut().unwrap().remove("cancellations");
    fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
}

#[tokio::test]
async fn schema_seven_upgrade_captures_deleted_sources_before_a_pre_source_restore() {
    let (dir, store) = setup().await;
    snapshot(&store, dir.path()).await;
    let source = remember();
    let MemoryOutcome::Stored { id, revision } = store
        .change_memory(Origin::Human, &source, NOW)
        .await
        .unwrap()
    else {
        panic!("not stored")
    };
    store
        .change_memory(
            Origin::Human,
            &request(MemoryCommand::Forget { id, revision }),
            NOW,
        )
        .await
        .unwrap();
    store.close().await;
    legacy_manifest(dir.path()).await;
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    let schema: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(schema, 10);
    store.close().await;
    fs::copy(dir.path().join("snapshot"), dir.path().join("db")).unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert!(matches!(
        store
            .change_memory(Origin::Human, &source, NOW)
            .await
            .unwrap(),
        MemoryOutcome::Redacted { .. }
    ));
}

#[tokio::test]
async fn legacy_upgrade_refuses_when_the_restored_db_cannot_prove_deleted_source_ids() {
    let (dir, store) = setup().await;
    snapshot(&store, dir.path()).await;
    let source = remember();
    let MemoryOutcome::Stored { id, revision } = store
        .change_memory(Origin::Human, &source, NOW)
        .await
        .unwrap()
    else {
        panic!("not stored")
    };
    store
        .change_memory(
            Origin::Human,
            &request(MemoryCommand::Forget { id, revision }),
            NOW,
        )
        .await
        .unwrap();
    store.close().await;
    fs::copy(dir.path().join("snapshot"), dir.path().join("db")).unwrap();
    legacy_manifest(dir.path()).await;
    let result = Store::open(dir.path().join("db"), "owner").await;
    assert!(
        matches!(result, Err(Error::Rejected(reason)) if reason.contains("legacy deletion sources unavailable"))
    );
}

#[tokio::test]
async fn migration_refuses_ambiguous_legacy_source_aliases_without_dropping_either_event() {
    use sqlx::{
        Connection,
        sqlite::{SqliteConnectOptions, SqliteConnection},
    };
    let (dir, store) = setup().await;
    let source = remember();
    store
        .change_memory(Origin::Human, &source, NOW)
        .await
        .unwrap();
    store.close().await;
    legacy_manifest(dir.path()).await;
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(dir.path().join("db")),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO memory_events(source_id,memory_id,revision,outcome) VALUES(?,?,1,'redacted')",
    )
    .bind(
        uuid::Uuid::parse_str(&source.source_id)
            .unwrap()
            .simple()
            .to_string(),
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .execute(&mut db)
    .await
    .unwrap();
    db.close().await.unwrap();
    assert!(Store::open(dir.path().join("db"), "owner").await.is_err());
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(dir.path().join("db")),
    )
    .await
    .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM memory_events")
        .fetch_one(&mut db)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn maximum_memory_view_with_json_escaped_source_text_fits_the_http_client_bound() {
    let uuid = uuid::Uuid::new_v4().to_string();
    let memory = Memory {
        id: uuid.clone(),
        revision: i64::MAX as u64,
        label: "\"".repeat(128),
        body: "\"".repeat(1024),
        source_id: uuid.clone(),
        source_at: i64::MAX - MEMORY_RETENTION_MS,
        expires_at: i64::MAX,
    };
    let view = MemoryView {
        keeper: uuid.clone(),
        epoch: u64::MAX,
        memories: vec![memory; 100],
        redacted_sources: vec![uuid; 20_100],
    };
    assert!(serde_json::to_vec(&view).unwrap().len() <= 1024 * 1024);
}

#[tokio::test]
async fn exhausted_unknown_revocations_still_allow_deleting_known_human_memory() {
    let (dir, store) = setup().await;
    let source = remember();
    store
        .change_memory(Origin::Human, &source, NOW)
        .await
        .unwrap();
    store.close().await;
    // Fill opaque evidence directly to avoid 10,000 fsyncs in this fixture.
    let path = dir.path().join("db.memory-restrictions.json");
    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let mut sources = serde_json::Map::new();
    for _ in 0..10_000 {
        let id = uuid::Uuid::new_v4().to_string();
        sources.insert(
            id.clone(),
            serde_json::json!({"id":id,"revision":1,"unknown":true}),
        );
    }
    value["sources"] = sources.into();
    value["epoch"] = 10_000.into();
    fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert!(
        store
            .change_memory(
                Origin::Human,
                &request(MemoryCommand::Revoke {
                    sources: vec![uuid::Uuid::new_v4().to_string()],
                    expected: None,
                }),
                NOW
            )
            .await
            .is_err()
    );
    store
        .change_memory(
            Origin::Human,
            &request(MemoryCommand::Revoke {
                sources: vec![source.source_id],
                expected: None,
            }),
            NOW,
        )
        .await
        .unwrap();
    assert!(store.list_memories(NOW).await.unwrap().is_empty());
}

#[test]
#[ignore = "child harness; parent checks durable source cancellation"]
fn source_cancellation_child() {
    let database = std::env::var_os("PULSE_SOURCE_CANCEL_DB").unwrap();
    let source = std::env::var("PULSE_SOURCE_CANCEL_ID").unwrap();
    let abort_sql = std::env::var("PULSE_SOURCE_CANCEL_FAIL_SQL").unwrap() == "true";
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let store = Store::open(std::path::PathBuf::from(database), "owner").await.unwrap();
        if abort_sql {
            store.application_pool().execute("CREATE TRIGGER fail_source BEFORE INSERT ON memory_events BEGIN SELECT RAISE(ABORT,'fixture failure'); END").await.unwrap();
        }
        let result = store.change_memory(Origin::Human, &request(MemoryCommand::Revoke {
            sources: vec![source], expected: None,
        }), NOW).await;
        assert_eq!(result.is_err(), abort_sql);
        if abort_sql {
            store.application_pool().execute("DROP TRIGGER fail_source").await.unwrap();
        }
        // No orderly Store close, destructors, or WAL flush.
        std::process::exit(74);
    });
}

#[tokio::test]
async fn hard_process_exit_after_source_restriction_or_commit_still_blocks_late_sources() {
    for abort_sql in [false, true] {
        let (dir, store) = setup().await;
        snapshot(&store, dir.path()).await;
        store.close().await;
        let source = remember();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "source_cancellation_child", "--ignored"])
            .env("PULSE_SOURCE_CANCEL_DB", dir.path().join("db"))
            .env("PULSE_SOURCE_CANCEL_ID", &source.source_id)
            .env("PULSE_SOURCE_CANCEL_FAIL_SQL", abort_sql.to_string())
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(74),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
        assert!(matches!(
            store
                .change_memory(Origin::Human, &source, NOW)
                .await
                .unwrap(),
            MemoryOutcome::Redacted { .. }
        ));
        store.close().await;
        fs::copy(dir.path().join("snapshot"), dir.path().join("db")).unwrap();
        let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
        assert!(matches!(
            store
                .change_memory(Origin::Human, &source, NOW)
                .await
                .unwrap(),
            MemoryOutcome::Redacted { .. }
        ));
    }
}

#[tokio::test]
async fn tool_sources_cannot_revoke_and_effective_cancellation_nonce_cannot_change_targets() {
    let (_dir, store) = setup().await;
    let cancel = request(MemoryCommand::Revoke {
        sources: vec![uuid::Uuid::new_v4().to_string()],
        expected: None,
    });
    assert!(
        store
            .change_memory(Origin::Tool, &cancel, NOW)
            .await
            .is_err()
    );
    store
        .change_memory(Origin::Human, &cancel, NOW)
        .await
        .unwrap();
    let mut changed = cancel.clone();
    changed.command = MemoryCommand::Revoke {
        sources: vec![uuid::Uuid::new_v4().to_string()],
        expected: None,
    };
    assert!(
        store
            .change_memory(Origin::Human, &changed, NOW)
            .await
            .is_err()
    );
    let forged = MemoryRequest {
        source_id: cancel.source_id,
        source_at: NOW,
        command: remember().command,
    };
    assert!(
        store
            .change_memory(Origin::Human, &forged, NOW)
            .await
            .is_err()
    );
}
