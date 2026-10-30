use pulse_core::*;
use serde_json::{Value, json};

const NOW: i64 = 1_800_000_000_000;

#[tokio::test]
async fn dependency_waits_for_verified_goal_not_just_a_tool_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    let event=store.ingest(Origin::Human,"ui","tasks",json!({"commands":[
        {"kind":"create","id":"parent","goal":"parent","scope":"pulse:read","priority":0,"requires_device":false},
        {"kind":"create","id":"child","goal":"child","scope":"pulse:read","priority":20,"requires_device":false,"parent":"parent","dependencies":["parent"]}
    ]}),NOW).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "parent");
    store
        .advance(
            &run.token,
            State::Ready,
            Value::Null,
            "planned",
            None,
            NOW + 2,
        )
        .await
        .unwrap();
    assert!(
        store
            .advance(
                &run.token,
                State::Dispatched,
                Value::Null,
                "no intent",
                None,
                NOW + 3
            )
            .await
            .is_err()
    );
    let op = store
        .prepare_operation(
            &run.token,
            "fixture:read",
            json!({"file":"source.rs"}),
            NOW + 3,
        )
        .await
        .unwrap();
    assert_eq!(op.idempotency_key, op.id);
    assert_eq!(op.intent["file"], "source.rs");
    let receipt = Receipt {
        operation_id: op.id,
        token: op.token,
        success: true,
        evidence: "file read".into(),
    };
    store.record_receipt(&receipt).await.unwrap();
    assert_eq!(store.task("parent").await.unwrap().state, State::Planning);
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 4)
            .await
            .unwrap()
            .is_none()
    );
    store
        .finish(&run.token, true, "verified analysis artifact", NOW + 5)
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_next(&Versions::fixture(), NOW + 6)
            .await
            .unwrap()
            .unwrap()
            .token
            .task_id,
        "child"
    );
}

#[tokio::test]
async fn checkpoint_and_intent_are_bounded_and_invalid_completion_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    let event=store.ingest(Origin::Human,"ui","task",json!({"commands":[
        {"kind":"create","id":"a","goal":"a","scope":"pulse:read","priority":0,"requires_device":false}
    ]}),NOW).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    assert!(store.finish(&run.token, true, "", NOW + 2).await.is_err());
    assert!(
        store
            .advance(
                &run.token,
                State::Ready,
                json!("x".repeat(65536)),
                "",
                None,
                NOW + 2
            )
            .await
            .is_err()
    );
    store
        .advance(
            &run.token,
            State::Ready,
            Value::Null,
            "planned",
            None,
            NOW + 2,
        )
        .await
        .unwrap();
    assert!(
        store
            .prepare_operation(
                &run.token,
                "fixture:read",
                json!("x".repeat(65536)),
                NOW + 3
            )
            .await
            .is_err()
    );
    store
        .prepare_operation(&run.token, "fixture:read", Value::Null, NOW + 3)
        .await
        .unwrap();
    assert!(
        store
            .finish(&run.token, true, "unverified outcome", NOW + 4)
            .await
            .is_err()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn database_rejects_shared_permissions_and_symlinks() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let store = Store::open(&db, "owner").await.unwrap();
    store.close().await;
    assert_eq!(
        std::fs::metadata(&db).unwrap().permissions().mode() & 0o777,
        0o600
    );
    std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Store::open(&db, "owner").await.is_err());
    std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();
    let link = dir.path().join("link");
    symlink(&db, &link).unwrap();
    assert!(Store::open(&link, "owner").await.is_err());
}

#[tokio::test]
async fn failed_tool_observation_does_not_decide_the_task_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path, "owner").await.unwrap();
    let event=store.ingest(Origin::Human,"ui","task",json!({"commands":[
        {"kind":"create","id":"a","goal":"a","scope":"pulse:read","priority":0,"requires_device":false}
    ]}),NOW).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .advance(
            &run.token,
            State::Ready,
            Value::Null,
            "planned",
            None,
            NOW + 2,
        )
        .await
        .unwrap();
    let op = store
        .prepare_operation(&run.token, "fixture:read", Value::Null, NOW + 3)
        .await
        .unwrap();
    store
        .record_receipt(&Receipt {
            operation_id: op.id,
            token: op.token,
            success: false,
            evidence: "file absent".into(),
        })
        .await
        .unwrap();
    assert_eq!(store.task("a").await.unwrap().state, State::Planning);
    assert!(!store.receipts("a").await.unwrap()[0].success);
    store
        .finish(
            &run.token,
            false,
            "goal impossible: required file absent",
            NOW + 4,
        )
        .await
        .unwrap();
    store.close().await;
    let store = Store::open(&path, "owner").await.unwrap();
    assert_eq!(store.task("a").await.unwrap().state, State::Failed);
    assert_eq!(
        store.receipts("a").await.unwrap()[0].evidence,
        "file absent"
    );
}

#[tokio::test]
async fn tool_limit_preserves_progress_and_stops_new_effects() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    let event=store.ingest(Origin::Human,"ui","task",json!({"commands":[
        {"kind":"create","id":"a","goal":"a","scope":"pulse:read","priority":0,"requires_device":false}
    ]}),NOW).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    for step in 0..16 {
        store
            .advance(
                &run.token,
                State::Ready,
                json!({"step":step}),
                "planned",
                None,
                NOW + 2,
            )
            .await
            .unwrap();
        let op = store
            .prepare_operation(&run.token, "fixture:read", json!({"step":step}), NOW + 3)
            .await
            .unwrap();
        store
            .record_receipt(&Receipt {
                operation_id: op.id,
                token: op.token,
                success: true,
                evidence: format!("step {step}"),
            })
            .await
            .unwrap();
    }
    store
        .advance(
            &run.token,
            State::Ready,
            json!({"step":16}),
            "planned",
            None,
            NOW + 4,
        )
        .await
        .unwrap();
    assert!(
        store
            .prepare_operation(&run.token, "fixture:read", Value::Null, NOW + 5)
            .await
            .is_err()
    );
    let task = store.task("a").await.unwrap();
    assert_eq!(task.tool_calls, 16);
    assert_eq!(task.state, State::Blocked);
    assert_eq!(store.receipts("a").await.unwrap().len(), 16);
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 6)
            .await
            .unwrap()
            .is_none()
    );
}
