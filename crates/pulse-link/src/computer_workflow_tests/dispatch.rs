use super::*;

#[tokio::test]
async fn native_approved_dispatch_reconciles_keeper_before_consuming_human_approval() {
    let mut f = Fixture::setup_with_memory("action-good", true).await;
    f.hub
        .core
        .reserve(
            &f.run.token,
            "before-action",
            20_000,
            "fixture",
            crate::now(),
        )
        .await
        .unwrap();
    let source = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    let request = f.propose().await;
    f.decide(&request, true).await;
    sqlx::query("CREATE TRIGGER block_action_redaction BEFORE DELETE ON memories BEGIN SELECT RAISE(ABORT,'controlled erasure failure'); END")
        .execute(&f.hub.db).await.unwrap();
    let now = crate::now();
    assert!(
        f.hub
            .core
            .change_memory(
                Origin::Human,
                &pulse_core::MemoryRequest {
                    source_id: uuid::Uuid::new_v4().to_string(),
                    source_at: now,
                    command: pulse_core::MemoryCommand::Forget {
                        id: source.id,
                        revision: source.revision
                    },
                },
                now
            )
            .await
            .is_err()
    );
    assert_eq!(f.hub.core.task("task").await.unwrap().state, State::Ready);
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .is_err()
    );
    let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM operations")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(operations, 0);
    let state: String = sqlx::query_scalar(
        "SELECT json_extract(data,'$.state') FROM operation_requests WHERE id=?",
    )
    .bind(&request.proposal.id)
    .fetch_one(&f.hub.db)
    .await
    .unwrap();
    assert_eq!(state, "approved");
    assert_eq!(f.effects(), 0);
    sqlx::query("DROP TRIGGER block_action_redaction")
        .execute(&f.hub.db)
        .await
        .unwrap();
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    let task = f.hub.core.task("task").await.unwrap();
    assert_eq!(task.state, State::Blocked);
    assert!(task.active_run.is_none());
    assert_eq!(task.tool_calls, 1); // Only the original inspection ran.
    assert_eq!(task.model_calls, 1);
    let state: String = sqlx::query_scalar(
        "SELECT json_extract(data,'$.state') FROM operation_requests WHERE id=?",
    )
    .bind(&request.proposal.id)
    .fetch_one(&f.hub.db)
    .await
    .unwrap();
    assert_eq!(state, "invalidated");
    let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM operations")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(operations, 0);
    assert_eq!(f.effects(), 0);
    let charge: i64 =
        sqlx::query_scalar("SELECT charged FROM budget WHERE call_id='before-action'")
            .fetch_one(&f.hub.db)
            .await
            .unwrap();
    assert_eq!(charge, 20_000);
}

#[tokio::test]
async fn native_bound_approval_and_enqueue_rollback_together_then_dispatch_once() {
    let mut f = Fixture::setup_with_memory("action-good", true).await;
    let request = f.propose().await;
    f.decide(&request, true).await;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(f._root.path().join("server.memory-lock"))
        .unwrap();
    lock.lock().unwrap();
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    lock.unlock().unwrap();
    sqlx::query("CREATE TRIGGER block_bound_delivery BEFORE INSERT ON link_outbox BEGIN SELECT RAISE(ABORT,'controlled delivery failure'); END")
        .execute(&f.hub.db).await.unwrap();
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .is_err()
    );
    assert_eq!(f.hub.core.task("task").await.unwrap().state, State::Ready);
    assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 1);
    let state: String = sqlx::query_scalar(
        "SELECT json_extract(data,'$.state') FROM operation_requests WHERE id=?",
    )
    .bind(&request.proposal.id)
    .fetch_one(&f.hub.db)
    .await
    .unwrap();
    assert_eq!(state, "approved");
    let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM operations")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(operations, 0);
    assert_eq!(f.effects(), 0);
    sqlx::query("DROP TRIGGER block_bound_delivery")
        .execute(&f.hub.db)
        .await
        .unwrap();
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    let work = f.execute().await;
    deliver(&f.hub, &f.journal, true).await;
    assert_eq!(f.effects(), 1);
    assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 2);
    assert!(f.journal.take_work().await.unwrap().is_none());
    let ServerPayload::Read { operation, .. } = &work.payload else {
        panic!()
    };
    assert!(operation.is_effect());
}

#[tokio::test]
async fn an_earlier_tick_cannot_extend_an_approved_action_deadline() {
    let mut f = Fixture::setup("action-good").await;
    let request = f.propose().await;
    f.decide(&request, true).await;
    let tick = crate::now();
    let mut task = f.hub.core.task("task").await.unwrap();
    task.deadline = tick + 100;
    sqlx::query("UPDATE tasks SET data=? WHERE id='task'")
        .bind(serde_json::to_string(&task).unwrap())
        .execute(&f.hub.db)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(crate::now() >= task.deadline);
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), tick)
            .await
            .unwrap()
    );
    let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM operations")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(operations, 0);
    assert_eq!(f.effects(), 0);
    assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 1);
}
