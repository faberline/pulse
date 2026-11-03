use super::*;

async fn fill_image_store(f: &Fixture) {
    // Retain the real source image. Fill the remaining slots with fixture copies.
    sqlx::query("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<127) INSERT INTO link_reads SELECT 'capacity-'||x,device,epoch,task_id,task_revision,request,state,result FROM link_reads,n WHERE id=?")
        .bind(&f.observation.binding.operation_id).execute(&f.hub.db).await.unwrap();
    sqlx::query("INSERT INTO link_artifacts SELECT r.id,a.metadata,a.png FROM link_reads r,link_artifacts a WHERE r.id LIKE 'capacity-%' AND a.operation_id=?")
        .bind(&f.observation.binding.operation_id).execute(&f.hub.db).await.unwrap();
}

#[tokio::test]
async fn native_image_capacity_preserves_human_approval_then_saves_the_original() {
    let mut f = Fixture::setup("action-good").await;
    let request = f.propose().await;
    f.decide(&request, true).await;
    fill_image_store(&f).await;
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    assert_eq!(
        f.hub
            .core
            .operation_request(&request.proposal.id)
            .await
            .unwrap()
            .state,
        ApprovalState::Approved
    );
    assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 1);
    assert_eq!(f.effects(), 0);
    let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM operations")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(operations, 0);
    // Remove only a known fixture copy. The approved source remains unchanged.
    sqlx::query("DELETE FROM link_artifacts WHERE operation_id='capacity-127'")
        .execute(&f.hub.db)
        .await
        .unwrap();
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    let work = f.execute().await;
    deliver(&f.hub, &f.journal, true).await;
    assert_eq!(f.effects(), 1);
    let ServerPayload::Read { operation_id, .. } = work.payload else {
        panic!()
    };
    let png: Vec<u8> = sqlx::query_scalar("SELECT png FROM link_artifacts WHERE operation_id=?")
        .bind(operation_id)
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    crate::artifact::png_dimensions(&png).unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM link_artifacts")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(count, 128);
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn full_image_store_blocks_model_and_direct_native_inspection_before_authorization() {
    let mut f = Fixture::setup("action-good").await;
    fill_image_store(&f).await;
    let selection = f.observation.selection();
    for id in ["inspect-model", "inspect-manual"] {
        let event = f.hub.core.ingest(Origin::Human,"capacity",id,
            json!({"commands":[{"kind":"create","id":id,"goal":"Inspect fixture","scope":selection.scope().unwrap(),"priority":0,"requires_device":false}]}),crate::now()).await.unwrap();
        f.hub.core.route(&event.event_id).await.unwrap();
    }
    let time = crate::now();
    f.hub
        .computer_interactive_ready(
            &f.local.device,
            f.epoch,
            ["inspect-model", "inspect-manual"]
                .into_iter()
                .map(|id| ComputerPermit {
                    task_id: id.into(),
                    revision: "1".into(),
                    selection: selection.clone(),
                    selected_at: time,
                    expires_at: time + 900_000,
                })
                .collect(),
            time,
        )
        .await
        .unwrap();
    // Claim whichever queued inspection the scheduler selects.
    let run = f
        .hub
        .core
        .claim_next(&remote::versions(), crate::now())
        .await
        .unwrap()
        .unwrap();
    let model_id = run.token.task_id.clone();
    let manual_id = if model_id == "inspect-model" {
        "inspect-manual"
    } else {
        "inspect-model"
    };
    f.hub
        .model_proposal(
            &run.token,
            &reply(
                ReadOp::ComputerInspect {
                    selection: selection.clone(),
                },
                "inspect-capacity",
            ),
            crate::now(),
        )
        .await
        .unwrap();
    assert!(
        !f.hub
            .dispatch_model_read(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    let action = HumanAction::Read {
        task_id: manual_id.into(),
        revision: "1".into(),
        operation: ReadOp::ComputerInspect { selection },
    };
    assert_eq!(f.human(action.clone()).await, "rejected");
    assert_eq!(f.hub.core.task(&model_id).await.unwrap().tool_calls, 0);
    assert_eq!(f.hub.core.task(manual_id).await.unwrap().tool_calls, 0);
    sqlx::query("DELETE FROM link_artifacts WHERE operation_id='capacity-127'")
        .execute(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(f.human(action).await, "accepted");
    assert_eq!(f.hub.core.task(manual_id).await.unwrap().tool_calls, 1);
    assert!(
        !f.hub
            .dispatch_model_read(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    assert_eq!(f.effects(), 0);
}

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
