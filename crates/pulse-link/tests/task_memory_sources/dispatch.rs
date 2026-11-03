use super::*;
use pulse_link::provider::ToolCall;

async fn proposed(hub: &Hub) -> Run {
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    hub.core
        .reserve(
            &run.token,
            "model-before-dispatch",
            20_000,
            "fixture",
            pulse_link::now(),
        )
        .await
        .unwrap();
    hub.model_proposal(
        &run.token,
        &Reply {
            text: String::new(),
            tool: Some(ToolCall {
                call_id: "read-fixture".into(),
                operation: ReadOp::Text {
                    path: "README.md".into(),
                },
            }),
            usage: json!({}),
            cost: 0,
        },
        pulse_link::now(),
    )
    .await
    .unwrap();
    run
}
async fn count(hub: &Hub, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap()
}
async fn negative_keeper(hub: &Hub, memory: &Memory) {
    sqlx::query("CREATE TRIGGER block_dispatch_redaction BEFORE DELETE ON memories BEGIN SELECT RAISE(ABORT,'controlled redaction failure'); END")
        .execute(&hub.core.application_pool()).await.unwrap();
    let now = pulse_link::now();
    assert!(
        hub.core
            .change_memory(
                Origin::Human,
                &MemoryRequest {
                    source_id: uuid::Uuid::new_v4().to_string(),
                    source_at: now,
                    command: MemoryCommand::Forget {
                        id: memory.id.clone(),
                        revision: memory.revision
                    },
                },
                now
            )
            .await
            .is_err()
    );
    // The negative file is durable, while positive SQL has rolled back.
    assert_eq!(count(hub, "SELECT count(*) FROM memories").await, 1);
    assert_eq!(
        hub.core.task("task").await.unwrap().state,
        State::WaitingDevice
    );
}

#[tokio::test]
async fn keeper_only_withdrawal_prevents_read_enqueue_and_repair_reconciles_without_a_retry() {
    let (_root, hub, source) = setup().await;
    let run = proposed(&hub).await;
    let (_worker, _) = pair(&hub, ClientRole::LocalExecutor).await;
    negative_keeper(&hub, &source).await;
    assert!(
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .is_err()
    );
    assert_eq!(count(&hub, "SELECT count(*) FROM link_reads").await, 0);
    assert_eq!(count(&hub, "SELECT count(*) FROM link_outbox").await, 0);
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 0);
    sqlx::query("DROP TRIGGER block_dispatch_redaction")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        !hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    let task = hub.core.task("task").await.unwrap();
    assert_eq!(task.state, State::Blocked);
    assert!(task.active_run.is_none());
    assert_eq!(task.model_calls, 1);
    assert_eq!(count(&hub, "SELECT count(*) FROM memories").await, 0);
    assert_eq!(count(&hub, "SELECT count(*) FROM link_reads").await, 0);
    let charge: i64 =
        sqlx::query_scalar("SELECT charged FROM budget WHERE call_id='model-before-dispatch'")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!(charge, 20_000);
    assert!(hub.model_context(&run).await.is_err());
}

#[tokio::test]
async fn invalid_source_task_does_not_starve_an_unrelated_waiting_read() {
    let (_root, hub, _source) = setup().await;
    let a = proposed(&hub).await;
    let event = hub.core.ingest(Origin::Human, "fixture", "other", json!({"commands":[Command::Create {
        id:"other".into(), goal:"Read independently".into(), scope:"pulse:read".into(), priority:0,
        requires_device:false, parent:None, dependencies:vec![],
    }]}), pulse_link::now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let b = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b.token.task_id, "other");
    hub.model_proposal(
        &b.token,
        &Reply {
            text: String::new(),
            tool: Some(ToolCall {
                call_id: "other-read".into(),
                operation: ReadOp::Text {
                    path: "Cargo.toml".into(),
                },
            }),
            usage: json!({}),
            cost: 0,
        },
        pulse_link::now(),
    )
    .await
    .unwrap();
    let binding = hub
        .core
        .task_memory_sources("task", 1)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE events SET digest='tampered' WHERE id=?")
        .bind(binding.authorization_event)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let (_worker, _) = pair(&hub, ClientRole::LocalExecutor).await;
    assert!(
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    let ids: Vec<String> = sqlx::query_scalar("SELECT task_id FROM link_reads")
        .fetch_all(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(ids, ["other"]);
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 0);
    assert_eq!(hub.core.task("other").await.unwrap().tool_calls, 1);
    assert!(hub.model_context(&a).await.is_err());
}

#[tokio::test]
async fn valid_bound_read_rolls_back_on_queue_failure_then_dispatches_exactly_once() {
    let (_root, hub, _source) = setup().await;
    let _run = proposed(&hub).await;
    let (worker, epoch) = pair(&hub, ClientRole::LocalExecutor).await;
    hub.worker_erasure_ready(&worker, epoch, pulse_link::now())
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_dispatch_delivery BEFORE INSERT ON link_outbox BEGIN SELECT RAISE(ABORT,'controlled delivery failure'); END")
        .execute(&hub.core.application_pool()).await.unwrap();
    assert!(
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .is_err()
    );
    assert_eq!(count(&hub, "SELECT count(*) FROM link_reads").await, 0);
    assert_eq!(
        hub.core.task("task").await.unwrap().state,
        State::WaitingDevice
    );
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 0);
    sqlx::query("DROP TRIGGER fail_dispatch_delivery")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    assert!(
        !hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    assert_eq!(count(&hub, "SELECT count(*) FROM link_reads").await, 1);
    let outgoing = hub.outgoing(&worker).await.unwrap();
    assert_eq!(outgoing.len(), 1);
    assert!(
        hub.permitted(&outgoing[0], epoch, pulse_link::now())
            .await
            .unwrap()
    );
    assert!(!serde_json::to_string(&outgoing[0]).unwrap().contains(BODY));
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 1);
    assert_eq!(hub.core.task("task").await.unwrap().model_calls, 1);
}

#[tokio::test]
async fn an_earlier_tick_cannot_extend_a_read_deadline() {
    let (_root, hub, _source) = setup().await;
    let _run = proposed(&hub).await;
    let (_worker, _) = pair(&hub, ClientRole::LocalExecutor).await;
    let tick = pulse_link::now();
    let mut task = hub.core.task("task").await.unwrap();
    task.deadline = tick + 100;
    sqlx::query("UPDATE tasks SET data=? WHERE id='task'")
        .bind(serde_json::to_string(&task).unwrap())
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(pulse_link::now() >= task.deadline);
    assert!(
        !hub.dispatch_model_read(&remote::versions(), tick)
            .await
            .unwrap()
    );
    assert_eq!(count(&hub, "SELECT count(*) FROM link_reads").await, 0);
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 0);
}

#[tokio::test]
async fn busy_keeper_defers_the_read_without_losing_the_proposal() {
    let (root, hub, _source) = setup().await;
    let _run = proposed(&hub).await;
    let (_worker, _) = pair(&hub, ClientRole::LocalExecutor).await;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.path().join("db.memory-lock"))
        .unwrap();
    lock.lock().unwrap();
    assert!(
        !hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    assert_eq!(count(&hub, "SELECT count(*) FROM link_reads").await, 0);
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 0);
    assert_eq!(
        hub.core.task("task").await.unwrap().state,
        State::WaitingDevice
    );
    lock.unlock().unwrap();
    assert!(
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    assert_eq!(count(&hub, "SELECT count(*) FROM link_reads").await, 1);
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 1);
}

#[tokio::test]
async fn expiry_while_waiting_for_source_lock_skips_dispatch_without_stopping_the_driver() {
    let (_root, hub, _source) = setup().await;
    let _run = proposed(&hub).await;
    let (_worker, _) = pair(&hub, ClientRole::LocalExecutor).await;
    let mut task = hub.core.task("task").await.unwrap();
    task.deadline = pulse_link::now() + 100;
    sqlx::query("UPDATE tasks SET data=? WHERE id='task'")
        .bind(serde_json::to_string(&task).unwrap())
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let held = hub
        .core
        .lease_task_memory("task", pulse_link::now())
        .await
        .unwrap();
    let tick = pulse_link::now();
    assert!(held.is_some());
    let versions = remote::versions();
    let (result, ()) = tokio::join!(hub.dispatch_model_read(&versions, tick), async {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        drop(held);
    });
    assert!(pulse_link::now() >= task.deadline);
    assert!(!result.unwrap());
    assert_eq!(count(&hub, "SELECT count(*) FROM link_reads").await, 0);
    assert_eq!(hub.core.task("task").await.unwrap().tool_calls, 0);
    assert_eq!(hub.core.task("task").await.unwrap().model_calls, 1);
}
