use pulse_core::{Origin, Run, State, Store};
use pulse_link::{
    protocol::*,
    provider::{Reply, ToolCall},
    remote,
    server::Hub,
};
use serde_json::{Value, json};

struct Fixture {
    _dir: tempfile::TempDir,
    hub: Hub,
    chat: Credential,
    worker: Credential,
    epoch: u64,
    run: Run,
    read: String,
}
async fn say(hub: &Hub, chat: &Credential, source: &str, text: &str) {
    let cursor: i64 = sqlx::query_scalar("SELECT in_cursor FROM link_device WHERE id=?")
        .bind(&chat.device)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    let message = Envelope::new(
        &chat.device,
        cursor as u64 + 1,
        0,
        DevicePayload::Human {
            source_id: source.into(),
            action: HumanAction::Submit {
                id: format!("task-{source}"),
                goal: text.into(),
            },
        },
    )
    .unwrap();
    hub.receive(&chat.device, &message, pulse_link::now())
        .await
        .unwrap();
}
async fn pair(hub: &Hub, role: ClientRole) -> Credential {
    let code = hub.pairing(role, pulse_link::now()).await.unwrap();
    hub.redeem(
        PairRequest {
            code,
            role,
            device: uuid::Uuid::new_v4().to_string(),
        },
        pulse_link::now(),
    )
    .await
    .unwrap()
}
async fn setup(unsettled: bool) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    let chat = pair(&hub, ClientRole::Chat).await;
    let worker = pair(&hub, ClientRole::LocalExecutor).await;
    let epoch = hub
        .begin(&worker.device, 0, pulse_link::now())
        .await
        .unwrap();
    say(&hub, &chat, "a", "任務 A：OLD_REQUIREMENT").await;
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    hub.core
        .reserve(
            &run.token,
            "fixture-call",
            20000,
            "fixture",
            pulse_link::now(),
        )
        .await
        .unwrap();
    if unsettled {
        hub.core.cost_unknown("fixture-call").await.unwrap();
    } else {
        hub.core
            .settle("fixture-call", 1, "fixture usage")
            .await
            .unwrap();
    }
    let reply = Reply {
        text: String::new(),
        tool: Some(ToolCall {
            call_id: "call_fixture".into(),
            operation: ReadOp::Text {
                path: "README.md".into(),
            },
        }),
        usage: json!({}),
        cost: 1,
    };
    hub.model_proposal(&run.token, &reply, pulse_link::now())
        .await
        .unwrap();
    hub.dispatch_model_read(&remote::versions(), pulse_link::now())
        .await
        .unwrap();
    let read: String = sqlx::query_scalar("SELECT id FROM link_reads")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    hub.end(&worker.device, epoch).await.unwrap();
    assert_eq!(
        hub.core.task("task-a").await.unwrap().state,
        State::OutcomeUnknown
    );
    Fixture {
        _dir: dir,
        hub,
        chat,
        worker,
        epoch,
        run,
        read,
    }
}
async fn late(f: &Fixture, outcome: ReadOutcome) {
    let message = Envelope::new(
        &f.worker.device,
        1,
        f.epoch,
        DevicePayload::Result {
            operation_id: f.read.clone(),
            task_id: "task-a".into(),
            revision: "1".into(),
            outcome,
        },
    )
    .unwrap();
    f.hub
        .receive(&f.worker.device, &message, pulse_link::now())
        .await
        .unwrap();
}
async fn counts(hub: &Hub) -> (i64, i64) {
    (
        sqlx::query_scalar("SELECT COUNT(*) FROM budget")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap(),
        sqlx::query_scalar("SELECT COUNT(*) FROM link_reads")
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn late_known_result_requires_human_review_then_restores_the_same_round_with_evidence() {
    let f = setup(false).await;
    say(&f.hub, &f.chat, "b", "任務 B：B_PRIVATE_MARKER").await;
    say(&f.hub, &f.chat, "missing", "核對 A").await;
    assert_eq!(
        f.hub.core.task("task-a").await.unwrap().state,
        State::OutcomeUnknown
    );
    f.hub.recover().await.unwrap();
    f.hub
        .core
        .recover(&remote::versions(), pulse_link::now())
        .await
        .unwrap();
    late(&f, ReadOutcome::text("A_VERIFIED_EVIDENCE".into(), false)).await;
    say(&f.hub, &f.chat, "too-early", "繼續 A").await;
    let a = f.hub.core.task("task-a").await.unwrap();
    assert_eq!(a.state, State::Blocked);
    assert!(a.active_run.is_none());
    say(&f.hub, &f.chat, "review", "核對 A").await;
    let a = f.hub.core.task("task-a").await.unwrap();
    assert_eq!(a.reason.as_deref(), Some("read_receipts_verified"));
    assert_eq!(a.active_run.as_deref(), Some(f.run.token.run_id.as_str()));
    assert_eq!(counts(&f.hub).await, (1, 1));
    say(&f.hub, &f.chat, "again", "核對 A").await;
    say(&f.hub, &f.chat, "go", "繼續 A").await;
    let run = f
        .hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.run_id, f.run.token.run_id);
    assert_ne!(run.token.epoch, f.run.token.epoch);
    assert_eq!(run.checkpoint["round"], 1);
    let (context, history) = f.hub.model_context(&run).await.unwrap();
    assert!(!context.contains("B_PRIVATE_MARKER"));
    assert_eq!(history.len(), 1);
    assert!(
        matches!(&history[0].outcome,ReadOutcome::Completed{text,..} if text=="A_VERIFIED_EVIDENCE")
    );
    assert_eq!(f.hub.core.task("task-b").await.unwrap().revision, 1);
    assert_eq!(counts(&f.hub).await, (1, 1));
    let repeat = Reply {
        text: String::new(),
        tool: Some(ToolCall {
            call_id: "new_call_id".into(),
            operation: ReadOp::Text {
                path: "README.md".into(),
            },
        }),
        usage: json!({}),
        cost: 1,
    };
    assert!(
        f.hub
            .model_proposal(&run.token, &repeat, pulse_link::now())
            .await
            .is_err()
    );
    assert_eq!(counts(&f.hub).await, (1, 1));
    let reply = Reply {
        text: "answer".into(),
        tool: None,
        usage: json!({}),
        cost: 1,
    };
    assert!(
        f.hub
            .model_reply(&f.run.token, &reply, "fixture", pulse_link::now())
            .await
            .is_err()
    );
    f.hub
        .model_reply(&run.token, &reply, "fixture", pulse_link::now())
        .await
        .unwrap();
    assert_eq!(
        f.hub.core.task("task-a").await.unwrap().state,
        State::Blocked
    );
}

#[tokio::test]
async fn unknown_cost_and_unknown_receipt_cannot_be_cleared_by_human_claims() {
    for unknown_cost in [false, true] {
        let f = setup(unknown_cost).await;
        late(
            &f,
            if unknown_cost {
                ReadOutcome::text("known".into(), false)
            } else {
                ReadOutcome::Unknown {
                    reason: "process ended before a receipt".into(),
                }
            },
        )
        .await;
        say(&f.hub, &f.chat, "review", "核對 A").await;
        say(&f.hub, &f.chat, "go", "繼續 A").await;
        let a = f.hub.core.task("task-a").await.unwrap();
        assert_ne!(a.reason.as_deref(), Some("read_receipts_verified"));
        assert!(!matches!(
            a.state,
            State::Queued | State::Planning | State::Succeeded
        ));
        assert_eq!(counts(&f.hub).await, (1, 1));
        if unknown_cost {
            let (state, charged): (String, i64) =
                sqlx::query_as("SELECT state,charged FROM budget")
                    .fetch_one(&f.hub.core.application_pool())
                    .await
                    .unwrap();
            assert_eq!((state, charged), ("unknown".into(), 20000));
        }
    }
}

#[tokio::test]
async fn revised_and_cancelled_tasks_never_recover_an_old_decision() {
    for cancelled in [false, true] {
        let f = setup(false).await;
        say(
            &f.hub,
            &f.chat,
            "change",
            if cancelled {
                "取消 A"
            } else {
                "修改 A：NEW_REQUIREMENT"
            },
        )
        .await;
        late(&f, ReadOutcome::text("OLD_RECEIPT".into(), false)).await;
        say(&f.hub, &f.chat, "review", "核對 A").await;
        let a = f.hub.core.task("task-a").await.unwrap();
        assert_eq!(a.revision, 2);
        if cancelled {
            assert_eq!(a.state, State::Cancelled);
        } else {
            assert_eq!(a.reason.as_deref(), Some("read_receipts_verified"));
            assert!(a.active_run.is_none());
            say(&f.hub, &f.chat, "go", "繼續 A").await;
            let run = f
                .hub
                .core
                .claim_next(&remote::versions(), pulse_link::now())
                .await
                .unwrap()
                .unwrap();
            let (context, history) = f.hub.model_context(&run).await.unwrap();
            assert!(context.contains("NEW_REQUIREMENT"));
            assert!(!context.contains("OLD_REQUIREMENT") && !context.contains("OLD_RECEIPT"));
            assert!(history.is_empty());
        }
        assert_eq!(counts(&f.hub).await, (1, 1));
    }
}

#[tokio::test]
async fn corrupt_evidence_and_tool_origin_cannot_authorize_review() {
    let f = setup(false).await;
    late(&f, ReadOutcome::text("known".into(), false)).await;
    let event = f
        .hub
        .core
        .ingest(
            Origin::Tool,
            "fixture",
            "fake-approval",
            json!({"commands":[{"kind":"reconcile_read","id":"task-a","expected_revision":"1"}]}),
            pulse_link::now(),
        )
        .await
        .unwrap();
    assert!(matches!(
        f.hub.core.route(&event.event_id).await.unwrap(),
        pulse_core::Outcome::Rejected { .. }
    ));
    let encoded: String = sqlx::query_scalar("SELECT result FROM link_reads")
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    let mut result: Value = serde_json::from_str(&encoded).unwrap();
    result["digest"] = json!("forged");
    sqlx::query("UPDATE link_reads SET result=?")
        .bind(result.to_string())
        .execute(&f.hub.core.application_pool())
        .await
        .unwrap();
    say(&f.hub, &f.chat, "review", "核對 A").await;
    assert!(
        f.hub
            .core
            .task("task-a")
            .await
            .unwrap()
            .active_run
            .is_none()
    );
    assert_eq!(
        f.hub.core.task("task-a").await.unwrap().state,
        State::Blocked
    );
    assert_eq!(counts(&f.hub).await, (1, 1));
}

#[tokio::test]
async fn manually_requested_known_read_is_available_without_a_provider_call_identity() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    let chat = pair(&hub, ClientRole::Chat).await;
    let worker = pair(&hub, ClientRole::LocalExecutor).await;
    let epoch = hub
        .begin(&worker.device, 0, pulse_link::now())
        .await
        .unwrap();
    say(&hub, &chat, "a", "任務 A：用現有證據回答").await;
    let read = Envelope::new(
        &chat.device,
        2,
        0,
        DevicePayload::Human {
            source_id: "manual".into(),
            action: HumanAction::Read {
                task_id: "task-a".into(),
                revision: "1".into(),
                operation: ReadOp::Text {
                    path: "README.md".into(),
                },
            },
        },
    )
    .unwrap();
    hub.receive(&chat.device, &read, pulse_link::now())
        .await
        .unwrap();
    let result = Envelope::new(
        &worker.device,
        1,
        epoch,
        DevicePayload::Result {
            operation_id: "read:manual".into(),
            task_id: "task-a".into(),
            revision: "1".into(),
            outcome: ReadOutcome::text("MANUAL_READ_PROOF".into(), false),
        },
    )
    .unwrap();
    hub.receive(&worker.device, &result, pulse_link::now())
        .await
        .unwrap();
    say(&hub, &chat, "review", "核對 A").await;
    say(&hub, &chat, "go", "繼續 A").await;
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    let (prompt, history) = hub.model_context(&run).await.unwrap();
    assert!(history.is_empty());
    let context: Value = serde_json::from_str(&prompt).unwrap();
    assert_eq!(
        context["prior_read_evidence"][0]["operation_id"],
        "read:manual"
    );
    assert!(prompt.contains("MANUAL_READ_PROOF"));
    assert_eq!(counts(&hub).await, (0, 1));
}
