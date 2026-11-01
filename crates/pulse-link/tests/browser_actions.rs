use base64::{Engine, engine::general_purpose::STANDARD};
use pulse_core::{
    BrowserEffect, BrowserElement, BrowserElementKind, BrowserObservation, Origin, State, Store,
};
use pulse_link::{
    artifact::ArtifactRef,
    protocol::*,
    provider::{Reply, ToolCall},
    remote,
    server::Hub,
};
use serde_json::json;
use sqlx::Row;

struct Fixture {
    _dir: tempfile::TempDir,
    hub: Hub,
    local: Credential,
    local_epoch: u64,
    chat: Credential,
    chat_epoch: u64,
    run: pulse_core::Run,
    observation: BrowserObservation,
    sequence: u64,
}
async fn pair(hub: &Hub, role: ClientRole) -> (Credential, u64) {
    let code = hub.pairing(role, pulse_link::now()).await.unwrap();
    let device = uuid::Uuid::new_v4().to_string();
    let credential = hub
        .redeem(
            PairRequest {
                code,
                device: device.clone(),
                role,
            },
            pulse_link::now(),
        )
        .await
        .unwrap();
    let epoch = hub.begin(&device, 0, pulse_link::now()).await.unwrap();
    (credential, epoch)
}
fn reply(operation: ReadOp, id: &str) -> Reply {
    Reply {
        text: String::new(),
        tool: Some(ToolCall {
            call_id: id.into(),
            operation,
        }),
        usage: json!({}),
        cost: 1,
    }
}
async fn setup() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
    let (local, local_epoch) = pair(&hub, ClientRole::LocalExecutor).await;
    let (chat, chat_epoch) = pair(&hub, ClientRole::Chat).await;
    let event=hub.core.ingest(Origin::Human,"test","create",json!({"commands":[{"kind":"create","id":"task","goal":"Click once after approval","scope":"browser-interact:https://example.com/","priority":0,"requires_device":false}]}),pulse_link::now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    hub.model_proposal(
        &run.token,
        &reply(
            ReadOp::BrowserInspect {
                url: "https://example.com/".into(),
            },
            "observe",
        ),
        pulse_link::now(),
    )
    .await
    .unwrap();
    hub.browser_ready(&local.device, local_epoch, None, pulse_link::now())
        .await
        .unwrap();
    assert!(
        !hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap(),
        "read-only local must not receive interaction work"
    );
    hub.browser_interactive_ready(&local.device, local_epoch, None, pulse_link::now())
        .await
        .unwrap();
    assert!(
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    let png=STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=").unwrap();
    let now = pulse_link::now();
    let observation = BrowserObservation {
        session_id: uuid::Uuid::new_v4().to_string(),
        nonce: uuid::Uuid::new_v4().to_string(),
        url: "https://example.com/".into(),
        title: "Mock page".into(),
        png_digest: hash_bytes(&png),
        elements: vec![
            BrowserElement {
                id: "button-1".into(),
                label: "Count".into(),
                kind: BrowserElementKind::Button { form: None },
            },
            BrowserElement {
                id: "text-1".into(),
                label: "Message".into(),
                kind: BrowserElementKind::Text {
                    value: String::new(),
                },
            },
        ],
        observed_at: now,
        expires_at: now + 120_000,
    };
    let id: String = sqlx::query_scalar("SELECT id FROM link_reads")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    let artifact = ArtifactRef {
        task_id: "task".into(),
        revision: "1".into(),
        digest: hash_bytes(&png),
        length: png.len() as u32,
        url: observation.url.clone(),
        title: observation.title.clone(),
        observed_at: now,
        width: 1,
        height: 1,
    };
    let chunk = Envelope::new(
        &local.device,
        1,
        local_epoch,
        DevicePayload::ArtifactChunk {
            operation_id: id.clone(),
            artifact,
            index: 0,
            data: STANDARD.encode(png),
        },
    )
    .unwrap();
    hub.receive(&local.device, &chunk, pulse_link::now())
        .await
        .unwrap();
    let mut outcome = ReadOutcome::text("Mock observation".into(), false);
    if let ReadOutcome::Completed {
        observation: field, ..
    } = &mut outcome
    {
        *field = Some(observation.clone());
    }
    let result = Envelope::new(
        &local.device,
        2,
        local_epoch,
        DevicePayload::Result {
            operation_id: id,
            task_id: "task".into(),
            revision: "1".into(),
            outcome,
        },
    )
    .unwrap();
    hub.receive(&local.device, &result, pulse_link::now())
        .await
        .unwrap();
    let next = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.token.run_id, run.token.run_id);
    Fixture {
        _dir: dir,
        hub,
        local,
        local_epoch,
        chat,
        chat_epoch,
        run: next,
        observation,
        sequence: 2,
    }
}
impl Fixture {
    fn action(&self) -> ReadOp {
        ReadOp::BrowserAction {
            session_id: self.observation.session_id.clone(),
            nonce: self.observation.nonce.clone(),
            element_id: "button-1".into(),
            effect: BrowserEffect::Click,
            grant: None,
        }
    }
    async fn propose(&self) -> pulse_core::OperationRequest {
        self.hub
            .model_proposal(
                &self.run.token,
                &reply(self.action(), "click"),
                pulse_link::now(),
            )
            .await
            .unwrap();
        let data: String = sqlx::query_scalar("SELECT data FROM operation_requests")
            .fetch_one(&self.hub.core.application_pool())
            .await
            .unwrap();
        serde_json::from_str(&data).unwrap()
    }
    async fn decide(&self, request: &pulse_core::OperationRequest, approve: bool) {
        let frame = Envelope::new(
            &self.chat.device,
            1,
            self.chat_epoch,
            DevicePayload::Human {
                source_id: format!("decide-{approve}"),
                action: HumanAction::DecideOperation {
                    binding: pulse_core::OperationBinding {
                        task_id: "task".into(),
                        expected_revision: 1,
                        request_id: request.proposal.id.clone(),
                        intent_digest: request.intent_digest.clone(),
                    },
                    approve,
                },
            },
        )
        .unwrap();
        self.hub
            .receive(&self.chat.device, &frame, pulse_link::now())
            .await
            .unwrap();
    }
    async fn result(&mut self, outcome: ReadOutcome) -> Envelope<DevicePayload> {
        let id:String=sqlx::query_scalar("SELECT id FROM link_reads WHERE request LIKE '%browser_action%' ORDER BY rowid DESC LIMIT 1").fetch_one(&self.hub.core.application_pool()).await.unwrap();
        self.sequence += 1;
        let frame = Envelope::new(
            &self.local.device,
            self.sequence,
            self.local_epoch,
            DevicePayload::Result {
                operation_id: id,
                task_id: "task".into(),
                revision: "1".into(),
                outcome,
            },
        )
        .unwrap();
        self.hub
            .receive(&self.local.device, &frame, pulse_link::now())
            .await
            .unwrap();
        frame
    }
}
#[tokio::test]
async fn browser_action_waits_for_human_and_known_receipt_continues_same_run_once() {
    let mut f = setup().await;
    let request = f.propose().await;
    for _ in 0..3 {
        assert!(!remote::step(&f.hub, pulse_link::now()).await.unwrap());
    }
    assert_eq!(
        f.hub.core.task("task").await.unwrap().state,
        State::WaitingApproval
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operations")
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    f.hub
        .snapshot(&f.chat.device, f.chat_epoch, pulse_link::now())
        .await
        .unwrap();
    let data: String = sqlx::query_scalar(
        "SELECT data FROM link_outbox WHERE device=? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&f.chat.device)
    .fetch_one(&f.hub.core.application_pool())
    .await
    .unwrap();
    let snapshot: Envelope<ServerPayload> = serde_json::from_str(&data).unwrap();
    let ServerPayload::Snapshot { messages, .. } = snapshot.payload else {
        panic!()
    };
    let card = messages
        .iter()
        .find(|m| m.id == request.proposal.id)
        .unwrap();
    assert!(card.approval.is_some());
    assert_eq!(
        card.artifact.as_ref().unwrap().digest,
        f.observation.png_digest
    );
    assert!(card.artifact_operation_id.is_some());
    f.decide(&request, true).await;
    assert!(remote::step(&f.hub, pulse_link::now()).await.unwrap());
    let data: String =
        sqlx::query_scalar("SELECT request FROM link_reads WHERE request LIKE '%browser_action%'")
            .fetch_one(&f.hub.core.application_pool())
            .await
            .unwrap();
    let action: ReadOp = serde_json::from_str(&data).unwrap();
    let ReadOp::BrowserAction {
        grant: Some(grant), ..
    } = action
    else {
        panic!("no trusted grant")
    };
    assert_eq!(grant.request.state, pulse_core::ApprovalState::Dispatched);
    assert!(!remote::step(&f.hub, pulse_link::now()).await.unwrap());
    let mut outcome = ReadOutcome::text("Mock click receipt".into(), false);
    let mut next = f.observation.clone();
    next.nonce = uuid::Uuid::new_v4().to_string();
    if let ReadOutcome::Completed { observation, .. } = &mut outcome {
        *observation = Some(next);
    }
    let frame = f.result(outcome).await;
    f.hub
        .receive(&f.local.device, &frame, pulse_link::now())
        .await
        .unwrap();
    let continued = f
        .hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(continued.token.run_id, f.run.token.run_id);
    assert_eq!(continued.checkpoint["round"], 2);
    let history = f.hub.model_history(&continued).await.unwrap();
    assert_eq!(history.len(), 2);
    assert!(matches!(
        history[1].call.operation,
        ReadOp::BrowserAction { grant: None, .. }
    ));
    let mut replay = frame;
    f.sequence += 1;
    replay = Envelope::new(&f.local.device, f.sequence, f.local_epoch, replay.payload).unwrap();
    f.hub
        .receive(&f.local.device, &replay, pulse_link::now())
        .await
        .unwrap();
    assert_eq!(
        f.hub.core.task("task").await.unwrap().state,
        State::Planning
    );
    assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 2);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM receipts")
        .fetch_one(&f.hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn rejected_expired_or_revised_approval_never_creates_effect_work() {
    for control in ["reject", "expire", "revise", "cancel", "restart"] {
        let f = setup().await;
        let request = f.propose().await;
        if control == "reject" {
            f.decide(&request, false).await;
        } else {
            f.decide(&request, true).await;
        }
        match control {
            "revise" | "cancel" => {
                let command = if control == "revise" {
                    json!({"kind":"revise","id":"task","expected_revision":"1","goal":"Changed goal"})
                } else {
                    json!({"kind":"cancel","id":"task","expected_revision":"1"})
                };
                let e = f
                    .hub
                    .core
                    .ingest(
                        Origin::Human,
                        "test",
                        "control",
                        json!({"commands":[command]}),
                        pulse_link::now(),
                    )
                    .await
                    .unwrap();
                f.hub.core.route(&e.event_id).await.unwrap();
            }
            "restart" => f
                .hub
                .core
                .recover(&remote::versions(), pulse_link::now())
                .await
                .unwrap(),
            _ => {}
        }
        let time = if control == "expire" {
            request.proposal.expires_at + 1
        } else {
            pulse_link::now()
        };
        remote::step(&f.hub, time).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operations")
            .fetch_one(&f.hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "{control}");
        assert_ne!(
            f.hub.core.task("task").await.unwrap().state,
            State::Succeeded
        );
    }
}

#[tokio::test]
async fn failed_dispatch_preserves_approval_and_rejected_or_unknown_effects_do_not_retry() {
    for rejected in [false, true] {
        let mut f = setup().await;
        let request = f.propose().await;
        f.decide(&request, true).await;
        sqlx::query("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<1000) INSERT INTO link_outbox SELECT ?,10000+x,'fault:'||x,'{}',0 FROM n").bind(&f.local.device).execute(&f.hub.core.application_pool()).await.unwrap();
        remote::step(&f.hub, pulse_link::now()).await.unwrap();
        assert_eq!(
            f.hub
                .core
                .operation_request(&request.proposal.id)
                .await
                .unwrap()
                .state,
            pulse_core::ApprovalState::Approved
        );
        assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 1);
        sqlx::query("DELETE FROM link_outbox WHERE sequence>=10000")
            .execute(&f.hub.core.application_pool())
            .await
            .unwrap();
        assert!(remote::step(&f.hub, pulse_link::now()).await.unwrap());
        f.result(if rejected {
            ReadOutcome::Rejected {
                reason: "changed view; no command issued".into(),
            }
        } else {
            ReadOutcome::Unknown {
                reason: "command may have run".into(),
            }
        })
        .await;
        let task = f.hub.core.task("task").await.unwrap();
        assert_eq!(
            task.state,
            if rejected {
                State::Blocked
            } else {
                State::OutcomeUnknown
            }
        );
        let row = sqlx::query("SELECT data FROM operations")
            .fetch_one(&f.hub.core.application_pool())
            .await
            .unwrap();
        let operation: pulse_core::Operation =
            serde_json::from_str(&row.get::<String, _>("data")).unwrap();
        assert_eq!(
            operation.state,
            if rejected {
                pulse_core::OperationState::NotExecuted
            } else {
                pulse_core::OperationState::Unknown
            }
        );
        assert!(!remote::step(&f.hub, pulse_link::now()).await.unwrap());
        assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 2);
    }
}

#[tokio::test]
async fn cancel_or_revision_after_dispatch_never_continues_the_old_action_run() {
    for control in ["cancel", "revise"] {
        for result in ["completed", "rejected", "unknown"] {
            let mut f = setup().await;
            let request = f.propose().await;
            f.decide(&request, true).await;
            assert!(remote::step(&f.hub, pulse_link::now()).await.unwrap());
            let model_calls = f.hub.core.task("task").await.unwrap().model_calls;
            let command = if control == "cancel" {
                json!({"kind":"cancel","id":"task","expected_revision":"1"})
            } else {
                json!({"kind":"revise","id":"task","expected_revision":"1","goal":"A different goal"})
            };
            let event = f
                .hub
                .core
                .ingest(
                    Origin::Human,
                    "test",
                    "interrupt-action",
                    json!({"commands":[command]}),
                    pulse_link::now(),
                )
                .await
                .unwrap();
            f.hub.core.route(&event.event_id).await.unwrap();
            let outcome = match result {
                "completed" => ReadOutcome::text("Late command receipt".into(), false),
                "rejected" => ReadOutcome::Rejected {
                    reason: "No command issued".into(),
                },
                _ => ReadOutcome::Unknown {
                    reason: "Command may have run".into(),
                },
            };
            f.result(outcome).await;
            let task = f.hub.core.task("task").await.unwrap();
            assert_eq!(task.revision, 2);
            assert!(
                !matches!(
                    task.state,
                    State::Planning | State::Queued | State::Ready | State::Succeeded
                ),
                "{control} {result}: {:?}",
                task.state
            );
            if control == "cancel" {
                assert_eq!(
                    task.state,
                    if result == "unknown" {
                        State::OutcomeUnknown
                    } else {
                        State::Cancelled
                    }
                );
            }
            let row: String = sqlx::query_scalar("SELECT data FROM operations")
                .fetch_one(&f.hub.core.application_pool())
                .await
                .unwrap();
            let operation: pulse_core::Operation = serde_json::from_str(&row).unwrap();
            assert_eq!(
                operation.state,
                match result {
                    "completed" => pulse_core::OperationState::Completed,
                    "rejected" => pulse_core::OperationState::NotExecuted,
                    _ => pulse_core::OperationState::Unknown,
                }
            );
            remote::step(&f.hub, pulse_link::now()).await.unwrap();
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operations")
                .fetch_one(&f.hub.core.application_pool())
                .await
                .unwrap();
            assert_eq!(count, 1, "old action was dispatched again");
            assert_eq!(
                f.hub.core.task("task").await.unwrap().model_calls,
                model_calls
            );
        }
    }
}
