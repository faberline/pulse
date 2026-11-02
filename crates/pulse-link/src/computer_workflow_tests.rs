//! Real journal/server/executor paths with a mock native child. No AX or paid API calls.
use super::*;
use crate::{
    browser_session::WorkBinding,
    computer_runtime::{ComputerRuntime, tests::fixture},
    executor::Executor,
    journal::{DeviceLock, Journal},
    provider::Reply,
    remote,
};
use pulse_core::{
    ApprovalState, ComputerEffect, ComputerObservation, ComputerPermit, ComputerSelection,
};

struct Fixture {
    _root: tempfile::TempDir,
    _helper: tempfile::TempDir,
    helper_path: std::path::PathBuf,
    hub: Hub,
    journal: Journal,
    _lock: DeviceLock,
    runtime: ComputerRuntime,
    executor: Executor,
    local: Credential,
    epoch: u64,
    chat: Credential,
    chat_epoch: u64,
    seq: u64,
    run: pulse_core::Run,
    observation: ComputerObservation,
}
async fn pair(hub: &Hub, role: ClientRole, device: String) -> (Credential, u64) {
    let credential = hub
        .redeem(
            PairRequest {
                code: hub.pairing(role, crate::now()).await.unwrap(),
                device,
                role,
            },
            crate::now(),
        )
        .await
        .unwrap();
    let epoch = hub
        .begin(&credential.device, 0, crate::now())
        .await
        .unwrap();
    if role == ClientRole::LocalExecutor {
        hub.worker_erasure_ready(&credential.device, epoch, crate::now())
            .await
            .unwrap();
    }
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
        cost: 0,
    }
}
fn binding(frame: &Envelope<ServerPayload>) -> WorkBinding {
    let ServerPayload::Read {
        operation_id,
        task_id,
        revision,
        ..
    } = &frame.payload
    else {
        panic!()
    };
    WorkBinding {
        operation_id: operation_id.clone(),
        task_id: task_id.clone(),
        revision: revision.clone(),
        device: frame.device.clone(),
        epoch: number(&frame.epoch).unwrap(),
    }
}
async fn accept_work(hub: &Hub, journal: &Journal, epoch: u64) -> Envelope<ServerPayload> {
    for frame in hub.outgoing(&journal.device).await.unwrap() {
        let permitted = hub.permitted(&frame, epoch, crate::now()).await.unwrap();
        journal
            .accept(&frame, true, epoch, permitted)
            .await
            .unwrap();
        hub.ack(&journal.device, number(&frame.sequence).unwrap(), &frame.id)
            .await
            .unwrap();
    }
    journal.take_work().await.unwrap().unwrap()
}
async fn deliver(hub: &Hub, journal: &Journal, ack: bool) {
    for frame in journal.outgoing().await.unwrap() {
        hub.receive(&journal.device, &frame, crate::now())
            .await
            .unwrap();
        if ack {
            journal
                .ack(number(&frame.sequence).unwrap(), &frame.id)
                .await
                .unwrap();
        }
    }
}
impl Fixture {
    async fn setup(mode: &str) -> Self {
        Self::setup_with_memory(mode, false).await
    }
    async fn setup_with_memory(mode: &str, bound: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let (helper, runtime) = fixture(mode);
        let hub = Hub::new(
            Store::open(root.path().join("server"), "owner")
                .await
                .unwrap(),
        );
        let (journal, lock) = Journal::open(
            &root.path().join("local"),
            "https://pulse.example/",
            root.path(),
        )
        .await
        .unwrap();
        let (local, epoch) = pair(&hub, ClientRole::LocalExecutor, journal.device.clone()).await;
        let (chat, chat_epoch) =
            pair(&hub, ClientRole::Chat, uuid::Uuid::new_v4().to_string()).await;
        let selection = ComputerSelection {
            target: runtime.configured_target().unwrap().clone(),
            process_launched_at: 1,
        };
        let frame = Envelope::new(
            &chat.device,
            1,
            chat_epoch,
            DevicePayload::Human {
                source_id: "create".into(),
                action: HumanAction::SubmitComputer {
                    id: "task".into(),
                    goal: "Press the owned count button once after approval".into(),
                    selection: selection.clone(),
                },
            },
        )
        .unwrap();
        hub.receive(&chat.device, &frame, crate::now())
            .await
            .unwrap();
        let now = crate::now();
        runtime
            .permit_local(ComputerPermit {
                task_id: "task".into(),
                revision: "1".into(),
                selection: selection.clone(),
                selected_at: now,
                expires_at: now + 900_000,
            })
            .unwrap();
        hub.computer_interactive_ready(&local.device, epoch, runtime.selections(), crate::now())
            .await
            .unwrap();
        if bound {
            let now = crate::now();
            hub.core
                .change_memory(
                    Origin::Human,
                    &pulse_core::MemoryRequest {
                        source_id: uuid::Uuid::new_v4().to_string(),
                        source_at: now,
                        command: pulse_core::MemoryCommand::Remember {
                            label: "偏好".into(),
                            body: "SOURCE_NATIVE_PRIVATE".into(),
                        },
                    },
                    now,
                )
                .await
                .unwrap();
            let memory = hub.core.list_memories(now).await.unwrap().remove(0);
            hub.core
                .bind_task_memories(
                    Origin::Human,
                    &pulse_core::TaskMemoryRequest {
                        source_id: uuid::Uuid::new_v4().to_string(),
                        source_at: now,
                        task_id: "task".into(),
                        task_revision: 1,
                        sources: vec![pulse_core::MemoryRef {
                            id: memory.id,
                            revision: memory.revision,
                        }],
                    },
                    now,
                )
                .await
                .unwrap();
        }
        let run = hub
            .core
            .claim_next(&remote::versions(), crate::now())
            .await
            .unwrap()
            .unwrap();
        hub.model_proposal(
            &run.token,
            &reply(ReadOp::ComputerInspect { selection }, "inspect"),
            crate::now(),
        )
        .await
        .unwrap();
        assert!(
            hub.dispatch_model_read(&remote::versions(), crate::now())
                .await
                .unwrap()
        );
        let work = accept_work(&hub, &journal, epoch).await;
        let executor = Executor::new(root.path())
            .unwrap()
            .with_computer(Some(runtime.clone()));
        let ServerPayload::Read { operation, .. } = &work.payload else {
            panic!()
        };
        let result = executor
            .execute_bound(operation.clone(), &binding(&work))
            .await;
        let ReadOutcome::Completed {
            computer: Some(observation),
            ..
        } = &result.outcome
        else {
            panic!("missing fixture source")
        };
        let observation = observation.as_ref().clone();
        journal
            .finish_work_image(&work, result.outcome, result.image)
            .await
            .unwrap();
        deliver(&hub, &journal, true).await;
        let run = hub
            .core
            .claim_next(&remote::versions(), crate::now())
            .await
            .unwrap()
            .unwrap();
        Self {
            helper_path: helper.path().join(format!("{mode}.py")),
            _root: root,
            _helper: helper,
            hub,
            journal,
            _lock: lock,
            runtime,
            executor,
            local,
            epoch,
            chat,
            chat_epoch,
            seq: 1,
            run,
            observation,
        }
    }
    fn action(&self) -> ReadOp {
        ReadOp::ComputerAction {
            selection: self.observation.selection(),
            nonce: self.observation.nonce.clone(),
            element_id: "window.0".into(),
            effect: ComputerEffect::Press,
            grant: None,
        }
    }
    async fn propose(&self) -> OperationRequest {
        self.hub
            .model_proposal(
                &self.run.token,
                &reply(self.action(), "press"),
                crate::now(),
            )
            .await
            .unwrap();
        self.hub
            .project_action_approvals(crate::now())
            .await
            .unwrap();
        let data: String = sqlx::query_scalar("SELECT data FROM operation_requests")
            .fetch_one(&self.hub.db)
            .await
            .unwrap();
        serde_json::from_str(&data).unwrap()
    }
    async fn human(&mut self, action: HumanAction) -> String {
        self.seq += 1;
        let frame = Envelope::new(
            &self.chat.device,
            self.seq,
            self.chat_epoch,
            DevicePayload::Human {
                source_id: format!("human-{}", self.seq),
                action,
            },
        )
        .unwrap();
        self.hub
            .receive(&self.chat.device, &frame, crate::now())
            .await
            .unwrap();
        sqlx::query_scalar("SELECT outcome FROM link_inbox WHERE device=? AND sequence=?")
            .bind(&self.chat.device)
            .bind(self.seq as i64)
            .fetch_one(&self.hub.db)
            .await
            .unwrap()
    }
    async fn decide(&mut self, request: &OperationRequest, approve: bool) {
        let outcome = self
            .human(HumanAction::DecideOperation {
                binding: pulse_core::OperationBinding {
                    task_id: "task".into(),
                    expected_revision: 1,
                    request_id: request.proposal.id.clone(),
                    intent_digest: request.intent_digest.clone(),
                },
                approve,
            })
            .await;
        assert_eq!(outcome, "accepted");
    }
    async fn execute(&self) -> Envelope<ServerPayload> {
        let work = accept_work(&self.hub, &self.journal, self.epoch).await;
        let ServerPayload::Read { operation, .. } = &work.payload else {
            panic!()
        };
        let result = self
            .executor
            .execute_bound(operation.clone(), &binding(&work))
            .await;
        self.journal
            .finish_work_image(&work, result.outcome, result.image)
            .await
            .unwrap();
        work
    }
    fn effects(&self) -> u64 {
        std::fs::read_to_string(self.helper_path.with_extension("effects"))
            .ok()
            .map(|n| n.parse().unwrap())
            .unwrap_or(0)
    }
}
#[tokio::test]
async fn native_model_proposal_human_image_approval_atomic_dispatch_and_receipt_reopen() {
    let mut f = Fixture::setup("action-good").await;
    let request = f.propose().await;
    assert_eq!(f.effects(), 0);
    assert_eq!(request.state, ApprovalState::Pending);
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<20) INSERT INTO link_chat SELECT printf('noise-%d',x),'Another task progress message',? FROM n").bind(crate::now()).execute(&f.hub.db).await.unwrap();
    f.hub
        .snapshot(&f.chat.device, f.chat_epoch, crate::now())
        .await
        .unwrap();
    let projected = f
        .hub
        .outgoing(&f.chat.device)
        .await
        .unwrap()
        .into_iter()
        .rev()
        .find_map(|frame| match frame.payload {
            ServerPayload::Snapshot { messages, .. } => Some(messages),
            _ => None,
        })
        .unwrap();
    let card = projected
        .iter()
        .find(|m| m.id == request.proposal.id)
        .unwrap();
    assert_eq!(
        card.artifact_operation_id.as_deref(),
        Some(f.observation.binding.operation_id.as_str())
    );
    assert!(
        card.artifact
            .as_ref()
            .unwrap()
            .matches_computer(&f.observation)
    );
    f.decide(&request, true).await;
    f.hub
        .computer_ready(
            &f.local.device,
            f.epoch,
            f.runtime.selections(),
            crate::now(),
        )
        .await
        .unwrap();
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap(),
        "read-only clients cannot execute effects"
    );
    f.hub
        .computer_interactive_ready(
            &f.local.device,
            f.epoch,
            f.runtime.selections(),
            crate::now(),
        )
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_native_delivery BEFORE INSERT ON link_outbox BEGIN SELECT RAISE(ABORT,'controlled delivery fault'); END;").execute(&f.hub.db).await.unwrap();
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .is_err()
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
    sqlx::raw_sql("DROP TRIGGER reject_native_delivery")
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
    f.execute().await;
    assert_eq!(f.effects(), 1);
    deliver(&f.hub, &f.journal, false).await;
    let pending = f.journal.outgoing().await.unwrap();
    assert_eq!(pending.len(), 2);
    let directory = f._root.path().join("local");
    drop(f.journal);
    drop(f._lock);
    let (journal, _lock) = Journal::open(&directory, "https://pulse.example/", f._root.path())
        .await
        .unwrap();
    journal.recover().await.unwrap();
    assert!(journal.take_work().await.unwrap().is_none());
    deliver(&f.hub, &journal, true).await;
    assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 2);
    let state: String = sqlx::query_scalar("SELECT json_extract(data,'$.state') FROM operations")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(state, "completed");
    assert_eq!(
        std::fs::read_to_string(f.helper_path.with_extension("effects")).unwrap(),
        "1"
    );
}
#[tokio::test]
async fn native_unknown_and_preflight_rejection_keep_distinct_durable_outcomes_without_replay() {
    for mode in ["action-unknown", "action-rejected"] {
        let mut f = Fixture::setup(mode).await;
        let request = f.propose().await;
        f.decide(&request, true).await;
        assert!(
            f.hub
                .dispatch_approved_action(&remote::versions(), crate::now())
                .await
                .unwrap()
        );
        f.execute().await;
        deliver(&f.hub, &f.journal, true).await;
        let task = f.hub.core.task("task").await.unwrap();
        if mode == "action-unknown" {
            assert_eq!(task.state, State::OutcomeUnknown);
            assert_eq!(f.effects(), 1);
        } else {
            assert_eq!(task.state, State::Blocked);
            assert_eq!(task.reason.as_deref(), Some("computer_action_not_executed"));
            assert_eq!(f.effects(), 0);
            let outcome = f
                .human(HumanAction::RebindComputer {
                    task_id: "task".into(),
                    revision: "1".into(),
                    selection: f.observation.selection(),
                })
                .await;
            assert_eq!(outcome, "accepted");
            assert_eq!(f.hub.core.task("task").await.unwrap().revision, 2);
        }
        f.hub = Hub::new(
            Store::open(f._root.path().join("server"), "owner")
                .await
                .unwrap(),
        );
        f.hub.recover().await.unwrap();
        f.hub
            .core
            .recover(&remote::versions(), crate::now())
            .await
            .unwrap();
        assert!(
            !f.hub
                .dispatch_approved_action(&remote::versions(), crate::now())
                .await
                .unwrap()
        );
        assert!(
            !f.hub
                .dispatch_model_read(&remote::versions(), crate::now())
                .await
                .unwrap()
        );
        assert!(f.journal.take_work().await.unwrap().is_none());
        assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 2);
    }
}
#[tokio::test]
async fn native_direct_read_bypass_rejection_cancellation_and_source_tamper_never_execute() {
    for change in ["direct", "reject", "cancel", "image"] {
        let mut f = Fixture::setup("action-good").await;
        if change == "direct" {
            let outcome = f
                .human(HumanAction::Read {
                    task_id: "task".into(),
                    revision: "1".into(),
                    operation: f.action(),
                })
                .await;
            assert_eq!(outcome, "rejected");
            assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 1);
        } else {
            let request = f.propose().await;
            if change == "reject" {
                f.decide(&request, false).await;
            } else {
                f.decide(&request, true).await;
            }
            if change == "cancel" {
                let outcome = f
                    .human(HumanAction::Cancel {
                        task_id: "task".into(),
                        revision: "1".into(),
                    })
                    .await;
                assert_eq!(outcome, "accepted");
            }
            if change == "image" {
                sqlx::query("UPDATE link_artifacts SET png=NULL")
                    .execute(&f.hub.db)
                    .await
                    .unwrap();
            }
            f.hub
                .dispatch_approved_action(&remote::versions(), crate::now())
                .await
                .unwrap();
            assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 1);
        }
        assert_eq!(f.effects(), 0, "{change}");
    }
}

#[tokio::test]
async fn native_action_reserves_original_image_capacity_before_local_acceptance() {
    let mut f = Fixture::setup("action-good").await;
    let request = f.propose().await;
    f.decide(&request, true).await;
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<950) INSERT INTO local_outbox SELECT x,printf('full-%d',x),'{}' FROM n").execute(&f.journal.db).await.unwrap();
    let work = f
        .hub
        .outgoing(&f.local.device)
        .await
        .unwrap()
        .into_iter()
        .find(|e| {
            matches!(
                e.payload,
                ServerPayload::Read {
                    operation: ReadOp::ComputerAction { .. },
                    ..
                }
            )
        })
        .unwrap();
    assert!(f.journal.accept(&work, true, f.epoch, true).await.is_err());
    assert!(f.journal.take_work().await.unwrap().is_none());
    assert_eq!(f.effects(), 0);
}

#[tokio::test]
async fn native_effect_before_server_restart_keeps_late_receipt_without_another_dispatch() {
    let mut f = Fixture::setup("action-good").await;
    let request = f.propose().await;
    f.decide(&request, true).await;
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    f.execute().await;
    assert_eq!(f.effects(), 1);
    f.hub = Hub::new(
        Store::open(f._root.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    f.hub
        .core
        .recover(&remote::versions(), crate::now())
        .await
        .unwrap();
    f.hub.recover().await.unwrap();
    assert_eq!(
        f.hub.core.task("task").await.unwrap().state,
        State::OutcomeUnknown
    );
    deliver(&f.hub, &f.journal, true).await;
    assert_eq!(f.effects(), 1);
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    assert!(
        !f.hub
            .dispatch_model_read(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    assert_ne!(
        f.hub.core.task("task").await.unwrap().state,
        State::Succeeded
    );
    assert_eq!(f.hub.core.task("task").await.unwrap().tool_calls, 2);
    assert!(f.journal.take_work().await.unwrap().is_none());
}

#[tokio::test]
async fn native_source_erasure_discards_late_images_without_inventing_a_known_effect() {
    let mut f = Fixture::setup_with_memory("action-good", true).await;
    f.hub
        .core
        .reserve(
            &f.run.token,
            "before-source-removal",
            20_000,
            "fixture",
            crate::now(),
        )
        .await
        .unwrap();
    let request = f.propose().await;
    f.decide(&request, true).await;
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    let work = f.execute().await;
    assert_eq!(f.effects(), 1);
    let memory = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    f.hub
        .core
        .change_memory(
            Origin::Human,
            &pulse_core::MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: crate::now(),
                command: pulse_core::MemoryCommand::Forget {
                    id: memory.id,
                    revision: memory.revision,
                },
            },
            crate::now(),
        )
        .await
        .unwrap();
    let ServerPayload::Read { operation_id, .. } = &work.payload else {
        panic!()
    };
    let packets = f.journal.outgoing().await.unwrap();
    assert!(
        packets
            .iter()
            .any(|e| matches!(e.payload, DevicePayload::ArtifactChunk { .. }))
    );
    for packet in &packets {
        f.hub
            .receive(&f.local.device, packet, crate::now())
            .await
            .unwrap();
    }
    for packet in &packets {
        f.hub
            .receive(&f.local.device, packet, crate::now())
            .await
            .unwrap();
    }
    assert_eq!(f.effects(), 1);
    let state: (String, Option<String>) =
        sqlx::query_as("SELECT state,result FROM link_reads WHERE id=?")
            .bind(operation_id)
            .fetch_one(&f.hub.db)
            .await
            .unwrap();
    assert_eq!(state, ("unknown".into(), None));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM link_artifacts")
            .fetch_one(&f.hub.db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM link_artifact_parts")
            .fetch_one(&f.hub.db)
            .await
            .unwrap(),
        0
    );
    let task = f.hub.core.task("task").await.unwrap();
    assert_eq!(task.state, State::OutcomeUnknown);
    assert!(task.active_run.is_none());
    assert_eq!(task.tool_calls, 2);
    let operation: String = sqlx::query_scalar("SELECT data FROM operations WHERE id=?")
        .bind(operation_id)
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Operation>(&operation).unwrap().state,
        pulse_core::OperationState::Unknown
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM budget WHERE call_id='before-source-removal'"
        )
        .fetch_one(&f.hub.db)
        .await
        .unwrap(),
        "reserved"
    );
    assert!(
        !f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    f.hub
        .snapshot(&f.chat.device, f.chat_epoch, crate::now())
        .await
        .unwrap();
    let latest: String = sqlx::query_scalar(
        "SELECT data FROM link_outbox WHERE device=? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&f.chat.device)
    .fetch_one(&f.hub.db)
    .await
    .unwrap();
    assert!(!latest.contains("source_operation_id"));
    assert!(!latest.contains("png_digest"));
}

#[tokio::test]
async fn native_worker_erasure_cancels_an_active_child_and_revokes_cached_selection() {
    let mut f = Fixture::setup_with_memory("action-hang", true).await;
    let request = f.propose().await;
    f.decide(&request, true).await;
    assert!(
        f.hub
            .dispatch_approved_action(&remote::versions(), crate::now())
            .await
            .unwrap()
    );
    for frame in f.hub.outgoing(&f.journal.device).await.unwrap() {
        f.journal.accept(&frame, true, f.epoch, true).await.unwrap();
        f.hub
            .ack(
                &f.journal.device,
                number(&frame.sequence).unwrap(),
                &frame.id,
            )
            .await
            .unwrap();
    }
    let worker = crate::client::test_live_worker(f.journal.clone(), f.executor.clone(), f.epoch);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while f.effects() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.effects(), 1);
    let memory = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    f.hub
        .core
        .change_memory(
            Origin::Human,
            &pulse_core::MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: crate::now(),
                command: pulse_core::MemoryCommand::Forget {
                    id: memory.id,
                    revision: memory.revision,
                },
            },
            crate::now(),
        )
        .await
        .unwrap();
    let operation = format!("approved:{}", request.proposal.id);
    let stored: String = sqlx::query_scalar("SELECT data FROM operations WHERE id=?")
        .bind(&operation)
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    let stored: Operation = serde_json::from_str(&stored).unwrap();
    assert_eq!(stored.intent, json!({"source_erased":true}));
    let proposal: String = sqlx::query_scalar("SELECT data FROM operation_requests WHERE id=?")
        .bind(&request.proposal.id)
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    let proposal: OperationRequest = serde_json::from_str(&proposal).unwrap();
    assert_eq!(proposal.proposal.intent, json!({"source_erased":true}));
    let context: pulse_core::WorkerErasure = serde_json::from_str(
        &sqlx::query_scalar::<_, String>("SELECT context FROM memory_erased_reads WHERE id=?")
            .bind(&operation)
            .fetch_one(&f.hub.db)
            .await
            .unwrap(),
    )
    .unwrap();
    f.journal
        .accept_worker_erasure(context.read_header.as_ref().unwrap(), &context)
        .await
        .unwrap();
    f.executor.invalidate_source(&context).await;
    crate::computer_runtime::tests::assert_reaped(&f.runtime).await;
    assert_eq!(f.effects(), 1);
    assert!(!f.runtime.permitted("task", "1", &f.observation.selection()));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let state: String = sqlx::query_scalar("SELECT state FROM local_work WHERE id=?")
                .bind(&operation)
                .fetch_one(&f.journal.db)
                .await
                .unwrap();
            if state == "erased" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    assert_eq!(
        f.hub.core.task("task").await.unwrap().state,
        State::OutcomeUnknown
    );
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn reselecting_an_undispatched_approval_keeps_the_task_and_invalidates_the_old_grant() {
    for approved in [false, true] {
        let mut f = Fixture::setup("action-good").await;
        let request = f.propose().await;
        if approved {
            f.decide(&request, true).await;
        }
        let before = f.hub.core.task("task").await.unwrap();
        let outcome = f
            .human(HumanAction::RebindComputer {
                task_id: "task".into(),
                revision: "1".into(),
                selection: f.observation.selection(),
            })
            .await;
        assert_eq!(outcome, "accepted");
        let after = f.hub.core.task("task").await.unwrap();
        assert_eq!(after.id, before.id);
        assert_eq!(after.revision, 2);
        assert_eq!(after.deadline, before.deadline);
        assert_eq!(after.scope, before.scope);
        assert_eq!(
            (after.model_calls, after.tool_calls),
            (before.model_calls, before.tool_calls)
        );
        assert_eq!(
            f.hub
                .core
                .operation_request(&request.proposal.id)
                .await
                .unwrap()
                .state,
            ApprovalState::Invalidated
        );
        assert!(
            !f.hub
                .dispatch_approved_action(&remote::versions(), crate::now())
                .await
                .unwrap()
        );
        assert_eq!(f.effects(), 0);
    }
}
