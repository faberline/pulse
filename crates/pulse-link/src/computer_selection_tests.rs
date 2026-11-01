//! Controlled journal/task fixtures. They are not native picker or capture acceptance.
use super::*;
use crate::server::Hub;
use pulse_core::{Origin, Store};
use serde_json::json;

fn selection() -> ComputerSelection {
    ComputerSelection {
        target: pulse_core::ComputerTarget {
            bundle_id: "com.faberline.PulseComputerFixture".into(),
            pid: 42,
            window_id: 7,
        },
        process_launched_at: 1,
    }
}
#[tokio::test]
async fn full_outbox_rolls_back_the_entire_intent_and_local_role_cannot_queue_it() {
    let root = tempfile::tempdir().unwrap();
    let (journal, _lock) = Journal::open_role(
        &root.path().join("chat"),
        "https://pulse.example/",
        root.path(),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000) INSERT INTO local_outbox SELECT x,printf('full-%d',x),'{}' FROM n")
        .execute(&journal.db).await.unwrap();
    let action = HumanAction::SubmitComputer {
        id: "task".into(),
        goal: "Read the owned fixture".into(),
        selection: selection(),
    };
    assert!(
        journal
            .queue_computer_selection("send", action.clone(), crate::now())
            .await
            .is_err()
    );
    let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_pending")
        .fetch_one(&journal.db)
        .await
        .unwrap();
    assert_eq!(pending, 0);
    assert!(
        journal
            .computer_selection_statuses()
            .await
            .unwrap()
            .is_empty()
    );
    let outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_outbox")
        .fetch_one(&journal.db)
        .await
        .unwrap();
    assert_eq!(outbox, 1000);
    let (local, _local_lock) = Journal::open_role(
        &root.path().join("local"),
        "https://pulse.example/",
        root.path(),
        ClientRole::LocalExecutor,
    )
    .await
    .unwrap();
    assert!(
        local
            .queue_computer_selection("forged", action, crate::now())
            .await
            .is_err()
    );
    assert!(local.outgoing().await.unwrap().is_empty());
}
async fn snapshot(journal: &Journal, tasks: Vec<Task>, sequence: u64) {
    let frame = Envelope::new(
        &journal.device,
        sequence,
        1,
        ServerPayload::Snapshot {
            tasks,
            messages: vec![],
            synced_at: crate::now(),
            local: LocalStatus::default(),
        },
    )
    .unwrap();
    journal.accept(&frame, true, 1, true).await.unwrap();
}
async fn task(store: &Store) -> Task {
    let event=store.ingest(Origin::Human,"fixture","create",json!({"commands":[{"kind":"create","id":"task","goal":"Read the owned fixture","scope":selection().scope().unwrap(),"priority":0,"requires_device":false}]}),crate::now()).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    store.task("task").await.unwrap()
}
#[tokio::test]
async fn offline_goal_and_original_selection_survive_reopen_without_duplicate_envelopes_or_renewal()
{
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("chat");
    let (journal, lock) = Journal::open_role(
        &directory,
        "https://pulse.example/",
        root.path(),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    let selected_at = crate::now();
    let action = HumanAction::SubmitComputer {
        id: "task".into(),
        goal: "Read the owned fixture".into(),
        selection: selection(),
    };
    journal
        .queue_computer_selection("send", action.clone(), selected_at)
        .await
        .unwrap();
    assert_eq!(journal.outgoing().await.unwrap().len(), 1);
    assert!(
        journal
            .view()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|m| m.id == "send")
    );
    assert!(
        journal
            .take_computer_selection(selected_at)
            .await
            .unwrap()
            .is_none()
    );
    drop(journal);
    drop(lock);
    let (journal, _lock) = Journal::open_role(
        &directory,
        "https://pulse.example/",
        root.path(),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    journal
        .queue_computer_selection("send", action.clone(), selected_at)
        .await
        .unwrap();
    assert_eq!(journal.outgoing().await.unwrap().len(), 1);
    assert!(
        journal
            .queue_computer_selection("send", action.clone(), selected_at + 1)
            .await
            .is_err()
    );
    let store = Store::open(root.path().join("server"), "owner")
        .await
        .unwrap();
    let original = task(&store).await;
    snapshot(&journal, vec![original], 1).await;
    let first = journal
        .take_computer_selection(selected_at)
        .await
        .unwrap()
        .unwrap();
    assert!(!first.replace);
    assert!(
        journal
            .take_computer_selection(selected_at + 1)
            .await
            .unwrap()
            .is_none()
    );
    let retry = journal
        .take_computer_selection(selected_at + 30_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.permit, retry.permit);
    assert_eq!(retry.permit.expires_at, selected_at + 900_000);
    journal.prepared_computer_selection("send").await.unwrap();
    assert_eq!(
        journal.computer_selection_statuses().await.unwrap()[0].state,
        SelectionPhase::Prepared
    );
    assert!(
        journal
            .take_computer_selection(selected_at + 60_000)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        journal
            .take_computer_selection(selected_at + 900_000)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        journal
            .computer_selection_statuses()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.task("task").await.unwrap().revision, 1);
}
#[tokio::test]
async fn replacement_waits_for_the_new_scope_and_invalidates_cancelled_or_unknown_tasks() {
    let root = tempfile::tempdir().unwrap();
    let (journal, _lock) = Journal::open_role(
        &root.path().join("chat"),
        "https://pulse.example/",
        root.path(),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    let store = Store::open(root.path().join("server"), "owner")
        .await
        .unwrap();
    let original = task(&store).await;
    snapshot(&journal, vec![original.clone()], 1).await;
    let mut selected = selection();
    selected.target.window_id += 1;
    let selected_at = crate::now();
    let action = HumanAction::RebindComputer {
        task_id: "task".into(),
        revision: "1".into(),
        selection: selected.clone(),
    };
    journal
        .queue_computer_selection("replace", action.clone(), selected_at)
        .await
        .unwrap();
    assert!(
        journal
            .take_computer_selection(selected_at)
            .await
            .unwrap()
            .is_none()
    );
    let mut updated = original.clone();
    updated.revision = 2;
    updated.scope = selected.scope().unwrap();
    snapshot(&journal, vec![updated.clone()], 2).await;
    let request = journal
        .take_computer_selection(selected_at)
        .await
        .unwrap()
        .unwrap();
    assert!(request.replace);
    assert_eq!(request.permit.revision, "2");
    journal
        .queue_computer_selection("replace", action, selected_at)
        .await
        .unwrap();
    assert_eq!(journal.outgoing().await.unwrap().len(), 1);
    updated.state = State::OutcomeUnknown;
    snapshot(&journal, vec![updated], 3).await;
    assert!(
        journal
            .take_computer_selection(selected_at + 30_000)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        journal.computer_selection_statuses().await.unwrap()[0].state,
        SelectionPhase::Invalidated
    );
    assert!(
        journal
            .queue_computer_selection(
                "unknown",
                HumanAction::RebindComputer {
                    task_id: "task".into(),
                    revision: "2".into(),
                    selection: selected
                },
                selected_at
            )
            .await
            .is_err()
    );
    let mut cancelled = original;
    cancelled.cancel_requested = true;
    snapshot(&journal, vec![cancelled], 4).await;
    assert!(
        journal
            .queue_computer_selection(
                "cancelled",
                HumanAction::RebindComputer {
                    task_id: "task".into(),
                    revision: "1".into(),
                    selection: selection()
                },
                selected_at
            )
            .await
            .is_err()
    );
}
async fn pair(hub: &Hub, role: ClientRole) -> (String, u64) {
    let time = crate::now();
    let device = uuid::Uuid::new_v4().to_string();
    hub.redeem(
        PairRequest {
            code: hub.pairing(role, time).await.unwrap(),
            device: device.clone(),
            role,
        },
        time,
    )
    .await
    .unwrap();
    let epoch = hub.begin(&device, 0, time).await.unwrap();
    (device, epoch)
}
async fn rebind(
    hub: &Hub,
    chat: &str,
    epoch: u64,
    seq: u64,
    source: &str,
    revision: &str,
    selected: ComputerSelection,
) -> String {
    let frame = Envelope::new(
        chat,
        seq,
        epoch,
        DevicePayload::Human {
            source_id: source.into(),
            action: HumanAction::RebindComputer {
                task_id: "task".into(),
                revision: revision.into(),
                selection: selected,
            },
        },
    )
    .unwrap();
    hub.receive(chat, &frame, crate::now()).await.unwrap();
    sqlx::query_scalar("SELECT outcome FROM link_inbox WHERE device=? AND sequence=?")
        .bind(chat)
        .bind(seq as i64)
        .fetch_one(&hub.db)
        .await
        .unwrap()
}
#[tokio::test]
async fn reselecting_keeps_task_identity_budget_and_deadline_and_replays_only_one_revision() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    task(&hub.core).await;
    let (local, local_epoch) = pair(&hub, ClientRole::LocalExecutor).await;
    let run = hub
        .core
        .claim_next(&crate::remote::versions(), crate::now())
        .await
        .unwrap()
        .unwrap();
    assert!(
        hub.core
            .reserve(
                &run.token,
                "fixture-call",
                1000,
                "fixture-price",
                crate::now()
            )
            .await
            .unwrap()
    );
    hub.core
        .settle("fixture-call", 100, "controlled usage")
        .await
        .unwrap();
    let reply = crate::provider::Reply {
        text: String::new(),
        tool: Some(crate::provider::ToolCall {
            call_id: "prior-read".into(),
            operation: ReadOp::ComputerInspect {
                selection: selection(),
            },
        }),
        usage: json!({}),
        cost: 100,
    };
    hub.model_proposal(&run.token, &reply, crate::now())
        .await
        .unwrap();
    let selected_at = crate::now();
    hub.computer_ready(
        &local,
        local_epoch,
        vec![ComputerPermit {
            task_id: "task".into(),
            revision: "1".into(),
            selection: selection(),
            selected_at,
            expires_at: selected_at + 900_000,
        }],
        selected_at,
    )
    .await
    .unwrap();
    assert!(
        hub.dispatch_model_read(&crate::remote::versions(), crate::now())
            .await
            .unwrap()
    );
    let frame = hub
        .outgoing(&local)
        .await
        .unwrap()
        .into_iter()
        .find(|frame| matches!(frame.payload, ServerPayload::Read { .. }))
        .unwrap();
    let ServerPayload::Read { operation_id, .. } = frame.payload else {
        panic!()
    };
    let receipt = Envelope::new(
        &local,
        1,
        local_epoch,
        DevicePayload::Result {
            operation_id,
            task_id: "task".into(),
            revision: "1".into(),
            outcome: ReadOutcome::Rejected {
                reason: "controlled read rejection".into(),
            },
        },
    )
    .unwrap();
    hub.receive(&local, &receipt, crate::now()).await.unwrap();
    let original = hub.core.task("task").await.unwrap();
    assert_eq!((original.model_calls, original.tool_calls), (1, 1));
    let (chat, epoch) = pair(&hub, ClientRole::Chat).await;
    assert_eq!(
        rebind(&hub, &chat, epoch, 1, "same", "1", selection()).await,
        "accepted"
    );
    let same = hub.core.task("task").await.unwrap();
    assert_eq!(same.revision, 1);
    assert_eq!(same.authorization_event, original.authorization_event);
    let mut changed = selection();
    changed.target.window_id += 1;
    assert_eq!(
        rebind(&hub, &chat, epoch, 2, "changed", "1", changed.clone()).await,
        "accepted"
    );
    assert_eq!(
        rebind(&hub, &chat, epoch, 3, "changed", "1", changed.clone()).await,
        "accepted"
    );
    let updated = hub.core.task("task").await.unwrap();
    assert_eq!(updated.revision, 2);
    assert_ne!(updated.authorization_event, original.authorization_event);
    assert_eq!(updated.scope, changed.scope().unwrap());
    assert_eq!(updated.goal, original.goal);
    assert_eq!(updated.deadline, original.deadline);
    assert_eq!(
        (updated.model_calls, updated.tool_calls),
        (original.model_calls, original.tool_calls)
    );
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
    assert_eq!(
        rebind(&hub, &chat, epoch, 4, "stale", "1", selection()).await,
        "rejected"
    );
    assert_eq!(hub.core.task("task").await.unwrap().revision, 2);
    let paid: i64 = sqlx::query_scalar("SELECT SUM(charged) FROM budget WHERE task_id='task'")
        .fetch_one(&hub.db)
        .await
        .unwrap();
    assert_eq!(paid, 100);
    let forged = Envelope::new(
        &local,
        2,
        local_epoch,
        DevicePayload::Human {
            source_id: "local-forgery".into(),
            action: HumanAction::RebindComputer {
                task_id: "task".into(),
                revision: "2".into(),
                selection: changed,
            },
        },
    )
    .unwrap();
    assert!(hub.receive(&local, &forged, crate::now()).await.is_err());
}
#[tokio::test]
async fn unknown_reads_cannot_be_reselected_or_dispatched_again() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    task(&hub.core).await;
    let (chat, epoch) = pair(&hub, ClientRole::Chat).await;
    let (local, local_epoch) = pair(&hub, ClientRole::LocalExecutor).await;
    let run = hub
        .core
        .claim_next(&crate::remote::versions(), crate::now())
        .await
        .unwrap()
        .unwrap();
    let reply = crate::provider::Reply {
        text: String::new(),
        tool: Some(crate::provider::ToolCall {
            call_id: "read".into(),
            operation: ReadOp::ComputerInspect {
                selection: selection(),
            },
        }),
        usage: json!({}),
        cost: 0,
    };
    hub.model_proposal(&run.token, &reply, crate::now())
        .await
        .unwrap();
    let selected_at = crate::now();
    hub.computer_ready(
        &local,
        local_epoch,
        vec![ComputerPermit {
            task_id: "task".into(),
            revision: "1".into(),
            selection: selection(),
            selected_at,
            expires_at: selected_at + 900_000,
        }],
        selected_at,
    )
    .await
    .unwrap();
    assert!(
        hub.dispatch_model_read(&crate::remote::versions(), crate::now())
            .await
            .unwrap()
    );
    hub.core
        .recover(&crate::remote::versions(), crate::now())
        .await
        .unwrap();
    let old = hub.core.task("task").await.unwrap();
    assert_eq!(old.state, State::OutcomeUnknown);
    assert_eq!(
        rebind(&hub, &chat, epoch, 1, "unknown", "1", selection()).await,
        "rejected"
    );
    assert!(
        !hub.dispatch_model_read(&crate::remote::versions(), crate::now())
            .await
            .unwrap()
    );
    let current = hub.core.task("task").await.unwrap();
    assert_eq!(current.state, State::OutcomeUnknown);
    assert_eq!(current.revision, 1);
    assert_eq!(current.tool_calls, 1);
}
