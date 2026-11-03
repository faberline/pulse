use pulse_core::*;
use serde_json::{Value, json};
use sqlx::{
    Connection, Executor,
    sqlite::{SqliteConnectOptions, SqliteConnection},
};

const NOW: i64 = 1_800_000_000_000;
async fn command(store: &Store, origin: Origin, key: &str, commands: Value, now: i64) -> Outcome {
    let event = store
        .ingest(origin, "ui", key, json!({"commands":commands}), now)
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap()
}
async fn setup() -> (tempfile::TempDir, Store, Run) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    command(&store,Origin::Human,"create",json!([{"kind":"create","id":"task","goal":"test fixture","scope":"pulse:read","priority":0,"requires_device":false}]),NOW).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    (dir, store, run)
}
async fn source(
    store: &Store,
    origin: Origin,
    key: &str,
    proposal: &OperationProposal,
    now: i64,
) -> String {
    let event=store.ingest(origin,"planner",key,json!({"commands":[{"kind":"query","ids":[proposal.token.task_id]}],"operation_proposal":proposal}),now).await.unwrap();
    assert!(matches!(
        store.route(&event.event_id).await.unwrap(),
        Outcome::Applied { .. }
    ));
    event.event_id
}
async fn propose(store: &Store, run: &Run, id: &str, now: i64) -> OperationRequest {
    let proposal = OperationProposal {
        id: id.into(),
        token: run.token.clone(),
        target: "fixture:counter".into(),
        intent: json!({"delta":1.0,"approved":true,"author":"human"}),
        expires_at: NOW + 60_000,
    };
    let event = source(store, Origin::Tool, id, &proposal, now).await;
    store
        .propose_operation(&event, proposal, now)
        .await
        .unwrap()
}
fn binding(request: &OperationRequest) -> OperationBinding {
    OperationBinding {
        task_id: request.proposal.token.task_id.clone(),
        expected_revision: request.proposal.token.revision,
        request_id: request.proposal.id.clone(),
        intent_digest: request.intent_digest.clone(),
    }
}
fn decision(request: &OperationRequest, approve: bool) -> Value {
    json!({"kind":if approve {"approve_operation"} else {"reject_operation"},"binding":binding(request)})
}
async fn approve(store: &Store, request: &OperationRequest, key: &str, now: i64) {
    assert!(matches!(
        command(
            store,
            Origin::Human,
            key,
            json!([decision(request, true)]),
            now
        )
        .await,
        Outcome::Applied { .. }
    ));
}
async fn count(store: &Store, table: &str) -> i64 {
    let query = match table {
        "budget" => "SELECT COUNT(*) FROM budget",
        "operations" => "SELECT COUNT(*) FROM operations",
        "operation_requests" => "SELECT COUNT(*) FROM operation_requests",
        _ => panic!("unknown test table"),
    };
    sqlx::query_scalar(query)
        .fetch_one(&store.application_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn proposals_need_applied_linked_sources_and_cannot_expand_effect_scope() {
    let (_dir, store, run) = setup().await;
    let base = OperationProposal {
        id: "request".into(),
        token: run.token.clone(),
        target: "fixture:counter".into(),
        intent: json!({}),
        expires_at: NOW + 60_000,
    };
    let event = store
        .ingest(
            Origin::Tool,
            "planner",
            "unapplied",
            json!({"commands":[{"kind":"query","ids":["task"]}],"operation_proposal":base}),
            NOW + 2,
        )
        .await
        .unwrap();
    assert!(
        store
            .propose_operation(&event.event_id, base.clone(), NOW + 2)
            .await
            .is_err()
    );
    let unlinked = store
        .ingest(
            Origin::Tool,
            "planner",
            "unlinked",
            json!({"commands":[],"operation_proposal":base}),
            NOW + 2,
        )
        .await
        .unwrap();
    store.route(&unlinked.event_id).await.unwrap();
    assert!(
        store
            .propose_operation(&unlinked.event_id, base.clone(), NOW + 2)
            .await
            .is_err()
    );
    for target in ["browser:session", "computer:app", "fixture:"] {
        let mut p = base.clone();
        p.target = target.into();
        assert!(matches!(
            store.propose_operation("missing", p, NOW + 2).await,
            Err(Error::Rejected(_))
        ));
    }
    for expires in [NOW + 2, NOW + 900_001] {
        let mut p = base.clone();
        p.expires_at = expires;
        let event = source(
            &store,
            Origin::Tool,
            &format!("expiry-{expires}"),
            &p,
            NOW + 2,
        )
        .await;
        assert!(store.propose_operation(&event, p, NOW + 2).await.is_err());
    }
    assert_eq!(count(&store, "operation_requests").await, 0);
    assert_eq!(store.task("task").await.unwrap().state, State::Planning);
}

#[tokio::test]
async fn waiting_approval_does_not_block_or_authorize_another_task() {
    let (_dir, store, run) = setup().await;
    let request = propose(&store, &run, "request", NOW + 2).await;
    command(&store,Origin::Human,"create-b",json!([{"kind":"create","id":"b","goal":"other task","scope":"pulse:read","priority":0,"requires_device":false}]),NOW+3).await;
    let b = store
        .claim_next(&Versions::fixture(), NOW + 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b.token.task_id, "b");
    let before = serde_json::to_value(store.task("b").await.unwrap()).unwrap();
    approve(&store, &request, "approve", NOW + 5).await;
    assert!(
        store
            .prepare_approved_operation(&b.token, "request", NOW + 6)
            .await
            .is_err()
    );
    command(
        &store,
        Origin::Human,
        "cancel-a",
        json!([{"kind":"cancel","id":"task","expected_revision":"1"}]),
        NOW + 7,
    )
    .await;
    assert_eq!(
        serde_json::to_value(store.task("b").await.unwrap()).unwrap(),
        before
    );
}

#[tokio::test]
async fn coordinator_finish_invalidates_an_unused_approval() {
    let (_dir, store, run) = setup().await;
    let request = propose(&store, &run, "request", NOW + 2).await;
    approve(&store, &request, "approve", NOW + 3).await;
    store
        .finish(
            &run.token,
            true,
            "verified goal needs no further effect",
            NOW + 4,
        )
        .await
        .unwrap();
    assert_eq!(
        store.operation_request("request").await.unwrap().state,
        ApprovalState::Invalidated
    );
    assert!(
        store
            .prepare_approved_operation(&run.token, "request", NOW + 5)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "operations").await, 0);
}

#[tokio::test]
async fn proposal_history_is_bounded_across_rejected_attempts_and_new_epochs() {
    let (_dir, store, mut run) = setup().await;
    for i in 0..32 {
        let at = NOW + 2 + i * 10;
        let request = propose(&store, &run, &format!("request-{i}"), at).await;
        let key = format!("reject-{i}");
        assert!(matches!(
            command(
                &store,
                Origin::Human,
                &key,
                json!([decision(&request, false)]),
                at + 1
            )
            .await,
            Outcome::Applied { .. }
        ));
        command(
            &store,
            Origin::Human,
            &format!("resume-{i}"),
            json!([{"kind":"resume","id":"task","expected_revision":"1"}]),
            at + 2,
        )
        .await;
        run = store
            .claim_next(&Versions::fixture(), at + 3)
            .await
            .unwrap()
            .unwrap();
    }
    let proposal = OperationProposal {
        id: "overflow".into(),
        token: run.token.clone(),
        target: "fixture:counter".into(),
        intent: json!({}),
        expires_at: NOW + 60_000,
    };
    let event = source(&store, Origin::Tool, "overflow", &proposal, NOW + 400).await;
    assert!(
        store
            .propose_operation(&event, proposal, NOW + 400)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "operation_requests").await, 32);
    assert_eq!(store.task("task").await.unwrap().tool_calls, 0);
}

#[tokio::test]
async fn durable_approval_dispatches_once_and_a_receipt_does_not_accept_the_goal() {
    let (dir, store, run) = setup().await;
    let request = propose(&store, &run, "request", NOW + 2).await;
    assert_eq!(
        store.task("task").await.unwrap().state,
        State::WaitingApproval
    );
    assert!(
        store
            .claim_next(&Versions::fixture(), NOW + 3)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .prepare_approved_operation(&run.token, "request", NOW + 3)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "budget").await, 0);
    approve(&store, &request, "approve", NOW + 4).await;
    let original = store.operation_request("request").await.unwrap();
    approve(&store, &request, "approve-again", NOW + 5).await;
    assert_eq!(store.operation_request("request").await.unwrap(), original);
    assert_eq!(original.approved_by.as_deref(), Some("owner"));
    assert!(original.approval_event.is_some());
    store.close().await;
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert_eq!(store.operation_request("request").await.unwrap(), original);
    assert!(
        store
            .prepare_operation(&run.token, "fixture:other", json!({}), NOW + 6)
            .await
            .is_err()
    );
    let op = store
        .prepare_approved_operation(&run.token, "request", NOW + 6)
        .await
        .unwrap();
    assert_eq!(op.idempotency_key, op.id);
    assert_eq!(op.intent_digest, request.intent_digest);
    assert_eq!(op.target, request.proposal.target);
    assert_eq!(store.task("task").await.unwrap().tool_calls, 1);
    assert!(
        store
            .prepare_approved_operation(&run.token, "request", NOW + 7)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "operations").await, 1);
    let receipt = Receipt {
        operation_id: op.id.clone(),
        token: op.token.clone(),
        success: true,
        evidence: "fixture counter = 1".into(),
    };
    assert_eq!(
        store.record_receipt(&receipt).await.unwrap(),
        ReceiptEffect::Applied
    );
    assert_eq!(
        store.record_receipt(&receipt).await.unwrap(),
        ReceiptEffect::Duplicate
    );
    assert_eq!(store.task("task").await.unwrap().state, State::Planning);
    assert!(
        store
            .prepare_approved_operation(&run.token, "request", NOW + 8)
            .await
            .is_err()
    );
    let replay = store
        .propose_operation(&request.source_event, request.proposal.clone(), NOW + 9)
        .await
        .unwrap();
    assert_eq!(replay.state, ApprovalState::Dispatched);
    approve(&store, &request, "approve", NOW + 4).await; // cached event cannot reactivate the grant
    assert_eq!(
        store.operation_request("request").await.unwrap().state,
        ApprovalState::Dispatched
    );
    assert_eq!(store.task("task").await.unwrap().model_calls, 0);
}

#[tokio::test]
async fn only_exact_human_bindings_can_approve_and_batches_roll_back() {
    let (_dir, store, run) = setup().await;
    let request = propose(&store, &run, "request", NOW + 2).await;
    for (i, origin) in [Origin::Tool, Origin::External].into_iter().enumerate() {
        assert!(matches!(
            command(
                &store,
                origin,
                &format!("fake-{i}"),
                json!([decision(&request, true)]),
                NOW + 3
            )
            .await,
            Outcome::Rejected { .. }
        ));
    }
    for (i, (field, value)) in [
        ("task_id", json!("other")),
        ("expected_revision", json!("2")),
        ("request_id", json!("missing")),
        ("intent_digest", json!("wrong")),
        ("approved_by", json!("owner")),
    ]
    .into_iter()
    .enumerate()
    {
        let mut cmd = decision(&request, true);
        cmd["binding"][field] = value;
        assert!(matches!(
            command(
                &store,
                Origin::Human,
                &format!("bad-{i}"),
                json!([cmd]),
                NOW + 3
            )
            .await,
            Outcome::Rejected { .. }
        ));
    }
    let batch = json!([decision(&request,true),{"kind":"revise","id":"missing","expected_revision":"1","goal":"not present"}]);
    assert!(matches!(
        command(&store, Origin::Human, "bad-batch", batch, NOW + 3).await,
        Outcome::Rejected { .. }
    ));
    assert_eq!(store.operation_request("request").await.unwrap(), request);
    assert_eq!(
        store.task("task").await.unwrap().state,
        State::WaitingApproval
    );
    assert_eq!(count(&store, "operations").await, 0);
}

#[tokio::test]
async fn proposal_sources_and_canonical_identity_cannot_be_rebound() {
    let (_dir, store, run) = setup().await;
    let p = OperationProposal {
        id: "request".into(),
        token: run.token.clone(),
        target: "fixture:counter".into(),
        intent: json!({"delta":1.0}),
        expires_at: NOW + 60_000,
    };
    let human = source(&store, Origin::Human, "human-proposal", &p, NOW + 2).await;
    assert!(
        store
            .propose_operation(&human, p.clone(), NOW + 2)
            .await
            .is_err()
    );
    let event = source(&store, Origin::Tool, "tool-proposal", &p, NOW + 2).await;
    let mut wrong = p.clone();
    wrong.target = "fixture:other".into();
    assert!(
        store
            .propose_operation(&event, wrong.clone(), NOW + 2)
            .await
            .is_err()
    );
    let request = store
        .propose_operation(&event, p.clone(), NOW + 2)
        .await
        .unwrap();
    let other = source(&store, Origin::Tool, "other", &wrong, NOW + 3).await;
    assert!(
        store
            .propose_operation(&other, wrong, NOW + 3)
            .await
            .is_err()
    );
    let mut canonical = p;
    canonical.intent = json!({"delta":1});
    assert_eq!(
        store
            .propose_operation(&event, canonical, NOW + 3)
            .await
            .unwrap(),
        request
    );
    assert_eq!(count(&store, "operation_requests").await, 1);
}

#[tokio::test]
async fn rejection_and_withdrawal_never_reopen_an_approval() {
    for approved in [false, true] {
        let (_dir, store, run) = setup().await;
        let request = propose(&store, &run, "request", NOW + 2).await;
        if approved {
            approve(&store, &request, "approve", NOW + 3).await;
        }
        assert!(matches!(
            command(
                &store,
                Origin::Human,
                "reject",
                json!([decision(&request, false)]),
                NOW + 4
            )
            .await,
            Outcome::Applied { .. }
        ));
        assert_eq!(
            store.operation_request("request").await.unwrap().state,
            if approved {
                ApprovalState::Withdrawn
            } else {
                ApprovalState::Rejected
            }
        );
        assert!(matches!(
            command(
                &store,
                Origin::Human,
                "new-approval",
                json!([decision(&request, true)]),
                NOW + 5
            )
            .await,
            Outcome::Rejected { .. }
        ));
        assert!(
            store
                .prepare_approved_operation(&run.token, "request", NOW + 5)
                .await
                .is_err()
        );
        let replay = store
            .propose_operation(&request.source_event, request.proposal, NOW + 6)
            .await
            .unwrap();
        assert!(matches!(
            replay.state,
            ApprovalState::Rejected | ApprovalState::Withdrawn
        ));
        assert_eq!(store.task("task").await.unwrap().state, State::Blocked);
        assert_eq!(count(&store, "operations").await, 0);
    }
}

#[tokio::test]
async fn revision_and_cancel_invalidate_pending_and_approved_requests() {
    for approved in [false, true] {
        for cancel in [false, true] {
            let (_dir, store, run) = setup().await;
            let request = propose(&store, &run, "request", NOW + 2).await;
            if approved {
                approve(&store, &request, "approve", NOW + 3).await;
            }
            let action = if cancel {
                json!({"kind":"cancel","id":"task","expected_revision":"1"})
            } else {
                json!({"kind":"revise","id":"task","expected_revision":"1","goal":"new decision"})
            };
            assert!(matches!(
                command(&store, Origin::Human, "change", json!([action]), NOW + 4).await,
                Outcome::Applied { .. }
            ));
            assert_eq!(
                store.operation_request("request").await.unwrap().state,
                ApprovalState::Invalidated
            );
            assert!(
                store
                    .prepare_approved_operation(&run.token, "request", NOW + 5)
                    .await
                    .is_err()
            );
            assert!(matches!(
                command(
                    &store,
                    Origin::Human,
                    "old-approval",
                    json!([decision(&request, true)]),
                    NOW + 5
                )
                .await,
                Outcome::Rejected { .. }
            ));
            if !cancel {
                let next = store
                    .claim_next(&Versions::fixture(), NOW + 5)
                    .await
                    .unwrap()
                    .unwrap();
                let fresh = propose(&store, &next, "fresh-request", NOW + 6).await;
                assert_eq!(fresh.proposal.token.revision, 2);
            }
            assert_eq!(count(&store, "operations").await, 0);
        }
    }
}

#[tokio::test]
async fn expiry_blocks_without_model_work_and_notifies_only_once() {
    for approved in [false, true] {
        let (_dir, store, run) = setup().await;
        let request = propose(&store, &run, "request", NOW + 2).await;
        if approved {
            approve(&store, &request, "approve", NOW + 3).await;
        }
        assert!(matches!(
            command(
                &store,
                Origin::Human,
                "late-approval",
                json!([decision(&request, true)]),
                NOW + 60_000
            )
            .await,
            Outcome::Rejected { .. }
        ));
        assert!(
            store
                .prepare_approved_operation(&run.token, "request", NOW + 60_000)
                .await
                .is_err()
        );
        let (next, changed) = store
            .claim_next_with_changes(&Versions::fixture(), NOW + 60_000)
            .await
            .unwrap();
        assert!(next.is_none() && changed);
        assert_eq!(
            store.operation_request("request").await.unwrap().state,
            ApprovalState::Expired
        );
        let task = store.task("task").await.unwrap();
        assert_eq!(task.state, State::Blocked);
        assert_eq!(task.reason.as_deref(), Some("operation_approval_expired"));
        assert_eq!(task.tool_calls, 0);
        assert_eq!(task.model_calls, 0);
        assert!(
            !store
                .claim_next_with_changes(&Versions::fixture(), NOW + 60_001)
                .await
                .unwrap()
                .1
        );
        assert_eq!(count(&store, "budget").await, 0);
    }
}

#[tokio::test]
async fn restart_requires_fresh_human_review_and_never_migrates_a_grant() {
    for approved in [false, true] {
        let (dir, store, run) = setup().await;
        let request = propose(&store, &run, "request", NOW + 2).await;
        if approved {
            approve(&store, &request, "approve", NOW + 3).await;
        }
        store.close().await;
        let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
        store.recover(&Versions::fixture(), NOW + 4).await.unwrap();
        assert_eq!(
            store.operation_request("request").await.unwrap().state,
            ApprovalState::Invalidated
        );
        assert_eq!(
            store.task("task").await.unwrap().reason.as_deref(),
            Some("approval_restart_review_required")
        );
        assert!(
            store
                .prepare_approved_operation(&run.token, "request", NOW + 5)
                .await
                .is_err()
        );
        assert!(
            store
                .claim_next(&Versions::fixture(), NOW + 5)
                .await
                .unwrap()
                .is_none()
        );
        if approved {
            approve(&store, &request, "approve", NOW + 3).await;
        }
        assert_eq!(
            store.operation_request("request").await.unwrap().state,
            ApprovalState::Invalidated
        );
        command(
            &store,
            Origin::Human,
            "resume",
            json!([{"kind":"resume","id":"task","expected_revision":"1"}]),
            NOW + 6,
        )
        .await;
        let next = store
            .claim_next(&Versions::fixture(), NOW + 7)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(next.token.epoch, run.token.epoch);
        assert!(
            store
                .prepare_approved_operation(&next.token, "request", NOW + 7)
                .await
                .is_err()
        );
        propose(&store, &next, "new-request", NOW + 8).await;
    }
}

#[tokio::test]
async fn dispatched_effects_stay_unknown_on_cancel_until_the_original_receipt_arrives() {
    let (_dir, store, run) = setup().await;
    let request = propose(&store, &run, "request", NOW + 2).await;
    approve(&store, &request, "approve", NOW + 3).await;
    let op = store
        .prepare_approved_operation(&run.token, "request", NOW + 4)
        .await
        .unwrap();
    assert!(matches!(
        command(
            &store,
            Origin::Human,
            "reject-too-late",
            json!([decision(&request, false)]),
            NOW + 5
        )
        .await,
        Outcome::Rejected { .. }
    ));
    assert_eq!(store.pending_operations("task").await.unwrap().len(), 1);
    command(
        &store,
        Origin::Human,
        "cancel",
        json!([{"kind":"cancel","id":"task","expected_revision":"1"}]),
        NOW + 6,
    )
    .await;
    assert_eq!(
        store.task("task").await.unwrap().state,
        State::OutcomeUnknown
    );
    let receipt = Receipt {
        operation_id: op.id,
        token: op.token,
        success: true,
        evidence: "counted once".into(),
    };
    assert_eq!(
        store.record_receipt(&receipt).await.unwrap(),
        ReceiptEffect::Late
    );
    assert_eq!(store.task("task").await.unwrap().state, State::Cancelled);
    assert_eq!(store.task("task").await.unwrap().tool_calls, 1);
}

async fn full_outbox(store: &Store) {
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000) INSERT INTO outbox(dedupe_key,payload) SELECT 'full:'||i,'{}' FROM n").execute(&store.application_pool()).await.unwrap();
}

#[tokio::test]
async fn dispatch_requires_both_original_sources_not_just_saved_approval_metadata() {
    for remove_approval in [false, true] {
        let (_dir, store, run) = setup().await;
        let request = propose(&store, &run, "request", NOW + 2).await;
        approve(&store, &request, "approve", NOW + 3).await;
        let approved = store.operation_request("request").await.unwrap();
        let id = if remove_approval {
            approved.approval_event.unwrap()
        } else {
            request.source_event
        };
        sqlx::query("UPDATE events SET payload='{\"commands\":[]}' WHERE id=?")
            .bind(id)
            .execute(&store.application_pool())
            .await
            .unwrap();
        assert!(
            store
                .prepare_approved_operation(&run.token, "request", NOW + 4)
                .await
                .is_err()
        );
        assert_eq!(count(&store, "operations").await, 0);
        assert_eq!(store.task("task").await.unwrap().tool_calls, 0);
    }
}

#[tokio::test]
async fn each_expired_proposal_has_its_own_durable_notification() {
    let (_dir, store, run) = setup().await;
    propose(&store, &run, "request", NOW + 2).await;
    store
        .claim_next(&Versions::fixture(), NOW + 60_000)
        .await
        .unwrap();
    command(
        &store,
        Origin::Human,
        "resume",
        json!([{"kind":"resume","id":"task","expected_revision":"1"}]),
        NOW + 60_001,
    )
    .await;
    let next = store
        .claim_next(&Versions::fixture(), NOW + 60_002)
        .await
        .unwrap()
        .unwrap();
    let p = OperationProposal {
        id: "second".into(),
        token: next.token,
        target: "fixture:counter".into(),
        intent: json!({}),
        expires_at: NOW + 120_000,
    };
    let event = source(&store, Origin::Tool, "second", &p, NOW + 60_003).await;
    store
        .propose_operation(&event, p, NOW + 60_003)
        .await
        .unwrap();
    store
        .claim_next(&Versions::fixture(), NOW + 120_000)
        .await
        .unwrap();
    let notifications = store.outbox(100).await.unwrap();
    let keys: Vec<_> = notifications
        .iter()
        .filter(|item| item.key.starts_with("operation-expired:"))
        .map(|item| item.key.as_str())
        .collect();
    assert_eq!(
        keys,
        vec!["operation-expired:request", "operation-expired:second"]
    );
}

#[tokio::test]
async fn new_run_epochs_invalidate_unconsumed_grants_before_accepting_new_proposals() {
    let (_dir, store, run) = setup().await;
    let request = propose(&store, &run, "request", NOW + 2).await;
    approve(&store, &request, "approve", NOW + 3).await;
    // A queued recovery fixture must not carry an older grant into its next epoch.
    sqlx::query("UPDATE tasks SET data=json_set(data,'$.state','queued') WHERE id='task'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    let next = store
        .claim_next(&Versions::fixture(), NOW + 4)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(next.token.epoch, run.token.epoch);
    assert_eq!(
        store.operation_request("request").await.unwrap().state,
        ApprovalState::Invalidated
    );
    assert!(
        store
            .prepare_approved_operation(&next.token, "request", NOW + 5)
            .await
            .is_err()
    );
    propose(&store, &next, "fresh-request", NOW + 6).await;
}

#[tokio::test]
async fn device_observations_that_clear_a_run_also_remove_its_unconsumed_grant() {
    for approved in [false, true] {
        let (_dir, store, run) = setup().await;
        let request = propose(&store, &run, "request", NOW + 2).await;
        if approved {
            approve(&store, &request, "approve", NOW + 3).await;
        }
        store
            .device_observation("task", 1, "fixture-read-result", false)
            .await
            .unwrap();
        assert!(store.task("task").await.unwrap().active_run.is_none());
        assert_eq!(
            store.operation_request("request").await.unwrap().state,
            ApprovalState::Invalidated
        );
        assert!(
            store
                .prepare_approved_operation(&run.token, "request", NOW + 4)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn only_one_proposal_can_wait_and_the_last_tool_slot_is_not_exceeded() {
    let (_dir, store, run) = setup().await;
    sqlx::query("UPDATE tasks SET data=json_set(data,'$.tool_calls',15) WHERE id='task'")
        .execute(&store.application_pool())
        .await
        .unwrap();
    let request = propose(&store, &run, "request", NOW + 2).await;
    let mut other = request.proposal.clone();
    other.id = "other-request".into();
    let event = source(&store, Origin::Tool, "other-request", &other, NOW + 3).await;
    assert!(
        store
            .propose_operation(&event, other.clone(), NOW + 3)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "operation_requests").await, 1);
    approve(&store, &request, "approve", NOW + 4).await;
    let op = store
        .prepare_approved_operation(&run.token, "request", NOW + 5)
        .await
        .unwrap();
    assert_eq!(store.task("task").await.unwrap().tool_calls, 16);
    store
        .record_receipt(&Receipt {
            operation_id: op.id,
            token: op.token,
            success: true,
            evidence: "final fixture tool slot".into(),
        })
        .await
        .unwrap();
    assert!(
        store
            .propose_operation(&event, other, NOW + 6)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "operations").await, 1);
}

#[tokio::test]
async fn outbox_backpressure_rolls_back_both_proposals_and_dispatch() {
    let (_dir, store, run) = setup().await;
    let proposal = OperationProposal {
        id: "request".into(),
        token: run.token.clone(),
        target: "fixture:counter".into(),
        intent: json!({}),
        expires_at: NOW + 60_000,
    };
    let event = source(&store, Origin::Tool, "request", &proposal, NOW + 2).await;
    full_outbox(&store).await;
    assert!(
        store
            .propose_operation(&event, proposal.clone(), NOW + 2)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "operation_requests").await, 0);
    assert_eq!(store.task("task").await.unwrap().state, State::Planning);
    sqlx::query("UPDATE outbox SET acknowledged=1")
        .execute(&store.application_pool())
        .await
        .unwrap();
    let request = store
        .propose_operation(&event, proposal, NOW + 2)
        .await
        .unwrap();
    approve(&store, &request, "approve", NOW + 3).await;
    sqlx::query("UPDATE outbox SET acknowledged=0")
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(
        store
            .prepare_approved_operation(&run.token, "request", NOW + 4)
            .await
            .is_err()
    );
    assert_eq!(
        store.operation_request("request").await.unwrap().state,
        ApprovalState::Approved
    );
    assert_eq!(store.task("task").await.unwrap().state, State::Ready);
    assert_eq!(store.task("task").await.unwrap().tool_calls, 0);
    assert_eq!(count(&store, "operations").await, 0);
}

#[tokio::test]
async fn schema_three_fixture_upgrades_without_losing_existing_task_data() {
    let (dir, store, _run) = setup().await;
    let before = serde_json::to_value(store.task("task").await.unwrap()).unwrap();
    store.close().await;
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(dir.path().join("db")),
    )
    .await
    .unwrap();
    // Reconstruct the previous empty approval schema in this disposable fixture only.
    db.execute("DROP TABLE link_worker_cleanup; ALTER TABLE link_device DROP COLUMN worker_cleanup; DROP TABLE link_worker_fences; DROP TABLE link_erased_work; ALTER TABLE link_device DROP COLUMN worker_erasure; DROP TABLE memory_erased_reads; DROP TABLE memory_derived_messages; DROP TABLE link_erased_frames; ALTER TABLE link_device DROP COLUMN projection_erasure; DROP TABLE task_memory_sources; DROP TABLE local_schedule_delivery; ALTER TABLE link_device DROP COLUMN schedule_notices; DROP TABLE schedules; DROP TABLE local_schedule_notices; DROP TABLE local_schedule_inputs; DROP TABLE link_pair_claims; DROP TABLE link_pair_tokens; DROP INDEX link_credential_digest; DROP TABLE local_memory_access; DROP TABLE local_memory_queue; DROP TABLE local_memory_cache; DROP TABLE memories; DROP TABLE memory_events; DELETE FROM settings WHERE key LIKE 'memory_%'; DROP TABLE link_artifact_parts; DROP TABLE link_artifacts; DROP TABLE operation_requests; DELETE FROM _sqlx_migrations WHERE version IN (4,5,6,7,8,9,10,11,12,13,14,15,16,17,18); PRAGMA user_version=3;").await.unwrap();
    db.close().await.unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    let schema: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(schema, 18);
    assert_eq!(
        serde_json::to_value(store.task("task").await.unwrap()).unwrap(),
        before
    );
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(integrity, "ok");
}
