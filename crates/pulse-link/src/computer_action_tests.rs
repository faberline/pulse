//! Mock native children verify dispatch boundaries. They are not actual AX effects.
use super::*;
use crate::computer_runtime::tests::{assert_reaped, fixture, publish_helper};
use pulse_core::{ComputerEffect, ComputerPermit, OperationProposal, Token};
use serde_json::json;

async fn prepared(
    mode: &str,
) -> (
    tempfile::TempDir,
    ComputerRuntime,
    WorkBinding,
    ComputerGrant,
) {
    let (root, runtime) = fixture(mode);
    let target = runtime.configured_target().unwrap().clone();
    let source = ComputerBinding {
        operation_id: "source-read".into(),
        task_id: "task".into(),
        revision: 1,
        device: uuid::Uuid::new_v4().to_string(),
        epoch: 2,
    };
    let selection = pulse_core::ComputerSelection {
        target,
        process_launched_at: 1,
    };
    let selected_at = crate::now();
    runtime
        .permit_local(ComputerPermit {
            task_id: "task".into(),
            revision: "1".into(),
            selection: selection.clone(),
            selected_at,
            expires_at: selected_at + 900_000,
        })
        .unwrap();
    let evidence = runtime.observe_selected(&source, &selection).await.unwrap();
    let intent = ComputerIntent {
        observation: evidence.observation,
        element_id: "window.0".into(),
        effect: ComputerEffect::Press,
    };
    let token = Token {
        run_id: "run".into(),
        task_id: "task".into(),
        revision: 1,
        epoch: 1,
    };
    let proposal = OperationProposal {
        id: "request".into(),
        token: token.clone(),
        target: intent.target().unwrap(),
        intent: serde_json::to_value(intent).unwrap(),
        expires_at: crate::now() + 60_000,
    };
    let digest = hash(&json!({"source_event":"proposal-source","proposal":proposal})).unwrap();
    let operation = Operation {
        id: "approved:request".into(),
        idempotency_key: "approved:request".into(),
        token,
        target: proposal.target.clone(),
        intent: proposal.intent.clone(),
        intent_digest: digest.clone(),
        state: OperationState::Unknown,
    };
    let request = OperationRequest {
        proposal,
        source_event: "proposal-source".into(),
        intent_digest: digest,
        state: ApprovalState::Dispatched,
        approval_event: Some("human-approval".into()),
        approved_by: Some("owner".into()),
        decision_event: Some("human-approval".into()),
        reason: None,
        operation_id: Some(operation.id.clone()),
    };
    let binding = WorkBinding {
        operation_id: operation.id.clone(),
        task_id: "task".into(),
        revision: "1".into(),
        device: source.device,
        epoch: 2,
    };
    (root, runtime, binding, ComputerGrant { operation, request })
}
#[tokio::test]
async fn fixed_native_action_checks_grant_original_view_and_uses_one_child_only() {
    let (_root, runtime, binding, grant) = prepared("action-good").await;
    let result = runtime.act(&binding, &grant).await;
    assert!(
        matches!(&result.outcome,ReadOutcome::Completed{text,computer:Some(o),..} if text.contains("One approved native command completed") && !text.contains("No effect was executed") && o.binding.operation_id==binding.operation_id)
    );
    assert!(result.image.is_some());
    let image = result.image.unwrap();
    image.artifact.verify(&image.png).unwrap();
    assert_eq!(
        std::fs::read_to_string(runtime.helper.with_extension("effects")).unwrap(),
        "1"
    );
    assert!(matches!(
        runtime.act(&binding, &grant).await.outcome,
        ReadOutcome::Rejected { .. }
    ));
    assert_eq!(
        std::fs::read_to_string(runtime.helper.with_extension("effects")).unwrap(),
        "1"
    );
    assert_reaped(&runtime).await;
}
#[tokio::test]
async fn missing_human_binding_altered_local_view_and_expired_grants_stop_before_spawn() {
    for attack in [
        "pending", "approval", "owner", "digest", "intent", "epoch", "binding", "original",
        "image", "stopped", "revoked", "expired", "restart",
    ] {
        let (_root, runtime, mut binding, mut grant) = prepared("action-good").await;
        match attack {
            "pending" => grant.request.state = ApprovalState::Approved,
            "approval" => grant.request.approval_event = None,
            "owner" => grant.request.approved_by = None,
            "digest" => grant.operation.intent_digest = "0".repeat(64),
            "intent" => grant.operation.intent["element_id"] = json!("window.7"),
            "epoch" => binding.epoch += 1,
            "binding" => binding.task_id = "other".into(),
            "original" => runtime.observations.lock().unwrap()[0].title = "changed".into(),
            "image" => {
                let intent: ComputerIntent =
                    serde_json::from_value(grant.operation.intent.clone()).unwrap();
                std::fs::write(
                    runtime
                        .directory
                        .join("artifacts")
                        .join(format!("{}.png", intent.observation.image.digest)),
                    b"corrupt",
                )
                .unwrap();
            }
            "stopped" => runtime.set_live(false),
            "revoked" => runtime.permits.lock().unwrap().clear(),
            "expired" => grant.request.proposal.expires_at = crate::now() - 1,
            "restart" => {
                let restarted = ComputerRuntime::new(
                    runtime.helper.clone(),
                    runtime.directory.clone(),
                    runtime.target.clone(),
                )
                .unwrap();
                restarted.set_live(true);
                let permit = runtime.selections().pop().unwrap();
                restarted.permit_local(permit).unwrap();
                assert!(matches!(
                    restarted.act(&binding, &grant).await.outcome,
                    ReadOutcome::Rejected { .. }
                ));
            }
            _ => unreachable!(),
        }
        if attack != "restart" {
            assert!(
                matches!(
                    runtime.act(&binding, &grant).await.outcome,
                    ReadOutcome::Rejected { .. }
                ),
                "{attack}"
            );
        }
        assert!(
            !runtime.helper.with_extension("effects").exists(),
            "{attack}"
        );
    }
}
#[tokio::test]
async fn native_rejection_is_distinct_from_unknown_and_neither_replays() {
    for mode in [
        "action-rejected",
        "action-unknown",
        "action-binding",
        "action-missing",
        "action-bad-image",
    ] {
        // Corrupt-image mode changes only the action result, not its valid source read.
        let (_root, runtime, binding, grant) = prepared(if mode == "action-bad-image" {
            "action-good"
        } else {
            mode
        })
        .await;
        if mode == "action-bad-image" {
            let code = std::fs::read_to_string(&runtime.helper).unwrap();
            publish_helper(
                &runtime.helper,
                &code.replace("mode=script.stem", "mode='action-bad-image'"),
            );
        }
        let result = runtime.act(&binding, &grant).await;
        if mode == "action-rejected" {
            assert!(matches!(result.outcome, ReadOutcome::Rejected { .. }));
            assert!(!runtime.helper.with_extension("effects").exists());
        } else {
            assert!(
                matches!(result.outcome, ReadOutcome::Unknown { .. }),
                "{mode}"
            );
        }
        assert!(result.image.is_none());
        assert!(matches!(
            runtime.act(&binding, &grant).await.outcome,
            ReadOutcome::Rejected { .. }
        ));
    }
}
#[tokio::test]
async fn interrupted_native_action_is_unknown_and_the_owned_child_is_reaped() {
    let (_root, runtime, binding, grant) = prepared("action-hang").await;
    let worker = runtime.clone();
    let b = binding.clone();
    let g = grant.clone();
    let job = tokio::spawn(async move { worker.act(&b, &g).await });
    let start = std::time::Instant::now();
    while !runtime.helper.with_extension("effects").exists() {
        assert!(start.elapsed() < Duration::from_secs(3));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    runtime.set_live(false);
    let result = job.await.unwrap();
    assert!(matches!(result.outcome, ReadOutcome::Unknown { .. }));
    assert_reaped(&runtime).await;
    runtime.set_live(true);
    assert!(matches!(
        runtime.act(&binding, &grant).await.outcome,
        ReadOutcome::Rejected { .. }
    ));
    assert_eq!(
        std::fs::read_to_string(runtime.helper.with_extension("effects")).unwrap(),
        "1"
    );
}

#[tokio::test]
async fn fixed_native_text_carries_only_approved_plain_text_and_retains_the_new_source_image() {
    let (_root, runtime, binding, mut grant) = prepared("action-good").await;
    let mut intent: ComputerIntent =
        serde_json::from_value(grant.operation.intent.clone()).unwrap();
    intent.element_id = "window.1".into();
    intent.effect = ComputerEffect::SetText {
        text: "中文 test".into(),
    };
    let wire = serde_json::to_value(intent).unwrap();
    grant.operation.intent = wire.clone();
    grant.request.proposal.intent = wire;
    let digest =
        hash(&json!({"source_event":grant.request.source_event,"proposal":grant.request.proposal}))
            .unwrap();
    grant.operation.intent_digest = digest.clone();
    grant.request.intent_digest = digest;
    let result = runtime.act(&binding, &grant).await;
    assert!(
        matches!(&result.outcome,ReadOutcome::Completed{computer:Some(o),..} if o.elements.iter().any(|e|e.id=="window.1" && e.value.as_deref()==Some("中文 test")))
    );
    assert!(result.image.is_some());
    assert_eq!(
        std::fs::read_to_string(runtime.helper.with_extension("effects")).unwrap(),
        "1"
    );
    assert!(matches!(
        runtime.act(&binding, &grant).await.outcome,
        ReadOutcome::Rejected { .. }
    ));
}

#[tokio::test]
async fn capability_probe_gate_is_released_before_the_single_native_command_starts() {
    let (_root, runtime, binding, grant) = prepared("action-good").await;
    let busy = runtime.gate.clone().acquire_owned().await.unwrap();
    let worker = runtime.clone();
    let task = tokio::spawn(async move { worker.act(&binding, &grant).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!runtime.helper.with_extension("effects").exists());
    drop(busy);
    let result = task.await.unwrap();
    assert!(matches!(result.outcome, ReadOutcome::Completed { .. }));
    assert_eq!(
        std::fs::read_to_string(runtime.helper.with_extension("effects")).unwrap(),
        "1"
    );
    assert_reaped(&runtime).await;
}
