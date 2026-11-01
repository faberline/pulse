//! Controlled source/approval transactions. No native capture or effects run here.
use pulse_core::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const NOW: i64 = 1_800_000_000_000;
async fn event(
    store: &Store,
    origin: Origin,
    source: &str,
    key: &str,
    payload: Value,
    now: i64,
) -> String {
    let event = store
        .ingest(origin, source, key, payload, now)
        .await
        .unwrap();
    assert!(matches!(
        store.route(&event.event_id).await.unwrap(),
        Outcome::Applied { .. }
    ));
    event.event_id
}
async fn source(store: &Store, proposal: &OperationProposal, key: &str) -> String {
    event(
        store,
        Origin::Tool,
        "planner",
        key,
        json!({"commands":[{"kind":"query","ids":["task"]}],"operation_proposal":proposal}),
        NOW + 3,
    )
    .await
}
async fn fixture() -> (tempfile::TempDir, Store, OperationProposal, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    let selected = ComputerSelection {
        target: ComputerTarget {
            bundle_id: "com.faberline.PulseComputerFixture".into(),
            pid: 42,
            window_id: 7,
        },
        process_launched_at: 1,
    };
    event(&store, Origin::Human, "ui", "create", json!({"commands":[{"kind":"create","id":"task","goal":"Operate the selected owned fixture","scope":selected.scope().unwrap(),"priority":0,"requires_device":false}]}), NOW).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    let hex = "89504e470d0a1a0a0000000d4948445200000001000000010804000000b51c0c020000000b4944415478da63fcff1f0003030200efa345990000000049454e44ae426082";
    let png: Vec<u8> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    let device = uuid::Uuid::new_v4().to_string();
    let observation = ComputerObservation {
        binding: ComputerBinding {
            operation_id: "observed".into(),
            task_id: "task".into(),
            revision: 1,
            device: device.clone(),
            epoch: 1,
        },
        target: selected.target.clone(),
        process_launched_at: 1,
        title: "Owned fixture".into(),
        frame: ComputerRect {
            x: 10.,
            y: 20.,
            width: 400.,
            height: 300.,
        },
        elements: vec![
            ComputerElement {
                id: "window.0".into(),
                role: "AXButton".into(),
                label: "Count once".into(),
                value: None,
                can_press: true,
                can_set_text: false,
            },
            ComputerElement {
                id: "window.1".into(),
                role: "AXTextField".into(),
                label: "Message".into(),
                value: Some("before".into()),
                can_press: false,
                can_set_text: true,
            },
        ],
        truncated: false,
        nonce: uuid::Uuid::new_v4().to_string(),
        observed_at: NOW,
        expires_at: NOW + 120_000,
        image: ComputerImageRef {
            digest: format!("{:x}", Sha256::digest(&png)),
            length: png.len() as u32,
            width: 1,
            height: 1,
        },
    };
    let mut result = ReadOutcome::text("Owned fixture source".into(), false);
    if let ReadOutcome::Completed { computer, .. } = &mut result {
        *computer = Some(Box::new(observation.clone()));
    }
    let receipt = event(
        &store,
        Origin::Tool,
        &format!("device-result:{device}"),
        "observed",
        json!({"commands":[{"kind":"query","ids":["task"]}],"result":result}),
        NOW + 2,
    )
    .await;
    sqlx::query("INSERT INTO link_reads VALUES('observed',?,1,'task','1',?,'completed',?)")
        .bind(&device)
        .bind(json!({"kind":"computer_inspect","selection":selected}).to_string())
        .bind(serde_json::to_string(&result).unwrap())
        .execute(&store.application_pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO link_artifacts VALUES('observed',?,?)")
        .bind(json!({"task_id":"task","revision":"1","computer":{"binding":observation.binding,"selection":selected},"title":observation.title,"observed_at":NOW,"digest":observation.image.digest,"length":png.len(),"width":1,"height":1}).to_string())
        .bind(png).execute(&store.application_pool()).await.unwrap();
    let intent = ComputerIntent {
        observation,
        element_id: "window.0".into(),
        effect: ComputerEffect::Press,
    };
    let proposal = OperationProposal {
        id: "request".into(),
        token: run.token,
        target: intent.target().unwrap(),
        intent: serde_json::to_value(intent).unwrap(),
        expires_at: NOW + 60_000,
    };
    let source = source(&store, &proposal, "proposal").await;
    (dir, store, proposal, source, receipt)
}
async fn decide(store: &Store, request: &OperationRequest, approve: bool, key: &str) {
    let binding = OperationBinding {
        task_id: "task".into(),
        expected_revision: 1,
        request_id: request.proposal.id.clone(),
        intent_digest: request.intent_digest.clone(),
    };
    let command = if approve {
        Command::ApproveOperation { binding }
    } else {
        Command::RejectOperation { binding }
    };
    event(
        store,
        Origin::Human,
        "ui",
        key,
        json!({"commands":[command]}),
        NOW + 5,
    )
    .await;
}
#[tokio::test]
async fn native_approval_survives_reopen_and_consumes_only_with_atomic_delivery() {
    let (dir, store, proposal, source, _) = fixture().await;
    let token = proposal.token.clone();
    let request = store
        .propose_operation(&source, proposal, NOW + 4)
        .await
        .unwrap();
    assert_eq!(request.state, ApprovalState::Pending);
    assert!(
        store
            .prepare_approved_operation(&token, "request", NOW + 4)
            .await
            .is_err()
    );
    assert_eq!(store.task("task").await.unwrap().tool_calls, 0);
    drop(store);
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    assert_eq!(
        store.operation_request("request").await.unwrap().state,
        ApprovalState::Pending
    );
    decide(&store, &request, true, "approve").await;
    let pool = store.application_pool();
    sqlx::raw_sql("CREATE TRIGGER reject_native_delivery BEFORE INSERT ON link_outbox BEGIN SELECT RAISE(ABORT,'controlled delivery fault'); END;").execute(&pool).await.unwrap();
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let operation = Store::prepare_approved_operation_in(&mut tx, &token, "request", NOW + 6)
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO link_outbox VALUES('local',1,?,'{}',0)")
            .bind(&operation.id)
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        store.operation_request("request").await.unwrap().state,
        ApprovalState::Approved
    );
    assert_eq!(store.task("task").await.unwrap().tool_calls, 0);
    sqlx::raw_sql("DROP TRIGGER reject_native_delivery")
        .execute(&pool)
        .await
        .unwrap();
    let operation = store
        .prepare_approved_operation(&token, "request", NOW + 7)
        .await
        .unwrap();
    assert_eq!(operation.state, OperationState::Unknown);
    assert_eq!(operation.target, request.proposal.target);
    assert_eq!(store.task("task").await.unwrap().tool_calls, 1);
    drop(store);
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    store.recover(&Versions::fixture(), NOW + 8).await.unwrap();
    assert_eq!(
        store.task("task").await.unwrap().state,
        State::OutcomeUnknown
    );
    assert!(
        store
            .prepare_approved_operation(&token, "request", NOW + 9)
            .await
            .is_err()
    );
    assert_eq!(store.task("task").await.unwrap().tool_calls, 1);
}
#[tokio::test]
async fn native_proposal_requires_the_exact_original_image_scope_and_linked_tool_receipt() {
    for attack in [
        "nonce",
        "target",
        "epoch",
        "task",
        "revision",
        "launch",
        "control",
        "secure",
        "effect",
        "source_kind",
        "source_scope",
        "image",
        "image_length",
        "image_source",
        "browser_source",
        "receipt_source",
        "receipt_payload",
    ] {
        let (_dir, store, mut proposal, _source, receipt) = fixture().await;
        let pool = store.application_pool();
        match attack {
            "nonce" => {
                proposal.intent["observation"]["nonce"] = json!(uuid::Uuid::new_v4().to_string())
            }
            "target" => proposal.target = "computer:com.faberline.Other:42:7:1".into(),
            "epoch" => proposal.intent["observation"]["binding"]["epoch"] = json!("2"),
            "task" => proposal.intent["observation"]["binding"]["task_id"] = json!("other"),
            "revision" => proposal.intent["observation"]["binding"]["revision"] = json!("2"),
            "launch" => proposal.intent["observation"]["process_launched_at"] = json!(2),
            "control" => proposal.intent["element_id"] = json!("window.2"),
            "secure" => {
                proposal.intent["observation"]["elements"][0]["role"] = json!("AXSecureTextField")
            }
            "effect" => {
                proposal.intent["effect"] =
                    json!({"kind":"set_text","text":"unapproved replacement"})
            }
            "source_kind" => {
                sqlx::query(
                    "UPDATE link_reads SET request=json_set(request,'$.kind','browser_inspect')",
                )
                .execute(&pool)
                .await
                .unwrap();
            }
            "source_scope" => {
                sqlx::query("UPDATE link_reads SET request=json_set(request,'$.selection.target.window_id',8)").execute(&pool).await.unwrap();
            }
            "image" => {
                sqlx::query("UPDATE link_artifacts SET png=X'00'")
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "image_length" => {
                sqlx::query("UPDATE link_artifacts SET metadata=json_set(metadata,'$.length',1)")
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "image_source" => {
                sqlx::query("UPDATE link_artifacts SET metadata=json_set(metadata,'$.computer.binding.epoch','2')").execute(&pool).await.unwrap();
            }
            "browser_source" => {
                sqlx::query("UPDATE link_artifacts SET metadata=json_set(metadata,'$.url','https://example.com/')").execute(&pool).await.unwrap();
            }
            "receipt_source" => {
                sqlx::query("DELETE FROM event_tasks WHERE event_id=?")
                    .bind(receipt)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "receipt_payload" => {
                sqlx::query("UPDATE events SET payload=json_set(payload,'$.result.text','forged') WHERE id=?").bind(receipt).execute(&pool).await.unwrap();
            }
            _ => unreachable!(),
        }
        let source = source(&store, &proposal, "changed").await;
        assert!(
            store
                .propose_operation(&source, proposal, NOW + 4)
                .await
                .is_err(),
            "{attack}"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operation_requests")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "{attack}");
        assert_eq!(store.task("task").await.unwrap().tool_calls, 0, "{attack}");
    }
}
#[tokio::test]
async fn native_approval_rechecks_evidence_and_honors_rejection_withdrawal_cancellation_and_revision()
 {
    for change in [
        "receipt", "image", "reject", "withdraw", "cancel", "revision", "expire",
    ] {
        let (_dir, store, proposal, source, receipt) = fixture().await;
        let token = proposal.token.clone();
        let request = store
            .propose_operation(&source, proposal, NOW + 4)
            .await
            .unwrap();
        if change != "reject" {
            decide(&store, &request, true, "approve").await;
        }
        match change {
            "receipt" => {
                sqlx::query("DELETE FROM event_tasks WHERE event_id=?")
                    .bind(receipt)
                    .execute(&store.application_pool())
                    .await
                    .unwrap();
            }
            "image" => {
                sqlx::query("UPDATE link_artifacts SET png=NULL")
                    .execute(&store.application_pool())
                    .await
                    .unwrap();
            }
            "reject" | "withdraw" => decide(&store, &request, false, "reject").await,
            "cancel" => {
                event(
                    &store,
                    Origin::Human,
                    "ui",
                    "cancel",
                    json!({"commands":[{"kind":"cancel","id":"task","expected_revision":"1"}]}),
                    NOW + 6,
                )
                .await;
            }
            "revision" => {
                event(&store,Origin::Human,"ui","revise",json!({"commands":[{"kind":"revise","id":"task","expected_revision":"1","goal":"Changed task"}]}),NOW+6).await;
            }
            "expire" => {}
            _ => unreachable!(),
        }
        let time = if change == "expire" {
            NOW + 60_000
        } else {
            NOW + 7
        };
        assert!(
            store
                .prepare_approved_operation(&token, "request", time)
                .await
                .is_err(),
            "{change}"
        );
        assert_eq!(store.task("task").await.unwrap().tool_calls, 0, "{change}");
    }
}
#[test]
fn native_text_targets_accept_plain_unicode_and_never_keys_or_secure_fields() {
    let observation:ComputerObservation=serde_json::from_value(json!({
        "binding":{"operation_id":"read","task_id":"task","revision":"1","device":uuid::Uuid::new_v4().to_string(),"epoch":"1"},
        "target":{"bundle_id":"com.faberline.PulseComputerFixture","pid":42,"window_id":7},"process_launched_at":1,
        "title":"Fixture","frame":{"x":0.,"y":0.,"width":400.,"height":300.},
        "elements":[{"id":"window.0","role":"AXTextField","label":"Message","value":"","can_press":false,"can_set_text":true}],
        "truncated":false,"nonce":uuid::Uuid::new_v4().to_string(),"observed_at":NOW,"expires_at":NOW+120_000,
        "image":{"digest":"a".repeat(64),"length":100,"width":1,"height":1}
    })).unwrap();
    let mut intent = ComputerIntent {
        observation,
        element_id: "window.0".into(),
        effect: ComputerEffect::SetText {
            text: "中文文字".into(),
        },
    };
    let scope = intent.target().unwrap();
    assert!(intent.valid(&scope, NOW));
    for text in [
        "send\n",
        "\r",
        "\t",
        "\u{e007}",
        "\u{2028}",
        &"a".repeat(1025),
    ] {
        intent.effect = ComputerEffect::SetText { text: text.into() };
        assert!(!intent.valid(&scope, NOW));
    }
    intent.effect = ComputerEffect::Press;
    assert!(!intent.valid(&scope, NOW));
    intent.effect = ComputerEffect::SetText {
        text: "plain".into(),
    };
    intent.observation.elements[0].role = "AXSecureTextField".into();
    assert!(!intent.valid(&scope, NOW));
    let mut wire = serde_json::to_value(intent).unwrap();
    wire["effect"] = json!({"kind":"script","script":"forged"});
    assert!(serde_json::from_value::<ComputerIntent>(wire).is_err());
}
