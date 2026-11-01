use pulse_core::*;
use serde_json::json;
use sha2::{Digest, Sha256};

const NOW: i64 = 1_800_000_000_000;
async fn event(
    store: &Store,
    origin: Origin,
    source: &str,
    key: &str,
    payload: serde_json::Value,
) -> String {
    let time = if key == "create" {
        NOW
    } else if matches!(&origin, Origin::Human) {
        NOW + 5
    } else if source == "planner" {
        NOW + 3
    } else {
        NOW + 2
    };
    let e = store
        .ingest(origin, source, key, payload, time)
        .await
        .unwrap();
    assert!(matches!(
        store.route(&e.event_id).await.unwrap(),
        Outcome::Applied { .. }
    ));
    e.event_id
}
async fn fixture(scope: &str) -> (tempfile::TempDir, Store, OperationProposal, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
    event(&store,Origin::Human,"ui","create",json!({"commands":[{"kind":"create","id":"task","goal":"Operate the selected page","scope":scope,"priority":0,"requires_device":false}]})).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    let hex = "89504e470d0a1a0a0000000d4948445200000001000000010804000000b51c0c020000000b4944415478da63fcff1f0003030200efa345990000000049454e44ae426082";
    let png: Vec<u8> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect();
    let digest = format!("{:x}", Sha256::digest(&png));
    let device = uuid::Uuid::new_v4().to_string();
    let observation = BrowserObservation {
        session_id: uuid::Uuid::new_v4().to_string(),
        nonce: uuid::Uuid::new_v4().to_string(),
        url: "https://example.com/".into(),
        title: "Test page".into(),
        png_digest: digest.clone(),
        elements: vec![BrowserElement {
            id: "button-1".into(),
            label: "Count".into(),
            kind: BrowserElementKind::Button { form: None },
        }],
        observed_at: NOW,
        expires_at: NOW + 120_000,
    };
    let mut result = ReadOutcome::text("Observed page".into(), false);
    if let ReadOutcome::Completed {
        observation: field, ..
    } = &mut result
    {
        *field = Some(observation.clone());
    }
    let receipt_source = event(
        &store,
        Origin::Tool,
        &format!("device-result:{device}"),
        "observed",
        json!({"commands":[{"kind":"query","ids":["task"]}],"result":result}),
    )
    .await;
    sqlx::query("INSERT INTO link_reads VALUES('observed',?,1,'task','1',?,'completed',?)")
        .bind(&device)
        .bind(json!({"kind":"browser_inspect","url":"https://example.com/"}).to_string())
        .bind(serde_json::to_string(&result).unwrap())
        .execute(&store.application_pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO link_artifacts VALUES('observed',?,?)").bind(json!({"task_id":"task","revision":"1","digest":digest,"length":png.len(),"url":observation.url,"title":observation.title,"observed_at":NOW,"width":1,"height":1}).to_string()).bind(png).execute(&store.application_pool()).await.unwrap();
    let intent = BrowserIntent {
        source_operation_id: "observed".into(),
        local_device: device,
        local_epoch: "1".into(),
        observation,
        element_id: "button-1".into(),
        effect: BrowserEffect::Click,
    };
    let proposal = OperationProposal {
        id: "request".into(),
        token: run.token,
        target: intent.target(),
        intent: serde_json::to_value(intent).unwrap(),
        expires_at: NOW + 60_000,
    };
    let source = proposal_source(&store, &proposal, "proposal").await;
    (dir, store, proposal, source, receipt_source)
}
async fn proposal_source(store: &Store, proposal: &OperationProposal, key: &str) -> String {
    event(
        store,
        Origin::Tool,
        "planner",
        key,
        json!({"commands":[{"kind":"query","ids":["task"]}],"operation_proposal":proposal}),
    )
    .await
}
async fn approve(store: &Store, request: &OperationRequest) {
    event(store,Origin::Human,"ui","approve",json!({"commands":[{"kind":"approve_operation","binding":{"task_id":"task","expected_revision":"1","request_id":request.proposal.id,"intent_digest":request.intent_digest}}]})).await;
}

#[tokio::test]
async fn browser_effect_needs_interaction_scope_and_original_device_evidence() {
    for attack in [
        "read_scope",
        "nonce",
        "image",
        "receipt_source",
        "target",
        "local_epoch",
    ] {
        let scope = if attack == "read_scope" {
            "browser:https://example.com/"
        } else {
            "browser-interact:https://example.com/"
        };
        let (_dir, store, mut proposal, _source, receipt_source) = fixture(scope).await;
        match attack {
            "nonce" => {
                proposal.intent["observation"]["nonce"] = json!(uuid::Uuid::new_v4().to_string())
            }
            "image" => {
                sqlx::query("UPDATE link_artifacts SET png=X'00'")
                    .execute(&store.application_pool())
                    .await
                    .unwrap();
            }
            "receipt_source" => {
                sqlx::query("DELETE FROM event_tasks WHERE event_id=?")
                    .bind(receipt_source)
                    .execute(&store.application_pool())
                    .await
                    .unwrap();
            }
            "target" => proposal.target = format!("browser:{}", uuid::Uuid::new_v4()),
            "local_epoch" => proposal.intent["local_epoch"] = json!("2"),
            _ => {}
        }
        let source = proposal_source(&store, &proposal, "changed-proposal").await;
        assert!(
            store
                .propose_operation(&source, proposal, NOW + 4)
                .await
                .is_err(),
            "{attack}"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operation_requests")
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn approval_consumption_rolls_back_with_failed_transport_and_cannot_dispatch_twice() {
    let (_dir, store, proposal, source, _) = fixture("browser-interact:https://example.com/").await;
    let token = proposal.token.clone();
    let request = store
        .propose_operation(&source, proposal, NOW + 4)
        .await
        .unwrap();
    approve(&store, &request).await;
    let pool = store.application_pool();
    sqlx::raw_sql("CREATE TRIGGER reject_browser_delivery BEFORE INSERT ON link_outbox BEGIN SELECT RAISE(ABORT,'test delivery fault'); END;").execute(&pool).await.unwrap();
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let operation = Store::prepare_approved_operation_in(&mut tx, &token, "request", NOW + 5)
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO link_outbox VALUES('local',1,?, '{}',0)")
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
    sqlx::raw_sql("DROP TRIGGER reject_browser_delivery;")
        .execute(&pool)
        .await
        .unwrap();
    let operation = store
        .prepare_approved_operation(&token, "request", NOW + 6)
        .await
        .unwrap();
    assert_eq!(operation.state, OperationState::Unknown);
    assert_eq!(store.task("task").await.unwrap().tool_calls, 1);
    assert!(
        store
            .prepare_approved_operation(&token, "request", NOW + 7)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn source_link_removal_after_approval_still_prevents_dispatch() {
    let (_dir, store, proposal, source, receipt_source) =
        fixture("browser-interact:https://example.com/").await;
    let token = proposal.token.clone();
    let request = store
        .propose_operation(&source, proposal, NOW + 4)
        .await
        .unwrap();
    approve(&store, &request).await;
    sqlx::query("DELETE FROM event_tasks WHERE event_id=?")
        .bind(receipt_source)
        .execute(&store.application_pool())
        .await
        .unwrap();
    assert!(
        store
            .prepare_approved_operation(&token, "request", NOW + 5)
            .await
            .is_err()
    );
    assert_eq!(
        store.operation_request("request").await.unwrap().state,
        ApprovalState::Approved
    );
    assert_eq!(store.task("task").await.unwrap().tool_calls, 0);
}

#[test]
fn text_cannot_be_used_to_send_submit_keys_or_control_codes() {
    assert!(browser_input_text("中文 text 123"));
    assert!(browser_input_text(""));
    for value in ["send\n", "\r", "\t", "\u{e007}", "\u{f0000}", "\u{2028}"] {
        assert!(!browser_input_text(value), "{value:?}");
    }
    assert!(!browser_input_text(&"a".repeat(1025)));
}
