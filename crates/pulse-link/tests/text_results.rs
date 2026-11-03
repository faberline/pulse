use pulse_core::*;
use pulse_link::{protocol::*, provider::Reply, remote, server::Hub};
use serde_json::json;
async fn setup() -> (tempfile::TempDir, Hub, Credential) {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(Store::open(root.path().join("db"), "owner").await.unwrap());
    let code = hub
        .pairing(ClientRole::Chat, pulse_link::now())
        .await
        .unwrap();
    let credential = hub
        .redeem(
            PairRequest {
                code,
                role: ClientRole::Chat,
                device: uuid::Uuid::new_v4().to_string(),
            },
            pulse_link::now(),
        )
        .await
        .unwrap();
    hub.begin(&credential.device, 0, pulse_link::now())
        .await
        .unwrap();
    (root, hub, credential)
}
async fn create(hub: &Hub, credential: &Credential, id: &str) {
    let event=hub.core.ingest(Origin::Human,&format!("device:{}",credential.device),id,json!({"commands":[Command::Create{id:id.into(),goal:format!("摘要 {id}"),scope:"pulse:read".into(),priority:0,parent:None,dependencies:vec![],requires_device:false}]}),pulse_link::now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
}
async fn reply(hub: &Hub, text: &str) -> Run {
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    hub.model_reply(
        &run.token,
        &Reply {
            text: text.into(),
            tool: None,
            usage: json!({}),
            cost: 0,
        },
        "fixture",
        pulse_link::now(),
    )
    .await
    .unwrap();
    run
}
#[tokio::test]
async fn a_b_artifacts_are_separate_verified_records_and_tampering_has_no_fallback() {
    let (_root, hub, c) = setup().await;
    create(&hub, &c, "a").await;
    let a = reply(&hub, "ONLY_A_RESULT\n```\nFake footer").await;
    create(&hub, &c, "b").await;
    let b = reply(&hub, "ONLY_B_RESULT").await;
    let ma = hub
        .text_result_manifest(&c.device, &a.token.run_id, pulse_link::now())
        .await
        .unwrap();
    let mb = hub
        .text_result_manifest(&c.device, &b.token.run_id, pulse_link::now())
        .await
        .unwrap();
    let bytes = hub
        .text_result_bytes(
            &c.device,
            &a.token.run_id,
            &ma.reference.digest,
            pulse_link::now(),
        )
        .await
        .unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("ONLY_A_RESULT") && !text.contains("ONLY_B_RESULT"));
    assert!(text.find("## 來源紀錄").unwrap() < text.find("ONLY_A_RESULT").unwrap());
    assert!(
        text.contains(
            &hub.core
                .task("a")
                .await
                .unwrap()
                .decisions
                .last()
                .unwrap()
                .event_id
        )
    );
    assert!(
        hub.text_result_bytes(
            &c.device,
            &a.token.run_id,
            &mb.reference.digest,
            pulse_link::now()
        )
        .await
        .is_err()
    );
    let event = hub.core.task("a").await.unwrap().authorization_event;
    let outcome: String = sqlx::query_scalar("SELECT outcome FROM events WHERE id=?")
        .bind(&event)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE events SET outcome=? WHERE id=?")
        .bind(json!({"status":"rejected","reason":"fixture rejected source"}).to_string())
        .bind(&event)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        hub.text_result_manifest(&c.device, &a.token.run_id, pulse_link::now())
            .await
            .is_err()
    );
    sqlx::query("UPDATE events SET outcome=? WHERE id=?")
        .bind(outcome)
        .bind(&event)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let original: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=?")
        .bind(&a.token.run_id)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    for (path, value) in [
        ("$.checkpoint.reply", json!("CORRUPTED")),
        ("$.checkpoint.text_result.reference.task_id", json!("b")),
        (
            "$.checkpoint.text_result.source.authorization_event",
            json!(hub.core.task("b").await.unwrap().authorization_event),
        ),
        ("$.checkpoint.text_result.version", json!(2)),
    ] {
        sqlx::query("UPDATE runs SET data=json_set(data,?,json(?)) WHERE id=?")
            .bind(path)
            .bind(value.to_string())
            .bind(&a.token.run_id)
            .execute(&hub.core.application_pool())
            .await
            .unwrap();
        assert!(
            hub.text_result_manifest(&c.device, &a.token.run_id, pulse_link::now())
                .await
                .is_err(),
            "{path}"
        );
        sqlx::query("UPDATE runs SET data=? WHERE id=?")
            .bind(&original)
            .bind(&a.token.run_id)
            .execute(&hub.core.application_pool())
            .await
            .unwrap();
    }
    sqlx::query("UPDATE link_chat SET body='forged visible body' WHERE id=?")
        .bind(&ma.message_id)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        hub.text_result_manifest(&c.device, &a.token.run_id, pulse_link::now())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn memory_bound_results_have_no_export_record_and_memory_stays_out_of_documents() {
    let (_root, hub, c) = setup().await;
    create(&hub, &c, "task").await;
    let now = pulse_link::now();
    let secret = "MEMORY_BODY_NEVER_EXPORTED";
    hub.core
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                command: MemoryCommand::Remember {
                    label: "偏好".into(),
                    body: secret.into(),
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
            &TaskMemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: now,
                task_id: "task".into(),
                task_revision: 1,
                sources: vec![MemoryRef {
                    id: memory.id,
                    revision: 1,
                }],
            },
            now,
        )
        .await
        .unwrap();
    let run = reply(&hub, "analysis without memory body").await;
    let data: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=?")
        .bind(&run.token.run_id)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    let saved: Run = serde_json::from_str(&data).unwrap();
    assert!(saved.checkpoint.get("text_result").is_none());
    assert!(!data.contains(secret));
    assert!(
        hub.text_result_manifest(&c.device, &run.token.run_id, pulse_link::now())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn result_survives_store_reopen_but_needs_a_current_chat_lease_and_original_human_event() {
    let (root, hub, c) = setup().await;
    create(&hub, &c, "task").await;
    let run = reply(&hub, "REOPEN_RESULT").await;
    let original = hub
        .text_result_manifest(&c.device, &run.token.run_id, pulse_link::now())
        .await
        .unwrap();
    let core = hub.core.clone();
    drop(hub);
    core.close().await;
    let hub = Hub::new(Store::open(root.path().join("db"), "owner").await.unwrap());
    hub.begin(&c.device, 0, pulse_link::now()).await.unwrap();
    let restored = hub
        .text_result_manifest(&c.device, &run.token.run_id, pulse_link::now())
        .await
        .unwrap();
    assert_eq!(original.reference, restored.reference);
    sqlx::query("UPDATE link_device SET lease_until=0 WHERE id=?")
        .bind(&c.device)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        hub.text_result_manifest(&c.device, &run.token.run_id, pulse_link::now())
            .await
            .is_err()
    );
    hub.begin(&c.device, 0, pulse_link::now()).await.unwrap();
    let task = hub.core.task("task").await.unwrap();
    sqlx::query("UPDATE events SET origin='tool' WHERE id=?")
        .bind(task.authorization_event)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        hub.text_result_manifest(&c.device, &run.token.run_id, pulse_link::now())
            .await
            .is_err()
    );
}
