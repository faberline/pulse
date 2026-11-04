use super::*;
use pulse_core::{Command, Origin, State};
use pulse_link::{provider::Reply, remote};
use serde_json::json;

#[tokio::test]
async fn real_server_accepts_a_saved_answer_and_replays_after_restart_without_new_work() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("server.db");
    let hub = Hub::new(Store::open(&database, "pulse-owner").await.unwrap());
    let code = hub.pairing(ClientRole::Chat, now()).await.unwrap();
    let credential = hub
        .redeem(
            PairRequest {
                code,
                role: ClientRole::Chat,
                device: uuid::Uuid::new_v4().to_string(),
            },
            now(),
        )
        .await
        .unwrap();
    let event = hub.core.ingest(Origin::Human, "owned-process-fixture", "create", json!({"commands":[Command::Create{id:"A".into(),goal:"任務 A：owned lifecycle answer".into(),scope:"pulse:read".into(),priority:0,parent:None,dependencies:vec![],requires_device:false}]}), now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let run = hub
        .core
        .claim_next(&remote::versions(), now())
        .await
        .unwrap()
        .unwrap();
    hub.model_reply(
        &run.token,
        &Reply {
            text: "OWNED_PROCESS_RESULT".into(),
            tool: None,
            usage: json!({}),
            cost: 0,
        },
        "fixture",
        now(),
    )
    .await
    .unwrap();
    let message = format!("model:{}:1:0", run.token.run_id);
    let body: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
        .bind(&message)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    hub.core.clone().close().await;
    drop(hub);
    let (mut server, origin) = start(&database, 0).await;
    let (mut chat, epoch) = connect(&origin, &credential).await;
    let envelope = Envelope::new(
        &credential.device,
        1,
        epoch,
        DevicePayload::Human {
            source_id: "process-review".into(),
            action: HumanAction::AcceptResult {
                task_id: "A".into(),
                revision: "1".into(),
                message_id: message,
                chat_digest: hash_bytes(body.as_bytes()),
                text: "接受成果 A".into(),
            },
        },
    )
    .unwrap();
    send(
        &mut chat,
        &ClientWire::Data {
            envelope: Box::new(envelope.clone()),
        },
    )
    .await;
    ack(&mut chat, &envelope).await;
    stop(&mut server).await;
    closed(&mut chat).await;
    let store = Store::open(&database, "pulse-owner").await.unwrap();
    assert_eq!(store.task("A").await.unwrap().state, State::Succeeded);
    let terminal: i64 =
        sqlx::query_scalar("SELECT terminal_at FROM task_retention WHERE task_id='A'")
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
    store.close().await;
    let (mut server, origin) = start(&database, 0).await;
    let (mut chat, new_epoch) = connect(&origin, &credential).await;
    assert!(new_epoch > epoch);
    send(
        &mut chat,
        &ClientWire::Data {
            envelope: Box::new(envelope.clone()),
        },
    )
    .await;
    ack(&mut chat, &envelope).await;
    stop(&mut server).await;
    closed(&mut chat).await;
    let store = Store::open(&database, "pulse-owner").await.unwrap();
    assert_eq!(store.task("A").await.unwrap().state, State::Succeeded);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT terminal_at FROM task_retention WHERE task_id='A'")
            .fetch_one(&store.application_pool())
            .await
            .unwrap(),
        terminal
    );
    for query in [
        "SELECT count(*) FROM budget",
        "SELECT count(*) FROM link_reads",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(query)
                .fetch_one(&store.application_pool())
                .await
                .unwrap(),
            0
        );
    }
    assert_eq!(store.list_tasks(100).await.unwrap().len(), 1);
    store.close().await;
}
