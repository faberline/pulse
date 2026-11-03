use base64::{Engine, engine::general_purpose::STANDARD};
use pulse_core::{Origin, Store};
use pulse_link::{
    artifact::*,
    journal::Journal,
    protocol::*,
    provider::{Reply, ToolCall},
    remote,
    server::{Hub, router},
};
use serde_json::json;
use std::future::IntoFuture;

#[path = "artifacts/capacity.rs"]
mod capacity;

fn image(multipart: bool) -> CapturedImage {
    let mut png=STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=").unwrap();
    if multipart {
        png.resize(CHUNK + 100, 0);
    }
    CapturedImage {
        artifact: ArtifactRef {
            task_id: "page".into(),
            revision: "1".into(),
            digest: hash_bytes(&png),
            length: png.len() as u32,
            url: "https://example.com/".into(),
            computer: None,
            title: "Native evidence".into(),
            observed_at: pulse_link::now(),
            width: 1,
            height: 1,
        },
        png,
    }
}
async fn pair(hub: &Hub, role: ClientRole, device: &str) -> (Credential, u64) {
    let device = if uuid::Uuid::parse_str(device).is_ok() {
        device.to_owned()
    } else {
        uuid::Uuid::new_v4().to_string()
    };
    let time = pulse_link::now();
    let code = hub.pairing(role, time).await.unwrap();
    let credential = hub
        .redeem(
            PairRequest {
                code,
                device: device.clone(),
                role,
            },
            time,
        )
        .await
        .unwrap();
    let epoch = hub.begin(&device, 0, time).await.unwrap();
    (credential, epoch)
}
async fn dispatch(hub: &Hub, device: &str, epoch: u64) -> Envelope<ServerPayload> {
    let event=hub.core.ingest(Origin::Human,"test","create",json!({"commands":[{"kind":"create","id":"page","goal":"Read the page","scope":"browser:https://example.com/","priority":0,"requires_device":false}]}),pulse_link::now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    let reply = Reply {
        text: String::new(),
        tool: Some(ToolCall {
            call_id: "call".into(),
            operation: ReadOp::Browser {
                url: "https://example.com/".into(),
            },
        }),
        usage: json!({}),
        cost: 1,
    };
    hub.model_proposal(&run.token, &reply, pulse_link::now())
        .await
        .unwrap();
    hub.browser_ready(device, epoch, None, pulse_link::now())
        .await
        .unwrap();
    hub.dispatch_model_read(&remote::versions(), pulse_link::now())
        .await
        .unwrap();
    let data: String =
        sqlx::query_scalar("SELECT data FROM link_outbox WHERE device=? ORDER BY sequence LIMIT 1")
            .bind(device)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    serde_json::from_str(&data).unwrap()
}

#[tokio::test]
async fn original_png_crosses_durable_journal_receipt_and_authenticated_chat_download() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(dir.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let (local, _lock) = Journal::open(
        &dir.path().join("local"),
        "https://pulse.example/",
        dir.path(),
    )
    .await
    .unwrap();
    let (local_cred, epoch) = pair(&hub, ClientRole::LocalExecutor, &local.device).await;
    let (chat, chat_epoch) = pair(&hub, ClientRole::Chat, "chat").await;
    let envelope = dispatch(&hub, &local.device, epoch).await;
    let ServerPayload::Read { operation_id, .. } = &envelope.payload else {
        panic!("not dispatched")
    };
    local.accept(&envelope, true, epoch, true).await.unwrap();
    local.take_work().await.unwrap().unwrap();
    let picture = image(true);
    let metadata = picture.artifact.clone();
    let expected = picture.png.clone();
    local
        .finish_work_image(
            &envelope,
            ReadOutcome::text("Bound browser evidence".into(), false),
            Some(picture),
        )
        .await
        .unwrap();
    local.recover().await.unwrap();
    assert!(local.take_work().await.unwrap().is_none());
    let outgoing = local.outgoing().await.unwrap();
    assert_eq!(outgoing.len(), 3);
    for frame in &outgoing {
        hub.receive(&local.device, frame, pulse_link::now())
            .await
            .unwrap();
        hub.receive(&local.device, frame, pulse_link::now())
            .await
            .unwrap();
        if matches!(frame.payload, DevicePayload::ArtifactChunk { .. }) {
            assert!(
                hub.artifact_png(
                    &chat.device,
                    operation_id,
                    &metadata.digest,
                    pulse_link::now()
                )
                .await
                .is_err()
            );
        }
        local
            .ack(number(&frame.sequence).unwrap(), &frame.id)
            .await
            .unwrap();
    }
    assert!(local.outgoing().await.unwrap().is_empty());
    assert_eq!(
        hub.artifact_png(
            &chat.device,
            operation_id,
            &metadata.digest,
            pulse_link::now()
        )
        .await
        .unwrap(),
        expected
    );
    assert!(
        hub.artifact_png(
            &local_cred.device,
            operation_id,
            &metadata.digest,
            pulse_link::now()
        )
        .await
        .is_err()
    );
    assert!(
        hub.artifact_png(
            &chat.device,
            operation_id,
            &"0".repeat(64),
            pulse_link::now()
        )
        .await
        .is_err()
    );
    hub.snapshot(&chat.device, chat_epoch, pulse_link::now())
        .await
        .unwrap();
    let data: String = sqlx::query_scalar(
        "SELECT data FROM link_outbox WHERE device=? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&chat.device)
    .fetch_one(&hub.core.application_pool())
    .await
    .unwrap();
    let snapshot: Envelope<ServerPayload> = serde_json::from_str(&data).unwrap();
    let ServerPayload::Snapshot { messages, .. } = snapshot.payload else {
        panic!("no snapshot")
    };
    assert_eq!(
        messages
            .iter()
            .find(|m| m.id == *operation_id)
            .unwrap()
            .artifact
            .as_ref(),
        Some(&metadata)
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    let cache = dir.path().join("chat");
    std::fs::create_dir(&cache).unwrap();
    let config = pulse_link::client::Config {
        server: server_url.parse().unwrap(),
        repo: std::path::PathBuf::new(),
        data_dir: cache,
    };
    let downloaded = pulse_link::artifact_client::download(&config, &chat, operation_id, &metadata)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&downloaded.path).unwrap(), expected);
    hub.revoke(&chat.device).await.unwrap();
    assert!(
        hub.artifact_png(
            &chat.device,
            operation_id,
            &metadata.digest,
            pulse_link::now()
        )
        .await
        .is_err()
    );
    server.abort();
}

#[tokio::test]
async fn forged_epoch_task_metadata_chunks_and_digest_cannot_create_a_preview() {
    for attack in ["epoch", "task", "revision", "metadata", "digest", "index"] {
        let dir = tempfile::tempdir().unwrap();
        let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await.unwrap());
        let (local, epoch) = pair(&hub, ClientRole::LocalExecutor, "local").await;
        let envelope = dispatch(&hub, &local.device, epoch).await;
        let ServerPayload::Read { operation_id, .. } = envelope.payload else {
            panic!()
        };
        let mut picture = image(false);
        let mut used_epoch = epoch;
        let mut index = 0;
        match attack {
            "epoch" => used_epoch += 1,
            "task" => picture.artifact.task_id = "other".into(),
            "revision" => picture.artifact.revision = "2".into(),
            "metadata" => picture.artifact.width = 8193,
            "digest" => picture.artifact.digest = "0".repeat(64),
            _ => index = 1,
        }
        let frame = Envelope::new(
            &local.device,
            1,
            used_epoch,
            DevicePayload::ArtifactChunk {
                operation_id,
                artifact: picture.artifact,
                index,
                data: STANDARD.encode(picture.png),
            },
        )
        .unwrap();
        hub.receive(&local.device, &frame, pulse_link::now())
            .await
            .unwrap();
        let outcome: String = sqlx::query_scalar("SELECT outcome FROM link_inbox WHERE device=?")
            .bind(&local.device)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(outcome, "rejected", "{attack}");
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM link_artifacts WHERE png IS NOT NULL")
                .fetch_one(&hub.core.application_pool())
                .await
                .unwrap();
        assert_eq!(count, 0, "{attack}");
    }
}

#[test]
fn image_and_chunk_bounds_are_checked_before_storage() {
    let picture = image(false);
    picture.artifact.verify(&picture.png).unwrap();
    let mut metadata = picture.artifact.clone();
    metadata.length = MAX_PNG as u32 + 1;
    assert!(metadata.validate().is_err());
    assert!(picture.artifact.chunk(0, &"A".repeat(CHUNK * 2)).is_err());
    assert!(png_dimensions(b"not png").is_err());
    let mut changed = picture.png;
    changed[20] ^= 1;
    assert!(picture.artifact.verify(&changed).is_err());
}
