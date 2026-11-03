//! Full Rust server -> authenticated download -> owned chat PNG -> source erasure.
use super::*;
use crate::artifact::ArtifactRef;
use base64::{Engine, engine::general_purpose::STANDARD};

#[tokio::test]
async fn authenticated_source_image_is_saved_then_erased_in_both_server_and_chat() {
    let f = setup().await;
    say(&f, "記住 圖片偏好：使用繁體中文").await;
    sync(&f).await;
    let memory = f
        .hub
        .core
        .list_memories(crate::now())
        .await
        .unwrap()
        .remove(0);
    let event=f.hub.core.ingest(Origin::Human,"image-fixture","create",
        serde_json::json!({"commands":[{"kind":"create","id":"picture","goal":"Read the owned image fixture","scope":"browser:https://example.com/","priority":0,"requires_device":false}]}),crate::now()).await.unwrap();
    f.hub.core.route(&event.event_id).await.unwrap();
    f.hub
        .core
        .bind_task_memories(
            Origin::Human,
            &pulse_core::TaskMemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: crate::now(),
                task_id: "picture".into(),
                task_revision: 1,
                sources: vec![pulse_core::MemoryRef {
                    id: memory.id.clone(),
                    revision: memory.revision,
                }],
            },
            crate::now(),
        )
        .await
        .unwrap();
    let png=STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=").unwrap();
    let artifact = ArtifactRef {
        task_id: "picture".into(),
        revision: "1".into(),
        digest: hash_bytes(&png),
        length: png.len() as u32,
        url: "https://example.com/".into(),
        computer: None,
        title: "PRIVATE_IMAGE_SOURCE_FIXTURE".into(),
        observed_at: crate::now(),
        width: 1,
        height: 1,
    };
    // A legacy completed read can lack the derived-message index. Resolve its real task lineage.
    sqlx::query("INSERT INTO link_reads VALUES('picture-read','owned-fixture',1,'picture','1',?,'completed',?)")
        .bind(serde_json::to_string(&ReadOp::Browser{url:artifact.url.clone()}).unwrap())
        .bind(serde_json::to_string(&ReadOutcome::text("Image fixture".into(),false)).unwrap())
        .execute(&f.hub.core.application_pool()).await.unwrap();
    sqlx::query("INSERT INTO link_artifacts VALUES('picture-read',?,?)")
        .bind(serde_json::to_string(&artifact).unwrap())
        .bind(&png)
        .execute(&f.hub.core.application_pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO link_chat VALUES('picture-read','Image fixture',?)")
        .bind(crate::now())
        .execute(&f.hub.core.application_pool())
        .await
        .unwrap();
    let epoch = f
        .hub
        .begin(&f.credential.device, 0, crate::now())
        .await
        .unwrap();
    f.hub
        .projection_erasure_ready(&f.credential.device, epoch, crate::now())
        .await
        .unwrap();
    f.hub
        .snapshot(&f.credential.device, epoch, crate::now())
        .await
        .unwrap();
    for frame in f.hub.outgoing(&f.credential.device).await.unwrap() {
        if let ServerPayload::DerivedSnapshot {
            messages, sources, ..
        } = &frame.payload
        {
            let message = messages.iter().find(|m| m.id == "picture-read").unwrap();
            assert_eq!(message.memory_sources.as_ref(), Some(sources));
            assert_eq!(sources[0].memory_id, memory.id);
        }
        f.journal.accept(&frame, true, epoch, true).await.unwrap();
        f.hub
            .ack(
                &f.credential.device,
                number(&frame.sequence).unwrap(),
                &frame.id,
            )
            .await
            .unwrap();
    }
    let image = f
        .journal
        .load_artifact(&f.config, &f.credential, "picture-read", &artifact.digest)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&image.path).unwrap(), png);
    assert_eq!(
        f.journal.view().await.unwrap().artifacts[0].artifact,
        artifact
    );
    say(&f, "忘記記憶 圖片偏好").await;
    assert!(!std::path::Path::new(&image.path).exists());
    sync(&f).await;
    let left: i64 =
        sqlx::query_scalar("SELECT count(*) FROM link_artifacts WHERE operation_id='picture-read'")
            .fetch_one(&f.hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!(left, 0);
    assert!(
        f.journal
            .load_artifact(&f.config, &f.credential, "picture-read", &artifact.digest)
            .await
            .is_err()
    );
    assert!(!std::path::Path::new(&image.path).exists());
}
