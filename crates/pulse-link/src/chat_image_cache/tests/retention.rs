use super::*;

async fn expired_task(f: &Fixture, id: &str) -> pulse_core::Task {
    let store = pulse_core::Store::open(f.root.path().join(format!("task-{id}.db")), "fixture")
        .await
        .unwrap();
    let now = crate::now();
    let e=store.ingest(pulse_core::Origin::Human,"fixture","create",serde_json::json!({"commands":[{"kind":"create","id":id,"goal":"owned retention fixture","scope":"pulse:read","priority":0,"requires_device":false}]}),now).await.unwrap();
    store.route(&e.event_id).await.unwrap();
    let e = store
        .ingest(
            pulse_core::Origin::Human,
            "fixture",
            "cancel",
            serde_json::json!({"commands":[{"kind":"cancel","id":id,"expected_revision":"1"}]}),
            now + 1,
        )
        .await
        .unwrap();
    store.route(&e.event_id).await.unwrap();
    let mut task = store.task(id).await.unwrap();
    store.close().await;
    task.reason = Some("task_retention_expired".into());
    task.goal = "內容已到期並清除".into();
    task.scope.clear();
    task.decisions.clear();
    task.evidence.clear();
    task
}
async fn frame(f: &mut Fixture, id: &str) -> Envelope<ServerPayload> {
    let mut current = f.journal.view().await.unwrap();
    for message in &mut current.messages {
        if message.artifact.as_ref().is_some_and(|a| a.task_id == id) {
            message.body = "內容已到期並清除。".into();
            message.artifact = None;
            message.artifact_operation_id = None;
            message.approval = None;
            message.memory_sources = Some(vec![]);
        }
    }
    let task = expired_task(f, id).await;
    f.sequence += 1;
    Envelope::new(
        &f.journal.device,
        f.sequence,
        1,
        ServerPayload::DerivedSnapshot {
            tasks: vec![task],
            messages: current.messages,
            synced_at: crate::now(),
            local: LocalStatus::default(),
            sources: current.memory_sources,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn terminal_expiry_erases_owned_a_reference_and_keeps_equal_pixel_b() {
    let mut f = Fixture::new(false).await;
    f.snapshot("A", 0).await;
    let a = f.load("A").await;
    f.snapshot("B", 1).await;
    let b = f.load("B").await;
    assert_eq!(a.path, b.path);
    let expired = frame(&mut f, "A").await;
    assert!(f.journal.accept(&expired, true, 1, true).await.unwrap());
    assert_eq!(f.images().await, 1);
    assert_eq!(std::fs::read(&b.path).unwrap(), png());
    assert_eq!(
        f.journal.view().await.unwrap().artifacts[0].operation_id,
        "B"
    );
    let refs: Vec<String> =
        sqlx::query_scalar("SELECT value FROM local_meta WHERE key GLOB 'image:*'")
            .fetch_all(&f.journal.db)
            .await
            .unwrap();
    assert!(!refs.join("").contains("PRIVATE_IMAGE_A"));
    assert!(refs.join("").contains("PRIVATE_IMAGE_B"));
    let db = std::fs::read(f.config.data_dir.join("device.db")).unwrap();
    assert!(
        !db.windows(b"PRIVATE_IMAGE_A".len())
            .any(|b| b == b"PRIVATE_IMAGE_A")
    );
    assert!(!f.journal.accept(&expired, true, 1, true).await.unwrap());
    assert_eq!(f.images().await, 1);
}

#[tokio::test]
async fn failed_file_erasure_is_not_acknowledged_by_a_duplicate_frame() {
    let mut f = Fixture::new(false).await;
    f.snapshot("A", 0).await;
    let a = f.load("A").await;
    std::fs::write(&a.path, b"UNVERIFIED_REPLACEMENT").unwrap();
    let expired = frame(&mut f, "A").await;
    assert!(f.journal.accept(&expired, true, 1, true).await.is_err());
    assert!(f.journal.accept(&expired, true, 1, true).await.is_err());
    assert_eq!(std::fs::read(&a.path).unwrap(), b"UNVERIFIED_REPLACEMENT");
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM local_meta WHERE key='task_retention_cleanup_pending'",
    )
    .fetch_one(&f.journal.db)
    .await
    .unwrap();
    assert_eq!(pending, 1);
    std::fs::write(&a.path, png()).unwrap();
    assert!(!f.journal.accept(&expired, true, 1, true).await.unwrap());
    assert!(!std::path::Path::new(&a.path).exists());
    assert_eq!(f.images().await, 0);
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM local_meta WHERE key='task_retention_cleanup_pending'",
    )
    .fetch_one(&f.journal.db)
    .await
    .unwrap();
    assert_eq!(pending, 0);
}
