use super::*;

async fn denied(f: &Fixture) -> bool {
    f.journal
        .core
        .worker_access_denied(&f.journal.device, &hash(&f.journal.server).unwrap())
        .await
        .unwrap()
}
fn absent(path: &std::path::Path, body: &[u8]) {
    let bytes = std::fs::read(path).unwrap();
    assert!(!bytes.windows(body.len()).any(|s| s == body));
    match std::fs::read(path.with_file_name("device.db-wal")) {
        Ok(bytes) => assert!(!bytes.windows(body.len()).any(|s| s == body)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("cannot inspect worker WAL: {error}"),
    }
}
#[tokio::test]
async fn worker_denial_erases_pending_work_results_and_views_without_settling_server_outcomes() {
    let f = setup().await;
    let work = accept(&f, true).await;
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    let before: (String, Option<String>) =
        sqlx::query_as("SELECT state,result FROM link_reads WHERE id=?")
            .bind(match &f.read.payload {
                ServerPayload::Read { operation_id, .. } => operation_id,
                _ => unreachable!(),
            })
            .fetch_one(&f.hub.db)
            .await
            .unwrap();
    assert!(!f.journal.deliveries().await.unwrap().is_empty());
    f.journal.deny_worker_access().await.unwrap();
    assert!(denied(&f).await);
    assert!(matches!(
        f.journal.deliveries().await,
        Err(crate::Error::CredentialRejected)
    ));
    assert!(f.journal.take_work().await.is_err());
    assert!(
        f.journal
            .finish_work(&work, ReadOutcome::text("LATE_DENIED_BODY".into(), false))
            .await
            .is_err()
    );
    assert!(
        f.journal
            .accept(&f.read, true, f.epoch, true)
            .await
            .is_err()
    );
    f.journal.stop(false).await.unwrap();
    let new_work = Envelope::new(
        &f.journal.device,
        number(&f.read.sequence).unwrap() + 1,
        f.epoch,
        ServerPayload::Read {
            operation_id: "new-work".into(),
            task_id: "B".into(),
            revision: "2".into(),
            operation: ReadOp::Text {
                path: "README.md".into(),
            },
        },
    )
    .unwrap();
    assert!(matches!(
        f.journal.accept(&new_work, true, f.epoch, true).await,
        Err(crate::Error::CredentialRejected)
    ));
    let view = f.journal.view().await.unwrap();
    assert!(view.stopped && view.messages.is_empty() && view.tasks.is_empty());
    assert_eq!(view.pending, 0);
    let after: (String, Option<String>) =
        sqlx::query_as("SELECT state,result FROM link_reads WHERE id=?")
            .bind(match &f.read.payload {
                ServerPayload::Read { operation_id, .. } => operation_id,
                _ => unreachable!(),
            })
            .fetch_one(&f.hub.db)
            .await
            .unwrap();
    assert_eq!(before, after);
    absent(&f.journal.directory.join("device.db"), BODY.as_bytes());
    absent(&f.journal.directory.join("device.db"), b"LATE_DENIED_BODY");
}

#[tokio::test]
async fn worker_denial_sql_failure_keeps_the_external_fence_and_repair_can_finish_erasure() {
    let f = setup().await;
    let work = accept(&f, true).await;
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_worker_denial BEFORE DELETE ON local_outbox BEGIN SELECT RAISE(ABORT,'fixture'); END;").execute(&f.journal.db).await.unwrap();
    assert!(f.journal.deny_worker_access().await.is_err());
    assert!(denied(&f).await);
    assert!(f.journal.deliveries().await.is_err());
    assert!(f.journal.take_work().await.is_err());
    assert!(
        f.journal
            .accept(&f.read, true, f.epoch, true)
            .await
            .is_err()
    );
    assert!(f.journal.view().await.unwrap().messages.is_empty());
    let bytes: String = sqlx::query_scalar("SELECT data FROM local_outbox LIMIT 1")
        .fetch_one(&f.journal.db)
        .await
        .unwrap();
    assert!(bytes.contains(BODY)); // Failed SQL is not physical erasure evidence.
    sqlx::raw_sql("DROP TRIGGER fail_worker_denial;")
        .execute(&f.journal.db)
        .await
        .unwrap();
    f.journal.deny_worker_access().await.unwrap();
    absent(&f.journal.directory.join("device.db"), BODY.as_bytes());
    assert!(matches!(
        f.journal.deliveries().await,
        Err(crate::Error::CredentialRejected)
    ));
}

#[tokio::test]
async fn worker_denial_cannot_be_cleared_by_sqlite_restore_or_a_new_empty_runtime() {
    use std::os::unix::fs::PermissionsExt;
    let mut f = setup().await;
    let work = accept(&f, true).await;
    f.journal
        .finish_work(&work, ReadOutcome::text(BODY.into(), false))
        .await
        .unwrap();
    let backup = f._root.path().join("old-worker.db");
    sqlx::query("VACUUM INTO ?")
        .bind(backup.to_str().unwrap())
        .execute(&f.journal.db)
        .await
        .unwrap();
    f.journal.deny_worker_access().await.unwrap();
    f.journal.core.clone().close().await;
    drop(f._lock);
    let path = f._root.path().join("worker/device.db");
    std::fs::copy(&backup, &path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let (journal, lock) = Journal::open(
        &f._root.path().join("worker"),
        "https://pulse.example/",
        f._root.path(),
    )
    .await
    .unwrap();
    f.journal = journal;
    f._lock = lock;
    assert!(denied(&f).await);
    assert!(f.journal.take_work().await.is_err());
    assert!(f.journal.deliveries().await.is_err());
    let executor = crate::executor::Executor::new(f._root.path()).unwrap();
    assert!(
        f.journal
            .begin_worker_runtime(&executor, &f.journal.directory)
            .await
            .is_err()
    );
    assert!(f.journal.view().await.unwrap().messages.is_empty());
    absent(&path, BODY.as_bytes());
}

#[tokio::test]
async fn worker_denial_requires_the_original_device_server_and_local_role() {
    let f = setup().await;
    let server = hash(&f.journal.server).unwrap();
    assert!(
        f.journal
            .core
            .deny_worker_access("other-device", &server)
            .await
            .is_err()
    );
    assert!(
        f.journal
            .core
            .deny_worker_access(&f.journal.device, &"0".repeat(64))
            .await
            .is_err()
    );
    assert!(!denied(&f).await);
    let (chat, _lock) = Journal::open_role(
        &f._root.path().join("chat"),
        "https://pulse.example/",
        std::path::Path::new(""),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    assert!(chat.deny_worker_access().await.is_err());
    assert!(
        chat.core
            .deny_worker_access(&chat.device, &server)
            .await
            .is_err()
    );
}
