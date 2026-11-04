//! Real HTTP and vault substitutes. These tests do not perform a DB cutover.
use super::*;

async fn restore_generation(f: &Fixture, generation: &str) {
    sqlx::query("INSERT INTO settings VALUES('restore_generation',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        .bind(generation).execute(&f.hub.core.application_pool()).await.unwrap();
}

#[tokio::test]
async fn failed_generation_claim_vault_save_sends_no_claim_and_resumes_after_restart() {
    let f = Fixture::new().await;
    let generation = uuid::Uuid::new_v4().to_string();
    restore_generation(&f, &generation).await;
    let device = f.open().await;
    f.vault.fail_pending_at.store(2, Ordering::SeqCst);
    assert!(f.begin(&device, f.code().await).await.is_err());
    let old = f.vault.pending("").unwrap().unwrap();
    assert!(old.server_generation.is_none());
    assert_eq!(f.posts.load(Ordering::SeqCst), 0);
    assert!(f.vault.load("").unwrap().is_none());
    drop(acquire_profile(&f, device).await);
    f.vault.fail_pending_at.store(0, Ordering::SeqCst);
    let device = f.open().await;
    let credential = f.begin(&device, old.code).await.unwrap();
    assert_ne!(credential.token, old.token);
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
    assert!(f.vault.pending("").unwrap().is_none());
    assert_eq!(
        f.hub
            .pairing_identity(&credential.token)
            .await
            .unwrap()
            .server_generation,
        Some(generation)
    );
}

#[tokio::test]
async fn pristine_worker_profile_can_pair_with_a_restored_server_without_starting_tools() {
    let f = Fixture::new().await;
    let generation = uuid::Uuid::new_v4().to_string();
    restore_generation(&f, &generation).await;
    let mut config = f.config.clone();
    config.data_dir = f._root.path().join("worker");
    config.repo = f._root.path().join("owned-repo");
    std::fs::create_dir(&config.repo).unwrap();
    let device = Device::open(config.clone(), f.vault.clone()).await.unwrap();
    let code = f
        .hub
        .pairing(ClientRole::LocalExecutor, crate::now())
        .await
        .unwrap();
    let credential = pairing::begin(
        config,
        device.journal.clone(),
        f.vault.clone(),
        device.account.clone(),
        code,
    )
    .await
    .unwrap();
    assert_eq!(credential.role, ClientRole::LocalExecutor);
    assert_eq!(
        f.hub.authenticate(&credential.token).await.unwrap(),
        device.journal.device
    );
    assert_eq!(f.count("SELECT count(*) FROM link_reads").await, 0);
    assert_eq!(f.count("SELECT count(*) FROM budget").await, 0);
}

#[tokio::test]
async fn nonpristine_or_incomplete_profile_never_submits_a_restore_claim() {
    for kind in [
        "human",
        "counter",
        "missing-counter",
        "access",
        "extra-table",
        "quoted-table",
    ] {
        let f = Fixture::new().await;
        let device = f.open().await;
        match kind {
            "human" => device
                .journal
                .action(
                    &uuid::Uuid::new_v4().to_string(),
                    HumanAction::Submit {
                        id: uuid::Uuid::new_v4().to_string(),
                        goal: "OLD_UNACKNOWLEDGED_INPUT".into(),
                    },
                )
                .await
                .unwrap(),
            "counter" => {
                sqlx::query("UPDATE local_meta SET value='1' WHERE key='next'")
                    .execute(&device.journal.db)
                    .await
                    .unwrap();
            }
            "missing-counter" => {
                sqlx::query("DELETE FROM local_meta WHERE key='next'")
                    .execute(&device.journal.db)
                    .await
                    .unwrap();
            }
            "access" => {
                sqlx::query("UPDATE local_memory_access SET generation=1")
                    .execute(&device.journal.db)
                    .await
                    .unwrap();
            }
            "extra-table" => {
                sqlx::raw_sql("CREATE TABLE local_extra(value TEXT); INSERT INTO local_extra VALUES('owned retained data');").execute(&device.journal.db).await.unwrap();
            }
            "quoted-table" => {
                sqlx::raw_sql("CREATE TABLE \"local_bad\"\"quote\"(value TEXT);")
                    .execute(&device.journal.db)
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let outgoing = serde_json::to_value(device.journal.outgoing().await.unwrap()).unwrap();
        restore_generation(&f, &uuid::Uuid::new_v4().to_string()).await;
        assert!(f.begin(&device, f.code().await).await.is_err(), "{kind}");
        assert_eq!(f.posts.load(Ordering::SeqCst), 0, "{kind}");
        assert_eq!(
            serde_json::to_value(device.journal.outgoing().await.unwrap()).unwrap(),
            outgoing
        );
        assert_eq!(f.count("SELECT count(*) FROM link_device").await, 0);
        assert_eq!(f.count("SELECT count(*) FROM budget").await, 0);
    }
}
async fn acquire_profile(f: &Fixture, device: Device) -> pulse_core::DatabaseMaintenance {
    device.journal.db.close().await;
    drop(device);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match pulse_core::DatabaseMaintenance::try_acquire(f.config.data_dir.join("device.db")) {
            Ok(lease) => return lease,
            Err(error) if std::time::Instant::now() >= deadline => panic!("{error}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

#[tokio::test]
async fn restored_pairing_survives_lost_response_expired_code_and_connector_restart() {
    let f = Fixture::new().await;
    let generation = uuid::Uuid::new_v4().to_string();
    restore_generation(&f, &generation).await;
    let device = f.open().await;
    f.mode.store(1, Ordering::SeqCst);
    assert!(f.begin(&device, f.code().await).await.is_err());
    let claim = f.vault.pending("").unwrap().unwrap();
    assert_eq!(
        claim.server_generation.as_deref(),
        Some(generation.as_str())
    );
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
    let manifest =
        std::fs::read(f.config.data_dir.join("device.db.memory-restrictions.json")).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    assert_eq!(value["format"], 9);
    assert_eq!(value["peer_restore"]["committed"], true);
    assert_eq!(value["peer_restore"]["generation"], generation);
    assert!(!String::from_utf8_lossy(&manifest).contains(&claim.token));
    sqlx::query("UPDATE link_pairings SET expires=0")
        .execute(&f.hub.core.application_pool())
        .await
        .unwrap();
    drop(acquire_profile(&f, device).await);
    f.mode.store(0, Ordering::SeqCst);
    let device = f.open().await;
    let (tx, rx) = mpsc::channel(8);
    let (state, mut view) = watch::channel(NativeState::default());
    let running = tokio::spawn(device.run(rx, state));
    wait_connected(&mut view).await;
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
    assert!(f.vault.pending("").unwrap().is_none());
    assert_eq!(f.count("SELECT count(*) FROM link_device").await, 1);
    tx.send(NativeCommand::Action {
        source_id: uuid::Uuid::new_v4().to_string(),
        action: HumanAction::Submit {
            id: uuid::Uuid::new_v4().to_string(),
            goal: "記住 還原後偏好：OWNED_POST_RESTORE_SOURCE".into(),
        },
    })
    .await
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        view.wait_for(|state| {
            state
                .memories
                .iter()
                .any(|memory| memory.body == "OWNED_POST_RESTORE_SOURCE")
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(f.count("SELECT count(*) FROM budget").await, 0);
    assert_eq!(f.count("SELECT count(*) FROM tasks").await, 0);
    tx.send(NativeCommand::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn restored_server_does_not_repair_a_profile_with_old_human_input_or_active_credentials() {
    for active in [false, true] {
        let f = Fixture::new().await;
        let device = f.open().await;
        if active {
            let credential = f.begin(&device, f.code().await).await.unwrap();
            device
                .journal
                .activate_memory_pairing(&credential)
                .await
                .unwrap();
            f.hub.revoke(&credential.device).await.unwrap();
        }
        let source = f.remember(&device).await;
        restore_generation(&f, &uuid::Uuid::new_v4().to_string()).await;
        let posts = f.posts.load(Ordering::SeqCst);
        let code = f.code().await;
        assert!(f.begin(&device, code.clone()).await.is_err());
        assert_eq!(f.posts.load(Ordering::SeqCst), posts);
        assert_eq!(
            device.journal.memory_requests().await.unwrap()[0].source_id,
            source
        );
        assert_eq!(
            f.vault.pending("").unwrap().unwrap().server_generation,
            None
        );
        let used: i64 = sqlx::query_scalar("SELECT used FROM link_pairings WHERE digest=?")
            .bind(hash_bytes(code.as_bytes()))
            .fetch_one(&f.hub.core.application_pool())
            .await
            .unwrap();
        assert_eq!(used, 0);
    }
}

#[tokio::test]
async fn database_only_rollback_cannot_remove_the_restored_profile_binding() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new().await;
    let device = f.open().await;
    let snapshot = f._root.path().join("before-generation.db");
    sqlx::query("VACUUM INTO ?")
        .bind(snapshot.to_str().unwrap())
        .execute(&device.journal.db)
        .await
        .unwrap();
    restore_generation(&f, &uuid::Uuid::new_v4().to_string()).await;
    f.begin(&device, f.code().await).await.unwrap();
    let lease = acquire_profile(&f, device).await;
    std::fs::copy(snapshot, lease.database()).unwrap();
    std::fs::set_permissions(lease.database(), std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(lease);
    let gets = f.gets.load(Ordering::SeqCst);
    assert!(
        Device::open_chat(f.config.clone(), f.vault.clone())
            .await
            .is_err()
    );
    assert_eq!(f.gets.load(Ordering::SeqCst), gets);
}

#[tokio::test]
async fn failed_sql_after_the_external_fence_can_resume_without_minting_authority() {
    let f = Fixture::new().await;
    let device = f.open().await;
    let generation = uuid::Uuid::new_v4().to_string();
    restore_generation(&f, &generation).await;
    sqlx::raw_sql("CREATE TRIGGER fail_generation BEFORE INSERT ON local_meta WHEN NEW.key='peer_restore_generation' BEGIN SELECT RAISE(ABORT,'fixture'); END;")
        .execute(&device.journal.db).await.unwrap();
    let code = f.code().await;
    assert!(f.begin(&device, code.clone()).await.is_err());
    assert_eq!(f.posts.load(Ordering::SeqCst), 0);
    let manifest = f.config.data_dir.join("device.db.memory-restrictions.json");
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    assert_eq!(value["peer_restore"]["committed"], false);
    drop(acquire_profile(&f, device).await);
    let device = f.open().await;
    sqlx::raw_sql("DROP TRIGGER fail_generation")
        .execute(&device.journal.db)
        .await
        .unwrap();
    f.begin(&device, code).await.unwrap();
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    assert_eq!(value["peer_restore"]["committed"], true);
}

#[tokio::test]
async fn pending_claim_cannot_cross_a_second_restore_or_malformed_generation() {
    let f = Fixture::new().await;
    let device = f.open().await;
    restore_generation(&f, &uuid::Uuid::new_v4().to_string()).await;
    f.mode.store(1, Ordering::SeqCst);
    assert!(f.begin(&device, f.code().await).await.is_err());
    let claim = f.vault.pending("").unwrap().unwrap();
    restore_generation(&f, &uuid::Uuid::new_v4().to_string()).await;
    f.hub.revoke(&device.journal.device).await.unwrap();
    f.mode.store(0, Ordering::SeqCst);
    assert!(f.begin(&device, claim.code.clone()).await.is_err());
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
    assert_eq!(
        hash(&f.vault.pending("").unwrap().unwrap()).unwrap(),
        hash(&claim).unwrap()
    );
    restore_generation(&f, "not-a-generation").await;
    let response = reqwest::Client::new()
        .get(f.config.server.join("v1/device-pairings/identity").unwrap())
        .bearer_auth(&claim.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        response
            .headers()
            .get("x-pulse-credential-denied")
            .is_none()
    );
    assert!(
        response
            .headers()
            .get("x-pulse-restore-generation")
            .is_none()
    );
}
