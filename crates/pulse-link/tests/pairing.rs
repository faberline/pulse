use pulse_core::Store;
use pulse_link::{protocol::*, server::Hub};
use sqlx::Row;

#[tokio::test]
async fn schema_ten_upgrade_preserves_identity_and_seeds_existing_token_history() {
    use sqlx::{Connection, Executor};
    let (dir, hub) = setup().await;
    let time = pulse_link::now();
    let request = claim(&hub, ClientRole::Chat, time).await;
    let value = hub
        .redeem(
            PairRequest {
                code: request.code,
                device: request.device,
                role: request.role,
            },
            time,
        )
        .await
        .unwrap();
    hub.core.application_pool().close().await;
    drop(hub);
    let options = sqlx::sqlite::SqliteConnectOptions::new().filename(dir.path().join("server.db"));
    let mut db = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    db.execute("DROP TABLE task_memory_sources; DROP TABLE local_schedule_delivery; ALTER TABLE link_device DROP COLUMN schedule_notices; DROP TABLE schedules; DROP TABLE local_schedule_notices; DROP TABLE local_schedule_inputs; DROP TABLE link_pair_claims; DROP TABLE link_pair_tokens; DROP INDEX link_credential_digest; DELETE FROM _sqlx_migrations WHERE version IN (11,12,13,14); PRAGMA user_version=10;").await.unwrap();
    db.close().await.unwrap();
    let hub = Hub::new(
        Store::open(dir.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    assert_eq!(hub.authenticate(&value.token).await.unwrap(), value.device);
    let seeded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM link_pair_tokens WHERE token_digest=?")
            .bind(hash_bytes(value.token.as_bytes()))
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
    assert_eq!(seeded, 1);
    hub.revoke(&value.device).await.unwrap();
    let mut changed = claim(&hub, ClientRole::Chat, time).await;
    changed.device = value.device;
    hub.claim_pairing(&changed, time).await.unwrap();
    hub.revoke(&changed.device).await.unwrap();
    let mut reused = claim(&hub, ClientRole::Chat, time).await;
    reused.token = value.token;
    assert!(hub.claim_pairing(&reused, time).await.is_err());
    assert_eq!(used(&hub, &reused.code).await, 0);
}

async fn setup() -> (tempfile::TempDir, Hub) {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(dir.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    (dir, hub)
}
async fn claim(hub: &Hub, role: ClientRole, time: i64) -> PairClaim {
    PairClaim {
        code: hub.pairing(role, time).await.unwrap(),
        device: uuid::Uuid::new_v4().to_string(),
        role,
        attempt_id: uuid::Uuid::new_v4().to_string(),
        token: format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        ),
    }
}
async fn used(hub: &Hub, code: &str) -> i64 {
    sqlx::query_scalar("SELECT used FROM link_pairings WHERE digest=?")
        .bind(hash_bytes(code.as_bytes()))
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn identical_retries_preserve_identity_lease_epoch_and_expiry() {
    let (_dir, hub) = setup().await;
    let time = pulse_link::now();
    let request = claim(&hub, ClientRole::Chat, time).await;
    let (a, b) = tokio::join!(
        hub.claim_pairing(&request, time),
        hub.claim_pairing(&request, time)
    );
    assert!(hash(&a.unwrap()).unwrap() == hash(&b.unwrap()).unwrap());
    sqlx::query("UPDATE link_device SET epoch=42,lease_until=? WHERE id=?")
        .bind(time + 30_000)
        .bind(&request.device)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let expiry: i64 = sqlx::query_scalar("SELECT expires FROM link_pairings WHERE digest=?")
        .bind(hash_bytes(request.code.as_bytes()))
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(hub.claim_pairing(&request, time + 599_999).await.is_ok());
    assert!(hub.claim_pairing(&request, expiry).await.is_err());
    let row = sqlx::query("SELECT epoch,lease_until FROM link_device WHERE id=?")
        .bind(&request.device)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>("epoch"), 42);
    assert_eq!(row.get::<i64, _>("lease_until"), time + 30_000);
    let identity = hub.pairing_identity(&request.token).await.unwrap();
    assert_eq!(identity.device, request.device);
    assert_eq!(identity.role, ClientRole::Chat);
    // Identity proof after code expiry changes no server state.
    let current: i64 = sqlx::query_scalar("SELECT expires FROM link_pairings WHERE digest=?")
        .bind(hash_bytes(request.code.as_bytes()))
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(current, expiry);
    assert_eq!(used(&hub, &request.code).await, 1);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM link_device")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn altered_claims_reused_attempts_and_expired_unused_codes_are_rejected() {
    let (_dir, hub) = setup().await;
    let time = pulse_link::now();
    let request = claim(&hub, ClientRole::Chat, time).await;
    hub.claim_pairing(&request, time).await.unwrap();
    for field in ["device", "role", "token", "attempt_id", "extra"] {
        let mut value = serde_json::to_value(&request).unwrap();
        value[field] = match field {
            "role" => serde_json::json!("local_executor"),
            "token" => serde_json::json!("e".repeat(64)),
            _ => serde_json::json!(uuid::Uuid::new_v4().to_string()),
        };
        if let Ok(changed) = serde_json::from_value::<PairClaim>(value) {
            assert!(hub.claim_pairing(&changed, time).await.is_err());
        } else {
            assert_eq!(field, "extra");
        }
    }
    hub.revoke(&request.device).await.unwrap();
    let mut second = claim(&hub, ClientRole::Chat, time).await;
    second.attempt_id = request.attempt_id.clone();
    assert!(hub.claim_pairing(&second, time).await.is_err());
    assert_eq!(used(&hub, &second.code).await, 0);
    second.attempt_id = uuid::Uuid::new_v4().to_string();
    assert!(hub.claim_pairing(&second, time + 600_000).await.is_err());
    assert_eq!(used(&hub, &second.code).await, 0);
    assert!(hub.claim_pairing(&request, time).await.is_err());
    assert!(hub.pairing_identity(&request.token).await.is_err());
}

#[tokio::test]
async fn retired_tokens_cannot_be_reused_after_multiple_rotations_or_by_other_roles() {
    let (_dir, hub) = setup().await;
    let time = pulse_link::now();
    let first = claim(&hub, ClientRole::Chat, time).await;
    hub.claim_pairing(&first, time).await.unwrap();
    hub.revoke(&first.device).await.unwrap();
    let mut second = claim(&hub, ClientRole::Chat, time).await;
    second.device = first.device.clone();
    hub.claim_pairing(&second, time).await.unwrap();
    assert!(hub.claim_pairing(&first, time).await.is_err());
    hub.revoke(&second.device).await.unwrap();
    for role in [ClientRole::Chat, ClientRole::LocalExecutor] {
        let mut reused = claim(&hub, role, time).await;
        reused.token = first.token.clone();
        assert!(hub.claim_pairing(&reused, time).await.is_err());
        assert_eq!(used(&hub, &reused.code).await, 0);
    }
    let mut wrong_role = claim(&hub, ClientRole::LocalExecutor, time).await;
    wrong_role.device = first.device.clone();
    assert!(hub.claim_pairing(&wrong_role, time).await.is_err());
    assert_eq!(used(&hub, &wrong_role.code).await, 0);
    let third = claim(&hub, ClientRole::Chat, time).await;
    hub.claim_pairing(&third, time).await.unwrap();
    let fourth = claim(&hub, ClientRole::Chat, time).await;
    assert!(hub.claim_pairing(&fourth, time).await.is_err());
    assert_eq!(used(&hub, &fourth.code).await, 0);
}

#[tokio::test]
async fn legacy_credentials_are_registered_in_token_history_and_receipts_store_no_secrets() {
    let (dir, hub) = setup().await;
    let time = pulse_link::now();
    let request = claim(&hub, ClientRole::Chat, time).await;
    let legacy = hub
        .redeem(
            PairRequest {
                code: request.code.clone(),
                device: request.device.clone(),
                role: request.role,
            },
            time,
        )
        .await
        .unwrap();
    hub.revoke(&legacy.device).await.unwrap();
    let mut next = claim(&hub, ClientRole::Chat, time).await;
    next.device = legacy.device;
    hub.claim_pairing(&next, time).await.unwrap();
    hub.revoke(&next.device).await.unwrap();
    let mut reuse = claim(&hub, ClientRole::LocalExecutor, time).await;
    reuse.token = legacy.token.clone();
    assert!(hub.claim_pairing(&reuse, time).await.is_err());
    hub.core.application_pool().close().await;
    for file in std::fs::read_dir(dir.path()).unwrap() {
        let bytes = std::fs::read(file.unwrap().path()).unwrap();
        for secret in [&request.code, &legacy.token, &next.code, &next.token] {
            assert!(!bytes.windows(secret.len()).any(|w| w == secret.as_bytes()));
        }
    }
}
