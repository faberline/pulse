use pulse_core::Store;
use pulse_link::{protocol::*, server::Hub};
use sqlx::Row;

#[tokio::test]
async fn checked_restore_retires_devices_and_freezes_work_without_inventing_results() {
    use pulse_core::{Origin, Receipt, ScheduleRequest, ScheduledAction, State, Versions};
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    let (dir, hub) = setup().await;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let time = pulse_link::now();
    let chat = claim(&hub, ClientRole::Chat, time).await;
    let worker = claim(&hub, ClientRole::LocalExecutor, time).await;
    hub.claim_pairing(&chat, time).await.unwrap();
    hub.claim_pairing(&worker, time).await.unwrap();
    let epoch = hub.begin(&worker.device, 0, time).await.unwrap();
    let unused = hub.pairing(ClientRole::Chat, time).await.unwrap();
    let reminder = ScheduleRequest {
        id: "restore-reminder".into(),
        label: "owned reminder".into(),
        source_at: time,
        due_at: time + 60_000,
        timezone: "UTC".into(),
        action: ScheduledAction::Remind {
            message: "owned fixture".into(),
        },
    };
    let event = hub.core.ingest(Origin::Human,"ui","restore-create",json!({"commands":[
        {"kind":"create","id":"terminal","goal":"owned completed fixture","scope":"pulse:read","priority":2,"requires_device":false},
        {"kind":"create","id":"running","goal":"owned fixture","scope":"pulse:read","priority":1,"requires_device":false},
        {"kind":"create","id":"queued","goal":"owned queued fixture","scope":"pulse:read","priority":0,"requires_device":false},
        {"kind":"schedule","request":reminder}
    ]}),time).await.unwrap();
    assert!(!matches!(
        hub.core.route(&event.event_id).await.unwrap(),
        pulse_core::Outcome::Rejected { .. }
    ));
    let completed = hub
        .core
        .claim_next(&Versions::fixture(), time)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.token.task_id, "terminal");
    hub.core
        .finish(&completed.token, true, "owned goal result", time)
        .await
        .unwrap();
    let terminal_before = hub.core.task("terminal").await.unwrap();
    let run = hub
        .core
        .claim_next(&Versions::fixture(), time)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, "running");
    hub.core
        .advance(
            &run.token,
            State::Ready,
            json!({"step":1}),
            "owned fixture",
            None,
            time,
        )
        .await
        .unwrap();
    let known = hub
        .core
        .prepare_operation(&run.token, "fixture:known", json!({"owned":true}), time)
        .await
        .unwrap();
    hub.core
        .record_receipt(&Receipt {
            operation_id: known.id.clone(),
            token: run.token.clone(),
            success: true,
            evidence: "owned original receipt".into(),
        })
        .await
        .unwrap();
    hub.core
        .advance(
            &run.token,
            State::Ready,
            json!({"step":2}),
            "owned fixture",
            None,
            time,
        )
        .await
        .unwrap();
    let unknown = hub
        .core
        .prepare_operation(&run.token, "fixture:unknown", json!({"owned":true}), time)
        .await
        .unwrap();
    let pending = hub.core.ingest(Origin::Human,"ui","pending-restore-event",json!({"commands":[
        {"kind":"create","id":"must-not-start","goal":"owned pending fixture","scope":"pulse:read","priority":0,"requires_device":false}
    ]}),time).await.unwrap();
    let pool = hub.core.application_pool();
    let read = ReadOp::List { path: ".".into() };
    for (id, state, result) in [
        (
            &known.id,
            "completed",
            Some(serde_json::to_string(&ReadOutcome::text("owned result".into(), false)).unwrap()),
        ),
        (&unknown.id, "accepted", None),
    ] {
        sqlx::query("INSERT INTO link_reads VALUES(?,?,?,?,?,?,?,?)")
            .bind(id)
            .bind(&worker.device)
            .bind(epoch as i64)
            .bind("running")
            .bind("1")
            .bind(serde_json::to_string(&read).unwrap())
            .bind(state)
            .bind(result)
            .execute(&pool)
            .await
            .unwrap();
    }
    let frame = Envelope::new(
        &worker.device,
        1,
        epoch,
        ServerPayload::Read {
            operation_id: unknown.id.clone(),
            task_id: "running".into(),
            revision: "1".into(),
            operation: read,
        },
    )
    .unwrap();
    sqlx::query("INSERT INTO link_outbox VALUES(?,?,?, ?,0)")
        .bind(&worker.device)
        .bind(1_i64)
        .bind(&frame.id)
        .bind(serde_json::to_string(&frame).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE link_device SET worker_cleanup=1,worker_erasure=1,projection_erasure=1,schedule_notices=1 WHERE id=?")
        .bind(&worker.device).execute(&pool).await.unwrap();
    let before_receipt: (String, String) =
        sqlx::query_as("SELECT digest,data FROM receipts WHERE operation_id=?")
            .bind(&known.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let schema: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    let backup = hub.core.maintain(time).await.unwrap().backup.unwrap();
    let checked = hub
        .core
        .check_backup(
            &backup,
            &dir.path().join("checked"),
            &Versions::fixture(),
            time + 1,
        )
        .await
        .unwrap();
    assert_eq!(checked.unknown_tasks, 1);
    assert!(Store::open(&checked.candidate, "owner").await.is_err());

    // Test only: open an isolated COPY to exercise the independent auth gates.
    // The actual candidate keeps its unsupported quarantine marker throughout.
    let fixture = dir.path().join("runnable-test-copy.db");
    let mut bytes = std::fs::read(&checked.candidate).unwrap();
    bytes[60..64].copy_from_slice(&u32::try_from(schema).unwrap().to_be_bytes());
    std::fs::write(&fixture, bytes).unwrap();
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o600)).unwrap();
    let sidecar = |p: &std::path::Path| {
        let mut name = p.as_os_str().to_owned();
        name.push(".memory-restrictions.json");
        std::path::PathBuf::from(name)
    };
    std::fs::copy(sidecar(&checked.candidate), sidecar(&fixture)).unwrap();
    let restored = Hub::new(Store::open(&fixture, "owner").await.unwrap());
    let restored_pool = restored.core.application_pool();
    for request in [&chat, &worker] {
        assert!(restored.authenticate(&request.token).await.is_err());
        assert!(restored.claim_pairing(request, time + 2).await.is_err());
        assert!(restored.begin(&request.device, 0, time + 2).await.is_err());
        assert_eq!(
            hub.authenticate(&request.token).await.unwrap(),
            request.device
        );
    }
    let late = Envelope::new(
        &worker.device,
        1,
        epoch,
        DevicePayload::Result {
            operation_id: unknown.id.clone(),
            task_id: "running".into(),
            revision: "1".into(),
            outcome: ReadOutcome::text("owned late result".into(), false),
        },
    )
    .unwrap();
    assert!(
        restored
            .receive(&worker.device, &late, time + 2)
            .await
            .is_err()
    );
    assert_eq!(restored.core.receipts("running").await.unwrap().len(), 1);
    let flags:(i64,i64,i64,i64,i64,i64,i64,i64)=sqlx::query_as("SELECT revoked,stopped,lease_until,epoch,worker_cleanup,worker_erasure,projection_erasure,schedule_notices FROM link_device WHERE id=?")
        .bind(&worker.device).fetch_one(&restored_pool).await.unwrap();
    assert_eq!(flags, (1, 1, 0, epoch as i64 + 1, 0, 0, 0, 0));
    let old_code = PairRequest {
        code: unused.clone(),
        device: uuid::Uuid::new_v4().to_string(),
        role: ClientRole::Chat,
    };
    assert!(restored.redeem(old_code, time + 2).await.is_err());
    assert_eq!(used(&hub, &unused).await, 0);
    let mut fresh = claim(&restored, ClientRole::LocalExecutor, time + 2).await;
    fresh.device = worker.device.clone();
    assert!(restored.claim_pairing(&fresh, time + 2).await.is_err());
    assert_eq!(used(&restored, &fresh.code).await, 0);
    fresh.device = uuid::Uuid::new_v4().to_string();
    restored.claim_pairing(&fresh, time + 2).await.unwrap();
    assert_eq!(
        restored.authenticate(&fresh.token).await.unwrap(),
        fresh.device
    );
    restored.begin(&fresh.device, 0, time + 2).await.unwrap();
    assert!(
        restored
            .outgoing(&fresh.device)
            .await
            .unwrap()
            .iter()
            .all(|f| !matches!(f.payload, ServerPayload::Read { .. }))
    );
    let queued: (String, i64) =
        sqlx::query_as("SELECT data,acknowledged FROM link_outbox WHERE device=? AND sequence=1")
            .bind(&worker.device)
            .fetch_one(&restored_pool)
            .await
            .unwrap();
    assert_eq!(queued, (serde_json::to_string(&frame).unwrap(), 0));
    assert!(restored.core.pending_events(100).await.unwrap().is_empty());
    assert!(
        matches!(restored.core.route(&pending.event_id).await.unwrap(),pulse_core::Outcome::Rejected{reason} if reason=="backup_restore_review_required")
    );
    assert!(restored.core.task("must-not-start").await.is_err());
    assert_eq!(
        restored.core.task("queued").await.unwrap().state,
        State::Blocked
    );
    assert_eq!(
        serde_json::to_value(restored.core.task("terminal").await.unwrap()).unwrap(),
        serde_json::to_value(terminal_before).unwrap()
    );
    assert_eq!(
        restored.core.task("running").await.unwrap().state,
        State::OutcomeUnknown
    );
    let schedule = restored.core.schedules(100).await.unwrap().pop().unwrap();
    assert_eq!(schedule.state, pulse_core::ScheduleState::Cancelled);
    assert_eq!(schedule.revision, 2);
    assert!(
        !restored
            .core
            .fire_schedule(time + 60_000)
            .await
            .unwrap()
            .fired
    );
    restored
        .core
        .recover(&Versions::fixture(), time + 3)
        .await
        .unwrap();
    restored.recover().await.unwrap();
    assert!(
        restored
            .core
            .claim_next(&Versions::fixture(), time + 3)
            .await
            .unwrap()
            .is_none()
    );
    let after_receipt: (String, String) =
        sqlx::query_as("SELECT digest,data FROM receipts WHERE operation_id=?")
            .bind(&known.id)
            .fetch_one(&restored_pool)
            .await
            .unwrap();
    assert_eq!(after_receipt, before_receipt);
    let states: Vec<(String, String)> =
        sqlx::query_as("SELECT id,state FROM link_reads ORDER BY id")
            .fetch_all(&restored_pool)
            .await
            .unwrap();
    assert!(states.contains(&(known.id.clone(), "completed".into())));
    assert!(states.contains(&(unknown.id.clone(), "unknown".into())));
    assert_eq!(hub.core.task("queued").await.unwrap().state, State::Queued);
    assert_eq!(
        hub.core.pending_events(100).await.unwrap(),
        vec![pending.event_id]
    );
    assert_eq!(
        hub.core.schedules(100).await.unwrap()[0].state,
        pulse_core::ScheduleState::Armed
    );
    assert!(Store::open(&checked.candidate, "owner").await.is_err());
}

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
    db.execute("DROP TABLE link_worker_cleanup; ALTER TABLE link_device DROP COLUMN worker_cleanup; DROP TABLE link_worker_fences; DROP TABLE link_erased_work; ALTER TABLE link_device DROP COLUMN worker_erasure; DROP TABLE memory_erased_reads; DROP TABLE memory_derived_messages; DROP TABLE link_erased_frames; ALTER TABLE link_device DROP COLUMN projection_erasure; DROP TABLE task_memory_sources; DROP TABLE local_schedule_delivery; ALTER TABLE link_device DROP COLUMN schedule_notices; DROP TABLE schedules; DROP TABLE local_schedule_notices; DROP TABLE local_schedule_inputs; DROP TABLE link_pair_claims; DROP TABLE link_pair_tokens; DROP INDEX link_credential_digest; DELETE FROM _sqlx_migrations WHERE version IN (11,12,13,14,15,16,17,18); PRAGMA user_version=10;").await.unwrap();
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
