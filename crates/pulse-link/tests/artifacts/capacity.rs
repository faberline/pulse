use super::*;

async fn propose(hub: &Hub, id: &str, operation: ReadOp) {
    let scope = if matches!(operation, ReadOp::Browser { .. }) {
        "browser:https://example.com/"
    } else {
        "pulse:read"
    };
    let event = hub.core.ingest(Origin::Human, "capacity", id,
        json!({"commands":[{"kind":"create","id":id,"goal":"Read fixture data","scope":scope,"priority":0,"requires_device":false}]}),
        pulse_link::now()).await.unwrap();
    hub.core.route(&event.event_id).await.unwrap();
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.token.task_id, id);
    hub.model_proposal(
        &run.token,
        &Reply {
            text: String::new(),
            tool: Some(ToolCall {
                call_id: id.into(),
                operation,
            }),
            usage: json!({}),
            cost: 0,
        },
        pulse_link::now(),
    )
    .await
    .unwrap();
}

async fn seed(hub: &Hub, count: i64) {
    let mut picture = image(false);
    picture.artifact.task_id = "capacity-fixture".into();
    let mut tx = hub.core.application_pool().begin().await.unwrap();
    for n in 0..count {
        let id = format!("capacity-seed-{n}");
        sqlx::query("INSERT INTO link_reads VALUES(?,'old-device',1,'capacity-fixture','1',?,'completed',NULL)")
            .bind(&id).bind(serde_json::to_string(&ReadOp::Browser { url: picture.artifact.url.clone() }).unwrap())
            .execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO link_artifacts VALUES(?,?,?)")
            .bind(id)
            .bind(serde_json::to_string(&picture.artifact).unwrap())
            .bind(&picture.png)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}

async fn count(hub: &Hub, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn full_image_store_waits_before_dispatch_without_starving_text_work() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(dir.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    let (worker, epoch) = pair(&hub, ClientRole::LocalExecutor, "worker").await;
    hub.browser_ready(&worker.device, epoch, None, pulse_link::now())
        .await
        .unwrap();
    seed(&hub, 128).await;
    propose(
        &hub,
        "image",
        ReadOp::Browser {
            url: "https://example.com/".into(),
        },
    )
    .await;
    assert!(
        !hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    assert_eq!(hub.core.task("image").await.unwrap().tool_calls, 0);
    assert!(hub.outgoing(&worker.device).await.unwrap().is_empty());
    propose(
        &hub,
        "text",
        ReadOp::Text {
            path: "README.md".into(),
        },
    )
    .await;
    assert!(
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    let frames = hub.outgoing(&worker.device).await.unwrap();
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0].payload, ServerPayload::Read { task_id, .. } if task_id == "text"));
    assert_eq!(hub.core.task("image").await.unwrap().tool_calls, 0);
    assert_eq!(
        count(&hub, "SELECT count(*) FROM link_artifacts").await,
        128
    );
}

#[tokio::test]
async fn last_image_slot_is_durable_and_available_to_its_original_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server");
    let hub = Hub::new(Store::open(&path, "owner").await.unwrap());
    let (worker, epoch) = pair(&hub, ClientRole::LocalExecutor, "worker").await;
    seed(&hub, 127).await;
    // One original is both stored and unresolved. It must only count once.
    sqlx::query("UPDATE link_reads SET state='unknown' WHERE id='capacity-seed-0'")
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    let work = dispatch(&hub, &worker.device, epoch).await;
    let ServerPayload::Read { operation_id, .. } = work.payload else {
        panic!()
    };
    // A reopened store must see the reservation before the first PNG arrives.
    let reopened = Hub::new(Store::open(&path, "owner").await.unwrap());
    propose(
        &reopened,
        "later",
        ReadOp::Browser {
            url: "https://example.com/".into(),
        },
    )
    .await;
    // Model a disconnected executor: its work stays unknown, while its lease is renewed.
    sqlx::query("UPDATE link_reads SET state='unknown' WHERE id=?")
        .bind(&operation_id)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
    assert!(
        !reopened
            .dispatch_model_read(&remote::versions(), pulse_link::now())
            .await
            .unwrap()
    );
    let picture = image(true);
    for (index, bytes) in picture.png.chunks(CHUNK).enumerate() {
        let frame = Envelope::new(
            &worker.device,
            index as u64 + 1,
            epoch,
            DevicePayload::ArtifactChunk {
                operation_id: operation_id.clone(),
                artifact: picture.artifact.clone(),
                index: index as u32,
                data: STANDARD.encode(bytes),
            },
        )
        .unwrap();
        reopened
            .receive(&worker.device, &frame, pulse_link::now())
            .await
            .unwrap();
        let outcome: String =
            sqlx::query_scalar("SELECT outcome FROM link_inbox WHERE device=? AND sequence=?")
                .bind(&worker.device)
                .bind(index as i64 + 1)
                .fetch_one(&hub.core.application_pool())
                .await
                .unwrap();
        assert_eq!(outcome, "accepted");
        assert_eq!(
            count(&hub, "SELECT count(*) FROM link_artifacts").await,
            128
        );
        reopened
            .receive(&worker.device, &frame, pulse_link::now())
            .await
            .unwrap();
    }
    let saved: Vec<u8> = sqlx::query_scalar("SELECT png FROM link_artifacts WHERE operation_id=?")
        .bind(operation_id)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(saved, picture.png);
}

#[tokio::test]
async fn pending_images_keep_capacity_until_a_known_no_image_result() {
    for state in ["sent", "accepted", "unknown"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server");
        let hub = Hub::new(Store::open(&path, "owner").await.unwrap());
        let (worker, epoch) = pair(&hub, ClientRole::LocalExecutor, "worker").await;
        hub.browser_ready(&worker.device, epoch, None, pulse_link::now())
            .await
            .unwrap();
        seed(&hub, 127).await;
        sqlx::query(
            "INSERT INTO link_reads VALUES('pending','disconnected-device',1,'other','1',?,?,NULL)",
        )
        .bind(
            serde_json::to_string(&ReadOp::Browser {
                url: "https://example.com/".into(),
            })
            .unwrap(),
        )
        .bind(state)
        .execute(&hub.core.application_pool())
        .await
        .unwrap();
        propose(
            &hub,
            "later",
            ReadOp::Browser {
                url: "https://example.com/".into(),
            },
        )
        .await;
        let reopened = Hub::new(Store::open(&path, "owner").await.unwrap());
        assert!(
            !reopened
                .dispatch_model_read(&remote::versions(), pulse_link::now())
                .await
                .unwrap(),
            "{state}"
        );
        assert_eq!(hub.core.task("later").await.unwrap().tool_calls, 0);
        if state == "unknown" {
            // Source erasure removes the request body, not the unknown outcome.
            sqlx::query("UPDATE link_reads SET request='null' WHERE id='pending'")
                .execute(&hub.core.application_pool())
                .await
                .unwrap();
            assert!(
                !reopened
                    .dispatch_model_read(&remote::versions(), pulse_link::now())
                    .await
                    .unwrap()
            );
        }
        sqlx::query("UPDATE link_reads SET state='rejected',result=? WHERE id='pending'")
            .bind(
                serde_json::to_string(&ReadOutcome::Rejected {
                    reason: "No command issued".into(),
                })
                .unwrap(),
            )
            .execute(&hub.core.application_pool())
            .await
            .unwrap();
        // A failed enqueue must roll back authorization and the new reservation.
        sqlx::query("CREATE TRIGGER fail_image_delivery BEFORE INSERT ON link_outbox BEGIN SELECT RAISE(ABORT,'controlled delivery failure'); END")
            .execute(&hub.core.application_pool()).await.unwrap();
        assert!(
            reopened
                .dispatch_model_read(&remote::versions(), pulse_link::now())
                .await
                .is_err()
        );
        assert_eq!(hub.core.task("later").await.unwrap().tool_calls, 0);
        assert_eq!(
            count(
                &hub,
                "SELECT count(*) FROM link_reads WHERE task_id='later'"
            )
            .await,
            0
        );
        sqlx::query("DROP TRIGGER fail_image_delivery")
            .execute(&hub.core.application_pool())
            .await
            .unwrap();
        let versions = remote::versions();
        let (a, b) = tokio::join!(
            hub.dispatch_model_read(&versions, pulse_link::now()),
            reopened.dispatch_model_read(&versions, pulse_link::now())
        );
        assert_eq!(usize::from(a.unwrap()) + usize::from(b.unwrap()), 1);
        assert_eq!(hub.core.task("later").await.unwrap().tool_calls, 1);
        assert_eq!(
            count(
                &hub,
                "SELECT count(*) FROM link_reads WHERE task_id='later'"
            )
            .await,
            1
        );
    }
}

#[tokio::test]
async fn late_unreserved_image_cannot_take_another_operations_slot() {
    let dir = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(dir.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    let (worker, epoch) = pair(&hub, ClientRole::LocalExecutor, "worker").await;
    seed(&hub, 127).await;
    let work = dispatch(&hub, &worker.device, epoch).await;
    let ServerPayload::Read { operation_id, .. } = work.payload else {
        panic!()
    };
    let db = hub.core.application_pool();
    // Model a known no-image result followed by another admitted image operation.
    sqlx::query("UPDATE link_reads SET state='rejected' WHERE id=?")
        .bind(&operation_id)
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO link_reads VALUES('new-reservation','other-device',1,'other','1',?,'sent',NULL)")
        .bind(serde_json::to_string(&ReadOp::Browser { url: "https://example.com/".into() }).unwrap())
        .execute(&db).await.unwrap();
    let picture = image(false);
    let frame = Envelope::new(
        &worker.device,
        1,
        epoch,
        DevicePayload::ArtifactChunk {
            operation_id: operation_id.clone(),
            artifact: picture.artifact,
            index: 0,
            data: STANDARD.encode(picture.png),
        },
    )
    .unwrap();
    hub.receive(&worker.device, &frame, pulse_link::now())
        .await
        .unwrap();
    let outcome: String =
        sqlx::query_scalar("SELECT outcome FROM link_inbox WHERE device=? AND sequence=1")
            .bind(&worker.device)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(outcome, "rejected");
    assert_eq!(
        count(&hub, "SELECT count(*) FROM link_artifacts").await,
        127
    );
    assert_eq!(
        count(
            &hub,
            "SELECT count(*) FROM link_reads WHERE id='new-reservation' AND state='sent'"
        )
        .await,
        1
    );
}
