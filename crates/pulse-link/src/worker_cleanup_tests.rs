// Uses the real source-bound server/journal fixture in worker_erasure_tests.
use super::*;
use crate::{executor::Executor, worker_cleanup::WorkerCleanup};

async fn cleanup_runtime(f: &Fixture) -> Executor {
    let executor = Executor::new(f._root.path()).unwrap();
    f.journal
        .begin_worker_runtime(&executor, &f.journal.directory)
        .await
        .unwrap();
    executor
}
async fn cleanup_ready(f: &Fixture, epoch: u64) {
    f.hub
        .worker_erasure_ready(&f.journal.device, epoch, crate::now())
        .await
        .unwrap();
    f.hub
        .worker_cleanup_ready(&f.journal.device, epoch, 1, crate::now())
        .await
        .unwrap();
}
async fn cleanup_frame(f: &Fixture) -> Envelope<DevicePayload> {
    f.journal
        .deliveries()
        .await
        .unwrap()
        .into_iter()
        .find_map(|d| match d {
            DeviceDelivery::Data(e) if matches!(e.payload, DevicePayload::WorkerCleanup { .. }) => {
                Some(*e)
            }
            _ => None,
        })
        .unwrap()
}
async fn runtime_verified(journal: &Journal, executor: &Executor) -> bool {
    journal
        .core
        .worker_runtime_verified(
            &journal.device,
            &hash(&journal.server).unwrap(),
            &executor.runtime_id,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn worker_cleanup_ack_is_not_evidence_and_verified_receipt_replays_after_reconnect() {
    let f = setup().await;
    let executor = cleanup_runtime(&f).await;
    assert!(runtime_verified(&f.journal, &executor).await);
    cleanup_ready(&f, f.epoch).await;
    accept(&f, true).await;
    let context = forget(&f).await;
    let header = context.read_header.as_ref().unwrap();
    f.journal
        .accept_worker_erasure(header, &context)
        .await
        .unwrap();
    f.hub
        .ack(
            &f.journal.device,
            number(&header.sequence).unwrap(),
            &header.id,
        )
        .await
        .unwrap();
    assert!(
        !f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    f.journal
        .complete_worker_cleanup(&executor, header, &context, true)
        .await
        .unwrap();
    let envelope = cleanup_frame(&f).await;
    let data = serde_json::to_string(&envelope).unwrap();
    assert!(!data.contains(BODY));
    let before: (String, Option<String>) =
        sqlx::query_as("SELECT state,result FROM link_reads WHERE id=?")
            .bind(&context.operation_id)
            .fetch_one(&f.hub.db)
            .await
            .unwrap();
    f.hub
        .receive(&f.journal.device, &envelope, crate::now())
        .await
        .unwrap();
    assert!(
        f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    f.hub.end(&f.journal.device, f.epoch).await.unwrap();
    let epoch = f
        .hub
        .begin(
            &f.journal.device,
            f.journal.cursor().await.unwrap(),
            crate::now(),
        )
        .await
        .unwrap();
    cleanup_ready(&f, epoch).await;
    // The original epoch, ID and digest remain bound. No effect is retried.
    f.hub
        .receive(&f.journal.device, &envelope, crate::now())
        .await
        .unwrap();
    f.journal
        .ack(number(&envelope.sequence).unwrap(), &envelope.id)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM local_worker_cleanup")
        .fetch_one(&f.journal.db)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let after: (String, Option<String>) =
        sqlx::query_as("SELECT state,result FROM link_reads WHERE id=?")
            .bind(&context.operation_id)
            .fetch_one(&f.hub.db)
            .await
            .unwrap();
    assert_eq!(before, after);
    executor.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&executor).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_rejects_wrong_binding_capability_role_and_replay() {
    let f = setup().await;
    let executor = cleanup_runtime(&f).await;
    accept(&f, true).await;
    let context = forget(&f).await;
    let receipt = WorkerCleanup {
        version: 1,
        control: context.read_header.clone().unwrap(),
        context: context.clone(),
    };
    let make = |r: WorkerCleanup, device: &str, epoch| {
        Envelope::new(
            device,
            1,
            epoch,
            DevicePayload::WorkerCleanup {
                receipt: Box::new(r),
            },
        )
        .unwrap()
    };
    assert!(
        f.hub
            .receive(
                &f.journal.device,
                &make(receipt.clone(), &f.journal.device, f.epoch),
                crate::now()
            )
            .await
            .is_err()
    );
    cleanup_ready(&f, f.epoch).await;
    for field in [
        "version",
        "task",
        "revision",
        "source",
        "operation",
        "read",
        "control",
        "epoch",
        "device",
    ] {
        let mut bad = receipt.clone();
        match field {
            "version" => bad.version = 2,
            "task" => bad.context.task_id = "other-task".into(),
            "revision" => bad.context.revision = "2".into(),
            "source" => bad.context.source_id = uuid::Uuid::new_v4().to_string(),
            "operation" => bad.context.operation_id = "other-operation".into(),
            "read" => bad.context.read_header = None,
            "control" => bad.control.id = uuid::Uuid::new_v4().to_string(),
            "epoch" => bad.control.epoch = "999".into(),
            "device" => bad.context.device = f.chat.clone(),
            _ => unreachable!(),
        }
        assert!(
            f.hub
                .receive(
                    &f.journal.device,
                    &make(bad, &f.journal.device, f.epoch),
                    crate::now()
                )
                .await
                .is_err(),
            "{field}"
        );
    }
    assert!(
        f.hub
            .receive(
                &f.chat,
                &make(receipt.clone(), &f.chat, f.chat_epoch),
                crate::now()
            )
            .await
            .is_err()
    );
    let good = make(receipt.clone(), &f.journal.device, f.epoch);
    f.hub
        .receive(&f.journal.device, &good, crate::now())
        .await
        .unwrap();
    assert!(
        f.hub
            .receive(
                &f.journal.device,
                &make(receipt, &f.journal.device, f.epoch),
                crate::now()
            )
            .await
            .is_err()
    );
    executor.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&executor).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_pending_intent_survives_sql_failure_and_full_outbox() {
    let f = setup().await;
    let executor = cleanup_runtime(&f).await;
    accept(&f, true).await;
    let context = forget(&f).await;
    let header = context.read_header.as_ref().unwrap();
    f.journal
        .accept_worker_erasure(header, &context)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_cleanup BEFORE INSERT ON local_worker_cleanup BEGIN SELECT RAISE(ABORT,'fixture'); END;").execute(&f.journal.db).await.unwrap();
    assert!(
        f.journal
            .complete_worker_cleanup(&executor, header, &context, true)
            .await
            .is_err()
    );
    sqlx::raw_sql("DROP TRIGGER fail_cleanup;")
        .execute(&f.journal.db)
        .await
        .unwrap();
    // Fill only the ordinary outbox. Cleanup evidence has a separate durable slot.
    let mut tx = f.journal.db.begin().await.unwrap();
    for sequence in 1..=1000 {
        let envelope = Envelope::new(
            &f.journal.device,
            sequence,
            f.epoch,
            DevicePayload::Result {
                operation_id: format!("independent-{sequence}"),
                task_id: "B".into(),
                revision: "1".into(),
                outcome: ReadOutcome::text("independent".into(), false),
            },
        )
        .unwrap();
        sqlx::query("INSERT INTO local_outbox VALUES(?,?,?)")
            .bind(sequence as i64)
            .bind(&envelope.id)
            .bind(serde_json::to_string(&envelope).unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE local_meta SET value='1000' WHERE key='next'")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    f.journal
        .complete_worker_cleanup(&executor, header, &context, true)
        .await
        .unwrap();
    f.journal.flush_worker_cleanup().await.unwrap();
    let sequence: Option<i64> = sqlx::query_scalar("SELECT sequence FROM local_worker_cleanup")
        .fetch_one(&f.journal.db)
        .await
        .unwrap();
    assert_eq!(sequence, None);
    sqlx::query("DELETE FROM local_outbox WHERE sequence=1")
        .execute(&f.journal.db)
        .await
        .unwrap();
    f.journal.flush_worker_cleanup().await.unwrap();
    let sequence: i64 = sqlx::query_scalar("SELECT sequence FROM local_worker_cleanup")
        .fetch_one(&f.journal.db)
        .await
        .unwrap();
    assert_eq!(sequence, 1001);
    executor.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&executor).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_runtime_crash_is_sticky_and_old_runtime_cannot_close_replacement() {
    let f = setup().await;
    let first = cleanup_runtime(&f).await;
    assert!(runtime_verified(&f.journal, &first).await);
    let second = cleanup_runtime(&f).await; // Simulate process replacement without verified shutdown.
    assert!(!runtime_verified(&f.journal, &second).await);
    assert!(f.journal.close_worker_runtime(&first).await.is_err());
    second.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&second).await.unwrap();
    let third = cleanup_runtime(&f).await;
    assert!(!runtime_verified(&f.journal, &third).await);
    let context = forget(&f).await;
    let header = context.read_header.as_ref().unwrap();
    f.journal
        .accept_worker_erasure(header, &context)
        .await
        .unwrap();
    f.journal
        .complete_worker_cleanup(&third, header, &context, true)
        .await
        .unwrap();
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    third.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&third).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_legacy_and_unknown_resources_do_not_gain_proof_after_empty_restart() {
    for mode in ["legacy", "browser", "computer", "symlink"] {
        let f = setup().await;
        match mode {
            "legacy" => {
                sqlx::query("DELETE FROM local_meta WHERE key='worker_runtime_origin'")
                    .execute(&f.journal.db)
                    .await
                    .unwrap();
            }
            "browser" => {
                let p = f.journal.directory.join("browser");
                std::fs::create_dir(&p).unwrap();
                std::fs::write(p.join("old-profile"), b"unknown").unwrap();
            }
            "computer" => {
                let p = f.journal.directory.join("computer/artifacts");
                std::fs::create_dir_all(&p).unwrap();
                std::fs::write(p.join("old.png"), b"unknown").unwrap();
            }
            "symlink" => {
                std::os::unix::fs::symlink(f._root.path(), f.journal.directory.join("browser"))
                    .unwrap()
            }
            _ => unreachable!(),
        }
        let executor = cleanup_runtime(&f).await;
        assert!(!runtime_verified(&f.journal, &executor).await, "{mode}");
        executor.shutdown().await.unwrap();
        f.journal.close_worker_runtime(&executor).await.unwrap();
        let replacement = cleanup_runtime(&f).await;
        assert!(!runtime_verified(&f.journal, &replacement).await, "{mode}");
        replacement.shutdown().await.unwrap();
        f.journal.close_worker_runtime(&replacement).await.unwrap();
    }
}

#[tokio::test]
async fn worker_cleanup_normal_shutdown_preserves_proof_across_real_journal_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("local");
    let (journal, lock) = Journal::open(&path, "https://pulse.example/", root.path())
        .await
        .unwrap();
    let executor = Executor::new(root.path()).unwrap();
    journal
        .begin_worker_runtime(&executor, &path)
        .await
        .unwrap();
    assert!(runtime_verified(&journal, &executor).await);
    executor.shutdown().await.unwrap();
    journal.close_worker_runtime(&executor).await.unwrap();
    journal.core.clone().close().await;
    drop(journal);
    drop(lock);
    let (journal, _lock) = Journal::open(&path, "https://pulse.example/", root.path())
        .await
        .unwrap();
    let executor = Executor::new(root.path()).unwrap();
    journal
        .begin_worker_runtime(&executor, &path)
        .await
        .unwrap();
    assert!(runtime_verified(&journal, &executor).await);
    executor.shutdown().await.unwrap();
    journal.close_worker_runtime(&executor).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_waits_for_the_actual_png_reader_before_minting_evidence() {
    let f = setup().await;
    let (_helper, runtime) = crate::computer_runtime::tests::fixture("good");
    let executor = Executor::new(f._root.path())
        .unwrap()
        .with_computer(Some(runtime.clone()));
    f.journal
        .begin_worker_runtime(&executor, &f.journal.directory)
        .await
        .unwrap();
    cleanup_ready(&f, f.epoch).await;
    accept(&f, true).await;
    let binding = pulse_core::ComputerBinding {
        operation_id: "fixture-image".into(),
        task_id: "task".into(),
        revision: 1,
        device: f.journal.device.clone(),
        epoch: f.epoch,
    };
    runtime.observe(&binding).await.unwrap();
    let reader = crate::computer_runtime::tests::image_reader(&runtime, "task").await;
    let directory = reader.path.clone();
    let context = forget(&f).await;
    let header = context.read_header.clone().unwrap();
    f.journal
        .accept_worker_erasure(&header, &context)
        .await
        .unwrap();
    let journal = f.journal.clone();
    let worker = executor.clone();
    let source = context.clone();
    let cleanup = tokio::spawn(async move {
        journal
            .complete_worker_cleanup(&worker, &header, &source, true)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while reader.valid().is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!cleanup.is_finished());
    assert!(directory.exists());
    assert!(
        !f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    drop(reader);
    cleanup.await.unwrap().unwrap();
    assert!(!directory.exists());
    let frame = cleanup_frame(&f).await;
    f.hub
        .receive(&f.journal.device, &frame, crate::now())
        .await
        .unwrap();
    assert!(
        f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    executor.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&executor).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_sqlite_restore_cannot_clear_a_crashed_runtime() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("local");
    let (journal, lock) = Journal::open(&path, "https://pulse.example/", root.path())
        .await
        .unwrap();
    let first = Executor::new(root.path()).unwrap();
    journal.begin_worker_runtime(&first, &path).await.unwrap();
    let backup = root.path().join("snapshot.db");
    sqlx::query("VACUUM INTO ?")
        .bind(backup.to_str().unwrap())
        .execute(&journal.db)
        .await
        .unwrap();
    let second = Executor::new(root.path()).unwrap();
    journal.begin_worker_runtime(&second, &path).await.unwrap();
    assert!(!runtime_verified(&journal, &second).await);
    journal.core.clone().close().await;
    drop(journal);
    drop(lock);
    std::fs::copy(&backup, path.join("device.db")).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        path.join("device.db"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let (journal, _lock) = Journal::open(&path, "https://pulse.example/", root.path())
        .await
        .unwrap();
    let third = Executor::new(root.path()).unwrap();
    journal.begin_worker_runtime(&third, &path).await.unwrap();
    assert!(!runtime_verified(&journal, &third).await);
    third.shutdown().await.unwrap();
    journal.close_worker_runtime(&third).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_reaps_a_real_controlled_writer_before_the_server_receipt() {
    let f = setup().await;
    let (helper, runtime) = crate::computer_runtime::tests::fixture("cache-hang");
    let executor = Executor::new(f._root.path())
        .unwrap()
        .with_computer(Some(runtime.clone()));
    f.journal
        .begin_worker_runtime(&executor, &f.journal.directory)
        .await
        .unwrap();
    cleanup_ready(&f, f.epoch).await;
    accept(&f, true).await;
    let binding = pulse_core::ComputerBinding {
        operation_id: "writer".into(),
        task_id: "task".into(),
        revision: 1,
        device: f.journal.device.clone(),
        epoch: f.epoch,
    };
    let worker = runtime.clone();
    let writing = tokio::spawn(async move { worker.observe(&binding).await });
    let directory = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(path) = std::fs::read_to_string(helper.path().join("cache-hang.cache")) {
                break std::path::PathBuf::from(path);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(helper.path().join("cache-hang.pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(directory.exists());
    assert!(!writing.is_finished());
    let context = forget(&f).await;
    let header = context.read_header.as_ref().unwrap();
    f.journal
        .accept_worker_erasure(header, &context)
        .await
        .unwrap();
    f.journal
        .complete_worker_cleanup(&executor, header, &context, true)
        .await
        .unwrap();
    assert!(writing.await.unwrap().is_err());
    assert!(!directory.exists());
    let pid = rustix::process::Pid::from_raw(pid).unwrap();
    assert_eq!(
        rustix::process::test_kill_process(pid),
        Err(rustix::io::Errno::SRCH)
    );
    assert_eq!(
        rustix::process::test_kill_process_group(pid),
        Err(rustix::io::Errno::SRCH)
    );
    let frame = cleanup_frame(&f).await;
    f.hub
        .receive(&f.journal.device, &frame, crate::now())
        .await
        .unwrap();
    assert!(
        f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    executor.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&executor).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_real_socket_negotiates_and_status_requires_authentication() {
    use futures_util::{SinkExt, StreamExt};
    use std::future::IntoFuture;
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
    let f = setup().await;
    let executor = cleanup_runtime(&f).await;
    let context = forget(&f).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server =
        tokio::spawn(axum::serve(listener, crate::server::router(f.hub.clone())).into_future());
    let status_url = format!(
        "http://{address}/v1/worker-cleanup/{}",
        context.operation_id
    );
    let http = reqwest::Client::new();
    assert_eq!(http.get(&status_url).send().await.unwrap().status(), 404);
    let pending: serde_json::Value = http
        .get(&status_url)
        .bearer_auth(&f.credential.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(pending["owned_runtime_cleanup"], "unverified");
    let mut request = format!("ws://{address}/v1/devices/connect")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", f.credential.token).parse().unwrap(),
    );
    let (mut socket, response) = tokio_tungstenite::connect_async(request).await.unwrap();
    assert_eq!(
        response.headers().get("x-pulse-worker-cleanup").unwrap(),
        "1"
    );
    socket
        .send(Message::Text(
            serde_json::to_string(&ClientWire::Hello {
                device: f.journal.device.clone(),
                cursor: "0".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            match parse::<ServerWire>(text.as_bytes()).unwrap() {
                ServerWire::Welcome { .. } => {
                    for message in [
                        ClientWire::WorkerErasureReady,
                        ClientWire::WorkerCleanupReady { version: 1 },
                    ] {
                        socket
                            .send(Message::Text(
                                serde_json::to_string(&message).unwrap().into(),
                            ))
                            .await
                            .unwrap();
                    }
                }
                ServerWire::ErasedWork { header, context } => {
                    f.journal
                        .accept_worker_erasure(&header, &context)
                        .await
                        .unwrap();
                    f.journal
                        .complete_worker_cleanup(&executor, &header, &context, true)
                        .await
                        .unwrap();
                    let envelope = cleanup_frame(&f).await;
                    socket
                        .send(Message::Text(
                            serde_json::to_string(&ClientWire::Data {
                                envelope: Box::new(envelope.clone()),
                            })
                            .unwrap()
                            .into(),
                        ))
                        .await
                        .unwrap();
                    break envelope;
                }
                ServerWire::Data {
                    envelope,
                    permitted,
                } => {
                    f.journal
                        .accept(&envelope, permitted, number(&envelope.epoch).unwrap(), true)
                        .await
                        .unwrap();
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            if let ServerWire::Ack { sequence, id } = parse::<ServerWire>(text.as_bytes()).unwrap()
            {
                assert_eq!(sequence, frame.sequence);
                assert_eq!(id, frame.id);
                f.journal
                    .ack(number(&sequence).unwrap(), &id)
                    .await
                    .unwrap();
                break;
            }
        }
    })
    .await
    .unwrap();
    let response = http
        .get(&status_url)
        .bearer_auth(&f.credential.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let status: serde_json::Value = response.json().await.unwrap();
    assert_eq!(status["owned_runtime_cleanup"], "verified");
    socket.close(None).await.unwrap();
    server.abort();
    let _ = server.await;
    executor.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&executor).await.unwrap();
}

#[tokio::test]
#[ignore = "explicit owned Chrome cleanup receipt; set PULSE_TEST_BROWSER_DRIVER; no provider calls"]
async fn worker_cleanup_real_chrome_receipt_follows_owned_profile_removal_and_keeps_b() {
    use std::future::IntoFuture;
    let f = setup().await;
    let driver =
        std::env::var_os("PULSE_TEST_BROWSER_DRIVER").expect("explicit installed driver required");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let page = axum::Router::new().route(
        "/",
        axum::routing::get(|| async {
            axum::response::Html("<!doctype html><title>Cleanup fixture</title><p>Owned source</p>")
        }),
    );
    let server = tokio::spawn(axum::serve(listener, page).into_future());
    let directory = f.journal.directory.join("browser");
    let runtime = crate::browser_runtime::BrowserRuntime::new(
        driver.into(),
        crate::browser_network::NetworkPolicy::new(Some(&origin)).unwrap(),
        directory.clone(),
    )
    .unwrap()
    .with_interactions(true);
    let executor = Executor::new(f._root.path())
        .unwrap()
        .with_browser(Some(runtime.clone()));
    f.journal
        .begin_worker_runtime(&executor, &f.journal.directory)
        .await
        .unwrap();
    assert!(runtime_verified(&f.journal, &executor).await);
    cleanup_ready(&f, f.epoch).await;
    accept(&f, true).await;
    let binding = |task: &str| crate::browser_session::WorkBinding {
        operation_id: format!("inspect-{task}"),
        task_id: task.into(),
        revision: "1".into(),
        device: f.journal.device.clone(),
        epoch: f.epoch,
    };
    let url = origin.parse().unwrap();
    runtime.inspect(&url, &binding("task")).await.unwrap();
    let second = runtime.inspect(&url, &binding("B")).await.unwrap();
    let ReadOutcome::Completed {
        observation: Some(second),
        ..
    } = second.outcome
    else {
        panic!("B observation required")
    };
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 2);
    let context = forget(&f).await;
    let header = context.read_header.as_ref().unwrap();
    f.journal
        .accept_worker_erasure(header, &context)
        .await
        .unwrap();
    assert!(
        !f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    f.journal
        .complete_worker_cleanup(&executor, header, &context, true)
        .await
        .unwrap();
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
    {
        let sessions = runtime.sessions.lock().await;
        assert_eq!(sessions.len(), 1);
        assert!(sessions.contains_key(&second.session_id));
    }
    assert!(
        !runtime
            .processes
            .source_idle(&crate::owned_process::SourceScope::new("B", "1").unwrap())
            .unwrap()
    );
    let frame = cleanup_frame(&f).await;
    f.hub
        .receive(&f.journal.device, &frame, crate::now())
        .await
        .unwrap();
    assert!(
        f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    executor.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&executor).await.unwrap();
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn worker_cleanup_binds_the_dedicated_control_after_the_read_was_acked() {
    let f = setup().await;
    let executor = cleanup_runtime(&f).await;
    cleanup_ready(&f, f.epoch).await;
    accept(&f, true).await;
    let context = forget(&f).await;
    f.hub
        .flush_worker_erasures(&f.journal.device, f.epoch)
        .await
        .unwrap();
    let mut control = None;
    for item in f.hub.deliveries(&f.journal.device).await.unwrap() {
        if let Delivery::Data(envelope) = item {
            f.journal
                .accept(&envelope, false, f.epoch, true)
                .await
                .unwrap();
            if let ServerPayload::EraseWork { context } = &envelope.payload {
                let header = pulse_core::ErasedProjection::from_value(
                    &serde_json::to_value(&envelope).unwrap(),
                )
                .unwrap();
                f.journal
                    .complete_worker_cleanup(&executor, &header, context, true)
                    .await
                    .unwrap();
                control = Some(header);
            }
        }
    }
    assert_ne!(control.as_ref(), context.read_header.as_ref());
    let frame = cleanup_frame(&f).await;
    let DevicePayload::WorkerCleanup { receipt } = &frame.payload else {
        panic!()
    };
    assert_eq!(Some(&receipt.control), control.as_ref());
    f.hub
        .receive(&f.journal.device, &frame, crate::now())
        .await
        .unwrap();
    assert!(
        f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    executor.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&executor).await.unwrap();
}

#[tokio::test]
async fn worker_cleanup_previously_verified_intent_survives_a_local_crash_and_reopen() {
    let mut f = setup().await;
    let executor = cleanup_runtime(&f).await;
    cleanup_ready(&f, f.epoch).await;
    accept(&f, true).await;
    let context = forget(&f).await;
    let header = context.read_header.as_ref().unwrap();
    f.journal
        .accept_worker_erasure(header, &context)
        .await
        .unwrap();
    f.journal
        .complete_worker_cleanup(&executor, header, &context, true)
        .await
        .unwrap();
    let before = cleanup_frame(&f).await;
    f.journal.core.clone().close().await;
    drop(f._lock);
    let (journal, lock) = Journal::open(
        &f._root.path().join("worker"),
        "https://pulse.example/",
        f._root.path(),
    )
    .await
    .unwrap();
    f.journal = journal;
    f._lock = lock;
    let replacement = cleanup_runtime(&f).await;
    assert!(!runtime_verified(&f.journal, &replacement).await);
    let after = cleanup_frame(&f).await;
    assert_eq!(
        serde_json::to_value(&before).unwrap(),
        serde_json::to_value(&after).unwrap()
    );
    f.hub
        .receive(&f.journal.device, &after, crate::now())
        .await
        .unwrap();
    assert!(
        f.hub
            .worker_cleanup_verified(&context.operation_id)
            .await
            .unwrap()
    );
    assert!(f.journal.work_erased(&context.operation_id).await.unwrap());
    f.journal
        .ack(number(&after.sequence).unwrap(), &after.id)
        .await
        .unwrap();
    assert!(f.journal.deliveries().await.unwrap().is_empty());
    replacement.shutdown().await.unwrap();
    f.journal.close_worker_runtime(&replacement).await.unwrap();
}
