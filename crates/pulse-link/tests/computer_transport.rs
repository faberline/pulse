//! Controlled transport fixtures, not native capture or Human UI acceptance.
use base64::{Engine, engine::general_purpose::STANDARD};
use pulse_core::{
    ComputerBinding, ComputerImageRef, ComputerObservation, ComputerRect, ComputerSelection,
    ComputerTarget, Store,
};
use pulse_link::{artifact::*, journal::Journal, protocol::*, remote, server::Hub};
use std::future::IntoFuture;

fn selection() -> ComputerSelection {
    ComputerSelection {
        target: ComputerTarget {
            bundle_id: "com.faberline.PulseComputerFixture".into(),
            pid: 42,
            window_id: 7,
        },
        process_launched_at: 1,
    }
}
async fn pair(hub: &Hub, role: ClientRole, device: &str) -> (Credential, u64) {
    let time = pulse_link::now();
    let credential = hub
        .redeem(
            PairRequest {
                code: hub.pairing(role, time).await.unwrap(),
                device: device.into(),
                role,
            },
            time,
        )
        .await
        .unwrap();
    let epoch = hub.begin(device, 0, time).await.unwrap();
    (credential, epoch)
}
async fn send(
    hub: &Hub,
    credential: &Credential,
    epoch: u64,
    sequence: u64,
    payload: DevicePayload,
) -> String {
    let frame = Envelope::new(&credential.device, sequence, epoch, payload).unwrap();
    hub.receive(&credential.device, &frame, pulse_link::now())
        .await
        .unwrap();
    sqlx::query_scalar("SELECT outcome FROM link_inbox WHERE device=? AND sequence=?")
        .bind(&credential.device)
        .bind(sequence as i64)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap()
}
async fn dispatch(hub: &Hub, device: &str) -> (Credential, u64, Envelope<ServerPayload>) {
    let (chat, epoch) = pair(hub, ClientRole::Chat, &uuid::Uuid::new_v4().to_string()).await;
    assert_eq!(
        send(
            hub,
            &chat,
            epoch,
            1,
            DevicePayload::Human {
                source_id: "selection".into(),
                action: HumanAction::SubmitComputer {
                    id: "window".into(),
                    goal: "Read the selected owned fixture window".into(),
                    selection: selection()
                }
            }
        )
        .await,
        "accepted"
    );
    let task = hub.core.task("window").await.unwrap();
    assert_eq!(task.scope, selection().scope().unwrap());
    assert!(!task.authorization_event.is_empty());
    // No installed app/capability UI yet: no model call or budget reservation.
    remote::step(hub, pulse_link::now()).await.unwrap();
    let task = hub.core.task("window").await.unwrap();
    assert_eq!(task.state, pulse_core::State::WaitingDevice);
    assert_eq!(task.model_calls, 0);
    let reserved: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM budget WHERE task_id='window'")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(reserved, 0);
    assert_eq!(task.reason.as_deref(), Some("computer_capability_wait"));
    // Controlled local capability report. Native probes are covered separately.
    let local_epoch: i64 = sqlx::query_scalar("SELECT epoch FROM link_device WHERE id=?")
        .bind(device)
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    let selected_at = pulse_link::now();
    hub.computer_ready(
        device,
        local_epoch as u64,
        vec![pulse_core::ComputerPermit {
            task_id: "window".into(),
            revision: "1".into(),
            selection: selection(),
            selected_at,
            expires_at: selected_at + 900_000,
        }],
        selected_at,
    )
    .await
    .unwrap();
    assert_eq!(
        send(
            hub,
            &chat,
            epoch,
            2,
            DevicePayload::Human {
                source_id: "observe".into(),
                action: HumanAction::Read {
                    task_id: "window".into(),
                    revision: "1".into(),
                    operation: ReadOp::ComputerInspect {
                        selection: selection()
                    }
                }
            }
        )
        .await,
        "accepted"
    );
    let frame = hub
        .outgoing(device)
        .await
        .unwrap()
        .into_iter()
        .find(|frame| matches!(frame.payload, ServerPayload::Read { .. }))
        .unwrap();
    (chat, epoch, frame)
}
fn evidence(frame: &Envelope<ServerPayload>) -> (ReadOutcome, CapturedImage) {
    let ServerPayload::Read {
        operation_id,
        task_id,
        revision,
        ..
    } = &frame.payload
    else {
        panic!()
    };
    let png = STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=").unwrap();
    // Archived reads remain valid after the action lifetime expires.
    let time = pulse_link::now() - 180_000;
    let observation = ComputerObservation {
        binding: ComputerBinding {
            operation_id: operation_id.clone(),
            task_id: task_id.clone(),
            revision: number(revision).unwrap(),
            device: frame.device.clone(),
            epoch: number(&frame.epoch).unwrap(),
        },
        target: selection().target,
        process_launched_at: 1,
        title: "Owned fixture".into(),
        frame: ComputerRect {
            x: 0.,
            y: 0.,
            width: 1.,
            height: 1.,
        },
        elements: vec![],
        truncated: false,
        nonce: uuid::Uuid::new_v4().to_string(),
        observed_at: time,
        expires_at: time + 120_000,
        image: ComputerImageRef {
            digest: hash_bytes(&png),
            length: png.len() as u32,
            width: 1,
            height: 1,
        },
    };
    let artifact = ArtifactRef {
        task_id: task_id.clone(),
        revision: revision.clone(),
        digest: observation.image.digest.clone(),
        length: png.len() as u32,
        url: String::new(),
        computer: Some(ComputerArtifactSource {
            binding: observation.binding.clone(),
            selection: selection(),
        }),
        title: observation.title.clone(),
        observed_at: time,
        width: 1,
        height: 1,
    };
    let mut outcome = ReadOutcome::text("Untrusted fixture data".into(), false);
    if let ReadOutcome::Completed { computer, .. } = &mut outcome {
        *computer = Some(Box::new(observation));
    }
    (outcome, CapturedImage { artifact, png })
}

#[tokio::test]
async fn exact_computer_source_survives_journal_restart_receipt_and_chat_download() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    let (local, lock) = Journal::open(
        &root.path().join("local"),
        "https://pulse.example/",
        root.path(),
    )
    .await
    .unwrap();
    let (credential, epoch) = pair(&hub, ClientRole::LocalExecutor, &local.device).await;
    let (chat, chat_epoch, frame) = dispatch(&hub, &local.device).await;
    assert!(
        hub.permitted(&frame, epoch, pulse_link::now())
            .await
            .unwrap()
    );
    local.accept(&frame, true, epoch, true).await.unwrap();
    local.take_work().await.unwrap().unwrap();
    let (outcome, image) = evidence(&frame);
    let metadata = image.artifact.clone();
    let expected = image.png.clone();
    let operation_id = metadata
        .computer
        .as_ref()
        .unwrap()
        .binding
        .operation_id
        .clone();
    let value = serde_json::to_value(&metadata).unwrap();
    assert!(value.get("url").is_none());
    assert!(value.get("path").is_none());
    local
        .finish_work_image(&frame, outcome, Some(image))
        .await
        .unwrap();
    drop(local);
    drop(lock);
    let (local, _lock) = Journal::open(
        &root.path().join("local"),
        "https://pulse.example/",
        root.path(),
    )
    .await
    .unwrap();
    local.recover().await.unwrap();
    assert!(local.take_work().await.unwrap().is_none());
    let outgoing = local.outgoing().await.unwrap();
    assert_eq!(outgoing.len(), 2);
    for message in outgoing {
        hub.receive(&local.device, &message, pulse_link::now())
            .await
            .unwrap();
        hub.receive(&local.device, &message, pulse_link::now())
            .await
            .unwrap();
        if matches!(message.payload, DevicePayload::ArtifactChunk { .. }) {
            assert!(
                hub.artifact_png(
                    &chat.device,
                    &operation_id,
                    &metadata.digest,
                    pulse_link::now()
                )
                .await
                .is_err()
            );
        }
        local
            .ack(number(&message.sequence).unwrap(), &message.id)
            .await
            .unwrap();
    }
    assert!(local.outgoing().await.unwrap().is_empty());
    // Reopen the authoritative server database, not only an in-memory cache.
    drop(hub);
    let hub = Hub::new(
        Store::open(root.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    assert_eq!(
        hub.artifact_png(
            &chat.device,
            &operation_id,
            &metadata.digest,
            pulse_link::now()
        )
        .await
        .unwrap(),
        expected
    );
    assert!(
        hub.artifact_png(
            &credential.device,
            &operation_id,
            &metadata.digest,
            pulse_link::now()
        )
        .await
        .is_err()
    );
    hub.snapshot(&chat.device, chat_epoch, pulse_link::now())
        .await
        .unwrap();
    let frame = hub.outgoing(&chat.device).await.unwrap().pop().unwrap();
    let ServerPayload::Snapshot { messages, .. } = frame.payload else {
        panic!()
    };
    assert_eq!(
        messages
            .iter()
            .find(|m| m.id == operation_id)
            .unwrap()
            .artifact
            .as_ref(),
        Some(&metadata)
    );
    assert_eq!(hub.core.task("window").await.unwrap().tool_calls, 1);
    assert_eq!(hub.core.task("window").await.unwrap().model_calls, 0);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_url = format!("http://{}/", listener.local_addr().unwrap());
    let server =
        tokio::spawn(axum::serve(listener, pulse_link::server::router(hub.clone())).into_future());
    let cache = root.path().join("chat");
    std::fs::create_dir(&cache).unwrap();
    let downloaded = pulse_link::artifact_client::download(
        &pulse_link::client::Config {
            server: server_url.parse().unwrap(),
            repo: root.path().into(),
            data_dir: cache,
        },
        &chat,
        &operation_id,
        &metadata,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(downloaded.path).unwrap(), expected);
    hub.revoke(&chat.device).await.unwrap();
    assert!(
        hub.artifact_png(
            &chat.device,
            &operation_id,
            &metadata.digest,
            pulse_link::now()
        )
        .await
        .is_err()
    );
    server.abort();
}

#[tokio::test]
async fn journal_refuses_missing_or_cross_bound_computer_evidence_without_persisting_any_chunk() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    let (local, _lock) = Journal::open(
        &root.path().join("local"),
        "https://pulse.example/",
        root.path(),
    )
    .await
    .unwrap();
    let (_, epoch) = pair(&hub, ClientRole::LocalExecutor, &local.device).await;
    let (_, _, frame) = dispatch(&hub, &local.device).await;
    local.accept(&frame, true, epoch, true).await.unwrap();
    local.take_work().await.unwrap().unwrap();
    for attack in [
        "device",
        "epoch",
        "operation",
        "task",
        "revision",
        "target",
        "launch",
        "title",
        "digest",
        "missing_image",
        "missing_observation",
        "future",
        "browser_url",
    ] {
        let (mut outcome, mut image) = evidence(&frame);
        if let ReadOutcome::Completed {
            computer: Some(observation),
            ..
        } = &mut outcome
        {
            match attack {
                "device" => observation.binding.device = uuid::Uuid::new_v4().to_string(),
                "epoch" => observation.binding.epoch += 1,
                "operation" => observation.binding.operation_id = "another-read".into(),
                "task" => observation.binding.task_id = "another-task".into(),
                "revision" => observation.binding.revision += 1,
                "target" => observation.target.window_id += 1,
                "launch" => observation.process_launched_at += 1,
                "title" => observation.title = "Another title".into(),
                "digest" => observation.image.digest = "0".repeat(64),
                "future" => {
                    observation.observed_at = pulse_link::now() + 60_000;
                    observation.expires_at = observation.observed_at + 120_000;
                }
                "browser_url" => image.artifact.url = "https://example.com/".into(),
                _ => {}
            }
        }
        if attack == "missing_observation" {
            outcome = ReadOutcome::text("fixture".into(), false);
        }
        let image = (attack != "missing_image").then_some(image);
        assert!(
            local
                .finish_work_image(&frame, outcome, image)
                .await
                .is_err(),
            "{attack}"
        );
        assert!(local.outgoing().await.unwrap().is_empty(), "{attack}");
    }
}

#[tokio::test]
async fn server_refuses_cross_tool_chunks_and_results_without_the_original_source_image() {
    for attack in [
        "source_device",
        "source_epoch",
        "source_operation",
        "selection",
        "url",
        "browser_only",
        "no_image",
        "result_source",
        "result_image",
    ] {
        let root = tempfile::tempdir().unwrap();
        let hub = Hub::new(
            Store::open(root.path().join("server"), "owner")
                .await
                .unwrap(),
        );
        let (local, epoch) = pair(
            &hub,
            ClientRole::LocalExecutor,
            &uuid::Uuid::new_v4().to_string(),
        )
        .await;
        let (_, _, frame) = dispatch(&hub, &local.device).await;
        let (mut outcome, mut image) = evidence(&frame);
        let operation_id = image
            .artifact
            .computer
            .as_ref()
            .unwrap()
            .binding
            .operation_id
            .clone();
        let mut sequence = 1;
        if attack != "no_image" {
            let source = image.artifact.computer.as_mut().unwrap();
            match attack {
                "source_device" => source.binding.device = uuid::Uuid::new_v4().to_string(),
                "source_epoch" => source.binding.epoch += 1,
                "source_operation" => source.binding.operation_id = "another-read".into(),
                "selection" => source.selection.target.window_id += 1,
                "url" => image.artifact.url = "https://example.com/".into(),
                "browser_only" => {
                    image.artifact.computer = None;
                    image.artifact.url = "https://example.com/".into();
                }
                _ => {}
            }
            let status = send(
                &hub,
                &local,
                epoch,
                sequence,
                DevicePayload::ArtifactChunk {
                    operation_id: operation_id.clone(),
                    artifact: image.artifact,
                    index: 0,
                    data: STANDARD.encode(image.png),
                },
            )
            .await;
            if !attack.starts_with("result_") {
                assert_eq!(status, "rejected", "{attack}");
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_artifacts")
                    .fetch_one(&hub.core.application_pool())
                    .await
                    .unwrap();
                assert_eq!(count, 0, "{attack}");
                continue;
            }
            assert_eq!(status, "accepted");
            sequence += 1;
        }
        if let ReadOutcome::Completed {
            computer: Some(observation),
            ..
        } = &mut outcome
        {
            if attack == "result_source" {
                observation.binding.operation_id = "other".into();
            }
            if attack == "result_image" {
                observation.image.digest = "0".repeat(64);
            }
        }
        assert_eq!(
            send(
                &hub,
                &local,
                epoch,
                sequence,
                DevicePayload::Result {
                    operation_id: operation_id.clone(),
                    task_id: "window".into(),
                    revision: "1".into(),
                    outcome
                }
            )
            .await,
            "rejected",
            "{attack}"
        );
        let result: Option<String> = sqlx::query_scalar("SELECT result FROM link_reads WHERE id=?")
            .bind(&operation_id)
            .fetch_one(&hub.core.application_pool())
            .await
            .unwrap();
        assert!(result.is_none(), "{attack}");
        assert_eq!(
            hub.core.task("window").await.unwrap().state,
            pulse_core::State::Dispatched
        );
    }
}

#[tokio::test]
async fn selected_app_scope_comes_only_from_chat_and_cannot_authorize_another_read() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        Store::open(root.path().join("server"), "owner")
            .await
            .unwrap(),
    );
    let (local, local_epoch) = pair(
        &hub,
        ClientRole::LocalExecutor,
        &uuid::Uuid::new_v4().to_string(),
    )
    .await;
    let (chat, epoch) = pair(&hub, ClientRole::Chat, &uuid::Uuid::new_v4().to_string()).await;
    let create = DevicePayload::Human {
        source_id: "selection".into(),
        action: HumanAction::SubmitComputer {
            id: "window".into(),
            goal: "Read the selected fixture".into(),
            selection: selection(),
        },
    };
    let forgery = Envelope::new(&local.device, 1, local_epoch, create.clone()).unwrap();
    assert!(
        hub.receive(&local.device, &forgery, pulse_link::now())
            .await
            .is_err()
    );
    assert!(hub.core.task("window").await.is_err());
    assert_eq!(
        send(&hub, &chat, epoch, 1, create.clone()).await,
        "accepted"
    );
    assert_eq!(send(&hub, &chat, epoch, 2, create).await, "accepted");
    let original = hub.core.task("window").await.unwrap();
    let mut other = selection();
    other.target.window_id += 1;
    let mut reused = selection();
    reused.process_launched_at += 1;
    for (index, operation) in [
        ReadOp::ComputerInspect { selection: other },
        ReadOp::ComputerInspect { selection: reused },
        ReadOp::GitStatus,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            send(
                &hub,
                &chat,
                epoch,
                index as u64 + 3,
                DevicePayload::Human {
                    source_id: format!("wrong-{index}"),
                    action: HumanAction::Read {
                        task_id: "window".into(),
                        revision: "1".into(),
                        operation
                    }
                }
            )
            .await,
            "rejected"
        );
        assert_eq!(hub.core.task("window").await.unwrap().tool_calls, 0);
    }
    assert_eq!(
        hub.core.task("window").await.unwrap().authorization_event,
        original.authorization_event
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_reads")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}
