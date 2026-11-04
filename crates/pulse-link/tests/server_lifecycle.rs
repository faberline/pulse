//! Actual server processes. Credentials here are synthetic protocol fixtures,
//! held only in this test; this does not accept NativeVault or a desktop flow.
use futures_util::{SinkExt, StreamExt};
use pulse_core::Store;
use pulse_link::{now, protocol::*, server::Hub};
use std::{
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::{Child, Command},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
struct Server {
    child: Child,
    log: PathBuf,
}

fn listener_origin(log: &str) -> Option<String> {
    log.split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .filter_map(|line| line.strip_prefix("Pulse listener: http://"))
        .find_map(|line| {
            let address: std::net::SocketAddr = line.split_whitespace().next()?.parse().ok()?;
            (address.ip().is_loopback() && address.port() != 0).then(|| format!("http://{address}"))
        })
}

#[test]
fn fragmented_listener_records_never_become_partial_endpoints() {
    let record = "Pulse listener: http://127.0.0.1:12345 (loopback; owned fixture)\n";
    // Every possible short read, including valid-but-incomplete IPs and ports.
    for end in 0..record.len() {
        assert_eq!(listener_origin(&record[..end]), None, "prefix {end}");
    }
    assert_eq!(
        listener_origin(record).as_deref(),
        Some("http://127.0.0.1:12345")
    );
    assert_eq!(
        listener_origin(&format!("owned diagnostic\n{record}")),
        listener_origin(record)
    );
    for invalid in [
        "Pulse listener: http://127.0.0.1\n",
        "Pulse listener: http://127.0.0.1:0\n",
        "Pulse listener: http://198.51.100.1:12345\n",
        "Pulse listener: http://127.0.0.1:12345oops\n",
    ] {
        assert_eq!(listener_origin(invalid), None);
    }
}

async fn start(database: &Path, port: u16) -> (Server, String) {
    let log = database.with_file_name(format!("server-{}.log", uuid::Uuid::new_v4()));
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&log)
        .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pulse-server"))
        .args(["serve", database.to_str().unwrap(), &port.to_string()])
        .env_remove("OPENAI_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .env("PULSE_PROVIDER", "openai")
        .env("PULSE_ENV_FILE", database.with_extension("no-env"))
        .stdout(Stdio::null())
        .stderr(Stdio::from(output))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    // Keep stderr open for the child's whole lifetime. Closing a startup pipe
    // would make later diagnostics fail with a broken pipe.
    let origin = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let text = std::fs::read_to_string(&log).unwrap();
            if let Some(origin) = listener_origin(&text) {
                break origin;
            }
            assert!(child.try_wait().unwrap().is_none(), "{text}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let origin = match origin {
        Ok(origin) => origin,
        Err(_) => {
            child.kill().await.unwrap();
            panic!("server startup unconfirmed");
        }
    };
    let live = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap()
        .get(format!("{origin}/health/live"))
        .send()
        .await
        .unwrap();
    assert_eq!(live.text().await.unwrap(), "live");
    (Server { child, log }, origin)
}

async fn stop(server: &mut Server) {
    let child = &mut server.child;
    let pid = rustix::process::Pid::from_raw(child.id().unwrap() as i32).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(status) => assert!(
            status.unwrap().success(),
            "{}",
            std::fs::read_to_string(&server.log).unwrap()
        ),
        Err(_) => {
            child.kill().await.unwrap();
            panic!("server did not exit within five seconds after SIGINT");
        }
    }
}

async fn send(socket: &mut Socket, frame: &ClientWire) {
    socket
        .send(Message::Text(serde_json::to_string(frame).unwrap().into()))
        .await
        .unwrap();
}
async fn receive(socket: &mut Socket) -> ServerWire {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = frame {
            let frame: ServerWire = parse(text.as_bytes()).unwrap();
            if let ServerWire::Data { envelope, .. } = &frame {
                send(
                    socket,
                    &ClientWire::Ack {
                        sequence: envelope.sequence.clone(),
                        id: envelope.id.clone(),
                    },
                )
                .await;
            }
            return frame;
        }
    }
}
async fn connect(origin: &str, credential: &Credential) -> (Socket, u64) {
    let url = format!("{}/v1/devices/connect", origin.replacen("http", "ws", 1));
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", credential.token).parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    send(
        &mut socket,
        &ClientWire::Hello {
            device: credential.device.clone(),
            cursor: "0".into(),
        },
    )
    .await;
    let ServerWire::Welcome { epoch, .. } = receive(&mut socket).await else {
        panic!("welcome required");
    };
    (socket, epoch.parse().unwrap())
}
async fn ack(socket: &mut Socket, envelope: &Envelope<DevicePayload>) {
    loop {
        match receive(socket).await {
            ServerWire::Ack { sequence, id }
                if sequence == envelope.sequence && id == envelope.id =>
            {
                return;
            }
            ServerWire::Error { .. } => panic!("server rejected the original fixture input"),
            _ => {}
        }
    }
}
async fn closed(socket: &mut Socket) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(Ok(frame)) = socket.next().await {
            if matches!(frame, Message::Close(_)) {
                return;
            }
        }
    })
    .await
    .unwrap();
}

async fn offline_command(mode: &str, database: &Path, value: Option<&Path>) -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulse-server"));
    command.arg(mode).arg(database);
    if let Some(value) = value {
        command.arg(value);
    }
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        command
            .env_remove("OPENAI_API_KEY")
            .env_remove("ANTHROPIC_API_KEY")
            .env("PULSE_ENV_FILE", database.with_extension("no-env"))
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    let text = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "{text}");
    assert!(output.stdout.is_empty());
    text
}

async fn exclusive(database: &Path) -> pulse_core::DatabaseMaintenance {
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match pulse_core::DatabaseMaintenance::try_acquire(database) {
            Ok(lease) => return lease,
            Err(error) if tokio::time::Instant::now() >= until => panic!("{error}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

#[tokio::test]
async fn real_cli_restore_and_server_require_fresh_generation_pairing_without_replaying_work() {
    for mode in ["readable", "missing", "corrupt"] {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("server.db");
        let hub = Hub::new(Store::open(&database, "pulse-owner").await.unwrap());
        let mut old = Vec::new();
        for role in [ClientRole::Chat, ClientRole::LocalExecutor] {
            let code = hub.pairing(role, now()).await.unwrap();
            old.push(
                hub.redeem(
                    PairRequest {
                        code,
                        role,
                        device: uuid::Uuid::new_v4().to_string(),
                    },
                    now(),
                )
                .await
                .unwrap(),
            );
        }
        let backup = hub.core.maintain(now()).await.unwrap().backup.unwrap();
        // This identity exists only after the backup. It must also be retired
        // when its original records are still readable at cutover.
        hub.revoke(&old[0].device).await.unwrap();
        let code = hub.pairing(ClientRole::Chat, now()).await.unwrap();
        old.push(
            hub.redeem(
                PairRequest {
                    code,
                    role: ClientRole::Chat,
                    device: uuid::Uuid::new_v4().to_string(),
                },
                now(),
            )
            .await
            .unwrap(),
        );
        hub.core.clone().close().await;
        drop(hub);
        let lease = exclusive(&database).await;
        match mode {
            "missing" => std::fs::remove_file(&database).unwrap(),
            "corrupt" => std::fs::write(&database, b"owned unreadable original").unwrap(),
            _ => {}
        }
        drop(lease);
        let restored = offline_command("restore-backup", &database, Some(&backup)).await;
        assert!(restored.contains("No tasks started."));
        assert!(restored.contains(if mode == "readable" {
            "records imported: true"
        } else {
            "records imported: false"
        }));
        assert!(!restored.contains(database.to_str().unwrap()));
        let chat_code = root.path().join("chat.code");
        offline_command("pair-chat", &database, Some(&chat_code)).await;
        let (mut server, origin) = start(&database, 0).await;
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let mut generation = None;
        for credential in &old {
            let response = http
                .get(format!("{origin}/v1/device-pairings/identity"))
                .bearer_auth(&credential.token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 401);
            let actual = response
                .headers()
                .get("x-pulse-restore-generation")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            assert!(generation.as_ref().is_none_or(|g| g == &actual));
            generation = Some(actual);
        }
        let mut claim = PairClaim {
            code: std::fs::read_to_string(&chat_code).unwrap(),
            attempt_id: uuid::Uuid::new_v4().to_string(),
            device: uuid::Uuid::new_v4().to_string(),
            role: ClientRole::Chat,
            token: "73".repeat(32),
            server_generation: None,
        };
        let claim_url = format!("{origin}/v1/device-pairings/claim");
        assert_eq!(
            http.post(&claim_url)
                .json(&claim)
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        claim.server_generation = generation.clone();
        // A retained token cannot acquire authority under a fresh device ID.
        let fresh_token = claim.token.clone();
        claim.token = old[0].token.clone();
        assert_eq!(
            http.post(&claim_url)
                .json(&claim)
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        claim.token = fresh_token;
        let fresh_device = claim.device.clone();
        claim.device = old[0].device.clone();
        assert_eq!(
            http.post(&claim_url)
                .json(&claim)
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        claim.device = fresh_device;
        let response = http.post(&claim_url).json(&claim).send().await.unwrap();
        assert_eq!(response.status(), 200);
        let credential: Credential = response.json().await.unwrap();
        let (mut chat, _) = connect(&origin, &credential).await;
        let local_code = root.path().join("local.code");
        offline_command("pair-local", &database, Some(&local_code)).await;
        let local_claim = PairClaim {
            code: std::fs::read_to_string(local_code).unwrap(),
            attempt_id: uuid::Uuid::new_v4().to_string(),
            device: uuid::Uuid::new_v4().to_string(),
            role: ClientRole::LocalExecutor,
            token: "74".repeat(32),
            server_generation: generation,
        };
        let response = http
            .post(&claim_url)
            .json(&local_claim)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let local: Credential = response.json().await.unwrap();
        let (mut worker, _) = connect(&origin, &local).await;
        stop(&mut server).await;
        closed(&mut chat).await;
        closed(&mut worker).await;
        let repeated = offline_command("resume-restore", &database, None).await;
        assert!(repeated.contains("already committed"));
        let store = Store::open(&database, "pulse-owner").await.unwrap();
        for query in [
            "SELECT count(*) FROM tasks",
            "SELECT count(*) FROM budget",
            "SELECT count(*) FROM link_reads",
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(query)
                    .fetch_one(&store.application_pool())
                    .await
                    .unwrap(),
                0
            );
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM link_device WHERE revoked=0")
                .fetch_one(&store.application_pool())
                .await
                .unwrap(),
            2
        );
        store.close().await;
    }
}

#[tokio::test]
async fn sigint_stops_an_incomplete_http_body_without_waiting_for_the_client() {
    let root = tempfile::tempdir().unwrap();
    let (mut server, origin) = start(&root.path().join("server.db"), 0).await;
    let address = origin.strip_prefix("http://").unwrap();
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(b"POST /v1/device-pairings/claim HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 1024\r\nExpect: 100-continue\r\n\r\n").await.unwrap();
    // The interim response proves the server has accepted this request and is
    // waiting for its body. A successful TCP write alone would not prove that.
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !response.ends_with(b"\r\n\r\n") {
            response.push(client.read_u8().await.unwrap());
            assert!(response.len() <= 1024);
        }
    })
    .await
    .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 100 Continue"));
    stop(&mut server).await;
    let result = tokio::time::timeout(Duration::from_secs(3), client.read(&mut [0; 1]))
        .await
        .unwrap();
    match result {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        _ => panic!("incomplete request socket remained open"),
    }
}

#[tokio::test]
async fn live_roles_disconnect_and_reconnect_after_sigint_without_replaying_work() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("server.db");
    let hub = Hub::new(Store::open(&database, "pulse-owner").await.unwrap());
    let mut credentials = Vec::new();
    for role in [ClientRole::Chat, ClientRole::LocalExecutor] {
        let code = hub.pairing(role, now()).await.unwrap();
        credentials.push(
            hub.redeem(
                PairRequest {
                    code,
                    role,
                    device: uuid::Uuid::new_v4().to_string(),
                },
                now(),
            )
            .await
            .unwrap(),
        );
    }
    hub.core.clone().close().await;
    drop(hub);
    let (mut server, origin) = start(&database, 0).await;
    let (mut chat, epoch) = connect(&origin, &credentials[0]).await;
    let (mut local, local_epoch) = connect(&origin, &credentials[1]).await;
    let envelope = Envelope::new(
        &credentials[0].device,
        1,
        epoch,
        DevicePayload::Human {
            source_id: "lifecycle-input".into(),
            action: HumanAction::Submit {
                id: "lifecycle-task".into(),
                goal: "Synthetic server lifecycle fixture".into(),
            },
        },
    )
    .unwrap();
    send(
        &mut chat,
        &ClientWire::Data {
            envelope: Box::new(envelope.clone()),
        },
    )
    .await;
    ack(&mut chat, &envelope).await;
    stop(&mut server).await;
    closed(&mut chat).await;
    closed(&mut local).await;
    let port = origin.rsplit(':').next().unwrap().parse().unwrap();
    let (mut server, reopened) = start(&database, port).await;
    assert_eq!(origin, reopened);
    let (mut chat, new_epoch) = connect(&reopened, &credentials[0]).await;
    let (mut local, new_local_epoch) = connect(&reopened, &credentials[1]).await;
    assert!(new_epoch > epoch && new_local_epoch > local_epoch);
    // A lost ingress ACK may cause this exact old-epoch envelope to return.
    send(
        &mut chat,
        &ClientWire::Data {
            envelope: Box::new(envelope.clone()),
        },
    )
    .await;
    ack(&mut chat, &envelope).await;
    stop(&mut server).await;
    closed(&mut chat).await;
    closed(&mut local).await;
    let store = Store::open(&database, "pulse-owner").await.unwrap();
    let tasks = store.list_tasks(100).await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id, "lifecycle-task");
    for query in [
        "SELECT count(*) FROM budget",
        "SELECT count(*) FROM link_reads",
    ] {
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    let devices: i64 = sqlx::query_scalar("SELECT count(*) FROM link_device")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(devices, 2);
    store.close().await;
}
