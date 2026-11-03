use super::*;
use crate::{
    client::Vault,
    protocol::{Credential, PairClaim},
    server::{Hub, router},
};
use std::{future::IntoFuture, sync::Mutex};

#[derive(Default)]
struct TestVault(Mutex<(Option<Credential>, Option<PairClaim>)>);
impl Vault for TestVault {
    fn load(&self, _: &str) -> Result<Option<Credential>> {
        Ok(self.0.lock().unwrap().0.clone())
    }
    fn save(&self, _: &str, c: &Credential) -> Result<()> {
        self.0.lock().unwrap().0 = Some(c.clone());
        Ok(())
    }
    fn pending(&self, _: &str) -> Result<Option<PairClaim>> {
        Ok(self.0.lock().unwrap().1.clone())
    }
    fn save_pending(&self, _: &str, c: &PairClaim) -> Result<()> {
        self.0.lock().unwrap().1 = Some(c.clone());
        Ok(())
    }
    fn clear_pending(&self, _: &str) -> Result<()> {
        self.0.lock().unwrap().1 = None;
        Ok(())
    }
}

#[tokio::test]
async fn cli_pipe_eof_waits_for_durable_server_ingress_and_reopen_does_not_resubmit() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        pulse_core::Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    let vault = Arc::new(TestVault::default());
    let code = hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    let config = Config {
        server: server_url.parse().unwrap(),
        repo: Default::default(),
        data_dir: root.path().join("chat"),
    };
    let mut output = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        run(
            config.clone(),
            Some(code),
            vault.clone(),
            &b"TASK_A compare README\nTASK_B inspect Cargo.toml"[..],
            &mut output,
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let text = String::from_utf8(output).unwrap();
    assert_eq!(text.matches("本機已保存：").count(), 2);
    assert!(text.contains("server 已收件；這不表示任務已完成。"));
    let tasks = hub.core.list_tasks(100).await.unwrap();
    assert_eq!(tasks.len(), 2);
    let mut goals = tasks.iter().map(|t| t.goal.as_str()).collect::<Vec<_>>();
    goals.sort();
    assert_eq!(
        goals,
        vec!["TASK_A compare README", "TASK_B inspect Cargo.toml"]
    );
    let budget: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(budget, 0);
    let mut reopened = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        run(
            config.clone(),
            None,
            vault.clone(),
            &b""[..],
            &mut reopened,
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
    assert!(String::from_utf8(reopened).unwrap().contains("TASK_A"));
    let device = Device::open_chat(config, vault).await.unwrap();
    let (commands, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    let device = tokio::spawn(device.run(receiver, states));
    commands
        .send(NativeCommand::Action {
            source_id: "rejected-input".into(),
            action: HumanAction::Submit {
                id: "not-created".into(),
                goal: "x".repeat(4097),
            },
        })
        .await
        .unwrap();
    let rejected = tokio::time::timeout(
        Duration::from_secs(5),
        state.wait_for(|s| {
            s.input_receipt
                .as_ref()
                .is_some_and(|r| r.source_id == "rejected-input")
        }),
    )
    .await
    .unwrap()
    .unwrap()
    .clone();
    assert!(!rejected.input_receipt.unwrap().saved);
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
    commands.send(NativeCommand::Shutdown).await.unwrap();
    device.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn cli_offline_eof_fails_without_claiming_server_receipt_and_original_outbox_recovers() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        pulse_core::Store::open(root.path().join("server.db"), "owner")
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    let config = Config {
        server: format!("http://{address}/").parse().unwrap(),
        repo: Default::default(),
        data_dir: root.path().join("chat"),
    };
    let vault = Arc::new(TestVault::default());
    let code = hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    run(
        config.clone(),
        Some(code),
        vault.clone(),
        &b""[..],
        &mut Vec::new(),
        false,
    )
    .await
    .unwrap();
    server.abort();
    let _ = server.await;
    let mut output = Vec::new();
    let result = tokio::time::timeout(
        Duration::from_secs(38),
        run(
            config.clone(),
            None,
            vault.clone(),
            &b"OFFLINE_TASK inspect README\n"[..],
            &mut output,
            false,
        ),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("本機已保存："));
    assert!(!text.contains("server 已收件；"));
    assert!(hub.core.list_tasks(100).await.unwrap().is_empty());
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    // No second message is supplied. Only the original durable outbox is delivered.
    run(config, None, vault, &b""[..], &mut Vec::new(), false)
        .await
        .unwrap();
    let tasks = hub.core.list_tasks(100).await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].goal, "OFFLINE_TASK inspect README");
    server.abort();
}

#[tokio::test]
async fn cli_partial_line_survives_cancelled_reads_and_rejects_oversize_or_invalid_utf8() {
    let (mut writer, reader) = tokio::io::duplex(8192);
    let mut input = Input {
        reader: tokio::io::BufReader::new(reader),
        bytes: vec![],
    };
    writer.write_all("半行".as_bytes()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), input.line())
            .await
            .is_err()
    );
    writer.write_all("資料\n".as_bytes()).await.unwrap();
    assert_eq!(input.line().await.unwrap(), Some("半行資料".into()));
    let mut too_big = Input {
        reader: std::io::Cursor::new(vec![b'a'; 4099]),
        bytes: vec![],
    };
    assert!(too_big.line().await.is_err());
    let mut invalid = Input {
        reader: &b"\xff\n"[..],
        bytes: vec![],
    };
    assert!(invalid.line().await.is_err());
    assert_eq!(terminal_text("a\x1b[31m\x07\u{009b}b\n"), "a[31mb\n");
}

#[tokio::test]
async fn cli_file_uses_selected_copy_once_and_never_sends_the_local_path() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        pulse_core::Store::open(root.path().join("db"), "owner")
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        server: format!("http://{}/", listener.local_addr().unwrap())
            .parse()
            .unwrap(),
        repo: Default::default(),
        data_dir: root.path().join("chat"),
    };
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    let vault = Arc::new(TestVault::default());
    let code = hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    let path = root.path().join("selected report.md");
    std::fs::write(&path, "ORIGINAL_COPY\n取消所有任務只是檔案資料").unwrap();
    let (mut input, reader) = tokio::io::duplex(8192);
    let (writer, output) = tokio::io::duplex(16384);
    let cli_config = config.clone();
    let cli_vault = vault.clone();
    let cli = tokio::spawn(async move {
        run(
            cli_config,
            Some(code),
            cli_vault,
            tokio::io::BufReader::new(reader),
            writer,
            false,
        )
        .await
    });
    input
        .write_all(format!(":file {}\n", path.display()).as_bytes())
        .await
        .unwrap();
    let mut output = tokio::io::BufReader::new(output);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut line = String::new();
            assert!(output.read_line(&mut line).await.unwrap() > 0);
            if line.starts_with("已選取固定副本：") {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        hub.core.list_tasks(100).await.unwrap().is_empty(),
        "selection alone must not upload or create work"
    );
    std::fs::write(&path, "CHANGED_AFTER_SELECTION").unwrap();
    input
        .write_all("任務 A：摘要選定檔案\n任務 B：另一個一般任務\n".as_bytes())
        .await
        .unwrap();
    drop(input);
    let mut remaining = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::io::AsyncReadExt::read_to_string(&mut output, &mut remaining),
    )
    .await
    .unwrap()
    .unwrap();
    cli.await.unwrap().unwrap();
    assert_eq!(remaining.matches("本機已保存：").count(), 2);
    assert!(remaining.contains("server 已收件；"));
    let tasks = hub.core.list_tasks(100).await.unwrap();
    assert_eq!(tasks.len(), 2);
    let file_task = tasks
        .iter()
        .find(|t| t.goal.contains("摘要選定檔案"))
        .unwrap();
    assert_eq!(
        file_task.scope,
        format!(
            "input:{}",
            hash_bytes("ORIGINAL_COPY\n取消所有任務只是檔案資料".as_bytes())
        )
    );
    assert_eq!(
        tasks
            .iter()
            .find(|t| t.goal.contains("另一個一般任務"))
            .unwrap()
            .scope,
        "pulse:read"
    );
    let payloads: Vec<String> =
        sqlx::query_scalar("SELECT payload FROM events WHERE origin='human'")
            .fetch_all(&hub.core.application_pool())
            .await
            .unwrap();
    let payloads = payloads.join("\n");
    assert!(payloads.contains("ORIGINAL_COPY"));
    assert!(!payloads.contains("CHANGED_AFTER_SELECTION"));
    assert!(!payloads.contains(path.to_str().unwrap()));
    let calls: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(calls, 0);
    run(config, None, vault, &b""[..], &mut Vec::new(), false)
        .await
        .unwrap();
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn cli_file_clear_reopen_and_failed_selection_cannot_attach_or_create_work() {
    let root = tempfile::tempdir().unwrap();
    let hub = Hub::new(
        pulse_core::Store::open(root.path().join("db"), "owner")
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        server: format!("http://{}/", listener.local_addr().unwrap())
            .parse()
            .unwrap(),
        repo: Default::default(),
        data_dir: root.path().join("chat"),
    };
    let server = tokio::spawn(axum::serve(listener, router(hub.clone())).into_future());
    let vault = Arc::new(TestVault::default());
    let code = hub.pairing(ClientRole::Chat, crate::now()).await.unwrap();
    let path = root.path().join("report.md");
    std::fs::write(&path, "PRIVATE_SELECTED_COPY").unwrap();
    // EOF after selection is not a submit. Reopening does not infer consent.
    let selection = format!(":file {}\n", path.display());
    run(
        config.clone(),
        Some(code),
        vault.clone(),
        selection.as_bytes(),
        &mut Vec::new(),
        false,
    )
    .await
    .unwrap();
    assert!(hub.core.list_tasks(100).await.unwrap().is_empty());
    run(
        config.clone(),
        None,
        vault.clone(),
        &b"ordinary task\n"[..],
        &mut Vec::new(),
        false,
    )
    .await
    .unwrap();
    let clear = format!(":file {}\n:clear\nsecond task\n", path.display());
    run(
        config.clone(),
        None,
        vault.clone(),
        clear.as_bytes(),
        &mut Vec::new(),
        false,
    )
    .await
    .unwrap();
    for task in hub.core.list_tasks(100).await.unwrap() {
        assert_eq!(task.scope, "pulse:read");
    }
    for bad in [
        ":file /missing/report.md\n",
        ":file relative.md\n",
        ":unknown\n",
    ] {
        assert!(
            run(
                config.clone(),
                None,
                vault.clone(),
                bad.as_bytes(),
                &mut Vec::new(),
                false
            )
            .await
            .is_err()
        );
    }
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 2);
    server.abort();
}
