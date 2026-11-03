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
