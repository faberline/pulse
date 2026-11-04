use pulse_core::{
    DatabaseMaintenance, MEMORY_RETENTION_MS, MemoryCommand, MemoryRequest, Origin, Store,
};
use pulse_link::{maintenance, now};
use std::time::Duration;

#[tokio::test]
async fn live_timer_wakes_for_new_earlier_expiry_without_model_work() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path().join("db"), "owner")
        .await
        .unwrap();
    let worker = tokio::spawn(maintenance::run(store.clone()));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ready: i64 =
                sqlx::query_scalar("SELECT count(*) FROM settings WHERE key='last_backup'")
                    .fetch_one(&store.application_pool())
                    .await
                    .unwrap();
            if ready == 1 {
                break;
            }
            assert!(!worker.is_finished());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let time = now();
    store
        .change_memory(
            Origin::Human,
            &MemoryRequest {
                source_id: uuid::Uuid::new_v4().to_string(),
                source_at: time - MEMORY_RETENTION_MS + 350,
                command: MemoryCommand::Remember {
                    label: "expire soon".into(),
                    body: "owned fixture".into(),
                },
            },
            time,
        )
        .await
        .unwrap();
    // No list_memories call: the maintenance timer must do this removal.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM memories")
                .fetch_one(&store.application_pool())
                .await
                .unwrap();
            if remaining == 0 {
                break;
            }
            assert!(!worker.is_finished());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!worker.is_finished());
    let charged: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&store.application_pool())
        .await
        .unwrap();
    assert_eq!(charged, 0);
    assert!(store.list_tasks(100).await.unwrap().is_empty());
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn server_process_backs_up_once_across_kill_restart_and_checks_quarantine() {
    use std::os::unix::fs::PermissionsExt;
    use std::{path::Path, process::Stdio};
    use tokio::{
        io::{AsyncBufReadExt, BufReader},
        process::{Child, Command},
    };
    async fn start(database: &Path) -> Child {
        let mut child = Command::new(env!("CARGO_BIN_EXE_pulse-server"))
            .args(["serve", database.to_str().unwrap(), "0"])
            .env_remove("OPENAI_API_KEY")
            .env_remove("ANTHROPIC_API_KEY")
            .env("PULSE_PROVIDER", "openai")
            .env("PULSE_ENV_FILE", database.with_extension("no-env"))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            line.starts_with("Pulse listener: http://127.0.0.1:"),
            "{line}"
        );
        let base = line
            .strip_prefix("Pulse listener: ")
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap();
        let response = reqwest::Client::new()
            .get(format!("{base}/health/live"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.text().await.unwrap(), "live");
        assert!(child.try_wait().unwrap().is_none());
        assert!(DatabaseMaintenance::try_acquire(database).is_err());
        child
    }
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("db");
    let mut child = start(&database).await;
    let pairing = tokio::time::timeout(
        Duration::from_secs(30),
        Command::new(env!("CARGO_BIN_EXE_pulse-server"))
            .arg("pair-chat")
            .arg(&database)
            .arg(directory.path().join("pairing-code"))
            .env_remove("OPENAI_API_KEY")
            .env_remove("ANTHROPIC_API_KEY")
            .env("PULSE_ENV_FILE", database.with_extension("no-env"))
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        pairing.status.success(),
        "{}",
        String::from_utf8_lossy(&pairing.stderr)
    );
    assert!(pairing.stdout.is_empty());
    assert!(directory.path().join("pairing-code").is_file());
    assert!(child.try_wait().unwrap().is_none());
    assert!(DatabaseMaintenance::try_acquire(&database).is_err());
    child.start_kill().unwrap();
    child.wait().await.unwrap();
    drop(DatabaseMaintenance::try_acquire(&database).unwrap());
    let backups = directory.path().join("db.backups");
    let files: Vec<_> = std::fs::read_dir(&backups)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1);
    let original = std::fs::read(&files[0]).unwrap();
    let mut child = start(&database).await;
    child.start_kill().unwrap();
    child.wait().await.unwrap();
    assert_eq!(std::fs::read_dir(&backups).unwrap().count(), 1);
    assert_eq!(std::fs::read(&files[0]).unwrap(), original);
    let destination = directory.path().join("checked");
    let output = Command::new(env!("CARGO_BIN_EXE_pulse-server"))
        .arg("check-backup")
        .arg(&database)
        .arg(&files[0])
        .arg(&destination)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("No tasks started."));
    assert!(
        Store::open(destination.join("candidate.db"), "pulse-owner")
            .await
            .is_err()
    );
}
