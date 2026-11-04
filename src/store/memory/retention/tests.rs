use super::*;
use crate::{Origin, Outcome, Versions};
use std::process::Stdio;
const NOW: i64 = 1_800_000_000_000;
const PRIVATE: &str = "OWNED_RETENTION_CRASH_PRIVATE_SOURCE";

async fn create(store: &Store, id: &str, goal: &str, now: i64) {
    let event=store.ingest(Origin::Human,"fixture",id,json!({"commands":[{"kind":"create","id":id,"goal":goal,"scope":"pulse:read","priority":0,"requires_device":false}]}),now).await.unwrap();
    assert!(matches!(
        store.route(&event.event_id).await.unwrap(),
        Outcome::Applied { .. }
    ));
}

#[tokio::test]
#[ignore = "owned child for process-exit retention recovery"]
async fn child_exits_at_a_durable_retention_boundary() {
    let root = std::path::PathBuf::from(std::env::var_os("PULSE_TEST_RETENTION_ROOT").unwrap())
        .canonicalize()
        .unwrap();
    let store = Store::open(root.join("db"), "fixture").await.unwrap();
    create(&store, "A", PRIVATE, NOW).await;
    let run = store
        .claim_next(&Versions::fixture(), NOW + 1)
        .await
        .unwrap()
        .unwrap();
    store
        .reserve(&run.token, "held-fee", 20_000, "fixture", NOW + 2)
        .await
        .unwrap();
    let event = store
        .ingest(
            Origin::Human,
            "fixture",
            "cancel-A",
            json!({"commands":[{"kind":"cancel","id":"A","expected_revision":"1"}]}),
            NOW + 3,
        )
        .await
        .unwrap();
    store.route(&event.event_id).await.unwrap();
    create(&store, "B", "UNRELATED_LIVE_SOURCE", NOW + 4).await;
    store.maintain(NOW + 3 + TASK_RETENTION_MS).await.unwrap();
    panic!("expected an owned process exit before backup");
}

#[tokio::test]
async fn actual_process_exits_before_and_after_sql_commit_recover_without_private_text_or_refund() {
    for point in [
        "restriction-published",
        "payload-erased",
        "retention-committed",
    ] {
        let root = tempfile::Builder::new()
            .prefix("pulse-retention-crash-")
            .tempdir()
            .unwrap();
        std::fs::write(root.path().join("KEEP"), b"HUMAN_FILE").unwrap();
        let mut cmd = tokio::process::Command::new(std::env::current_exe().unwrap());
        cmd.args([
            "--exact",
            "store::memory::retention::tests::child_exits_at_a_durable_retention_boundary",
            "--ignored",
            "--nocapture",
        ])
        .env_clear()
        .env("PULSE_TEST_RETENTION_ROOT", root.path())
        .env("PULSE_TEST_RETENTION_POINT", point)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            cmd.env("LLVM_PROFILE_FILE", profile);
        }
        let mut child = cmd.spawn().unwrap();
        let status =
            match tokio::time::timeout(std::time::Duration::from_secs(20), child.wait()).await {
                Ok(status) => status.unwrap(),
                Err(_) => {
                    child.kill().await.unwrap();
                    let _ = child.wait().await;
                    panic!("owned retention child timeout at {point}");
                }
            };
        assert_eq!(status.code(), Some(73), "{point}");
        let store = Store::open(root.path().join("db"), "fixture")
            .await
            .unwrap();
        assert_eq!(
            store.task("A").await.unwrap().reason.as_deref(),
            Some("task_retention_expired"),
            "{point}"
        );
        assert_eq!(store.task("B").await.unwrap().goal, "UNRELATED_LIVE_SOURCE");
        let fee: i64 = sqlx::query_scalar("SELECT charged FROM budget WHERE call_id='held-fee'")
            .fetch_one(&store.application_pool())
            .await
            .unwrap();
        assert_eq!(fee, 20_000);
        assert_eq!(
            std::fs::read(root.path().join("KEEP")).unwrap(),
            b"HUMAN_FILE"
        );
        let bytes = std::fs::read(root.path().join("db")).unwrap();
        assert!(
            !bytes
                .windows(PRIVATE.len())
                .any(|v| v == PRIVATE.as_bytes()),
            "{point}"
        );
        store.close().await;
    }
}
