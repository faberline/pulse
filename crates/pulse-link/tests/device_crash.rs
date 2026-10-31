use pulse_link::{journal::Journal, protocol::*};
use std::{path::Path, process::Command};

#[tokio::test]
#[ignore = "subprocess fixture; parent runs all sixty crash samples"]
async fn device_crash_child() {
    let dir = std::env::var("PULSE_CRASH_DIR").unwrap();
    let cut = std::env::var("PULSE_CRASH_CUT").unwrap();
    let base = Path::new(&dir);
    let (journal, _lock) = Journal::open(&base.join("private"), "https://pulse.example/", base)
        .await
        .unwrap();
    let envelope = Envelope::new(
        &journal.device,
        1,
        4,
        ServerPayload::Read {
            operation_id: "fixture-read".into(),
            task_id: "task".into(),
            revision: "1".into(),
            operation: ReadOp::GitStatus,
        },
    )
    .unwrap();
    journal.accept(&envelope, true, 4, true).await.unwrap();
    if cut != "accepted" {
        let work = journal.take_work().await.unwrap().unwrap();
        std::fs::write(base.join("fixture-count"), b"1").unwrap();
        if cut == "result" {
            journal
                .finish_work(&work, ReadOutcome::text("fixture evidence".into(), false))
                .await
                .unwrap();
        }
    }
    // No destructors or SQLite pool shutdown. This is process loss, not power loss.
    std::process::exit(73);
}

#[tokio::test]
async fn hard_exit_preserves_acceptance_and_result_without_repeating_work() {
    for cut in ["accepted", "running", "result"] {
        for _sample in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", "device_crash_child"])
                .env("PULSE_CRASH_DIR", dir.path())
                .env("PULSE_CRASH_CUT", cut)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let (journal, _lock) = Journal::open(
                &dir.path().join("private"),
                "https://pulse.example/",
                dir.path(),
            )
            .await
            .unwrap();
            assert_eq!(journal.cursor().await.unwrap(), 1);
            journal.recover().await.unwrap();
            journal.recover().await.unwrap();
            assert!(journal.take_work().await.unwrap().is_none());
            let out = journal.outgoing().await.unwrap();
            assert_eq!(out.len(), 1);
            match &out[0].payload {
                DevicePayload::Result {
                    outcome: ReadOutcome::Completed { .. },
                    ..
                } => assert_eq!(cut, "result"),
                DevicePayload::Result {
                    outcome: ReadOutcome::Unknown { .. },
                    ..
                } => assert_ne!(cut, "result"),
                _ => panic!("missing result"),
            }
            if cut != "accepted" {
                assert_eq!(
                    std::fs::read(dir.path().join("fixture-count")).unwrap(),
                    b"1"
                );
            }
        }
    }
}
