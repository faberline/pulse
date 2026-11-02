//! A UI crash/pipe close must not leave its transport waiting on native APIs.
#[test]
fn chat_transport_exits_after_parent_closes_stdin() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pulse-chat"))
        .args(["--server", "https://pulse.example", "--data-dir"])
        .arg(dir.path().join("chat"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    drop(child.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("chat transport outlived its UI pipe");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn a_locked_startup_database_cannot_keep_the_chat_alive_after_ui_pipe_closes() {
    locked_startup(false).await;
}

#[tokio::test]
async fn a_full_input_queue_cannot_hide_ui_pipe_close_during_locked_startup() {
    locked_startup(true).await;
}

async fn locked_startup(fill: bool) {
    use std::{
        os::unix::fs::PermissionsExt,
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("chat");
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = pulse_core::Store::open(dir.join("device.db"), "pulse-local-journal")
        .await
        .unwrap();
    let locked = store
        .application_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pulse-chat"))
        .args(["--server", "https://pulse.example", "--data-dir"])
        .arg(&dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // The same exit deadline applies before and during initialization. Do not
    // unlock SQLite to help the child; EOF must cancel its startup future.
    if fill {
        use std::io::Write;
        let mut pipe = child.stdin.take().unwrap();
        // Keep the whole fixture below even a small OS pipe buffer, so the
        // parent never waits on a blocked child before starting its deadline.
        for _ in 0..64 {
            if pipe.write_all(b"{\"kind\":\"shutdown\"}\n").is_err() {
                break;
            }
        }
        drop(pipe);
    } else {
        drop(child.stdin.take());
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("locked startup outlived its UI pipe");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    locked.rollback().await.unwrap();
    store.close().await;
}
