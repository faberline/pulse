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
