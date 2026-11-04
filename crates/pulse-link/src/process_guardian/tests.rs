use super::*;
use crate::{
    executor::Executor,
    journal::Journal,
    owned_process::{ProcessTracker, SourceScope},
};
use tokio::io::{AsyncBufReadExt, BufReader};

mod ownership;
mod reboot;

fn executable() -> PathBuf {
    let current = std::env::current_exe().unwrap();
    let binary = current
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("pulse-guardian");
    assert!(
        binary.is_file(),
        "build pulse-guardian before focused --lib tests"
    );
    binary
}
fn registry(root: &Path) -> Registry {
    Registry::new(
        &root.join("runtime"),
        executable(),
        uuid::Uuid::new_v4().to_string(),
    )
    .unwrap()
}
async fn bound_registry(root: &Path) -> Registry {
    let (journal, _lock) = Journal::open(root, "http://127.0.0.1:1/", &root.join("repo"))
        .await
        .unwrap();
    let registry = registry(root);
    registry.begin(&journal, true).await.unwrap();
    journal.core.close().await;
    registry
}
async fn hanging(
    tracker: &ProcessTracker,
    resource: OwnedDirectory,
) -> crate::owned_process::OwnedProcess {
    let mut command = Command::new("/bin/sh");
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .args(["-c", "printf '%s\\n' \"$$\"; exec sleep 60"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    tracker
        .spawn_browser(
            &mut command,
            Some(SourceScope::new("owned-task", "1").unwrap()),
            resource,
        )
        .unwrap()
}

#[tokio::test]
#[ignore = "owned parent invoked by guardian hard-exit tests"]
async fn executor_child() {
    let root = PathBuf::from(std::env::var_os("PULSE_OWNED_GUARDIAN_ROOT").unwrap())
        .canonicalize()
        .unwrap();
    Directory::capture(&root).unwrap();
    let (journal, _lock) = Journal::open(&root, "http://127.0.0.1:1/", &root.join("repo"))
        .await
        .unwrap();
    let mut executor = Executor::new(&root.join("repo")).unwrap();
    executor.supervise(&root, executable()).unwrap();
    journal
        .begin_worker_runtime(&executor, &root)
        .await
        .unwrap();
    let registry = executor.guardian.clone().unwrap();
    let caches = crate::computer_cache::ImageCaches::default();
    caches.supervise(registry.clone()).unwrap();
    let image = caches
        .acquire(&root.join("computer"), "owned-task", "1")
        .await
        .unwrap();
    crate::journal::private_write(
        &image.path.join(format!("{:064x}.png", 1)),
        b"OWNED_IMAGE_SOURCE",
    )
    .unwrap();
    let profile = registry
        .create_resource(&root.join("browser"), "session-")
        .unwrap();
    crate::journal::private_write(
        &profile.path().join("owned-profile"),
        b"OWNED_PROFILE_SOURCE",
    )
    .unwrap();
    let profile_path = profile.path().to_path_buf();
    let tracker = ProcessTracker::default();
    tracker.supervise(registry).unwrap();
    let mut child = hanging(&tracker, profile).await;
    let line = BufReader::new(child.child().stdout.take().unwrap())
        .lines()
        .next_line()
        .await
        .unwrap()
        .unwrap();
    let pid: u32 = line.parse().unwrap();
    crate::journal::private_write(
        &root.join("ready.json"),
        &serde_json::to_vec(&(pid, profile_path, &image.path, executor.runtime_id)).unwrap(),
    )
    .unwrap();
    std::future::pending::<()>().await;
}

async fn killed_owner(root: &Path) -> (u32, PathBuf, PathBuf, String) {
    struct Owner(std::process::Child);
    impl Drop for Owner {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    let mut child = Owner(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process_guardian::tests::executor_child",
                "--ignored",
                "--nocapture",
            ])
            .env("PULSE_OWNED_GUARDIAN_ROOT", root)
            .env_remove("OPENAI_API_KEY")
            .env_remove("ANTHROPIC_API_KEY")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let data = loop {
        if let Ok(bytes) = std::fs::read(root.join("ready.json"))
            && let Ok(value) = serde_json::from_slice(&bytes)
        {
            break value;
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "owned executor exited before ready"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "owned executor readiness timeout"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    data
}
fn root() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir_in(std::env::temp_dir().canonicalize().unwrap())
        .unwrap();
    crate::journal::private_dir(&root.path().join("repo")).unwrap();
    root
}

#[tokio::test]
async fn killed_executor_leaves_original_stop_proof_and_restarts_without_stale_png_or_profile() {
    let root = root();
    crate::journal::private_write(&root.path().join("KEEP"), b"HUMAN_FILE").unwrap();
    let (pid, profile, image, prior_runtime) = killed_owner(root.path()).await;
    let (journal, _lock) = Journal::open(
        root.path(),
        "http://127.0.0.1:1/",
        &root.path().join("repo"),
    )
    .await
    .unwrap();
    let mut executor = Executor::new(&root.path().join("repo")).unwrap();
    assert_ne!(executor.runtime_id, prior_runtime);
    assert!(
        !journal
            .core
            .worker_runtime_verified(
                &journal.device,
                &crate::protocol::hash(&journal.server).unwrap(),
                &executor.runtime_id
            )
            .await
            .unwrap()
    );
    executor.supervise(root.path(), executable()).unwrap();
    executor.guardian.as_ref().unwrap().recover().await.unwrap();
    assert!(!profile.exists() && !image.exists());
    journal
        .begin_worker_runtime(&executor, root.path())
        .await
        .unwrap();
    assert!(
        journal
            .core
            .worker_runtime_verified(
                &journal.device,
                &crate::protocol::hash(&journal.server).unwrap(),
                &executor.runtime_id
            )
            .await
            .unwrap()
    );
    assert_eq!(
        rustix::process::test_kill_process_group(
            rustix::process::Pid::from_raw(pid as i32).unwrap()
        ),
        Err(rustix::io::Errno::SRCH)
    );
    assert_eq!(
        std::fs::read(root.path().join("KEEP")).unwrap(),
        b"HUMAN_FILE"
    );
    assert!(
        std::fs::read_dir(root.path().join("runtime/processes"))
            .unwrap()
            .next()
            .is_none()
    );
    assert!(
        std::fs::read_dir(root.path().join("runtime/resources"))
            .unwrap()
            .next()
            .is_none()
    );
    executor.shutdown().await.unwrap();
    journal.close_worker_runtime(&executor).await.unwrap();
    journal.core.close().await;
}

#[tokio::test]
async fn a_replaced_resource_is_preserved_and_cannot_be_claimed_as_recovered() {
    let root = root();
    let (_pid, profile, image, _) = killed_owner(root.path()).await;
    let moved = root.path().join("moved-owned-profile");
    std::fs::rename(&profile, &moved).unwrap();
    crate::journal::private_dir(&profile).unwrap();
    crate::journal::private_write(&profile.join("KEEP"), b"UNRELATED_REPLACEMENT").unwrap();
    assert!(registry(root.path()).recover().await.is_err());
    assert_eq!(
        std::fs::read(profile.join("KEEP")).unwrap(),
        b"UNRELATED_REPLACEMENT"
    );
    assert_eq!(
        std::fs::read(moved.join("owned-profile")).unwrap(),
        b"OWNED_PROFILE_SOURCE"
    );
    assert!(image.exists());
}

#[tokio::test]
async fn released_lease_and_success_response_without_child_exit_never_prove_browser_stop() {
    use std::future::IntoFuture;
    let root = root();
    let registry = bound_registry(root.path()).await;
    let tracker = ProcessTracker::default();
    tracker.supervise(registry.clone()).unwrap();
    let resource = registry
        .create_resource(&root.path().join("browser"), "session-")
        .unwrap();
    let path = resource.path().to_path_buf();
    crate::journal::private_write(&path.join("owned-profile"), b"KEEP_UNVERIFIED").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint: url::Url = format!("http://{}/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let app = axum::Router::new().route(
        "/shutdown",
        axum::routing::get(|| async { axum::Json(serde_json::json!({"value":null})) }),
    );
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let mut child = hanging(&tracker, resource).await;
    let _ = BufReader::new(child.child().stdout.take().unwrap())
        .lines()
        .next_line()
        .await
        .unwrap();
    child.driver_shutdown(&endpoint).unwrap();
    assert!(child.shutdown().await.is_err());
    assert!(path.join("owned-profile").exists());
    assert!(registry.recover().await.is_err());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn an_unknown_guardian_file_or_missing_stop_proof_never_erases_resources() {
    for kind in ["unknown", "starting"] {
        let root = root();
        let registry = bound_registry(root.path()).await;
        let resource = registry
            .create_resource(&root.path().join("browser"), "session-")
            .unwrap();
        let identity = Directory::capture(resource.path()).unwrap();
        crate::journal::private_write(&identity.path.join("KEEP"), b"UNVERIFIED").unwrap();
        resource.keep();
        let id = uuid::Uuid::new_v4().to_string();
        let dir = registry.root.join(&id);
        crate::journal::private_dir(&dir).unwrap();
        drop(private_file(&dir.join("lease"), true).unwrap());
        write(
            &dir.join("state.json"),
            &Record {
                version: 1,
                id,
                runtime: registry.runtime.clone(),
                resource: Some(identity.clone()),
                browser: false,
                phase: if kind == "starting" {
                    Phase::Starting
                } else {
                    Phase::Stopped
                },
                directory: Directory::capture(&dir).unwrap(),
                driver_endpoint: None,
                boot: registry.boot.clone(),
            },
        )
        .unwrap();
        if kind == "unknown" {
            crate::journal::private_write(&dir.join("human-file"), b"KEEP").unwrap();
        }
        assert!(registry.recover().await.is_err());
        assert_eq!(
            std::fs::read(identity.path.join("KEEP")).unwrap(),
            b"UNVERIFIED"
        );
    }
}

#[tokio::test]
async fn missing_runtime_identity_and_copied_operation_directories_cannot_erase_owned_bytes() {
    for kind in ["missing-runtime", "copied-operation", "unknown-version"] {
        let root = root();
        let registry = bound_registry(root.path()).await;
        let resource = registry
            .create_resource(&root.path().join("browser"), "session-")
            .unwrap();
        let identity = Directory::capture(resource.path()).unwrap();
        crate::journal::private_write(&identity.path.join("KEEP"), b"OWNED_SOURCE").unwrap();
        resource.keep();
        if kind == "missing-runtime" {
            std::fs::remove_file(root.path().join("runtime/runtime.json")).unwrap();
        } else if kind == "unknown-version" {
            let path = root.path().join("runtime/runtime.json");
            let mut value: serde_json::Value = read(&path).unwrap();
            value["version"] = serde_json::json!(2);
            write(&path, &value).unwrap();
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            let dir = registry.root.join(&id);
            crate::journal::private_dir(&dir).unwrap();
            drop(private_file(&dir.join("lease"), true).unwrap());
            write(
                &dir.join("state.json"),
                &Record {
                    version: 1,
                    id: id.clone(),
                    runtime: registry.runtime.clone(),
                    resource: Some(identity.clone()),
                    browser: false,
                    phase: Phase::Stopped,
                    directory: Directory::capture(&dir).unwrap(),
                    driver_endpoint: None,
                    boot: registry.boot.clone(),
                },
            )
            .unwrap();
            let moved = root.path().join("moved-operation");
            std::fs::rename(&dir, &moved).unwrap();
            crate::journal::private_dir(&dir).unwrap();
            crate::journal::private_write(
                &dir.join("state.json"),
                &std::fs::read(moved.join("state.json")).unwrap(),
            )
            .unwrap();
            drop(private_file(&dir.join("lease"), true).unwrap());
        }
        assert!(registry.recover().await.is_err());
        assert_eq!(
            std::fs::read(identity.path.join("KEEP")).unwrap(),
            b"OWNED_SOURCE"
        );
    }
}

#[tokio::test]
async fn supervised_stdin_stdout_and_normal_exit_preserve_original_tool_bytes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = root();
    let registry = bound_registry(root.path()).await;
    let tracker = ProcessTracker::default();
    tracker.supervise(registry.clone()).unwrap();
    let mut command = Command::new("/bin/cat");
    command
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = tracker
        .spawn(
            &mut command,
            Some(SourceScope::new("stdio", "1").unwrap()),
            None,
        )
        .unwrap();
    let source = b"OWNED_JSON_SOURCE_\xe7\xb9\x81\xe9\xab\x94\xe4\xb8\xad\xe6\x96\x87\n";
    let mut stdin = child.child().stdin.take().unwrap();
    stdin.write_all(source).await.unwrap();
    stdin.shutdown().await.unwrap();
    drop(stdin);
    let mut bytes = Vec::new();
    child
        .child()
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, source);
    assert!(child.wait().await.unwrap().success());
    tracker.shutdown().await.unwrap();
    assert!(tracker.stopped());
    assert!(std::fs::read_dir(&registry.root).unwrap().next().is_none());
}

#[tokio::test]
async fn supervised_source_stop_and_cache_erasure_leave_another_task_running() {
    let root = root();
    let registry = bound_registry(root.path()).await;
    let tracker = ProcessTracker::default();
    tracker.supervise(registry.clone()).unwrap();
    let caches = crate::computer_cache::ImageCaches::default();
    caches.supervise(registry).unwrap();
    let mut owners = Vec::new();
    for name in ["A", "B"] {
        let mut command = Command::new("/bin/sh");
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .args(["-c", "printf '%s\\n' \"$$\"; exec sleep 60"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped());
        let scope = SourceScope::new(name, "1").unwrap();
        let mut child = tracker
            .spawn(&mut command, Some(scope.clone()), None)
            .unwrap();
        let line = BufReader::new(child.child().stdout.take().unwrap())
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap();
        let image = caches
            .acquire(&root.path().join("computer"), name, "1")
            .await
            .unwrap();
        crate::journal::private_write(&image.path.join(format!("{:064x}.png", 1)), name.as_bytes())
            .unwrap();
        owners.push((child, scope, line.parse::<i32>().unwrap(), image));
    }
    let (first, scope, _, image_a) = owners.remove(0);
    let path_a = image_a.path.clone();
    drop(image_a);
    tracker.fence(&scope).unwrap();
    drop(first);
    tracker.wait_source(&scope).await.unwrap();
    caches.fence("A", "1").unwrap();
    caches
        .clear(&root.path().join("computer"), "A", "1")
        .await
        .unwrap();
    assert!(!path_a.exists());
    let (second, scope, pid, image_b) = owners.remove(0);
    image_b.valid().unwrap();
    assert!(
        rustix::process::test_kill_process_group(rustix::process::Pid::from_raw(pid).unwrap())
            .is_ok()
    );
    assert!(
        tracker
            .spawn(
                &mut Command::new("/usr/bin/true"),
                Some(SourceScope::new("A", "1").unwrap()),
                None
            )
            .is_err()
    );
    let path_b = image_b.path.clone();
    drop(image_b);
    tracker.fence(&scope).unwrap();
    drop(second);
    tracker.wait_source(&scope).await.unwrap();
    caches.fence("B", "1").unwrap();
    caches
        .clear(&root.path().join("computer"), "B", "1")
        .await
        .unwrap();
    assert!(!path_b.exists());
}

#[tokio::test]
async fn recovery_preserves_unknown_image_files_and_pre_inode_source_content() {
    for kind in [
        "unknown-image",
        "pre-inode-empty",
        "pre-inode-content",
        "pre-inode-marker-partial",
        "pre-inode-before-marker",
        "pre-inode-partial-with-content",
    ] {
        let root = root();
        let registry = bound_registry(root.path()).await;
        let resource = registry
            .create_images(&root.path().join("computer/artifacts"), ".source-owned-")
            .unwrap();
        let path = resource.path().to_path_buf();
        resource.keep();
        let empty = matches!(
            kind,
            "pre-inode-empty" | "pre-inode-marker-partial" | "pre-inode-before-marker"
        );
        if !empty {
            crate::journal::private_write(&path.join("KEEP"), b"HUMAN_OR_UNVERIFIED").unwrap();
        }
        if kind.starts_with("pre-inode") {
            let file = std::fs::read_dir(root.path().join("runtime/resources"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            let mut data: serde_json::Value = read(&file).unwrap();
            data["identity"] = serde_json::Value::Null;
            write(&file, &data).unwrap();
            if matches!(
                kind,
                "pre-inode-marker-partial"
                    | "pre-inode-before-marker"
                    | "pre-inode-partial-with-content"
            ) {
                std::fs::remove_file(path.join(OWNER_FILE)).unwrap();
                if kind != "pre-inode-before-marker" {
                    crate::journal::private_write(
                        &path.join(".pulse-owner.partial"),
                        b"interrupted marker",
                    )
                    .unwrap();
                }
            }
        }
        if empty {
            registry.recover().await.unwrap();
            assert!(!path.exists());
        } else {
            assert!(registry.recover().await.is_err());
            assert_eq!(
                std::fs::read(path.join("KEEP")).unwrap(),
                b"HUMAN_OR_UNVERIFIED"
            );
        }
    }
}

#[tokio::test]
async fn unpublished_guardian_creation_can_be_recovered_without_launching_a_tool() {
    let root = root();
    let registry = bound_registry(root.path()).await;
    let dir = registry.root.join(uuid::Uuid::new_v4().to_string());
    crate::journal::private_dir(&dir).unwrap();
    drop(private_file(&dir.join("lease"), true).unwrap());
    crate::journal::private_write(&dir.join("state.partial"), b"interrupted metadata").unwrap();
    registry.recover().await.unwrap();
    assert!(!dir.exists());
}

#[tokio::test]
async fn dropping_an_owner_outside_tokio_keeps_the_guardian_alive_to_confirm_stop() {
    let root = root();
    let registry = bound_registry(root.path()).await;
    let tracker = ProcessTracker::default();
    tracker.supervise(registry.clone()).unwrap();
    let resource = registry
        .create_resource(&root.path().join("browser"), "session-")
        .unwrap();
    let path = resource.path().to_path_buf();
    let mut child = hanging(&tracker, resource).await;
    let _ = BufReader::new(child.child().stdout.take().unwrap())
        .lines()
        .next_line()
        .await
        .unwrap();
    std::thread::spawn(move || drop(child)).join().unwrap();
    registry.recover().await.unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn repeated_process_exits_during_runtime_admission_and_close_resume_original_core_state() {
    for point in ["begun-core", "closed-prior-core"] {
        let root = root();
        let run = |point: &str| {
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "process_guardian::tests::executor_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("PULSE_OWNED_GUARDIAN_ROOT", root.path())
                .env("PULSE_OWNED_GUARDIAN_CRASH", point)
                .env_remove("OPENAI_API_KEY")
                .env_remove("ANTHROPIC_API_KEY")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap()
        };
        async fn exited(mut child: std::process::Child) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert_eq!(status.code(), Some(73));
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("owned admission crash timeout");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        exited(run("begun-core")).await;
        if point == "closed-prior-core" {
            exited(run(point)).await;
        }
        let (journal, _lock) = Journal::open(
            root.path(),
            "http://127.0.0.1:1/",
            &root.path().join("repo"),
        )
        .await
        .unwrap();
        let mut executor = Executor::new(&root.path().join("repo")).unwrap();
        executor.supervise(root.path(), executable()).unwrap();
        journal
            .begin_worker_runtime(&executor, root.path())
            .await
            .unwrap();
        assert!(
            journal
                .core
                .worker_runtime_verified(
                    &journal.device,
                    &crate::protocol::hash(&journal.server).unwrap(),
                    &executor.runtime_id
                )
                .await
                .unwrap()
        );
        executor.shutdown().await.unwrap();
        journal.close_worker_runtime(&executor).await.unwrap();
        journal.core.close().await;
    }
}

#[tokio::test]
#[ignore = "owned real Chrome parent, invoked only by explicit browser recovery test"]
async fn chrome_executor_child() {
    let root = PathBuf::from(std::env::var_os("PULSE_OWNED_GUARDIAN_ROOT").unwrap())
        .canonicalize()
        .unwrap();
    let origin = std::env::var("PULSE_OWNED_GUARDIAN_ORIGIN").unwrap();
    let marker = std::env::var("PULSE_OWNED_GUARDIAN_MARKER").unwrap();
    let driver = PathBuf::from(std::env::var_os("PULSE_TEST_BROWSER_DRIVER").unwrap());
    let runtime = crate::browser_runtime::BrowserRuntime::new(
        driver,
        crate::browser_network::NetworkPolicy::new(Some(&origin)).unwrap(),
        root.join("browser"),
    )
    .unwrap()
    .with_interactions(true);
    let (journal, _lock) = Journal::open(&root, "http://127.0.0.1:1/", &root.join("repo"))
        .await
        .unwrap();
    let mut executor = Executor::new(&root.join("repo"))
        .unwrap()
        .with_browser(Some(runtime));
    executor.supervise(&root, executable()).unwrap();
    journal
        .begin_worker_runtime(&executor, &root)
        .await
        .unwrap();
    for task in ["A", "B"] {
        let binding = crate::browser_session::WorkBinding {
            operation_id: format!("owned-browser-{task}"),
            task_id: task.into(),
            revision: "1".into(),
            device: journal.device.clone(),
            epoch: 1,
        };
        let read = executor
            .execute_bound(
                crate::protocol::ReadOp::BrowserInspect {
                    url: origin.clone(),
                },
                &binding,
            )
            .await;
        match read.outcome {
            crate::protocol::ReadOutcome::Completed { text, .. } => assert!(text.contains(&marker)),
            other => panic!("owned browser observation failed: {other:?}"),
        }
    }
    let registry = executor.guardian.as_ref().unwrap();
    let mut endpoints = Vec::new();
    let mut profiles = Vec::new();
    for entry in std::fs::read_dir(&registry.root).unwrap() {
        let state = record(&entry.unwrap().path()).unwrap();
        endpoints.push(state.driver_endpoint.unwrap());
        profiles.push(state.resource.unwrap().path);
    }
    assert_eq!(profiles.len(), 2);
    crate::journal::private_write(
        &root.join("chrome-ready.json"),
        &serde_json::to_vec(&(endpoints, profiles)).unwrap(),
    )
    .unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
#[ignore = "explicit real Chrome hard-exit recovery; set PULSE_TEST_BROWSER_DRIVER; no API or personal profile"]
async fn real_chrome_executor_kill_stops_both_drivers_and_allows_a_fresh_browser_read() {
    use std::future::IntoFuture;
    struct Preserve(Option<tempfile::TempDir>);
    impl Drop for Preserve {
        fn drop(&mut self) {
            if std::thread::panicking()
                && let Some(dir) = self.0.take()
            {
                eprintln!(
                    "Unverified owned fixture retained: {}",
                    dir.keep().display()
                );
            }
        }
    }
    struct Owner(std::process::Child);
    impl Drop for Owner {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    let driver = PathBuf::from(
        std::env::var_os("PULSE_TEST_BROWSER_DRIVER").expect("explicit installed driver required"),
    );
    let saved = Preserve(Some(root()));
    let root = saved.0.as_ref().unwrap().path();
    crate::journal::private_write(&root.join("KEEP"), b"HUMAN_FILE").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let marker = uuid::Uuid::new_v4().to_string();
    let page = marker.clone();
    let app =
        axum::Router::new().route(
            "/",
            axum::routing::get(move || {
                let page = page.clone();
                async move {
                    axum::response::Html(format!("<h1>Owned recovery fixture</h1><p>{page}</p>"))
                }
            }),
        );
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let mut child = Owner(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process_guardian::tests::chrome_executor_child",
                "--ignored",
                "--nocapture",
            ])
            .env("PULSE_OWNED_GUARDIAN_ROOT", root)
            .env("PULSE_OWNED_GUARDIAN_ORIGIN", &origin)
            .env("PULSE_OWNED_GUARDIAN_MARKER", &marker)
            .env("PULSE_TEST_BROWSER_DRIVER", &driver)
            .env_remove("OPENAI_API_KEY")
            .env_remove("ANTHROPIC_API_KEY")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let (endpoints, profiles): (Vec<String>, Vec<PathBuf>) = loop {
        if let Ok(bytes) = std::fs::read(root.join("chrome-ready.json"))
            && let Ok(value) = serde_json::from_slice(&bytes)
        {
            break value;
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "owned Chrome executor exited before ready"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "owned Chrome readiness timeout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(profiles.len(), 2);
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    let runtime = crate::browser_runtime::BrowserRuntime::new(
        driver,
        crate::browser_network::NetworkPolicy::new(Some(&origin)).unwrap(),
        root.join("browser"),
    )
    .unwrap()
    .with_interactions(true);
    let (journal, _lock) = Journal::open(root, "http://127.0.0.1:1/", &root.join("repo"))
        .await
        .unwrap();
    let mut executor = Executor::new(&root.join("repo"))
        .unwrap()
        .with_browser(Some(runtime));
    executor.supervise(root, executable()).unwrap();
    executor.guardian.as_ref().unwrap().recover().await.unwrap();
    assert!(profiles.iter().all(|path| !path.exists()));
    for endpoint in endpoints {
        let url: url::Url = endpoint.parse().unwrap();
        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", url.port().unwrap()))
                .await
                .is_err()
        );
    }
    journal.begin_worker_runtime(&executor, root).await.unwrap();
    let binding = crate::browser_session::WorkBinding {
        operation_id: "owned-browser-C".into(),
        task_id: "C".into(),
        revision: "1".into(),
        device: journal.device.clone(),
        epoch: 2,
    };
    let read = executor
        .execute_bound(
            crate::protocol::ReadOp::BrowserInspect { url: origin },
            &binding,
        )
        .await;
    let crate::protocol::ReadOutcome::Completed { text, .. } = read.outcome else {
        panic!("new browser read failed")
    };
    assert!(text.contains(&marker));
    executor.shutdown().await.unwrap();
    journal.close_worker_runtime(&executor).await.unwrap();
    assert_eq!(std::fs::read(root.join("KEEP")).unwrap(), b"HUMAN_FILE");
    assert_eq!(
        std::fs::read_dir(root.join("runtime/processes"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(root.join("runtime/resources"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM budget")
            .fetch_one(&journal.db)
            .await
            .unwrap(),
        0
    );
    journal.core.close().await;
    server.abort();
    let _ = server.await;
}
