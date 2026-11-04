use super::*;

async fn interrupted(root: &Path) -> (Registry, PathBuf, PathBuf) {
    let registry = bound_registry(root).await;
    let resource = registry
        .create_resource(&root.join("browser"), "session-")
        .unwrap();
    let identity = Directory::capture(resource.path()).unwrap();
    let profile = identity.path.clone();
    crate::journal::private_write(&profile.join("KEEP"), b"ORIGINAL_PROFILE").unwrap();
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
            resource: Some(identity),
            browser: true,
            phase: Phase::Starting,
            directory: Directory::capture(&dir).unwrap(),
            driver_endpoint: None,
            boot: registry.boot.clone(),
        },
    )
    .unwrap();
    (registry, dir, profile)
}

#[tokio::test]
async fn current_missing_foreign_and_mismatched_boot_evidence_preserves_uncertain_profile() {
    for kind in [
        "current",
        "missing",
        "mismatched",
        "foreign",
        "invalid",
        "unavailable",
        "cached-only",
        "closed-runtime",
    ] {
        let root = root();
        let (mut registry, dir, profile) = interrupted(root.path()).await;
        let mut state: Record = read(&dir.join("state.json")).unwrap();
        let old: boot::Identity = serde_json::from_value(serde_json::json!({
            "platform": if cfg!(target_os="macos") {"macos"} else {"linux"},
            "uuid": uuid::Uuid::new_v4().to_string(),
        }))
        .unwrap();
        match kind {
            "missing" => state.boot = None,
            "mismatched" => state.boot = Some(old),
            "foreign" => {
                state.runtime = uuid::Uuid::new_v4().to_string();
                state.boot = Some(old);
            }
            "invalid" => {
                state.boot = Some(
                    serde_json::from_value(
                        serde_json::json!({"platform":"macos", "uuid":"invalid"}),
                    )
                    .unwrap(),
                )
            }
            "unavailable" => registry.boot = None,
            "cached-only" | "closed-runtime" => {
                state.boot = Some(old.clone());
                let path = root.path().join("runtime/runtime.json");
                let mut value: serde_json::Value = read(&path).unwrap();
                value["boot"] = serde_json::to_value(&old).unwrap();
                if kind == "closed-runtime" {
                    value["phase"] = serde_json::json!("closed");
                } else {
                    let mut observed = serde_json::to_value(&old).unwrap();
                    observed["uuid"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
                    registry.boot = Some(serde_json::from_value(observed).unwrap());
                }
                write(&path, &value).unwrap();
            }
            _ => {}
        }
        write(&dir.join("state.json"), &state).unwrap();
        assert!(registry.recover().await.is_err(), "{kind}");
        assert_eq!(
            std::fs::read(profile.join("KEEP")).unwrap(),
            b"ORIGINAL_PROFILE"
        );
        assert!(record(&dir).unwrap().phase == Phase::Starting);
    }
}

#[tokio::test]
#[cfg_attr(
    target_os = "macos",
    ignore = "requires host kern.bootsessionuuid access; explicit no-API host test"
)]
async fn verified_new_kernel_boot_retires_original_operation_and_reopens_core_runtime() {
    let actual = boot::current().expect("host kernel boot identity must be available");
    assert!(boot::current().unwrap() == actual);
    let root = root();
    crate::journal::private_write(&root.path().join("KEEP"), b"HUMAN_FILE").unwrap();
    let (registry, dir, profile) = interrupted(root.path()).await;
    let old: boot::Identity = {
        let mut value = serde_json::to_value(&actual).unwrap();
        value["uuid"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
        serde_json::from_value(value).unwrap()
    };
    let mut state: Record = read(&dir.join("state.json")).unwrap();
    let old_runtime = state.runtime.clone();
    state.boot = Some(old.clone());
    write(&dir.join("state.json"), &state).unwrap();
    let runtime = root.path().join("runtime/runtime.json");
    let mut value: serde_json::Value = read(&runtime).unwrap();
    value["boot"] = serde_json::to_value(&old).unwrap();
    write(&runtime, &value).unwrap();
    // Only the prior boot record is simulated. The new boot must come from
    // the actual kernel, and the production recovery path re-reads it.
    registry.recover().await.unwrap();
    assert!(!profile.exists() && !dir.exists());
    let (journal, _lock) = Journal::open(
        root.path(),
        "http://127.0.0.1:1/",
        &root.path().join("repo"),
    )
    .await
    .unwrap();
    let fresh = super::registry(root.path());
    assert_ne!(fresh.runtime, old_runtime);
    fresh.begin(&journal, true).await.unwrap();
    let server = crate::protocol::hash(&journal.server).unwrap();
    assert!(
        journal
            .core
            .worker_runtime_verified(&journal.device, &server, &fresh.runtime)
            .await
            .unwrap()
    );
    assert_eq!(
        std::fs::read(root.path().join("KEEP")).unwrap(),
        b"HUMAN_FILE"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM budget")
            .fetch_one(&journal.db)
            .await
            .unwrap(),
        0
    );
    fresh.prepare_close().unwrap();
    journal
        .core
        .close_worker_runtime(&journal.device, &server, &fresh.runtime)
        .await
        .unwrap();
    fresh.close().unwrap();
    journal.core.close().await;
}
