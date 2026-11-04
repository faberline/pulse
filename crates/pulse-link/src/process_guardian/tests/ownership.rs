use super::*;

#[tokio::test]
async fn missing_changed_linked_or_public_marker_preserves_original_source_content() {
    use std::os::unix::fs::PermissionsExt;
    for kind in [
        "missing",
        "changed",
        "hard-link",
        "symlink",
        "public",
        "malformed",
    ] {
        let root = root();
        let registry = bound_registry(root.path()).await;
        let owned = registry
            .create_resource(&root.path().join("browser"), "session-")
            .unwrap();
        let path = owned.path().to_path_buf();
        let marker = path.join(OWNER_FILE);
        crate::journal::private_write(&path.join("KEEP"), b"ORIGINAL_SOURCE").unwrap();
        owned.keep();
        match kind {
            "missing" => std::fs::remove_file(&marker).unwrap(),
            "changed" => write(&marker, &uuid::Uuid::new_v4().to_string()).unwrap(),
            "hard-link" => std::fs::hard_link(&marker, root.path().join("linked-marker")).unwrap(),
            "symlink" => {
                std::fs::rename(&marker, root.path().join("marker")).unwrap();
                std::os::unix::fs::symlink(root.path().join("marker"), &marker).unwrap();
            }
            "public" => {
                std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o644)).unwrap()
            }
            "malformed" => std::fs::write(&marker, b"not-owned").unwrap(),
            _ => unreachable!(),
        }
        assert!(registry.recover().await.is_err(), "{kind}");
        assert_eq!(
            std::fs::read(path.join("KEEP")).unwrap(),
            b"ORIGINAL_SOURCE"
        );
    }
}

#[tokio::test]
async fn reused_inode_without_original_marker_cannot_erase_replacement_content() {
    let root = root();
    let registry = bound_registry(root.path()).await;
    let owned = registry
        .create_resource(&root.path().join("browser"), "session-")
        .unwrap();
    let path = owned.path().to_path_buf();
    owned.keep();
    std::fs::remove_dir_all(&path).unwrap();
    crate::journal::private_dir(&path).unwrap();
    crate::journal::private_write(&path.join("KEEP"), b"UNRELATED_REPLACEMENT").unwrap();
    let meta = std::fs::symlink_metadata(&path).unwrap();
    let record = std::fs::read_dir(root.path().join("runtime/resources"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut value: serde_json::Value = read(&record).unwrap();
    // Model inode reuse deterministically. Keep the original unpredictable
    // ownership value while making the old metadata match the replacement.
    value["identity"]["device"] = serde_json::json!(meta.dev());
    value["identity"]["inode"] = serde_json::json!(meta.ino());
    write(&record, &value).unwrap();
    assert!(registry.recover().await.is_err());
    assert_eq!(
        std::fs::read(path.join("KEEP")).unwrap(),
        b"UNRELATED_REPLACEMENT"
    );
}

#[tokio::test]
async fn interrupted_cleanup_retries_with_marker_and_only_removes_empty_unmarked_directory() {
    for kind in ["partial", "empty-unmarked", "nonempty-unmarked", "legacy"] {
        let root = root();
        let registry = bound_registry(root.path()).await;
        let owned = registry
            .create_resource(&root.path().join("browser"), "session-")
            .unwrap();
        let path = owned.path().to_path_buf();
        owned.keep();
        crate::journal::private_dir(&path.join("remaining")).unwrap();
        crate::journal::private_write(&path.join("remaining/KEEP"), b"SOURCE").unwrap();
        if kind != "partial" {
            std::fs::remove_file(path.join(OWNER_FILE)).unwrap();
        }
        if kind == "empty-unmarked" {
            std::fs::remove_dir_all(path.join("remaining")).unwrap();
        }
        if kind == "legacy" {
            let record = std::fs::read_dir(root.path().join("runtime/resources"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            let mut value: serde_json::Value = read(&record).unwrap();
            value["version"] = serde_json::json!(1);
            value.as_object_mut().unwrap().remove("owner");
            value["identity"].as_object_mut().unwrap().remove("owner");
            write(&record, &value).unwrap();
        }
        if matches!(kind, "partial" | "empty-unmarked") {
            registry.recover().await.unwrap();
            assert!(!path.exists());
        } else {
            assert!(registry.recover().await.is_err());
            assert_eq!(
                std::fs::read(path.join("remaining/KEEP")).unwrap(),
                b"SOURCE"
            );
        }
    }
}

#[tokio::test]
async fn normal_image_clear_checks_original_marker_and_retains_failed_ownership_for_retry() {
    let root = root();
    let registry = bound_registry(root.path()).await;
    let caches = crate::computer_cache::ImageCaches::default();
    caches.supervise(registry).unwrap();
    let image = caches
        .acquire(&root.path().join("computer"), "image-task", "1")
        .await
        .unwrap();
    let path = image.path.clone();
    let marker = std::fs::read(path.join(OWNER_FILE)).unwrap();
    let png = path.join(format!("{:064x}.png", 1));
    crate::journal::private_write(&png, b"SOURCE_IMAGE").unwrap();
    drop(image);
    caches.fence("image-task", "1").unwrap();
    write(&path.join(OWNER_FILE), &uuid::Uuid::new_v4().to_string()).unwrap();
    assert!(
        caches
            .clear(&root.path().join("computer"), "image-task", "1")
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&png).unwrap(), b"SOURCE_IMAGE");
    std::fs::write(path.join(OWNER_FILE), &marker).unwrap();
    caches
        .clear(&root.path().join("computer"), "image-task", "1")
        .await
        .unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn verified_owner_marker_does_not_reduce_the_original_image_capacity() {
    let root = root();
    let registry = bound_registry(root.path()).await;
    let caches = crate::computer_cache::ImageCaches::default();
    caches.supervise(registry).unwrap();
    let directory = root.path().join("computer");
    let image = caches.acquire(&directory, "image-task", "1").await.unwrap();
    for n in 0..126 {
        crate::journal::private_write(&image.path.join(format!("{n:064x}.png")), b"PNG").unwrap();
    }
    caches.capacity(&directory, 2).unwrap();
    crate::journal::private_write(&image.path.join(format!("{:064x}.png", 126)), b"PNG").unwrap();
    caches.capacity(&directory, 1).unwrap();
    assert!(caches.capacity(&directory, 2).is_err());
    crate::journal::private_write(&image.path.join(format!("{:064x}.png", 127)), b"PNG").unwrap();
    assert!(caches.capacity(&directory, 1).is_err());
    drop(image);
    caches.fence("image-task", "1").unwrap();
    caches.clear(&directory, "image-task", "1").await.unwrap();
}
