use super::*;
use std::os::unix::fs::{PermissionsExt, symlink};
fn reference(bytes: &[u8]) -> TextResultRef {
    let id = uuid::Uuid::new_v4().to_string();
    TextResultRef {
        id: id.clone(),
        task_id: "fixture".into(),
        revision: "1".into(),
        digest: hash_bytes(bytes),
        length: bytes.len() as u32,
        created_at: 1,
        file_name: format!("Pulse-{id}.md"),
    }
}
#[test]
fn export_creates_private_complete_markdown_and_never_replaces_files_or_links() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let bytes = b"# Verified fixture\n";
    let r = reference(bytes);
    let stop = AtomicBool::new(false);
    let path = root.join("result.md");
    assert_eq!(
        save_new(&path, &r, bytes, &stop, || true),
        ExportStatus::Saved
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let other = b"must not replace";
    assert_eq!(
        save_new(&path, &reference(other), other, &stop, || true),
        ExportStatus::NotSaved
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let link = root.join("link.md");
    symlink(&path, &link).unwrap();
    assert_eq!(
        save_new(&link, &r, bytes, &stop, || true),
        ExportStatus::NotSaved
    );
    let hard = root.join("hard.md");
    std::fs::hard_link(&path, &hard).unwrap();
    assert_eq!(
        save_new(&hard, &r, bytes, &stop, || true),
        ExportStatus::NotSaved
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    for bad in [
        root.join("file.txt"),
        root.join(".private.md"),
        root.join("../escape.md"),
    ] {
        assert_eq!(
            save_new(&bad, &r, bytes, &stop, || true),
            ExportStatus::NotSaved
        );
    }
    let private = root.join(".SSH");
    std::fs::create_dir(&private).unwrap();
    let alias = root.join("alias");
    symlink(&private, &alias).unwrap();
    assert_eq!(
        save_new(&alias.join("no.md"), &r, bytes, &stop, || true),
        ExportStatus::NotSaved
    );
    assert!(std::fs::read_dir(&root).unwrap().all(|p| {
        !p.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".pulse-result-")
    }));
}
#[test]
fn export_cancellation_and_bad_digest_leave_no_published_or_temporary_file() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cancel.md");
    let bytes = b"cancel fixture";
    let r = reference(bytes);
    let stop = AtomicBool::new(false);
    let calls = std::cell::Cell::new(0);
    assert_eq!(
        save_new(&path, &r, bytes, &stop, || {
            calls.set(calls.get() + 1);
            calls.get() == 1
        }),
        ExportStatus::NotSaved
    );
    assert_eq!(calls.get(), 2);
    assert!(!path.exists());
    assert_eq!(
        save_new(&path, &r, b"corrupt", &stop, || true),
        ExportStatus::NotSaved
    );
    stop.store(true, Ordering::Release);
    assert_eq!(
        save_new(&path, &r, bytes, &stop, || true),
        ExportStatus::NotSaved
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
#[test]
fn export_failure_after_publish_is_unknown_and_preserves_the_complete_file() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("unknown.md");
    let bytes = b"published before directory sync failed";
    let r = reference(bytes);
    assert_eq!(
        save_with_sync(
            &path,
            &r,
            bytes,
            &AtomicBool::new(false),
            || true,
            |_| Err(crate::Error::Io(std::io::Error::other(
                "injected directory sync failure"
            )))
        ),
        ExportStatus::Unknown
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    assert_eq!(
        save_new(&path, &r, bytes, &AtomicBool::new(false), || true),
        ExportStatus::NotSaved
    );
}

mod native_loop;
