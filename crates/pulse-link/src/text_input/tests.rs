use super::*;
use crate::journal::{DeviceLock, Journal};
use std::path::PathBuf;
mod native_loop;

fn owned_file(text: &str) -> (tempfile::TempDir, PathBuf, TextInput) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap().join("report.md");
    std::fs::write(&path, text).unwrap();
    let input = TextInput::read(&path).unwrap();
    (root, path, input)
}
async fn journal(root: &Path) -> (Journal, DeviceLock, Credential) {
    let (j, lock) = Journal::open_role(
        &root.join("chat"),
        "https://pulse.example/",
        Path::new(""),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    let credential = Credential {
        device: j.device.clone(),
        role: ClientRole::Chat,
        token: "a".repeat(64),
    };
    assert!(j.bind_memory_credential(&credential).await.unwrap());
    (j, lock, credential)
}

#[test]
fn selected_text_reader_freezes_bytes_and_rejects_paths_links_binary_and_special_files() {
    use std::os::unix::fs::symlink;
    let (root, path, input) = owned_file("選定時的內容\nPRIVATE_INPUT_BODY");
    std::fs::write(&path, "different bytes").unwrap();
    assert!(input.text.contains("PRIVATE_INPUT_BODY"));
    assert_eq!(input.digest, hash_bytes(input.text.as_bytes()));
    assert!(
        !serde_json::to_string(&input)
            .unwrap()
            .contains(root.path().to_str().unwrap())
    );
    assert!(!format!("{input:?}").contains("PRIVATE_INPUT_BODY"));
    assert!(
        serde_json::to_value(input.reference())
            .unwrap()
            .get("text")
            .is_none()
    );
    let base = path.parent().unwrap();
    for (name, bytes) in [
        ("binary.txt", vec![255]),
        ("control.md", b"a\0b".to_vec()),
        ("large.md", vec![b'a'; MAX_TEXT_INPUT + 1]),
        ("secret.pem", b"fixture-only".to_vec()),
        ("empty.txt", vec![]),
    ] {
        let file = base.join(name);
        std::fs::write(&file, bytes).unwrap();
        assert!(TextInput::read(&file).is_err(), "{name}");
    }
    let alias = base.join("alias.md");
    symlink(&path, &alias).unwrap();
    assert!(TextInput::read(&alias).is_err());
    let alias_dir = base.join("alias-dir");
    symlink(base, &alias_dir).unwrap();
    assert_eq!(
        TextInput::read(&alias_dir.join("report.md")).unwrap().text,
        "different bytes"
    );
    let hard = base.join("hard.md");
    std::fs::hard_link(&path, &hard).unwrap();
    assert!(TextInput::read(&hard).is_err());
    let private = base.join(".SSH");
    std::fs::create_dir(&private).unwrap();
    std::fs::write(private.join("notes.json"), "fake fixture").unwrap();
    assert!(TextInput::read(&private.join("notes.json")).is_err());
    let private_alias = base.join("private-alias");
    symlink(&private, &private_alias).unwrap();
    assert!(TextInput::read(&private_alias.join("notes.json")).is_err());
    let directory = base.join("folder.txt");
    std::fs::create_dir(&directory).unwrap();
    assert!(TextInput::read(&directory).is_err());
    let fifo = base.join("pipe.txt");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let start = std::time::Instant::now();
    assert!(TextInput::read(&fifo).is_err());
    assert!(start.elapsed() < std::time::Duration::from_secs(1));
    assert!(TextInput::read(Path::new("relative.md")).is_err());
    assert!(TextInput::read(&base.join("../report.md")).is_err());
}

#[tokio::test]
async fn selected_file_is_private_durable_and_queues_only_the_original_snapshot_on_submit() {
    let (root, path, input) = owned_file("BEFORE_USER_EDIT");
    let (j, lock, cred) = journal(root.path()).await;
    let identity = j.text_input_identity().unwrap();
    j.store_text_input(input.clone(), &identity).await.unwrap();
    assert!(j.outgoing().await.unwrap().is_empty());
    assert_eq!(
        j.view().await.unwrap().text_input.unwrap(),
        input.reference()
    );
    drop(j);
    drop(lock);
    std::fs::write(&path, "AFTER_USER_EDIT").unwrap();
    let (j, _lock) = Journal::open_role(
        &root.path().join("chat"),
        "https://pulse.example/",
        Path::new(""),
        ClientRole::Chat,
    )
    .await
    .unwrap();
    assert!(j.bind_memory_credential(&cred).await.unwrap());
    assert_eq!(j.selected_text_input().await.unwrap().unwrap(), input);
    let source = uuid::Uuid::new_v4().to_string();
    for _ in 0..2 {
        j.submit_text_input(
            &source,
            "file-task".into(),
            "摘要這份資料".into(),
            &input.id,
        )
        .await
        .unwrap();
    }
    let frames = j.outgoing().await.unwrap();
    assert_eq!(frames.len(), 2);
    for frame in frames {
        let DevicePayload::Human {
            source_id,
            action: HumanAction::SubmitTextFile { input: queued, .. },
        } = frame.payload
        else {
            panic!("wrong payload")
        };
        assert_eq!(source_id, source);
        assert_eq!(queued, input);
    }
    j.clear_text_input(Some("other selection")).await.unwrap();
    assert!(j.selected_text_input().await.unwrap().is_some());
    j.clear_text_input(Some(&input.id)).await.unwrap();
    assert!(j.selected_text_input().await.unwrap().is_none());
    // Removing the composer selection does not withdraw an already queued message.
    assert_eq!(j.outgoing().await.unwrap().len(), 2);
}

#[tokio::test]
async fn discard_expiry_corrupt_slot_and_credential_denial_remove_the_unsubmitted_copy() {
    let (root, _path, input) = owned_file("PRIVATE_UNSUBMITTED_INPUT_SENTINEL");
    let (j, _lock, _cred) = journal(root.path()).await;
    let identity = j.text_input_identity().unwrap();
    j.store_text_input(input.clone(), &identity).await.unwrap();
    j.clear_text_input(Some(&input.id)).await.unwrap();
    let disk = std::fs::read(root.path().join("chat/device.db")).unwrap();
    assert!(
        !disk
            .windows(input.text.len())
            .any(|w| w == input.text.as_bytes())
    );
    let mut expired = input.clone();
    expired.captured_at = crate::now() - TTL - 1;
    j.store_text_input(expired, &identity).await.unwrap();
    assert!(j.selected_text_input().await.unwrap().is_none());
    sqlx::query("INSERT INTO local_meta VALUES(?,'not json')")
        .bind(KEY)
        .execute(&j.db)
        .await
        .unwrap();
    assert!(j.selected_text_input().await.unwrap().is_none());
    j.store_text_input(input.clone(), &identity).await.unwrap();
    assert!(
        j.store_text_input(input.clone(), "wrong identity")
            .await
            .is_err()
    );
    assert_eq!(j.selected_text_input().await.unwrap().unwrap(), input);
    j.deny_memory_access().await.unwrap();
    assert!(j.selected_text_input().await.unwrap().is_none());
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM local_meta WHERE key=?")
        .bind(KEY)
        .fetch_one(&j.db)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
    assert!(j.store_text_input(input, &identity).await.is_err());
    assert!(j.outgoing().await.unwrap().is_empty());
}

#[test]
fn input_validation_binds_hash_length_and_encoded_provider_budget() {
    let (_root, _path, mut input) = owned_file("a");
    input.text = "a".repeat(MAX_TEXT_INPUT);
    input.length = input.text.len() as u32;
    input.digest = hash_bytes(input.text.as_bytes());
    input.validate_goal("摘要").unwrap();
    let mut changed = input.clone();
    changed.digest = "0".repeat(64);
    assert!(changed.validate().is_err());
    changed = input.clone();
    changed.length = 1;
    assert!(changed.validate().is_err());
    assert!(input.validate_goal("記住 偏好：附件內容").is_err());
    assert!(input.validate_goal("提醒我：明天").is_err());
    input.text = "a\n".repeat(MAX_TEXT_INPUT / 2);
    input.length = input.text.len() as u32;
    input.digest = hash_bytes(input.text.as_bytes());
    assert!(input.validate_goal("摘要").is_err());
}
