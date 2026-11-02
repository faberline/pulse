//! Fake helper processes only. No OS permission request, capture or window access.
use super::*;
use crate::computer_runtime::tests::{assert_reaped, fixture};
use std::os::unix::fs::PermissionsExt;

const SERVER: &str = "https://pulse.example/";
fn control_dir() -> tempfile::TempDir {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

fn request(id: &str, permission: Option<pulse_core::ComputerPermission>) -> WindowRequest {
    WindowRequest::Permissions {
        version: 1,
        server: SERVER.into(),
        request_id: id.into(),
        request: permission,
    }
}

#[tokio::test]
async fn permission_control_status_is_quiet_and_pending_prompt_never_becomes_a_grant() {
    let (root, runtime) = fixture("permission-pending");
    let dir = control_dir();
    let channel = PickerChannel::bind_at(dir.path(), SERVER).await.unwrap();
    let worker = PickerWorker(tokio::spawn(channel.serve(runtime.clone())));
    let id = uuid::Uuid::new_v4().to_string();
    let value: PermissionResponse = exchange(dir.path(), SERVER, &request(&id, None))
        .await
        .unwrap();
    assert_eq!(value.request_id, id);
    assert!(!value.permissions.can_observe());
    assert!(!root.path().join("permission-pending.requests").exists());
    let prompt = request(&id, Some(pulse_core::ComputerPermission::Accessibility));
    let value: PermissionResponse = exchange(dir.path(), SERVER, &prompt).await.unwrap();
    assert!(!value.permissions.accessibility);
    assert_eq!(
        std::fs::read_to_string(root.path().join("permission-pending.requested")).unwrap(),
        "accessibility"
    );
    assert!(
        exchange::<_, PermissionResponse>(dir.path(), SERVER, &prompt)
            .await
            .is_err()
    );
    assert!(
        exchange::<_, PermissionResponse>(
            dir.path(),
            SERVER,
            &request(
                &id.to_uppercase(),
                Some(pulse_core::ComputerPermission::Accessibility)
            )
        )
        .await
        .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("permission-pending.requests")).unwrap(),
        "1"
    );
    let value: PermissionResponse = exchange(dir.path(), SERVER, &request(&id, None))
        .await
        .unwrap();
    assert!(!value.permissions.can_observe());
    let id = uuid::Uuid::new_v4().to_string();
    let _: PermissionResponse = exchange(
        dir.path(),
        SERVER,
        &request(&id, Some(pulse_core::ComputerPermission::ScreenCapture)),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(root.path().join("permission-pending.requested")).unwrap(),
        "screen_capture"
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("permission-pending.requests")).unwrap(),
        "2"
    );
    assert!(runtime.selections().is_empty());
    assert!(!root.path().join("private/artifacts").exists());
    worker.stop().await;
}

#[tokio::test]
async fn permission_control_bad_binding_or_extra_authority_never_starts_a_helper() {
    let (root, runtime) = fixture("permission-pending");
    let dir = control_dir();
    let channel = PickerChannel::bind_at(dir.path(), SERVER).await.unwrap();
    let path = channel.path.clone();
    let worker = PickerWorker(tokio::spawn(channel.serve(runtime)));
    let value = serde_json::to_value(request(
        &uuid::Uuid::new_v4().to_string(),
        Some(pulse_core::ComputerPermission::Accessibility),
    ))
    .unwrap();
    for mutation in [
        serde_json::json!({"server":"https://other.example/"}),
        serde_json::json!({"version":2}),
        serde_json::json!({"request_id":"not-a-uuid"}),
        serde_json::json!({"request":"all"}),
        serde_json::json!({"grant":true}),
    ] {
        let mut input = value.clone();
        for (key, changed) in mutation.as_object().unwrap() {
            input[key] = changed.clone();
        }
        let mut stream = UnixStream::connect(&path).await.unwrap();
        let mut bytes = serde_json::to_vec(&input).unwrap();
        bytes.push(b'\n');
        stream.write_all(&bytes).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(response.is_empty());
        assert!(!root.path().join("permission-pending.pid").exists());
    }
    worker.stop().await;
}

#[tokio::test]
async fn closing_permission_control_reaps_the_owned_child_without_an_automatic_retry() {
    let (root, runtime) = fixture("permission-request-hang");
    let dir = control_dir();
    let channel = PickerChannel::bind_at(dir.path(), SERVER).await.unwrap();
    let path = channel.path.clone();
    let worker = PickerWorker(tokio::spawn(channel.serve(runtime.clone())));
    let id = uuid::Uuid::new_v4().to_string();
    let mut stream = UnixStream::connect(&path).await.unwrap();
    let mut bytes = serde_json::to_vec(&request(
        &id,
        Some(pulse_core::ComputerPermission::Accessibility),
    ))
    .unwrap();
    bytes.push(b'\n');
    stream.write_all(&bytes).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !root
            .path()
            .join("permission-request-hang.requests")
            .exists()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(stream);
    assert_reaped(&runtime).await;
    assert!(!runtime.permissions().await.unwrap().can_observe());
    assert_eq!(
        std::fs::read_to_string(root.path().join("permission-request-hang.requests")).unwrap(),
        "1"
    );
    assert!(runtime.selections().is_empty());
    worker.stop().await;
}

#[tokio::test]
async fn permission_runtime_rejects_old_helpers_and_observation_in_a_prompt_reply() {
    for mode in ["permission-legacy", "permission-extra"] {
        let (_root, runtime) = fixture(mode);
        assert!(
            runtime
                .request_permission(pulse_core::ComputerPermission::Accessibility)
                .await
                .is_err()
        );
        assert!(runtime.selections().is_empty());
    }
    // The permission request is local Native IPC, not a remote Human/model command.
    let command = serde_json::json!({"kind":"computer_permissions","request_id":uuid::Uuid::new_v4().to_string(),"request":"accessibility"});
    assert!(serde_json::from_value::<NativeCommand>(command.clone()).is_ok());
    assert!(serde_json::from_value::<HumanAction>(command.clone()).is_err());
    assert!(serde_json::from_value::<ReadOp>(command).is_err());
}
