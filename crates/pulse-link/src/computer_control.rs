//! Local Human picker channel. It is never exposed through the server or model tools.
use crate::{Result, computer_runtime::ComputerRuntime, ensure, journal::private_dir, protocol::*};
use pulse_core::ComputerPermit;
use serde::{Deserialize, Serialize};
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectionRequest {
    version: u8,
    server: String,
    permit: ComputerPermit,
    replace: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectionResponse {
    version: u8,
    ready: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum WindowRequest {
    List {
        version: u8,
        server: String,
    },
    Permissions {
        version: u8,
        server: String,
        request_id: String,
        request: Option<pulse_core::ComputerPermission>,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PermissionResponse {
    version: u8,
    request_id: String,
    permissions: crate::computer_runtime::ComputerPermissions,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowResponse {
    version: u8,
    windows: Vec<pulse_core::ComputerWindowChoice>,
}

pub(crate) struct PickerChannel {
    listener: UnixListener,
    path: PathBuf,
    inode: (u64, u64),
    server: String,
    permission_requests: std::sync::Mutex<std::collections::BTreeSet<String>>,
}
pub(crate) struct PickerWorker(tokio::task::JoinHandle<Result<()>>);
impl PickerWorker {
    pub(crate) async fn start(server: &str, runtime: ComputerRuntime) -> Result<Self> {
        let channel = PickerChannel::bind(server).await?;
        Ok(Self(tokio::spawn(channel.serve(runtime))))
    }
    pub(crate) async fn stop(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}
impl Drop for PickerWorker {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub(crate) fn control_directory() -> PathBuf {
    std::env::var_os("PULSE_LOCAL_CONTROL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| crate::journal::default_data_dir().join("control"))
}
fn socket_path(directory: &Path, server: &str) -> Result<PathBuf> {
    // A short hash keeps the path inside macOS's Unix socket limit. The full origin is checked again.
    let path = directory.join(format!("{}.sock", &hash_bytes(server.as_bytes())[..32]));
    ensure(
        path.as_os_str().as_encoded_bytes().len() < 104,
        "local picker socket path too long",
    )?;
    Ok(path)
}
fn owned_socket(path: &Path) -> Result<std::fs::Metadata> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure(
        metadata.file_type().is_socket()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o077 == 0,
        "private local picker socket required",
    )?;
    Ok(metadata)
}
fn same_user(stream: &UnixStream) -> Result<()> {
    ensure(
        stream.peer_cred()?.uid() == rustix::process::geteuid().as_raw(),
        "local picker peer identity",
    )
}
impl PickerChannel {
    pub(crate) async fn bind(server: &str) -> Result<Self> {
        Self::bind_at(&control_directory(), server).await
    }
    pub(crate) async fn bind_at(directory: &Path, server: &str) -> Result<Self> {
        private_dir(directory)?;
        let directory = directory.canonicalize()?;
        ensure(
            std::fs::metadata(&directory)?.uid() == rustix::process::geteuid().as_raw(),
            "local picker directory owner",
        )?;
        let path = socket_path(&directory, server)?;
        if path.symlink_metadata().is_ok() {
            let old = owned_socket(&path)?;
            match tokio::time::timeout(Duration::from_millis(250), UnixStream::connect(&path)).await
            {
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                    let current = owned_socket(&path)?;
                    ensure(
                        (old.dev(), old.ino()) == (current.dev(), current.ino()),
                        "local picker socket changed",
                    )?;
                    std::fs::remove_file(&path)?;
                }
                _ => {
                    return Err(crate::Error::Rejected(
                        "another local picker owns this server".into(),
                    ));
                }
            }
        }
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let metadata = owned_socket(&path)?;
        Ok(Self {
            listener,
            path,
            inode: (metadata.dev(), metadata.ino()),
            server: server.into(),
            permission_requests: Default::default(),
        })
    }
    pub(crate) async fn serve(self, runtime: ComputerRuntime) -> Result<()> {
        loop {
            let (mut stream, _) = self.listener.accept().await?;
            // One bounded request at a time. A second caller cannot create unbounded child work.
            let _ =
                tokio::time::timeout(Duration::from_secs(22), self.handle(&mut stream, &runtime))
                    .await;
        }
    }
    async fn handle(&self, stream: &mut UnixStream, runtime: &ComputerRuntime) -> Result<()> {
        same_user(stream)?;
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut reader = BufReader::new(&mut *stream);
            (&mut reader)
                .take(16_385)
                .read_until(b'\n', &mut bytes)
                .await?;
            ensure(reader.buffer().is_empty(), "extra local picker input")?;
            Ok::<_, crate::Error>(())
        })
        .await
        .map_err(|_| crate::Error::Rejected("local picker request timeout".into()))??;
        ensure(bytes.len() <= 16_384, "local picker request too large")?;
        let value: serde_json::Value = parse(&bytes)?;
        if value.get("kind").is_some() {
            let request: WindowRequest = parse(&bytes)?;
            if let WindowRequest::Permissions {
                version,
                server,
                request_id,
                request,
            } = request
            {
                ensure(
                    version == 1
                        && server == self.server
                        && uuid::Uuid::parse_str(&request_id).is_ok(),
                    "invalid permission request binding",
                )?;
                if request.is_some() {
                    let mut seen = self
                        .permission_requests
                        .lock()
                        .map_err(|_| crate::Error::Rejected("permission request lock".into()))?;
                    let canonical = uuid::Uuid::parse_str(&request_id)
                        .map_err(|_| crate::Error::Rejected("permission request identity".into()))?
                        .to_string();
                    ensure(
                        seen.len() < 64 && seen.insert(canonical),
                        "permission request replay or capacity",
                    )?;
                }
                let mut extra = [0];
                let permissions = tokio::select! {
                    result=async {match request {Some(permission)=>runtime.request_permission(permission).await,None=>runtime.permissions().await}}=>result?,
                    _=stream.read(&mut extra)=>return Err(crate::Error::Rejected("permission request cancelled or extra input".into())),
                };
                stream
                    .write_all(&serde_json::to_vec(&PermissionResponse {
                        version: 1,
                        request_id,
                        permissions,
                    })?)
                    .await?;
                stream.shutdown().await?;
                return Ok(());
            }
            let WindowRequest::List { version, server } = request else {
                unreachable!()
            };
            ensure(
                version == 3 && server == self.server,
                "invalid window list binding",
            )?;
            let mut extra = [0];
            let windows = tokio::select! {
                result=runtime.list_windows()=>result?,
                _=stream.read(&mut extra)=>return Err(crate::Error::Rejected("window list cancelled or extra input".into())),
            };
            stream
                .write_all(&serde_json::to_vec(&WindowResponse {
                    version: 3,
                    windows,
                })?)
                .await?;
            stream.shutdown().await?;
            return Ok(());
        }
        let request: SelectionRequest = parse(&bytes)?;
        ensure(
            request.version == 2
                && request.server == self.server
                && request.permit.valid(crate::now()),
            "invalid local picker request",
        )?;
        // No pixels or Accessibility text are collected by this native identity/permission probe.
        let ready = if runtime
            .probe_selected(&request.permit.selection)
            .await
            .is_ok()
        {
            if request.replace {
                runtime.replace_local(request.permit.clone()).is_ok()
            } else {
                runtime.permit_local(request.permit.clone()).is_ok()
            }
        } else {
            false
        };
        let written = stream
            .write_all(&serde_json::to_vec(&SelectionResponse {
                version: 2,
                ready,
            })?)
            .await;
        if written.is_err() && ready {
            runtime.revoke_local(&request.permit);
        }
        written?;
        stream.shutdown().await?;
        Ok(())
    }
}

async fn list_windows(
    directory: &Path,
    server: &str,
) -> Result<Vec<pulse_core::ComputerWindowChoice>> {
    let value: WindowResponse = exchange(
        directory,
        server,
        &WindowRequest::List {
            version: 3,
            server: server.into(),
        },
    )
    .await?;
    ensure(
        value.version == 3
            && value.windows.len() <= 64
            && value
                .windows
                .iter()
                .all(pulse_core::ComputerWindowChoice::valid),
        "invalid window list",
    )?;
    Ok(value.windows)
}
pub(crate) struct WindowInventory {
    pub permissions: crate::computer_runtime::ComputerPermissions,
    pub windows: Result<Vec<pulse_core::ComputerWindowChoice>>,
}
pub(crate) async fn inventory(
    directory: &Path,
    server: &str,
    request_id: &str,
) -> Result<WindowInventory> {
    let permissions = permissions(directory, server, request_id, None).await?;
    let windows = if permissions.screen_capture {
        list_windows(directory, server).await
    } else {
        Ok(vec![])
    };
    Ok(WindowInventory {
        permissions,
        windows,
    })
}
pub(crate) async fn permissions(
    directory: &Path,
    server: &str,
    request_id: &str,
    request: Option<pulse_core::ComputerPermission>,
) -> Result<crate::computer_runtime::ComputerPermissions> {
    let value: PermissionResponse = exchange(
        directory,
        server,
        &WindowRequest::Permissions {
            version: 1,
            server: server.into(),
            request_id: request_id.into(),
            request,
        },
    )
    .await?;
    ensure(
        value.version == 1 && value.request_id == request_id,
        "permission response binding",
    )?;
    Ok(value.permissions)
}
async fn exchange<T: Serialize, R: serde::de::DeserializeOwned>(
    directory: &Path,
    server: &str,
    request: &T,
) -> Result<R> {
    let metadata = std::fs::symlink_metadata(directory)?;
    ensure(
        metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o077 == 0,
        "private picker directory required",
    )?;
    let directory = directory.canonicalize()?;
    let path = socket_path(&directory, server)?;
    owned_socket(&path)?;
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut stream = UnixStream::connect(&path).await?;
        same_user(&stream)?;
        let mut data = serde_json::to_vec(request)?;
        data.push(b'\n');
        stream.write_all(&data).await?;
        // Keep the write half open. Full disconnect cancels the executor child.
        let mut bytes = Vec::new();
        stream.take(65_537).read_to_end(&mut bytes).await?;
        ensure(bytes.len() <= 65_536, "window list response limit")?;
        parse(&bytes)
    })
    .await
    .map_err(|_| crate::Error::Rejected("window list timeout".into()))?
}
impl Drop for PickerChannel {
    fn drop(&mut self) {
        if owned_socket(&self.path).is_ok_and(|m| (m.dev(), m.ino()) == self.inode) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub(crate) async fn prepare(server: &str, permit: ComputerPermit, replace: bool) -> Result<()> {
    prepare_mode(&control_directory(), server, permit, replace).await
}
#[cfg(test)]
async fn prepare_at(directory: &Path, server: &str, permit: ComputerPermit) -> Result<()> {
    prepare_mode(directory, server, permit, false).await
}
async fn prepare_mode(
    directory: &Path,
    server: &str,
    permit: ComputerPermit,
    replace: bool,
) -> Result<()> {
    ensure(permit.valid(crate::now()), "local selection expired")?;
    let directory = directory.canonicalize()?;
    let metadata = std::fs::symlink_metadata(&directory)?;
    ensure(
        metadata.is_dir()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o077 == 0,
        "private picker directory required",
    )?;
    let path = socket_path(&directory, server)?;
    owned_socket(&path)?;
    tokio::time::timeout(Duration::from_secs(23), async {
        let mut stream = UnixStream::connect(&path).await?;
        same_user(&stream)?;
        stream
            .write_all(&serde_json::to_vec(&SelectionRequest {
                version: 2,
                server: server.into(),
                permit,
                replace,
            })?)
            .await?;
        stream.shutdown().await?;
        let mut bytes = Vec::new();
        stream.take(4097).read_to_end(&mut bytes).await?;
        ensure(bytes.len() <= 4096, "local picker response limit")?;
        let response: SelectionResponse = parse(&bytes)?;
        ensure(
            response.version == 2 && response.ready,
            "local window or permissions unavailable",
        )
    })
    .await
    .map_err(|_| crate::Error::Rejected("local picker timed out".into()))?
}

#[cfg(test)]
#[path = "computer_permission_tests.rs"]
mod permission_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::computer_runtime::tests::fixture;
    async fn window_stream(path: &Path, server: &str) -> UnixStream {
        let mut stream = UnixStream::connect(path).await.unwrap();
        let mut bytes = serde_json::to_vec(&WindowRequest::List {
            version: 3,
            server: server.into(),
        })
        .unwrap();
        bytes.push(b'\n');
        stream.write_all(&bytes).await.unwrap();
        stream
    }
    #[tokio::test]
    async fn frontend_neutral_local_inventory_does_not_capture_or_create_task_access() {
        let (_fixture, runtime) = fixture("list-good");
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let directory = root.path().join("control");
        let channel = PickerChannel::bind_at(&directory, "https://pulse.example/")
            .await
            .unwrap();
        let path = channel.path.clone();
        let worker = PickerWorker(tokio::spawn(channel.serve(runtime.clone())));
        let stream = window_stream(&path, "https://pulse.example/").await;
        let mut bytes = Vec::new();
        stream.take(65537).read_to_end(&mut bytes).await.unwrap();
        let response: WindowResponse = parse(&bytes).unwrap();
        assert_eq!(response.version, 3);
        assert_eq!(response.windows.len(), 1);
        assert_eq!(
            response.windows[0].selection.target,
            runtime.configured_target().unwrap().clone()
        );
        assert!(runtime.selections().is_empty());
        assert!(!_fixture.path().join("private/artifacts").exists());
        worker.stop().await;
    }
    #[tokio::test]
    async fn closing_window_picker_reaps_its_native_list_child_and_frees_capacity() {
        let (fixture, runtime) = fixture("list-hang");
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let directory = root.path().join("control");
        let channel = PickerChannel::bind_at(&directory, "https://pulse.example/")
            .await
            .unwrap();
        let path = channel.path.clone();
        let worker = PickerWorker(tokio::spawn(channel.serve(runtime.clone())));
        let stream = window_stream(&path, "https://pulse.example/").await;
        for _ in 0..200 {
            if fixture.path().join("list-hang.pid").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(fixture.path().join("list-hang.pid").exists());
        drop(stream);
        crate::computer_runtime::tests::assert_reaped(&runtime).await;
        assert!(runtime.permissions().await.is_ok());
        assert!(runtime.selections().is_empty());
        worker.stop().await;
    }
    #[tokio::test]
    async fn bad_inventory_origin_extra_authority_or_multiple_frames_never_start_a_child() {
        let (fixture, runtime) = fixture("list-good");
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let directory = root.path().join("control");
        let channel = PickerChannel::bind_at(&directory, "https://pulse.example/")
            .await
            .unwrap();
        let path = channel.path.clone();
        let worker = PickerWorker(tokio::spawn(channel.serve(runtime)));
        for bytes in [b"{\"kind\":\"list\",\"version\":3,\"server\":\"https://other.example/\"}\n".as_slice(),b"{\"kind\":\"list\",\"version\":3,\"server\":\"https://pulse.example/\",\"grant\":true}\n",b"{\"kind\":\"list\",\"version\":3,\"server\":\"https://pulse.example/\"}\n{}"] {
            let mut stream=UnixStream::connect(&path).await.unwrap();stream.write_all(bytes).await.unwrap();
            let mut response=Vec::new();let _=stream.read_to_end(&mut response).await;assert!(response.is_empty());
            assert!(!fixture.path().join("list-good.pid").exists());
        }
        worker.stop().await;
    }
    fn permit(runtime: &ComputerRuntime, task: &str) -> ComputerPermit {
        let selected_at = crate::now();
        ComputerPermit {
            task_id: task.into(),
            revision: "1".into(),
            selection: pulse_core::ComputerSelection {
                target: runtime.configured_target().unwrap().clone(),
                process_launched_at: 1,
            },
            selected_at,
            expires_at: selected_at + 900_000,
        }
    }
    #[tokio::test]
    async fn private_picker_grants_only_probed_task_instances_and_capacity_is_bounded() {
        let (_fixture, runtime) = fixture("good");
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let directory = root.path().join("control");
        let channel = PickerChannel::bind_at(&directory, "https://pulse.example/")
            .await
            .unwrap();
        let path = channel.path.clone();
        let mut worker = PickerWorker(tokio::spawn(channel.serve(runtime.clone())));
        let a = permit(&runtime, "task-a");
        prepare_at(&directory, "https://pulse.example/", a.clone())
            .await
            .unwrap();
        assert!(runtime.permitted("task-a", "1", &a.selection));
        assert!(!runtime.permitted("task-b", "1", &a.selection));
        assert!(!runtime.permitted("task-a", "2", &a.selection));
        assert!(!_fixture.path().join("private/artifacts").exists());
        prepare_at(&directory, "https://pulse.example/", a.clone())
            .await
            .unwrap();
        let mut changed = a.clone();
        changed.selected_at += 1;
        changed.expires_at += 1;
        assert!(
            prepare_at(&directory, "https://pulse.example/", changed)
                .await
                .is_err()
        );
        prepare_at(
            &directory,
            "https://pulse.example/",
            permit(&runtime, "task-b"),
        )
        .await
        .unwrap();
        assert!(
            prepare_at(
                &directory,
                "https://pulse.example/",
                permit(&runtime, "task-c")
            )
            .await
            .is_err()
        );
        assert_eq!(runtime.selections().len(), 2);
        let mut renewed = a.clone();
        renewed.selected_at = crate::now();
        renewed.expires_at = renewed.selected_at + 900_000;
        prepare_mode(&directory, "https://pulse.example/", renewed.clone(), true)
            .await
            .unwrap();
        assert!(runtime.selections().contains(&renewed));
        assert!(
            prepare_at(&directory, "https://pulse.example/", a)
                .await
                .is_err()
        );
        worker.0.abort();
        let _ = (&mut worker.0).await;
        assert!(!path.exists());
    }
    #[tokio::test]
    async fn wrong_origin_bad_probe_and_expired_selection_cannot_create_local_access() {
        for mode in ["permission", "probe-other"] {
            let (_fixture, runtime) = fixture(mode);
            let root = tempfile::tempdir_in("/tmp").unwrap();
            let directory = root.path().join("control");
            let channel = PickerChannel::bind_at(&directory, "https://pulse.example/")
                .await
                .unwrap();
            let path = channel.path.clone();
            let worker = PickerWorker(tokio::spawn(channel.serve(runtime.clone())));
            assert!(
                prepare_at(
                    &directory,
                    "https://pulse.example/",
                    permit(&runtime, "task")
                )
                .await
                .is_err()
            );
            let mut stream = UnixStream::connect(&path).await.unwrap();
            stream
                .write_all(
                    &serde_json::to_vec(&SelectionRequest {
                        version: 2,
                        server: "https://other.example/".into(),
                        permit: permit(&runtime, "other"),
                        replace: false,
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
            let mut output = Vec::new();
            stream.read_to_end(&mut output).await.unwrap();
            assert!(output.is_empty());
            let mut expired = permit(&runtime, "expired");
            expired.selected_at -= 900_001;
            expired.expires_at -= 900_001;
            assert!(
                prepare_at(&directory, "https://pulse.example/", expired)
                    .await
                    .is_err()
            );
            assert!(runtime.selections().is_empty());
            worker.stop().await;
        }
    }
    #[tokio::test]
    async fn picker_socket_refuses_active_owners_and_never_replaces_private_files() {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let directory = root.path().join("control");
        let first = PickerChannel::bind_at(&directory, "https://pulse.example/")
            .await
            .unwrap();
        assert!(
            PickerChannel::bind_at(&directory, "https://pulse.example/")
                .await
                .is_err()
        );
        let path = first.path.clone();
        drop(first);
        assert!(!path.exists());
        std::fs::write(&path, b"keep").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            PickerChannel::bind_at(&directory, "https://pulse.example/")
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"keep");
        std::fs::remove_file(&path).unwrap();
        let stale = std::os::unix::net::UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        drop(stale);
        let recovered = PickerChannel::bind_at(&directory, "https://pulse.example/")
            .await
            .unwrap();
        assert!(owned_socket(&path).is_ok());
        drop(recovered);
        assert!(!path.exists());
    }
    #[tokio::test]
    async fn stopped_picker_owner_reaps_a_slow_native_probe_and_removes_only_its_socket() {
        let (fixture, runtime) = fixture("hang");
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let directory = root.path().join("control");
        let channel = PickerChannel::bind_at(&directory, "https://pulse.example/")
            .await
            .unwrap();
        let path = channel.path.clone();
        let worker = PickerWorker(tokio::spawn(channel.serve(runtime.clone())));
        let chosen = permit(&runtime, "task");
        let selected_directory = directory.clone();
        let client = tokio::spawn(async move {
            prepare_at(&selected_directory, "https://pulse.example/", chosen).await
        });
        for _ in 0..200 {
            if fixture.path().join("hang.pid").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(fixture.path().join("hang.pid").exists());
        runtime.set_live(false);
        assert!(client.await.unwrap().is_err());
        crate::computer_runtime::tests::assert_reaped(&runtime).await;
        worker.stop().await;
        assert!(!path.exists());
        assert!(runtime.selections().is_empty());
    }
}
