//! Fixed native helper IPC. The model cannot select its executable, arguments or cache path.
use crate::{Result, artifact::png_dimensions, ensure, journal::private_dir, protocol::hash_bytes};
use pulse_core::{ComputerBinding, ComputerImageRef, ComputerObservation, ComputerTarget};
use rustix::process::{Pid, Signal, kill_process_group};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
};

const VERSION: &str = "pulse-computer-observe-5";
const RESPONSE_LIMIT: usize = 65_536;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComputerPermissions {
    pub accessibility: bool,
    pub screen_capture: bool,
}
impl ComputerPermissions {
    pub fn can_observe(&self) -> bool {
        self.accessibility && self.screen_capture
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeResponse {
    version: String,
    permissions: ComputerPermissions,
    observation: Option<NativeObservation>,
    error: Option<NativeError>,
    selection: Option<pulse_core::ComputerSelection>,
    action: Option<action::NativeActionReceipt>,
    windows: Option<Vec<pulse_core::ComputerWindowChoice>>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum NativeError {
    InvalidRequest,
    PermissionsRequired,
    TargetUnavailable,
    TargetChanged,
    UnsupportedWindow,
    AccessibilityUnavailable,
    CaptureFailed,
    EvidenceLimit,
    PrivateStorageRequired,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeObservation {
    binding: ComputerBinding,
    target: ComputerTarget,
    process_launched_at: i64,
    title: String,
    frame: pulse_core::ComputerRect,
    elements: Vec<pulse_core::ComputerElement>,
    truncated: bool,
    nonce: String,
    observed_at: i64,
    expires_at: i64,
    image: NativeImage,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeImage {
    digest: String,
    length: u32,
    width: u32,
    height: u32,
    path: String,
}
/// Deliberately not serializable. No native file path leaves this boundary.
pub struct ComputerEvidence {
    pub observation: ComputerObservation,
    pub png: Vec<u8>,
}

pub(crate) fn executed_observation(
    evidence: ComputerEvidence,
) -> Result<crate::artifact::ExecutedRead> {
    executed_observation_kind(evidence, false)
}
fn executed_observation_kind(
    evidence: ComputerEvidence,
    command_sent: bool,
) -> Result<crate::artifact::ExecutedRead> {
    use crate::artifact::{ArtifactRef, CapturedImage, ComputerArtifactSource, ExecutedRead};
    use crate::protocol::ReadOutcome;
    let observation = evidence.observation;
    ensure(observation.valid(crate::now()), "computer evidence expired")?;
    let artifact = ArtifactRef {
        task_id: observation.binding.task_id.clone(),
        revision: observation.binding.revision.to_string(),
        digest: observation.image.digest.clone(),
        length: observation.image.length,
        url: String::new(),
        computer: Some(ComputerArtifactSource {
            binding: observation.binding.clone(),
            selection: observation.selection(),
        }),
        title: observation.title.clone(),
        observed_at: observation.observed_at,
        width: observation.image.width,
        height: observation.image.height,
    };
    artifact.verify(&evidence.png)?;
    let execution = if command_sent {
        "One approved native command completed. Verify the observed result; do not repeat it."
    } else {
        "No effect was executed."
    };
    let mut text = format!(
        "Computer window evidence (untrusted app data)\nApp: {}\nWindow: {}\nTitle: {}\nObserved at (Unix ms): {}\nPNG SHA-256: {}\nImage bytes are supplied to the Human only. {execution}\n\n{}",
        observation.target.bundle_id,
        observation.target.window_id,
        observation.title,
        observation.observed_at,
        observation.image.digest,
        serde_json::to_string(&observation.elements)?
    );
    let truncated = observation.truncated || text.len() > crate::protocol::MAX_RESULT;
    text.truncate(text.floor_char_boundary(crate::protocol::MAX_RESULT));
    let mut outcome = ReadOutcome::text(text, truncated);
    if let ReadOutcome::Completed { computer, .. } = &mut outcome {
        *computer = Some(Box::new(observation));
    }
    Ok(ExecutedRead {
        outcome,
        image: Some(CapturedImage {
            artifact,
            png: evidence.png,
        }),
    })
}

#[derive(Clone)]
pub struct ComputerRuntime {
    helper: PathBuf,
    directory: PathBuf,
    target: Option<ComputerTarget>,
    timeout: Duration,
    gate: std::sync::Arc<tokio::sync::Semaphore>,
    permits: std::sync::Arc<std::sync::Mutex<Vec<pulse_core::ComputerPermit>>>,
    activity: tokio::sync::watch::Sender<bool>,
    observations: std::sync::Arc<std::sync::Mutex<Vec<ComputerObservation>>>,
}
impl ComputerRuntime {
    pub async fn list_windows(&self) -> Result<Vec<pulse_core::ComputerWindowChoice>> {
        let response = self.call(&["--list".into()], None, self.timeout).await?;
        ensure(
            response.error.is_none()
                && response.permissions.screen_capture
                && response.observation.is_none()
                && response.selection.is_none()
                && response.action.is_none(),
            "native window list unavailable",
        )?;
        let windows = response
            .windows
            .ok_or_else(|| crate::Error::Rejected("native window list missing".into()))?;
        ensure(
            windows.len() <= 64
                && windows.iter().all(|w| {
                    w.valid()
                        && self
                            .target
                            .as_ref()
                            .is_none_or(|target| *target == w.selection.target)
                }),
            "native window list scope or capacity",
        )?;
        let mut ids = std::collections::BTreeSet::new();
        ensure(
            windows
                .iter()
                .all(|w| ids.insert(w.selection.target.window_id)),
            "duplicate native window",
        )?;
        Ok(windows)
    }
    /// A local operator's selection only. No request or model can set these values.
    pub fn from_environment(directory: &Path) -> Result<Option<Self>> {
        let bundle = std::env::var("PULSE_COMPUTER_BUNDLE_ID").ok();
        let pid = std::env::var("PULSE_COMPUTER_PID").ok();
        let window = std::env::var("PULSE_COMPUTER_WINDOW_ID").ok();
        let target = match (bundle, pid, window) {
            (None, None, None) => None,
            (Some(bundle_id), Some(pid), Some(window)) => Some(ComputerTarget {
                bundle_id,
                pid: pid
                    .parse()
                    .map_err(|_| crate::Error::Rejected("invalid local computer pid".into()))?,
                window_id: window
                    .parse()
                    .map_err(|_| crate::Error::Rejected("invalid local computer window".into()))?,
            }),
            _ => {
                return Err(crate::Error::Rejected(
                    "incomplete local computer selection".into(),
                ));
            }
        };
        Self::bundled(&directory.join("computer"), target)
    }
    /// Only the installed sibling of this Rust local process is eligible.
    pub fn bundled(directory: &Path, target: Option<ComputerTarget>) -> Result<Option<Self>> {
        if !cfg!(target_os = "macos") {
            return Ok(None);
        }
        let executable = std::env::current_exe()?;
        let Some(parent) = executable
            .parent()
            .filter(|p| p.ends_with("Contents/MacOS"))
        else {
            return Ok(None);
        };
        let helper = parent.join("pulse-computer");
        if !helper.is_file() {
            return Ok(None);
        }
        Self::new(helper, directory.to_path_buf(), target).map(Some)
    }
    fn new(helper: PathBuf, directory: PathBuf, target: Option<ComputerTarget>) -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::symlink_metadata(&helper)?;
        ensure(
            helper.is_absolute()
                && metadata.is_file()
                && metadata.permissions().mode() & 0o022 == 0
                && metadata.permissions().mode() & 0o111 != 0,
            "native helper must be a fixed private executable",
        )?;
        ensure(
            target.as_ref().is_none_or(ComputerTarget::valid),
            "invalid local computer target",
        )?;
        Ok(Self {
            helper,
            directory,
            target,
            timeout: Duration::from_secs(18),
            gate: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            permits: Default::default(),
            observations: Default::default(),
            activity: tokio::sync::watch::channel(false).0,
        })
    }
    pub fn configured_target(&self) -> Option<&ComputerTarget> {
        self.target.as_ref()
    }
    pub async fn permissions(&self) -> Result<ComputerPermissions> {
        let response = self
            .call(
                &["--status".into()],
                None,
                self.timeout.min(Duration::from_secs(3)),
            )
            .await?;
        ensure(
            response.error.is_none()
                && response.observation.is_none()
                && response.selection.is_none()
                && response.action.is_none(),
            "invalid native permission response",
        )?;
        Ok(response.permissions)
    }
    /// Local caller metadata is validated here. Human authorization remains upstream.
    pub async fn observe(&self, binding: &ComputerBinding) -> Result<ComputerEvidence> {
        let target = self
            .target
            .as_ref()
            .ok_or_else(|| crate::Error::Rejected("no local computer target".into()))?;
        self.observe_inner(binding, None, target).await
    }
    pub async fn observe_selected(
        &self,
        binding: &ComputerBinding,
        selection: &pulse_core::ComputerSelection,
    ) -> Result<ComputerEvidence> {
        ensure(
            self.permitted(&binding.task_id, &binding.revision.to_string(), selection),
            "computer task lacks live local Human selection",
        )?;
        let evidence = self
            .observe_inner(binding, Some(selection), &selection.target)
            .await?;
        ensure(
            self.permitted(&binding.task_id, &binding.revision.to_string(), selection),
            "local selection was withdrawn",
        )?;
        {
            let mut observations = self
                .observations
                .lock()
                .map_err(|_| crate::Error::Rejected("local observations unavailable".into()))?;
            observations
                .retain(|old| old.valid(crate::now()) && old.binding.task_id != binding.task_id);
            ensure(observations.len() < 2, "computer observation capacity")?;
            observations.push(evidence.observation.clone());
        }
        Ok(evidence)
    }
    pub fn permitted(
        &self,
        task: &str,
        revision: &str,
        selection: &pulse_core::ComputerSelection,
    ) -> bool {
        *self.activity.borrow()
            && self.permits.lock().is_ok_and(|entries| {
                entries.iter().any(|entry| {
                    entry.valid(crate::now())
                        && entry.task_id == task
                        && entry.revision == revision
                        && entry.selection == *selection
                })
            })
    }
    pub(crate) fn set_live(&self, live: bool) {
        if !live && let Ok(mut observations) = self.observations.lock() {
            observations.clear();
        }
        self.activity.send_if_modified(|old| {
            if *old == live {
                false
            } else {
                *old = live;
                true
            }
        });
    }
    pub(crate) fn permit_local(&self, permit: pulse_core::ComputerPermit) -> Result<()> {
        self.admit_local(permit, false)
    }
    pub(crate) fn replace_local(&self, permit: pulse_core::ComputerPermit) -> Result<()> {
        self.admit_local(permit, true)
    }
    fn admit_local(&self, permit: pulse_core::ComputerPermit, replace: bool) -> Result<()> {
        ensure(
            *self.activity.borrow(),
            "local computer executor is not live",
        )?;
        ensure(
            permit.valid(crate::now())
                && self
                    .target
                    .as_ref()
                    .is_none_or(|target| *target == permit.selection.target),
            "invalid local computer consent or operator scope",
        )?;
        let mut entries = self
            .permits
            .lock()
            .map_err(|_| crate::Error::Rejected("local selection unavailable".into()))?;
        entries.retain(|entry| entry.valid(crate::now()));
        if let Some(old) = entries
            .iter_mut()
            .find(|entry| entry.task_id == permit.task_id)
        {
            if old == &permit {
                return Ok(());
            }
            let old_revision = crate::protocol::number(&old.revision)?;
            let new_revision = crate::protocol::number(&permit.revision)?;
            ensure(
                replace
                    && permit.selected_at > old.selected_at
                    && (new_revision > old_revision
                        || (new_revision == old_revision && permit.selection == old.selection)),
                "local computer selection identity conflict",
            )?;
            *old = permit;
            return Ok(());
        }
        ensure(entries.len() < 2, "local computer selection capacity")?;
        entries.push(permit);
        Ok(())
    }
    pub(crate) fn selections(&self) -> Vec<pulse_core::ComputerPermit> {
        self.permits
            .lock()
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| entry.valid(crate::now()))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
    pub(crate) fn revoke_local(&self, permit: &pulse_core::ComputerPermit) {
        if let Ok(mut entries) = self.permits.lock() {
            entries.retain(|entry| entry != permit);
        }
    }
    pub(crate) fn invalidate_source(&self, task: &str, revision: &str) {
        if let Ok(mut permits) = self.permits.lock() {
            permits.retain(|p| p.task_id != task || p.revision != revision);
        }
        if let Ok(mut observations) = self.observations.lock() {
            observations.retain(|o| {
                o.binding.task_id != task || o.binding.revision.to_string() != revision
            });
        }
    }
    pub(crate) fn maintain(&self, tasks: &[pulse_core::Task]) {
        if let Ok(mut entries) = self.permits.lock() {
            entries.retain(|entry| {
                entry.valid(crate::now())
                    && tasks
                        .iter()
                        .find(|task| task.id == entry.task_id)
                        .is_none_or(|task| {
                            !task.state.terminal()
                                && task.deadline > crate::now()
                                && !task.cancel_requested
                                && task.reason.as_deref() != Some("model_reply_ready")
                                && task.revision.to_string() == entry.revision
                                && entry.selection.scope().as_deref() == Some(&task.scope)
                        })
            });
        }
        let permits = self.selections();
        if let Ok(mut observations) = self.observations.lock() {
            observations.retain(|view| {
                view.valid(crate::now())
                    && permits.iter().any(|permit| {
                        permit.task_id == view.binding.task_id
                            && permit.revision == view.binding.revision.to_string()
                            && permit.selection == view.selection()
                    })
            });
        }
    }
    pub async fn probe_selected(&self, selection: &pulse_core::ComputerSelection) -> Result<()> {
        ensure(
            *self.activity.borrow(),
            "local computer executor is not live",
        )?;
        ensure(
            selection.valid()
                && self
                    .target
                    .as_ref()
                    .is_none_or(|target| *target == selection.target),
            "outside local operator scope",
        )?;
        let args = self.arguments(&selection.target, &self.directory)?;
        let request = serde_json::to_vec(
            &serde_json::json!({"kind":"probe","expected_process_launched_at":selection.process_launched_at.to_string()}),
        )?;
        let mut activity = self.activity.subscribe();
        let response = tokio::select! {
            response=self.call(&args,Some(request),self.timeout)=>response?,
            _=activity.wait_for(|live| !*live)=>return Err(crate::Error::Rejected("local executor stopped during probe".into())),
        };
        ensure(
            response.error.is_none()
                && response.observation.is_none()
                && response.action.is_none()
                && response.permissions.can_observe()
                && response.selection.as_ref() == Some(selection),
            "selected window or native permissions unavailable",
        )
    }
    fn arguments(&self, target: &ComputerTarget, cache: &Path) -> Result<Vec<String>> {
        Ok(vec![
            "--bundle-id".into(),
            target.bundle_id.clone(),
            "--pid".into(),
            target.pid.to_string(),
            "--window-id".into(),
            target.window_id.to_string(),
            "--cache".into(),
            cache
                .to_str()
                .ok_or_else(|| crate::Error::Rejected("computer cache encoding".into()))?
                .into(),
        ])
    }
    async fn observe_inner(
        &self,
        binding: &ComputerBinding,
        selection: Option<&pulse_core::ComputerSelection>,
        target: &ComputerTarget,
    ) -> Result<ComputerEvidence> {
        ensure(binding.valid(), "invalid computer work binding")?;
        private_dir(&self.directory)?;
        let directory = self.directory.canonicalize()?;
        let cache = directory.join("artifacts");
        private_dir(&cache)?;
        ensure(
            std::fs::read_dir(&cache)?.take(128).count() < 128,
            "computer artifact capacity",
        )?;
        let mut request = serde_json::json!({"kind":"observe","binding":binding});
        if let Some(selection) = selection {
            request["expected_process_launched_at"] =
                selection.process_launched_at.to_string().into();
        }
        let request = serde_json::to_vec(&request)?;
        ensure(request.len() <= 16_384, "computer request limit")?;
        let args = self.arguments(target, &cache)?;
        let started = crate::now();
        let response = self.call(&args, Some(request), self.timeout).await?;
        ensure(
            response.error.is_none()
                && response.selection.is_none()
                && response.action.is_none()
                && response.permissions.can_observe(),
            "native observation unavailable; check permission and selected window",
        )?;
        let raw = response
            .observation
            .ok_or_else(|| crate::Error::Rejected("native observation missing".into()))?;
        self.validate_evidence(raw, binding, selection, target, started, &cache)
            .await
    }
    async fn validate_evidence(
        &self,
        raw: NativeObservation,
        binding: &ComputerBinding,
        selection: Option<&pulse_core::ComputerSelection>,
        target: &ComputerTarget,
        started: i64,
        cache: &Path,
    ) -> Result<ComputerEvidence> {
        let expected = cache.join(format!("{}.png", raw.image.digest));
        let observation = ComputerObservation {
            binding: raw.binding,
            target: raw.target,
            process_launched_at: raw.process_launched_at,
            title: raw.title,
            frame: raw.frame,
            elements: raw.elements,
            truncated: raw.truncated,
            nonce: raw.nonce,
            observed_at: raw.observed_at,
            expires_at: raw.expires_at,
            image: ComputerImageRef {
                digest: raw.image.digest,
                length: raw.image.length,
                width: raw.image.width,
                height: raw.image.height,
            },
        };
        ensure(
            observation.valid(crate::now())
                && observation.binding == *binding
                && observation.target == *target
                && selection.is_none_or(|selected| observation.selection() == *selected)
                && observation.observed_at >= started.saturating_sub(1000)
                && Path::new(&raw.image.path) == expected,
            "native computer evidence source mismatch",
        )?;
        let meta = observation.image.clone();
        let png = tokio::task::spawn_blocking(move || read_image(&expected, &meta))
            .await
            .map_err(|_| crate::Error::Rejected("native image worker failed".into()))??;
        ensure(
            observation.valid(crate::now()),
            "native observation expired before delivery",
        )?;
        Ok(ComputerEvidence { observation, png })
    }
    async fn call(
        &self,
        args: &[String],
        input: Option<Vec<u8>>,
        timeout: Duration,
    ) -> Result<NativeResponse> {
        let _permit = self
            .gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| crate::Error::Capacity)?;
        self.call_with_permit(args, input, timeout, _permit).await
    }
    async fn call_with_permit(
        &self,
        args: &[String],
        input: Option<Vec<u8>>,
        timeout: Duration,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<NativeResponse> {
        tokio::time::timeout(timeout, async {
            let mut command = Command::new(&self.helper);
            command
                .env_clear()
                .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
                .args(args)
                .stdin(if input.is_some() {
                    Stdio::piped()
                } else {
                    Stdio::null()
                })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .process_group(0)
                .kill_on_drop(true);
            if let Some(home) = std::env::var_os("HOME") {
                command.env("HOME", home);
            }
            if let Some(target) = &self.target {
                command
                    .env("PULSE_COMPUTER_BUNDLE_ID", &target.bundle_id)
                    .env("PULSE_COMPUTER_PID", target.pid.to_string())
                    .env("PULSE_COMPUTER_WINDOW_ID", target.window_id.to_string());
            }
            let child = command.spawn()?;
            let pid = child
                .id()
                .and_then(|id| i32::try_from(id).ok())
                .and_then(Pid::from_raw)
                .ok_or_else(|| crate::Error::Rejected("native process group missing".into()))?;
            let mut owner = NativeChild {
                child,
                pid: Some(pid),
            };
            let mut stdout = owner
                .child
                .stdout
                .take()
                .ok_or_else(|| crate::Error::Rejected("native output missing".into()))?;
            let mut stderr = owner
                .child
                .stderr
                .take()
                .ok_or_else(|| crate::Error::Rejected("native diagnostics missing".into()))?;
            let stdin = owner.child.stdin.take();
            let (bytes, _) = tokio::try_join!(
                async {
                    let mut bytes = Vec::new();
                    (&mut stdout)
                        .take((RESPONSE_LIMIT + 1) as u64)
                        .read_to_end(&mut bytes)
                        .await?;
                    ensure(bytes.len() <= RESPONSE_LIMIT, "native output limit")?;
                    Ok::<_, crate::Error>(bytes)
                },
                async {
                    if let Some(mut stdin) = stdin {
                        if let Some(input) = input {
                            stdin.write_all(&input).await?;
                        }
                        stdin.shutdown().await?;
                        drop(stdin);
                    }
                    let mut discarded = Vec::new();
                    (&mut stderr).take(4097).read_to_end(&mut discarded).await?;
                    ensure(discarded.len() <= 4096, "native diagnostics limit")?;
                    Ok::<_, crate::Error>(())
                }
            )?;
            let status = owner.child.wait().await?;
            owner.pid = None;
            ensure(
                status.success(),
                "native helper exited without a valid result",
            )?;
            let response: NativeResponse = serde_json::from_slice(&bytes)
                .map_err(|_| crate::Error::Rejected("invalid native helper response".into()))?;
            ensure(
                response.version == VERSION,
                "native helper version mismatch",
            )?;
            if args != ["--list"] {
                ensure(
                    response.windows.is_none(),
                    "unexpected native window inventory",
                )?;
            }
            Ok(response)
        })
        .await
        .map_err(|_| crate::Error::Rejected("native helper timed out".into()))?
    }
}
struct NativeChild {
    child: Child,
    pid: Option<Pid>,
}
impl Drop for NativeChild {
    fn drop(&mut self) {
        if let Some(pid) = self.pid.take() {
            let _ = kill_process_group(pid, Signal::KILL);
        }
    }
}
fn read_image(path: &Path, image: &ComputerImageRef) -> Result<Vec<u8>> {
    use std::{
        io::Read,
        os::unix::fs::{MetadataExt, OpenOptionsExt},
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    ensure(
        metadata.is_file()
            && metadata.mode() & 0o777 == 0o600
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.len() == u64::from(image.length),
        "invalid native image cache entry",
    )?;
    let mut bytes = Vec::new();
    file.take(u64::from(image.length) + 1)
        .read_to_end(&mut bytes)?;
    ensure(
        bytes.len() == image.length as usize
            && hash_bytes(&bytes) == image.digest
            && png_dimensions(&bytes)? == (image.width, image.height),
        "native image hash, length or dimensions mismatch",
    )?;
    Ok(bytes)
}

#[cfg(test)]
#[path = "computer_runtime_tests.rs"]
pub(crate) mod tests;

#[path = "computer_action.rs"]
mod action;
pub use action::ComputerGrant;
