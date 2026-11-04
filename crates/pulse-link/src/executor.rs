use crate::{Result, ensure, protocol::*};
use rustix::fs::{Mode, OFlags, open, openat};
use std::{
    fs::File,
    io::Read,
    path::{Component, Path, PathBuf},
    process::Stdio,
};
use tokio::io::AsyncReadExt;

#[derive(Clone)]
pub struct Executor {
    pub(crate) runtime_id: String,
    root: PathBuf,
    browser: Option<crate::browser_runtime::BrowserRuntime>,
    computer: Option<crate::computer_runtime::ComputerRuntime>,
    processes: crate::owned_process::ProcessTracker,
    pub(crate) guardian: Option<crate::process_guardian::Registry>,
}
impl Executor {
    pub(crate) fn supervise(&mut self, directory: &Path, executable: PathBuf) -> Result<()> {
        let registry = crate::process_guardian::Registry::new(
            &directory.join("runtime"),
            executable,
            self.runtime_id.clone(),
        )?;
        self.processes.supervise(registry.clone())?;
        if let Some(browser) = &mut self.browser {
            browser.processes.supervise(registry.clone())?;
            browser.guardian = Some(registry.clone());
        }
        if let Some(computer) = &self.computer {
            computer.supervise(registry.clone())?;
        }
        self.guardian = Some(registry);
        Ok(())
    }
    pub(crate) fn prior_resources_absent(&self) -> bool {
        self.browser
            .as_ref()
            .is_none_or(|b| b.prior_resources_absent())
            && self
                .computer
                .as_ref()
                .is_none_or(|c| c.prior_resources_absent())
    }
    pub(crate) async fn shutdown(&self) -> Result<()> {
        // Fence every adapter before waiting for any one adapter to finish.
        let mut results = vec![self.processes.close_admission()];
        if let Some(runtime) = &self.computer {
            results.push(runtime.begin_shutdown());
        }
        if let Some(runtime) = &self.browser {
            results.push(runtime.begin_shutdown());
        }
        let finished = tokio::time::timeout(std::time::Duration::from_secs(25), async {
            let (processes, computer, browser) = tokio::join!(
                self.processes.shutdown(),
                async {
                    match &self.computer {
                        Some(runtime) => runtime.finish_shutdown().await,
                        None => Ok(()),
                    }
                },
                async {
                    match &self.browser {
                        Some(runtime) => runtime.finish_shutdown().await,
                        None => Ok(()),
                    }
                },
            );
            [processes, computer, browser]
                .into_iter()
                .find_map(Result::err)
                .map_or(Ok(()), Err)
        })
        .await
        .map_err(|_| crate::Error::Rejected("executor shutdown unverified".into()));
        results.push(finished.and_then(|value| value));
        results
            .into_iter()
            .find_map(Result::err)
            .map_or(Ok(()), Err)
    }
    pub(crate) async fn invalidate_source(
        &self,
        context: &pulse_core::WorkerErasure,
    ) -> Result<()> {
        let scope = crate::owned_process::SourceScope::new(&context.task_id, &context.revision)?;
        let mut results = vec![self.processes.fence(&scope)];
        if let Some(computer) = &self.computer {
            results.push(computer.invalidate_source(&context.task_id, &context.revision));
        }
        if let Some(browser) = &self.browser {
            results.push(
                browser
                    .invalidate_source(&context.task_id, &context.revision)
                    .await,
            );
        }
        if let Some(computer) = &self.computer {
            results.push(
                computer
                    .wait_source(&context.task_id, &context.revision)
                    .await,
            );
        }
        results.push(self.processes.wait_source(&scope).await);
        results
            .into_iter()
            .find_map(Result::err)
            .map_or(Ok(()), Err)
    }
    pub fn new(root: &Path) -> Result<Self> {
        ensure(
            !std::fs::symlink_metadata(root)?.file_type().is_symlink(),
            "repo root must not be a symlink",
        )?;
        let root = root.canonicalize()?;
        ensure(root.is_dir(), "repo root must be a directory")?;
        Ok(Self {
            runtime_id: uuid::Uuid::new_v4().to_string(),
            root,
            browser: None,
            computer: None,
            processes: Default::default(),
            guardian: None,
        })
    }
    pub fn with_browser(mut self, browser: Option<crate::browser_runtime::BrowserRuntime>) -> Self {
        self.browser = browser;
        self
    }
    pub fn browser_ready(&self) -> Option<Option<String>> {
        self.browser
            .as_ref()
            .map(|b| b.policy.fixture_origin.as_ref().map(ToString::to_string))
    }
    pub fn with_computer(
        mut self,
        computer: Option<crate::computer_runtime::ComputerRuntime>,
    ) -> Self {
        self.computer = computer;
        self
    }
    pub(crate) fn computer_runtime(&self) -> Option<crate::computer_runtime::ComputerRuntime> {
        self.computer.clone()
    }
    pub(crate) async fn computer_capabilities(&self) -> Vec<pulse_core::ComputerPermit> {
        let Some(runtime) = &self.computer else {
            return vec![];
        };
        let entries = runtime.selections();
        if entries.is_empty() {
            return entries;
        }
        let mut live = Vec::new();
        for entry in entries {
            if runtime.probe_selected(&entry.selection).await.is_ok()
                && runtime.permitted(&entry.task_id, &entry.revision, &entry.selection)
            {
                live.push(entry);
            }
        }
        live
    }
    pub(crate) fn computer_permitted(
        &self,
        operation: &ReadOp,
        task: &str,
        revision: &str,
    ) -> bool {
        match operation {
            ReadOp::ComputerInspect { selection } | ReadOp::ComputerAction { selection, .. } => {
                self.computer
                    .as_ref()
                    .is_some_and(|runtime| runtime.permitted(task, revision, selection))
            }
            _ => true,
        }
    }
    pub async fn computer_permissions(
        &self,
    ) -> Result<Option<crate::computer_runtime::ComputerPermissions>> {
        match &self.computer {
            Some(runtime) => runtime.permissions().await.map(Some),
            None => Ok(None),
        }
    }
    pub async fn observe_computer(
        &self,
        binding: &pulse_core::ComputerBinding,
    ) -> Result<crate::computer_runtime::ComputerEvidence> {
        self.computer
            .as_ref()
            .ok_or_else(|| {
                crate::Error::Rejected("native computer observation is not installed".into())
            })?
            .observe(binding)
            .await
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn browser_interactions(&self) -> bool {
        self.browser.as_ref().is_some_and(|b| b.interactions)
    }
    pub async fn maintain_browser(
        &self,
        epoch: Option<u64>,
        tasks: &[pulse_core::Task],
    ) -> Result<()> {
        if let Some(runtime) = &self.computer {
            runtime.set_live(epoch.is_some());
            runtime.maintain(tasks)?;
        }
        if let Some(browser) = &self.browser {
            browser.maintain(epoch, tasks).await?;
        }
        Ok(())
    }
    pub async fn execute_bound(
        &self,
        operation: ReadOp,
        binding: &crate::browser_session::WorkBinding,
    ) -> crate::artifact::ExecutedRead {
        let rejected = |reason: &str| crate::artifact::ExecutedRead {
            outcome: ReadOutcome::Rejected {
                reason: reason.into(),
            },
            image: None,
        };
        if self.processes.closed() {
            return rejected("local executor is closing");
        }
        match operation {
            ReadOp::Browser { url } => self
                .browser_read(&url, Some(binding))
                .await
                .unwrap_or_else(|_| rejected("browser read rejected or failed")),
            ReadOp::GitStatus | ReadOp::GitDiff { .. } => {
                let source = match crate::owned_process::SourceScope::new(
                    &binding.task_id,
                    &binding.revision,
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("invalid process source"),
                };
                let path = match &operation {
                    ReadOp::GitDiff { path } => Some(path.as_str()),
                    _ => None,
                };
                let outcome =
                    self.git(path, Some(source))
                        .await
                        .unwrap_or_else(|_| ReadOutcome::Rejected {
                            reason: "local read rejected or failed; no write was executed".into(),
                        });
                crate::artifact::ExecutedRead {
                    outcome,
                    image: None,
                }
            }
            ReadOp::ComputerAction {
                selection,
                nonce,
                element_id,
                effect,
                grant,
            } => {
                let (Some(runtime), Some(grant)) = (&self.computer, grant) else {
                    return rejected("computer action lacks executor or approved grant");
                };
                let intent = serde_json::from_value::<pulse_core::ComputerIntent>(
                    grant.operation.intent.clone(),
                );
                if !intent.is_ok_and(|intent| {
                    intent.observation.selection() == selection
                        && intent.observation.nonce == nonce
                        && intent.element_id == element_id
                        && intent.effect == effect
                }) {
                    return rejected("computer action and approval mismatch");
                }
                runtime.act(binding, &grant).await
            }
            ReadOp::ComputerInspect { selection } => {
                let binding = pulse_core::ComputerBinding {
                    operation_id: binding.operation_id.clone(),
                    task_id: binding.task_id.clone(),
                    revision: match number(&binding.revision) {
                        Ok(value) => value,
                        Err(_) => return rejected("invalid computer binding"),
                    },
                    device: binding.device.clone(),
                    epoch: binding.epoch,
                };
                let Some(runtime) = &self.computer else {
                    return rejected("computer helper unavailable");
                };
                match runtime.observe_selected(&binding, &selection).await {
                    Ok(evidence) if evidence.observation.selection() == selection => {
                        crate::computer_runtime::executed_observation(evidence)
                            .unwrap_or_else(|_| rejected("computer evidence exceeds limits"))
                    }
                    _ => rejected("computer observation unavailable or selected process changed"),
                }
            }
            ReadOp::BrowserInspect { url } => {
                let Some(browser) = &self.browser else {
                    return rejected("browser unavailable");
                };
                let Some(url) = pulse_core::browser_url(&format!("browser:{url}")) else {
                    return rejected("browser URL rejected");
                };
                browser
                    .inspect(&url, binding)
                    .await
                    .unwrap_or_else(|_| rejected("browser inspection failed or exceeds limits"))
            }
            ReadOp::BrowserAction {
                session_id,
                nonce,
                element_id,
                effect,
                grant,
            } => {
                let (Some(browser), Some(grant)) = (&self.browser, grant) else {
                    return rejected("browser action lacks configured executor or approval grant");
                };
                browser
                    .act(binding, &session_id, &nonce, &element_id, &effect, &grant)
                    .await
            }
            operation => self.execute_work(operation).await,
        }
    }
    fn parts(path: &str) -> Result<Vec<&str>> {
        ensure(
            !path.is_empty() && path.len() <= 4096 && !Path::new(path).is_absolute(),
            "relative path required",
        )?;
        let mut result = Vec::new();
        for part in Path::new(path).components() {
            let Component::Normal(part) = part else {
                return Err(crate::Error::Rejected("path traversal".into()));
            };
            let s = part
                .to_str()
                .ok_or_else(|| crate::Error::Rejected("non UTF-8 path".into()))?;
            let lower = s.to_ascii_lowercase();
            ensure(
                !s.starts_with('.')
                    && !matches!(
                        lower.as_str(),
                        "target" | "node_modules" | "vendor" | "coverage"
                    )
                    && !lower.contains("secret")
                    && !lower.contains("credential")
                    && !lower.contains("token")
                    && !lower.contains("password")
                    && !lower.contains("passwd")
                    && !lower.contains("private")
                    && !lower.contains("keychain")
                    && !lower.ends_with(".pem")
                    && !lower.ends_with(".key")
                    && !lower.ends_with(".p12"),
                "private or excluded path",
            )?;
            result.push(s);
        }
        ensure(!result.is_empty(), "empty path")?;
        Ok(result)
    }
    fn descriptor(&self, path: &str, directory: bool) -> Result<File> {
        let parts = Self::parts(path)?;
        let mut fd = open(
            &self.root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        for (index, part) in parts.iter().enumerate() {
            let is_dir = index + 1 < parts.len() || directory;
            fd = openat(
                &fd,
                *part,
                OFlags::RDONLY
                    | OFlags::NOFOLLOW
                    | OFlags::CLOEXEC
                    | OFlags::NONBLOCK
                    | if is_dir {
                        OFlags::DIRECTORY
                    } else {
                        OFlags::empty()
                    },
                Mode::empty(),
            )?;
        }
        Ok(File::from(fd))
    }
    pub fn read_text(&self, path: &str) -> Result<ReadOutcome> {
        let ext = Path::new(path)
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        ensure(
            matches!(
                ext.as_str(),
                "md" | "rs" | "toml" | "json" | "yaml" | "yml" | "txt" | "lock"
            ),
            "text file extension is not allowed",
        )?;
        let file = self.descriptor(path, false)?;
        ensure(file.metadata()?.is_file(), "regular file required")?;
        let mut bytes = Vec::new();
        file.take((MAX_RESULT + 1) as u64).read_to_end(&mut bytes)?;
        let truncated = bytes.len() > MAX_RESULT;
        bytes.truncate(MAX_RESULT);
        let text = bounded_utf8(bytes, truncated)?;
        Ok(ReadOutcome::text(text, truncated))
    }
    pub fn list(&self, path: &str) -> Result<ReadOutcome> {
        let directory = if path.is_empty() {
            File::from(open(
                &self.root,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?)
        } else {
            self.descriptor(path, true)?
        };
        let mut directory = rustix::fs::Dir::read_from(&directory)?;
        let mut names = Vec::new();
        for entry in &mut directory {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy();
            if Self::parts(&name).is_ok() && entry.file_type() != rustix::fs::FileType::Symlink {
                names.push(name.into_owned());
            }
            if names.len() >= 256 {
                break;
            }
        }
        names.sort();
        Ok(ReadOutcome::text(names.join("\n"), names.len() >= 256))
    }
    async fn git(
        &self,
        diff: Option<&str>,
        source: Option<crate::owned_process::SourceScope>,
    ) -> Result<ReadOutcome> {
        let metadata = std::fs::symlink_metadata(self.root.join(".git"))?;
        ensure(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "Git reads require a local .git directory",
        )?;
        ensure(
            !self.root.join(".git/commondir").exists()
                && !self.root.join(".git/objects/info/alternates").exists(),
            "external Git object stores are not enabled",
        )?;
        if let Some(path) = diff {
            let _ = self.descriptor(path, false)?;
            let _ = self.read_text(path)?;
        }
        let mut command = tokio::process::Command::new("git");
        command
            .current_dir(&self.root)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .args([
                "--literal-pathspecs",
                "--no-optional-locks",
                "--no-pager",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "diff.external=",
                "-c",
                "core.pager=cat",
                "-c",
                "status.submoduleSummary=false",
            ])
            .arg(format!("--work-tree={}", self.root.display()))
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if let Some(path) = diff {
            command.args([
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--ignore-submodules=all",
                "--",
                path,
            ]);
        } else {
            command.args([
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=no",
                "--no-renames",
                "--ignore-submodules=all",
            ]);
        }
        let mut owner = self.processes.spawn(&mut command, source, None)?;
        let mut out = owner
            .child()
            .stdout
            .take()
            .ok_or_else(|| crate::Error::Rejected("missing git output".into()))?;
        let mut bytes = Vec::new();
        let execution = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            (&mut out)
                .take((MAX_RESULT + 1) as u64)
                .read_to_end(&mut bytes)
                .await?;
            if bytes.len() > MAX_RESULT {
                owner.terminate()?;
            }
            owner.wait().await
        })
        .await;
        let status = match execution {
            Ok(result) => result?,
            Err(_) => {
                let _ = owner.terminate();
                let _ = owner.wait().await;
                return Err(crate::Error::Rejected("git timed out".into()));
            }
        };
        let truncated = bytes.len() > MAX_RESULT;
        bytes.truncate(MAX_RESULT);
        ensure(status.success() || truncated, "git read failed")?;
        let text = bounded_utf8(bytes, truncated)?;
        let text = if diff.is_some() {
            text
        } else {
            text.split('\0')
                .filter(|s| s.len() > 3 && Self::parts(&s[3..]).is_ok())
                .collect::<Vec<_>>()
                .join("\n")
        };
        Ok(ReadOutcome::text(text, truncated))
    }
    pub async fn execute(&self, operation: ReadOp) -> ReadOutcome {
        self.execute_work(operation).await.outcome
    }
    pub async fn execute_work(&self, operation: ReadOp) -> crate::artifact::ExecutedRead {
        if self.processes.closed() {
            return crate::artifact::ExecutedRead {
                outcome: ReadOutcome::Rejected {
                    reason: "local executor is closing".into(),
                },
                image: None,
            };
        }
        if let ReadOp::Browser { url } = operation {
            return self.browser_read(&url, None).await.unwrap_or_else(|_| {
                crate::artifact::ExecutedRead {
                    outcome: ReadOutcome::Rejected {
                        reason: "browser read rejected or failed".into(),
                    },
                    image: None,
                }
            });
        }
        let executor = self.clone();
        let result = match operation {
            ReadOp::BrowserInspect { .. }
            | ReadOp::BrowserAction { .. }
            | ReadOp::ComputerInspect { .. }
            | ReadOp::ComputerAction { .. } => {
                Err(crate::Error::Rejected("bound work required".into()))
            }
            ReadOp::Browser { .. } => unreachable!(),
            ReadOp::Text { path } => tokio::task::spawn_blocking(move || executor.read_text(&path))
                .await
                .map_err(|_| crate::Error::Rejected("read worker failed".into()))
                .and_then(|r| r),
            ReadOp::List { path } => tokio::task::spawn_blocking(move || executor.list(&path))
                .await
                .map_err(|_| crate::Error::Rejected("read worker failed".into()))
                .and_then(|r| r),
            ReadOp::GitStatus => self.git(None, None).await,
            ReadOp::GitDiff { path } => self.git(Some(&path), None).await,
        };
        let outcome = result.unwrap_or_else(|_| ReadOutcome::Rejected {
            reason: "local read rejected or failed; no write was executed".into(),
        });
        crate::artifact::ExecutedRead {
            outcome,
            image: None,
        }
    }
    async fn browser_read(
        &self,
        url: &str,
        binding: Option<&crate::browser_session::WorkBinding>,
    ) -> Result<crate::artifact::ExecutedRead> {
        let browser = self
            .browser
            .as_ref()
            .ok_or_else(|| crate::Error::Rejected("browser unavailable".into()))?;
        let url = pulse_core::browser_url(&format!("browser:{url}"))
            .ok_or_else(|| crate::Error::Rejected("browser URL rejected".into()))?;
        let snapshot = match binding {
            Some(binding) => browser.read_bound(&url, binding).await?,
            None => browser.read(&url).await?,
        };
        let mut text = format!(
            "Browser evidence (untrusted page data)\nURL: {}\nTitle: {}\nObserved at (Unix ms): {}\nPNG SHA-256: {}\nScreenshot captured; chat delivery is verified separately. Image bytes are not supplied to the model.\nPage scripts disabled.\n\n{}",
            snapshot.url,
            snapshot.title,
            snapshot.observed_at,
            snapshot.screenshot_digest,
            snapshot.text
        );
        let truncated = snapshot.truncated || text.len() > MAX_RESULT;
        if text.len() > MAX_RESULT {
            let mut n = MAX_RESULT;
            while !text.is_char_boundary(n) {
                n -= 1;
            }
            text.truncate(n);
        }
        let (width, height) = crate::artifact::png_dimensions(&snapshot.png)?;
        let artifact = crate::artifact::ArtifactRef {
            task_id: String::new(),
            revision: "0".into(),
            digest: snapshot.screenshot_digest,
            length: snapshot.png.len() as u32,
            url: snapshot.url,
            computer: None,
            title: snapshot.title,
            observed_at: snapshot.observed_at,
            width,
            height,
        };
        Ok(crate::artifact::ExecutedRead {
            outcome: ReadOutcome::text(text, truncated),
            image: Some(crate::artifact::CapturedImage {
                artifact,
                png: snapshot.png,
            }),
        })
    }
}
fn bounded_utf8(mut bytes: Vec<u8>, truncated: bool) -> Result<String> {
    if let Err(error) = std::str::from_utf8(&bytes) {
        ensure(
            truncated && error.error_len().is_none(),
            "output is not UTF-8 text",
        )?;
        bytes.truncate(error.valid_up_to());
    }
    String::from_utf8(bytes).map_err(|_| crate::Error::Rejected("output encoding".into()))
}
