//! One private Chrome profile per read. No shell, personal profile or model-selected executable.
use crate::{
    Result,
    browser::{BrowserSnapshot, FixtureBrowser},
    browser_network::{BrowserProxy, NetworkPolicy},
    ensure,
    journal::{private_dir, private_write},
    owned_process::{OwnedProcess, ProcessTracker, SourceScope},
};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
};
use url::Url;

#[derive(Clone)]
pub struct BrowserRuntime {
    driver: PathBuf,
    pub policy: NetworkPolicy,
    directory: PathBuf,
    pub(crate) sessions: std::sync::Arc<
        tokio::sync::Mutex<
            std::collections::BTreeMap<String, crate::browser_session::InteractiveSession>,
        >,
    >,
    pub interactions: bool,
    pub(crate) processes: ProcessTracker,
}
impl BrowserRuntime {
    pub fn from_environment(directory: &Path) -> Result<Option<Self>> {
        let Some(driver) = std::env::var_os("PULSE_BROWSER_DRIVER") else {
            return Ok(None);
        };
        let driver = PathBuf::from(driver);
        if !driver.is_absolute() || !driver.is_file() {
            return Ok(None);
        }
        let fixture = std::env::var("PULSE_BROWSER_FIXTURE_ORIGIN").ok();
        let policy = NetworkPolicy::new(fixture.as_deref())?;
        let directory = directory.join("browser");
        private_dir(&directory)?;
        private_dir(&directory.join("artifacts"))?;
        Ok(Some(Self {
            driver,
            policy,
            directory,
            sessions: Default::default(),
            interactions: std::env::var("PULSE_BROWSER_INTERACTION").as_deref() == Ok("1"),
            processes: Default::default(),
        }))
    }
    pub fn new(driver: PathBuf, policy: NetworkPolicy, directory: PathBuf) -> Result<Self> {
        ensure(
            driver.is_absolute() && driver.is_file(),
            "browser driver unavailable",
        )?;
        private_dir(&directory)?;
        private_dir(&directory.join("artifacts"))?;
        Ok(Self {
            driver,
            policy,
            directory,
            sessions: Default::default(),
            interactions: false,
            processes: Default::default(),
        })
    }
    pub async fn read(&self, url: &Url) -> Result<BrowserSnapshot> {
        ensure(!self.processes.closed(), "browser executor is closing")?;
        self.policy.validate_url(url)?;
        self.artifact_room()?;
        tokio::time::timeout(Duration::from_secs(35), self.read_inner(url, None))
            .await
            .map_err(|_| crate::Error::Rejected("browser read timed out".into()))?
    }
    pub(crate) async fn read_bound(
        &self,
        url: &Url,
        binding: &crate::browser_session::WorkBinding,
    ) -> Result<BrowserSnapshot> {
        ensure(!self.processes.closed(), "browser executor is closing")?;
        self.policy.validate_url(url)?;
        self.artifact_room()?;
        let scope = SourceScope::new(&binding.task_id, &binding.revision)?;
        tokio::time::timeout(Duration::from_secs(35), self.read_inner(url, Some(scope)))
            .await
            .map_err(|_| crate::Error::Rejected("browser read timed out".into()))?
    }
    pub(crate) fn artifact_room(&self) -> Result<()> {
        ensure(
            std::fs::read_dir(self.directory.join("artifacts"))?
                .take(128)
                .count()
                < 128,
            "browser artifact limit reached",
        )
    }
    pub fn with_interactions(mut self, enabled: bool) -> Self {
        self.interactions = enabled;
        self
    }
    async fn read_inner(&self, url: &Url, source: Option<SourceScope>) -> Result<BrowserSnapshot> {
        let mut session = self.start(url, source).await?;
        let result = async {
            session.browser.navigate(url).await?;
            session.browser.observe().await
        }
        .await;
        let closed = session.browser.close().await;
        session.owner.shutdown().await?;
        let snapshot = result?;
        closed?;
        self.store_snapshot(&snapshot)?;
        Ok(snapshot)
    }
    pub(crate) async fn start(
        &self,
        url: &Url,
        source: Option<SourceScope>,
    ) -> Result<OwnedBrowser> {
        use std::os::unix::fs::PermissionsExt;
        ensure(!self.processes.closed(), "browser executor is closing")?;
        self.policy.validate_url(url)?;
        let profile = tempfile::Builder::new()
            .prefix("session-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir_in(&self.directory)?;
        let profile_path = profile.path().to_path_buf();
        let proxy = BrowserProxy::start(self.policy.clone()).await?;
        let mut command = Command::new(&self.driver);
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
            .env("TMPDIR", profile.path())
            .args(["--port=0", "--allowed-ips=127.0.0.1", "--log-level=SEVERE"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        if let Some(home) = std::env::var_os("HOME") {
            command.env("HOME", home);
        }
        let mut owner = self.processes.spawn(&mut command, source, Some(profile))?;
        let stdout = owner
            .child()
            .stdout
            .take()
            .ok_or_else(|| crate::Error::Rejected("browser startup output missing".into()))?;
        let mut lines = BufReader::new(stdout.take(8192)).lines();
        let endpoint = tokio::time::timeout(Duration::from_secs(8), async {
            while let Some(line) = lines.next_line().await? {
                if let Some(port) =
                    line.strip_prefix("ChromeDriver was started successfully on port ")
                {
                    let port: u16 = port
                        .trim_end_matches('.')
                        .parse()
                        .map_err(|_| crate::Error::Rejected("invalid driver port".into()))?;
                    ensure(port > 0, "invalid driver port")?;
                    return Ok::<_, crate::Error>(format!("http://127.0.0.1:{port}/").parse()?);
                }
            }
            Err(crate::Error::Rejected(
                "browser driver startup failed".into(),
            ))
        })
        .await
        .map_err(|_| crate::Error::Rejected("browser driver startup timed out".into()))??;
        owner.driver_shutdown(&endpoint)?;
        let browser = FixtureBrowser::open_guarded(
            endpoint,
            url.clone(),
            self.policy.clone(),
            proxy.address,
            &profile_path.join("profile"),
        )
        .await?;
        Ok(OwnedBrowser {
            browser,
            owner,
            proxy,
        })
    }
    pub(crate) fn store_snapshot(&self, snapshot: &BrowserSnapshot) -> Result<()> {
        let artifact = self
            .directory
            .join("artifacts")
            .join(format!("{}.png", snapshot.screenshot_digest));
        if artifact.exists() {
            use std::os::unix::fs::OpenOptionsExt;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&artifact)?;
            ensure(
                file.metadata()?.is_file() && file.metadata()?.len() <= 4 * 1024 * 1024,
                "invalid browser artifact",
            )?;
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(
                &mut std::io::Read::take(file, 4 * 1024 * 1024 + 1),
                &mut bytes,
            )?;
            ensure(
                crate::protocol::hash_bytes(&bytes) == snapshot.screenshot_digest,
                "browser artifact conflict",
            )?;
        } else {
            self.artifact_room()?;
            private_write(&artifact, &snapshot.png)?;
        }
        Ok(())
    }
}
pub(crate) struct OwnedBrowser {
    pub browser: FixtureBrowser,
    owner: OwnedProcess,
    pub proxy: BrowserProxy,
}
impl OwnedBrowser {
    pub(crate) async fn stop(mut self) -> Result<()> {
        let _ = tokio::time::timeout(Duration::from_secs(3), self.browser.close()).await;
        self.owner.shutdown().await?;
        Ok(())
    }
}
