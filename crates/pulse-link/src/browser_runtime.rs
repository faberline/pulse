//! One private Chrome profile per read. No shell, personal profile or model-selected executable.
use crate::{
    Result,
    browser::{BrowserSnapshot, FixtureBrowser},
    browser_network::{BrowserProxy, NetworkPolicy},
    ensure,
    journal::{private_dir, private_write},
};
use rustix::process::{Pid, Signal, kill_process_group};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, Command},
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
        })
    }
    pub async fn read(&self, url: &Url) -> Result<BrowserSnapshot> {
        self.policy.validate_url(url)?;
        self.artifact_room()?;
        tokio::time::timeout(Duration::from_secs(35), self.read_inner(url))
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
    async fn read_inner(&self, url: &Url) -> Result<BrowserSnapshot> {
        let mut session = self.start(url).await?;
        let result = async {
            session.browser.navigate(url).await?;
            session.browser.observe().await
        }
        .await;
        let closed = session.browser.close().await;
        session.owner.stop().await?;
        let snapshot = result?;
        closed?;
        self.store_snapshot(&snapshot)?;
        Ok(snapshot)
    }
    pub(crate) async fn start(&self, url: &Url) -> Result<OwnedBrowser> {
        self.policy.validate_url(url)?;
        let profile = tempfile::Builder::new()
            .prefix("session-")
            .tempdir_in(&self.directory)?;
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
        let mut child = command.spawn()?;
        let group = child
            .id()
            .and_then(|p| i32::try_from(p).ok())
            .and_then(Pid::from_raw)
            .ok_or_else(|| crate::Error::Rejected("browser process group missing".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| crate::Error::Rejected("browser startup output missing".into()))?;
        let owner = Driver {
            child,
            group: Some(group),
            _profile: profile,
        };
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
        let browser = FixtureBrowser::open_guarded(
            endpoint,
            url.clone(),
            self.policy.clone(),
            proxy.address,
            &owner._profile.path().join("profile"),
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
    owner: Driver,
    pub proxy: BrowserProxy,
}
struct Driver {
    child: Child,
    group: Option<Pid>,
    _profile: tempfile::TempDir,
}
impl Driver {
    fn kill(&mut self) -> Result<()> {
        if let Some(group) = self.group {
            match kill_process_group(group, Signal::KILL) {
                Ok(()) | Err(rustix::io::Errno::SRCH) => self.group = None,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        self.kill()?;
        tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .map_err(|_| crate::Error::Rejected("browser process did not exit".into()))??;
        Ok(())
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}
