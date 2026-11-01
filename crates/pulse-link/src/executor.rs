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
    root: PathBuf,
    browser: Option<crate::browser_runtime::BrowserRuntime>,
}
impl Executor {
    pub fn new(root: &Path) -> Result<Self> {
        ensure(
            !std::fs::symlink_metadata(root)?.file_type().is_symlink(),
            "repo root must not be a symlink",
        )?;
        let root = root.canonicalize()?;
        ensure(root.is_dir(), "repo root must be a directory")?;
        Ok(Self {
            root,
            browser: None,
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
    pub fn root(&self) -> &Path {
        &self.root
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
    async fn git(&self, diff: Option<&str>) -> Result<ReadOutcome> {
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
        let mut child = command.spawn()?;
        let mut out = child
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
                child.kill().await?;
            }
            child.wait().await.map_err(crate::Error::from)
        })
        .await;
        let status = match execution {
            Ok(result) => result?,
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
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
        if let ReadOp::Browser { url } = operation {
            return self.browser_read(&url).await.unwrap_or_else(|_| {
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
            ReadOp::Browser { .. } => unreachable!(),
            ReadOp::Text { path } => tokio::task::spawn_blocking(move || executor.read_text(&path))
                .await
                .map_err(|_| crate::Error::Rejected("read worker failed".into()))
                .and_then(|r| r),
            ReadOp::List { path } => tokio::task::spawn_blocking(move || executor.list(&path))
                .await
                .map_err(|_| crate::Error::Rejected("read worker failed".into()))
                .and_then(|r| r),
            ReadOp::GitStatus => self.git(None).await,
            ReadOp::GitDiff { path } => self.git(Some(&path)).await,
        };
        let outcome = result.unwrap_or_else(|_| ReadOutcome::Rejected {
            reason: "local read rejected or failed; no write was executed".into(),
        });
        crate::artifact::ExecutedRead {
            outcome,
            image: None,
        }
    }
    async fn browser_read(&self, url: &str) -> Result<crate::artifact::ExecutedRead> {
        let browser = self
            .browser
            .as_ref()
            .ok_or_else(|| crate::Error::Rejected("browser unavailable".into()))?;
        let url = pulse_core::browser_url(&format!("browser:{url}"))
            .ok_or_else(|| crate::Error::Rejected("browser URL rejected".into()))?;
        let snapshot = browser.read(&url).await?;
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
