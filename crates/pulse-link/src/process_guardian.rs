//! A child owner that survives the executor. FD 2 is a private control socket,
//! never a tool's stderr. Tools cannot inherit that socket.
use crate::{Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt},
        net::UnixStream,
    },
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::process::{Child, Command};

mod lifecycle;
mod resources;
mod runner;
pub(crate) use resources::OwnedDirectory;
pub use runner::run;
#[cfg(test)]
mod tests;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Directory {
    path: PathBuf,
    device: u64,
    inode: u64,
}
impl Directory {
    pub(crate) fn capture(path: &Path) -> Result<Self> {
        let meta = std::fs::symlink_metadata(path)?;
        ensure(
            meta.is_dir()
                && meta.uid() == rustix::process::geteuid().as_raw()
                && meta.mode() & 0o777 == 0o700,
            "private owned resource required",
        )?;
        Ok(Self {
            path: path.canonicalize()?,
            device: meta.dev(),
            inode: meta.ino(),
        })
    }
    fn valid(&self) -> Result<()> {
        let current = Self::capture(&self.path)?;
        ensure(
            current.device == self.device && current.inode == self.inode,
            "owned resource identity changed",
        )
    }
    fn remove(&self) -> Result<()> {
        match std::fs::symlink_metadata(&self.path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
            Ok(_) => {}
        }
        self.valid()?;
        std::fs::remove_dir_all(&self.path)?;
        File::open(self.path.parent().unwrap())?.sync_all()?;
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u8,
    id: String,
    runtime: String,
    resource: Option<Directory>,
    browser: bool,
    phase: Phase,
    directory: Directory,
    driver_endpoint: Option<String>,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Prepared,
    Starting,
    Stopped,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    program: PathBuf,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    cwd: Option<PathBuf>,
    diagnostics_limit: Option<usize>,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Control {
    Stop,
    Driver { endpoint: String },
}

#[derive(Clone)]
pub(crate) struct Registry {
    root: PathBuf,
    executable: PathBuf,
    runtime: String,
}
#[derive(Clone)]
pub(crate) struct Guardian {
    stream: Arc<Mutex<UnixStream>>,
    directory: PathBuf,
    id: String,
}
fn exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
fn private_file(path: &Path, create: bool) -> Result<File> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(create)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let m = file.metadata()?;
    ensure(
        m.is_file()
            && m.nlink() == 1
            && m.uid() == rustix::process::geteuid().as_raw()
            && m.mode() & 0o077 == 0,
        "guardian file must be private and owned",
    )?;
    Ok(file)
}
fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let file = private_file(path, false)?;
    ensure(file.metadata()?.len() <= 65536, "guardian record capacity")?;
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    ensure(bytes.len() <= 65536, "guardian record capacity")?;
    Ok(serde_json::from_slice(&bytes)?)
}
fn write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let partial = path.with_extension("partial");
    if exists(&partial)? {
        drop(private_file(&partial, false)?);
        std::fs::remove_file(&partial)?;
    }
    let mut file = private_file(&partial, true)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    std::fs::rename(partial, path)?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}
fn record(dir: &Path) -> Result<Record> {
    Directory::capture(dir)?;
    let record: Record = read(&dir.join("state.json"))?;
    record.directory.valid()?;
    ensure(
        record.version == 1
            && record.directory.path == dir
            && uuid::Uuid::parse_str(&record.runtime).is_ok()
            && uuid::Uuid::parse_str(&record.id).is_ok_and(|id| id.to_string() == record.id)
            && dir
                .file_name()
                .is_some_and(|name| name == record.id.as_str()),
        "guardian record identity",
    )?;
    Ok(record)
}
fn validate_contents(dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        ensure(
            path.file_name().is_some_and(|name| {
                [
                    "state.json",
                    "state.partial",
                    "request.json",
                    "request.partial",
                    "lease",
                ]
                .iter()
                .any(|allowed| name == *allowed)
            }),
            "unknown guardian file",
        )?;
        drop(private_file(&path, false)?);
    }
    Ok(())
}
fn remove_operation(dir: &Path) -> Result<()> {
    validate_contents(dir)?;
    for name in [
        "request.json",
        "request.partial",
        "state.partial",
        "state.json",
        "lease",
    ] {
        let path = dir.join(name);
        if exists(&path)? {
            drop(private_file(&path, false)?);
            std::fs::remove_file(path)?;
        }
    }
    std::fs::remove_dir(dir)?;
    File::open(dir.parent().unwrap())?.sync_all()?;
    Ok(())
}
impl Registry {
    pub(crate) fn new(directory: &Path, executable: PathBuf, runtime: String) -> Result<Self> {
        ensure(
            uuid::Uuid::parse_str(&runtime).is_ok(),
            "invalid guardian runtime",
        )?;
        let meta = std::fs::symlink_metadata(&executable)?;
        ensure(
            executable.is_absolute()
                && meta.is_file()
                && meta.mode() & 0o022 == 0
                && meta.mode() & 0o111 != 0,
            "fixed guardian executable required",
        )?;
        crate::journal::private_dir(directory)?;
        let root = directory.canonicalize()?.join("processes");
        crate::journal::private_dir(&root)?;
        Directory::capture(&root)?;
        Ok(Self {
            root,
            executable,
            runtime,
        })
    }
    pub(crate) async fn recover(&self) -> Result<()> {
        self.validate_runtime_root()?;
        Directory::capture(&self.root)?;
        let entries = std::fs::read_dir(&self.root)?.collect::<std::io::Result<Vec<_>>>()?;
        ensure(entries.len() <= 256, "guardian recovery capacity")?;
        for entry in entries {
            let dir = entry.path();
            Directory::capture(&dir)?;
            ensure(
                dir.file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| uuid::Uuid::parse_str(s).is_ok_and(|id| id.to_string() == s)),
                "guardian operation directory",
            )?;
            if !exists(&dir.join("state.json"))? {
                validate_contents(&dir)?;
                ensure(
                    std::fs::read_dir(&dir)?.all(|entry| {
                        entry.is_ok_and(|entry| {
                            entry.file_name() == "lease" || entry.file_name() == "state.partial"
                        })
                    }),
                    "guardian state missing after launch preparation",
                )?;
                remove_operation(&dir)?;
                continue;
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let lease = private_file(&dir.join("lease"), false)?;
                match lease.try_lock_exclusive() {
                    Ok(()) => {
                        let mut state = record(&dir)?;
                        validate_contents(&dir)?;
                        if state.phase == Phase::Prepared {
                            // A late guardian must acquire this same lease and
                            // observe Stopped before it can launch a tool.
                            state.phase = Phase::Stopped;
                            write(&dir.join("state.json"), &state)?;
                        }
                        ensure(
                            state.phase == Phase::Stopped,
                            "guardian stopped proof unavailable",
                        )?;
                        if let Some(resource) = state.resource {
                            resource.remove()?;
                        }
                        remove_operation(&dir)?;
                        break;
                    }
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        self.recover_resources()?;
        Ok(())
    }
    pub(crate) fn spawn(
        &self,
        original: &Command,
        resource: Option<Directory>,
        browser: bool,
        diagnostics_limit: Option<usize>,
    ) -> Result<(Child, Guardian)> {
        ensure(
            std::fs::read_dir(&self.root)?.take(256).count() < 256,
            "guardian process capacity",
        )?;
        let command = original.as_std();
        let text = |value: &std::ffi::OsStr| {
            value
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| crate::Error::Rejected("guardian arguments must be UTF-8".into()))
        };
        let request = Request {
            program: PathBuf::from(command.get_program()),
            args: command.get_args().map(text).collect::<Result<_>>()?,
            env: command
                .get_envs()
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .map(|(key, value)| Ok((text(key)?, text(value)?)))
                .collect::<Result<_>>()?,
            cwd: command.get_current_dir().map(Path::to_path_buf),
            diagnostics_limit,
        };
        ensure(
            request.program.is_absolute()
                && request.args.len() <= 128
                && serde_json::to_vec(&request)?.len() <= 65536,
            "guardian launch capacity or executable",
        )?;
        let id = uuid::Uuid::new_v4().to_string();
        let directory = self.root.join(&id);
        crate::journal::private_dir(&directory)?;
        File::open(&self.root)?.sync_all()?;
        drop(private_file(&directory.join("lease"), true)?);
        write(
            &directory.join("state.json"),
            &Record {
                version: 1,
                id: id.clone(),
                runtime: self.runtime.clone(),
                resource,
                browser,
                phase: Phase::Prepared,
                directory: Directory::capture(&directory)?,
                driver_endpoint: None,
            },
        )?;
        write(&directory.join("request.json"), &request)?;
        let (parent, child) = UnixStream::pair()?;
        parent.set_write_timeout(Some(Duration::from_secs(1)))?;
        parent.set_read_timeout(Some(Duration::from_secs(3)))?;
        child.set_nonblocking(true)?;
        let mut guardian = Command::new(&self.executable);
        guardian
            .arg(&directory)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(std::os::fd::OwnedFd::from(child)))
            .process_group(0)
            .kill_on_drop(false);
        // Test instrumentation belongs to this Rust helper, never its tool.
        #[cfg(test)]
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            guardian.env("LLVM_PROFILE_FILE", profile);
        }
        let child = guardian.spawn()?;
        Ok((
            child,
            Guardian {
                stream: Arc::new(Mutex::new(parent)),
                directory,
                id,
            },
        ))
    }
}
impl Guardian {
    fn send(&self, control: &Control, ack: bool) -> Result<()> {
        let mut stream = self
            .stream
            .lock()
            .map_err(|_| crate::Error::Rejected("guardian control unavailable".into()))?;
        let mut bytes = serde_json::to_vec(control)?;
        bytes.push(b'\n');
        stream.write_all(&bytes)?;
        if ack {
            let mut response = [0];
            stream.read_exact(&mut response)?;
            ensure(response == [1], "guardian did not accept driver ownership")?;
        }
        Ok(())
    }
    pub(crate) fn stop(&self) -> Result<()> {
        self.send(&Control::Stop, false)
            .or_else(|error| self.stopped().map_err(|_| error))
    }
    pub(crate) fn driver(&self, endpoint: &url::Url) -> Result<()> {
        self.send(
            &Control::Driver {
                endpoint: endpoint.to_string(),
            },
            true,
        )
    }
    pub(crate) fn stopped(&self) -> Result<()> {
        let state = record(&self.directory)?;
        ensure(
            state.id == self.id && state.phase == Phase::Stopped,
            "guardian stop or descendants unverified",
        )
    }
    pub(crate) fn finish(&self) -> Result<()> {
        self.stopped()?;
        let lease = private_file(&self.directory.join("lease"), false)?;
        lease.try_lock_exclusive()?;
        remove_operation(&self.directory)
    }
}
