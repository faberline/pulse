//! Executor-owned process groups. A signal is not a stopped receipt.
use crate::{Result, ensure};
use rustix::process::{Pid, Signal, kill_process_group, test_kill_process_group};
use std::{
    collections::{BTreeMap, BTreeSet},
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    process::{Child, Command},
    sync::watch,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceScope {
    task: String,
    revision: String,
}
impl SourceScope {
    pub(crate) fn new(task: &str, revision: &str) -> Result<Self> {
        ensure(
            !task.is_empty()
                && task.len() <= 128
                && revision
                    .parse::<u64>()
                    .is_ok_and(|n| n > 0 && n <= i64::MAX as u64 && n.to_string() == revision),
            "invalid process source",
        )?;
        Ok(Self {
            task: task.into(),
            revision: revision.into(),
        })
    }
}
#[derive(Default)]
struct State {
    next: u64,
    entries: BTreeMap<u64, Arc<Entry>>,
    blocked: BTreeSet<SourceScope>,
    closed: bool,
    guardian: Option<crate::process_guardian::Registry>,
}
struct Entry {
    source: Option<SourceScope>,
    // Reaping and sending a signal serialize on this lock. Never signal a PID
    // after try_wait has reaped it and made that PID eligible for reuse.
    live: Mutex<Option<Pid>>,
    group: Pid,
    done: watch::Sender<Option<bool>>,
    guardian: Option<crate::process_guardian::Guardian>,
}
#[derive(Clone, Default)]
pub(crate) struct ProcessTracker(Arc<Mutex<State>>);

impl ProcessTracker {
    pub(crate) fn supervise(&self, registry: crate::process_guardian::Registry) -> Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("process registry unavailable".into()))?;
        ensure(
            !state.closed && state.entries.is_empty(),
            "process supervision must precede launch",
        )?;
        state.guardian = Some(registry);
        Ok(())
    }
    pub(crate) fn closed(&self) -> bool {
        self.0.lock().map_or(true, |state| state.closed)
    }
    pub(crate) fn stopped(&self) -> bool {
        self.0
            .lock()
            .is_ok_and(|state| state.closed && state.entries.is_empty())
    }
    pub(crate) fn close_admission(&self) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| crate::Error::Rejected("process registry unavailable".into()))?
            .closed = true;
        Ok(())
    }
    pub(crate) async fn shutdown(&self) -> Result<()> {
        let entries = {
            let mut state = self
                .0
                .lock()
                .map_err(|_| crate::Error::Rejected("process registry unavailable".into()))?;
            state.closed = true;
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        let mut failure = None;
        for entry in &entries {
            if let Err(error) = terminate(entry) {
                failure.get_or_insert(error);
            }
        }
        let waited = wait_entries(entries).await;
        failure.map_or(waited, Err)
    }
    /// Callers must separately prevent new source work while using this result.
    pub(crate) fn source_idle(&self, source: &SourceScope) -> Result<bool> {
        let state = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("process registry unavailable".into()))?;
        Ok(!state
            .entries
            .values()
            .any(|entry| entry.source.as_ref() == Some(source)))
    }
    pub(crate) fn spawn(
        &self,
        command: &mut Command,
        source: Option<SourceScope>,
        resource: Option<tempfile::TempDir>,
    ) -> Result<OwnedProcess> {
        self.spawn_kind(
            command,
            source,
            resource.map(crate::process_guardian::OwnedDirectory::Temporary),
            false,
            None,
        )
    }
    pub(crate) fn spawn_browser(
        &self,
        command: &mut Command,
        source: Option<SourceScope>,
        resource: crate::process_guardian::OwnedDirectory,
    ) -> Result<OwnedProcess> {
        self.spawn_kind(command, source, Some(resource), true, None)
    }
    pub(crate) fn spawn_native(
        &self,
        command: &mut Command,
        source: Option<SourceScope>,
    ) -> Result<OwnedProcess> {
        self.spawn_kind(command, source, None, false, Some(4096))
    }
    fn spawn_kind(
        &self,
        command: &mut Command,
        source: Option<SourceScope>,
        resource: Option<crate::process_guardian::OwnedDirectory>,
        browser: bool,
        diagnostics_limit: Option<usize>,
    ) -> Result<OwnedProcess> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("process registry unavailable".into()))?;
        ensure(!state.closed, "executor process admission closed")?;
        ensure(state.entries.len() < 64, "process registry capacity")?;
        ensure(
            source.as_ref().is_none_or(|s| !state.blocked.contains(s)),
            "process source withdrawn",
        )?;
        let next = state
            .next
            .checked_add(1)
            .ok_or_else(|| crate::Error::Rejected("process identity exhausted".into()))?;
        let mut resource = PrivateResource::new(resource)?;
        let launch = match &state.guardian {
            Some(registry) => registry
                .spawn(
                    command,
                    resource
                        .0
                        .as_ref()
                        .map(|(dir, _, _)| crate::process_guardian::Directory::capture(dir.path()))
                        .transpose()?,
                    browser,
                    diagnostics_limit,
                )
                .map(|(child, guardian)| (child, Some(guardian))),
            None => command
                .process_group(0)
                .kill_on_drop(true)
                .spawn()
                .map(|child| (child, None))
                .map_err(crate::Error::from),
        };
        let (mut child, guardian) = match launch {
            Ok(child) => child,
            Err(error) => {
                resource.close();
                return Err(error);
            }
        };
        let Some(group) = child
            .id()
            .and_then(|v| i32::try_from(v).ok())
            .and_then(Pid::from_raw)
        else {
            let _ = child.start_kill();
            return Err(crate::Error::Rejected("process group unavailable".into()));
        };
        let entry = Arc::new(Entry {
            source,
            live: Mutex::new(Some(group)),
            group,
            done: watch::channel(None).0,
            guardian,
        });
        state.next = next;
        state.entries.insert(next, entry.clone());
        Ok(OwnedProcess {
            child: Some(child),
            entry,
            tracker: self.clone(),
            id: next,
            resource,
            finished: false,
            shutdown: None,
            shutdown_confirmed: false,
        })
    }
    fn block_entries(&self, source: &SourceScope) -> Result<Vec<Arc<Entry>>> {
        let entries = {
            let mut state = self
                .0
                .lock()
                .map_err(|_| crate::Error::Rejected("process registry unavailable".into()))?;
            ensure(
                state.blocked.contains(source) || state.blocked.len() < 1000,
                "process source capacity",
            )?;
            state.blocked.insert(source.clone());
            state
                .entries
                .values()
                .filter(|e| e.source.as_ref() == Some(source))
                .cloned()
                .collect::<Vec<_>>()
        };
        Ok(entries)
    }
    pub(crate) fn block(&self, source: &SourceScope) -> Result<()> {
        self.block_entries(source).map(|_| ())
    }
    pub(crate) fn fence(&self, source: &SourceScope) -> Result<()> {
        let entries = self.block_entries(source)?;
        // Request all terminations before waiting for any one child.
        let mut failure = None;
        for entry in entries {
            if let Err(error) = terminate(&entry) {
                failure.get_or_insert(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    pub(crate) async fn wait_source(&self, source: &SourceScope) -> Result<()> {
        let entries = {
            let state = self
                .0
                .lock()
                .map_err(|_| crate::Error::Rejected("process registry unavailable".into()))?;
            ensure(
                state.blocked.contains(source),
                "process source is not fenced",
            )?;
            state
                .entries
                .values()
                .filter(|e| e.source.as_ref() == Some(source))
                .cloned()
                .collect::<Vec<_>>()
        };
        wait_entries(entries).await
    }
    fn finish(&self, id: u64, entry: &Entry, success: bool) {
        entry.done.send_replace(Some(success));
        if success && let Ok(mut state) = self.0.lock() {
            state.entries.remove(&id);
        }
    }
}
async fn wait_entries(entries: Vec<Arc<Entry>>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        for entry in entries {
            let mut result = entry.done.subscribe();
            let value = *result
                .wait_for(|v| v.is_some())
                .await
                .map_err(|_| crate::Error::Rejected("process completion lost".into()))?;
            ensure(
                value == Some(true),
                "process stop or private resource cleanup unverified",
            )?;
        }
        Ok(())
    })
    .await
    .map_err(|_| crate::Error::Rejected("process stop deadline exceeded".into()))?
}
fn terminate(entry: &Entry) -> Result<()> {
    if let Some(guardian) = &entry.guardian {
        return guardian
            .stop()
            .or_else(|error| guardian.stopped().map_err(|_| error));
    }
    let mut live = entry
        .live
        .lock()
        .map_err(|_| crate::Error::Rejected("process identity unavailable".into()))?;
    if let Some(pid) = *live {
        match kill_process_group(pid, Signal::KILL) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => *live = None,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
fn keep(resource: Option<crate::process_guardian::OwnedDirectory>) {
    if let Some(resource) = resource {
        resource.keep();
    }
}
#[derive(Default)]
struct PrivateResource(Option<(crate::process_guardian::OwnedDirectory, u64, u64)>);
impl PrivateResource {
    fn new(directory: Option<crate::process_guardian::OwnedDirectory>) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let Some(directory) = directory else {
            return Ok(Self::default());
        };
        let meta = std::fs::symlink_metadata(directory.path())?;
        ensure(
            meta.is_dir()
                && meta.mode() & 0o777 == 0o700
                && meta.uid() == rustix::process::geteuid().as_raw(),
            "private process resource required",
        )?;
        Ok(Self(Some((directory, meta.dev(), meta.ino()))))
    }
    fn close(&mut self) -> bool {
        use std::os::unix::fs::MetadataExt;
        if let Some((directory, dev, ino)) = &self.0
            && !std::fs::symlink_metadata(directory.path()).is_ok_and(|m| {
                m.is_dir()
                    && m.dev() == *dev
                    && m.ino() == *ino
                    && m.mode() & 0o777 == 0o700
                    && m.uid() == rustix::process::geteuid().as_raw()
            })
        {
            return false;
        }
        self.0
            .take()
            .is_none_or(|(directory, _, _)| directory.close().is_ok())
    }
}
impl Drop for PrivateResource {
    fn drop(&mut self) {
        keep(self.0.take().map(|(directory, _, _)| directory));
    }
}
async fn group_empty(group: Pid) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match test_kill_process_group(group) {
                Err(rustix::io::Errno::SRCH) => return true,
                Ok(()) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(_) => return false,
            }
        }
    })
    .await
    .unwrap_or(false)
}

pub(crate) struct OwnedProcess {
    child: Option<Child>,
    entry: Arc<Entry>,
    tracker: ProcessTracker,
    id: u64,
    resource: PrivateResource,
    finished: bool,
    shutdown: Option<url::Url>,
    shutdown_confirmed: bool,
}
// This guard also runs when the runtime aborts the reaper before its first poll.
// Its resource keeps its path on drop; cancellation cannot imply erasure.
struct ReapOwner {
    child: Child,
    entry: Arc<Entry>,
    tracker: ProcessTracker,
    id: u64,
    resource: PrivateResource,
    finished: bool,
}
impl Drop for ReapOwner {
    fn drop(&mut self) {
        if !self.finished {
            let _ = terminate(&self.entry);
            if self.entry.guardian.is_none() {
                let _ = self.child.start_kill();
            }
            self.tracker.finish(self.id, &self.entry, false);
        }
    }
}
impl OwnedProcess {
    pub(crate) fn supervised(&self) -> bool {
        self.entry.guardian.is_some()
    }
    pub(crate) fn driver_shutdown(&mut self, endpoint: &url::Url) -> Result<()> {
        ensure(
            endpoint.scheme() == "http"
                && endpoint.host_str() == Some("127.0.0.1")
                && endpoint.port().is_some_and(|p| p > 0)
                && endpoint.path() == "/"
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.username().is_empty()
                && endpoint.password().is_none(),
            "owned driver endpoint required",
        )?;
        if let Some(guardian) = &self.entry.guardian {
            guardian.driver(endpoint)?;
        }
        self.shutdown = Some(endpoint.join("shutdown")?);
        Ok(())
    }
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        if let Some(guardian) = &self.entry.guardian {
            guardian.stop()?;
            tokio::time::timeout(Duration::from_secs(20), self.wait())
                .await
                .map_err(|_| {
                    crate::Error::Rejected("guardian shutdown exit unverified".into())
                })??;
            return Ok(());
        }
        let url = self
            .shutdown
            .as_ref()
            .ok_or_else(|| crate::Error::Rejected("driver shutdown unavailable".into()))?;
        self.shutdown_confirmed = request_shutdown(url).await;
        ensure(
            self.shutdown_confirmed,
            "driver did not confirm browser shutdown",
        )?;
        tokio::time::timeout(Duration::from_secs(5), self.wait())
            .await
            .map_err(|_| crate::Error::Rejected("driver shutdown exit unverified".into()))??;
        Ok(())
    }
    pub(crate) fn child(&mut self) -> &mut Child {
        self.child.as_mut().expect("owned child")
    }
    pub(crate) fn terminate(&self) -> Result<()> {
        terminate(&self.entry)
    }
    pub(crate) async fn wait(&mut self) -> Result<ExitStatus> {
        loop {
            let status = {
                let entry = self.entry.clone();
                let mut live = entry
                    .live
                    .lock()
                    .map_err(|_| crate::Error::Rejected("process identity unavailable".into()))?;
                let status = self.child().try_wait()?;
                if status.is_some() {
                    *live = None;
                }
                status
            };
            if let Some(status) = status {
                let success = (self.entry.guardian.is_some()
                    || self.shutdown.is_none()
                    || (self.shutdown_confirmed && status.success()))
                    && group_empty(self.entry.group).await
                    && self
                        .entry
                        .guardian
                        .as_ref()
                        .is_none_or(|guardian| guardian.stopped().is_ok())
                    && self.resource.close()
                    && self
                        .entry
                        .guardian
                        .as_ref()
                        .is_none_or(|guardian| guardian.finish().is_ok());
                self.tracker.finish(self.id, &self.entry, success);
                self.finished = true;
                ensure(
                    success,
                    "process group or private resource cleanup unverified",
                )?;
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let supervised = self.entry.guardian.is_some();
        let shutdown = if supervised {
            self.shutdown.take();
            None
        } else {
            self.shutdown.take()
        };
        let confirmed = self.shutdown_confirmed;
        let killed = if shutdown.is_none() {
            self.terminate().is_ok()
        } else {
            true
        };
        let Some(mut child) = self.child.take() else {
            return;
        };
        if !killed && !supervised {
            let _ = child.start_kill();
        }
        let resource = std::mem::take(&mut self.resource);
        let entry = self.entry.clone();
        let tracker = self.tracker.clone();
        let id = self.id;
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            let _ = terminate(&entry);
            if !supervised {
                let _ = child.start_kill();
            }
            tracker.finish(id, &entry, false);
            return;
        };
        let mut owner = ReapOwner {
            child,
            entry,
            tracker,
            id,
            resource,
            finished: false,
        };
        runtime.spawn(async move {
            let graceful = confirmed
                || match &shutdown {
                    Some(url) => request_shutdown(url).await,
                    None => true,
                };
            let status = if graceful {
                tokio::time::timeout(
                    Duration::from_secs(if supervised { 20 } else { 5 }),
                    wait_child(&mut owner.child, &owner.entry),
                )
                .await
                .ok()
                .and_then(Result::ok)
            } else {
                None
            };
            let clean = status.is_some_and(|s| shutdown.is_none() || s.success());
            let waited = if status.is_some() {
                true
            } else {
                let _ = terminate(&owner.entry);
                if !supervised {
                    let _ = owner.child.start_kill();
                }
                matches!(
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        wait_child(&mut owner.child, &owner.entry)
                    )
                    .await,
                    Ok(Ok(_))
                )
            };
            let success = killed
                && waited
                && graceful
                && (shutdown.is_none() || clean)
                && group_empty(owner.entry.group).await
                && owner
                    .entry
                    .guardian
                    .as_ref()
                    .is_none_or(|guardian| guardian.stopped().is_ok())
                && owner.resource.close()
                && owner
                    .entry
                    .guardian
                    .as_ref()
                    .is_none_or(|guardian| guardian.finish().is_ok());
            owner.tracker.finish(owner.id, &owner.entry, success);
            owner.finished = true;
            drop(owner);
        });
    }
}

async fn wait_child(child: &mut Child, entry: &Entry) -> Result<ExitStatus> {
    loop {
        let status = {
            let mut live = entry
                .live
                .lock()
                .map_err(|_| crate::Error::Rejected("process identity unavailable".into()))?;
            let value = child.try_wait()?;
            if value.is_some() {
                *live = None;
            }
            value
        };
        if let Some(status) = status {
            return Ok(status);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
pub(crate) async fn request_shutdown(url: &url::Url) -> bool {
    let result = async {
        let unavailable =
            |_: reqwest::Error| crate::Error::Rejected("owned driver shutdown unavailable".into());
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(3))
            .build()
            .map_err(unavailable)?;
        let mut response = client.get(url.clone()).send().await.map_err(unavailable)?;
        ensure(response.status().is_success(), "driver shutdown rejected")?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(unavailable)? {
            ensure(
                bytes.len() + chunk.len() <= 4096,
                "driver shutdown response limit",
            )?;
            bytes.extend_from_slice(&chunk);
        }
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        ensure(
            value.get("value").is_some_and(|v| v.get("error").is_none())
                && value.get("status").is_none_or(|v| v == 0),
            "driver shutdown protocol error",
        )?;
        Ok::<_, crate::Error>(())
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(3), result).await,
        Ok(Ok(()))
    )
}

#[cfg(test)]
#[path = "owned_process_tests.rs"]
mod tests;
