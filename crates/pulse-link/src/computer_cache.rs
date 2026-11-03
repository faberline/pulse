//! Current-runtime image ownership. Unknown old resources are never erased.
use crate::{Result, ensure, journal::private_dir, owned_process::SourceScope, protocol::hash};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

#[derive(Clone, Default)]
pub(crate) struct ImageCaches(
    Arc<Mutex<BTreeMap<SourceScope, Arc<Cache>>>>,
    Arc<AtomicBool>,
);
struct Cache {
    path: Option<PathBuf>,
    identity: Option<(u64, u64)>,
    _directory: Mutex<Option<File>>,
    owned: Mutex<Option<tempfile::TempDir>>,
    io: Arc<RwLock<()>>,
    blocked: AtomicBool,
    last_used: Mutex<Instant>,
}
impl Drop for Cache {
    fn drop(&mut self) {
        // A runtime drop or failed stop is not erasure evidence.
        if let Some(directory) = self
            .owned
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = directory.keep();
        }
    }
}
pub(crate) struct ImageLease {
    pub(crate) path: PathBuf,
    cache: Arc<Cache>,
    _read: OwnedRwLockReadGuard<()>,
}
impl Drop for ImageLease {
    fn drop(&mut self) {
        // The actual blocking reader owns this lease through its last read.
        *self
            .cache
            .last_used
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Instant::now();
    }
}
impl ImageLease {
    pub(crate) fn valid(&self) -> Result<()> {
        ensure(
            !self.cache.blocked.load(Ordering::Acquire),
            "image source withdrawn",
        )?;
        self.cache.validate()
    }
}
fn prefix(task: &str, revision: &str) -> Result<String> {
    Ok(format!(".source-{}-", hash(&(task, revision))?))
}
impl Cache {
    fn blocked() -> Self {
        Self {
            path: None,
            identity: None,
            _directory: Mutex::new(None),
            owned: Mutex::new(None),
            io: Arc::new(RwLock::new(())),
            blocked: AtomicBool::new(true),
            last_used: Mutex::new(Instant::now()),
        }
    }
    fn create(root: &Path, task: &str, revision: &str) -> Result<Self> {
        private_dir(root)?;
        ensure(
            std::fs::read_dir(root)?.take(128).count() < 128,
            "computer cache directory capacity",
        )?;
        let directory = tempfile::Builder::new()
            .prefix(&prefix(task, revision)?)
            .tempdir_in(root)?;
        let path = directory.path().to_path_buf();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
        let fd = rustix::fs::open(
            &path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        let file = File::from(fd);
        let meta = file.metadata()?;
        ensure(
            meta.is_dir()
                && meta.mode() & 0o777 == 0o700
                && meta.uid() == rustix::process::geteuid().as_raw(),
            "private image directory required",
        )?;
        Ok(Self {
            path: Some(path),
            identity: Some((meta.dev(), meta.ino())),
            _directory: Mutex::new(Some(file)),
            owned: Mutex::new(Some(directory)),
            io: Arc::new(RwLock::new(())),
            blocked: AtomicBool::new(false),
            last_used: Mutex::new(Instant::now()),
        })
    }
    fn validate(&self) -> Result<()> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| crate::Error::Rejected("image source unavailable".into()))?;
        let meta = std::fs::symlink_metadata(path)?;
        ensure(
            meta.is_dir()
                && Some((meta.dev(), meta.ino())) == self.identity
                && meta.mode() & 0o777 == 0o700
                && meta.uid() == rustix::process::geteuid().as_raw(),
            "image directory identity changed",
        )
    }
    fn clear(&self) -> Result<()> {
        let mut owned = self
            .owned
            .lock()
            .map_err(|_| crate::Error::Rejected("image directory unavailable".into()))?;
        let Some(directory) = owned.as_ref() else {
            return Ok(());
        };
        self.validate()?;
        for (count, entry) in std::fs::read_dir(directory.path())?.take(129).enumerate() {
            ensure(count < 128, "image cache entry capacity")?;
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| crate::Error::Rejected("unrecognized image cache entry".into()))?;
            let png = name.strip_suffix(".png").is_some_and(|s| {
                s.len() == 64
                    && s.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            });
            let partial = name
                .strip_prefix(".partial-")
                .is_some_and(|s| uuid::Uuid::parse_str(s).is_ok_and(|id| id.to_string() == s));
            let meta = std::fs::symlink_metadata(entry.path())?;
            ensure(
                (png || partial)
                    && meta.is_file()
                    && meta.nlink() == 1
                    && meta.mode() & 0o777 == 0o600
                    && meta.uid() == rustix::process::geteuid().as_raw()
                    && meta.len() <= 4 * 1024 * 1024,
                "unverified image cache entry",
            )?;
        }
        // Retain ownership on error, so a later attempt cannot claim a failed
        // removal succeeded just because its TempDir handle was consumed.
        std::fs::remove_dir_all(directory.path())?;
        if let Some(directory) = owned.take() {
            let _ = directory.keep();
        }
        self._directory
            .lock()
            .map_err(|_| crate::Error::Rejected("image directory unavailable".into()))?
            .take();
        Ok(())
    }
}
impl ImageCaches {
    pub(crate) fn closed(&self) -> bool {
        self.1.load(Ordering::Acquire)
    }
    pub(crate) fn begin_shutdown(&self) -> Result<()> {
        self.1.store(true, Ordering::Release);
        let entries = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?;
        for cache in entries.values() {
            cache.blocked.store(true, Ordering::Release);
        }
        Ok(())
    }
    pub(crate) async fn finish_shutdown(
        &self,
        processes: &crate::owned_process::ProcessTracker,
    ) -> Result<()> {
        ensure(
            self.closed() && processes.stopped(),
            "native process stop unverified",
        )?;
        let caches = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut failure = None;
        for cache in caches {
            let result = async {
                let _exclusive =
                    tokio::time::timeout(Duration::from_secs(5), cache.io.clone().write_owned())
                        .await
                        .map_err(|_| crate::Error::Rejected("image read stop unverified".into()))?;
                cache.clear()
            }
            .await;
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    /// Expiry collection is not source erasure. Fenced revisions keep their tombstones.
    pub(crate) fn collect_idle(
        &self,
        processes: &crate::owned_process::ProcessTracker,
        protected: &BTreeSet<SourceScope>,
        now: Instant,
    ) -> Result<usize> {
        let mut entries = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?;
        let mut removed = Vec::new();
        let mut failure = None;
        for (source, cache) in entries.iter() {
            // The registry lock prevents a new acquire. A cloned cache also
            // covers callers waiting for their read guard, not only live leases.
            if cache.blocked.load(Ordering::Acquire)
                || protected.contains(source)
                || Arc::strong_count(cache) != 1
            {
                continue;
            }
            let last = *cache
                .last_used
                .lock()
                .map_err(|_| crate::Error::Rejected("image activity unavailable".into()))?;
            if now.saturating_duration_since(last) < Duration::from_secs(120) {
                continue;
            }
            let Ok(_exclusive) = cache.io.try_write() else {
                continue;
            };
            // A dropped future may leave a reaper running. Only an empty source
            // registry proves its helper/group has finished successfully.
            match processes.source_idle(source).and_then(|idle| {
                if idle {
                    cache.clear().map(|()| true)
                } else {
                    Ok(false)
                }
            }) {
                Ok(true) => removed.push(source.clone()),
                Ok(false) => {}
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        let count = removed.len();
        for source in removed {
            entries.remove(&source);
        }
        failure.map_or(Ok(count), Err)
    }
    pub(crate) async fn acquire(
        &self,
        directory: &Path,
        task: &str,
        revision: &str,
    ) -> Result<ImageLease> {
        let scope = SourceScope::new(task, revision)?;
        let cache = {
            let mut entries = self
                .0
                .lock()
                .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?;
            ensure(!self.closed(), "native image admission closed")?;
            if !entries.contains_key(&scope) {
                ensure(entries.len() < 1000, "image source capacity")?;
                private_dir(directory)?;
                let root = directory.canonicalize()?.join("artifacts");
                entries.insert(
                    scope.clone(),
                    Arc::new(Cache::create(&root, task, revision)?),
                );
            }
            entries[&scope].clone()
        };
        Self::lease(cache).await
    }
    pub(crate) async fn existing(&self, task: &str, revision: &str) -> Result<ImageLease> {
        ensure(!self.closed(), "native image admission closed")?;
        let scope = SourceScope::new(task, revision)?;
        let cache = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?
            .get(&scope)
            .cloned()
            .ok_or_else(|| crate::Error::Rejected("original image cache unavailable".into()))?;
        Self::lease(cache).await
    }
    async fn lease(cache: Arc<Cache>) -> Result<ImageLease> {
        let read = cache.io.clone().read_owned().await;
        ensure(
            !cache.blocked.load(Ordering::Acquire),
            "image source withdrawn",
        )?;
        cache.validate()?;
        Ok(ImageLease {
            path: cache.path.clone().expect("owned image directory"),
            cache,
            _read: read,
        })
    }
    pub(crate) fn fence(&self, task: &str, revision: &str) -> Result<()> {
        let scope = SourceScope::new(task, revision)?;
        let mut entries = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?;
        ensure(
            entries.contains_key(&scope) || entries.len() < 1000,
            "image source capacity",
        )?;
        entries
            .entry(scope)
            .or_insert_with(|| Arc::new(Cache::blocked()))
            .blocked
            .store(true, Ordering::Release);
        Ok(())
    }
    pub(crate) async fn clear(&self, directory: &Path, task: &str, revision: &str) -> Result<()> {
        let scope = SourceScope::new(task, revision)?;
        let cache = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?
            .get(&scope)
            .cloned()
            .ok_or_else(|| crate::Error::Rejected("image source was not fenced".into()))?;
        ensure(
            cache.blocked.load(Ordering::Acquire),
            "image source was not fenced",
        )?;
        let _write = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cache.io.clone().write_owned(),
        )
        .await
        .map_err(|_| crate::Error::Rejected("image read stop unverified".into()))?;
        let _serial = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?;
        let root = if directory.try_exists()? {
            private_dir(directory)?;
            directory.canonicalize()?.join("artifacts")
        } else {
            directory.join("artifacts")
        };
        if root.try_exists()? {
            private_dir(&root)?;
            let expected = prefix(task, revision)?;
            for (count, entry) in std::fs::read_dir(&root)?.take(129).enumerate() {
                ensure(count < 128, "image cache directory capacity")?;
                let entry = entry?;
                let name = entry.file_name();
                ensure(
                    !name.to_string_lossy().ends_with(".png"),
                    "legacy image ownership requires recovery",
                )?;
                if name.to_string_lossy().starts_with(&expected) {
                    ensure(
                        cache.path.as_ref() == Some(&entry.path())
                            && cache.owned.lock().is_ok_and(|owned| owned.is_some()),
                        "old image ownership requires recovery",
                    )?;
                }
            }
        }
        cache.clear()
    }
    pub(crate) fn capacity(&self, directory: &Path, slots: usize) -> Result<()> {
        ensure((1..=2).contains(&slots), "invalid image reservation")?;
        let _serial = self
            .0
            .lock()
            .map_err(|_| crate::Error::Rejected("image registry unavailable".into()))?;
        let root = directory.join("artifacts");
        if !root.try_exists()? {
            return Ok(());
        }
        private_dir(&root)?;
        let mut files = 0;
        let mut directories = 0;
        for (count, entry) in std::fs::read_dir(root)?.take(129).enumerate() {
            ensure(count < 128, "image cache directory capacity")?;
            let entry = entry?;
            let meta = std::fs::symlink_metadata(entry.path())?;
            if meta.is_dir() {
                directories += 1;
                ensure(
                    entry.file_name().to_string_lossy().starts_with(".source-")
                        && meta.mode() & 0o777 == 0o700
                        && meta.uid() == rustix::process::geteuid().as_raw(),
                    "unknown image directory",
                )?;
                files += std::fs::read_dir(entry.path())?.take(129).count();
            } else {
                ensure(meta.is_file(), "invalid image cache entry")?;
                files += 1;
            }
            ensure(
                files <= 128 - slots && directories <= 128,
                "computer artifact capacity",
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod expiry_tests;
