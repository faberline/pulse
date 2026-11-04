use super::{Store, digest, require};
use crate::{
    Error, MEMORY_RETENTION_MS, Memory, MemoryCommand, MemoryOutcome, MemoryRequest, Origin, Result,
};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tokio::sync::{Mutex, MutexGuard};

const EVENT_LIMIT: i64 = 10_000;
const MANIFEST_LIMIT: u64 = 8 * 1024 * 1024;

mod backup;
mod checkpoint;
mod copy;
pub(super) mod derived_messages;
mod derived_results;
mod revocations;
pub(super) mod task_sources;
mod withdrawals;
mod worker_copy;
pub use backup::{BackupCheck, Maintenance, OfflineBackupCheck};
pub use checkpoint::truncate as truncate_wal;
pub use copy::MemoryCopyChange;
pub use task_sources::TaskMemoryLease;
pub use withdrawals::MemoryWithdrawalRules;

/// This file is deliberately outside the database backup. It is restriction
/// evidence, not another task or knowledge store. Never put source text in it.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Restrictions {
    format: u8,
    keeper: String,
    epoch: u64,
    removed: BTreeMap<String, u64>,
    #[serde(default)]
    sources: BTreeMap<String, revocations::SourceRestriction>,
    #[serde(default)]
    cancellations: BTreeMap<String, revocations::Cancellation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    copy: Option<copy::CopyFence>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    withdrawals: BTreeMap<String, withdrawals::Withdrawal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    worker: Option<worker_copy::WorkerCopy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    restore: Option<RestoreFence>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreFence {
    generation: String,
    committed: bool,
}

pub(super) struct MemoryGuard {
    database: PathBuf,
    manifest: PathBuf,
    lock: PathBuf,
    owner: u32,
    serial: Mutex<()>,
    changed: tokio::sync::Notify,
}

struct Lease<'a> {
    _file: File,
    _serial: MutexGuard<'a, ()>,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        // Closing only this descriptor can leave the lock held by a descriptor
        // inherited during a concurrent process spawn. Release the lock itself.
        let _ = self._file.unlock();
    }
}

impl MemoryGuard {
    pub(super) fn new(database: &Path) -> Result<Self> {
        std::fs::metadata(database)?;
        Self::target(database)
    }

    fn target(database: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let name = database
                .file_name()
                .ok_or_else(|| Error::Rejected("database filename required".into()))?;
            let parent = database
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let parent = parent.canonicalize()?;
            let metadata = std::fs::metadata(&parent)?;
            require(
                metadata.uid() == rustix::process::geteuid().as_raw(),
                "database directory must be owned",
            )?;
            let absolute = parent.join(name);
            let mut manifest = absolute.as_os_str().to_owned();
            manifest.push(".memory-restrictions.json");
            let mut lock = absolute.as_os_str().to_owned();
            lock.push(".memory-lock");
            Ok(Self {
                database: absolute,
                manifest: manifest.into(),
                lock: lock.into(),
                owner: metadata.uid(),
                serial: Mutex::new(()),
                changed: tokio::sync::Notify::new(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = database;
            Err(Error::Rejected(
                "private memory storage requires Unix".into(),
            ))
        }
    }

    async fn lease(&self) -> Result<Lease<'_>> {
        let serial = self.serial.lock().await;
        let file = self.open_private(&self.lock, true, false)?;
        let started = std::time::Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::Error(e))
                    if e.kind() == std::io::ErrorKind::Interrupted
                        && started.elapsed() < std::time::Duration::from_millis(250) =>
                {
                    continue;
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    if started.elapsed() >= std::time::Duration::from_millis(250) {
                        return Err(Error::Busy);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
            }
        }
        Ok(Lease {
            _serial: serial,
            _file: file,
        })
    }

    fn open_private(&self, path: &Path, create: bool, exclusive: bool) -> Result<File> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(create)
                .create_new(exclusive)
                .truncate(false)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)?;
            let metadata = file.metadata()?;
            require(
                metadata.is_file()
                    && metadata.nlink() == 1
                    && metadata.uid() == self.owner
                    && metadata.permissions().mode() & 0o077 == 0,
                "memory restrictions must be private owned regular files",
            )?;
            Ok(file)
        }
        #[cfg(not(unix))]
        {
            let _ = (path, create, exclusive);
            Err(Error::Rejected(
                "private memory storage requires Unix".into(),
            ))
        }
    }

    fn read(&self) -> Result<Option<Restrictions>> {
        let file = match self.open_private(&self.manifest, false, false) {
            Ok(file) => file,
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        require(
            file.metadata()?.len() <= MANIFEST_LIMIT,
            "memory restriction capacity",
        )?;
        let mut bytes = Vec::new();
        file.take(MANIFEST_LIMIT + 1).read_to_end(&mut bytes)?;
        require(
            bytes.len() as u64 <= MANIFEST_LIMIT,
            "memory restriction capacity",
        )?;
        let value: Restrictions = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Rejected("invalid memory restrictions".into()))?;
        require(
            (1..=8).contains(&value.format)
                && match &value.restore {
                    Some(restore) => {
                        value.format == 8 && uuid::Uuid::parse_str(&restore.generation).is_ok()
                    }
                    None => value.format < 8,
                }
                && value
                    .worker
                    .as_ref()
                    .is_none_or(|w| w.compatible(value.format))
                && uuid::Uuid::parse_str(&value.keeper).is_ok()
                && value.removed.len() <= 20_100
                && revocations::valid(&value)
                && value
                    .copy
                    .as_ref()
                    .is_none_or(|c| value.format >= 3 && c.valid())
                && withdrawals::valid(&value)
                && value.removed.iter().all(|(id, revision)| {
                    uuid::Uuid::parse_str(id).is_ok()
                        && *revision > 0
                        && *revision <= i64::MAX as u64
                })
                && ((value.removed.is_empty() && value.sources.is_empty()) == (value.epoch == 0)),
            "invalid memory restrictions",
        )?;
        Ok(Some(value))
    }

    fn write(&self, value: &Restrictions) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        require(
            bytes.len() as u64 <= MANIFEST_LIMIT,
            "memory restriction capacity",
        )?;
        let temporary = self
            .manifest
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut file = self.open_private(&temporary, true, true)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, &self.manifest)?;
            File::open(self.manifest.parent().unwrap())?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(temporary);
        }
        result
    }
}

impl Store {
    pub(super) async fn initialize_memory(&self) -> Result<()> {
        let _lease = self.memory_guard.lease().await?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let bound: Option<String> =
            sqlx::query_scalar("SELECT value FROM settings WHERE key='memory_keeper_id'")
                .fetch_optional(&mut *tx)
                .await?;
        let manifest = self.memory_guard.read()?;
        if let Some(manifest) = &manifest {
            restore_binding(&mut tx, manifest).await?;
        }
        let mut manifest = match (bound, manifest) {
            (Some(keeper), Some(manifest)) => {
                require(keeper == manifest.keeper, "wrong memory restriction keeper")?;
                manifest
            }
            (Some(_), None) => {
                return Err(Error::Rejected(
                    "latest memory restrictions are missing; restore refused".into(),
                ));
            }
            (None, manifest) => {
                let records: i64 = sqlx::query_scalar(
                    "SELECT (SELECT count(*) FROM memories)+(SELECT count(*) FROM memory_events)",
                )
                .fetch_one(&mut *tx)
                .await?;
                require(records == 0, "unbound knowledge database")?;
                let manifest = match manifest {
                    Some(manifest) => manifest,
                    None => {
                        let manifest = Restrictions {
                            format: 4,
                            keeper: uuid::Uuid::new_v4().to_string(),
                            epoch: 0,
                            removed: BTreeMap::new(),
                            sources: BTreeMap::new(),
                            cancellations: BTreeMap::new(),
                            copy: None,
                            withdrawals: BTreeMap::new(),
                            worker: None,
                            restore: None,
                        };
                        self.memory_guard.write(&manifest)?;
                        manifest
                    }
                };
                sqlx::query("INSERT INTO settings(key,value) VALUES('memory_keeper_id',?),('memory_redaction_epoch','0')")
                    .bind(&manifest.keeper).execute(&mut *tx).await?;
                manifest
            }
        };
        let upgraded = revocations::upgrade(&mut tx, &mut manifest).await?;
        let imported = if manifest.copy.is_some() {
            withdrawals::import(&mut tx, &mut manifest).await?
        } else {
            false
        };
        if upgraded || imported || manifest.format < 4 {
            manifest.format = manifest.format.max(4);
            self.memory_guard.write(&manifest)?;
        }
        reconcile(&mut tx, &manifest).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        Ok(())
    }

    /// Explicit Human writes only. Not a model tool. The request ID must be a
    /// random UUID with no private text. A replay cannot renew the source date.
    pub async fn change_memory(
        &self,
        origin: Origin,
        request: &MemoryRequest,
        now: i64,
    ) -> Result<MemoryOutcome> {
        let mut normalized = request.clone();
        normalized.source_id = revocations::canonical(&request.source_id)?;
        let request = &normalized;
        require(origin == Origin::Human, "only Human may change knowledge")?;
        require(
            uuid::Uuid::parse_str(&request.source_id).is_ok(),
            "memory source must be a UUID",
        )?;
        require(
            now >= 0 && request.source_at >= 0 && request.source_at <= now,
            "invalid memory source time",
        )?;
        match &request.command {
            MemoryCommand::Remember { label, body }
            | MemoryCommand::Correct { label, body, .. } => {
                require(
                    !label.is_empty()
                        && label.len() <= 128
                        && label.trim() == label
                        && !label.chars().any(char::is_control),
                    "memory label must be 1..128 UTF-8 bytes",
                )?;
                require(
                    !body.trim().is_empty()
                        && body.len() <= 1024
                        && !body
                            .chars()
                            .any(|c| c.is_control() && c != '\n' && c != '\t'),
                    "memory body must be 1..1024 UTF-8 bytes",
                )?;
            }
            MemoryCommand::Forget { .. } | MemoryCommand::Revoke { .. } => {}
        }
        if let MemoryCommand::Revoke { sources, expected } = &request.command {
            return self.revoke_sources(request, sources, expected, now).await;
        }
        let _lease = self.memory_guard.lease().await?;
        let mut restrictions = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &restrictions).await?;
        require(
            !restrictions.cancellations.contains_key(&request.source_id),
            "source belongs to a cancellation",
        )?;
        self.expire_memory_in(&mut tx, &mut restrictions, now)
            .await?;
        if let Some(row) = sqlx::query(
            "SELECT source_id,memory_id,revision,fingerprint,outcome FROM memory_events WHERE source_uuid=?",
        )
        .bind(&request.source_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            let id = row.get("memory_id");
            let revision = row.get::<i64, _>("revision") as u64;
            let fingerprint: Option<String> = row.get("fingerprint");
            let outcome: String = row.get("outcome");
            let reply = if let Some(fingerprint) = fingerprint {
                let mut original=request.clone();
                original.source_id=row.get("source_id");
                require(fingerprint == digest(&original)?, "memory source ID conflict")?;
                match outcome.as_str() {
                    "stored" => MemoryOutcome::Stored { id, revision },
                    "forgotten" => MemoryOutcome::Forgotten { id, revision },
                    _ => MemoryOutcome::Redacted { id, revision },
                }
            } else {
                MemoryOutcome::Redacted { id, revision }
            };
            tx.commit().await?;
            self.checkpoint_memory_redaction().await?;
            return Ok(reply);
        }
        let expires_at = request
            .source_at
            .checked_add(MEMORY_RETENTION_MS)
            .ok_or_else(|| Error::Rejected("invalid memory expiry".into()))?;
        require(expires_at > now, "memory source has expired")?;
        let (id, revision) = match &request.command {
            MemoryCommand::Revoke { .. } => unreachable!(),
            MemoryCommand::Remember { .. } => (uuid::Uuid::new_v4().to_string(), 1),
            MemoryCommand::Correct { id, revision, .. }
            | MemoryCommand::Forget { id, revision } => {
                require(
                    uuid::Uuid::parse_str(id).is_ok()
                        && *revision > 0
                        && *revision < i64::MAX as u64,
                    "invalid memory reference",
                )?;
                let current: Option<i64> =
                    sqlx::query_scalar("SELECT revision FROM memories WHERE id=?")
                        .bind(id)
                        .fetch_optional(&mut *tx)
                        .await?;
                require(
                    current == Some(*revision as i64),
                    "memory revision is stale or unavailable",
                )?;
                (id.clone(), *revision)
            }
        };
        if !matches!(request.command, MemoryCommand::Forget { .. }) {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM memory_events")
                .fetch_one(&mut *tx)
                .await?;
            require(
                count < EVENT_LIMIT,
                "memory source capacity; new knowledge refused",
            )?;
        }
        if let MemoryCommand::Remember { .. } = &request.command {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM memories")
                .fetch_one(&mut *tx)
                .await?;
            require(count < 100, "memory capacity is 100")?;
        }
        if let MemoryCommand::Remember { label, .. } | MemoryCommand::Correct { label, .. } =
            &request.command
        {
            let exists: i64 =
                sqlx::query_scalar("SELECT count(*) FROM memories WHERE label=? AND id<>?")
                    .bind(label)
                    .bind(&id)
                    .fetch_one(&mut *tx)
                    .await?;
            require(
                exists == 0,
                "memory label exists; correct its current revision",
            )?;
        }
        if !matches!(request.command, MemoryCommand::Remember { .. }) {
            // Durable restriction FIRST. A crash or failed SQL commit is recovered
            // by reconcile before any subsequent retrieval or correction.
            restrict(&mut restrictions, &id, revision)?;
            revocations::capture(&mut tx, &mut restrictions, &id, revision).await?;
            self.memory_guard.write(&restrictions)?;
            reconcile(&mut tx, &restrictions).await?;
        }
        let reply = match &request.command {
            MemoryCommand::Revoke { .. } => unreachable!(),
            MemoryCommand::Remember { label, body }
            | MemoryCommand::Correct { label, body, .. } => {
                let revision = if matches!(request.command, MemoryCommand::Correct { .. }) {
                    revision + 1
                } else {
                    revision
                };
                sqlx::query("INSERT INTO memories(id,revision,label,body,source_id,source_at,expires_at) VALUES(?,?,?,?,?,?,?)")
                    .bind(&id).bind(revision as i64).bind(label).bind(body).bind(&request.source_id)
                    .bind(request.source_at).bind(expires_at).execute(&mut *tx).await?;
                MemoryOutcome::Stored {
                    id: id.clone(),
                    revision,
                }
            }
            MemoryCommand::Forget { .. } => MemoryOutcome::Forgotten {
                id: id.clone(),
                revision,
            },
        };
        let (revision, outcome) = match &reply {
            MemoryOutcome::Stored { revision, .. } => (*revision, "stored"),
            MemoryOutcome::Forgotten { revision, .. } => (*revision, "forgotten"),
            MemoryOutcome::Redacted { .. } | MemoryOutcome::Revoked { .. } => unreachable!(),
        };
        sqlx::query("INSERT INTO memory_events(source_id,memory_id,revision,fingerprint,outcome) VALUES(?,?,?,?,?)")
            .bind(&request.source_id).bind(&id).bind(revision as i64).bind(digest(request)?)
            .bind(outcome).execute(&mut *tx).await?;
        tx.commit().await?;
        self.memory_guard.changed.notify_one();
        self.checkpoint_memory_redaction().await?;
        Ok(reply)
    }

    /// Bounded live projection. Expired and redacted source text is never returned.
    pub async fn list_memories(&self, now: i64) -> Result<Vec<Memory>> {
        Ok(self.memory_view(now).await?.memories)
    }

    pub async fn memory_view(&self, now: i64) -> Result<crate::MemoryView> {
        require(now >= 0, "invalid memory clock")?;
        let _lease = self.memory_guard.lease().await?;
        let mut restrictions = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &restrictions).await?;
        self.expire_memory_in(&mut tx, &mut restrictions, now)
            .await?;
        let rows = sqlx::query("SELECT * FROM memories ORDER BY source_at,id LIMIT 100")
            .fetch_all(&mut *tx)
            .await?;
        let memories = rows
            .into_iter()
            .map(|row| Memory {
                id: row.get("id"),
                revision: row.get::<i64, _>("revision") as u64,
                label: row.get("label"),
                body: row.get("body"),
                source_id: row.get("source_uuid"),
                source_at: row.get("source_at"),
                expires_at: row.get("expires_at"),
            })
            .collect();
        let redacted_sources = sqlx::query_scalar("SELECT source_uuid FROM memory_events WHERE fingerprint IS NULL ORDER BY source_uuid LIMIT 20100")
            .fetch_all(&mut *tx).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        Ok(crate::MemoryView {
            keeper: restrictions.keeper,
            epoch: restrictions.epoch,
            memories,
            redacted_sources,
        })
    }

    async fn checkpoint_memory_redaction(&self) -> Result<()> {
        checkpoint::truncate(&self.pool).await
    }

    async fn expire_memory_in(
        &self,
        db: &mut SqliteConnection,
        restrictions: &mut Restrictions,
        now: i64,
    ) -> Result<()> {
        let expired = sqlx::query("SELECT id,revision FROM memories WHERE expires_at<=?")
            .bind(now)
            .fetch_all(&mut *db)
            .await?;
        if expired.is_empty() {
            return Ok(());
        }
        for row in expired {
            revocations::capture(
                db,
                restrictions,
                &row.get::<String, _>("id"),
                row.get::<i64, _>("revision") as u64,
            )
            .await?;
            restrict(
                restrictions,
                &row.get::<String, _>("id"),
                row.get::<i64, _>("revision") as u64,
            )?;
        }
        self.memory_guard.write(restrictions)?;
        reconcile(db, restrictions).await
    }
}

fn restrict(value: &mut Restrictions, id: &str, revision: u64) -> Result<()> {
    let existing = value.removed.entry(id.to_owned()).or_default();
    *existing = (*existing).max(revision);
    value.epoch = value
        .epoch
        .checked_add(1)
        .ok_or_else(|| Error::Rejected("memory restriction epoch exhausted".into()))?;
    Ok(())
}

async fn reconcile(db: &mut SqliteConnection, manifest: &Restrictions) -> Result<()> {
    restore_binding(db, manifest).await?;
    let keeper: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='memory_keeper_id'")
            .fetch_one(&mut *db)
            .await?;
    require(keeper == manifest.keeper, "wrong memory restriction keeper")?;
    let epoch: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='memory_redaction_epoch'")
            .fetch_one(&mut *db)
            .await?;
    let epoch: u64 = epoch
        .parse()
        .map_err(|_| Error::Rejected("invalid memory restriction epoch".into()))?;
    require(
        epoch <= manifest.epoch,
        "memory restrictions are stale; restore refused",
    )?;
    if epoch == manifest.epoch {
        return task_sources::redact_missing(db).await;
    }
    for (id, revision) in &manifest.removed {
        sqlx::query("DELETE FROM memories WHERE id=? AND revision<=?")
            .bind(id)
            .bind(*revision as i64)
            .execute(&mut *db)
            .await?;
        sqlx::query("UPDATE memory_events SET fingerprint=NULL,outcome='redacted' WHERE memory_id=? AND revision<=?")
            .bind(id).bind(*revision as i64).execute(&mut *db).await?;
    }
    revocations::replay(db, manifest).await?;
    task_sources::redact_missing(db).await?;
    sqlx::query("UPDATE settings SET value=? WHERE key='memory_redaction_epoch'")
        .bind(manifest.epoch.to_string())
        .execute(&mut *db)
        .await?;
    Ok(())
}

async fn restore_binding(db: &mut SqliteConnection, manifest: &Restrictions) -> Result<()> {
    if let Some(fence) = &manifest.restore {
        require(
            fence.committed,
            "offline restore is incomplete; resume required",
        )?;
        let generation: Option<String> =
            sqlx::query_scalar("SELECT value FROM settings WHERE key='restore_generation'")
                .fetch_optional(&mut *db)
                .await?;
        require(
            generation.as_deref() == Some(fence.generation.as_str()),
            "database predates the current restore; offline recovery required",
        )?;
    }
    Ok(())
}

pub(super) fn admit_database_open(database: &Path) -> Result<()> {
    let guard = MemoryGuard::target(database)?;
    let manifest = guard.read()?;
    if let Some(fence) = manifest
        .as_ref()
        .and_then(|manifest| manifest.restore.as_ref())
    {
        require(
            fence.committed && database.exists(),
            "offline restore is incomplete; resume required",
        )?;
    }
    let missing_or_empty = match std::fs::symlink_metadata(&guard.database) {
        Ok(metadata) => metadata.is_file() && metadata.len() == 0,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => return Err(error.into()),
    };
    // Local copies retain their own rebuilding and identity checks, including
    // external denials. Server task, receipt and budget data cannot be recreated.
    let restricted_copy = manifest
        .as_ref()
        .is_some_and(|value| value.copy.is_some() || value.worker.is_some());
    if !missing_or_empty || restricted_copy {
        return Ok(());
    }
    if manifest.is_some() {
        return Err(Error::DatabaseRecoveryRequired);
    }
    // Empty lock files alone are not evidence of accepted application data.
    // Inspect metadata only, including dangling links; never open SQLite or
    // regenerate identity material while deciding whether startup is safe.
    for ending in [".backup-key", ".backups", "-wal", "-shm", "-journal"] {
        let mut name = guard.database.as_os_str().to_owned();
        name.push(ending);
        match std::fs::symlink_metadata(Path::new(&name)) {
            Ok(_) => return Err(Error::DatabaseRecoveryRequired),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(super) async fn admit_database_pool(database: &Path, pool: &sqlx::SqlitePool) -> Result<()> {
    let guard = MemoryGuard::target(database)?;
    if let Some(manifest) = guard.read()?
        && manifest.restore.is_some()
    {
        let mut db = pool.acquire().await?;
        restore_binding(&mut db, &manifest).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn lease_releases_lock_despite_an_inherited_file_description() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
        let lease = store.memory_guard.lease().await.unwrap();
        let inherited = lease._file.try_clone().unwrap();
        drop(lease);
        let next = store.memory_guard.lease().await.unwrap();
        drop(next);
        drop(inherited);
    }
}
