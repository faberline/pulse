use super::*;
use crate::Versions;
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha256};
use std::sync::Arc;

mod crypto;
mod dispatch;
mod restore;
pub use restore::RestoreReport;
const DAY: i64 = 24 * 60 * 60 * 1_000;
const RETENTION: i64 = 30 * DAY;
const QUARANTINE_SCHEMA: i64 = 2_147_483_647;

#[derive(Debug)]
pub struct Maintenance {
    pub backup: Option<PathBuf>,
    pub next_at: i64,
    pub removed_backups: usize,
}

/// This is a quarantined validation result, never a runnable restored server.
#[derive(Debug)]
pub struct BackupCheck {
    pub candidate: PathBuf,
    pub memories: usize,
    pub tasks: usize,
    pub unknown_tasks: usize,
}

/// Offline validation only. This never replaces the original database.
#[derive(Debug)]
pub struct OfflineBackupCheck {
    pub backup: BackupCheck,
    /// Owner, keeper and key match a readable current database. This is not
    /// proof that its complete receipt, credential or financial ledger is current.
    pub source_identity_verified: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Secret {
    version: u8,
    keeper: String,
    key: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LastBackup {
    filename: String,
    created_at: i64,
    digest: String,
}

fn suffix(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

impl Store {
    pub async fn wait_for_maintenance_change(&self) {
        self.memory_guard.changed.notified().await;
    }

    /// Local maintenance only. It makes no model call and grants no authority.
    /// Call at startup, then at next_at or after a new earlier memory expiry.
    pub async fn maintain(&self, now: i64) -> Result<Maintenance> {
        require(
            (0..=i64::MAX - RETENTION - DAY).contains(&now),
            "invalid maintenance clock",
        )?;
        let _lease = self.memory_guard.lease().await?;
        let mut manifest = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &manifest).await?;
        self.expire_memory_in(&mut tx, &mut manifest, now).await?;
        let next_expiry: Option<i64> = sqlx::query_scalar("SELECT min(expires_at) FROM memories")
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        let key = self.backup_secret(&manifest.keeper, true).await?;
        let directory = self.backup_directory()?;
        let last: Option<String> =
            sqlx::query_scalar("SELECT value FROM settings WHERE key='last_backup'")
                .fetch_optional(&self.pool)
                .await?;
        let last = last
            .map(|s| {
                serde_json::from_str::<LastBackup>(&s)
                    .map_err(|_| Error::Rejected("invalid backup checkpoint".into()))
            })
            .transpose()?;
        let guard = self.memory_guard.clone();
        let directory_copy = directory.clone();
        let validation_key = key.key;
        let recovery_secret = key.clone();
        let keeper = manifest.keeper.clone();
        let (removed, next_removal, current) = tokio::task::spawn_blocking(move || {
            let recovery_expiry = restore::cleanup(&guard, &recovery_secret, now)?;
            let (removed, next, current) = manage_files(
                &guard,
                &directory_copy,
                last.as_ref(),
                &validation_key,
                &keeper,
                now,
            )?;
            Ok::<_, Error>((
                removed,
                next.min(recovery_expiry.unwrap_or(i64::MAX)),
                current,
            ))
        })
        .await
        .map_err(|_| Error::Rejected("backup file worker stopped".into()))??;
        let mut backup = None;
        if !current {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
                .fetch_one(&mut *tx)
                .await?;
            let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
                .fetch_one(&mut *tx)
                .await?;
            require(
                pages
                    .checked_mul(page_size)
                    .is_some_and(|n| n > 0 && n <= crypto::MAX_DATABASE as i64),
                "backup database exceeds 512 MiB capacity",
            )?;
            // SQLite supplies a transaction-consistent image including WAL pages.
            // The plaintext image stays in memory; there is no plaintext temp file.
            let snapshot = tx.serialize(None).await?;
            require(
                snapshot.len() <= crypto::MAX_DATABASE,
                "backup image capacity",
            )?;
            tx.commit().await?;
            let id = uuid::Uuid::new_v4().to_string();
            let filename = format!("{now}-{id}.pulsebak");
            let target = directory.join(&filename);
            let temporary = directory.join(format!("{id}.partial"));
            let header = crypto::Header {
                version: 1,
                keeper: manifest.keeper,
                id,
                created_at: now,
                length: snapshot.len(),
            };
            let guard = self.memory_guard.clone();
            let target_copy = target.clone();
            let hash = tokio::task::spawn_blocking(move || -> Result<String> {
                let result = (|| {
                    let mut file = guard.open_private(&temporary, true, true)?;
                    let hash = crypto::seal(&key.key, &header, &snapshot, &mut file)?;
                    file.sync_all()?;
                    // Never overwrite a previously published backup.
                    rustix::fs::renameat_with(
                        rustix::fs::CWD,
                        &temporary,
                        rustix::fs::CWD,
                        &target_copy,
                        rustix::fs::RenameFlags::NOREPLACE,
                    )
                    .map_err(std::io::Error::from)?;
                    File::open(target_copy.parent().unwrap())?.sync_all()?;
                    Ok(hash)
                })();
                if result.is_err() {
                    let _ = std::fs::remove_file(&temporary);
                }
                result
            })
            .await
            .map_err(|_| Error::Rejected("backup encryption worker stopped".into()))??;
            let value = serde_json::to_string(&LastBackup {
                filename,
                created_at: now,
                digest: hash,
            })?;
            sqlx::query("INSERT INTO settings(key,value) VALUES('last_backup',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                .bind(value).execute(&self.pool).await?;
            backup = Some(target);
        }
        Ok(Maintenance {
            backup,
            next_at: ((now / DAY + 1) * DAY)
                .min(next_expiry.unwrap_or(i64::MAX))
                .min(next_removal),
            removed_backups: removed,
        })
    }

    fn backup_directory(&self) -> Result<PathBuf> {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        let path = suffix(&self.memory_guard.database, ".backups");
        match std::fs::DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => File::open(path.parent().unwrap())?.sync_all()?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let metadata = std::fs::symlink_metadata(&path)?;
        require(
            metadata.is_dir()
                && metadata.uid() == self.memory_guard.owner
                && metadata.permissions().mode() & 0o077 == 0,
            "backup directory must be private and owned",
        )?;
        Ok(path)
    }

    async fn backup_secret(&self, keeper: &str, create: bool) -> Result<Secret> {
        let path = suffix(&self.memory_guard.database, ".backup-key");
        let bound: Option<String> =
            sqlx::query_scalar("SELECT value FROM settings WHERE key='backup_key_fingerprint'")
                .fetch_optional(&self.pool)
                .await?;
        let file = match self.memory_guard.open_private(&path, false, false) {
            Ok(file) => file,
            Err(Error::Io(e))
                if e.kind() == std::io::ErrorKind::NotFound && bound.is_none() && create =>
            {
                require(
                    std::fs::read_dir(self.backup_directory()?)?
                        .next()
                        .is_none(),
                    "missing backup key with existing files",
                )?;
                let mut key = [0; 32];
                SystemRandom::new()
                    .fill(&mut key)
                    .map_err(|_| Error::Rejected("backup key creation failed".into()))?;
                let secret = Secret {
                    version: 1,
                    keeper: keeper.to_owned(),
                    key,
                };
                // Exclusive creation prevents a missing or corrupt key being replaced.
                let mut file = self.memory_guard.open_private(&path, true, true)?;
                file.write_all(&serde_json::to_vec(&secret)?)?;
                file.sync_all()?;
                File::open(path.parent().unwrap())?.sync_all()?;
                file
            }
            Err(e) => return Err(e),
        };
        let secret = read_secret(file, keeper)?;
        let fingerprint = format!("{:x}", Sha256::digest(secret.key));
        if let Some(bound) = bound {
            require(bound == fingerprint, "backup key fingerprint mismatch")?;
        } else {
            require(create, "backup key not bound to live database")?;
            sqlx::query("INSERT INTO settings(key,value) VALUES('backup_key_fingerprint',?)")
                .bind(fingerprint)
                .execute(&self.pool)
                .await?;
        }
        Ok(secret)
    }

    /// Decrypt only using this live keeper and key. The output remains quarantined
    /// with an unsupported schema marker even if this process dies during checks.
    /// This does NOT promote a backup or reconnect its devices to an executor.
    pub async fn check_backup(
        &self,
        input: &Path,
        destination: &Path,
        versions: &Versions,
        now: i64,
    ) -> Result<BackupCheck> {
        require(
            versions.valid() && now >= 0,
            "invalid restore manifest or clock",
        )?;
        let _lease = self.memory_guard.lease().await?;
        let manifest = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("latest restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &manifest).await?;
        tx.commit().await?;
        let secret = self.backup_secret(&manifest.keeper, false).await?;
        let owner: String = sqlx::query_scalar("SELECT value FROM settings WHERE key='owner'")
            .fetch_one(&self.pool)
            .await?;
        let candidate = stage_backup(
            BackupSource {
                guard: &self.memory_guard,
                manifest: &manifest,
                owner: &owner,
                secret,
            },
            input,
            destination,
            versions,
            now,
        )
        .await?;
        finish_check(candidate, destination, now).await
    }
}

fn read_secret(mut file: File, keeper: &str) -> Result<Secret> {
    use std::io::{Seek, SeekFrom};
    require(file.metadata()?.len() <= 1024, "invalid backup key file")?;
    file.seek(SeekFrom::Start(0))?;
    let mut encoded = Vec::new();
    file.take(1025).read_to_end(&mut encoded)?;
    let secret: Secret = serde_json::from_slice(&encoded)
        .map_err(|_| Error::Rejected("invalid backup key file".into()))?;
    require(
        secret.version == 1 && secret.keeper == keeper,
        "wrong backup key keeper",
    )?;
    Ok(secret)
}

impl crate::DatabaseMaintenance {
    /// Validate an encrypted backup with the original key and current external
    /// restrictions, even when the current database is missing or unreadable.
    /// No source database is created, migrated, replaced or granted work.
    pub async fn check_backup(
        &self,
        input: &Path,
        destination: &Path,
        owner: &str,
        versions: &Versions,
        now: i64,
    ) -> Result<OfflineBackupCheck> {
        require(
            !owner.is_empty() && versions.valid() && now >= 0,
            "invalid offline backup check",
        )?;
        let guard = MemoryGuard::target(self.database())?;
        let _lease = guard.lease().await?;
        let manifest = guard
            .read()?
            .ok_or_else(|| Error::Rejected("latest restrictions missing".into()))?;
        require(
            manifest.restore.as_ref().is_none_or(|f| f.committed),
            "offline restore is incomplete; resume required",
        )?;
        let secret = read_secret(
            guard.open_private(&suffix(self.database(), ".backup-key"), false, false)?,
            &manifest.keeper,
        )?;
        let source_identity_verified = source_identity(self, owner, &manifest, &secret).await?;
        let candidate = stage_backup(
            BackupSource {
                guard: &guard,
                manifest: &manifest,
                owner,
                secret,
            },
            input,
            destination,
            versions,
            now,
        )
        .await?;
        Ok(OfflineBackupCheck {
            backup: finish_check(candidate, destination, now).await?,
            source_identity_verified,
        })
    }
}

async fn source_identity(
    admission: &crate::DatabaseMaintenance,
    owner: &str,
    manifest: &Restrictions,
    secret: &Secret,
) -> Result<bool> {
    if !admission.database().exists() {
        return Ok(false);
    }
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(admission.database())
        .read_only(true)
        .create_if_missing(false)
        .busy_timeout(std::time::Duration::from_millis(250));
    let Ok(pool) = admission.pool_options().connect_with(options).await else {
        return Ok(false);
    };
    let result = async {
        let rows: std::result::Result<Vec<(String, String)>, sqlx::Error> = sqlx::query_as(
            "SELECT key,value FROM settings WHERE key IN ('owner','memory_keeper_id','backup_key_fingerprint')",
        )
        .fetch_all(&pool)
        .await;
        let Ok(rows) = rows else { return Ok(false) };
        let values: BTreeMap<_, _> = rows.into_iter().collect();
        let fingerprint = format!("{:x}", Sha256::digest(secret.key));
        for (key, expected) in [
            ("owner", owner),
            ("memory_keeper_id", manifest.keeper.as_str()),
            ("backup_key_fingerprint", fingerprint.as_str()),
        ] {
            if let Some(value) = values.get(key) {
                require(value == expected, "current database binding mismatch")?;
            }
        }
        let schema: i64 = sqlx::query_scalar("PRAGMA user_version").fetch_one(&pool).await?;
        if let Some(fence) = &manifest.restore {
            let generation: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key='restore_generation'")
                .fetch_optional(&pool)
                .await?;
            require(
                generation.as_deref() == Some(fence.generation.as_str()),
                "current database predates restore generation",
            )?;
        }
        Ok(values.len() == 3 && schema == crate::store::SCHEMA)
    }
    .await;
    pool.close().await;
    result
}

async fn finish_check(candidate: Store, destination: &Path, now: i64) -> Result<BackupCheck> {
    let memories = candidate.list_memories(now).await?.len();
    let tasks = candidate.list_tasks(100).await?;
    let unknown_tasks = tasks
        .iter()
        .filter(|t| t.state == crate::State::OutcomeUnknown)
        .count();
    let marker: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&candidate.pool)
        .await?;
    require(
        marker == QUARANTINE_SCHEMA,
        "restore quarantine marker changed",
    )?;
    candidate.close().await;
    File::open(destination)?.sync_all()?;
    Ok(BackupCheck {
        candidate: destination.join("candidate.db"),
        memories,
        tasks: tasks.len(),
        unknown_tasks,
    })
}

struct BackupSource<'a> {
    guard: &'a MemoryGuard,
    manifest: &'a Restrictions,
    owner: &'a str,
    secret: Secret,
}

async fn stage_backup(
    source: BackupSource<'_>,
    input: &Path,
    destination: &Path,
    versions: &Versions,
    now: i64,
) -> Result<Store> {
    require(
        versions.valid() && now >= 0,
        "invalid restore manifest or clock",
    )?;
    let file = source.guard.open_private(input, false, false)?;
    require(
        file.metadata()?.len() <= crypto::MAX_DATABASE as u64 + 64 * 1024,
        "encrypted backup capacity",
    )?;
    let key = source.secret.key;
    let (header, bytes) = tokio::task::spawn_blocking(move || crypto::open(&key, file))
        .await
        .map_err(|_| Error::Rejected("backup decryption worker stopped".into()))??;
    require(
        header.keeper == source.manifest.keeper
            && header.created_at <= now
            && now - header.created_at < RETENTION,
        "backup is expired, future-dated or belongs to another keeper",
    )?;
    stage_image(source, bytes, destination, versions, now, false).await
}

async fn stage_image(
    source: BackupSource<'_>,
    mut bytes: Vec<u8>,
    destination: &Path,
    versions: &Versions,
    now: i64,
    already_fenced: bool,
) -> Result<Store> {
    let BackupSource {
        guard: source_guard,
        manifest,
        owner: live_owner,
        secret,
    } = source;
    let fingerprint = format!("{:x}", Sha256::digest(secret.key));
    require(
        bytes.starts_with(b"SQLite format 3\0")
            && bytes.len() >= 100
            && u32::from_be_bytes(bytes[60..64].try_into().unwrap()) == crate::store::SCHEMA as u32,
        "backup schema incompatible",
    )?;
    // SQLite header offset60 is user_version. Quarantine BEFORE any plaintext
    // is written. Every released Pulse binary rejects this schema marker.
    bytes[60..64].copy_from_slice(&(QUARANTINE_SCHEMA as u32).to_be_bytes());
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    let parent = destination
        .parent()
        .ok_or_else(|| Error::Rejected("restore parent directory missing".into()))?;
    let metadata = std::fs::symlink_metadata(parent)?;
    require(
        metadata.is_dir()
            && metadata.uid() == source_guard.owner
            && metadata.permissions().mode() & 0o077 == 0,
        "restore parent must be a private owned directory",
    )?;
    std::fs::DirBuilder::new().mode(0o700).create(destination)?;
    let database = destination.join("candidate.db");
    let pool_options = super::super::database_access::shared_pool(&database)?;
    let mut file = source_guard.open_private(&database, true, true)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(bytes);
    File::open(destination)?.sync_all()?;
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&database)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Full)
        .foreign_keys(true)
        .pragma("secure_delete", "ON");
    let pool = pool_options.connect_with(options).await?;
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await?;
    require(integrity == "ok", "backup database integrity failed")?;
    let foreign_keys = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await?;
    require(foreign_keys.is_empty(), "backup foreign keys failed")?;
    // Validate, never migrate a candidate: a migration could reset its
    // quarantine schema marker before a later validation failure.
    let rows =
        sqlx::query("SELECT version,success,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await?;
    let migrator = sqlx::migrate!();
    let expected: Vec<_> = migrator
        .iter()
        .filter(|m| m.migration_type.is_up_migration())
        .collect();
    require(
        rows.len() == expected.len(),
        "backup migration set incompatible",
    )?;
    for (row, migration) in rows.iter().zip(expected) {
        require(
            row.get::<i64, _>("version") == migration.version
                && row.get::<bool, _>("success")
                && row.get::<Vec<u8>, _>("checksum").as_slice() == migration.checksum.as_ref(),
            "backup migration checksum mismatch",
        )?;
    }
    let owner: String = sqlx::query_scalar("SELECT value FROM settings WHERE key='owner'")
        .fetch_one(&pool)
        .await?;
    require(owner == live_owner, "backup owner mismatch")?;
    let bound: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='backup_key_fingerprint'")
            .fetch_one(&pool)
            .await?;
    require(bound == fingerprint, "backup key fingerprint mismatch")?;
    if let Some(restore) = &manifest.restore {
        require(restore.committed, "restore source is incomplete")?;
        sqlx::query("INSERT INTO settings(key,value) VALUES('restore_generation',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(&restore.generation).execute(&pool).await?;
    }
    let guard = super::MemoryGuard::new(&database)?;
    guard.write(manifest)?;
    let candidate = Store {
        schedule_wake: tokio::sync::watch::channel(0).0,
        pool,
        memory_guard: Arc::new(guard),
    };
    candidate.initialize_memory().await?;
    candidate.list_memories(now).await?;
    if already_fenced {
        let active: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM link_device d WHERE revoked!=1 OR stopped!=1 OR lease_until!=0 OR NOT EXISTS(SELECT 1 FROM settings s WHERE s.key='restore_retired_device:'||d.id))+(SELECT count(*) FROM tasks t WHERE NOT EXISTS(SELECT 1 FROM settings s WHERE s.key='restore_retired_task:'||t.id))+(SELECT count(*) FROM schedules WHERE json_extract(data,'$.state')='armed')+(SELECT count(*) FROM events WHERE outcome IS NULL)")
            .fetch_one(&candidate.pool).await?;
        require(active == 0, "paused image still carries work authority")?;
    } else {
        candidate.recover(versions, now).await?;
        dispatch::fence(&candidate).await?;
    }
    Ok(candidate)
}

fn managed_name(name: &str) -> Option<i64> {
    let stem = name.strip_suffix(".pulsebak")?;
    let (date, id) = stem.split_once('-')?;
    let date: i64 = date.parse().ok()?;
    (date >= 0 && uuid::Uuid::parse_str(id).ok()?.to_string() == id).then_some(date)
}

fn manage_files(
    guard: &MemoryGuard,
    directory: &Path,
    last: Option<&LastBackup>,
    secret: &[u8; 32],
    keeper: &str,
    now: i64,
) -> Result<(usize, i64, bool)> {
    let mut removed = 0;
    let mut next = i64::MAX;
    let mut files = 0;
    let mut published = 0;
    let mut current = false;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        files += 1;
        require(files <= 128, "backup directory capacity exceeded")?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name.strip_suffix(".partial") {
            require(
                uuid::Uuid::parse_str(id).is_ok(),
                "unknown backup staging file",
            )?;
            let file = guard.open_private(&entry.path(), false, false)?;
            drop(file);
            std::fs::remove_file(entry.path())?;
            continue;
        }
        let date = managed_name(&name)
            .ok_or_else(|| Error::Rejected("unknown file in managed backup directory".into()))?;
        let mut file = guard.open_private(&entry.path(), false, false)?;
        require(
            file.metadata()?.len() <= crypto::MAX_DATABASE as u64 + 64 * 1024,
            "encrypted backup capacity",
        )?;
        let header = crypto::authenticate_header(secret, &mut file)?;
        require(
            header.keeper == keeper
                && header.created_at == date
                && name == format!("{}-{}.pulsebak", header.created_at, header.id),
            "backup filename or keeper mismatch",
        )?;
        published += 1;
        if now.checked_sub(date).is_some_and(|age| age >= RETENTION) {
            drop(file);
            std::fs::remove_file(entry.path())?;
            removed += 1;
            continue;
        }
        require(date <= now, "backup clock moved backwards")?;
        next = next.min(date.saturating_add(RETENTION));
        if last.is_some_and(|last| {
            last.filename == name && last.created_at == date && date / DAY == now / DAY
        }) {
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(0))?;
            let mut hasher = Sha256::new();
            let mut buffer = [0; 64 * 1024];
            let mut reader = file.take(crypto::MAX_DATABASE as u64 + 64 * 1024 + 1);
            loop {
                let n = reader.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            current = last.is_some_and(|last| last.digest == format!("{:x}", hasher.finalize()));
        }
    }
    // A finite cap also covers repeated crashes before the daily checkpoint.
    require(
        published - removed < 64,
        "backup directory has no publication capacity",
    )?;
    File::open(directory)?.sync_all()?;
    Ok((removed, next, current))
}
