//! Cooperative database-use admission. A lease does not restore data or grant work.
use super::*;
use std::{fs::File, path::PathBuf, sync::Arc};

struct Lease {
    file: File,
    database: PathBuf,
}
impl Drop for Lease {
    fn drop(&mut self) {
        // Release the lock itself, including descriptions inherited before exec.
        let _ = self.file.unlock();
    }
}

/// Exclusive admission for a future offline maintenance operation.
/// This performs no database replacement, recovery, migration or execution.
/// Every Store, derived pool and detached connection must be closed and dropped first.
/// This is cooperative admission; it does not constrain arbitrary raw SQLite users.
pub struct DatabaseMaintenance {
    lease: Lease,
}
impl DatabaseMaintenance {
    pub fn try_acquire(database: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            lease: acquire(database.as_ref(), true)?,
        })
    }
    pub fn database(&self) -> &Path {
        &self.lease.database
    }
}

#[cfg(unix)]
pub(super) fn validate_database(path: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => require(
            metadata.is_file()
                && metadata.nlink() == 1
                && metadata.uid() == rustix::process::geteuid().as_raw()
                && metadata.permissions().mode() & 0o077 == 0,
            "database must be a private owned regular file without links",
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn acquire(database: &Path, exclusive: bool) -> Result<Lease> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
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
        metadata.is_dir()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.permissions().mode() & 0o022 == 0,
        "database directory must be owned and not writable by other users",
    )?;
    let database = parent.join(name);
    validate_database(&database)?;
    let mut lock = database.as_os_str().to_owned();
    lock.push(".use-lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(PathBuf::from(lock))?;
    let metadata = file.metadata()?;
    require(
        metadata.is_file()
            && metadata.nlink() == 1
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.permissions().mode() & 0o777 == 0o600
            && metadata.len() == 0,
        "database use lock must be an empty private owned regular file without links",
    )?;
    let result = if exclusive {
        file.try_lock()
    } else {
        file.try_lock_shared()
    };
    match result {
        Ok(()) => Ok(Lease { file, database }),
        Err(std::fs::TryLockError::WouldBlock) => Err(Error::Rejected(
            "database is in use; close active pools before maintenance".into(),
        )),
        Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn acquire(_: &Path, _: bool) -> Result<Lease> {
    Err(Error::Rejected(
        "database-use admission requires Unix permissions".into(),
    ))
}
#[cfg(not(unix))]
pub(super) fn validate_database(_: &Path) -> Result<()> {
    Err(Error::Rejected(
        "database-use admission requires Unix permissions".into(),
    ))
}

pub(super) fn shared_pool(database: &Path) -> Result<SqlitePoolOptions> {
    let lease = Arc::new(acquire(database, false)?);
    // PoolOptions is retained by SQLx's pool inner state. Keep the shared lease
    // there, so an exported SqlitePool cannot outlive admission protection.
    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            let lease = lease.clone();
            Box::pin(async move {
                // A detached connection can outlive its pool. SQLite retains
                // this unused collation until the actual handle closes, so it
                // also retains admission through SQLx's worker-thread shutdown.
                connection.lock_handle().await?.create_collation(
                    "pulse_database_use_lease",
                    move |left, right| {
                        let _keep_alive = &lease;
                        left.cmp(right)
                    },
                )?;
                Ok(())
            })
        }))
}
