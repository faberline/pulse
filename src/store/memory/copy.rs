//! Bodyless local delivery restrictions, kept with the current external keeper.
//! This is not a credential store. Fingerprints never grant server authority.
use super::*;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CopyFence {
    device: String,
    server: String,
    pub(super) generation: u64,
    pub(super) serial: u64,
    pub(super) denied: bool,
    credential: Option<String>,
    pending: Option<String>,
    preserve_bootstrap: bool,
}

fn fingerprint(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl CopyFence {
    pub(super) fn valid(&self) -> bool {
        revocations::canonical(&self.device).is_ok_and(|v| v == self.device)
            && fingerprint(&self.server)
            && self.generation <= i64::MAX as u64
            && (1..=i64::MAX as u64).contains(&self.serial)
            && self.credential.as_deref().is_none_or(fingerprint)
            && self.pending.as_deref().is_none_or(fingerprint)
            && (self.pending.is_none() || self.denied)
            && (self.denied || self.credential.is_some() || self.generation == 0)
            && (!self.denied || self.generation > 0 || self.preserve_bootstrap)
            && (!self.preserve_bootstrap
                || (self.generation == 0 && self.credential.is_none() && self.pending.is_some()))
    }
    fn advance(&mut self) -> Result<()> {
        self.generation = self
            .generation
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| Error::Rejected("memory copy generation exhausted".into()))?;
        Ok(())
    }
    pub(super) fn active(&self, hash: &str) -> bool {
        !self.denied && self.pending.is_none() && self.credential.as_deref() == Some(hash)
    }
    pub(super) fn bound(&self, device: &str, server: &str) -> bool {
        self.device == device && self.server == server
    }
}

/// Trusted local journal lifecycle only. Inputs are hashes, never credentials.
pub enum MemoryCopyChange<'a> {
    Open,
    Bind(&'a str),
    Prepare(&'a str),
    Activate(&'a str),
    Deny,
}

impl Store {
    /// Reconcile a chat journal with the current external copy fence. Return
    /// whether this identity can use memory. Old database generations lose their
    /// queued bodies before any caller can reconnect. Keep the current keeper
    /// file when restoring; rolling back both files remains unsupported.
    pub async fn memory_copy(
        &self,
        device: &str,
        server: &str,
        change: MemoryCopyChange<'_>,
    ) -> Result<bool> {
        require(
            revocations::canonical(device).is_ok_and(|v| v == device) && fingerprint(server),
            "invalid memory copy binding",
        )?;
        if let MemoryCopyChange::Bind(hash)
        | MemoryCopyChange::Prepare(hash)
        | MemoryCopyChange::Activate(hash) = &change
        {
            require(fingerprint(hash), "invalid credential fingerprint")?;
        }
        let _lease = self.memory_guard.lease().await?;
        let mut manifest = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &manifest).await?;
        let role: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='role'")
            .fetch_one(&mut *tx)
            .await?;
        require(role == "chat", "memory copy requires chat journal")?;
        let actual_device: String =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='device'")
                .fetch_one(&mut *tx)
                .await?;
        let actual_server: String =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='server'")
                .fetch_one(&mut *tx)
                .await?;
        require(
            actual_device == device && digest(&actual_server)? == server,
            "memory copy journal binding mismatch",
        )?;
        let row = sqlx::query(
            "SELECT denied,generation,serial FROM local_memory_access WHERE singleton=1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let db_generation = row.get::<i64, _>("generation") as u64;
        let db_serial = row.get::<i64, _>("serial") as u64;
        let created = manifest.copy.is_none();
        if manifest.copy.is_none() {
            require(
                db_generation == 0 && db_serial == 0,
                "memory copy restrictions missing",
            )?;
            let denied = row.get::<i64, _>("denied") != 0;
            manifest.copy = Some(CopyFence {
                device: device.into(),
                server: server.into(),
                generation: u64::from(denied),
                serial: 0,
                denied,
                credential: None,
                pending: None,
                preserve_bootstrap: false,
            });
        }
        let copy = manifest.copy.as_mut().unwrap();
        require(
            copy.device == device && copy.server == server,
            "memory copy binding changed",
        )?;
        require(
            db_generation <= copy.generation && db_serial <= copy.serial,
            "memory copy restrictions are stale",
        )?;
        let mut mismatch = false;
        let prior = copy.clone();
        let opening = matches!(change, MemoryCopyChange::Open);
        match change {
            MemoryCopyChange::Open => {}
            MemoryCopyChange::Bind(hash) => {
                if copy.pending.as_deref() == Some(hash) {
                    copy.credential = Some(hash.into());
                    copy.pending = None;
                    copy.denied = false;
                    copy.preserve_bootstrap = false;
                } else if copy.active(hash) {
                } else if copy.credential.is_none()
                    && !copy.denied
                    && copy.pending.is_none()
                    && copy.generation == 0
                {
                    copy.credential = Some(hash.into());
                } else {
                    mismatch = true;
                }
            }
            MemoryCopyChange::Prepare(hash) => {
                if !copy.active(hash) && copy.pending.as_deref() != Some(hash) {
                    let bootstrap = copy.credential.is_none()
                        && copy.generation == 0
                        && (!copy.denied || copy.preserve_bootstrap);
                    if !bootstrap {
                        copy.advance()?;
                    }
                    copy.pending = Some(hash.into());
                    copy.denied = true;
                    copy.preserve_bootstrap = bootstrap;
                }
            }
            MemoryCopyChange::Activate(hash) => {
                require(
                    copy.pending.as_deref() == Some(hash) || copy.active(hash),
                    "pairing was not prepared",
                )?;
                copy.credential = Some(hash.into());
                copy.pending = None;
                copy.denied = false;
                copy.preserve_bootstrap = false;
            }
            MemoryCopyChange::Deny => {
                if !copy.denied || copy.pending.is_some() {
                    copy.advance()?;
                }
                copy.denied = true;
                copy.pending = None;
                copy.preserve_bootstrap = false;
            }
        }
        if created || *copy != prior {
            copy.serial = copy
                .serial
                .checked_add(1)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or_else(|| Error::Rejected("memory copy serial exhausted".into()))?;
        }
        let generation = copy.generation;
        let serial = copy.serial;
        let allowed = !copy.denied && !mismatch && (!opening || row.get::<i64, _>("denied") == 0);
        let clear = db_generation != generation || (copy.denied && !copy.preserve_bootstrap);
        let imported = super::withdrawals::import(&mut tx, &mut manifest).await?;
        if created || imported || manifest.copy.as_ref().unwrap() != &prior {
            self.memory_guard.write(&manifest)?; // Durable restriction precedes SQLite.
        }
        if clear || !allowed {
            sqlx::query("DELETE FROM local_memory_cache")
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM local_meta WHERE key IN ('memory_dirty','memory_synced_at')")
                .execute(&mut *tx)
                .await?;
        }
        if clear {
            // Human withdrawal is bodyless and must survive credential rotation.
            sqlx::query("UPDATE local_memory_queue SET request=NULL,phase='access_denied' WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') NOT IN ('forget','revoke')").execute(&mut *tx).await?;
        }
        sqlx::query(
            "UPDATE local_memory_access SET denied=?,generation=?,serial=? WHERE singleton=1",
        )
        .bind(!allowed)
        .bind(generation as i64)
        .bind(serial as i64)
        .execute(&mut *tx)
        .await?;
        super::withdrawals::replay(&mut tx, &manifest).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        Ok(allowed)
    }
}
