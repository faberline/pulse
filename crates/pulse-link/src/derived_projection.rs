//! Rust-owned source checks and bodyless erasure for coalescible chat copies.
use crate::{Result, ensure, journal::Journal, protocol::*};
use pulse_core::{ErasedProjection, TaskMemorySource};
use sqlx::{Row, SqliteConnection};

impl Journal {
    pub(crate) async fn projection_token(&self) -> Result<Option<String>> {
        if self.role != ClientRole::Chat || !self.memory_access_allowed().await? {
            return Ok(None);
        }
        let keeper: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='memory_keeper'")
                .fetch_optional(&self.db)
                .await?;
        let epoch: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='memory_epoch'")
                .fetch_optional(&self.db)
                .await?;
        keeper.zip(epoch).map(|pair| hash(&pair)).transpose()
    }
    pub async fn accept_erased_projection(&self, header: &ErasedProjection) -> Result<bool> {
        ensure(
            self.role == ClientRole::Chat && header.device == self.device && header.valid(),
            "invalid projection erasure or role",
        )?;
        let _lifecycle = self.memory_lifecycle.lock().await;
        self.accept_erased_projection_locked(header).await
    }
    pub(crate) async fn accept_erased_projection_locked(
        &self,
        header: &ErasedProjection,
    ) -> Result<bool> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let cursor: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cursor'")
            .fetch_one(&mut *tx)
            .await?;
        let sequence = number(&header.sequence)?;
        let cursor = number(&cursor)?;
        let fresh = sequence > cursor;
        if !fresh {
            let row = sqlx::query("SELECT id,digest FROM local_inbox WHERE sequence=?")
                .bind(sequence as i64)
                .fetch_one(&mut *tx)
                .await?;
            ensure(
                row.get::<String, _>("id") == header.id
                    && row.get::<String, _>("digest") == header.binding_digest,
                "erasure replay conflict",
            )?;
        } else {
            ensure(sequence == cursor + 1, "erasure sequence gap")?;
            sqlx::query("INSERT INTO local_inbox VALUES(?,?,?)")
                .bind(sequence as i64)
                .bind(&header.id)
                .bind(&header.binding_digest)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE local_meta SET value=? WHERE key='cursor'")
                .bind(&header.sequence)
                .execute(&mut *tx)
                .await?;
        }
        let cached: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cache_sequence'")
                .fetch_optional(&mut *tx)
                .await?;
        if cached
            .map(|s| number(&s))
            .transpose()?
            .is_none_or(|s| s <= sequence)
        {
            clear(&mut tx).await?;
        }
        tx.commit().await?;
        // A lost ACK redoes this physical checkpoint even after the logical row
        // was removed. Never acknowledge erasure based only on the cursor.
        if let Err(error) = self.flush_memory().await {
            self.memory_denied
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error);
        }
        Ok(fresh)
    }
    pub(crate) async fn projection_sources_allowed(
        &self,
        sources: &[TaskMemorySource],
        now: i64,
    ) -> Result<bool> {
        if self.role != ClientRole::Chat
            || sources.is_empty()
            || sources.len() > 100
            || !self.memory_access_allowed().await?
        {
            return Ok(false);
        }
        match self
            .core
            .lease_local_projection_sources(&self.device, &hash(&self.server)?, sources, now)
            .await
        {
            Ok(_lease) => Ok(true),
            Err(pulse_core::Error::Rejected(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}
pub(crate) async fn clear(db: &mut SqliteConnection) -> Result<()> {
    let count=sqlx::query("DELETE FROM local_meta WHERE key='cache' AND json_array_length(value,'$.memory_sources')>0").execute(&mut *db).await?.rows_affected();
    if count > 0 {
        sqlx::query("DELETE FROM local_meta WHERE key='cache_sequence'")
            .execute(db)
            .await?;
    }
    Ok(())
}
