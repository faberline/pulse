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
            clear_view(&mut tx).await?;
        }
        let ready: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='image_erasure_ready'")
                .fetch_optional(&mut *tx)
                .await?;
        let pending: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='image_erasure_pending'")
                .fetch_optional(&mut *tx)
                .await?;
        let sourced:i64=sqlx::query_scalar("SELECT count(*) FROM local_meta WHERE key GLOB 'image:*' AND json_array_length(value,'$.memory_sources')>0")
                .fetch_one(&mut *tx).await?;
        if sourced == 0 || (ready.as_deref() == Some(&header.sequence) && pending.is_none()) {
            // One sync authorizes one ACK attempt. An older ACK cannot certify a later withdrawal.
            sqlx::query("DELETE FROM local_meta WHERE key IN ('image_erasure_pending','image_erasure_ready')")
                .execute(&mut *tx).await?;
        } else if pending.as_deref() != Some(&header.sequence) {
            crate::memory_chat::journal::dirty(&mut tx).await?;
            sqlx::query("INSERT INTO local_meta VALUES('image_erasure_pending',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                .bind(&header.sequence).execute(&mut *tx).await?;
            sqlx::query("DELETE FROM local_meta WHERE key='image_erasure_ready'")
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        // A lost ACK redoes this physical checkpoint even after the logical row
        // was removed. Never acknowledge erasure based only on the cursor.
        if let Err(error) = self.flush_memory().await {
            self.memory_denied
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error);
        }
        let pending: i64 =
            sqlx::query_scalar("SELECT count(*) FROM local_meta WHERE key='image_erasure_pending'")
                .fetch_one(&self.db)
                .await?;
        ensure(pending == 0, "image erasure awaits a fresh source sync")?;
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
    sqlx::query("UPDATE local_meta SET value=json_object('erased',json('true'),'file',json_extract(value,'$.file'),'writing',coalesce(json_extract(value,'$.writing'),0)) WHERE key GLOB 'image:*' AND json_array_length(value,'$.memory_sources')>0").execute(&mut *db).await?;
    clear_view(db).await
}
async fn clear_view(db: &mut SqliteConnection) -> Result<()> {
    let count=sqlx::query("DELETE FROM local_meta WHERE key='cache' AND json_array_length(value,'$.memory_sources')>0").execute(&mut *db).await?.rows_affected();
    if count > 0 {
        sqlx::query("DELETE FROM local_meta WHERE key='cache_sequence'")
            .execute(db)
            .await?;
    }
    Ok(())
}
