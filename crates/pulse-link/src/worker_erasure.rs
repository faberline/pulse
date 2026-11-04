//! Worker journal erasure and bodyless retry transport. No execution receipt.
use crate::{Result, ensure, journal::Journal, protocol::*};
use pulse_core::{ErasedProjection, WorkerErasure};
use sqlx::Row;
pub struct ErasedOutput {
    pub header: ErasedProjection,
    pub operation_id: String,
    pub source_id: String,
}
pub enum DeviceDelivery {
    Data(Box<Envelope<DevicePayload>>),
    Erased(Box<ErasedOutput>),
}
#[cfg(test)]
#[path = "worker_erasure_tests.rs"]
mod tests;
impl Journal {
    pub(crate) async fn ensure_worker_access(&self) -> Result<()> {
        if self.role == ClientRole::LocalExecutor
            && self
                .core
                .worker_access_denied(&self.device, &hash(&self.server)?)
                .await?
        {
            return Err(crate::Error::CredentialRejected);
        }
        Ok(())
    }
    pub(crate) async fn deny_worker_access(&self) -> Result<()> {
        ensure(
            self.role == ClientRole::LocalExecutor,
            "worker denial requires local role",
        )?;
        let _lifecycle = self.memory_lifecycle.lock().await;
        self.core
            .deny_worker_access(&self.device, &hash(&self.server)?)
            .await?;
        self.flush_memory().await
    }
    pub(crate) async fn replay_worker_erasure_rules(&self) -> Result<()> {
        if self.role == ClientRole::LocalExecutor {
            self.core
                .replay_worker_erasures(&self.device, &hash(&self.server)?, false)
                .await?;
            self.ensure_worker_access().await?;
        }
        Ok(())
    }
    pub async fn work_erased(&self, operation_id: &str) -> Result<bool> {
        let row = sqlx::query("SELECT state,envelope FROM local_work WHERE id=?")
            .bind(operation_id)
            .fetch_optional(&self.db)
            .await?;
        let Some(row) = row else { return Ok(false) };
        if matches!(row.get::<String, _>("state").as_str(), "erasing" | "erased") {
            return Ok(true);
        }
        let envelope: Envelope<ServerPayload> = parse(row.get::<String, _>("envelope").as_bytes())?;
        if let ServerPayload::Read {
            task_id, revision, ..
        } = envelope.payload
        {
            return Ok(self
                .core
                .worker_task_erased(&self.device, &hash(&self.server)?, &task_id, &revision)
                .await?);
        }
        Err(crate::Error::Rejected("invalid local work".into()))
    }
    pub async fn accept_worker_erasure(
        &self,
        header: &ErasedProjection,
        context: &WorkerErasure,
    ) -> Result<bool> {
        ensure(
            context.read_header.as_ref() == Some(header),
            "erased read header mismatch",
        )?;
        self.accept_erasure_control(header, context).await
    }
    pub(crate) async fn accept_erasure_control(
        &self,
        header: &ErasedProjection,
        context: &WorkerErasure,
    ) -> Result<bool> {
        ensure(
            self.role == ClientRole::LocalExecutor
                && header.valid()
                && header.device == self.device
                && context.valid()
                && context.device == self.device,
            "invalid erased work identity or role",
        )?;
        let _lifecycle = self.memory_lifecycle.lock().await;
        let sequence = number(&header.sequence)?;
        let cursor = self.cursor().await?;
        if sequence <= cursor {
            let row = sqlx::query("SELECT id,digest FROM local_inbox WHERE sequence=?")
                .bind(sequence as i64)
                .fetch_one(&self.db)
                .await?;
            ensure(
                row.get::<String, _>("id") == header.id
                    && row.get::<String, _>("digest") == header.binding_digest,
                "worker erasure replay conflict",
            )?;
        } else {
            ensure(sequence == cursor + 1, "worker erasure sequence gap")?;
        }
        self.core
            .record_worker_erasure(context, &hash(&self.server)?)
            .await?;
        if sequence > cursor {
            let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
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
            tx.commit().await?;
        }
        self.flush_memory().await?;
        Ok(sequence > cursor)
    }
    pub async fn deliveries(&self) -> Result<Vec<DeviceDelivery>> {
        self.replay_worker_erasure_rules().await?;
        self.flush_worker_cleanup().await?;
        let rows=sqlx::query("SELECT o.data,e.header,e.operation_id,e.source_id FROM local_outbox o LEFT JOIN local_erased_outbox e ON e.sequence=o.sequence ORDER BY o.sequence LIMIT 100").fetch_all(&self.db).await?;
        let mut output = Vec::new();
        for row in rows {
            if let Some(data) = row.get::<Option<String>, _>("header") {
                let header: ErasedProjection = serde_json::from_str(&data)?;
                ensure(
                    header.valid() && header.device == self.device,
                    "invalid erased output header",
                )?;
                output.push(DeviceDelivery::Erased(Box::new(ErasedOutput {
                    header,
                    operation_id: row.get("operation_id"),
                    source_id: row.get("source_id"),
                })));
            } else {
                output.push(DeviceDelivery::Data(Box::new(parse(
                    row.get::<String, _>("data").as_bytes(),
                )?)));
            }
        }
        Ok(output)
    }
}
