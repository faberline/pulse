//! Owned-runtime cleanup evidence. A transport ACK never supplies this evidence.
use crate::{Result, ensure, executor::Executor, journal::Journal, protocol::*, server::Hub};
use pulse_core::{ErasedProjection, WorkerErasure};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCleanup {
    pub version: u8,
    pub context: WorkerErasure,
    pub control: ErasedProjection,
}
impl WorkerCleanup {
    fn valid(&self) -> bool {
        self.version == 1
            && self.context.valid()
            && self.control.valid()
            && self.control.device == self.context.device
    }
}

// Unknown directories, symlinks and I/O failures are uncertainty, never a clean start.
pub(crate) fn resources_absent(path: &Path, computer: bool) -> bool {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(e) => return e.kind() == std::io::ErrorKind::NotFound,
    };
    if !metadata.is_dir() {
        return false;
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return false;
    };
    for entry in entries.take(2) {
        let Ok(entry) = entry else { return false };
        if !computer || entry.file_name() != "artifacts" || !resources_absent(&entry.path(), false)
        {
            return false;
        }
    }
    true
}
impl Journal {
    pub(crate) async fn begin_worker_runtime(
        &self,
        executor: &Executor,
        directory: &Path,
    ) -> Result<()> {
        let absent = resources_absent(&directory.join("browser"), false)
            && resources_absent(&directory.join("computer"), true)
            && executor.prior_resources_absent();
        self.core
            .begin_worker_runtime(
                &self.device,
                &hash(&self.server)?,
                &executor.runtime_id,
                absent,
            )
            .await?;
        Ok(())
    }
    pub(crate) async fn close_worker_runtime(&self, executor: &Executor) -> Result<()> {
        self.core
            .close_worker_runtime(&self.device, &hash(&self.server)?, &executor.runtime_id)
            .await?;
        Ok(())
    }
    pub(crate) async fn complete_worker_cleanup(
        &self,
        executor: &Executor,
        header: &ErasedProjection,
        context: &WorkerErasure,
        supported: bool,
    ) -> Result<()> {
        executor.invalidate_source(context).await?;
        if !supported {
            return Ok(());
        }
        let _lifecycle = self.memory_lifecycle.lock().await;
        let receipt = WorkerCleanup {
            version: 1,
            context: context.clone(),
            control: header.clone(),
        };
        ensure(
            self.role == ClientRole::LocalExecutor
                && receipt.valid()
                && context.device == self.device,
            "invalid cleanup identity",
        )?;
        ensure(
            self.core
                .worker_task_erased(
                    &self.device,
                    &hash(&self.server)?,
                    &context.task_id,
                    &context.revision,
                )
                .await?,
            "cleanup source not fenced",
        )?;
        if !self
            .core
            .worker_runtime_verified(&self.device, &hash(&self.server)?, &executor.runtime_id)
            .await?
        {
            return Ok(());
        }
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT id,digest FROM local_inbox WHERE sequence=?")
            .bind(number(&header.sequence)? as i64)
            .fetch_one(&mut *tx)
            .await?;
        ensure(
            row.get::<String, _>("id") == header.id
                && row.get::<String, _>("digest") == header.binding_digest,
            "cleanup control was not accepted",
        )?;
        if let Some(data) = sqlx::query_scalar::<_, String>(
            "SELECT receipt FROM local_worker_cleanup WHERE operation_id=?",
        )
        .bind(&context.operation_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            let old: WorkerCleanup = serde_json::from_str(&data)?;
            ensure(old.context == *context, "cleanup intent conflict")?;
        } else {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM local_worker_cleanup")
                .fetch_one(&mut *tx)
                .await?;
            if count >= 1000 {
                return Err(crate::Error::Capacity);
            }
            sqlx::query("INSERT INTO local_worker_cleanup(operation_id,receipt) VALUES(?,?)")
                .bind(&context.operation_id)
                .bind(serde_json::to_string(&receipt)?)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub(crate) async fn flush_worker_cleanup(&self) -> Result<()> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let receipts: Vec<(String,String)> = sqlx::query_as("SELECT operation_id,receipt FROM local_worker_cleanup WHERE sequence IS NULL ORDER BY operation_id LIMIT 8").fetch_all(&mut *tx).await?;
        for (id, data) in receipts {
            let receipt: WorkerCleanup = serde_json::from_str(&data)?;
            ensure(
                receipt.valid() && receipt.context.device == self.device,
                "invalid persisted cleanup",
            )?;
            match self
                .enqueue_in(
                    &mut tx,
                    DevicePayload::WorkerCleanup {
                        receipt: Box::new(receipt.clone()),
                    },
                    number(&receipt.control.epoch)?,
                )
                .await
            {
                Ok(()) => {
                    let sequence: String =
                        sqlx::query_scalar("SELECT value FROM local_meta WHERE key='next'")
                            .fetch_one(&mut *tx)
                            .await?;
                    sqlx::query("UPDATE local_worker_cleanup SET sequence=? WHERE operation_id=?")
                        .bind(number(&sequence)? as i64)
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                }
                Err(crate::Error::Capacity) => break,
                Err(error) => return Err(error),
            }
        }
        tx.commit().await?;
        Ok(())
    }
}
impl Hub {
    pub async fn worker_cleanup_ready(
        &self,
        device: &str,
        epoch: u64,
        version: u8,
        time: i64,
    ) -> Result<()> {
        ensure(version == 1, "unsupported worker cleanup version")?;
        let count = sqlx::query("UPDATE link_device SET worker_cleanup=1 WHERE id=? AND role='local_executor' AND epoch=? AND revoked=0 AND worker_erasure=1 AND lease_until>?").bind(device).bind(epoch as i64).bind(time).execute(&self.db).await?.rows_affected();
        ensure(count == 1, "worker cleanup requires current local lease")
    }
    pub(crate) async fn receive_worker_cleanup(
        &self,
        device: &str,
        envelope: &Envelope<DevicePayload>,
        receipt: &WorkerCleanup,
        time: i64,
    ) -> Result<()> {
        let (sequence, _) = envelope.validate()?;
        ensure(
            receipt.valid()
                && receipt.context.device == device
                && envelope.device == device
                && envelope.epoch == receipt.control.epoch,
            "invalid worker cleanup binding",
        )?;
        let _source = self
            .core
            .lease_read_source_state(&receipt.context.operation_id, time)
            .await?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT e.context,d.in_cursor FROM memory_erased_reads e JOIN link_reads r ON r.id=e.id JOIN link_device d ON d.id=r.device WHERE e.id=? AND r.device=? AND d.role='local_executor' AND d.revoked=0 AND d.worker_cleanup=1 AND d.worker_erasure=1 AND d.lease_until>?").bind(&receipt.context.operation_id).bind(device).bind(time).fetch_one(&mut *tx).await?;
        let expected: WorkerErasure = serde_json::from_str(&row.get::<String, _>("context"))?;
        ensure(
            expected == receipt.context,
            "worker cleanup source conflict",
        )?;
        if expected.read_header.as_ref() != Some(&receipt.control) {
            let data: String = sqlx::query_scalar("SELECT o.data FROM link_worker_fences f JOIN link_outbox o ON o.device=f.device AND o.sequence=f.sequence WHERE f.operation_id=? AND f.device=? AND f.sequence=?").bind(&expected.operation_id).bind(device).bind(number(&receipt.control.sequence)? as i64).fetch_one(&mut *tx).await?;
            let control: Envelope<ServerPayload> = parse(data.as_bytes())?;
            ensure(
                matches!(&control.payload, ServerPayload::EraseWork { context } if *context == expected)
                    && ErasedProjection::from_value(&serde_json::to_value(control)?)?
                        == receipt.control,
                "worker cleanup control conflict",
            )?;
        }
        let cursor: i64 = row.get("in_cursor");
        if let Some(prior) =
            sqlx::query("SELECT id,digest FROM link_inbox WHERE device=? AND sequence=?")
                .bind(device)
                .bind(sequence as i64)
                .fetch_optional(&mut *tx)
                .await?
        {
            ensure(
                prior.get::<String, _>("id") == envelope.id
                    && prior.get::<String, _>("digest") == envelope.binding_digest,
                "worker cleanup replay conflict",
            )?;
        } else {
            ensure(sequence == cursor as u64 + 1, "worker cleanup sequence gap")?;
            sqlx::query("INSERT INTO link_inbox(device,sequence,id,digest,outcome) VALUES(?,?,?,?,'owned_runtime_cleanup')").bind(device).bind(sequence as i64).bind(&envelope.id).bind(&envelope.binding_digest).execute(&mut *tx).await?;
        }
        sqlx::query("INSERT OR IGNORE INTO link_worker_cleanup(operation_id,receipt,received_at) VALUES(?,?,?)").bind(&expected.operation_id).bind(serde_json::to_string(receipt)?).bind(time).execute(&mut *tx).await?;
        if sequence > cursor as u64 {
            ensure(sequence == cursor as u64 + 1, "worker cleanup sequence gap")?;
            sqlx::query("UPDATE link_device SET in_cursor=? WHERE id=?")
                .bind(sequence as i64)
                .bind(device)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    /// Metadata only. This does not settle an operation outcome or a budget hold.
    pub async fn worker_cleanup_verified(&self, operation_id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM link_worker_cleanup WHERE operation_id=?",
        )
        .bind(operation_id)
        .fetch_one(&self.db)
        .await?
            == 1)
    }
}
