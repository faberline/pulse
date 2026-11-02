//! Durable bodyless worker restrictions, outside SQLite restores. No work grant.
use super::*;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerCopy {
    device: String,
    server: String,
    groups: BTreeMap<String, (String, String)>,
}
impl WorkerCopy {
    pub(super) fn valid(&self) -> bool {
        !self.device.is_empty()
            && self.device.len() <= 128
            && self.server.len() == 64
            && self.server.bytes().all(|b| b.is_ascii_hexdigit())
            && self.groups.len() <= 1000
            && self.groups.iter().all(|(id, (task, rev))| {
                uuid::Uuid::parse_str(id).is_ok()
                    && !task.is_empty()
                    && task.len() <= 128
                    && rev
                        .parse::<u64>()
                        .is_ok_and(|n| n > 0 && n <= i64::MAX as u64 && n.to_string() == *rev)
            })
    }
}
async fn binding(db: &mut SqliteConnection, device: &str, server: &str) -> Result<()> {
    let values: Vec<(String, String)> =
        sqlx::query_as("SELECT key,value FROM local_meta WHERE key IN ('device','server','role')")
            .fetch_all(db)
            .await?;
    let values: BTreeMap<_, _> = values.into_iter().collect();
    require(
        values.get("device").is_some_and(|v| v == device)
            && values
                .get("server")
                .is_some_and(|v| digest(v).is_ok_and(|h| h == server))
            && values.get("role").is_some_and(|v| v == "local_executor"),
        "worker restriction binding mismatch",
    )
}
async fn replay(db: &mut SqliteConnection, copy: &WorkerCopy, opening: bool) -> Result<()> {
    for (source, (task, revision)) in &copy.groups {
        sqlx::query("UPDATE local_work SET envelope='null',state=CASE WHEN state='running' AND ?=0 THEN 'erasing' ELSE 'erased' END WHERE json_extract(envelope,'$.payload.task_id')=? AND json_extract(envelope,'$.payload.revision')=?").bind(opening).bind(task).bind(revision).execute(&mut *db).await?;
        let rows:Vec<(i64,String)>=sqlx::query_as("SELECT sequence,data FROM local_outbox WHERE json_extract(data,'$.payload.kind') IN ('result','artifact_chunk') AND ((json_extract(data,'$.payload.task_id')=? AND json_extract(data,'$.payload.revision')=?) OR (json_extract(data,'$.payload.artifact.task_id')=? AND json_extract(data,'$.payload.artifact.revision')=?))").bind(task).bind(revision).bind(task).bind(revision).fetch_all(&mut *db).await?;
        for (sequence, data) in rows {
            let value: serde_json::Value = serde_json::from_str(&data)?;
            let header = crate::ErasedProjection::from_value(&value)?;
            require(
                header.device == copy.device,
                "erased outbox device mismatch",
            )?;
            let operation = value["payload"]["operation_id"]
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 256)
                .ok_or_else(|| Error::Rejected("erased outbox operation missing".into()))?;
            sqlx::query("INSERT OR IGNORE INTO local_erased_outbox VALUES(?,?,?,?)")
                .bind(sequence)
                .bind(operation)
                .bind(source)
                .bind(serde_json::to_string(&header)?)
                .execute(&mut *db)
                .await?;
            sqlx::query("UPDATE local_outbox SET data='null' WHERE sequence=?")
                .bind(sequence)
                .execute(&mut *db)
                .await?;
        }
    }
    if opening {
        sqlx::query("UPDATE local_work SET state='erased' WHERE state='erasing'")
            .execute(db)
            .await?;
    }
    Ok(())
}
impl Store {
    pub async fn record_worker_erasure(
        &self,
        context: &crate::WorkerErasure,
        server: &str,
    ) -> Result<()> {
        require(context.valid(), "invalid worker erasure")?;
        let _lease = self.memory_guard.lease().await?;
        let mut value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("worker restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        binding(&mut tx, &context.device, server).await?;
        let copy = value.worker.get_or_insert_with(|| WorkerCopy {
            device: context.device.clone(),
            server: server.into(),
            groups: BTreeMap::new(),
        });
        require(
            copy.device == context.device && copy.server == server,
            "worker keeper changed",
        )?;
        if let Some(pair) = copy.groups.get(&context.source_id) {
            require(
                pair == &(context.task_id.clone(), context.revision.clone()),
                "worker source identity conflict",
            )?;
        }
        copy.groups.insert(
            context.source_id.clone(),
            (context.task_id.clone(), context.revision.clone()),
        );
        require(copy.valid(), "worker restriction capacity or identity")?;
        value.format = 5;
        self.memory_guard.write(&value)?;
        replay(&mut tx, value.worker.as_ref().unwrap(), false).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await
    }
    pub async fn replay_worker_erasures(
        &self,
        device: &str,
        server: &str,
        opening: bool,
    ) -> Result<()> {
        let _lease = self.memory_guard.lease().await?;
        let value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("worker restrictions missing".into()))?;
        let Some(copy) = value.worker else {
            return Ok(());
        };
        require(
            copy.device == device && copy.server == server,
            "worker keeper changed",
        )?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        binding(&mut tx, device, server).await?;
        replay(&mut tx, &copy, opening).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await
    }
    pub async fn worker_task_erased(
        &self,
        device: &str,
        server: &str,
        task: &str,
        revision: &str,
    ) -> Result<bool> {
        let _lease = self.memory_guard.lease().await?;
        let value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("worker restrictions missing".into()))?;
        let Some(copy) = value.worker else {
            return Ok(false);
        };
        require(
            copy.device == device && copy.server == server,
            "worker keeper changed",
        )?;
        Ok(copy
            .groups
            .values()
            .any(|(id, rev)| id == task && rev == revision))
    }
}
