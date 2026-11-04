//! Durable bodyless worker restrictions, outside SQLite restores. No work grant.
use super::*;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerCopy {
    device: String,
    server: String,
    groups: BTreeMap<String, (String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    runtime: Option<WorkerRuntime>,
    #[serde(default, skip_serializing_if = "not_denied")]
    denied: bool,
}
fn not_denied(value: &bool) -> bool {
    !*value
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRuntime {
    id: String,
    prior_clean: bool,
    closed: bool,
}
impl WorkerCopy {
    pub(super) fn compatible(&self, format: u8) -> bool {
        format >= 5
            && (self.runtime.is_none() || format >= 6)
            && (!self.denied || format >= 7)
            && self.valid()
    }
    pub(super) fn valid(&self) -> bool {
        !self.device.is_empty()
            && self.device.len() <= 128
            && self.server.len() == 64
            && self.server.bytes().all(|b| b.is_ascii_hexdigit())
            && self.groups.len() <= 1000
            && self
                .runtime
                .as_ref()
                .is_none_or(|r| uuid::Uuid::parse_str(&r.id).is_ok())
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
    if copy.denied {
        // Keep the fence outside SQLite. A restored body cannot be sent again.
        Store::prune_local_memory_projection_in(db, None, true).await?;
        sqlx::query("UPDATE local_work SET envelope='null',state='erased'")
            .execute(&mut *db)
            .await?;
        sqlx::query("DELETE FROM local_worker_cleanup")
            .execute(&mut *db)
            .await?;
        sqlx::query("DELETE FROM local_outbox")
            .execute(&mut *db)
            .await?;
        sqlx::query("DELETE FROM local_pending")
            .execute(&mut *db)
            .await?;
        sqlx::query("DELETE FROM local_meta WHERE key IN ('cache','cache_sequence','selected_text_input_v1') OR key LIKE 'computer-selection:%'").execute(&mut *db).await?;
        sqlx::query("UPDATE local_meta SET value='true' WHERE key='stopped'")
            .execute(&mut *db)
            .await?;
        return Ok(());
    }
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
    /// A definitive server denial is a negative fence, never a work grant.
    pub async fn deny_worker_access(&self, device: &str, server: &str) -> Result<()> {
        let _lease = self.memory_guard.lease().await?;
        let mut value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("worker restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        binding(&mut tx, device, server).await?;
        let copy = value.worker.get_or_insert_with(|| WorkerCopy {
            device: device.into(),
            server: server.into(),
            groups: BTreeMap::new(),
            runtime: None,
            denied: false,
        });
        require(
            copy.device == device && copy.server == server,
            "worker keeper changed",
        )?;
        copy.denied = true;
        require(copy.valid(), "invalid worker denial identity")?;
        value.format = value.format.max(7);
        self.memory_guard.write(&value)?;
        replay(&mut tx, value.worker.as_ref().unwrap(), false).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await
    }
    pub async fn worker_access_denied(&self, device: &str, server: &str) -> Result<bool> {
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
        Ok(copy.denied)
    }
    /// A fresh process does not prove that a previous process has stopped.
    /// Keep that distinction outside SQLite so a database restore cannot clear it.
    pub async fn begin_worker_runtime(
        &self,
        device: &str,
        server: &str,
        runtime: &str,
        resources_absent: bool,
    ) -> Result<()> {
        require(
            uuid::Uuid::parse_str(runtime).is_ok(),
            "invalid worker runtime",
        )?;
        let _lease = self.memory_guard.lease().await?;
        let mut value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("worker restrictions missing".into()))?;
        let mut db = self.pool.acquire().await?;
        binding(&mut db, device, server).await?;
        let legacy: i64 = sqlx::query_scalar("SELECT count(*) FROM local_work")
            .fetch_one(&mut *db)
            .await?;
        let fresh: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM local_meta WHERE key='worker_runtime_origin' AND value='fresh'",
        )
        .fetch_one(&mut *db)
        .await?;
        let copy = value.worker.get_or_insert_with(|| WorkerCopy {
            device: device.into(),
            server: server.into(),
            groups: BTreeMap::new(),
            runtime: None,
            denied: false,
        });
        require(
            copy.device == device && copy.server == server,
            "worker keeper changed",
        )?;
        require(
            !copy.denied,
            "worker identity denied; fresh pairing required",
        )?;
        if let Some(prior) = &copy.runtime
            && prior.id == runtime
        {
            return require(!prior.closed, "worker runtime already closed");
        }
        let prior_clean = resources_absent
            && match &copy.runtime {
                Some(prior) => prior.prior_clean && prior.closed,
                None => fresh == 1 && legacy == 0 && copy.groups.is_empty(),
            };
        copy.runtime = Some(WorkerRuntime {
            id: runtime.into(),
            prior_clean,
            closed: false,
        });
        value.format = value.format.max(6);
        self.memory_guard.write(&value)
    }
    /// Call only after the owned process groups, readers and caches have closed.
    pub async fn close_worker_runtime(
        &self,
        device: &str,
        server: &str,
        runtime: &str,
    ) -> Result<()> {
        let _lease = self.memory_guard.lease().await?;
        let mut value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("worker restrictions missing".into()))?;
        let mut db = self.pool.acquire().await?;
        binding(&mut db, device, server).await?;
        let copy = value
            .worker
            .as_mut()
            .ok_or_else(|| Error::Rejected("worker runtime missing".into()))?;
        require(
            copy.device == device && copy.server == server,
            "worker keeper changed",
        )?;
        let current = copy
            .runtime
            .as_mut()
            .ok_or_else(|| Error::Rejected("worker runtime missing".into()))?;
        require(current.id == runtime, "worker runtime changed")?;
        current.closed = true;
        self.memory_guard.write(&value)
    }
    pub async fn worker_runtime_verified(
        &self,
        device: &str,
        server: &str,
        runtime: &str,
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
        Ok(!copy.denied
            && copy
                .runtime
                .is_some_and(|r| r.id == runtime && r.prior_clean && !r.closed))
    }
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
            runtime: None,
            denied: false,
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
        value.format = value.format.max(5);
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
        Ok(copy.denied
            || copy
                .groups
                .values()
                .any(|(id, rev)| id == task && rev == revision))
    }
}
