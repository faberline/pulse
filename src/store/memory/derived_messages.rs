//! Provenance and erasure of generated chat projections. No prompt permission.
use super::*;
use crate::ErasedProjection;

impl Store {
    /// Prune Rust-owned desktop projections after a local source change.
    /// Serialized data keeps this core check independent of any UI crate.
    pub async fn prune_local_memory_projection_in(
        db: &mut SqliteConnection,
        now: Option<i64>,
        denied: bool,
    ) -> Result<bool> {
        let count = sqlx::query("DELETE FROM local_meta WHERE key='cache' AND json_array_length(local_meta.value,'$.memory_sources')>0 AND (? OR EXISTS (SELECT 1 FROM json_each(local_meta.value,'$.memory_sources') s WHERE NOT EXISTS(SELECT 1 FROM local_memory_cache m WHERE m.id=json_extract(s.value,'$.memory_id') AND json_extract(m.data,'$.revision')=json_extract(s.value,'$.revision') AND json_extract(m.data,'$.source_id')=json_extract(s.value,'$.source_id') AND json_extract(m.data,'$.expires_at')=json_extract(s.value,'$.expires_at') AND (? IS NULL OR json_extract(m.data,'$.expires_at')>?) AND NOT EXISTS(SELECT 1 FROM local_memory_queue q WHERE q.request IS NOT NULL AND json_extract(q.request,'$.command.kind')='correct' AND json_extract(q.request,'$.command.id')=m.id AND json_extract(q.request,'$.command.revision')>=json_extract(m.data,'$.revision')))))")
            .bind(denied).bind(now).bind(now).execute(&mut *db).await?.rows_affected();
        if count > 0 {
            sqlx::query("DELETE FROM local_meta WHERE key='cache_sequence'")
                .execute(db)
                .await?;
        }
        Ok(count > 0)
    }

    /// Hold only over a bounded projection send, never a provider request.
    pub async fn lease_projection_sources(
        &self,
        sources: &[crate::TaskMemorySource],
        now: i64,
    ) -> Result<super::task_sources::TaskMemoryLease<'_>> {
        require(
            (1..=100).contains(&sources.len()),
            "invalid projection source count",
        )?;
        let lease = self.memory_guard.lease().await?;
        let mut restrictions = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &restrictions).await?;
        self.expire_memory_in(&mut tx, &mut restrictions, now)
            .await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        let mut db = self.pool.acquire().await?;
        for source in sources {
            let valid:i64=sqlx::query_scalar("SELECT count(*) FROM memories WHERE id=? AND revision=? AND source_uuid=? AND expires_at=? AND expires_at>?").bind(&source.memory_id).bind(source.revision as i64).bind(&source.source_id).bind(source.expires_at).bind(now).fetch_one(&mut *db).await?;
            require(valid == 1, "projection source unavailable")?;
        }
        Ok(super::task_sources::TaskMemoryLease { _lease: lease })
    }
    /// Must share the transaction which saves the generated message. An existing
    /// lineage cannot be removed or changed by replaying the same message ID.
    pub async fn track_memory_message_in(
        db: &mut SqliteConnection,
        task_id: &str,
        revision: u64,
        message: &str,
        now: i64,
    ) -> Result<()> {
        require(
            !message.is_empty() && message.len() <= 256,
            "invalid derived message identity",
        )?;
        let prior: Option<(String, String, i64)> = sqlx::query_as(
            "SELECT task_id,task_revision,redacted FROM memory_derived_messages WHERE id=?",
        )
        .bind(message)
        .fetch_optional(&mut *db)
        .await?;
        if let Some((task, rev, redacted)) = &prior {
            require(
                task == task_id && *rev == revision.to_string() && *redacted == 0,
                "derived message lineage conflict",
            )?;
        }
        super::task_sources::validate(db, task_id, revision, now).await?;
        let bound: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_memory_sources WHERE task_id=? AND task_revision=?",
        )
        .bind(task_id)
        .bind(revision.to_string())
        .fetch_one(&mut *db)
        .await?;
        if bound == 1 {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM memory_derived_messages")
                .fetch_one(&mut *db)
                .await?;
            require(
                prior.is_some() || count < 10_000,
                "derived message capacity",
            )?;
            sqlx::query("INSERT OR IGNORE INTO memory_derived_messages(id,task_id,task_revision) VALUES(?,?,?)").bind(message).bind(task_id).bind(revision.to_string()).execute(&mut *db).await?;
        } else {
            require(prior.is_none(), "derived message lineage cannot be dropped")?;
        }
        Ok(())
    }
}

pub(super) async fn erase_invalidated(db: &mut SqliteConnection) -> Result<()> {
    let rows:Vec<(String,String)>=sqlx::query_as("SELECT DISTINCT d.task_id,d.task_revision FROM memory_derived_messages d JOIN task_memory_sources b ON b.task_id=d.task_id AND b.task_revision=d.task_revision WHERE d.redacted=0 AND json_extract(b.data,'$.invalidated')=1").fetch_all(&mut *db).await?;
    for (task, revision) in rows {
        erase(
            db,
            &task,
            revision
                .parse()
                .map_err(|_| Error::Rejected("invalid derived revision".into()))?,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn erase(db: &mut SqliteConnection, task: &str, revision: u64) -> Result<()> {
    sqlx::query(
        "UPDATE memory_derived_messages SET redacted=1 WHERE task_id=? AND task_revision=?",
    )
    .bind(task)
    .bind(revision.to_string())
    .execute(&mut *db)
    .await?;
    sqlx::query("UPDATE link_chat SET body='記憶來源已移除，回覆內容已清除。' WHERE id IN (SELECT id FROM memory_derived_messages WHERE redacted=1)").execute(&mut *db).await?;
    // Include acknowledged history: a database or ACK retry must never recover
    // the old body. Keep its original header, not a newly hashed replacement.
    let rows=sqlx::query("SELECT o.device,o.sequence,o.data FROM link_outbox o WHERE json_extract(o.data,'$.payload.kind') IN ('snapshot','derived_snapshot') AND (EXISTS(SELECT 1 FROM json_each(o.data,'$.payload.messages') m JOIN memory_derived_messages d ON d.id=json_extract(m.value,'$.id') WHERE d.redacted=1) OR EXISTS(SELECT 1 FROM json_each(o.data,'$.payload.sources') s JOIN json_each((SELECT data FROM task_memory_sources WHERE task_id=? AND task_revision=?),'$.sources') b ON json_extract(s.value,'$.memory_id')=json_extract(b.value,'$.memory_id') AND json_extract(s.value,'$.revision')=json_extract(b.value,'$.revision') AND json_extract(s.value,'$.source_id')=json_extract(b.value,'$.source_id') AND json_extract(s.value,'$.expires_at')=json_extract(b.value,'$.expires_at')))").bind(task).bind(revision.to_string()).fetch_all(&mut *db).await?;
    for row in rows {
        let value: serde_json::Value = serde_json::from_str(&row.get::<String, _>("data"))?;
        let erased = ErasedProjection {
            version: serde_json::from_value(value["version"].clone())?,
            id: serde_json::from_value(value["id"].clone())?,
            device: row.get("device"),
            sequence: row.get::<i64, _>("sequence").to_string(),
            epoch: serde_json::from_value(value["epoch"].clone())?,
            digest: serde_json::from_value(value["digest"].clone())?,
            binding_digest: serde_json::from_value(value["binding_digest"].clone())?,
        };
        require(erased.valid(), "invalid projection erasure identity")?;
        sqlx::query("INSERT OR IGNORE INTO link_erased_frames VALUES(?,?,?)")
            .bind(&erased.device)
            .bind(row.get::<i64, _>("sequence"))
            .bind(serde_json::to_string(&erased)?)
            .execute(&mut *db)
            .await?;
        sqlx::query("UPDATE link_outbox SET data='null' WHERE device=? AND sequence=?")
            .bind(&erased.device)
            .bind(row.get::<i64, _>("sequence"))
            .execute(&mut *db)
            .await?;
    }
    Ok(())
}
