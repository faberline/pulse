//! Erase saved tool evidence without inventing an execution result or a grant.
use super::*;
use crate::State;

impl Store {
    /// Reconcile the exact read revision, including reads of an older task
    /// revision. This is an ordering lease, never positive permission to use it.
    pub async fn lease_read_source_state(
        &self,
        operation_id: &str,
        now: i64,
    ) -> Result<Option<super::task_sources::TaskMemoryLease<'_>>> {
        let bound:i64=sqlx::query_scalar("SELECT count(*) FROM link_reads r JOIN task_memory_sources b ON b.task_id=r.task_id AND b.task_revision=r.task_revision WHERE r.id=?").bind(operation_id).fetch_one(&self.pool).await?;
        if bound == 0 {
            let task: Option<String> =
                sqlx::query_scalar("SELECT task_id FROM link_reads WHERE id=?")
                    .bind(operation_id)
                    .fetch_optional(&self.pool)
                    .await?;
            return match task {
                Some(id) => self.lease_retained_task_data(Some(&id)).await,
                None => Ok(None),
            };
        }
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
        Ok(Some(super::task_sources::TaskMemoryLease { _lease: lease }))
    }

    /// The authenticated transport has checked device, epoch and task binding.
    /// No body is accepted here. A discarded read cannot resume a run. An effect
    /// whose receipt was not validated before erasure remains unknown.
    pub async fn discard_erased_read_result_in(
        db: &mut SqliteConnection,
        operation_id: &str,
        reported_unknown: bool,
    ) -> Result<bool> {
        let row=sqlx::query("SELECT r.task_id,r.task_revision,r.state,e.context FROM link_reads r JOIN memory_erased_reads e ON e.id=r.id WHERE r.id=?").bind(operation_id).fetch_optional(&mut *db).await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let state: String = row.get("state");
        let context: crate::WorkerErasure = serde_json::from_str(&row.get::<String, _>("context"))?;
        require(context.valid(), "invalid worker erasure context")?;
        let effect = context.is_effect;
        let state = if state == "discarded" || (!effect && !reported_unknown) {
            "discarded"
        } else {
            "unknown"
        };
        sqlx::query("UPDATE link_reads SET state=?,result=NULL WHERE id=?")
            .bind(state)
            .bind(operation_id)
            .execute(&mut *db)
            .await?;
        let task_id: String = row.get("task_id");
        let mut task: crate::Task = super::super::load(db, "tasks", &task_id).await?;
        if task.revision.to_string() == row.get::<String, _>("task_revision")
            && task.state == State::OutcomeUnknown
            && task.reason.as_deref() == Some("memory_source_withdrawn")
            && !super::super::unresolved(db, &task_id).await?
        {
            task.state = State::Blocked;
            super::super::put(db, "tasks", &task_id, &task).await?;
        }
        Ok(true)
    }
}

pub(super) async fn erase_invalidated(db: &mut SqliteConnection) -> Result<()> {
    let bindings:Vec<(String,String)>=sqlx::query_as("SELECT b.task_id,b.task_revision FROM task_memory_sources b WHERE json_extract(b.data,'$.invalidated')=1 AND (EXISTS(SELECT 1 FROM link_reads r WHERE r.task_id=b.task_id AND r.task_revision=b.task_revision AND NOT EXISTS(SELECT 1 FROM memory_erased_reads e WHERE e.id=r.id AND e.context IS NOT NULL)) OR EXISTS(SELECT 1 FROM events e WHERE e.origin='tool' AND ((json_extract(e.payload,'$.operation_proposal.token.task_id')=b.task_id AND json_extract(e.payload,'$.operation_proposal.token.revision')=b.task_revision) OR EXISTS(SELECT 1 FROM link_reads r WHERE r.task_id=b.task_id AND r.task_revision=b.task_revision AND e.source='device-result:'||r.device AND e.source_id=r.id AND coalesce(json_extract(e.payload,'$.source_erased'),0)=0))))").fetch_all(&mut *db).await?;
    for (task, revision) in bindings {
        let revision = revision
            .parse()
            .map_err(|_| Error::Rejected("invalid erased read revision".into()))?;
        erase(db, &task, revision).await?;
        super::derived_messages::erase(db, &task, revision).await?;
    }
    Ok(())
}

pub(super) async fn erase(db: &mut SqliteConnection, task: &str, revision: u64) -> Result<()> {
    let rows=sqlx::query("SELECT r.id,r.device,r.epoch,r.request,b.request_id FROM link_reads r JOIN task_memory_sources b ON b.task_id=r.task_id AND b.task_revision=r.task_revision WHERE r.task_id=? AND r.task_revision=? AND NOT EXISTS(SELECT 1 FROM memory_erased_reads e WHERE e.id=r.id AND e.context IS NOT NULL)").bind(task).bind(revision.to_string()).fetch_all(&mut *db).await?;
    for row in rows {
        let id: String = row.get("id");
        let device: String = row.get("device");
        let request: serde_json::Value = serde_json::from_str(&row.get::<String, _>("request"))?;
        let frames:Vec<(i64,String)>=sqlx::query_as("SELECT sequence,data FROM link_outbox WHERE device=? AND json_extract(data,'$.payload.kind')='read' AND json_extract(data,'$.payload.operation_id')=?").bind(&device).bind(&id).fetch_all(&mut *db).await?;
        let header = frames
            .first()
            .map(|(_, s)| {
                serde_json::from_str::<serde_json::Value>(s)
                    .map_err(Error::from)
                    .and_then(|v| crate::ErasedProjection::from_value(&v))
            })
            .transpose()?;
        let context = crate::WorkerErasure {
            operation_id: id.clone(),
            task_id: task.into(),
            revision: revision.to_string(),
            device: device.clone(),
            epoch: row.get::<i64, _>("epoch").to_string(),
            source_id: row.get("request_id"),
            is_effect: !matches!(
                request["kind"].as_str(),
                Some(
                    "list"
                        | "text"
                        | "git_status"
                        | "git_diff"
                        | "browser"
                        | "browser_inspect"
                        | "computer_inspect"
                )
            ),
            read_header: header,
        };
        require(context.valid(), "invalid worker erasure context")?;
        let data = serde_json::to_string(&context)?;
        sqlx::query("INSERT INTO memory_erased_reads(id,context) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET context=excluded.context").bind(&id).bind(&data).execute(&mut *db).await?;
        for (sequence, _) in frames {
            sqlx::query("INSERT OR IGNORE INTO link_erased_work VALUES(?,?,?)")
                .bind(&device)
                .bind(sequence)
                .bind(&data)
                .execute(&mut *db)
                .await?;
            sqlx::query("UPDATE link_outbox SET data='null' WHERE device=? AND sequence=?")
                .bind(&device)
                .bind(sequence)
                .execute(&mut *db)
                .await?;
        }
    }
    erase_registered(db, task, revision).await
}

/// Shared payload scrub after a trusted erasure context was durably registered.
pub(super) async fn erase_registered(
    db: &mut SqliteConnection,
    task: &str,
    revision: u64,
) -> Result<()> {
    sqlx::query("UPDATE link_reads SET request='null' WHERE id IN (SELECT id FROM memory_erased_reads WHERE context IS NOT NULL)").execute(&mut *db).await?;
    let revision_text = revision.to_string();
    let values:Vec<(String,String,String)>=sqlx::query_as("SELECT 'operation',id,data FROM operations UNION ALL SELECT 'request',id,data FROM operation_requests").fetch_all(&mut *db).await?;
    for (kind, id, data) in values {
        let mut value: serde_json::Value = serde_json::from_str(&data)?;
        let token = if kind == "operation" {
            &value["token"]
        } else {
            &value["proposal"]["token"]
        };
        if token["task_id"] == task && token["revision"].as_str() == Some(revision_text.as_str()) {
            let query = if kind == "operation" {
                value["intent"] = serde_json::json!({"source_erased":true});
                "UPDATE operations SET data=? WHERE id=?"
            } else {
                value["proposal"]["intent"] = serde_json::json!({"source_erased":true});
                "UPDATE operation_requests SET data=? WHERE id=?"
            };
            sqlx::query(query)
                .bind(serde_json::to_string(&value)?)
                .bind(id)
                .execute(&mut *db)
                .await?;
        }
    }
    // Register existing chat evidence before the common projection scrub. This
    // also covers the old result-save/chat-save gap during schema migration.
    sqlx::query("INSERT OR IGNORE INTO memory_derived_messages(id,task_id,task_revision,redacted) SELECT c.id,r.task_id,r.task_revision,1 FROM link_chat c JOIN link_reads r ON r.id=c.id JOIN memory_erased_reads e ON e.id=r.id JOIN task_memory_sources b ON b.task_id=r.task_id AND b.task_revision=r.task_revision WHERE r.task_id=? AND r.task_revision=?").bind(task).bind(revision.to_string()).execute(&mut *db).await?;
    sqlx::query("DELETE FROM link_artifact_parts WHERE operation_id IN (SELECT id FROM memory_erased_reads)").execute(&mut *db).await?;
    sqlx::query(
        "DELETE FROM link_artifacts WHERE operation_id IN (SELECT id FROM memory_erased_reads)",
    )
    .execute(&mut *db)
    .await?;
    sqlx::query("UPDATE link_reads SET result=NULL,state=CASE WHEN state IN ('completed','rejected','discarded') THEN 'discarded' ELSE 'unknown' END WHERE id IN (SELECT id FROM memory_erased_reads)").execute(&mut *db).await?;
    let events:Vec<String>=sqlx::query_scalar("SELECT e.id FROM events e WHERE e.origin='tool' AND coalesce(json_extract(e.payload,'$.source_erased'),0)=0 AND (EXISTS(SELECT 1 FROM link_reads r JOIN memory_erased_reads erased ON erased.id=r.id WHERE e.source='device-result:'||r.device AND e.source_id=r.id) OR (json_extract(e.payload,'$.operation_proposal.token.task_id')=? AND json_extract(e.payload,'$.operation_proposal.token.revision')=?))").bind(task).bind(revision.to_string()).fetch_all(&mut *db).await?;
    let payload = serde_json::json!({"commands":[],"source_erased":true});
    let hash = digest(&serde_json::json!({"origin":"tool","payload":payload}))?;
    for event in events {
        sqlx::query("UPDATE events SET payload=?,digest=? WHERE id=?")
            .bind(serde_json::to_string(&payload)?)
            .bind(&hash)
            .bind(event)
            .execute(&mut *db)
            .await?;
    }
    Ok(())
}
