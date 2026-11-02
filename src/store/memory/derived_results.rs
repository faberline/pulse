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
            return Ok(None);
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
        let row=sqlx::query("SELECT r.task_id,r.task_revision,r.state,r.request FROM link_reads r JOIN memory_erased_reads e ON e.id=r.id WHERE r.id=?").bind(operation_id).fetch_optional(&mut *db).await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let state: String = row.get("state");
        let request: serde_json::Value = serde_json::from_str(&row.get::<String, _>("request"))?;
        let effect = matches!(
            request["kind"].as_str(),
            Some("browser_action" | "computer_action")
        );
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
    let bindings:Vec<(String,String)>=sqlx::query_as("SELECT b.task_id,b.task_revision FROM task_memory_sources b WHERE json_extract(b.data,'$.invalidated')=1 AND (EXISTS(SELECT 1 FROM link_reads r WHERE r.task_id=b.task_id AND r.task_revision=b.task_revision AND NOT EXISTS(SELECT 1 FROM memory_erased_reads e WHERE e.id=r.id)) OR EXISTS(SELECT 1 FROM events e WHERE e.origin='tool' AND ((json_extract(e.payload,'$.operation_proposal.token.task_id')=b.task_id AND json_extract(e.payload,'$.operation_proposal.token.revision')=b.task_revision) OR EXISTS(SELECT 1 FROM link_reads r WHERE r.task_id=b.task_id AND r.task_revision=b.task_revision AND e.source='device-result:'||r.device AND e.source_id=r.id AND coalesce(json_extract(e.payload,'$.source_erased'),0)=0))))").fetch_all(&mut *db).await?;
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
    sqlx::query("INSERT OR IGNORE INTO memory_erased_reads SELECT id FROM link_reads WHERE task_id=? AND task_revision=?").bind(task).bind(revision.to_string()).execute(&mut *db).await?;
    // Register existing chat evidence before the common projection scrub. This
    // also covers the old result-save/chat-save gap during schema migration.
    sqlx::query("INSERT OR IGNORE INTO memory_derived_messages(id,task_id,task_revision,redacted) SELECT c.id,r.task_id,r.task_revision,1 FROM link_chat c JOIN link_reads r ON r.id=c.id JOIN memory_erased_reads e ON e.id=r.id WHERE r.task_id=? AND r.task_revision=?").bind(task).bind(revision.to_string()).execute(&mut *db).await?;
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
