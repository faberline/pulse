//! Required provenance/fencing foundation. This does not authorize prompt use.
use super::super::{load, notify, put, unresolved};
use super::*;
use crate::{Outcome, Run, State, Task, TaskMemoryRequest, TaskMemorySource, TaskMemorySources};
use serde_json::json;

/// Restriction lease for a controller's bounded local commit. Do not hold it over
/// provider calls. Dispatch adapters still need to adopt this before prompt use.
pub struct TaskMemoryLease<'a> {
    pub(super) _lease: super::Lease<'a>,
}

impl Store {
    pub async fn lease_task_memory(
        &self,
        task_id: &str,
        now: i64,
    ) -> Result<Option<TaskMemoryLease<'_>>> {
        let revision:Option<String>=sqlx::query_scalar("SELECT b.task_revision FROM task_memory_sources b JOIN tasks t ON t.id=b.task_id WHERE b.task_id=? AND b.task_revision=json_extract(t.data,'$.revision')").bind(task_id).fetch_optional(&self.pool).await?;
        let Some(revision) = revision else {
            return Ok(None);
        };
        let revision = revision
            .parse()
            .map_err(|_| Error::Rejected("invalid task memory revision".into()))?;
        let lease = self.memory_guard.lease().await?;
        let mut restrictions = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &restrictions).await?;
        self.expire_memory_in(&mut tx, &mut restrictions, now)
            .await?;
        // Persist mandatory withdrawal even when the requested old use is denied.
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        let mut db = self.pool.acquire().await?;
        validate(&mut db, task_id, revision, now).await?;
        Ok(Some(TaskMemoryLease { _lease: lease }))
    }
    /// Bind before any model/tool work. The controller supplies authenticated Human
    /// origin, never a model proposal. This method returns references, not bodies.
    pub async fn bind_task_memories(
        &self,
        origin: Origin,
        request: &TaskMemoryRequest,
        now: i64,
    ) -> Result<TaskMemorySources> {
        require(origin == Origin::Human, "only Human may bind task memory")?;
        let mut request = request.clone();
        request.source_id = super::revocations::canonical(&request.source_id)?;
        require(
            !request.task_id.is_empty()
                && request.task_id.len() <= 128
                && request.task_revision > 0
                && request.task_revision <= i64::MAX as u64,
            "invalid task memory identity",
        )?;
        require(
            request.source_at >= 0 && request.source_at <= now,
            "invalid memory adoption time",
        )?;
        require(
            (1..=8).contains(&request.sources.len()),
            "task memory needs 1..8 sources",
        )?;
        for source in &mut request.sources {
            source.id = super::revocations::canonical(&source.id)?;
        }
        request.sources.sort_by(|a, b| a.id.cmp(&b.id));
        require(
            !request.sources.windows(2).any(|w| w[0].id == w[1].id),
            "duplicate task memory source",
        )?;
        let _lease = self.memory_guard.lease().await?;
        let mut restrictions = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &restrictions).await?;
        self.expire_memory_in(&mut tx, &mut restrictions, now)
            .await?;
        let prior: Option<String> =
            sqlx::query_scalar("SELECT data FROM task_memory_sources WHERE request_id=?")
                .bind(&request.source_id)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(prior) = prior {
            let prior: TaskMemorySources = serde_json::from_str(&prior)?;
            require(
                prior.request == request,
                "task memory source identity conflict",
            )?;
            tx.commit().await?;
            self.checkpoint_memory_redaction().await?;
            return Ok(prior);
        }
        let task: Task = load(&mut tx, "tasks", &request.task_id).await?;
        require(
            task.revision == request.task_revision
                && !task.cancel_requested
                && !task.state.terminal()
                && task.deadline > now
                && request.source_at >= task.created_at,
            "task memory revision or state conflict",
        )?;
        require(
            task.active_run.is_none() && matches!(task.state, State::Queued | State::WaitingDevice),
            "memory adoption must precede execution",
        )?;
        // Counters span all revisions and must never be reset to adopt new
        // sources. Prove that this specific revision has not started instead.
        let started:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM runs WHERE json_extract(data,'$.token.task_id')=? AND json_extract(data,'$.token.revision')=?) + (SELECT count(*) FROM operations WHERE json_extract(data,'$.token.task_id')=? AND json_extract(data,'$.token.revision')=?) + (SELECT count(*) FROM link_reads WHERE task_id=? AND task_revision=?)")
            .bind(&task.id).bind(task.revision.to_string()).bind(&task.id).bind(task.revision.to_string()).bind(&task.id).bind(task.revision.to_string()).fetch_one(&mut *tx).await?;
        require(
            started == 0,
            "memory adoption requires an unstarted task revision",
        )?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_memory_sources")
            .fetch_one(&mut *tx)
            .await?;
        require(count < 1000, "task memory binding capacity")?;
        let used: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_memory_sources WHERE task_id=? AND task_revision=?",
        )
        .bind(&task.id)
        .bind(task.revision.to_string())
        .fetch_one(&mut *tx)
        .await?;
        require(used == 0, "task revision already has memory sources")?;
        let mut sources = Vec::new();
        for source in &request.sources {
            require(
                source.revision > 0 && source.revision <= i64::MAX as u64,
                "invalid memory source revision",
            )?;
            let row =
                sqlx::query("SELECT revision,source_uuid,expires_at FROM memories WHERE id=?")
                    .bind(&source.id)
                    .fetch_optional(&mut *tx)
                    .await?
                    .ok_or_else(|| Error::Rejected("memory source unavailable".into()))?;
            require(
                row.get::<i64, _>("revision") as u64 == source.revision
                    && row.get::<i64, _>("expires_at") > now,
                "memory source is stale or expired",
            )?;
            sources.push(TaskMemorySource {
                memory_id: source.id.clone(),
                revision: source.revision,
                source_id: row.get("source_uuid"),
                expires_at: row.get("expires_at"),
            });
        }
        let payload = json!({"task_memory_binding":request});
        let event = uuid::Uuid::new_v4().to_string();
        let outcome = Outcome::Applied {
            task_ids: vec![task.id.clone()],
        };
        sqlx::query("INSERT INTO events(id,source,source_id,origin,digest,payload,received_at,outcome) VALUES(?,'task-memory',?,'human',?,?,?,?)")
            .bind(&event).bind(&request.source_id).bind(digest(&json!({"origin":"human","payload":payload}))?).bind(serde_json::to_string(&payload)?).bind(now).bind(serde_json::to_string(&outcome)?).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO event_tasks VALUES(?,?)")
            .bind(&event)
            .bind(&task.id)
            .execute(&mut *tx)
            .await?;
        let binding = TaskMemorySources {
            request,
            authorization_event: event.clone(),
            sources,
            invalidated: false,
        };
        sqlx::query("INSERT INTO task_memory_sources VALUES(?,?,?,?)")
            .bind(&task.id)
            .bind(task.revision.to_string())
            .bind(&binding.request.source_id)
            .bind(serde_json::to_string(&binding)?)
            .execute(&mut *tx)
            .await?;
        notify(
            &mut tx,
            &format!("event:{event}"),
            &json!({"event_id":event,"outcome":outcome}),
        )
        .await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        Ok(binding)
    }

    /// Bodyless provenance only. A returned binding is not a model/tool grant.
    pub async fn task_memory_sources(
        &self,
        task_id: &str,
        revision: u64,
    ) -> Result<Option<TaskMemorySources>> {
        let data: Option<String> = sqlx::query_scalar(
            "SELECT data FROM task_memory_sources WHERE task_id=? AND task_revision=?",
        )
        .bind(task_id)
        .bind(revision.to_string())
        .fetch_optional(&self.pool)
        .await?;
        data.map(|s| serde_json::from_str(&s).map_err(Error::from))
            .transpose()
    }
    /// Shared SQL gate for adapters. Filesystem restrictions still need their own
    /// lease before future prompt use; this gate alone never permits memory prompts.
    pub async fn validate_task_memory_in(
        db: &mut SqliteConnection,
        task_id: &str,
        revision: u64,
        now: i64,
    ) -> Result<()> {
        validate(db, task_id, revision, now).await
    }

    /// Coalesce metadata changes when the normal outbox has room. Source erasure
    /// itself must succeed even if this handoff has to wait.
    pub async fn flush_memory_task_changes(&self) -> Result<bool> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let pending: Option<String> = sqlx::query_scalar(
            "SELECT value FROM settings WHERE key='memory_task_changes_pending'",
        )
        .fetch_optional(&mut *tx)
        .await?;
        if pending.is_none() {
            return Ok(false);
        }
        notify(
            &mut tx,
            &format!("memory-tasks:{}", uuid::Uuid::new_v4()),
            &json!({"kind":"memory_task_sources_changed"}),
        )
        .await?;
        sqlx::query("DELETE FROM settings WHERE key='memory_task_changes_pending'")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
}

pub(in crate::store) async fn validate(
    db: &mut SqliteConnection,
    task_id: &str,
    revision: u64,
    now: i64,
) -> Result<()> {
    let data: Option<String> = sqlx::query_scalar(
        "SELECT data FROM task_memory_sources WHERE task_id=? AND task_revision=?",
    )
    .bind(task_id)
    .bind(revision.to_string())
    .fetch_optional(&mut *db)
    .await?;
    let Some(data) = data else {
        return Ok(());
    };
    let binding: TaskMemorySources = serde_json::from_str(&data)?;
    require(
        !binding.invalidated
            && binding.request.task_id == task_id
            && binding.request.task_revision == revision
            && (1..=8).contains(&binding.sources.len())
            && binding.sources.len() == binding.request.sources.len(),
        "task memory source unavailable",
    )?;
    let row = sqlx::query("SELECT source,source_id,origin,payload FROM events WHERE id=?")
        .bind(&binding.authorization_event)
        .fetch_optional(&mut *db)
        .await?
        .ok_or_else(|| Error::Rejected("task memory authorization unavailable".into()))?;
    let payload: serde_json::Value = serde_json::from_str(&row.get::<String, _>("payload"))?;
    require(
        row.get::<String, _>("origin") == "human"
            && row.get::<String, _>("source") == "task-memory"
            && row.get::<String, _>("source_id") == binding.request.source_id
            && payload["task_memory_binding"] == serde_json::to_value(&binding.request)?,
        "task memory authorization changed",
    )?;
    for (adopted, source) in binding.request.sources.iter().zip(&binding.sources) {
        require(
            adopted.id == source.memory_id && adopted.revision == source.revision,
            "task memory source changed",
        )?;
        let valid:i64=sqlx::query_scalar("SELECT count(*) FROM memories WHERE id=? AND revision=? AND source_uuid=? AND expires_at=? AND expires_at>?").bind(&source.memory_id).bind(source.revision as i64).bind(&source.source_id).bind(source.expires_at).bind(now).fetch_one(&mut *db).await?;
        require(valid == 1, "task memory source unavailable")?;
    }
    Ok(())
}

/// Required erasure cannot depend on notification capacity. The caller holds the
/// same keeper lease/transaction as source removal. Keep operation uncertainty.
pub(super) async fn redact_missing(db: &mut SqliteConnection) -> Result<()> {
    let rows:Vec<String>=sqlx::query_scalar("SELECT b.data FROM task_memory_sources b WHERE json_extract(b.data,'$.invalidated')=0 AND EXISTS (SELECT 1 FROM json_each(b.data,'$.sources') s WHERE NOT EXISTS (SELECT 1 FROM memories m WHERE m.id=json_extract(s.value,'$.memory_id') AND m.revision=json_extract(s.value,'$.revision') AND m.source_uuid=json_extract(s.value,'$.source_id') AND m.expires_at=json_extract(s.value,'$.expires_at')))").fetch_all(&mut *db).await?;
    if !rows.is_empty() {
        sqlx::query("INSERT INTO settings(key,value) VALUES('memory_task_changes_pending','true') ON CONFLICT(key) DO UPDATE SET value=excluded.value").execute(&mut *db).await?;
    }
    for data in rows {
        let mut binding: TaskMemorySources = serde_json::from_str(&data)?;
        binding.invalidated = true;
        sqlx::query("UPDATE task_memory_sources SET data=? WHERE task_id=? AND task_revision=?")
            .bind(serde_json::to_string(&binding)?)
            .bind(&binding.request.task_id)
            .bind(binding.request.task_revision.to_string())
            .execute(&mut *db)
            .await?;
        let mut task: Task = load(db, "tasks", &binding.request.task_id).await?;
        super::derived_results::erase(db, &task.id, binding.request.task_revision).await?;
        super::derived_messages::erase(db, &task.id, binding.request.task_revision).await?;
        let runs:Vec<String>=sqlx::query_scalar("SELECT data FROM runs WHERE json_extract(data,'$.token.task_id')=? AND json_extract(data,'$.token.revision')=?").bind(&task.id).bind(binding.request.task_revision.to_string()).fetch_all(&mut *db).await?;
        for data in runs {
            let mut run: Run = serde_json::from_str(&data)?;
            run.checkpoint = json!({"phase":"memory_source_withdrawn"});
            put(db, "runs", &run.token.run_id, &run).await?;
        }
        if task.revision != binding.request.task_revision {
            continue;
        }
        super::super::approvals::invalidate(db, &task.id, "memory_source_withdrawn").await?;
        task.active_run = None;
        task.wake_at = None;
        task.evidence.clear();
        if !matches!(task.state, State::Cancelled | State::Failed) {
            task.state = if unresolved(db, &task.id).await? {
                State::OutcomeUnknown
            } else {
                State::Blocked
            };
        }
        task.reason = Some("memory_source_withdrawn".into());
        put(db, "tasks", &task.id, &task).await?;
    }
    super::derived_results::erase_invalidated(db).await?;
    super::derived_messages::erase_invalidated(db).await
}
