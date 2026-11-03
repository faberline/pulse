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

const MAX_SOURCE_LINEAGE: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParentMemory {
    task_id: String,
    #[serde(with = "crate::model::decimal")]
    revision: u64,
    authorization_event: String,
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
        self.lease_bound_task_memory(task_id, revision, now)
            .await
            .map(Some)
    }

    /// Historical projections retain their original revision's provenance.
    /// This is a short local lease, never permission to call a model or tool.
    pub async fn lease_task_memory_revision(
        &self,
        task_id: &str,
        revision: u64,
        now: i64,
    ) -> Result<Option<TaskMemoryLease<'_>>> {
        require(
            revision > 0 && revision <= i64::MAX as u64,
            "invalid task memory revision",
        )?;
        let bound: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_memory_sources WHERE task_id=? AND task_revision=?",
        )
        .bind(task_id)
        .bind(revision.to_string())
        .fetch_one(&self.pool)
        .await?;
        if bound == 0 {
            return Ok(None);
        }
        self.lease_bound_task_memory(task_id, revision, now)
            .await
            .map(Some)
    }

    async fn lease_bound_task_memory(
        &self,
        task_id: &str,
        revision: u64,
        now: i64,
    ) -> Result<TaskMemoryLease<'_>> {
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
        Ok(TaskMemoryLease { _lease: lease })
    }
    /// Bind before any model/tool work. The controller supplies authenticated Human
    /// origin, never a model proposal. This method returns references, not bodies.
    pub async fn bind_task_memories(
        &self,
        origin: Origin,
        request: &TaskMemoryRequest,
        now: i64,
    ) -> Result<TaskMemorySources> {
        self.bind_task_memories_with_parent(origin, request, None, now)
            .await
    }

    /// Explicit Human adoption of the exact source set of the child's actual
    /// parent revision. This records provenance, never a prompt/tool grant.
    pub async fn inherit_task_memories(
        &self,
        origin: Origin,
        request: &TaskMemoryRequest,
        parent_revision: u64,
        now: i64,
    ) -> Result<TaskMemorySources> {
        require(
            parent_revision > 0 && parent_revision <= i64::MAX as u64,
            "invalid parent memory revision",
        )?;
        self.bind_task_memories_with_parent(origin, request, Some(parent_revision), now)
            .await
    }

    async fn bind_task_memories_with_parent(
        &self,
        origin: Origin,
        request: &TaskMemoryRequest,
        parent_revision: Option<u64>,
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
            let payload: Option<String> =
                sqlx::query_scalar("SELECT payload FROM events WHERE id=?")
                    .bind(&prior.authorization_event)
                    .fetch_optional(&mut *tx)
                    .await?
                    .flatten();
            let payload: serde_json::Value =
                serde_json::from_str(&payload.ok_or_else(|| {
                    Error::Rejected("task memory authorization unavailable".into())
                })?)?;
            let parent = parent_from_payload(&payload)?;
            require(
                parent.as_ref().map(|p| p.revision) == parent_revision,
                "task memory inheritance identity conflict",
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
        let parent = if let Some(revision) = parent_revision {
            let parent_id = task
                .parent
                .as_ref()
                .ok_or_else(|| Error::Rejected("task has no parent".into()))?;
            let parent: Task = load(&mut tx, "tasks", parent_id).await?;
            require(parent.revision == revision, "parent task revision changed")?;
            let data: Option<String> = sqlx::query_scalar(
                "SELECT data FROM task_memory_sources WHERE task_id=? AND task_revision=?",
            )
            .bind(parent_id)
            .bind(revision.to_string())
            .fetch_optional(&mut *tx)
            .await?;
            let binding: TaskMemorySources = serde_json::from_str(
                &data.ok_or_else(|| Error::Rejected("parent memory sources unavailable".into()))?,
            )?;
            let depth = validate_lineage(&mut tx, parent_id, revision, now).await?;
            require(depth < MAX_SOURCE_LINEAGE, "task memory lineage limit")?;
            require(
                binding.request.sources == request.sources
                    && request.source_at >= binding.request.source_at,
                "inheritance requires the exact parent source set",
            )?;
            Some(ParentMemory {
                task_id: parent_id.clone(),
                revision,
                authorization_event: binding.authorization_event,
            })
        } else {
            None
        };
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
        let mut payload = json!({"task_memory_binding":request});
        if let Some(parent) = parent {
            payload["task_memory_parent"] = serde_json::to_value(parent)?;
        }
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
    validate_lineage(db, task_id, revision, now)
        .await
        .map(|_| ())
}

fn parent_from_payload(payload: &serde_json::Value) -> Result<Option<ParentMemory>> {
    payload
        .get("task_memory_parent")
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|_| Error::Rejected("invalid task memory parent proof".into()))
        })
        .transpose()
}

async fn validate_lineage(
    db: &mut SqliteConnection,
    task_id: &str,
    revision: u64,
    now: i64,
) -> Result<usize> {
    let mut next = (task_id.to_owned(), revision);
    let mut visited = std::collections::BTreeSet::new();
    let mut expected: Option<(String, Vec<TaskMemorySource>)> = None;
    loop {
        require(
            visited.len() < MAX_SOURCE_LINEAGE && visited.insert(next.clone()),
            "task memory lineage limit or cycle",
        )?;
        let Some((binding, payload)) = validated_binding(db, &next.0, next.1, now).await? else {
            require(expected.is_none(), "parent memory sources unavailable")?;
            return Ok(0);
        };
        if let Some((event, sources)) = expected.take() {
            require(
                binding.authorization_event == event && binding.sources == sources,
                "parent memory authorization or sources changed",
            )?;
        }
        let Some(parent) = parent_from_payload(&payload)? else {
            return Ok(visited.len());
        };
        require(
            !parent.task_id.is_empty()
                && parent.task_id.len() <= 128
                && parent.revision > 0
                && parent.revision <= i64::MAX as u64
                && uuid::Uuid::parse_str(&parent.authorization_event).is_ok(),
            "invalid parent memory identity",
        )?;
        let task: Task = load(db, "tasks", &binding.request.task_id).await?;
        require(
            task.parent.as_ref() == Some(&parent.task_id),
            "task memory parent relationship changed",
        )?;
        expected = Some((parent.authorization_event, binding.sources));
        next = (parent.task_id, parent.revision);
    }
}

async fn validated_binding(
    db: &mut SqliteConnection,
    task_id: &str,
    revision: u64,
    now: i64,
) -> Result<Option<(TaskMemorySources, serde_json::Value)>> {
    let data: Option<String> = sqlx::query_scalar(
        "SELECT data FROM task_memory_sources WHERE task_id=? AND task_revision=?",
    )
    .bind(task_id)
    .bind(revision.to_string())
    .fetch_optional(&mut *db)
    .await?;
    let Some(data) = data else {
        return Ok(None);
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
    let row = sqlx::query("SELECT source,source_id,origin,payload,digest FROM events WHERE id=?")
        .bind(&binding.authorization_event)
        .fetch_optional(&mut *db)
        .await?
        .ok_or_else(|| Error::Rejected("task memory authorization unavailable".into()))?;
    let payload: serde_json::Value = serde_json::from_str(
        &row.get::<Option<String>, _>("payload")
            .ok_or_else(|| Error::Rejected("task memory authorization unavailable".into()))?,
    )?;
    require(
        row.get::<String, _>("origin") == "human"
            && row.get::<String, _>("source") == "task-memory"
            && row.get::<String, _>("source_id") == binding.request.source_id
            && payload["task_memory_binding"] == serde_json::to_value(&binding.request)?
            && row.get::<String, _>("digest")
                == digest(&json!({"origin":"human","payload":payload}))?,
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
    Ok(Some((binding, payload)))
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
