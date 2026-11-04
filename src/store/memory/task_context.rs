//! Trusted local source retrieval. Provider prompt memory remains disabled.
use super::*;
use crate::{State, TASK_MEMORY_CONTEXT_BYTES, TaskMemoryContext, TaskMemorySources, Token};

impl Store {
    /// Read only the exact current run's explicit Human adoption. Do not hold a
    /// SQL transaction or keeper lease over provider I/O. A caller must still
    /// validate_model_call before send and during its wait, then fence results.
    pub async fn task_memory_context(
        &self,
        token: &Token,
        maximum_bytes: usize,
        now: i64,
    ) -> Result<Option<TaskMemoryContext>> {
        require(
            now >= 0 && (1..=TASK_MEMORY_CONTEXT_BYTES).contains(&maximum_bytes),
            "invalid task memory context clock or capacity",
        )?;
        // This commits mandatory expiry/revocation before a stale read fails.
        let _lease = self.lease_task_memory(&token.task_id, now).await?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let (task, _) = super::super::current(&mut tx, token, now).await?;
        require(
            task.state == State::Planning && crate::read_scope(&task.scope),
            "task memory context requires current read planning",
        )?;
        let data: Option<String> = sqlx::query_scalar(
            "SELECT data FROM task_memory_sources WHERE task_id=? AND task_revision=?",
        )
        .bind(&task.id)
        .bind(task.revision.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(data) = data else {
            tx.commit().await?;
            return Ok(None);
        };
        let binding: TaskMemorySources = serde_json::from_str(&data)?;
        let mut memories = Vec::with_capacity(binding.sources.len());
        let mut valid_until = task.deadline;
        for source in &binding.sources {
            // Do not read unrelated bodies, nor allocate an oversized legacy
            // row before checking the original per-source limits.
            let row = sqlx::query("SELECT m.*,e.fingerprint,e.outcome,e.source_id AS original_source FROM memories m JOIN memory_events e ON e.memory_id=m.id AND e.revision=m.revision AND e.source_uuid=m.source_uuid WHERE m.id=? AND m.revision=? AND m.source_uuid=? AND m.expires_at=? AND m.expires_at>? AND length(CAST(m.label AS BLOB)) BETWEEN 1 AND 128 AND length(CAST(m.body AS BLOB)) BETWEEN 1 AND 1024")
                .bind(&source.memory_id).bind(source.revision as i64).bind(&source.source_id)
                .bind(source.expires_at).bind(now).fetch_optional(&mut *tx).await?
                .ok_or_else(|| Error::Rejected("task memory content proof unavailable".into()))?;
            let memory = Memory {
                id: row.get("id"),
                revision: row.get::<i64, _>("revision") as u64,
                label: row.get("label"),
                body: row.get("body"),
                source_id: row.get("source_uuid"),
                source_at: row.get("source_at"),
                expires_at: row.get("expires_at"),
            };
            let command = if memory.revision == 1 {
                MemoryCommand::Remember {
                    label: memory.label.clone(),
                    body: memory.body.clone(),
                }
            } else {
                MemoryCommand::Correct {
                    id: memory.id.clone(),
                    revision: memory.revision - 1,
                    label: memory.label.clone(),
                    body: memory.body.clone(),
                }
            };
            let original = MemoryRequest {
                source_id: row.get("original_source"),
                source_at: memory.source_at,
                command,
            };
            require(
                row.get::<String, _>("outcome") == "stored"
                    && row.get::<String, _>("source_id") == original.source_id
                    && memory.source_at >= 0
                    && memory.source_at <= now
                    && memory.source_at.checked_add(MEMORY_RETENTION_MS) == Some(memory.expires_at)
                    && row.get::<Option<String>, _>("fingerprint").as_deref()
                        == Some(digest(&original)?.as_str()),
                "task memory content proof changed",
            )?;
            valid_until = valid_until.min(memory.expires_at);
            memories.push(memory);
        }
        let context = TaskMemoryContext {
            token: token.clone(),
            binding,
            memories,
            checked_at: now,
            valid_until,
        };
        require(
            serde_json::to_vec(&context)?.len() <= maximum_bytes,
            "task memory context capacity; no partial sources returned",
        )?;
        tx.commit().await?;
        Ok(Some(context))
    }
}
