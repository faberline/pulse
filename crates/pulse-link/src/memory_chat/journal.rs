use super::{Intent, parse, validate};
use crate::{
    Result, ensure,
    journal::Journal,
    protocol::{ChatMessage, ClientRole},
};
use pulse_core::{MEMORY_RETENTION_MS, Memory, MemoryCommand, MemoryRequest, MemoryView};
use sqlx::Row;

impl Journal {
    pub(crate) async fn reset_memory_session(&self) -> Result<()> {
        // A restored local DB cannot prove that cached remote restrictions are
        // current. Never reveal its old source content before a fresh sync.
        sqlx::query("DELETE FROM local_memory_cache")
            .execute(&self.db)
            .await?;
        sqlx::query("DELETE FROM local_meta WHERE key='memory_synced_at'")
            .execute(&self.db)
            .await?;
        dirty(&mut *self.db.acquire().await?).await?;
        self.flush_memory().await
    }
    pub async fn memory_control(&self, source: &str, text: &str, now: i64) -> Result<bool> {
        let Some(intent) = parse(text) else {
            return Ok(false);
        };
        ensure(
            self.role == ClientRole::Chat && uuid::Uuid::parse_str(source).is_ok(),
            "memory source or role invalid",
        )?;
        ensure(
            (0..=i64::MAX - MEMORY_RETENTION_MS).contains(&now),
            "invalid memory source clock",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let exists: i64 =
            sqlx::query_scalar("SELECT count(*) FROM local_memory_queue WHERE source_id=?")
                .bind(source)
                .fetch_one(&mut *tx)
                .await?;
        if exists > 0 {
            return Ok(true);
        } // Never replace a previously saved source.
        let pending: i64 =
            sqlx::query_scalar("SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL")
                .fetch_one(&mut *tx)
                .await?;
        ensure(pending < 100, "memory delivery capacity")?;
        let (command, phase) = match intent {
            Intent::Query => (None, "query"),
            Intent::Clarify => (None, "clarify"),
            Intent::Remember { label, body } => {
                (Some(MemoryCommand::Remember { label, body }), "queued")
            }
            Intent::Correct { label, body } => {
                let memory = selected(&mut tx, &label, now).await?;
                erase_local_source(&mut tx, &memory).await?;
                (
                    Some(MemoryCommand::Correct {
                        id: memory.id,
                        revision: memory.revision,
                        label,
                        body,
                    }),
                    "queued",
                )
            }
            Intent::Forget { label } => {
                let memory = selected(&mut tx, &label, now).await?;
                erase_local_source(&mut tx, &memory).await?;
                (
                    Some(MemoryCommand::Forget {
                        id: memory.id,
                        revision: memory.revision,
                    }),
                    "queued",
                )
            }
        };
        let request = if let Some(command) = command {
            validate(&command)?;
            Some(serde_json::to_string(&MemoryRequest {
                source_id: source.into(),
                source_at: now,
                command,
            })?)
        } else {
            None
        };
        sqlx::query("DELETE FROM local_memory_queue WHERE source_id IN (SELECT source_id FROM local_memory_queue WHERE request IS NULL ORDER BY saved_at,source_id LIMIT max(0,(SELECT count(*) FROM local_memory_queue)-999))").execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO local_memory_queue(source_id,request,phase,saved_at) VALUES(?,?,?,?)",
        )
        .bind(source)
        .bind(request)
        .bind(phase)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO local_meta VALUES('memory_visible','true') ON CONFLICT(key) DO UPDATE SET value='true'").execute(&mut *tx).await?;
        dirty(&mut tx).await?;
        tx.commit().await?;
        self.flush_memory().await?;
        Ok(true)
    }

    pub(crate) async fn memory_dirty(&self) -> Result<Option<String>> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='memory_dirty'")
                .fetch_optional(&self.db)
                .await?;
        Ok(value.filter(|s| !s.is_empty()))
    }
    pub(crate) async fn memory_clean(&self, generation: &str) -> Result<()> {
        sqlx::query("UPDATE local_meta SET value='' WHERE key='memory_dirty' AND value=?")
            .bind(generation)
            .execute(&self.db)
            .await?;
        Ok(())
    }
    pub(crate) async fn memory_requests(&self) -> Result<Vec<MemoryRequest>> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT request FROM local_memory_queue WHERE request IS NOT NULL ORDER BY saved_at,rowid LIMIT 100").fetch_all(&self.db).await?;
        Ok(rows
            .into_iter()
            .map(|s| serde_json::from_str(&s))
            .collect::<std::result::Result<_, _>>()?)
    }
    pub(crate) async fn finish_memory(&self, source: &str, phase: &str) -> Result<()> {
        ensure(
            matches!(phase, "accepted" | "rejected" | "redacted"),
            "invalid memory acknowledgement",
        )?;
        sqlx::query("UPDATE local_memory_queue SET request=NULL,phase=? WHERE source_id=?")
            .bind(phase)
            .bind(source)
            .execute(&self.db)
            .await?;
        self.flush_memory().await
    }
    pub async fn apply_memory_view(&self, view: &MemoryView, now: i64) -> Result<()> {
        ensure(
            self.role == ClientRole::Chat
                && uuid::Uuid::parse_str(&view.keeper).is_ok()
                && view.memories.len() <= 100
                && view.redacted_sources.len() <= 10100,
            "memory view identity or capacity",
        )?;
        for memory in &view.memories {
            ensure(
                uuid::Uuid::parse_str(&memory.id).is_ok()
                    && uuid::Uuid::parse_str(&memory.source_id).is_ok()
                    && memory.revision > 0
                    && memory.source_at >= 0
                    && memory.source_at <= now
                    && memory.source_at.checked_add(MEMORY_RETENTION_MS) == Some(memory.expires_at)
                    && memory.expires_at > now,
                "invalid memory source",
            )?;
            validate(&MemoryCommand::Remember {
                label: memory.label.clone(),
                body: memory.body.clone(),
            })?;
        }
        ensure(
            view.redacted_sources
                .iter()
                .all(|s| uuid::Uuid::parse_str(s).is_ok()),
            "invalid memory redaction IDs",
        )?;
        ensure(
            view.memories
                .iter()
                .all(|m| !view.redacted_sources.contains(&m.source_id)),
            "redacted source cannot remain visible",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let keeper: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='memory_keeper'")
                .fetch_optional(&mut *tx)
                .await?;
        ensure(
            keeper.as_ref().is_none_or(|s| s == &view.keeper),
            "memory keeper changed; review pairing",
        )?;
        let epoch: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='memory_epoch'")
                .fetch_optional(&mut *tx)
                .await?;
        ensure(
            epoch
                .map(|s| s.parse::<u64>())
                .transpose()
                .map_err(|_| crate::Error::Rejected("invalid memory epoch".into()))?
                .is_none_or(|e| e <= view.epoch),
            "stale memory restrictions",
        )?;
        for source in &view.redacted_sources {
            sqlx::query(
                "UPDATE local_memory_queue SET request=NULL,phase='redacted' WHERE source_id=?",
            )
            .bind(source)
            .execute(&mut *tx)
            .await?;
        }
        let pending: Vec<String> =
            sqlx::query_scalar("SELECT request FROM local_memory_queue WHERE request IS NOT NULL")
                .fetch_all(&mut *tx)
                .await?;
        let requests: Vec<MemoryRequest> = pending
            .into_iter()
            .map(|s| serde_json::from_str(&s))
            .collect::<std::result::Result<_, _>>()?;
        let hidden: Vec<String> = requests
            .into_iter()
            .filter_map(|r| match r.command {
                MemoryCommand::Correct { id, .. } | MemoryCommand::Forget { id, .. } => Some(id),
                _ => None,
            })
            .collect();
        sqlx::query("DELETE FROM local_memory_cache")
            .execute(&mut *tx)
            .await?;
        for memory in &view.memories {
            if !hidden.contains(&memory.id) {
                sqlx::query("INSERT INTO local_memory_cache VALUES(?,?)")
                    .bind(&memory.id)
                    .bind(serde_json::to_string(memory)?)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        for (key, value) in [
            ("memory_keeper", view.keeper.clone()),
            ("memory_epoch", view.epoch.to_string()),
            ("memory_synced_at", now.to_string()),
        ] {
            sqlx::query("INSERT INTO local_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(key).bind(value).execute(&mut *tx).await?;
        }
        sqlx::query("UPDATE local_memory_queue SET phase='queried' WHERE phase='query'")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.flush_memory().await
    }
    pub(crate) async fn memory_projection(
        &self,
        state: &mut crate::protocol::NativeState,
        now: i64,
    ) -> Result<()> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let removed = sqlx::query(
            "DELETE FROM local_memory_cache WHERE json_extract(data,'$.expires_at')<=?",
        )
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let expired = sqlx::query("UPDATE local_memory_queue SET request=NULL,phase='expired' WHERE request IS NOT NULL AND json_extract(request,'$.source_at')<=?").bind(now-MEMORY_RETENTION_MS).execute(&mut *tx).await?.rows_affected();
        let visible: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='memory_visible'")
                .fetch_optional(&mut *tx)
                .await?;
        if visible.as_deref() == Some("true") {
            let rows: Vec<String> = sqlx::query_scalar("SELECT data FROM local_memory_cache ORDER BY json_extract(data,'$.source_at'),id LIMIT 100").fetch_all(&mut *tx).await?;
            state.memories = rows
                .into_iter()
                .map(|s| serde_json::from_str(&s))
                .collect::<std::result::Result<_, _>>()?;
            state.memory_synced_at = sqlx::query_scalar::<_, String>(
                "SELECT value FROM local_meta WHERE key='memory_synced_at'",
            )
            .fetch_optional(&mut *tx)
            .await?
            .and_then(|s| s.parse().ok());
        }
        if visible.as_deref() == Some("true") {
            let rows = sqlx::query("SELECT source_id,phase FROM local_memory_queue ORDER BY saved_at DESC,rowid DESC LIMIT 10").fetch_all(&mut *tx).await?;
            for row in rows.into_iter().rev() {
                let phase: String = row.get("phase");
                let body = match phase.as_str() {
                    "queued" => "記憶要求已在本機待送，尚未由 server 確認。",
                    "accepted" => {
                        "記憶要求已由 server 確認，目前尚未用於回答。其他離線副本在重連後同步。"
                    }
                    "rejected" => "記憶要求已拒絕。請先查詢目前來源與修訂，再提出更正。",
                    "redacted" => "舊記憶來源已清除，不再重送。",
                    "expired" => "待送記憶已到期，內容已清除。",
                    "clarify" => {
                        "請用「記住 回答語言：繁體中文」、「查詢記憶」、「更正記憶 回答語言：內容」或「忘記記憶 回答語言」。"
                    }
                    "queried" => "記憶來源已核對。有效內容列在下方。",
                    _ => "記憶查詢已保存。連線後會核對目前來源。",
                };
                state.messages.push(ChatMessage {
                    id: row.get("source_id"),
                    body: body.into(),
                    artifact: None,
                    artifact_operation_id: None,
                    approval: None,
                });
            }
        }
        state.pending += sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL",
        )
        .fetch_one(&mut *tx)
        .await? as usize;
        tx.commit().await?;
        if removed + expired > 0 {
            self.flush_memory().await?;
        }
        Ok(())
    }
    pub(crate) async fn flush_memory(&self) -> Result<()> {
        let row = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetch_one(&self.db)
            .await?;
        ensure(row.get::<i64, _>(0) == 0, "memory cache checkpoint busy")
    }
}

pub(crate) async fn dirty(db: &mut sqlx::SqliteConnection) -> Result<()> {
    sqlx::query("INSERT INTO local_meta VALUES('memory_dirty',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(uuid::Uuid::new_v4().to_string()).execute(db).await?;
    Ok(())
}
async fn selected(db: &mut sqlx::SqliteConnection, label: &str, now: i64) -> Result<Memory> {
    let rows: Vec<String> = sqlx::query_scalar("SELECT data FROM local_memory_cache WHERE json_extract(data,'$.label')=? AND json_extract(data,'$.expires_at')>?").bind(label).bind(now).fetch_all(db).await?;
    ensure(
        rows.len() == 1,
        "query current memory before correction or deletion",
    )?;
    Ok(serde_json::from_str(&rows[0])?)
}
async fn erase_local_source(db: &mut sqlx::SqliteConnection, memory: &Memory) -> Result<()> {
    sqlx::query("DELETE FROM local_memory_cache WHERE id=?")
        .bind(&memory.id)
        .execute(&mut *db)
        .await?;
    sqlx::query("UPDATE local_memory_queue SET request=NULL,phase='redacted' WHERE source_id=?")
        .bind(&memory.source_id)
        .execute(db)
        .await?;
    Ok(())
}
