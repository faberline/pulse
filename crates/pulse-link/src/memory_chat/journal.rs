use super::{Intent, parse, validate};
use crate::{
    Result, ensure,
    journal::Journal,
    protocol::{ChatMessage, ClientRole},
};
use pulse_core::{
    MEMORY_RETENTION_MS, Memory, MemoryCommand, MemoryRef, MemoryRequest, MemoryView,
};
use sqlx::Row;

impl Journal {
    pub(crate) async fn memory_access_allowed(&self) -> Result<bool> {
        if self
            .memory_denied
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(false);
        }
        Ok(
            sqlx::query_scalar::<_, i64>(
                "SELECT denied FROM local_memory_access WHERE singleton=1",
            )
            .fetch_one(&self.db)
            .await?
                == 0,
        )
    }
    pub(crate) async fn deny_memory_access(&self) -> Result<()> {
        if self.role != ClientRole::Chat {
            return Ok(());
        }
        self.memory_copy(pulse_core::MemoryCopyChange::Deny).await?;
        Ok(())
    }
    async fn memory_copy(&self, change: pulse_core::MemoryCopyChange<'_>) -> Result<bool> {
        if self.role != ClientRole::Chat {
            return Ok(true);
        }
        let _lifecycle = self.memory_lifecycle.lock().await;
        self.memory_copy_locked(change).await
    }
    async fn memory_copy_locked(&self, change: pulse_core::MemoryCopyChange<'_>) -> Result<bool> {
        let prior_identity = self.memory_identity.lock().unwrap().clone();
        let identity = match &change {
            pulse_core::MemoryCopyChange::Bind(hash)
            | pulse_core::MemoryCopyChange::Activate(hash) => Some((*hash).to_owned()),
            _ => None,
        };
        self.memory_denied
            .store(true, std::sync::atomic::Ordering::Release);
        *self.memory_identity.lock().unwrap() = None;
        let allowed = self
            .core
            .memory_copy(&self.device, &crate::protocol::hash(&self.server)?, change)
            .await?;
        if !allowed || prior_identity != identity {
            sqlx::query("DELETE FROM local_meta WHERE key='image_erasure_ready'")
                .execute(&self.db)
                .await?;
        }
        self.flush_memory().await?;
        self.memory_denied
            .store(!allowed, std::sync::atomic::Ordering::Release);
        if allowed {
            *self.memory_identity.lock().unwrap() = identity;
        }
        Ok(allowed)
    }
    pub(crate) fn require_memory_identity(
        &self,
        credential: &crate::protocol::Credential,
    ) -> Result<()> {
        self.require_memory_copy()?;
        ensure(
            self.memory_identity.lock().unwrap().as_ref()
                == Some(&crate::protocol::hash(credential)?),
            "memory identity changed",
        )
    }
    pub(crate) async fn deny_memory_credential(
        &self,
        credential: &crate::protocol::Credential,
    ) -> Result<bool> {
        let _lifecycle = self.memory_lifecycle.lock().await;
        if self
            .memory_denied
            .load(std::sync::atomic::Ordering::Acquire)
            || self.memory_identity.lock().unwrap().as_ref()
                != Some(&crate::protocol::hash(credential)?)
        {
            return Ok(false);
        }
        self.memory_copy_locked(pulse_core::MemoryCopyChange::Deny)
            .await?;
        Ok(true)
    }
    pub(crate) async fn finish_memory_for(
        &self,
        source: &str,
        phase: &str,
        credential: &crate::protocol::Credential,
    ) -> Result<()> {
        let _lifecycle = self.memory_lifecycle.lock().await;
        self.require_memory_identity(credential)?;
        if self
            .core
            .finish_memory_withdrawal(
                &self.device,
                &crate::protocol::hash(&self.server)?,
                &crate::protocol::hash(credential)?,
                source,
                phase,
            )
            .await?
        {
            return Ok(());
        }
        self.finish_memory(source, phase).await
    }
    pub(crate) async fn apply_memory_view_for(
        &self,
        view: &MemoryView,
        credential: &crate::protocol::Credential,
        now: i64,
    ) -> Result<()> {
        let _lifecycle = self.memory_lifecycle.lock().await;
        self.require_memory_identity(credential)?;
        self.apply_memory_view_locked(view, now).await
    }
    pub(crate) async fn bind_memory_credential(
        &self,
        credential: &crate::protocol::Credential,
    ) -> Result<bool> {
        ensure(
            credential.device == self.device && credential.role == self.role,
            "memory credential binding",
        )?;
        self.memory_copy(pulse_core::MemoryCopyChange::Bind(&crate::protocol::hash(
            credential,
        )?))
        .await
    }
    pub(crate) async fn prepare_memory_pairing(
        &self,
        credential: &crate::protocol::Credential,
    ) -> Result<()> {
        ensure(
            credential.device == self.device && credential.role == self.role,
            "memory credential binding",
        )?;
        self.memory_copy(pulse_core::MemoryCopyChange::Prepare(
            &crate::protocol::hash(credential)?,
        ))
        .await?;
        Ok(())
    }
    pub(crate) async fn activate_memory_pairing(
        &self,
        credential: &crate::protocol::Credential,
    ) -> Result<()> {
        ensure(
            credential.device == self.device && credential.role == self.role,
            "memory credential binding",
        )?;
        self.memory_copy(pulse_core::MemoryCopyChange::Activate(
            &crate::protocol::hash(credential)?,
        ))
        .await?;
        if self.role == ClientRole::Chat {
            dirty(&mut *self.db.acquire().await?).await?;
        }
        Ok(())
    }
    pub(crate) async fn memory_request_active(&self, request: &MemoryRequest) -> Result<bool> {
        if self
            .memory_denied
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(false);
        }
        let data:Option<String>=sqlx::query_scalar("SELECT request FROM local_memory_queue WHERE source_uuid=lower(?) AND request IS NOT NULL")
            .bind(canonical(&request.source_id)?).fetch_optional(&self.db).await?;
        Ok(data.is_some_and(|s| {
            serde_json::from_str::<MemoryRequest>(&s)
                .ok()
                .is_some_and(|r| serde_json::to_value(r).ok() == serde_json::to_value(request).ok())
        }))
    }
    pub(crate) async fn pending_memory_revocation(&self) -> Result<bool> {
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') IN ('forget','revoke')").fetch_one(&self.db).await?;
        Ok(count > 0)
    }
    pub(crate) async fn reset_memory_session(&self) -> Result<()> {
        self.memory_copy(pulse_core::MemoryCopyChange::Open).await?;
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
        let _lifecycle = self.memory_lifecycle.lock().await;
        self.require_memory_copy()?;
        ensure(
            self.role == ClientRole::Chat && uuid::Uuid::parse_str(source).is_ok(),
            "memory source or role invalid",
        )?;
        let source_uuid = canonical(source)?;
        let rules = self
            .core
            .memory_withdrawal_rules(&self.device, &crate::protocol::hash(&self.server)?)
            .await?;
        if matches!(intent, Intent::Remember { .. } | Intent::Correct { .. }) {
            ensure(
                rules.allows_source(&source_uuid),
                "memory source was withdrawn",
            )?;
        }
        ensure(
            (0..=i64::MAX - MEMORY_RETENTION_MS).contains(&now),
            "invalid memory source clock",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        require_access(&mut tx).await?;
        let exists: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM local_memory_queue WHERE source_uuid=lower(?)",
        )
        .bind(&source_uuid)
        .fetch_one(&mut *tx)
        .await?;
        if exists > 0 {
            return Ok(true);
        } // Never replace a previously saved source.
        let pending: i64 =
            sqlx::query_scalar("SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') NOT IN ('forget','revoke')")
                .fetch_one(&mut *tx)
                .await?;
        let (command, phase) = match intent {
            Intent::Query => (None, "query"),
            Intent::Clarify => (None, "clarify"),
            Intent::Remember { label, body } => {
                ensure(pending < 100, "memory delivery capacity")?;
                let exists: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM local_memory_cache WHERE json_extract(data,'$.label')=?",
                )
                .bind(&label)
                .fetch_one(&mut *tx)
                .await?;
                ensure(exists == 0, "correct the existing named memory")?;
                (Some(MemoryCommand::Remember { label, body }), "queued")
            }
            Intent::Correct { label, body } => {
                ensure(pending < 100, "memory delivery capacity")?;
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
            Intent::Forget { label } => (
                Some(revoke_named(&mut tx, &label, now, true).await?),
                "queued",
            ),
            Intent::Cancel { label } => (
                Some(revoke_named(&mut tx, &label, now, false).await?),
                "queued",
            ),
        };
        let request = if let Some(command) = command {
            validate(&command)?;
            let request = MemoryRequest {
                source_id: source_uuid,
                source_at: now,
                command,
            };
            if matches!(
                request.command,
                MemoryCommand::Revoke { .. } | MemoryCommand::Forget { .. }
            ) {
                // The deletion plan above is read-only. Commit only a bodyless
                // topic marker before Core takes its keeper lease and erases data.
                // This never needs a slot in the ordinary transport queue.
                self.mark_conversation_boundary_in(&mut tx, &request.source_id)
                    .await?;
                tx.commit().await?;
                self.memory_denied
                    .store(true, std::sync::atomic::Ordering::Release);
                self.core
                    .queue_memory_withdrawal(
                        pulse_core::Origin::Human,
                        &self.device,
                        &crate::protocol::hash(&self.server)?,
                        &request,
                        now,
                    )
                    .await?;
                self.flush_memory().await?;
                self.memory_denied
                    .store(false, std::sync::atomic::Ordering::Release);
                return Ok(true);
            }
            Some(serde_json::to_string(&request)?)
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
        self.mark_conversation_boundary_in(&mut tx, source).await?;
        self.require_memory_copy()?;
        pulse_core::Store::prune_local_memory_projection_in(&mut tx, Some(now), false).await?;
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
        let _lifecycle = self.memory_lifecycle.lock().await;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let changed =
            sqlx::query("UPDATE local_meta SET value='' WHERE key='memory_dirty' AND value=?")
                .bind(generation)
                .execute(&mut *tx)
                .await?
                .rows_affected();
        if changed > 0 {
            sqlx::query("INSERT INTO local_meta SELECT 'image_erasure_ready',value FROM local_meta WHERE key='image_erasure_pending' ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                .execute(&mut *tx).await?;
            sqlx::query("DELETE FROM local_meta WHERE key='image_erasure_pending'")
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub(crate) async fn memory_requests(&self) -> Result<Vec<MemoryRequest>> {
        if self
            .memory_denied
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(Vec::new());
        }
        let rows: Vec<String> = sqlx::query_scalar("SELECT request FROM local_memory_queue WHERE request IS NOT NULL ORDER BY CASE WHEN json_extract(request,'$.command.kind') IN ('forget','revoke') THEN 0 ELSE 1 END,saved_at,rowid LIMIT 200").fetch_all(&self.db).await?;
        Ok(rows
            .into_iter()
            .map(|s| serde_json::from_str(&s))
            .collect::<std::result::Result<_, _>>()?)
    }
    pub(crate) async fn finish_memory(&self, source: &str, phase: &str) -> Result<()> {
        ensure(
            matches!(phase, "accepted" | "rejected" | "redacted" | "review"),
            "invalid memory acknowledgement",
        )?;
        sqlx::query(
            "UPDATE local_memory_queue SET request=NULL,phase=? WHERE source_uuid=lower(?)",
        )
        .bind(phase)
        .bind(canonical(source)?)
        .execute(&self.db)
        .await?;
        self.flush_memory().await
    }
    pub async fn apply_memory_view(&self, view: &MemoryView, now: i64) -> Result<()> {
        let _lifecycle = self.memory_lifecycle.lock().await;
        self.apply_memory_view_locked(view, now).await
    }
    async fn apply_memory_view_locked(&self, view: &MemoryView, now: i64) -> Result<()> {
        self.require_memory_copy()?;
        let rules = self
            .core
            .memory_withdrawal_rules(&self.device, &crate::protocol::hash(&self.server)?)
            .await?;
        ensure(
            self.role == ClientRole::Chat
                && uuid::Uuid::parse_str(&view.keeper).is_ok()
                && view.memories.len() <= 100
                && view.redacted_sources.len() <= 20100,
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
        let redacted_sources = view
            .redacted_sources
            .iter()
            .map(|s| canonical(s))
            .collect::<Result<Vec<_>>>()?;
        ensure(
            view.memories
                .iter()
                .all(|m| !redacted_sources.contains(&canonical(&m.source_id).unwrap())),
            "redacted source cannot remain visible",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        require_access(&mut tx).await?;
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
        for source in &redacted_sources {
            sqlx::query(
                "UPDATE local_memory_queue SET request=NULL,phase='redacted' WHERE source_uuid=lower(?)",
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
        let hidden_sources: Vec<String> = requests
            .iter()
            .flat_map(|r| match &r.command {
                MemoryCommand::Revoke { sources, .. } => sources.clone(),
                _ => vec![],
            })
            .collect();
        let hidden: Vec<String> = requests
            .into_iter()
            .filter_map(|r| match r.command {
                MemoryCommand::Correct { id, .. } | MemoryCommand::Forget { id, .. } => Some(id),
                MemoryCommand::Revoke {
                    expected: Some(reference),
                    ..
                } => Some(reference.id),
                _ => None,
            })
            .collect();
        sqlx::query("DELETE FROM local_memory_cache")
            .execute(&mut *tx)
            .await?;
        for memory in &view.memories {
            let mut memory = memory.clone();
            memory.source_id = canonical(&memory.source_id)?;
            if rules.allows(&memory)
                && !hidden.contains(&memory.id)
                && !hidden_sources
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(&memory.source_id))
            {
                sqlx::query("INSERT INTO local_memory_cache VALUES(?,?)")
                    .bind(&memory.id)
                    .bind(serde_json::to_string(&memory)?)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        pulse_core::Store::prune_local_memory_projection_in(&mut tx, Some(now), false).await?;
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
        self.require_memory_copy()?;
        tx.commit().await?;
        self.flush_memory().await
    }
    pub(crate) async fn memory_projection(
        &self,
        state: &mut crate::protocol::NativeState,
        now: i64,
    ) -> Result<()> {
        let _lifecycle = self.memory_lifecycle.lock().await;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let removed = sqlx::query(
            "DELETE FROM local_memory_cache WHERE json_extract(data,'$.expires_at')<=?",
        )
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let expired = sqlx::query("UPDATE local_memory_queue SET request=NULL,phase='expired' WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') NOT IN ('forget','revoke') AND json_extract(request,'$.source_at')<=?").bind(now-MEMORY_RETENTION_MS).execute(&mut *tx).await?.rows_affected();
        let projection_removed =
            pulse_core::Store::prune_local_memory_projection_in(&mut tx, Some(now), false).await?;
        let visible: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='memory_visible'")
                .fetch_optional(&mut *tx)
                .await?;
        if visible.as_deref() == Some("true")
            && !self
                .memory_denied
                .load(std::sync::atomic::Ordering::Acquire)
        {
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
                    "review" => "待送來源已取消。較新的記憶已保留，請查詢後再處理。",
                    "redacted" => "舊記憶來源已清除，不再重送。",
                    "expired" => "待送記憶已到期，內容已清除。",
                    "access_denied" => "對話身分已失效，記憶已停止使用。請重新配對。",
                    "clarify" => {
                        "可用「記住 名稱：內容」、「查詢記憶」、「更正記憶 名稱：內容」、「忘記記憶 名稱」或「取消待送記憶 名稱」。"
                    }
                    "queried" => "記憶來源已核對。有效內容列在下方。",
                    _ => "記憶查詢已保存。連線後會核對目前來源。",
                };
                state.messages.push(ChatMessage {
                    memory_sources: Some(vec![]),
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
        if removed + expired > 0 || projection_removed {
            self.flush_memory().await?;
        }
        Ok(())
    }
    pub(crate) async fn flush_memory(&self) -> Result<()> {
        crate::chat_image_cache::drain(self).await?;
        let row = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetch_one(&self.db)
            .await?;
        ensure(row.get::<i64, _>(0) == 0, "memory cache checkpoint busy")
    }
    pub(crate) fn require_memory_copy(&self) -> Result<()> {
        if self
            .memory_denied
            .load(std::sync::atomic::Ordering::Acquire)
        {
            Err(crate::Error::CredentialRejected)
        } else {
            Ok(())
        }
    }
}

async fn require_access(db: &mut sqlx::SqliteConnection) -> Result<()> {
    let denied: i64 =
        sqlx::query_scalar("SELECT denied FROM local_memory_access WHERE singleton=1")
            .fetch_one(db)
            .await?;
    if denied == 0 {
        Ok(())
    } else {
        Err(crate::Error::CredentialRejected)
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
    sqlx::query(
        "UPDATE local_memory_queue SET request=NULL,phase='redacted' WHERE source_uuid=lower(?)",
    )
    .bind(canonical(&memory.source_id)?)
    .execute(db)
    .await?;
    Ok(())
}

fn canonical(source: &str) -> Result<String> {
    uuid::Uuid::parse_str(source)
        .map(|id| id.to_string())
        .map_err(|_| crate::Error::Rejected("invalid source UUID".into()))
}

async fn revoke_named(
    db: &mut sqlx::SqliteConnection,
    label: &str,
    now: i64,
    forget: bool,
) -> Result<MemoryCommand> {
    let deletes:i64=sqlx::query_scalar("SELECT count(*) FROM local_memory_queue WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') IN ('forget','revoke')").fetch_one(&mut *db).await?;
    ensure(deletes < 100, "deletion delivery capacity")?;
    let cached:Vec<String>=sqlx::query_scalar("SELECT data FROM local_memory_cache WHERE json_extract(data,'$.label')=? AND json_extract(data,'$.expires_at')>?")
        .bind(label).bind(now).fetch_all(&mut *db).await?;
    ensure(cached.len() <= 1, "ambiguous memory name")?;
    let cached: Option<Memory> = cached
        .first()
        .map(|s| serde_json::from_str(s))
        .transpose()?;
    let rows:Vec<String>=sqlx::query_scalar("SELECT request FROM local_memory_queue WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') IN ('remember','correct')").fetch_all(&mut *db).await?;
    let mut sources = Vec::new();
    let mut references = Vec::<MemoryRef>::new();
    for data in rows {
        let request: MemoryRequest = serde_json::from_str(&data)?;
        match &request.command {
            MemoryCommand::Remember { label: name, .. } if name == label => {
                sources.push(request.source_id)
            }
            MemoryCommand::Correct {
                id,
                revision,
                label: name,
                ..
            } if name == label => {
                sources.push(request.source_id);
                references.push(MemoryRef {
                    id: id.clone(),
                    revision: *revision,
                });
            }
            _ => {}
        }
    }
    let expected = if forget {
        if let Some(memory) = &cached {
            sources.push(memory.source_id.clone());
            Some(MemoryRef {
                id: memory.id.clone(),
                revision: memory.revision,
            })
        } else {
            references.sort_by(|a, b| (&a.id, a.revision).cmp(&(&b.id, b.revision)));
            references.dedup_by(|a, b| a.id == b.id && a.revision == b.revision);
            ensure(references.len() <= 1, "query latest memory before deletion")?;
            references.pop()
        }
    } else {
        None
    };
    ensure(
        !sources.is_empty() || expected.is_some(),
        "no matching pending or current memory",
    )?;
    for source in &mut sources {
        *source = uuid::Uuid::parse_str(source)
            .map_err(|_| crate::Error::Rejected("invalid source UUID".into()))?
            .to_string();
    }
    sources.sort();
    sources.dedup();
    // Plan only. Core saves bodyless evidence before it erases the local copy.
    Ok(MemoryCommand::Revoke { sources, expected })
}
