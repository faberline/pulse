use crate::{Result, ensure, now, protocol::*};
use axum::extract::ws::{Message, WebSocket};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use futures_util::SinkExt;
use pulse_core::{Command, Origin, Store};
use serde_json::json;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;

pub enum Delivery {
    Data(Box<Envelope<ServerPayload>>),
    Erased(pulse_core::ErasedProjection),
    ErasedWork(Box<pulse_core::WorkerErasure>),
}

#[derive(Clone)]
pub struct Hub {
    pub core: Store,
    pub(crate) db: SqlitePool,
    changes: tokio::sync::watch::Sender<u64>,
    // One active chat role. Retry source-busy projections on the existing tick.
    projection_retry: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) human: Arc<tokio::sync::Mutex<()>>,
}
impl Hub {
    pub async fn worker_erasure_ready(&self, device: &str, epoch: u64, time: i64) -> Result<()> {
        let count=sqlx::query("UPDATE link_device SET worker_erasure=1 WHERE id=? AND role='local_executor' AND epoch=? AND revoked=0 AND lease_until>?").bind(device).bind(epoch as i64).bind(time).execute(&self.db).await?.rows_affected();
        ensure(count == 1, "worker erasure requires current local lease")?;
        self.notify_change();
        Ok(())
    }
    pub async fn flush_worker_erasures(&self, device: &str, epoch: u64) -> Result<()> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let ready:i64=sqlx::query_scalar("SELECT count(*) FROM link_device WHERE id=? AND epoch=? AND worker_erasure=1 AND role='local_executor' AND revoked=0 AND lease_until>?").bind(device).bind(epoch as i64).bind(now()).fetch_one(&mut *tx).await?;
        if ready == 0 {
            return Ok(());
        }
        let contexts:Vec<String>=sqlx::query_scalar("SELECT e.context FROM memory_erased_reads e JOIN link_reads r ON r.id=e.id WHERE r.device=? AND e.context IS NOT NULL AND NOT EXISTS(SELECT 1 FROM link_worker_fences f WHERE f.operation_id=e.id) LIMIT 8").bind(device).fetch_all(&mut *tx).await?;
        for data in contexts {
            let context: pulse_core::WorkerErasure = serde_json::from_str(&data)?;
            Self::enqueue(
                &mut tx,
                device,
                epoch,
                ServerPayload::EraseWork {
                    context: context.clone(),
                },
            )
            .await?;
            sqlx::query("INSERT INTO link_worker_fences SELECT ?,device,sequence FROM link_outbox WHERE device=? ORDER BY sequence DESC LIMIT 1").bind(&context.operation_id).bind(device).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn receive_erased_worker(
        &self,
        device: &str,
        header: &pulse_core::ErasedProjection,
        operation_id: &str,
        source_id: &str,
    ) -> Result<()> {
        ensure(
            header.valid() && header.device == device,
            "invalid erased worker header",
        )?;
        let _source = self
            .core
            .lease_read_source_state(operation_id, now())
            .await?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let row=sqlx::query("SELECT r.device,r.epoch,e.context,d.in_cursor,d.role,d.revoked FROM link_reads r JOIN memory_erased_reads e ON e.id=r.id JOIN link_device d ON d.id=r.device WHERE r.id=?").bind(operation_id).fetch_one(&mut *tx).await?;
        let context: pulse_core::WorkerErasure =
            serde_json::from_str(&row.get::<String, _>("context"))?;
        ensure(
            context.valid()
                && row.get::<String, _>("device") == device
                && row.get::<String, _>("role") == "local_executor"
                && row.get::<i64, _>("revoked") == 0
                && row.get::<i64, _>("epoch").to_string() == header.epoch
                && context.source_id == source_id,
            "erased worker source mismatch",
        )?;
        let sequence = number(&header.sequence)?;
        let cursor = row.get::<i64, _>("in_cursor") as u64;
        if let Some(row) =
            sqlx::query("SELECT id,digest FROM link_inbox WHERE device=? AND sequence=?")
                .bind(device)
                .bind(sequence as i64)
                .fetch_optional(&mut *tx)
                .await?
        {
            ensure(
                row.get::<String, _>("id") == header.id
                    && row.get::<String, _>("digest") == header.binding_digest,
                "erased worker replay conflict",
            )?;
        } else {
            ensure(sequence == cursor + 1, "erased worker sequence gap")?;
            sqlx::query("INSERT INTO link_inbox(device,sequence,id,digest,outcome) VALUES(?,?,?,?,'source_erased')").bind(device).bind(sequence as i64).bind(&header.id).bind(&header.binding_digest).execute(&mut *tx).await?;
        }
        if sequence > cursor {
            ensure(sequence == cursor + 1, "erased worker sequence gap")?;
            sqlx::query("UPDATE link_device SET in_cursor=? WHERE id=?")
                .bind(sequence as i64)
                .bind(device)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn projection_erasure_ready(
        &self,
        device: &str,
        epoch: u64,
        time: i64,
    ) -> Result<()> {
        let count=sqlx::query("UPDATE link_device SET projection_erasure=1 WHERE id=? AND role='chat' AND epoch=? AND revoked=0 AND lease_until>?").bind(device).bind(epoch as i64).bind(time).execute(&self.db).await?.rows_affected();
        ensure(
            count == 1,
            "projection erasure requires a current chat lease",
        )?;
        self.notify_change();
        Ok(())
    }
    pub async fn erased_projection(
        &self,
        device: &str,
        sequence: &str,
    ) -> Result<Option<pulse_core::ErasedProjection>> {
        let data: Option<String> =
            sqlx::query_scalar("SELECT data FROM link_erased_frames WHERE device=? AND sequence=?")
                .bind(device)
                .bind(number(sequence)? as i64)
                .fetch_optional(&self.db)
                .await?;
        data.map(|s| serde_json::from_str(&s).map_err(crate::Error::from))
            .transpose()
    }
    pub fn new(core: Store) -> Self {
        let db = core.application_pool();
        Self {
            core,
            db,
            changes: tokio::sync::watch::channel(0).0,
            projection_retry: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            human: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    fn changed(&self) {
        self.changes
            .send_modify(|version| *version = version.wrapping_add(1));
    }
    pub(crate) fn notify_change(&self) {
        self.changed();
    }
    pub async fn model_reply(
        &self,
        token: &pulse_core::Token,
        reply: &crate::provider::Reply,
        model: &str,
        time: i64,
    ) -> Result<()> {
        let _memory = self.core.lease_task_memory(&token.task_id, time).await?;
        ensure(
            reply.tool.is_none(),
            "tool proposal cannot be a final reply",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let data: String = sqlx::query_scalar("SELECT data FROM tasks WHERE id=?")
            .bind(&token.task_id)
            .fetch_one(&mut *tx)
            .await?;
        let mut task: pulse_core::Task = serde_json::from_str(&data)?;
        let data: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=?")
            .bind(&token.run_id)
            .fetch_one(&mut *tx)
            .await?;
        let mut run: pulse_core::Run = serde_json::from_str(&data)?;
        ensure(
            task.revision == token.revision
                && !task.cancel_requested
                && task.deadline > time
                && task.state == pulse_core::State::Planning
                && task.active_run.as_deref() == Some(&token.run_id)
                && run.token == *token,
            "stale model reply",
        )?;
        pulse_core::Store::validate_task_memory_in(&mut tx, &task.id, task.revision, time).await?;
        let id = format!(
            "model:{}:{}:{}",
            token.run_id,
            token.revision,
            run.checkpoint["round"].as_u64().unwrap_or(0)
        );
        run.checkpoint["phase"] = json!("reply_saved");
        run.checkpoint["model"] = json!(model);
        run.checkpoint["reply"] = json!(reply.text);
        run.checkpoint["usage"] = reply.usage.clone();
        task.state = pulse_core::State::Blocked;
        task.reason = Some("model_reply_ready".into());
        task.evidence.push(id.clone());
        sqlx::query("UPDATE tasks SET data=? WHERE id=?")
            .bind(serde_json::to_string(&task)?)
            .bind(&task.id)
            .execute(&mut *tx)
            .await?;
        let input = Self::task_input_in(&mut tx, &task).await?;
        let source = input
            .map(|input| format!("\n\n{}", input.source_label()))
            .unwrap_or_default();
        let chat = format!(
            "模型回覆（{model} · {}）：\n\n{}{source}\n\n分析已保存，任務待確認。",
            crate::conversation::label(&task),
            reply.text
        );
        if let Some(result) = Self::make_text_result(&mut tx, &task, &run, &id, &chat, time).await?
        {
            let result = serde_json::to_value(result)?;
            let mut candidate = run.checkpoint.clone();
            candidate["text_result"] = result;
            if serde_json::to_vec(&candidate)?.len() <= 64 * 1024 {
                run.checkpoint = candidate;
            }
        }
        sqlx::query("UPDATE runs SET data=? WHERE id=?")
            .bind(serde_json::to_string(&run)?)
            .bind(&token.run_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT OR IGNORE INTO link_chat VALUES(?,?,?)")
            .bind(&id)
            .bind(chat)
            .bind(time)
            .execute(&mut *tx)
            .await?;
        Store::track_memory_message_in(&mut tx, &task.id, task.revision, &id, time).await?;
        tx.commit().await?;
        self.changed();
        Ok(())
    }
    pub async fn pairing(&self, role: ClientRole, time: i64) -> Result<String> {
        let code = secret()?;
        sqlx::query("INSERT INTO link_pairings(digest,expires,role) VALUES(?,?,?)")
            .bind(hash_bytes(code.as_bytes()))
            .bind(time + 600_000)
            .bind(role.as_str())
            .execute(&self.db)
            .await?;
        Ok(code)
    }
    pub async fn authenticate(&self, token: &str) -> Result<String> {
        ensure(token.len() == 64, "invalid device credential")?;
        sqlx::query_scalar("SELECT id FROM link_device WHERE token_digest=? AND revoked=0")
            .bind(hash_bytes(token.as_bytes()))
            .fetch_optional(&self.db)
            .await?
            .ok_or_else(|| crate::Error::Rejected("credential revoked or invalid".into()))
    }
    pub async fn revoke(&self, device: &str) -> Result<()> {
        sqlx::query("UPDATE link_device SET revoked=1,epoch=epoch+1,lease_until=0 WHERE id=?")
            .bind(device)
            .execute(&self.db)
            .await?;
        self.unknown_reads(device, None).await?;
        self.changed();
        Ok(())
    }
    async fn unknown_reads(&self, device: &str, epoch: Option<u64>) -> Result<()> {
        let rows=sqlx::query("SELECT id,task_id,task_revision FROM link_reads WHERE device=? AND state IN ('sent','accepted') AND (? IS NULL OR epoch=?)")
            .bind(device).bind(epoch.map(|e|e as i64)).bind(epoch.map(|e|e as i64)).fetch_all(&self.db).await?;
        for row in rows {
            let id: String = row.get("id");
            let task: String = row.get("task_id");
            let revision: String = row.get("task_revision");
            let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
            let changed =
                sqlx::query("UPDATE link_reads SET state='unknown' WHERE id=? AND result IS NULL")
                    .bind(&id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
            if changed == 1 {
                Store::device_observation_in(&mut tx, &task, number(&revision)?, &id, true).await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }
    pub async fn recover(&self) -> Result<()> {
        sqlx::query("UPDATE link_device SET worker_erasure=0,projection_erasure=0,schedule_notices=0,lease_until=0,epoch=epoch+1")
            .execute(&self.db)
            .await?;
        let devices: Vec<String> = sqlx::query_scalar("SELECT id FROM link_device")
            .fetch_all(&self.db)
            .await?;
        for device in devices {
            self.unknown_reads(&device, None).await?;
        }
        self.changed();
        Ok(())
    }
    pub async fn end(&self, device: &str, epoch: u64) -> Result<()> {
        let changed = sqlx::query(
            "UPDATE link_device SET worker_erasure=0,projection_erasure=0,schedule_notices=0,lease_until=0 WHERE id=? AND epoch=?",
        )
        .bind(device)
        .bind(epoch as i64)
        .execute(&self.db)
        .await?
        .rows_affected();
        if changed == 1 {
            self.unknown_reads(device, Some(epoch)).await?;
            self.changed();
        }
        Ok(())
    }
    pub async fn begin(&self, device: &str, cursor: u64, time: i64) -> Result<u64> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let max: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(sequence),0) FROM link_outbox WHERE device=?")
                .bind(device)
                .fetch_one(&mut *tx)
                .await?;
        ensure(cursor <= max as u64, "resume cursor exceeds sent data")?;
        Self::reset_conversation_capability(&mut tx, device).await?;
        let row=sqlx::query("UPDATE link_device SET worker_erasure=0,projection_erasure=0,schedule_notices=0,epoch=epoch+1,lease_until=? WHERE id=? AND revoked=0 RETURNING epoch").bind(time+LEASE_MS).bind(device).fetch_optional(&mut *tx).await?.ok_or_else(||crate::Error::Rejected("device revoked".into()))?;
        let epoch: i64 = row.get("epoch");
        sqlx::query("UPDATE link_outbox SET acknowledged=1 WHERE device=? AND sequence<=?")
            .bind(device)
            .bind(cursor as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.changed();
        Ok(epoch as u64)
    }
    pub async fn live(&self, device: &str, epoch: u64, time: i64) -> Result<bool> {
        let n:i64=sqlx::query_scalar("SELECT COUNT(*) FROM link_device WHERE id=? AND epoch=? AND revoked=0 AND lease_until>?").bind(device).bind(epoch as i64).bind(time).fetch_one(&self.db).await?;
        Ok(n == 1)
    }
    pub async fn renew(&self, device: &str, epoch: u64, time: i64) -> Result<()> {
        let n=sqlx::query("UPDATE link_device SET lease_until=? WHERE id=? AND epoch=? AND revoked=0 AND lease_until>?").bind(time+LEASE_MS).bind(device).bind(epoch as i64).bind(time).execute(&self.db).await?.rows_affected();
        ensure(n == 1, "lease expired or fenced")
    }
    pub(crate) async fn enqueue(
        db: &mut sqlx::SqliteConnection,
        device: &str,
        epoch: u64,
        payload: ServerPayload,
    ) -> Result<()> {
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM link_outbox WHERE device=? AND acknowledged=0",
        )
        .bind(device)
        .fetch_one(&mut *db)
        .await?;
        if pending >= 1000 {
            return Err(crate::Error::Capacity);
        }
        let last: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(sequence),0) FROM link_outbox WHERE device=?")
                .bind(device)
                .fetch_one(&mut *db)
                .await?;
        let envelope = Envelope::new(device, last as u64 + 1, epoch, payload)?;
        envelope.validate()?;
        sqlx::query("INSERT INTO link_outbox(device,sequence,id,data) VALUES(?,?,?,?)")
            .bind(device)
            .bind(last + 1)
            .bind(&envelope.id)
            .bind(serde_json::to_string(&envelope)?)
            .execute(db)
            .await?;
        Ok(())
    }
    async fn chat(&self, id: &str, body: &str, time: i64) -> Result<()> {
        let body = short(body, 8192);
        sqlx::query("INSERT OR IGNORE INTO link_chat VALUES(?,?,?)")
            .bind(id)
            .bind(body)
            .bind(time)
            .execute(&self.db)
            .await?;
        Ok(())
    }
    pub async fn snapshot(&self, device: &str, epoch: u64, time: i64) -> Result<()> {
        // Never wait for the keeper while holding the single SQL writer.
        let time = time.max(now());
        let (_source, covered, mut retry_projection) =
            self.projection_lease(device, epoch, time).await?;
        // Projection reads and the domain-notification handoff share one committed view.
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let time = time.max(now());
        let erasure_ready: i64 =
            sqlx::query_scalar("SELECT projection_erasure FROM link_device WHERE id=?")
                .bind(device)
                .fetch_one(&mut *tx)
                .await?;
        let active_chat:i64=sqlx::query_scalar("SELECT count(*) FROM link_device WHERE id=? AND role='chat' AND revoked=0 AND lease_until>?")
            .bind(device).bind(time).fetch_one(&mut *tx).await?;
        let notices_ready: i64 =
            sqlx::query_scalar("SELECT schedule_notices FROM link_device WHERE id=?")
                .bind(device)
                .fetch_one(&mut *tx)
                .await?;
        let mut more_notices = false;
        if active_chat == 1 && notices_ready == 1 {
            let notices=sqlx::query("SELECT sequence,payload FROM outbox WHERE acknowledged=0 AND json_extract(payload,'$.kind')='schedule_due' ORDER BY sequence LIMIT 8").fetch_all(&mut *tx).await?;
            more_notices = notices.len() == 8;
            for row in notices {
                let payload: serde_json::Value =
                    serde_json::from_str(&row.get::<String, _>("payload"))?;
                let notice: pulse_core::ScheduleNotice =
                    serde_json::from_value(payload["notice"].clone())?;
                ensure(notice.valid(), "invalid durable schedule notice")?;
                Self::enqueue(
                    &mut tx,
                    device,
                    epoch,
                    ServerPayload::ScheduleNotice { notice },
                )
                .await?;
                sqlx::query("UPDATE outbox SET acknowledged=1 WHERE sequence=?")
                    .bind(row.get::<i64, _>("sequence"))
                    .execute(&mut *tx)
                    .await?;
            }
        }
        let local = sqlx::query(
            "SELECT lease_until,stopped FROM link_device WHERE role='local_executor' AND revoked=0",
        )
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| LocalStatus {
            connected: row.get::<i64, _>("lease_until") > time,
            stopped: row.get::<i64, _>("stopped") != 0,
        })
        .unwrap_or_default();
        let data: Vec<String> = sqlx::query_scalar(
            "SELECT data FROM tasks ORDER BY json_extract(data,'$.created_at') DESC,id LIMIT 20",
        )
        .fetch_all(&mut *tx)
        .await?;
        let mut tasks = data
            .into_iter()
            .map(|data| serde_json::from_str::<pulse_core::Task>(&data))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for task in &mut tasks {
            task.goal = short(&task.goal, 2048);
            task.decisions.clear();
            task.evidence = task
                .evidence
                .iter()
                .rev()
                .take(3)
                .map(|s| short(s, 256))
                .collect();
            let bound: i64 =
                sqlx::query_scalar("SELECT count(*) FROM task_memory_sources WHERE task_id=?")
                    .bind(&task.id)
                    .fetch_one(&mut *tx)
                    .await?;
            if bound > 0 {
                task.evidence.clear();
                if !matches!(
                    task.reason.as_deref(),
                    None | Some("memory_source_withdrawn" | "memory_source_unavailable")
                ) {
                    task.reason = Some("memory_context_review".into());
                }
            }
        }
        let mut rows = crate::server_projection::messages(&mut tx, time).await?;
        rows.reverse();
        let mut messages = Vec::new();
        let mut memory_sources = std::collections::BTreeMap::new();
        for row in rows {
            let id: String = row.get("id");
            let redacted: i64 = row.get("redacted");
            if redacted == 1 {
                messages.push(ChatMessage {
                    id,
                    body: "記憶來源已移除，回覆內容已清除。".into(),
                    approval: None,
                    artifact: None,
                    artifact_operation_id: None,
                });
                continue;
            }
            if let Some(lineage) = row.get::<Option<String>, _>("lineage") {
                if erasure_ready == 0 {
                    continue;
                }
                let binding: pulse_core::TaskMemorySources = serde_json::from_str(&lineage)?;
                match Store::validate_task_memory_in(
                    &mut tx,
                    &binding.request.task_id,
                    binding.request.task_revision,
                    time,
                )
                .await
                {
                    Ok(()) => {}
                    Err(pulse_core::Error::Rejected(_)) => continue,
                    Err(error) => return Err(error.into()),
                }
                if _source.is_none() || binding.sources.iter().any(|s| !covered.contains(s)) {
                    // A new message may have appeared after the candidate read.
                    // Keep ordinary messages and retry on the existing bounded tick.
                    retry_projection = true;
                    continue;
                }
                for source in binding.sources {
                    memory_sources.insert(source.source_id.clone(), source);
                }
            }
            let approval: Option<String> =
                sqlx::query_scalar("SELECT data FROM operation_requests WHERE id=?")
                    .bind(&id)
                    .fetch_optional(&mut *tx)
                    .await?;
            let approval: Option<pulse_core::OperationRequest> = approval
                .map(|data| serde_json::from_str(&data))
                .transpose()?;
            let source_id = if let Some(request) = approval
                .as_ref()
                .filter(|r| r.proposal.target.starts_with("browser:"))
            {
                let intent: pulse_core::BrowserIntent =
                    serde_json::from_value(request.proposal.intent.clone())?;
                intent.source_operation_id
            } else if let Some(request) = approval
                .as_ref()
                .filter(|r| r.proposal.target.starts_with("computer:"))
            {
                let intent: pulse_core::ComputerIntent =
                    serde_json::from_value(request.proposal.intent.clone())?;
                intent.observation.binding.operation_id
            } else {
                id.clone()
            };
            let metadata:Option<String>=sqlx::query_scalar("SELECT a.metadata FROM link_artifacts a JOIN link_reads r ON a.operation_id=r.id WHERE a.operation_id=? AND r.state='completed' AND a.png IS NOT NULL").bind(&source_id).fetch_optional(&mut *tx).await?;
            messages.push(ChatMessage {
                approval,
                artifact_operation_id: (source_id != id).then_some(source_id),
                id,
                body: row.get("body"),
                artifact: metadata.map(|s| serde_json::from_str(&s)).transpose()?,
            });
        }
        let mut payload = if memory_sources.is_empty() {
            ServerPayload::Snapshot {
                tasks,
                messages,
                synced_at: time,
                local,
            }
        } else {
            ServerPayload::DerivedSnapshot {
                tasks,
                messages,
                synced_at: time,
                local,
                sources: memory_sources.into_values().collect(),
            }
        };
        while serde_json::to_vec(&payload)?.len() > MAX_FRAME - 2048 {
            if let ServerPayload::Snapshot {
                tasks, messages, ..
            }
            | ServerPayload::DerivedSnapshot {
                tasks, messages, ..
            } = &mut payload
            {
                if !messages.is_empty() {
                    messages.remove(0);
                } else if !tasks.is_empty() {
                    tasks.pop();
                } else {
                    return Err(crate::Error::Capacity);
                }
            }
        }
        Self::enqueue(&mut tx, device, epoch, payload).await?;
        // This ACK means durable transfer of coalescible state updates, never user read/acceptance.
        // Human journal replays may carry their original epoch; the current device lease owns delivery.
        let active_chat: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_device WHERE id=? AND role='chat' AND revoked=0 AND lease_until>?")
            .bind(device).bind(time).fetch_one(&mut *tx).await?;
        if active_chat == 1 {
            let cursor: Option<String> = sqlx::query_scalar(
                "SELECT value FROM settings WHERE key='chat_core_outbox_cursor'",
            )
            .fetch_optional(&mut *tx)
            .await?;
            let cursor = cursor
                .as_deref()
                .unwrap_or("0")
                .parse::<i64>()
                .map_err(|_| crate::Error::Rejected("invalid core notification cursor".into()))?;
            ensure(cursor >= 0, "invalid core notification cursor")?;
            let allocated: i64 = sqlx::query_scalar(
                "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name='outbox'),0)",
            )
            .fetch_one(&mut *tx)
            .await?;
            ensure(
                cursor <= allocated,
                "core notification cursor exceeds its history",
            )?;
            let latest: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(sequence),0) FROM outbox")
                .fetch_one(&mut *tx)
                .await?;
            if latest > cursor {
                sqlx::query("UPDATE outbox SET acknowledged=1 WHERE acknowledged=0 AND sequence>? AND sequence<=? AND coalesce(json_extract(payload,'$.kind'),'')!='schedule_due'")
                    .bind(cursor).bind(latest).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO settings(key,value) VALUES('chat_core_outbox_cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                    .bind(latest.to_string()).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        if retry_projection {
            self.projection_retry
                .store(true, std::sync::atomic::Ordering::Release);
        }
        // Watch updates can merge many timer occurrences. Drain the next bounded
        // batch even when no further Human input or timer event arrives.
        if more_notices {
            self.notify_change();
        }
        Ok(())
    }
    pub async fn outgoing(&self, device: &str) -> Result<Vec<Envelope<ServerPayload>>> {
        let mut output = Vec::new();
        for item in self.deliveries(device).await? {
            match item {
                Delivery::Data(envelope) => output.push(*envelope),
                Delivery::Erased(_) | Delivery::ErasedWork(_) => break,
            }
        }
        Ok(output)
    }
    pub async fn deliveries(&self, device: &str) -> Result<Vec<Delivery>> {
        let rows=sqlx::query("SELECT o.data,e.data AS erased,w.context AS erased_work FROM link_outbox o LEFT JOIN link_erased_frames e ON e.device=o.device AND e.sequence=o.sequence LEFT JOIN link_erased_work w ON w.device=o.device AND w.sequence=o.sequence WHERE o.device=? AND o.acknowledged=0 ORDER BY o.sequence LIMIT 100").bind(device).fetch_all(&self.db).await?;
        let ready: i64 = sqlx::query_scalar("SELECT schedule_notices FROM link_device WHERE id=?")
            .bind(device)
            .fetch_one(&self.db)
            .await?;
        let mut outgoing = Vec::new();
        let erased_ready: i64 =
            sqlx::query_scalar("SELECT projection_erasure FROM link_device WHERE id=?")
                .bind(device)
                .fetch_one(&self.db)
                .await?;
        let worker_ready: i64 =
            sqlx::query_scalar("SELECT worker_erasure FROM link_device WHERE id=?")
                .bind(device)
                .fetch_one(&self.db)
                .await?;
        for row in rows {
            if let Some(data) = row.get::<Option<String>, _>("erased_work") {
                if worker_ready == 0 {
                    break;
                }
                let context: pulse_core::WorkerErasure = serde_json::from_str(&data)?;
                ensure(
                    context.valid() && context.device == device,
                    "invalid erased work context",
                )?;
                ensure(context.read_header.is_some(), "missing erased work header")?;
                outgoing.push(Delivery::ErasedWork(Box::new(context)));
                continue;
            }
            if let Some(data) = row.get::<Option<String>, _>("erased") {
                if erased_ready == 0 {
                    break;
                }
                let header: pulse_core::ErasedProjection = serde_json::from_str(&data)?;
                ensure(
                    header.valid() && header.device == device,
                    "invalid erased projection",
                )?;
                outgoing.push(Delivery::Erased(header));
                continue;
            }
            let envelope: Envelope<ServerPayload> = parse(row.get::<String, _>("data").as_bytes())?;
            if let ServerPayload::Read {
                task_id, revision, ..
            } = &envelope.payload
            {
                let bound: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM task_memory_sources WHERE task_id=? AND task_revision=?",
                )
                .bind(task_id)
                .bind(revision)
                .fetch_one(&self.db)
                .await?;
                if bound > 0 && worker_ready == 0 {
                    break;
                }
            }
            if worker_ready == 0 && matches!(envelope.payload, ServerPayload::EraseWork { .. }) {
                break;
            }
            if ready == 0 && matches!(envelope.payload, ServerPayload::ScheduleNotice { .. }) {
                break;
            }
            if erased_ready == 0
                && matches!(envelope.payload, ServerPayload::DerivedSnapshot { .. })
            {
                break;
            }
            outgoing.push(Delivery::Data(Box::new(envelope)));
        }
        Ok(outgoing)
    }
    pub async fn permitted(
        &self,
        envelope: &Envelope<ServerPayload>,
        epoch: u64,
        time: i64,
    ) -> Result<bool> {
        let _source = if let ServerPayload::Read { operation_id, .. } = &envelope.payload {
            match self.core.lease_read_source_state(operation_id, time).await {
                Ok(lease) => lease,
                Err(pulse_core::Error::Rejected(_)) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        self.permitted_current(envelope, epoch, time).await
    }
    async fn permitted_current(
        &self,
        envelope: &Envelope<ServerPayload>,
        epoch: u64,
        time: i64,
    ) -> Result<bool> {
        if let ServerPayload::Read {
            task_id,
            revision,
            operation_id,
            ..
        } = &envelope.payload
        {
            let erased: i64 =
                sqlx::query_scalar("SELECT count(*) FROM memory_erased_reads WHERE id=?")
                    .bind(operation_id)
                    .fetch_one(&self.db)
                    .await?;
            if erased == 1 {
                return Ok(false);
            }
            let task = self.core.task(task_id).await?;
            let mut db = self.db.acquire().await?;
            match pulse_core::Store::validate_task_memory_in(&mut db, &task.id, task.revision, time)
                .await
            {
                Ok(()) => {}
                Err(pulse_core::Error::Rejected(_)) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
            drop(db);
            return Ok(number(&envelope.epoch)? == epoch
                && self.live(&envelope.device, epoch, time).await?
                && sqlx::query_scalar::<_, i64>("SELECT stopped FROM link_device WHERE id=?")
                    .bind(&envelope.device)
                    .fetch_one(&self.db)
                    .await?
                    == 0
                && task.revision.to_string() == *revision
                && !task.cancel_requested
                && !task.state.terminal()
                && task.deadline > time);
        }
        Ok(true)
    }
    pub async fn ack(&self, device: &str, sequence: u64, id: &str) -> Result<()> {
        let n = sqlx::query(
            "UPDATE link_outbox SET acknowledged=1 WHERE device=? AND sequence=? AND id=?",
        )
        .bind(device)
        .bind(sequence as i64)
        .bind(id)
        .execute(&self.db)
        .await?
        .rows_affected();
        ensure(n == 1, "cloud ACK identity mismatch")
    }
    pub async fn receive(
        &self,
        device: &str,
        envelope: &Envelope<DevicePayload>,
        time: i64,
    ) -> Result<()> {
        let (sequence, _) = envelope.validate()?;
        ensure(envelope.device == device, "wrong device")?;
        let role: String =
            sqlx::query_scalar("SELECT role FROM link_device WHERE id=? AND revoked=0")
                .bind(device)
                .fetch_one(&self.db)
                .await?;
        ensure(
            matches!(
                (&envelope.payload, role.as_str()),
                (DevicePayload::Human { .. }, "chat")
                    | (DevicePayload::Result { .. }, "local_executor")
                    | (DevicePayload::ArtifactChunk { .. }, "local_executor")
            ),
            "client role cannot send this payload",
        )?;
        {
            let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
            let row = sqlx::query("SELECT in_cursor FROM link_device WHERE id=? AND revoked=0")
                .bind(device)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| crate::Error::Rejected("device revoked".into()))?;
            let cursor: i64 = row.get("in_cursor");
            if sequence <= cursor as u64 {
                let r =
                    sqlx::query("SELECT id,digest FROM link_inbox WHERE device=? AND sequence=?")
                        .bind(device)
                        .bind(sequence as i64)
                        .fetch_one(&mut *tx)
                        .await?;
                ensure(
                    r.get::<String, _>("id") == envelope.id
                        && r.get::<String, _>("digest") == envelope.binding_digest,
                    "device replay conflict",
                )?;
                return Ok(());
            }
            ensure(sequence == cursor as u64 + 1, "device sequence gap")?;
            if let Some(r) =
                sqlx::query("SELECT id,digest FROM link_inbox WHERE device=? AND sequence=?")
                    .bind(device)
                    .bind(sequence as i64)
                    .fetch_optional(&mut *tx)
                    .await?
            {
                ensure(
                    r.get::<String, _>("id") == envelope.id
                        && r.get::<String, _>("digest") == envelope.binding_digest,
                    "in-flight replay conflict",
                )?;
            } else {
                sqlx::query("INSERT INTO link_inbox(device,sequence,id,digest) VALUES(?,?,?,?)")
                    .bind(device)
                    .bind(sequence as i64)
                    .bind(&envelope.id)
                    .bind(&envelope.binding_digest)
                    .execute(&mut *tx)
                    .await?;
            }
            tx.commit().await?;
        }
        // Core ingest and link delivery use separate transactions. Stable source IDs
        // make the gap replayable; no ACK is emitted until all commits finish.
        let outcome = match self.apply(device, envelope, time).await {
            Ok(()) => "accepted",
            Err(
                crate::Error::Rejected(_)
                | crate::Error::Core(
                    pulse_core::Error::Rejected(_)
                    | pulse_core::Error::Conflict(_)
                    | pulse_core::Error::Missing(_),
                ),
            ) => {
                self.chat(
                    match &envelope.payload {
                        DevicePayload::Human { source_id, .. } if source_id.len() <= 128 => {
                            source_id
                        }
                        _ => &envelope.id,
                    },
                    "要求已拒絕：範圍、修訂或操作狀態不符。",
                    time,
                )
                .await?;
                "rejected"
            }
            Err(error) => return Err(error),
        };
        // Cache delivery must also survive a crash before the durable cursor/ACK.
        self.snapshot(device, number(&envelope.epoch)?, time)
            .await?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("UPDATE link_inbox SET outcome=? WHERE device=? AND sequence=?")
            .bind(outcome)
            .bind(device)
            .bind(sequence as i64)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE link_device SET in_cursor=? WHERE id=?")
            .bind(sequence as i64)
            .bind(device)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.changed();
        Ok(())
    }
    async fn apply(
        &self,
        device: &str,
        envelope: &Envelope<DevicePayload>,
        time: i64,
    ) -> Result<()> {
        match &envelope.payload {
            DevicePayload::ArtifactChunk {
                operation_id,
                artifact,
                index,
                data,
            } => {
                self.artifact_chunk(
                    device,
                    number(&envelope.epoch)?,
                    operation_id,
                    artifact,
                    *index,
                    data,
                )
                .await?
            }
            DevicePayload::Human { source_id, action } => {
                let _human = self.human.lock().await;
                ensure(
                    !source_id.is_empty() && source_id.len() <= 128,
                    "source ID limit",
                )?;
                let replay: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM events WHERE source=? AND source_id=? AND origin='human')",
                )
                .bind(format!("device:{device}"))
                .bind(source_id)
                .fetch_one(&self.db)
                .await?;
                let context = if replay {
                    None
                } else {
                    self.take_conversation_context(device, time).await?
                };
                let mut pending_targets = None;
                let commands = match action {
                    HumanAction::SubmitTextFile { id, goal, input } => {
                        input.validate_goal(goal)?;
                        ensure(
                            id.len() <= 128 && !id.is_empty(),
                            "file input task ID invalid",
                        )?;
                        let prior:Option<String>=sqlx::query_scalar("SELECT payload FROM events WHERE source=? AND source_id=? AND origin='human'")
                            .bind(format!("device:{device}")).bind(source_id).fetch_optional(&self.db).await?;
                        if let Some(prior) = prior {
                            let prior: serde_json::Value = serde_json::from_str(&prior)?;
                            ensure(
                                prior["native_action"] == serde_json::to_value(action)?,
                                "file source identity conflict",
                            )?;
                            serde_json::from_value(prior["commands"].clone())?
                        } else {
                            let mut commands = crate::conversation::plan(
                                id,
                                goal,
                                &self.core.list_tasks(100).await?,
                            );
                            for command in &mut commands {
                                if let Command::Create {
                                    scope,
                                    requires_device,
                                    ..
                                } = command
                                {
                                    *scope = input.scope();
                                    *requires_device = false;
                                }
                            }
                            commands
                        }
                    }
                    HumanAction::Schedule { request, text } => {
                        ensure(text.len() <= 4096, "schedule input size")?;
                        let expected =
                            crate::schedule_chat::prepare(&request.id, text, request.source_at)
                                .ok_or_else(|| {
                                crate::Error::Rejected("schedule input mismatch".into())
                            })??;
                        ensure(expected == *request, "schedule input mismatch")?;
                        vec![Command::Schedule {
                            request: request.clone(),
                        }]
                    }
                    HumanAction::DecideOperation { binding, approve } => vec![if *approve {
                        Command::ApproveOperation {
                            binding: binding.clone(),
                        }
                    } else {
                        Command::RejectOperation {
                            binding: binding.clone(),
                        }
                    }],
                    HumanAction::Submit { id, goal } => {
                        ensure(
                            crate::memory_chat::parse(goal).is_none(),
                            "memory control requires current chat client",
                        )?;
                        ensure(
                            id.len() <= 128 && !goal.trim().is_empty() && goal.len() <= 4096,
                            "task size",
                        )?;
                        // Replays use the durable original plan, not newer task revisions.
                        let prior:Option<String>=sqlx::query_scalar("SELECT payload FROM events WHERE source=? AND source_id=? AND origin='human'").bind(format!("device:{device}")).bind(source_id).fetch_optional(&self.db).await?;
                        if let Some(prior) = prior {
                            let payload: serde_json::Value = serde_json::from_str(&prior)?;
                            ensure(
                                payload["native_action"] == serde_json::to_value(action)?,
                                "source identity conflict",
                            )?;
                            serde_json::from_value(payload["commands"].clone())?
                        } else {
                            if let Some(commands) =
                                crate::schedule_chat::plan(&self.core, id, goal).await?
                            {
                                commands
                            } else {
                                let (commands, pending) = crate::conversation_context::plan(
                                    id,
                                    goal,
                                    &self.core.list_tasks(100).await?,
                                    context.as_ref(),
                                );
                                pending_targets = pending;
                                commands
                            }
                        }
                    }
                    HumanAction::SubmitComputer {
                        id,
                        goal,
                        selection,
                    } => {
                        ensure(
                            crate::memory_chat::parse(goal).is_none()
                                && !crate::schedule_chat::reserved(goal),
                            "memory control cannot create a computer task",
                        )?;
                        ensure(
                            !id.is_empty()
                                && id.len() <= 128
                                && !goal.trim().is_empty()
                                && goal.len() <= 4096,
                            "computer task size",
                        )?;
                        vec![Command::Create {
                            id: id.clone(),
                            goal: goal.clone(),
                            scope: selection.scope().ok_or_else(|| {
                                crate::Error::Rejected("invalid computer selection".into())
                            })?,
                            priority: 0,
                            requires_device: false,
                            parent: None,
                            dependencies: vec![],
                        }]
                    }
                    HumanAction::RebindComputer {
                        task_id,
                        revision,
                        selection,
                    } => {
                        let prior:Option<String>=sqlx::query_scalar("SELECT payload FROM events WHERE source=? AND source_id=? AND origin='human'").bind(format!("device:{device}")).bind(source_id).fetch_optional(&self.db).await?;
                        if let Some(prior) = prior {
                            let payload: serde_json::Value = serde_json::from_str(&prior)?;
                            ensure(
                                payload["native_action"] == serde_json::to_value(action)?,
                                "source identity conflict",
                            )?;
                            serde_json::from_value(payload["commands"].clone())?
                        } else {
                            let task = self.core.task(task_id).await?;
                            let next = crate::computer_selection::rebind_revision(
                                &task, revision, selection, time,
                            )?;
                            vec![if next == task.revision {
                                Command::Query {
                                    ids: vec![task_id.clone()],
                                }
                            } else {
                                Command::Revise {
                                    id: task_id.clone(),
                                    expected_revision: task.revision,
                                    goal: task.goal.clone(),
                                    scope: selection.scope(),
                                }
                            }]
                        }
                    }
                    HumanAction::Cancel { task_id, revision } => vec![Command::Cancel {
                        id: task_id.clone(),
                        expected_revision: number(revision)?,
                    }],
                    HumanAction::Read { task_id, .. } => vec![Command::Query {
                        ids: vec![task_id.clone()],
                    }],
                    HumanAction::Status
                    | HumanAction::ConversationBoundary {}
                    | HumanAction::StopLocal { .. } => vec![],
                };
                let event = self
                    .core
                    .ingest(
                        Origin::Human,
                        &format!("device:{device}"),
                        source_id,
                        json!({"commands":commands,"native_action":action}),
                        time,
                    )
                    .await?;
                let outcome = self.core.route(&event.event_id).await?;
                if !replay
                    && matches!(
                        action,
                        HumanAction::Submit { .. } | HumanAction::SubmitTextFile { .. }
                    )
                {
                    self.save_conversation_context(
                        device,
                        &event.event_id,
                        &outcome,
                        pending_targets,
                        time,
                    )
                    .await?;
                }
                if let (
                    pulse_core::Outcome::Rejected { reason },
                    HumanAction::Submit { goal, .. }
                    | HumanAction::SubmitTextFile { goal, .. }
                    | HumanAction::SubmitComputer { goal, .. }
                    | HumanAction::Schedule { text: goal, .. },
                ) = (&outcome, action)
                {
                    let notice = match reason.as_str() {
                        "read outcome is still unknown" | "missing read receipt" => {
                            "結果仍未知。請等待本機回報；目前不會重新執行。"
                        }
                        "model cost is still unknown; keep reservations" => {
                            "模型費用仍待核對。已保留預算，未重新呼叫。"
                        }
                        "read evidence must be reviewed first" => {
                            "請先核對已保存的工具證據，再要求繼續。"
                        }
                        "no local outcome to review" => "沒有待核對的本機結果。任務保持原狀。",
                        _ => "要求未執行。請查詢目前任務狀態，核對對象與限制。",
                    };
                    self.chat(source_id, &format!("已保存訊息：{goal}"), time)
                        .await?;
                    self.chat(&format!("reply:{source_id}"), notice, time)
                        .await?;
                }
                ensure(
                    !matches!(outcome, pulse_core::Outcome::Rejected { .. }),
                    "task control rejected",
                )?;
                match action {
                    HumanAction::RebindComputer { task_id, .. } => {
                        let task = self.core.task(task_id).await?;
                        self.chat(
                            source_id,
                            &format!(
                                "視窗選擇已保存。\n{}\n原任務、費用與期限會保留。",
                                crate::conversation::status(&task)
                            ),
                            time,
                        )
                        .await?;
                    }
                    HumanAction::DecideOperation { binding, approve } => {
                        let applied = matches!(&outcome,pulse_core::Outcome::Applied{task_ids} if task_ids.contains(&binding.task_id));
                        self.chat(
                            source_id,
                            &format!(
                                "{}：{} · {}",
                                if applied {
                                    "操作決定已保存"
                                } else {
                                    "操作決定未套用，請更新畫面"
                                },
                                binding.request_id,
                                if *approve { "批准" } else { "拒絕" }
                            ),
                            time,
                        )
                        .await?;
                    }
                    HumanAction::Submit { goal, .. }
                    | HumanAction::SubmitTextFile { goal, .. }
                    | HumanAction::SubmitComputer { goal, .. }
                    | HumanAction::Schedule { text: goal, .. } => {
                        let body = match &outcome {
                            pulse_core::Outcome::Clarification { question } => question.clone(),
                            pulse_core::Outcome::Applied { task_ids } => {
                                let mut lines = vec!["訊息已保存。".to_owned()];
                                for id in task_ids {
                                    let task = self.core.task(id).await?;
                                    lines.push(crate::conversation::status(&task));
                                }
                                lines.join("\n")
                            }
                            pulse_core::Outcome::Schedules {
                                task_ids,
                                schedule_ids,
                            } => {
                                let mut lines = vec![if schedule_ids.is_empty() {
                                    "目前沒有排程。".to_string()
                                } else {
                                    "排程指令已保存。".to_string()
                                }];
                                for id in schedule_ids.iter().take(10) {
                                    lines.push(crate::schedule_chat::status(
                                        &self.core.schedule(id).await?,
                                    ));
                                }
                                if schedule_ids.len() > 10 {
                                    lines.push(format!(
                                        "另有 {} 筆；可用「查詢排程 名稱或識別」查看。",
                                        schedule_ids.len() - 10
                                    ));
                                }
                                for id in task_ids {
                                    lines.push(crate::conversation::status(
                                        &self.core.task(id).await?,
                                    ));
                                }
                                lines.join("\n")
                            }
                            pulse_core::Outcome::Rejected { .. } => unreachable!("rejected above"),
                        };
                        let saved = match action {
                            HumanAction::SubmitTextFile { input, .. } => {
                                format!("已保存訊息：{goal}\n\n{}", input.source_label())
                            }
                            _ => format!("已保存訊息：{goal}"),
                        };
                        self.chat(source_id, &saved, time).await?;
                        self.chat(&format!("reply:{source_id}"), &body, time)
                            .await?
                    }
                    HumanAction::Read {
                        task_id,
                        revision,
                        operation,
                    } => {
                        let id = format!("read:{source_id}");
                        let existing: i64 =
                            sqlx::query_scalar("SELECT COUNT(*) FROM link_reads WHERE id=?")
                                .bind(&id)
                                .fetch_one(&self.db)
                                .await?;
                        if existing > 0 {
                            return Ok(());
                        }
                        let _memory = self.core.lease_task_memory(task_id, time).await?;
                        let task = self.core.task(task_id).await?;
                        ensure(
                            task.revision == number(revision)?
                                && !task.state.terminal()
                                && !task.cancel_requested
                                && (task.scope == "pulse:read"
                                    || pulse_core::ComputerSelection::from_scope(&task.scope)
                                        .is_some())
                                && !operation.is_effect()
                                && operation.permitted_by(&task.scope)
                                && task.deadline > time,
                            "read authorization invalid",
                        )?;
                        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
                        let worker = sqlx::query("SELECT id,epoch FROM link_device WHERE role='local_executor' AND revoked=0 AND stopped=0 AND lease_until>?").bind(time).fetch_optional(&mut *tx).await?.ok_or_else(||crate::Error::Rejected("local executor unavailable".into()))?;
                        let worker_id: String = worker.get("id");
                        let epoch: i64 = worker.get("epoch");
                        if let ReadOp::ComputerInspect { selection } = operation {
                            ensure(
                                Self::computer_capable(
                                    &mut tx, &worker_id, epoch, task_id, revision, selection, time,
                                )
                                .await?,
                                "computer task lacks local Human selection",
                            )?;
                        }
                        let existing: i64 =
                            sqlx::query_scalar("SELECT COUNT(*) FROM link_reads WHERE id=?")
                                .bind(&id)
                                .fetch_one(&mut *tx)
                                .await?;
                        if existing == 0 {
                            let pending:i64=sqlx::query_scalar("SELECT COUNT(*) FROM link_reads WHERE device=? AND state IN ('sent','accepted','unknown')").bind(&worker_id).fetch_one(&mut *tx).await?;
                            ensure(pending == 0, "device worker already has unresolved work")?;
                            Store::authorize_device_read(
                                &mut tx,
                                task_id,
                                number(revision)?,
                                &id,
                                time,
                            )
                            .await?;
                            sqlx::query("INSERT INTO link_reads VALUES(?,?,?,?,?,?, 'sent',NULL)")
                                .bind(&id)
                                .bind(&worker_id)
                                .bind(epoch)
                                .bind(task_id)
                                .bind(revision)
                                .bind(serde_json::to_string(operation)?)
                                .execute(&mut *tx)
                                .await?;
                            Self::enqueue(
                                &mut tx,
                                &worker_id,
                                epoch as u64,
                                ServerPayload::Read {
                                    operation_id: id,
                                    task_id: task_id.clone(),
                                    revision: revision.clone(),
                                    operation: operation.clone(),
                                },
                            )
                            .await?;
                        }
                        tx.commit().await?;
                    }
                    HumanAction::Cancel { .. } => {
                        self.chat(source_id, "取消要求已保存；結果未知的工作仍需核對。", time)
                            .await?
                    }
                    HumanAction::StopLocal { stopped } => {
                        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
                        let worker = sqlx::query("UPDATE link_device SET stopped=? WHERE role='local_executor' AND revoked=0 RETURNING id,epoch").bind(*stopped).fetch_optional(&mut *tx).await?.ok_or_else(||crate::Error::Rejected("local executor is not paired".into()))?;
                        Self::enqueue(
                            &mut tx,
                            &worker.get::<String, _>("id"),
                            worker.get::<i64, _>("epoch") as u64,
                            ServerPayload::SetStopped { stopped: *stopped },
                        )
                        .await?;
                        tx.commit().await?;
                    }
                    HumanAction::Status | HumanAction::ConversationBoundary {} => {}
                }
            }
            DevicePayload::Result {
                operation_id,
                task_id,
                revision,
                outcome,
            } => {
                ensure(outcome.valid(), "invalid device result")?;
                let _source = self
                    .core
                    .lease_read_source_state(operation_id, time)
                    .await?;
                let row = sqlx::query(
                    "SELECT device,epoch,task_id,task_revision,request,result FROM link_reads WHERE id=?",
                )
                .bind(operation_id)
                .fetch_optional(&self.db)
                .await?
                .ok_or_else(|| crate::Error::Rejected("unknown read operation".into()))?;
                ensure(
                    row.get::<String, _>("device") == device
                        && row.get::<i64, _>("epoch") as u64 == number(&envelope.epoch)?
                        && row.get::<String, _>("task_id") == *task_id
                        && row.get::<String, _>("task_revision") == *revision,
                    "result binding mismatch",
                )?;
                let mut erased_tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
                if Store::discard_erased_read_result_in(
                    &mut erased_tx,
                    operation_id,
                    matches!(outcome, ReadOutcome::Unknown { .. }),
                )
                .await?
                {
                    erased_tx.commit().await?;
                    return Ok(());
                }
                erased_tx.rollback().await?;
                let encoded = serde_json::to_string(outcome)?;
                let operation: ReadOp = serde_json::from_str(&row.get::<String, _>("request"))?;
                let artifact = if operation.computer_selection().is_some()
                    && matches!(outcome, ReadOutcome::Completed { .. })
                {
                    let row = sqlx::query("SELECT metadata,png FROM link_artifacts WHERE operation_id=? AND png IS NOT NULL")
                        .bind(operation_id).fetch_optional(&self.db).await?
                        .ok_or_else(|| crate::Error::Rejected("computer original image is incomplete".into()))?;
                    let artifact: crate::artifact::ArtifactRef =
                        serde_json::from_str(&row.get::<String, _>("metadata"))?;
                    artifact.verify(&row.get::<Vec<u8>, _>("png"))?;
                    Some(artifact)
                } else {
                    None
                };
                crate::computer_transport::validate_result(
                    &ServerPayload::Read {
                        operation_id: operation_id.clone(),
                        task_id: task_id.clone(),
                        revision: revision.clone(),
                        operation: operation.clone(),
                    },
                    device,
                    number(&envelope.epoch)?,
                    outcome,
                    artifact.as_ref(),
                )?;
                if let Some(old) = row.get::<Option<String>, _>("result") {
                    ensure(old == encoded, "result conflict")?;
                }
                let source=self.core.ingest(Origin::Tool,&format!("device-result:{device}"),operation_id,json!({"commands":[Command::Query{ids:vec![task_id.clone()]}],"result":outcome}),time).await?;
                self.core.route(&source.event_id).await?;
                let state = match outcome {
                    ReadOutcome::Unknown { .. } => "unknown",
                    ReadOutcome::Completed { .. } => "completed",
                    ReadOutcome::Rejected { .. } => "rejected",
                };
                let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
                let before_data: String = sqlx::query_scalar("SELECT data FROM tasks WHERE id=?")
                    .bind(task_id)
                    .fetch_one(&mut *tx)
                    .await?;
                let before: pulse_core::Task = serde_json::from_str(&before_data)?;
                let changed = sqlx::query(
                    "UPDATE link_reads SET state=?,result=? WHERE id=? AND result IS NULL",
                )
                .bind(state)
                .bind(&encoded)
                .bind(operation_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if changed == 1 {
                    let continue_run =
                        Self::action_receipt(&mut tx, operation_id, &operation, &before, outcome)
                            .await?;
                    Store::device_observation_in(
                        &mut tx,
                        task_id,
                        number(revision)?,
                        operation_id,
                        matches!(outcome, ReadOutcome::Unknown { .. }),
                    )
                    .await?;
                    if continue_run {
                        Self::continue_model_read(&mut tx, &before, operation_id, outcome, time)
                            .await?;
                    }
                    if operation.is_effect() && matches!(outcome, ReadOutcome::Rejected { .. }) {
                        let data: String = sqlx::query_scalar("SELECT data FROM tasks WHERE id=?")
                            .bind(task_id)
                            .fetch_one(&mut *tx)
                            .await?;
                        let mut task: pulse_core::Task = serde_json::from_str(&data)?;
                        if task.revision == number(revision)?
                            && !task.cancel_requested
                            && !task.state.terminal()
                        {
                            task.reason = Some(
                                if matches!(operation, ReadOp::ComputerAction { .. }) {
                                    "computer_action_not_executed"
                                } else {
                                    "browser_action_not_executed"
                                }
                                .into(),
                            );
                            sqlx::query("UPDATE tasks SET data=? WHERE id=?")
                                .bind(serde_json::to_string(&task)?)
                                .bind(task_id)
                                .execute(&mut *tx)
                                .await?;
                        }
                    }
                } else {
                    let stored: String =
                        sqlx::query_scalar("SELECT result FROM link_reads WHERE id=?")
                            .bind(operation_id)
                            .fetch_one(&mut *tx)
                            .await?;
                    ensure(stored == encoded, "result conflict")?;
                }
                let evidence_label = if operation.is_effect() {
                    "操作工具結果"
                } else {
                    "只讀工具結果"
                };
                let read_label = match operation {
                    ReadOp::List { path } => format!("目錄 {path}"),
                    ReadOp::Text { path } | ReadOp::GitDiff { path } => path,
                    ReadOp::GitStatus => "Git 狀態".into(),
                    ReadOp::Browser { url } => format!("網頁 {url}"),
                    ReadOp::BrowserInspect { url } => format!("觀察網頁 {url}"),
                    ReadOp::ComputerInspect { selection } => format!(
                        "程式視窗 {} · {}",
                        selection.target.bundle_id, selection.target.window_id
                    ),
                    ReadOp::BrowserAction { .. } => "已批准的瀏覽器操作".into(),
                    ReadOp::ComputerAction { .. } => "已批准的程式視窗操作".into(),
                };
                let body = match outcome {
                    ReadOutcome::Completed {
                        text, truncated, ..
                    } => format!(
                        "{} · {} · {}{}\n\n```text\n{}\n```\n\n這是工具證據，任務尚待確認。",
                        evidence_label,
                        crate::conversation::label(&before),
                        read_label,
                        if *truncated { "（已截斷）" } else { "" },
                        short(text, 7000)
                    ),
                    ReadOutcome::Rejected { reason } => format!("本機拒絕：{reason}"),
                    ReadOutcome::Unknown { reason } => format!("結果未知：{reason}"),
                };
                sqlx::query("INSERT OR IGNORE INTO link_chat VALUES(?,?,?)")
                    .bind(operation_id)
                    .bind(short(&body, 8192))
                    .bind(time)
                    .execute(&mut *tx)
                    .await?;
                Store::track_memory_message_in(
                    &mut tx,
                    task_id,
                    number(revision)?,
                    operation_id,
                    time,
                )
                .await?;
                tx.commit().await?;
            }
        }
        Ok(())
    }
}

pub fn router(hub: Hub) -> Router {
    Router::new()
        .route("/health/live", get(|| async { "live" }))
        .route("/v1/device-pairings/redeem", post(pair))
        .route("/v1/device-pairings/claim", post(crate::pairing::claim))
        .route(
            "/v1/device-pairings/identity",
            get(crate::pairing::identity),
        )
        .route("/v1/devices/connect", get(upgrade))
        .route("/v1/tasks", get(tasks))
        .route(
            "/v1/memories",
            get(crate::memory_chat::server::view).post(crate::memory_chat::server::change),
        )
        .route("/v1/tasks/{id}", get(task))
        .route("/v1/artifacts/{id}/{digest}", get(artifact))
        .route("/v1/text-results/{id}", get(text_result_manifest))
        .route("/v1/text-results/{id}/{digest}", get(text_result))
        .layer(DefaultBodyLimit::max(MAX_FRAME))
        .with_state(Arc::new(hub))
}
fn bearer(headers: &HeaderMap) -> Result<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| crate::Error::Rejected("missing bearer".into()))
}
async fn pair(State(hub): State<Arc<Hub>>, bytes: Bytes) -> impl IntoResponse {
    let result = match parse::<PairRequest>(&bytes) {
        Ok(request) => hub.redeem(request, now()).await,
        Err(e) => Err(e),
    };
    match result {
        Ok(credential) => (StatusCode::OK, axum::Json(json!(credential))),
        Err(_) => (
            StatusCode::FORBIDDEN,
            axum::Json(json!({"error":"pairing rejected"})),
        ),
    }
}
async fn artifact(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path((id, digest)): Path<(String, String)>,
) -> axum::response::Response {
    if let Ok(token) = bearer(&headers)
        && let Ok(device) = hub.authenticate(token).await
        && let Ok(png) = hub.artifact_png(&device, &id, &digest, now()).await
    {
        return (
            [
                ("content-type", "image/png"),
                ("cache-control", "no-store"),
                ("x-content-type-options", "nosniff"),
            ],
            png,
        )
            .into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}
async fn text_result(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path((id, digest)): Path<(String, String)>,
) -> axum::response::Response {
    if let Ok(token) = bearer(&headers)
        && let Ok(device) = hub.authenticate(token).await
        && let Ok(bytes) = hub.text_result_bytes(&device, &id, &digest, now()).await
    {
        return (
            [
                ("content-type", "text/markdown; charset=utf-8"),
                ("cache-control", "no-store"),
                ("x-content-type-options", "nosniff"),
            ],
            bytes,
        )
            .into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}
async fn text_result_manifest(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> axum::response::Response {
    if let Ok(token) = bearer(&headers)
        && let Ok(device) = hub.authenticate(token).await
        && let Ok(manifest) = hub.text_result_manifest(&device, &id, now()).await
    {
        return (
            [
                ("cache-control", "no-store"),
                ("x-content-type-options", "nosniff"),
            ],
            axum::Json(manifest),
        )
            .into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}
async fn tasks(State(hub): State<Arc<Hub>>, headers: HeaderMap) -> impl IntoResponse {
    if let Ok(token) = bearer(&headers)
        && hub.authenticate(token).await.is_ok()
        && let Ok(tasks) = hub.core.list_tasks(100).await
    {
        return (StatusCode::OK, axum::Json(json!(tasks)));
    }
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(json!({"error":"unauthorized"})),
    )
}
async fn task(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Ok(token) = bearer(&headers)
        && hub.authenticate(token).await.is_ok()
        && let Ok(task) = hub.core.task(&id).await
    {
        return (StatusCode::OK, axum::Json(json!(task)));
    }
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(json!({"error":"unavailable"})),
    )
}
async fn upgrade(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    let Ok(token) = bearer(&headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let device = match hub.authenticate(token).await {
        Ok(device) => device,
        Err(error) => {
            return authentication_response(&error, StatusCode::UNAUTHORIZED);
        }
    };
    ws.max_message_size(MAX_FRAME)
        .max_frame_size(MAX_FRAME)
        .on_upgrade(move |socket| session(hub, device, socket))
}

/// A storage failure is not evidence that an identity was revoked. Clients may
/// erase local source content on denial, so transient failures must remain 503.
pub(crate) fn authentication_response(
    error: &crate::Error,
    denied: StatusCode,
) -> axum::response::Response {
    let definitive = matches!(
        error,
        crate::Error::Rejected(_) | crate::Error::CredentialRejected
    );
    let mut response = if definitive {
        denied
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
    .into_response();
    if definitive {
        response
            .headers_mut()
            .insert("x-pulse-credential-denied", "1".parse().unwrap());
    }
    response
}
async fn session(hub: Arc<Hub>, device: String, mut socket: WebSocket) {
    let mut session_epoch = None;
    let mut changes = hub.changes.subscribe();
    let result=async {
        let first=tokio::time::timeout(std::time::Duration::from_secs(10),socket.recv()).await.map_err(|_|crate::Error::Rejected("hello timeout".into()))?.ok_or_else(||crate::Error::Rejected("no hello".into()))?.map_err(|_|crate::Error::Rejected("invalid socket".into()))?;
        let Message::Text(text)=first else{return Err(crate::Error::Rejected("hello required".into()));};
        let ClientWire::Hello{device:claimed,cursor}=parse(text.as_bytes())? else{return Err(crate::Error::Rejected("hello required".into()));};ensure(claimed==device,"wrong hello device")?;
        let epoch=hub.begin(&device,number(&cursor)?,now()).await?;session_epoch=Some(epoch);
        send(&mut socket,&ServerWire::Welcome{device:device.clone(),epoch:epoch.to_string(),lease_ms:LEASE_MS as u32}).await?;
        hub.snapshot(&device,epoch,now()).await?;
        let mut ticker=tokio::time::interval(std::time::Duration::from_millis(150));
        let mut projection_retry_at=tokio::time::Instant::now()+std::time::Duration::from_secs(1);
        loop {tokio::select! {
            result=changes.changed()=>{
                result.map_err(|_|crate::Error::Rejected("server stopped".into()))?;
                let role:String=sqlx::query_scalar("SELECT role FROM link_device WHERE id=?").bind(&device).fetch_one(&hub.db).await?;
                if role == "chat" {hub.snapshot(&device,epoch,now()).await?;}
            }
            _=ticker.tick()=>{
                ensure(hub.live(&device,epoch,now()).await?,"revoked, expired or superseded connection")?;
                let role:String=sqlx::query_scalar("SELECT role FROM link_device WHERE id=?").bind(&device).fetch_one(&hub.db).await?;
                if role == "chat" && tokio::time::Instant::now()>=projection_retry_at
                    && hub.projection_retry.swap(false,std::sync::atomic::Ordering::AcqRel) {
                    hub.snapshot(&device,epoch,now()).await?;
                    projection_retry_at=tokio::time::Instant::now()+std::time::Duration::from_secs(1);
                }
                match hub.flush_worker_erasures(&device,epoch).await {Ok(())|Err(crate::Error::Capacity)=>{},Err(error)=>return Err(error)}
                for delivery in hub.deliveries(&device).await? {
                    match delivery {
                        Delivery::ErasedWork(context)=>{let header=context.read_header.clone().ok_or_else(||crate::Error::Rejected("missing erased work header".into()))?;send(&mut socket,&ServerWire::ErasedWork{header,context}).await?;},
                        Delivery::Erased(header)=>send(&mut socket,&ServerWire::ErasedProjection{header}).await?,
                        Delivery::Data(envelope)=>{
                            if let ServerPayload::Read{operation_id,..}=&envelope.payload {
                                let _source=hub.core.lease_read_source_state(operation_id,now()).await?;
                                let context:Option<String>=sqlx::query_scalar("SELECT context FROM memory_erased_reads WHERE id=?").bind(operation_id).fetch_optional(&hub.db).await?;
                                if let Some(data)=context {
                                    let context:pulse_core::WorkerErasure=serde_json::from_str(&data)?;
                                    let header=context.read_header.clone().ok_or_else(||crate::Error::Rejected("erased work header missing".into()))?;
                                    send(&mut socket,&ServerWire::ErasedWork{header,context:Box::new(context)}).await?;
                                } else {
                                    let permitted=hub.permitted_current(&envelope,epoch,now()).await?;
                                    send(&mut socket,&ServerWire::Data{envelope,permitted}).await?;
                                }
                            } else if let ServerPayload::DerivedSnapshot{sources,..}=&envelope.payload {
                                match hub.core.lease_projection_sources(sources,now()).await {
                                    Ok(_lease)=>send(&mut socket,&ServerWire::Data{envelope,permitted:true}).await?,
                                    Err(pulse_core::Error::Rejected(reason))=>{
                                        if let Some(header)=hub.erased_projection(&device,&envelope.sequence).await? {send(&mut socket,&ServerWire::ErasedProjection{header}).await?;}
                                        else{return Err(crate::Error::Rejected(reason));}
                                    }
                                    Err(error)=>return Err(error.into()),
                                }
                            } else {let permitted=hub.permitted(&envelope,epoch,now()).await?;send(&mut socket,&ServerWire::Data{envelope,permitted}).await?;}
                        }
                    }
                }
            }
            frame=socket.recv()=>{
                let Some(Ok(frame))=frame else{break;};
                match frame {
                    Message::Text(text)=>{ensure(hub.live(&device,epoch,now()).await?,"inactive device")?;match parse::<ClientWire>(text.as_bytes())? {
                        ClientWire::Data{envelope}=>{hub.receive(&device,&envelope,now()).await?;send(&mut socket,&ServerWire::Ack{sequence:envelope.sequence,id:envelope.id}).await?;}
                        ClientWire::Ack{sequence,id}=>hub.ack(&device,number(&sequence)?,&id).await?,
                        ClientWire::Heartbeat{epoch:claimed}=>{ensure(number(&claimed)?==epoch,"heartbeat epoch")?;hub.renew(&device,epoch,now()).await?;send(&mut socket,&ServerWire::Renewed{epoch:epoch.to_string(),lease_ms:LEASE_MS as u32}).await?;}
                        ClientWire::Hello{..}=>return Err(crate::Error::Rejected("duplicate hello".into())),
                        ClientWire::ScheduleReady=>hub.schedule_ready(&device,epoch,now()).await?,
                        ClientWire::ConversationReady{version}=>hub.conversation_ready(&device,epoch,version,now()).await?,
                        ClientWire::WorkerErasureReady=>hub.worker_erasure_ready(&device,epoch,now()).await?,
                        ClientWire::ErasedData{header,operation_id,source_id}=>{hub.receive_erased_worker(&device,&header,&operation_id,&source_id).await?;send(&mut socket,&ServerWire::Ack{sequence:header.sequence,id:header.id}).await?;}
                        ClientWire::ProjectionErasureReady=>hub.projection_erasure_ready(&device,epoch,now()).await?,
                        ClientWire::ComputerReady{permits,interactions}=>hub.set_computer_capability(&device,epoch,permits,interactions,now()).await?,
                        ClientWire::BrowserReady{fixture_origin}=>hub.browser_ready(&device,epoch,fixture_origin.as_deref(),now()).await?,
                        ClientWire::BrowserInteractionReady{fixture_origin}=>hub.browser_interactive_ready(&device,epoch,fixture_origin.as_deref(),now()).await?,
                    }}
                    Message::Close(_)=>break,
                    Message::Ping(data)=>{socket.send(Message::Pong(data)).await.map_err(|_|crate::Error::Rejected("socket closed".into()))?;}
                    Message::Pong(_)=>{},
                    _=>return Err(crate::Error::Rejected("binary frames not supported".into())),
                }
            }
        }}Ok::<_,crate::Error>(())
    }.await;
    if let Some(epoch) = session_epoch {
        let _ = hub.end(&device, epoch).await;
    }
    if result.is_err() {
        let _=send(&mut socket,&ServerWire::Error{reason:"connection stopped: authentication, sequence, capacity or protocol check failed".into()}).await;
    }
    let _ = socket.close().await;
}
async fn send(socket: &mut WebSocket, message: &ServerWire) -> Result<()> {
    let bytes = serde_json::to_string(message)?;
    ensure(bytes.len() <= MAX_FRAME, "outbound frame limit")?;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.send(Message::Text(bytes.into())),
    )
    .await
    .map_err(|_| crate::Error::Rejected("socket write timeout".into()))?
    .map_err(|_| crate::Error::Rejected("socket write failed".into()))
}
fn short(s: &str, limit: usize) -> String {
    let mut end = s.len().min(limit);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].into()
}
