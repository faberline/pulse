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

#[derive(Clone)]
pub struct Hub {
    pub core: Store,
    pub(crate) db: SqlitePool,
    changes: tokio::sync::watch::Sender<u64>,
    human: Arc<tokio::sync::Mutex<()>>,
}
impl Hub {
    pub fn new(core: Store) -> Self {
        let db = core.application_pool();
        Self {
            core,
            db,
            changes: tokio::sync::watch::channel(0).0,
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
        sqlx::query("UPDATE runs SET data=? WHERE id=?")
            .bind(serde_json::to_string(&run)?)
            .bind(&token.run_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT OR IGNORE INTO link_chat VALUES(?,?,?)")
            .bind(id)
            .bind(format!(
                "模型回覆（{model} · {}）：\n\n{}\n\n分析已保存，任務待確認。",
                crate::conversation::label(&task),
                reply.text
            ))
            .bind(time)
            .execute(&mut *tx)
            .await?;
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
    pub async fn redeem(&self, request: PairRequest, time: i64) -> Result<Credential> {
        ensure(
            uuid::Uuid::parse_str(&request.device).is_ok() && request.code.len() == 64,
            "invalid pairing",
        )?;
        let token = secret()?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let active: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM link_device WHERE revoked=0 AND role=?")
                .bind(request.role.as_str())
                .fetch_one(&mut *tx)
                .await?;
        ensure(
            active == 0,
            "an active device already exists; operator revocation is required",
        )?;
        let changed = sqlx::query(
            "UPDATE link_pairings SET used=1 WHERE digest=? AND used=0 AND expires>? AND role=?",
        )
        .bind(hash_bytes(request.code.as_bytes()))
        .bind(time)
        .bind(request.role.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        ensure(changed == 1, "pairing expired, invalid or already used")?;
        // A stable client ID never changes roles, including after revocation.
        let old_role: Option<String> =
            sqlx::query_scalar("SELECT role FROM link_device WHERE id=?")
                .bind(&request.device)
                .fetch_optional(&mut *tx)
                .await?;
        ensure(
            old_role
                .as_deref()
                .is_none_or(|role| role == request.role.as_str()),
            "client role is immutable",
        )?;
        sqlx::query("INSERT INTO link_device(id,token_digest,role) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET token_digest=excluded.token_digest,revoked=0,epoch=epoch+1,lease_until=0")
            .bind(&request.device).bind(hash_bytes(token.as_bytes())).bind(request.role.as_str()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Credential {
            device: request.device,
            token,
            role: request.role,
        })
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
        sqlx::query("UPDATE link_device SET lease_until=0,epoch=epoch+1")
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
        let changed = sqlx::query("UPDATE link_device SET lease_until=0 WHERE id=? AND epoch=?")
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
        let row=sqlx::query("UPDATE link_device SET epoch=epoch+1,lease_until=? WHERE id=? AND revoked=0 RETURNING epoch").bind(time+LEASE_MS).bind(device).fetch_optional(&mut *tx).await?.ok_or_else(||crate::Error::Rejected("device revoked".into()))?;
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
        let local = sqlx::query(
            "SELECT lease_until,stopped FROM link_device WHERE role='local_executor' AND revoked=0",
        )
        .fetch_optional(&self.db)
        .await?
        .map(|row| LocalStatus {
            connected: row.get::<i64, _>("lease_until") > time,
            stopped: row.get::<i64, _>("stopped") != 0,
        })
        .unwrap_or_default();
        let mut tasks = self.core.list_tasks(20).await?;
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
        }
        let mut rows = sqlx::query(
            "SELECT id,body FROM link_chat ORDER BY created_at DESC,rowid DESC LIMIT 10",
        )
        .fetch_all(&self.db)
        .await?;
        rows.reverse();
        let messages = rows
            .into_iter()
            .map(|r| ChatMessage {
                id: r.get("id"),
                body: r.get("body"),
            })
            .collect();
        let mut payload = ServerPayload::Snapshot {
            tasks,
            messages,
            synced_at: time,
            local,
        };
        while serde_json::to_vec(&payload)?.len() > MAX_FRAME - 2048 {
            if let ServerPayload::Snapshot {
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
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        Self::enqueue(&mut tx, device, epoch, payload).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn outgoing(&self, device: &str) -> Result<Vec<Envelope<ServerPayload>>> {
        let rows:Vec<String>=sqlx::query_scalar("SELECT data FROM link_outbox WHERE device=? AND acknowledged=0 ORDER BY sequence LIMIT 100").bind(device).fetch_all(&self.db).await?;
        rows.into_iter().map(|s| parse(s.as_bytes())).collect()
    }
    pub async fn permitted(
        &self,
        envelope: &Envelope<ServerPayload>,
        epoch: u64,
        time: i64,
    ) -> Result<bool> {
        if let ServerPayload::Read {
            task_id, revision, ..
        } = &envelope.payload
        {
            let task = self.core.task(task_id).await?;
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
            DevicePayload::Human { source_id, action } => {
                let _human = self.human.lock().await;
                ensure(
                    !source_id.is_empty() && source_id.len() <= 128,
                    "source ID limit",
                )?;
                let commands = match action {
                    HumanAction::Submit { id, goal } => {
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
                            crate::conversation::plan(id, goal, &self.core.list_tasks(100).await?)
                        }
                    }
                    HumanAction::Cancel { task_id, revision } => vec![Command::Cancel {
                        id: task_id.clone(),
                        expected_revision: number(revision)?,
                    }],
                    HumanAction::Read { task_id, .. } => vec![Command::Query {
                        ids: vec![task_id.clone()],
                    }],
                    HumanAction::Status | HumanAction::StopLocal { .. } => vec![],
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
                ensure(
                    !matches!(outcome, pulse_core::Outcome::Rejected { .. }),
                    "task control rejected",
                )?;
                match action {
                    HumanAction::Submit { goal, .. } => {
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
                            pulse_core::Outcome::Rejected { .. } => unreachable!("rejected above"),
                        };
                        self.chat(source_id, &format!("已保存訊息：{goal}"), time)
                            .await?;
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
                        let task = self.core.task(task_id).await?;
                        ensure(
                            task.revision == number(revision)?
                                && !task.state.terminal()
                                && !task.cancel_requested
                                && task.scope == "pulse:read"
                                && task.deadline > time,
                            "read authorization invalid",
                        )?;
                        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
                        let worker = sqlx::query("SELECT id,epoch FROM link_device WHERE role='local_executor' AND revoked=0 AND stopped=0 AND lease_until>?").bind(time).fetch_optional(&mut *tx).await?.ok_or_else(||crate::Error::Rejected("local executor unavailable".into()))?;
                        let worker_id: String = worker.get("id");
                        let epoch: i64 = worker.get("epoch");
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
                    HumanAction::Status => {}
                }
            }
            DevicePayload::Result {
                operation_id,
                task_id,
                revision,
                outcome,
            } => {
                ensure(outcome.valid(), "invalid device result")?;
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
                let encoded = serde_json::to_string(outcome)?;
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
                    Store::device_observation_in(
                        &mut tx,
                        task_id,
                        number(revision)?,
                        operation_id,
                        matches!(outcome, ReadOutcome::Unknown { .. }),
                    )
                    .await?;
                    Self::continue_model_read(&mut tx, &before, operation_id, outcome, time)
                        .await?;
                } else {
                    let stored: String =
                        sqlx::query_scalar("SELECT result FROM link_reads WHERE id=?")
                            .bind(operation_id)
                            .fetch_one(&mut *tx)
                            .await?;
                    ensure(stored == encoded, "result conflict")?;
                }
                tx.commit().await?;
                let operation: ReadOp = serde_json::from_str(&row.get::<String, _>("request"))?;
                let read_label = match operation {
                    ReadOp::List { path } => format!("目錄 {path}"),
                    ReadOp::Text { path } | ReadOp::GitDiff { path } => path,
                    ReadOp::GitStatus => "Git 狀態".into(),
                };
                let body = match outcome {
                    ReadOutcome::Completed {
                        text, truncated, ..
                    } => format!(
                        "只讀工具結果 · {} · {}{}\n\n```text\n{}\n```\n\n這是工具證據，任務尚待確認。",
                        crate::conversation::label(&before),
                        read_label,
                        if *truncated { "（已截斷）" } else { "" },
                        short(text, 7000)
                    ),
                    ReadOutcome::Rejected { reason } => format!("本機拒絕：{reason}"),
                    ReadOutcome::Unknown { reason } => format!("結果未知：{reason}"),
                };
                self.chat(operation_id, &body, time).await?;
            }
        }
        Ok(())
    }
}

pub fn router(hub: Hub) -> Router {
    Router::new()
        .route("/health/live", get(|| async { "live" }))
        .route("/v1/device-pairings/redeem", post(pair))
        .route("/v1/devices/connect", get(upgrade))
        .route("/v1/tasks", get(tasks))
        .route("/v1/tasks/{id}", get(task))
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
    let Ok(device) = hub.authenticate(token).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    ws.max_message_size(MAX_FRAME)
        .max_frame_size(MAX_FRAME)
        .on_upgrade(move |socket| session(hub, device, socket))
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
        loop {tokio::select! {
            result=changes.changed()=>{
                result.map_err(|_|crate::Error::Rejected("server stopped".into()))?;
                let role:String=sqlx::query_scalar("SELECT role FROM link_device WHERE id=?").bind(&device).fetch_one(&hub.db).await?;
                if role == "chat" {hub.snapshot(&device,epoch,now()).await?;}
            }
            _=ticker.tick()=>{
                ensure(hub.live(&device,epoch,now()).await?,"revoked, expired or superseded connection")?;
                for envelope in hub.outgoing(&device).await? {let permitted=hub.permitted(&envelope,epoch,now()).await?;send(&mut socket,&ServerWire::Data{envelope:Box::new(envelope),permitted}).await?;}
            }
            frame=socket.recv()=>{
                let Some(Ok(frame))=frame else{break;};
                match frame {
                    Message::Text(text)=>{ensure(hub.live(&device,epoch,now()).await?,"inactive device")?;match parse::<ClientWire>(text.as_bytes())? {
                        ClientWire::Data{envelope}=>{hub.receive(&device,&envelope,now()).await?;send(&mut socket,&ServerWire::Ack{sequence:envelope.sequence,id:envelope.id}).await?;}
                        ClientWire::Ack{sequence,id}=>hub.ack(&device,number(&sequence)?,&id).await?,
                        ClientWire::Heartbeat{epoch:claimed}=>{ensure(number(&claimed)?==epoch,"heartbeat epoch")?;hub.renew(&device,epoch,now()).await?;send(&mut socket,&ServerWire::Renewed{epoch:epoch.to_string(),lease_ms:LEASE_MS as u32}).await?;}
                        ClientWire::Hello{..}=>return Err(crate::Error::Rejected("duplicate hello".into())),
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
fn secret() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| crate::Error::Rejected("secure randomness unavailable".into()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
fn short(s: &str, limit: usize) -> String {
    let mut end = s.len().min(limit);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].into()
}
