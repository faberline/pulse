//! Model proposals remain data. Only a durable Human decision can create local action work.
use crate::{
    Result,
    action_intent::ActionIntent,
    ensure,
    protocol::*,
    provider::ToolCall,
    remote_io::{ProposedRead, current},
    server::Hub,
};
use pulse_core::{
    Command, Operation, OperationProposal, OperationRequest, Origin, State, Store, Task, Token,
    Versions,
};
use serde_json::json;
use sqlx::{Row, SqliteConnection};

impl Hub {
    pub(crate) async fn propose_action(
        &self,
        token: &Token,
        call: &ToolCall,
        time: i64,
    ) -> Result<()> {
        let (task, mut run) = {
            let mut db = self.db.acquire().await?;
            current(&mut db, token, time).await?
        };
        let intent = self.action_intent(&task, &call.operation, time).await?;
        let round = run.checkpoint["round"].as_u64().unwrap_or(0);
        let mut calls: Vec<ProposedRead> =
            serde_json::from_value(run.checkpoint.get("calls").cloned().unwrap_or(json!([])))?;
        ensure(
            round < 8 && calls.len() < 8 && !calls.iter().any(|c| c.call.call_id == call.call_id),
            "browser proposal limit",
        )?;
        let request_id = format!(
            "{}-action:{}:{}:{round}",
            intent.kind(),
            token.run_id,
            token.revision
        );
        let operation_id = format!("approved:{request_id}");
        let proposal = OperationProposal {
            id: request_id.clone(),
            token: token.clone(),
            target: intent.target()?,
            expires_at: intent.expires().min(task.deadline),
            intent: intent.value()?,
        };
        let source=self.core.ingest(Origin::Tool,&format!("{}-proposal",intent.kind()),&request_id,json!({"commands":[Command::Query{ids:vec![task.id.clone()]}],"operation_proposal":proposal}),time).await?;
        self.core.route(&source.event_id).await?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let (task, current_run) = current(&mut tx, token, time).await?;
        ensure(
            task.state == State::Planning && current_run.checkpoint == run.checkpoint,
            "browser proposal changed",
        )?;
        Store::propose_operation_in(&mut tx, &source.event_id, proposal, time).await?;
        calls.push(ProposedRead {
            operation_id,
            call: call.clone(),
        });
        run.checkpoint =
            json!({"phase":"waiting_approval","round":round,"calls":calls,"request_id":request_id});
        sqlx::query("UPDATE runs SET data=? WHERE id=?")
            .bind(serde_json::to_string(&run)?)
            .bind(&token.run_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.notify_change();
        Ok(())
    }
    pub(crate) async fn project_action_approvals(&self, time: i64) -> Result<()> {
        // Do not acquire a keeper lease while holding the single SQL writer.
        let rows:Vec<String>=sqlx::query_scalar("SELECT r.data FROM operation_requests r WHERE (json_extract(r.data,'$.proposal.target') LIKE 'browser:%' OR json_extract(r.data,'$.proposal.target') LIKE 'computer:%') AND (NOT EXISTS(SELECT 1 FROM link_chat c WHERE c.id=r.id) OR (EXISTS(SELECT 1 FROM task_memory_sources b WHERE b.task_id=r.task_id AND b.task_revision=json_extract(r.data,'$.proposal.token.revision')) AND NOT EXISTS(SELECT 1 FROM memory_derived_messages d WHERE d.id=r.id))) AND NOT EXISTS(SELECT 1 FROM task_memory_sources b WHERE b.task_id=r.task_id AND b.task_revision=json_extract(r.data,'$.proposal.token.revision') AND json_extract(b.data,'$.invalidated')=1) ORDER BY r.rowid").fetch_all(&self.db).await?;
        for data in rows {
            let candidate: OperationRequest = serde_json::from_str(&data)?;
            let time = time.max(crate::now());
            let _source = match self
                .core
                .lease_task_memory_revision(
                    &candidate.proposal.token.task_id,
                    candidate.proposal.token.revision,
                    time,
                )
                .await
            {
                Ok(lease) => lease,
                Err(pulse_core::Error::Rejected(_) | pulse_core::Error::Busy) => continue,
                Err(error) => return Err(error.into()),
            };
            let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
            let time = time.max(crate::now());
            let data: Option<String> =
                sqlx::query_scalar("SELECT data FROM operation_requests WHERE id=?")
                    .bind(&candidate.proposal.id)
                    .fetch_optional(&mut *tx)
                    .await?;
            let Some(data) = data else { continue };
            let request: OperationRequest = serde_json::from_str(&data)?;
            if request != candidate {
                continue;
            }
            let body = if request.proposal.target.starts_with("computer:") {
                "程式視窗操作提議。請核對畫面、目標與效果。"
            } else {
                "瀏覽器操作提議。請核對畫面、目標與效果。"
            };
            let created = request.proposal.intent["observation"]["observed_at"]
                .as_i64()
                .ok_or_else(|| {
                    crate::Error::Rejected("proposal observation time missing".into())
                })?;
            let changed = sqlx::query("INSERT OR IGNORE INTO link_chat VALUES(?,?,?)")
                .bind(&request.proposal.id)
                .bind(body)
                .bind(created)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                > 0;
            Store::track_memory_message_in(
                &mut tx,
                &request.proposal.token.task_id,
                request.proposal.token.revision,
                &request.proposal.id,
                time,
            )
            .await?;
            tx.commit().await?;
            if changed {
                self.notify_change();
            }
        }
        Ok(())
    }
    pub(crate) async fn dispatch_approved_action(
        &self,
        versions: &Versions,
        time: i64,
    ) -> Result<bool> {
        // Never wait for a keeper lease while holding the single SQL writer.
        let rows:Vec<String>=sqlx::query_scalar("SELECT data FROM operation_requests WHERE json_extract(data,'$.state')='approved' AND (json_extract(data,'$.proposal.target') LIKE 'browser:%' OR json_extract(data,'$.proposal.target') LIKE 'computer:%') ORDER BY rowid").fetch_all(&self.db).await?;
        for data in rows {
            let candidate: OperationRequest = serde_json::from_str(&data)?;
            let time = time.max(crate::now());
            let _memory = match self
                .core
                .lease_task_memory(&candidate.proposal.token.task_id, time)
                .await
            {
                Ok(lease) => lease,
                Err(pulse_core::Error::Rejected(_) | pulse_core::Error::Busy) => continue,
                Err(error) => return Err(error.into()),
            };
            let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
            let time = time.max(crate::now());
            let data: Option<String> =
                sqlx::query_scalar("SELECT data FROM operation_requests WHERE id=?")
                    .bind(&candidate.proposal.id)
                    .fetch_optional(&mut *tx)
                    .await?;
            let Some(data) = data else { continue };
            let request: OperationRequest = serde_json::from_str(&data)?;
            if request != candidate || request.state != pulse_core::ApprovalState::Approved {
                continue;
            }
            if request.proposal.expires_at <= time {
                continue;
            }
            let intent = ActionIntent::from_request(&request)?;
            let token = &request.proposal.token;
            let (task, mut run) = match current(&mut tx, token, time).await {
                Ok(value) => value,
                Err(
                    crate::Error::Rejected(_) | crate::Error::Core(pulse_core::Error::Rejected(_)),
                ) => continue,
                Err(error) => return Err(error),
            };
            if task.state != State::Ready
                || run.versions != *versions
                || run.checkpoint["phase"] != "waiting_approval"
                || run.checkpoint["request_id"] != request.proposal.id
            {
                continue;
            }
            let worker=sqlx::query("SELECT epoch FROM link_device WHERE id=? AND role='local_executor' AND epoch=? AND revoked=0 AND stopped=0 AND lease_until>? AND NOT EXISTS(SELECT 1 FROM link_reads WHERE device=link_device.id AND state IN ('sent','accepted','unknown'))")
                .bind(intent.device()).bind(intent.epoch()? as i64).bind(time).fetch_optional(&mut *tx).await?;
            let Some(worker) = worker else { continue };
            let epoch: i64 = worker.get("epoch");
            let calls: Vec<ProposedRead> = serde_json::from_value(run.checkpoint["calls"].clone())?;
            let read = calls
                .last()
                .ok_or_else(|| crate::Error::Rejected("missing approved model call".into()))?;
            let probe = intent.operation(
                Operation {
                    id: read.operation_id.clone(),
                    idempotency_key: read.operation_id.clone(),
                    token: token.clone(),
                    target: request.proposal.target.clone(),
                    intent: request.proposal.intent.clone(),
                    intent_digest: request.intent_digest.clone(),
                    state: pulse_core::OperationState::Unknown,
                },
                request.clone(),
            );
            if !Self::local_capable(
                &mut tx,
                intent.device(),
                epoch,
                &task.id,
                &task.revision.to_string(),
                &probe,
                time,
            )
            .await?
            {
                continue;
            }
            ensure(
                read.call.operation == probe.canonical()?
                    && read.operation_id == format!("approved:{}", request.proposal.id),
                "approved call mismatch",
            )?;
            let operation = match Store::prepare_approved_operation_in(
                &mut tx,
                token,
                &request.proposal.id,
                time,
            )
            .await
            {
                Ok(operation) => operation,
                Err(pulse_core::Error::Rejected(_) | pulse_core::Error::Missing(_)) => {
                    Store::invalidate_action_request_in(
                        &mut tx,
                        token,
                        &request.proposal.id,
                        &format!("{}_approval_invalid", intent.kind()),
                        time,
                    )
                    .await?;
                    tx.commit().await?;
                    self.notify_change();
                    return Ok(true);
                }
                Err(error) => return Err(error.into()),
            };
            let data: String = sqlx::query_scalar("SELECT data FROM tasks WHERE id=?")
                .bind(&task.id)
                .fetch_one(&mut *tx)
                .await?;
            let mut dispatched: Task = serde_json::from_str(&data)?;
            dispatched
                .evidence
                .push(format!("dispatch:{}", read.operation_id));
            sqlx::query("UPDATE tasks SET data=? WHERE id=?")
                .bind(serde_json::to_string(&dispatched)?)
                .bind(&task.id)
                .execute(&mut *tx)
                .await?;
            let updated: String =
                sqlx::query_scalar("SELECT data FROM operation_requests WHERE id=?")
                    .bind(&request.proposal.id)
                    .fetch_one(&mut *tx)
                    .await?;
            let payload = intent.operation(operation, serde_json::from_str(&updated)?);
            sqlx::query("INSERT INTO link_reads VALUES(?,?,?,?,?,?,'sent',NULL)")
                .bind(&read.operation_id)
                .bind(intent.device())
                .bind(epoch)
                .bind(&task.id)
                .bind(task.revision.to_string())
                .bind(serde_json::to_string(&payload)?)
                .execute(&mut *tx)
                .await?;
            Self::enqueue(
                &mut tx,
                intent.device(),
                epoch as u64,
                ServerPayload::Read {
                    operation_id: read.operation_id.clone(),
                    task_id: task.id.clone(),
                    revision: task.revision.to_string(),
                    operation: payload,
                },
            )
            .await?;
            run.checkpoint["phase"] = json!("sent_tool");
            run.checkpoint["operation_id"] = json!(read.operation_id);
            run.checkpoint["dispatch_epoch"] = json!(token.epoch);
            sqlx::query("UPDATE runs SET data=? WHERE id=?")
                .bind(serde_json::to_string(&run)?)
                .bind(&token.run_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            self.notify_change();
            return Ok(true);
        }
        Ok(false)
    }
    pub(crate) async fn action_receipt(
        db: &mut SqliteConnection,
        operation_id: &str,
        operation: &ReadOp,
        before: &Task,
        outcome: &ReadOutcome,
    ) -> Result<bool> {
        let grant = match operation {
            ReadOp::BrowserAction {
                grant: Some(grant), ..
            } => &grant.operation,
            ReadOp::ComputerAction {
                grant: Some(grant), ..
            } => &grant.operation,
            ReadOp::BrowserAction { .. } | ReadOp::ComputerAction { .. } => {
                return Err(crate::Error::Rejected("action receipt lacks grant".into()));
            }
            _ => return Ok(true),
        };
        ensure(
            grant.id == operation_id,
            "action receipt operation mismatch",
        )?;
        let kind = if matches!(operation, ReadOp::ComputerAction { .. }) {
            "computer"
        } else {
            "browser"
        };
        match outcome {
            ReadOutcome::Unknown { .. } => Ok(false),
            ReadOutcome::Completed { .. } => {
                Store::record_receipt_in(
                    db,
                    &pulse_core::Receipt {
                        operation_id: operation_id.into(),
                        token: grant.token.clone(),
                        success: true,
                        evidence: format!("{kind}-action:{operation_id}:{}", hash(outcome)?),
                    },
                )
                .await?;
                Ok(true)
            }
            ReadOutcome::Rejected { .. } => {
                Store::action_not_executed_in(
                    db,
                    &grant.token,
                    operation_id,
                    &format!("{kind}-rejected:{}", hash(outcome)?),
                )
                .await?;
                let _ = before;
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
#[path = "computer_workflow_tests.rs"]
mod computer_tests;
