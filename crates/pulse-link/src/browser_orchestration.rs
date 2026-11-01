//! Model proposals remain data. Only a durable Human decision can create local action work.
use crate::{
    Result,
    browser_session::BrowserGrant,
    ensure,
    protocol::*,
    provider::ToolCall,
    remote_io::{ProposedRead, current},
    server::Hub,
};
use pulse_core::{
    BrowserIntent, Command, Operation, OperationProposal, OperationRequest, Origin, State, Store,
    Task, Token, Versions,
};
use serde_json::json;
use sqlx::{Row, SqliteConnection};

impl Hub {
    pub(crate) async fn propose_browser_action(
        &self,
        token: &Token,
        call: &ToolCall,
        time: i64,
    ) -> Result<()> {
        let ReadOp::BrowserAction {
            session_id,
            nonce,
            element_id,
            effect,
            grant: None,
        } = &call.operation
        else {
            return Err(crate::Error::Rejected("model grant forbidden".into()));
        };
        ensure(
            call.operation.canonical()? == call.operation,
            "canonical browser action required",
        )?;
        let (task, mut run) = {
            let mut db = self.db.acquire().await?;
            current(&mut db, token, time).await?
        };
        ensure(
            task.state == State::Planning
                && task.tool_calls < 16
                && call.operation.permitted_by(&task.scope),
            "browser proposal scope or state",
        )?;
        let row=sqlx::query("SELECT id,device,epoch,result FROM link_reads WHERE task_id=? AND task_revision=? AND state='completed' AND json_extract(result,'$.observation.session_id')=? AND json_extract(result,'$.observation.nonce')=? ORDER BY rowid DESC LIMIT 1")
            .bind(&task.id).bind(task.revision.to_string()).bind(session_id).bind(nonce).fetch_optional(&self.db).await?.ok_or_else(||crate::Error::Rejected("browser observation unavailable".into()))?;
        let outcome: ReadOutcome = serde_json::from_str(&row.get::<String, _>("result"))?;
        ensure(outcome.valid(), "invalid browser evidence")?;
        let ReadOutcome::Completed {
            observation: Some(observation),
            ..
        } = outcome
        else {
            return Err(crate::Error::Rejected("browser observation missing".into()));
        };
        let intent = BrowserIntent {
            source_operation_id: row.get("id"),
            local_device: row.get("device"),
            local_epoch: row.get::<i64, _>("epoch").to_string(),
            observation,
            element_id: element_id.clone(),
            effect: effect.clone(),
        };
        ensure(
            intent.valid(&task.scope, time),
            "browser intent outside observation",
        )?;
        let round = run.checkpoint["round"].as_u64().unwrap_or(0);
        let mut calls: Vec<ProposedRead> =
            serde_json::from_value(run.checkpoint.get("calls").cloned().unwrap_or(json!([])))?;
        ensure(
            round < 8 && calls.len() < 8 && !calls.iter().any(|c| c.call.call_id == call.call_id),
            "browser proposal limit",
        )?;
        let request_id = format!("browser-action:{}:{}:{round}", token.run_id, token.revision);
        let operation_id = format!("approved:{request_id}");
        let proposal = OperationProposal {
            id: request_id.clone(),
            token: token.clone(),
            target: intent.target(),
            expires_at: intent.observation.expires_at.min(task.deadline),
            intent: serde_json::to_value(intent)?,
        };
        let source=self.core.ingest(Origin::Tool,"browser-proposal",&request_id,json!({"commands":[Command::Query{ids:vec![task.id.clone()]}],"operation_proposal":proposal}),time).await?;
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
    pub(crate) async fn project_browser_approvals(&self, _time: i64) -> Result<()> {
        // Also recovers the post-commit chat projection gap without another model call.
        let changed=sqlx::query("INSERT OR IGNORE INTO link_chat(id,body,created_at) SELECT id,'瀏覽器操作提議。請核對畫面、目標與效果。',json_extract(data,'$.proposal.intent.observation.observed_at') FROM operation_requests WHERE json_extract(data,'$.proposal.target') LIKE 'browser:%' ORDER BY json_extract(data,'$.proposal.intent.observation.observed_at'),id")
            .execute(&self.db).await?.rows_affected();
        if changed > 0 {
            self.notify_change();
        }
        Ok(())
    }
    pub(crate) async fn dispatch_approved_browser(
        &self,
        versions: &Versions,
        time: i64,
    ) -> Result<bool> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let rows:Vec<String>=sqlx::query_scalar("SELECT data FROM operation_requests WHERE json_extract(data,'$.state')='approved' AND json_extract(data,'$.proposal.target') LIKE 'browser:%' ORDER BY rowid").fetch_all(&mut *tx).await?;
        for data in rows {
            let request: OperationRequest = serde_json::from_str(&data)?;
            if request.proposal.expires_at <= time {
                continue;
            }
            let intent: BrowserIntent = serde_json::from_value(request.proposal.intent.clone())?;
            let token = &request.proposal.token;
            let (task, mut run) = match current(&mut tx, token, time).await {
                Ok(value) => value,
                Err(crate::Error::Rejected(_)) => continue,
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
                .bind(&intent.local_device).bind(number(&intent.local_epoch)? as i64).bind(time).fetch_optional(&mut *tx).await?;
            let Some(worker) = worker else { continue };
            let epoch: i64 = worker.get("epoch");
            let calls: Vec<ProposedRead> = serde_json::from_value(run.checkpoint["calls"].clone())?;
            let read = calls
                .last()
                .ok_or_else(|| crate::Error::Rejected("missing approved model call".into()))?;
            let probe = ReadOp::BrowserAction {
                session_id: intent.observation.session_id.clone(),
                nonce: intent.observation.nonce.clone(),
                element_id: intent.element_id.clone(),
                effect: intent.effect.clone(),
                grant: Some(Box::new(BrowserGrant {
                    operation: Operation {
                        id: read.operation_id.clone(),
                        idempotency_key: read.operation_id.clone(),
                        token: token.clone(),
                        target: request.proposal.target.clone(),
                        intent: request.proposal.intent.clone(),
                        intent_digest: request.intent_digest.clone(),
                        state: pulse_core::OperationState::Unknown,
                    },
                    request: request.clone(),
                })),
            };
            if !Self::browser_capable(&mut tx, &intent.local_device, epoch, &probe).await? {
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
                    Store::invalidate_browser_request_in(
                        &mut tx,
                        token,
                        &request.proposal.id,
                        "browser_approval_invalid",
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
            let mut payload = read.call.operation.clone();
            let ReadOp::BrowserAction { grant, .. } = &mut payload else {
                unreachable!()
            };
            *grant = Some(Box::new(BrowserGrant {
                operation,
                request: serde_json::from_str(&updated)?,
            }));
            sqlx::query("INSERT INTO link_reads VALUES(?,?,?,?,?,?,'sent',NULL)")
                .bind(&read.operation_id)
                .bind(&intent.local_device)
                .bind(epoch)
                .bind(&task.id)
                .bind(task.revision.to_string())
                .bind(serde_json::to_string(&payload)?)
                .execute(&mut *tx)
                .await?;
            Self::enqueue(
                &mut tx,
                &intent.local_device,
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
        tx.commit().await?;
        Ok(false)
    }
    pub(crate) async fn browser_action_receipt(
        db: &mut SqliteConnection,
        operation_id: &str,
        operation: &ReadOp,
        before: &Task,
        outcome: &ReadOutcome,
    ) -> Result<bool> {
        let grant = match operation {
            ReadOp::BrowserAction {
                grant: Some(grant), ..
            } => grant,
            ReadOp::BrowserAction { .. } => {
                return Err(crate::Error::Rejected("action receipt lacks grant".into()));
            }
            _ => return Ok(true),
        };
        ensure(
            grant.operation.id == operation_id,
            "action receipt operation mismatch",
        )?;
        match outcome {
            ReadOutcome::Unknown { .. } => Ok(false),
            ReadOutcome::Completed { .. } => {
                Store::record_receipt_in(
                    db,
                    &pulse_core::Receipt {
                        operation_id: operation_id.into(),
                        token: grant.operation.token.clone(),
                        success: true,
                        evidence: format!("browser-action:{operation_id}:{}", hash(outcome)?),
                    },
                )
                .await?;
                Ok(true)
            }
            ReadOutcome::Rejected { .. } => {
                Store::browser_not_executed_in(
                    db,
                    &grant.operation.token,
                    operation_id,
                    &format!("browser-rejected:{}", hash(outcome)?),
                )
                .await?;
                let _ = before;
                Ok(false)
            }
        }
    }
}
