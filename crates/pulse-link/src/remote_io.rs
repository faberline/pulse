//! Trusted orchestration over the existing device queue. Model data grants no authority.
use crate::{
    Result, ensure,
    protocol::*,
    provider::{Exchange, Reply, ToolCall},
    server::Hub,
};
use pulse_core::{Run, State, Task, Token, Versions};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{Row, SqliteConnection};

#[derive(Clone, Serialize, Deserialize)]
struct ProposedRead {
    operation_id: String,
    call: ToolCall,
}

async fn save(db: &mut SqliteConnection, task: &Task, run: &Run) -> Result<()> {
    sqlx::query("UPDATE tasks SET data=? WHERE id=?")
        .bind(serde_json::to_string(task)?)
        .bind(&task.id)
        .execute(&mut *db)
        .await?;
    sqlx::query("UPDATE runs SET data=? WHERE id=?")
        .bind(serde_json::to_string(run)?)
        .bind(&run.token.run_id)
        .execute(db)
        .await?;
    Ok(())
}
async fn current(db: &mut SqliteConnection, token: &Token, time: i64) -> Result<(Task, Run)> {
    let data: String = sqlx::query_scalar("SELECT data FROM tasks WHERE id=?")
        .bind(&token.task_id)
        .fetch_one(&mut *db)
        .await?;
    let task: Task = serde_json::from_str(&data)?;
    let data: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=?")
        .bind(&token.run_id)
        .fetch_one(db)
        .await?;
    let run: Run = serde_json::from_str(&data)?;
    ensure(
        task.active_run.as_deref() == Some(&token.run_id)
            && task.revision == token.revision
            && run.token == *token
            && !task.cancel_requested
            && !task.state.terminal()
            && task.deadline > time,
        "stale remote run",
    )?;
    Ok((task, run))
}

impl Hub {
    /// Build context from the current Human decision and same-revision evidence only.
    pub async fn model_context(&self, run: &Run) -> Result<(String, Vec<Exchange>)> {
        let task = {
            let mut db = self.db.acquire().await?;
            let (task, _) = current(&mut db, &run.token, crate::now()).await?;
            ensure(
                task.state == State::Planning,
                "context requires a claimed task",
            )?;
            task
        };
        let history = self.model_history(run).await?;
        let mut prior_evidence = Vec::new();
        if history.is_empty() {
            // A manually requested read has no provider tool-call ID. Keep it as data.
            let rows=sqlx::query("SELECT id,request,result FROM link_reads WHERE task_id=? AND task_revision=? AND state IN ('completed','rejected') ORDER BY rowid DESC LIMIT 4").bind(&task.id).bind(task.revision.to_string()).fetch_all(&self.db).await?;
            for row in rows {
                let mut outcome: ReadOutcome =
                    serde_json::from_str(&row.get::<String, _>("result"))?;
                ensure(outcome.valid(), "invalid prior evidence")?;
                trim_evidence(&mut outcome);
                let operation: ReadOp = serde_json::from_str(&row.get::<String, _>("request"))?;
                prior_evidence.push(json!({"operation_id":row.get::<String,_>("id"),"operation":operation,"outcome":outcome}));
            }
        }
        let prompt = serde_json::to_string(&json!({
            "goal":task.goal,"effective_decision":task.decisions.last(),
            "task_id":task.id,"revision":task.revision.to_string(),
            "authorization_event":task.authorization_event,"scope":task.scope,"prior_read_evidence":prior_evidence
        }))?;
        Ok((prompt, history))
    }

    /// Persist a model proposal before attempting any dispatch. This is not Human input.
    pub async fn model_proposal(&self, token: &Token, reply: &Reply, time: i64) -> Result<()> {
        let call = reply
            .tool
            .as_ref()
            .ok_or_else(|| crate::Error::Rejected("missing tool proposal".into()))?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let (mut task, mut run) = current(&mut tx, token, time).await?;
        ensure(
            task.state == State::Planning
                && call.operation.permitted_by(&task.scope)
                && task.tool_calls < 16,
            "model proposal policy",
        )?;
        let canonical = call.operation.canonical()?;
        ensure(canonical == call.operation, "canonical read path required")?;
        let completed:Vec<String>=sqlx::query_scalar("SELECT request FROM link_reads WHERE task_id=? AND task_revision=? AND state='completed'").bind(&task.id).bind(task.revision.to_string()).fetch_all(&mut *tx).await?;
        for request in completed {
            let previous: ReadOp = serde_json::from_str(&request)?;
            ensure(
                previous.canonical()? != canonical,
                "completed read evidence already exists; review or revise the request",
            )?;
        }
        let round = run.checkpoint["round"].as_u64().unwrap_or(0);
        let mut calls: Vec<ProposedRead> =
            serde_json::from_value(run.checkpoint.get("calls").cloned().unwrap_or(json!([])))?;
        ensure(
            round < 8 && calls.len() < 8 && !calls.iter().any(|c| c.call.call_id == call.call_id),
            "model proposal limit or duplicate identity",
        )?;
        let operation_id = format!("model-read:{}:{}:{round}", token.run_id, token.revision);
        calls.push(ProposedRead {
            operation_id: operation_id.clone(),
            call: call.clone(),
        });
        run.checkpoint =
            json!({"phase":"waiting_tool","round":round,"calls":calls,"operation_id":operation_id});
        task.state = State::WaitingDevice;
        task.reason = Some("model_tool_waiting".into());
        save(&mut tx, &task, &run).await?;
        tx.commit().await?;
        self.notify_change();
        Ok(())
    }

    /// Wait without model calls while no live worker is free. Dispatch once in one transaction.
    pub async fn dispatch_model_read(&self, versions: &Versions, time: i64) -> Result<bool> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let workers=sqlx::query("SELECT id,epoch FROM link_device WHERE role='local_executor' AND revoked=0 AND stopped=0 AND lease_until>? AND NOT EXISTS(SELECT 1 FROM link_reads WHERE device=link_device.id AND state IN ('sent','accepted','unknown')) ORDER BY id").bind(time).fetch_all(&mut *tx).await?;
        if workers.is_empty() {
            return Ok(false);
        }
        let rows:Vec<String>=sqlx::query_scalar("SELECT data FROM tasks WHERE json_extract(data,'$.state')='waiting_device' ORDER BY json_extract(data,'$.created_at'),id").fetch_all(&mut *tx).await?;
        for data in rows {
            let task: Task = serde_json::from_str(&data)?;
            if task.reason.as_deref() != Some("model_tool_waiting")
                || task.cancel_requested
                || task.deadline <= time
            {
                continue;
            }
            let Some(id) = task.active_run.as_ref() else {
                continue;
            };
            let data: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=?")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
            let mut run: Run = serde_json::from_str(&data)?;
            if run.versions != *versions
                || run.token.revision != task.revision
                || run.checkpoint["phase"] != "waiting_tool"
            {
                continue;
            }
            let calls: Vec<ProposedRead> = serde_json::from_value(run.checkpoint["calls"].clone())?;
            let read = calls
                .last()
                .ok_or_else(|| crate::Error::Rejected("missing durable proposal".into()))?;
            ensure(
                read.call.operation.permitted_by(&task.scope),
                "dispatch outside Human scope",
            )?;
            let mut selected = None;
            for worker in &workers {
                let device: String = worker.get("id");
                let epoch: i64 = worker.get("epoch");
                if Self::browser_capable(&mut tx, &device, epoch, &read.call.operation).await? {
                    selected = Some((device, epoch));
                    break;
                }
            }
            let Some((device, epoch)) = selected else {
                continue;
            };
            pulse_core::Store::authorize_remote_read(&mut tx, &run.token, &read.operation_id, time)
                .await?;
            sqlx::query("INSERT INTO link_reads VALUES(?,?,?,?,?,?,'sent',NULL)")
                .bind(&read.operation_id)
                .bind(&device)
                .bind(epoch)
                .bind(&task.id)
                .bind(task.revision.to_string())
                .bind(serde_json::to_string(&read.call.operation)?)
                .execute(&mut *tx)
                .await?;
            Self::enqueue(
                &mut tx,
                &device,
                epoch as u64,
                ServerPayload::Read {
                    operation_id: read.operation_id.clone(),
                    task_id: task.id.clone(),
                    revision: task.revision.to_string(),
                    operation: read.call.operation.clone(),
                },
            )
            .await?;
            run.checkpoint["phase"] = json!("sent_tool");
            run.checkpoint["dispatch_epoch"] = json!(run.token.epoch);
            sqlx::query("UPDATE runs SET data=? WHERE id=?")
                .bind(serde_json::to_string(&run)?)
                .bind(id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            self.notify_change();
            return Ok(true);
        }
        tx.commit().await?;
        Ok(false)
    }

    /// Called in the receipt transaction after recording Tool evidence, never goal acceptance.
    pub(crate) async fn continue_model_read(
        db: &mut SqliteConnection,
        before: &Task,
        operation_id: &str,
        outcome: &ReadOutcome,
        time: i64,
    ) -> Result<()> {
        if matches!(outcome, ReadOutcome::Unknown { .. })
            || before.cancel_requested
            || before.deadline <= time
            || before.state != State::Dispatched
        {
            return Ok(());
        }
        let Some(id) = before.active_run.as_ref() else {
            return Ok(());
        };
        let data: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=?")
            .bind(id)
            .fetch_one(&mut *db)
            .await?;
        let mut run: Run = serde_json::from_str(&data)?;
        if run.token.revision != before.revision
            || run.checkpoint["phase"] != "sent_tool"
            || run.checkpoint["operation_id"] != operation_id
            || run.checkpoint["dispatch_epoch"].as_u64() != Some(run.token.epoch)
        {
            return Ok(());
        }
        let data: String = sqlx::query_scalar("SELECT data FROM tasks WHERE id=?")
            .bind(&before.id)
            .fetch_one(&mut *db)
            .await?;
        let mut task: Task = serde_json::from_str(&data)?;
        // The transport observation has appended evidence. Preserve it and the same logical run.
        task.active_run = Some(id.clone());
        task.state = State::Queued;
        task.reason = Some("model_tool_result_ready".into());
        run.checkpoint["phase"] = json!("ready");
        run.checkpoint["round"] = json!(run.checkpoint["round"].as_u64().unwrap_or(0) + 1);
        save(db, &task, &run).await
    }

    pub async fn model_history(&self, run: &Run) -> Result<Vec<Exchange>> {
        let calls: Vec<ProposedRead> =
            serde_json::from_value(run.checkpoint.get("calls").cloned().unwrap_or(json!([])))?;
        ensure(calls.len() <= 8, "history limit")?;
        let mut history = Vec::new();
        for read in calls {
            let encoded:String=sqlx::query_scalar("SELECT result FROM link_reads WHERE id=? AND task_id=? AND task_revision=? AND state IN ('completed','rejected')").bind(&read.operation_id).bind(&run.token.task_id).bind(run.token.revision.to_string()).fetch_one(&self.db).await?;
            let mut outcome: ReadOutcome = serde_json::from_str(&encoded)?;
            trim_evidence(&mut outcome);
            history.push(Exchange {
                call: read.call,
                outcome,
            });
        }
        Ok(history)
    }
}

fn trim_evidence(outcome: &mut ReadOutcome) {
    if let ReadOutcome::Completed {
        text, truncated, ..
    } = outcome
        && text.len() > 2500
    {
        text.truncate(text.floor_char_boundary(2500));
        *truncated = true;
    }
}
