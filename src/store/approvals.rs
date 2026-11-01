//! Durable action-bound approvals. No adapter or effect executes in these transactions.
use super::*;

impl Store {
    pub async fn operation_request(&self, request_id: &str) -> Result<OperationRequest> {
        let mut db = self.pool.acquire().await?;
        request(&mut db, request_id).await
    }

    /// A Tool event contains this exact proposal and must already be linked to its task.
    /// Replaying a proposal returns its original record, never a renewed approval.
    pub async fn propose_operation(
        &self,
        source_event: &str,
        proposal: OperationProposal,
        now: i64,
    ) -> Result<OperationRequest> {
        require(
            !proposal.id.is_empty()
                && proposal.id.len() <= 128
                && proposal.target.starts_with("fixture:")
                && proposal.target.len() > 8
                && proposal.target.len() <= 512
                && proposal.intent.is_object()
                && serde_jcs::to_vec(&proposal)?.len() <= 64 * 1024,
            "invalid or non-fixture operation proposal",
        )?;
        let hash = digest(&json!({"source_event":source_event,"proposal":proposal}))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        verify_source(&mut tx, source_event, &proposal, now).await?;
        if let Some(data) =
            sqlx::query_scalar::<_, String>("SELECT data FROM operation_requests WHERE id=?")
                .bind(&proposal.id)
                .fetch_optional(&mut *tx)
                .await?
        {
            let old: OperationRequest = serde_json::from_str(&data)?;
            require(
                old.intent_digest == hash,
                "operation proposal identity conflict",
            )?;
            tx.commit().await?;
            return Ok(old);
        }
        let (mut task, _) = current(&mut tx, &proposal.token, now).await?;
        require(
            task.state == State::Planning
                && task.scope == "pulse:read"
                && task.tool_calls < 16
                && proposal.expires_at > now
                && proposal.expires_at <= task.deadline
                && !unresolved(&mut tx, &task.id).await?,
            "operation proposal policy, expiry or state",
        )?;
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM operation_requests WHERE task_id=?")
                .bind(&task.id)
                .fetch_one(&mut *tx)
                .await?;
        require(count < 32, "operation proposal limit")?;
        let result = OperationRequest {
            proposal,
            source_event: source_event.into(),
            intent_digest: hash,
            state: ApprovalState::Pending,
            approval_event: None,
            approved_by: None,
            decision_event: None,
            reason: None,
            operation_id: None,
        };
        save(&mut tx, &result).await?;
        task.state = State::WaitingApproval;
        task.reason = Some("operation_approval_required".into());
        put(&mut tx, "tasks", &task.id, &task).await?;
        notify(
            &mut tx,
            &format!("operation-request:{}", result.proposal.id),
            &json!({"task_id":task.id,"request_id":result.proposal.id,"state":"waiting_approval"}),
        )
        .await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Consumes the exact one-time grant and persists an unknown intent before any effect.
    pub async fn prepare_approved_operation(
        &self,
        token: &Token,
        request_id: &str,
        now: i64,
    ) -> Result<Operation> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut request = request(&mut tx, request_id).await?;
        let (mut task, _) = current(&mut tx, token, now).await?;
        require(
            request.proposal.token == *token
                && request.state == ApprovalState::Approved
                && request.proposal.expires_at > now
                && task.state == State::Ready
                && request.proposal.target.starts_with("fixture:")
                && task.scope == "pulse:read"
                && !unresolved(&mut tx, &task.id).await?,
            "operation approval is stale, consumed or unavailable",
        )?;
        verify_source(&mut tx, &request.source_event, &request.proposal, now).await?;
        require(
            request.intent_digest
                == digest(
                    &json!({"source_event":request.source_event,"proposal":request.proposal}),
                )?,
            "operation proposal digest changed",
        )?;
        let event_id = request
            .approval_event
            .as_deref()
            .ok_or_else(|| Error::Rejected("missing human approval".into()))?;
        let event = sqlx::query("SELECT origin,payload,outcome,received_at FROM events WHERE id=?")
            .bind(event_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| Error::Missing("human approval event".into()))?;
        let payload: Value = serde_json::from_str(&event.get::<String, _>("payload"))?;
        let expected = OperationBinding {
            task_id: task.id.clone(),
            expected_revision: task.revision,
            request_id: request.proposal.id.clone(),
            intent_digest: request.intent_digest.clone(),
        };
        let commands =
            serde_json::from_value::<Vec<Command>>(payload["commands"].clone()).unwrap_or_default();
        let approved = commands.iter().any(
            |command| matches!(command,Command::ApproveOperation { binding } if binding==&expected),
        );
        let owner: String = sqlx::query_scalar("SELECT value FROM settings WHERE key='owner'")
            .fetch_one(&mut *tx)
            .await?;
        require(
            event.get::<String, _>("origin") == "human"
                && request.approved_by.as_deref() == Some(&owner)
                && applied_to(
                    event.get::<Option<String>, _>("outcome").as_deref(),
                    &task.id,
                )
                && approved
                && event.get::<i64, _>("received_at") < request.proposal.expires_at
                && event.get::<i64, _>("received_at") <= now,
            "missing or invalid human approval source and binding",
        )?;
        if task.tool_calls >= 16 {
            invalidate(&mut tx, &task.id, "tool_calls_exhausted").await?;
            block(&mut tx, &mut task, "tool_calls_exhausted").await?;
            tx.commit().await?;
            return Err(Error::Rejected("tool call limit reached".into()));
        }
        let operation_id = format!("approved:{}", request.proposal.id);
        let operation = Operation {
            id: operation_id.clone(),
            idempotency_key: operation_id.clone(),
            token: token.clone(),
            target: request.proposal.target.clone(),
            intent: request.proposal.intent.clone(),
            intent_digest: request.intent_digest.clone(),
            state: OperationState::Unknown,
        };
        request.state = ApprovalState::Dispatched;
        request.operation_id = Some(operation_id);
        save(&mut tx, &request).await?;
        put(&mut tx, "operations", &operation.id, &operation).await?;
        task.state = State::Dispatched;
        task.tool_calls += 1;
        task.reason = Some("approved_operation_dispatched".into());
        put(&mut tx, "tasks", &task.id, &task).await?;
        notify(
            &mut tx,
            &format!("approved-operation:{}", operation.id),
            &json!({"task_id":task.id,"operation_id":operation.id,"state":"dispatched"}),
        )
        .await?;
        tx.commit().await?;
        Ok(operation)
    }
}

fn applied_to(outcome: Option<&str>, task_id: &str) -> bool {
    outcome.is_some_and(|text| matches!(serde_json::from_str::<Outcome>(text),Ok(Outcome::Applied{task_ids}) if task_ids.iter().any(|id| id==task_id)))
}
async fn verify_source(
    db: &mut SqliteConnection,
    event_id: &str,
    proposal: &OperationProposal,
    now: i64,
) -> Result<()> {
    let source = sqlx::query("SELECT origin,payload,outcome,received_at FROM events WHERE id=?")
        .bind(event_id)
        .fetch_optional(&mut *db)
        .await?
        .ok_or_else(|| Error::Missing("operation proposal source".into()))?;
    let payload: Value = serde_json::from_str(&source.get::<String, _>("payload"))?;
    let linked: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_tasks WHERE event_id=? AND task_id=?")
            .bind(event_id)
            .bind(&proposal.token.task_id)
            .fetch_one(db)
            .await?;
    require(
        source.get::<String, _>("origin") == "tool"
            && source.get::<i64, _>("received_at") <= now
            && digest(&payload["operation_proposal"])? == digest(proposal)?
            && linked == 1
            && applied_to(
                source.get::<Option<String>, _>("outcome").as_deref(),
                &proposal.token.task_id,
            ),
        "proposal source is not a linked, applied Tool event",
    )
}

async fn request(db: &mut SqliteConnection, id: &str) -> Result<OperationRequest> {
    let data: String = sqlx::query_scalar("SELECT data FROM operation_requests WHERE id=?")
        .bind(id)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| Error::Missing("operation request".into()))?;
    Ok(serde_json::from_str(&data)?)
}
async fn save(db: &mut SqliteConnection, request: &OperationRequest) -> Result<()> {
    sqlx::query("INSERT INTO operation_requests(id,task_id,data) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data")
        .bind(&request.proposal.id).bind(&request.proposal.token.task_id).bind(serde_json::to_string(request)?).execute(db).await?;
    Ok(())
}

pub(super) async fn decide(
    db: &mut SqliteConnection,
    event: &str,
    binding: &OperationBinding,
    approve: bool,
    now: i64,
) -> Result<()> {
    let mut request = request(db, &binding.request_id).await?;
    let (mut task, _) = current(db, &request.proposal.token, now).await?;
    require(
        task.id == binding.task_id
            && task.revision == binding.expected_revision
            && request.intent_digest == binding.intent_digest
            && matches!(
                request.state,
                ApprovalState::Pending | ApprovalState::Approved
            ),
        "approval task, revision, digest or request state mismatch",
    )?;
    if approve {
        require(request.proposal.expires_at > now, "approval expired")?;
        if request.state == ApprovalState::Approved {
            require(task.state == State::Ready, "approved task is not ready")?;
            return Ok(());
        }
        require(
            task.state == State::WaitingApproval,
            "task is not waiting for approval",
        )?;
        let owner: String = sqlx::query_scalar("SELECT value FROM settings WHERE key='owner'")
            .fetch_one(&mut *db)
            .await?;
        request.state = ApprovalState::Approved;
        request.approval_event = Some(event.into());
        request.approved_by = Some(owner);
        task.state = State::Ready;
        task.reason = Some("operation_approved".into());
    } else {
        request.state = if request.state == ApprovalState::Approved {
            ApprovalState::Withdrawn
        } else {
            ApprovalState::Rejected
        };
        request.reason = Some("human_rejected_operation".into());
        task.state = State::Blocked;
        task.reason = Some("operation_rejected".into());
    }
    request.decision_event = Some(event.into());
    save(db, &request).await?;
    put(db, "tasks", &task.id, &task).await
}

pub(super) async fn invalidate(
    db: &mut SqliteConnection,
    task_id: &str,
    reason: &str,
) -> Result<bool> {
    let rows: Vec<String> = sqlx::query_scalar("SELECT data FROM operation_requests WHERE task_id=? AND json_extract(data,'$.state') IN ('pending','approved')").bind(task_id).fetch_all(&mut *db).await?;
    for data in &rows {
        let mut request: OperationRequest = serde_json::from_str(data)?;
        request.state = ApprovalState::Invalidated;
        request.reason = Some(reason.into());
        save(db, &request).await?;
    }
    Ok(!rows.is_empty())
}

pub(super) async fn expire(db: &mut SqliteConnection, now: i64) -> Result<bool> {
    let rows: Vec<String> = sqlx::query_scalar("SELECT data FROM operation_requests WHERE json_extract(data,'$.state') IN ('pending','approved') AND json_extract(data,'$.proposal.expires_at')<=?").bind(now).fetch_all(&mut *db).await?;
    for data in &rows {
        let mut request: OperationRequest = serde_json::from_str(data)?;
        request.state = ApprovalState::Expired;
        request.reason = Some("approval_expired".into());
        save(db, &request).await?;
        notify(db,&format!("operation-expired:{}",request.proposal.id),&json!({"task_id":request.proposal.token.task_id,"request_id":request.proposal.id,"state":"expired"})).await?;
        let mut task: Task = load(db, "tasks", &request.proposal.token.task_id).await?;
        if task.revision == request.proposal.token.revision
            && task.active_run.as_deref() == Some(&request.proposal.token.run_id)
            && matches!(task.state, State::WaitingApproval | State::Ready)
            && !task.cancel_requested
        {
            block(db, &mut task, "operation_approval_expired").await?;
        }
    }
    Ok(!rows.is_empty())
}
