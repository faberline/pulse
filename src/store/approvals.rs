//! Durable action-bound approvals. No adapter or effect executes in these transactions.
use super::*;

impl Store {
    pub async fn invalidate_browser_request_in(
        db: &mut SqliteConnection,
        token: &Token,
        request_id: &str,
        reason: &str,
        now: i64,
    ) -> Result<()> {
        let request = request(db, request_id).await?;
        require(
            request.proposal.target.starts_with("browser:"),
            "browser request invalidation binding",
        )?;
        Self::invalidate_action_request_in(db, token, request_id, reason, now).await
    }
    pub async fn invalidate_action_request_in(
        db: &mut SqliteConnection,
        token: &Token,
        request_id: &str,
        reason: &str,
        now: i64,
    ) -> Result<()> {
        let (mut task, _) = current(db, token, now).await?;
        let mut request = request(db, request_id).await?;
        require(
            request.proposal.token == *token
                && (request.proposal.target.starts_with("browser:")
                    || request.proposal.target.starts_with("computer:"))
                && !reason.is_empty()
                && reason.len() <= 256,
            "browser request invalidation binding",
        )?;
        if request.state == ApprovalState::Invalidated {
            return Ok(());
        }
        require(
            matches!(
                request.state,
                ApprovalState::Pending | ApprovalState::Approved
            ),
            "browser request already consumed",
        )?;
        request.state = ApprovalState::Invalidated;
        request.reason = Some(reason.into());
        save(db, &request).await?;
        task.state = State::Blocked;
        task.reason = Some(reason.into());
        put(db, "tasks", &task.id, &task).await?;
        notify(
            db,
            &format!("browser-invalid:{request_id}"),
            &json!({"task_id":task.id,"request_id":request_id,"state":"invalidated"}),
        )
        .await
    }
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
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let result = Self::propose_operation_in(&mut tx, source_event, proposal, now).await?;
        tx.commit().await?;
        Ok(result)
    }
    /// Caller owns the transaction, including its checkpoint and outbox updates.
    pub async fn propose_operation_in(
        db: &mut SqliteConnection,
        source_event: &str,
        proposal: OperationProposal,
        now: i64,
    ) -> Result<OperationRequest> {
        require(
            !proposal.id.is_empty()
                && proposal.id.len() <= 128
                && (proposal.target.starts_with("fixture:")
                    || serde_json::from_value::<BrowserIntent>(proposal.intent.clone()).is_ok_and(
                        |intent| intent.target() == proposal.target && intent.observation.valid(),
                    )
                    || serde_json::from_value::<ComputerIntent>(proposal.intent.clone())
                        .is_ok_and(|intent| {
                            intent.target().as_deref() == Some(&proposal.target)
                                && intent.observation.valid_evidence()
                        }))
                && proposal.target.len() > 8
                && proposal.target.len() <= 512
                && proposal.intent.is_object()
                && serde_jcs::to_vec(&proposal)?.len() <= 64 * 1024,
            "invalid or unsupported operation proposal",
        )?;
        let hash = digest(&json!({"source_event":source_event,"proposal":proposal}))?;
        verify_source(&mut *db, source_event, &proposal, now).await?;
        if let Some(data) =
            sqlx::query_scalar::<_, String>("SELECT data FROM operation_requests WHERE id=?")
                .bind(&proposal.id)
                .fetch_optional(&mut *db)
                .await?
        {
            let old: OperationRequest = serde_json::from_str(&data)?;
            require(
                old.intent_digest == hash,
                "operation proposal identity conflict",
            )?;
            return Ok(old);
        }
        let (mut task, _) = current(&mut *db, &proposal.token, now).await?;
        require(
            task.state == State::Planning
                && target_policy(&task, &proposal, now)
                && task.tool_calls < 16
                && proposal.expires_at > now
                && proposal.expires_at <= task.deadline
                && !unresolved(&mut *db, &task.id).await?,
            "operation proposal policy, expiry or state",
        )?;
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM operation_requests WHERE task_id=?")
                .bind(&task.id)
                .fetch_one(&mut *db)
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
        save(&mut *db, &result).await?;
        task.state = State::WaitingApproval;
        task.reason = Some("operation_approval_required".into());
        put(&mut *db, "tasks", &task.id, &task).await?;
        notify(
            &mut *db,
            &format!("operation-request:{}", result.proposal.id),
            &json!({"task_id":task.id,"request_id":result.proposal.id,"state":"waiting_approval"}),
        )
        .await?;
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
        let result = Self::prepare_approved_operation_in(&mut tx, token, request_id, now).await;
        // Preserve the existing committed stop at the tool limit. Other failures roll back.
        if result.is_ok()
            || matches!(&result, Err(Error::Rejected(reason)) if reason == "tool call limit reached")
        {
            tx.commit().await?;
        }
        result
    }
    /// Approval consumption and transport dispatch must share the caller's transaction.
    pub async fn prepare_approved_operation_in(
        db: &mut SqliteConnection,
        token: &Token,
        request_id: &str,
        now: i64,
    ) -> Result<Operation> {
        let mut request = request(&mut *db, request_id).await?;
        let (mut task, _) = current(&mut *db, token, now).await?;
        require(
            request.proposal.token == *token
                && request.state == ApprovalState::Approved
                && request.proposal.expires_at > now
                && task.state == State::Ready
                && target_policy(&task, &request.proposal, now)
                && !unresolved(&mut *db, &task.id).await?,
            "operation approval is stale, consumed or unavailable",
        )?;
        verify_source(&mut *db, &request.source_event, &request.proposal, now).await?;
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
            .fetch_optional(&mut *db)
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
            .fetch_one(&mut *db)
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
            invalidate(&mut *db, &task.id, "tool_calls_exhausted").await?;
            block(&mut *db, &mut task, "tool_calls_exhausted").await?;
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
        save(&mut *db, &request).await?;
        put(&mut *db, "operations", &operation.id, &operation).await?;
        task.state = State::Dispatched;
        task.tool_calls += 1;
        task.reason = Some("approved_operation_dispatched".into());
        put(&mut *db, "tasks", &task.id, &task).await?;
        notify(
            &mut *db,
            &format!("approved-operation:{}", operation.id),
            &json!({"task_id":task.id,"operation_id":operation.id,"state":"dispatched"}),
        )
        .await?;
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
            .fetch_one(&mut *db)
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
    )?;
    if proposal.target.starts_with("browser:") {
        verify_browser_evidence(db, proposal).await?;
    } else if proposal.target.starts_with("computer:") {
        verify_computer_evidence(db, proposal).await?;
    }
    Ok(())
}

fn target_policy(task: &Task, proposal: &OperationProposal, now: i64) -> bool {
    if proposal.target.starts_with("fixture:") {
        return task.scope == "pulse:read";
    }
    if proposal.target.starts_with("computer:") {
        return serde_json::from_value::<ComputerIntent>(proposal.intent.clone()).is_ok_and(
            |intent| {
                intent.target().as_deref() == Some(&proposal.target)
                    && intent.valid(&task.scope, now)
                    && intent.observation.binding.task_id == proposal.token.task_id
                    && intent.observation.binding.revision == proposal.token.revision
                    && proposal.expires_at <= intent.observation.expires_at
            },
        );
    }
    serde_json::from_value::<BrowserIntent>(proposal.intent.clone()).is_ok_and(|intent| {
        proposal.target == intent.target()
            && intent.valid(&task.scope, now)
            && proposal.expires_at <= intent.observation.expires_at
    })
}

async fn verify_computer_evidence(
    db: &mut SqliteConnection,
    proposal: &OperationProposal,
) -> Result<()> {
    let intent: ComputerIntent = serde_json::from_value(proposal.intent.clone())?;
    let observation = &intent.observation;
    let binding = &observation.binding;
    let revision = binding.revision.to_string();
    let row=sqlx::query("SELECT r.device,r.epoch,r.task_id,r.task_revision,r.request,r.result,a.metadata,a.png FROM link_reads r JOIN link_artifacts a ON a.operation_id=r.id WHERE r.id=? AND r.state='completed' AND json_extract(r.request,'$.kind')='computer_inspect' AND a.png IS NOT NULL")
        .bind(&binding.operation_id).fetch_optional(&mut *db).await?
        .ok_or_else(||Error::Rejected("computer proposal lacks original image and receipt".into()))?;
    let outcome: ReadOutcome = serde_json::from_str(&row.get::<String, _>("result"))?;
    let image: Value = serde_json::from_str(&row.get::<String, _>("metadata"))?;
    let read: Value = serde_json::from_str(&row.get::<String, _>("request"))?;
    let png: Vec<u8> = row.get("png");
    let dimensions = browser_png_dimensions(&png)
        .ok_or_else(|| Error::Rejected("invalid computer source PNG".into()))?;
    require(
        observation.valid_evidence()
            && binding.task_id == proposal.token.task_id
            && binding.revision == proposal.token.revision
            && row.get::<String, _>("device") == binding.device
            && row.get::<i64, _>("epoch").to_string() == binding.epoch.to_string()
            && row.get::<String, _>("task_id") == binding.task_id
            && row.get::<String, _>("task_revision") == binding.revision.to_string()
            && read["selection"] == serde_json::to_value(observation.selection())?
            && format!("{:x}", Sha256::digest(&png)) == observation.image.digest
            && png.len() == observation.image.length as usize
            && dimensions == (observation.image.width, observation.image.height)
            && image["task_id"] == binding.task_id
            && image["revision"].as_str() == Some(revision.as_str())
            && image.get("url").is_none_or(|value| value == "")
            && image["computer"]["binding"] == serde_json::to_value(binding)?
            && image["computer"]["selection"] == serde_json::to_value(observation.selection())?
            && image["title"] == observation.title
            && image["observed_at"] == observation.observed_at
            && image["digest"] == observation.image.digest
            && image["length"] == observation.image.length
            && image["width"] == observation.image.width
            && image["height"] == observation.image.height
            && outcome.valid()
            && matches!(&outcome,ReadOutcome::Completed{computer:Some(actual),observation:None,..} if actual.as_ref()==observation),
        "computer source receipt or image binding mismatch",
    )?;
    verify_receipt_source(
        db,
        &binding.device,
        &binding.operation_id,
        &binding.task_id,
        outcome,
    )
    .await
}

async fn verify_browser_evidence(
    db: &mut SqliteConnection,
    proposal: &OperationProposal,
) -> Result<()> {
    let intent: BrowserIntent = serde_json::from_value(proposal.intent.clone())?;
    let row=sqlx::query("SELECT r.device,r.epoch,r.task_id,r.task_revision,r.result,a.metadata,a.png FROM link_reads r JOIN link_artifacts a ON a.operation_id=r.id WHERE r.id=? AND r.state='completed' AND json_extract(r.request,'$.kind') IN ('browser_inspect','browser_action') AND a.png IS NOT NULL")
        .bind(&intent.source_operation_id).fetch_optional(&mut *db).await?
        .ok_or_else(||Error::Rejected("browser proposal lacks original image and receipt".into()))?;
    let outcome: ReadOutcome = serde_json::from_str(&row.get::<String, _>("result"))?;
    let image: Value = serde_json::from_str(&row.get::<String, _>("metadata"))?;
    let png: Vec<u8> = row.get("png");
    let (width, height) =
        browser_png_dimensions(&png).ok_or_else(|| Error::Rejected("invalid source PNG".into()))?;
    let png_digest = format!("{:x}", Sha256::digest(&png));
    let revision = proposal.token.revision.to_string();
    require(
        png_digest == intent.observation.png_digest
            && image["width"] == width
            && image["height"] == height
            && row.get::<String, _>("device") == intent.local_device
            && row.get::<i64, _>("epoch").to_string() == intent.local_epoch
            && row.get::<String, _>("task_id") == proposal.token.task_id
            && row.get::<String, _>("task_revision") == proposal.token.revision.to_string()
            && image["digest"] == intent.observation.png_digest
            && image["task_id"] == proposal.token.task_id
            && image["revision"].as_str() == Some(revision.as_str())
            && outcome.valid()
            && matches!(&outcome,ReadOutcome::Completed{observation:Some(observation),..} if observation==&intent.observation),
        "browser source receipt or image binding mismatch",
    )?;
    verify_receipt_source(
        db,
        &intent.local_device,
        &intent.source_operation_id,
        &proposal.token.task_id,
        outcome,
    )
    .await
}

async fn verify_receipt_source(
    db: &mut SqliteConnection,
    device: &str,
    operation_id: &str,
    task_id: &str,
    outcome: ReadOutcome,
) -> Result<()> {
    let source = sqlx::query(
        "SELECT id,payload,outcome FROM events WHERE source=? AND source_id=? AND origin='tool'",
    )
    .bind(format!("device-result:{device}"))
    .bind(operation_id)
    .fetch_optional(&mut *db)
    .await?
    .ok_or_else(|| Error::Rejected("operation receipt source missing".into()))?;
    let payload: Value = serde_json::from_str(&source.get::<String, _>("payload"))?;
    let linked: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_tasks WHERE event_id=? AND task_id=?")
            .bind(source.get::<String, _>("id"))
            .bind(task_id)
            .fetch_one(db)
            .await?;
    require(
        digest(&payload["result"])? == digest(&outcome)?
            && linked == 1
            && applied_to(
                source.get::<Option<String>, _>("outcome").as_deref(),
                task_id,
            ),
        "operation receipt must retain its linked Tool source",
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
