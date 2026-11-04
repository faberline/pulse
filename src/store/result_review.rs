//! Human review of an exact saved answer. A receipt or model cannot accept it.
use super::*;

pub(super) fn targets(payload: &str) -> Vec<String> {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|payload| {
            payload
                .get("commands")
                .and_then(Value::as_array)
                .map(|commands| {
                    commands
                        .iter()
                        .filter(|c| c["kind"] == "accept_result")
                        .filter_map(|c| c["id"].as_str().map(str::to_owned))
                        .collect()
                })
        })
        .unwrap_or_default()
}

pub(super) async fn accept(
    db: &mut SqliteConnection,
    event: &str,
    id: &str,
    revision: u64,
    message: &str,
    chat_digest: &str,
    now: i64,
) -> Result<()> {
    require(!retired_task(db, id).await?, "result task was retired")?;
    require(
        !id.is_empty()
            && id.len() <= 128
            && revision > 0
            && revision <= i64::MAX as u64
            && !message.is_empty()
            && message.len() <= 256
            && chat_digest.len() == 64
            && chat_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid result identity",
    )?;
    let bound: i64 = sqlx::query_scalar("SELECT count(*) FROM task_memory_sources WHERE task_id=?")
        .bind(id)
        .fetch_one(&mut *db)
        .await?;
    require(bound == 0, "memory-derived result review is not enabled")?;
    let mut task: Task = load(db, "tasks", id).await?;
    let already_accepted =
        task.state == State::Succeeded && task.reason.as_deref() == Some("human_accepted_result");
    require(
        task.revision == revision
            && !task.cancel_requested
            && (already_accepted
                || (task.state == State::Blocked
                    && task.reason.as_deref() == Some("model_reply_ready")))
            && crate::read_scope(&task.scope)
            && !unresolved(db, id).await?,
        "result is not current or has unresolved work",
    )?;
    let run_id = task
        .active_run
        .as_deref()
        .ok_or_else(|| Error::Rejected("result run unavailable".into()))?;
    let run: Run = load(db, "runs", run_id).await?;
    let review = &run.checkpoint["result_review"];
    let round = run.checkpoint["round"].as_u64().unwrap_or(0);
    require(
        run.token.task_id == task.id
            && run.token.revision == revision
            && run.token.run_id == run_id
            && run.checkpoint["phase"] == "reply_saved"
            && round < 8
            && message == format!("model:{run_id}:{revision}:{round}")
            && task.evidence.iter().any(|id| id == message)
            && run.checkpoint["reply"]
                .as_str()
                .is_some_and(|text| !text.trim().is_empty())
            && review["version"] == 1
            && review["message_id"].as_str() == Some(message)
            && review["chat_digest"].as_str() == Some(chat_digest)
            && review["saved_at"]
                .as_i64()
                .is_some_and(|at| at >= task.created_at && at <= now),
        "result review evidence unavailable or changed",
    )?;
    let body: Option<String> = sqlx::query_scalar(
        "SELECT body FROM link_chat WHERE id=? AND length(CAST(body AS BLOB)) BETWEEN 1 AND 65536",
    )
    .bind(message)
    .fetch_optional(&mut *db)
    .await?;
    require(
        body.is_some_and(|text| format!("{:x}", Sha256::digest(text.as_bytes())) == chat_digest),
        "displayed result changed",
    )?;
    if already_accepted {
        return Ok(());
    }
    // This acknowledges the saved answer. It cannot grant a pending action,
    // replay an effect, release a reservation, or claim an unknown effect stopped.
    task.state = State::Succeeded;
    task.reason = Some("human_accepted_result".into());
    task.evidence
        .push(format!("human-result:{event}:{message}"));
    task.wake_at = None;
    approvals::invalidate(db, id, "result_accepted").await?;
    put(db, "tasks", id, &task).await?;
    Store::record_task_terminal_in(db, id, now).await?;
    notify(
        db,
        &format!("terminal:{id}:{revision}"),
        &json!({"task_id":id,"state":task.state}),
    )
    .await
}
