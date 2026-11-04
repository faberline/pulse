//! Freeze the displayed result in Rust. A server must never guess what was read.
use crate::{Result, ensure, protocol::*};
use sqlx::SqliteConnection;

#[cfg(test)]
mod tests;

pub(crate) fn target(text: &str) -> Option<&str> {
    let text = text.trim().trim_end_matches(['。', '.', '！', '!']).trim();
    if let Some(rest) = text
        .strip_prefix("接受成果")
        .or_else(|| text.strip_prefix("確認成果"))
    {
        return Some(rest.trim());
    }
    text.strip_prefix("接受")
        .or_else(|| text.strip_prefix("確認"))
        .and_then(|rest| rest.strip_suffix("的成果"))
        .map(str::trim)
}

pub(crate) async fn freeze(
    db: &mut SqliteConnection,
    source: &str,
    text: &str,
) -> Result<Option<HumanAction>> {
    let Some(target) = target(text) else {
        return Ok(None);
    };
    ensure(
        !text.trim().is_empty() && text.len() <= 4096,
        "result review text size",
    )?;
    // Retry the originally frozen action. A later cache must not change the
    // result behind an already accepted local source ID.
    let prior: Option<String> = sqlx::query_scalar("SELECT data FROM local_outbox WHERE json_extract(data,'$.payload.kind')='human' AND json_extract(data,'$.payload.source_id')=? ORDER BY sequence LIMIT 1")
        .bind(source).fetch_optional(&mut *db).await?;
    if let Some(prior) = prior {
        let frame: Envelope<DevicePayload> = parse(prior.as_bytes())?;
        let DevicePayload::Human { action, .. } = frame.payload else {
            unreachable!()
        };
        ensure(
            matches!(&action, HumanAction::AcceptResult { text: original, .. } if original==text),
            "result review source conflict",
        )?;
        return Ok(Some(action));
    }
    let cache: Option<String> =
        sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cache'")
            .fetch_optional(&mut *db)
            .await?;
    let state: NativeState = cache
        .map(|s| serde_json::from_str(&s))
        .transpose()?
        .unwrap_or_default();
    let tasks = if target.is_empty() {
        let pending = state
            .tasks
            .iter()
            .filter(|task| {
                !task.cancel_requested
                    && task.state == pulse_core::State::Blocked
                    && task.reason.as_deref() == Some("model_reply_ready")
            })
            .collect::<Vec<_>>();
        if pending.is_empty() {
            state
                .tasks
                .iter()
                .filter(|task| {
                    !task.cancel_requested
                        && task.state == pulse_core::State::Succeeded
                        && task.reason.as_deref() == Some("human_accepted_result")
                })
                .collect::<Vec<_>>()
        } else {
            pending
        }
    } else {
        crate::conversation::selected(target, &state.tasks).ok_or_else(|| {
            crate::Error::Rejected("result review needs one displayed task".into())
        })?
    };
    ensure(tasks.len() == 1, "result review needs one displayed task")?;
    let mut candidates = vec![];
    for task in tasks {
        if task.cancel_requested
            || !matches!(
                (task.state, task.reason.as_deref()),
                (pulse_core::State::Blocked, Some("model_reply_ready"))
                    | (pulse_core::State::Succeeded, Some("human_accepted_result"))
            )
        {
            continue;
        }
        let Some(run) = &task.active_run else {
            continue;
        };
        let prefix = format!("model:{run}:{}:", task.revision);
        for message in &state.messages {
            let sources = message
                .memory_sources
                .as_ref()
                .unwrap_or(&state.memory_sources);
            if !sources.is_empty() || !task.evidence.contains(&message.id) {
                continue;
            }
            if message
                .id
                .strip_prefix(&prefix)
                .is_some_and(|n| number(n).is_ok_and(|n| n < 8))
            {
                candidates.push((task, message));
            }
        }
    }
    ensure(
        candidates.len() == 1,
        "result review needs one displayed task",
    )?;
    let (task, message) = candidates[0];
    Ok(Some(HumanAction::AcceptResult {
        task_id: task.id.clone(),
        revision: task.revision.to_string(),
        message_id: message.id.clone(),
        chat_digest: hash_bytes(message.body.as_bytes()),
        text: text.into(),
    }))
}
