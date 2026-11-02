//! Durable Human intent and local preparation state in the existing chat journal.
use crate::{Result, ensure, journal::Journal, protocol::*};
use pulse_core::{ComputerPermit, ComputerSelection, State, Task};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

const PREFIX: &str = "computer-selection:";
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SelectionPhase {
    Pending,
    Prepared,
    Invalidated,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SelectionStatus {
    pub source_id: String,
    pub task_id: String,
    pub revision: String,
    pub state: SelectionPhase,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingSelection {
    pub source_id: String,
    pub permit: ComputerPermit,
    action: HumanAction,
    pub replace: bool,
    next_try_at: i64,
    state: SelectionPhase,
}
pub(crate) fn rebind_revision(
    task: &Task,
    revision: &str,
    selection: &ComputerSelection,
    now: i64,
) -> Result<u64> {
    ensure(
        task.revision == number(revision)?
            && ComputerSelection::from_scope(&task.scope).is_some()
            && selection.valid()
            && !task.cancel_requested
            && task.deadline > now
            && (matches!(
                task.state,
                State::Queued | State::WaitingDevice | State::WaitingApproval | State::Ready
            ) || (task.state == State::Blocked
                && task.reason.as_deref() == Some("computer_action_not_executed"))),
        "task cannot reselect a window",
    )?;
    let requires_fresh_run = matches!(
        task.state,
        State::WaitingApproval | State::Ready | State::Blocked
    ) || task.reason.as_deref() == Some("model_reply_ready");
    if !requires_fresh_run && selection.scope().as_deref() == Some(&task.scope) {
        Ok(task.revision)
    } else {
        task.revision
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| crate::Error::Rejected("revision overflow".into()))
    }
}
async fn cached_tasks(db: &mut SqliteConnection) -> Result<Vec<Task>> {
    let data: Option<String> = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cache'")
        .fetch_optional(db)
        .await?;
    Ok(data
        .map(|s| serde_json::from_str::<NativeState>(&s))
        .transpose()?
        .unwrap_or_default()
        .tasks)
}
fn eligibility(entry: &PendingSelection, tasks: &[Task], now: i64) -> Option<bool> {
    let task = tasks.iter().find(|task| task.id == entry.permit.task_id)?;
    if task.cancel_requested
        || task.state.terminal()
        || task.deadline <= now
        || matches!(task.state, State::OutcomeUnknown | State::Reconciling)
    {
        return Some(false);
    }
    if let HumanAction::RebindComputer { revision, .. } = &entry.action
        && task.revision.to_string() == *revision
        && task.revision.to_string() != entry.permit.revision
    {
        return None;
    }
    if task.reason.as_deref() == Some("model_reply_ready") {
        return Some(false);
    }
    if task.revision.to_string() == entry.permit.revision
        && entry.permit.selection.scope().as_deref() == Some(&task.scope)
    {
        return Some(true);
    }
    Some(false)
}
impl Journal {
    pub async fn queue_computer_selection(
        &self,
        source_id: &str,
        action: HumanAction,
        selected_at: i64,
    ) -> Result<()> {
        if let HumanAction::SubmitComputer { goal, .. } = &action
            && self.memory_control(source_id, goal, crate::now()).await?
        {
            return Ok(());
        }
        let now = crate::now();
        ensure(
            self.role == ClientRole::Chat && !source_id.is_empty() && source_id.len() <= 128,
            "computer selection requires chat identity",
        )?;
        let expires_at = selected_at
            .checked_add(900_000)
            .ok_or_else(|| crate::Error::Rejected("selection time overflow".into()))?;
        let key = format!("{PREFIX}{source_id}");
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(old) =
            sqlx::query_scalar::<_, String>("SELECT value FROM local_meta WHERE key=?")
                .bind(&key)
                .fetch_optional(&mut *tx)
                .await?
        {
            let old: PendingSelection = serde_json::from_str(&old)?;
            ensure(
                old.permit.selected_at == selected_at
                    && serde_json::to_value(&old.action)? == serde_json::to_value(&action)?,
                "computer selection source conflict",
            )?;
            return Ok(());
        }
        sqlx::query("DELETE FROM local_meta WHERE key LIKE 'computer-selection:%' AND json_extract(value,'$.permit.expires_at')<=?").bind(now).execute(&mut *tx).await?;
        let used: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM local_meta WHERE key LIKE 'computer-selection:%'",
        )
        .fetch_one(&mut *tx)
        .await?;
        ensure(used < 32, "pending computer selection capacity")?;
        let (id, revision, selection, replace, body) = match &action {
            HumanAction::SubmitComputer {
                id,
                goal,
                selection,
            } => {
                ensure(
                    !goal.trim().is_empty() && goal.len() <= 4096,
                    "computer goal size",
                )?;
                (id.clone(), 1, selection.clone(), false, goal.clone())
            }
            HumanAction::RebindComputer {
                task_id,
                revision,
                selection,
            } => {
                let tasks = cached_tasks(&mut tx).await?;
                let task = tasks
                    .iter()
                    .find(|task| task.id == *task_id)
                    .ok_or_else(|| {
                        crate::Error::Rejected("task is not in the current cache".into())
                    })?;
                (
                    task_id.clone(),
                    rebind_revision(task, revision, selection, now)?,
                    selection.clone(),
                    true,
                    format!(
                        "替這個任務重新選擇視窗：{}",
                        task.goal.chars().take(120).collect::<String>()
                    ),
                )
            }
            _ => {
                return Err(crate::Error::Rejected(
                    "computer selection action required".into(),
                ));
            }
        };
        let permit = ComputerPermit {
            task_id: id,
            revision: revision.to_string(),
            selection,
            selected_at,
            expires_at,
        };
        ensure(permit.valid(now), "invalid or expired computer selection")?;
        sqlx::query("INSERT INTO local_pending(id,body) VALUES(?,?)")
            .bind(source_id)
            .bind(body)
            .execute(&mut *tx)
            .await?;
        self.enqueue_in(
            &mut tx,
            DevicePayload::Human {
                source_id: source_id.into(),
                action: action.clone(),
            },
            0,
        )
        .await?;
        let entry = PendingSelection {
            source_id: source_id.into(),
            permit,
            action,
            replace,
            next_try_at: 0,
            state: SelectionPhase::Pending,
        };
        sqlx::query("INSERT INTO local_meta VALUES(?,?)")
            .bind(key)
            .bind(serde_json::to_string(&entry)?)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    pub(crate) async fn take_computer_selection(
        &self,
        now: i64,
    ) -> Result<Option<PendingSelection>> {
        if self.role != ClientRole::Chat {
            return Ok(None);
        }
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let tasks = cached_tasks(&mut tx).await?;
        let rows=sqlx::query("SELECT key,value FROM local_meta WHERE key LIKE 'computer-selection:%' ORDER BY rowid LIMIT 32").fetch_all(&mut *tx).await?;
        let mut chosen = None;
        for row in rows {
            let key: String = row.get("key");
            let mut entry: PendingSelection = serde_json::from_str(&row.get::<String, _>("value"))?;
            if !entry.permit.valid(now) {
                sqlx::query("DELETE FROM local_meta WHERE key=?")
                    .bind(key)
                    .execute(&mut *tx)
                    .await?;
                continue;
            }
            match eligibility(&entry, &tasks, now) {
                Some(false) => entry.state = SelectionPhase::Invalidated,
                Some(true)
                    if chosen.is_none()
                        && entry.state == SelectionPhase::Pending
                        && entry.next_try_at <= now =>
                {
                    entry.next_try_at = now.saturating_add(30_000);
                    chosen = Some(entry.clone());
                }
                _ => continue,
            }
            sqlx::query("UPDATE local_meta SET value=? WHERE key=?")
                .bind(serde_json::to_string(&entry)?)
                .bind(key)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(chosen)
    }
    pub(crate) async fn prepared_computer_selection(&self, source_id: &str) -> Result<bool> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let key = format!("{PREFIX}{source_id}");
        let mut prepared = false;
        if let Some(data) =
            sqlx::query_scalar::<_, String>("SELECT value FROM local_meta WHERE key=?")
                .bind(&key)
                .fetch_optional(&mut *tx)
                .await?
        {
            let mut entry: PendingSelection = serde_json::from_str(&data)?;
            if entry.state == SelectionPhase::Pending {
                let tasks = cached_tasks(&mut tx).await?;
                entry.state = if eligibility(&entry, &tasks, crate::now()) == Some(true)
                    && entry.permit.valid(crate::now())
                {
                    SelectionPhase::Prepared
                } else {
                    SelectionPhase::Invalidated
                };
                sqlx::query("UPDATE local_meta SET value=? WHERE key=?")
                    .bind(serde_json::to_string(&entry)?)
                    .bind(&key)
                    .execute(&mut *tx)
                    .await?;
            }
            prepared = entry.state == SelectionPhase::Prepared;
        }
        tx.commit().await?;
        Ok(prepared)
    }
    pub(crate) async fn computer_selection_statuses(&self) -> Result<Vec<SelectionStatus>> {
        let rows:Vec<String>=sqlx::query_scalar("SELECT value FROM local_meta WHERE key LIKE 'computer-selection:%' ORDER BY rowid LIMIT 32").fetch_all(&self.db).await?;
        rows.into_iter()
            .map(|data| {
                let entry: PendingSelection = serde_json::from_str(&data)?;
                Ok(SelectionStatus {
                    source_id: entry.source_id,
                    task_id: entry.permit.task_id,
                    revision: entry.permit.revision,
                    state: entry.state,
                })
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "computer_selection_tests.rs"]
mod tests;
