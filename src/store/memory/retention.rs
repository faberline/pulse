//! Task retention uses terminal evidence, never creation time or a free lease.
use super::*;
use crate::{State, Task};
use serde_json::json;

mod erase;
#[cfg(test)]
mod tests;
pub const TASK_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
const LIMIT: usize = super::super::TASK_HISTORY_LIMIT as usize;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExpiredTask {
    revision: u64,
    terminal_at: i64,
    erased_at: i64,
    source_id: String,
}
pub(super) fn valid(tasks: &BTreeMap<String, ExpiredTask>) -> bool {
    tasks.len() <= LIMIT
        && tasks.iter().all(|(id, value)| {
            !id.is_empty()
                && id.len() <= 128
                && (1..=i64::MAX as u64).contains(&value.revision)
                && value.terminal_at >= 0
                && value
                    .terminal_at
                    .checked_add(TASK_RETENTION_MS)
                    .is_some_and(|due| due <= value.erased_at)
                && uuid::Uuid::parse_str(&value.source_id)
                    .is_ok_and(|id| id.to_string() == value.source_id)
        })
}

impl Store {
    /// Hold over a bounded data read or projection. Never hold over a provider.
    pub async fn lease_retained_task_data(
        &self,
        task: Option<&str>,
    ) -> Result<Option<TaskMemoryLease<'_>>> {
        let tracked: i64 =
            sqlx::query_scalar("SELECT count(*) FROM task_retention WHERE ? IS NULL OR task_id=?")
                .bind(task)
                .bind(task)
                .fetch_one(&self.pool)
                .await?;
        if tracked == 0 {
            let restrictions = self.memory_guard.read()?;
            if restrictions.as_ref().is_none_or(|r| match task {
                Some(id) => !r.expired_tasks.contains_key(id),
                None => r.expired_tasks.is_empty(),
            }) {
                return Ok(None);
            }
        }
        let lease = self.memory_guard.lease().await?;
        let restrictions = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("task retention restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        super::reconcile(&mut tx, &restrictions).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        Ok(Some(TaskMemoryLease { _lease: lease }))
    }
    /// The adapter supplies its trusted observation time, not a tool timestamp.
    /// This must share the transaction that commits the terminal transition.
    pub async fn record_task_terminal_in(
        db: &mut SqliteConnection,
        id: &str,
        now: i64,
    ) -> Result<()> {
        let task: Task = super::super::load(db, "tasks", id).await?;
        if task.state.terminal() {
            let expired: i64 = sqlx::query_scalar("SELECT count(*) FROM settings WHERE key=?")
                .bind(format!("retention_retired_task:{id}"))
                .fetch_one(&mut *db)
                .await?;
            if expired != 0 {
                return Ok(());
            }
            require(
                now >= task.created_at && now <= i64::MAX - TASK_RETENTION_MS,
                "invalid task terminal time",
            )?;
            sqlx::query("INSERT OR IGNORE INTO task_retention(task_id,terminal_at) VALUES(?,?)")
                .bind(id)
                .bind(now)
                .execute(db)
                .await?;
        }
        Ok(())
    }
    pub(in crate::store) fn wake_task_retention(&self) {
        self.memory_guard.changed.notify_one();
    }
    pub(super) async fn expire_tasks_in(
        &self,
        db: &mut SqliteConnection,
        manifest: &mut Restrictions,
        now: i64,
    ) -> Result<usize> {
        let rows: Vec<(String,i64,String)> = sqlx::query_as("SELECT t.id,r.terminal_at,t.data FROM task_retention r JOIN tasks t ON t.id=r.task_id WHERE r.erased_at IS NULL AND r.terminal_at<=? ORDER BY r.terminal_at,t.id LIMIT 64")
            .bind(now.saturating_sub(TASK_RETENTION_MS)).fetch_all(&mut *db).await?;
        if now < TASK_RETENTION_MS {
            return Ok(0);
        }
        for (id, terminal_at, data) in &rows {
            let task: Task = serde_json::from_str(data)?;
            require(
                task.state.terminal()
                    && *terminal_at >= task.created_at
                    && !super::super::unresolved(db, id).await?,
                "retention requires a verified terminal task",
            )?;
            require(
                manifest.expired_tasks.contains_key(id) || manifest.expired_tasks.len() < LIMIT,
                "task retention restriction capacity",
            )?;
            manifest
                .expired_tasks
                .entry(id.clone())
                .or_insert_with(|| ExpiredTask {
                    revision: task.revision,
                    terminal_at: *terminal_at,
                    erased_at: now,
                    source_id: uuid::Uuid::new_v4().to_string(),
                });
        }
        if !rows.is_empty() {
            require(
                valid(&manifest.expired_tasks),
                "invalid task retention identity; original data preserved",
            )?;
            // A database rollback or old backup cannot remove this restriction.
            self.memory_guard.write(manifest)?;
            crash_boundary(self, "restriction-published");
            reconcile(db, &manifest.expired_tasks).await?;
            crash_boundary(self, "payload-erased");
        }
        Ok(rows.len())
    }
    pub(super) async fn next_task_expiry_in(db: &mut SqliteConnection) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT min(terminal_at+?) FROM task_retention WHERE erased_at IS NULL",
        )
        .bind(TASK_RETENTION_MS)
        .fetch_one(db)
        .await?)
    }
}

pub(super) fn crash_boundary(store: &Store, point: &str) {
    #[cfg(test)]
    if std::env::var("PULSE_TEST_RETENTION_POINT").as_deref() == Ok(point)
        && std::env::var_os("PULSE_TEST_RETENTION_ROOT").is_some_and(|root| {
            std::path::PathBuf::from(root)
                .canonicalize()
                .is_ok_and(|root| Some(root.as_path()) == store.memory_guard.database.parent())
        })
    {
        std::process::exit(73);
    }
    #[cfg(not(test))]
    let _ = (store, point);
}

pub(super) async fn reconcile(
    db: &mut SqliteConnection,
    expired: &BTreeMap<String, ExpiredTask>,
) -> Result<()> {
    let applied: Option<String> =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='task_retention_applied'")
            .fetch_optional(&mut *db)
            .await?;
    let applied = applied
        .map(|v| v.parse::<usize>())
        .transpose()
        .map_err(|_| Error::Rejected("invalid task retention epoch".into()))?
        .unwrap_or(0);
    require(
        applied <= expired.len(),
        "task retention restrictions are stale",
    )?;
    if applied == expired.len() {
        return Ok(());
    }
    for (id, value) in expired {
        let prior: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key=?")
            .bind(format!("retention_retired_task:{id}"))
            .fetch_optional(&mut *db)
            .await?;
        if let Some(prior) = prior {
            require(prior == value.source_id, "task retention identity changed")?;
            continue;
        }
        sqlx::query("INSERT INTO settings(key,value) VALUES(?,?)")
            .bind(format!("retention_retired_task:{id}"))
            .bind(&value.source_id)
            .execute(&mut *db)
            .await?;
        if let Some(data) = sqlx::query_scalar::<_, String>("SELECT data FROM tasks WHERE id=?")
            .bind(id)
            .fetch_optional(&mut *db)
            .await?
        {
            let mut task: Task = serde_json::from_str(&data)?;
            erase::task(db, id, value).await?;
            task.goal = "內容已到期並清除".into();
            task.scope.clear();
            task.decisions.clear();
            task.evidence.clear();
            task.reason = Some("task_retention_expired".into());
            task.active_run = None;
            task.wake_at = None;
            task.cancel_requested = true;
            if !task.state.terminal() {
                task.state = if super::super::unresolved(db, id).await? {
                    State::OutcomeUnknown
                } else {
                    State::Cancelled
                };
            }
            super::super::put(db, "tasks", id, &task).await?;
            sqlx::query("INSERT INTO task_retention(task_id,terminal_at,erased_at) VALUES(?,?,?) ON CONFLICT(task_id) DO UPDATE SET terminal_at=excluded.terminal_at,erased_at=excluded.erased_at")
                .bind(id).bind(value.terminal_at).bind(value.erased_at).execute(&mut *db).await?;
        }
    }
    erase::shared_sources(db).await?;
    sqlx::query("INSERT INTO settings(key,value) VALUES('task_retention_applied',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        .bind(expired.len().to_string()).execute(&mut *db).await?;
    sqlx::query("INSERT INTO settings(key,value) VALUES('memory_task_changes_pending','true') ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        .execute(db).await?;
    Ok(())
}
