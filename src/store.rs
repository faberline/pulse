use crate::*;
use chrono::DateTime;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{
    Row, SqliteConnection, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{collections::BTreeMap, path::Path, time::Duration};

const TASK_LIMIT: usize = 100;
const OUTBOX_LIMIT: i64 = 1_000;
const DAY_LIMIT: i64 = 1_000_000;
const TASK_BUDGET: i64 = 250_000;
const DEADLINE_MS: i64 = 15 * 60 * 1_000;

mod approvals;
mod memory;

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    memory_guard: std::sync::Arc<memory::MemoryGuard>,
}

impl Store {
    pub async fn open(path: impl AsRef<Path>, owner: &str) -> Result<Self> {
        require(!owner.is_empty(), "owner must not be empty")?;
        private_database(path.as_ref())?;
        let memory_guard = memory::MemoryGuard::new(path.as_ref())?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .pragma("secure_delete", "ON")
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let version: String = sqlx::query_scalar("SELECT sqlite_version()")
            .fetch_one(&pool)
            .await?;
        let numbers: Vec<u32> = version.split('.').map(|v| v.parse().unwrap_or(0)).collect();
        require(
            numbers.as_slice() >= [3, 51, 3].as_slice(),
            "SQLite >= 3.51.3 required for WAL safety",
        )?;
        let schema: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        require(
            schema <= 6,
            "database schema is newer than this application",
        )?;
        sqlx::migrate!().run(&pool).await?;
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("INSERT OR IGNORE INTO settings(key,value) VALUES('owner',?)")
            .bind(owner)
            .execute(&mut *tx)
            .await?;
        let existing: String = sqlx::query_scalar("SELECT value FROM settings WHERE key='owner'")
            .fetch_one(&mut *tx)
            .await?;
        require(existing == owner, "database belongs to another owner")?;
        tx.commit().await?;
        let store = Self {
            pool,
            memory_guard: std::sync::Arc::new(memory_guard),
        };
        store.initialize_memory().await?;
        Ok(store)
    }

    pub async fn close(self) {
        self.pool.close().await;
    }

    /// Trusted application extension storage, never exposed to remote callers.
    pub fn application_pool(&self) -> SqlitePool {
        self.pool.clone()
    }

    pub async fn list_tasks(&self, limit: u32) -> Result<Vec<Task>> {
        require((1..=100).contains(&limit), "task page must be 1..100")?;
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT data FROM tasks ORDER BY json_extract(data,'$.created_at') DESC,id LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|s| serde_json::from_str(&s).map_err(Error::from))
            .collect()
    }

    /// Share the caller's transport transaction so limits and dispatch commit together.
    pub async fn authorize_device_read(
        db: &mut SqliteConnection,
        id: &str,
        revision: u64,
        operation: &str,
        now: i64,
    ) -> Result<()> {
        let mut task: Task = load(db, "tasks", id).await?;
        require(
            task.revision == revision
                && (task.scope == "pulse:read"
                    || crate::ComputerSelection::from_scope(&task.scope).is_some())
                && !task.cancel_requested
                && matches!(
                    task.state,
                    State::Queued | State::Blocked | State::WaitingDevice
                )
                && task.deadline > now
                && task.tool_calls < 16
                && !unresolved(db, id).await?,
            "device read policy or limit",
        )?;
        task.tool_calls += 1;
        task.state = State::Dispatched;
        task.reason = Some("device_read_dispatched".into());
        task.evidence.push(format!("dispatch:{operation}"));
        put(db, "tasks", id, &task).await
    }

    /// Trusted remote dispatch shares the transport transaction and requires a live run token.
    pub async fn authorize_remote_read(
        db: &mut SqliteConnection,
        token: &Token,
        operation: &str,
        now: i64,
    ) -> Result<()> {
        let (mut task, _) = current(db, token, now).await?;
        require(
            task.state == State::WaitingDevice
                && crate::read_scope(&task.scope)
                && task.tool_calls < 16
                && !unresolved(db, &task.id).await?,
            "remote read policy or limit",
        )?;
        task.tool_calls += 1;
        task.state = State::Dispatched;
        task.reason = Some("model_read_dispatched".into());
        task.evidence.push(format!("dispatch:{operation}"));
        put(db, "tasks", &task.id, &task).await
    }

    /// Trusted transport observer. Records device evidence, never verifies the goal.
    pub async fn device_observation(
        &self,
        id: &str,
        revision: u64,
        receipt_id: &str,
        unknown: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        Self::device_observation_in(&mut tx, id, revision, receipt_id, unknown).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn device_observation_in(
        db: &mut SqliteConnection,
        id: &str,
        revision: u64,
        receipt_id: &str,
        unknown: bool,
    ) -> Result<()> {
        let mut task: Task = load(db, "tasks", id).await?;
        let evidence = format!("device:{receipt_id}");
        if !task.evidence.contains(&evidence) {
            task.evidence.push(evidence);
        }
        let current = task
            .evidence
            .iter()
            .rev()
            .find(|s| s.starts_with("dispatch:"))
            .is_none_or(|s| s == &format!("dispatch:{receipt_id}"));
        if !task.state.terminal() && current {
            if task.cancel_requested && !unresolved(db, id).await? {
                task.state = State::Cancelled;
                task.reason = Some("device_result_stopped_after_cancel".into());
            } else if task.revision == revision && !task.cancel_requested {
                task.state = if unknown {
                    State::OutcomeUnknown
                } else {
                    State::Blocked
                };
                task.reason = Some(
                    if unknown {
                        "device_outcome_unknown"
                    } else {
                        "device_result_ready_model_not_connected"
                    }
                    .into(),
                );
                task.active_run = None;
                approvals::invalidate(db, id, "local_result_changed_run").await?;
            }
        }
        put(db, "tasks", id, &task).await
    }

    /// Durable receipt only. Routing is a separate, replayable transaction.
    pub async fn ingest(
        &self,
        origin: Origin,
        source: &str,
        source_id: &str,
        payload: Value,
        now: i64,
    ) -> Result<Ingested> {
        require(
            !source.is_empty() && !source_id.is_empty(),
            "empty source identity",
        )?;
        let body = serde_jcs::to_vec(&payload)?;
        require(body.len() <= 256 * 1024, "event too large")?;
        let digest = digest(&json!({"origin": origin.name(), "payload": payload}))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(row) =
            sqlx::query("SELECT id,digest FROM events WHERE source=? AND source_id=?")
                .bind(source)
                .bind(source_id)
                .fetch_optional(&mut *tx)
                .await?
        {
            let id: String = row.get("id");
            if row.get::<String, _>("digest") != digest {
                sqlx::query("INSERT INTO conflicts VALUES(?,?,?)")
                    .bind(&id)
                    .bind(&digest)
                    .bind(now)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                return Err(Error::Conflict(
                    "source identity reused with different content or origin".into(),
                ));
            }
            tx.commit().await?;
            return Ok(Ingested {
                event_id: id,
                duplicate: true,
            });
        }
        let id = id();
        sqlx::query("INSERT INTO events(id,source,source_id,origin,digest,payload,received_at) VALUES(?,?,?,?,?,?,?)")
            .bind(&id).bind(source).bind(source_id).bind(origin.name()).bind(digest)
            .bind(String::from_utf8(body).map_err(|_| Error::Rejected("invalid UTF-8".into()))?).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Ingested {
            event_id: id,
            duplicate: false,
        })
    }

    /// Commands must be part of the persisted source payload, not a second proposal.
    pub async fn route(&self, event_id: &str) -> Result<Outcome> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT origin,payload,received_at,outcome FROM events WHERE id=?")
            .bind(event_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| Error::Missing("event".into()))?;
        if let Some(outcome) = row.get::<Option<String>, _>("outcome") {
            return Ok(serde_json::from_str(&outcome)?);
        }
        let payload: Value = serde_json::from_str(&row.get::<String, _>("payload"))?;
        let origin: String = row.get("origin");
        let now: i64 = row.get("received_at");
        let commands = serde_json::from_value::<Vec<Command>>(
            payload.get("commands").cloned().unwrap_or(json!([])),
        );
        sqlx::query("SAVEPOINT routing").execute(&mut *tx).await?;
        let result = match commands {
            Ok(commands) => apply(&mut tx, event_id, &origin, commands, now).await,
            Err(_) => Err(Error::Rejected("invalid commands".into())),
        };
        let outcome = match result {
            Ok(value) => value,
            Err(Error::Rejected(reason) | Error::Conflict(reason) | Error::Missing(reason)) => {
                sqlx::query("ROLLBACK TO routing").execute(&mut *tx).await?;
                Outcome::Rejected { reason }
            }
            Err(error) => return Err(error),
        };
        sqlx::query("RELEASE routing").execute(&mut *tx).await?;
        sqlx::query("UPDATE events SET outcome=? WHERE id=?")
            .bind(serde_json::to_string(&outcome)?)
            .bind(event_id)
            .execute(&mut *tx)
            .await?;
        notify(
            &mut tx,
            &format!("event:{event_id}"),
            &json!({"event_id":event_id,"outcome":outcome}),
        )
        .await?;
        tx.commit().await?;
        Ok(outcome)
    }

    pub async fn pending_events(&self, limit: u32) -> Result<Vec<String>> {
        require((1..=100).contains(&limit), "event page must be 1..100")?;
        Ok(sqlx::query_scalar(
            "SELECT id FROM events WHERE outcome IS NULL ORDER BY received_at,id LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn task(&self, id: &str) -> Result<Task> {
        let mut db = self.pool.acquire().await?;
        load(&mut db, "tasks", id).await
    }

    /// Durable tool observations, including failures and late results, for context recovery.
    pub async fn receipts(&self, task_id: &str) -> Result<Vec<Receipt>> {
        let rows:Vec<String>=sqlx::query_scalar("SELECT receipts.data FROM receipts JOIN operations ON operations.id=receipts.operation_id WHERE json_extract(operations.data,'$.token.task_id')=? ORDER BY operations.id").bind(task_id).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|s| serde_json::from_str(&s).map_err(Error::from))
            .collect()
    }

    pub async fn claim_next(&self, versions: &Versions, now: i64) -> Result<Option<Run>> {
        Ok(self.claim_next_with_changes(versions, now).await?.0)
    }

    /// Returns committed task changes even when no work can be claimed.
    pub async fn claim_next_with_changes(
        &self,
        versions: &Versions,
        now: i64,
    ) -> Result<(Option<Run>, bool)> {
        require(versions.valid(), "invalid version manifest")?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut changed = approvals::expire(&mut tx, now).await?;
        let mut tasks = tasks(&mut tx).await?;
        for task in &mut tasks {
            if !task.state.terminal() && task.deadline <= now {
                let state = if unresolved(&mut tx, &task.id).await? {
                    State::OutcomeUnknown
                } else {
                    State::Blocked
                };
                if task.state != state || task.reason.as_deref() != Some("deadline_exhausted") {
                    task.state = state;
                    task.reason = Some("deadline_exhausted".into());
                    put(&mut tx, "tasks", &task.id, task).await?;
                    changed = true;
                }
            } else if task.state == State::RetryWait && task.wake_at.is_some_and(|t| t <= now) {
                task.state = State::Queued;
                task.wake_at = None;
                put(&mut tx, "tasks", &task.id, task).await?;
                changed = true;
            }
        }
        if tasks.iter().filter(|t| t.state.active()).count() >= 2 {
            tx.commit().await?;
            return Ok((None, changed));
        }
        tasks.sort_by_key(|t| (std::cmp::Reverse(t.priority), t.created_at, t.id.clone()));
        for mut task in tasks {
            if task.state != State::Queued || task.cancel_requested || task.deadline <= now {
                continue;
            }
            if unresolved(&mut tx, &task.id).await? {
                continue;
            }
            let mut dependencies_ready = true;
            for dependency in &task.dependencies {
                if load::<Task>(&mut tx, "tasks", dependency).await?.state != State::Succeeded {
                    dependencies_ready = false;
                }
            }
            if !dependencies_ready {
                continue;
            }
            if task.requires_device {
                task.state = State::WaitingDevice;
                put(&mut tx, "tasks", &task.id, &task).await?;
                changed = true;
                continue;
            }
            let mut run = if let Some(run_id) = &task.active_run {
                let run: Run = load(&mut tx, "runs", run_id).await?;
                if run.versions != *versions || run.token.revision != task.revision {
                    task.state = State::Blocked;
                    task.reason = Some("version_incompatible".into());
                    put(&mut tx, "tasks", &task.id, &task).await?;
                    changed = true;
                    continue;
                }
                run
            } else {
                Run {
                    token: Token {
                        run_id: id(),
                        task_id: task.id.clone(),
                        revision: task.revision,
                        epoch: 0,
                    },
                    versions: versions.clone(),
                    checkpoint: Value::Null,
                }
            };
            approvals::invalidate(&mut tx, &task.id, "run_reclaimed").await?;
            run.token.epoch = epoch(&mut tx).await?;
            task.state = State::Planning;
            task.reason = None;
            task.active_run = Some(run.token.run_id.clone());
            put(&mut tx, "runs", &run.token.run_id, &run).await?;
            put(&mut tx, "tasks", &task.id, &task).await?;
            tx.commit().await?;
            return Ok((Some(run), true));
        }
        tx.commit().await?;
        Ok((None, changed))
    }

    pub async fn advance(
        &self,
        token: &Token,
        next: State,
        checkpoint: Value,
        evidence: &str,
        wake_at: Option<i64>,
        now: i64,
    ) -> Result<()> {
        require(
            serde_jcs::to_vec(&checkpoint)?.len() <= 64 * 1024 && evidence.len() <= 64 * 1024,
            "checkpoint or evidence too large",
        )?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let (mut task, mut run) = current(&mut tx, token, now).await?;
        require(task.state.permits(next), "invalid state transition")?;
        require(
            next != State::Dispatched,
            "dispatch requires a durable operation intent",
        )?;
        require(
            !next.terminal() || !evidence.is_empty(),
            "terminal result needs evidence",
        )?;
        require(
            !unresolved(&mut tx, &task.id).await?
                || matches!(
                    next,
                    State::OutcomeUnknown | State::Reconciling | State::Blocked
                ),
            "operation must be reconciled first",
        )?;
        require(
            next != State::RetryWait || wake_at.is_some_and(|t| t > now && t < task.deadline),
            "retry must have a bounded future wake time",
        )?;
        if next == State::RetryWait {
            if task.retry_count >= 3 {
                run.checkpoint = checkpoint;
                put(&mut tx, "runs", &token.run_id, &run).await?;
                block(&mut tx, &mut task, "retry_exhausted").await?;
                tx.commit().await?;
                return Err(Error::Rejected("retry limit reached".into()));
            }
            task.retry_count += 1;
        }
        run.checkpoint = checkpoint;
        task.state = next;
        task.reason = (!evidence.is_empty()).then(|| evidence.into());
        task.wake_at = wake_at;
        if !evidence.is_empty() {
            task.evidence.push(evidence.into());
        }
        put(&mut tx, "runs", &token.run_id, &run).await?;
        put(&mut tx, "tasks", &task.id, &task).await?;
        notify(
            &mut tx,
            &format!("run:{}:{}", token.run_id, id()),
            &json!({"task_id":task.id,"state":next}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Call on startup while holding exclusive process ownership. Reclaims no effects.
    pub async fn recover(&self, versions: &Versions, now: i64) -> Result<()> {
        require(versions.valid(), "invalid manifest")?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        for mut task in tasks(&mut tx).await? {
            if task.state.terminal() {
                continue;
            }
            if let Some(run_id) = &task.active_run {
                let invalidated = approvals::invalidate(&mut tx, &task.id, "run_restarted").await?;
                let mut run: Run = load(&mut tx, "runs", run_id).await?;
                run.token.epoch = epoch(&mut tx).await?;
                put(&mut tx, "runs", run_id, &run).await?;
                if unresolved(&mut tx, &task.id).await? {
                    task.state = State::OutcomeUnknown;
                    task.reason = Some("restart_unknown_operation".into());
                } else if run.versions != *versions {
                    task.state = State::Blocked;
                    task.reason = Some("version_incompatible".into());
                } else if task.deadline <= now {
                    task.state = State::Blocked;
                    task.reason = Some("deadline_exhausted".into());
                } else if invalidated {
                    task.state = State::Blocked;
                    task.reason = Some("approval_restart_review_required".into());
                } else if task.state.active() {
                    task.state = State::Queued;
                }
                put(&mut tx, "tasks", &task.id, &task).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Coordinator-only goal verification. A tool receipt alone cannot finish a task.
    pub async fn finish(
        &self,
        token: &Token,
        success: bool,
        evidence: &str,
        now: i64,
    ) -> Result<()> {
        require(
            !evidence.is_empty() && evidence.len() <= 64 * 1024,
            "goal completion requires bounded evidence",
        )?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let (mut task, _) = current(&mut tx, token, now).await?;
        require(
            matches!(task.state, State::Planning | State::Ready | State::Running)
                && !unresolved(&mut tx, &task.id).await?,
            "goal cannot finish with an unknown operation or waiting state",
        )?;
        task.state = if success {
            State::Succeeded
        } else {
            State::Failed
        };
        task.reason = Some(evidence.into());
        task.evidence.push(evidence.into());
        approvals::invalidate(&mut tx, &task.id, "task_finished").await?;
        put(&mut tx, "tasks", &task.id, &task).await?;
        notify(
            &mut tx,
            &format!("terminal:{}:{}", task.id, task.revision),
            &json!({"task_id":task.id,"state":task.state}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn reserve(
        &self,
        token: &Token,
        call_id: &str,
        maximum_micro_usd: i64,
        rate_version: &str,
        now: i64,
    ) -> Result<bool> {
        require(
            !call_id.is_empty()
                && maximum_micro_usd > 0
                && maximum_micro_usd <= TASK_BUDGET
                && !rate_version.is_empty(),
            "invalid cost reservation",
        )?;
        let day = DateTime::from_timestamp_millis(now)
            .ok_or_else(|| Error::Rejected("invalid time".into()))?
            .with_timezone(&chrono_tz::Asia::Taipei)
            .format("%Y-%m-%d")
            .to_string();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let (mut task, _) = current(&mut tx, token, now).await?;
        if let Some(row) =
            sqlx::query("SELECT task_id,reserved,rate_version FROM budget WHERE call_id=?")
                .bind(call_id)
                .fetch_optional(&mut *tx)
                .await?
        {
            require(
                row.get::<String, _>("task_id") == task.id
                    && row.get::<i64, _>("reserved") == maximum_micro_usd
                    && row.get::<String, _>("rate_version") == rate_version,
                "call ID conflict",
            )?;
            return Ok(false);
        }
        require(
            task.state == State::Planning,
            "model request in invalid phase",
        )?;
        if task.model_calls >= 8 {
            block(&mut tx, &mut task, "model_calls_exhausted").await?;
            tx.commit().await?;
            return Err(Error::Rejected("model call limit reached".into()));
        }
        let task_used: i64 =
            sqlx::query_scalar("SELECT COALESCE(SUM(charged),0) FROM budget WHERE task_id=?")
                .bind(&task.id)
                .fetch_one(&mut *tx)
                .await?;
        let day_used: i64 =
            sqlx::query_scalar("SELECT COALESCE(SUM(charged),0) FROM budget WHERE day=?")
                .bind(&day)
                .fetch_one(&mut *tx)
                .await?;
        if task_used > TASK_BUDGET - maximum_micro_usd || day_used > DAY_LIMIT - maximum_micro_usd {
            block(&mut tx, &mut task, "budget_exhausted").await?;
            tx.commit().await?;
            return Err(Error::Rejected("budget exhausted".into()));
        }
        sqlx::query("INSERT INTO budget(call_id,task_id,day,reserved,charged,state,rate_version) VALUES(?,?,?,?,?,'reserved',?)")
            .bind(call_id).bind(&task.id).bind(day).bind(maximum_micro_usd).bind(maximum_micro_usd).bind(rate_version).execute(&mut *tx).await?;
        task.model_calls += 1;
        put(&mut tx, "tasks", &task.id, &task).await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn cost_unknown(&self, call_id: &str) -> Result<()> {
        let changed =
            sqlx::query("UPDATE budget SET state='unknown' WHERE call_id=? AND state='reserved'")
                .bind(call_id)
                .execute(&self.pool)
                .await?
                .rows_affected();
        require(changed == 1, "call missing or no longer reserved")
    }

    pub async fn settle(&self, call_id: &str, actual_micro_usd: i64, evidence: &str) -> Result<()> {
        require(
            actual_micro_usd >= 0 && !evidence.is_empty(),
            "settlement requires usage evidence",
        )?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT state,charged,evidence FROM budget WHERE call_id=?")
            .bind(call_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| Error::Missing("call".into()))?;
        if row.get::<String, _>("state") == "settled" {
            require(
                row.get::<i64, _>("charged") == actual_micro_usd
                    && row.get::<Option<String>, _>("evidence").as_deref() == Some(evidence),
                "settlement conflict",
            )?;
        } else {
            sqlx::query("UPDATE budget SET state='settled',charged=?,evidence=? WHERE call_id=?")
                .bind(actual_micro_usd)
                .bind(evidence)
                .bind(call_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn prepare_operation(
        &self,
        token: &Token,
        target: &str,
        intent: Value,
        now: i64,
    ) -> Result<Operation> {
        require(
            target.starts_with("fixture:"),
            "v0.2 permits simulated effects only",
        )?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let (mut task, _) = current(&mut tx, token, now).await?;
        let gated: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operation_requests WHERE json_extract(data,'$.proposal.token.run_id')=? AND json_extract(data,'$.proposal.token.revision')=? AND json_extract(data,'$.proposal.token.epoch')=?")
            .bind(&token.run_id).bind(token.revision.to_string()).bind(token.epoch.to_string()).fetch_one(&mut *tx).await?;
        require(gated == 0, "approved requests require gated dispatch")?;
        require(
            task.state == State::Ready && !unresolved(&mut tx, &task.id).await?,
            "operation not ready",
        )?;
        if task.tool_calls >= 16 {
            block(&mut tx, &mut task, "tool_calls_exhausted").await?;
            tx.commit().await?;
            return Err(Error::Rejected("tool call limit reached".into()));
        }
        require(
            serde_jcs::to_vec(&intent)?.len() <= 64 * 1024,
            "intent too large",
        )?;
        let operation_id = id();
        let operation = Operation {
            id: operation_id.clone(),
            idempotency_key: operation_id,
            token: token.clone(),
            target: target.into(),
            intent_digest: digest(&json!({"token":token,"target":target,"intent":intent}))?,
            intent,
            state: OperationState::Unknown,
        };
        put(&mut tx, "operations", &operation.id, &operation).await?;
        task.state = State::Dispatched;
        task.tool_calls += 1;
        put(&mut tx, "tasks", &task.id, &task).await?;
        tx.commit().await?;
        Ok(operation)
    }

    /// Old tokens may supply evidence for their original operation, never a new one.
    pub async fn record_receipt(&self, receipt: &Receipt) -> Result<ReceiptEffect> {
        require(!receipt.evidence.is_empty(), "receipt needs evidence")?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let effect = receipt_in(&mut tx, receipt).await?;
        tx.commit().await?;
        Ok(effect)
    }
    /// Adapter receipt and transport acknowledgement can share one transaction.
    pub async fn record_receipt_in(
        db: &mut SqliteConnection,
        receipt: &Receipt,
    ) -> Result<ReceiptEffect> {
        receipt_in(db, receipt).await
    }
    /// A trusted local adapter rejected this grant before issuing any effect command.
    pub async fn browser_not_executed_in(
        db: &mut SqliteConnection,
        token: &Token,
        operation_id: &str,
        evidence: &str,
    ) -> Result<()> {
        Self::action_not_executed_in(db, token, operation_id, evidence).await
    }
    pub async fn action_not_executed_in(
        db: &mut SqliteConnection,
        token: &Token,
        operation_id: &str,
        evidence: &str,
    ) -> Result<()> {
        let mut operation: Operation = load(db, "operations", operation_id).await?;
        require(
            operation.token == *token
                && operation.state == OperationState::Unknown
                && !evidence.is_empty()
                && evidence.len() <= 1024,
            "invalid not-executed proof",
        )?;
        operation.state = OperationState::NotExecuted;
        put(db, "operations", operation_id, &operation).await?;
        let mut task: Task = load(db, "tasks", &token.task_id).await?;
        task.evidence.push(evidence.into());
        if !task.state.terminal()
            && task.revision == token.revision
            && task.active_run.as_deref() == Some(&token.run_id)
        {
            task.state = State::Blocked;
            task.reason = Some(
                if operation.target.starts_with("computer:") {
                    "computer_action_not_executed"
                } else {
                    "browser_action_not_executed"
                }
                .into(),
            );
            task.active_run = None;
        }
        put(db, "tasks", &task.id, &task).await
    }

    pub async fn reconcile(
        &self,
        operation_id: &str,
        resolution: Resolution,
        now: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut op: Operation = load(&mut tx, "operations", operation_id).await?;
        require(
            matches!(
                op.state,
                OperationState::Unknown | OperationState::Ambiguous
            ),
            "operation already reconciled",
        )?;
        let mut task: Task = load(&mut tx, "tasks", &op.token.task_id).await?;
        match resolution {
            Resolution::Executed(receipt) => {
                require(
                    receipt.operation_id == operation_id,
                    "wrong operation receipt",
                )?;
                receipt_in(&mut tx, &receipt).await?;
                // A verified conclusion may recover an old epoch, not complete a new goal.
                task = load(&mut tx, "tasks", &task.id).await?;
                if !task.state.terminal() {
                    if task.cancel_requested {
                        task.state = State::Cancelled;
                    } else if task.revision == op.token.revision {
                        task.state = State::Queued;
                    } else {
                        task.state = State::Blocked;
                    }
                    task.reason = Some("verified_old_operation_result".into());
                    put(&mut tx, "tasks", &task.id, &task).await?;
                }
            }
            Resolution::NotExecuted { evidence } => {
                require(!evidence.is_empty(), "reconciliation requires proof")?;
                op.state = OperationState::NotExecuted;
                put(&mut tx, "operations", &op.id, &op).await?;
                task.evidence.push(evidence);
                if !task.state.terminal() {
                    task.state = if task.cancel_requested {
                        State::Cancelled
                    } else if task.deadline <= now {
                        State::Blocked
                    } else {
                        State::Queued
                    };
                    task.active_run = None;
                    task.reason = Some("verified_not_executed".into());
                    put(&mut tx, "tasks", &task.id, &task).await?;
                }
            }
            Resolution::Ambiguous { evidence } => {
                require(!evidence.is_empty(), "reconciliation needs evidence")?;
                op.state = OperationState::Ambiguous;
                put(&mut tx, "operations", &op.id, &op).await?;
                if !task.state.terminal() {
                    task.state = State::Blocked;
                    task.reason = Some("operation_ambiguous".into());
                    task.evidence.push(evidence);
                    put(&mut tx, "tasks", &task.id, &task).await?;
                }
            }
        }
        notify(
            &mut tx,
            &format!("reconcile:{}:{}", op.id, id()),
            &json!({"task_id":task.id,"reconciled_operation":op.id}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn pending_operations(&self, task_id: &str) -> Result<Vec<Operation>> {
        let rows:Vec<String>=sqlx::query_scalar("SELECT data FROM operations WHERE json_extract(data,'$.token.task_id')=? AND json_extract(data,'$.state') IN ('unknown','ambiguous') ORDER BY id").bind(task_id).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|s| serde_json::from_str(&s).map_err(Error::from))
            .collect()
    }

    pub async fn outbox(&self, limit: u32) -> Result<Vec<OutboxItem>> {
        require((1..=100).contains(&limit), "outbox page must be 1..100")?;
        let rows=sqlx::query("SELECT sequence,dedupe_key,payload FROM outbox WHERE acknowledged=0 ORDER BY sequence LIMIT ?").bind(limit).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                Ok(OutboxItem {
                    sequence: r.get::<i64, _>("sequence") as u64,
                    key: r.get("dedupe_key"),
                    payload: serde_json::from_str(&r.get::<String, _>("payload"))?,
                })
            })
            .collect()
    }

    pub async fn notification_capacity_available(&self) -> Result<bool> {
        let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox WHERE acknowledged=0")
            .fetch_one(&self.pool)
            .await?;
        Ok(pending < OUTBOX_LIMIT)
    }

    pub async fn acknowledge(&self, sequence: u64, key: &str) -> Result<()> {
        require(sequence <= i64::MAX as u64, "invalid sequence")?;
        let changed =
            sqlx::query("UPDATE outbox SET acknowledged=1 WHERE sequence=? AND dedupe_key=?")
                .bind(sequence as i64)
                .bind(key)
                .execute(&self.pool)
                .await?
                .rows_affected();
        require(changed == 1, "ACK does not match an outbox item")
    }

    pub async fn health(&self, now: i64) -> Result<Health> {
        let mut tx = self.pool.begin().await?;
        let mut states = BTreeMap::new();
        let mut age = 0;
        for row in sqlx::query("SELECT json_extract(data,'$.state') AS state,COUNT(*) AS count,MIN(json_extract(data,'$.created_at')) AS oldest FROM tasks GROUP BY json_extract(data,'$.state')").fetch_all(&mut *tx).await? {
            let key:String=row.get("state");
            if key=="queued" {age=now.saturating_sub(row.get::<i64,_>("oldest")).max(0);}
            states.insert(key,row.get::<i64,_>("count") as u64);
        }
        let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox WHERE acknowledged=0")
            .fetch_one(&mut *tx)
            .await?;
        let ops:i64=sqlx::query_scalar("SELECT COUNT(*) FROM operations WHERE json_extract(data,'$.state') IN ('unknown','ambiguous')").fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(Health {
            states,
            queue_age_ms: age,
            pending_notifications: pending as u64,
            unresolved_operations: ops as u64,
        })
    }
}

async fn apply(
    db: &mut SqliteConnection,
    event_id: &str,
    origin: &str,
    commands: Vec<Command>,
    now: i64,
) -> Result<Outcome> {
    require(commands.len() <= 100, "too many commands")?;
    require(
        !commands
            .iter()
            .any(|c| matches!(c, Command::Clarify { .. }))
            || commands.len() == 1,
        "clarification must be a standalone event",
    )?;
    let mut ids = Vec::new();
    for command in commands {
        let mutates = !matches!(&command, Command::Query { .. } | Command::Clarify { .. });
        require(
            !mutates || origin == "human",
            "only human ingress can change tasks",
        )?;
        let task_id = match command {
            Command::Create {
                id,
                goal,
                scope,
                priority,
                requires_device,
                parent,
                dependencies,
            } => {
                require(
                    !id.is_empty() && !goal.trim().is_empty() && crate::read_scope(&scope),
                    "invalid task goal/scope/ID",
                )?;
                let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE id=?")
                    .bind(&id)
                    .fetch_one(&mut *db)
                    .await?;
                require(existing == 0, "task ID already exists")?;
                require(
                    tasks(db)
                        .await?
                        .iter()
                        .filter(|t| !t.state.terminal())
                        .count()
                        < TASK_LIMIT,
                    "task queue full",
                )?;
                if let Some(parent) = &parent {
                    let _: Task = load(db, "tasks", parent).await?;
                }
                for dependency in &dependencies {
                    let _: Task = load(db, "tasks", dependency).await?;
                }
                let task = Task {
                    id: id.clone(),
                    goal: goal.clone(),
                    scope,
                    authorization_event: event_id.into(),
                    revision: 1,
                    priority,
                    state: if requires_device {
                        State::WaitingDevice
                    } else {
                        State::Queued
                    },
                    reason: None,
                    decisions: vec![Decision {
                        event_id: event_id.into(),
                        text: goal,
                        supersedes: None,
                    }],
                    parent,
                    dependencies,
                    requires_device,
                    created_at: now,
                    deadline: now
                        .checked_add(DEADLINE_MS)
                        .ok_or_else(|| Error::Rejected("time overflow".into()))?,
                    wake_at: None,
                    active_run: None,
                    cancel_requested: false,
                    model_calls: 0,
                    tool_calls: 0,
                    retry_count: 0,
                    evidence: Vec::new(),
                };
                put(db, "tasks", &id, &task).await?;
                id
            }
            Command::Revise {
                id,
                expected_revision,
                goal,
                scope,
            } => {
                let mut task: Task = load(db, "tasks", &id).await?;
                require(
                    task.revision == expected_revision
                        && !task.state.terminal()
                        && !task.cancel_requested
                        && !goal.trim().is_empty()
                        && scope.as_ref().is_none_or(|s| crate::read_scope(s)),
                    "revision or task state conflict",
                )?;
                task.revision += 1;
                task.goal = goal.clone();
                if let Some(scope) = scope {
                    task.scope = scope;
                    task.authorization_event = event_id.into();
                }
                task.decisions.push(Decision {
                    event_id: event_id.into(),
                    text: goal,
                    supersedes: task.decisions.last().map(|d| d.event_id.clone()),
                });
                task.active_run = None;
                approvals::invalidate(db, &id, "task_revised").await?;
                task.state = if unresolved(db, &id).await? {
                    State::OutcomeUnknown
                } else if task.requires_device {
                    State::WaitingDevice
                } else {
                    State::Queued
                };
                put(db, "tasks", &id, &task).await?;
                id
            }
            Command::Cancel {
                id,
                expected_revision,
            } => {
                let mut task: Task = load(db, "tasks", &id).await?;
                require(
                    task.revision == expected_revision && !task.state.terminal(),
                    "revision or task state conflict",
                )?;
                task.revision += 1;
                task.cancel_requested = true;
                approvals::invalidate(db, &id, "task_cancelled").await?;
                task.state = if unresolved(db, &id).await? {
                    State::OutcomeUnknown
                } else {
                    State::Cancelled
                };
                task.evidence.push(format!(
                    "cancel:{event_id}:no_unresolved_effect={}",
                    task.state == State::Cancelled
                ));
                put(db, "tasks", &id, &task).await?;
                id
            }
            Command::ReconcileRead {
                id,
                expected_revision,
            } => {
                reconcile_read(db, &id, expected_revision, event_id, now).await?;
                id
            }
            Command::ApproveOperation { binding } => {
                approvals::decide(db, event_id, &binding, true, now).await?;
                binding.task_id
            }
            Command::RejectOperation { binding } => {
                approvals::decide(db, event_id, &binding, false, now).await?;
                binding.task_id
            }
            Command::Resume {
                id,
                expected_revision,
            } => {
                let mut task: Task = load(db, "tasks", &id).await?;
                require(
                    task.revision == expected_revision
                        && task.state == State::Blocked
                        && !task.cancel_requested
                        && task.deadline > now
                        && !unresolved(db, &id).await?,
                    "task cannot resume",
                )?;
                // Cannot silently clear a version pin or reset any counter/deadline.
                require(
                    task.reason.as_deref() != Some("version_incompatible"),
                    "version migration is required",
                )?;
                require(
                    task.reason.as_deref() != Some("device_result_ready_model_not_connected"),
                    "read evidence must be reviewed first",
                )?;
                require(
                    crate::interaction_url(&task.scope).is_none()
                        || !matches!(
                            task.reason.as_deref(),
                            Some(
                                "operation_rejected"
                                    | "operation_approval_expired"
                                    | "browser_action_not_executed"
                                    | "browser_approval_invalid"
                                    | "approval_restart_review_required"
                            )
                        ),
                    "browser observation requires a fresh Human revision",
                )?;
                task.state = State::Queued;
                task.reason = None;
                put(db, "tasks", &id, &task).await?;
                id
            }
            Command::Query { ids: query } => {
                for id in query {
                    let _: Task = load(db, "tasks", &id).await?;
                    ids.push(id);
                }
                continue;
            }
            Command::Clarify { question } => {
                require(!question.trim().is_empty(), "empty clarification")?;
                return Ok(Outcome::Clarification { question });
            }
        };
        ids.push(task_id);
    }
    ids.sort();
    ids.dedup();
    for id in &ids {
        sqlx::query("INSERT OR IGNORE INTO event_tasks VALUES(?,?)")
            .bind(event_id)
            .bind(id)
            .execute(&mut *db)
            .await?;
    }
    Ok(Outcome::Applied { task_ids: ids })
}

async fn current(db: &mut SqliteConnection, token: &Token, now: i64) -> Result<(Task, Run)> {
    let task: Task = load(db, "tasks", &token.task_id).await?;
    let run: Run = load(db, "runs", &token.run_id).await?;
    require(
        task.active_run.as_deref() == Some(&token.run_id)
            && task.revision == token.revision
            && run.token == *token
            && !task.cancel_requested
            && !task.state.terminal()
            && task.deadline > now,
        "stale run, cancelled task or deadline",
    )?;
    Ok((task, run))
}

async fn receipt_in(db: &mut SqliteConnection, receipt: &Receipt) -> Result<ReceiptEffect> {
    require(!receipt.evidence.is_empty(), "empty receipt evidence")?;
    let mut operation: Operation = load(db, "operations", &receipt.operation_id).await?;
    require(
        operation.token == receipt.token,
        "receipt token does not match operation",
    )?;
    let hash = digest(receipt)?;
    if let Some(existing) =
        sqlx::query_scalar::<_, String>("SELECT digest FROM receipts WHERE operation_id=?")
            .bind(&operation.id)
            .fetch_optional(&mut *db)
            .await?
    {
        require(existing == hash, "receipt content conflict")?;
        return Ok(ReceiptEffect::Duplicate);
    }
    require(
        matches!(
            operation.state,
            OperationState::Unknown | OperationState::Ambiguous
        ),
        "operation was already resolved",
    )?;
    sqlx::query("INSERT INTO receipts VALUES(?,?,?)")
        .bind(&operation.id)
        .bind(hash)
        .bind(serde_json::to_string(receipt)?)
        .execute(&mut *db)
        .await?;
    operation.state = OperationState::Completed;
    put(db, "operations", &operation.id, &operation).await?;
    let mut task: Task = load(db, "tasks", &receipt.token.task_id).await?;
    task.evidence.push(receipt.evidence.clone());
    let run: Run = load(db, "runs", &receipt.token.run_id).await?;
    let applicable = !task.state.terminal()
        && !task.cancel_requested
        && task.revision == receipt.token.revision
        && task.active_run.as_deref() == Some(&receipt.token.run_id)
        && run.token == receipt.token;
    if applicable {
        task.state = State::Planning;
        task.reason = Some(receipt.evidence.clone());
    } else if !task.state.terminal() {
        // A late result ends uncertainty but never starts work or completes a revised goal.
        task.state = if task.cancel_requested {
            State::Cancelled
        } else {
            State::Blocked
        };
        task.reason = Some("late_operation_result_requires_review".into());
    }
    put(db, "tasks", &task.id, &task).await?;
    notify(
        db,
        &format!("receipt:{}", operation.id),
        &json!({"task_id":task.id,"operation_id":operation.id,"late":!applicable}),
    )
    .await?;
    Ok(if applicable {
        ReceiptEffect::Applied
    } else {
        ReceiptEffect::Late
    })
}

/// Human review of transport-validated read receipts. Never invents a missing outcome.
async fn reconcile_read(
    db: &mut SqliteConnection,
    id: &str,
    revision: u64,
    event: &str,
    now: i64,
) -> Result<()> {
    let mut task: Task = load(db, "tasks", id).await?;
    require(
        task.revision == revision
            && !task.cancel_requested
            && crate::read_scope(&task.scope)
            && task.deadline > now,
        "read review revision, scope or deadline",
    )?;
    if task.state == State::Blocked && task.reason.as_deref() == Some("read_receipts_verified") {
        return Ok(());
    }
    require(
        task.state == State::OutcomeUnknown
            || (task.state == State::Blocked
                && task.reason.as_deref() == Some("device_result_ready_model_not_connected")),
        "no local outcome to review",
    )?;
    require(!unresolved(db, id).await?, "read outcome is still unknown")?;
    let unsettled: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM budget WHERE task_id=? AND state!='settled'")
            .bind(id)
            .fetch_one(&mut *db)
            .await?;
    require(
        unsettled == 0,
        "model cost is still unknown; keep reservations",
    )?;
    // A failed proof rolls this intermediate state back in the same transaction.
    task.state = State::Reconciling;
    put(db, "tasks", id, &task).await?;
    let rows = sqlx::query(
        "SELECT id,task_revision,state,result FROM link_reads WHERE task_id=? ORDER BY rowid",
    )
    .bind(id)
    .fetch_all(&mut *db)
    .await?;
    require(!rows.is_empty(), "no durable local receipt")?;
    for row in &rows {
        let encoded: String = row
            .try_get("result")
            .map_err(|_| Error::Rejected("missing read receipt".into()))?;
        let receipt: ReadOutcome = serde_json::from_str(&encoded)?;
        let state: String = row.get("state");
        require(
            receipt.valid()
                && matches!(
                    (state.as_str(), &receipt),
                    ("completed", ReadOutcome::Completed { .. })
                        | ("rejected", ReadOutcome::Rejected { .. })
                ),
            "read evidence is not a known validated outcome",
        )?;
    }
    task.active_run = None;
    if let Some(latest) = rows
        .iter()
        .rev()
        .find(|row| row.get::<String, _>("task_revision") == revision.to_string())
    {
        let data:Option<String>=sqlx::query_scalar("SELECT data FROM runs WHERE json_extract(data,'$.token.task_id')=? AND json_extract(data,'$.token.revision')=? AND json_extract(data,'$.checkpoint.operation_id')=? AND json_extract(data,'$.checkpoint.phase')='sent_tool'").bind(id).bind(revision.to_string()).bind(latest.get::<String,_>("id")).fetch_optional(&mut *db).await?;
        if let Some(data) = data {
            let mut run: Run = serde_json::from_str(&data)?;
            let round = run.checkpoint["round"]
                .as_u64()
                .ok_or_else(|| Error::Rejected("missing model round".into()))?;
            require(
                round < 8 && run.token.task_id == id && run.token.revision == revision,
                "read run binding",
            )?;
            // Restore the same logical run, advance exactly one round, and retain its calls.
            // claim_next will mint a new epoch before any model invocation.
            run.checkpoint["phase"] = json!("ready");
            run.checkpoint["round"] = json!(round + 1);
            task.active_run = Some(run.token.run_id.clone());
            put(db, "runs", &run.token.run_id, &run).await?;
        }
    }
    task.state = State::Blocked;
    task.reason = Some("read_receipts_verified".into());
    task.evidence.push(format!("read-review:{event}"));
    for row in rows {
        let evidence = format!(
            "verified-read:{}:revision:{}",
            row.get::<String, _>("id"),
            row.get::<String, _>("task_revision")
        );
        if !task.evidence.contains(&evidence) {
            task.evidence.push(evidence);
        }
    }
    put(db, "tasks", id, &task).await
}

async fn unresolved(db: &mut SqliteConnection, task_id: &str) -> Result<bool> {
    let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM operations WHERE json_extract(data,'$.token.task_id')=? AND json_extract(data,'$.state') IN ('unknown','ambiguous')").bind(task_id).fetch_one(&mut *db).await?;
    let reads:i64=sqlx::query_scalar("SELECT COUNT(*) FROM link_reads WHERE task_id=? AND state IN ('sent','accepted','unknown')").bind(task_id).fetch_one(db).await?;
    Ok(count > 0 || reads > 0)
}

async fn tasks(db: &mut SqliteConnection) -> Result<Vec<Task>> {
    let rows:Vec<String>=sqlx::query_scalar("SELECT data FROM tasks WHERE json_extract(data,'$.state') NOT IN ('succeeded','failed','cancelled') ORDER BY id").fetch_all(db).await?;
    rows.into_iter()
        .map(|s| serde_json::from_str(&s).map_err(Error::from))
        .collect()
}

async fn load<T: DeserializeOwned>(
    db: &mut SqliteConnection,
    table: &'static str,
    id: &str,
) -> Result<T> {
    let statement = match table {
        "tasks" => "SELECT data FROM tasks WHERE id=?",
        "runs" => "SELECT data FROM runs WHERE id=?",
        "operations" => "SELECT data FROM operations WHERE id=?",
        _ => return Err(Error::Rejected("invalid internal table".into())),
    };
    let text: String = sqlx::query_scalar(statement)
        .bind(id)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| Error::Missing(format!("{table}:{id}")))?;
    Ok(serde_json::from_str(&text)?)
}

async fn put<T: Serialize>(
    db: &mut SqliteConnection,
    table: &'static str,
    id: &str,
    value: &T,
) -> Result<()> {
    let statement = match table {
        "tasks" => {
            "INSERT INTO tasks(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data"
        }
        "runs" => {
            "INSERT INTO runs(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data"
        }
        "operations" => {
            "INSERT INTO operations(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data"
        }
        _ => return Err(Error::Rejected("invalid internal table".into())),
    };
    sqlx::query(statement)
        .bind(id)
        .bind(serde_json::to_string(value)?)
        .execute(db)
        .await?;
    Ok(())
}

async fn epoch(db: &mut SqliteConnection) -> Result<u64> {
    sqlx::query("INSERT INTO settings(key,value) VALUES('epoch','1') ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+1").execute(&mut *db).await?;
    let text: String = sqlx::query_scalar("SELECT value FROM settings WHERE key='epoch'")
        .fetch_one(db)
        .await?;
    text.parse()
        .map_err(|_| Error::Rejected("epoch overflow".into()))
}

async fn notify(db: &mut SqliteConnection, key: &str, payload: &Value) -> Result<()> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox WHERE acknowledged=0")
        .fetch_one(&mut *db)
        .await?;
    if count >= OUTBOX_LIMIT {
        return Err(Error::Capacity);
    }
    sqlx::query("INSERT OR IGNORE INTO outbox(dedupe_key,payload) VALUES(?,?)")
        .bind(key)
        .bind(serde_json::to_string(payload)?)
        .execute(db)
        .await?;
    Ok(())
}

async fn block(db: &mut SqliteConnection, task: &mut Task, reason: &str) -> Result<()> {
    task.state = State::Blocked;
    task.reason = Some(reason.into());
    put(db, "tasks", &task.id, task).await?;
    notify(
        db,
        &format!("blocked:{}:{}:{reason}", task.id, task.revision),
        &json!({"task_id":task.id,"state":"blocked","reason":reason}),
    )
    .await
}

fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(unix)]
fn private_database(path: &Path) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    require(
        std::fs::metadata(parent)?.permissions().mode() & 0o022 == 0,
        "database directory must not be writable by other users",
    )?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    require(
        metadata.is_file() && metadata.permissions().mode() & 0o077 == 0,
        "database must be a private regular file (0600)",
    )
}

#[cfg(not(unix))]
fn private_database(_path: &Path) -> Result<()> {
    Err(Error::Rejected(
        "v0.2 supports Linux and macOS file permissions only".into(),
    ))
}
fn require(value: bool, reason: &str) -> Result<()> {
    if value {
        Ok(())
    } else {
        Err(Error::Rejected(reason.into()))
    }
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_jcs::to_vec(value)?)))
}
