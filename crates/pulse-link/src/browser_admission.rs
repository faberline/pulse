//! Capability is announced only by the authenticated local process, per connection epoch.
use crate::{Result, browser_network::NetworkPolicy, ensure, protocol::ReadOp, server::Hub};
use pulse_core::{State, Task};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Capability {
    epoch: i64,
    fixture_origin: Option<String>,
    #[serde(default)]
    interactions: bool,
}

impl Hub {
    pub async fn browser_ready(
        &self,
        device: &str,
        epoch: u64,
        fixture: Option<&str>,
        time: i64,
    ) -> Result<()> {
        self.set_browser_capability(device, epoch, fixture, false, time)
            .await
    }
    pub async fn browser_interactive_ready(
        &self,
        device: &str,
        epoch: u64,
        fixture: Option<&str>,
        time: i64,
    ) -> Result<()> {
        self.set_browser_capability(device, epoch, fixture, true, time)
            .await
    }
    async fn set_browser_capability(
        &self,
        device: &str,
        epoch: u64,
        fixture: Option<&str>,
        interactions: bool,
        time: i64,
    ) -> Result<()> {
        let policy = NetworkPolicy::new(fixture)?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let allowed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_device WHERE id=? AND epoch=? AND role='local_executor' AND revoked=0 AND stopped=0 AND lease_until>?")
            .bind(device).bind(epoch as i64).bind(time).fetch_one(&mut *tx).await?;
        ensure(allowed == 1, "browser capability requires live local role")?;
        let capability = Capability {
            epoch: epoch as i64,
            interactions,
            fixture_origin: policy.fixture_origin.map(Into::into),
        };
        sqlx::query("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(format!("browser:{device}")).bind(serde_json::to_string(&capability)?).execute(&mut *tx).await?;
        tx.commit().await?;
        self.notify_change();
        Ok(())
    }

    pub(crate) async fn browser_capable(
        db: &mut SqliteConnection,
        device: &str,
        epoch: i64,
        operation: &ReadOp,
    ) -> Result<bool> {
        let (url, interactive) = match operation {
            ReadOp::Browser { url } => (url.clone(), false),
            ReadOp::BrowserInspect { url } => (url.clone(), true),
            ReadOp::BrowserAction {
                grant: Some(grant), ..
            } => {
                let intent: pulse_core::BrowserIntent =
                    serde_json::from_value(grant.operation.intent.clone())?;
                (intent.observation.url, true)
            }
            ReadOp::BrowserAction { .. } => return Ok(false),
            _ => return Ok(true),
        };
        let data: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key=?")
            .bind(format!("browser:{device}"))
            .fetch_optional(&mut *db)
            .await?;
        let Some(data) = data else { return Ok(false) };
        let capability: Capability = serde_json::from_str(&data)?;
        let Some(url) = pulse_core::browser_url(&format!("browser:{url}")) else {
            return Ok(false);
        };
        Ok(capability.epoch == epoch
            && (!interactive || capability.interactions)
            && NetworkPolicy::new(capability.fixture_origin.as_deref())?
                .validate_url(&url)
                .is_ok())
    }

    /// Fresh browser work waits without reserving model cost. Completed evidence can still be summarized offline.
    pub(crate) async fn gate_browser_tasks(&self, time: i64) -> Result<()> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let workers = sqlx::query("SELECT id,epoch FROM link_device WHERE role='local_executor' AND revoked=0 AND stopped=0 AND lease_until>? ORDER BY id")
            .bind(time).fetch_all(&mut *tx).await?;
        let rows: Vec<String> = sqlx::query_scalar("SELECT data FROM tasks WHERE json_extract(data,'$.state') IN ('queued','waiting_device')")
            .fetch_all(&mut *tx).await?;
        let mut changed = false;
        for data in rows {
            let mut task: Task = serde_json::from_str(&data)?;
            let Some(url) = pulse_core::browser_url(&task.scope)
                .or_else(|| pulse_core::interaction_url(&task.scope))
            else {
                continue;
            };
            if task.active_run.is_some() || task.cancel_requested || task.deadline <= time {
                continue;
            }
            if task.state == State::WaitingDevice
                && task.reason.as_deref() != Some("browser_capability_wait")
            {
                continue;
            }
            let operation = if pulse_core::interaction_url(&task.scope).is_some() {
                ReadOp::BrowserInspect { url: url.into() }
            } else {
                ReadOp::Browser { url: url.into() }
            };
            let mut available = false;
            for worker in &workers {
                if Self::browser_capable(
                    &mut tx,
                    &worker.get::<String, _>("id"),
                    worker.get("epoch"),
                    &operation,
                )
                .await?
                {
                    available = true;
                    break;
                }
            }
            let state = if available {
                State::Queued
            } else {
                State::WaitingDevice
            };
            if task.state != state {
                task.state = state;
                task.reason = (!available).then(|| "browser_capability_wait".into());
                sqlx::query("UPDATE tasks SET data=? WHERE id=?")
                    .bind(serde_json::to_string(&task)?)
                    .bind(&task.id)
                    .execute(&mut *tx)
                    .await?;
                changed = true;
            }
        }
        tx.commit().await?;
        if changed {
            self.notify_change()
        }
        Ok(())
    }
}
