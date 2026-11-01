//! Only the live local process can report locally granted window/task capabilities.
use crate::{Result, ensure, protocol::ReadOp, server::Hub};
use pulse_core::{ComputerPermit, ComputerSelection, State, Task};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Capability {
    epoch: i64,
    checked_at: i64,
    permits: Vec<ComputerPermit>,
}

impl Hub {
    pub async fn computer_ready(
        &self,
        device: &str,
        epoch: u64,
        permits: Vec<ComputerPermit>,
        time: i64,
    ) -> Result<()> {
        ensure(
            permits.len() <= 2
                && permits.iter().all(|permit| permit.valid(time))
                && permits.iter().enumerate().all(|(i, permit)| {
                    permits[..i].iter().all(|old| old.task_id != permit.task_id)
                }),
            "invalid local computer capabilities",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let live: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_device WHERE id=? AND epoch=? AND role='local_executor' AND revoked=0 AND stopped=0 AND lease_until>?")
            .bind(device).bind(epoch as i64).bind(time).fetch_one(&mut *tx).await?;
        ensure(live == 1, "computer capability requires live local role")?;
        sqlx::query("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(format!("computer:{device}")).bind(serde_json::to_string(&Capability { epoch:epoch as i64, checked_at:time, permits })?).execute(&mut *tx).await?;
        tx.commit().await?;
        self.notify_change();
        Ok(())
    }
    pub(crate) async fn computer_capable(
        db: &mut SqliteConnection,
        device: &str,
        epoch: i64,
        task: &str,
        revision: &str,
        selection: &ComputerSelection,
        time: i64,
    ) -> Result<bool> {
        let data: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key=?")
            .bind(format!("computer:{device}"))
            .fetch_optional(&mut *db)
            .await?;
        let Some(data) = data else {
            return Ok(false);
        };
        let capability: Capability = serde_json::from_str(&data)?;
        Ok(capability.epoch == epoch
            && capability.checked_at <= time
            && capability.checked_at.saturating_add(20_000) > time
            && capability.permits.iter().any(|permit| {
                permit.valid(time)
                    && permit.task_id == task
                    && permit.revision == revision
                    && permit.selection == *selection
            }))
    }
    pub(crate) async fn local_capable(
        db: &mut SqliteConnection,
        device: &str,
        epoch: i64,
        task: &str,
        revision: &str,
        operation: &ReadOp,
        time: i64,
    ) -> Result<bool> {
        if let ReadOp::ComputerInspect { selection } = operation {
            Self::computer_capable(db, device, epoch, task, revision, selection, time).await
        } else {
            Self::browser_capable(db, device, epoch, operation).await
        }
    }
    pub(crate) async fn gate_computer_tasks(&self, time: i64) -> Result<()> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let workers = sqlx::query("SELECT id,epoch FROM link_device WHERE role='local_executor' AND revoked=0 AND stopped=0 AND lease_until>? AND NOT EXISTS(SELECT 1 FROM link_reads WHERE device=link_device.id AND state IN ('sent','accepted','unknown'))").bind(time).fetch_all(&mut *tx).await?;
        let rows: Vec<String> = sqlx::query_scalar("SELECT data FROM tasks WHERE json_extract(data,'$.state') IN ('queued','waiting_device')").fetch_all(&mut *tx).await?;
        let mut changed = false;
        for row in rows {
            let mut task: Task = serde_json::from_str(&row)?;
            let Some(selection) = ComputerSelection::from_scope(&task.scope) else {
                continue;
            };
            if task.cancel_requested
                || task.deadline <= time
                || (task.state == State::WaitingDevice
                    && !matches!(
                        task.reason.as_deref(),
                        Some("computer_selection_wait" | "computer_capability_wait")
                    ))
            {
                continue;
            }
            // A known, validated source can be summarized without capturing it again.
            let completed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_reads r JOIN link_artifacts a ON a.operation_id=r.id WHERE r.task_id=? AND r.task_revision=? AND r.state='completed' AND json_extract(r.request,'$.kind')='computer_inspect' AND a.png IS NOT NULL")
                .bind(&task.id).bind(task.revision.to_string()).fetch_one(&mut *tx).await?;
            let mut available = completed > 0;
            for worker in &workers {
                if Self::computer_capable(
                    &mut tx,
                    &worker.get::<String, _>("id"),
                    worker.get("epoch"),
                    &task.id,
                    &task.revision.to_string(),
                    &selection,
                    time,
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
            let reason = (!available).then(|| "computer_capability_wait".to_owned());
            if task.state != state || (state == State::WaitingDevice && task.reason != reason) {
                task.state = state;
                task.reason = reason;
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
            self.notify_change();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::*,
        provider::{Reply, ToolCall},
        remote,
    };
    use serde_json::json;
    fn permit(task: &str, time: i64) -> ComputerPermit {
        ComputerPermit {
            task_id: task.into(),
            revision: "1".into(),
            selection: ComputerSelection {
                target: pulse_core::ComputerTarget {
                    bundle_id: "com.faberline.PulseComputerFixture".into(),
                    pid: 42,
                    window_id: 7,
                },
                process_launched_at: 1,
            },
            selected_at: time,
            expires_at: time + 900_000,
        }
    }
    async fn pair(hub: &Hub, role: ClientRole, time: i64) -> (String, u64) {
        let device = uuid::Uuid::new_v4().to_string();
        let credential = hub
            .redeem(
                PairRequest {
                    code: hub.pairing(role, time).await.unwrap(),
                    device,
                    role,
                },
                time,
            )
            .await
            .unwrap();
        let epoch = hub.begin(&credential.device, 0, time).await.unwrap();
        (credential.device, epoch)
    }
    async fn create(hub: &Hub, id: &str, time: i64) {
        let event=hub.core.ingest(pulse_core::Origin::Human,"fixture",id,json!({"commands":[{"kind":"create","id":id,"goal":"Read the selected owned fixture","scope":permit(id,time).selection.scope().unwrap(),"priority":0,"requires_device":false}]}),time).await.unwrap();
        hub.core.route(&event.event_id).await.unwrap();
    }
    #[tokio::test]
    async fn missing_stale_or_other_task_capability_waits_without_calls_or_reservations() {
        let root = tempfile::tempdir().unwrap();
        let hub = Hub::new(
            pulse_core::Store::open(root.path().join("db"), "owner")
                .await
                .unwrap(),
        );
        let time = crate::now();
        let (local, epoch) = pair(&hub, ClientRole::LocalExecutor, time).await;
        let (chat, chat_epoch) = pair(&hub, ClientRole::Chat, time).await;
        create(&hub, "a", time).await;
        create(&hub, "b", time).await;
        hub.gate_computer_tasks(time).await.unwrap();
        assert_eq!(
            hub.core.task("a").await.unwrap().state,
            State::WaitingDevice
        );
        assert!(
            hub.computer_ready(&chat, chat_epoch, vec![permit("a", time)], time)
                .await
                .is_err()
        );
        assert!(
            hub.computer_ready(&local, epoch + 1, vec![permit("a", time)], time)
                .await
                .is_err()
        );
        hub.computer_ready(&local, epoch, vec![permit("a", time)], time)
            .await
            .unwrap();
        hub.gate_computer_tasks(time).await.unwrap();
        assert_eq!(hub.core.task("a").await.unwrap().state, State::Queued);
        assert_eq!(
            hub.core.task("b").await.unwrap().state,
            State::WaitingDevice
        );
        hub.gate_computer_tasks(time + 20_000).await.unwrap();
        assert_eq!(
            hub.core.task("a").await.unwrap().state,
            State::WaitingDevice
        );
        hub.computer_ready(&local, epoch, vec![permit("a", time)], time + 20_000)
            .await
            .unwrap();
        hub.gate_computer_tasks(time + 20_000).await.unwrap();
        assert_eq!(hub.core.task("a").await.unwrap().state, State::Queued);
        let new_epoch = hub.begin(&local, 0, time + 20_000).await.unwrap();
        assert_ne!(epoch, new_epoch);
        hub.gate_computer_tasks(time + 20_000).await.unwrap();
        assert_eq!(
            hub.core.task("a").await.unwrap().state,
            State::WaitingDevice
        );
        for id in ["a", "b"] {
            assert_eq!(hub.core.task(id).await.unwrap().model_calls, 0);
        }
        let reserved: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM budget")
            .fetch_one(&hub.db)
            .await
            .unwrap();
        assert_eq!(reserved, 0);
    }
    #[tokio::test]
    async fn model_window_read_dispatch_requires_the_exact_local_task_revision_and_selection() {
        let root = tempfile::tempdir().unwrap();
        let hub = Hub::new(
            pulse_core::Store::open(root.path().join("db"), "owner")
                .await
                .unwrap(),
        );
        let time = crate::now();
        let (local, epoch) = pair(&hub, ClientRole::LocalExecutor, time).await;
        create(&hub, "a", time).await;
        let run = hub
            .core
            .claim_next(&remote::versions(), time)
            .await
            .unwrap()
            .unwrap();
        let reply = |selection| Reply {
            text: String::new(),
            tool: Some(ToolCall {
                call_id: "inspect".into(),
                operation: ReadOp::ComputerInspect { selection },
            }),
            usage: json!({}),
            cost: 0,
        };
        let mut wrong = permit("a", time).selection;
        wrong.target.window_id += 1;
        assert!(
            hub.model_proposal(&run.token, &reply(wrong), time)
                .await
                .is_err()
        );
        hub.model_proposal(&run.token, &reply(permit("a", time).selection), time)
            .await
            .unwrap();
        assert!(
            !hub.dispatch_model_read(&remote::versions(), time)
                .await
                .unwrap()
        );
        hub.computer_ready(&local, epoch, vec![permit("b", time)], time)
            .await
            .unwrap();
        assert!(
            !hub.dispatch_model_read(&remote::versions(), time)
                .await
                .unwrap()
        );
        let mut revised = permit("a", time);
        revised.revision = "2".into();
        hub.computer_ready(&local, epoch, vec![revised], time)
            .await
            .unwrap();
        assert!(
            !hub.dispatch_model_read(&remote::versions(), time)
                .await
                .unwrap()
        );
        hub.computer_ready(&local, epoch, vec![permit("a", time)], time)
            .await
            .unwrap();
        assert!(
            hub.dispatch_model_read(&remote::versions(), time)
                .await
                .unwrap()
        );
        assert!(
            !hub.dispatch_model_read(&remote::versions(), time)
                .await
                .unwrap()
        );
        assert_eq!(hub.core.task("a").await.unwrap().tool_calls, 1);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_reads")
            .fetch_one(&hub.db)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
}
