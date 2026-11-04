//! Negative authority for a quarantined restore candidate. No result is inferred.
use super::*;
use crate::{Outcome, Schedule, State};

const DEVICE_PREFIX: &str = "restore_retired_device:";
const REVIEW: &str = "backup_restore_review_required";

impl Store {
    /// Trusted transport admission. A restored identity cannot reuse its old queue.
    /// This does not authenticate the caller or grant a replacement identity.
    pub async fn device_retired_by_restore_in(
        db: &mut SqliteConnection,
        device: &str,
    ) -> Result<bool> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM settings WHERE key=?")
                .bind(format!("{DEVICE_PREFIX}{device}"))
                .fetch_one(db)
                .await?
                != 0,
        )
    }
}

pub(super) async fn fence(candidate: &Store) -> Result<()> {
    let mut tx = candidate.pool.begin_with("BEGIN IMMEDIATE").await?;
    let exhausted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM link_device WHERE typeof(epoch)!='integer' OR epoch<0 OR epoch=9223372036854775807",
    )
    .fetch_one(&mut *tx)
    .await?;
    require(exhausted == 0, "restore device epoch invalid or exhausted")?;
    // Preserve pairing receipts and retired token hashes as evidence. Fresh
    // pairing must use a different identity, so queued old frames stay inert.
    sqlx::query("INSERT OR IGNORE INTO settings(key,value) SELECT ? || id,'1' FROM link_device")
        .bind(DEVICE_PREFIX)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE link_device SET revoked=1,stopped=1,lease_until=0,epoch=epoch+1,worker_cleanup=0,worker_erasure=0,projection_erasure=0,schedule_notices=0")
        .execute(&mut *tx).await?;
    sqlx::query("UPDATE link_pairings SET used=1")
        .execute(&mut *tx)
        .await?;
    // These rows carry no result. Do not treat them as completion or erase a
    // known receipt. Existing unknown budget amounts remain fully reserved.
    sqlx::query("UPDATE link_reads SET state='unknown' WHERE state IN ('sent','accepted') AND result IS NULL")
        .execute(&mut *tx).await?;
    sqlx::query("UPDATE budget SET state='unknown' WHERE state='reserved'")
        .execute(&mut *tx)
        .await?;
    for mut task in crate::store::tasks(&mut tx).await? {
        crate::store::approvals::invalidate(&mut tx, &task.id, REVIEW).await?;
        if task.state.terminal() {
            continue;
        }
        let pending_read: i64 = sqlx::query_scalar("SELECT count(*) FROM link_reads WHERE task_id=? AND task_revision=? AND result IS NULL AND state='unknown'")
            .bind(&task.id).bind(task.revision.to_string()).fetch_one(&mut *tx).await?;
        let unknown = task.state == State::OutcomeUnknown
            || pending_read != 0
            || crate::store::unresolved(&mut tx, &task.id).await?;
        task.state = if unknown {
            State::OutcomeUnknown
        } else {
            State::Blocked
        };
        task.reason = Some(
            if unknown {
                "backup_restore_unknown_operation"
            } else {
                REVIEW
            }
            .into(),
        );
        crate::store::put(&mut tx, "tasks", &task.id, &task).await?;
    }
    // An accepted but unrouted old event must not create fresh runnable work.
    // Preserve its original source, payload and digest; replay gets this result.
    sqlx::query("UPDATE events SET outcome=? WHERE outcome IS NULL")
        .bind(serde_json::to_string(&Outcome::Rejected {
            reason: REVIEW.into(),
        })?)
        .execute(&mut *tx)
        .await?;
    let armed: Vec<String> =
        sqlx::query_scalar("SELECT data FROM schedules WHERE json_extract(data,'$.state')='armed'")
            .fetch_all(&mut *tx)
            .await?;
    for data in armed {
        let mut schedule: Schedule = serde_json::from_str(&data)?;
        crate::store::schedules::cancel(&mut tx, &schedule.request.id, schedule.revision).await?;
        schedule = crate::store::load(&mut tx, "schedules", &schedule.request.id).await?;
        schedule.reason = Some(REVIEW.into());
        crate::store::put(&mut tx, "schedules", &schedule.request.id, &schedule).await?;
    }
    tx.commit().await?;
    Ok(())
}
