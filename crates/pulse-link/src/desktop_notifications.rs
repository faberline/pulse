//! Rust owns durable notification policy. Frontends only present a committed offer.
use crate::{Result, ensure, journal::Journal, protocol::*};
use sqlx::{Row, SqliteConnection};
const MAX_AGE: i64 = 24 * 60 * 60 * 1000;
const OFFER_TIMEOUT: i64 = 30_000;

pub(crate) async fn enabled_at(db: &mut SqliteConnection) -> Result<Option<i64>> {
    let text: Option<String> =
        sqlx::query_scalar("SELECT value FROM local_meta WHERE key='desktop_notifications_at'")
            .fetch_optional(db)
            .await?;
    text.map(|v| {
        v.parse::<i64>()
            .map_err(|_| crate::Error::Rejected("notification setting invalid".into()))
    })
    .transpose()
}

pub(crate) async fn received(
    db: &mut SqliteConnection,
    notice: &pulse_core::ScheduleNotice,
    now: i64,
) -> Result<()> {
    if enabled_at(db)
        .await?
        .is_some_and(|at| notice.due_at >= at && now.saturating_sub(notice.due_at) <= MAX_AGE)
    {
        sqlx::query("INSERT INTO local_schedule_delivery(notice_id,state) VALUES(?,'pending')")
            .bind(&notice.id)
            .execute(db)
            .await?;
    }
    Ok(())
}

impl Journal {
    pub async fn set_desktop_notifications(&self, enabled: bool, now: i64) -> Result<()> {
        ensure(
            self.role == ClientRole::Chat && now > 0,
            "notifications require Human chat",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        if enabled {
            if enabled_at(&mut tx).await?.is_none() {
                sqlx::query(
                    "INSERT INTO local_meta(key,value) VALUES('desktop_notifications_at',?)",
                )
                .bind(now.to_string())
                .execute(&mut *tx)
                .await?;
            }
        } else {
            sqlx::query("DELETE FROM local_meta WHERE key='desktop_notifications_at'")
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE local_schedule_delivery SET state='suppressed' WHERE state IN ('pending','offered')")
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Commit before exposing an OS effect. An unconfirmed offer is never replayed after a restart.
    pub async fn offer_desktop_notification(&self, now: i64) -> Result<()> {
        ensure(
            self.role == ClientRole::Chat,
            "notifications require Human chat",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        if enabled_at(&mut tx).await?.is_none() {
            return Ok(());
        }
        sqlx::query("UPDATE local_schedule_delivery SET state='unknown' WHERE state='offered' AND offered_at<=?")
            .bind(now.saturating_sub(OFFER_TIMEOUT)).execute(&mut *tx).await?;
        sqlx::query("UPDATE local_schedule_delivery SET state='suppressed' WHERE state='pending' AND notice_id IN (SELECT id FROM local_schedule_notices WHERE json_extract(data,'$.due_at')<=?)")
            .bind(now.saturating_sub(MAX_AGE)).execute(&mut *tx).await?;
        let current: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM local_schedule_delivery WHERE state='offered'",
        )
        .fetch_one(&mut *tx)
        .await?;
        if current == 0 {
            let next: Option<String> = sqlx::query_scalar("SELECT n.id FROM local_schedule_notices n JOIN local_schedule_delivery d ON n.id=d.notice_id WHERE d.state='pending' AND json_extract(n.data,'$.due_at')<=? ORDER BY n.received_at,n.id LIMIT 1")
                .bind(now).fetch_optional(&mut *tx).await?;
            if let Some(id) = next {
                sqlx::query("UPDATE local_schedule_delivery SET state='offered',attempt_id=?,offered_at=? WHERE notice_id=? AND state='pending'")
                    .bind(uuid::Uuid::new_v4().to_string()).bind(now).bind(id).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn desktop_notification_result(
        &self,
        attempt: &str,
        outcome: DesktopNotificationOutcome,
    ) -> Result<()> {
        ensure(
            self.role == ClientRole::Chat,
            "notifications require Human chat",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let row =
            sqlx::query("SELECT state,result FROM local_schedule_delivery WHERE attempt_id=?")
                .bind(attempt)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(row) = row else {
            return Err(crate::Error::Rejected("unknown notification offer".into()));
        };
        let prior: Option<String> = row.get("result");
        if let Some(prior) = prior {
            ensure(prior == outcome.as_str(), "notification result conflict")?;
            return Ok(());
        }
        let state: String = row.get("state");
        // A late answer must not turn a disabled or interrupted offer into a success.
        if state == "suppressed" || state == "unknown" {
            return Ok(());
        }
        ensure(state == "offered", "notification was not offered")?;
        sqlx::query("UPDATE local_schedule_delivery SET state=?,result=? WHERE attempt_id=?")
            .bind(if outcome == DesktopNotificationOutcome::Submitted {
                "submitted"
            } else {
                "failed"
            })
            .bind(outcome.as_str())
            .bind(attempt)
            .execute(&mut *tx)
            .await?;
        if outcome == DesktopNotificationOutcome::Denied {
            sqlx::query("DELETE FROM local_meta WHERE key='desktop_notifications_at'")
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "UPDATE local_schedule_delivery SET state='suppressed' WHERE state='pending'",
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn desktop_notifications(&self) -> Result<DesktopNotifications> {
        let mut tx = self.db.begin().await?;
        let enabled = enabled_at(&mut tx).await?.is_some();
        let uncertain: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM local_schedule_delivery WHERE state='unknown'",
        )
        .fetch_one(&mut *tx)
        .await?;
        let failed: i64 =
            sqlx::query_scalar("SELECT count(*) FROM local_schedule_delivery WHERE state='failed'")
                .fetch_one(&mut *tx)
                .await?;
        let row=sqlx::query("SELECT n.data,d.attempt_id FROM local_schedule_delivery d JOIN local_schedule_notices n ON n.id=d.notice_id WHERE d.state='offered'")
            .fetch_optional(&mut *tx).await?;
        let current = if enabled {
            row.map(|row| -> Result<_> {
                let notice: pulse_core::ScheduleNotice =
                    serde_json::from_str(&row.get::<String, _>("data"))?;
                ensure(notice.valid(), "invalid notification source")?;
                Ok(DesktopNotification {
                    attempt_id: row.get("attempt_id"),
                    identifier: format!(
                        "pulse-reminder-{}",
                        hash(&(&self.server, &self.device, &notice.id))?
                    ),
                    title: notice.label,
                    body: format!(
                        "{}\n{}",
                        pulse_core::schedule_time_label(notice.due_at, &notice.timezone),
                        notice.message
                    ),
                })
            })
            .transpose()?
        } else {
            None
        };
        tx.commit().await?;
        Ok(DesktopNotifications {
            enabled,
            current,
            uncertain: uncertain as u32,
            failed: failed as u32,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn open(root: &std::path::Path) -> (Journal, crate::journal::DeviceLock) {
        Journal::open_role(
            &root.join("chat"),
            "https://pulse.example/",
            std::path::Path::new(""),
            ClientRole::Chat,
        )
        .await
        .unwrap()
    }
    fn envelope(journal: &Journal, sequence: u64, due: i64) -> Envelope<ServerPayload> {
        let schedule_id = format!("schedule-{sequence}");
        let notice = pulse_core::ScheduleNotice {
            id: format!("schedule-notice-{}", hash(&schedule_id).unwrap()),
            schedule_id,
            label: "喝水".into(),
            due_at: due,
            timezone: "Asia/Taipei".into(),
            message: "喝水\n休息一下".into(),
            task_id: None,
            missed: false,
        };
        Envelope::new(
            &journal.device,
            sequence,
            0,
            ServerPayload::ScheduleNotice { notice },
        )
        .unwrap()
    }
    async fn receive(journal: &Journal, sequence: u64, due: i64) {
        assert!(
            journal
                .accept(&envelope(journal, sequence, due), false, 0, false)
                .await
                .unwrap()
        );
    }
    async fn count(journal: &Journal, sql: &'static str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&journal.db)
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn opt_in_is_local_future_only_and_submitted_is_never_a_read_receipt() {
        let root = tempfile::tempdir().unwrap();
        let (journal, _lock) = open(root.path()).await;
        let now = crate::now();
        receive(&journal, 1, now - 1).await;
        journal.offer_desktop_notification(now).await.unwrap();
        assert!(!journal.desktop_notifications().await.unwrap().enabled);
        journal.set_desktop_notifications(true, now).await.unwrap();
        journal.offer_desktop_notification(now).await.unwrap();
        assert!(
            journal
                .desktop_notifications()
                .await
                .unwrap()
                .current
                .is_none()
        );
        receive(&journal, 2, now + 1000).await;
        journal.offer_desktop_notification(now).await.unwrap();
        assert!(
            journal
                .desktop_notifications()
                .await
                .unwrap()
                .current
                .is_none()
        );
        journal
            .offer_desktop_notification(now + 1000)
            .await
            .unwrap();
        let offer = journal
            .desktop_notifications()
            .await
            .unwrap()
            .current
            .unwrap();
        assert!(offer.identifier.starts_with("pulse-reminder-"));
        assert!(offer.body.contains("Asia/Taipei"));
        assert!(offer.body.contains("喝水\n休息一下"));
        journal
            .offer_desktop_notification(now + 1001)
            .await
            .unwrap();
        assert_eq!(
            journal
                .desktop_notifications()
                .await
                .unwrap()
                .current
                .unwrap()
                .attempt_id,
            offer.attempt_id
        );
        journal
            .desktop_notification_result(&offer.attempt_id, DesktopNotificationOutcome::Submitted)
            .await
            .unwrap();
        journal
            .desktop_notification_result(&offer.attempt_id, DesktopNotificationOutcome::Submitted)
            .await
            .unwrap();
        assert!(
            journal
                .desktop_notification_result(&offer.attempt_id, DesktopNotificationOutcome::Denied)
                .await
                .is_err()
        );
        assert!(
            journal
                .desktop_notification_result("invented", DesktopNotificationOutcome::Submitted)
                .await
                .is_err()
        );
        journal
            .offer_desktop_notification(now + 2000)
            .await
            .unwrap();
        assert!(
            journal
                .desktop_notifications()
                .await
                .unwrap()
                .current
                .is_none()
        );
        assert_eq!(
            count(
                &journal,
                "SELECT count(*) FROM local_schedule_delivery WHERE state='submitted'"
            )
            .await,
            1
        );
        assert_eq!(journal.view().await.unwrap().schedule_notices.len(), 2);
    }
    #[tokio::test]
    async fn reopening_an_unanswered_offer_marks_unknown_and_never_reposts_it() {
        let root = tempfile::tempdir().unwrap();
        let (journal, lock) = open(root.path()).await;
        let now = crate::now();
        journal.set_desktop_notifications(true, now).await.unwrap();
        receive(&journal, 1, now).await;
        journal.offer_desktop_notification(now).await.unwrap();
        let offer = journal
            .desktop_notifications()
            .await
            .unwrap()
            .current
            .unwrap();
        journal.db.close().await;
        drop(journal);
        drop(lock);
        let (journal, _lock) = open(root.path()).await;
        journal.offer_desktop_notification(now + 1).await.unwrap();
        let state = journal.desktop_notifications().await.unwrap();
        assert!(state.current.is_none());
        assert_eq!(state.uncertain, 1);
        journal
            .desktop_notification_result(&offer.attempt_id, DesktopNotificationOutcome::Submitted)
            .await
            .unwrap();
        assert_eq!(journal.desktop_notifications().await.unwrap().uncertain, 1);
        receive(&journal, 2, now + 2).await;
        journal.offer_desktop_notification(now + 2).await.unwrap();
        assert_ne!(
            journal
                .desktop_notifications()
                .await
                .unwrap()
                .current
                .unwrap()
                .attempt_id,
            offer.attempt_id
        );
        assert_eq!(journal.view().await.unwrap().schedule_notices.len(), 2);
    }
    #[tokio::test]
    async fn offer_and_notice_are_atomic_and_disable_denial_and_late_replies_cannot_rearm() {
        let root = tempfile::tempdir().unwrap();
        let (journal, _lock) = open(root.path()).await;
        let now = crate::now();
        journal.set_desktop_notifications(true, now).await.unwrap();
        sqlx::query("CREATE TRIGGER stop_delivery BEFORE INSERT ON local_schedule_delivery BEGIN SELECT RAISE(ABORT,'fixture failure'); END").execute(&journal.db).await.unwrap();
        assert!(
            journal
                .accept(&envelope(&journal, 1, now), false, 0, false)
                .await
                .is_err()
        );
        assert_eq!(journal.cursor().await.unwrap(), 0);
        assert_eq!(
            count(&journal, "SELECT count(*) FROM local_schedule_notices").await,
            0
        );
        sqlx::query("DROP TRIGGER stop_delivery")
            .execute(&journal.db)
            .await
            .unwrap();
        receive(&journal, 1, now).await;
        journal.offer_desktop_notification(now).await.unwrap();
        let first = journal
            .desktop_notifications()
            .await
            .unwrap()
            .current
            .unwrap();
        journal.set_desktop_notifications(false, now).await.unwrap();
        journal
            .desktop_notification_result(&first.attempt_id, DesktopNotificationOutcome::Submitted)
            .await
            .unwrap();
        journal
            .set_desktop_notifications(true, now + 1)
            .await
            .unwrap();
        journal.offer_desktop_notification(now + 1).await.unwrap();
        assert!(
            journal
                .desktop_notifications()
                .await
                .unwrap()
                .current
                .is_none()
        );
        receive(&journal, 2, now + 2).await;
        receive(&journal, 3, now + 3).await;
        journal.offer_desktop_notification(now + 3).await.unwrap();
        let second = journal
            .desktop_notifications()
            .await
            .unwrap()
            .current
            .unwrap();
        journal
            .desktop_notification_result(&second.attempt_id, DesktopNotificationOutcome::Denied)
            .await
            .unwrap();
        let state = journal.desktop_notifications().await.unwrap();
        assert!(!state.enabled);
        assert_eq!(state.failed, 1);
        assert!(state.current.is_none());
        assert_eq!(
            count(
                &journal,
                "SELECT count(*) FROM local_schedule_delivery WHERE state='pending'"
            )
            .await,
            0
        );
    }
    #[tokio::test]
    async fn expired_pending_reminders_are_chat_only_and_executor_cannot_request_banners() {
        let root = tempfile::tempdir().unwrap();
        let (journal, _lock) = open(root.path()).await;
        let now = crate::now();
        journal
            .set_desktop_notifications(true, now - 2 * MAX_AGE)
            .await
            .unwrap();
        receive(&journal, 1, now - MAX_AGE - 1).await;
        receive(&journal, 2, now).await;
        journal
            .offer_desktop_notification(now + MAX_AGE + 1)
            .await
            .unwrap();
        assert!(
            journal
                .desktop_notifications()
                .await
                .unwrap()
                .current
                .is_none()
        );
        assert_eq!(journal.view().await.unwrap().schedule_notices.len(), 2);
        let worker_root = tempfile::tempdir().unwrap();
        let (worker, _lock) = Journal::open_role(
            &worker_root.path().join("worker"),
            "https://pulse.example/",
            std::path::Path::new(""),
            ClientRole::LocalExecutor,
        )
        .await
        .unwrap();
        assert!(worker.set_desktop_notifications(true, now).await.is_err());
        assert!(worker.offer_desktop_notification(now).await.is_err());
        assert!(
            worker
                .desktop_notification_result("anything", DesktopNotificationOutcome::Submitted)
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn a_lost_frontend_reply_times_out_without_replay_or_blocking_the_next_notice() {
        let root = tempfile::tempdir().unwrap();
        let (journal, _lock) = open(root.path()).await;
        let now = crate::now();
        journal.set_desktop_notifications(true, now).await.unwrap();
        receive(&journal, 1, now).await;
        journal.offer_desktop_notification(now).await.unwrap();
        let first = journal
            .desktop_notifications()
            .await
            .unwrap()
            .current
            .unwrap();
        receive(&journal, 2, now + 1).await;
        journal
            .offer_desktop_notification(now + OFFER_TIMEOUT)
            .await
            .unwrap();
        let state = journal.desktop_notifications().await.unwrap();
        assert_eq!(state.uncertain, 1);
        assert_ne!(state.current.as_ref().unwrap().attempt_id, first.attempt_id);
        journal
            .desktop_notification_result(&first.attempt_id, DesktopNotificationOutcome::Submitted)
            .await
            .unwrap();
        assert_eq!(journal.desktop_notifications().await.unwrap().uncertain, 1);
        journal
            .desktop_notification_result(
                &state.current.unwrap().attempt_id,
                DesktopNotificationOutcome::Submitted,
            )
            .await
            .unwrap();
        journal
            .offer_desktop_notification(now + OFFER_TIMEOUT + 1)
            .await
            .unwrap();
        assert!(
            journal
                .desktop_notifications()
                .await
                .unwrap()
                .current
                .is_none()
        );
    }
}
