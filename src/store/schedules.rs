use super::*;
const HORIZON: i64 = 366 * 24 * 60 * 60 * 1000;

pub(super) fn valid(request: &ScheduleRequest, now: i64) -> bool {
    !request.id.trim().is_empty()
        && request.id.trim() == request.id
        && !request.id.chars().any(char::is_control)
        && request.id.len() <= 128
        && !request.label.trim().is_empty()
        && request.label.trim() == request.label
        && request.label.len() <= 64
        && !request.label.chars().any(char::is_control)
        && (0..=i64::MAX - HORIZON - DEADLINE_MS).contains(&request.source_at)
        && request.source_at <= now.saturating_add(120_000)
        && now.saturating_sub(request.source_at) <= HORIZON
        && request.due_at > request.source_at
        && request.due_at - request.source_at <= HORIZON
        && (request.timezone.parse::<chrono_tz::Tz>().is_ok()
            || request.timezone.parse::<chrono::FixedOffset>().is_ok())
        && match &request.action {
            ScheduledAction::Remind { message } => {
                !message.trim().is_empty()
                    && message.len() <= 1024
                    && !message
                        .chars()
                        .any(|c| c.is_control() && c != '\n' && c != '\t')
            }
            ScheduledAction::Task { goal, scope, .. } => {
                !goal.trim().is_empty()
                    && goal.len() <= 4096
                    && crate::read_scope(scope)
                    && crate::ComputerSelection::from_scope(scope).is_none()
            }
        }
}
pub(super) async fn list(db: &mut SqliteConnection, limit: u32) -> Result<Vec<Schedule>> {
    require((1..=100).contains(&limit), "schedule page must be 1..100")?;
    let rows:Vec<String>=sqlx::query_scalar("SELECT data FROM schedules ORDER BY (json_extract(data,'$.state')='armed') DESC,json_extract(data,'$.request.source_at') DESC,id LIMIT ?")
        .bind(limit).fetch_all(db).await?;
    rows.into_iter()
        .map(|s| serde_json::from_str(&s).map_err(Error::from))
        .collect()
}
pub(super) async fn create(
    db: &mut SqliteConnection,
    event: &str,
    request: ScheduleRequest,
    now: i64,
) -> Result<String> {
    require(
        valid(&request, now),
        "invalid schedule time, scope or content",
    )?;
    let exists:i64=sqlx::query_scalar("SELECT count(*) FROM schedules WHERE id=? OR (json_extract(data,'$.state')='armed' AND json_extract(data,'$.request.label')=?)")
        .bind(&request.id).bind(&request.label).fetch_one(&mut *db).await?;
    require(exists == 0, "schedule ID or active name already exists")?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM schedules")
        .fetch_one(&mut *db)
        .await?;
    let armed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM schedules WHERE json_extract(data,'$.state')='armed'",
    )
    .fetch_one(&mut *db)
    .await?;
    require(count < 1000 && armed < 100, "schedule capacity")?;
    let missed = matches!(request.action, ScheduledAction::Task { .. }) && request.due_at < now;
    let schedule = Schedule {
        request,
        authorization_event: event.into(),
        revision: 1,
        state: if missed {
            ScheduleState::Missed
        } else {
            ScheduleState::Armed
        },
        task_id: None,
        fired_at: None,
        reason: missed.then(|| "created_after_due".into()),
    };
    put(db, "schedules", &schedule.request.id, &schedule).await?;
    Ok(schedule.request.id)
}
pub(super) async fn cancel(db: &mut SqliteConnection, id: &str, revision: u64) -> Result<()> {
    let mut schedule: Schedule = load(db, "schedules", id).await?;
    require(schedule.revision == revision, "schedule revision conflict")?;
    if schedule.state != ScheduleState::Cancelled {
        schedule.state = ScheduleState::Cancelled;
        schedule.revision = schedule
            .revision
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| Error::Rejected("schedule revision exhausted".into()))?;
        put(db, "schedules", id, &schedule).await?;
    }
    Ok(())
}
impl Store {
    pub fn schedule_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.schedule_wake.subscribe()
    }
    pub fn wake_schedules(&self) {
        self.schedule_wake.send_modify(|n| *n = n.wrapping_add(1));
    }
    pub async fn schedules(&self, limit: u32) -> Result<Vec<Schedule>> {
        list(&mut *self.pool.acquire().await?, limit).await
    }
    pub async fn schedule(&self, id: &str) -> Result<Schedule> {
        load(&mut *self.pool.acquire().await?, "schedules", id).await
    }
    /// One durable occurrence. No provider or adapter runs inside this transaction.
    pub async fn fire_schedule(&self, now: i64) -> Result<ScheduleTick> {
        require(
            (0..=i64::MAX - DEADLINE_MS).contains(&now),
            "invalid schedule clock",
        )?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let data:Option<String>=sqlx::query_scalar("SELECT data FROM schedules WHERE json_extract(data,'$.state')='armed' ORDER BY json_extract(data,'$.request.due_at'),id LIMIT 1").fetch_optional(&mut *tx).await?;
        let Some(data) = data else {
            return Ok(ScheduleTick {
                fired: false,
                next_at: None,
                blocked: false,
            });
        };
        let mut schedule: Schedule = serde_json::from_str(&data)?;
        let due = schedule.request.due_at;
        if due > now {
            return Ok(ScheduleTick {
                fired: false,
                next_at: Some(due),
                blocked: false,
            });
        }
        let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE outcome IS NULL")
            .fetch_one(&mut *tx)
            .await?;
        if pending > 0 {
            return Ok(ScheduleTick {
                fired: false,
                next_at: Some(due),
                blocked: true,
            });
        }
        let room: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE acknowledged=0")
            .fetch_one(&mut *tx)
            .await?;
        if room >= OUTBOX_LIMIT {
            return Ok(ScheduleTick {
                fired: false,
                next_at: Some(due),
                blocked: true,
            });
        }
        let row = sqlx::query("SELECT origin,payload,received_at FROM events WHERE id=?")
            .bind(&schedule.authorization_event)
            .fetch_one(&mut *tx)
            .await?;
        let payload: Value = serde_json::from_str(&row.get::<String, _>("payload"))?;
        let original: Vec<Command> =
            serde_json::from_value(payload.get("commands").cloned().unwrap_or(json!([])))?;
        let authorized = row.get::<String, _>("origin") == "human"
            && valid(&schedule.request, row.get("received_at"))
            && original
                .iter()
                .any(|c| matches!(c,Command::Schedule {request} if request==&schedule.request));
        let message;
        let mut reason = None;
        if !authorized {
            reason = Some("authorization_changed");
            message = "排程授權不符，未執行。請重新提出指令。".into();
        } else {
            match &schedule.request.action {
                ScheduledAction::Remind { message: text } => message = text.clone(),
                ScheduledAction::Task {
                    goal,
                    scope,
                    requires_device,
                } => {
                    if now >= due + DEADLINE_MS {
                        reason = Some("deadline_exhausted");
                        message = "排程已逾期，未啟動工作。請重新提出指令。".into();
                    } else {
                        let task_id = format!("schedule-{}", digest(&schedule.request.id)?);
                        let occupied: i64 =
                            sqlx::query_scalar("SELECT count(*) FROM tasks WHERE id=?")
                                .bind(&task_id)
                                .fetch_one(&mut *tx)
                                .await?;
                        if occupied > 0 {
                            reason = Some("task_identity_conflict");
                            message = "排程工作身分衝突，未啟動。請核對目前任務。".into();
                        } else if tasks(&mut tx)
                            .await?
                            .iter()
                            .filter(|t| !t.state.terminal())
                            .count()
                            >= TASK_LIMIT
                        {
                            return Ok(ScheduleTick {
                                fired: false,
                                next_at: Some(due),
                                blocked: true,
                            });
                        } else {
                            apply(
                                &mut tx,
                                &schedule.authorization_event,
                                "human",
                                vec![Command::Create {
                                    id: task_id.clone(),
                                    goal: goal.clone(),
                                    scope: scope.clone(),
                                    priority: 0,
                                    requires_device: *requires_device,
                                    parent: None,
                                    dependencies: vec![],
                                }],
                                due,
                            )
                            .await?;
                            message = "排程工作已建立，將依原範圍、期限與費用上限處理。".into();
                            schedule.task_id = Some(task_id);
                        }
                    }
                }
            }
        }
        schedule.state = if reason.is_some() {
            ScheduleState::Missed
        } else {
            ScheduleState::Fired
        };
        schedule.reason = reason.map(str::to_owned);
        schedule.fired_at = Some(now);
        let notice = ScheduleNotice {
            id: format!("schedule-notice-{}", digest(&schedule.request.id)?),
            schedule_id: schedule.request.id.clone(),
            label: schedule.request.label.clone(),
            due_at: due,
            timezone: schedule.request.timezone.clone(),
            message,
            task_id: schedule.task_id.clone(),
            missed: reason.is_some(),
        };
        notify(
            &mut tx,
            &notice.id,
            &json!({"kind":"schedule_due","notice":notice}),
        )
        .await?;
        put(&mut tx, "schedules", &schedule.request.id, &schedule).await?;
        tx.commit().await?;
        Ok(ScheduleTick {
            fired: true,
            next_at: None,
            blocked: false,
        })
    }
}
