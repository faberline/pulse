use crate::{Result, ensure};
use pulse_core::{Command, Schedule, ScheduleRequest, ScheduleState, ScheduledAction};

fn text(value: &str) -> &str {
    value
        .trim()
        .strip_prefix("請")
        .unwrap_or(value.trim())
        .trim()
        .trim_end_matches('。')
}
pub(crate) fn reserved(value: &str) -> bool {
    let value = text(value);
    [
        "排程",
        "提醒",
        "查詢排程",
        "我的排程",
        "取消排程",
        "停止排程",
    ]
    .iter()
    .any(|p| value.starts_with(p))
        || value.contains("提醒我")
        || (value.contains('後') && value.contains("執行"))
}
fn parts(value: &str) -> Option<(String, &str, &str, bool)> {
    let value = text(value);
    if let Some(rest) = value.strip_prefix("排程 ") {
        let (label, rest) = rest.split_once(['：', ':'])?;
        let (when, action) = rest.split_once('：')?;
        if let Some(body) = action.trim().strip_prefix("提醒 ") {
            return Some((label.trim().into(), when.trim(), body.trim(), false));
        }
        if let Some(body) = action.trim().strip_prefix("執行 ") {
            return Some((label.trim().into(), when.trim(), body.trim(), true));
        }
        return None;
    }
    if let Some((when, body)) = value.split_once("提醒我") {
        if when.trim().is_empty() {
            return None;
        }
        let body = body.trim().trim_start_matches(['：', ':']).trim();
        let mut end = body.len().min(64);
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        return Some((body[..end].into(), when.trim(), body, false));
    }
    if let Some(rest) = value.strip_prefix("提醒 ") {
        let (body, when) = rest.rsplit_once('：')?;
        let body = body.trim();
        let mut end = body.len().min(64);
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        return Some((body[..end].into(), when.trim(), body, false));
    }
    None
}
pub(crate) fn prepare(id: &str, value: &str, now: i64) -> Option<Result<ScheduleRequest>> {
    let (label, when, body, task) = parts(value)?;
    Some((|| {
        ensure(
            !label.is_empty() && !body.is_empty(),
            "schedule label or work missing",
        )?;
        let (due_at, timezone) = pulse_core::resolve_schedule_time(when, now)?;
        let action = if task {
            ensure(
                crate::memory_chat::parse(body).is_none(),
                "scheduled memory mutation unsupported",
            )?;
            ensure(!reserved(body), "nested schedule unsupported")?;
            let proposed = crate::conversation::plan(id, body, &[]);
            ensure(proposed.len() == 1, "schedule needs one finite new task")?;
            let Some(Command::Create {
                goal,
                scope,
                requires_device,
                ..
            }) = proposed.into_iter().next()
            else {
                return Err(crate::Error::Rejected(
                    "schedule needs one finite new task".into(),
                ));
            };
            ScheduledAction::Task {
                goal,
                scope,
                requires_device,
            }
        } else {
            ScheduledAction::Remind {
                message: body.into(),
            }
        };
        Ok(ScheduleRequest {
            id: id.into(),
            label,
            source_at: now,
            due_at,
            timezone,
            action,
        })
    })())
}
fn clarify() -> Vec<Command> {
    vec![Command::Clarify {question:"請指明時間與動作。例如「10分鐘後提醒我喝水」、「排程 A：明天09:00：執行 分析 Pulse」或「取消排程 A」。時間預設 Asia/Taipei；不明確時不建立排程。".into()}]
}
pub(crate) async fn plan(
    core: &pulse_core::Store,
    id: &str,
    value: &str,
) -> Result<Option<Vec<Command>>> {
    let value = text(value);
    if !reserved(value) {
        return Ok(None);
    }
    if matches!(value, "查詢排程" | "我的排程") {
        return Ok(Some(vec![Command::QuerySchedules { ids: vec![] }]));
    }
    let control = [("取消排程", true), ("停止排程", true), ("查詢排程", false)]
        .iter()
        .find_map(|(p, c)| value.strip_prefix(p).map(|s| (s.trim(), *c)));
    if let Some((target, cancel)) = control {
        let schedules = core.schedules(100).await?;
        let mut found: Vec<_> = schedules
            .iter()
            .filter(|s| s.request.id == target || s.request.label == target)
            .collect();
        if found.len() > 1 {
            let armed: Vec<_> = found
                .iter()
                .copied()
                .filter(|s| s.state == ScheduleState::Armed)
                .collect();
            if armed.len() == 1 {
                found = armed;
            }
        }
        if found.is_empty()
            && let Ok(schedule) = core.schedule(target).await
        {
            return Ok(Some(vec![if cancel {
                Command::CancelSchedule {
                    id: schedule.request.id,
                    expected_revision: schedule.revision,
                }
            } else {
                Command::QuerySchedules {
                    ids: vec![schedule.request.id],
                }
            }]));
        }
        if found.len() != 1 {
            return Ok(Some(clarify()));
        }
        let schedule = found[0];
        return Ok(Some(vec![if cancel {
            Command::CancelSchedule {
                id: schedule.request.id.clone(),
                expected_revision: schedule.revision,
            }
        } else {
            Command::QuerySchedules {
                ids: vec![schedule.request.id.clone()],
            }
        }]));
    }
    // New Rust clients freeze time locally before queuing. Legacy creates do not
    // resolve relative time at reconnect or fall through into an immediate task.
    let _ = id;
    Ok(Some(clarify()))
}
pub(crate) fn status(schedule: &Schedule) -> String {
    let state = match schedule.state {
        ScheduleState::Armed => "等待到期",
        ScheduleState::Fired => "已到期處理",
        ScheduleState::Cancelled => "排程已停用",
        ScheduleState::Missed => "逾期或授權不符，未執行",
    };
    let mut body = format!(
        "{} · {} · {state}\n識別：{}",
        schedule.request.label,
        pulse_core::schedule_time_label(schedule.request.due_at, &schedule.request.timezone),
        schedule.request.id
    );
    if let Some(task) = &schedule.task_id {
        body.push_str(&format!("\n已建立任務：{task}。停用排程不會取消既有任務。"));
    }
    body
}
