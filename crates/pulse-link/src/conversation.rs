//! Deterministic Human routing. Unclear control is a question, never a new task.
use pulse_core::{Command, Task};

pub(crate) fn status(task: &Task) -> String {
    use pulse_core::State::*;
    let state = match task.state {
        Queued => "等待處理",
        Planning | Ready | Running => "分析中",
        Dispatched => "本機讀取中",
        WaitingDevice => "等待本機連線",
        WaitingApproval => "等待確認",
        RetryWait => "等待重試",
        OutcomeUnknown | Reconciling => "結果待核對，暫不繼續",
        CancelRequested => "取消中",
        Cancelled => "已取消",
        Succeeded => "已完成驗收",
        Failed => "未完成",
        Blocked if task.reason.as_deref() == Some("model_reply_ready") => "分析已保存，待確認",
        Blocked if task.reason.as_deref() == Some("budget_exhausted") => "已達費用上限",
        Blocked if task.reason.as_deref() == Some("model_calls_exhausted") => "已達模型呼叫上限",
        Blocked if task.reason.as_deref() == Some("deadline_exhausted") => "已達任務時間上限",
        Blocked if task.reason.as_deref() == Some("provider_request_invalid") => {
            "請求超過限制或設定無效，尚未呼叫模型"
        }
        Blocked
            if matches!(
                task.reason.as_deref(),
                Some("provider_call_failed_cost_reserved" | "previous_call_not_retried")
            ) =>
        {
            "模型結果或費用待核對，暫不重送"
        }
        Blocked if task.reason.as_deref() == Some("read_receipts_verified") => "證據已核對，可繼續",
        Blocked if task.reason.as_deref() == Some("device_result_ready_model_not_connected") => {
            "結果已保存，請先核對"
        }
        Blocked => "已停止，需確認原因",
    };
    format!("{} · {state} · 修訂 {}", label(task), task.revision)
}

fn split(text: &str) -> Option<(&str, &str)> {
    text.split_once(['：', ':'])
        .map(|(a, b)| (a.trim(), b.trim()))
}
fn name(text: &str) -> Option<(&str, &str)> {
    let rest = text
        .strip_prefix("新任務")
        .or_else(|| text.strip_prefix("任務"))?
        .trim();
    let (label, goal) = split(rest)?;
    (!label.is_empty()
        && label.len() <= 32
        && label
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        && !goal.is_empty())
    .then_some((label, goal))
}
pub(crate) fn label(task: &Task) -> &str {
    task.decisions
        .first()
        .and_then(|d| name(&d.text))
        .map(|(label, _)| label)
        .unwrap_or(&task.id)
}
fn clarify() -> Vec<Command> {
    vec![Command::Clarify{question:"請指明任務與動作。例如「查詢 A、B」、「修改 A：新的需求」或「取消 B」。新工作可寫「任務 A：需求」。".into()}]
}
fn selected<'a>(text: &str, tasks: &'a [Task]) -> Option<Vec<&'a Task>> {
    let text = text.trim().trim_end_matches(['。', '？', '?']).trim();
    let text = text.strip_prefix("任務 ").unwrap_or(text);
    let joined = if tasks.iter().any(|t| label(t) == text || t.id == text) {
        text.to_owned()
    } else {
        text.replace(['和', '與', '及'], "、")
            .replace(" and ", "、")
    };
    let parts: Vec<&str> = joined.split(['、', ',', '，']).map(str::trim).collect();
    if parts.is_empty() || parts.len() > 8 {
        return None;
    }
    let mut result = Vec::new();
    for part in parts {
        let found: Vec<&Task> = tasks
            .iter()
            .filter(|task| label(task) == part || task.id == part)
            .collect();
        if found.len() != 1 || result.iter().any(|task: &&Task| task.id == found[0].id) {
            return None;
        }
        result.push(found[0]);
    }
    Some(result)
}
fn create(id: String, goal: String) -> Command {
    Command::Create {
        id,
        goal,
        scope: "pulse:read".into(),
        priority: 0,
        requires_device: false,
        parent: None,
        dependencies: vec![],
    }
}
fn line(id: &str, text: &str, tasks: &[Task]) -> Vec<Command> {
    if let Some((label, goal)) = name(text) {
        if tasks.iter().any(|task| self::label(task) == label) {
            return clarify();
        }
        return vec![create(id.into(), format!("任務 {label}：{goal}"))];
    }
    let text = text.trim_end_matches(['。', '？', '?']).trim();
    let text = text.strip_prefix("請").unwrap_or(text).trim();
    let text = text.strip_prefix("先").unwrap_or(text).trim();
    if matches!(
        text,
        "查詢" | "狀態" | "進度" | "目前進度" | "目前狀態" | "查詢狀態" | "status"
    ) {
        return vec![Command::Query {
            ids: tasks.iter().map(|t| t.id.clone()).collect(),
        }];
    }
    if let Some(target) = text
        .strip_suffix("進度")
        .or_else(|| text.strip_suffix("狀態"))
    {
        let target = target.trim().trim_end_matches('的').trim();
        return selected(target, tasks)
            .map(|targets| {
                vec![Command::Query {
                    ids: targets.iter().map(|t| t.id.clone()).collect(),
                }]
            })
            .unwrap_or_else(clarify);
    }
    let action = ["取消", "停止", "繼續", "修改", "補充", "查詢", "核對"]
        .into_iter()
        .find_map(|verb| text.strip_prefix(verb).map(|rest| (verb, rest.trim())));
    if let Some((verb, rest)) = action {
        if verb == "修改" || verb == "補充" {
            let Some((target, goal)) = split(rest) else {
                return clarify();
            };
            let Some(targets) = selected(target, tasks) else {
                return clarify();
            };
            if targets.len() != 1 || goal.is_empty() {
                return clarify();
            }
            let task = targets[0];
            let goal = if verb == "補充" {
                format!("{}\n補充要求：{goal}", task.goal)
            } else if task
                .decisions
                .first()
                .is_some_and(|d| name(&d.text).is_some())
            {
                format!("任務 {}：{goal}", label(task))
            } else {
                goal.into()
            };
            if goal.len() > 4096 {
                return clarify();
            }
            return vec![Command::Revise {
                id: task.id.clone(),
                expected_revision: task.revision,
                goal,
            }];
        }
        let Some(targets) = selected(rest, tasks) else {
            return clarify();
        };
        if verb == "查詢" {
            return vec![Command::Query {
                ids: targets.iter().map(|t| t.id.clone()).collect(),
            }];
        }
        return targets
            .into_iter()
            .map(|task| {
                if verb == "核對" {
                    Command::ReconcileRead {
                        id: task.id.clone(),
                        expected_revision: task.revision,
                    }
                } else if verb == "繼續" {
                    Command::Resume {
                        id: task.id.clone(),
                        expected_revision: task.revision,
                    }
                } else {
                    Command::Cancel {
                        id: task.id.clone(),
                        expected_revision: task.revision,
                    }
                }
            })
            .collect();
    }
    // Detect control without a supported, unambiguous target. Do not guess the latest task.
    if [
        "取消", "停止", "繼續", "修改", "改成", "查詢", "核對", "刪除", "刪掉",
    ]
    .iter()
    .any(|word| text.contains(word))
        || tasks.iter().any(|task| {
            task.decisions
                .first()
                .is_some_and(|d| name(&d.text).is_some())
                && split(text).is_some_and(|(head, _)| head == label(task))
        })
    {
        return clarify();
    }
    vec![create(id.into(), text.into())]
}

pub(crate) fn plan(id: &str, text: &str, tasks: &[Task]) -> Vec<Command> {
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    // Multi-action input needs explicit lines. Ordinary multiline prose stays one goal.
    let explicit = |s: &str| {
        name(s).is_some()
            || [
                "取消 ", "停止 ", "繼續 ", "修改 ", "補充 ", "查詢 ", "核對 ",
            ]
            .iter()
            .any(|v| s.starts_with(v))
    };
    let pieces = if lines.len() > 1 && lines.iter().all(|s| explicit(s)) {
        lines
    } else {
        vec![text.trim()]
    };
    if pieces.len() > 8 {
        return clarify();
    }
    let mut commands = Vec::new();
    let mut aliases = std::collections::BTreeSet::new();
    let mut mutations = std::collections::BTreeSet::new();
    for (index, piece) in pieces.into_iter().enumerate() {
        let child = if index == 0 {
            id.to_owned()
        } else {
            format!(
                "chat-{}",
                crate::protocol::hash_bytes(format!("{id}:{index}").as_bytes())
            )
        };
        if let Some((label, _)) = name(piece)
            && !aliases.insert(label)
        {
            return clarify();
        }
        let proposed = line(&child, piece, tasks);
        if proposed
            .iter()
            .any(|c| matches!(c, Command::Clarify { .. }))
        {
            return clarify();
        }
        for command in &proposed {
            let target = match command {
                Command::Revise { id, .. }
                | Command::Cancel { id, .. }
                | Command::Resume { id, .. }
                | Command::ReconcileRead { id, .. } => Some(id),
                _ => None,
            };
            if let Some(target) = target
                && !mutations.insert(target.clone())
            {
                return clarify();
            }
        }
        commands.extend(proposed);
    }
    commands
}
