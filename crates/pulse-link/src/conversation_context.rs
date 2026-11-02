//! Human conversation focus. This is bodyless routing metadata, not model memory.
use crate::{Result, conversation, protocol::HumanAction, server::Hub};
use pulse_core::{Command, Outcome, Task};
use serde::{Deserialize, Serialize};
use sqlx::Row;

const KEY: &str = "chat_conversation_context_v1";
const MAX_AGE_MS: i64 = 30 * 60 * 1000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
    id: String,
    revision: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    version: u8,
    device: String,
    event_id: String,
    recorded_at: i64,
    pending: bool,
    targets: Vec<Target>,
}
pub(crate) struct Context {
    source_event_id: String,
    targets: Vec<Target>,
    // Read from the original Human event only. Never stored a second time.
    pending_text: Option<String>,
}
pub(crate) struct Pending {
    source_event_id: Option<String>,
    targets: Vec<Target>,
}

fn current<'a>(target: &Target, tasks: &'a [Task]) -> Option<&'a Task> {
    tasks.iter().find(|task| {
        task.id == target.id
            && task.revision == target.revision
            && !task.state.terminal()
            && !task.cancel_requested
    })
}

pub(crate) fn plan(
    id: &str,
    text: &str,
    tasks: &[Task],
    context: Option<&Context>,
) -> (Vec<Command>, Option<Pending>) {
    if let Some(refinement) = conversation::refinement(text) {
        if let Some(context) = context
            && context.pending_text.is_none()
            && context.targets.len() == 1
            && let Some(task) = current(&context.targets[0], tasks)
        {
            return (conversation::refine(task, refinement), None);
        }
        let candidates: Vec<_> = tasks
            .iter()
            .filter(|task| !task.state.terminal() && !task.cancel_requested)
            .collect();
        let labels = candidates
            .iter()
            .take(8)
            .map(|task| conversation::label(task))
            .collect::<Vec<_>>()
            .join("、");
        let question = if candidates.is_empty() {
            "目前沒有可接續的任務。請先傳送完整需求。".to_string()
        } else {
            format!("這項補充要套用到哪個任務？請回覆任務名稱或識別，例如：{labels}。")
        };
        let targets = candidates
            .iter()
            .map(|task| Target {
                id: task.id.clone(),
                revision: task.revision,
            })
            .collect();
        return (
            vec![Command::Clarify { question }],
            Some(Pending {
                source_event_id: None,
                targets,
            }),
        );
    }
    if let Some(context) = context
        && let Some(refinement) = &context.pending_text
        && let Some(targets) = conversation::selected(text, tasks)
    {
        if targets.len() == 1
            && context
                .targets
                .iter()
                .any(|target| current(target, tasks).is_some_and(|task| task.id == targets[0].id))
        {
            return (conversation::refine(targets[0], refinement), None);
        }
        return (
            vec![Command::Clarify {
                question: "任務已變更或對象不唯一。請重新輸入「補充 任務名稱：需求」。".into(),
            }],
            None,
        );
    }
    let commands = conversation::plan(id, text, tasks);
    if let Some(context) = context.filter(|context| context.pending_text.is_some())
        && commands
            .iter()
            .any(|command| matches!(command, Command::Create { .. }))
        && !conversation::named_request(text)
    {
        return (
            vec![Command::Clarify {
                question: "請回覆要補充的任務名稱。要另開工作，請用「任務 名稱：需求」。".into(),
            }],
            Some(Pending {
                source_event_id: Some(context.source_event_id.clone()),
                targets: context.targets.clone(),
            }),
        );
    }
    (commands, None)
}

impl Hub {
    pub(crate) async fn clear_conversation_context(&self) -> Result<()> {
        sqlx::query("DELETE FROM settings WHERE key=?")
            .bind(KEY)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    /// Consume before routing. A crash can lose convenience, never reuse stale focus.
    /// The caller holds the shared Human lock. Replays must not consume or save focus.
    pub(crate) async fn take_conversation_context(
        &self,
        device: &str,
        time: i64,
    ) -> Result<Option<Context>> {
        let data: Option<String> =
            sqlx::query_scalar("DELETE FROM settings WHERE key=? RETURNING value")
                .bind(KEY)
                .fetch_optional(&self.db)
                .await?;
        let Some(data) = data.filter(|data| data.len() <= 32768) else {
            return Ok(None);
        };
        let Ok(saved) = serde_json::from_str::<Saved>(&data) else {
            return Ok(None);
        };
        if saved.version != 1
            || saved.device != device
            || saved.recorded_at < 0
            || time < saved.recorded_at
            || time.saturating_sub(saved.recorded_at) > MAX_AGE_MS
            || saved.targets.is_empty()
            || saved.targets.len() > 100
            || saved
                .targets
                .iter()
                .any(|target| target.id.is_empty() || target.id.len() > 128 || target.revision == 0)
        {
            return Ok(None);
        }
        let row = sqlx::query(
            "SELECT payload,outcome FROM events WHERE id=? AND source=? AND origin='human'",
        )
        .bind(&saved.event_id)
        .bind(format!("device:{device}"))
        .fetch_optional(&self.db)
        .await?;
        let Some(row) = row else { return Ok(None) };
        let Ok(payload) =
            serde_json::from_str::<serde_json::Value>(&row.get::<String, _>("payload"))
        else {
            return Ok(None);
        };
        let Some(outcome) = row.get::<Option<String>, _>("outcome") else {
            return Ok(None);
        };
        let Ok(outcome) = serde_json::from_str::<Outcome>(&outcome) else {
            return Ok(None);
        };
        let Ok(HumanAction::Submit { goal, .. }) =
            serde_json::from_value::<HumanAction>(payload["native_action"].clone())
        else {
            return Ok(None);
        };
        let pending_text = if saved.pending {
            if !matches!(outcome, Outcome::Clarification { .. }) {
                return Ok(None);
            }
            let Some(refinement) = conversation::refinement(&goal) else {
                return Ok(None);
            };
            Some(refinement.to_owned())
        } else {
            let Outcome::Applied { task_ids } = outcome else {
                return Ok(None);
            };
            if task_ids.len() != saved.targets.len()
                || saved
                    .targets
                    .iter()
                    .any(|target| !task_ids.contains(&target.id))
            {
                return Ok(None);
            }
            None
        };
        Ok(Some(Context {
            source_event_id: saved.event_id,
            targets: saved.targets,
            pending_text,
        }))
    }

    pub(crate) async fn save_conversation_context(
        &self,
        device: &str,
        event_id: &str,
        outcome: &Outcome,
        pending_targets: Option<Pending>,
        time: i64,
    ) -> Result<()> {
        let mut source_event_id = event_id.to_owned();
        let (pending, targets) = match outcome {
            Outcome::Applied { task_ids } if !task_ids.is_empty() && task_ids.len() <= 100 => {
                let mut targets = Vec::new();
                for id in task_ids {
                    let task = self.core.task(id).await?;
                    targets.push(Target {
                        id: id.clone(),
                        revision: task.revision,
                    });
                }
                (false, targets)
            }
            Outcome::Clarification { .. } => match pending_targets {
                Some(pending) if !pending.targets.is_empty() => {
                    if let Some(original) = pending.source_event_id {
                        source_event_id = original;
                    }
                    (true, pending.targets)
                }
                _ => return Ok(()),
            },
            _ => return Ok(()),
        };
        let saved = Saved {
            version: 1,
            device: device.into(),
            event_id: source_event_id,
            recorded_at: time,
            pending,
            targets,
        };
        sqlx::query("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(KEY)
            .bind(serde_json::to_string(&saved)?)
            .execute(&self.db)
            .await?;
        Ok(())
    }
}
