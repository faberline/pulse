//! Shared dispatch metadata; each resource keeps its own typed source and scope rules.
use crate::{
    Result, browser_session::BrowserGrant, computer_runtime::ComputerGrant, ensure, protocol::*,
    server::Hub,
};
use pulse_core::{BrowserIntent, ComputerIntent, Operation, OperationRequest, State, Task};
use sqlx::Row;

pub(crate) enum ActionIntent {
    Browser(Box<BrowserIntent>),
    Computer(Box<ComputerIntent>),
}
impl ActionIntent {
    pub(crate) fn from_request(request: &OperationRequest) -> Result<Self> {
        if request.proposal.target.starts_with("computer:") {
            Ok(Self::Computer(Box::new(serde_json::from_value(
                request.proposal.intent.clone(),
            )?)))
        } else {
            Ok(Self::Browser(Box::new(serde_json::from_value(
                request.proposal.intent.clone(),
            )?)))
        }
    }
    pub(crate) fn device(&self) -> &str {
        match self {
            Self::Browser(i) => &i.local_device,
            Self::Computer(i) => &i.observation.binding.device,
        }
    }
    pub(crate) fn epoch(&self) -> Result<u64> {
        match self {
            Self::Browser(i) => number(&i.local_epoch),
            Self::Computer(i) => Ok(i.observation.binding.epoch),
        }
    }
    pub(crate) fn expires(&self) -> i64 {
        match self {
            Self::Browser(i) => i.observation.expires_at,
            Self::Computer(i) => i.observation.expires_at,
        }
    }
    pub(crate) fn target(&self) -> Result<String> {
        match self {
            Self::Browser(i) => Ok(i.target()),
            Self::Computer(i) => i
                .target()
                .ok_or_else(|| crate::Error::Rejected("invalid native target".into())),
        }
    }
    pub(crate) fn value(&self) -> Result<serde_json::Value> {
        Ok(match self {
            Self::Browser(i) => serde_json::to_value(i)?,
            Self::Computer(i) => serde_json::to_value(i)?,
        })
    }
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Browser(_) => "browser",
            Self::Computer(_) => "computer",
        }
    }
    pub(crate) fn operation(&self, operation: Operation, request: OperationRequest) -> ReadOp {
        match self {
            Self::Browser(i) => ReadOp::BrowserAction {
                session_id: i.observation.session_id.clone(),
                nonce: i.observation.nonce.clone(),
                element_id: i.element_id.clone(),
                effect: i.effect.clone(),
                grant: Some(Box::new(BrowserGrant { operation, request })),
            },
            Self::Computer(i) => ReadOp::ComputerAction {
                selection: i.observation.selection(),
                nonce: i.observation.nonce.clone(),
                element_id: i.element_id.clone(),
                effect: i.effect.clone(),
                grant: Some(Box::new(ComputerGrant { operation, request })),
            },
        }
    }
}
impl Hub {
    pub(crate) async fn action_intent(
        &self,
        task: &Task,
        operation: &ReadOp,
        time: i64,
    ) -> Result<ActionIntent> {
        ensure(
            task.state == State::Planning
                && task.tool_calls < 16
                && operation.is_effect()
                && operation.canonical()? == *operation
                && operation.permitted_by(&task.scope),
            "action proposal scope, grant or state",
        )?;
        match operation {
            ReadOp::ComputerAction {
                selection,
                nonce,
                element_id,
                effect,
                grant: None,
            } => {
                let row=sqlx::query("SELECT result FROM link_reads WHERE task_id=? AND task_revision=? AND state='completed' AND json_extract(request,'$.kind')='computer_inspect' AND json_extract(result,'$.computer.nonce')=? ORDER BY rowid DESC LIMIT 1")
                    .bind(&task.id).bind(task.revision.to_string()).bind(nonce).fetch_optional(&self.db).await?.ok_or_else(||crate::Error::Rejected("computer observation unavailable".into()))?;
                let outcome: ReadOutcome = serde_json::from_str(&row.get::<String, _>("result"))?;
                ensure(outcome.valid(), "invalid native evidence")?;
                let ReadOutcome::Completed {
                    computer: Some(observation),
                    observation: None,
                    ..
                } = outcome
                else {
                    return Err(crate::Error::Rejected(
                        "computer observation missing".into(),
                    ));
                };
                let intent = ComputerIntent {
                    observation: *observation,
                    element_id: element_id.clone(),
                    effect: effect.clone(),
                };
                ensure(
                    intent.valid(&task.scope, time) && intent.observation.selection() == *selection,
                    "computer intent outside selected view",
                )?;
                Ok(ActionIntent::Computer(Box::new(intent)))
            }
            ReadOp::BrowserAction {
                session_id,
                nonce,
                element_id,
                effect,
                grant: None,
            } => {
                let row=sqlx::query("SELECT id,device,epoch,result FROM link_reads WHERE task_id=? AND task_revision=? AND state='completed' AND json_extract(result,'$.observation.session_id')=? AND json_extract(result,'$.observation.nonce')=? ORDER BY rowid DESC LIMIT 1")
                    .bind(&task.id).bind(task.revision.to_string()).bind(session_id).bind(nonce).fetch_optional(&self.db).await?.ok_or_else(||crate::Error::Rejected("browser observation unavailable".into()))?;
                let outcome: ReadOutcome = serde_json::from_str(&row.get::<String, _>("result"))?;
                ensure(outcome.valid(), "invalid browser evidence")?;
                let ReadOutcome::Completed {
                    observation: Some(observation),
                    ..
                } = outcome
                else {
                    return Err(crate::Error::Rejected("browser observation missing".into()));
                };
                let intent = BrowserIntent {
                    source_operation_id: row.get("id"),
                    local_device: row.get("device"),
                    local_epoch: row.get::<i64, _>("epoch").to_string(),
                    observation,
                    element_id: element_id.clone(),
                    effect: effect.clone(),
                };
                ensure(
                    intent.valid(&task.scope, time),
                    "browser intent outside observation",
                )?;
                Ok(ActionIntent::Browser(Box::new(intent)))
            }
            _ => Err(crate::Error::Rejected("model grant forbidden".into())),
        }
    }
}
