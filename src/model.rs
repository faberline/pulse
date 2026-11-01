use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Trusted ingress classification, never deserialized from an event payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Human,
    Tool,
    External,
}

impl Origin {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Tool => "tool",
            Self::External => "external",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Queued,
    Planning,
    Ready,
    Dispatched,
    Running,
    WaitingApproval,
    WaitingDevice,
    RetryWait,
    Blocked,
    CancelRequested,
    OutcomeUnknown,
    Reconciling,
    Succeeded,
    Failed,
    Cancelled,
}

impl State {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
    pub fn active(self) -> bool {
        matches!(
            self,
            Self::Planning | Self::Ready | Self::Dispatched | Self::Running
        )
    }
    pub(crate) fn permits(self, next: Self) -> bool {
        use State::*;
        matches!(
            (self, next),
            (Planning, Ready | WaitingApproval | Blocked | RetryWait)
                | (Ready, Dispatched | WaitingDevice | WaitingApproval)
                | (Dispatched, Running | OutcomeUnknown)
                | (
                    Running,
                    Planning | Succeeded | Failed | OutcomeUnknown | WaitingApproval | Blocked
                )
                | (CancelRequested, Cancelled | OutcomeUnknown)
                | (OutcomeUnknown, Reconciling)
                | (Reconciling, Blocked)
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Versions {
    pub workflow: String,
    pub schema: String,
    pub prompt: String,
    pub tools: String,
    pub model: String,
    pub provider_api: String,
    pub policy: String,
}
impl Versions {
    /// Explicitly synthetic versions for the local demonstration and tests.
    pub fn fixture() -> Self {
        Self {
            workflow: "core-1".into(),
            schema: "1".into(),
            prompt: "fixture-1".into(),
            tools: "fixture-1".into(),
            model: "fixture".into(),
            provider_api: "fixture".into(),
            policy: "read-only-1".into(),
        }
    }
    pub(crate) fn valid(&self) -> bool {
        [
            &self.workflow,
            &self.schema,
            &self.prompt,
            &self.tools,
            &self.model,
            &self.provider_api,
            &self.policy,
        ]
        .iter()
        .all(|s| !s.is_empty())
            && self.schema == "1"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub event_id: String,
    pub text: String,
    pub supersedes: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub goal: String,
    pub scope: String,
    pub authorization_event: String,
    #[serde(with = "decimal")]
    pub revision: u64,
    pub priority: i32,
    pub state: State,
    pub reason: Option<String>,
    pub decisions: Vec<Decision>,
    pub parent: Option<String>,
    pub dependencies: Vec<String>,
    pub requires_device: bool,
    pub created_at: i64,
    pub deadline: i64,
    pub wake_at: Option<i64>,
    pub active_run: Option<String>,
    pub cancel_requested: bool,
    pub model_calls: u32,
    pub tool_calls: u32,
    pub retry_count: u32,
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Create {
        id: String,
        goal: String,
        scope: String,
        priority: i32,
        requires_device: bool,
        parent: Option<String>,
        #[serde(default)]
        dependencies: Vec<String>,
    },
    Revise {
        id: String,
        #[serde(with = "decimal")]
        expected_revision: u64,
        goal: String,
    },
    Cancel {
        id: String,
        #[serde(with = "decimal")]
        expected_revision: u64,
    },
    Query {
        ids: Vec<String>,
    },
    Clarify {
        question: String,
    },
    Resume {
        id: String,
        #[serde(with = "decimal")]
        expected_revision: u64,
    },
    ReconcileRead {
        id: String,
        #[serde(with = "decimal")]
        expected_revision: u64,
    },
    ApproveOperation {
        binding: OperationBinding,
    },
    RejectOperation {
        binding: OperationBinding,
    },
}

/// Untrusted proposal data, bound to a persisted Tool event before it can wait for approval.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OperationProposal {
    pub id: String,
    pub token: Token,
    pub target: String,
    pub intent: Value,
    pub expires_at: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    Pending,
    Approved,
    Rejected,
    Withdrawn,
    Expired,
    Invalidated,
    Dispatched,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OperationRequest {
    pub proposal: OperationProposal,
    pub source_event: String,
    pub intent_digest: String,
    pub state: ApprovalState,
    pub approval_event: Option<String>,
    pub approved_by: Option<String>,
    pub decision_event: Option<String>,
    pub reason: Option<String>,
    pub operation_id: Option<String>,
}

/// Exact binding displayed to the human. No field can choose its author or renew a grant.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationBinding {
    pub task_id: String,
    #[serde(with = "decimal")]
    pub expected_revision: u64,
    pub request_id: String,
    pub intent_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Applied { task_ids: Vec<String> },
    Rejected { reason: String },
    Clarification { question: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ingested {
    pub event_id: String,
    pub duplicate: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Token {
    pub run_id: String,
    pub task_id: String,
    #[serde(with = "decimal")]
    pub revision: u64,
    #[serde(with = "decimal")]
    pub epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    pub token: Token,
    pub versions: Versions,
    pub checkpoint: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Unknown,
    Completed,
    NotExecuted,
    Ambiguous,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub idempotency_key: String,
    pub token: Token,
    pub target: String,
    pub intent: Value,
    pub intent_digest: String,
    pub state: OperationState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub operation_id: String,
    pub token: Token,
    pub success: bool,
    pub evidence: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReceiptEffect {
    Applied,
    Late,
    Duplicate,
}

/// A verified adapter conclusion. Not a model-provided instruction.
pub enum Resolution {
    NotExecuted { evidence: String },
    Executed(Receipt),
    Ambiguous { evidence: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxItem {
    #[serde(with = "decimal")]
    pub sequence: u64,
    pub key: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Health {
    pub states: std::collections::BTreeMap<String, u64>,
    pub queue_age_ms: i64,
    pub pending_notifications: u64,
    pub unresolved_operations: u64,
}

mod decimal {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let text = String::deserialize(deserializer)?;
        if text.is_empty()
            || (text.len() > 1 && text.starts_with('0'))
            || !text.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(D::Error::custom(
                "expected canonical unsigned decimal string",
            ));
        }
        text.parse().map_err(D::Error::custom)
    }
}
