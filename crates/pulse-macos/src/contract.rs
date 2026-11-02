use pulse_core::{ComputerBinding, ComputerIntent, ComputerObservation, ComputerSelection};
use serde::{Deserialize, Serialize};

pub const VERSION: &str = "pulse-computer-observe-6";
pub const LIMIT: usize = 65_536;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Error {
    InvalidRequest,
    PermissionsRequired,
    TargetUnavailable,
    TargetChanged,
    UnsupportedWindow,
    AccessibilityUnavailable,
    CaptureFailed,
    EvidenceLimit,
    PrivateStorageRequired,
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Permissions {
    pub accessibility: bool,
    pub screen_capture: bool,
}
impl Permissions {
    pub fn can_observe(self) -> bool {
        self.accessibility && self.screen_capture
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status {},
    Probe {
        expected_process_launched_at: String,
    },
    Observe {
        binding: ComputerBinding,
        expected_process_launched_at: Option<String>,
    },
    Act {
        binding: ComputerBinding,
        intent: Box<ComputerIntent>,
    },
}
impl Request {
    pub fn decode(bytes: &[u8], now: i64) -> Result<Self> {
        if bytes.len() > LIMIT {
            return Err(Error::InvalidRequest);
        }
        let request: Self = serde_json::from_slice(bytes).map_err(|_| Error::InvalidRequest)?;
        request.validate(now)?;
        if !matches!(request, Self::Act { .. }) && bytes.len() > 16_384 {
            return Err(Error::InvalidRequest);
        }
        Ok(request)
    }
    pub(crate) fn validate(&self, now: i64) -> Result<()> {
        match self {
            Self::Status {} => {}
            Self::Probe {
                expected_process_launched_at,
            } => {
                launch_time(expected_process_launched_at)?;
            }
            Self::Observe {
                binding,
                expected_process_launched_at,
            } => {
                if !binding.valid() {
                    return Err(Error::InvalidRequest);
                }
                if let Some(time) = expected_process_launched_at {
                    launch_time(time)?;
                }
            }
            Self::Act { binding, intent } => {
                let original = &intent.observation.binding;
                if !binding.valid()
                    || binding.task_id != original.task_id
                    || binding.revision != original.revision
                    || binding.device != original.device
                    || binding.epoch != original.epoch
                    || binding.operation_id == original.operation_id
                    || !intent
                        .target()
                        .is_some_and(|scope| intent.valid(&scope, now))
                {
                    return Err(Error::InvalidRequest);
                }
            }
        }
        Ok(())
    }
}
pub fn launch_time(text: &str) -> Result<i64> {
    text.parse::<i64>()
        .ok()
        .filter(|n| *n > 0 && n.to_string() == text)
        .ok_or(Error::InvalidRequest)
}
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub struct Observation {
    pub view: ComputerObservation,
    pub path: String,
}
impl Serialize for Observation {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut value = serde_json::to_value(&self.view).map_err(serde::ser::Error::custom)?;
        value["image"]["path"] = self.path.clone().into();
        value.serialize(serializer)
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionState {
    NotExecuted,
    Completed,
    Unknown,
}
#[derive(Serialize)]
pub struct ActionReceipt {
    pub binding: ComputerBinding,
    pub source_nonce: String,
    pub state: ActionState,
}

#[derive(Serialize)]
pub struct Response {
    pub version: &'static str,
    pub permissions: Permissions,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation: Option<Observation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection: Option<ComputerSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<ActionReceipt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub windows: Option<Vec<pulse_core::ComputerWindowChoice>>,
}
impl Response {
    pub fn empty() -> Self {
        Self {
            version: VERSION,
            permissions: crate::permissions(),
            observation: None,
            error: None,
            selection: None,
            action: None,
            windows: None,
        }
    }
    pub fn error(error: Error) -> Self {
        Self {
            error: Some(error),
            ..Self::empty()
        }
    }
}
#[cfg(target_os = "macos")]
pub(crate) fn bounded(text: &str, max: usize) -> String {
    text[..text.floor_char_boundary(max.min(text.len()))].into()
}
