use crate::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_FRAME: usize = 256 * 1024;
pub const MAX_IPC: usize = 1024 * 1024;
// Reserve room for worst-case JSON escaping within a 256 KiB frame.
pub const MAX_RESULT: usize = pulse_core::MAX_READ_RESULT;
pub const LEASE_MS: i64 = 60_000;

/// The server binds this role when it creates a one-use pairing code.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClientRole {
    Chat,
    LocalExecutor,
}
impl ClientRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::LocalExecutor => "local_executor",
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalStatus {
    pub connected: bool,
    pub stopped: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadOp {
    List {
        path: String,
    },
    Text {
        path: String,
    },
    GitStatus,
    GitDiff {
        path: String,
    },
    Browser {
        url: String,
    },
    BrowserInspect {
        url: String,
    },
    ComputerInspect {
        selection: pulse_core::ComputerSelection,
    },
    ComputerAction {
        selection: pulse_core::ComputerSelection,
        nonce: String,
        element_id: String,
        effect: pulse_core::ComputerEffect,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        grant: Option<Box<crate::computer_runtime::ComputerGrant>>,
    },
    BrowserAction {
        session_id: String,
        nonce: String,
        element_id: String,
        effect: pulse_core::BrowserEffect,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        grant: Option<Box<crate::browser_session::BrowserGrant>>,
    },
}
impl ReadOp {
    pub fn is_effect(&self) -> bool {
        matches!(
            self,
            Self::BrowserAction { .. } | Self::ComputerAction { .. }
        )
    }
    pub(crate) fn computer_selection(&self) -> Option<&pulse_core::ComputerSelection> {
        match self {
            Self::ComputerInspect { selection } | Self::ComputerAction { selection, .. } => {
                Some(selection)
            }
            _ => None,
        }
    }
    /// Stable lexical identity only. Executor scope, secret and symlink checks remain separate.
    pub(crate) fn canonical(&self) -> Result<Self> {
        if let Self::ComputerAction {
            selection,
            nonce,
            element_id,
            effect,
            ..
        } = self
        {
            ensure(
                selection.valid()
                    && uuid::Uuid::parse_str(nonce).is_ok()
                    && pulse_core::computer_element_path(element_id)
                    && match effect {
                        pulse_core::ComputerEffect::SetText { text } => {
                            pulse_core::browser_input_text(text)
                        }
                        _ => true,
                    },
                "invalid computer action",
            )?;
            return Ok(Self::ComputerAction {
                selection: selection.clone(),
                nonce: nonce.clone(),
                element_id: element_id.clone(),
                effect: effect.clone(),
                grant: None,
            });
        }
        if let Self::ComputerInspect { selection } = self {
            ensure(selection.valid(), "invalid computer selection")?;
            return Ok(self.clone());
        }
        if let Self::BrowserAction {
            session_id,
            nonce,
            element_id,
            effect,
            ..
        } = self
        {
            ensure(
                uuid::Uuid::parse_str(session_id).is_ok()
                    && uuid::Uuid::parse_str(nonce).is_ok()
                    && pulse_core::browser_identifier(element_id)
                    && match effect {
                        pulse_core::BrowserEffect::SetText { text } => {
                            pulse_core::browser_input_text(text)
                        }
                        _ => true,
                    },
                "invalid browser action",
            )?;
            return Ok(Self::BrowserAction {
                session_id: session_id.clone(),
                nonce: nonce.clone(),
                element_id: element_id.clone(),
                effect: effect.clone(),
                grant: None,
            });
        }
        if let Self::Browser { url } | Self::BrowserInspect { url } = self {
            let url = pulse_core::browser_url(&format!("browser:{url}"))
                .ok_or_else(|| crate::Error::Rejected("invalid browser URL".into()))?;
            return Ok(if matches!(self, Self::Browser { .. }) {
                Self::Browser { url: url.into() }
            } else {
                Self::BrowserInspect { url: url.into() }
            });
        }
        let path = match self {
            Self::List { path } if path.is_empty() => return Ok(self.clone()),
            Self::List { path } | Self::Text { path } | Self::GitDiff { path } => path,
            Self::GitStatus => return Ok(self.clone()),
            Self::Browser { .. }
            | Self::BrowserInspect { .. }
            | Self::BrowserAction { .. }
            | Self::ComputerInspect { .. }
            | Self::ComputerAction { .. } => {
                unreachable!()
            }
        };
        let parts = std::path::Path::new(path)
            .components()
            .map(|part| match part {
                std::path::Component::Normal(part) => part
                    .to_str()
                    .ok_or_else(|| crate::Error::Rejected("invalid read path".into())),
                _ => Err(crate::Error::Rejected("relative read path required".into())),
            })
            .collect::<Result<Vec<_>>>()?;
        ensure(!parts.is_empty(), "read path required")?;
        let path = parts.join("/");
        Ok(match self {
            Self::List { .. } => Self::List { path },
            Self::Text { .. } => Self::Text { path },
            Self::GitDiff { .. } => Self::GitDiff { path },
            Self::GitStatus => unreachable!(),
            Self::Browser { .. }
            | Self::BrowserInspect { .. }
            | Self::BrowserAction { .. }
            | Self::ComputerInspect { .. }
            | Self::ComputerAction { .. } => {
                unreachable!()
            }
        })
    }
    pub fn permitted_by(&self, scope: &str) -> bool {
        match self {
            Self::ComputerInspect { selection } | Self::ComputerAction { selection, .. } => {
                selection.scope().as_deref() == Some(scope)
            }
            Self::Browser { url } => {
                pulse_core::browser_url(scope).is_some_and(|allowed| allowed.as_str() == url)
            }
            Self::BrowserInspect { url } => {
                pulse_core::interaction_url(scope).is_some_and(|allowed| allowed.as_str() == url)
            }
            Self::BrowserAction { .. } => pulse_core::interaction_url(scope).is_some(),
            _ => scope == "pulse:read",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HumanAction {
    Schedule {
        request: pulse_core::ScheduleRequest,
        text: String,
    },
    DecideOperation {
        binding: pulse_core::OperationBinding,
        approve: bool,
    },
    Submit {
        id: String,
        goal: String,
    },
    SubmitComputer {
        id: String,
        goal: String,
        selection: pulse_core::ComputerSelection,
    },
    RebindComputer {
        task_id: String,
        revision: String,
        selection: pulse_core::ComputerSelection,
    },
    Cancel {
        task_id: String,
        revision: String,
    },
    Read {
        task_id: String,
        revision: String,
        operation: ReadOp,
    },
    Status,
    StopLocal {
        stopped: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DevicePayload {
    ArtifactChunk {
        operation_id: String,
        artifact: crate::artifact::ArtifactRef,
        index: u32,
        data: String,
    },
    Human {
        source_id: String,
        action: HumanAction,
    },
    Result {
        operation_id: String,
        task_id: String,
        revision: String,
        outcome: ReadOutcome,
    },
}

pub use pulse_core::ReadOutcome;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope<T> {
    pub version: u8,
    pub id: String,
    pub device: String,
    pub sequence: String,
    pub epoch: String,
    pub payload: T,
    pub digest: String,
    pub binding_digest: String,
}
impl<T: Serialize> Envelope<T> {
    pub fn new(device: &str, sequence: u64, epoch: u64, payload: T) -> Result<Self> {
        let mut value = Self {
            version: 1,
            id: uuid::Uuid::new_v4().to_string(),
            device: device.into(),
            sequence: sequence.to_string(),
            epoch: epoch.to_string(),
            digest: hash(&payload)?,
            binding_digest: String::new(),
            payload,
        };
        value.binding_digest = value.expected_digest()?;
        Ok(value)
    }
    fn expected_digest(&self) -> Result<String> {
        hash(&(
            self.version,
            &self.id,
            &self.device,
            &self.sequence,
            &self.epoch,
            &self.digest,
        ))
    }
    pub fn validate(&self) -> Result<(u64, u64)> {
        ensure(
            self.version == 1
                && !self.id.is_empty()
                && self.id.len() <= 128
                && !self.device.is_empty()
                && self.device.len() <= 128,
            "envelope identity/version",
        )?;
        ensure(
            self.digest == hash(&self.payload)? && self.binding_digest == self.expected_digest()?,
            "envelope digest mismatch",
        )?;
        ensure(
            serde_json::to_vec(self)?.len() <= MAX_FRAME,
            "frame too large",
        )?;
        Ok((number(&self.sequence)?, number(&self.epoch)?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServerPayload {
    ScheduleNotice {
        notice: pulse_core::ScheduleNotice,
    },
    Snapshot {
        tasks: Vec<pulse_core::Task>,
        messages: Vec<ChatMessage>,
        synced_at: i64,
        local: LocalStatus,
    },
    SetStopped {
        stopped: bool,
    },
    Read {
        operation_id: String,
        task_id: String,
        revision: String,
        operation: ReadOp,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub id: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<crate::artifact::ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<pulse_core::OperationRequest>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientWire {
    ScheduleReady,
    ComputerReady {
        permits: Vec<pulse_core::ComputerPermit>,
        #[serde(default)]
        interactions: bool,
    },
    BrowserInteractionReady {
        fixture_origin: Option<String>,
    },
    BrowserReady {
        fixture_origin: Option<String>,
    },
    Hello {
        device: String,
        cursor: String,
    },
    Data {
        envelope: Box<Envelope<DevicePayload>>,
    },
    Ack {
        sequence: String,
        id: String,
    },
    Heartbeat {
        epoch: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServerWire {
    Welcome {
        device: String,
        epoch: String,
        lease_ms: u32,
    },
    Data {
        envelope: Box<Envelope<ServerPayload>>,
        permitted: bool,
    },
    Ack {
        sequence: String,
        id: String,
    },
    Renewed {
        epoch: String,
        lease_ms: u32,
    },
    Error {
        reason: String,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairRequest {
    pub code: String,
    pub device: String,
    pub role: ClientRole,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub device: String,
    pub token: String,
    pub role: ClientRole,
}

/// Secret pending claim. Keep it in the vault, never a journal or UI state.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairClaim {
    pub code: String,
    pub attempt_id: String,
    pub device: String,
    pub role: ClientRole,
    pub token: String,
}
impl PairClaim {
    pub fn valid(&self) -> bool {
        uuid::Uuid::parse_str(&self.device).is_ok_and(|v| v.to_string() == self.device)
            && uuid::Uuid::parse_str(&self.attempt_id)
                .is_ok_and(|v| v.to_string() == self.attempt_id)
            && [&self.code, &self.token]
                .iter()
                .all(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
    }
    pub fn credential(&self) -> Credential {
        Credential {
            device: self.device.clone(),
            role: self.role,
            token: self.token.clone(),
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairIdentity {
    pub device: String,
    pub role: ClientRole,
}

pub(crate) fn secret() -> crate::Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| crate::Error::Rejected("randomness unavailable".into()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCommand {
    ListComputerWindows {
        request_id: String,
    },
    CancelComputerWindows {
        request_id: String,
    },
    RebindComputer {
        source_id: String,
        task_id: String,
        revision: String,
        selection: pulse_core::ComputerSelection,
        selected_at: i64,
    },
    SubmitComputer {
        source_id: String,
        id: String,
        goal: String,
        selection: pulse_core::ComputerSelection,
        selected_at: i64,
    },
    LoadArtifact {
        operation_id: String,
        digest: String,
    },
    Pair {
        code: String,
    },
    Action {
        source_id: String,
        action: HumanAction,
    },
    KillSwitch {
        stopped: bool,
    },
    Shutdown,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NativeState {
    #[serde(default)]
    pub schedule_notices: Vec<pulse_core::ScheduleNotice>,
    #[serde(default)]
    pub computer_windows: Option<ComputerWindowList>,
    #[serde(default)]
    pub memories: Vec<pulse_core::Memory>,
    #[serde(default)]
    pub memory_synced_at: Option<i64>,
    #[serde(default)]
    pub input_failure: Option<String>,
    #[serde(default)]
    pub computer_selections: Vec<crate::computer_selection::SelectionStatus>,
    #[serde(default)]
    pub computer_selecting: Option<String>,
    #[serde(default)]
    pub computer_failure: Option<String>,
    #[serde(default)]
    pub artifact_loading: Option<String>,
    #[serde(default)]
    pub artifact_failure: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<crate::artifact::LocalArtifact>,
    #[serde(default)]
    pub server: String,
    pub connected: bool,
    pub stopped: bool,
    #[serde(default)]
    pub local_connected: bool,
    pub paired: bool,
    pub message: String,
    pub pending: usize,
    pub synced_at: Option<i64>,
    pub tasks: Vec<pulse_core::Task>,
    pub messages: Vec<ChatMessage>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComputerWindowList {
    pub request_id: String,
    pub loading: bool,
    pub failed: bool,
    pub windows: Vec<pulse_core::ComputerWindowChoice>,
}

pub fn hash(value: &impl Serialize) -> Result<String> {
    Ok(hash_bytes(&serde_jcs::to_vec(value)?))
}
pub fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn number(s: &str) -> Result<u64> {
    ensure(
        !s.is_empty()
            && (s.len() == 1 || !s.starts_with('0'))
            && s.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal",
    )?;
    let value: u64 = s
        .parse()
        .map_err(|_| crate::Error::Rejected("decimal overflow".into()))?;
    ensure(value <= i64::MAX as u64, "decimal exceeds database range")?;
    Ok(value)
}
pub fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ensure(bytes.len() <= MAX_FRAME, "frame too large")?;
    let checked = serde_json::from_slice::<NoDuplicates>(bytes)?;
    Ok(serde_json::from_value(checked.0)?)
}

// Reject duplicate keys before converting wire input into a typed value.
struct NoDuplicates(serde_json::Value);
impl<'de> Deserialize<'de> for NoDuplicates {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = NoDuplicates;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut m: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut map = serde_json::Map::new();
                while let Some((k, v)) = m.next_entry::<String, NoDuplicates>()? {
                    if map.insert(k, v.0).is_some() {
                        return Err(serde::de::Error::custom("duplicate key"));
                    }
                }
                Ok(NoDuplicates(map.into()))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(v) = a.next_element::<NoDuplicates>()? {
                    values.push(v.0);
                }
                Ok(NoDuplicates(values.into()))
            }
            fn visit_str<E: serde::de::Error>(
                self,
                s: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(NoDuplicates(s.into()))
            }
            fn visit_string<E: serde::de::Error>(
                self,
                s: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(NoDuplicates(s.into()))
            }
            fn visit_bool<E: serde::de::Error>(
                self,
                b: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(NoDuplicates(b.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, n: i64) -> std::result::Result<Self::Value, E> {
                Ok(NoDuplicates(n.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, n: u64) -> std::result::Result<Self::Value, E> {
                Ok(NoDuplicates(n.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, n: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(n)
                    .map(|n| NoDuplicates(n.into()))
                    .ok_or_else(|| E::custom("nonfinite number"))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(NoDuplicates(serde_json::Value::Null))
            }
        }
        d.deserialize_any(Visitor)
    }
}
