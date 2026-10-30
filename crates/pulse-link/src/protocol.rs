use crate::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_FRAME: usize = 256 * 1024;
pub const MAX_IPC: usize = 1024 * 1024;
// Reserve room for worst-case JSON escaping within a 256 KiB frame.
pub const MAX_RESULT: usize = 32 * 1024;
pub const LEASE_MS: i64 = 60_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadOp {
    List { path: String },
    Text { path: String },
    GitStatus,
    GitDiff { path: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HumanAction {
    Submit {
        id: String,
        goal: String,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DevicePayload {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadOutcome {
    Completed {
        text: String,
        digest: String,
        truncated: bool,
    },
    Rejected {
        reason: String,
    },
    Unknown {
        reason: String,
    },
}

impl ReadOutcome {
    pub fn text(text: String, truncated: bool) -> Self {
        let digest = hash_bytes(text.as_bytes());
        Self::Completed {
            text,
            digest,
            truncated,
        }
    }
    pub fn valid(&self) -> bool {
        match self {
            Self::Completed { text, digest, .. } => {
                text.len() <= MAX_RESULT && *digest == hash_bytes(text.as_bytes())
            }
            Self::Rejected { reason } | Self::Unknown { reason } => {
                !reason.is_empty() && reason.len() <= 1024
            }
        }
    }
}

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
    Snapshot {
        tasks: Vec<pulse_core::Task>,
        messages: Vec<ChatMessage>,
        synced_at: i64,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientWire {
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
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub device: String,
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCommand {
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
    pub server: String,
    pub connected: bool,
    pub stopped: bool,
    pub paired: bool,
    pub message: String,
    pub pending: usize,
    pub synced_at: Option<i64>,
    pub tasks: Vec<pulse_core::Task>,
    pub messages: Vec<ChatMessage>,
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
