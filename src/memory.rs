//! Explicit Human knowledge. This API does not feed prompts or grant tool access.
use serde::{Deserialize, Serialize};

/// Maximum lifetime of a source, including retries and delayed delivery.
pub const MEMORY_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Memory {
    pub id: String,
    pub revision: u64,
    pub label: String,
    pub body: String,
    /// Random request ID. Source text exists only in this live record.
    pub source_id: String,
    pub source_at: i64,
    pub expires_at: i64,
}

impl std::fmt::Debug for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memory")
            .field("id", &self.id)
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryView {
    pub keeper: String,
    pub epoch: u64,
    pub memories: Vec<Memory>,
    pub redacted_sources: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemoryCommand {
    Revoke {
        sources: Vec<String>,
        expected: Option<MemoryRef>,
    },
    Remember {
        label: String,
        body: String,
    },
    Correct {
        id: String,
        revision: u64,
        label: String,
        body: String,
    },
    Forget {
        id: String,
        revision: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemoryRef {
    pub id: String,
    pub revision: u64,
}

/// Human adoption of source references for one task revision. No source body.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskMemoryRequest {
    pub source_id: String,
    pub source_at: i64,
    pub task_id: String,
    #[serde(with = "crate::model::decimal")]
    pub task_revision: u64,
    pub sources: Vec<MemoryRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskMemorySource {
    pub memory_id: String,
    pub revision: u64,
    pub source_id: String,
    pub expires_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskMemorySources {
    pub request: TaskMemoryRequest,
    pub authorization_event: String,
    pub sources: Vec<TaskMemorySource>,
    pub invalidated: bool,
}

/// A server-authorized erasure of an existing chat frame. Header identity stays
/// intact so a lost ACK does not resurrect content or create a sequence conflict.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ErasedProjection {
    pub version: u32,
    pub id: String,
    pub device: String,
    pub sequence: String,
    pub epoch: String,
    pub digest: String,
    pub binding_digest: String,
}
impl ErasedProjection {
    pub fn valid(&self) -> bool {
        let number = |s: &str| {
            s.parse::<u64>()
                .is_ok_and(|n| n <= i64::MAX as u64 && n.to_string() == s)
        };
        self.version == 1
            && !self.id.is_empty()
            && self.id.len() <= 128
            && !self.device.is_empty()
            && self.device.len() <= 128
            && number(&self.sequence)
            && self.sequence != "0"
            && number(&self.epoch)
            && self.digest.len() == 64
            && self.digest.bytes().all(|b| b.is_ascii_hexdigit())
            && crate::store::digest(&(
                self.version,
                &self.id,
                &self.device,
                &self.sequence,
                &self.epoch,
                &self.digest,
            ))
            .is_ok_and(|s| s == self.binding_digest)
    }
}

/// Only the authenticated Human ingress may construct a mutation.
/// Never deserialize `Origin` from this request or an untrusted payload.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryRequest {
    pub source_id: String,
    pub source_at: i64,
    pub command: MemoryCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MemoryOutcome {
    Revoked { current_changed: bool },
    Stored { id: String, revision: u64 },
    Forgotten { id: String, revision: u64 },
    Redacted { id: String, revision: u64 },
}
