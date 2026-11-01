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

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemoryCommand {
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
    Stored { id: String, revision: u64 },
    Forgotten { id: String, revision: u64 },
    Redacted { id: String, revision: u64 },
}
