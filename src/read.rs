//! Shared bounded read evidence. Transport and reconciliation use the same checks.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_READ_RESULT: usize = 32 * 1024;
fn hash_read_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
        let digest = hash_read_bytes(text.as_bytes());
        Self::Completed {
            text,
            digest,
            truncated,
        }
    }
    pub fn valid(&self) -> bool {
        match self {
            Self::Completed { text, digest, .. } => {
                text.len() <= MAX_READ_RESULT && *digest == hash_read_bytes(text.as_bytes())
            }
            Self::Rejected { reason } | Self::Unknown { reason } => {
                !reason.is_empty() && reason.len() <= 1024
            }
        }
    }
}
