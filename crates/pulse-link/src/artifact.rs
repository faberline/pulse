//! Source-bound PNG transport. Never accepts a client-selected local path.
use crate::{Result, ensure, protocol::hash_bytes};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

pub const MAX_PNG: usize = 4 * 1024 * 1024;
pub const CHUNK: usize = 64 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub task_id: String,
    pub revision: String,
    pub digest: String,
    pub length: u32,
    pub url: String,
    pub title: String,
    pub observed_at: i64,
    pub width: u32,
    pub height: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalArtifact {
    pub operation_id: String,
    pub artifact: ArtifactRef,
    pub path: String,
}
pub struct CapturedImage {
    pub artifact: ArtifactRef,
    pub png: Vec<u8>,
}
pub struct ExecutedRead {
    pub outcome: crate::protocol::ReadOutcome,
    pub image: Option<CapturedImage>,
}
pub fn digest_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn png_dimensions(bytes: &[u8]) -> Result<(u32, u32)> {
    ensure(
        bytes.len() >= 33
            && bytes.len() <= MAX_PNG
            && bytes.starts_with(b"\x89PNG\r\n\x1a\n")
            && &bytes[12..16] == b"IHDR",
        "invalid artifact PNG",
    )?;
    let width = u32::from_be_bytes(bytes[16..20].try_into().expect("checked header"));
    let height = u32::from_be_bytes(bytes[20..24].try_into().expect("checked header"));
    ensure(
        width > 0
            && height > 0
            && width <= 8192
            && height <= 8192
            && u64::from(width) * u64::from(height) <= 16_777_216,
        "artifact dimensions exceed limit",
    )?;
    Ok((width, height))
}
impl ArtifactRef {
    pub fn validate(&self) -> Result<()> {
        ensure(
            !self.task_id.is_empty()
                && self.task_id.len() <= 128
                && crate::protocol::number(&self.revision).is_ok_and(|n| n > 0)
                && digest_id(&self.digest)
                && (33..=MAX_PNG as u32).contains(&self.length)
                && pulse_core::browser_url(&format!("browser:{}", self.url)).is_some()
                && self.title.len() <= 4096
                && self.observed_at > 0
                && self.width > 0
                && self.height > 0
                && self.width <= 8192
                && self.height <= 8192
                && u64::from(self.width) * u64::from(self.height) <= 16_777_216,
            "invalid artifact metadata",
        )
    }
    pub fn verify(&self, png: &[u8]) -> Result<()> {
        self.validate()?;
        ensure(
            png.len() == self.length as usize
                && hash_bytes(png) == self.digest
                && png_dimensions(png)? == (self.width, self.height),
            "artifact digest, size or dimensions mismatch",
        )
    }
    pub fn chunks(&self) -> usize {
        (self.length as usize).div_ceil(CHUNK)
    }
    pub fn chunk(&self, index: u32, data: &str) -> Result<Vec<u8>> {
        self.validate()?;
        ensure(
            (index as usize) < self.chunks() && data.len() <= CHUNK.div_ceil(3) * 4,
            "artifact chunk limit",
        )?;
        let bytes = STANDARD
            .decode(data)
            .map_err(|_| crate::Error::Rejected("invalid artifact encoding".into()))?;
        let expected = (self.length as usize - (index as usize) * CHUNK).min(CHUNK);
        ensure(bytes.len() == expected, "artifact chunk size mismatch")?;
        Ok(bytes)
    }
}
