//! Typed evidence and effects for one Human-authorized browser session.
use serde::{Deserialize, Serialize};

pub const BROWSER_MAX_PNG: usize = 4 * 1024 * 1024;
pub fn browser_png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 33
        || bytes.len() > BROWSER_MAX_PNG
        || !bytes.starts_with(b"\x89PNG\r\n\x1a\n")
        || &bytes[12..16] != b"IHDR"
    {
        return None;
    }
    let width = u32::from_be_bytes(bytes[16..20].try_into().ok()?);
    let height = u32::from_be_bytes(bytes[20..24].try_into().ok()?);
    (width > 0
        && height > 0
        && width <= 8192
        && height <= 8192
        && u64::from(width) * u64::from(height) <= 16_777_216)
        .then_some((width, height))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrowserForm {
    pub url: String,
    pub method: String,
    pub fields: Vec<(String, String)>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserElementKind {
    Button { form: Option<BrowserForm> },
    Text { value: String },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrowserElement {
    pub id: String,
    pub label: String,
    pub kind: BrowserElementKind,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrowserObservation {
    pub session_id: String,
    pub nonce: String,
    pub url: String,
    pub title: String,
    pub png_digest: String,
    pub elements: Vec<BrowserElement>,
    pub observed_at: i64,
    pub expires_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserEffect {
    Click,
    SetText { text: String },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrowserIntent {
    pub source_operation_id: String,
    pub local_device: String,
    pub local_epoch: String,
    pub observation: BrowserObservation,
    pub element_id: String,
    pub effect: BrowserEffect,
}
pub fn browser_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-:".contains(&b))
}
pub fn browser_input_text(text: &str) -> bool {
    text.len()<=1024 && !text.chars().any(|c|c.is_control() || matches!(c,'\u{e000}'..='\u{f8ff}'|'\u{f0000}'..='\u{ffffd}'|'\u{100000}'..='\u{10fffd}'|'\u{2028}'|'\u{2029}'))
}
fn sha_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl BrowserForm {
    pub fn valid(&self, origin: &url::Url) -> bool {
        crate::browser_url(&format!("browser:{}", self.url))
            .is_some_and(|u| u.origin() == origin.origin())
            && matches!(self.method.as_str(), "get" | "post")
            && self.fields.len() <= 16
            && self
                .fields
                .iter()
                .all(|(name, value)| !name.is_empty() && name.len() <= 128 && value.len() <= 1024)
            && self.encoded().len() <= 8192
    }
    pub fn encoded(&self) -> String {
        url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(self.fields.iter().map(|(k, v)| (k, v)))
            .finish()
    }
}
impl BrowserObservation {
    pub fn valid(&self) -> bool {
        let Some(url) = crate::browser_url(&format!("browser:{}", self.url)) else {
            return false;
        };
        uuid::Uuid::parse_str(&self.session_id).is_ok()
            && uuid::Uuid::parse_str(&self.nonce).is_ok()
            && sha_id(&self.png_digest)
            && self.title.len() <= 4096
            && self.elements.len() <= 16
            && self.observed_at > 0
            && self.expires_at > self.observed_at
            && self.expires_at.saturating_sub(self.observed_at) <= 120_000
            && serde_jcs::to_vec(self).is_ok_and(|v| v.len() <= 16 * 1024)
            && self.elements.iter().all(|e| {
                browser_identifier(&e.id)
                    && e.label.len() <= 256
                    && match &e.kind {
                        BrowserElementKind::Text { value } => value.len() <= 1024,
                        BrowserElementKind::Button { form } => {
                            form.as_ref().is_none_or(|f| f.valid(&url))
                        }
                    }
            })
            && self
                .elements
                .iter()
                .enumerate()
                .all(|(i, e)| !self.elements[..i].iter().any(|old| old.id == e.id))
    }
    pub fn same_view(
        &self,
        url: &str,
        title: &str,
        png_digest: &str,
        elements: &[BrowserElement],
    ) -> bool {
        self.url == url
            && self.title == title
            && self.png_digest == png_digest
            && self.elements == elements
    }
}
impl BrowserIntent {
    pub fn target(&self) -> String {
        format!("browser:{}", self.observation.session_id)
    }
    pub fn valid(&self, scope: &str, now: i64) -> bool {
        let Some(authorized) = crate::interaction_url(scope) else {
            return false;
        };
        let Some(actual) = crate::browser_url(&format!("browser:{}", self.observation.url)) else {
            return false;
        };
        self.observation.valid()
            && self.observation.observed_at <= now.saturating_add(5_000)
            && self.observation.expires_at <= now.saturating_add(125_000)
            && self.observation.expires_at > now
            && authorized.origin() == actual.origin()
            && !self.source_operation_id.is_empty()
            && self.source_operation_id.len() <= 256
            && uuid::Uuid::parse_str(&self.local_device).is_ok()
            && self
                .local_epoch
                .parse::<u64>()
                .is_ok_and(|n| n > 0 && n <= i64::MAX as u64)
            && self.observation.elements.iter().any(|e| {
                e.id == self.element_id
                    && match (&self.effect, &e.kind) {
                        (BrowserEffect::Click, BrowserElementKind::Button { .. }) => true,
                        (BrowserEffect::SetText { text }, BrowserElementKind::Text { .. }) => {
                            browser_input_text(text)
                        }
                        _ => false,
                    }
            })
    }
}
