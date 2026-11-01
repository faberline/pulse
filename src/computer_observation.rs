//! Native observation data. It does not grant permission to read an app or perform an effect.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComputerTarget {
    pub bundle_id: String,
    pub pid: i32,
    pub window_id: u32,
}
impl ComputerTarget {
    pub fn valid(&self) -> bool {
        self.pid > 0
            && self.window_id > 0
            && self.bundle_id.len() <= 255
            && self.bundle_id.split('.').count() >= 2
            && self.bundle_id.split('.').all(|s| !s.is_empty())
            && self
                .bundle_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.'))
    }
}

/// Exact Human-selected process instance and window. It is not a permission grant.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComputerSelection {
    pub target: ComputerTarget,
    pub process_launched_at: i64,
}
impl ComputerSelection {
    pub fn valid(&self) -> bool {
        self.target.valid() && self.process_launched_at > 0
    }
    pub fn scope(&self) -> Option<String> {
        self.valid().then(|| {
            format!(
                "computer:{}:{}:{}:{}",
                self.target.bundle_id,
                self.target.pid,
                self.target.window_id,
                self.process_launched_at
            )
        })
    }
    pub fn from_scope(scope: &str) -> Option<Self> {
        let parts: Vec<_> = scope.strip_prefix("computer:")?.split(':').collect();
        if parts.len() != 4 {
            return None;
        }
        let selection = Self {
            target: ComputerTarget {
                bundle_id: parts[0].into(),
                pid: parts[1].parse().ok()?,
                window_id: parts[2].parse().ok()?,
            },
            process_launched_at: parts[3].parse().ok()?,
        };
        (selection.scope().as_deref() == Some(scope)).then_some(selection)
    }
}

/// Local Human selection cache. It has no authority over task state or effects.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComputerPermit {
    pub task_id: String,
    pub revision: String,
    pub selection: ComputerSelection,
    pub selected_at: i64,
    pub expires_at: i64,
}
impl ComputerPermit {
    pub fn valid(&self, now: i64) -> bool {
        !self.task_id.is_empty()
            && self.task_id.len() <= 128
            && !self.task_id.chars().any(char::is_control)
            && self
                .revision
                .parse::<u64>()
                .is_ok_and(|n| n > 0 && n <= i64::MAX as u64 && n.to_string() == self.revision)
            && self.selection.valid()
            && self.selected_at > 0
            && self.selected_at <= now.saturating_add(1000)
            && self.selected_at.checked_add(900_000) == Some(self.expires_at)
            && self.expires_at > now
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComputerBinding {
    pub operation_id: String,
    pub task_id: String,
    #[serde(with = "crate::model::decimal")]
    pub revision: u64,
    pub device: String,
    #[serde(with = "crate::model::decimal")]
    pub epoch: u64,
}
impl ComputerBinding {
    pub fn valid(&self) -> bool {
        !self.operation_id.is_empty()
            && self.operation_id.len() <= 256
            && !self.task_id.is_empty()
            && self.task_id.len() <= 128
            && !self
                .operation_id
                .chars()
                .chain(self.task_id.chars())
                .any(char::is_control)
            && self.revision > 0
            && self.epoch > 0
            && uuid::Uuid::parse_str(&self.device).is_ok()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ComputerRect {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}
impl ComputerRect {
    pub fn valid(&self) -> bool {
        [self.x, self.y, self.width, self.height]
            .iter()
            .all(|n| n.is_finite())
            && self.width > 0.
            && self.height > 0.
            && self.width <= 8192.
            && self.height <= 8192.
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComputerElement {
    pub id: String,
    pub role: String,
    pub label: String,
    pub value: Option<String>,
    pub can_press: bool,
    pub can_set_text: bool,
}
impl ComputerElement {
    fn valid(&self) -> bool {
        computer_element_path(&self.id)
            && self.role.len() <= 64
            && self.role != "AXSecureTextField"
            && self.label.len() <= 256
            && self.value.as_ref().is_none_or(|s| s.len() <= 1024)
            && (!self.can_press || self.role == "AXButton")
            && (!self.can_set_text
                || (matches!(self.role.as_str(), "AXTextField" | "AXTextArea")
                    && self.value.is_some()))
    }
}
pub fn computer_element_path(value: &str) -> bool {
    let parts: Vec<_> = value.split('.').collect();
    parts.first() == Some(&"window")
        && parts.len() <= 9
        && parts[1..].iter().all(|part| {
            part.parse::<u8>()
                .is_ok_and(|n| n < 128 && n.to_string() == *part)
        })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComputerImageRef {
    pub digest: String,
    pub length: u32,
    pub width: u32,
    pub height: u32,
}
impl ComputerImageRef {
    pub fn valid(&self) -> bool {
        self.digest.len() == 64
            && self
                .digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && (33..=crate::BROWSER_MAX_PNG as u32).contains(&self.length)
            && self.width > 0
            && self.height > 0
            && self.width <= 8192
            && self.height <= 8192
            && u64::from(self.width) * u64::from(self.height) <= 16_777_216
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ComputerObservation {
    pub binding: ComputerBinding,
    pub target: ComputerTarget,
    pub process_launched_at: i64,
    pub title: String,
    pub frame: ComputerRect,
    pub elements: Vec<ComputerElement>,
    pub truncated: bool,
    pub nonce: String,
    pub observed_at: i64,
    pub expires_at: i64,
    pub image: ComputerImageRef,
}
impl ComputerObservation {
    /// Retained evidence may outlive its action lifetime. It still has to be well formed.
    pub fn valid_evidence(&self) -> bool {
        self.valid(self.observed_at)
    }
    pub fn selection(&self) -> ComputerSelection {
        ComputerSelection {
            target: self.target.clone(),
            process_launched_at: self.process_launched_at,
        }
    }
    pub fn valid(&self, now: i64) -> bool {
        self.binding.valid()
            && self.target.valid()
            && self.image.valid()
            && self.frame.valid()
            && self.process_launched_at > 0
            && self.process_launched_at <= self.observed_at
            && !self.title.is_empty()
            && self.title.len() <= 512
            && uuid::Uuid::parse_str(&self.nonce).is_ok()
            && self.observed_at > 0
            && now
                .checked_add(1000)
                .is_some_and(|latest| self.observed_at <= latest)
            && self.observed_at.checked_add(120_000) == Some(self.expires_at)
            && self.expires_at > now
            && self.elements.len() <= 128
            && self.elements.iter().all(ComputerElement::valid)
            && self
                .elements
                .iter()
                .map(|e| &e.id)
                .collect::<BTreeSet<_>>()
                .len()
                == self.elements.len()
            && self
                .elements
                .iter()
                .filter(|e| e.can_press || e.can_set_text)
                .count()
                <= 16
            && serde_json::to_vec(&self.elements).is_ok_and(|s| s.len() <= 32_768)
    }
}
