//! An exact observed native control and effect. This data alone grants no permission.
use crate::{ComputerObservation, browser_input_text};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ComputerEffect {
    Press,
    SetText { text: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ComputerIntent {
    pub observation: ComputerObservation,
    pub element_id: String,
    pub effect: ComputerEffect,
}
impl ComputerIntent {
    pub fn target(&self) -> Option<String> {
        self.observation.selection().scope()
    }
    pub fn valid(&self, scope: &str, now: i64) -> bool {
        self.observation.valid(now)
            && self.target().as_deref() == Some(scope)
            && self.observation.elements.iter().any(|element| {
                element.id == self.element_id
                    && match &self.effect {
                        ComputerEffect::Press => element.can_press,
                        // Use the same bounded plain-text rule. No keys or submit controls.
                        ComputerEffect::SetText { text } => {
                            element.can_set_text && browser_input_text(text)
                        }
                    }
            })
    }
}
