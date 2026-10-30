use iced::widget::text_editor;
use pulse_link::protocol::ChatMessage;

/// A draft stays visible until the helper reports the same durable message ID.
pub(crate) struct Composer {
    pub content: text_editor::Content,
    awaiting: Option<(String, String)>,
}
impl Default for Composer {
    fn default() -> Self {
        Self {
            content: text_editor::Content::new(),
            awaiting: None,
        }
    }
}
impl Composer {
    pub fn edit(&mut self, action: text_editor::Action) {
        self.content.perform(action);
    }
    pub fn text(&self) -> String {
        self.content.text()
    }
    pub fn ready(&self) -> Option<String> {
        let text = self.text();
        (!text.trim().is_empty() && text.len() <= 4096 && self.awaiting.is_none()).then_some(text)
    }
    pub fn sent(&mut self, id: String, text: String) {
        self.awaiting = Some((id, text));
    }
    pub fn observe(&mut self, messages: &[ChatMessage]) {
        if let Some(id) = messages
            .iter()
            .find(|message| {
                self.awaiting
                    .as_ref()
                    .is_some_and(|(id, _)| message.id == *id)
            })
            .map(|message| message.id.clone())
        {
            self.confirm(&id);
        }
    }
    pub fn waiting(&self) -> bool {
        self.awaiting.is_some()
    }
    pub fn retain_draft(&mut self) {
        self.awaiting = None;
    }
    pub fn confirm(&mut self, source_id: &str) {
        if let Some((id, submitted)) = &self.awaiting
            && id == source_id
        {
            if self.text() == *submitted {
                self.content = text_editor::Content::new();
            }
            self.awaiting = None;
        }
    }
}

#[cfg(test)]
#[path = "tests/composer.rs"]
mod tests;
