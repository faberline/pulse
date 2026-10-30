use super::*;
fn paste(composer: &mut Composer, text: &str) {
    composer.edit(text_editor::Action::Edit(text_editor::Edit::Paste(
        std::sync::Arc::new(text.into()),
    )));
}
#[test]
fn multiline_draft_keeps_chinese_and_code() {
    let mut c = Composer::default();
    let text = "你好\n\n```rust\nlet n = 1;\n```";
    paste(&mut c, text);
    assert_eq!(c.ready().as_deref(), Some(text));
}
#[test]
fn only_matching_durable_message_clears_the_draft() {
    let mut c = Composer::default();
    paste(&mut c, "保留我");
    c.sent("sent".into(), c.text());
    assert!(c.ready().is_none());
    c.observe(&[ChatMessage {
        id: "other".into(),
        body: "receipt".into(),
    }]);
    assert_eq!(c.text(), "保留我");
    c.observe(&[ChatMessage {
        id: "sent".into(),
        body: "receipt".into(),
    }]);
    assert!(c.text().is_empty());
}
#[test]
fn acknowledgement_preserves_later_edits() {
    let mut c = Composer::default();
    paste(&mut c, "第一則");
    c.sent("sent".into(), c.text());
    c.edit(text_editor::Action::SelectAll);
    paste(&mut c, "新的草稿");
    c.observe(&[ChatMessage {
        id: "sent".into(),
        body: "receipt".into(),
    }]);
    assert_eq!(c.ready().as_deref(), Some("新的草稿"));
}
#[test]
fn oversized_input_is_retained_for_editing() {
    let mut c = Composer::default();
    paste(&mut c, &"中".repeat(1400));
    assert!(c.ready().is_none());
    assert_eq!(c.text().chars().count(), 1400);
}
