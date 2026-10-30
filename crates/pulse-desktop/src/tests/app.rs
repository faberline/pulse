use super::*;
fn key_press(
    key: iced::keyboard::Key,
    code: iced::keyboard::key::Code,
    modifiers: iced::keyboard::Modifiers,
) -> text_editor::KeyPress {
    text_editor::KeyPress {
        modified_key: key.clone(),
        key,
        physical_key: iced::keyboard::key::Physical::Code(code),
        modifiers,
        text: Some("i".into()),
        status: text_editor::Status::Focused { is_hovered: false },
    }
}
#[test]
fn picture_shortcut_does_not_insert_a_letter_into_the_draft() {
    let press = key_press(
        iced::keyboard::Key::Character("i".into()),
        iced::keyboard::key::Code::KeyI,
        iced::keyboard::Modifiers::COMMAND,
    );
    assert!(matches!(
        editor_key_binding(press),
        Some(text_editor::Binding::Custom(Message::Pick(Picker::Image)))
    ));
}
#[test]
fn enter_keeps_newlines_and_command_enter_sends_only_when_focused() {
    use iced::keyboard::{
        Key, Modifiers,
        key::{Code, Named},
    };
    let mut press = key_press(Key::Named(Named::Enter), Code::Enter, Modifiers::NONE);
    assert!(matches!(
        editor_key_binding(press.clone()),
        Some(text_editor::Binding::Enter)
    ));
    press.modifiers = Modifiers::COMMAND;
    assert!(matches!(
        editor_key_binding(press.clone()),
        Some(text_editor::Binding::Custom(Message::Submit))
    ));
    press.status = text_editor::Status::Active;
    assert!(editor_key_binding(press).is_none());
}
#[test]
fn retry_keeps_message_identity_and_cannot_cross_connections() {
    let binding = [
        "https://pulse.example/".into(),
        "/repo".into(),
        "/private".into(),
    ];
    let pending = PendingSubmit {
        source_id: "source".into(),
        task_id: "task".into(),
        body: "message".into(),
        binding: binding.clone(),
    };
    let a = serde_json::to_value(pending.command(&binding).unwrap()).unwrap();
    let b = serde_json::to_value(pending.command(&binding).unwrap()).unwrap();
    assert_eq!(a, b);
    assert_eq!(a["source_id"], "source");
    assert_eq!(a["action"]["id"], "task");
    let mut other = binding;
    other[0] = "https://other.example/".into();
    assert!(pending.command(&other).is_none());
}
#[test]
fn unavailable_helper_does_not_clear_or_lock_the_draft() {
    let mut app = App::empty(ConnectionForm::default());
    app.composer
        .edit(text_editor::Action::Edit(text_editor::Edit::Paste(
            std::sync::Arc::new("草稿\n第二行".into()),
        )));
    drop(app.update(Message::Submit));
    assert_eq!(app.composer.ready().as_deref(), Some("草稿\n第二行"));
    assert!(app.pending.is_none());
}
#[test]
fn first_launch_opens_chat_without_showing_settings_or_tools() {
    let app = App::empty(ConnectionForm::default());
    assert_eq!(app.screen, Screen::Chat);
    assert!(!app.progress && !app.advanced && !app.operations);
    assert!(app.composer.text().is_empty());
}
#[test]
fn saved_messages_render_as_user_messages_without_system_setup_text() {
    let b = Bubble::new("已保存任務：你好\n\n模型尚未接入。可選任務後使用只讀工具。");
    assert!(b.user);
    assert_eq!(b.label, "你 · 已保存");
    assert_eq!(b.blocks.len(), 1);
}
