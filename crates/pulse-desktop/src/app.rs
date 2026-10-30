use crate::{ChatConnection, RichBlock, composer::Composer, rich_content, selected_image};
use iced::{
    Element, Length, Task,
    widget::{
        self, button, column, container, markdown, row, scrollable, text, text_editor, text_input,
    },
};
use pulse_link::protocol::{HumanAction, NativeCommand, NativeState, ReadOp};
use std::path::PathBuf;
#[cfg(target_os = "macos")]
use std::process::Command;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Screen {
    Chat,
    Settings,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum Picker {
    Image,
    Data,
}
#[derive(Clone, Debug)]
pub(crate) struct Preview {
    handle: widget::image::Handle,
    name: String,
}
#[derive(Clone, Debug)]
pub(crate) enum Message {
    Quit,
    Poll,
    Edit(text_editor::Action),
    Submit,
    Screen(Screen),
    Progress,
    Advanced,
    Operations,
    PairCode(String),
    Pair,
    Select(String),
    Cancel,
    Refresh,
    Readme,
    Status,
    Kill,
    Restart,
    Link(markdown::Uri),
    Server(String),
    DataDir(String),
    Development(bool),
    Connect,
    Pick(Picker),
    Picked(Picker, Option<PathBuf>),
    ImageLoaded(Result<Preview, String>),
    RemoveImage,
    Key(iced::keyboard::Event),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConnectionForm {
    server: String,
    data_dir: String,
    dev_loopback: bool,
}
impl Default for ConnectionForm {
    fn default() -> Self {
        Self {
            server: String::new(),
            data_dir: pulse_link::journal::default_data_dir()
                .join("chat")
                .to_string_lossy()
                .into(),
            dev_loopback: false,
        }
    }
}
impl ConnectionForm {
    fn args(&self) -> Vec<String> {
        let mut args = vec![
            "--server".into(),
            self.server.clone(),
            "--data-dir".into(),
            self.data_dir.clone(),
        ];
        if self.dev_loopback {
            args.push("--dev-loopback".into());
        }
        args
    }
    fn load() -> Self {
        let args: Vec<_> = std::env::args().skip(1).collect();
        if !args.is_empty() {
            if let Ok(config) = pulse_link::client::Config::parse_chat(args.clone()) {
                return Self {
                    server: config.server.to_string(),
                    data_dir: config.data_dir.to_string_lossy().into(),
                    dev_loopback: args.iter().any(|a| a == "--dev-loopback"),
                };
            }
            return Self::default();
        }
        pulse_link::journal::private_read(
            &pulse_link::journal::default_data_dir().join("desktop-config.json"),
        )
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
    }
    fn save(&self) -> pulse_link::Result<()> {
        let dir = pulse_link::journal::default_data_dir();
        pulse_link::journal::private_dir(&dir)?;
        let temporary = dir.join(format!("desktop-config-{}.tmp", uuid::Uuid::new_v4()));
        pulse_link::journal::private_write(&temporary, &serde_json::to_vec(self)?)?;
        std::fs::rename(temporary, dir.join("desktop-config.json"))?;
        Ok(())
    }
}

struct Bubble {
    label: &'static str,
    user: bool,
    blocks: Vec<RichBlock>,
}
impl Bubble {
    fn new(body: &str) -> Self {
        let (label, user, content) = if let Some(body) = body.strip_prefix("已保存任務：") {
            (
                "你 · 已保存",
                true,
                body.strip_suffix("\n\n模型尚未接入。可選任務後使用只讀工具。")
                    .unwrap_or(body),
            )
        } else if let Some(body) = body.strip_prefix("本機待送：") {
            ("你 · 待送", true, body)
        } else if let Some(body) = body.strip_prefix("雲端已收件：") {
            ("你 · 已收件", true, body)
        } else {
            ("Pulse", false, body)
        };
        Self {
            label,
            user,
            blocks: rich_content(content),
        }
    }
    fn view(&self) -> Element<'_, Message> {
        let body = container(
            column![
                text(self.label).size(12),
                column(self.blocks.iter().map(RichBlock::view)).spacing(10)
            ]
            .spacing(8),
        )
        .padding(14)
        .width(Length::Fill);
        if self.user {
            row![
                widget::Space::new().width(36),
                body.style(container::rounded_box)
            ]
            .into()
        } else {
            body.into()
        }
    }
}

struct PendingSubmit {
    source_id: String,
    task_id: String,
    body: String,
    binding: [String; 3],
}
impl PendingSubmit {
    fn command(&self, binding: &[String; 3]) -> Option<NativeCommand> {
        (self.binding == *binding).then(|| NativeCommand::Action {
            source_id: self.source_id.clone(),
            action: HumanAction::Submit {
                id: self.task_id.clone(),
                goal: self.body.clone(),
            },
        })
    }
}
pub(crate) struct App {
    connection: Option<ChatConnection>,
    state: NativeState,
    composer: Composer,
    pair_code: String,
    selected: Option<String>,
    notice: String,
    timeline: Vec<Bubble>,
    last_render: String,
    form: ConnectionForm,
    screen: Screen,
    progress: bool,
    advanced: bool,
    operations: bool,
    image: Option<Preview>,
    picking: bool,
    pending: Option<PendingSubmit>,
    active_binding: Option<[String; 3]>,
}
impl App {
    pub(crate) fn new() -> Self {
        let mut app = Self::empty(ConnectionForm::load());
        app.restart();
        app
    }
    fn empty(form: ConnectionForm) -> Self {
        Self {
            connection: None,
            state: NativeState::default(),
            composer: Composer::default(),
            pair_code: String::new(),
            selected: None,
            notice: String::new(),
            timeline: vec![],
            last_render: String::new(),
            form,
            screen: Screen::Chat,
            progress: false,
            advanced: false,
            operations: false,
            image: None,
            picking: false,
            pending: None,
            active_binding: None,
        }
    }
    fn restart(&mut self) {
        let previous = self.connection.take();
        self.state.connected = false;
        let binding = self.binding();
        let switched = self
            .active_binding
            .as_ref()
            .is_some_and(|current| *current != binding);
        let unconfirmed = switched && self.pending.is_some();
        if switched {
            self.state = NativeState::default();
            self.timeline.clear();
            self.last_render.clear();
            self.selected = None;
            if self.pending.take().is_some() {
                self.composer.retain_draft();
            }
        }
        self.active_binding = Some(binding);
        if self.form.server.is_empty() {
            self.notice = "請先到設定完成連線。你可以先寫好訊息。".into();
            return;
        }
        match ChatConnection::start(&self.form.args(), previous) {
            Ok(connection) => {
                self.connection = Some(connection);
                self.notice = "正在連線…".into();
                if let Some(command) = self
                    .pending
                    .as_ref()
                    .and_then(|pending| pending.command(&self.binding()))
                {
                    self.send(command);
                }
                if unconfirmed {
                    self.notice = "連線已切換。上一個連線的待送訊息仍須確認，草稿會保留。".into();
                }
            }
            Err(_) => self.notice = "本機連線無法啟動。請到設定檢查連線。".into(),
        }
    }
    fn binding(&self) -> [String; 3] {
        [
            url::Url::parse(&self.form.server)
                .map(|url| url.to_string())
                .unwrap_or_else(|_| self.form.server.clone()),
            "chat".into(),
            self.form.data_dir.clone(),
        ]
    }
    fn send(&mut self, command: NativeCommand) -> bool {
        if self
            .connection
            .as_ref()
            .is_some_and(|connection| connection.commands.try_send(command).is_ok())
        {
            true
        } else {
            self.notice = "訊息尚未送出。請到設定檢查連線；草稿仍保留。".into();
            false
        }
    }
    fn action(&mut self, action: HumanAction) -> bool {
        self.send(NativeCommand::Action {
            source_id: uuid::Uuid::new_v4().to_string(),
            action,
        })
    }
    pub(crate) fn update(&mut self, message: Message) -> Task<Message> {
        use iced::keyboard::{Event, Key, key::Named};
        if let Message::Key(Event::KeyPressed {
            key,
            modifiers,
            physical_key,
            ..
        }) = &message
        {
            if key.as_ref() == Key::Named(Named::Tab) {
                return if modifiers.shift() {
                    widget::operation::focus_previous()
                } else {
                    widget::operation::focus_next()
                };
            }
            if key.as_ref() == Key::Named(Named::Escape) {
                self.screen = Screen::Chat;
                self.progress = false;
                return widget::operation::focus("pulse-chat");
            }
            if modifiers.command() {
                return match key.to_latin(*physical_key) {
                    Some('q') => self.update(Message::Quit),
                    Some('n') => self.update(Message::Screen(Screen::Chat)),
                    Some('l') => self.update(Message::Screen(Screen::Settings)),
                    Some('i') if self.screen == Screen::Chat => {
                        self.update(Message::Pick(Picker::Image))
                    }
                    _ => Task::none(),
                };
            }
        }
        match message {
            Message::Quit => return iced::exit(),
            Message::Key(_) => {}
            Message::Screen(screen) => {
                self.screen = screen;
                if screen == Screen::Chat {
                    self.pair_code.clear();
                    return widget::operation::focus("pulse-chat");
                }
                return widget::operation::focus("pulse-server");
            }
            Message::Progress => self.progress = !self.progress,
            Message::Advanced => self.advanced = !self.advanced,
            Message::Operations => self.operations = !self.operations,
            Message::Edit(action) => self.composer.edit(action),
            Message::Submit => {
                if let Some(body) = self.composer.ready() {
                    let source_id = uuid::Uuid::new_v4().to_string();
                    let id = uuid::Uuid::new_v4().to_string();
                    let binding = self.active_binding.clone().unwrap_or(self.binding());
                    if self.send(NativeCommand::Action {
                        source_id: source_id.clone(),
                        action: HumanAction::Submit {
                            id: id.clone(),
                            goal: body.clone(),
                        },
                    }) {
                        self.pending = Some(PendingSubmit {
                            source_id: source_id.clone(),
                            task_id: id.clone(),
                            body: body.clone(),
                            binding,
                        });
                        self.composer.sent(source_id, body);
                        self.selected = Some(id);
                        self.notice.clear();
                    }
                }
            }
            Message::Poll => {
                if let Some(connection) = &mut self.connection {
                    while let Ok(state) = connection.states.try_recv() {
                        if !state.paired && !state.connected && self.notice == "正在連線…" {
                            self.notice = "請到設定完成對話框配對。".into();
                        }
                        self.state = state;
                    }
                    if connection.is_finished() {
                        self.state.connected = false;
                        self.notice = "本機連線已停止。草稿與已保存的訊息仍保留。".into();
                    }
                }
                if self.state.connected
                    && matches!(
                        self.notice.as_str(),
                        "正在連線…" | "請到設定完成對話框配對。"
                    )
                {
                    self.notice.clear();
                }
                self.composer.observe(&self.state.messages);
                if let Some(pending) = &self.pending
                    && self.state.server == pending.binding[0]
                    && self
                        .state
                        .tasks
                        .iter()
                        .any(|task| task.id == pending.task_id)
                {
                    self.composer.confirm(&pending.source_id);
                }
                if !self.composer.waiting() {
                    self.pending = None;
                }
                let source = serde_json::to_string(&self.state.messages).unwrap_or_default();
                if source != self.last_render {
                    self.timeline = self
                        .state
                        .messages
                        .iter()
                        .map(|m| Bubble::new(&m.body))
                        .collect();
                    self.last_render = source;
                    return widget::operation::snap_to_end("pulse-timeline");
                }
            }
            Message::Server(value) => self.form.server = value,
            Message::DataDir(value) => self.form.data_dir = value,
            Message::Development(value) => self.form.dev_loopback = value,
            Message::Connect => {
                if pulse_link::client::Config::parse_chat(self.form.args()).is_err() {
                    self.notice = "連線設定無效。請核對伺服器位址。".into();
                    return Task::none();
                }
                let saved = self.form.save().is_ok();
                self.restart();
                if !saved {
                    self.notice.push_str(" 設定未保存。");
                }
            }
            Message::PairCode(value) => self.pair_code = value,
            Message::Pair => {
                if self.pair_code.trim().len() == 64
                    && self.send(NativeCommand::Pair {
                        code: self.pair_code.trim().into(),
                    })
                {
                    self.pair_code.clear();
                    self.notice = "正在配對…".into();
                }
            }
            Message::Pick(picker) => {
                if !self.picking {
                    self.picking = true;
                    return pick(picker);
                }
            }
            Message::Picked(picker, path) => {
                self.picking = false;
                if let Some(path) = path {
                    match picker {
                        Picker::Data => self.form.data_dir = path.to_string_lossy().into(),
                        Picker::Image => {
                            self.picking = true;
                            return Task::perform(
                                async move {
                                    let (tx, rx) = iced::futures::channel::oneshot::channel();
                                    std::thread::spawn(move || {
                                        let result = selected_image(&path.to_string_lossy())
                                            .map(|handle| Preview {
                                                handle,
                                                name: path
                                                    .file_name()
                                                    .unwrap_or_default()
                                                    .to_string_lossy()
                                                    .into(),
                                            })
                                            .map_err(|_| {
                                                "圖片未載入。請選擇較小的 PNG 或 JPEG 圖片。".into()
                                            });
                                        let _ = tx.send(result);
                                    });
                                    rx.await
                                        .unwrap_or_else(|_| Err("圖片未載入，請再試一次。".into()))
                                },
                                Message::ImageLoaded,
                            );
                        }
                    }
                }
            }
            Message::ImageLoaded(result) => {
                self.picking = false;
                match result {
                    Ok(preview) => {
                        self.image = Some(preview);
                        self.notice.clear();
                    }
                    Err(error) => self.notice = error,
                }
            }
            Message::RemoveImage => self.image = None,
            Message::Select(id) => {
                self.selected = Some(id);
                self.operations = false;
            }
            Message::Cancel => {
                if let Some(task) = self.selected_task() {
                    self.action(HumanAction::Cancel {
                        task_id: task.id.clone(),
                        revision: task.revision.to_string(),
                    });
                }
            }
            Message::Refresh => {
                self.action(HumanAction::Status);
            }
            Message::Readme | Message::Status => {
                if self.can_read()
                    && let Some(task) = self.selected_task()
                {
                    let operation = if matches!(message, Message::Readme) {
                        ReadOp::Text {
                            path: "README.md".into(),
                        }
                    } else {
                        ReadOp::GitStatus
                    };
                    self.action(HumanAction::Read {
                        task_id: task.id.clone(),
                        revision: task.revision.to_string(),
                        operation,
                    });
                }
            }
            Message::Kill => {
                self.send(NativeCommand::KillSwitch {
                    stopped: !self.state.stopped,
                });
            }
            Message::Restart => self.restart(),
            Message::Link(uri) => {
                if crate::safe_link(&uri.to_string()) {
                    #[cfg(target_os = "macos")]
                    {
                        let _ = Command::new("/usr/bin/open").arg("--").arg(&uri).spawn();
                    }
                } else {
                    self.notice = "此連結無法開啟。".into();
                }
            }
        }
        Task::none()
    }
    fn selected_task(&self) -> Option<&pulse_core::Task> {
        self.selected
            .as_ref()
            .and_then(|id| self.state.tasks.iter().find(|task| task.id == *id))
    }
    fn can_read(&self) -> bool {
        !self.state.stopped
            && self.selected_task().is_some_and(|task| {
                matches!(
                    task.state,
                    pulse_core::State::Queued
                        | pulse_core::State::Blocked
                        | pulse_core::State::WaitingDevice
                ) && !task.cancel_requested
                    && task.tool_calls < 16
            })
    }
    pub(crate) fn view(&self) -> Element<'_, Message> {
        match self.screen {
            Screen::Chat => self.chat(),
            Screen::Settings => self.settings(),
        }
    }
    fn chat(&self) -> Element<'_, Message> {
        let status = if self.state.connected {
            "已連線"
        } else if self.state.pending > 0 {
            "離線 · 有訊息待送"
        } else {
            "尚未連線"
        };
        let mut header = row![
            text("Pulse").size(26),
            widget::Space::new().width(Length::Fill),
            text(status).size(13)
        ]
        .spacing(16)
        .align_y(iced::Alignment::Center);
        if !self.state.tasks.is_empty() {
            header = header.push(
                button("進度")
                    .on_press(Message::Progress)
                    .style(button::text),
            );
        }
        header = header.push(
            button("設定")
                .on_press(Message::Screen(Screen::Settings))
                .style(button::text),
        );
        let history: Element<'_, Message> = if self.timeline.is_empty() {
            container(
                column![
                    text("從一句話開始。").size(26),
                    text("目前是連線預覽版，模型尚未接入。").size(14)
                ]
                .spacing(12),
            )
            .center_x(Length::Fill)
            .center_y(Length::Fill)
            .into()
        } else {
            scrollable(
                column(self.timeline.iter().map(Bubble::view))
                    .spacing(18)
                    .padding([12, 0]),
            )
            .id("pulse-timeline")
            .height(Length::Fill)
            .into()
        };
        let mut conversation = column![history].spacing(12).height(Length::Fill);
        if let Some(preview) = &self.image {
            conversation = conversation.push(
                container(
                    row![
                        widget::image(preview.handle.clone())
                            .width(120)
                            .height(80)
                            .content_fit(iced::ContentFit::Contain),
                        column![
                            text(&preview.name).size(13),
                            text("本機預覽，尚未上傳").size(12)
                        ]
                        .spacing(6),
                        widget::Space::new().width(Length::Fill),
                        button("移除")
                            .on_press_maybe((!self.picking).then_some(Message::RemoveImage))
                            .style(button::text)
                    ]
                    .spacing(12)
                    .align_y(iced::Alignment::Center),
                )
                .padding(10)
                .style(container::rounded_box),
            );
        }
        let editor = text_editor(&self.composer.content)
            .id("pulse-chat")
            .placeholder("傳訊息給 Pulse…")
            .height(104)
            .on_action(Message::Edit)
            .key_binding(editor_key_binding);
        conversation = conversation.push(
            container(
                column![
                    editor,
                    row![
                        button(if self.picking {
                            "選取中…"
                        } else {
                            "圖片"
                        })
                        .on_press_maybe((!self.picking).then_some(Message::Pick(Picker::Image)))
                        .style(button::text),
                        widget::Space::new().width(Length::Fill),
                        text("⌘ Enter 傳送").size(12),
                        button("傳送")
                            .on_press_maybe(self.composer.ready().map(|_| Message::Submit))
                    ]
                    .spacing(12)
                    .align_y(iced::Alignment::Center)
                ]
                .spacing(8),
            )
            .padding(12)
            .style(container::rounded_box),
        );
        if !self.notice.is_empty() {
            conversation = conversation.push(text(&self.notice).size(12));
        }
        if self.composer.text().len() > 4096 {
            conversation = conversation.push(text("訊息太長，請分段傳送。").size(12));
        }
        if !self.timeline.is_empty() {
            conversation = conversation.push(text("模型尚未接入，訊息會先保存。").size(12));
        }
        let body: Element<'_, Message> = if self.progress {
            row![container(self.progress()).width(260), conversation]
                .spacing(20)
                .height(Length::Fill)
                .into()
        } else {
            conversation.into()
        };
        container(
            container(column![header, body].spacing(20))
                .max_width(860)
                .height(Length::Fill),
        )
        .padding(24)
        .center_x(Length::Fill)
        .height(Length::Fill)
        .into()
    }
    fn progress(&self) -> Element<'_, Message> {
        let mut list = column![row![
            text("進度").size(20),
            widget::Space::new().width(Length::Fill),
            button("收起")
                .on_press(Message::Progress)
                .style(button::text)
        ]]
        .spacing(10);
        for task in &self.state.tasks {
            let title = task
                .goal
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(50)
                .collect::<String>();
            list = list.push(
                button(
                    column![text(title).size(14), text(task_status(task.state)).size(12)]
                        .spacing(4),
                )
                .width(Length::Fill)
                .on_press(Message::Select(task.id.clone())),
            );
        }
        if let Some(task) = self.selected_task() {
            list = list
                .push(button("取消這項工作").on_press_maybe(
                    (!task.state.terminal() && !task.cancel_requested).then_some(Message::Cancel),
                ))
                .push(
                    button("更多操作")
                        .on_press(Message::Operations)
                        .style(button::text),
                );
            if self.operations {
                list = list
                    .push(
                        button("讀取專案說明")
                            .on_press_maybe(self.can_read().then_some(Message::Readme)),
                    )
                    .push(
                        button("查看檔案變更")
                            .on_press_maybe(self.can_read().then_some(Message::Status)),
                    )
                    .push(text("工具只讀，不會修改檔案。").size(12));
            }
        }
        list = list
            .push(
                button(if self.state.stopped {
                    "恢復本機工具"
                } else {
                    "暫停本機工具"
                })
                .on_press(Message::Kill),
            )
            .push(
                button("更新進度")
                    .on_press(Message::Refresh)
                    .style(button::text),
            );
        scrollable(list).height(Length::Fill).into()
    }
    fn settings(&self) -> Element<'_, Message> {
        let mut body = column![
            row![
                text("設定").size(26),
                widget::Space::new().width(Length::Fill),
                button("返回對話")
                    .on_press(Message::Screen(Screen::Chat))
                    .style(button::text)
            ],
            text("連線").size(20),
            text_input("HTTPS 伺服器位址", &self.form.server)
                .id("pulse-server")
                .on_input(Message::Server)
                .on_submit(Message::Connect),
            button("儲存並連線").on_press(Message::Connect),
            text("對話框配對").size(20),
            text(if self.state.paired {
                "本機已有配對資料。重新配對前，須先在伺服器撤銷舊身分。"
            } else {
                "輸入伺服器提供的一次性配對碼。"
            })
            .size(13),
            row![
                text_input("一次性配對碼", &self.pair_code)
                    .secure(true)
                    .on_input(Message::PairCode)
                    .on_submit(Message::Pair),
                button("配對").on_press(Message::Pair)
            ]
            .spacing(12),
            button("進階設定")
                .on_press(Message::Advanced)
                .style(button::text)
        ]
        .spacing(18);
        if self.advanced {
            body = body
                .push(text("本機資料只允許你自己存取。更換服務時，請選另一個私有資料夾。").size(13))
                .push(
                    row![
                        text_input("私有資料目錄", &self.form.data_dir).on_input(Message::DataDir),
                        button("選擇")
                            .on_press_maybe((!self.picking).then_some(Message::Pick(Picker::Data)))
                    ]
                    .spacing(8),
                )
                .push(
                    widget::checkbox(self.form.dev_loopback)
                        .label("只限本機 HTTP 測試")
                        .on_toggle(Message::Development),
                )
                .push(
                    button(if self.state.stopped {
                        "恢復本機工具"
                    } else {
                        "暫停本機工具"
                    })
                    .on_press(Message::Kill),
                )
                .push(button("重新連線").on_press(Message::Restart))
                .push(
                    text(format!(
                        "待送：{}\n快取來源：{}\n本機 executor：{}\n{}",
                        self.state.pending,
                        self.state.server,
                        if self.state.local_connected {
                            "已連線"
                        } else {
                            "離線"
                        },
                        self.state.message
                    ))
                    .size(12),
                );
        }
        if !self.notice.is_empty() {
            body = body.push(text(&self.notice).size(13));
        }
        container(
            container(scrollable(body))
                .max_width(640)
                .height(Length::Fill),
        )
        .padding(24)
        .center_x(Length::Fill)
        .height(Length::Fill)
        .into()
    }
}

fn editor_key_binding(press: text_editor::KeyPress) -> Option<text_editor::Binding<Message>> {
    use iced::keyboard::{Key, key::Named};
    if press.modifiers.command() && matches!(press.status, text_editor::Status::Focused { .. }) {
        let command = if press.key.as_ref() == Key::Named(Named::Enter) {
            Some(Message::Submit)
        } else {
            match press
                .key
                .to_latin(press.physical_key)
                .map(|c| c.to_ascii_lowercase())
            {
                Some('i') => Some(Message::Pick(Picker::Image)),
                Some('l') => Some(Message::Screen(Screen::Settings)),
                Some('n') => Some(Message::Screen(Screen::Chat)),
                Some('q') => Some(Message::Quit),
                _ => None,
            }
        };
        if let Some(command) = command {
            return Some(text_editor::Binding::Custom(command));
        }
    }
    text_editor::Binding::from_key_press(press)
}
fn task_status(state: pulse_core::State) -> &'static str {
    use pulse_core::State::*;
    match state {
        Succeeded => "已完成",
        Failed => "未完成",
        Cancelled => "已取消",
        OutcomeUnknown | Reconciling => "需要核對",
        Running | Dispatched | Planning => "處理中",
        Blocked | WaitingApproval | WaitingDevice => "等待下一步",
        CancelRequested => "正在取消",
        _ => "等待處理",
    }
}
fn pick(picker: Picker) -> Task<Message> {
    #[cfg(target_os = "macos")]
    {
        Task::perform(
            async move {
                let dialog = rfd::AsyncFileDialog::new();
                let selected = match picker {
                    Picker::Image => {
                        dialog
                            .set_title("選擇本機圖片")
                            .add_filter("圖片", &["png", "jpg", "jpeg"])
                            .pick_file()
                            .await
                    }
                    Picker::Data => dialog.set_title("選擇私有資料夾").pick_folder().await,
                };
                selected.map(|file| file.path().to_path_buf())
            },
            move |path| Message::Picked(picker, path),
        )
    }
    #[cfg(not(target_os = "macos"))]
    {
        Task::done(Message::Picked(picker, None))
    }
}

#[cfg(test)]
#[path = "tests/app.rs"]
mod tests;
