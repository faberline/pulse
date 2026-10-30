use iced::{
    Element, Length, Theme,
    widget::{self, button, column, container, markdown, row, scrollable, text, text_input},
};
use pulse_link::protocol::{HumanAction, NativeCommand, NativeState, ReadOp};
use std::{
    io::{BufRead, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
};

fn main() -> iced::Result {
    iced::application(App::new, App::update, App::view)
        .title("Pulse — desktop preview")
        .window_size((1120.0, 800.0))
        .theme(Theme::TokyoNight)
        .subscription(|_| {
            iced::time::every(std::time::Duration::from_millis(100)).map(|_| Message::Poll)
        })
        .run()
}
struct Helper {
    child: Child,
    commands: mpsc::SyncSender<NativeCommand>,
    states: mpsc::Receiver<NativeState>,
}
impl Drop for Helper {
    fn drop(&mut self) {
        let _ = self.commands.try_send(NativeCommand::Shutdown);
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Helper {
    fn start(args: &[String]) -> std::io::Result<Self> {
        let path = std::env::current_exe()?.with_file_name("pulse-device");
        let mut child = Command::new(path)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let mut input = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("missing helper input"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("missing helper output"))?;
        let (commands, rx) = mpsc::sync_channel(32);
        let (tx, states) = mpsc::sync_channel(8);
        std::thread::spawn(move || {
            while let Ok(command) = rx.recv() {
                let Ok(mut bytes) = serde_json::to_vec(&command) else {
                    break;
                };
                bytes.push(b'\n');
                if input.write_all(&bytes).and_then(|_| input.flush()).is_err() {
                    break;
                }
            }
        });
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(output);
            loop {
                let mut bytes = Vec::new();
                loop {
                    let Ok(chunk) = reader.fill_buf() else {
                        return;
                    };
                    if chunk.is_empty() {
                        return;
                    }
                    let end = chunk
                        .iter()
                        .position(|b| *b == b'\n')
                        .map(|n| n + 1)
                        .unwrap_or(chunk.len());
                    if bytes.len() + end > pulse_link::protocol::MAX_IPC {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..end]);
                    reader.consume(end);
                    if bytes.ends_with(b"\n") {
                        break;
                    }
                }
                let Ok(state) = serde_json::from_slice::<NativeState>(&bytes) else {
                    return;
                };
                if tx.send(state).is_err() {
                    return;
                }
            }
        });
        Ok(Self {
            child,
            commands,
            states,
        })
    }
}
#[derive(Debug, Clone)]
enum Message {
    Poll,
    Goal(String),
    Submit,
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
    ImagePath(String),
    Image,
    Server(String),
    Repo(String),
    DataDir(String),
    Development(bool),
    Connect,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionForm {
    server: String,
    repo: String,
    data_dir: String,
    dev_loopback: bool,
}
impl Default for ConnectionForm {
    fn default() -> Self {
        Self {
            server: String::new(),
            repo: String::new(),
            data_dir: pulse_link::journal::default_data_dir()
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
            "--repo".into(),
            self.repo.clone(),
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
            if let Ok(config) = pulse_link::client::Config::parse(args.clone()) {
                return Self {
                    server: config.server.to_string(),
                    repo: config.repo.to_string_lossy().into(),
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
struct App {
    helper: Option<Helper>,
    state: NativeState,
    goal: String,
    pair_code: String,
    selected: Option<String>,
    notice: String,
    timeline: Vec<RichBlock>,
    image_path: String,
    image: Option<widget::image::Handle>,
    last_render: String,
    awaiting: Option<(String, String)>,
    form: ConnectionForm,
}
impl App {
    fn new() -> Self {
        let mut app = Self {
            helper: None,
            state: NativeState::default(),
            goal: String::new(),
            pair_code: String::new(),
            selected: None,
            notice: String::new(),
            timeline: vec![],
            image_path: String::new(),
            image: None,
            last_render: String::new(),
            awaiting: None,
            form: ConnectionForm::load(),
        };
        app.restart();
        app
    }
    fn restart(&mut self) {
        self.helper = None;
        self.state.connected = false;
        if self.form.server.is_empty() || self.form.repo.is_empty() {
            self.notice = "請填入伺服器與 Pulse 專案路徑，再按「連線」。".into();
            return;
        }
        match Helper::start(&self.form.args()) {
            Ok(helper) => {
                self.helper = Some(helper);
                self.notice = "Helper 已啟動。等待狀態。".into();
            }
            Err(_) => self.notice = "Helper 無法啟動。請檢查啟動參數與程式套件。".into(),
        }
    }
    fn send(&mut self, command: NativeCommand) -> bool {
        if self
            .helper
            .as_ref()
            .is_some_and(|helper| helper.commands.try_send(command).is_ok())
        {
            true
        } else {
            self.notice = "Helper 不可用或輸入佇列已滿。請保留輸入並重啟 helper。".into();
            false
        }
    }
    fn action(&mut self, action: HumanAction) -> bool {
        self.send(NativeCommand::Action {
            source_id: uuid::Uuid::new_v4().to_string(),
            action,
        })
    }
    fn update(&mut self, message: Message) {
        match message {
            Message::Server(value) => self.form.server = value,
            Message::Repo(value) => self.form.repo = value,
            Message::DataDir(value) => self.form.data_dir = value,
            Message::Development(value) => self.form.dev_loopback = value,
            Message::Connect => {
                if pulse_link::client::Config::parse(self.form.args()).is_err() {
                    self.notice = "設定無效。請核對 HTTPS 來源與本機專案路徑。".into();
                    return;
                }
                let saved = self.form.save().is_ok();
                self.restart();
                if !saved {
                    self.notice.push_str(" 連線設定未保存。");
                }
            }
            Message::Poll => {
                if let Some(helper) = &mut self.helper {
                    while let Ok(state) = helper.states.try_recv() {
                        self.state = state;
                    }
                    if helper.child.try_wait().ok().flatten().is_some() {
                        self.state.connected = false;
                        self.notice = "Helper 已停止。快取仍可讀。請重啟 helper。".into();
                    }
                }
                if let Some((id, submitted)) = &self.awaiting
                    && self.state.messages.iter().any(|m| m.id == *id)
                {
                    if self.goal == *submitted {
                        self.goal.clear();
                    }
                    self.awaiting = None;
                }
                let source = self
                    .state
                    .messages
                    .iter()
                    .map(|m| m.body.as_str())
                    .collect::<Vec<_>>()
                    .join("\n\n---\n\n");
                if source != self.last_render {
                    self.timeline = rich_content(&source);
                    self.last_render = source;
                }
            }
            Message::Goal(value) => self.goal = value,
            Message::Submit => {
                if !self.goal.trim().is_empty() && self.goal.len() <= 4096 {
                    let source_id = uuid::Uuid::new_v4().to_string();
                    if self.send(NativeCommand::Action {
                        source_id: source_id.clone(),
                        action: HumanAction::Submit {
                            id: uuid::Uuid::new_v4().to_string(),
                            goal: self.goal.clone(),
                        },
                    }) {
                        self.awaiting = Some((source_id, self.goal.clone()));
                    }
                }
            }
            Message::PairCode(value) => self.pair_code = value,
            Message::Pair => {
                if self.pair_code.len() == 64
                    && self.send(NativeCommand::Pair {
                        code: self.pair_code.trim().into(),
                    })
                {
                    self.pair_code.clear();
                    self.notice = "正在配對。".into();
                }
            }
            Message::Select(id) => self.selected = Some(id),
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
                if let Some(task) = self.selected_task() {
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
                if safe_link(&uri.to_string()) {
                    #[cfg(target_os = "macos")]
                    {
                        let _ = Command::new("/usr/bin/open").arg("--").arg(&uri).spawn();
                    }
                    self.notice = "已要求開啟 HTTPS 連結。".into();
                } else {
                    self.notice = "連結已拒絕。只支援沒有登入資料的 HTTPS 連結。".into();
                }
            }
            Message::ImagePath(value) => self.image_path = value,
            Message::Image => match selected_image(&self.image_path) {
                Ok(image) => {
                    self.image = Some(image);
                    self.notice = "已顯示你選定的本機圖片。".into();
                }
                Err(_) => {
                    self.notice =
                        "圖片未載入。只支援最多 5 MiB、4096×4096 的本機 PNG/JPEG 檔。".into()
                }
            },
        }
    }
    fn selected_task(&self) -> Option<&pulse_core::Task> {
        self.selected
            .as_ref()
            .and_then(|id| self.state.tasks.iter().find(|t| t.id == *id))
    }
    fn view(&self) -> Element<'_, Message> {
        let connection = if self.state.connected {
            "已連線"
        } else {
            "離線／快取"
        };
        let header = row![
            text("Pulse").size(28),
            text(format!(
                "{connection} · 待送 {} · 同步 {}",
                self.state.pending,
                self.state
                    .synced_at
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "尚未同步".into())
            )),
            button("更新狀態").on_press(Message::Refresh),
            button(if self.state.stopped {
                "啟用本機只讀工具"
            } else {
                "停止本機工具"
            })
            .on_press(Message::Kill),
            button("重啟 helper").on_press(Message::Restart)
        ]
        .spacing(14);
        let mut tasks = column![text("任務").size(22)].spacing(8);
        for task in &self.state.tasks {
            tasks = tasks.push(
                button(column![
                    text(task.goal.chars().take(42).collect::<String>()),
                    text(format!("{:?} · r{}", task.state, task.revision)).size(12)
                ])
                .on_press(Message::Select(task.id.clone()))
                .width(Length::Fill),
            );
        }
        if let Some(task) = self.selected_task() {
            tasks = tasks.push(
                text(format!(
                    "選取：{}\n{}",
                    task.id,
                    task.reason.as_deref().unwrap_or("等待處理")
                ))
                .size(12),
            );
        }
        let applicable = self
            .selected_task()
            .is_some_and(|task| !task.state.terminal() && !task.cancel_requested);
        tasks = tasks
            .push(button("取消所選任務").on_press_maybe(applicable.then_some(Message::Cancel)))
            .push(
                button("讀取 README.md")
                    .on_press_maybe((applicable && !self.state.stopped).then_some(Message::Readme)),
            )
            .push(
                button("讀取 Git 狀態")
                    .on_press_maybe((applicable && !self.state.stopped).then_some(Message::Status)),
            )
            .push(
                text("目前沒有寫入操作可批准。\n模型尚未接入。工具結果不代表任務成功。").size(13),
            );
        let conversation = scrollable(
            container(column(self.timeline.iter().map(RichBlock::view)).spacing(12))
                .width(Length::Fill)
                .padding(16),
        )
        .height(Length::Fill);
        let mut body = column![conversation].spacing(10);
        body = body.push(
            row![
                text_input(
                    if self.state.paired {
                        "重新配對碼（先在伺服器撤銷舊身分）"
                    } else {
                        "一次性配對碼"
                    },
                    &self.pair_code
                )
                .secure(true)
                .on_input(Message::PairCode),
                button("配對").on_press(Message::Pair)
            ]
            .spacing(8),
        );
        body = body
            .push(
                row![
                    text_input("新增任務（離線時存入待送佇列）", &self.goal)
                        .on_input(Message::Goal)
                        .on_submit(Message::Submit),
                    button("送出").on_press(Message::Submit)
                ]
                .spacing(8),
            )
            .push(
                row![
                    text_input("你選定的本機 PNG/JPEG 完整路徑", &self.image_path)
                        .on_input(Message::ImagePath),
                    button("顯示圖片").on_press(Message::Image)
                ]
                .spacing(8),
            );
        if let Some(image) = &self.image {
            body = body.push(
                widget::image(image.clone())
                    .width(240)
                    .height(140)
                    .content_fit(iced::ContentFit::Contain),
            );
        }
        container(
            column![
                header,
                row![
                    text_input("HTTPS 伺服器來源", &self.form.server).on_input(Message::Server),
                    text_input("Pulse 專案完整路徑", &self.form.repo).on_input(Message::Repo),
                    button("連線").on_press(Message::Connect)
                ]
                .spacing(8),
                row![
                    text_input("本機私有資料目錄", &self.form.data_dir).on_input(Message::DataDir),
                    widget::checkbox(self.form.dev_loopback)
                        .label("只限本機 HTTP 測試")
                        .on_toggle(Message::Development)
                ]
                .spacing(8),
                text(&self.state.message).size(13),
                text(format!(
                    "快取來源：{}",
                    if self.state.server.is_empty() {
                        "尚未取得"
                    } else {
                        &self.state.server
                    }
                ))
                .size(12),
                row![scrollable(tasks).width(290), body.width(Length::Fill)]
                    .spacing(16)
                    .height(Length::Fill),
                text(&self.notice).size(13)
            ]
            .spacing(12),
        )
        .padding(20)
        .into()
    }
}
enum RichBlock {
    Markdown(Vec<markdown::Item>),
    Table {
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}
impl RichBlock {
    fn view(&self) -> Element<'_, Message> {
        match self {
            Self::Markdown(items) => markdown::view(items, markdown_settings()).map(Message::Link),
            Self::Table { headers, rows } => {
                // A fixed cell width keeps the horizontal scroll content finite.
                let mut table = column![table_line(headers)].spacing(1);
                for cells in rows.iter().take(128) {
                    table = table.push(table_line(cells));
                }
                if headers.len() > 16 || rows.len() > 128 {
                    table = table.push(text("表格顯示已截斷。"));
                }
                scrollable(table)
                    .direction(scrollable::Direction::Horizontal(
                        scrollable::Scrollbar::default(),
                    ))
                    .into()
            }
        }
    }
}
fn markdown_settings() -> markdown::Settings {
    let mut settings = markdown::Settings::from(Theme::TokyoNight);
    // macOS's generic monospace fallback can produce invalid CJK glyph offsets.
    settings.style.inline_code_font = iced::Font::DEFAULT;
    settings.style.code_block_font = iced::Font::DEFAULT;
    settings
}
fn table_line(cells: &[String]) -> widget::Row<'_, Message> {
    row(cells
        .iter()
        .take(16)
        .map(|cell| container(text(cell)).padding(8).width(180).into()))
    .spacing(1)
}
fn rich_content(source: &str) -> Vec<RichBlock> {
    use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
    let mut result = Vec::new();
    let mut last = 0;
    let mut table = false;
    let mut headers = Vec::new();
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut cell = String::new();
    for (event, range) in Parser::new_ext(source, Options::ENABLE_TABLES).into_offset_iter() {
        match event {
            Event::Start(Tag::Table(_)) => {
                if range.start > last {
                    result.push(RichBlock::Markdown(
                        markdown::parse(&source[last..range.start]).collect(),
                    ));
                }
                table = true;
                headers.clear();
                rows.clear();
            }
            Event::Start(Tag::TableHead | Tag::TableRow) if table => row.clear(),
            Event::Start(Tag::TableCell) if table => cell.clear(),
            Event::Text(value) | Event::Code(value) if table => cell.push_str(&value),
            Event::SoftBreak | Event::HardBreak if table => cell.push(' '),
            Event::End(TagEnd::TableCell) if table => row.push(cell.clone()),
            Event::End(TagEnd::TableHead) if table => headers = row.clone(),
            Event::End(TagEnd::TableRow) if table => rows.push(row.clone()),
            Event::End(TagEnd::Table) if table => {
                result.push(RichBlock::Table {
                    headers: headers.clone(),
                    rows: rows.clone(),
                });
                last = range.end;
                table = false;
            }
            _ => {}
        }
    }
    if last < source.len() {
        result.push(RichBlock::Markdown(
            markdown::parse(&source[last..]).collect(),
        ));
    }
    result
}
fn safe_link(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
    })
}
fn selected_image(path: &str) -> Result<widget::image::Handle, Box<dyn std::error::Error>> {
    use std::{io::Read, os::unix::fs::OpenOptionsExt};
    if !std::path::Path::new(path).is_absolute() {
        return Err("absolute image path required".into());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc_flags())
        .open(path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() > 5 * 1024 * 1024 {
        return Err("image size/type".into());
    }
    let mut bytes = Vec::new();
    file.take(5 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 5 * 1024 * 1024 {
        return Err("image size".into());
    }
    let mut reader =
        image_codec::ImageReader::new(std::io::Cursor::new(bytes)).with_guessed_format()?;
    if !matches!(
        reader.format(),
        Some(image_codec::ImageFormat::Png | image_codec::ImageFormat::Jpeg)
    ) {
        return Err("image format".into());
    }
    let mut limits = image_codec::Limits::default();
    limits.max_image_width = Some(4096);
    limits.max_image_height = Some(4096);
    limits.max_alloc = Some(64 * 1024 * 1024);
    reader.limits(limits);
    let image = reader.decode()?.to_rgba8();
    Ok(widget::image::Handle::from_rgba(
        image.width(),
        image.height(),
        image.into_raw(),
    ))
}
fn libc_flags() -> i32 {
    libc::O_NOFOLLOW | libc::O_NONBLOCK
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_images_are_local_bounded_and_do_not_follow_links() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("image.png");
        image_codec::DynamicImage::new_rgba8(2, 2)
            .save_with_format(&path, image_codec::ImageFormat::Png)
            .unwrap();
        assert!(selected_image(path.to_str().unwrap()).is_ok());
        let link = dir.path().join("link.png");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(selected_image(link.to_str().unwrap()).is_err());
        assert!(selected_image("https://example.com/tracker.png").is_err());
        image_codec::DynamicImage::new_rgba8(4097, 1)
            .save_with_format(&path, image_codec::ImageFormat::Png)
            .unwrap();
        assert!(selected_image(path.to_str().unwrap()).is_err());
        std::fs::write(&path, b"GIF87a\0\0\0\0").unwrap();
        assert!(selected_image(path.to_str().unwrap()).is_err());
    }
    #[test]
    fn desktop_settings_never_accept_a_credential_field() {
        assert!(serde_json::from_value::<ConnectionForm>(serde_json::json!({"server":"https://example.com/","repo":"/tmp/pulse","data_dir":"/tmp/private","dev_loopback":false,"token":"unexpected"})).is_err());
    }
    #[test]
    fn native_table_avoids_infinite_width_markdown_table() {
        let blocks = rich_content("before\n\n| 欄位 | 值 |\n|---|---|\n| A | `B` |\n\nafter");
        assert_eq!(blocks.len(), 3);
        let RichBlock::Table { headers, rows } = &blocks[1] else {
            panic!("native table missing")
        };
        assert_eq!(headers, &["欄位", "值"]);
        assert_eq!(rows, &[vec!["A", "B"]]);
        for block in blocks {
            if let RichBlock::Markdown(items) = block {
                assert!(
                    !items
                        .iter()
                        .any(|item| matches!(item, markdown::Item::Table { .. }))
                );
            }
        }
    }
    #[test]
    fn links_require_explicit_safe_https() {
        for url in [
            "javascript:alert(1)",
            "file:///etc/passwd",
            "data:text/plain,x",
            "pulse://run",
            "https://user:pass@example.com",
        ] {
            assert!(!safe_link(url));
        }
        assert!(safe_link("https://example.com/artifact/1"));
    }
    #[test]
    fn markdown_supports_native_code_table_and_image() {
        let items:Vec<_>=markdown::parse("## 中文\n\n```rust\nlet x = 1;\n```\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n![image](https://example.com/image.png)\n\n<script>alert(1)</script>").collect();
        assert!(
            items
                .iter()
                .any(|i| matches!(i, markdown::Item::CodeBlock { .. }))
        );
        assert!(
            items
                .iter()
                .any(|i| matches!(i, markdown::Item::Table { .. }))
        );
        assert!(
            items
                .iter()
                .any(|i| matches!(i, markdown::Item::Image { .. }))
        );
    }
}
