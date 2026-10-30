mod app;
mod composer;
#[cfg(test)]
use app::ConnectionForm;
use app::{App, Message};

use iced::{
    Element, Subscription, Theme,
    widget::{self, column, container, markdown, row, scrollable, text},
};
use pulse_link::protocol::{NativeCommand, NativeState};
use std::{
    io::{BufRead, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
};

fn main() -> iced::Result {
    iced::application(
        || (App::new(), widget::operation::focus("pulse-chat")),
        App::update,
        App::view,
    )
    .title("Pulse")
    .window_size((900.0, 740.0))
    .theme(Theme::TokyoNight)
    .subscription(|_| {
        Subscription::batch([
            iced::time::every(std::time::Duration::from_millis(100)).map(|_| Message::Poll),
            iced::keyboard::listen().map(Message::Key),
        ])
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
#[path = "tests/rendering.rs"]
mod tests;
