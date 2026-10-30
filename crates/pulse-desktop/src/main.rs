mod app;
mod composer;
mod connection;
#[cfg(test)]
use app::ConnectionForm;
use app::{App, Message};
use connection::ChatConnection;

use iced::{
    Element, Subscription, Theme,
    widget::{self, column, container, markdown, row, scrollable, text},
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
