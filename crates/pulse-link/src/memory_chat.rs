//! Explicit Human memory controls, separate from task/model history.
use crate::{Result, ensure};
use pulse_core::MemoryCommand;

pub mod client;
pub mod journal;
pub mod server;
#[cfg(test)]
mod tests;

pub enum Intent {
    Query,
    Remember { label: String, body: String },
    Correct { label: String, body: String },
    Forget { label: String },
    Clarify,
}

/// Reserved prefixes must never fall through into a model task.
pub fn parse(text: &str) -> Option<Intent> {
    let text = text.trim();
    let text = text.strip_prefix("請").unwrap_or(text).trim();
    if matches!(
        text.trim_end_matches(['。', '？', '?']),
        "查詢記憶" | "我的記憶" | "列出記憶"
    ) {
        return Some(Intent::Query);
    }
    if ["查詢記憶", "我的記憶", "列出記憶"]
        .iter()
        .any(|prefix| text.starts_with(prefix))
    {
        return Some(Intent::Clarify);
    }
    for (prefix, correction) in [("記住", false), ("更正記憶", true), ("修正記憶", true)]
    {
        if let Some(rest) = text.strip_prefix(prefix) {
            let rest = rest.trim();
            let parts = if rest.starts_with('"') {
                quoted_name(rest).and_then(|(name, tail)| {
                    tail.trim_start()
                        .strip_prefix(['：', ':'])
                        .map(|body| (name, body.to_owned()))
                })
            } else {
                rest.split_once(['：', ':'])
                    .map(|(name, body)| (name.trim().to_owned(), body.to_owned()))
            };
            let Some((label, body)) = parts else {
                return Some(Intent::Clarify);
            };
            let body = body.trim().to_owned();
            return Some(if label.is_empty() || body.is_empty() {
                Intent::Clarify
            } else if correction {
                Intent::Correct { label, body }
            } else {
                Intent::Remember { label, body }
            });
        }
    }
    if let Some(label) = text
        .strip_prefix("忘記記憶")
        .or_else(|| text.strip_prefix("刪除記憶"))
    {
        let rest = label.trim().trim_start_matches(['：', ':']).trim();
        let label = if rest.starts_with('"') {
            let Some((name, tail)) = quoted_name(rest) else {
                return Some(Intent::Clarify);
            };
            if !tail.trim().is_empty() {
                return Some(Intent::Clarify);
            }
            name
        } else {
            rest.to_owned()
        };
        return Some(if label.is_empty() {
            Intent::Clarify
        } else {
            Intent::Forget { label }
        });
    }
    None
}

fn quoted_name(text: &str) -> Option<(String, &str)> {
    let mut stream = serde_json::Deserializer::from_str(text).into_iter::<String>();
    let name = stream.next()?.ok()?;
    Some((name, &text[stream.byte_offset()..]))
}

fn validate(command: &MemoryCommand) -> Result<()> {
    match command {
        MemoryCommand::Remember { label, body } | MemoryCommand::Correct { label, body, .. } => {
            ensure(
                !label.is_empty() && label.len() <= 128 && !label.chars().any(char::is_control),
                "memory label limit",
            )?;
            ensure(
                !body.is_empty()
                    && body.len() <= 1024
                    && !body
                        .chars()
                        .any(|c| c.is_control() && c != '\n' && c != '\t'),
                "memory content limit",
            )?;
        }
        MemoryCommand::Forget { .. } => {}
    }
    Ok(())
}
