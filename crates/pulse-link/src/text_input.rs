//! Explicit Human-selected immutable text. Paths never enter a task or provider.
use crate::{Result, ensure, journal::Journal, protocol::*};
use serde::{Deserialize, Serialize};
use std::{
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Component, Path},
};

pub const MAX_TEXT_INPUT: usize = 32 * 1024;
pub const INPUT_CONTEXT_LIMIT: usize = 48 * 1024;
pub(crate) const KEY: &str = "selected_text_input_v1";
const TTL: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TextInput {
    pub id: String,
    pub name: String,
    pub digest: String,
    pub length: u32,
    pub captured_at: i64,
    pub text: String,
}
impl std::fmt::Debug for TextInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextInput")
            .field("reference", &self.reference())
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TextInputRef {
    pub id: String,
    pub name: String,
    pub digest: String,
    pub length: u32,
    pub captured_at: i64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    identity: String,
    input: TextInput,
}
fn allowed_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.starts_with('.')
        && !name.chars().any(|c| c.is_control() || "/\\".contains(c))
        && (matches!(name, "Cargo.lock" | "README" | "LICENSE" | "NOTICE")
            || Path::new(name)
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|s| {
                    matches!(
                        s.to_ascii_lowercase().as_str(),
                        "txt" | "md" | "rs" | "toml" | "json" | "yaml" | "yml" | "csv"
                    )
                }))
}
pub(crate) fn private_path(path: &Path) -> bool {
    path.components().any(|part|matches!(part,Component::Normal(value) if matches!(value.to_string_lossy().to_ascii_lowercase().as_str(),".git"|".ssh"|".aws"|".codex"|".agents")))
}
impl TextInput {
    pub fn reference(&self) -> TextInputRef {
        TextInputRef {
            id: self.id.clone(),
            name: self.name.clone(),
            digest: self.digest.clone(),
            length: self.length,
            captured_at: self.captured_at,
        }
    }
    pub fn scope(&self) -> String {
        format!("input:{}", self.digest)
    }
    pub fn validate(&self) -> Result<()> {
        ensure(
            uuid::Uuid::parse_str(&self.id).is_ok()
                && allowed_name(&self.name)
                && self.captured_at > 0
                && !self.text.trim().is_empty()
                && self.text.len() <= MAX_TEXT_INPUT
                && self.length as usize == self.text.len()
                && !self
                    .text
                    .chars()
                    .any(|c| c.is_control() && !"\n\r\t".contains(c))
                && crate::artifact::digest_id(&self.digest)
                && hash_bytes(self.text.as_bytes()) == self.digest,
            "invalid selected text input",
        )?;
        Ok(())
    }
    pub fn fresh(&self, time: i64) -> bool {
        self.captured_at <= time && time.saturating_sub(self.captured_at) <= TTL
    }
    pub fn validate_goal(&self, goal: &str) -> Result<()> {
        self.validate()?;
        ensure(
            !goal.trim().is_empty()
                && goal.len() <= 4096
                && crate::memory_chat::parse(goal).is_none()
                && !crate::schedule_chat::reserved(goal),
            "file input needs an ordinary task goal",
        )?;
        // The provider encodes a JSON prompt inside a message. Reserve space for
        // two copies of the goal and fixed decision/source metadata.
        let input = serde_json::to_vec(&serde_json::to_string(self)?)?.len();
        let goal = serde_json::to_vec(&serde_json::to_string(goal)?)?.len();
        ensure(
            input + goal * 2 + 4096 <= INPUT_CONTEXT_LIMIT,
            "selected input context too large",
        )
    }
    pub(crate) fn read(path: &Path) -> Result<Self> {
        use rustix::fs::{Mode, OFlags, open, openat};
        ensure(
            path.is_absolute() && path.as_os_str().as_encoded_bytes().len() <= 4096,
            "selected file must be absolute and bounded",
        )?;
        let parts: Vec<_> = path.components().collect();
        ensure(
            parts
                .iter()
                .all(|c| matches!(c, Component::RootDir | Component::Normal(_))),
            "selected file path",
        )?;
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| crate::Error::Rejected("file name".into()))?;
        ensure(allowed_name(name), "unsupported text file")?;
        ensure(!private_path(path), "private input path")?;
        // A Human selected this exact file, not a directory grant. Resolve parent
        // aliases (including macOS /tmp) once, then walk that path without links.
        // The final selected file itself must never be a symlink.
        let resolved = path
            .parent()
            .ok_or_else(|| crate::Error::Rejected("file parent".into()))?
            .canonicalize()?
            .join(name);
        ensure(!private_path(&resolved), "private input path")?;
        let parts: Vec<_> = resolved.components().collect();
        let mut directory = open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        for part in &parts[1..parts.len() - 1] {
            let Component::Normal(part) = part else {
                unreachable!()
            };
            ensure(
                !matches!(
                    part.to_string_lossy().to_ascii_lowercase().as_str(),
                    ".git" | ".ssh" | ".aws" | ".codex" | ".agents"
                ),
                "private input path",
            )?;
            directory = openat(
                &directory,
                *part,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
        }
        let fd = openat(
            &directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let file = std::fs::File::from(fd);
        let before = file.metadata()?;
        ensure(
            before.is_file() && before.nlink() == 1 && before.len() <= MAX_TEXT_INPUT as u64,
            "selected file type or size",
        )?;
        let mut bytes = Vec::new();
        (&file)
            .take(MAX_TEXT_INPUT as u64 + 1)
            .read_to_end(&mut bytes)?;
        let after = file.metadata()?;
        ensure(
            (
                before.dev(),
                before.ino(),
                before.len(),
                before.mtime(),
                before.mtime_nsec(),
                before.ctime(),
                before.ctime_nsec(),
            ) == (
                after.dev(),
                after.ino(),
                after.len(),
                after.mtime(),
                after.mtime_nsec(),
                after.ctime(),
                after.ctime_nsec(),
            ) && bytes.len() == after.len() as usize,
            "selected file changed during read",
        )?;
        let input = Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            digest: hash_bytes(&bytes),
            length: bytes.len() as u32,
            captured_at: crate::now(),
            text: String::from_utf8(bytes)
                .map_err(|_| crate::Error::Rejected("selected file is not UTF8".into()))?,
        };
        input.validate()?;
        Ok(input)
    }
    pub(crate) fn source_label(&self) -> String {
        let mut name = String::new();
        for c in self.name.chars() {
            if "\\`*_{}[]<>()#+-.!|".contains(c) {
                name.push('\\');
            }
            name.push(c);
        }
        let date = chrono::DateTime::from_timestamp_millis(self.captured_at)
            .map(|t| t.to_rfc3339())
            .unwrap_or_default();
        format!("來源檔案：{name} · 選取於 {date}")
    }
}
impl Journal {
    pub(crate) async fn submit_text_input(
        &self,
        source: &str,
        id: String,
        goal: String,
        input_id: &str,
    ) -> Result<()> {
        let identity = self.text_input_identity()?;
        let input = self
            .selected_text_input()
            .await?
            .filter(|input| input.id == input_id)
            .ok_or_else(|| crate::Error::Rejected("selected input unavailable".into()))?;
        let _guard = self.memory_lifecycle.lock().await;
        ensure(
            self.text_input_identity()? == identity,
            "selected input identity changed",
        )?;
        self.action(source, HumanAction::SubmitTextFile { id, goal, input })
            .await
    }
    pub(crate) fn text_input_identity(&self) -> Result<String> {
        ensure(self.role == ClientRole::Chat, "text input requires chat")?;
        self.require_memory_copy()?;
        self.memory_identity
            .lock()
            .unwrap()
            .clone()
            .ok_or(crate::Error::CredentialRejected)
    }
    pub(crate) async fn store_text_input(&self, input: TextInput, identity: &str) -> Result<()> {
        let _guard = self.memory_lifecycle.lock().await;
        ensure(
            self.text_input_identity()? == identity && self.memory_access_allowed().await?,
            "text input identity changed",
        )?;
        input.validate()?;
        sqlx::query("INSERT INTO local_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(KEY).bind(serde_json::to_string(&Selection{identity:identity.into(),input})?).execute(&self.db).await?;
        Ok(())
    }
    pub(crate) async fn selected_text_input(&self) -> Result<Option<TextInput>> {
        let _guard = self.memory_lifecycle.lock().await;
        if self.role != ClientRole::Chat || !self.memory_access_allowed().await? {
            return Ok(None);
        }
        let Ok(identity) = self.text_input_identity() else {
            return Ok(None);
        };
        let value: Option<String> = sqlx::query_scalar("SELECT value FROM local_meta WHERE key=?")
            .bind(KEY)
            .fetch_optional(&self.db)
            .await?;
        let Some(value) = value else { return Ok(None) };
        if value.len() <= 128 * 1024
            && let Ok(selected) = serde_json::from_str::<Selection>(&value)
            && selected.identity == identity
            && selected.input.validate().is_ok()
            && selected.input.fresh(crate::now())
        {
            return Ok(Some(selected.input));
        }
        self.clear_text_input_in(None).await?;
        Ok(None)
    }
    async fn clear_text_input_in(&self, id: Option<&str>) -> Result<()> {
        let count = if let Some(id) = id {
            sqlx::query("DELETE FROM local_meta WHERE key=? AND CASE WHEN json_valid(value) THEN json_extract(value,'$.input.id')=? ELSE 0 END")
                .bind(KEY).bind(id).execute(&self.db).await?.rows_affected()
        } else {
            sqlx::query("DELETE FROM local_meta WHERE key=?")
                .bind(KEY)
                .execute(&self.db)
                .await?
                .rows_affected()
        };
        if count > 0 {
            pulse_core::truncate_wal(&self.db).await?;
        }
        Ok(())
    }
    pub(crate) async fn clear_text_input(&self, id: Option<&str>) -> Result<()> {
        let _guard = self.memory_lifecycle.lock().await;
        self.clear_text_input_in(id).await
    }
}

impl crate::server::Hub {
    pub(crate) async fn task_input_in(
        db: &mut sqlx::SqliteConnection,
        task: &pulse_core::Task,
    ) -> Result<Option<TextInput>> {
        if pulse_core::input_digest(&task.scope).is_none() {
            return Ok(None);
        }
        let row: (String, String, Option<String>) =
            sqlx::query_as("SELECT origin,source,payload FROM events WHERE id=?")
                .bind(&task.authorization_event)
                .fetch_one(&mut *db)
                .await?;
        ensure(
            row.0 == "human" && row.1.starts_with("device:"),
            "file input requires Human source",
        )?;
        let payload: serde_json::Value = serde_json::from_str(
            &row.2
                .ok_or_else(|| crate::Error::Rejected("file source unavailable".into()))?,
        )?;
        let HumanAction::SubmitTextFile { input, .. } =
            serde_json::from_value(payload["native_action"].clone())?
        else {
            return Err(crate::Error::Rejected("file authorization mismatch".into()));
        };
        input.validate()?;
        let commands: Vec<pulse_core::Command> =
            serde_json::from_value(payload["commands"].clone())?;
        ensure(input.scope()==task.scope && commands.iter().any(|command|
            matches!(command,pulse_core::Command::Create{id,scope,..} if id==&task.id && scope==&task.scope)),"file source scope mismatch")?;
        Ok(Some(input))
    }
}

#[cfg(test)]
mod tests;
