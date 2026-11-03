//! Rust-owned, source-bound Markdown results. UI input never supplies the body.
use crate::{Result, ensure, protocol::*, server::Hub};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

pub const MAX_DOCUMENT: usize = 32 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextResultManifest {
    pub reference: TextResultRef,
    pub message_id: String,
    pub chat_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TextResultRef {
    pub id: String,
    pub task_id: String,
    pub revision: String,
    pub digest: String,
    pub length: u32,
    pub created_at: i64,
    pub file_name: String,
}
impl TextResultRef {
    pub fn validate(&self) -> Result<()> {
        ensure(
            uuid::Uuid::parse_str(&self.id).is_ok()
                && !self.task_id.is_empty()
                && self.task_id.len() <= 128
                && number(&self.revision).is_ok_and(|n| n > 0)
                && crate::artifact::digest_id(&self.digest)
                && self.created_at > 0
                && (1..=MAX_DOCUMENT as u32).contains(&self.length)
                && self.file_name == format!("Pulse-{}.md", self.id),
            "invalid text result reference",
        )
    }
    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        self.validate()?;
        ensure(
            bytes.len() == self.length as usize
                && hash_bytes(bytes) == self.digest
                && std::str::from_utf8(bytes).is_ok(),
            "text result content mismatch",
        )
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    authorization_event: String,
    decision_event: String,
    scope: String,
    input: Option<crate::text_input::TextInputRef>,
    reads: Vec<ReadSource>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadSource {
    operation_id: String,
    resource: String,
    digest: String,
    truncated: bool,
    observed_at: Option<i64>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    version: u8,
    reference: TextResultRef,
    message_id: String,
    chat_digest: String,
    goal: String,
    model: String,
    source: Source,
}
fn plain(value: &str) -> String {
    value
        .chars()
        .flat_map(|c| {
            if "\\`*_{}[]<>()#+-.!|".contains(c) {
                vec!['\\', c]
            } else {
                vec![c]
            }
        })
        .collect()
}
impl Record {
    fn render(&self, reply: &str) -> Result<Vec<u8>> {
        ensure(
            self.version == 1
                && !reply.trim().is_empty()
                && reply.len() <= 7000
                && self.goal.len() <= 4096
                && !self.model.is_empty()
                && self.model.len() <= 128
                && self.source.reads.len() <= 16,
            "invalid text result record",
        )?;
        let date = chrono::DateTime::from_timestamp_millis(self.reference.created_at)
            .ok_or_else(|| crate::Error::Rejected("result time".into()))?
            .to_rfc3339();
        ensure(
            serde_json::to_vec(&self.source)?.len() <= 16 * 1024,
            "result source too large",
        )?;
        let mut sources = String::new();
        if let Some(input) = &self.source.input {
            let selected = chrono::DateTime::from_timestamp_millis(input.captured_at)
                .map(|d| d.to_rfc3339())
                .unwrap_or_default();
            sources.push_str(&format!(
                "- 文字副本：{}；選取時間：{selected}；內容摘要：{}。\n",
                plain(&input.name),
                input.digest
            ));
        }
        for read in &self.source.reads {
            let observed = read
                .observed_at
                .and_then(chrono::DateTime::from_timestamp_millis)
                .map(|d| format!("；取得時間：{}", d.to_rfc3339()))
                .unwrap_or_default();
            let clipped = if read.truncated {
                "；內容已截斷"
            } else {
                ""
            };
            sources.push_str(&format!(
                "- {}{observed}{clipped}。證據識別：{}；內容摘要：{}。\n",
                plain(&read.resource),
                plain(&read.operation_id),
                read.digest
            ));
        }
        if sources.is_empty() {
            sources.push_str("尚無選定文字副本或外部工具讀取證據。\n");
        }
        let body = format!(
            "# Pulse 成果\n\n{}\n\n建立時間：{date}\n\n任務：{} · 修訂 {}\n\n模型：{}\n\n## 來源紀錄\n\n{sources}\n來源要求：{}\n\n有效決策：{}\n\n來源來自已保存的副本與工具紀錄，不表示模型已讀取全文。\n\n## 分析草稿\n\n這是模型分析草稿，不代表任務已完成或外部操作已成功。\n\n{reply}\n",
            plain(&self.goal),
            plain(&self.reference.task_id),
            self.reference.revision,
            plain(&self.model),
            self.source.authorization_event,
            self.source.decision_event
        );
        ensure(body.len() <= MAX_DOCUMENT, "result document too large")?;
        Ok(body.into_bytes())
    }
}
async fn no_memory(db: &mut SqliteConnection, task_id: &str) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_memory_sources WHERE task_id=?")
        .bind(task_id)
        .fetch_one(db)
        .await?;
    Ok(count == 0)
}
fn applied(outcome: Option<&str>, task_id: &str) -> bool {
    let Some(outcome) = outcome else { return false };
    match serde_json::from_str::<pulse_core::Outcome>(outcome) {
        Ok(
            pulse_core::Outcome::Applied { task_ids }
            | pulse_core::Outcome::Schedules { task_ids, .. },
        ) => task_ids.iter().any(|id| id == task_id),
        _ => false,
    }
}
async fn source_valid(
    db: &mut SqliteConnection,
    task_id: &str,
    goal: &str,
    source: &Source,
) -> Result<bool> {
    let row: Option<(String, String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT origin,source,payload,outcome FROM events WHERE id=?")
            .bind(&source.authorization_event)
            .fetch_optional(&mut *db)
            .await?;
    let Some((origin, from, Some(payload), outcome)) = row else {
        return Ok(false);
    };
    if origin != "human" || !from.starts_with("device:") || !applied(outcome.as_deref(), task_id) {
        return Ok(false);
    }
    let payload: serde_json::Value = serde_json::from_str(&payload)?;
    let commands: Vec<pulse_core::Command> = serde_json::from_value(payload["commands"].clone())?;
    let bound = commands.iter().any(|command| match command {
        pulse_core::Command::Create { id, scope, .. } => id == task_id && scope == &source.scope,
        pulse_core::Command::Revise {
            id,
            scope: Some(scope),
            ..
        } => id == task_id && scope == &source.scope,
        _ => false,
    });
    if !bound {
        return Ok(false);
    }
    let decision: Option<(String, String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT origin,source,payload,outcome FROM events WHERE id=?")
            .bind(&source.decision_event)
            .fetch_optional(&mut *db)
            .await?;
    let Some((actor, from, Some(data), outcome)) = decision else {
        return Ok(false);
    };
    if actor != "human" || !from.starts_with("device:") || !applied(outcome.as_deref(), task_id) {
        return Ok(false);
    }
    let decision: serde_json::Value = serde_json::from_str(&data)?;
    let changes: Vec<pulse_core::Command> = serde_json::from_value(decision["commands"].clone())?;
    if !changes.iter().any(|c| match c {
        pulse_core::Command::Create { id, goal: text, .. }
        | pulse_core::Command::Revise { id, goal: text, .. } => id == task_id && text == goal,
        _ => false,
    }) {
        return Ok(false);
    }
    if pulse_core::input_digest(&source.scope).is_some() {
        let Ok(HumanAction::SubmitTextFile { input, .. }) =
            serde_json::from_value(payload["native_action"].clone())
        else {
            return Ok(false);
        };
        return Ok(input.validate().is_ok()
            && input.scope() == source.scope
            && source.input.as_ref() == Some(&input.reference()));
    }
    Ok(source.input.is_none() && pulse_core::read_scope(&source.scope))
}
fn resource(op: &ReadOp) -> String {
    match op {
        ReadOp::List { path } => format!("Pulse 目錄：{path}"),
        ReadOp::Text { path } => format!("Pulse 文字：{path}"),
        ReadOp::GitStatus => "Pulse Git status".into(),
        ReadOp::GitDiff { path } => format!("Pulse Git diff：{path}"),
        ReadOp::Browser { url } | ReadOp::BrowserInspect { url } => url.clone(),
        ReadOp::ComputerInspect { selection } | ReadOp::ComputerAction { selection, .. } => {
            format!(
                "程式 {} · PID {} · 視窗 {}",
                selection.target.bundle_id, selection.target.pid, selection.target.window_id
            )
        }
        ReadOp::BrowserAction { .. } => "瀏覽器操作；另見原批准與結果紀錄".into(),
    }
}
impl Hub {
    pub(crate) async fn make_text_result(
        db: &mut SqliteConnection,
        task: &pulse_core::Task,
        run: &pulse_core::Run,
        message_id: &str,
        chat: &str,
        time: i64,
    ) -> Result<Option<Record>> {
        // Model memory is still off. Do not create unmanaged derived copies.
        if !no_memory(db, &task.id).await? {
            return Ok(None);
        }
        let model = run.checkpoint["model"]
            .as_str()
            .ok_or_else(|| crate::Error::Rejected("result model".into()))?;
        let reply = run.checkpoint["reply"]
            .as_str()
            .ok_or_else(|| crate::Error::Rejected("result reply".into()))?;
        let input = Self::task_input_in(db, task).await?.map(|i| i.reference());
        let mut source = Source {
            authorization_event: task.authorization_event.clone(),
            decision_event: task
                .decisions
                .last()
                .map(|d| d.event_id.clone())
                .unwrap_or_default(),
            scope: task.scope.clone(),
            input,
            reads: vec![],
        };
        if !source_valid(db, &task.id, &task.goal, &source).await? {
            return Ok(None);
        }
        let rows=sqlx::query("SELECT id,request,result FROM link_reads WHERE task_id=? AND task_revision=? AND state='completed' ORDER BY rowid LIMIT 16")
            .bind(&task.id).bind(task.revision.to_string()).fetch_all(&mut *db).await?;
        for row in rows {
            let op: ReadOp = serde_json::from_str(&row.get::<String, _>("request"))?;
            let outcome: ReadOutcome = serde_json::from_str(&row.get::<String, _>("result"))?;
            ensure(
                op.permitted_by(&task.scope) && outcome.valid(),
                "result evidence outside source",
            )?;
            if let ReadOutcome::Completed {
                digest,
                truncated,
                observation,
                computer,
                ..
            } = outcome
            {
                let label = resource(&op);
                if label.len() > 2048 {
                    return Ok(None);
                }
                source.reads.push(ReadSource {
                    operation_id: row.get("id"),
                    resource: label,
                    digest,
                    truncated,
                    observed_at: observation
                        .map(|o| o.observed_at)
                        .or_else(|| computer.map(|o| o.observed_at)),
                });
            }
        }
        let mut record = Record {
            version: 1,
            reference: TextResultRef {
                id: run.token.run_id.clone(),
                task_id: task.id.clone(),
                revision: task.revision.to_string(),
                digest: String::new(),
                length: 0,
                created_at: time,
                file_name: format!("Pulse-{}.md", run.token.run_id),
            },
            message_id: message_id.into(),
            chat_digest: hash_bytes(chat.as_bytes()),
            goal: task.goal.clone(),
            model: model.into(),
            source,
        };
        let bytes = match record.render(reply) {
            Ok(bytes) => bytes,
            Err(crate::Error::Rejected(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        record.reference.digest = hash_bytes(&bytes);
        record.reference.length = bytes.len() as u32;
        record.reference.validate()?;
        Ok(Some(record))
    }
    async fn text_result_in(
        db: &mut SqliteConnection,
        id: &str,
    ) -> Result<(TextResultManifest, Vec<u8>)> {
        ensure(uuid::Uuid::parse_str(id).is_ok(), "result ID")?;
        let data: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=?")
            .bind(id)
            .fetch_one(&mut *db)
            .await?;
        let run: pulse_core::Run = serde_json::from_str(&data)?;
        let record: Record = serde_json::from_value(run.checkpoint["text_result"].clone())?;
        let reply = run.checkpoint["reply"]
            .as_str()
            .ok_or_else(|| crate::Error::Rejected("result reply unavailable".into()))?;
        ensure(
            run.checkpoint["phase"] == "reply_saved"
                && record.reference.id == run.token.run_id
                && record.reference.task_id == run.token.task_id
                && number(&record.reference.revision)? == run.token.revision
                && record.message_id
                    == format!(
                        "model:{}:{}:{}",
                        run.token.run_id,
                        run.token.revision,
                        run.checkpoint["round"].as_u64().unwrap_or(0)
                    )
                && no_memory(db, &run.token.task_id).await?
                && source_valid(db, &run.token.task_id, &record.goal, &record.source).await?,
            "result provenance unavailable",
        )?;
        let chat: String = sqlx::query_scalar("SELECT body FROM link_chat WHERE id=?")
            .bind(&record.message_id)
            .fetch_one(&mut *db)
            .await?;
        ensure(
            hash_bytes(chat.as_bytes()) == record.chat_digest,
            "result chat changed",
        )?;
        let bytes = record.render(reply)?;
        record.reference.verify(&bytes)?;
        Ok((
            TextResultManifest {
                reference: record.reference,
                message_id: record.message_id,
                chat_digest: record.chat_digest,
            },
            bytes,
        ))
    }
    pub async fn text_result_manifest(
        &self,
        device: &str,
        id: &str,
        time: i64,
    ) -> Result<TextResultManifest> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        Self::text_result_chat_in(&mut tx, device, time).await?;
        let (manifest, _) = Self::text_result_in(&mut tx, id).await?;
        tx.commit().await?;
        Ok(manifest)
    }
    async fn text_result_chat_in(db: &mut SqliteConnection, device: &str, time: i64) -> Result<()> {
        let allowed:i64=sqlx::query_scalar("SELECT count(*) FROM link_device WHERE id=? AND role='chat' AND revoked=0 AND lease_until>?").bind(device).bind(time).fetch_one(db).await?;
        ensure(allowed == 1, "result requires live chat")
    }
    pub async fn text_result_bytes(
        &self,
        device: &str,
        id: &str,
        digest: &str,
        time: i64,
    ) -> Result<Vec<u8>> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        Self::text_result_chat_in(&mut tx, device, time).await?;
        ensure(crate::artifact::digest_id(digest), "result digest")?;
        let (manifest, bytes) = Self::text_result_in(&mut tx, id).await?;
        ensure(manifest.reference.digest == digest, "result digest changed")?;
        tx.commit().await?;
        Ok(bytes)
    }
}
