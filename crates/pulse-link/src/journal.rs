use crate::{Result, ensure, protocol::*};
use fs2::FileExt;
use sqlx::{Row, SqlitePool};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

#[derive(Clone)]
pub struct Journal {
    pub(crate) db: SqlitePool,
    pub device: String,
    pub server: String,
    pub role: ClientRole,
}
pub struct DeviceLock(File);
impl Drop for DeviceLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

pub fn private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    if !path.exists() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
    }
    ensure(
        path.is_dir()
            && !std::fs::symlink_metadata(path)?.file_type().is_symlink()
            && std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
        "private data directory must be 0700",
    )
}
pub fn private_write(path: &Path, data: &[u8]) -> Result<()> {
    use std::{
        io::Write,
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    ensure(
        file.metadata()?.permissions().mode() & 0o077 == 0,
        "private file mode",
    )?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}
pub fn private_read(path: &Path) -> Result<String> {
    use std::{
        io::Read,
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    ensure(
        file.metadata()?.is_file() && file.metadata()?.permissions().mode() & 0o077 == 0,
        "private file must be 0600",
    )?;
    let mut text = String::new();
    file.by_ref().take(4096).read_to_string(&mut text)?;
    Ok(text.trim().into())
}

impl Journal {
    pub async fn open(dir: &Path, server: &str, repo: &Path) -> Result<(Self, DeviceLock)> {
        Self::open_role(dir, server, repo, ClientRole::LocalExecutor).await
    }
    pub async fn open_role(
        dir: &Path,
        server: &str,
        repo: &Path,
        role: ClientRole,
    ) -> Result<(Self, DeviceLock)> {
        use std::os::unix::fs::OpenOptionsExt;
        private_dir(dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(dir.join("helper.lock"))?;
        lock.try_lock_exclusive().map_err(|_| {
            crate::Error::Rejected("another helper owns this data directory".into())
        })?;
        let core = pulse_core::Store::open(dir.join("device.db"), "pulse-local-journal").await?;
        let db = core.application_pool();
        sqlx::raw_sql("CREATE TABLE IF NOT EXISTS local_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS local_outbox(sequence INTEGER PRIMARY KEY,id TEXT NOT NULL,data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS local_inbox(sequence INTEGER PRIMARY KEY,id TEXT NOT NULL,digest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS local_work(id TEXT PRIMARY KEY,envelope TEXT NOT NULL,state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS local_pending(id TEXT PRIMARY KEY,body TEXT NOT NULL,accepted INTEGER NOT NULL DEFAULT 0);").execute(&db).await?;
        let mut tx = db.begin_with("BEGIN IMMEDIATE").await?;
        let existing: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_meta WHERE key='device'")
                .fetch_one(&mut *tx)
                .await?;
        sqlx::query("INSERT OR IGNORE INTO local_meta VALUES('role',?)")
            .bind(if existing == 0 {
                role.as_str()
            } else {
                "local_executor"
            })
            .execute(&mut *tx)
            .await?;
        let device = uuid::Uuid::new_v4().to_string();
        for (key, value) in [
            ("device", device.as_str()),
            ("server", server),
            (
                "repo",
                repo.to_str()
                    .ok_or_else(|| crate::Error::Rejected("repo path encoding".into()))?,
            ),
            ("cursor", "0"),
            ("next", "0"),
            ("stopped", "false"),
        ] {
            sqlx::query("INSERT OR IGNORE INTO local_meta VALUES(?,?)")
                .bind(key)
                .bind(value)
                .execute(&mut *tx)
                .await?;
        }
        for (key, expected) in [
            ("server", server),
            ("repo", repo.to_str().unwrap_or("")),
            ("role", role.as_str()),
        ] {
            let actual: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key=?")
                .bind(key)
                .fetch_one(&mut *tx)
                .await?;
            ensure(
                actual == expected,
                "data directory is bound to another server or repository",
            )?;
        }
        let device: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='device'")
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((
            Self {
                db,
                device,
                server: server.into(),
                role,
            },
            DeviceLock(lock),
        ))
    }
    pub async fn cursor(&self) -> Result<u64> {
        let s: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cursor'")
            .fetch_one(&self.db)
            .await?;
        number(&s)
    }
    pub async fn pending(&self) -> Result<usize> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM local_outbox")
                .fetch_one(&self.db)
                .await? as usize,
        )
    }
    pub async fn stopped(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT value FROM local_meta WHERE key='stopped'")
                .fetch_one(&self.db)
                .await?
                == "true",
        )
    }
    pub async fn stop(&self, value: bool) -> Result<()> {
        sqlx::query("UPDATE local_meta SET value=? WHERE key='stopped'")
            .bind(value.to_string())
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn enqueue_in(
        &self,
        db: &mut sqlx::SqliteConnection,
        payload: DevicePayload,
        epoch: u64,
    ) -> Result<()> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_outbox")
            .fetch_one(&mut *db)
            .await?;
        let size: i64 =
            sqlx::query_scalar("SELECT COALESCE(SUM(length(data)),0) FROM local_outbox")
                .fetch_one(&mut *db)
                .await?;
        let work: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM local_work WHERE state IN ('accepted','running')",
        )
        .fetch_one(&mut *db)
        .await?;
        if count + work >= 1000 || size + (work + 1) * MAX_FRAME as i64 > 64 * 1024 * 1024 {
            return Err(crate::Error::Capacity);
        }
        let previous: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='next'")
            .fetch_one(&mut *db)
            .await?;
        let next = number(&previous)?
            .checked_add(1)
            .ok_or_else(|| crate::Error::Rejected("sequence overflow".into()))?;
        let envelope = Envelope::new(&self.device, next, epoch, payload)?;
        envelope.validate()?;
        sqlx::query("INSERT INTO local_outbox VALUES(?,?,?)")
            .bind(next as i64)
            .bind(&envelope.id)
            .bind(serde_json::to_string(&envelope)?)
            .execute(&mut *db)
            .await?;
        sqlx::query("UPDATE local_meta SET value=? WHERE key='next'")
            .bind(next.to_string())
            .execute(&mut *db)
            .await?;
        Ok(())
    }
    pub async fn action(&self, source_id: &str, action: HumanAction) -> Result<()> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        if let HumanAction::Submit { goal, .. } = &action {
            ensure(
                !goal.trim().is_empty() && goal.len() <= 4096,
                "goal must be 1..4096 bytes",
            )?;
            sqlx::query("INSERT OR IGNORE INTO local_pending(id,body) VALUES(?,?)")
                .bind(source_id)
                .bind(goal)
                .execute(&mut *tx)
                .await?;
        }
        self.enqueue_in(
            &mut tx,
            DevicePayload::Human {
                source_id: source_id.into(),
                action,
            },
            0,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn outgoing(&self) -> Result<Vec<Envelope<DevicePayload>>> {
        let rows: Vec<String> =
            sqlx::query_scalar("SELECT data FROM local_outbox ORDER BY sequence LIMIT 100")
                .fetch_all(&self.db)
                .await?;
        rows.into_iter().map(|s| parse(s.as_bytes())).collect()
    }
    pub async fn ack(&self, sequence: u64, id: &str) -> Result<()> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT data,id FROM local_outbox WHERE sequence=?")
            .bind(sequence as i64)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(row) = row {
            ensure(row.get::<String, _>("id") == id, "ACK identity conflict")?;
            let envelope: Envelope<DevicePayload> = parse(row.get::<String, _>("data").as_bytes())?;
            if let DevicePayload::Human { source_id, .. } = envelope.payload {
                sqlx::query("UPDATE local_pending SET accepted=1 WHERE id=?")
                    .bind(source_id)
                    .execute(&mut *tx)
                    .await?;
            }
            sqlx::query("DELETE FROM local_outbox WHERE sequence=? AND id=?")
                .bind(sequence as i64)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    /// Commit cache or accepted work before allowing the caller to ACK.
    pub async fn accept(
        &self,
        envelope: &Envelope<ServerPayload>,
        permitted: bool,
        epoch: u64,
        lease: bool,
    ) -> Result<bool> {
        let (sequence, message_epoch) = envelope.validate()?;
        ensure(envelope.device == self.device, "wrong device")?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let cursor: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cursor'")
            .fetch_one(&mut *tx)
            .await?;
        let cursor = number(&cursor)?;
        if sequence <= cursor {
            let row = sqlx::query("SELECT id,digest FROM local_inbox WHERE sequence=?")
                .bind(sequence as i64)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| crate::Error::Rejected("missing dedupe journal".into()))?;
            ensure(
                row.get::<String, _>("id") == envelope.id
                    && row.get::<String, _>("digest") == envelope.binding_digest,
                "server replay conflict",
            )?;
            return Ok(false);
        }
        ensure(sequence == cursor + 1, "server sequence gap")?;
        match &envelope.payload {
            ServerPayload::SetStopped { stopped } => {
                ensure(
                    self.role == ClientRole::LocalExecutor,
                    "chat cannot receive worker control",
                )?;
                sqlx::query("UPDATE local_meta SET value=? WHERE key='stopped'")
                    .bind(stopped.to_string())
                    .execute(&mut *tx)
                    .await?;
            }
            ServerPayload::Snapshot {
                tasks,
                messages,
                synced_at,
                local,
            } => {
                let state = NativeState {
                    tasks: tasks.clone(),
                    messages: messages.clone(),
                    synced_at: Some(*synced_at),
                    stopped: local.stopped,
                    local_connected: local.connected,
                    ..Default::default()
                };
                sqlx::query("INSERT INTO local_meta VALUES('cache',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(serde_json::to_string(&state)?).execute(&mut *tx).await?;
                for message in messages {
                    sqlx::query("DELETE FROM local_pending WHERE id=?")
                        .bind(&message.id)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            ServerPayload::Read {
                operation_id,
                task_id,
                revision,
                operation,
            } => {
                ensure(
                    self.role == ClientRole::LocalExecutor,
                    "chat cannot accept local work",
                )?;
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM local_work WHERE state IN ('accepted','running')",
                )
                .fetch_one(&mut *tx)
                .await?;
                ensure(count < 1, "worker busy")?;
                let used: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_outbox")
                    .fetch_one(&mut *tx)
                    .await?;
                let size: i64 =
                    sqlx::query_scalar("SELECT COALESCE(SUM(length(data)),0) FROM local_outbox")
                        .fetch_one(&mut *tx)
                        .await?;
                let browser = matches!(
                    operation,
                    ReadOp::Browser { .. }
                        | ReadOp::BrowserInspect { .. }
                        | ReadOp::BrowserAction { .. }
                        | ReadOp::ComputerInspect { .. }
                );
                ensure(
                    used < if browser { 934 } else { 998 }
                        && size
                            + if browser {
                                8 * 1024 * 1024
                            } else {
                                2 * MAX_FRAME as i64
                            }
                            <= 64 * 1024 * 1024,
                    "result outbox capacity unavailable",
                )?;
                if permitted && lease && epoch == message_epoch {
                    sqlx::query("INSERT INTO local_work VALUES(?,?,'accepted')")
                        .bind(operation_id)
                        .bind(serde_json::to_string(envelope)?)
                        .execute(&mut *tx)
                        .await?;
                } else {
                    self.enqueue_in(
                        &mut tx,
                        DevicePayload::Result {
                            operation_id: operation_id.clone(),
                            task_id: task_id.clone(),
                            revision: revision.clone(),
                            outcome: ReadOutcome::Unknown {
                                reason: "stale epoch, stopped worker or expired lease".into(),
                            },
                        },
                        message_epoch,
                    )
                    .await?;
                }
            }
        }
        sqlx::query("INSERT INTO local_inbox VALUES(?,?,?)")
            .bind(sequence as i64)
            .bind(&envelope.id)
            .bind(&envelope.binding_digest)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE local_meta SET value=? WHERE key='cursor'")
            .bind(sequence.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
    pub async fn take_work(&self) -> Result<Option<Envelope<ServerPayload>>> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT id,envelope FROM local_work WHERE state='accepted' LIMIT 1")
            .fetch_optional(&mut *tx)
            .await?;
        let result = if let Some(row) = row {
            sqlx::query("UPDATE local_work SET state='running' WHERE id=?")
                .bind(row.get::<String, _>("id"))
                .execute(&mut *tx)
                .await?;
            Some(parse(row.get::<String, _>("envelope").as_bytes())?)
        } else {
            None
        };
        tx.commit().await?;
        Ok(result)
    }
    pub async fn finish_work(
        &self,
        envelope: &Envelope<ServerPayload>,
        outcome: ReadOutcome,
    ) -> Result<()> {
        self.finish_work_image(envelope, outcome, None).await
    }
    pub async fn finish_work_image(
        &self,
        envelope: &Envelope<ServerPayload>,
        outcome: ReadOutcome,
        image: Option<crate::artifact::CapturedImage>,
    ) -> Result<()> {
        ensure(outcome.valid(), "invalid result")?;
        if let ServerPayload::Read {
            operation_id,
            task_id,
            revision,
            ..
        } = &envelope.payload
        {
            let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
            let state: Option<String> =
                sqlx::query_scalar("SELECT state FROM local_work WHERE id=?")
                    .bind(operation_id)
                    .fetch_optional(&mut *tx)
                    .await?;
            ensure(
                matches!(state.as_deref(), Some("accepted" | "running")),
                "work already finished or missing",
            )?;
            crate::computer_transport::validate_result(
                &envelope.payload,
                &self.device,
                number(&envelope.epoch)?,
                &outcome,
                image.as_ref().map(|image| &image.artifact),
            )?;
            if let Some(mut image) = image {
                image.artifact.task_id = task_id.clone();
                image.artifact.revision = revision.clone();
                use base64::Engine;
                ensure(
                    matches!(
                        &envelope.payload,
                        ServerPayload::Read {
                            operation: ReadOp::Browser { .. }
                                | ReadOp::BrowserInspect { .. }
                                | ReadOp::BrowserAction { .. },
                            ..
                        } | ServerPayload::Read {
                            operation: ReadOp::ComputerInspect { .. },
                            ..
                        }
                    ) && matches!(outcome, ReadOutcome::Completed { .. }),
                    "image requires completed image work",
                )?;
                image.artifact.verify(&image.png)?;
                let used: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_outbox")
                    .fetch_one(&mut *tx)
                    .await?;
                let size: i64 =
                    sqlx::query_scalar("SELECT COALESCE(SUM(length(data)),0) FROM local_outbox")
                        .fetch_one(&mut *tx)
                        .await?;
                ensure(
                    used + (image.artifact.chunks() as i64) < 1000
                        && size + 8 * 1024 * 1024 <= 64 * 1024 * 1024,
                    "artifact result capacity unavailable",
                )?;
                for (index, bytes) in image.png.chunks(crate::artifact::CHUNK).enumerate() {
                    self.enqueue_in(
                        &mut tx,
                        DevicePayload::ArtifactChunk {
                            operation_id: operation_id.clone(),
                            artifact: image.artifact.clone(),
                            index: index as u32,
                            data: base64::engine::general_purpose::STANDARD.encode(bytes),
                        },
                        number(&envelope.epoch)?,
                    )
                    .await?;
                }
            }
            self.enqueue_in(
                &mut tx,
                DevicePayload::Result {
                    operation_id: operation_id.clone(),
                    task_id: task_id.clone(),
                    revision: revision.clone(),
                    outcome,
                },
                number(&envelope.epoch)?,
            )
            .await?;
            sqlx::query("UPDATE local_work SET state='finished' WHERE id=?")
                .bind(operation_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        Ok(())
    }
    pub async fn recover(&self) -> Result<()> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT envelope FROM local_work WHERE state IN ('accepted','running')",
        )
        .fetch_all(&self.db)
        .await?;
        for row in rows {
            let e = parse(row.as_bytes())?;
            self.finish_work(
                &e,
                ReadOutcome::Unknown {
                    reason: "helper restarted before a durable result; execution is not repeated"
                        .into(),
                },
            )
            .await?;
        }
        Ok(())
    }
    pub async fn view(&self) -> Result<NativeState> {
        let cache: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cache'")
                .fetch_optional(&self.db)
                .await?;
        let mut state = cache
            .map(|s| serde_json::from_str::<NativeState>(&s))
            .transpose()?
            .unwrap_or_default();
        for message in &state.messages {
            if let Some(artifact) = &message.artifact {
                let operation_id = message
                    .artifact_operation_id
                    .as_ref()
                    .unwrap_or(&message.id);
                let data: Option<String> =
                    sqlx::query_scalar("SELECT value FROM local_meta WHERE key=?")
                        .bind(format!("image:{operation_id}"))
                        .fetch_optional(&self.db)
                        .await?;
                if let Some(data) = data {
                    let image: crate::artifact::LocalArtifact = serde_json::from_str(&data)?;
                    if image.artifact == *artifact {
                        state.artifacts.push(image);
                    }
                }
            }
        }
        for row in
            sqlx::query("SELECT id,body,accepted FROM local_pending ORDER BY rowid LIMIT 100")
                .fetch_all(&self.db)
                .await?
        {
            let accepted = row.get::<i64, _>("accepted") != 0;
            let body: String = row.get("body");
            let preview = if body.len() > 1024 {
                let mut end = 1024;
                while !body.is_char_boundary(end) {
                    end -= 1;
                }
                format!("{}（本機預覽已截斷）", &body[..end])
            } else {
                body
            };
            state.messages.push(ChatMessage {
                artifact_operation_id: None,
                approval: None,
                artifact: None,
                id: row.get("id"),
                body: format!(
                    "{}：{}",
                    if accepted {
                        "雲端已收件"
                    } else {
                        "本機待送"
                    },
                    preview
                ),
            });
        }
        state.pending = self.pending().await?;
        if self.role == ClientRole::LocalExecutor {
            state.stopped = self.stopped().await?;
        }
        Ok(state)
    }
}

pub fn default_data_dir() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    home.join("Library/Application Support/Pulse")
}
