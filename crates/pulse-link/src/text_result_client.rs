//! Explicit local Human export. No model-supplied body, path, or overwrite.
use crate::{
    Result, client::Config, ensure, journal::Journal, protocol::*, text_result::TextResultRef,
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Component, Path},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExportStatus {
    Working,
    Cancelling,
    Saved,
    NotSaved,
    Unknown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExportState {
    pub request_id: String,
    pub status: ExportStatus,
    pub file_name: Option<String>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExportRequest {
    pub request_id: String,
    pub message_id: String,
    pub path: String,
}
#[derive(PartialEq, Eq)]
struct CachedResult {
    id: String,
    message_id: String,
    chat_digest: String,
}
const RECEIPT_PREFIX: &str = "text_result_export_v1:";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportReceipt {
    identity: String,
    request: ExportRequest,
    reference: TextResultRef,
    created_at: i64,
    status: ExportStatus,
}
#[derive(Default)]
pub(crate) struct Exports {
    job: Option<tokio::task::JoinHandle<ExportStatus>>,
    job_id: Option<String>,
    identity: Option<String>,
    stop: Option<Arc<AtomicBool>>,
    state: Option<ExportState>,
}
struct Stop(Arc<AtomicBool>);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
impl Exports {
    pub async fn restore(&mut self, journal: &Journal) -> Result<()> {
        let Ok(identity) = journal.text_input_identity() else {
            return Ok(());
        };
        let row:Option<(String,String)>=sqlx::query_as("SELECT key,value FROM local_meta WHERE key GLOB 'text_result_export_v1:*' AND CASE WHEN json_valid(value) THEN json_extract(value,'$.identity')=? ELSE 0 END ORDER BY (json_extract(value,'$.status')!='saved') DESC,json_extract(value,'$.created_at') DESC,rowid DESC LIMIT 1")
            .bind(identity).fetch_optional(&journal.db).await?;
        let Some((key, value)) = row else {
            return Ok(());
        };
        self.state = Some(match serde_json::from_str::<ExportReceipt>(&value) {
            Ok(receipt)
                if key == format!("{RECEIPT_PREFIX}{}", receipt.request.request_id)
                    && receipt.reference.validate().is_ok() =>
            {
                ExportState {
                    request_id: receipt.request.request_id,
                    status: if receipt.status == ExportStatus::Saved {
                        ExportStatus::Saved
                    } else {
                        ExportStatus::Unknown
                    },
                    file_name: Path::new(&receipt.request.path)
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_owned),
                }
            }
            _ => ExportState {
                request_id: key.strip_prefix(RECEIPT_PREFIX).unwrap_or_default().into(),
                status: ExportStatus::Unknown,
                file_name: None,
            },
        });
        Ok(())
    }
    pub fn state(&self) -> Option<ExportState> {
        self.state.clone()
    }
    pub async fn start(
        &mut self,
        journal: Journal,
        config: Config,
        credential: Option<Credential>,
        request: ExportRequest,
    ) -> Result<()> {
        ensure(
            journal.role == ClientRole::Chat && uuid::Uuid::parse_str(&request.request_id).is_ok(),
            "result export requires Human chat",
        )?;
        let mut state = ExportState {
            request_id: request.request_id.clone(),
            status: ExportStatus::NotSaved,
            file_name: None,
        };
        if self.job.is_some() {
            if self
                .state
                .as_ref()
                .is_some_and(|s| s.request_id == request.request_id)
            {
                return Ok(());
            }
            self.state = Some(state);
            return Ok(());
        }
        let identity = journal.text_input_identity();
        if let Ok(identity) = &identity {
            match journal.previous_export(&request, identity).await {
                Ok(Some(status)) => {
                    state.status = status;
                    state.file_name = Path::new(&request.path)
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_owned);
                    self.state = Some(state);
                    return Ok(());
                }
                Ok(None) => {}
                Err(_) => {
                    state.status = ExportStatus::Unknown;
                    self.state = Some(state);
                    return Ok(());
                }
            }
        }
        let cached = journal.text_result_request(&request.message_id).await;
        let (Ok(identity), Ok(cached), Some(credential)) = (identity, cached, credential) else {
            self.state = Some(state);
            return Ok(());
        };
        if credential.role != ClientRole::Chat || credential.device != journal.device {
            self.state = Some(state);
            return Ok(());
        }
        let stop = Arc::new(AtomicBool::new(false));
        self.stop = Some(stop.clone());
        self.identity = Some(identity.clone());
        self.job_id = Some(request.request_id.clone());
        state.status = ExportStatus::Working;
        state.file_name = Path::new(&request.path)
            .file_name()
            .and_then(|s| s.to_str())
            .map(str::to_owned);
        self.state = Some(state);
        self.job = Some(tokio::spawn(async move {
            let _stop = Stop(stop.clone());
            let downloaded = tokio::select! {
                _=async {while !stop.load(Ordering::Acquire){tokio::time::sleep(std::time::Duration::from_millis(50)).await;}}=>return ExportStatus::NotSaved,
                bytes=download(&config,&credential,&cached)=>bytes,
            };
            let Ok((reference, bytes)) = downloaded else {
                return ExportStatus::NotSaved;
            };
            let guard = journal.memory_lifecycle.clone().lock_owned().await;
            if journal.text_input_identity().ok().as_ref() != Some(&identity)
                || !journal.memory_access_allowed().await.unwrap_or(false)
                || journal
                    .text_result_request(&cached.message_id)
                    .await
                    .ok()
                    .as_ref()
                    != Some(&cached)
            {
                return ExportStatus::NotSaved;
            }
            if journal
                .prepare_export(&request, &identity, &reference)
                .await
                .is_err()
            {
                return ExportStatus::NotSaved;
            }
            let request_id = request.request_id.clone();
            let receipt_journal = journal.clone();
            let receipt_identity = identity.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                let _guard = guard;
                save_new(Path::new(&request.path), &reference, &bytes, &stop, || {
                    journal.text_input_identity().ok().as_ref() == Some(&identity)
                })
            })
            .await
            .unwrap_or(ExportStatus::Unknown);
            if receipt_journal
                .finish_export_receipt(&request_id, &receipt_identity, &outcome)
                .await
                .is_err()
                && outcome == ExportStatus::Saved
            {
                ExportStatus::Unknown
            } else {
                outcome
            }
        }));
        Ok(())
    }
    pub fn cancel(&mut self, id: &str) {
        if let Some(state) = self
            .state
            .as_mut()
            .filter(|s| s.request_id == id && s.status == ExportStatus::Working)
        {
            if let Some(stop) = &self.stop {
                stop.store(true, Ordering::Release);
            }
            state.status = ExportStatus::Cancelling;
        }
    }
    pub fn invalidate(&mut self) {
        if let Some(stop) = &self.stop {
            stop.store(true, Ordering::Release);
        }
        if let Some(state) = self
            .state
            .as_mut()
            .filter(|s| matches!(s.status, ExportStatus::Working | ExportStatus::Cancelling))
        {
            state.status = ExportStatus::Unknown;
        }
    }
    pub async fn completed(&mut self) -> std::result::Result<ExportStatus, tokio::task::JoinError> {
        match self.job.as_mut() {
            Some(job) => job.await,
            None => std::future::pending().await,
        }
    }
    pub fn finish(
        &mut self,
        outcome: std::result::Result<ExportStatus, tokio::task::JoinError>,
        identity: Option<String>,
    ) {
        self.job = None;
        let id = self.job_id.take();
        let original = self.identity.take();
        self.stop = None;
        if let Some(state) = self.state.as_mut().filter(|s| {
            Some(&s.request_id) == id.as_ref()
                && matches!(s.status, ExportStatus::Working | ExportStatus::Cancelling)
        }) {
            state.status = if identity == original {
                outcome.unwrap_or(ExportStatus::Unknown)
            } else {
                ExportStatus::Unknown
            };
        }
    }
}
impl Drop for Exports {
    fn drop(&mut self) {
        self.invalidate();
        if let Some(job) = self.job.take() {
            job.abort();
        }
    }
}
impl Journal {
    async fn previous_export(
        &self,
        request: &ExportRequest,
        identity: &str,
    ) -> Result<Option<ExportStatus>> {
        let value: Option<String> = sqlx::query_scalar("SELECT value FROM local_meta WHERE key=?")
            .bind(format!("{RECEIPT_PREFIX}{}", request.request_id))
            .fetch_optional(&self.db)
            .await?;
        let Some(value) = value else { return Ok(None) };
        let receipt: ExportReceipt = serde_json::from_str(&value)?;
        if receipt.identity != identity || &receipt.request != request {
            return Ok(Some(ExportStatus::NotSaved));
        }
        receipt.reference.validate()?;
        Ok(Some(match receipt.status {
            ExportStatus::Saved => ExportStatus::Saved,
            _ => ExportStatus::Unknown,
        }))
    }
    async fn prepare_export(
        &self,
        request: &ExportRequest,
        identity: &str,
        reference: &TextResultRef,
    ) -> Result<()> {
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM local_meta WHERE key GLOB 'text_result_export_v1:*'",
        )
        .fetch_one(&mut *tx)
        .await?;
        ensure(count < 128, "export receipt capacity")?;
        let receipt = ExportReceipt {
            identity: identity.into(),
            request: request.clone(),
            reference: reference.clone(),
            created_at: crate::now(),
            status: ExportStatus::Working,
        };
        let value = serde_json::to_string(&receipt)?;
        ensure(value.len() <= 16 * 1024, "export receipt size")?;
        sqlx::query("INSERT INTO local_meta(key,value) VALUES(?,?)")
            .bind(format!("{RECEIPT_PREFIX}{}", request.request_id))
            .bind(value)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn finish_export_receipt(
        &self,
        id: &str,
        identity: &str,
        status: &ExportStatus,
    ) -> Result<()> {
        let key = format!("{RECEIPT_PREFIX}{id}");
        let changed = if *status == ExportStatus::NotSaved {
            sqlx::query("DELETE FROM local_meta WHERE key=? AND json_extract(value,'$.identity')=?")
                .bind(key)
                .bind(identity)
                .execute(&self.db)
                .await?
                .rows_affected()
        } else {
            sqlx::query("UPDATE local_meta SET value=json_set(value,'$.status',json(?)) WHERE key=? AND json_extract(value,'$.identity')=?")
                .bind(serde_json::to_string(status)?).bind(key).bind(identity).execute(&self.db).await?.rows_affected()
        };
        ensure(changed == 1, "export receipt no longer owned")
    }
    async fn text_result_request(&self, message_id: &str) -> Result<CachedResult> {
        self.text_input_identity()?;
        let data: Option<String> =
            sqlx::query_scalar("SELECT value FROM local_meta WHERE key='cache'")
                .fetch_optional(&self.db)
                .await?;
        let state: NativeState = serde_json::from_str(
            &data.ok_or_else(|| crate::Error::Rejected("result cache unavailable".into()))?,
        )?;
        let message = state
            .messages
            .into_iter()
            .find(|m| m.id == message_id)
            .ok_or_else(|| crate::Error::Rejected("result is not in current chat".into()))?;
        let id = message
            .id
            .strip_prefix("model:")
            .and_then(|s| s.split(':').next())
            .ok_or_else(|| crate::Error::Rejected("not a model result".into()))?;
        ensure(uuid::Uuid::parse_str(id).is_ok(), "result run ID")?;
        Ok(CachedResult {
            id: id.into(),
            message_id: message.id,
            chat_digest: hash_bytes(message.body.as_bytes()),
        })
    }
}
async fn bounded(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| crate::Error::Rejected("result body".into()))?
    {
        ensure(
            bytes.len() + chunk.len() <= limit,
            "result response too large",
        )?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
async fn download(
    config: &Config,
    credential: &Credential,
    cached: &CachedResult,
) -> Result<(TextResultRef, Vec<u8>)> {
    let mut url = config.server.join("v1/text-results/")?;
    url.path_segments_mut()
        .map_err(|_| crate::Error::Rejected("result origin".into()))?
        .pop_if_empty()
        .push(&cached.id);
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|_| crate::Error::Rejected("result client".into()))?;
    let response = client
        .get(url.clone())
        .bearer_auth(&credential.token)
        .send()
        .await
        .map_err(|_| crate::Error::Rejected("result manifest".into()))?;
    ensure(
        response.status() == reqwest::StatusCode::OK
            && response
                .headers()
                .get("content-type")
                .and_then(|s| s.to_str().ok())
                == Some("application/json"),
        "result manifest rejected",
    )?;
    let manifest: crate::text_result::TextResultManifest =
        serde_json::from_slice(&bounded(response, 4096).await?)?;
    let reference = manifest.reference;
    reference.validate()?;
    ensure(
        reference.id == cached.id
            && manifest.message_id == cached.message_id
            && manifest.chat_digest == cached.chat_digest,
        "result manifest does not match visible reply",
    )?;
    url.path_segments_mut()
        .map_err(|_| crate::Error::Rejected("result origin".into()))?
        .push(&reference.digest);
    let response = client
        .get(url)
        .bearer_auth(&credential.token)
        .send()
        .await
        .map_err(|_| crate::Error::Rejected("result download".into()))?;
    ensure(
        response.status() == reqwest::StatusCode::OK
            && response
                .headers()
                .get("content-type")
                .and_then(|s| s.to_str().ok())
                == Some("text/markdown; charset=utf-8"),
        "result response rejected",
    )?;
    let bytes = bounded(response, reference.length as usize).await?;
    reference.verify(&bytes)?;
    Ok((reference, bytes))
}
fn save_new(
    path: &Path,
    reference: &TextResultRef,
    bytes: &[u8],
    stop: &AtomicBool,
    allowed: impl Fn() -> bool,
) -> ExportStatus {
    save_with_sync(path, reference, bytes, stop, allowed, |dir| {
        Ok(rustix::fs::fsync(dir)?)
    })
}
fn save_with_sync(
    path: &Path,
    reference: &TextResultRef,
    bytes: &[u8],
    stop: &AtomicBool,
    allowed: impl Fn() -> bool,
    sync: impl Fn(&std::os::fd::OwnedFd) -> Result<()>,
) -> ExportStatus {
    use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, open, openat, renameat_with, unlinkat};
    use std::{io::Write, os::fd::OwnedFd};
    struct Temp<'a> {
        dir: &'a OwnedFd,
        name: String,
    }
    impl Drop for Temp<'_> {
        fn drop(&mut self) {
            let _ = unlinkat(self.dir, self.name.as_str(), AtFlags::empty());
        }
    }
    let mut published = false;
    let attempt = (|| -> Result<()> {
        reference.verify(bytes)?;
        ensure(
            !stop.load(Ordering::Acquire) && allowed(),
            "export withdrawn",
        )?;
        ensure(
            path.is_absolute()
                && path.as_os_str().as_encoded_bytes().len() <= 4096
                && path
                    .components()
                    .all(|p| matches!(p, Component::RootDir | Component::Normal(_)))
                && !crate::text_input::private_path(path),
            "output path",
        )?;
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| crate::Error::Rejected("output name".into()))?;
        ensure(
            name.len() <= 255
                && !name.starts_with('.')
                && !name.chars().any(|c| c.is_control() || "/\\".contains(c))
                && path
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| s.eq_ignore_ascii_case("md")),
            "Markdown output name",
        )?;
        let parent = path
            .parent()
            .ok_or_else(|| crate::Error::Rejected("output directory".into()))?
            .canonicalize()?;
        ensure(
            !crate::text_input::private_path(&parent),
            "private output directory",
        )?;
        let mut dir = open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        for part in parent.components().skip(1) {
            let Component::Normal(part) = part else {
                return Err(crate::Error::Rejected("output directory".into()));
            };
            dir = openat(
                &dir,
                part,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
        }
        let temp_name = format!(".pulse-result-{}", uuid::Uuid::new_v4());
        let fd = openat(
            &dir,
            temp_name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?;
        let temp = Temp {
            dir: &dir,
            name: temp_name,
        };
        let mut file = std::fs::File::from(fd);
        file.write_all(bytes)?;
        file.sync_all()?;
        ensure(
            !stop.load(Ordering::Acquire) && allowed(),
            "export withdrawn",
        )?;
        renameat_with(&dir, temp.name.as_str(), &dir, name, RenameFlags::NOREPLACE)?;
        published = true;
        sync(&dir)?;
        Ok(())
    })();
    match (attempt.is_ok(), published) {
        (true, _) => ExportStatus::Saved,
        (false, true) => ExportStatus::Unknown,
        _ => ExportStatus::NotSaved,
    }
}

#[cfg(test)]
mod tests;
