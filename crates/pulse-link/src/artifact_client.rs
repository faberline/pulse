use crate::{Result, artifact::*, client::Config, ensure, journal::Journal, protocol::*};
use pulse_core::TaskMemorySource;
use sqlx::Row;

struct Request {
    artifact: ArtifactRef,
    sources: Vec<TaskMemorySource>,
    sequence: String,
    identity: String,
}

impl Journal {
    async fn image_request(&self, operation_id: &str, digest: &str) -> Result<Request> {
        ensure(
            self.role == ClientRole::Chat && digest_id(digest) && operation_id.len() <= 256,
            "artifact request role or identity",
        )?;
        let identity = self.text_input_identity()?;
        let row = sqlx::query("SELECT value,(SELECT value FROM local_meta WHERE key='cache_sequence') AS sequence FROM local_meta WHERE key='cache'")
            .fetch_optional(&self.db).await?
            .ok_or_else(|| crate::Error::Rejected("artifact is not in current evidence".into()))?;
        let view: NativeState = serde_json::from_str(&row.get::<String, _>("value"))?;
        let message = view
            .messages
            .iter()
            .find(|m| m.artifact_operation_id.as_ref().unwrap_or(&m.id) == operation_id)
            .ok_or_else(|| crate::Error::Rejected("artifact is not in current evidence".into()))?;
        let artifact = message
            .artifact
            .clone()
            .ok_or_else(|| crate::Error::Rejected("artifact is not in current evidence".into()))?;
        let sources = message
            .memory_sources
            .clone()
            .unwrap_or(view.memory_sources);
        ensure(
            artifact.digest == digest,
            "artifact request digest mismatch",
        )?;
        artifact.validate()?;
        if !sources.is_empty() {
            ensure(
                self.projection_sources_allowed(&sources, crate::now())
                    .await?,
                "artifact source unavailable",
            )?;
        }
        Ok(Request {
            artifact,
            sources,
            sequence: row.get("sequence"),
            identity,
        })
    }

    pub async fn artifact_request(&self, operation_id: &str, digest: &str) -> Result<ArtifactRef> {
        let _lifecycle = self.memory_lifecycle.lock().await;
        Ok(self.image_request(operation_id, digest).await?.artifact)
    }

    /// Network input stays in bounded memory. Only a still-current request may write a file.
    pub async fn load_artifact(
        &self,
        config: &Config,
        credential: &Credential,
        operation_id: &str,
        digest: &str,
    ) -> Result<LocalArtifact> {
        let (request, cached) = {
            let _lifecycle = self.memory_lifecycle.lock().await;
            self.require_memory_identity(credential)?;
            ensure(
                config.server.as_str() == self.server
                    && config.data_dir.canonicalize()? == self.directory,
                "artifact configuration binding mismatch",
            )?;
            let request = self.image_request(operation_id, digest).await?;
            let _io = self.image_io.lock().await;
            let cached =
                crate::chat_image_cache::cached(self, operation_id, &request.artifact).await?;
            (request, cached)
        };
        let picture = match cached {
            Some(png) => CapturedImage {
                artifact: request.artifact.clone(),
                png,
            },
            None => {
                let pending = download(config, credential, operation_id, &request.artifact);
                tokio::pin!(pending);
                let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
                loop {
                    tokio::select! {
                        result=&mut pending=>break result?,
                        _=tick.tick()=>{
                            let _lifecycle=self.memory_lifecycle.lock().await;
                            self.require_memory_identity(credential)?;
                            let current=self.image_request(operation_id,digest).await?;
                            ensure(current.identity==request.identity && current.artifact==request.artifact
                                && current.sources==request.sources,"artifact source changed during download")?;
                        }
                    }
                }
            }
        };
        let saved = async {
            let _lifecycle = self.memory_lifecycle.lock().await;
            self.require_memory_identity(credential)?;
            let current = self.image_request(operation_id, digest).await?;
            ensure(
                current.identity == request.identity
                    && current.artifact == request.artifact
                    && current.sources == request.sources,
                "artifact source changed during download",
            )?;
            let _source = if current.sources.is_empty() {
                None
            } else {
                Some(
                    self.core
                        .lease_local_projection_sources(
                            &self.device,
                            &hash(&self.server)?,
                            &current.sources,
                            crate::now(),
                        )
                        .await?,
                )
            };
            let _io = self.image_io.lock().await;
            crate::chat_image_cache::save(
                self,
                operation_id,
                picture,
                &current.sources,
                &current.sequence,
            )
            .await
        }
        .await;
        if saved.is_err() {
            self.flush_memory().await?;
        }
        saved
    }
}

pub async fn download(
    config: &Config,
    credential: &Credential,
    operation_id: &str,
    artifact: &ArtifactRef,
) -> Result<CapturedImage> {
    ensure(
        credential.role == ClientRole::Chat && operation_id.len() <= 256,
        "artifact download requires chat",
    )?;
    artifact.validate()?;
    let mut url = config.server.join("v1/artifacts/")?;
    url.path_segments_mut()
        .map_err(|_| crate::Error::Rejected("artifact server origin".into()))?
        .pop_if_empty()
        .push(operation_id)
        .push(&artifact.digest);
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|_| crate::Error::Rejected("artifact client setup failed".into()))?;
    let mut response = client
        .get(url)
        .bearer_auth(&credential.token)
        .send()
        .await
        .map_err(|_| crate::Error::Rejected("artifact download failed".into()))?;
    ensure(
        response.status() == reqwest::StatusCode::OK
            && response
                .headers()
                .get("content-type")
                .and_then(|h| h.to_str().ok())
                == Some("image/png"),
        "artifact download rejected",
    )?;
    let mut png = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| crate::Error::Rejected("artifact response failed".into()))?
    {
        ensure(
            png.len() + chunk.len() <= artifact.length as usize,
            "artifact response too large",
        )?;
        png.extend_from_slice(&chunk);
    }
    artifact.verify(&png)?;
    Ok(CapturedImage {
        artifact: artifact.clone(),
        png,
    })
}
