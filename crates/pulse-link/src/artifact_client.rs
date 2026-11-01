use crate::{
    Result,
    artifact::*,
    client::Config,
    ensure,
    journal::{Journal, private_dir, private_write},
    protocol::*,
};
use std::path::Path;
impl Journal {
    pub async fn artifact_request(&self, operation_id: &str, digest: &str) -> Result<ArtifactRef> {
        ensure(
            self.role == ClientRole::Chat && digest_id(digest) && operation_id.len() <= 256,
            "artifact request role or identity",
        )?;
        let view = self.view().await?;
        let artifact = view
            .messages
            .iter()
            .find(|m| m.id == operation_id)
            .and_then(|m| m.artifact.clone())
            .ok_or_else(|| crate::Error::Rejected("artifact is not in current evidence".into()))?;
        ensure(
            artifact.digest == digest,
            "artifact request digest mismatch",
        )?;
        artifact.validate()?;
        Ok(artifact)
    }
    pub async fn save_artifact(&self, image: &LocalArtifact) -> Result<()> {
        self.artifact_request(&image.operation_id, &image.artifact.digest)
            .await?;
        sqlx::query("INSERT INTO local_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(format!("image:{}",image.operation_id)).bind(serde_json::to_string(image)?).execute(&self.db).await?;
        Ok(())
    }
}
pub async fn download(
    config: &Config,
    credential: &Credential,
    operation_id: &str,
    artifact: &ArtifactRef,
) -> Result<LocalArtifact> {
    ensure(
        credential.role == ClientRole::Chat && operation_id.len() <= 256,
        "artifact download requires chat",
    )?;
    artifact.validate()?;
    let directory = config.data_dir.canonicalize()?.join("artifacts");
    private_dir(&directory)?;
    let path = directory.join(format!("{}.png", artifact.digest));
    if path.exists() {
        artifact.verify(&read_png(&path)?)?;
    } else {
        ensure(
            std::fs::read_dir(&directory)?.take(128).count() < 128,
            "chat artifact capacity reached",
        )?;
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
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| crate::Error::Rejected("artifact response failed".into()))?
        {
            ensure(
                bytes.len() + chunk.len() <= artifact.length as usize,
                "artifact response too large",
            )?;
            bytes.extend_from_slice(&chunk);
        }
        artifact.verify(&bytes)?;
        private_write(&path, &bytes)?;
    }
    Ok(LocalArtifact {
        operation_id: operation_id.into(),
        artifact: artifact.clone(),
        path: path
            .to_str()
            .ok_or_else(|| crate::Error::Rejected("artifact path encoding".into()))?
            .into(),
    })
}
fn read_png(path: &Path) -> Result<Vec<u8>> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    ensure(
        file.metadata()?.is_file() && file.metadata()?.len() <= MAX_PNG as u64,
        "invalid cached artifact",
    )?;
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(
        &mut std::io::Read::take(file, MAX_PNG as u64 + 1),
        &mut bytes,
    )?;
    Ok(bytes)
}
