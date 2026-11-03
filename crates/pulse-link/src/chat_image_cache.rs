//! Durable ownership of chat PNG files. No caller-selected file is erased.
use crate::{Result, artifact::*, ensure, journal::Journal, protocol::*};
use pulse_core::TaskMemorySource;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    digest: String,
    length: u32,
    directory_dev: u64,
    directory_ino: u64,
    dev: u64,
    ino: u64,
}

#[derive(Serialize, Deserialize)]
struct Entry {
    #[serde(flatten)]
    image: LocalArtifact,
    memory_sources: Vec<TaskMemorySource>,
    sequence: String,
    file: Claim,
    writing: bool,
}

struct Directory {
    file: File,
    path: PathBuf,
}
pub(crate) fn image(data: &str) -> Result<LocalArtifact> {
    Ok(serde_json::from_str::<Entry>(data)?.image)
}
fn private(meta: &std::fs::Metadata, directory: bool) -> bool {
    meta.uid() == rustix::process::geteuid().as_raw()
        && meta.mode() & 0o777 == if directory { 0o700 } else { 0o600 }
        && if directory {
            meta.is_dir()
        } else {
            meta.is_file() && meta.nlink() == 1
        }
}
impl Directory {
    fn open(journal: &Journal) -> Result<Self> {
        let path = journal.directory.join("artifacts");
        crate::journal::private_dir(&path)?;
        let file = File::from(rustix::fs::open(
            &path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?);
        let result = Self { file, path };
        result.validate(None)?;
        Ok(result)
    }
    fn validate(&self, claim: Option<&Claim>) -> Result<()> {
        let fd = self.file.metadata()?;
        let path = std::fs::symlink_metadata(&self.path)?;
        ensure(
            private(&fd, true)
                && private(&path, true)
                && (fd.dev(), fd.ino()) == (path.dev(), path.ino())
                && claim.is_none_or(|c| (c.directory_dev, c.directory_ino) == (fd.dev(), fd.ino())),
            "chat image directory identity changed",
        )
    }
    fn path(&self, digest: &str) -> PathBuf {
        self.path.join(format!("{digest}.png"))
    }
    fn open_file(&self, digest: &str, create: bool) -> Result<File> {
        ensure(digest_id(digest), "invalid cached image digest")?;
        let flags = if create {
            rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::CREATE | rustix::fs::OFlags::EXCL
        } else {
            rustix::fs::OFlags::RDONLY
        };
        Ok(File::from(rustix::fs::openat(
            &self.file,
            format!("{digest}.png"),
            flags | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?))
    }
    fn claim(&self, file: &File, artifact: &ArtifactRef) -> Result<Claim> {
        self.validate(None)?;
        let directory = self.file.metadata()?;
        let meta = file.metadata()?;
        ensure(
            private(&meta, false) && meta.len() <= u64::from(artifact.length),
            "unverified cached image file",
        )?;
        Ok(Claim {
            digest: artifact.digest.clone(),
            length: artifact.length,
            directory_dev: directory.dev(),
            directory_ino: directory.ino(),
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }
    fn read(&self, claim: &Claim, complete: bool) -> Result<Option<Vec<u8>>> {
        ensure(
            digest_id(&claim.digest) && (33..=MAX_PNG as u32).contains(&claim.length),
            "invalid image ownership receipt",
        )?;
        self.validate(Some(claim))?;
        let mut file = match self.open_file(&claim.digest, false) {
            Err(crate::Error::System(rustix::io::Errno::NOENT)) => return Ok(None),
            result => result?,
        };
        let meta = file.metadata()?;
        ensure(
            private(&meta, false)
                && (meta.dev(), meta.ino()) == (claim.dev, claim.ino)
                && meta.len() <= u64::from(claim.length),
            "cached image ownership changed",
        )?;
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(MAX_PNG as u64 + 1)
            .read_to_end(&mut bytes)?;
        if complete {
            ensure(
                bytes.len() == claim.length as usize && hash_bytes(&bytes) == claim.digest,
                "cached image bytes changed",
            )?;
            png_dimensions(&bytes)?;
        }
        Ok(Some(bytes))
    }
    fn remove(&self, claim: &Claim, complete: bool) -> Result<()> {
        if self.read(claim, complete)?.is_some() {
            self.validate(Some(claim))?;
            let stat = self.open_file(&claim.digest, false)?.metadata()?;
            ensure(
                private(&stat, false) && (stat.dev(), stat.ino()) == (claim.dev, claim.ino),
                "cached image changed before unlink",
            )?;
            rustix::fs::unlinkat(
                &self.file,
                format!("{}.png", claim.digest),
                rustix::fs::AtFlags::empty(),
            )?;
        }
        self.file.sync_all()?;
        Ok(())
    }
}

pub(crate) async fn cached(
    journal: &Journal,
    operation: &str,
    artifact: &ArtifactRef,
) -> Result<Option<Vec<u8>>> {
    let directory = Directory::open(journal)?;
    let expected = directory.path(&artifact.digest);
    if !expected.try_exists()? {
        return Ok(None);
    }
    let rows:Vec<String>=sqlx::query_scalar("SELECT value FROM local_meta WHERE key GLOB 'image:*' AND (json_extract(value,'$.file.digest')=? OR key=?)")
        .bind(&artifact.digest).bind(format!("image:{operation}")).fetch_all(&journal.db).await?;
    for data in rows {
        if let Ok(entry) = serde_json::from_str::<Entry>(&data) {
            if entry.file.digest == artifact.digest
                && !entry.writing
                && let Some(bytes) = directory.read(&entry.file, true)?
            {
                artifact.verify(&bytes)?;
                return Ok(Some(bytes));
            }
        } else if let Ok(old) = serde_json::from_str::<LocalArtifact>(&data) {
            // Adopt only an exact legacy journal reference, never an untracked file.
            if old.operation_id == operation
                && old.artifact == *artifact
                && std::path::Path::new(&old.path) == expected
            {
                let file = directory.open_file(&artifact.digest, false)?;
                let claim = directory.claim(&file, artifact)?;
                let bytes = directory
                    .read(&claim, true)?
                    .ok_or_else(|| crate::Error::Rejected("legacy image disappeared".into()))?;
                artifact.verify(&bytes)?;
                return Ok(Some(bytes));
            }
        }
    }
    Err(crate::Error::Rejected(
        "unowned chat image file retained".into(),
    ))
}

pub(crate) async fn save(
    journal: &Journal,
    operation: &str,
    picture: CapturedImage,
    sources: &[TaskMemorySource],
    sequence: &str,
) -> Result<LocalArtifact> {
    picture.artifact.verify(&picture.png)?;
    let directory = Directory::open(journal)?;
    let path = directory.path(&picture.artifact.digest);
    let exists = path.try_exists()?;
    if exists {
        cached(journal, operation, &picture.artifact).await?;
    } else {
        ensure(
            std::fs::read_dir(&directory.path)?.take(128).count() < 128,
            "chat artifact capacity reached",
        )?;
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM local_meta WHERE key GLOB 'image:*'")
        .fetch_one(&journal.db)
        .await?;
    let prior: Option<String> = sqlx::query_scalar("SELECT value FROM local_meta WHERE key=?")
        .bind(format!("image:{operation}"))
        .fetch_optional(&journal.db)
        .await?;
    ensure(
        count < 1000 || prior.is_some(),
        "chat image reference capacity reached",
    )?;
    if let Some(data) = prior {
        let value: serde_json::Value = serde_json::from_str(&data)?;
        ensure(
            value["erased"] != true && value["writing"] != true,
            "image cleanup still pending",
        )?;
        if let Ok(previous) = serde_json::from_str::<Entry>(&data) {
            ensure(
                previous.memory_sources == sources && previous.image.artifact == picture.artifact,
                "cached image lineage cannot change",
            )?;
        } else {
            let previous: LocalArtifact = serde_json::from_str(&data)?;
            ensure(
                previous.operation_id == operation && previous.artifact == picture.artifact,
                "legacy cached image binding mismatch",
            )?;
        }
    }
    let mut file = directory.open_file(&picture.artifact.digest, !exists)?;
    let claim = directory.claim(&file, &picture.artifact)?;
    let image = LocalArtifact {
        operation_id: operation.into(),
        artifact: picture.artifact,
        path: path
            .to_str()
            .ok_or_else(|| crate::Error::Rejected("artifact path encoding".into()))?
            .into(),
    };
    let entry = Entry {
        image: image.clone(),
        memory_sources: sources.to_vec(),
        sequence: sequence.into(),
        file: claim.clone(),
        writing: !exists,
    };
    let key = format!("image:{operation}");
    if let Err(error)=sqlx::query("INSERT INTO local_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        .bind(&key).bind(serde_json::to_string(&entry)?).execute(&journal.db).await {
        if !exists { directory.remove(&claim,false)?; }
        return Err(error.into());
    }
    if !exists {
        // The FULL-synchronous SQL intent precedes every private PNG byte.
        file.write_all(&picture.png)?;
        file.sync_all()?;
        directory.file.sync_all()?;
        sqlx::query(
            "UPDATE local_meta SET value=json_set(value,'$.writing',json('false')) WHERE key=?",
        )
        .bind(key)
        .execute(&journal.db)
        .await?;
    }
    Ok(image)
}

pub(crate) async fn drain(journal: &Journal) -> Result<()> {
    let _io = journal.image_io.lock().await;
    let mut tx = journal.db.begin_with("BEGIN IMMEDIATE").await?;
    pulse_core::Store::prune_local_memory_projection_in(&mut tx, Some(crate::now()), false).await?;
    tx.commit().await?;
    let rows:Vec<(String,String)>=sqlx::query_as("SELECT key,value FROM local_meta WHERE key GLOB 'image:*' AND (json_extract(value,'$.erased')=1 OR json_extract(value,'$.writing')=1)")
        .fetch_all(&journal.db).await?;
    if rows.is_empty() {
        return Ok(());
    }
    let directory = Directory::open(journal)?;
    let mut groups =
        std::collections::BTreeMap::<String, (Claim, bool, Vec<(String, String)>)>::new();
    for (key, data) in rows {
        let value: serde_json::Value = serde_json::from_str(&data)?;
        let claim: Claim = serde_json::from_value(value["file"].clone())?;
        let group = groups
            .entry(claim.digest.clone())
            .or_insert_with(|| (claim.clone(), false, Vec::new()));
        ensure(group.0 == claim, "conflicting pending image ownership")?;
        group.1 |= value["writing"] != true && value["writing"] != 1;
        group.2.push((key, data));
    }
    for (_, (claim, complete, receipts)) in groups {
        // Many message references may share one PNG. Verify each actual file once.
        let others:Vec<String>=sqlx::query_scalar("SELECT DISTINCT json_extract(value,'$.file') FROM local_meta WHERE key GLOB 'image:*' AND json_extract(value,'$.file.digest')=? AND coalesce(json_extract(value,'$.erased'),0)=0 AND coalesce(json_extract(value,'$.writing'),0)=0")
            .bind(&claim.digest).fetch_all(&journal.db).await?;
        if others.is_empty() {
            directory.remove(&claim, complete)?;
        } else {
            for other in others {
                ensure(
                    serde_json::from_str::<Claim>(&other)? == claim,
                    "conflicting image ownership references",
                )?;
            }
            ensure(
                directory.read(&claim, true)?.is_some(),
                "referenced image missing",
            )?;
        }
        let mut tx = journal.db.begin_with("BEGIN IMMEDIATE").await?;
        for (key, data) in receipts {
            let deleted = sqlx::query("DELETE FROM local_meta WHERE key=? AND value=?")
                .bind(key)
                .bind(data)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            ensure(deleted == 1, "image receipt changed during cleanup")?;
        }
        tx.commit().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
