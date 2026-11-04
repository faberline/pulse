//! Private authenticated restore progress and owned artifacts.
use super::*;
use std::io::{Seek, SeekFrom};

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Kind {
    Main,
    Wal,
    Shm,
    Journal,
}
impl Kind {
    pub(super) const ALL: [Self; 4] = [Self::Main, Self::Wal, Self::Shm, Self::Journal];
    pub(super) fn ending(self) -> &'static str {
        match self {
            Self::Main => "",
            Self::Wal => "-wal",
            Self::Shm => "-shm",
            Self::Journal => "-journal",
        }
    }
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Main => "main.evidence",
            Self::Wal => "wal.evidence",
            Self::Shm => "shm.evidence",
            Self::Journal => "journal.evidence",
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Artifact {
    pub kind: Kind,
    pub digest: String,
    pub archive_digest: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Prepared {
    pub image_digest: String,
    pub imported_current: bool,
    pub budget_floor: String,
    pub artifacts: Vec<Artifact>,
    pub next_memory_expiry: Option<i64>,
    pub slot: Slot,
    pub prior_images: Vec<String>,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Slot {
    Candidate,
    Refresh,
}
impl Slot {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Refresh => "refresh",
        }
    }
    pub(super) fn other(self) -> Self {
        match self {
            Self::Candidate => Self::Refresh,
            Self::Refresh => Self::Candidate,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Progress {
    pub format: u8,
    pub schema: i64,
    pub versions: Versions,
    pub keeper: String,
    pub generation: String,
    pub owner: String,
    pub created_at: i64,
    pub previous_restore: Option<RestoreFence>,
    pub previous_format: u8,
    pub backup_digest: String,
    pub prepared: Option<Prepared>,
    pub completed: bool,
    pub abandoned: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Signed {
    payload: Progress,
    proof: Vec<u8>,
}

pub(super) fn root(guard: &MemoryGuard) -> PathBuf {
    suffix(&guard.database, ".restores")
}
pub(super) fn private_dir(guard: &MemoryGuard, path: &Path, create: bool) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    if create {
        match std::fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => File::open(path.parent().unwrap())?.sync_all()?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    let metadata = std::fs::symlink_metadata(path)?;
    require(
        metadata.is_dir()
            && metadata.uid() == guard.owner
            && metadata.permissions().mode() & 0o077 == 0,
        "restore directory must be private and owned",
    )
}
pub(super) fn exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
pub(super) fn bytes(guard: &MemoryGuard, path: &Path, limit: u64) -> Result<Vec<u8>> {
    let file = guard.open_private(path, false, false)?;
    require(file.metadata()?.len() <= limit, "restore artifact capacity")?;
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    require(bytes.len() as u64 <= limit, "restore artifact capacity")?;
    Ok(bytes)
}
pub(super) fn digest(guard: &MemoryGuard, path: &Path, mask: bool) -> Result<String> {
    let mut file = guard.open_private(path, false, false)?;
    require(
        file.metadata()?.len() <= crypto::MAX_DATABASE as u64 + if mask { 0 } else { 65536 },
        "restore file capacity",
    )?;
    let mut hash = Sha256::new();
    let mut block = [0; 65536];
    let mut offset = 0;
    loop {
        let count = file.read(&mut block)?;
        if count == 0 {
            break;
        }
        let start = offset.max(60);
        let end = (offset + count).min(64);
        if mask && start < end {
            block[start - offset..end - offset].fill(0);
        }
        hash.update(&block[..count]);
        offset += count;
    }
    if mask {
        require(offset >= 100, "restore database header missing")?;
    }
    Ok(format!("{:x}", hash.finalize()))
}
pub(super) fn write_new(guard: &MemoryGuard, path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = guard.open_private(path, true, true)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}
fn proof_bytes(progress: &Progress) -> Result<Vec<u8>> {
    let mut bytes = b"pulse-restore-progress-v1\0".to_vec();
    bytes.extend(serde_json::to_vec(progress)?);
    Ok(bytes)
}
pub(super) fn save(
    guard: &MemoryGuard,
    dir: &Path,
    secret: &Secret,
    progress: &Progress,
) -> Result<()> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &secret.key);
    let proof = ring::hmac::sign(&key, &proof_bytes(progress)?)
        .as_ref()
        .to_vec();
    let temporary = dir.join("progress.partial");
    if exists(&temporary)? {
        drop(guard.open_private(&temporary, false, false)?);
        std::fs::remove_file(&temporary)?;
    }
    write_new(
        guard,
        &temporary,
        &serde_json::to_vec(&Signed {
            payload: progress.clone(),
            proof,
        })?,
    )?;
    std::fs::rename(temporary, dir.join("progress.json"))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}
pub(super) fn load(guard: &MemoryGuard, dir: &Path, secret: &Secret) -> Result<Progress> {
    private_dir(guard, dir, false)?;
    let signed: Signed =
        serde_json::from_slice(&bytes(guard, &dir.join("progress.json"), 1024 * 1024)?)
            .map_err(|_| Error::Rejected("invalid restore progress".into()))?;
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &secret.key);
    ring::hmac::verify(&key, &proof_bytes(&signed.payload)?, &signed.proof)
        .map_err(|_| Error::Rejected("restore progress authentication failed".into()))?;
    let p = signed.payload;
    let hash = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    require(
        p.format == 1
            && p.keeper == secret.keeper
            && !p.owner.is_empty()
            && p.owner.len() <= 1024
            && p.created_at >= 0
            && p.versions.valid()
            && p.schema > 0
            && hash(&p.backup_digest)
            && uuid::Uuid::parse_str(&p.generation).is_ok_and(|id| id.to_string() == p.generation)
            && dir
                .file_name()
                .is_some_and(|name| name == p.generation.as_str())
            && p.prepared.as_ref().is_none_or(|ready| {
                ready.artifacts.len() <= 4
                    && hash(&ready.image_digest)
                    && ready.prior_images.len() <= 128
                    && ready.prior_images.iter().all(|value| hash(value))
                    && ready.artifacts.iter().enumerate().all(|(i, artifact)| {
                        hash(&artifact.digest)
                            && hash(&artifact.archive_digest)
                            && !ready.artifacts[..i]
                                .iter()
                                .any(|prior| prior.kind.ending() == artifact.kind.ending())
                    })
            }),
        "restore progress binding mismatch",
    )?;
    Ok(p)
}
pub(super) fn archive(
    guard: &MemoryGuard,
    dir: &Path,
    secret: &Secret,
    now: i64,
) -> Result<Vec<Artifact>> {
    let mut artifacts = Vec::new();
    let mut total = 0;
    for kind in Kind::ALL {
        let path = suffix(&guard.database, kind.ending());
        if !exists(&path)? {
            continue;
        }
        let bytes = bytes(guard, &path, crypto::MAX_DATABASE as u64)?;
        total += bytes.len();
        require(
            total <= 2 * crypto::MAX_DATABASE,
            "original recovery evidence exceeds 1 GiB",
        )?;
        let original_digest = format!("{:x}", Sha256::digest(&bytes));
        let target = dir.join(kind.name());
        let temporary = target.with_extension("partial");
        let mut output = guard.open_private(&temporary, true, true)?;
        let header = crypto::Header {
            version: 1,
            keeper: secret.keeper.clone(),
            id: uuid::Uuid::new_v4().to_string(),
            created_at: now,
            length: bytes.len(),
        };
        let archive_digest = crypto::seal_recovery(&secret.key, &header, &bytes, &mut output)?;
        output.sync_all()?;
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            temporary,
            rustix::fs::CWD,
            &target,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(std::io::Error::from)?;
        File::open(dir)?.sync_all()?;
        artifacts.push(Artifact {
            kind,
            digest: original_digest,
            archive_digest,
        });
    }
    Ok(artifacts)
}

pub(super) fn validate_original(guard: &MemoryGuard) -> Result<()> {
    let mut total = 0;
    for kind in Kind::ALL {
        let path = suffix(&guard.database, kind.ending());
        if exists(&path)? {
            let file = guard.open_private(&path, false, false)?;
            let length = file.metadata()?.len();
            require(
                length <= crypto::MAX_DATABASE as u64,
                "original recovery artifact capacity",
            )?;
            total += length;
        }
    }
    require(
        total <= (2 * crypto::MAX_DATABASE) as u64,
        "original recovery evidence exceeds 1 GiB",
    )
}
pub(super) fn verify_archives(
    guard: &MemoryGuard,
    dir: &Path,
    secret: &Secret,
    ready: &Prepared,
) -> Result<()> {
    for artifact in &ready.artifacts {
        let path = dir.join(artifact.kind.name());
        require(
            digest(guard, &path, false)? == artifact.archive_digest,
            "recovery evidence changed",
        )?;
        let (header, bytes) =
            crypto::open_recovery(&secret.key, guard.open_private(&path, false, false)?)?;
        require(
            header.keeper == secret.keeper
                && format!("{:x}", Sha256::digest(&bytes)) == artifact.digest,
            "recovery evidence binding mismatch",
        )?;
    }
    Ok(())
}
pub(super) fn remove_original(guard: &MemoryGuard, ready: &Prepared) -> Result<()> {
    // Check every sidecar before removing any original file.
    for kind in Kind::ALL {
        let path = suffix(&guard.database, kind.ending());
        if !exists(&path)? {
            continue;
        }
        let artifact = ready
            .artifacts
            .iter()
            .find(|artifact| artifact.kind.ending() == kind.ending())
            .ok_or_else(|| Error::Rejected("unexpected original database artifact".into()))?;
        require(
            digest(guard, &path, false)? == artifact.digest,
            "original database changed during restore",
        )?;
    }
    for kind in Kind::ALL {
        let path = suffix(&guard.database, kind.ending());
        if !exists(&path)? {
            continue;
        }
        std::fs::remove_file(path)?;
        File::open(guard.database.parent().unwrap())?.sync_all()?;
        if matches!(kind, Kind::Main) {
            super::crash_point(guard, "removed-main");
        }
    }
    Ok(())
}
pub(super) fn activate(guard: &MemoryGuard, ready: &Prepared) -> Result<()> {
    require(
        digest(guard, &guard.database, true)? == ready.image_digest,
        "installed restore image changed",
    )?;
    let mut file = guard.open_private(&guard.database, false, false)?;
    let mut header = [0; 16];
    file.read_exact(&mut header)?;
    require(
        &header == b"SQLite format 3\0",
        "invalid installed SQLite header",
    )?;
    file.seek(SeekFrom::Start(60))?;
    #[cfg(test)]
    if std::env::var("PULSE_OWNED_RESTORE_CRASH").as_deref() == Ok("torn-activation") {
        file.write_all(&[0, 0])?;
        file.sync_all()?;
        super::crash_point(guard, "torn-activation");
        file.seek(SeekFrom::Start(60))?;
    }
    file.write_all(&(crate::store::SCHEMA as u32).to_be_bytes())?;
    file.sync_all()?;
    File::open(guard.database.parent().unwrap())?.sync_all()?;
    Ok(())
}
pub(super) fn clean_stage(guard: &MemoryGuard, dir: &Path) -> Result<()> {
    if !exists(dir)? {
        return Ok(());
    }
    private_dir(guard, dir, false)?;
    let _admission = crate::DatabaseMaintenance::try_acquire(dir.join("candidate.db"))?;
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| Error::Rejected("unknown restore staging name".into()))?;
        let known = [
            "candidate.db",
            "candidate.db-wal",
            "candidate.db-shm",
            "candidate.db-journal",
            "candidate.db.memory-restrictions.json",
            "candidate.db.memory-lock",
            "candidate.db.use-lock",
        ]
        .contains(&name)
            || name
                .strip_prefix("candidate.db.memory-restrictions.")
                .and_then(|name| name.strip_suffix(".tmp"))
                .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok());
        require(known, "unknown restore staging file")?;
        drop(guard.open_private(&entry.path(), false, false)?);
        paths.push(entry.path());
    }
    for path in paths {
        std::fs::remove_file(path)?;
    }
    std::fs::remove_dir(dir)?;
    File::open(dir.parent().unwrap())?.sync_all()?;
    Ok(())
}
pub(super) fn reset_unpublished(guard: &MemoryGuard, dir: &Path) -> Result<()> {
    clean_stage(guard, &dir.join("candidate"))?;
    clean_stage(guard, &dir.join("current"))?;
    clean_stage(guard, &dir.join("refresh"))?;
    // An unpublished attempt may contain the last readable original evidence.
    // Keep its encrypted archives until authenticated retention cleanup.
    File::open(dir)?.sync_all()?;
    Ok(())
}
