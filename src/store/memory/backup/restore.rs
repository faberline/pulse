//! Offline forward-only restore. Publication requires a durable external fence.
use super::*;
mod evidence;
mod files;
use files::{Prepared, Progress};

#[cfg(test)]
mod tests;

fn crash_point(guard: &MemoryGuard, point: &str) {
    #[cfg(test)]
    if std::env::var("PULSE_OWNED_RESTORE_CRASH").as_deref() == Ok(point)
        && std::env::var_os("PULSE_OWNED_RESTORE_DIR").is_some_and(|root| {
            PathBuf::from(root)
                .canonicalize()
                .is_ok_and(|root| guard.database == root.join("db"))
        })
    {
        std::process::exit(73);
    }
    #[cfg(not(test))]
    let _ = (guard, point);
}

#[derive(Debug)]
pub struct RestoreReport {
    pub generation: String,
    pub current_records_imported: bool,
    pub budget_held_through: String,
    pub already_committed: bool,
}

fn report(progress: &Progress, already_committed: bool) -> Result<RestoreReport> {
    let ready = progress
        .prepared
        .as_ref()
        .ok_or_else(|| Error::Rejected("restore image was not prepared".into()))?;
    Ok(RestoreReport {
        generation: progress.generation.clone(),
        current_records_imported: ready.imported_current,
        budget_held_through: ready.budget_floor.clone(),
        already_committed,
    })
}
fn validate_request(
    manifest: &Restrictions,
    owner: &str,
    versions: &Versions,
    now: i64,
) -> Result<()> {
    require(
        !owner.is_empty() && owner.len() <= 1024 && versions.valid() && now >= 0,
        "invalid offline restore request",
    )?;
    require(
        manifest.copy.is_none() && manifest.worker.is_none() && manifest.peer_restore.is_none(),
        "offline server restore cannot replace a local journal",
    )?;
    evidence::day(now)?;
    Ok(())
}
fn operations(guard: &MemoryGuard, secret: &Secret) -> Result<Vec<(PathBuf, Progress)>> {
    let root = files::root(guard);
    if !files::exists(&root)? {
        return Ok(Vec::new());
    }
    files::private_dir(guard, &root, false)?;
    let mut result = Vec::new();
    let mut entries = 0;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        entries += 1;
        require(entries <= 256, "restore directory capacity")?;
        require(result.len() < 64, "restore operation capacity")?;
        files::private_dir(guard, &entry.path(), false)?;
        if !files::exists(&entry.path().join("progress.json"))? {
            require(
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| uuid::Uuid::parse_str(name).is_ok()),
                "unknown restore directory",
            )?;
            for child in std::fs::read_dir(entry.path())? {
                let child = child?;
                require(
                    child.file_name() == "progress.partial",
                    "unknown unregistered restore file",
                )?;
                drop(guard.open_private(&child.path(), false, false)?);
            }
            // No plaintext work can start before progress.json is durable.
            // Preserve incomplete metadata, but do not grant it restore authority.
            continue;
        }
        let progress = files::load(guard, &entry.path(), secret)?;
        result.push((entry.path(), progress));
    }
    Ok(result)
}

pub(super) fn cleanup(guard: &MemoryGuard, secret: &Secret, now: i64) -> Result<Option<i64>> {
    let current = guard.read()?.and_then(|manifest| manifest.restore);
    let mut next: Option<i64> = None;
    for (dir, mut progress) in operations(guard, secret)? {
        if current
            .as_ref()
            .is_some_and(|fence| !fence.committed && fence.generation == progress.generation)
        {
            continue;
        }
        require(
            progress.created_at >= 0 && progress.created_at <= now,
            "recovery evidence clock moved backwards",
        )?;
        let progress_expiry = progress.created_at.saturating_add(RETENTION);
        if progress_expiry > now {
            next = Some(next.unwrap_or(i64::MAX).min(progress_expiry));
        }
        let published = current
            .as_ref()
            .is_some_and(|fence| fence.committed && fence.generation == progress.generation);
        if published && !progress.completed {
            progress.completed = true;
            files::save(guard, &dir, secret, &progress)?;
        }
        files::reset_unpublished(guard, &dir)?;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            require(
                name.to_str().is_some_and(|name| {
                    [
                        "progress.json",
                        "progress.partial",
                        "backup.pulsebak",
                        "main.evidence",
                        "main.partial",
                        "wal.evidence",
                        "wal.partial",
                        "shm.evidence",
                        "shm.partial",
                        "journal.evidence",
                        "journal.partial",
                    ]
                    .contains(&name)
                }),
                "unknown recovery evidence file",
            )?;
            drop(guard.open_private(&entry.path(), false, false)?);
        }
        let backup = dir.join("backup.pulsebak");
        if files::exists(&backup)? {
            let header = crypto::authenticate_header(
                &secret.key,
                guard.open_private(&backup, false, false)?,
            );
            let expiry = match header {
                Ok(header) => {
                    require(
                        header.keeper == secret.keeper,
                        "recovery backup keeper mismatch",
                    )?;
                    header.created_at.saturating_add(RETENTION)
                }
                Err(error) if !progress.completed && progress.abandoned => {
                    let _ = error;
                    progress.created_at.saturating_add(RETENTION)
                }
                Err(error) => return Err(error),
            };
            if expiry <= now {
                if progress.completed {
                    require(
                        files::digest(guard, &backup, false)? == progress.backup_digest,
                        "recovery backup changed",
                    )?;
                }
                std::fs::remove_file(&backup)?;
            } else {
                next = Some(next.unwrap_or(i64::MAX).min(expiry));
            }
        }
        for kind in files::Kind::ALL {
            let path = dir.join(kind.name());
            if files::exists(&path)? {
                let header = crypto::authenticate_recovery_header(
                    &secret.key,
                    guard.open_private(&path, false, false)?,
                )?;
                require(
                    header.keeper == secret.keeper && header.created_at <= now,
                    "recovery archive keeper or clock mismatch",
                )?;
                let expiry = header.created_at.saturating_add(RETENTION);
                if expiry <= now {
                    if let Some(artifact) = progress.prepared.as_ref().and_then(|ready| {
                        ready
                            .artifacts
                            .iter()
                            .find(|artifact| artifact.kind.ending() == kind.ending())
                    }) {
                        require(
                            files::digest(guard, &path, false)? == artifact.archive_digest,
                            "recovery archive changed",
                        )?;
                    }
                    std::fs::remove_file(&path)?;
                } else {
                    next = Some(next.unwrap_or(i64::MAX).min(expiry));
                }
            }
            let partial = path.with_extension("partial");
            if files::exists(&partial)? && now - progress.created_at >= RETENTION {
                drop(guard.open_private(&partial, false, false)?);
                std::fs::remove_file(partial)?;
            }
        }
        File::open(&dir)?.sync_all()?;
        if now - progress.created_at >= RETENTION && !published {
            for name in ["progress.partial", "progress.json"] {
                let path = dir.join(name);
                if files::exists(&path)? {
                    drop(guard.open_private(&path, false, false)?);
                    std::fs::remove_file(path)?;
                }
            }
            std::fs::remove_dir(&dir)?;
            File::open(dir.parent().unwrap())?.sync_all()?;
        }
    }
    Ok(next)
}

impl crate::DatabaseMaintenance {
    /// Replace a server DB only after checking an isolated image and archiving
    /// its originals. Call while also holding the legacy server process lock.
    pub async fn restore_backup(
        &self,
        input: &Path,
        owner: &str,
        versions: &Versions,
        now: i64,
    ) -> Result<RestoreReport> {
        let guard = Arc::new(MemoryGuard::target(self.database())?);
        let _lease = guard.lease().await?;
        let mut manifest = guard
            .read()?
            .ok_or_else(|| Error::Rejected("latest restrictions missing".into()))?;
        validate_request(&manifest, owner, versions, now)?;
        require(
            manifest
                .restore
                .as_ref()
                .is_none_or(|fence| fence.committed),
            "offline restore is incomplete; resume required",
        )?;
        let secret = read_secret(
            guard.open_private(&suffix(self.database(), ".backup-key"), false, false)?,
            &manifest.keeper,
        )?;
        files::validate_original(&guard)?;
        source_identity(self, owner, &manifest, &secret).await?;
        cleanup(&guard, &secret, now)?;
        // Abandon only unpublished attempts. Keep their encrypted evidence.
        let previous = operations(&guard, &secret)?;
        require(previous.len() < 64, "restore operation capacity")?;
        for (dir, mut old) in previous {
            if !old.completed && !old.abandoned {
                let published = manifest
                    .restore
                    .as_ref()
                    .is_some_and(|fence| fence.committed && fence.generation == old.generation);
                old.completed = published;
                old.abandoned = !published;
                files::save(&guard, &dir, &secret, &old)?;
                files::reset_unpublished(&guard, &dir)?;
            }
        }
        let input = files::bytes(&guard, input, crypto::MAX_DATABASE as u64 + 65536)?;
        let validation_secret = secret.clone();
        let input = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let (header, plain) = crypto::open(&validation_secret.key, input.as_slice())?;
            require(
                header.keeper == validation_secret.keeper
                    && header.created_at <= now
                    && now - header.created_at < RETENTION
                    && plain.len() >= 100
                    && plain.starts_with(b"SQLite format 3\0")
                    && u32::from_be_bytes(plain[60..64].try_into().unwrap())
                        == crate::store::SCHEMA as u32,
                "invalid restore backup binding, age or schema",
            )?;
            Ok(input)
        })
        .await
        .map_err(|_| Error::Rejected("restore backup validation stopped".into()))??;
        let generation = uuid::Uuid::new_v4().to_string();
        let root = files::root(&guard);
        files::private_dir(&guard, &root, true)?;
        let dir = root.join(&generation);
        files::private_dir(&guard, &dir, true)?;
        let mut progress = Progress {
            format: 1,
            schema: crate::store::SCHEMA,
            versions: versions.clone(),
            keeper: manifest.keeper.clone(),
            generation: generation.clone(),
            owner: owner.into(),
            created_at: now,
            previous_restore: manifest.restore.clone(),
            previous_format: manifest.format,
            backup_digest: format!("{:x}", Sha256::digest(&input)),
            prepared: None,
            completed: false,
            abandoned: false,
        };
        files::save(&guard, &dir, &secret, &progress)?;
        files::write_new(&guard, &dir.join("backup.pulsebak"), &input)?;
        drop(input);
        let prepared = prepare(self, &guard, &dir, &secret, &manifest, &progress, now).await;
        match prepared {
            Ok((ready, latest)) => {
                progress.prepared = Some(ready);
                manifest = latest;
            }
            Err(error) => {
                progress.abandoned = true;
                files::save(&guard, &dir, &secret, &progress)?;
                // Drop/close failures leave quarantine in this private namespace.
                let _ = files::reset_unpublished(&guard, &dir);
                return Err(error);
            }
        }
        files::save(&guard, &dir, &secret, &progress)?;
        crash_point(&guard, "prepared");
        manifest.restore = Some(RestoreFence {
            generation,
            committed: false,
        });
        manifest.format = 8;
        guard.write(&manifest)?;
        crash_point(&guard, "fenced");
        cutover(&guard, &dir, &secret, &mut manifest, &mut progress, now)?;
        report(&progress, false)
    }

    /// Resume the fenced operation. A completed publication never rewinds later
    /// tasks, costs or restrictions. Unpublished preparation is rebuilt fresh.
    pub async fn resume_restore(
        &self,
        owner: &str,
        versions: &Versions,
        now: i64,
    ) -> Result<RestoreReport> {
        let guard = Arc::new(MemoryGuard::target(self.database())?);
        let lease = guard.lease().await?;
        let mut manifest = guard
            .read()?
            .ok_or_else(|| Error::Rejected("latest restrictions missing".into()))?;
        validate_request(&manifest, owner, versions, now)?;
        let secret = read_secret(
            guard.open_private(&suffix(self.database(), ".backup-key"), false, false)?,
            &manifest.keeper,
        )?;
        let all = operations(&guard, &secret)?;
        files::validate_original(&guard)?;
        let unpublished: Vec<_> = all
            .iter()
            .filter(|(_, p)| {
                !p.completed
                    && !p.abandoned
                    && manifest
                        .restore
                        .as_ref()
                        .is_none_or(|fence| fence.committed && fence.generation != p.generation)
            })
            .cloned()
            .collect();
        if manifest
            .restore
            .as_ref()
            .is_none_or(|fence| fence.committed)
            && !unpublished.is_empty()
        {
            require(unpublished.len() == 1, "ambiguous restore preparation")?;
            let (dir, mut progress) = unpublished.into_iter().next().unwrap();
            require(
                progress.owner == owner
                    && progress
                        .previous_restore
                        .as_ref()
                        .map(|f| (&f.generation, f.committed))
                        == manifest
                            .restore
                            .as_ref()
                            .map(|f| (&f.generation, f.committed)),
                "stale restore preparation",
            )?;
            progress.abandoned = true;
            files::save(&guard, &dir, &secret, &progress)?;
            files::reset_unpublished(&guard, &dir)?;
            drop(lease);
            return self
                .restore_backup(&dir.join("backup.pulsebak"), owner, versions, now)
                .await;
        }
        if let Some(fence) = &manifest.restore {
            let (dir, mut progress) = all
                .iter()
                .find(|(_, p)| p.generation == fence.generation)
                .cloned()
                .ok_or_else(|| Error::Rejected("restore progress missing".into()))?;
            require(
                progress.owner == owner && !progress.abandoned,
                "restore progress owner mismatch",
            )?;
            if fence.committed {
                require(
                    source_identity(self, owner, &manifest, &secret).await?,
                    "published restore identity unavailable",
                )?;
                progress.completed = true;
                files::save(&guard, &dir, &secret, &progress)?;
                files::reset_unpublished(&guard, &dir)?;
                return report(&progress, true);
            }
            require(
                progress.schema == crate::store::SCHEMA && progress.versions == *versions,
                "restore candidate version differs; verified migration required",
            )?;
            let today = evidence::day(now)?;
            if progress.prepared.as_ref().is_some_and(|ready| {
                ready.next_memory_expiry.is_some_and(|expiry| expiry <= now)
                    || ready.budget_floor < today
            }) {
                refresh(
                    &guard,
                    &dir,
                    &secret,
                    &mut manifest,
                    &mut progress,
                    versions,
                    now,
                )
                .await?;
            }
            cutover(&guard, &dir, &secret, &mut manifest, &mut progress, now)?;
            return report(&progress, false);
        }
        let mut pending = all
            .into_iter()
            .filter(|(_, p)| !p.completed && !p.abandoned);
        let (dir, mut progress) = pending
            .next()
            .ok_or_else(|| Error::Rejected("no restore to resume".into()))?;
        require(
            pending.next().is_none() && progress.owner == owner,
            "ambiguous restore preparation",
        )?;
        progress.abandoned = true;
        files::save(&guard, &dir, &secret, &progress)?;
        files::reset_unpublished(&guard, &dir)?;
        drop(lease);
        self.restore_backup(&dir.join("backup.pulsebak"), owner, versions, now)
            .await
    }
}

async fn prepare(
    admission: &crate::DatabaseMaintenance,
    guard: &Arc<MemoryGuard>,
    dir: &Path,
    secret: &Secret,
    manifest: &Restrictions,
    progress: &Progress,
    now: i64,
) -> Result<(Prepared, Restrictions)> {
    let versions = &progress.versions;
    require(
        files::digest(guard, &dir.join("backup.pulsebak"), false)? == progress.backup_digest,
        "restore backup changed",
    )?;
    let candidate_dir = dir.join("candidate");
    let candidate = stage_backup(
        BackupSource {
            guard,
            manifest,
            owner: &progress.owner,
            secret: secret.clone(),
        },
        &dir.join("backup.pulsebak"),
        &candidate_dir,
        versions,
        now,
    )
    .await?;
    let local: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='local_meta'",
    )
    .fetch_one(&candidate.pool)
    .await?;
    require(
        local == 0,
        "offline server restore cannot replace a local journal",
    )?;
    let mut current = evidence::capture(admission, manifest, secret, &progress.owner).await?;
    let mut latest = candidate
        .memory_guard
        .read()?
        .ok_or_else(|| Error::Rejected("candidate restrictions missing".into()))?;
    let imported_current = if let Some(image) = current.image.take() {
        match stage_image(
            BackupSource {
                guard,
                manifest,
                owner: &progress.owner,
                secret: secret.clone(),
            },
            image,
            &dir.join("current"),
            versions,
            now,
            false,
        )
        .await
        {
            Ok(source) => {
                let restrictions = source
                    .memory_guard
                    .read()?
                    .ok_or_else(|| Error::Rejected("current restrictions missing".into()))?;
                combine_restrictions(&mut latest, &restrictions)?;
                source.close().await;
                evidence::merge(&candidate, &dir.join("current/candidate.db")).await?;
                true
            }
            Err(_) => false,
        }
    } else {
        false
    };
    candidate.memory_guard.write(&latest)?;
    let budget_floor =
        evidence::finish(&candidate, &current, &progress.generation, now, versions).await?;
    let next_memory_expiry =
        sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(expires_at) FROM memories")
            .fetch_one(&candidate.pool)
            .await?;
    let latest = candidate
        .memory_guard
        .read()?
        .ok_or_else(|| Error::Rejected("candidate restrictions missing".into()))?;
    super::super::checkpoint::truncate(&candidate.pool).await?;
    candidate.close().await;
    let image_digest = files::digest(guard, &candidate_dir.join("candidate.db"), true)?;
    let worker_guard = guard.clone();
    let worker_dir = dir.to_path_buf();
    let worker_secret = secret.clone();
    let artifacts = tokio::task::spawn_blocking(move || {
        files::archive(&worker_guard, &worker_dir, &worker_secret, now)
    })
    .await
    .map_err(|_| Error::Rejected("original evidence archive stopped".into()))??;
    Ok((
        Prepared {
            image_digest,
            imported_current,
            budget_floor,
            artifacts,
            next_memory_expiry,
            slot: files::Slot::Candidate,
            prior_images: Vec::new(),
        },
        latest,
    ))
}

fn combine_restrictions(target: &mut Restrictions, other: &Restrictions) -> Result<()> {
    require(
        target.keeper == other.keeper
            && target.copy.is_none()
            && other.copy.is_none()
            && target.worker.is_none()
            && other.worker.is_none()
            && target.peer_restore.is_none()
            && other.peer_restore.is_none(),
        "restore restriction keeper mismatch",
    )?;
    let before = serde_json::to_vec(&(&target.removed, &target.sources, &target.cancellations))?;
    target.epoch = target.epoch.max(other.epoch);
    for (id, revision) in &other.removed {
        let held = target.removed.entry(id.clone()).or_default();
        *held = (*held).max(*revision);
    }
    for (source, restriction) in &other.sources {
        if let Some(held) = target.sources.get_mut(source) {
            require(
                held.id == restriction.id,
                "conflicting source restriction identity",
            )?;
            held.revision = held.revision.max(restriction.revision);
            held.unknown |= restriction.unknown;
        } else {
            target.sources.insert(source.clone(), restriction.clone());
        }
    }
    for (id, cancellation) in &other.cancellations {
        if let Some(held) = target.cancellations.get_mut(id) {
            require(
                held.fingerprint == cancellation.fingerprint,
                "conflicting cancellation restriction",
            )?;
            held.current_changed |= cancellation.current_changed;
        } else {
            target
                .cancellations
                .insert(id.clone(), cancellation.clone());
        }
    }
    require(
        target.removed.len() <= 20100 && super::super::revocations::valid(target),
        "combined restore restriction capacity",
    )?;
    if before != serde_json::to_vec(&(&target.removed, &target.sources, &target.cancellations))? {
        target.epoch = target
            .epoch
            .checked_add(1)
            .ok_or_else(|| Error::Rejected("restore restriction epoch exhausted".into()))?;
    }
    Ok(())
}

async fn refresh(
    guard: &MemoryGuard,
    dir: &Path,
    secret: &Secret,
    manifest: &mut Restrictions,
    progress: &mut Progress,
    versions: &Versions,
    now: i64,
) -> Result<()> {
    require(
        now >= progress.created_at,
        "restore progress clock moved backwards",
    )?;
    let previous = progress
        .prepared
        .as_ref()
        .ok_or_else(|| Error::Rejected("restore image not prepared".into()))?
        .clone();
    files::verify_archives(guard, dir, secret, &previous)?;
    let old_stage = dir.join(previous.slot.name()).join("candidate.db");
    let source = if files::exists(&old_stage)? {
        old_stage
    } else {
        guard.database.clone()
    };
    require(
        files::digest(guard, &source, true)? == previous.image_digest,
        "paused restore image changed",
    )?;
    let mut bytes = files::bytes(guard, &source, crypto::MAX_DATABASE as u64)?;
    require(
        bytes.len() >= 100 && bytes.starts_with(b"SQLite format 3\0"),
        "paused restore image header missing",
    )?;
    bytes[60..64].copy_from_slice(&(crate::store::SCHEMA as u32).to_be_bytes());
    let slot = previous.slot.other();
    let destination = dir.join(slot.name());
    files::clean_stage(guard, &destination)?;
    let mut private = manifest.clone();
    private.restore.as_mut().unwrap().committed = true;
    let candidate = stage_image(
        BackupSource {
            guard,
            manifest: &private,
            owner: &progress.owner,
            secret: secret.clone(),
        },
        bytes,
        &destination,
        versions,
        now,
        true,
    )
    .await?;
    let budget_floor = previous.budget_floor.max(evidence::day(now)?);
    sqlx::query("UPDATE settings SET value=? WHERE key='restore_budget_floor_day'")
        .bind(&budget_floor)
        .execute(&candidate.pool)
        .await?;
    let latest = candidate
        .memory_guard
        .read()?
        .ok_or_else(|| Error::Rejected("refreshed restrictions missing".into()))?;
    let next_memory_expiry =
        sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(expires_at) FROM memories")
            .fetch_one(&candidate.pool)
            .await?;
    super::super::checkpoint::truncate(&candidate.pool).await?;
    candidate.close().await;
    let image_digest = files::digest(guard, &destination.join("candidate.db"), true)?;
    let mut prior_images = previous.prior_images;
    if !prior_images.contains(&previous.image_digest) {
        prior_images.push(previous.image_digest);
    }
    require(prior_images.len() <= 128, "restore refresh capacity")?;
    // Persist new negative evidence before replacing the authenticated image.
    combine_restrictions(manifest, &latest)?;
    guard.write(manifest)?;
    crash_point(guard, "refresh-restrictions");
    progress.prepared = Some(Prepared {
        image_digest,
        imported_current: previous.imported_current,
        budget_floor,
        artifacts: previous.artifacts,
        next_memory_expiry,
        slot,
        prior_images,
    });
    files::save(guard, dir, secret, progress)?;
    crash_point(guard, "refresh-prepared");
    Ok(())
}

fn cutover(
    guard: &MemoryGuard,
    dir: &Path,
    secret: &Secret,
    manifest: &mut Restrictions,
    progress: &mut Progress,
    now: i64,
) -> Result<()> {
    require(
        manifest
            .restore
            .as_ref()
            .is_some_and(|fence| !fence.committed && fence.generation == progress.generation)
            && progress.owner.len() <= 1024
            && !progress.abandoned,
        "restore fence does not match progress",
    )?;
    let ready = progress
        .prepared
        .as_ref()
        .ok_or_else(|| Error::Rejected("restore image not prepared".into()))?;
    require(
        now >= progress.created_at,
        "restore progress clock moved backwards",
    )?;
    require(
        ready.next_memory_expiry.is_none_or(|expiry| expiry > now),
        "paused restore needs memory expiry refresh",
    )?;
    files::verify_archives(guard, dir, secret, ready)?;
    let candidate = dir.join(ready.slot.name()).join("candidate.db");
    let installed = files::exists(&guard.database)?
        && files::digest(guard, &guard.database, true)
            .is_ok_and(|digest| digest == ready.image_digest);
    if !installed {
        require(
            files::digest(guard, &candidate, true)? == ready.image_digest,
            "restore candidate changed",
        )?;
        let ours = files::exists(&guard.database)?
            && files::digest(guard, &guard.database, true)
                .is_ok_and(|digest| ready.prior_images.contains(&digest));
        if ours {
            for kind in files::Kind::ALL.into_iter().skip(1) {
                require(
                    !files::exists(&suffix(&guard.database, kind.ending()))?,
                    "unexpected sidecar on paused image",
                )?;
            }
            std::fs::remove_file(&guard.database)?;
            File::open(guard.database.parent().unwrap())?.sync_all()?;
        } else {
            files::remove_original(guard, ready)?;
        }
        crash_point(guard, "removed-originals");
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            &candidate,
            rustix::fs::CWD,
            &guard.database,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(std::io::Error::from)?;
        File::open(guard.database.parent().unwrap())?.sync_all()?;
        File::open(candidate.parent().unwrap())?.sync_all()?;
    }
    crash_point(guard, "installed");
    // No original WAL may attach to the new main file, including after a crash.
    for kind in files::Kind::ALL.into_iter().skip(1) {
        require(
            !files::exists(&suffix(&guard.database, kind.ending()))?,
            "unexpected SQLite sidecar at activation",
        )?;
    }
    files::activate(guard, ready)?;
    crash_point(guard, "activated");
    files::clean_stage(guard, &dir.join("candidate"))?;
    files::clean_stage(guard, &dir.join("current"))?;
    files::clean_stage(guard, &dir.join("refresh"))?;
    manifest.restore.as_mut().unwrap().committed = true;
    guard.write(manifest)?;
    crash_point(guard, "published");
    progress.completed = true;
    files::save(guard, dir, secret, progress)
}
