use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Resource {
    version: u8,
    id: String,
    runtime: String,
    parent: Directory,
    path: PathBuf,
    identity: Option<Directory>,
    kind: ResourceKind,
    #[serde(default)]
    owner: Option<String>,
}
#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResourceKind {
    Profile,
    Images,
}
pub(crate) enum OwnedDirectory {
    Temporary(tempfile::TempDir),
    Persistent {
        identity: Directory,
        record: PathBuf,
    },
}
impl OwnedDirectory {
    pub(crate) fn remove(&self) -> Result<()> {
        match self {
            Self::Temporary(dir) => Ok(std::fs::remove_dir_all(dir.path())?),
            Self::Persistent { identity, .. } => identity.remove(),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Temporary(_) => Ok(()),
            Self::Persistent { identity, .. } => identity.valid(),
        }
    }
    pub(crate) fn owner(&self) -> Option<&str> {
        match self {
            Self::Temporary(_) => None,
            Self::Persistent { identity, .. } => identity.owner.as_deref(),
        }
    }
    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::Temporary(dir) => dir.path(),
            Self::Persistent { identity, .. } => &identity.path,
        }
    }
    pub(crate) fn keep(self) {
        if let Self::Temporary(dir) = self {
            let _ = dir.keep();
        }
    }
    pub(crate) fn close(self) -> Result<()> {
        match self {
            Self::Temporary(dir) => {
                if !exists(dir.path())? {
                    let _ = dir.keep();
                    Ok(())
                } else {
                    Ok(dir.close()?)
                }
            }
            Self::Persistent { identity, record } => {
                identity.remove()?;
                drop(private_file(&record, false)?);
                std::fs::remove_file(&record)?;
                File::open(record.parent().unwrap())?.sync_all()?;
                Ok(())
            }
        }
    }
}
impl Registry {
    fn resources(&self) -> PathBuf {
        self.root.parent().unwrap().join("resources")
    }
    pub(crate) fn create_resource(&self, parent: &Path, prefix: &str) -> Result<OwnedDirectory> {
        self.create_kind(parent, prefix, ResourceKind::Profile)
    }
    pub(crate) fn create_images(&self, parent: &Path, prefix: &str) -> Result<OwnedDirectory> {
        self.create_kind(parent, prefix, ResourceKind::Images)
    }
    fn create_kind(
        &self,
        parent: &Path,
        prefix: &str,
        kind: ResourceKind,
    ) -> Result<OwnedDirectory> {
        use std::os::unix::fs::DirBuilderExt;
        ensure(
            prefix.len() <= 128
                && prefix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b)),
            "guardian resource prefix",
        )?;
        crate::journal::private_dir(parent)?;
        let parent = Directory::capture(parent)?;
        ensure(
            parent
                .path
                .starts_with(self.root.parent().unwrap().parent().unwrap()),
            "guardian resource outside local data directory",
        )?;
        let root = self.resources();
        crate::journal::private_dir(&root)?;
        ensure(
            std::fs::read_dir(&root)?.take(128).count() < 128,
            "guardian resource capacity",
        )?;
        let id = uuid::Uuid::new_v4().to_string();
        let path = parent.path.join(format!("{prefix}{id}"));
        let record = root.join(format!("{id}.json"));
        // Commit an unpredictable marker before mkdir. Inodes can be reused
        // after the executor dies, so inode equality alone cannot own content.
        let mut resource = Resource {
            version: 2,
            id,
            runtime: self.runtime.clone(),
            parent,
            path,
            identity: None,
            kind,
            owner: Some(uuid::Uuid::new_v4().to_string()),
        };
        write(&record, &resource)?;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&resource.path)?;
        File::open(&resource.parent.path)?.sync_all()?;
        write(
            &resource.path.join(OWNER_FILE),
            resource.owner.as_ref().unwrap(),
        )?;
        let identity = Directory::capture(&resource.path)?;
        resource.identity = Some(identity.clone());
        write(&record, &resource)?;
        Ok(OwnedDirectory::Persistent { identity, record })
    }
    pub(super) fn recover_resources(&self) -> Result<()> {
        let root = self.resources();
        if !exists(&root)? {
            return Ok(());
        }
        Directory::capture(&root)?;
        let entries = std::fs::read_dir(&root)?.collect::<std::io::Result<Vec<_>>>()?;
        ensure(entries.len() <= 256, "guardian resource recovery capacity")?;
        for entry in &entries {
            let path = entry.path();
            // Partial writes precede publication. A complete matching JSON
            // record, or its absence before mkdir, is handled below.
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| crate::Error::Rejected("unknown resource record".into()))?;
            ensure(
                name.strip_suffix(".json")
                    .or_else(|| name.strip_suffix(".partial"))
                    .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok_and(|v| v.to_string() == id)),
                "unknown resource record",
            )?;
            drop(private_file(&path, false)?);
        }
        for entry in entries {
            let path = entry.path();
            if path.extension().is_some_and(|s| s == "partial") {
                if exists(&path.with_extension("json"))? {
                    std::fs::remove_file(&path)?;
                    continue;
                }
                // No path was created before the first complete JSON record.
                std::fs::remove_file(&path)?;
                continue;
            }
            let resource: Resource = read(&path)?;
            ensure(
                ((resource.version == 1 && resource.owner.is_none())
                    || (resource.version == 2
                        && resource.owner.as_ref().is_some_and(|owner| {
                            uuid::Uuid::parse_str(owner).is_ok_and(|id| id.to_string() == *owner)
                        })))
                    && uuid::Uuid::parse_str(&resource.runtime).is_ok()
                    && path
                        .file_stem()
                        .is_some_and(|name| name == resource.id.as_str())
                    && resource.path.parent() == Some(resource.parent.path.as_path())
                    && resource
                        .path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.ends_with(&resource.id))
                    && resource
                        .parent
                        .path
                        .starts_with(self.root.parent().unwrap().parent().unwrap()),
                "guardian resource binding",
            )?;
            resource.parent.valid()?;
            match resource.identity {
                Some(identity) => {
                    ensure(
                        identity.path == resource.path && identity.owner == resource.owner,
                        "guardian resource path changed",
                    )?;
                    if resource.kind == ResourceKind::Images
                        && exists(&identity.path)?
                        && std::fs::read_dir(&identity.path)?.next().is_some()
                    {
                        identity.valid()?;
                        crate::computer_cache::validate_png_directory(
                            &identity.path,
                            identity.owner.as_deref(),
                        )?;
                    }
                    identity.remove()?;
                }
                None if exists(&resource.path)? => {
                    let identity = Directory::capture(&resource.path)?;
                    // Creation may have committed before its inode record.
                    // Nothing may be written at this boundary; never recurse.
                    if let Some(owner) = identity.owner {
                        ensure(
                            resource.owner.as_ref() == Some(&owner),
                            "unpublished resource marker changed",
                        )?;
                    }
                    let entries =
                        std::fs::read_dir(&resource.path)?.collect::<std::io::Result<Vec<_>>>()?;
                    ensure(
                        entries.len() <= 2
                            && entries.iter().all(|entry| {
                                entry.file_name() == OWNER_FILE
                                    || entry.file_name() == ".pulse-owner.partial"
                            }),
                        "unpublished resource contains unknown content",
                    )?;
                    for entry in &entries {
                        drop(private_file(&entry.path(), false)?);
                    }
                    for entry in entries {
                        std::fs::remove_file(entry.path())?;
                    }
                    File::open(&resource.path)?.sync_all()?;
                    std::fs::remove_dir(&resource.path)?;
                }
                None => {}
            }
            std::fs::remove_file(&path)?;
            File::open(&resource.parent.path)?.sync_all()?;
        }
        File::open(root)?.sync_all()?;
        Ok(())
    }
}
