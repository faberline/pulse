use crate::contract::{Error, Result};
use rustix::fs::{self, AtFlags, Mode, OFlags, RenameFlags};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::Path,
};

pub(crate) fn save(data: &[u8], directory: &Path) -> Result<String> {
    if !directory.is_absolute()
        || data.len() > 4 * 1024 * 1024
        || !data.starts_with(b"\x89PNG\r\n\x1a\n")
    {
        return Err(Error::EvidenceLimit);
    }
    let run = || -> std::io::Result<String> {
        let dir = fs::open(
            directory,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let stat = fs::fstat(&dir)?;
        if stat.st_uid != rustix::process::geteuid().as_raw() || stat.st_mode & 0o777 != 0o700 {
            return Err(std::io::ErrorKind::PermissionDenied.into());
        }
        let name = format!("{:x}.png", Sha256::digest(data));
        let check = || -> std::io::Result<()> {
            let fd = fs::openat(
                &dir,
                &name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
            let mut file = File::from(fd);
            let meta = file.metadata()?;
            if !meta.is_file()
                || meta.nlink() != 1
                || meta.uid() != rustix::process::geteuid().as_raw()
                || meta.mode() & 0o777 != 0o600
                || meta.len() != data.len() as u64
            {
                return Err(std::io::ErrorKind::PermissionDenied.into());
            }
            let mut bytes = Vec::new();
            Read::by_ref(&mut file)
                .take(data.len() as u64 + 1)
                .read_to_end(&mut bytes)?;
            if bytes != data {
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            Ok(())
        };
        match fs::statat(&dir, &name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(_) => check()?,
            Err(rustix::io::Errno::NOENT) => {
                let temporary = format!(".partial-{}", uuid::Uuid::new_v4());
                let result = (|| -> std::io::Result<()> {
                    let fd = fs::openat(
                        &dir,
                        &temporary,
                        OFlags::WRONLY
                            | OFlags::CREATE
                            | OFlags::EXCL
                            | OFlags::NOFOLLOW
                            | OFlags::CLOEXEC,
                        Mode::RUSR | Mode::WUSR,
                    )?;
                    fs::fchmod(&fd, Mode::RUSR | Mode::WUSR)?;
                    let mut count = 0;
                    for entry in fs::Dir::read_from(&dir)? {
                        let entry = entry?;
                        if entry.file_name() != c"." && entry.file_name() != c".." {
                            count += 1;
                        }
                    }
                    if count > 128 {
                        return Err(std::io::ErrorKind::StorageFull.into());
                    }
                    let mut file = File::from(fd);
                    file.write_all(data)?;
                    file.sync_all()?;
                    match fs::renameat_with(&dir, &temporary, &dir, &name, RenameFlags::NOREPLACE) {
                        Ok(()) => {}
                        Err(rustix::io::Errno::EXIST) => check()?,
                        Err(e) => return Err(e.into()),
                    }
                    fs::fsync(&dir)?;
                    Ok(())
                })();
                let _ = fs::unlinkat(&dir, &temporary, AtFlags::empty());
                result?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(directory.join(name).to_string_lossy().into_owned())
    };
    run().map_err(|_| Error::PrivateStorageRequired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    fn data() -> Vec<u8> {
        [b"\x89PNG\r\n\x1a\n".as_slice(), &[0; 64]].concat()
    }
    #[test]
    fn cache_rejects_symlinks_modes_tamper_hardlinks_and_fifo_without_replacing_them() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let data = data();
        let path = std::path::PathBuf::from(save(&data, dir.path()).unwrap());
        assert_eq!(save(&data, dir.path()).unwrap(), path.to_str().unwrap());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(save(&data, dir.path()).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let alias = dir.path().join("directory-alias");
        symlink(dir.path(), &alias).unwrap();
        assert!(save(&data, &alias).is_err());
        std::fs::hard_link(&path, dir.path().join("alias")).unwrap();
        assert!(save(&data, dir.path()).is_err());
        std::fs::remove_file(dir.path().join("alias")).unwrap();
        std::fs::write(&path, vec![1; data.len()]).unwrap();
        assert!(save(&data, dir.path()).is_err());
        std::fs::remove_file(&path).unwrap();
        symlink("missing", &path).unwrap();
        assert!(save(&data, dir.path()).is_err());
        assert!(path.is_symlink());
        std::fs::remove_file(&path).unwrap();
        assert!(
            std::process::Command::new("/usr/bin/mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        assert!(save(&data, dir.path()).is_err());
        std::fs::remove_file(&path).unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(save(&data, dir.path()).is_err());
    }
    #[test]
    fn full_cache_leaves_no_partial_or_published_artifact() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        for n in 0..128 {
            std::fs::write(dir.path().join(n.to_string()), []).unwrap();
        }
        assert!(save(&data(), dir.path()).is_err());
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 128);
        assert!(
            !names
                .iter()
                .any(|n| n.to_string_lossy().starts_with(".partial"))
        );
    }
}
