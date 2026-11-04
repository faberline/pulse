//! Kernel boot evidence. There is no clock, environment or process-ID fallback.
use super::*;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Identity {
    platform: String,
    uuid: String,
}
fn parse(platform: &str, bytes: &[u8]) -> Result<Identity> {
    ensure(bytes.len() <= 64, "boot identity capacity")?;
    let value = std::str::from_utf8(bytes)
        .map_err(|_| crate::Error::Rejected("invalid kernel boot identity".into()))?
        .trim_end_matches(['\n', '\r']);
    ensure(value.len() == 36, "invalid kernel boot identity")?;
    let uuid = uuid::Uuid::parse_str(value)
        .map_err(|_| crate::Error::Rejected("invalid kernel boot UUID".into()))?;
    ensure(!uuid.is_nil() && !uuid.is_max(), "invalid kernel boot UUID")?;
    Ok(Identity {
        platform: platform.into(),
        uuid: uuid.to_string(),
    })
}
pub(super) fn current() -> Result<Identity> {
    #[cfg(target_os = "linux")]
    {
        let mut bytes = Vec::new();
        File::open("/proc/sys/kernel/random/boot_id")?
            .take(65)
            .read_to_end(&mut bytes)?;
        parse("linux", &bytes)
    }
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("/usr/sbin/sysctl")
            .args(["-n", "kern.bootsessionuuid"])
            .env_clear()
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()?;
        ensure(output.status.success(), "kernel boot identity unavailable")?;
        parse("macos", &output.stdout)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    Err(crate::Error::Rejected(
        "kernel boot identity unavailable".into(),
    ))
}
impl Identity {
    fn valid(&self) -> bool {
        matches!(self.platform.as_str(), "linux" | "macos")
            && parse(&self.platform, self.uuid.as_bytes()).is_ok_and(|id| id == *self)
    }
    pub(super) fn changed_from(&self, prior: &Self) -> bool {
        self.valid() && prior.valid() && self.platform == prior.platform && self.uuid != prior.uuid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_uuid_parser_rejects_empty_nil_oversized_and_non_uuid_evidence() {
        for bytes in [
            b"".as_slice(),
            b"unknown",
            b"0",
            b"1970-01-01T00:00:00Z",
            b"00000000-0000-0000-0000-000000000000",
            b"ffffffff-ffff-ffff-ffff-ffffffffffff",
            &[b'x'; 65],
        ] {
            assert!(parse("linux", bytes).is_err());
        }
        let a = parse("linux", b"12345678-1234-4234-8234-123456789ABC\n").unwrap();
        let b = parse("linux", b"12345678-1234-4234-8234-123456789abc").unwrap();
        assert!(a == b);
        assert!(!a.changed_from(&b));
        let other = parse("macos", uuid::Uuid::new_v4().to_string().as_bytes()).unwrap();
        assert!(!a.changed_from(&other));
    }
}
