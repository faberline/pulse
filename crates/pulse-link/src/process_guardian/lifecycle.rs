use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Runtime {
    version: u8,
    id: String,
    device: String,
    server: String,
    phase: RuntimePhase,
    directory: Directory,
    #[serde(default)]
    boot: Option<boot::Identity>,
}
#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RuntimePhase {
    Prepared,
    Active,
    Closing,
    Closed,
}
impl Registry {
    pub(super) fn stopped_by_reboot(&self, state: &Record) -> Result<bool> {
        let Some(current) = &self.boot else {
            return Ok(false);
        };
        let Some(prior_boot) = &state.boot else {
            return Ok(false);
        };
        if !current.changed_from(prior_boot) {
            return Ok(false);
        }
        // The original runtime and operation must agree. A missing or foreign
        // record cannot turn an unproved launch into a stopped operation.
        let prior: Runtime = read(&self.marker())?;
        ensure(
            prior.version == 1
                && prior.id == state.runtime
                && prior.boot.as_ref() == Some(prior_boot)
                && prior.phase != RuntimePhase::Closed,
            "reboot evidence does not match original runtime",
        )?;
        // Re-read the kernel, never infer a reboot from the cached registry alone.
        Ok(boot::current().is_ok_and(|actual| actual == *current))
    }
    fn crash_boundary(&self, point: &str) {
        #[cfg(test)]
        if std::env::var("PULSE_OWNED_GUARDIAN_CRASH").as_deref() == Ok(point)
            && std::env::var_os("PULSE_OWNED_GUARDIAN_ROOT").is_some_and(|root| {
                PathBuf::from(root)
                    .canonicalize()
                    .is_ok_and(|root| root == self.root.parent().unwrap().parent().unwrap())
            })
        {
            std::process::exit(73);
        }
        #[cfg(not(test))]
        let _ = point;
    }
    pub(super) fn validate_runtime_root(&self) -> Result<()> {
        if exists(&self.marker())? {
            let prior: Runtime = read(&self.marker())?;
            ensure(
                prior.version == 1 && uuid::Uuid::parse_str(&prior.id).is_ok(),
                "unknown guardian runtime version or identity",
            )?;
            ensure(
                prior.directory.path == self.root.parent().unwrap(),
                "guardian runtime directory changed",
            )?;
            prior.directory.valid()?;
        } else {
            let resources = self.root.parent().unwrap().join("resources");
            ensure(
                std::fs::read_dir(&self.root)?.next().is_none()
                    && (!exists(&resources)? || std::fs::read_dir(resources)?.next().is_none()),
                "guardian runtime ownership record missing",
            )?;
        }
        Ok(())
    }
    fn marker(&self) -> PathBuf {
        self.root.parent().unwrap().join("runtime.json")
    }
    pub(crate) async fn begin(
        &self,
        journal: &crate::journal::Journal,
        resources_absent: bool,
    ) -> Result<()> {
        let server = crate::protocol::hash(&journal.server)?;
        self.recover().await?;
        if exists(&self.marker())? {
            let mut prior: Runtime = read(&self.marker())?;
            ensure(
                prior.version == 1
                    && uuid::Uuid::parse_str(&prior.id).is_ok()
                    && prior.device == journal.device
                    && prior.server == server,
                "guardian runtime identity changed",
            )?;
            // Unknown legacy resources cannot be promoted into a clean runtime.
            ensure(resources_absent, "unknown prior local resources remain")?;
            if prior.phase == RuntimePhase::Prepared {
                journal
                    .core
                    .begin_worker_runtime(&prior.device, &prior.server, &prior.id, true)
                    .await?;
            }
            if prior.phase != RuntimePhase::Closed {
                prior.phase = RuntimePhase::Closing;
                write(&self.marker(), &prior)?;
                journal
                    .core
                    .close_worker_runtime(&prior.device, &prior.server, &prior.id)
                    .await?;
                self.crash_boundary("closed-prior-core");
                prior.phase = RuntimePhase::Closed;
                write(&self.marker(), &prior)?;
            }
        }
        let mut current = Runtime {
            version: 1,
            id: self.runtime.clone(),
            device: journal.device.clone(),
            server: server.clone(),
            phase: RuntimePhase::Prepared,
            directory: Directory::capture(self.root.parent().unwrap())?,
            boot: self.boot.clone(),
        };
        write(&self.marker(), &current)?;
        journal
            .core
            .begin_worker_runtime(&journal.device, &server, &self.runtime, resources_absent)
            .await?;
        self.crash_boundary("begun-core");
        current.phase = RuntimePhase::Active;
        write(&self.marker(), &current)
    }
    pub(crate) fn close(&self) -> Result<()> {
        let mut current: Runtime = read(&self.marker())?;
        ensure(current.id == self.runtime, "guardian runtime changed")?;
        current.phase = RuntimePhase::Closed;
        write(&self.marker(), &current)
    }
    pub(crate) fn prepare_close(&self) -> Result<()> {
        let mut current: Runtime = read(&self.marker())?;
        ensure(current.id == self.runtime, "guardian runtime changed")?;
        current.phase = RuntimePhase::Closing;
        write(&self.marker(), &current)
    }
}
