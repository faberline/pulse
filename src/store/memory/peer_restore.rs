//! A local profile's server generation survives database-only rollback.
use super::*;

const KEY: &str = "peer_restore_generation";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PeerRestore {
    device: String,
    server: String,
    role: String,
    generation: String,
    committed: bool,
}
impl PeerRestore {
    pub(super) fn valid(&self) -> bool {
        [&self.device, &self.generation]
            .iter()
            .all(|value| uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == **value))
            && self.server.len() == 64
            && self.server.bytes().all(|b| b.is_ascii_hexdigit())
            && matches!(self.role.as_str(), "chat" | "local_executor")
    }
}

async fn binding(db: &mut SqliteConnection, peer: &PeerRestore) -> Result<()> {
    let values: Vec<(String, String)> =
        sqlx::query_as("SELECT key,value FROM local_meta WHERE key IN ('device','server','role')")
            .fetch_all(&mut *db)
            .await?;
    let values: BTreeMap<_, _> = values.into_iter().collect();
    require(
        values.get("device") == Some(&peer.device)
            && values
                .get("server")
                .is_some_and(|value| digest(value).is_ok_and(|h| h == peer.server))
            && values.get("role").is_some_and(|role| role == &peer.role),
        "restored server profile binding mismatch",
    )
}

async fn pristine(db: &mut SqliteConnection) -> Result<()> {
    let counters: i64 = sqlx::query_scalar("SELECT count(*) FROM local_meta WHERE (key IN ('cursor','next') AND value='0') OR (key='stopped' AND value='false')")
        .fetch_one(&mut *db).await?;
    let access: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM local_memory_access WHERE singleton=1 AND denied=0 AND generation=0",
    )
    .fetch_one(&mut *db)
    .await?;
    require(
        counters == 3 && access == 1,
        "restored server requires a fresh local profile",
    )?;
    // Do not load cached messages or other retained private values just to
    // reject a used profile.
    let unexpected: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM local_meta WHERE NOT (key IN ('device','server','repo','role','memory_dirty') OR (key IN ('cursor','next') AND value='0') OR (key='stopped' AND value='false') OR (key='worker_runtime_origin' AND value='fresh')))")
        .fetch_one(&mut *db).await?;
    require(
        unexpected == 0,
        "restored server requires a fresh local profile",
    )?;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema WHERE type='table' AND substr(name,1,6)='local_' LIMIT 65",
    )
    .fetch_all(&mut *db)
    .await?;
    require(tables.len() <= 64, "local profile table capacity")?;
    for table in tables {
        if matches!(table.as_str(), "local_meta" | "local_memory_access") {
            continue;
        }
        require(
            table
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "invalid local table name",
        )?;
        // Names come from sqlite_schema and the identifier alphabet is checked
        // above. No user text or SQL punctuation can enter this statement.
        let populated: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS(SELECT 1 FROM \"{table}\")"
        )))
        .fetch_one(&mut *db)
        .await?;
        require(
            populated == 0,
            "restored server requires a fresh local profile",
        )?;
    }
    Ok(())
}

pub(super) async fn check(db: &mut SqliteConnection, manifest: &Restrictions) -> Result<()> {
    let Some(peer) = &manifest.peer_restore else {
        return Ok(());
    };
    binding(db, peer).await?;
    let generation: Option<String> = sqlx::query_scalar("SELECT value FROM local_meta WHERE key=?")
        .bind(KEY)
        .fetch_optional(&mut *db)
        .await?;
    if generation.as_deref() == Some(peer.generation.as_str()) {
        return Ok(());
    }
    require(
        !peer.committed && generation.is_none(),
        "local database predates server restore binding",
    )?;
    pristine(db).await
}

impl Store {
    /// Validate a previously saved pairing claim without creating new evidence.
    pub async fn require_peer_restore(
        &self,
        device: &str,
        server: &str,
        generation: &str,
    ) -> Result<()> {
        let _lease = self.memory_guard.lease().await?;
        let manifest = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("local restrictions missing".into()))?;
        require(
            manifest.peer_restore.as_ref().is_some_and(|peer| {
                peer.committed
                    && peer.device == device
                    && peer.server == server
                    && peer.generation == generation
            }),
            "saved pairing predates server restore binding",
        )?;
        check(&mut *self.pool.acquire().await?, &manifest).await
    }

    /// Trusted local pairing only, after the caller proves that its vault has
    /// no prior active credential. This grants no server or tool authority.
    pub async fn bind_peer_restore(
        &self,
        device: &str,
        server: &str,
        generation: &str,
    ) -> Result<()> {
        let _lease = self.memory_guard.lease().await?;
        let mut manifest = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("local restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let role: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='role'")
            .fetch_one(&mut *tx)
            .await?;
        let requested = PeerRestore {
            device: device.into(),
            server: server.into(),
            role,
            generation: generation.into(),
            committed: false,
        };
        require(requested.valid(), "invalid server restore binding")?;
        binding(&mut tx, &requested).await?;
        if let Some(peer) = &manifest.peer_restore {
            require(
                peer.device == device && peer.server == server && peer.generation == generation,
                "restored server requires a fresh local profile",
            )?;
            check(&mut tx, &manifest).await?;
        } else {
            require(
                manifest.restore.is_none()
                    && manifest.worker.is_none()
                    && manifest.copy.as_ref().is_none_or(|copy| copy.pristine()),
                "restored server requires a fresh local profile",
            )?;
            pristine(&mut tx).await?;
            manifest.peer_restore = Some(requested);
            manifest.format = 9;
            self.memory_guard.write(&manifest)?;
        }
        sqlx::query("INSERT INTO local_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(KEY).bind(generation).execute(&mut *tx).await?;
        tx.commit().await?;
        manifest.peer_restore.as_mut().unwrap().committed = true;
        self.memory_guard.write(&manifest)
    }
}
