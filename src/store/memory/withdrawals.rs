//! Local Human withdrawal evidence survives a database-only restore.
//! No source text, credential or task authority belongs in this ledger.
use super::*;
use crate::MemoryRef;
use std::collections::BTreeSet;

const RECEIPTS: usize = 10_100;
const UNKNOWN_RECEIPTS: usize = 10_000;
const DELETE_RESERVE: u64 = 512 * 1024;
const PENDING: usize = 100;
const SOURCES: usize = 20_100;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Withdrawal {
    request: MemoryRequest,
    phase: String,
}

/// Content-free filters. These restrict a local copy, never grant server access.
#[derive(Default)]
pub struct MemoryWithdrawalRules {
    sources: BTreeSet<String>,
    controls: BTreeSet<String>,
    references: BTreeMap<String, u64>,
}
impl MemoryWithdrawalRules {
    pub fn allows_source(&self, source: &str) -> bool {
        revocations::canonical(source)
            .is_ok_and(|s| !self.sources.contains(&s) && !self.controls.contains(&s))
    }
    pub fn allows(&self, memory: &Memory) -> bool {
        revocations::canonical(&memory.source_id).is_ok_and(|s| !self.sources.contains(&s))
            && self
                .references
                .get(&memory.id)
                .is_none_or(|r| memory.revision > *r)
    }
}
fn reference(command: &MemoryCommand) -> Option<MemoryRef> {
    match command {
        MemoryCommand::Revoke { expected, .. } => expected.clone(),
        MemoryCommand::Forget { id, revision } => Some(MemoryRef {
            id: id.clone(),
            revision: *revision,
        }),
        _ => None,
    }
}
fn valid_request(request: &MemoryRequest) -> bool {
    revocations::canonical(&request.source_id).is_ok_and(|s| s == request.source_id)
        && (0..=i64::MAX - MEMORY_RETENTION_MS).contains(&request.source_at)
        && match &request.command {
            MemoryCommand::Revoke { sources, expected } => {
                sources.len() <= 101
                    && (!sources.is_empty() || expected.is_some())
                    && sources
                        .iter()
                        .all(|s| revocations::canonical(s).is_ok_and(|v| v == *s))
                    && sources.windows(2).all(|s| s[0] < s[1])
            }
            MemoryCommand::Forget { .. } => true,
            _ => false,
        }
        && reference(&request.command).is_none_or(|r| {
            revocations::canonical(&r.id).is_ok_and(|v| v == r.id)
                && r.revision > 0
                && r.revision <= i64::MAX as u64
        })
}
fn terminal(phase: &str) -> bool {
    matches!(phase, "accepted" | "rejected" | "redacted" | "review")
}
fn rules(value: &Restrictions) -> MemoryWithdrawalRules {
    let mut rules = MemoryWithdrawalRules::default();
    for (id, entry) in &value.withdrawals {
        rules.controls.insert(id.clone());
        if let MemoryCommand::Revoke { sources, .. } = &entry.request.command {
            rules.sources.extend(sources.iter().cloned());
        }
        if let Some(r) = reference(&entry.request.command) {
            let revision = rules.references.entry(r.id).or_default();
            *revision = (*revision).max(r.revision);
        }
    }
    rules
}
pub(super) fn valid(value: &Restrictions) -> bool {
    let unknown: Vec<_> = value
        .withdrawals
        .values()
        .filter(|e| reference(&e.request.command).is_none())
        .collect();
    let unknown_sources: BTreeSet<_> = unknown
        .iter()
        .flat_map(|e| match &e.request.command {
            MemoryCommand::Revoke { sources, .. } => sources.clone(),
            _ => vec![],
        })
        .collect();
    value.withdrawals.len() <= RECEIPTS
        && unknown.len() <= UNKNOWN_RECEIPTS
        && unknown_sources.len() <= 10_000
        && (value.withdrawals.is_empty() || (value.format == 4 && value.copy.is_some()))
        && value
            .withdrawals
            .values()
            .filter(|e| e.phase == "queued")
            .count()
            <= PENDING
        && value.withdrawals.iter().all(|(id, e)| {
            id == &e.request.source_id
                && valid_request(&e.request)
                && (e.phase == "queued" || terminal(&e.phase))
        })
        && rules(value).sources.len() <= SOURCES
}
fn advance(value: &mut Restrictions, reserve: bool) -> Result<()> {
    let copy = value
        .copy
        .as_mut()
        .ok_or_else(|| Error::Rejected("withdrawal copy binding missing".into()))?;
    copy.serial = copy
        .serial
        .checked_add(1)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or_else(|| Error::Rejected("memory copy serial exhausted".into()))?;
    value.format = 4;
    require(valid(value), "withdrawal evidence capacity or identity")?;
    if reserve {
        require(
            serde_json::to_vec(value)?.len() as u64 <= MANIFEST_LIMIT - DELETE_RESERVE,
            "withdrawal deletion reserve",
        )?;
    }
    Ok(())
}
async fn binding(
    db: &mut SqliteConnection,
    value: &Restrictions,
    device: &str,
    server: &str,
) -> Result<()> {
    let copy = value
        .copy
        .as_ref()
        .ok_or_else(|| Error::Rejected("withdrawal copy binding missing".into()))?;
    require(
        copy.bound(device, server),
        "withdrawal copy binding changed",
    )?;
    let role: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='role'")
        .fetch_one(&mut *db)
        .await?;
    let actual_device: String =
        sqlx::query_scalar("SELECT value FROM local_meta WHERE key='device'")
            .fetch_one(&mut *db)
            .await?;
    let actual_server: String =
        sqlx::query_scalar("SELECT value FROM local_meta WHERE key='server'")
            .fetch_one(&mut *db)
            .await?;
    require(
        role == "chat" && device == actual_device && server == digest(&actual_server)?,
        "withdrawal journal binding changed",
    )?;
    let row = sqlx::query("SELECT generation,serial FROM local_memory_access WHERE singleton=1")
        .fetch_one(&mut *db)
        .await?;
    require(
        row.get::<i64, _>("generation") as u64 == copy.generation
            && row.get::<i64, _>("serial") as u64 <= copy.serial,
        "withdrawal restrictions are stale; reopen required",
    )
}

/// Capture pending requests from an older journal before the format cutover.
pub(super) async fn import(db: &mut SqliteConnection, value: &mut Restrictions) -> Result<bool> {
    if value.copy.is_none() {
        return Ok(false);
    }
    let role: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='role'")
        .fetch_one(&mut *db)
        .await?;
    let device: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='device'")
        .fetch_one(&mut *db)
        .await?;
    let server: String = sqlx::query_scalar("SELECT value FROM local_meta WHERE key='server'")
        .fetch_one(&mut *db)
        .await?;
    require(
        role == "chat"
            && value
                .copy
                .as_ref()
                .unwrap()
                .bound(&device, &digest(&server)?),
        "legacy withdrawal binding changed",
    )?;
    // Import must not make a stale keeper look current by advancing its serial.
    let row = sqlx::query("SELECT generation,serial FROM local_memory_access WHERE singleton=1")
        .fetch_one(&mut *db)
        .await?;
    let copy = value.copy.as_ref().unwrap();
    require(
        row.get::<i64, _>("generation") as u64 <= copy.generation
            && row.get::<i64, _>("serial") as u64 <= copy.serial,
        "withdrawal restrictions are stale",
    )?;
    let rows:Vec<String>=sqlx::query_scalar("SELECT request FROM local_memory_queue WHERE request IS NOT NULL AND json_extract(request,'$.command.kind') IN ('forget','revoke')").fetch_all(&mut *db).await?;
    let mut changed = false;
    let mut reserve = false;
    for data in rows {
        let request: MemoryRequest = serde_json::from_str(&data)?;
        require(valid_request(&request), "invalid legacy withdrawal")?;
        if let Some(entry) = value.withdrawals.get(&request.source_id) {
            require(
                digest(&entry.request)? == digest(&request)?,
                "withdrawal source conflict",
            )?;
        } else {
            reserve |= reference(&request.command).is_none();
            value.withdrawals.insert(
                request.source_id.clone(),
                Withdrawal {
                    request,
                    phase: "queued".into(),
                },
            );
            changed = true;
        }
    }
    if changed {
        advance(value, reserve)?;
    }
    Ok(changed)
}

pub(super) async fn replay(db: &mut SqliteConnection, value: &Restrictions) -> Result<()> {
    if value.withdrawals.is_empty() {
        return Ok(());
    }
    let filters = rules(value);
    let sources = serde_json::to_string(&filters.sources)?;
    sqlx::query("UPDATE local_memory_queue SET request=NULL,phase='redacted' WHERE source_uuid IN (SELECT value FROM json_each(?)) AND request IS NOT NULL AND json_extract(request,'$.command.kind') NOT IN ('forget','revoke')")
        .bind(&sources).execute(&mut *db).await?;
    sqlx::query("DELETE FROM local_memory_cache WHERE lower(json_extract(data,'$.source_id')) IN (SELECT value FROM json_each(?))")
        .bind(sources).execute(&mut *db).await?;
    let references = serde_json::to_string(&filters.references)?;
    sqlx::query("DELETE FROM local_memory_cache WHERE EXISTS(SELECT 1 FROM json_each(?) r WHERE r.key=local_memory_cache.id AND json_extract(data,'$.revision')<=r.value)")
        .bind(references).execute(&mut *db).await?;
    let completed: BTreeMap<_, _> = value
        .withdrawals
        .iter()
        .filter(|(_, e)| e.phase != "queued")
        .map(|(id, e)| (id, &e.phase))
        .collect();
    sqlx::query("UPDATE local_memory_queue AS q SET request=NULL,phase=r.value FROM json_each(?) AS r WHERE q.source_uuid=r.key")
        .bind(serde_json::to_string(&completed)?).execute(&mut *db).await?;
    for entry in value.withdrawals.values().filter(|e| e.phase == "queued") {
        let row = sqlx::query("SELECT request FROM local_memory_queue WHERE source_uuid=?")
            .bind(&entry.request.source_id)
            .fetch_optional(&mut *db)
            .await?;
        if let Some(row) = row {
            if let Some(data) = row.get::<Option<String>, _>("request") {
                require(
                    digest(&serde_json::from_str::<MemoryRequest>(&data)?)?
                        == digest(&entry.request)?,
                    "withdrawal source conflict",
                )?;
            }
            sqlx::query(
                "UPDATE local_memory_queue SET request=?,phase='queued' WHERE source_uuid=?",
            )
            .bind(serde_json::to_string(&entry.request)?)
            .bind(&entry.request.source_id)
            .execute(&mut *db)
            .await?;
        } else {
            sqlx::query("DELETE FROM local_memory_queue WHERE source_id IN (SELECT source_id FROM local_memory_queue WHERE request IS NULL ORDER BY saved_at,source_id LIMIT max(0,(SELECT count(*) FROM local_memory_queue)-999))").execute(&mut *db).await?;
            sqlx::query("INSERT INTO local_memory_queue(source_id,request,phase,saved_at) VALUES(?,?,'queued',?)")
                    .bind(&entry.request.source_id).bind(serde_json::to_string(&entry.request)?).bind(entry.request.source_at).execute(&mut *db).await?;
        }
    }
    if value.withdrawals.values().any(|e| e.phase == "queued") {
        sqlx::query("INSERT INTO local_meta VALUES('memory_visible','true') ON CONFLICT(key) DO UPDATE SET value='true'").execute(&mut *db).await?;
        sqlx::query("INSERT INTO local_meta VALUES('memory_dirty',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(uuid::Uuid::new_v4().to_string()).execute(&mut *db).await?;
    }
    sqlx::query("UPDATE local_memory_access SET serial=? WHERE singleton=1")
        .bind(value.copy.as_ref().unwrap().serial as i64)
        .execute(db)
        .await?;
    Ok(())
}

impl Store {
    /// Trusted Human ingress only. Persist bodyless evidence before SQLite.
    pub async fn queue_memory_withdrawal(
        &self,
        origin: Origin,
        device: &str,
        server: &str,
        request: &MemoryRequest,
        now: i64,
    ) -> Result<()> {
        require(
            origin == Origin::Human && valid_request(request) && request.source_at <= now,
            "invalid Human withdrawal",
        )?;
        let _lease = self.memory_guard.lease().await?;
        let mut value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        binding(&mut tx, &value, device, server).await?;
        require(
            !value.copy.as_ref().unwrap().denied,
            "memory copy is denied",
        )?;
        if let Some(entry) = value.withdrawals.get(&request.source_id) {
            require(
                digest(&entry.request)? == digest(request)?,
                "withdrawal source conflict",
            )?;
        } else {
            let used: i64 =
                sqlx::query_scalar("SELECT count(*) FROM local_memory_queue WHERE source_uuid=?")
                    .bind(&request.source_id)
                    .fetch_one(&mut *tx)
                    .await?;
            require(used == 0, "withdrawal source already used")?;
            value.withdrawals.insert(
                request.source_id.clone(),
                Withdrawal {
                    request: request.clone(),
                    phase: "queued".into(),
                },
            );
            advance(&mut value, reference(&request.command).is_none())?;
            self.memory_guard.write(&value)?;
        }
        replay(&mut tx, &value).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await
    }
    pub async fn memory_withdrawal_rules(
        &self,
        device: &str,
        server: &str,
    ) -> Result<MemoryWithdrawalRules> {
        let _lease = self.memory_guard.lease().await?;
        let value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut db = self.pool.acquire().await?;
        binding(&mut db, &value, device, server).await?;
        Ok(rules(&value))
    }
    /// Credential-scoped receipt. A stale worker cannot consume a new delivery.
    pub async fn finish_memory_withdrawal(
        &self,
        device: &str,
        server: &str,
        credential: &str,
        source: &str,
        phase: &str,
    ) -> Result<bool> {
        require(terminal(phase), "invalid withdrawal acknowledgement")?;
        let source = revocations::canonical(source)?;
        let _lease = self.memory_guard.lease().await?;
        let mut value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        binding(&mut tx, &value, device, server).await?;
        require(
            value.copy.as_ref().unwrap().active(credential),
            "withdrawal credential changed",
        )?;
        let Some(entry) = value.withdrawals.get_mut(&source) else {
            tx.rollback().await?;
            return Ok(false);
        };
        if entry.phase == "queued" {
            entry.phase = phase.into();
            advance(&mut value, false)?;
            self.memory_guard.write(&value)?;
        } else {
            require(entry.phase == phase, "withdrawal acknowledgement changed")?;
        }
        replay(&mut tx, &value).await?;
        tx.commit().await?;
        self.checkpoint_memory_redaction().await?;
        Ok(true)
    }
}
