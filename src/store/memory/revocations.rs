use super::*;
use crate::{MemoryOutcome, MemoryRef, MemoryRequest};

const UNKNOWN_LIMIT: usize = 10_000;
const SOURCE_LIMIT: usize = 20_100;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceRestriction {
    pub id: String,
    pub revision: u64,
    pub unknown: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cancellation {
    pub fingerprint: String,
    pub current_changed: bool,
}

pub(super) fn canonical(id: &str) -> Result<String> {
    Ok(uuid::Uuid::parse_str(id)
        .map_err(|_| Error::Rejected("invalid memory UUID".into()))?
        .to_string())
}

pub(super) fn valid(value: &Restrictions) -> bool {
    value.sources.len() <= SOURCE_LIMIT
        && value.cancellations.len() <= SOURCE_LIMIT
        && value.sources.values().filter(|s| s.unknown).count() <= UNKNOWN_LIMIT
        && value.sources.values().filter(|s| !s.unknown).count() <= EVENT_LIMIT as usize + 100
        && value.sources.iter().all(|(id, s)| {
            canonical(id).is_ok_and(|v| v == *id)
                && canonical(&s.id).is_ok_and(|v| v == s.id)
                && s.revision > 0
                && s.revision <= i64::MAX as u64
        })
        && value.cancellations.iter().all(|(id, c)| {
            canonical(id).is_ok_and(|v| v == *id)
                && c.fingerprint.len() == 64
                && c.fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
        })
}

fn block(value: &mut Restrictions, source: String, restriction: SourceRestriction) -> Result<bool> {
    if value.sources.contains_key(&source) {
        return Ok(false);
    }
    let count = value
        .sources
        .values()
        .filter(|s| s.unknown == restriction.unknown)
        .count();
    require(
        count
            < if restriction.unknown {
                UNKNOWN_LIMIT
            } else {
                EVENT_LIMIT as usize + 100
            },
        "source revocation capacity",
    )?;
    if restriction.unknown {
        // A source UUID is not a memory ID. Unknown sources must never acquire
        // ID/revision authority over an unrelated memory with the same spelling.
        value.epoch = value
            .epoch
            .checked_add(1)
            .ok_or_else(|| Error::Rejected("memory restriction epoch exhausted".into()))?;
    } else {
        restrict(value, &restriction.id, restriction.revision)?;
    }
    value.sources.insert(source, restriction);
    Ok(true)
}

/// Capture source identity as well as memory identity. A backup can predate the
/// original remember event, so an ID/revision-only tombstone is insufficient.
pub(super) async fn capture(
    db: &mut SqliteConnection,
    value: &mut Restrictions,
    id: &str,
    revision: u64,
) -> Result<()> {
    let rows = sqlx::query(
        "SELECT source_id,revision FROM memory_events WHERE memory_id=? AND revision<=?",
    )
    .bind(id)
    .bind(revision as i64)
    .fetch_all(&mut *db)
    .await?;
    for row in rows {
        block(
            value,
            canonical(&row.get::<String, _>("source_id"))?,
            SourceRestriction {
                id: id.into(),
                revision: row.get::<i64, _>("revision") as u64,
                unknown: false,
            },
        )?;
    }
    Ok(())
}

pub(super) async fn upgrade(db: &mut SqliteConnection, value: &mut Restrictions) -> Result<bool> {
    if value.format == 2 {
        return Ok(false);
    }
    require(
        value.format == 1 && value.sources.is_empty() && value.cancellations.is_empty(),
        "invalid legacy memory restrictions",
    )?;
    let removed = value.removed.clone();
    for (id, revision) in removed {
        let present: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM memory_events WHERE memory_id=? AND revision=?",
        )
        .bind(&id)
        .bind(revision as i64)
        .fetch_one(&mut *db)
        .await?;
        require(
            present > 0,
            "legacy deletion sources unavailable; upgrade refused",
        )?;
        capture(db, value, &id, revision).await?;
    }
    value.format = 2;
    Ok(true)
}

impl Store {
    pub(super) async fn revoke_sources(
        &self,
        request: &MemoryRequest,
        sources: &[String],
        expected: &Option<MemoryRef>,
        now: i64,
    ) -> Result<MemoryOutcome> {
        require(
            sources.len() <= 101 && (!sources.is_empty() || expected.is_some()),
            "revocation target limit",
        )?;
        let mut sources = sources
            .iter()
            .map(|s| canonical(s))
            .collect::<Result<Vec<_>>>()?;
        sources.sort();
        sources.dedup();
        let expected = expected
            .as_ref()
            .map(|r| -> Result<MemoryRef> {
                require(
                    r.revision > 0 && r.revision <= i64::MAX as u64,
                    "invalid memory revision",
                )?;
                Ok(MemoryRef {
                    id: canonical(&r.id)?,
                    revision: r.revision,
                })
            })
            .transpose()?;
        let request = MemoryRequest {
            source_id: canonical(&request.source_id)?,
            source_at: request.source_at,
            command: MemoryCommand::Revoke {
                sources: sources.clone(),
                expected: expected.clone(),
            },
        };
        let fingerprint = digest(&request)?;
        let _lease = self.memory_guard.lease().await?;
        let mut value = self
            .memory_guard
            .read()?
            .ok_or_else(|| Error::Rejected("memory restrictions missing".into()))?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reconcile(&mut tx, &value).await?;
        self.expire_memory_in(&mut tx, &mut value, now).await?;
        if let Some(previous) = value.cancellations.get(&request.source_id) {
            require(
                previous.fingerprint == fingerprint,
                "cancellation source conflict",
            )?;
            let result = MemoryOutcome::Revoked {
                current_changed: previous.current_changed,
            };
            tx.commit().await?;
            self.checkpoint_memory_redaction().await?;
            return Ok(result);
        }
        let used: i64 =
            sqlx::query_scalar("SELECT count(*) FROM memory_events WHERE source_uuid=?")
                .bind(&request.source_id)
                .fetch_one(&mut *tx)
                .await?;
        require(
            used == 0,
            "cancellation source already belongs to knowledge",
        )?;
        if let Some(reference) = &expected {
            let source: Option<String> =
                sqlx::query_scalar("SELECT source_id FROM memories WHERE id=? AND revision=?")
                    .bind(&reference.id)
                    .bind(reference.revision as i64)
                    .fetch_optional(&mut *tx)
                    .await?;
            if let Some(source) = source {
                sources.push(canonical(&source)?);
            }
        }
        let mut changed = false;
        for source in sources {
            if value.sources.contains_key(&source) {
                continue;
            }
            let event =
                sqlx::query("SELECT memory_id,revision FROM memory_events WHERE source_uuid=?")
                    .bind(&source)
                    .fetch_optional(&mut *tx)
                    .await?;
            let restriction = match event {
                Some(row) => SourceRestriction {
                    id: row.get("memory_id"),
                    revision: row.get::<i64, _>("revision") as u64,
                    unknown: false,
                },
                None => SourceRestriction {
                    id: source.clone(),
                    revision: 1,
                    unknown: true,
                },
            };
            changed |= block(&mut value, source, restriction)?;
        }
        // Newer Human sources are preserved. Cancelling an unacknowledged write
        // still removes that exact source, whether it arrived before or after us.
        let current_changed = if let Some(reference) = &expected {
            let row = sqlx::query("SELECT revision,source_id FROM memories WHERE id=?")
                .bind(&reference.id)
                .fetch_optional(&mut *tx)
                .await?;
            match row {
                Some(row) => {
                    row.get::<i64, _>("revision") as u64 != reference.revision
                        && !value
                            .sources
                            .contains_key(&canonical(&row.get::<String, _>("source_id"))?)
                }
                None => value
                    .removed
                    .get(&reference.id)
                    .is_none_or(|v| *v < reference.revision),
            }
        } else {
            false
        };
        if changed {
            require(
                value.cancellations.len() < SOURCE_LIMIT,
                "cancellation receipt capacity",
            )?;
            value.cancellations.insert(
                request.source_id.clone(),
                Cancellation {
                    fingerprint,
                    current_changed,
                },
            );
            self.memory_guard.write(&value)?; // restriction evidence before SQL
            reconcile(&mut tx, &value).await?;
        }
        tx.commit().await?;
        self.memory_guard.changed.notify_one();
        self.checkpoint_memory_redaction().await?;
        Ok(MemoryOutcome::Revoked { current_changed })
    }
}

pub(super) async fn replay(db: &mut SqliteConnection, value: &Restrictions) -> Result<()> {
    for (source, restriction) in &value.sources {
        sqlx::query("DELETE FROM memories WHERE source_uuid=?")
            .bind(source)
            .execute(&mut *db)
            .await?;
        sqlx::query(
            "UPDATE memory_events SET fingerprint=NULL,outcome='redacted' WHERE source_uuid=?",
        )
        .bind(source)
        .execute(&mut *db)
        .await?;
        sqlx::query("INSERT INTO memory_events(source_id,memory_id,revision,fingerprint,outcome) SELECT ?,?,?,NULL,'redacted' WHERE NOT EXISTS(SELECT 1 FROM memory_events WHERE source_uuid=?)")
            .bind(source).bind(&restriction.id).bind(restriction.revision as i64).bind(source).execute(&mut *db).await?;
    }
    Ok(())
}
