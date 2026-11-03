//! Bounded source checks for server-owned chat projections. No prompt grant.
use crate::{Result, server::Hub};
use pulse_core::{TaskMemoryLease, TaskMemorySource, TaskMemorySources};
use sqlx::{Row, SqliteConnection, sqlite::SqliteRow};

pub(crate) async fn messages(db: &mut SqliteConnection, time: i64) -> Result<Vec<SqliteRow>> {
    Ok(sqlx::query("SELECT c.id,c.body,COALESCE((SELECT d.redacted FROM memory_derived_messages d WHERE d.id=c.id),0) AS redacted,COALESCE((SELECT b.data FROM memory_derived_messages d JOIN task_memory_sources b ON b.task_id=d.task_id AND b.task_revision=d.task_revision WHERE d.id=c.id),(SELECT b.data FROM operation_requests r JOIN task_memory_sources b ON b.task_id=r.task_id AND b.task_revision=json_extract(r.data,'$.proposal.token.revision') WHERE r.id=c.id),(SELECT b.data FROM link_reads r JOIN task_memory_sources b ON b.task_id=r.task_id AND b.task_revision=r.task_revision WHERE r.id=c.id)) AS lineage FROM link_chat c ORDER BY EXISTS(SELECT 1 FROM operation_requests r WHERE r.id=c.id AND json_extract(r.data,'$.state') IN ('pending','approved') AND json_extract(r.data,'$.proposal.expires_at')>?) DESC,c.created_at DESC,c.rowid DESC LIMIT 10")
        .bind(time).fetch_all(db).await?)
}

impl Hub {
    pub(crate) async fn projection_lease(
        &self,
        device: &str,
        epoch: u64,
        time: i64,
    ) -> Result<(Option<TaskMemoryLease<'_>>, Vec<TaskMemorySource>, bool)> {
        let ready: i64 = sqlx::query_scalar("SELECT count(*) FROM link_device WHERE id=? AND epoch=? AND role='chat' AND projection_erasure=1 AND revoked=0 AND lease_until>?")
            .bind(device).bind(epoch as i64).bind(time).fetch_one(&self.db).await?;
        if ready == 0 {
            return Ok((None, vec![], false));
        }
        let rows = {
            let mut db = self.db.acquire().await?;
            messages(&mut db, time).await?
        };
        let mut sources = std::collections::BTreeMap::new();
        for row in rows {
            if row.get::<i64, _>("redacted") != 0 {
                continue;
            }
            if let Some(data) = row.get::<Option<String>, _>("lineage") {
                let binding: TaskMemorySources = serde_json::from_str(&data)?;
                for source in binding.sources {
                    sources.insert(source.source_id.clone(), source);
                }
            }
        }
        let sources: Vec<_> = sources.into_values().collect();
        if sources.is_empty() {
            return Ok((None, sources, false));
        }
        match self.core.lease_projection_sources(&sources, time).await {
            Ok(lease) => Ok((Some(lease), sources, false)),
            Err(pulse_core::Error::Busy) => Ok((None, vec![], true)),
            Err(pulse_core::Error::Rejected(_)) => Ok((None, vec![], false)),
            Err(error) => Err(error.into()),
        }
    }
}
