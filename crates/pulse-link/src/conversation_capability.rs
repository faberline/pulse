//! Transport capability only. It grants no task, tool or memory permission.
use crate::{Result, ensure, server::Hub};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Capability {
    version: u8,
    epoch: i64,
}
fn key(device: &str) -> String {
    format!("chat-context:{device}")
}
fn matches(data: Option<&str>, epoch: i64) -> bool {
    data.filter(|s| s.len() <= 256)
        .and_then(|s| serde_json::from_str::<Capability>(s).ok())
        .is_some_and(|c| c.version == 1 && c.epoch == epoch && epoch > 0)
}

impl Hub {
    /// Called by the authenticated chat connection before its queued Human frames.
    pub async fn conversation_ready(
        &self,
        device: &str,
        epoch: u64,
        version: u8,
        time: i64,
    ) -> Result<()> {
        ensure(version == 1, "unsupported conversation capability")?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let live: i64 = sqlx::query_scalar("SELECT count(*) FROM link_device WHERE id=? AND epoch=? AND role='chat' AND revoked=0 AND lease_until>?")
            .bind(device).bind(epoch as i64).bind(time).fetch_one(&mut *tx).await?;
        ensure(
            live == 1,
            "conversation capability requires current chat lease",
        )?;
        sqlx::query("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(key(device)).bind(serde_json::to_string(&Capability {version,epoch:epoch as i64})?).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn conversation_capable(&self, device: &str, time: i64) -> Result<bool> {
        let row = sqlx::query("SELECT d.epoch,s.value FROM link_device d LEFT JOIN settings s ON s.key=? WHERE d.id=? AND d.role='chat' AND d.revoked=0 AND d.lease_until>?")
            .bind(key(device)).bind(device).bind(time).fetch_optional(&self.db).await?;
        Ok(row.is_some_and(|r| {
            matches(
                r.get::<Option<String>, _>("value").as_deref(),
                r.get("epoch"),
            )
        }))
    }

    pub(crate) async fn reset_conversation_capability(
        db: &mut SqliteConnection,
        device: &str,
    ) -> Result<()> {
        let row = sqlx::query("SELECT d.role,d.epoch,s.value FROM link_device d LEFT JOIN settings s ON s.key=? WHERE d.id=?")
            .bind(key(device)).bind(device).fetch_one(&mut *db).await?;
        if row.get::<String, _>("role") == "chat"
            && !matches(
                row.get::<Option<String>, _>("value").as_deref(),
                row.get("epoch"),
            )
        {
            // An unready client can change topics without a server event. Its
            // successor must not inherit that client's old implicit focus.
            sqlx::query("DELETE FROM settings WHERE key=?")
                .bind(crate::conversation_context::KEY)
                .execute(&mut *db)
                .await?;
        }
        // begin increments the epoch in this same transaction. The old record
        // remains historical metadata, but is ineligible until a new declaration.
        Ok(())
    }
}
