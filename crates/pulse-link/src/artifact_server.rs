use crate::{Result, artifact::*, ensure, protocol::*, server::Hub};
use sqlx::Row;

impl Hub {
    pub(crate) async fn artifact_chunk(
        &self,
        device: &str,
        epoch: u64,
        operation_id: &str,
        artifact: &ArtifactRef,
        index: u32,
        data: &str,
    ) -> Result<()> {
        let bytes = artifact.chunk(index, data)?;
        let metadata = serde_json::to_string(artifact)?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query(
            "SELECT device,epoch,task_id,task_revision,request FROM link_reads WHERE id=?",
        )
        .bind(operation_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| crate::Error::Rejected("artifact has no read intent".into()))?;
        let operation: ReadOp = serde_json::from_str(&row.get::<String, _>("request"))?;
        ensure(
            row.get::<String, _>("device") == device
                && row.get::<i64, _>("epoch") as u64 == epoch
                && row.get::<String, _>("task_id") == artifact.task_id
                && row.get::<String, _>("task_revision") == artifact.revision
                && matches!(
                    operation,
                    ReadOp::Browser { .. }
                        | ReadOp::BrowserInspect { .. }
                        | ReadOp::BrowserAction { .. }
                ),
            "artifact source binding mismatch",
        )?;
        if let Some(row) =
            sqlx::query("SELECT metadata,png FROM link_artifacts WHERE operation_id=?")
                .bind(operation_id)
                .fetch_optional(&mut *tx)
                .await?
        {
            ensure(
                row.get::<String, _>("metadata") == metadata,
                "artifact metadata conflict",
            )?;
            if let Some(png) = row.get::<Option<Vec<u8>>, _>("png") {
                let offset = index as usize * CHUNK;
                ensure(
                    png.get(offset..offset + bytes.len()) == Some(bytes.as_slice()),
                    "artifact replay conflict",
                )?;
                return Ok(());
            }
        } else {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM link_artifacts")
                .fetch_one(&mut *tx)
                .await?;
            ensure(count < 128, "server artifact capacity reached")?;
            sqlx::query("INSERT INTO link_artifacts VALUES(?,?,NULL)")
                .bind(operation_id)
                .bind(&metadata)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(old) = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT bytes FROM link_artifact_parts WHERE operation_id=? AND part=?",
        )
        .bind(operation_id)
        .bind(index)
        .fetch_optional(&mut *tx)
        .await?
        {
            ensure(old == bytes, "artifact chunk conflict")?;
        } else {
            sqlx::query("INSERT INTO link_artifact_parts VALUES(?,?,?)")
                .bind(operation_id)
                .bind(index)
                .bind(bytes)
                .execute(&mut *tx)
                .await?;
        }
        let parts: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT bytes FROM link_artifact_parts WHERE operation_id=? ORDER BY part",
        )
        .bind(operation_id)
        .fetch_all(&mut *tx)
        .await?;
        if parts.len() == artifact.chunks() {
            let png = parts.concat();
            artifact.verify(&png)?;
            sqlx::query("UPDATE link_artifacts SET png=? WHERE operation_id=?")
                .bind(png)
                .bind(operation_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM link_artifact_parts WHERE operation_id=?")
                .bind(operation_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn artifact_png(
        &self,
        device: &str,
        operation_id: &str,
        digest: &str,
        time: i64,
    ) -> Result<Vec<u8>> {
        ensure(
            digest_id(digest) && operation_id.len() <= 256,
            "invalid artifact identity",
        )?;
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        let allowed:i64=sqlx::query_scalar("SELECT COUNT(*) FROM link_device WHERE id=? AND role='chat' AND revoked=0 AND lease_until>?").bind(device).bind(time).fetch_one(&mut *tx).await?;
        ensure(allowed == 1, "artifact requires live chat role")?;
        let row=sqlx::query("SELECT a.metadata,a.png FROM link_artifacts a JOIN link_reads r ON r.id=a.operation_id WHERE a.operation_id=? AND r.state='completed' AND a.png IS NOT NULL").bind(operation_id).fetch_optional(&mut *tx).await?.ok_or_else(||crate::Error::Rejected("artifact unavailable".into()))?;
        let artifact: ArtifactRef = serde_json::from_str(&row.get::<String, _>("metadata"))?;
        ensure(artifact.digest == digest, "artifact identity mismatch")?;
        let png: Vec<u8> = row.get("png");
        artifact.verify(&png)?;
        tx.commit().await?;
        Ok(png)
    }
}
