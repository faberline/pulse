//! Reuse the original negative transport records. A TTL never proves an effect.
use super::*;
use crate::{ErasedProjection, WorkerErasure};

pub(super) async fn task(db: &mut SqliteConnection, id: &str, expired: &ExpiredTask) -> Result<()> {
    // Reconstruct old projections before removing their original references.
    sqlx::query("INSERT OR IGNORE INTO task_retention_messages(id,task_id) SELECT c.id,? FROM link_chat c WHERE EXISTS(SELECT 1 FROM link_reads r WHERE r.task_id=? AND r.id=c.id) OR EXISTS(SELECT 1 FROM operation_requests r WHERE r.task_id=? AND r.id=c.id) OR EXISTS(SELECT 1 FROM runs r WHERE json_extract(r.data,'$.token.task_id')=? AND instr(c.id,'model:'||r.id||':'||json_extract(r.data,'$.token.revision')||':')=1)")
        .bind(id).bind(id).bind(id).bind(id).execute(&mut *db).await?;
    let reads = sqlx::query("SELECT id,device,epoch,task_revision,request FROM link_reads WHERE task_id=? AND NOT EXISTS(SELECT 1 FROM memory_erased_reads e WHERE e.id=link_reads.id AND e.context IS NOT NULL)")
        .bind(id).fetch_all(&mut *db).await?;
    for row in reads {
        let operation: String = row.get("id");
        let device: String = row.get("device");
        let request: serde_json::Value = serde_json::from_str(&row.get::<String, _>("request"))?;
        require(request.is_object(), "retention read ownership unavailable")?;
        let frames: Vec<(i64,String)> = sqlx::query_as("SELECT sequence,data FROM link_outbox WHERE device=? AND json_extract(data,'$.payload.kind')='read' AND json_extract(data,'$.payload.operation_id')=?")
            .bind(&device).bind(&operation).fetch_all(&mut *db).await?;
        let context = WorkerErasure {
            operation_id: operation.clone(),
            task_id: id.into(),
            revision: row.get("task_revision"),
            device: device.clone(),
            epoch: row.get::<i64, _>("epoch").to_string(),
            source_id: expired.source_id.clone(),
            is_effect: !matches!(
                request["kind"].as_str(),
                Some(
                    "list"
                        | "text"
                        | "git_status"
                        | "git_diff"
                        | "browser"
                        | "browser_inspect"
                        | "computer_inspect"
                )
            ),
            read_header: frames
                .first()
                .map(|(_, data)| {
                    serde_json::from_str(data)
                        .map_err(Error::from)
                        .and_then(|v| ErasedProjection::from_value(&v))
                })
                .transpose()?,
        };
        require(context.valid(), "invalid retained worker context")?;
        let context = serde_json::to_string(&context)?;
        sqlx::query("INSERT INTO memory_erased_reads(id,context) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET context=excluded.context")
            .bind(&operation).bind(&context).execute(&mut *db).await?;
        for (sequence, _) in frames {
            sqlx::query(
                "INSERT OR IGNORE INTO link_erased_work(device,sequence,context) VALUES(?,?,?)",
            )
            .bind(&device)
            .bind(sequence)
            .bind(&context)
            .execute(&mut *db)
            .await?;
            sqlx::query("UPDATE link_outbox SET data='null' WHERE device=? AND sequence=?")
                .bind(&device)
                .bind(sequence)
                .execute(&mut *db)
                .await?;
        }
    }
    let revisions: Vec<String> = sqlx::query_scalar("SELECT task_revision FROM link_reads WHERE task_id=? UNION SELECT json_extract(data,'$.token.revision') FROM operations WHERE json_extract(data,'$.token.task_id')=? UNION SELECT json_extract(data,'$.proposal.token.revision') FROM operation_requests WHERE task_id=? UNION SELECT json_extract(data,'$.token.revision') FROM runs WHERE json_extract(data,'$.token.task_id')=?")
        .bind(id).bind(id).bind(id).bind(id).fetch_all(&mut *db).await?;
    for revision in revisions {
        let revision = revision
            .parse()
            .map_err(|_| Error::Rejected("invalid retained revision".into()))?;
        super::super::derived_results::erase_registered(db, id, revision).await?;
    }
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id,data FROM runs WHERE json_extract(data,'$.token.task_id')=?")
            .bind(id)
            .fetch_all(&mut *db)
            .await?;
    for (run_id, data) in rows {
        let mut run: crate::Run = serde_json::from_str(&data)?;
        run.checkpoint = json!({"phase":"task_retention_expired"});
        super::super::super::put(db, "runs", &run_id, &run).await?;
    }
    // Preserve operation identity/state and the financial amounts. Remove text
    // evidence and target fingerprints, which are no longer retained content.
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id,data FROM operations WHERE json_extract(data,'$.token.task_id')=?",
    )
    .bind(id)
    .fetch_all(&mut *db)
    .await?;
    for (operation, data) in rows {
        let mut value: serde_json::Value = serde_json::from_str(&data)?;
        value["target"] = json!("");
        value["intent_digest"] = json!("");
        sqlx::query("UPDATE operations SET data=? WHERE id=?")
            .bind(serde_json::to_string(&value)?)
            .bind(&operation)
            .execute(&mut *db)
            .await?;
        if let Some(data) =
            sqlx::query_scalar::<_, String>("SELECT data FROM receipts WHERE operation_id=?")
                .bind(&operation)
                .fetch_optional(&mut *db)
                .await?
        {
            let mut receipt: crate::Receipt = serde_json::from_str(&data)?;
            receipt.evidence.clear();
            sqlx::query("UPDATE receipts SET digest='',data=? WHERE operation_id=?")
                .bind(serde_json::to_string(&receipt)?)
                .bind(&operation)
                .execute(&mut *db)
                .await?;
        }
    }
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id,data FROM operation_requests WHERE task_id=?")
            .bind(id)
            .fetch_all(&mut *db)
            .await?;
    for (request, data) in rows {
        let mut value: serde_json::Value = serde_json::from_str(&data)?;
        value["proposal"]["target"] = json!("");
        value["intent_digest"] = json!("");
        value["reason"] = json!("task_retention_expired");
        sqlx::query("UPDATE operation_requests SET data=? WHERE id=?")
            .bind(serde_json::to_string(&value)?)
            .bind(request)
            .execute(&mut *db)
            .await?;
    }
    sqlx::query("UPDATE budget SET evidence=NULL WHERE task_id=?")
        .bind(id)
        .execute(&mut *db)
        .await?;
    scrub_messages(db).await
}

pub(super) async fn shared_sources(db: &mut SqliteConnection) -> Result<()> {
    let events: Vec<String> = sqlx::query_scalar("SELECT e.id FROM events e WHERE NOT EXISTS(SELECT 1 FROM settings s WHERE s.key='retention_erased_event:'||e.id) AND EXISTS(SELECT 1 FROM event_tasks x WHERE x.event_id=e.id) AND NOT EXISTS(SELECT 1 FROM event_tasks x WHERE x.event_id=e.id AND NOT EXISTS(SELECT 1 FROM settings s WHERE s.key='retention_retired_task:'||x.task_id))")
        .fetch_all(&mut *db).await?;
    for event in events {
        sqlx::query("INSERT OR IGNORE INTO settings(key,value) VALUES(?,'1')")
            .bind(format!("retention_erased_event:{event}"))
            .execute(&mut *db)
            .await?;
        sqlx::query("INSERT OR IGNORE INTO task_retention_messages(id,task_id) SELECT c.id,min(x.task_id) FROM link_chat c JOIN events e ON e.source_id=c.id OR 'reply:'||e.source_id=c.id JOIN event_tasks x ON x.event_id=e.id WHERE e.id=? GROUP BY c.id")
            .bind(&event).execute(&mut *db).await?;
        sqlx::query("UPDATE events SET digest='',payload=?,outcome=? WHERE id=?")
            .bind(json!({"commands":[],"source_erased":true,"retention_expired":true}).to_string())
            .bind(serde_json::to_string(&crate::Outcome::Rejected {
                reason: "source_retention_expired".into(),
            })?)
            .bind(&event)
            .execute(&mut *db)
            .await?;
        sqlx::query("DELETE FROM conflicts WHERE event_id=?")
            .bind(&event)
            .execute(&mut *db)
            .await?;
    }
    scrub_messages(db).await
}

async fn scrub_messages(db: &mut SqliteConnection) -> Result<()> {
    sqlx::query("UPDATE link_chat SET body='內容已到期並清除。' WHERE id IN (SELECT m.id FROM task_retention_messages m JOIN settings s ON s.key='retention_retired_task:'||m.task_id)").execute(&mut *db).await?;
    let frames: Vec<(String,i64,String)> = sqlx::query_as("SELECT device,sequence,data FROM link_outbox o WHERE json_extract(o.data,'$.payload.kind') IN ('snapshot','derived_snapshot') AND (EXISTS(SELECT 1 FROM json_each(o.data,'$.payload.tasks') t JOIN settings s ON s.key='retention_retired_task:'||json_extract(t.value,'$.id')) OR EXISTS(SELECT 1 FROM json_each(o.data,'$.payload.messages') m JOIN task_retention_messages t ON t.id=json_extract(m.value,'$.id') JOIN settings s ON s.key='retention_retired_task:'||t.task_id))")
        .fetch_all(&mut *db).await?;
    for (device, sequence, data) in frames {
        let value: serde_json::Value = serde_json::from_str(&data)?;
        let header = ErasedProjection::from_value(&value)?;
        require(
            header.device == device && header.sequence == sequence.to_string(),
            "retained frame identity changed",
        )?;
        sqlx::query("INSERT OR IGNORE INTO link_erased_frames(device,sequence,data) VALUES(?,?,?)")
            .bind(&device)
            .bind(sequence)
            .bind(serde_json::to_string(&header)?)
            .execute(&mut *db)
            .await?;
        sqlx::query("UPDATE link_outbox SET data='null' WHERE device=? AND sequence=?")
            .bind(&device)
            .bind(sequence)
            .execute(&mut *db)
            .await?;
    }
    Ok(())
}
