//! Preserve readable current evidence. No ACK or identity check proves effects.
use super::*;

#[derive(Default)]
pub(super) struct Current {
    pub image: Option<Vec<u8>>,
    pub retired: Vec<String>,
    pub tokens: Vec<String>,
    pub floor: Option<String>,
    pub retired_tasks: Vec<String>,
    pub epoch: u64,
}

pub(super) fn day(now: i64) -> Result<String> {
    let date = chrono::DateTime::from_timestamp_millis(now)
        .ok_or_else(|| Error::Rejected("invalid restore clock".into()))?
        .with_timezone(&chrono_tz::Asia::Taipei)
        .date_naive();
    let value = date.format("%Y-%m-%d").to_string();
    valid_day(&value)?;
    Ok(value)
}
pub(super) fn valid_day(value: &str) -> Result<()> {
    require(
        value.len() == 10
            && chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .is_ok_and(|day| day.format("%Y-%m-%d").to_string() == value),
        "invalid restore accounting day",
    )
}

pub(super) async fn capture(
    admission: &crate::DatabaseMaintenance,
    manifest: &Restrictions,
    secret: &Secret,
    owner: &str,
) -> Result<Current> {
    let identity = source_identity(admission, owner, manifest, secret).await?;
    if !files::exists(admission.database())? {
        return Ok(Current::default());
    }
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(admission.database())
        .read_only(true)
        .create_if_missing(false);
    let Ok(pool) = admission.pool_options().connect_with(options).await else {
        return Ok(Current::default());
    };
    let result: Result<Current> = async {
        let mut result = Current::default();
        if let Ok(Some(value)) = sqlx::query_scalar::<_,String>("SELECT value FROM settings WHERE key='epoch'").fetch_optional(&pool).await {
            result.epoch = value.parse().map_err(|_| Error::Rejected("invalid current run epoch".into()))?;
            require(result.epoch < i64::MAX as u64, "current run epoch exhausted")?;
        }
        if let Ok(ids) = sqlx::query_scalar::<_,String>("SELECT id FROM tasks LIMIT 100001").fetch_all(&pool).await {
            require(ids.len() <= 100000 && ids.iter().all(|id| !id.is_empty() && id.len() <= 128), "invalid current task evidence")?;
            result.retired_tasks.extend(ids);
        }
        if let Ok(ids) = sqlx::query_scalar::<_,String>("SELECT substr(key,22) FROM settings WHERE key LIKE 'restore_retired_task:%' LIMIT 100001").fetch_all(&pool).await {
            require(ids.len() <= 100000 && ids.iter().all(|id| !id.is_empty() && id.len() <= 128), "invalid retired task evidence")?;
            result.retired_tasks.extend(ids);
        }
        // These can only remove authority. A partial readable source is never
        // promoted into a claim of complete receipts or accounting.
        if let Ok(rows) = sqlx::query_as::<_, (String, String)>("SELECT id,token_digest FROM link_device LIMIT 100001").fetch_all(&pool).await {
            require(rows.len() <= 100000, "current device evidence capacity")?;
            for (id, token) in rows {
                require(uuid::Uuid::parse_str(&id).is_ok() && token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()), "invalid current credential evidence")?;
                result.retired.push(id.clone());
                result.retired.push(uuid::Uuid::parse_str(&id).unwrap().to_string());
                result.tokens.push(token);
            }
        }
        if let Ok(tokens) = sqlx::query_scalar::<_, String>("SELECT token_digest FROM link_pair_tokens LIMIT 100001").fetch_all(&pool).await {
            require(tokens.len() <= 100000 && tokens.iter().all(|token| token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit())), "invalid current token history")?;
            result.tokens.extend(tokens);
        }
        if let Ok(ids) = sqlx::query_scalar::<_, String>("SELECT substr(key,24) FROM settings WHERE key LIKE 'restore_retired_device:%' LIMIT 100001").fetch_all(&pool).await {
            require(ids.len() <= 100000 && ids.iter().all(|id| uuid::Uuid::parse_str(id).is_ok()), "invalid retired identity evidence")?;
            result.retired.extend(ids);
        }
        if let Ok(Some(value)) = sqlx::query_scalar::<_, Option<String>>("SELECT MAX(day) FROM budget").fetch_one(&pool).await {
            valid_day(&value)?; result.floor = Some(value);
        }
        if let Ok(Some(value)) = sqlx::query_scalar::<_, String>("SELECT value FROM settings WHERE key='restore_budget_floor_day'").fetch_optional(&pool).await {
            valid_day(&value)?;
            if result.floor.as_ref().is_none_or(|old| old < &value) { result.floor = Some(value); }
        }
        if identity {
            let integrity = sqlx::query_scalar::<_, String>("PRAGMA integrity_check").fetch_one(&pool).await;
            if matches!(integrity.as_deref(), Ok("ok")) {
                let mut tx = pool.begin().await?;
                let snapshot = tx.serialize(None).await?;
                require(snapshot.len() <= crypto::MAX_DATABASE, "current snapshot capacity")?;
                tx.commit().await?;
                result.image = Some(snapshot.to_vec());
            }
        }
        Ok(result)
    }.await;
    pool.close().await;
    result
}

async fn merge_tasks(db: &mut sqlx::SqliteConnection) -> Result<()> {
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT id,data FROM recovery.tasks")
        .fetch_all(&mut *db)
        .await?;
    require(rows.len() <= 100000, "current task evidence capacity")?;
    for (id, data) in rows {
        let mut current: crate::Task = serde_json::from_str(&data)?;
        require(
            current.id == id && current.revision > 0 && current.revision <= i64::MAX as u64,
            "invalid current task binding",
        )?;
        if let Some(data) = sqlx::query_scalar::<_, String>("SELECT data FROM tasks WHERE id=?")
            .bind(&id)
            .fetch_optional(&mut *db)
            .await?
        {
            let mut backup: crate::Task = serde_json::from_str(&data)?;
            if backup.revision > current.revision {
                continue;
            }
            if backup.revision == current.revision {
                require(
                    backup.goal == current.goal
                        && backup.scope == current.scope
                        && serde_json::to_value(&backup.decisions)?
                            == serde_json::to_value(&current.decisions)?,
                    "conflicting task revision evidence",
                )?;
                require(
                    !(backup.state.terminal()
                        && current.state.terminal()
                        && backup.state != current.state),
                    "conflicting terminal task evidence",
                )?;
                if backup.state.terminal() && !current.state.terminal() {
                    std::mem::swap(&mut backup, &mut current);
                }
                current.model_calls = current.model_calls.max(backup.model_calls);
                current.tool_calls = current.tool_calls.max(backup.tool_calls);
                current.retry_count = current.retry_count.max(backup.retry_count);
                current.cancel_requested |= backup.cancel_requested;
                for item in backup.evidence {
                    if !current.evidence.contains(&item) {
                        current.evidence.push(item);
                    }
                }
            }
        }
        crate::store::put(db, "tasks", &id, &current).await?;
    }
    Ok(())
}

async fn merge_operations(db: &mut sqlx::SqliteConnection) -> Result<()> {
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT id,data FROM recovery.operations")
        .fetch_all(&mut *db)
        .await?;
    for (id, data) in rows {
        let mut current: crate::Operation = serde_json::from_str(&data)?;
        let erased = serde_json::json!({"source_erased":true});
        let expected = if let Some(request_id) = id.strip_prefix("approved:") {
            let data: String =
                sqlx::query_scalar("SELECT data FROM recovery.operation_requests WHERE id=?")
                    .bind(request_id)
                    .fetch_one(&mut *db)
                    .await?;
            let request: crate::OperationRequest = serde_json::from_str(&data)?;
            require(
                request.proposal.id == request_id
                    && request.proposal.token == current.token
                    && request.proposal.target == current.target
                    && request.proposal.intent == current.intent
                    && request.intent_digest == current.intent_digest,
                "invalid approved operation evidence",
            )?;
            crate::store::digest(
                &serde_json::json!({"source_event":request.source_event,"proposal":request.proposal}),
            )?
        } else {
            crate::store::digest(
                &serde_json::json!({"token":current.token,"target":current.target,"intent":current.intent}),
            )?
        };
        require(
            current.id == id && (current.intent == erased || expected == current.intent_digest),
            "invalid current operation binding",
        )?;
        if let Some(data) =
            sqlx::query_scalar::<_, String>("SELECT data FROM operations WHERE id=?")
                .bind(&id)
                .fetch_optional(&mut *db)
                .await?
        {
            let backup: crate::Operation = serde_json::from_str(&data)?;
            let was_erased = backup.intent == erased || current.intent == erased;
            let mut a = backup.clone();
            let mut b = current.clone();
            if a.intent == erased || b.intent == erased {
                a.intent = erased.clone();
                b.intent = erased.clone();
                current.intent = erased.clone();
            }
            a.state = crate::OperationState::Unknown;
            b.state = crate::OperationState::Unknown;
            require(a == b, "conflicting original operation intent")?;
            let known = |state: &crate::OperationState| {
                matches!(
                    state,
                    crate::OperationState::Completed | crate::OperationState::NotExecuted
                )
            };
            require(
                !(known(&backup.state) && known(&current.state) && backup.state != current.state),
                "conflicting original operation result",
            )?;
            if known(&backup.state) && !known(&current.state) {
                current = backup;
            }
            if was_erased {
                current.intent = erased.clone();
            }
        }
        crate::store::put(db, "operations", &id, &current).await?;
    }
    let rows: Vec<(String, String, String)> =
        sqlx::query_as("SELECT operation_id,digest,data FROM recovery.receipts")
            .fetch_all(&mut *db)
            .await?;
    for (id, digest, data) in rows {
        let mut current: crate::Receipt = serde_json::from_str(&data)?;
        require(
            current.operation_id == id,
            "invalid current receipt binding",
        )?;
        let operation: crate::Operation = crate::store::load(db, "operations", &id).await?;
        require(
            operation.token == current.token
                && (current.evidence.is_empty() || crate::store::digest(&current)? == digest),
            "invalid original receipt proof",
        )?;
        if let Some(data) =
            sqlx::query_scalar::<_, String>("SELECT data FROM receipts WHERE operation_id=?")
                .bind(&id)
                .fetch_optional(&mut *db)
                .await?
        {
            let backup: crate::Receipt = serde_json::from_str(&data)?;
            require(
                backup.operation_id == current.operation_id
                    && backup.token == current.token
                    && backup.success == current.success,
                "conflicting original receipt identity",
            )?;
            require(
                backup.evidence == current.evidence
                    || backup.evidence.is_empty()
                    || current.evidence.is_empty(),
                "conflicting original receipt evidence",
            )?;
            if backup.evidence.is_empty() {
                current.evidence.clear();
            }
        }
        sqlx::query("INSERT INTO receipts VALUES(?,?,?) ON CONFLICT(operation_id) DO UPDATE SET data=excluded.data")
            .bind(id).bind(digest).bind(serde_json::to_string(&current)?).execute(&mut *db).await?;
    }
    Ok(())
}

pub(super) async fn merge(candidate: &Store, source: &Path) -> Result<()> {
    let mut connection = candidate.pool.acquire().await?;
    sqlx::query("ATTACH DATABASE ? AS recovery")
        .bind(
            source
                .to_str()
                .ok_or_else(|| Error::Rejected("current snapshot path encoding".into()))?,
        )
        .execute(&mut *connection)
        .await?;
    let result: Result<()> = async {
        let mut tx = sqlx::Acquire::begin(&mut *connection).await?;
        sqlx::query("PRAGMA defer_foreign_keys=ON").execute(&mut *tx).await?;
        let conflicting: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM receipts a JOIN recovery.receipts b USING(operation_id) WHERE a.digest!=b.digest)+(SELECT count(*) FROM events a JOIN recovery.events b USING(id) WHERE a.digest!=b.digest OR a.source!=b.source OR a.source_id!=b.source_id)+(SELECT count(*) FROM budget a JOIN recovery.budget b USING(call_id) WHERE a.task_id!=b.task_id OR a.day!=b.day OR a.reserved!=b.reserved OR a.rate_version!=b.rate_version OR (a.state='settled' AND b.state='settled' AND (a.charged!=b.charged OR a.evidence IS NOT b.evidence)))")
            .fetch_one(&mut *tx).await?;
        require(conflicting == 0, "conflicting original execution or cost evidence")?;
        merge_tasks(&mut tx).await?;
        merge_operations(&mut tx).await?;
        // Restore-generated notifications have local sequence numbers. Merge
        // current notifications by their stable dedupe key, never by that slot.
        sqlx::query("INSERT INTO outbox(dedupe_key,payload,acknowledged) SELECT dedupe_key,payload,acknowledged FROM recovery.outbox WHERE 1 ON CONFLICT(dedupe_key) DO UPDATE SET payload=excluded.payload,acknowledged=MAX(outbox.acknowledged,excluded.acknowledged)").execute(&mut *tx).await?;
        // Both images have already passed migration, integrity, foreign key,
        // current restriction and work-fence checks. Only known app tables
        // enter the candidate. Original receipt digests must match above.
        for table in ["events","conflicts","event_tasks","runs","operation_requests","task_memory_sources","memories","memory_events","link_pairings","link_device","link_pair_tokens","link_pair_claims","link_inbox","link_outbox","link_reads","link_chat","link_artifacts","link_artifact_parts","memory_derived_messages","memory_erased_reads","link_erased_frames","link_erased_work","link_worker_fences","link_worker_cleanup","schedules","budget","task_retention","task_retention_messages"] {
            let columns: Vec<(String,i64)> = sqlx::query_as("SELECT name,pk FROM pragma_table_xinfo(?) WHERE hidden=0 ORDER BY cid").bind(table).fetch_all(&mut *tx).await?;
            require(!columns.is_empty() && columns.len() <= 32 && columns.iter().all(|(name,_)| name.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')), "unexpected recovery table columns")?;
            let names = columns.iter().map(|(name,_)| format!("\"{name}\"")).collect::<Vec<_>>().join(",");
            let primary = columns.iter().filter(|(_,key)| *key > 0).map(|(name,_)| format!("\"{name}\"")).collect::<Vec<_>>().join(",");
            let updates = columns.iter().filter(|(_,key)| *key == 0).map(|(name,_)| {
                let value = match (table, name.as_str()) {
                    ("budget","state") => "CASE WHEN budget.state='settled' OR excluded.state='settled' THEN 'settled' ELSE 'unknown' END".to_owned(),
                    ("budget","charged") => "CASE WHEN budget.state='settled' THEN budget.charged WHEN excluded.state='settled' THEN excluded.charged ELSE MAX(budget.charged,excluded.charged) END".to_owned(),
                    ("budget","evidence") => "CASE WHEN budget.state='settled' THEN budget.evidence ELSE excluded.evidence END".to_owned(),
                    ("link_outbox"|"outbox","acknowledged") => format!("MAX({table}.acknowledged,excluded.acknowledged)"),
                    _ => format!("excluded.\"{name}\""),
                };
                format!("\"{name}\"={value}")
            }).collect::<Vec<_>>().join(",");
            let sql = if primary.is_empty() {
                format!("INSERT INTO \"{table}\"({names}) SELECT {names} FROM recovery.\"{table}\" EXCEPT SELECT {names} FROM main.\"{table}\"")
            } else {
                let conflict = if updates.is_empty() { "DO NOTHING".to_owned() } else {
                    let newer = match table {
                        "memories" => " WHERE excluded.revision>=memories.revision",
                        "schedules" => " WHERE CAST(json_extract(excluded.data,'$.revision') AS INTEGER)>=CAST(json_extract(schedules.data,'$.revision') AS INTEGER)",
                        "link_reads" => " WHERE link_reads.result IS NULL OR excluded.result IS NOT NULL",
                        _ => "",
                    };
                    format!("DO UPDATE SET {updates}{newer}")
                };
                format!("INSERT INTO \"{table}\"({names}) SELECT {names} FROM recovery.\"{table}\" WHERE 1 ON CONFLICT({primary}) {conflict}")
            };
            // The table list is constant. Column identifiers are checked above;
            // the remaining SQL fragments are fixed application policy.
            sqlx::query(sqlx::AssertSqlSafe(sql)).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }.await;
    let detached = sqlx::query("DETACH DATABASE recovery")
        .execute(&mut *connection)
        .await;
    result?;
    detached?;
    Ok(())
}

pub(super) async fn finish(
    candidate: &Store,
    current: &Current,
    generation: &str,
    now: i64,
    versions: &Versions,
) -> Result<String> {
    let mut floor = day(now)?;
    for value in [
        current.floor.clone(),
        sqlx::query_scalar::<_, String>(
            "SELECT value FROM settings WHERE key='restore_budget_floor_day'",
        )
        .fetch_optional(&candidate.pool)
        .await?,
        sqlx::query_scalar::<_, Option<String>>("SELECT MAX(day) FROM budget")
            .fetch_one(&candidate.pool)
            .await?,
    ]
    .into_iter()
    .flatten()
    {
        valid_day(&value)?;
        floor = floor.max(value);
    }
    let mut tx = candidate.pool.begin_with("BEGIN IMMEDIATE").await?;
    let previous_epoch: Option<String> =
        sqlx::query_scalar("SELECT value FROM settings WHERE key='epoch'")
            .fetch_optional(&mut *tx)
            .await?;
    let run_epoch: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(CAST(json_extract(data,'$.token.epoch') AS INTEGER)) FROM runs",
    )
    .fetch_one(&mut *tx)
    .await?;
    let epoch = previous_epoch
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| Error::Rejected("invalid backup run epoch".into()))
        })
        .transpose()?
        .unwrap_or(0)
        .max(current.epoch)
        .max(run_epoch.unwrap_or(0) as u64);
    require(epoch < i64::MAX as u64, "restore run epoch exhausted")?;
    sqlx::query("INSERT INTO settings VALUES('epoch',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(epoch.to_string()).execute(&mut *tx).await?;
    for id in &current.retired {
        sqlx::query("INSERT OR IGNORE INTO settings VALUES(?, '1')")
            .bind(format!("restore_retired_device:{id}"))
            .execute(&mut *tx)
            .await?;
    }
    for token in &current.tokens {
        sqlx::query("INSERT OR IGNORE INTO link_pair_tokens VALUES(?)")
            .bind(token)
            .execute(&mut *tx)
            .await?;
    }
    for id in &current.retired_tasks {
        sqlx::query("INSERT OR IGNORE INTO settings VALUES(?,'1')")
            .bind(format!("restore_retired_task:{id}"))
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("INSERT OR IGNORE INTO settings SELECT 'restore_retired_task:'||id,'1' FROM tasks")
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE settings SET value='0' WHERE key='memory_redaction_epoch'")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM settings WHERE key='last_backup'")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    candidate.initialize_memory().await?;
    candidate.list_memories(now).await?;
    candidate.recover(versions, now).await?;
    dispatch::fence(candidate).await?;
    sqlx::query("INSERT INTO settings VALUES('restore_budget_floor_day',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(&floor).execute(&candidate.pool).await?;
    sqlx::query("INSERT INTO settings VALUES('restore_generation',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(generation).execute(&candidate.pool).await?;
    Ok(floor)
}
