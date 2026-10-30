//! Local core demonstration. Not the final desktop UI or a real model/tool.
use pulse_core::*;
use serde_json::json;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: cargo run --example durable_demo -- /path/to/demo.db")?;
    let store = Store::open(&path, "demo-owner").await?;
    let now = 1_800_000_000_000;
    let source=store.ingest(Origin::Human,"demo","two-tasks",json!({"commands":[
        {"kind":"create","id":"analysis","goal":"fixture analysis of Pulse","scope":"pulse:read","priority":0,"requires_device":false},
        {"kind":"create","id":"local","goal":"wait for local Pulse files","scope":"pulse:read","priority":0,"requires_device":true}
    ]}),now).await?;
    store.route(&source.event_id).await?;
    if store.task("analysis").await?.state.terminal() {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &json!({"replayed_source":source.duplicate,"analysis":store.task("analysis").await?,"local":store.task("local").await?})
            )?
        );
        return Ok(());
    }
    store.recover(&Versions::fixture(), now + 1).await?;
    let pending = store.pending_operations("analysis").await?;
    let operation = if let Some(operation) = pending.into_iter().next() {
        operation
    } else {
        let run = store
            .claim_next(&Versions::fixture(), now + 2)
            .await?
            .ok_or("no runnable task")?;
        store
            .advance(
                &run.token,
                State::Ready,
                json!({"fixture_step":"ready"}),
                "fixture plan",
                None,
                now + 3,
            )
            .await?;
        store
            .prepare_operation(
                &run.token,
                "fixture:analysis",
                json!({"result":"fixture artifact"}),
                now + 4,
            )
            .await?
    };
    // Simulate losing the process after an external result, before storing its receipt.
    store.close().await;
    let store = Store::open(&path, "demo-owner").await?;
    store.recover(&Versions::fixture(), now + 5).await?;
    assert!(
        store
            .claim_next(&Versions::fixture(), now + 6)
            .await?
            .is_none()
    );
    store
        .reconcile(
            &operation.id,
            Resolution::Executed(Receipt {
                operation_id: operation.id.clone(),
                token: operation.token,
                success: true,
                evidence: "fixture artifact was independently found".into(),
            }),
            now + 7,
        )
        .await?;
    let run = store
        .claim_next(&Versions::fixture(), now + 8)
        .await?
        .ok_or("missing resumable task")?;
    store
        .finish(
            &run.token,
            true,
            "fixture goal and artifact verified",
            now + 9,
        )
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"mode":"fixture_only","analysis":store.task("analysis").await?,"local":store.task("local").await?,"health":store.health(now+10).await?})
        )?
    );
    Ok(())
}
