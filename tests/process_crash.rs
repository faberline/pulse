//! Actual process exits at durable boundaries. No sleeps or external services.
use pulse_core::*;
use serde_json::json;
use sqlx::{
    Connection, Executor,
    sqlite::{SqliteConnectOptions, SqliteConnection},
};
use std::{path::Path, process::Command};

const NOW: i64 = 1_800_000_000_000;
fn payload() -> serde_json::Value {
    json!({"commands":[{"kind":"create","id":"a","goal":"fixture goal","scope":"pulse:read","priority":0,"requires_device":false}]})
}
async fn connect(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true),
    )
    .await
    .unwrap()
}

#[test]
#[ignore = "child harness, executed explicitly by every_crash_boundary_is_recoverable"]
fn crash_boundary_child() {
    let path = std::path::PathBuf::from(std::env::var_os("PULSE_CRASH_DB").unwrap());
    let mode = std::env::var("PULSE_CRASH_MODE").unwrap();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let store=Store::open(&path,"owner").await.unwrap();
        if mode=="before_commit" {
            let mut db=connect(&path).await;
            db.execute("BEGIN IMMEDIATE").await.unwrap();
            sqlx::query("INSERT INTO events(id,source,source_id,origin,digest,payload,received_at) VALUES('uncommitted','ui','create','human','uncommitted','{}',?)").bind(NOW).execute(&mut db).await.unwrap();
            // Exit without running Rust destructors or a transaction rollback.
            std::process::exit(73);
        }
        let event=store.ingest(Origin::Human,"ui","create",payload(),NOW).await.unwrap();
        if mode=="after_ingest" {std::process::exit(73);}
        store.route(&event.event_id).await.unwrap();
        if mode=="after_route" {std::process::exit(73);}
        let run=store.claim_next(&Versions::fixture(),NOW+1).await.unwrap().unwrap();
        store.advance(&run.token,State::Ready,json!({"planned":true}),"fixture plan",None,NOW+2).await.unwrap();
        let op=store.prepare_operation(&run.token,"fixture:effect",json!({"value":1}),NOW+3).await.unwrap();
        let mut sink=connect(&path.with_extension("effects")).await;
        sink.execute("PRAGMA synchronous=FULL").await.unwrap();
        sink.execute("CREATE TABLE effects(operation_id TEXT NOT NULL)").await.unwrap();
        sqlx::query("INSERT INTO effects VALUES(?)").bind(op.id).execute(&mut sink).await.unwrap();
        std::process::exit(73);
    });
}

#[tokio::test]
async fn every_crash_boundary_is_recoverable() {
    let executable = std::env::current_exe().unwrap();
    for mode in [
        "before_commit",
        "after_ingest",
        "after_route",
        "after_effect",
    ] {
        for sample in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("core.db");
            let result = Command::new(&executable)
                .args(["--exact", "crash_boundary_child", "--ignored"])
                .env("PULSE_CRASH_DB", &path)
                .env("PULSE_CRASH_MODE", mode)
                .output()
                .unwrap();
            assert_eq!(
                result.status.code(),
                Some(73),
                "mode {mode}, sample {sample}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            let store = Store::open(&path, "owner").await.unwrap();
            let event = store
                .ingest(Origin::Human, "ui", "create", payload(), NOW + 4)
                .await
                .unwrap();
            assert_eq!(event.duplicate, mode != "before_commit");
            assert!(matches!(
                store.route(&event.event_id).await.unwrap(),
                Outcome::Applied { .. }
            ));
            if mode == "after_effect" {
                store.recover(&Versions::fixture(), NOW + 5).await.unwrap();
                assert!(
                    store
                        .claim_next(&Versions::fixture(), NOW + 6)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert_eq!(store.task("a").await.unwrap().state, State::OutcomeUnknown);
                let op = store.pending_operations("a").await.unwrap().remove(0);
                let mut sink = connect(&path.with_extension("effects")).await;
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effects")
                    .fetch_one(&mut sink)
                    .await
                    .unwrap();
                let observed: String = sqlx::query_scalar("SELECT operation_id FROM effects")
                    .fetch_one(&mut sink)
                    .await
                    .unwrap();
                assert_eq!(count, 1);
                assert_eq!(observed, op.id);
                let receipt = Receipt {
                    operation_id: op.id.clone(),
                    token: op.token,
                    success: true,
                    evidence: "sink lookup found the original effect".into(),
                };
                store
                    .reconcile(&op.id, Resolution::Executed(receipt), NOW + 7)
                    .await
                    .unwrap();
                let run = store
                    .claim_next(&Versions::fixture(), NOW + 8)
                    .await
                    .unwrap()
                    .unwrap();
                store
                    .finish(
                        &run.token,
                        true,
                        "goal independently checked against sink",
                        NOW + 9,
                    )
                    .await
                    .unwrap();
                assert_eq!(store.task("a").await.unwrap().state, State::Succeeded);
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effects")
                    .fetch_one(&mut sink)
                    .await
                    .unwrap();
                assert_eq!(count, 1, "no replayed effect");
            } else {
                assert_eq!(store.task("a").await.unwrap().revision, 1);
                assert_eq!(store.outbox(100).await.unwrap().len(), 1);
            }
            store.close().await;
        }
    }
}
