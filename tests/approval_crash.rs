//! Real hard exits around approval, intent, effect and receipt commits.
use pulse_core::*;
use serde_json::json;
use sqlx::{
    Connection, Executor,
    sqlite::{SqliteConnectOptions, SqliteConnection},
};
use std::{path::Path, process::Command};

const NOW: i64 = 1_800_000_000_000;
async fn sink(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true),
    )
    .await
    .unwrap()
}
fn approve(request: &OperationRequest) -> serde_json::Value {
    json!({"commands":[{"kind":"approve_operation","binding":{"task_id":"task","expected_revision":"1","request_id":request.proposal.id,"intent_digest":request.intent_digest}}]})
}
#[test]
#[ignore = "child harness; parent runs every boundary explicitly"]
fn approval_boundary_child() {
    let path = std::path::PathBuf::from(std::env::var_os("PULSE_APPROVAL_CRASH_DB").unwrap());
    let mode = std::env::var("PULSE_APPROVAL_CRASH_MODE").unwrap();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let store=Store::open(&path,"owner").await.unwrap();
        let mut effects=sink(&path.with_extension("effects")).await;
        effects.execute("PRAGMA synchronous=FULL; CREATE TABLE effects(operation_id TEXT NOT NULL)").await.unwrap();
        let event=store.ingest(Origin::Human,"ui","create",json!({"commands":[{"kind":"create","id":"task","goal":"count one fixture effect","scope":"pulse:read","priority":0,"requires_device":false}]}),NOW).await.unwrap();
        store.route(&event.event_id).await.unwrap();
        let run=store.claim_next(&Versions::fixture(),NOW+1).await.unwrap().unwrap();
        let proposal=OperationProposal{id:"request".into(),token:run.token,target:"fixture:counter".into(),intent:json!({"delta":1}),expires_at:NOW+60_000};
        let event=store.ingest(Origin::Tool,"planner","proposal",json!({"commands":[{"kind":"query","ids":["task"]}],"operation_proposal":proposal}),NOW+2).await.unwrap();
        store.route(&event.event_id).await.unwrap();
        let request=store.propose_operation(&event.event_id,proposal,NOW+2).await.unwrap();
        if mode=="pending" { std::process::exit(73); }
        let event=store.ingest(Origin::Human,"ui","approve",approve(&request),NOW+3).await.unwrap();
        assert!(matches!(store.route(&event.event_id).await.unwrap(),Outcome::Applied{..}));
        if mode=="approved" { std::process::exit(73); }
        let op=store.prepare_approved_operation(&request.proposal.token,"request",NOW+4).await.unwrap();
        if mode=="intent" { std::process::exit(73); }
        // No unique constraint: duplicate execution would create another physical row.
        sqlx::query("INSERT INTO effects VALUES(?)").bind(&op.id).execute(&mut effects).await.unwrap();
        if mode=="effect" { std::process::exit(73); }
        store.record_receipt(&Receipt{operation_id:op.id,token:op.token,success:true,evidence:"original effect persisted".into()}).await.unwrap();
        std::process::exit(73);
    });
}

#[tokio::test]
async fn approval_intents_and_real_fixture_effects_survive_process_crashes_without_replay() {
    for mode in ["pending", "approved", "intent", "effect", "receipt"] {
        for sample in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("core.db");
            let result = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "approval_boundary_child", "--ignored"])
                .env("PULSE_APPROVAL_CRASH_DB", &path)
                .env("PULSE_APPROVAL_CRASH_MODE", mode)
                .output()
                .unwrap();
            assert_eq!(
                result.status.code(),
                Some(73),
                "{mode}/{sample}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            let store = Store::open(&path, "owner").await.unwrap();
            let request = store.operation_request("request").await.unwrap();
            store.recover(&Versions::fixture(), NOW + 5).await.unwrap();
            let mut effects = sink(&path.with_extension("effects")).await;
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effects")
                .fetch_one(&mut effects)
                .await
                .unwrap();
            assert_eq!(
                count,
                if matches!(mode, "effect" | "receipt") {
                    1
                } else {
                    0
                }
            );
            let replay = store
                .propose_operation(&request.source_event, request.proposal.clone(), NOW + 6)
                .await
                .unwrap();
            if matches!(mode, "pending" | "approved") {
                assert_eq!(replay.state, ApprovalState::Invalidated);
                assert_eq!(store.task("task").await.unwrap().state, State::Blocked);
                assert!(
                    store
                        .claim_next(&Versions::fixture(), NOW + 6)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(store.pending_operations("task").await.unwrap().is_empty());
                if mode == "approved" {
                    let event = store
                        .ingest(Origin::Human, "ui", "approve", approve(&request), NOW + 3)
                        .await
                        .unwrap();
                    store.route(&event.event_id).await.unwrap();
                    assert_eq!(
                        store.operation_request("request").await.unwrap().state,
                        ApprovalState::Invalidated
                    );
                }
            } else {
                assert_eq!(replay.state, ApprovalState::Dispatched);
                if mode != "receipt" {
                    assert_eq!(
                        store.task("task").await.unwrap().state,
                        State::OutcomeUnknown
                    );
                    assert!(
                        store
                            .claim_next(&Versions::fixture(), NOW + 6)
                            .await
                            .unwrap()
                            .is_none()
                    );
                    let op = store.pending_operations("task").await.unwrap().remove(0);
                    if mode == "effect" {
                        let observed: String =
                            sqlx::query_scalar("SELECT operation_id FROM effects")
                                .fetch_one(&mut effects)
                                .await
                                .unwrap();
                        assert_eq!(observed, op.id);
                        store
                            .reconcile(
                                &op.id,
                                Resolution::Executed(Receipt {
                                    operation_id: op.id.clone(),
                                    token: op.token,
                                    success: true,
                                    evidence: "original operation found in fixture sink".into(),
                                }),
                                NOW + 7,
                            )
                            .await
                            .unwrap();
                    } else {
                        // The harness proves this process exited before the synchronous sink call.
                        store.reconcile(&op.id,Resolution::NotExecuted{evidence:"verified child death before any sink call; sink has no effect".into()},NOW+7).await.unwrap();
                    }
                }
                let next = store
                    .claim_next(&Versions::fixture(), NOW + 8)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    store
                        .prepare_approved_operation(&next.token, "request", NOW + 9)
                        .await
                        .is_err()
                );
                assert!(store.pending_operations("task").await.unwrap().is_empty());
            }
            assert!(
                store
                    .prepare_approved_operation(&request.proposal.token, "request", NOW + 10)
                    .await
                    .is_err()
            );
            let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effects")
                .fetch_one(&mut effects)
                .await
                .unwrap();
            assert_eq!(after, count, "no blind effect replay at {mode}");
            assert_ne!(store.task("task").await.unwrap().state, State::Succeeded);
        }
    }
}
