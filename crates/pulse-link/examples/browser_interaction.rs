//! Explicit native component test. Model and Human decisions are fixture-controlled.
//! This does not replace the separate SwiftUI/provider/process acceptance.
use axum::{
    Router,
    response::Html,
    routing::{get, post},
};
use pulse_core::{BrowserEffect, BrowserElementKind, OperationRequest, Origin, Store};
use pulse_link::{
    Result,
    artifact::ExecutedRead,
    browser_network::NetworkPolicy,
    browser_runtime::BrowserRuntime,
    browser_session::WorkBinding,
    executor::Executor,
    journal::Journal,
    protocol::*,
    provider::{Reply, ToolCall},
    remote,
    server::Hub,
};
use serde_json::json;
use std::{
    future::IntoFuture,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn ensure(ok: bool, why: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(pulse_link::Error::Rejected(why.into()))
    }
}

fn page(count: usize) -> Html<String> {
    Html(format!(
        "<!doctype html><meta charset=utf-8><title>Pulse interaction fixture</title><h1>Counter: {count}</h1><form action=/count method=post accept-charset=UTF-8><input type=hidden name=job value=pulse-fixture><button type=submit>Count once</button></form><label>Message <input type=text value=''></label>"
    ))
}
fn model(operation: ReadOp, id: &str) -> Reply {
    Reply {
        text: String::new(),
        tool: Some(ToolCall {
            call_id: id.into(),
            operation,
        }),
        usage: json!({}),
        cost: 0,
    }
}
async fn execute(
    hub: &Hub,
    journal: &Journal,
    epoch: u64,
    executor: &Executor,
    cut: Option<&str>,
) -> Result<ReadOutcome> {
    for envelope in hub.outgoing(&journal.device).await? {
        journal
            .accept(
                &envelope,
                hub.permitted(&envelope, epoch, pulse_link::now()).await?,
                epoch,
                true,
            )
            .await?;
        hub.ack(&journal.device, number(&envelope.sequence)?, &envelope.id)
            .await?;
    }
    let envelope = journal
        .take_work()
        .await?
        .ok_or_else(|| pulse_link::Error::Rejected("native work missing".into()))?;
    let ServerPayload::Read {
        operation_id,
        task_id,
        revision,
        operation,
    } = &envelope.payload
    else {
        unreachable!()
    };
    let binding = WorkBinding {
        operation_id: operation_id.clone(),
        task_id: task_id.clone(),
        revision: revision.clone(),
        device: journal.device.clone(),
        epoch,
    };
    let ExecutedRead { outcome, image } = executor.execute_bound(operation.clone(), &binding).await;
    let saved = outcome.clone();
    if cut == Some("effect") {
        ensure(
            matches!(saved, ReadOutcome::Completed { .. }),
            "effect failed before crash cut",
        )?;
        executor.maintain_browser(None, &[]).await?;
        std::process::exit(73);
    }
    journal.finish_work_image(&envelope, outcome, image).await?;
    if cut == Some("result") {
        executor.maintain_browser(None, &[]).await?;
        std::process::exit(73);
    }
    for envelope in journal.outgoing().await? {
        hub.receive(&journal.device, &envelope, pulse_link::now())
            .await?;
        journal
            .ack(number(&envelope.sequence)?, &envelope.id)
            .await?;
    }
    Ok(saved)
}
async fn proposal(hub: &Hub, run: &pulse_core::Run, reply: &Reply) -> Result<OperationRequest> {
    hub.model_proposal(&run.token, reply, pulse_link::now())
        .await?;
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT data FROM operation_requests ORDER BY rowid DESC LIMIT 1")
            .fetch_all(&hub.core.application_pool())
            .await?;
    Ok(serde_json::from_str(&rows[0])?)
}
async fn approve(hub: &Hub, request: &OperationRequest, key: &str, allowed: bool) -> Result<()> {
    let e=hub.core.ingest(Origin::Human,"native-fixture",key,json!({"commands":[{"kind":if allowed{"approve_operation"}else{"reject_operation"},"binding":{"task_id":request.proposal.token.task_id,"expected_revision":request.proposal.token.revision.to_string(),"request_id":request.proposal.id,"intent_digest":request.intent_digest}}]}),pulse_link::now()).await?;
    let outcome = hub.core.route(&e.event_id).await?;
    ensure(
        matches!(outcome, pulse_core::Outcome::Applied { .. }),
        "native approval failed",
    )
}
#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("recover") {
        let dir = PathBuf::from(
            std::env::args_os()
                .nth(2)
                .expect("native crash fixture directory"),
        );
        let cut = std::env::args().nth(3).expect("native crash cut");
        let (journal, _lock) =
            Journal::open(&dir.join("local"), "https://pulse.example/", &dir).await?;
        journal.recover().await?;
        journal.recover().await?;
        ensure(
            journal.take_work().await?.is_none(),
            "native action repeated after restart",
        )?;
        let outgoing = journal.outgoing().await?;
        let results: Vec<_> = outgoing
            .iter()
            .filter_map(|e| match &e.payload {
                DevicePayload::Result { outcome, .. } => Some(outcome),
                _ => None,
            })
            .collect();
        ensure(results.len() == 1, "recovered native result count")?;
        ensure(
            if cut == "effect" {
                matches!(results[0], ReadOutcome::Unknown { .. })
            } else {
                matches!(results[0], ReadOutcome::Completed { .. })
            },
            "native recovery verdict",
        )?;
        ensure(
            std::fs::read_to_string(dir.join("counter.txt"))? == "1",
            "native effect count changed on restart",
        )?;
        std::fs::write(
            dir.join("recovery.json"),
            serde_json::to_vec_pretty(
                &json!({"cut":cut,"counter":1,"reexecuted":false,"result":results[0],"pending_frames":outgoing.len()}),
            )?,
        )?;
        println!("Native crash recovery passed: {}", dir.display());
        return Ok(());
    }
    let cut = std::env::args().nth(2);
    ensure(
        cut.as_deref().is_none_or(|c| {
            matches!(
                c,
                "effect"
                    | "result"
                    | "stale"
                    | "reject"
                    | "external"
                    | "charset"
                    | "implicit"
                    | "direction"
                    | "wrap"
                    | "charset-field"
                    | "base"
            )
        }),
        "unknown native crash cut",
    )?;
    let driver = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .expect("absolute ChromeDriver path"),
    );
    let dir = tempfile::Builder::new()
        .prefix("pulse-browser-interaction-")
        .tempdir()?;
    if cut.is_some() {
        println!("Native crash fixture: {}", dir.path().display());
    }
    let counter = Arc::new(AtomicUsize::new(0));
    let get_count = counter.clone();
    let post_count = counter.clone();
    let visible_revision = Arc::new(AtomicUsize::new(0));
    let get_revision = visible_revision.clone();
    let refresh = cut.as_deref() == Some("stale");
    let unsupported = cut
        .as_deref()
        .filter(|c| {
            matches!(
                *c,
                "external"
                    | "charset"
                    | "implicit"
                    | "direction"
                    | "wrap"
                    | "charset-field"
                    | "base"
            )
        })
        .map(str::to_owned);
    let form_case = unsupported.clone();
    let count_path = dir.path().join("counter.txt");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}/", listener.local_addr()?);
    let app = Router::new()
        .route(
            "/",
            get(move || {
                let counter = get_count.clone();
                let revision = get_revision.clone();
                let form_case = form_case.clone();
                async move {
                    let mut result = page(counter.load(Ordering::SeqCst));
                    match form_case.as_deref() {
                        Some("external") => {
                            result.0 = result
                                .0
                                .replace("<form action=", "<form id=fixture action=");
                            result.0.push_str(
                                "<input form=fixture type=hidden name=external value=extra>",
                            );
                        }
                        Some("charset") => {
                            result.0 = result
                                .0
                                .replace("accept-charset=UTF-8", "accept-charset=Shift_JIS")
                        }
                        Some("implicit") => result.0 = result.0.replace("accept-charset=UTF-8", ""),
                        Some("direction") => {
                            result.0 = result.0.replace(
                                "<input type=hidden name=job value=pulse-fixture>",
                                "<input type=text name=job value=pulse-fixture dirname=job.dir>",
                            )
                        }
                        Some("wrap") => {
                            result.0 = result.0.replace(
                                "<input type=hidden name=job value=pulse-fixture>",
                                "<textarea name=job wrap=hard cols=1>pulse-fixture</textarea>",
                            )
                        }
                        Some("charset-field") => {
                            result.0 = result.0.replace("name=job", "name=_charset_")
                        }
                        Some("base") => result.0.push_str("<base target=another-window>"),
                        _ => {}
                    }
                    if refresh {
                        result.0.push_str(&format!(
                            "<p>View revision: {}</p><meta http-equiv=refresh content=4>",
                            revision.load(Ordering::SeqCst)
                        ));
                    }
                    result
                }
            }),
        )
        .route(
            "/count",
            post(move |body: String| {
                let counter = post_count.clone();
                let count_path = count_path.clone();
                async move {
                    assert_eq!(body, "job=pulse-fixture");
                    let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    std::fs::write(count_path, n.to_string()).unwrap();
                    page(n)
                }
            }),
        );
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let hub = Hub::new(Store::open(dir.path().join("db"), "owner").await?);
    let (journal, _lock) = Journal::open(
        &dir.path().join("local"),
        "https://pulse.example/",
        dir.path(),
    )
    .await?;
    let code = hub
        .pairing(ClientRole::LocalExecutor, pulse_link::now())
        .await?;
    hub.redeem(
        PairRequest {
            code,
            device: journal.device.clone(),
            role: ClientRole::LocalExecutor,
        },
        pulse_link::now(),
    )
    .await?;
    let epoch = hub.begin(&journal.device, 0, pulse_link::now()).await?;
    hub.browser_interactive_ready(&journal.device, epoch, Some(&origin), pulse_link::now())
        .await?;
    let runtime = BrowserRuntime::new(
        driver,
        NetworkPolicy::new(Some(&origin))?,
        dir.path().join("browser"),
    )?
    .with_interactions(true);
    let executor = Executor::new(dir.path())?.with_browser(Some(runtime.clone()));
    let e=hub.core.ingest(Origin::Human,"native-fixture","create",json!({"commands":[{"kind":"create","id":"task","goal":"Count once and type text with separate approval","scope":format!("browser-interact:{origin}"),"priority":0,"requires_device":false}]}),pulse_link::now()).await?;
    hub.core.route(&e.event_id).await?;
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await?
        .unwrap();
    hub.model_proposal(
        &run.token,
        &model(
            ReadOp::BrowserInspect {
                url: origin.clone(),
            },
            "inspect",
        ),
        pulse_link::now(),
    )
    .await?;
    hub.dispatch_model_read(&remote::versions(), pulse_link::now())
        .await?;
    let outcome = execute(&hub, &journal, epoch, &executor, None).await?;
    let ReadOutcome::Completed {
        observation: Some(observation),
        ..
    } = &outcome
    else {
        return Err(pulse_link::Error::Rejected(format!(
            "native inspection failed: {outcome:?}"
        )));
    };
    if let Some(case) = unsupported {
        ensure(
            !observation
                .elements
                .iter()
                .any(|e| matches!(e.kind, BrowserElementKind::Button { .. })),
            "unsupported form exposed for approval",
        )?;
        ensure(
            counter.load(Ordering::SeqCst) == 0,
            "unsupported form submitted",
        )?;
        std::fs::write(
            dir.path().join("unsupported-form.json"),
            serde_json::to_vec_pretty(&json!({"case":case,"counter":0,"outcome":outcome}))?,
        )?;
        runtime.maintain(None, &[]).await?;
        server.abort();
        println!(
            "Native unsupported form passed ({case}): {}",
            dir.keep().display()
        );
        return Ok(());
    }
    ensure(
        observation.elements.len() == 2,
        "native control catalog missing",
    )?;
    ensure(
        counter.load(Ordering::SeqCst) == 0,
        "effect before approval",
    )?;
    let button = observation
        .elements
        .iter()
        .find(|e| matches!(e.kind, BrowserElementKind::Button { .. }))
        .unwrap();
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await?
        .unwrap();
    let request = proposal(
        &hub,
        &run,
        &model(
            ReadOp::BrowserAction {
                session_id: observation.session_id.clone(),
                nonce: observation.nonce.clone(),
                element_id: button.id.clone(),
                effect: BrowserEffect::Click,
                grant: None,
            },
            "click",
        ),
    )
    .await?;
    remote::step(&hub, pulse_link::now()).await?;
    ensure(
        counter.load(Ordering::SeqCst) == 0,
        "effect before approval",
    )?;
    if cut.is_none() {
        let event=hub.core.ingest(Origin::Human,"native-fixture","second-task",json!({"commands":[{"kind":"create","id":"other-task","goal":"Independent browser approval","scope":format!("browser-interact:{origin}"),"priority":0,"requires_device":false}]}),pulse_link::now()).await?;
        hub.core.route(&event.event_id).await?;
        let other = hub
            .core
            .claim_next(&remote::versions(), pulse_link::now())
            .await?
            .expect("second task claim");
        ensure(other.token.task_id == "other-task", "wrong second task")?;
        hub.model_proposal(
            &other.token,
            &model(
                ReadOp::BrowserInspect {
                    url: origin.clone(),
                },
                "other-inspect",
            ),
            pulse_link::now(),
        )
        .await?;
        hub.dispatch_model_read(&remote::versions(), pulse_link::now())
            .await?;
        let second = execute(&hub, &journal, epoch, &executor, None).await?;
        let ReadOutcome::Completed {
            observation: Some(second),
            ..
        } = second
        else {
            return Err(pulse_link::Error::Rejected(
                "second native session missing".into(),
            ));
        };
        ensure(
            second.session_id != observation.session_id,
            "two tasks shared browser session",
        )?;
        let other = hub
            .core
            .claim_next(&remote::versions(), pulse_link::now())
            .await?
            .expect("second continuation");
        ensure(
            hub.model_proposal(
                &other.token,
                &model(
                    ReadOp::BrowserAction {
                        session_id: observation.session_id.clone(),
                        nonce: observation.nonce.clone(),
                        element_id: button.id.clone(),
                        effect: BrowserEffect::Click,
                        grant: None,
                    },
                    "cross-task",
                ),
                pulse_link::now(),
            )
            .await
            .is_err(),
            "second task borrowed first session",
        )?;
        let target = second
            .elements
            .iter()
            .find(|e| matches!(e.kind, BrowserElementKind::Button { .. }))
            .expect("second button");
        let second_request = proposal(
            &hub,
            &other,
            &model(
                ReadOp::BrowserAction {
                    session_id: second.session_id.clone(),
                    nonce: second.nonce.clone(),
                    element_id: target.id.clone(),
                    effect: BrowserEffect::Click,
                    grant: None,
                },
                "second-click",
            ),
        )
        .await?;
        approve(&hub, &second_request, "reject-second-task", false).await?;
        runtime
            .maintain(
                Some(epoch),
                &[
                    hub.core.task("task").await?,
                    hub.core.task("other-task").await?,
                ],
            )
            .await?;
        ensure(
            hub.core
                .operation_request(&request.proposal.id)
                .await?
                .state
                == pulse_core::ApprovalState::Pending,
            "second task altered first approval",
        )?;
        ensure(
            counter.load(Ordering::SeqCst) == 0,
            "second task caused effect",
        )?;
    }
    if cut.as_deref() == Some("reject") {
        approve(&hub, &request, "reject-click", false).await?;
        remote::step(&hub, pulse_link::now()).await?;
        ensure(
            counter.load(Ordering::SeqCst) == 0,
            "rejected native action changed counter",
        )?;
        let operations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operations")
            .fetch_one(&hub.core.application_pool())
            .await?;
        ensure(operations == 0, "rejected native action dispatched")?;
        std::fs::write(
            dir.path().join("rejected.json"),
            b"{\"counter\":0,\"operations\":0}",
        )?;
        runtime.maintain(None, &[]).await?;
        server.abort();
        println!("Native rejection passed: {}", dir.keep().display());
        return Ok(());
    }
    if refresh {
        visible_revision.store(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
    approve(&hub, &request, "approve-click", true).await?;
    remote::step(&hub, pulse_link::now()).await?;
    let work = hub
        .outgoing(&journal.device)
        .await?
        .into_iter()
        .find(|e| {
            matches!(
                &e.payload,
                ServerPayload::Read {
                    operation: ReadOp::BrowserAction { .. },
                    ..
                }
            )
        })
        .expect("approved native action");
    let ServerPayload::Read {
        operation_id,
        task_id,
        revision,
        operation,
    } = &work.payload
    else {
        unreachable!()
    };
    let binding = WorkBinding {
        operation_id: operation_id.clone(),
        task_id: task_id.clone(),
        revision: revision.clone(),
        device: journal.device.clone(),
        epoch,
    };
    for forged in [
        "device",
        "epoch",
        "task",
        "revision",
        "operation",
        "digest",
        "effect",
        "nonce",
        "unapproved",
    ] {
        let mut binding = binding.clone();
        let mut operation = operation.clone();
        let ReadOp::BrowserAction {
            grant: Some(grant),
            effect,
            nonce,
            ..
        } = &mut operation
        else {
            unreachable!()
        };
        match forged {
            "device" => binding.device = uuid::Uuid::new_v4().to_string(),
            "epoch" => binding.epoch += 1,
            "task" => binding.task_id = "another-task".into(),
            "revision" => binding.revision = "2".into(),
            "operation" => binding.operation_id = "another-operation".into(),
            "digest" => grant.request.intent_digest = "0".repeat(64),
            "effect" => {
                *effect = BrowserEffect::SetText {
                    text: "forged".into(),
                }
            }
            "nonce" => *nonce = uuid::Uuid::new_v4().to_string(),
            "unapproved" => grant.request.state = pulse_core::ApprovalState::Pending,
            _ => unreachable!(),
        }
        ensure(
            matches!(
                executor.execute_bound(operation, &binding).await.outcome,
                ReadOutcome::Rejected { .. }
            ),
            "forged native action was not rejected",
        )?;
        ensure(
            counter.load(Ordering::SeqCst) == 0,
            "forged native action changed counter",
        )?;
    }
    let clicked = execute(&hub, &journal, epoch, &executor, cut.as_deref()).await?;
    if refresh {
        ensure(
            matches!(clicked, ReadOutcome::Rejected { .. }),
            "changed native view not rejected",
        )?;
        ensure(
            counter.load(Ordering::SeqCst) == 0,
            "changed native view performed effect",
        )?;
        std::fs::write(
            dir.path().join("changed-view.json"),
            serde_json::to_vec_pretty(&json!({"counter":0,"result":clicked}))?,
        )?;
        runtime.maintain(None, &[]).await?;
        server.abort();
        println!(
            "Native changed-view rejection passed: {}",
            dir.keep().display()
        );
        return Ok(());
    }
    ensure(
        counter.load(Ordering::SeqCst) == 1,
        &format!("native click did not commit exactly once: {clicked:?}"),
    )?;
    ensure(
        matches!(
            executor
                .execute_bound(operation.clone(), &binding)
                .await
                .outcome,
            ReadOutcome::Rejected { .. }
        ),
        "native action replay not rejected",
    )?;
    ensure(
        counter.load(Ordering::SeqCst) == 1,
        "native action replay duplicated effect",
    )?;
    let ReadOutcome::Completed {
        observation: Some(observation),
        ..
    } = &clicked
    else {
        return Err(pulse_link::Error::Rejected(format!(
            "native click failed: {clicked:?}"
        )));
    };
    let input = observation
        .elements
        .iter()
        .find(|e| matches!(e.kind, BrowserElementKind::Text { .. }))
        .unwrap();
    let run = hub
        .core
        .claim_next(&remote::versions(), pulse_link::now())
        .await?
        .unwrap();
    let request = proposal(
        &hub,
        &run,
        &model(
            ReadOp::BrowserAction {
                session_id: observation.session_id.clone(),
                nonce: observation.nonce.clone(),
                element_id: input.id.clone(),
                effect: BrowserEffect::SetText {
                    text: "Pulse native text".into(),
                },
                grant: None,
            },
            "type",
        ),
    )
    .await?;
    approve(&hub, &request, "approve-type", true).await?;
    remote::step(&hub, pulse_link::now()).await?;
    let typed = execute(&hub, &journal, epoch, &executor, None).await?;
    let ReadOutcome::Completed {
        observation: Some(observation),
        ..
    } = &typed
    else {
        return Err(pulse_link::Error::Rejected(format!(
            "native typing failed: {typed:?}"
        )));
    };
    ensure(
        observation.elements.iter().any(
            |e| matches!(&e.kind,BrowserElementKind::Text{value} if value=="Pulse native text"),
        ),
        "typed value missing",
    )?;
    ensure(
        counter.load(Ordering::SeqCst) == 1,
        "text input submitted the form",
    )?;
    std::fs::write(
        dir.path().join("evidence.json"),
        serde_json::to_vec_pretty(
            &json!({"inspect":outcome,"click":clicked,"type":typed,"counter":1,"provider_calls":0,"fixture_controlled_decisions":true}),
        )?,
    )?;
    runtime.maintain(None, &[]).await?;
    server.abort();
    println!(
        "Native interaction component passed. Evidence: {}",
        dir.keep().display()
    );
    Ok(())
}
