//! Pulse-owned bounded model execution. Proposals never grant authority.
use crate::{Result, now, provider::Provider, server::Hub};
use pulse_core::{State, Versions};
use serde_json::json;
use std::time::Duration;

pub fn versions() -> Versions {
    manifest(None)
}
fn manifest(provider: Option<&Provider>) -> Versions {
    Versions {
        workflow: "pulse-remote-7".into(),
        schema: "1".into(),
        prompt: "effective-read-2".into(),
        tools: "pulse-read-2".into(),
        model: provider.map(|p| p.model).unwrap_or("unconfigured").into(),
        provider_api: "function-call-1".into(),
        policy: "read-only-2".into(),
    }
}
pub async fn step(hub: &Hub, time: i64) -> Result<bool> {
    step_with(hub, None, time).await
}
pub async fn step_with(hub: &Hub, provider: Option<&Provider>, time: i64) -> Result<bool> {
    if !hub.core.notification_capacity_available().await? {
        return Ok(false);
    }
    let pending = hub.core.pending_events(100).await?;
    if !pending.is_empty() {
        // Finish durable ingress (including cancellation) before starting more work.
        for event in pending {
            hub.core.route(&event).await?;
            hub.notify_change();
        }
        return Ok(true);
    }
    match hub.dispatch_model_read(&manifest(provider), time).await {
        Ok(true) => return Ok(true),
        Ok(false) | Err(crate::Error::Capacity) => {}
        Err(error) => return Err(error),
    }
    let (run, changed) = hub
        .core
        .claim_next_with_changes(&manifest(provider), time)
        .await?;
    if changed {
        hub.notify_change();
    }
    let Some(run) = run else {
        return Ok(false);
    };
    let mut reason = "provider_not_configured";
    if let Some(provider) = provider {
        let task = hub.core.task(&run.token.task_id).await?;
        if task.scope != "pulse:read" {
            reason = "scope_not_supported";
        } else {
            let (prompt, history) = match hub.model_context(&run).await {
                Ok(context) => context,
                Err(crate::Error::Rejected(_)) => return Ok(true),
                Err(error) => return Err(error),
            };
            match provider.prepare(&prompt, &history) {
                Err(_) => reason = "provider_request_invalid",
                Ok(request) => {
                    let round = run.checkpoint["round"].as_u64().unwrap_or(0);
                    let call = format!("model:{}:{}:{round}", run.token.run_id, run.token.revision);
                    match commit_when_room(|| {
                        hub.core.reserve(
                            &run.token,
                            &call,
                            20_000,
                            "luna-haiku-short-ceiling-2026-11-01",
                            now(),
                        )
                    })
                    .await
                    {
                        Ok(true) => match request.send().await {
                            Ok(reply) => {
                                hub.core
                                    .settle(
                                        &call,
                                        reply.cost,
                                        &serde_json::to_string(&reply.usage)?,
                                    )
                                    .await?;
                                let saved = if reply.tool.is_some() {
                                    hub.model_proposal(&run.token, &reply, now()).await
                                } else {
                                    hub.model_reply(&run.token, &reply, provider.model, now())
                                        .await
                                };
                                match saved {
                                    Ok(()) => return Ok(true),
                                    Err(crate::Error::Rejected(_)) => {
                                        reason = "model_result_not_applied"
                                    }
                                    Err(error) => return Err(error),
                                }
                            }
                            Err(_) => {
                                hub.core.cost_unknown(&call).await?;
                                reason = "provider_call_failed_cost_reserved";
                            }
                        },
                        Ok(false) => reason = "previous_call_not_retried",
                        Err(pulse_core::Error::Rejected(_)) => {
                            reason = "model_reservation_rejected";
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
    }
    let result = commit_when_room(|| {
        hub.core.advance(
            &run.token,
            State::Blocked,
            json!({"phase":"waiting","reason":reason}),
            reason,
            None,
            now(),
        )
    })
    .await;
    if let Err(error) = result
        && !matches!(error, pulse_core::Error::Rejected(_))
    {
        return Err(error.into());
    }
    hub.notify_change();
    Ok(true)
}
pub async fn run(hub: Hub) -> Result<()> {
    let name = std::env::var("PULSE_PROVIDER").unwrap_or_else(|_| "openai".into());
    let provider = Provider::load(&name)?;
    hub.core
        .recover(&manifest(provider.as_ref()), now())
        .await?;
    loop {
        let progressed = match step_with(&hub, provider.as_ref(), now()).await {
            Ok(progressed) => progressed,
            Err(crate::Error::Capacity | crate::Error::Core(pulse_core::Error::Capacity)) => false,
            Err(error) => return Err(error),
        };
        if !progressed {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

// Retry only local transactions. Each attempt rechecks the token and deadline;
// never wrap an HTTP/provider call in this loop.
async fn commit_when_room<T, F, Fut>(mut commit: F) -> pulse_core::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = pulse_core::Result<T>>,
{
    loop {
        match commit().await {
            Err(pulse_core::Error::Capacity) => tokio::time::sleep(Duration::from_secs(1)).await,
            result => return result,
        }
    }
}
