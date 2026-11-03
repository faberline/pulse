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
        workflow: "pulse-remote-19".into(),
        schema: "1".into(),
        prompt: "effective-read-7".into(),
        tools: "pulse-tools-6".into(),
        model: provider.map(|p| p.model).unwrap_or("unconfigured").into(),
        provider_api: "function-call-1".into(),
        policy: "approved-effects-memory-fences-6".into(),
    }
}
pub async fn step(hub: &Hub, time: i64) -> Result<bool> {
    step_with(hub, None, time).await
}
pub async fn step_with(hub: &Hub, provider: Option<&Provider>, time: i64) -> Result<bool> {
    if !hub.core.notification_capacity_available().await? {
        return Ok(false);
    }
    if hub.core.flush_memory_task_changes().await? {
        hub.notify_change();
        return Ok(true);
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
    hub.project_action_approvals(time).await?;
    hub.gate_computer_tasks(time).await?;
    hub.gate_browser_tasks(time).await?;
    match hub
        .dispatch_approved_action(&manifest(provider), time)
        .await
    {
        Ok(true) => return Ok(true),
        Ok(false)
        | Err(crate::Error::Capacity | crate::Error::Core(pulse_core::Error::Capacity)) => {}
        Err(error) => return Err(error),
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
        if !pulse_core::read_scope(&task.scope) {
            reason = "scope_not_supported";
        } else {
            let (prompt, history) = match hub.model_context(&run).await {
                Ok(context) => context,
                Err(crate::Error::Rejected(_)) => return Ok(true),
                Err(error) => return Err(error),
            };
            match provider.prepare_for_scope(&prompt, &history, &task.scope) {
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
                        Ok(true) => match send_fenced(hub, &run.token, &call, request.send())
                            .await?
                        {
                            Some(reply) => {
                                hub.core
                                    .settle(
                                        &call,
                                        reply.cost,
                                        &serde_json::to_string(&reply.usage)?,
                                    )
                                    .await?;
                                let saved = if reply.tool.is_some() {
                                    loop {
                                        match hub.model_proposal(&run.token, &reply, now()).await {
                                            Err(
                                                crate::Error::Capacity
                                                | crate::Error::Core(pulse_core::Error::Capacity),
                                            ) => tokio::time::sleep(Duration::from_secs(1)).await,
                                            result => break result,
                                        }
                                    }
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
                            None => {
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

// `send` is lazy: an invalid preflight never polls provider I/O. Do not retain
// a keeper lease across a network wait. Periodic rechecks also see restrictions
// written by a different process, which an in-process watch cannot guarantee.
// Dropping I/O stops our wait; it does not prove provider cancellation or refund.
async fn send_fenced<F>(
    hub: &Hub,
    token: &pulse_core::Token,
    call: &str,
    send: F,
) -> Result<Option<crate::provider::Reply>>
where
    F: std::future::Future<Output = Result<crate::provider::Reply>>,
{
    match wait_fenced(hub, token, call, send).await {
        Ok(reply) => Ok(Some(reply)),
        Err(_) => {
            // A ledger write failure must propagate to the controller. It must
            // not be mistaken for a recorded, recoverable provider failure.
            hub.core.cost_unknown(call).await?;
            Ok(None)
        }
    }
}

async fn wait_fenced<F>(
    hub: &Hub,
    token: &pulse_core::Token,
    call: &str,
    send: F,
) -> Result<crate::provider::Reply>
where
    F: std::future::Future<Output = Result<crate::provider::Reply>>,
{
    let mut until = hub.core.validate_model_call(token, call, now()).await?;
    tokio::pin!(send);
    loop {
        let delay = Duration::from_millis(until.saturating_sub(now()).clamp(0, 1_000) as u64);
        tokio::select! {
            // At a known deadline, validate before accepting a simultaneous reply.
            biased;
            _ = tokio::time::sleep(delay) => {
                until = hub.core.validate_model_call(token, call, now()).await?;
            }
            reply = &mut send => return reply,
        }
    }
}

#[cfg(test)]
#[path = "remote/tests.rs"]
mod tests;
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
