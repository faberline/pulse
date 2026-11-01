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
        workflow: "pulse-remote-5".into(),
        schema: "1".into(),
        prompt: "effective-read-2".into(),
        tools: "pulse-read-1".into(),
        model: provider.map(|p| p.model).unwrap_or("unconfigured").into(),
        provider_api: "function-call-1".into(),
        policy: "read-only-2".into(),
    }
}
pub async fn step(hub: &Hub, time: i64) -> Result<bool> {
    step_with(hub, None, time).await
}
pub async fn step_with(hub: &Hub, provider: Option<&Provider>, time: i64) -> Result<bool> {
    match hub.dispatch_model_read(&manifest(provider), time).await {
        Ok(true) => return Ok(true),
        Ok(false) | Err(crate::Error::Capacity) => {}
        Err(error) => return Err(error),
    }
    let Some(run) = hub.core.claim_next(&manifest(provider), time).await? else {
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
            let round = run.checkpoint["round"].as_u64().unwrap_or(0);
            let call = format!("model:{}:{}:{round}", run.token.run_id, run.token.revision);
            match hub
                .core
                .reserve(
                    &run.token,
                    &call,
                    20_000,
                    "luna-haiku-short-ceiling-2026-11-01",
                    now(),
                )
                .await
            {
                Ok(true) => match provider.generate_with(&prompt, &history).await {
                    Ok(reply) => {
                        hub.core
                            .settle(&call, reply.cost, &serde_json::to_string(&reply.usage)?)
                            .await?;
                        let saved = if reply.tool.is_some() {
                            hub.model_proposal(&run.token, &reply, now()).await
                        } else {
                            hub.model_reply(&run.token, &reply, provider.model, now())
                                .await
                        };
                        match saved {
                            Ok(()) => return Ok(true),
                            Err(crate::Error::Rejected(_)) => reason = "model_result_not_applied",
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
    let result = hub
        .core
        .advance(
            &run.token,
            State::Blocked,
            json!({"phase":"waiting","reason":reason}),
            reason,
            None,
            now(),
        )
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
        if !step_with(&hub, provider.as_ref(), now()).await? {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
