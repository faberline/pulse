use crate::{
    Result,
    client::Config,
    ensure,
    journal::Journal,
    protocol::{ClientRole, Credential},
};
use pulse_core::{MemoryOutcome, MemoryView};
use serde::de::DeserializeOwned;
use std::time::Duration;

async fn check_access(
    journal: &Journal,
    credential: &Credential,
    response: &reqwest::Response,
) -> Result<()> {
    if matches!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
    ) && response
        .headers()
        .get("x-pulse-credential-denied")
        .is_some_and(|v| v == "1")
    {
        let denied = journal
            .deny_memory_credential(credential)
            .await
            .map_err(|error| crate::Error::CredentialCleanup(Box::new(error)))?;
        if !denied {
            return Err(crate::Error::Rejected("memory identity changed".into()));
        }
        return Err(crate::Error::CredentialRejected);
    }
    Ok(())
}

async fn body<T: DeserializeOwned>(mut response: reqwest::Response) -> Result<T> {
    let mut bytes = Vec::new();
    while let Some(part) = response
        .chunk()
        .await
        .map_err(|_| crate::Error::Rejected("memory response interrupted".into()))?
    {
        ensure(
            bytes.len() + part.len() <= 1024 * 1024,
            "memory response capacity",
        )?;
        bytes.extend_from_slice(&part);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub async fn sync(config: &Config, journal: &Journal, credential: &Credential) -> Result<()> {
    ensure(
        credential.role == ClientRole::Chat
            && journal.role == ClientRole::Chat
            && credential.device == journal.device,
        "memory delivery role mismatch",
    )?;
    if !journal.memory_access_allowed().await? {
        return Err(crate::Error::CredentialRejected);
    }
    journal.require_memory_identity(credential)?;
    let Some(generation) = journal.memory_dirty().await? else {
        return Ok(());
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|_| crate::Error::Rejected("memory HTTP configuration failed".into()))?;
    let url = config.server.join("v1/memories")?;
    let get = || async {
        journal.require_memory_identity(credential)?;
        let response = client
            .get(url.clone())
            .bearer_auth(&credential.token)
            .send()
            .await
            .map_err(|_| crate::Error::Rejected("memory sync unavailable".into()))?;
        check_access(journal, credential, &response).await?;
        ensure(
            response.status() == reqwest::StatusCode::OK,
            "memory sync denied",
        )?;
        body::<MemoryView>(response).await
    };
    // Restrictions FIRST, before replaying any unacknowledged source payload.
    journal
        .apply_memory_view_for(&get().await?, credential, crate::now())
        .await?;
    for request in journal.memory_requests().await? {
        if !journal.memory_request_active(&request).await? {
            continue;
        }
        journal.require_memory_identity(credential)?;
        if !matches!(
            request.command,
            pulse_core::MemoryCommand::Revoke { .. } | pulse_core::MemoryCommand::Forget { .. }
        ) && journal.pending_memory_revocation().await?
        {
            break;
        }
        let response = client
            .post(url.clone())
            .bearer_auth(&credential.token)
            .json(&request)
            .send()
            .await
            .map_err(|_| crate::Error::Rejected("memory delivery interrupted".into()))?;
        check_access(journal, credential, &response).await?;
        match response.status() {
            reqwest::StatusCode::OK => {
                let outcome: MemoryOutcome = body(response).await?;
                if let pulse_core::MemoryCommand::Revoke { expected, .. } = &request.command {
                    ensure(
                        matches!(outcome,MemoryOutcome::Revoked {current_changed} if expected.is_some() || !current_changed),
                        "withdrawal acknowledgement mismatch",
                    )?;
                }
                let phase = match outcome {
                    MemoryOutcome::Redacted { .. } => "redacted",
                    MemoryOutcome::Revoked {
                        current_changed: true,
                    } => "review",
                    _ => "accepted",
                };
                journal
                    .finish_memory_for(&request.source_id, phase, credential)
                    .await?;
            }
            reqwest::StatusCode::CONFLICT | reqwest::StatusCode::BAD_REQUEST => {
                journal
                    .finish_memory_for(&request.source_id, "rejected", credential)
                    .await?
            }
            _ => {
                return Err(crate::Error::Rejected(
                    "memory request not acknowledged".into(),
                ));
            }
        }
    }
    journal
        .apply_memory_view_for(&get().await?, credential, crate::now())
        .await?;
    journal.memory_clean(&generation).await
}
