use super::*;

async fn body<T: serde::de::DeserializeOwned>(mut response: reqwest::Response) -> Result<T> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| crate::Error::Rejected("pair response interrupted".into()))?
    {
        ensure(bytes.len() + chunk.len() <= 4096, "pair response size")?;
        bytes.extend_from_slice(&chunk);
    }
    parse(&bytes)
}

/// A claim is made durable in the secret vault BEFORE any server request.
pub(super) async fn begin(
    config: Config,
    journal: Journal,
    vault: Arc<dyn Vault>,
    account: String,
    code: String,
) -> Result<Credential> {
    ensure(
        code.len() == 64 && code.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid pairing code",
    )?;
    let copy = vault.clone();
    let key = account.clone();
    let mut pending = tokio::task::spawn_blocking(move || copy.pending(&key))
        .await
        .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
    if !pending.as_ref().is_some_and(|p| {
        p.code == code && p.device == journal.device && p.role == journal.role && p.valid()
    }) {
        let claim = PairClaim {
            code,
            device: journal.device.clone(),
            role: journal.role,
            attempt_id: uuid::Uuid::new_v4().to_string(),
            token: secret()?,
            server_generation: None,
        };
        let copy = vault.clone();
        let key = account.clone();
        let saved = claim.clone();
        tokio::task::spawn_blocking(move || copy.save_pending(&key, &saved))
            .await
            .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
        pending = Some(claim);
    }
    recover(config, journal, vault, account, pending.unwrap()).await
}

pub(super) async fn recover(
    config: Config,
    journal: Journal,
    vault: Arc<dyn Vault>,
    account: String,
    mut claim: PairClaim,
) -> Result<Credential> {
    ensure(
        claim.valid() && claim.device == journal.device && claim.role == journal.role,
        "pending pairing identity mismatch",
    )?;
    if let Some(generation) = &claim.server_generation {
        journal
            .core
            .require_peer_restore(&journal.device, &hash(&journal.server)?, generation)
            .await?;
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| crate::Error::Rejected("pair HTTP setup failed".into()))?;
    let response = client
        .get(config.server.join("v1/device-pairings/identity")?)
        .bearer_auth(&claim.token)
        .send()
        .await
        .map_err(|_| crate::Error::Rejected("pair verification unavailable".into()))?;
    let value = match response.status() {
        reqwest::StatusCode::OK => {
            let identity: PairIdentity = body(response).await?;
            ensure(
                identity.device == claim.device
                    && identity.role == claim.role
                    && identity.server_generation == claim.server_generation,
                "pair identity proof mismatch",
            )?;
            claim.credential()
        }
        reqwest::StatusCode::UNAUTHORIZED
            if response
                .headers()
                .get("x-pulse-credential-denied")
                .is_some_and(|v| v == "1") =>
        {
            let generation = response
                .headers()
                .get("x-pulse-restore-generation")
                .map(|value| value.to_str().map(str::to_owned))
                .transpose()
                .map_err(|_| crate::Error::Rejected("invalid server restore generation".into()))?;
            ensure(
                generation.as_ref().is_none_or(|value| {
                    uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == *value)
                }),
                "invalid server restore generation",
            )?;
            if generation != claim.server_generation {
                ensure(
                    claim.server_generation.is_none() && generation.is_some(),
                    "restored server requires a new empty data directory",
                )?;
                let copy = vault.clone();
                let key = account.clone();
                let active = tokio::task::spawn_blocking(move || copy.load(&key))
                    .await
                    .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
                ensure(
                    active.is_none(),
                    "restored server requires a new empty data directory; pair before submitting input",
                )?;
                journal
                    .core
                    .bind_peer_restore(
                        &journal.device,
                        &hash(&journal.server)?,
                        generation.as_deref().unwrap(),
                    )
                    .await?;
                claim.server_generation = generation;
                claim.token = secret()?;
                claim.attempt_id = uuid::Uuid::new_v4().to_string();
                let copy = vault.clone();
                let key = account.clone();
                let saved = claim.clone();
                tokio::task::spawn_blocking(move || copy.save_pending(&key, &saved))
                    .await
                    .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
            }
            let response = client
                .post(config.server.join("v1/device-pairings/claim")?)
                .json(&claim)
                .send()
                .await
                .map_err(|_| crate::Error::Rejected("pair claim interrupted".into()))?;
            if matches!(
                response.status(),
                reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::BAD_REQUEST
            ) && response
                .headers()
                .get("x-pulse-pairing-denied")
                .is_some_and(|v| v == "1")
            {
                let copy = vault.clone();
                let key = account.clone();
                tokio::task::spawn_blocking(move || copy.clear_pending(&key))
                    .await
                    .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
                return Err(crate::Error::PairingRejected);
            }
            ensure(
                response.status() == reqwest::StatusCode::OK,
                "pair claim unavailable",
            )?;
            let value: Credential = body(response).await?;
            ensure(
                hash(&value)? == hash(&claim.credential())?,
                "pair claim proof mismatch",
            )?;
            value
        }
        _ => {
            return Err(crate::Error::Rejected(
                "pair verification unavailable".into(),
            ));
        }
    };
    journal.prepare_memory_pairing(&value).await?;
    let copy = vault.clone();
    let key = account.clone();
    let saved = value.clone();
    tokio::task::spawn_blocking(move || copy.save(&key, &saved))
        .await
        .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
    let copy = vault.clone();
    tokio::task::spawn_blocking(move || copy.clear_pending(&account))
        .await
        .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
    Ok(value)
}
