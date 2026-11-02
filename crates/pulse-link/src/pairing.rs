//! Retry-safe pairing. Server receipts contain hashes, never recoverable tokens.
use crate::{Result, ensure, protocol::*, server::Hub};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use sqlx::Row;
use std::sync::Arc;

impl Hub {
    pub async fn redeem(&self, request: PairRequest, time: i64) -> Result<Credential> {
        self.install_pairing(request, secret()?, None, time).await
    }
    pub async fn claim_pairing(&self, request: &PairClaim, time: i64) -> Result<Credential> {
        ensure(request.valid(), "invalid pairing claim")?;
        self.install_pairing(
            PairRequest {
                code: request.code.clone(),
                device: request.device.clone(),
                role: request.role,
            },
            request.token.clone(),
            Some(request),
            time,
        )
        .await
    }
    async fn install_pairing(
        &self,
        request: PairRequest,
        token: String,
        claim: Option<&PairClaim>,
        time: i64,
    ) -> Result<Credential> {
        ensure(
            uuid::Uuid::parse_str(&request.device).is_ok() && request.code.len() == 64,
            "invalid pairing",
        )?;
        let code = hash_bytes(request.code.as_bytes());
        let token_digest = hash_bytes(token.as_bytes());
        let mut tx = self.db.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(claim) = claim {
            let receipt=sqlx::query("SELECT c.request_digest,p.expires FROM link_pair_claims c JOIN link_pairings p ON p.digest=c.code_digest WHERE c.code_digest=?")
                .bind(&code).fetch_optional(&mut *tx).await?;
            if let Some(receipt) = receipt {
                ensure(
                    receipt.get::<String, _>("request_digest") == hash(claim)?
                        && receipt.get::<i64, _>("expires") > time,
                    "pairing claim mismatch or expired",
                )?;
                let active:i64=sqlx::query_scalar("SELECT count(*) FROM link_device WHERE id=? AND role=? AND token_digest=? AND revoked=0")
                    .bind(&request.device).bind(request.role.as_str()).bind(&token_digest).fetch_one(&mut *tx).await?;
                ensure(active == 1, "claimed identity was revoked or replaced")?;
                tx.commit().await?;
                return Ok(Credential {
                    device: request.device,
                    token,
                    role: request.role,
                });
            }
            let reused: i64 =
                sqlx::query_scalar("SELECT count(*) FROM link_pair_claims WHERE attempt_id=?")
                    .bind(&claim.attempt_id)
                    .fetch_one(&mut *tx)
                    .await?;
            ensure(reused == 0, "pairing attempt already used")?;
        }
        let active: i64 =
            sqlx::query_scalar("SELECT count(*) FROM link_device WHERE revoked=0 AND role=?")
                .bind(request.role.as_str())
                .fetch_one(&mut *tx)
                .await?;
        ensure(
            active == 0,
            "an active device already exists; operator revocation is required",
        )?;
        let reused_token: i64 =
            sqlx::query_scalar("SELECT count(*) FROM link_pair_tokens WHERE token_digest=?")
                .bind(&token_digest)
                .fetch_one(&mut *tx)
                .await?;
        ensure(reused_token == 0, "pairing requires a new credential")?;
        let changed = sqlx::query(
            "UPDATE link_pairings SET used=1 WHERE digest=? AND used=0 AND expires>? AND role=?",
        )
        .bind(&code)
        .bind(time)
        .bind(request.role.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        ensure(changed == 1, "pairing expired, invalid or already used")?;
        let old_role: Option<String> =
            sqlx::query_scalar("SELECT role FROM link_device WHERE id=?")
                .bind(&request.device)
                .fetch_optional(&mut *tx)
                .await?;
        ensure(
            old_role
                .as_deref()
                .is_none_or(|role| role == request.role.as_str()),
            "client role is immutable",
        )?;
        sqlx::query("INSERT INTO link_device(id,token_digest,role) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET token_digest=excluded.token_digest,revoked=0,epoch=epoch+1,lease_until=0")
            .bind(&request.device).bind(&token_digest).bind(request.role.as_str()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO link_pair_tokens VALUES(?)")
            .bind(&token_digest)
            .execute(&mut *tx)
            .await?;
        if let Some(claim) = claim {
            sqlx::query("INSERT INTO link_pair_claims VALUES(?,?,?,?,?,?)")
                .bind(&code)
                .bind(&claim.attempt_id)
                .bind(hash(claim)?)
                .bind(&request.device)
                .bind(request.role.as_str())
                .bind(&token_digest)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(Credential {
            device: request.device,
            token,
            role: request.role,
        })
    }
    pub async fn pairing_identity(&self, token: &str) -> Result<PairIdentity> {
        ensure(token.len() == 64, "invalid credential")?;
        let row = sqlx::query("SELECT id,role FROM link_device WHERE token_digest=? AND revoked=0")
            .bind(hash_bytes(token.as_bytes()))
            .fetch_optional(&self.db)
            .await?
            .ok_or_else(|| crate::Error::Rejected("credential revoked or invalid".into()))?;
        Ok(PairIdentity {
            device: row.get("id"),
            role: serde_json::from_value(serde_json::Value::String(row.get("role")))?,
        })
    }
}

fn private(response: impl IntoResponse) -> Response {
    let mut response = response.into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    response
}
fn denied(status: StatusCode) -> Response {
    let mut response = private(status);
    response
        .headers_mut()
        .insert("x-pulse-pairing-denied", "1".parse().unwrap());
    response
}
pub(crate) async fn claim(State(hub): State<Arc<Hub>>, bytes: Bytes) -> Response {
    let Ok(request) = parse::<PairClaim>(&bytes) else {
        return denied(StatusCode::BAD_REQUEST);
    };
    match hub.claim_pairing(&request, crate::now()).await {
        Ok(value) => private(axum::Json(value)),
        Err(crate::Error::Rejected(_)) => denied(StatusCode::FORBIDDEN),
        Err(_) => private(StatusCode::SERVICE_UNAVAILABLE),
    }
}
pub(crate) async fn identity(State(hub): State<Arc<Hub>>, headers: HeaderMap) -> Response {
    let Some(token) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return private(StatusCode::UNAUTHORIZED);
    };
    match hub.pairing_identity(token).await {
        Ok(value) => private(axum::Json(value)),
        Err(error) => private(crate::server::authentication_response(
            &error,
            StatusCode::UNAUTHORIZED,
        )),
    }
}
