use crate::{
    Result, ensure,
    protocol::{ClientRole, parse},
    server::Hub,
};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pulse_core::{MemoryOutcome, MemoryRequest, MemoryView, Origin};
use std::sync::Arc;

impl Hub {
    async fn memory_identity(&self, device: &str) -> Result<()> {
        let role: Option<String> =
            sqlx::query_scalar("SELECT role FROM link_device WHERE id=? AND revoked=0")
                .bind(device)
                .fetch_optional(&self.db)
                .await?;
        ensure(
            role.as_deref() == Some(ClientRole::Chat.as_str()),
            "only chat may access memory",
        )
    }
    pub async fn memory_view(&self, device: &str, time: i64) -> Result<MemoryView> {
        self.memory_identity(device).await?;
        Ok(self.core.memory_view(time).await?)
    }
    pub async fn memory_change(
        &self,
        device: &str,
        request: &MemoryRequest,
        time: i64,
    ) -> Result<MemoryOutcome> {
        let _human = self.human.lock().await;
        self.memory_identity(device).await?;
        // A memory instruction breaks implicit task focus. It is a different topic.
        self.clear_conversation_context().await?;
        let result = self
            .core
            .change_memory(Origin::Human, request, time)
            .await?;
        self.notify_change();
        Ok(result)
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
pub async fn view(State(hub): State<Arc<Hub>>, headers: HeaderMap) -> Response {
    if let Some(token) = headers
        .get("authorization")
        .and_then(|s| s.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    {
        let device = match hub.authenticate(token).await {
            Ok(device) => device,
            Err(error) => {
                return private(crate::server::authentication_response(
                    &error,
                    StatusCode::FORBIDDEN,
                ));
            }
        };
        return match hub.memory_view(&device, crate::now()).await {
            Ok(view) => private(axum::Json(view)),
            Err(error) => private(crate::server::authentication_response(
                &error,
                StatusCode::FORBIDDEN,
            )),
        };
    }
    private(StatusCode::FORBIDDEN)
}
pub async fn change(State(hub): State<Arc<Hub>>, headers: HeaderMap, bytes: Bytes) -> Response {
    let Some(token) = headers
        .get("authorization")
        .and_then(|s| s.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    else {
        return private(StatusCode::FORBIDDEN);
    };
    let device = match hub.authenticate(token).await {
        Ok(device) => device,
        Err(error) => {
            return private(crate::server::authentication_response(
                &error,
                StatusCode::FORBIDDEN,
            ));
        }
    };
    if let Err(error) = hub.memory_identity(&device).await {
        return private(crate::server::authentication_response(
            &error,
            StatusCode::FORBIDDEN,
        ));
    }
    let Ok(request) = parse::<MemoryRequest>(&bytes) else {
        return private(StatusCode::BAD_REQUEST);
    };
    match hub.memory_change(&device, &request, crate::now()).await {
        Ok(outcome) => private(axum::Json(outcome)),
        Err(crate::Error::Core(
            pulse_core::Error::Rejected(_)
            | pulse_core::Error::Conflict(_)
            | pulse_core::Error::Missing(_),
        )) => private(StatusCode::CONFLICT),
        Err(error @ crate::Error::Rejected(_)) => private(crate::server::authentication_response(
            &error,
            StatusCode::FORBIDDEN,
        )),
        Err(_) => private(StatusCode::SERVICE_UNAVAILABLE),
    }
}
