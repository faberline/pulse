mod action_intent;
pub mod browser;
mod browser_admission;
mod browser_controls;
pub mod browser_network;
pub mod browser_runtime;
pub mod browser_session;
pub mod cli;
pub mod client;
mod computer_admission;
mod computer_control;
pub mod computer_runtime;
pub mod computer_selection;
mod computer_transport;
mod conversation;
mod conversation_capability;
mod conversation_context;
mod derived_projection;
pub mod desktop_notifications;
pub mod executor;
pub mod journal;
mod operation_orchestration;
mod owned_process;
mod pairing;
pub mod protocol;
pub mod provider;
pub mod remote;
mod remote_io;
mod schedule_chat;
pub mod schedules;
pub mod server;
pub mod text_input;
pub mod text_result;
pub mod text_result_client;
pub mod worker_erasure;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("core: {0}")]
    Core(#[from] pulse_core::Error),
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("file: {0}")]
    Io(#[from] std::io::Error),
    #[error("local file policy: {0}")]
    System(#[from] rustix::io::Errno),
    #[error("invalid JSON")]
    Json(#[from] serde_json::Error),
    #[error("invalid URL")]
    Url(#[from] url::ParseError),
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("queue capacity unavailable")]
    Capacity,
    #[error("credential rejected; fresh pairing required")]
    CredentialRejected,
    #[error("system credential storage failed ({0})")]
    CredentialStore(i32),
    #[error("credential rejected; local memory cleanup failed: {0}")]
    CredentialCleanup(#[source] Box<Error>),
    #[error("pairing claim rejected")]
    PairingRejected,
}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn ensure(ok: bool, why: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Rejected(why.into()))
    }
}
pub fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
pub mod artifact;
pub mod artifact_client;
mod artifact_server;
pub mod maintenance;
pub mod memory_chat;
