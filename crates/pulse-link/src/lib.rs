pub mod client;
mod conversation;
pub mod executor;
pub mod journal;
pub mod protocol;
pub mod provider;
pub mod remote;
mod remote_io;
pub mod server;

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
