//! In-process durable task core. No network listener, model client or tool executor.
//! Callers are trusted application code; transport authentication belongs to v0.3.
mod model;
mod store;

pub use model::*;
pub use store::Store;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("file access: {0}")]
    Io(#[from] std::io::Error),
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("missing: {0}")]
    Missing(String),
}
pub type Result<T> = std::result::Result<T, Error>;
