//! In-process durable task core. No network listener, model client or tool executor.
//! Callers are trusted application code; transport authentication belongs to v0.3.
mod browser_operation;
mod computer_observation;
mod computer_operation;
mod memory;
mod model;
mod read;
mod schedule;
mod scope;
mod store;

pub use browser_operation::*;
pub use computer_observation::*;
pub use computer_operation::*;
pub use memory::*;
pub use model::*;
pub use read::*;
pub use schedule::*;
pub use scope::*;
pub use store::{
    BackupCheck, DatabaseMaintenance, Maintenance, MemoryCopyChange, MemoryWithdrawalRules,
    OfflineBackupCheck, RestoreReport, Store, TaskMemoryLease, truncate_wal,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database missing or empty with retained storage; offline recovery required")]
    DatabaseRecoveryRequired,
    #[error("memory storage is busy; retry required")]
    Busy,
    #[error("notification queue capacity unavailable")]
    Capacity,
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
