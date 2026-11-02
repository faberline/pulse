use crate::{Error, Result};
use sqlx::{Row, Sqlite, SqlitePool, pool::PoolConnection};

/// Checkpoint waits must fit inside the caller's HTTP deadline. The normal
/// writer timeout is retained for every other statement on this connection.
struct Checkpoint {
    connection: PoolConnection<Sqlite>,
    original_timeout: i64,
    restored: bool,
}

impl Drop for Checkpoint {
    fn drop(&mut self) {
        if !self.restored {
            // Cancellation cannot return a connection with a modified busy
            // handler to the shared pool. Its replacement uses pool defaults.
            self.connection.close_on_drop();
        }
    }
}

impl Checkpoint {
    async fn begin(pool: &SqlitePool) -> Result<Self> {
        let mut guard = Self {
            connection: pool.acquire().await?,
            original_timeout: 0,
            restored: false,
        };
        guard.original_timeout = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&mut *guard.connection)
            .await?;
        sqlx::query("PRAGMA busy_timeout=250")
            .execute(&mut *guard.connection)
            .await?;
        Ok(guard)
    }
    async fn restore(&mut self) -> Result<()> {
        // SQLite PRAGMA assignments do not support bind parameters. Only the
        // integer read from this connection is formatted, never caller text.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "PRAGMA busy_timeout={}",
            self.original_timeout
        )))
        .execute(&mut *self.connection)
        .await?;
        self.restored = true;
        Ok(())
    }
}

pub(super) async fn truncate(pool: &SqlitePool) -> Result<()> {
    let mut guard = Checkpoint::begin(pool).await?;
    let result = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .fetch_one(&mut *guard.connection)
        .await;
    guard.restore().await?;
    if result?.get::<i64, _>(0) == 0 {
        Ok(())
    } else {
        Err(Error::Busy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    #[tokio::test]
    async fn cancelled_checkpoint_never_returns_a_modified_writer_timeout_to_the_pool() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db"), "owner").await.unwrap();
        let pool = store.application_pool();
        let worker_pool = pool.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let _guard = Checkpoint::begin(&worker_pool).await.unwrap();
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started.await.unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        let timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(timeout, 5000);
        truncate(&pool).await.unwrap();
        let timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(timeout, 5000);
    }
}
