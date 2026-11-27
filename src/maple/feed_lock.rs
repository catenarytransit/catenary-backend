// Copyright Catenary Transit Initiatives
// Attribution cannot be removed

use catenary::catenaryconfig;
use diesel::sql_types::{Bool, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use std::error::Error;

type LockError = Box<dyn Error + Send + Sync>;

/// Unpooled sessions ensure that dropping a connection releases its locks,
/// including error/cancellation paths. Do not return locked sessions to bb8.
pub(crate) async fn connect() -> Result<AsyncPgConnection, LockError> {
    let database_url = std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| catenaryconfig::config().postgres.database_url.clone())
        .ok_or("DATABASE_URL must be set")?;
    Ok(AsyncPgConnection::establish(&database_url).await?)
}

#[derive(diesel::QueryableByName)]
struct LockResult {
    #[diesel(sql_type = Bool)]
    acquired: bool,
}

async fn lock(feed_id: &str, wait: bool) -> Result<Option<AsyncPgConnection>, LockError> {
    let mut conn = connect().await?;
    let key = format!("maple:feed:{feed_id}");
    let sql = if wait {
        "SELECT true AS acquired FROM pg_advisory_lock(hashtextextended($1, 0))"
    } else {
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS acquired"
    };
    let result = diesel::sql_query(sql)
        .bind::<Text, _>(&key)
        .get_result::<LockResult>(&mut conn)
        .await?;
    Ok(result.acquired.then_some(conn))
}

/// Ingestion waits only for maintenance of the same feed.
pub(crate) async fn acquire(feed_id: &str) -> Result<AsyncPgConnection, LockError> {
    lock(feed_id, true)
        .await?
        .ok_or_else(|| "Failed to acquire feed lock".into())
}

/// Maintenance skips feeds already locked by another Maple process.
pub(crate) async fn try_acquire(feed_id: &str) -> Result<Option<AsyncPgConnection>, LockError> {
    lock(feed_id, false).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires PostgreSQL DATABASE_URL"]
    async fn locks_are_session_scoped_and_independent() {
        let feed = format!("test-maple-cleanup-{}", std::process::id());
        let first = acquire(&feed).await.unwrap();
        assert!(try_acquire(&feed).await.unwrap().is_none());
        let other = try_acquire(&format!("{feed}-other")).await.unwrap();
        assert!(other.is_some());
        drop(first);
        let released = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(lock) = try_acquire(&feed).await.unwrap() {
                    break lock;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(released.is_ok());
    }
}
