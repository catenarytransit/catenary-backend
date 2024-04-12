// Copyright: Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of the attribution is not allowed, as covered under the AGPL license
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use std::env;

/// This type alias is the pool, which can be quried for connections.
/// It is typically wrapped in Arc to allow thread safe cloning to the same pool
pub type CatenaryPostgresPool =
    bb8::Pool<AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>>;

/// Type alias to the pooled connection
/// This must be used in a single thread, since it is mutable
pub type CatenaryConn<'a> = &'a mut bb8::PooledConnection<
    'a,
    diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
>;

pub async fn make_async_pool() -> Result<
    bb8::Pool<AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>>,
    Box<dyn std::error::Error + Sync + Send>,
> {
    // create a new connection pool with the default config
    let config: AsyncDieselConnectionManager<diesel_async::AsyncPgConnection> =
        AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(database_url_for_env());
    let pool = Pool::builder().build(config).await?;

    Ok(pool)
}

fn database_url_for_env() -> String {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    database_url
}
