// Copyright: Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of the attribution is not allowed, as covered under the AGPL license
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::ManagerConfig;
use diesel_async::pooled_connection::RecyclingMethod;
use diesel_async::pooled_connection::bb8::Pool;
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
    let mut custom_conf = ManagerConfig::default();

    custom_conf.recycling_method = RecyclingMethod::Fast;

    // create a new connection pool with the default config
    let config: AsyncDieselConnectionManager<diesel_async::AsyncPgConnection> =
        AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new_with_config(
            database_url_for_env(),
            custom_conf,
        );
    let max_size = env::var("POSTGRES_MAX_CONNECTIONS")
        .unwrap_or_else(|_| "128".to_string())
        .parse::<u32>()
        .unwrap_or(128);
    let pool = Pool::builder()
        .max_size(max_size)
        .min_idle(Some(16))
        .build(config)
        .await?;

    Ok(pool)
}

fn database_url_for_env() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL must be set")
}

pub async fn check_is_active(
    conn: &mut bb8::PooledConnection<
        '_,
        AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
    >,
) -> bool {
    use diesel_async::RunQueryDsl;
    diesel::sql_query("SELECT 1")
        .execute(&mut **conn)
        .await
        .is_ok()
}

pub async fn check_postgres_alive(
    pool: &CatenaryPostgresPool,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    use diesel_async::RunQueryDsl;

    let mut conn = pool.get().await?;
    let _ = diesel::sql_query("SELECT 1").execute(&mut conn).await?;

    Ok(())
}
