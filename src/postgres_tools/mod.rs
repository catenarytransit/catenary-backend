// Copyright: Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of the attribution is not allowed, as covered under the AGPL license
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use std::env;

pub type CatenaryPostgresPool =
    bb8::Pool<AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>>;

pub type CatenaryConn<'a> = &'a mut bb8::PooledConnection<
    'a,
    diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
>;

/// This type alias is the pool, which can be quried for connections.
/// It is typically wrapped in Arc to allow thread safe cloning to the same pool
/*pub type CatenaryPostgresPool<'a> = db_pool::r#async::Reusable<
    'a,
    db_pool::r#async::ConnectionPool<
        db_pool::r#async::DieselAsyncPostgresBackend<db_pool::r#async::DieselBb8>,
    >,
>;*/

/// Type alias to the pooled connection
/// This must be used in a single thread, since it is mutable
/*pub type CatenaryPostgresConnection<'b> = &'b mut bb8::PooledConnection<
    'b,
    diesel_async::pooled_connection::AsyncDieselConnectionManager<
        diesel_async::pg::AsyncPgConnection,
    >,
>;
*/
// This is very broken, i think it's db-pool being a problem
// This returns a pool with a specified lifetime
/*
pub async fn get_connection_pool<'pool_lifespan>() -> CatenaryPostgresPool<'pool_lifespan> {
    static POOL: OnceCell<DatabasePool<DieselAsyncPostgresBackend<DieselBb8>>> =
        OnceCell::const_new();

    let db_pool: &DatabasePool<DieselAsyncPostgresBackend<DieselBb8>> = POOL
        .get_or_init(|| async {
            dotenv().ok();

            let config = PrivilegedPostgresConfig::new()
            .password(Some("potgres".to_owned()));


            let backend = DieselAsyncPostgresBackend::new(
                config,
                || Pool::builder().max_size(10),
                || Pool::builder().max_size(2),
                move |mut conn| Box::pin(async { conn }),
            )
            .await
            .unwrap();

            backend.create_database_pool().await.unwrap()
        })
        .await;

    db_pool.pull().await
}*/

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
