// Copyright: Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of the attribution is not allowed, as covered under the AGPL license

use bb8::Pool;
use db_pool::r#async::DatabasePool;
use db_pool::r#async::DatabasePoolBuilderTrait;
use db_pool::r#async::DieselAsyncPostgresBackend;
use db_pool::r#async::DieselBb8;
use db_pool::PrivilegedPostgresConfig;
use dotenvy::dotenv;
use std::env;
use tokio::sync::OnceCell;

/// This type alias is the pool, which can be quried for connections.
/// It is typically wrapped in Arc to allow thread safe cloning to the same pool
pub type CatenaryPostgresPool<'a> = db_pool::r#async::Reusable<
    'a,
    db_pool::r#async::ConnectionPool<
        db_pool::r#async::DieselAsyncPostgresBackend<db_pool::r#async::DieselBb8>,
    >,
>;

/// Type alias to the pooled connection
/// This must be used in a single thread, since it is mutable
pub type CatenaryPostgresConnection<'b> = &'b mut bb8::PooledConnection<
    'b,
    diesel_async::pooled_connection::AsyncDieselConnectionManager<
        diesel_async::pg::AsyncPgConnection,
    >,
>;

/// This returns a pool with a specified lifetime
pub async fn get_connection_pool<'pool_lifespan>() -> CatenaryPostgresPool<'pool_lifespan> {
    static POOL: OnceCell<DatabasePool<DieselAsyncPostgresBackend<DieselBb8>>> =
        OnceCell::const_new();

    let db_pool: &DatabasePool<DieselAsyncPostgresBackend<DieselBb8>> = POOL
        .get_or_init(|| async {
            dotenv().ok();

            let config = PrivilegedPostgresConfig::from_env().unwrap();

            let backend = DieselAsyncPostgresBackend::new(
                config,
                || Pool::builder().max_size(10),
                || Pool::builder().max_size(2),
                move |conn| {
                    Box::pin(async {
                        //don't create any entities
                        conn
                    })
                },
            )
            .await
            .unwrap();

            backend.create_database_pool().await.unwrap()
        })
        .await;

    db_pool.pull().await
}

fn database_url_for_env() -> String {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    database_url
}
