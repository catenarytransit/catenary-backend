use diesel::prelude::*;
use diesel::prelude::*;
use dotenvy::dotenv;
use bb8::Pool;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::thread;
use tokio::sync::OnceCell;
use db_pool::r#async::ConnectionPool;
use db_pool::r#async::DieselAsyncPostgresBackend;
use db_pool::r#async::Reusable;
use db_pool::r#async::DatabasePool;
use db_pool::r#async::DieselBb8;
use db_pool::PrivilegedPostgresConfig;
use db_pool::r#async::DatabasePoolBuilderTrait;

pub type CatenaryPostgresPool<'a> = db_pool::r#async::Reusable<'a, db_pool::r#async::ConnectionPool<db_pool::r#async::DieselAsyncPostgresBackend<db_pool::r#async::DieselBb8>>>;
pub type CatenaryPostgresConnection<'b> = &'b mut bb8::PooledConnection<'b, diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::pg::AsyncPgConnection>>;

pub async fn get_connection_pool(
) -> CatenaryPostgresPool<'static> {
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
                move |mut conn| {
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
