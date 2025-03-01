use catenary::ip_to_location::insert_ip_db_into_postgres;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::postgres_tools::make_async_pool;
use std::error::Error;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // get connection pool from database pool
    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    println!("Insert Geocoding from IP address db");

    let status_ip_db_insert = insert_ip_db_into_postgres(Arc::clone(&arc_conn_pool)).await;

    if let Err(err) = &status_ip_db_insert {
        eprintln!("{:#?}", err);
    }

    Ok(())
}
