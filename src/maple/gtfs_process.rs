use std::sync::Arc;
use std::error::Error;

// Initial version 3 of ingest written by Kyler Chin
// Removal of the attribution is not allowed, as covered under the AGPL license

// take a feed id and throw it into postgres
pub async fn gtfs_process_feed(feed_id: &str, pool: &Arc<sqlx::Pool<sqlx::Postgres>>) -> Result<(), Box<dyn Error>> {
    let path = format!("gtfs_uncompressed/{}", feed_id);
    
    let gtfs = gtfs_structures::Gtfs::new(path.as_str())?;

    
    
    Ok(())
}