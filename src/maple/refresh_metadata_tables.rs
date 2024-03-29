use chateau::Chateau;
use diesel::query_dsl::methods::SelectDsl;
use diesel::SelectableHelper;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use dmfr_folder_reader::ReturnDmfrAnalysis;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

// Written by Kyler Chin at Catenary Transit Initiatives
// https://github.com/CatenaryTransit/catenary-backend
//You are required under the APGL license to retain this annotation as is

pub async fn refresh_metadata_assignments(
    dmfr_result: &ReturnDmfrAnalysis,
    chateau_result: &HashMap<String, Chateau>,
    pool: Arc<catenary::postgres_tools::CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    //update or create realtime tables and static tables

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let existing_chateaus = catenary::schema::gtfs::chateaus::table
        .select(catenary::models::Chateau::as_select())
        .load::<catenary::models::Chateau>(conn)
        .await?;

    let existing_realtime_feeds = catenary::schema::gtfs::realtime_feeds::table
        .select(catenary::models::RealtimeFeed::as_select())
        .load::<catenary::models::RealtimeFeed>(conn)
        .await?;

    let existing_static_feeds = catenary::schema::gtfs::static_feeds::table
        .select(catenary::models::StaticFeed::as_select())
        .load::<catenary::models::StaticFeed>(conn)
        .await?;

    // if the new chateau id is different for any of the feeds, run the update function
    Ok(())
}
