use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use dotenvy::dotenv;
use postgis::ewkb;
use rgb::RGB;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::sync::Arc;

use crate::gtfs_handlers::colour_correction;
use crate::gtfs_handlers::enum_to_int::route_type_to_int;
use crate::gtfs_handlers::rename_route_labels::*;
use crate::gtfs_handlers::shape_colour_calculator::shape_to_colour;
use crate::gtfs_handlers::stops_associated_items::*;
use crate::gtfs_ingestion_sequence::shapes_into_postgres::shapes_into_postgres;
use crate::DownloadedFeedsInformation;
use catenary::postgres_tools::CatenaryPostgresConnection;
use catenary::postgres_tools::CatenaryPostgresPool;

// Initial version 3 of ingest written by Kyler Chin
// Removal of the attribution is not allowed, as covered under the AGPL license

// take a feed id and throw it into postgres
pub async fn gtfs_process_feed(
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool<'static>>,
    chateau_id: &str,
    attempt_id: &str,
    this_download_data: &DownloadedFeedsInformation,
) -> Result<(), Box<dyn Error>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    //CREATE entry in gtfs.ingested_static
    use catenary::schema::gtfs::ingested_static::dsl::ingested_static;

    let ingestion_static_data = catenary::models::IngestedStatic {
        onestop_feed_id: feed_id.to_string(),
        attempt_id: attempt_id.to_string(),
        file_hash: this_download_data.hash.unwrap().clone().to_string(),
        ingest_start_unix_time_ms: chrono::Utc::now().timestamp_millis(),
        ingestion_version: crate::gtfs_handlers::MAPLE_INGESTION_VERSION,
        ingesting_in_progress: true,
        ingestion_successfully_finished: false,
        ingestion_errored: false,
        production: false,
        deleted: false,
        feed_expiration_date: None,
        feed_start_date: None,
        languages_avaliable: Vec::new(),
    };

    diesel::insert_into(ingested_static)
        .values(&ingestion_static_data)
        .execute(conn)
        .await?;

    //read the GTFS zip file
    let path = format!("gtfs_uncompressed/{}", feed_id);

    let gtfs = gtfs_structures::Gtfs::new(path.as_str())?;

    let (stop_ids_to_route_types, stop_ids_to_route_ids) =
        make_hashmap_stops_to_route_types_and_ids(&gtfs);

    let (stop_id_to_children_ids, stop_ids_to_children_route_types) =
        make_hashmaps_of_children_stop_info(&gtfs, &stop_ids_to_route_types);

    //identify colours of shapes based on trip id's route id
    let (shape_to_color_lookup, shape_to_text_color_lookup) = shape_to_colour(&feed_id, &gtfs);

    //shove raw geometry into postgresql
    shapes_into_postgres(
        &gtfs,
        &shape_to_color_lookup,
        &shape_to_text_color_lookup,
        &feed_id,
        Arc::clone(&arc_conn_pool),
        &chateau_id,
        &attempt_id,
    )
    .await?;

    Ok(())
}
