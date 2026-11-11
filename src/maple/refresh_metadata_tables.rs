// Written by Kyler Chin at Catenary Transit Initiatives
// Attribution cannot be removed
// https://github.com/CatenaryTransit/catenary-backend
//You are required under the APGL license to retain this annotation as is

use crate::chateau_postprocess::feed_id_to_chateau_id_pivot_table;
use crate::polygon_union::union_polygons;
use crate::update_schedules_with_new_chateau_id::update_schedules_with_new_chateau_id;
use catenary::schema::gtfs as gtfs_schema;
use chateau::Chateau;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel_async::{AsyncConnection, RunQueryDsl};
use dmfr_dataset_reader::ReturnDmfrAnalysis;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

pub async fn refresh_metadata_assignments(
    dmfr_result: &ReturnDmfrAnalysis,
    chateau_result: &HashMap<String, Chateau>,
    pool: Arc<catenary::postgres_tools::CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    //create a reverse table
    let feed_id_to_chateau_id_lookup_table = feed_id_to_chateau_id_pivot_table(chateau_result);

    //update or create realtime tables and static tables

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let existing_chateaus = catenary::schema::gtfs::chateaus::table
        .select(catenary::models::Chateau::as_select())
        .load::<catenary::models::Chateau>(conn)
        .await?;

    // create HashMap
    let existing_chateaus_map = existing_chateaus
        .iter()
        .map(|x| (x.chateau.clone(), x.clone()))
        .collect::<HashMap<String, catenary::models::Chateau>>();

    let existing_realtime_feeds = catenary::schema::gtfs::realtime_feeds::table
        .select(catenary::models::RealtimeFeed::as_select())
        .load::<catenary::models::RealtimeFeed>(conn)
        .await?;

    //create hashmap of realtime_feeds
    let existing_realtime_feeds_map = existing_realtime_feeds
        .iter()
        .map(|x| (x.onestop_feed_id.clone(), x.clone()))
        .collect::<HashMap<String, catenary::models::RealtimeFeed>>();

    let existing_static_feeds = catenary::schema::gtfs::static_feeds::table
        .select(catenary::models::StaticFeed::as_select())
        .load::<catenary::models::StaticFeed>(conn)
        .await?;

    //create hashmap of static_feeds

    let existing_static_feeds_map = existing_static_feeds
        .iter()
        .map(|x| (x.onestop_feed_id.clone(), x.clone()))
        .collect::<HashMap<String, catenary::models::StaticFeed>>();

    // Move owned inputs into a blocking task so CPU-bound geometry work does not
    // occupy a Tokio worker. Rayon bounds concurrency across independent chateaux.
    let chateau_inputs = chateau_result
        .iter()
        .map(|(id, chateau)| {
            (
                id.clone(),
                chateau.static_feeds.iter().cloned().collect::<Vec<_>>(),
                chateau.realtime_feeds.iter().cloned().collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let chateaus_pg = tokio::task::spawn_blocking(move || {
        chateau_inputs
            .into_par_iter()
            .map(|(id, static_feeds, realtime_feeds)| {
                let languages_avaliable_pg: HashSet<String> = {
                    let mut languages_avaliable_pg = HashSet::new();

                    for static_id in &static_feeds {
                        if let Some(static_feed) = existing_static_feeds_map.get(static_id) {
                            if let Some(default_lang) = static_feed.default_lang.clone() {
                                languages_avaliable_pg.insert(default_lang);
                            }

                            languages_avaliable_pg.extend(
                                static_feed
                                    .languages_avaliable
                                    .iter()
                                    .filter_map(|x| x.clone()),
                            );
                        }
                    }
                    languages_avaliable_pg
                };

                // Read borrowed hulls directly; avoid cloning the PostGIS geometries
                // and repeatedly overlaying an ever-growing intermediate multipolygon.
                let polygons = static_feeds
                    .iter()
                    .filter_map(|id| existing_static_feeds_map.get(id))
                    .filter_map(|feed| feed.hull.as_ref())
                    .filter_map(|hull| hull.rings.first())
                    .filter(|ring| ring.len() >= 4)
                    .map(|ring| {
                        geo::Polygon::new(
                            geo::LineString::from(
                                ring.iter().map(|p| (p.x, p.y)).collect::<Vec<_>>(),
                            ),
                            vec![],
                        )
                    })
                    .collect::<Vec<_>>();
                let hull = if polygons.is_empty() {
                    None
                } else {
                    Some(union_polygons(polygons))
                };

                let hull_pg: Option<
                    postgis_diesel::types::MultiPolygon<postgis_diesel::types::Point>,
                > = hull.map(catenary::postgis_to_diesel::multi_polygon_geo_to_diesel);

                catenary::models::Chateau {
                    chateau: id,
                    hull: hull_pg,
                    static_feeds: static_feeds.into_iter().map(Some).collect(),
                    realtime_feeds: realtime_feeds.into_iter().map(Some).collect(),
                    languages_avaliable: languages_avaliable_pg
                        .into_iter()
                        .map(Some)
                        .collect::<Vec<Option<String>>>(),
                }
            })
            .collect::<Vec<catenary::models::Chateau>>()
    })
    .await?;

    for delete_chateau in existing_chateaus {
        //if the current chateau doesn't exist anymore, delete it
        if !chateau_result.contains_key(&delete_chateau.chateau) {
            println!("Deleting chateau {}", delete_chateau.chateau);
            let _ = diesel::delete(catenary::schema::gtfs::chateaus::dsl::chateaus.filter(
                catenary::schema::gtfs::chateaus::dsl::chateau.eq(&delete_chateau.chateau),
            ))
            .execute(conn)
            .await?;
        }
    }

    for chateau_pg in chateaus_pg {
        diesel::insert_into(catenary::schema::gtfs::chateaus::dsl::chateaus)
            .values(chateau_pg.clone())
            .on_conflict(catenary::schema::gtfs::chateaus::dsl::chateau)
            .do_update()
            .set((
                catenary::schema::gtfs::chateaus::dsl::static_feeds.eq(chateau_pg.static_feeds),
                catenary::schema::gtfs::chateaus::dsl::realtime_feeds.eq(chateau_pg.realtime_feeds),
                catenary::schema::gtfs::chateaus::dsl::hull.eq(chateau_pg.hull),
                catenary::schema::gtfs::chateaus::dsl::languages_avaliable
                    .eq(chateau_pg.languages_avaliable),
            ))
            .execute(conn)
            .await?;
    }

    //set each realtime feed to the new chateau id

    let new_realtime_dataset = dmfr_result
        .feed_hashmap
        .iter()
        .filter(|(feed_id, feed)| match feed.spec {
            dmfr::FeedSpec::GtfsRt => true,
            _ => false,
        })
        .filter_map(|(feed_id, feed)| {
            feed_id_to_chateau_id_lookup_table
                .get(&feed_id.clone())
                .map(|chateau_id| catenary::models::RealtimeFeed {
                    onestop_feed_id: feed_id.clone(),
                    chateau: chateau_id.clone(),
                    previous_chateau_name: chateau_id.clone(),
                    fetch_interval_ms: match existing_realtime_feeds_map.get(&feed_id.clone()) {
                        Some(existing_realtime_feed) => existing_realtime_feed.fetch_interval_ms,
                        None => None,
                    },
                    realtime_vehicle_positions: feed
                        .urls
                        .realtime_vehicle_positions
                        .as_deref()
                        .cloned(),
                    realtime_trip_updates: feed.urls.realtime_trip_updates.as_deref().cloned(),
                    realtime_alerts: feed.urls.realtime_alerts.as_deref().cloned(),
                })
        })
        .collect::<Vec<catenary::models::RealtimeFeed>>();

    let new_rt_keys = new_realtime_dataset
        .iter()
        .map(|x| x.onestop_feed_id.clone())
        .collect::<HashSet<String>>();

    let rt_feeds_to_delete = existing_realtime_feeds_map
        .keys()
        .filter(|existing_feed| !new_rt_keys.contains(existing_feed.as_str()))
        .map(|x| x.clone())
        .collect::<Vec<String>>();

    for feed_to_delete in rt_feeds_to_delete {
        let _ = diesel::delete(
            gtfs_schema::realtime_feeds::dsl::realtime_feeds
                .filter(gtfs_schema::realtime_feeds::dsl::onestop_feed_id.eq(feed_to_delete)),
        )
        .execute(conn)
        .await?;
    }

    for new_realtime in new_realtime_dataset {
        let _ = diesel::insert_into(gtfs_schema::realtime_feeds::dsl::realtime_feeds)
            .values(new_realtime.clone())
            .on_conflict(gtfs_schema::realtime_feeds::dsl::onestop_feed_id)
            .do_update()
            .set((
                gtfs_schema::realtime_feeds::dsl::fetch_interval_ms
                    .eq(new_realtime.fetch_interval_ms),
                gtfs_schema::realtime_feeds::dsl::chateau.eq(new_realtime.chateau),
                gtfs_schema::realtime_feeds::dsl::realtime_vehicle_positions
                    .eq(new_realtime.realtime_vehicle_positions),
                gtfs_schema::realtime_feeds::dsl::realtime_trip_updates
                    .eq(new_realtime.realtime_trip_updates),
                gtfs_schema::realtime_feeds::dsl::realtime_alerts.eq(new_realtime.realtime_alerts),
            ))
            .execute(conn)
            .await?;
    }

    //set each static feed to the new chateau id
    // if static feed has a different chateau id, call on the update function
    for existing_schedule in &existing_static_feeds {
        if let Some(new_chateau_id) =
            feed_id_to_chateau_id_lookup_table.get(&existing_schedule.onestop_feed_id)
        {
            if &existing_schedule.chateau != new_chateau_id {
                update_schedules_with_new_chateau_id(
                    &existing_schedule.onestop_feed_id,
                    new_chateau_id,
                    Arc::clone(&pool),
                )
                .await?;

                //the table has been updated
                let _ = diesel::update(gtfs_schema::static_feeds::dsl::static_feeds)
                    .filter(
                        gtfs_schema::static_feeds::dsl::onestop_feed_id
                            .eq(&existing_schedule.onestop_feed_id),
                    )
                    .set((
                        gtfs_schema::static_feeds::dsl::chateau.eq(&new_chateau_id),
                        gtfs_schema::static_feeds::dsl::previous_chateau_name.eq(&new_chateau_id),
                    ))
                    .execute(conn)
                    .await?;
            }
        }
    }

    Ok(())
}
