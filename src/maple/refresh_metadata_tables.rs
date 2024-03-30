use chateau::Chateau;
use diesel::query_dsl::methods::SelectDsl;
use diesel::SelectableHelper;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use dmfr_folder_reader::ReturnDmfrAnalysis;
use geo::BooleanOps;
use geo::Polygon;
use geo::{polygon, MultiPolygon};
use geo_repair_polygon::join::Join;
use std::collections::HashMap;
use std::collections::HashSet;
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

    let chateaus_pg = chateau_result
        .iter()
        .map(|(k, v)| {
            let languages_avaliable_pg: HashSet<String> = {
                let mut languages_avaliable_pg = HashSet::new();
                for static_id in v.static_feeds.iter() {
                    if let Some(static_feed) = existing_static_feeds_map.get(static_id) {
                        languages_avaliable_pg.extend(
                            static_feed
                                .languages_avaliable
                                .iter()
                                .filter(|x| x.is_some())
                                .map(|x| x.clone().unwrap()),
                        );
                    }
                }
                languages_avaliable_pg
            };

            //use geo_repair_polygon to merge hulls from each feed together

            let hulls_from_static: Vec<
                postgis_diesel::types::Polygon<postgis_diesel::types::Point>,
            > = {
                let mut hulls_from_static = vec![];
                for static_id in v.static_feeds.iter() {
                    if let Some(static_feed) = existing_static_feeds_map.get(static_id) {
                        if let Some(hull) = static_feed.hull.clone() {
                            hulls_from_static.push(hull);
                        }
                    }
                }
                hulls_from_static
            };
            //conversion to geo_types
            let hulls_from_static_geo_types: Vec<geo::Polygon<f64>> = hulls_from_static
                .iter()
                .filter(|x| x.rings.len() > 0)
                .map(|x| {
                    let mut points = vec![];
                    for point in x.rings[0].iter() {
                        points.push(geo::Point::new(point.x, point.y));
                    }
                    geo::Polygon::new(geo::LineString::from(points), vec![])
                })
                .collect();

            //merge hulls

            let hull: Option<geo::MultiPolygon> = match hulls_from_static_geo_types.is_empty() {
                true => None,
                false => Some({
                    let mut merged_hull: geo::MultiPolygon =
                        hulls_from_static_geo_types[0].clone().into();
                    for i in 1..hulls_from_static_geo_types.len() {
                        merged_hull =
                            merged_hull.union(&hulls_from_static_geo_types[i].clone().into());
                    }
                    merged_hull
                }),
            };

            catenary::models::Chateau {
                chateau: k.to_string(),
                hull: None,
                static_feeds: v.static_feeds.iter().map(|x| Some(x.to_string())).collect(),
                realtime_feeds: v
                    .realtime_feeds
                    .iter()
                    .map(|x| Some(x.to_string()))
                    .collect(),
                languages_avaliable: languages_avaliable_pg
                    .clone()
                    .into_iter()
                    .map(|x| Some(x))
                    .collect::<Vec<Option<String>>>(),
            }
        })
        .collect::<Vec<catenary::models::Chateau>>();

    //set each realtime feed to the new chateau id

            //set each static feed to the new chateau id
            // if static feed has a different chateau id, call on the update function
            // update_chateau_id_for_gtfs_schedule(feed_id, new_chateau_id, conn).await?;

    // if the new chateau id is different for any of the feeds, run the update function
    Ok(())
}
