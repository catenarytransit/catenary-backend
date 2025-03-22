// Written by Kyler Chin at Catenary Transit Initiatives
// Attribution cannot be removed
// https://github.com/CatenaryTransit/catenary-backend
// You are required under the APGL license to retain this annotation as is

use crate::CatenaryPostgresPool;
use diesel::ExpressionMethods;
use diesel::query_dsl::methods::FilterDsl;
use diesel_async::RunQueryDsl;
use std::error::Error;
use std::sync::Arc;

pub async fn update_schedules_with_new_chateau_id(
    feed_id: &str,
    new_chateau_id: &str,
    pool: Arc<CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    println!("Reassigning feed {} to Château {}", feed_id, new_chateau_id);

    use catenary::schema::gtfs::agencies;
    use catenary::schema::gtfs::agencies::dsl::agencies as agencies_table;

    let _ = diesel::update(agencies_table)
        .filter(agencies::dsl::static_onestop_id.eq(feed_id))
        .set(agencies::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    //routes

    use catenary::schema::gtfs::routes;
    use catenary::schema::gtfs::routes::dsl::routes as routes_table;

    let _ = diesel::update(routes_table)
        .filter(routes::dsl::onestop_feed_id.eq(feed_id))
        .set(routes::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    //shapes

    use catenary::schema::gtfs::shapes;
    use catenary::schema::gtfs::shapes::dsl::shapes as shapes_table;

    let _ = diesel::update(shapes_table)
        .filter(shapes::dsl::onestop_feed_id.eq(feed_id))
        .set(shapes::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    //stops

    use catenary::schema::gtfs::stops;
    use catenary::schema::gtfs::stops::dsl::stops as stops_table;

    let _ = diesel::update(stops_table)
        .filter(stops::dsl::onestop_feed_id.eq(feed_id))
        .set(stops::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    //calendar

    use catenary::schema::gtfs::calendar;
    use catenary::schema::gtfs::calendar::dsl::calendar as calendar_table;

    let _ = diesel::update(calendar_table)
        .filter(calendar::dsl::onestop_feed_id.eq(feed_id))
        .set(calendar::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    //calendar_dates

    use catenary::schema::gtfs::calendar_dates;
    use catenary::schema::gtfs::calendar_dates::dsl::calendar_dates as calendar_dates_table;

    let _ = diesel::update(calendar_dates_table)
        .filter(calendar_dates::dsl::onestop_feed_id.eq(feed_id))
        .set(calendar_dates::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    //feed info

    use catenary::schema::gtfs::feed_info;
    use catenary::schema::gtfs::feed_info::dsl::feed_info as feed_info_table;

    let _ = diesel::update(feed_info_table)
        .filter(feed_info::dsl::onestop_feed_id.eq(feed_id))
        .set(feed_info::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    use catenary::schema::gtfs::trips_compressed;

    let _ = diesel::update(trips_compressed::dsl::trips_compressed)
        .filter(trips_compressed::dsl::onestop_feed_id.eq(feed_id))
        .set(trips_compressed::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    use catenary::schema::gtfs::itinerary_pattern;

    let _ = diesel::update(itinerary_pattern::dsl::itinerary_pattern)
        .filter(itinerary_pattern::dsl::onestop_feed_id.eq(feed_id))
        .set(itinerary_pattern::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    use catenary::schema::gtfs::itinerary_pattern_meta;

    let _ = diesel::update(itinerary_pattern_meta::dsl::itinerary_pattern_meta)
        .filter(itinerary_pattern_meta::dsl::onestop_feed_id.eq(feed_id))
        .set(itinerary_pattern_meta::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    //reassign directions

    use catenary::schema::gtfs::direction_pattern;

    let _ = diesel::update(direction_pattern::dsl::direction_pattern)
        .filter(direction_pattern::dsl::onestop_feed_id.eq(feed_id))
        .set(direction_pattern::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    use catenary::schema::gtfs::direction_pattern_meta;

    let _ = diesel::update(direction_pattern_meta::dsl::direction_pattern_meta)
        .filter(direction_pattern_meta::dsl::onestop_feed_id.eq(feed_id))
        .set(direction_pattern_meta::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    println!(
        "Finished reassignment of feed {} to Château {}",
        feed_id, new_chateau_id
    );

    Ok(())
}
