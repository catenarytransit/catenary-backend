use std::sync::Arc;
use diesel::query_dsl::methods::FilterDsl;
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use diesel_async::{pooled_connection::bb8::PooledConnection, AsyncPgConnection};
use std::error::Error;
use crate::CatenaryPostgresPool;

pub async fn update_schedules_with_new_chateau_id(
    feed_id: &str,
    new_chateau_id: &str,
    pool: Arc<CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    use catenary::schema::gtfs::trips;
    use catenary::schema::gtfs::trips::dsl::trips as trips_table;

    let _ = diesel::update(trips_table)
        .filter(trips::dsl::onestop_feed_id.eq(feed_id))
        .set(trips::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

    use catenary::schema::gtfs::stoptimes;
    use catenary::schema::gtfs::stoptimes::dsl::stoptimes as stop_times_table;

    let _ = diesel::update(stop_times_table)
        .filter(stoptimes::dsl::onestop_feed_id.eq(feed_id))
        .set(stoptimes::dsl::chateau.eq(new_chateau_id))
        .execute(conn)
        .await?;

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

    //shapes_not_bus

    use catenary::schema::gtfs::shapes_not_bus;
    use catenary::schema::gtfs::shapes_not_bus::dsl::shapes_not_bus as shapes_not_bus_table;

    let _ = diesel::update(shapes_not_bus_table)
        .filter(shapes_not_bus::dsl::onestop_feed_id.eq(feed_id))
        .set(shapes_not_bus::dsl::chateau.eq(new_chateau_id))
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

    Ok(())
}
