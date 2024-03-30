use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::query_dsl::methods::FilterDsl;
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use std::error::Error;
use std::sync::Arc;

pub async fn delete_attempt_objects(
    feed_id: &str,
    attempt_id: &str,
    pool: Arc<CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + std::marker::Send + Sync>> {
    use catenary::schema::gtfs::trips;
    use catenary::schema::gtfs::trips::dsl::trips as trips_table;

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let _ = diesel::delete(
        trips_table.filter(
            trips::dsl::onestop_feed_id
                .eq(&feed_id)
                .and(trips::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    use catenary::schema::gtfs::stoptimes;
    use catenary::schema::gtfs::stoptimes::dsl::stoptimes as stop_times_table;

    let _ = diesel::delete(
        stop_times_table.filter(
            stoptimes::dsl::onestop_feed_id
                .eq(&feed_id)
                .and(stoptimes::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    //cleanup trip freq

    use catenary::schema::gtfs::trip_frequencies;
    use catenary::schema::gtfs::trip_frequencies::dsl::trip_frequencies as frequencies_table;
    let _ = diesel::delete(
        frequencies_table.filter(
            trip_frequencies::dsl::onestop_feed_id
                .eq(&feed_id)
                .and(trip_frequencies::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    use catenary::schema::gtfs::agencies;
    use catenary::schema::gtfs::agencies::dsl::agencies as agencies_table;

    let _ = diesel::delete(
        agencies_table.filter(
            agencies::dsl::static_onestop_id
                .eq(&feed_id)
                .and(agencies::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    use catenary::schema::gtfs::calendar_dates;
    use catenary::schema::gtfs::calendar_dates::dsl::calendar_dates as calendar_dates_table;

    let _ = diesel::delete(
        calendar_dates_table.filter(
            calendar_dates::dsl::onestop_feed_id
                .eq(&feed_id)
                .and(calendar_dates::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    use catenary::schema::gtfs::calendar;
    use catenary::schema::gtfs::calendar::dsl::calendar as calendar_table;

    let _ = diesel::delete(
        calendar_table.filter(
            calendar::dsl::onestop_feed_id
                .eq(&feed_id)
                .and(calendar::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    use catenary::schema::gtfs::routes;
    use catenary::schema::gtfs::routes::dsl::routes as routes_table;

    let _ = diesel::delete(
        routes_table.filter(
            routes::dsl::onestop_feed_id
                .eq(&feed_id)
                .and(routes::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    use catenary::schema::gtfs::shapes;
    use catenary::schema::gtfs::shapes::dsl::shapes as shapes_table;

    let _ = diesel::delete(
        shapes_table.filter(
            shapes::dsl::onestop_feed_id
                .eq(&feed_id)
                .and(shapes::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    use catenary::schema::gtfs::stops;
    use catenary::schema::gtfs::stops::dsl::stops as stops_table;

    let _ = diesel::delete(
        stops_table.filter(
            stops::dsl::onestop_feed_id
                .eq(&feed_id)
                .and(stops::dsl::attempt_id.eq(&attempt_id)),
        ),
    )
    .execute(conn)
    .await;

    //delete ingested static_download_attempts
    /*

    use catenary::schema::gtfs::static_download_attempts;
    use catenary::schema::gtfs::static_download_attempts::dsl::static_download_attempts as static_download_attempts_table;

    let _ = diesel::delete(
        static_download_attempts_table
        .filter(static_download_attempts::dsl::onestop_feed_id.eq(&feed_id).and(static_download_attempts::dsl::attempt_id.eq(&attempt_id)))
    ).execute(conn).await;
     */

    Ok(())
}
