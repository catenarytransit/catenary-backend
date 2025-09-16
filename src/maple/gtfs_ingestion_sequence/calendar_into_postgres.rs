use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::calendar_dates;
use diesel_async::RunQueryDsl;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;

pub async fn calendar_into_postgres(
    gtfs: &gtfs_structures::Gtfs,
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    //establish a connection to the database
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let mut vec_of_calendar = Vec::new();

    for (service_id, calendar) in &gtfs.calendar {
        let calendar_into_pg = catenary::models::Calendar {
            onestop_feed_id: feed_id.to_string(),
            service_id: calendar.id.to_string(),
            monday: calendar.monday,
            tuesday: calendar.tuesday,
            wednesday: calendar.wednesday,
            thursday: calendar.thursday,
            friday: calendar.friday,
            saturday: calendar.saturday,
            sunday: calendar.sunday,
            gtfs_start_date: calendar.start_date,
            gtfs_end_date: calendar.end_date,
            chateau: chateau_id.to_string(),
            attempt_id: attempt_id.to_string(),
        };

        vec_of_calendar.push(calendar_into_pg);
    }

    for calendar_chunk in vec_of_calendar.chunks(1000) {
        let _ = diesel::insert_into(catenary::schema::gtfs::calendar::table)
            .values(calendar_chunk)
            .execute(conn)
            .await?;
    }

    let mut vec_of_calendar_dates = Vec::new();

    for (service_id, calendar_dates) in &gtfs.calendar_dates {
        let mut calendar_dedup = calendar_dates.clone();

        calendar_dedup.dedup();

        for date in calendar_dedup {
            let calendar_date_pg = catenary::models::CalendarDate {
                onestop_feed_id: feed_id.to_string(),
                service_id: date.service_id.to_string(),
                gtfs_date: date.date,
                chateau: chateau_id.to_string(),
                exception_type: match date.exception_type {
                    gtfs_structures::Exception::Added => 1,
                    gtfs_structures::Exception::Deleted => 2,
                },
                attempt_id: attempt_id.to_string(),
            };

            vec_of_calendar_dates.push(calendar_date_pg);
        }
    }

    for calendar_date_chunk in vec_of_calendar_dates.chunks(2000) {
        let _ = diesel::insert_into(calendar_dates::table)
            .values(calendar_date_chunk)
            .execute(conn)
            .await?;
    }

    Ok(())
}
