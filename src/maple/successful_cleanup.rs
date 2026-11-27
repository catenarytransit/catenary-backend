// Copyright Catenary Transit Initiatives
// Attribution cannot be removed

use crate::{assign_production_tables, cleanup, feed_lock};
use catenary::models::IngestedStatic;
use catenary::schema::gtfs::{ingested_static, in_progress_static_ingests, shapes, stops};
use diesel::dsl::exists;
use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use std::error::Error;

type CleanupError = Box<dyn Error + Send + Sync>;

#[derive(diesel::QueryableByName)]
struct CandidateFeed {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
}

pub(crate) async fn run(feed_id: Option<&str>, dry_run: bool) -> Result<(), CleanupError> {
    let feeds: Vec<String> = if let Some(feed_id) = feed_id {
        vec![feed_id.to_owned()]
    } else {
        let mut conn = feed_lock::connect().await?;
        diesel::sql_query(
            "SELECT onestop_feed_id FROM gtfs.ingested_static
             WHERE production AND NOT deleted AND ingestion_successfully_finished
               AND NOT ingestion_errored AND NOT ingesting_in_progress
             GROUP BY onestop_feed_id HAVING count(*) > 1
             ORDER BY onestop_feed_id",
        )
        .load::<CandidateFeed>(&mut conn)
        .await?
        .into_iter()
        .map(|row| row.onestop_feed_id)
        .collect()
    };
    println!("Maple cleanup: {} candidate feeds, dry_run={dry_run}", feeds.len());
    let mut retired = 0usize;
    let mut failures = 0usize;
    // Process sequentially to limit database I/O and WAL pressure.
    for feed in &feeds {
        match cleanup_feed(feed, dry_run).await {
            Ok(count) => retired += count,
            Err(error) => {
                failures += 1;
                eprintln!("Cleanup failed for {feed}: {error}");
            }
        }
    }
    println!("Maple cleanup finished: {retired} attempts {}, {failures} failed feeds",
        if dry_run { "eligible" } else { "retired" });
    if failures != 0 {
        return Err(format!("Cleanup failed for {failures} feeds").into());
    }
    Ok(())
}

async fn cleanup_feed(feed_id: &str, dry_run: bool) -> Result<usize, CleanupError> {
    let Some(mut conn) = feed_lock::try_acquire(feed_id).await? else {
        println!("Skipping {feed_id}: locked by another Maple process");
        return Ok(0);
    };
    if diesel::select(exists(
        in_progress_static_ingests::table
            .filter(in_progress_static_ingests::onestop_feed_id.eq(feed_id)),
    )).get_result::<bool>(&mut conn).await? {
        println!("Skipping {feed_id}: in-progress ingest marker");
        return Ok(0);
    }

    // Read the attempts again after obtaining the lock.
    let attempts = ingested_static::table
        .filter(ingested_static::onestop_feed_id.eq(feed_id))
        .filter(ingested_static::deleted.eq(false))
        .select(IngestedStatic::as_select())
        .load::<IngestedStatic>(&mut conn).await?;
    let today = catenary_time::utc_now().naive_utc().date();
    let Some(plan) = assign_production_tables::successful_cleanup_plan(feed_id, &attempts, today)
    else {
        println!("Skipping {feed_id}: no completed production schedule or assignment pending");
        return Ok(0);
    };
    if plan.obsolete_attempts.is_empty() {
        return Ok(0);
    }

    // Do not promote a future-dated feed or remove a still-visible schedule.
    let Some(current) = plan.current_attempt else {
        println!("Skipping {feed_id}: no retained attempt covers today");
        return Ok(0);
    };
    if !has_active_stops(&mut conn, feed_id, &current).await? {
        println!("Skipping {feed_id}: replacement {current} is not spatially active");
        return Ok(0);
    }

    let mut retired = 0;
    for obsolete in &plan.obsolete_attempts {
        if has_active_stops(&mut conn, feed_id, obsolete).await?
            || diesel::select(exists(
                shapes::table
                    .filter(shapes::onestop_feed_id.eq(feed_id))
                    .filter(shapes::attempt_id.eq(obsolete))
                    .filter(shapes::allowed_spatial_query.eq(true)),
            )).get_result::<bool>(&mut conn).await?
        {
            println!("Skipping {feed_id} / {obsolete}: obsolete attempt is still spatially active");
            continue;
        }

        println!("{} {feed_id} / {obsolete}; retaining {current}",
            if dry_run { "Would retire" } else { "Retiring" });
        if !dry_run {
            let current = current.as_str();
            let obsolete = obsolete.as_str();
            // Recheck and lock both ingestion records, then delete atomically
            // on the same session holding the advisory lock.
            conn.transaction::<_, diesel::result::Error, _>(|conn| {
                async move {
                    require_completed_production(conn, feed_id, current).await?;
                    require_completed_production(conn, feed_id, obsolete).await?;
                    cleanup::delete_attempt_objects_on_connection(conn, feed_id, obsolete).await
                }.scope_boxed()
            }).await?;
        }
        retired += 1;
    }
    Ok(retired)
}

async fn require_completed_production(
    conn: &mut AsyncPgConnection,
    feed_id: &str,
    attempt_id: &str,
) -> Result<(), diesel::result::Error> {
    ingested_static::table
        .filter(ingested_static::onestop_feed_id.eq(feed_id))
        .filter(ingested_static::attempt_id.eq(attempt_id))
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .filter(ingested_static::ingestion_successfully_finished.eq(true))
        .filter(ingested_static::ingestion_errored.eq(false))
        .filter(ingested_static::ingesting_in_progress.eq(false))
        .select(ingested_static::attempt_id)
        .for_update()
        .first::<String>(conn).await?;
    Ok(())
}

async fn has_active_stops(
    conn: &mut AsyncPgConnection,
    feed_id: &str,
    attempt_id: &str,
) -> Result<bool, diesel::result::Error> {
    diesel::select(exists(
        stops::table
            .filter(stops::onestop_feed_id.eq(feed_id))
            .filter(stops::attempt_id.eq(attempt_id))
            .filter(stops::allowed_spatial_query.eq(true)),
    )).get_result::<bool>(conn).await
}
