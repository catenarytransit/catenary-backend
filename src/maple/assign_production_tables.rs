// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::naive::NaiveDate;
use diesel::SelectableHelper;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use std::error::Error;
use std::ops::Sub;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

struct FeedTimeRange {
    attempt_id: String,
    start_date: Option<NaiveDate>,
    expiration_date: Option<NaiveDate>,
}

fn date_is_within_feed_time_range(date: NaiveDate, feed_time_range: &FeedTimeRange) -> bool {
    let within_start_bounds = match feed_time_range.start_date {
        None => true,
        Some(start_date) => date >= start_date,
    };

    let within_end_bounds = match feed_time_range.expiration_date {
        None => true,
        Some(expiration_date) => date <= expiration_date,
    };

    within_start_bounds && within_end_bounds
}

// This feed replaces its previous schedule on each successful import rather
// than using the normal, date-range-based retention of historical GTFS copies.
const SINGLE_INGEST_FEEDS: &[&str] = &[
    "f-9-amtrak~amtrakcalifornia~amtrakcharteredvehicle",
    "f-networkrail",
    "f-sncf~fr",
];

fn retain_only_latest_ingest(feed_id: &str) -> bool {
    SINGLE_INGEST_FEEDS.contains(&feed_id)
}

fn single_ingest_retention(
    all_feeds: &[catenary::models::IngestedStatic],
) -> Option<(FeedTimeRange, Vec<String>)> {
    // Use start time, as Maple does for normal retention. Break ties
    // deterministically to avoid selecting a different attempt on each run.
    let newest = all_feeds
        .iter()
        .filter(|attempt| attempt.ingestion_successfully_finished && !attempt.deleted)
        .max_by(|a, b| {
            a.ingest_start_unix_time_ms
                .cmp(&b.ingest_start_unix_time_ms)
                .then_with(|| a.ingest_end_unix_time_ms.cmp(&b.ingest_end_unix_time_ms))
                .then_with(|| a.attempt_id.cmp(&b.attempt_id))
        })?;

    // Do not destroy an attempt which is still actively ingesting.
    let mut drop_attempt_ids = all_feeds
        .iter()
        .filter(|attempt| attempt.attempt_id != newest.attempt_id && !attempt.ingesting_in_progress)
        .map(|attempt| attempt.attempt_id.clone())
        .collect::<Vec<_>>();
    drop_attempt_ids.sort();
    drop_attempt_ids.dedup();

    Some((
        FeedTimeRange {
            attempt_id: newest.attempt_id.clone(),
            start_date: newest.feed_start_date,
            expiration_date: newest.feed_expiration_date,
        },
        drop_attempt_ids,
    ))
}

// Share the date-range policy between production assignment and standalone cleanup.
fn dated_ingest_retention(
    all_feeds: &[catenary::models::IngestedStatic],
    now: NaiveDate,
) -> (Vec<FeedTimeRange>, Vec<String>) {
    let mut sorted_feeds = all_feeds
        .iter()
        .filter(|a| a.ingestion_successfully_finished && !a.deleted)
        .collect::<Vec<_>>();
    sorted_feeds.sort_by(|a, b| {
        a.ingest_start_unix_time_ms
            .cmp(&b.ingest_start_unix_time_ms)
            .then_with(|| a.ingest_end_unix_time_ms.cmp(&b.ingest_end_unix_time_ms))
            .then_with(|| a.attempt_id.cmp(&b.attempt_id))
    });
    let mut retained: Vec<FeedTimeRange> = Vec::new();
    let mut obsolete = Vec::new();
    for attempt in sorted_feeds.into_iter().rev() {
        let expiration_date = match retained.last() {
            None => attempt.feed_expiration_date,
            Some(newer) => match newer.start_date {
                None => {
                    obsolete.push(attempt.attempt_id.clone());
                    continue;
                }
                Some(start) => {
                    let end = start.sub(chrono::Days::new(1));
                    let end = attempt
                        .feed_expiration_date
                        .map_or(end, |date| date.min(end));
                    if end < now.sub(chrono::Days::new(5)) {
                        obsolete.push(attempt.attempt_id.clone());
                        continue;
                    }
                    Some(end)
                }
            },
        };
        retained.push(FeedTimeRange {
            attempt_id: attempt.attempt_id.clone(),
            start_date: attempt.feed_start_date,
            expiration_date,
        });
    }
    (retained, obsolete)
}

pub(crate) struct SuccessfulCleanupPlan {
    pub(crate) obsolete_attempts: Vec<String>,
    pub(crate) current_attempt: Option<String>,
}

/// Only completed production attempts qualify for standalone cleanup.
pub(crate) fn successful_cleanup_plan(
    feed_id: &str,
    all_feeds: &[catenary::models::IngestedStatic],
    now: NaiveDate,
) -> Option<SuccessfulCleanupPlan> {
    // An unfinished production assignment must not be mistaken for retirement.
    if all_feeds.iter().any(|a| {
        !a.deleted
            && (a.ingesting_in_progress || (a.ingestion_successfully_finished && !a.production))
    }) {
        return None;
    }
    let production = all_feeds
        .iter()
        .filter(|a| {
            a.production
                && !a.deleted
                && a.ingestion_successfully_finished
                && !a.ingestion_errored
                && !a.ingesting_in_progress
        })
        .cloned()
        .collect::<Vec<_>>();
    let (retained, obsolete_attempts) = if retain_only_latest_ingest(feed_id) {
        let (latest, obsolete) = single_ingest_retention(&production)?;
        (vec![latest], obsolete)
    } else {
        dated_ingest_retention(&production, now)
    };
    if retained.is_empty() {
        return None;
    }
    let current_attempt = retained
        .iter()
        .find(|range| date_is_within_feed_time_range(now, range))
        .map(|range| range.attempt_id.clone());
    Some(SuccessfulCleanupPlan {
        obsolete_attempts,
        current_attempt,
    })
}

pub async fn assign_production_tables(
    feed_id: &str,
    attempt_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    bbox: Option<geo::Rect>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let bbox_slippy = match bbox {
        Some(bbox) => Some(slippy_map_tiles::BBox::new(
            bbox.max().y as f32,
            bbox.min().x as f32,
            bbox.min().y as f32,
            bbox.max().x as f32,
        )),
        None => None,
    }
    .flatten();

    let now: NaiveDate = catenary_time::utc_now().naive_utc().date();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    //determine if the old one should be deleted, if so, delete it

    //algorithm:
    // If the latest file does not contain a feed info, wipe all old feeds and put the latest file into production

    //call function to clean old gtfs feeds, accepting feed_id, sqlx pool as arguments
    //greedy algorithm starts from newest feeds and examines date ranges, and works successively towards older feeds, assigning date ranges to feeds not already taken.
    //data structure can be a Vec of (start_date, end_date, attempt_id or hash)
    // older feeds cannot claim dates that are after a newer feed's experation date
    //any feed that does not have a date range any
    // more or is sufficiently old (over 7 days old) is wiped

    //query all the feeds in the production table

    //end date is always inclusive
    // "service in the period from the beginning of the feed_start_date day to the end of the feed_end_date day"
    //https://gtfs.org/schedule/reference/#feed_infotxt

    use catenary::schema::gtfs::ingested_static::dsl as ingested_static_columns;
    use catenary::schema::gtfs::ingested_static::dsl::ingested_static;
    use diesel::ExpressionMethods;

    use catenary::schema::gtfs::feed_info::dsl as feed_info_columns;
    use catenary::schema::gtfs::feed_info::dsl::feed_info;

    let current_feed_dates = feed_info
        .filter(feed_info_columns::onestop_feed_id.eq(feed_id))
        .filter(feed_info_columns::attempt_id.eq(attempt_id))
        .select((
            feed_info_columns::feed_start_date,
            feed_info_columns::feed_end_date,
        ))
        .load::<(Option<NaiveDate>, Option<NaiveDate>)>(conn)
        .await?
        .into_iter()
        .next();

    if let Some((feed_start_date, feed_end_date)) = current_feed_dates {
        diesel::update(
            ingested_static
                .filter(ingested_static_columns::onestop_feed_id.eq(feed_id))
                .filter(ingested_static_columns::attempt_id.eq(attempt_id)),
        )
        .set((
            ingested_static_columns::feed_start_date.eq(feed_start_date),
            ingested_static_columns::feed_expiration_date.eq(feed_end_date),
        ))
        .execute(conn)
        .await?;

        println!(
            "GTFS lifecycle dates for {} / {}: start={:?}, end={:?}",
            feed_id, attempt_id, feed_start_date, feed_end_date
        );
    }

    let all_feeds: Vec<catenary::models::IngestedStatic> = ingested_static
        .filter(ingested_static_columns::onestop_feed_id.eq(feed_id))
        .select(catenary::models::IngestedStatic::as_select())
        .load::<catenary::models::IngestedStatic>(conn)
        .await?;

    let (feed_time_ranges, mut drop_attempt_list) = if retain_only_latest_ingest(feed_id) {
        let (latest, old_attempts) = single_ingest_retention(&all_feeds)
            .ok_or_else(|| format!("No successful ingest to retain for {}", feed_id))?;
        (vec![latest], old_attempts)
    } else {
        let (retained, mut obsolete) = dated_ingest_retention(&all_feeds, now);
        // Failed attempts are handled only on the normal ingestion path.
        obsolete.extend(
            all_feeds
                .iter()
                .filter(|a| {
                    !a.ingestion_successfully_finished
                        && !a.ingesting_in_progress
                        && a.ingest_start_unix_time_ms < (now_ms as i64 - (1000 * 86400))
                })
                .map(|a| a.attempt_id.clone()),
        );
        (retained, obsolete)
    };
    drop_attempt_list.sort();
    drop_attempt_list.dedup();

    //prepare data for the commit
    let drop_attempt_list_transaction = drop_attempt_list.clone();
    // transactions to mark as successful
    let production_list_ids: Vec<String> = feed_time_ranges
        .iter()
        .map(|feed_time_range| feed_time_range.attempt_id.clone())
        .collect();

    let now: NaiveDate = catenary_time::utc_now().naive_utc().date();

    // feed_time_ranges is ordered newest ingest -> oldest ingest. Pick the first
    // range that actually contains today. A future feed remains in production but
    // is not spatially active until feed_start_date. Likewise, an expired feed is
    // not selected just because it is the only retained attempt.
    let current_feed_id = feed_time_ranges
        .iter()
        .find(|feed_time_range| date_is_within_feed_time_range(now, feed_time_range))
        .map(|feed_time_range| feed_time_range.attempt_id.clone());

    println!(
        "GTFS lifecycle for {}: retaining {:?}, dropping {:?}, spatially active {:?}",
        feed_id, production_list_ids, drop_attempt_list, current_feed_id
    );

    //mark old feeds as not in production anymore and new feeds as in production
    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        {
            async move {
                use catenary::schema::gtfs::ingested_static::dsl as ingested_static_columns;
                use catenary::schema::gtfs::ingested_static::dsl::ingested_static;

                use catenary::schema::gtfs::shapes::dsl as shapes_columns;
                use catenary::schema::gtfs::shapes::dsl::shapes;

                use catenary::schema::gtfs::stops::dsl as stops_columns;
                use catenary::schema::gtfs::stops::dsl::stops;

                //determine which one is active for map queries. All retained
                // attempts stay in production; only the attempt whose effective
                // feed_info date range contains today is spatially queriable.
                for production_list_id in production_list_ids {
                    let is_this_feed_spatial_queriable = current_feed_id
                        .as_ref()
                        .map(|current_feed_id| current_feed_id == &production_list_id)
                        .unwrap_or(false);

                    //update the shapes to be queriable
                    let _ = diesel::update(
                        shapes
                            .filter(shapes_columns::onestop_feed_id.eq(&feed_id))
                            .filter(shapes_columns::attempt_id.eq(&production_list_id)),
                    )
                    .set(shapes_columns::allowed_spatial_query.eq(is_this_feed_spatial_queriable))
                    .execute(conn)
                    .await?;

                    //update the stops to be queriable
                    let _ = diesel::update(
                        stops
                            .filter(stops_columns::onestop_feed_id.eq(&feed_id))
                            .filter(stops_columns::attempt_id.eq(&production_list_id)),
                    )
                    .set(stops_columns::allowed_spatial_query.eq(is_this_feed_spatial_queriable))
                    .execute(conn)
                    .await?;

                    let _ = diesel::update(
                        ingested_static
                            .filter(ingested_static_columns::onestop_feed_id.eq(&feed_id))
                            .filter(ingested_static_columns::attempt_id.eq(&production_list_id)),
                    )
                    .set((
                        ingested_static_columns::deleted.eq(false),
                        ingested_static_columns::production.eq(true),
                    ))
                    .execute(conn)
                    .await?;
                }

                for drop_id in drop_attempt_list_transaction {
                    let _ = diesel::update(
                        ingested_static
                            .filter(ingested_static_columns::onestop_feed_id.eq(&feed_id))
                            .filter(ingested_static_columns::attempt_id.eq(&drop_id)),
                    )
                    .set((
                        ingested_static_columns::deleted.eq(true),
                        ingested_static_columns::production.eq(false),
                    ))
                    .execute(conn)
                    .await?;

                    //delete all the shapes
                    let _ = diesel::delete(
                        shapes
                            .filter(shapes_columns::onestop_feed_id.eq(&feed_id))
                            .filter(shapes_columns::attempt_id.eq(&drop_id)),
                    )
                    .execute(conn)
                    .await?;

                    //delete all the stops
                    let _ = diesel::delete(
                        stops
                            .filter(stops_columns::onestop_feed_id.eq(&feed_id))
                            .filter(stops_columns::attempt_id.eq(&drop_id)),
                    )
                    .execute(conn)
                    .await?;
                }

                Ok(())
            }
        }
        .scope_boxed()
    })
    .await?;

    //drop and cleanup everything in drop_attempt_list

    for drop_attempt_id in &drop_attempt_list {
        use crate::cleanup::delete_attempt_objects;

        println!(
            "Deleting old data {} in feed {}",
            &drop_attempt_id, &feed_id
        );
        let cleanup_result =
            delete_attempt_objects(feed_id, drop_attempt_id, Arc::clone(&arc_conn_pool)).await;

        if retain_only_latest_ingest(feed_id) {
            // Delete the ingest record itself only after its PostgreSQL schedule
            // rows have been cleaned. A failed cleanup is surfaced for retry.
            cleanup_result?;
            let mut conn = arc_conn_pool.get().await?;
            diesel::delete(
                ingested_static
                    .filter(ingested_static_columns::onestop_feed_id.eq(feed_id))
                    .filter(ingested_static_columns::attempt_id.eq(drop_attempt_id)),
            )
            .execute(&mut conn)
            .await?;
        } else if let Err(error) = cleanup_result {
            eprintln!(
                "Failed to delete old attempt {} for feed {}: {}",
                drop_attempt_id, feed_id, error
            );
        }
    }

    //wipe the old tile cache

    if let Some(bbox_slippy) = bbox_slippy {
        println!("Wiping tile cache for feed {}", &feed_id);

        let start_timer = std::time::Instant::now();

        let _ =
            catenary::shape_fetcher::wipe_all_relevant_tiles_for_bbox(conn, &bbox_slippy).await?;

        println!(
            "Wiped tile cache for feed {}, took {:?}",
            &feed_id,
            start_timer.elapsed()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(
        id: &str,
        start_ms: i64,
        successful: bool,
        in_progress: bool,
        deleted: bool,
    ) -> catenary::models::IngestedStatic {
        catenary::models::IngestedStatic {
            onestop_feed_id: SINGLE_INGEST_FEEDS[0].to_string(),
            attempt_id: id.to_string(),
            ingest_start_unix_time_ms: start_ms,
            ingest_end_unix_time_ms: start_ms + 100,
            ingest_duration_ms: 100,
            file_hash: id.to_string(),
            ingesting_in_progress: in_progress,
            ingestion_errored: !successful && !in_progress,
            ingestion_successfully_finished: successful,
            production: successful,
            deleted,
            feed_expiration_date: None,
            feed_start_date: None,
            ingestion_version: 1,
            default_lang: None,
            languages_avaliable: vec![],
            hash_of_file_contents: None,
        }
    }

    #[test]
    fn single_ingest_keeps_only_latest_successful_ingest() {
        let mut older = attempt("older", 100, true, false, false);
        older.feed_expiration_date = NaiveDate::from_ymd_opt(2026, 11, 30);
        let mut newer = attempt("newer", 200, true, false, false);
        newer.feed_start_date = NaiveDate::from_ymd_opt(2026, 12, 1);

        let (retained, removed) =
            single_ingest_retention(&[older, newer]).expect("one successful ingest");
        assert_eq!(retained.attempt_id, "newer");
        assert_eq!(retained.start_date, NaiveDate::from_ymd_opt(2026, 12, 1));
        assert_eq!(removed, vec!["older"]);
    }

    #[test]
    fn single_ingest_preserves_active_and_last_successful_ingests() {
        let older = attempt("older", 100, true, false, false);
        let latest_success = attempt("success", 200, true, false, false);
        let failed = attempt("failed", 300, false, false, false);
        let in_progress = attempt("processing", 400, false, true, false);
        let already_deleted = attempt("deleted", 50, true, false, true);

        let (retained, removed) =
            single_ingest_retention(&[older, latest_success, failed, in_progress, already_deleted])
                .expect("one successful ingest");

        assert_eq!(retained.attempt_id, "success");
        assert_eq!(removed, vec!["deleted", "failed", "older"]);
    }

    #[test]
    fn retention_policy_is_limited_to_the_two_requested_feeds() {
        assert!(retain_only_latest_ingest(SINGLE_INGEST_FEEDS[0]));
        assert!(retain_only_latest_ingest("f-networkrail"));
        assert!(!retain_only_latest_ingest("f-u0-switzerland"));
    }

    #[test]
    fn single_ingest_does_not_prune_without_successful_ingest() {
        assert!(
            single_ingest_retention(&[
                attempt("failed", 200, false, false, false),
                attempt("processing", 300, false, true, false),
            ])
            .is_none()
        );
    }
    fn england_attempts() -> Vec<catenary::models::IngestedStatic> {
        let mut october = attempt("october", 100, true, false, false);
        october.feed_start_date = NaiveDate::from_ymd_opt(2026, 10, 5);
        october.feed_expiration_date = NaiveDate::from_ymd_opt(2027, 12, 1);
        let mut november = attempt("november", 200, true, false, false);
        november.feed_start_date = NaiveDate::from_ymd_opt(2026, 11, 16);
        november.feed_expiration_date = NaiveDate::from_ymd_opt(2027, 12, 27);
        vec![october, november]
    }

    #[test]
    fn standalone_cleanup_retires_england_october_after_grace_period() {
        let attempts = england_attempts();
        let feed = "f-bus~dft~gov~uk~england";
        for day in [15, 16, 20] {
            let plan = successful_cleanup_plan(
                feed,
                &attempts,
                NaiveDate::from_ymd_opt(2026, 11, day).unwrap(),
            )
            .unwrap();
            assert!(plan.obsolete_attempts.is_empty());
            assert_eq!(
                plan.current_attempt.as_deref(),
                Some(if day == 15 { "october" } else { "november" })
            );
        }
        let plan = successful_cleanup_plan(
            feed,
            &attempts,
            NaiveDate::from_ymd_opt(2026, 11, 26).unwrap(),
        )
        .unwrap();
        assert_eq!(plan.obsolete_attempts, vec!["october"]);
        assert_eq!(plan.current_attempt.as_deref(), Some("november"));
    }

    #[test]
    fn standalone_cleanup_does_not_remove_failed_or_active_ingests() {
        let mut attempts = england_attempts();
        let now = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        let feed = "f-bus~dft~gov~uk~england";
        attempts.push(attempt("failed", 300, false, false, false));
        let plan = successful_cleanup_plan(feed, &attempts, now).unwrap();
        assert_eq!(plan.obsolete_attempts, vec!["october"]);
        attempts.push(attempt("active", 400, false, true, false));
        assert!(successful_cleanup_plan(feed, &attempts, now).is_none());
        attempts.pop();
        let mut pending = attempt("pending", 500, true, false, false);
        pending.production = false;
        attempts.push(pending);
        assert!(successful_cleanup_plan(feed, &attempts, now).is_none());
    }

    #[test]
    fn standalone_cleanup_never_removes_last_successful_attempt() {
        let attempts = england_attempts();
        let now = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        let plan =
            successful_cleanup_plan("f-bus~dft~gov~uk~england", &attempts[1..], now).unwrap();
        assert!(plan.obsolete_attempts.is_empty());
        assert_eq!(plan.current_attempt.as_deref(), Some("november"));
    }

    #[test]
    fn standalone_cleanup_preserves_single_ingest_policy() {
        let attempts = england_attempts();
        let now = NaiveDate::from_ymd_opt(2026, 11, 16).unwrap();
        let plan = successful_cleanup_plan("f-networkrail", &attempts, now).unwrap();
        assert_eq!(plan.obsolete_attempts, vec!["october"]);
        assert_eq!(plan.current_attempt.as_deref(), Some("november"));
    }
}
