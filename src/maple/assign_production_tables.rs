// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::naive::NaiveDate;
use chrono::offset::Utc;
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

    let now: NaiveDate = Utc::now().naive_utc().date();
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

    let all_feeds: Vec<catenary::models::IngestedStatic> = ingested_static
        .filter(ingested_static_columns::onestop_feed_id.eq(feed_id))
        .select(catenary::models::IngestedStatic::as_select())
        .load::<catenary::models::IngestedStatic>(conn)
        .await?;

    //filter only successful ingests
    let mut sorted_feeds: Vec<catenary::models::IngestedStatic> = all_feeds
        .clone()
        .into_iter()
        .filter(|ingested| ingested.ingestion_successfully_finished && !ingested.deleted)
        .collect();

    //sort all feeds by ingest_start_unix_time_ms

    sorted_feeds.sort_by(|a, b| {
        a.ingest_start_unix_time_ms
            .cmp(&b.ingest_start_unix_time_ms)
    });

    let mut feed_time_ranges: Vec<FeedTimeRange> = Vec::new();

    let mut drop_attempt_list: Vec<String> = Vec::new();

    //drop feeds older than 1 day and are not successfully ingested

    for to_drop_feed in all_feeds.clone().into_iter().filter(|ingested| {
        !ingested.ingestion_successfully_finished
            && ingested.ingest_start_unix_time_ms < (now_ms as i64 - (1000 * 86400))
    }) {
        drop_attempt_list.push(to_drop_feed.attempt_id);
    }

    let mut last_claimed_start_time: Option<Option<NaiveDate>> = None;

    // go from latest ingest feed to earliest ingest feed
    for (i, ingested_item) in sorted_feeds.into_iter().rev().enumerate() {
        //i = 0 is latest, followed by earlier and earlier data
        match last_claimed_start_time {
            None => {
                last_claimed_start_time = Some(ingested_item.feed_start_date);

                feed_time_ranges.push(FeedTimeRange {
                    attempt_id: ingested_item.attempt_id.clone(),
                    start_date: ingested_item.feed_start_date,
                    expiration_date: ingested_item.feed_expiration_date,
                });
            }
            Some(last_claimed_start_time_prev) => {
                //if the last claimed time is none, drop this attempt because the newer feed claims all timestamp_millis

                match last_claimed_start_time_prev {
                    None => {
                        drop_attempt_list.push(ingested_item.attempt_id.clone());
                    }
                    Some(last_claimed_start_time_prev) => {
                        // calculate new expiration date

                        //does this feed have an expiration date
                        let new_expiration_date = match ingested_item.feed_expiration_date {
                            Some(this_feed_expiration) => last_claimed_start_time_prev
                                .sub(chrono::Days::new(1))
                                .min(this_feed_expiration),
                            // look at the previous feed's start date and subtract 1 as the expiration date
                            None => last_claimed_start_time_prev.sub(chrono::Days::new(1)),
                        };

                        // if the new expiration date is more than 5 days ago, then drop it

                        if new_expiration_date < now.sub(chrono::Days::new(5)) {
                            //drop the feed
                            drop_attempt_list.push(ingested_item.attempt_id.clone());
                        } else {
                            // add to the feed time range, claim the time range
                            last_claimed_start_time = Some(ingested_item.feed_start_date);

                            feed_time_ranges.push(FeedTimeRange {
                                attempt_id: ingested_item.attempt_id.clone(),
                                start_date: ingested_item.feed_start_date,
                                expiration_date: ingested_item.feed_expiration_date,
                            });
                        }
                    }
                }
            }
        }
    }

    //prepare data for the commit
    let drop_attempt_list_transaction = drop_attempt_list.clone();
    // transactions to mark as successful
    let production_list_ids: Vec<String> = feed_time_ranges
        .iter()
        .map(|feed_time_range| feed_time_range.attempt_id.clone())
        .collect();

    let now: NaiveDate = Utc::now().naive_utc().date();

    let current_feed_id = match production_list_ids.len() {
        0 => None,
        1 => Some(production_list_ids[0].clone()),
        _ => {
            let mut valid = production_list_ids[0].clone();

            for (i, feed_time_range) in feed_time_ranges.iter().enumerate() {
                let within_start_bounds = match feed_time_range.start_date {
                    None => true,
                    Some(feed_time_range_start_date) => now >= feed_time_range_start_date,
                };

                let within_end_bounds = match feed_time_range.expiration_date {
                    None => true,
                    Some(feed_time_range_start_date) => now <= feed_time_range_start_date,
                };

                if within_start_bounds && within_end_bounds {
                    valid = feed_time_range.attempt_id.clone();
                }
            }

            Some(valid)
        }
    };

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

                //determine which one is active for map queries

                if let Some(current_feed_id) = current_feed_id {
                    for production_list_id in production_list_ids {
                        let is_this_feed_spatial_queriable = current_feed_id == production_list_id;

                        //update the shapes to be queriable
                        let _ = diesel::update(
                            shapes
                                .filter(shapes_columns::onestop_feed_id.eq(&feed_id))
                                .filter(shapes_columns::attempt_id.eq(&production_list_id)),
                        )
                        .set(
                            shapes_columns::allowed_spatial_query
                                .eq(is_this_feed_spatial_queriable),
                        )
                        .execute(conn)
                        .await?;

                        //update the stops to be queriable
                        let _ = diesel::update(
                            stops
                                .filter(stops_columns::onestop_feed_id.eq(&feed_id))
                                .filter(stops_columns::attempt_id.eq(&production_list_id)),
                        )
                        .set(
                            stops_columns::allowed_spatial_query.eq(is_this_feed_spatial_queriable),
                        )
                        .execute(conn)
                        .await?;

                        let _ = diesel::update(
                            ingested_static
                                .filter(ingested_static_columns::onestop_feed_id.eq(&feed_id))
                                .filter(
                                    ingested_static_columns::attempt_id.eq(&production_list_id),
                                ),
                        )
                        .set((
                            ingested_static_columns::deleted.eq(false),
                            ingested_static_columns::production.eq(true),
                        ))
                        .execute(conn)
                        .await?;
                    }
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
        let _ = delete_attempt_objects(feed_id, drop_attempt_id, Arc::clone(&arc_conn_pool)).await;
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
