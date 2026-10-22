// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

//! Persist directed, source-scoped GTFS transfers.txt rules for Gentian.
//! Retain route/trip selectors for future fully stateful transfer routing.
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::sql_types::Text;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use std::{error::Error, path::Path, sync::Arc};

#[derive(Deserialize)]
struct RawTransfer {
    #[serde(default)]
    from_stop_id: String,
    #[serde(default)]
    to_stop_id: String,
    #[serde(default)]
    transfer_type: Option<i16>,
    #[serde(default)]
    min_transfer_time: Option<i32>,
    #[serde(default)]
    from_route_id: Option<String>,
    #[serde(default)]
    to_route_id: Option<String>,
    #[serde(default)]
    from_trip_id: Option<String>,
    #[serde(default)]
    to_trip_id: Option<String>,
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|s| (!s.trim().is_empty()).then(|| s.trim().to_owned()))
}

async fn insert_batch(
    conn: &mut diesel_async::AsyncPgConnection,
    feed: &str,
    attempt: &str,
    records: &[serde_json::Value],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let payload = serde_json::to_string(records)?;
    diesel::sql_query(
        "INSERT INTO gtfs.transfers
            (onestop_feed_id,attempt_id,from_stop_id,to_stop_id,transfer_type,
             min_transfer_seconds,from_route_id,to_route_id,from_trip_id,to_trip_id)
         SELECT $1,$2,r.from_stop_id,r.to_stop_id,r.transfer_type,
                r.min_transfer_seconds,r.from_route_id,r.to_route_id,
                r.from_trip_id,r.to_trip_id
         FROM jsonb_to_recordset($3::jsonb) AS r(
           from_stop_id text,to_stop_id text,transfer_type smallint,
           min_transfer_seconds integer,from_route_id text,to_route_id text,
           from_trip_id text,to_trip_id text)",
    )
    .bind::<Text, _>(feed)
    .bind::<Text, _>(attempt)
    .bind::<Text, _>(payload)
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn transfers_into_postgres(
    gtfs_dir: &Path,
    feed_id: &str,
    attempt_id: &str,
    pool: Arc<CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut conn = pool.get().await?;
    // Preserve idempotency for an attempted feed import.
    diesel::sql_query(
        "DELETE FROM gtfs.transfers WHERE onestop_feed_id=$1 AND attempt_id=$2",
    )
    .bind::<Text, _>(feed_id)
    .bind::<Text, _>(attempt_id)
    .execute(&mut conn)
    .await?;

    let filename = gtfs_dir.join("transfers.txt");
    if !filename.exists() {
        return Ok(());
    }
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_path(&filename)?;
    let mut chunk = Vec::<serde_json::Value>::with_capacity(1000);
    let mut inserted = 0usize;
    for row in rdr.deserialize::<RawTransfer>() {
        let rule = row?;
        let kind = rule.transfer_type.unwrap_or(0);
        if !(0..=5).contains(&kind) {
            return Err(format!("{}: invalid transfer_type {kind}", filename.display()).into());
        }
        if rule.min_transfer_time.is_some_and(|seconds| seconds < 0) {
            return Err(format!("{}: negative min_transfer_time", filename.display()).into());
        }
        if kind == 2 && rule.min_transfer_time.is_none() {
            return Err(format!("{}: transfer_type=2 requires min_transfer_time", filename.display()).into());
        }
        chunk.push(serde_json::json!({
            "from_stop_id": rule.from_stop_id.trim(),
            "to_stop_id": rule.to_stop_id.trim(),
            "transfer_type": kind,
            "min_transfer_seconds": rule.min_transfer_time,
            "from_route_id": nonempty(rule.from_route_id),
            "to_route_id": nonempty(rule.to_route_id),
            "from_trip_id": nonempty(rule.from_trip_id),
            "to_trip_id": nonempty(rule.to_trip_id)
        }));
        if chunk.len() == 1000 {
            insert_batch(&mut conn, feed_id, attempt_id, &chunk).await?;
            inserted += chunk.len();
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        insert_batch(&mut conn, feed_id, attempt_id, &chunk).await?;
        inserted += chunk.len();
    }
    println!("Persisted {inserted} GTFS transfer rules from {}", filename.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn trims_optional_scope_fields() {
        assert_eq!(nonempty(Some(" ".into())), None);
        assert_eq!(nonempty(Some(" A ".into())), Some("A".into()));
    }
}
