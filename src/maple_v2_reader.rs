//! Native Maple Syrup v2 readers. Fetch each packed payload once per selected
//! stop column / timing variant; decode in Rust, not in compatibility views.
use crate::models::{ItineraryPatternMeta, ItineraryPatternRow, MapleV2Stop, MapleV2StopColumn, MapleV2StopMajor, MapleV2TimingKey, MapleV2TripMajor};
use diesel::{QueryResult, prelude::*, sql_types::*};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

type Times = (Option<i32>, Option<i32>, Option<i32>);
type Key = (String, String, String);

fn key(row: &ItineraryPatternRow) -> Key {
    (row.onestop_feed_id.clone(), row.attempt_id.clone(), row.itinerary_pattern_id.clone())
}

/// Prefer v2 for dual-written patterns, but retain all v1-only patterns and
/// repeated stop occurrences. Never deduplicate across feed attempts.
pub fn merge<T>(mut legacy: Vec<T>, native: Vec<T>, row: impl Fn(&T) -> &ItineraryPatternRow) -> Vec<T> {
    let keys: HashSet<_> = native.iter().map(|v| key(row(v))).collect();
    legacy.retain(|v| !keys.contains(&key(row(v))));
    legacy.extend(native);
    legacy
}

fn invalid(message: &'static str) -> diesel::result::Error {
    diesel::result::Error::DeserializationError(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData, message,
    )))
}

#[derive(Debug)]
struct Exceptions<'a> {
    exception_indices: &'a [i32],
    exception_arrivals: &'a [Option<i32>],
    exception_departures: &'a [Option<i32>],
    exception_interpolated: &'a [Option<i32>],
}

macro_rules! exceptions_from {
    ($model:ty) => {
        impl<'a> From<&'a $model> for Exceptions<'a> {
            fn from(value: &'a $model) -> Self {
                Self {
                    exception_indices: &value.exception_indices,
                    exception_arrivals: &value.exception_arrivals,
                    exception_departures: &value.exception_departures,
                    exception_interpolated: &value.exception_interpolated,
                }
            }
        }
    };
}
exceptions_from!(MapleV2TripMajor);
exceptions_from!(MapleV2StopMajor);

impl Exceptions<'_> {
    fn times(&self, idx: i32, primary: impl FnOnce() -> QueryResult<i32>) -> QueryResult<Times> {
        if let Ok(pos) = self.exception_indices.binary_search(&idx) {
            let a = self.exception_arrivals.get(pos).ok_or_else(|| invalid("v2 arrival exception"))?;
            let d = self.exception_departures.get(pos).ok_or_else(|| invalid("v2 departure exception"))?;
            let i = self.exception_interpolated.get(pos).ok_or_else(|| invalid("v2 interpolation exception"))?;
            return Ok((*a, *d, *i));
        }
        let p = primary()?;
        Ok((Some(p), Some(p), None))
    }
}

fn fixed(data: &[u8], width: i32, idx: i32) -> QueryResult<i32> {
    let width = match width { 2 => 2usize, 4 => 4, _ => return Err(invalid("v2 fixed width")) };
    let start = usize::try_from(idx).ok().and_then(|i| i.checked_mul(width))
        .ok_or_else(|| invalid("v2 fixed index"))?;
    let bytes = data.get(start..start.checked_add(width).ok_or_else(|| invalid("v2 fixed range"))?)
        .ok_or_else(|| invalid("v2 fixed payload truncated"))?;
    let mut word = [0u8; 4];
    word[..width].copy_from_slice(bytes);
    i32::try_from(u32::from_le_bytes(word)).map_err(|_| invalid("v2 fixed overflow"))
}

fn column(data: &[u8], base: i64, unit: i64, width: i32, idx: i32) -> QueryResult<i32> {
    if !(0..=32).contains(&width) || idx < 0 || base < 0 || unit <= 0 {
        return Err(invalid("v2 column parameters"));
    }
    if width == 0 {
        return i32::try_from(base).map_err(|_| invalid("v2 column overflow"));
    }
    let bit = (idx as u64) * (width as u64);
    let start = usize::try_from(bit / 8).map_err(|_| invalid("v2 column index"))?;
    let shift = (bit % 8) as u32;
    let count = ((shift + width as u32 + 7) / 8) as usize;
    let bytes = data.get(start..start.checked_add(count).ok_or_else(|| invalid("v2 column range"))?)
        .ok_or_else(|| invalid("v2 column payload truncated"))?;
    let mut word = 0u64;
    for (i, &b) in bytes.iter().enumerate() { word |= (b as u64) << (8*i); }
    let scaled = ((word >> shift) & ((1u64 << width) - 1)) as i64;
    let value = unit.checked_mul(scaled).and_then(|v| base.checked_add(v))
        .ok_or_else(|| invalid("v2 column overflow"))?;
    i32::try_from(value).map_err(|_| invalid("v2 column overflow"))
}

#[derive(Deserialize)]
struct StopMetadata {
    timepoint: Option<bool>,
    stop_headsign_idx: Option<i16>,
    pickup_type: i16,
    drop_off_type: i16,
}

struct Stop {
    stop_index: i32,
    stop_id: String,
    gtfs_stop_sequence: u32,
    metadata: StopMetadata,
}

impl TryFrom<MapleV2Stop> for Stop {
    type Error = diesel::result::Error;

    fn try_from(stop: MapleV2Stop) -> QueryResult<Self> {
        Ok(Self {
            stop_index: stop.stop_index,
            stop_id: stop.stop_id,
            gtfs_stop_sequence: u32::try_from(stop.gtfs_stop_sequence)
                .map_err(|_| invalid("v2 GTFS stop sequence out of range"))?,
            metadata: from_json(stop.metadata)?,
        })
    }
}

fn make_row(feed: &str, attempt: &str, pattern: &str, chateau: &str, stop: &Stop, times: Times) -> ItineraryPatternRow {
    ItineraryPatternRow {
        onestop_feed_id: feed.into(), attempt_id: attempt.into(), itinerary_pattern_id: pattern.into(),
        stop_sequence: stop.stop_index, stop_id: stop.stop_id.as_str().into(), chateau: chateau.into(),
        gtfs_stop_sequence: stop.gtfs_stop_sequence,
        arrival_time_since_start: times.0, departure_time_since_start: times.1,
        interpolated_time_since_start: times.2, timepoint: stop.metadata.timepoint,
        stop_headsign_idx: stop.metadata.stop_headsign_idx,
        pickup_type: stop.metadata.pickup_type, drop_off_type: stop.metadata.drop_off_type,
    }
}

fn from_json<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> QueryResult<T> {
    serde_json::from_value(value).map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))
}


// Keep the index anchor and the exact payload lookup in a small statement.
// Resolve the feed/attempt from the actual stop table, not the v2 allowlist.
// Include child stops even when allowed_spatial_query is false: that flag is
// applied by the caller when selecting the station, not its children.
const STOP_SQL: &str = r#"
WITH stop_keys AS MATERIALIZED (
    SELECT DISTINCT onestop_feed_id, attempt_id, gtfs_id
    FROM gtfs.stops
    WHERE chateau = $1 AND gtfs_id = ANY($2)
), selected AS MATERIALIZED (
    SELECT s.*
    FROM stop_keys k
    CROSS JOIN LATERAL (
        SELECT s.* FROM gtfs.maple_v2_stop s
        WHERE s.stop_id = k.gtfs_id
          AND s.onestop_feed_id = k.onestop_feed_id
          AND s.attempt_id = k.attempt_id
        OFFSET 0
    ) s
)
SELECT c.*, s.stop_id, s.gtfs_stop_sequence, s.metadata
FROM selected s
CROSS JOIN LATERAL (
    SELECT c.* FROM gtfs.maple_v2_stop_major c
    WHERE c.onestop_feed_id = s.onestop_feed_id
      AND c.attempt_id = s.attempt_id
      AND c.structure_id = s.structure_id
      AND c.stop_index = s.stop_index
    OFFSET 0
) c
"#;

// Choose the representation per complete feed attempt in the same
// repeatable-read snapshot. Unbackfilled attempts and older ingesters
// must never lose stops. Only the legacy branch performs the second lookup.
const UNIFIED_STOP_SQL: &str = r#"
WITH stop_keys AS MATERIALIZED (
    SELECT DISTINCT s.onestop_feed_id, s.attempt_id, s.gtfs_id,
           COALESCE(a.ready, false) AS ready
    FROM gtfs.stops s
    LEFT JOIN gtfs.maple_v2_stop_unified_attempt a
      USING (onestop_feed_id, attempt_id)
    WHERE s.chateau = $1 AND s.gtfs_id = ANY($2)
)
SELECT u.onestop_feed_id, u.attempt_id, u.structure_id, u.stop_index,
       u.base, u.unit, u.bit_width, u.payload, u.exception_indices,
       u.exception_arrivals, u.exception_departures, u.exception_interpolated,
       u.min_departure_offset, u.max_departure_offset,
       u.stop_id, u.gtfs_stop_sequence, u.metadata
FROM (SELECT * FROM stop_keys WHERE ready) k
CROSS JOIN LATERAL (
    SELECT u.* FROM gtfs.maple_v2_stop_unified u
    WHERE u.stop_id = k.gtfs_id
      AND u.onestop_feed_id = k.onestop_feed_id
      AND u.attempt_id = k.attempt_id
    OFFSET 0
) u
UNION ALL
SELECT c.*, s.stop_id, s.gtfs_stop_sequence, s.metadata
FROM (SELECT * FROM stop_keys WHERE NOT ready) k
CROSS JOIN LATERAL (
    SELECT s.* FROM gtfs.maple_v2_stop s
    WHERE s.stop_id = k.gtfs_id
      AND s.onestop_feed_id = k.onestop_feed_id
      AND s.attempt_id = k.attempt_id
    OFFSET 0
) s
CROSS JOIN LATERAL (
    SELECT c.* FROM gtfs.maple_v2_stop_major c
    WHERE c.onestop_feed_id = s.onestop_feed_id
      AND c.attempt_id = s.attempt_id
      AND c.structure_id = s.structure_id
      AND c.stop_index = s.stop_index
    OFFSET 0
) c
"#;

fn unified_stops_enabled() -> bool {
    std::env::var("MAPLE_V2_USE_UNIFIED_STOPS")
        .is_ok_and(|value| value.eq_ignore_ascii_case("true") || value == "1")
}

// Only the mapping columns are read. The primary key prefix is
// (onestop_feed_id, attempt_id, structure_id), so each lateral lookup can
// use the same small range scan regardless of the size of the whole feed.
const TIMING_KEYS_SQL: &str = r#"
SELECT t.*
FROM unnest($1::text[], $2::text[], $3::text[])
    AS k(onestop_feed_id, attempt_id, structure_id)
CROSS JOIN LATERAL (
    SELECT t.onestop_feed_id, t.attempt_id, t.structure_id,
           t.timing_index, t.itinerary_pattern_id
    FROM gtfs.maple_v2_trip_major t
    WHERE t.onestop_feed_id = k.onestop_feed_id
      AND t.attempt_id = k.attempt_id
      AND t.structure_id = k.structure_id
    OFFSET 0
) t
"#;

// Fetch metadata once per exact pattern key. No trip_ids, no JSONB
// aggregation, no planner freedom to start from all metadata in a chateau.
// Keep the existing direction, priority and local-mode/coach filters.
const METADATA_SQL: &str = r#"
SELECT m.*
FROM unnest($1::text[], $2::text[], $3::text[])
    AS k(onestop_feed_id, attempt_id, itinerary_pattern_id)
CROSS JOIN LATERAL (
    SELECT im.onestop_feed_id, im.attempt_id, im.itinerary_pattern_id,
           im.chateau, ARRAY[]::text[] AS trip_ids, im.trip_headsign,
           im.trip_headsign_translations, im.shape_id, im.timezone,
           im.route_id, im.direction_pattern_id, im.row_count
    FROM gtfs.itinerary_pattern_meta im
    WHERE im.onestop_feed_id = k.onestop_feed_id
      AND im.attempt_id = k.attempt_id
      AND im.itinerary_pattern_id = k.itinerary_pattern_id
      AND im.chateau = $4
      AND ($5::text[] IS NULL OR im.direction_pattern_id = ANY($5))
      AND ($6::text[] IS NULL OR im.route_id = ANY($6))
      AND (NOT $8 OR EXISTS (
          SELECT 1 FROM gtfs.routes r
          WHERE r.onestop_feed_id = im.onestop_feed_id
            AND r.attempt_id = im.attempt_id
            AND r.route_id = im.route_id
            AND r.chateau = $4 AND r.route_type <> 200
            AND (r.route_type <> ALL(ARRAY[0,1,3]::smallint[]) OR r.route_id <> ALL($7))
      ))
    OFFSET 0
) m
"#;

// Nearby already knows its direction patterns. Read their metadata once per
// native stop batch using the (chateau, direction_pattern_id) index, rather
// than probing the metadata heap for each timing variant. The explicit
// boolean test keeps route membership as a hashed subplan (rather than
// pulling up a correlated EXISTS into a repeated feed-wide route scan).
// The caller intersects the result with the complete timing mapping keys
// from the same repeatable-read snapshot.
const NEARBY_DIRECTION_METADATA_SQL: &str = r#"
SELECT im.onestop_feed_id, im.attempt_id, im.itinerary_pattern_id,
       im.chateau, ARRAY[]::text[] AS trip_ids, im.trip_headsign,
       im.trip_headsign_translations, im.shape_id, im.timezone,
       im.route_id, im.direction_pattern_id, im.row_count
FROM gtfs.itinerary_pattern_meta im
WHERE im.chateau = $1
  AND im.direction_pattern_id = ANY($2::text[])
  AND ($3::text[] IS NULL OR im.route_id = ANY($3))
  AND ((im.onestop_feed_id, im.attempt_id, im.route_id) IN (
      SELECT r.onestop_feed_id, r.attempt_id, r.route_id
      FROM gtfs.routes r
      WHERE r.chateau = $1 AND r.route_type <> 200
        AND (r.route_type <> ALL(ARRAY[0,1,3]::smallint[]) OR r.route_id <> ALL($4::text[]))
  )) IS TRUE
"#;

const LOOKUP_BATCH_SIZE: usize = 512;

fn key_arrays(keys: &[Key]) -> (Vec<&str>, Vec<&str>, Vec<&str>) {
    let mut feeds = Vec::with_capacity(keys.len());
    let mut attempts = Vec::with_capacity(keys.len());
    let mut ids = Vec::with_capacity(keys.len());
    for (feed, attempt, id) in keys {
        feeds.push(feed.as_str());
        attempts.push(attempt.as_str());
        ids.push(id.as_str());
    }
    (feeds, attempts, ids)
}

fn log_stage(chateau: &str, stage: &str, started: Instant, rows: usize) {
    let elapsed_ms = started.elapsed().as_millis() as u64;
    if elapsed_ms >= 500 {
        tracing::warn!(chateau, stage, elapsed_ms, rows, "slow Maple v2 stop fetch");
    } else {
        tracing::debug!(chateau, stage, elapsed_ms, rows, "Maple v2 stop fetch");
    }
}

pub async fn load_v2_stops(
    conn: &mut AsyncPgConnection, chateau: &str, stops: &[String],
    patterns: Option<&[String]>, directions: Option<&[String]>, routes: Option<&[String]>,
    excluded_local: &[String], nearby: bool,
) -> QueryResult<Vec<(ItineraryPatternRow, ItineraryPatternMeta)>> {
    if stops.is_empty()
        || patterns.is_some_and(|p| p.is_empty())
        || directions.is_some_and(|d| d.is_empty())
        || routes.is_some_and(|r| r.is_empty())
    {
        return Ok(Vec::new());
    }

    // All three SQL stages see the same ingestion snapshot. A server-side
    // statement timeout cancels the SQL even if the client disconnects.
    // SET LOCAL is scoped to this transaction and never leaks into the pool.
    conn.build_transaction()
        .repeatable_read()
        .read_only()
        .run::<_, diesel::result::Error, _>(|conn| {
            Box::pin(async move {
                diesel::sql_query("SET LOCAL statement_timeout = '10s'")
                    .execute(conn).await?;
                // These indexed, short-lived nearby queries do not benefit
                // from compiling a fresh high-cost plan for every request.
                // Keep this setting local to the native nearby transaction.
                if nearby && directions.is_some() {
                    diesel::sql_query("SET LOCAL jit = off")
                        .execute(conn).await?;
                }
                load_v2_stops_inner(conn, chateau, stops, patterns, directions, routes, excluded_local, nearby).await
            })
        })
        .await
}

async fn load_v2_stops_inner(
    conn: &mut AsyncPgConnection, chateau: &str, stops: &[String],
    patterns: Option<&[String]>, directions: Option<&[String]>, routes: Option<&[String]>,
    excluded_local: &[String], nearby: bool,
) -> QueryResult<Vec<(ItineraryPatternRow, ItineraryPatternMeta)>> {
    let started = Instant::now();
    let unified = unified_stops_enabled();
    let columns = diesel::sql_query(if unified { UNIFIED_STOP_SQL } else { STOP_SQL })
        .bind::<Text,_>(chateau)
        .bind::<Array<Text>,_>(stops)
        .load::<MapleV2StopColumn>(conn).await?;
    log_stage(chateau, if unified { "stop_columns_unified" } else { "stop_columns" }, started, columns.len());
    let payload_bytes: usize = columns.iter().map(|c| c.timing.payload.len()).sum();
    tracing::debug!(chateau, payload_bytes, "Maple v2 stop-major packed bytes");
    if columns.is_empty() { return Ok(Vec::new()); }

    let mut structure_keys: Vec<Key> = columns.iter().map(|c| (
        c.stop.onestop_feed_id.clone(), c.stop.attempt_id.clone(), c.stop.structure_id.clone()
    )).collect::<HashSet<_>>().into_iter().collect();
    structure_keys.sort_unstable();
    let allowed_patterns = patterns.map(|p| p.iter().map(String::as_str).collect::<HashSet<_>>());
    let mut timing_keys = Vec::new();
    let started = Instant::now();
    for batch in structure_keys.chunks(LOOKUP_BATCH_SIZE) {
        let (feeds, attempts, ids) = key_arrays(batch);
        let mut rows = diesel::sql_query(TIMING_KEYS_SQL)
            .bind::<Array<Text>,_>(&feeds)
            .bind::<Array<Text>,_>(&attempts)
            .bind::<Array<Text>,_>(&ids)
            .load::<MapleV2TimingKey>(conn).await?;
        rows.retain(|t| allowed_patterns.as_ref()
            .is_none_or(|p| p.contains(t.itinerary_pattern_id.as_str())));
        timing_keys.extend(rows);
    }
    log_stage(chateau, "timing_keys", started, timing_keys.len());

    if timing_keys.is_empty() { return Ok(Vec::new()); }
    let pattern_keys: HashSet<Key> = timing_keys.iter().map(|t| (
        t.onestop_feed_id.clone(), t.attempt_id.clone(), t.itinerary_pattern_id.clone()
    )).collect();
    let mut metadata = HashMap::new();
    let started = Instant::now();
    if let Some(directions) = directions.filter(|_| nearby) {
        let rows = diesel::sql_query(NEARBY_DIRECTION_METADATA_SQL)
            .bind::<Text,_>(chateau)
            .bind::<Array<Text>,_>(directions)
            .bind::<Nullable<Array<Text>>,_>(routes)
            .bind::<Array<Text>,_>(excluded_local)
            .load::<ItineraryPatternMeta>(conn).await?;
        // A direction can contain legacy-only patterns, other feed attempts
        // or patterns that never visit this stop batch. Keep only the exact
        // keys represented by its timing mappings, not bare pattern IDs.
        for row in rows {
            let key = (row.onestop_feed_id.clone(), row.attempt_id.clone(), row.itinerary_pattern_id.clone());
            if pattern_keys.contains(&key) {
                metadata.insert(key, row);
            }
        }
    } else {
        // Stop-board, single-trip and direction-less callers retain their
        // pattern-first, bounded lookup and existing filter semantics.
        let mut pattern_keys: Vec<Key> = pattern_keys.into_iter().collect();
        pattern_keys.sort_unstable();
        for batch in pattern_keys.chunks(LOOKUP_BATCH_SIZE) {
            let (feeds, attempts, ids) = key_arrays(batch);
            let rows = diesel::sql_query(METADATA_SQL)
                .bind::<Array<Text>,_>(&feeds)
                .bind::<Array<Text>,_>(&attempts)
                .bind::<Array<Text>,_>(&ids)
                .bind::<Text,_>(chateau)
                .bind::<Nullable<Array<Text>>,_>(directions)
                .bind::<Nullable<Array<Text>>,_>(routes)
                .bind::<Array<Text>,_>(excluded_local)
                .bind::<Bool,_>(nearby)
                .load::<ItineraryPatternMeta>(conn).await?;
            for row in rows {
                metadata.insert((row.onestop_feed_id.clone(), row.attempt_id.clone(), row.itinerary_pattern_id.clone()), row);
            }
        }
    }
    log_stage(chateau, "pattern_metadata", started, metadata.len());

    // Join mappings to metadata once per structure, not once per stop.
    // Every column still uses the original timing_index, including gaps
    // created by filtering out unrelated patterns or routes.
    let started = Instant::now();
    let mut by_structure: HashMap<Key, Vec<(i32, &ItineraryPatternMeta)>> = HashMap::new();
    for t in timing_keys {
        if let Some(m) = metadata.get(&(t.onestop_feed_id.clone(), t.attempt_id.clone(), t.itinerary_pattern_id)) {
            by_structure.entry((t.onestop_feed_id, t.attempt_id, t.structure_id))
                .or_default().push((t.timing_index, m));
        }
    }
    let mut out = Vec::new();
    for c in columns {
        let timing = c.timing;
        let structure = (timing.onestop_feed_id.clone(), timing.attempt_id.clone(), timing.structure_id.clone());
        let Some(mappings) = by_structure.get(&structure) else { continue; };
        let stop = Stop::try_from(c.stop)?;
        let exceptions = Exceptions::from(&timing);
        for &(idx, m) in mappings {
            let times = exceptions.times(idx, || column(&timing.payload, timing.base, timing.unit, timing.bit_width, idx))?;
            let row = make_row(&timing.onestop_feed_id, &timing.attempt_id, &m.itinerary_pattern_id, chateau, &stop, times);
            out.push((row, m.clone()));
        }
    }
    log_stage(chateau, "decode_and_join", started, out.len());
    Ok(out)
}

pub async fn load_stop_rows(
    conn: &mut AsyncPgConnection, chateau: &str, patterns: &[String], stops: &[String],
) -> QueryResult<Vec<ItineraryPatternRow>> {
    if patterns.is_empty() || stops.is_empty() { return Ok(Vec::new()); }
    use crate::schema::gtfs::itinerary_pattern as ip;
    let legacy = ip::table
        .filter(ip::chateau.eq(chateau))
        .filter(ip::itinerary_pattern_id.eq_any(patterns))
        .filter(ip::stop_id.eq_any(stops))
        .select(ItineraryPatternRow::as_select())
        .load::<ItineraryPatternRow>(conn).await?;
    let native = load_v2_stops(conn, chateau, stops, Some(patterns), None, None, &[], false)
        .await?.into_iter().map(|(row, _)| row).collect();
    Ok(merge(legacy, native, |r| r))
}


const TRIP_SQL: &str = r#"
SELECT t.* FROM gtfs.itinerary_pattern_meta m
CROSS JOIN LATERAL (
    SELECT t.* FROM gtfs.maple_v2_trip_major t
    WHERE t.onestop_feed_id=m.onestop_feed_id AND t.attempt_id=m.attempt_id
      AND t.itinerary_pattern_id=m.itinerary_pattern_id OFFSET 0
) t
WHERE m.chateau=$1 AND m.itinerary_pattern_id=ANY($2)
  AND ($3::text IS NULL OR m.onestop_feed_id=$3)
  AND ($4::text IS NULL OR m.attempt_id=$4)
"#;

pub async fn load_trip_rows(conn: &mut AsyncPgConnection, chateau: &str, patterns: &[String]) -> QueryResult<Vec<ItineraryPatternRow>> {
    load_trip_rows_scoped(conn, chateau, patterns, None, None).await
}

pub async fn load_trip_rows_scoped(
    conn: &mut AsyncPgConnection, chateau: &str, patterns: &[String],
    feed: Option<&str>, attempt: Option<&str>,
) -> QueryResult<Vec<ItineraryPatternRow>> {
    if patterns.is_empty() { return Ok(Vec::new()); }
    use crate::schema::gtfs::itinerary_pattern as ip;
    let mut legacy_query = ip::table
        .filter(ip::chateau.eq(chateau))
        .filter(ip::itinerary_pattern_id.eq_any(patterns))
        .select(ItineraryPatternRow::as_select())
        .into_boxed::<diesel::pg::Pg>();
    if let Some(feed) = feed {
        legacy_query = legacy_query.filter(ip::onestop_feed_id.eq(feed));
    }
    if let Some(attempt) = attempt {
        legacy_query = legacy_query.filter(ip::attempt_id.eq(attempt));
    }
    let legacy = legacy_query.load::<ItineraryPatternRow>(conn).await?;
    let trips = diesel::sql_query(TRIP_SQL).bind::<Text,_>(chateau).bind::<Array<Text>,_>(patterns)
        .bind::<Nullable<Text>,_>(feed).bind::<Nullable<Text>,_>(attempt)
        .load::<MapleV2TripMajor>(conn).await?;
    let keys: HashSet<_> = trips.iter().map(|t| (t.onestop_feed_id.clone(),t.attempt_id.clone(),t.structure_id.clone())).collect();
    let (mut feeds, mut attempts, mut structures) = (Vec::new(), Vec::new(), Vec::new());
    for (f,a,s) in keys { feeds.push(f); attempts.push(a); structures.push(s); }
    let mut decoded: std::collections::HashMap<Key, Vec<Stop>> = std::collections::HashMap::new();
    if !structures.is_empty() {
        let stops = diesel::sql_query(r#"
            SELECT s.*
            FROM unnest($1::text[],$2::text[],$3::text[]) AS k(onestop_feed_id,attempt_id,structure_id)
            CROSS JOIN LATERAL (
                SELECT s.* FROM gtfs.maple_v2_stop s WHERE s.onestop_feed_id=k.onestop_feed_id
                  AND s.attempt_id=k.attempt_id AND s.structure_id=k.structure_id
                OFFSET 0
            ) s
        "#).bind::<Array<Text>,_>(&feeds).bind::<Array<Text>,_>(&attempts).bind::<Array<Text>,_>(&structures)
            .load::<MapleV2Stop>(conn).await?;
        for s in stops {
            let key = (s.onestop_feed_id.clone(), s.attempt_id.clone(), s.structure_id.clone());
            decoded.entry(key).or_default().push(Stop::try_from(s)?);
        }
    }
    let mut native = Vec::new();
    for t in trips {
        let stops = decoded.get(&(t.onestop_feed_id.clone(),t.attempt_id.clone(),t.structure_id.clone()))
            .ok_or_else(|| invalid("v2 missing structure"))?;
        let exceptions = Exceptions::from(&t);
        for stop in stops {
            let times = exceptions.times(stop.stop_index, || fixed(&t.payload,t.byte_width,stop.stop_index))?;
            native.push(make_row(&t.onestop_feed_id,&t.attempt_id,&t.itinerary_pattern_id,chateau,stop,times));
        }
    }
    let mut out = merge(legacy,native,|r| r);
    out.sort_unstable_by(|a,b| a.stop_sequence.cmp(&b.stop_sequence));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unified_query_preserves_attempt_scoped_fallback() {
        assert!(UNIFIED_STOP_SQL.contains("COALESCE(a.ready, false)"));
        assert!(UNIFIED_STOP_SQL.contains("WHERE ready"));
        assert!(UNIFIED_STOP_SQL.contains("WHERE NOT ready"));
        assert!(UNIFIED_STOP_SQL.contains("u.onestop_feed_id = k.onestop_feed_id"));
        assert!(UNIFIED_STOP_SQL.contains("u.attempt_id = k.attempt_id"));
        assert!(UNIFIED_STOP_SQL.contains("u.stop_id = k.gtfs_id"));
        assert!(UNIFIED_STOP_SQL.contains("UNION ALL"));
    }

    #[test]
    fn staged_queries_keep_composite_index_anchors() {
        assert!(STOP_SQL.contains("s.stop_id = k.gtfs_id"));
        assert!(STOP_SQL.contains("s.onestop_feed_id = k.onestop_feed_id"));
        assert!(STOP_SQL.contains("s.attempt_id = k.attempt_id"));
        assert!(STOP_SQL.contains("c.stop_index = s.stop_index"));
        assert_eq!(STOP_SQL.matches("OFFSET 0").count(), 2);
        assert!(!STOP_SQL.contains("maple_v2_trip_major"));
        assert!(!STOP_SQL.contains("jsonb_agg"));
        assert!(TIMING_KEYS_SQL.contains("t.structure_id = k.structure_id"));
        assert!(!TIMING_KEYS_SQL.contains("payload"));
        assert!(METADATA_SQL.contains("im.itinerary_pattern_id = k.itinerary_pattern_id"));
        assert!(METADATA_SQL.contains("im.attempt_id = k.attempt_id"));
        assert!(METADATA_SQL.contains("ARRAY[]::text[] AS trip_ids"));
        assert!(METADATA_SQL.contains("r.route_type <> 200"));
        assert!(!METADATA_SQL.contains("jsonb_agg"));
    }

    #[test]
    fn nearby_metadata_uses_direction_index_and_complete_route_keys() {
        assert!(NEARBY_DIRECTION_METADATA_SQL.contains("im.direction_pattern_id = ANY($2::text[])"));
        assert!(NEARBY_DIRECTION_METADATA_SQL.contains("(im.onestop_feed_id, im.attempt_id, im.route_id) IN"));
        assert!(NEARBY_DIRECTION_METADATA_SQL.contains("SELECT r.onestop_feed_id, r.attempt_id, r.route_id"));
        assert!(NEARBY_DIRECTION_METADATA_SQL.contains(")) IS TRUE"));
        assert!(NEARBY_DIRECTION_METADATA_SQL.contains("r.route_type <> 200"));
        assert!(NEARBY_DIRECTION_METADATA_SQL.contains("r.route_id <> ALL($4::text[])"));
        assert!(NEARBY_DIRECTION_METADATA_SQL.contains("im.route_id = ANY($3)"));
        assert!(NEARBY_DIRECTION_METADATA_SQL.contains("ARRAY[]::text[] AS trip_ids"));
        assert!(!NEARBY_DIRECTION_METADATA_SQL.contains("CROSS JOIN LATERAL"));
    }

    // Exercise the actual SQL constants against isolated PostgreSQL temp
    // tables. Run with MAPLE_V2_TEST_DATABASE_URL and --ignored.
    #[test]
    #[ignore = "requires MAPLE_V2_TEST_DATABASE_URL (PostgreSQL)"]
    fn nearby_direction_metadata_postgres_parity() {
        let url = std::env::var("MAPLE_V2_TEST_DATABASE_URL")
            .expect("set MAPLE_V2_TEST_DATABASE_URL");
        let mut db = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        db.batch_execute(r#"
            CREATE TEMP TABLE itinerary_pattern_meta (
                onestop_feed_id text NOT NULL, attempt_id text NOT NULL,
                itinerary_pattern_id text NOT NULL, chateau text NOT NULL,
                trip_headsign text, trip_headsign_translations jsonb,
                shape_id text, timezone text, route_id text NOT NULL,
                direction_pattern_id text, row_count integer,
                PRIMARY KEY (onestop_feed_id, attempt_id, itinerary_pattern_id)
            );
            CREATE TEMP TABLE routes (
                onestop_feed_id text NOT NULL, attempt_id text NOT NULL,
                route_id text NOT NULL, chateau text NOT NULL, route_type smallint,
                PRIMARY KEY (onestop_feed_id, attempt_id, route_id)
            );
            INSERT INTO routes VALUES
                ('f','a','rail','c',2), ('f','b','rail','c',200),
                ('g','a','rail','c',3), ('f','a','local','c',3),
                ('f','a','other','c',2), ('f','a','foreign','other-c',2);
            INSERT INTO itinerary_pattern_meta
                (onestop_feed_id,attempt_id,itinerary_pattern_id,chateau,route_id,direction_pattern_id)
            VALUES
                ('f','a','p','c','rail','d'),
                ('f','b','p','c','rail','d'),
                ('g','a','p','c','rail','d'),
                ('f','a','q','c','local','d'),
                ('f','a','z','c','other','d'),
                ('f','a','unmapped','c','rail','d'),
                ('f','a','wrong-direction','c','rail','other-d'),
                ('f','a','missing-route','c','missing','d'),
                ('f','a','foreign-route','c','foreign','d'),
                ('f','a','wrong-chateau','other-c','rail','d');
        "#).unwrap();

        let keys: Vec<Key> = vec![
            ("f".into(),"a".into(),"p".into()),
            ("f".into(),"b".into(),"p".into()),
            ("g".into(),"a".into(),"p".into()),
            ("f".into(),"a".into(),"q".into()),
            ("f".into(),"a".into(),"z".into()),
            ("f".into(),"a".into(),"wrong-direction".into()),
            ("f".into(),"a".into(),"missing-route".into()),
            ("f".into(),"a".into(),"foreign-route".into()),
            ("f".into(),"a".into(),"wrong-chateau".into()),
        ];
        let (feeds, attempts, ids) = key_arrays(&keys);
        let wanted: HashSet<_> = keys.iter().cloned().collect();
        let old_sql = METADATA_SQL.replace("gtfs.", "pg_temp.");
        let new_sql = NEARBY_DIRECTION_METADATA_SQL.replace("gtfs.", "pg_temp.");
        let directions = vec!["d".to_string()];
        let old_directions = Some(directions.clone());
        let row_key = |r: &postgres::Row| -> Key { (r.get(0), r.get(1), r.get(2)) };
        for (priority, excluded, expected) in [
            (None, vec![], vec![("f","a","p"), ("f","a","q"), ("f","a","z"), ("g","a","p")]),
            (None, vec!["local".to_string(),"rail".to_string()], vec![("f","a","p"), ("f","a","z")]),
            (Some(vec!["rail".to_string()]), vec![], vec![("f","a","p"), ("g","a","p")]),
            (Some(vec![]), vec![], vec![]),
        ] {
            let mut original: Vec<Key> = db.query(&old_sql,
                &[&feeds, &attempts, &ids, &"c", &old_directions, &priority, &excluded, &true])
                .unwrap().iter().map(row_key).collect();
            let mut optimized: Vec<Key> = db.query(&new_sql,
                &[&"c", &directions, &priority, &excluded])
                .unwrap().iter().map(row_key).filter(|k| wanted.contains(k)).collect();
            let mut expected: Vec<Key> = expected.into_iter()
                .map(|(f,a,p)| (f.into(),a.into(),p.into())).collect();
            original.sort_unstable();
            optimized.sort_unstable();
            expected.sort_unstable();
            assert_eq!(optimized, original);
            assert_eq!(optimized, expected);
        }
    }

    #[test]
    fn batched_keys_preserve_feed_attempt_identity() {
        let keys = vec![("feed1".into(), "old".into(), "same".into()),
                        ("feed2".into(), "new".into(), "same".into())];
        let (feeds, attempts, ids) = key_arrays(&keys);
        assert_eq!(feeds, vec!["feed1", "feed2"]);
        assert_eq!(attempts, vec!["old", "new"]);
        assert_eq!(ids, vec!["same", "same"]);
    }
    #[test]
    fn fixed_and_packed_boundaries() {
        assert_eq!(fixed(&[0xff,0xff,1,0],2,0).unwrap(),65535);
        assert_eq!(fixed(&[0,0,1,0],4,0).unwrap(),65536);
        assert_eq!(column(&[],123,1,0,400).unwrap(),123);
        // Three 5-bit values 3, 31, 17; last two cross byte boundaries.
        let data = (3u16 | (31<<5) | (17<<10)).to_le_bytes();
        assert_eq!(column(&data,10,2,5,0).unwrap(),16);
        assert_eq!(column(&data,10,2,5,1).unwrap(),72);
        assert_eq!(column(&data,10,2,5,2).unwrap(),44);
        assert!(column(&[],0,1,5,1).is_err());
        assert!(fixed(&[0],2,0).is_err());
        let data = [0xff, 0xff, 0xff, 0x7f, 0x78, 0x56, 0x34, 0x12];
        assert_eq!(column(&data,0,1,32,1).unwrap(),0x12345678);
    }
    #[test]
    fn nullable_signed_and_interpolated_exceptions() {
        let e = Exceptions { exception_indices: &[1,3], exception_arrivals: &[None,Some(-8)],
            exception_departures: &[None,Some(7)], exception_interpolated: &[Some(4),None] };
        assert_eq!(e.times(1, || panic!("exception must bypass primary")).unwrap(),(None,None,Some(4)));
        assert_eq!(e.times(3, || panic!("exception must bypass primary")).unwrap(),(Some(-8),Some(7),None));
        assert_eq!(e.times(2, || Ok(42)).unwrap(),(Some(42),Some(42),None));
    }
    #[test]
    fn dual_written_attempts_are_not_duplicated() {
        let stop = Stop {stop_index:0,stop_id:"x".into(),gtfs_stop_sequence:1,
            metadata:StopMetadata{timepoint:Some(true),stop_headsign_idx:None,pickup_type:0,drop_off_type:0}};
        let row = |attempt,pattern| make_row("f",attempt,pattern,"c",&stop,(Some(1),Some(1),None));
        let mut loop_row = row("new","p"); loop_row.stop_sequence = 2;
        let result = merge(vec![row("old","p"),row("new","p"),row("new","q")],
            vec![row("new","p"),loop_row], |r| r);
        assert_eq!(result.len(),4);
        assert_eq!(result.iter().filter(|r|r.attempt_id=="new" && r.itinerary_pattern_id=="p").count(),2);
    }
}
