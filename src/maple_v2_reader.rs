//! Native Maple Syrup v2 readers. Fetch each packed payload once per selected
//! stop column / timing variant; decode in Rust, not in compatibility views.
use crate::models::{ItineraryPatternMeta, ItineraryPatternRow, MapleV2Stop, MapleV2StopColumn, MapleV2StopMajor, MapleV2TimingMapping, MapleV2TripMajor};
use diesel::{QueryResult, prelude::*, sql_types::*};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::Deserialize;
use std::collections::HashSet;

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


// Anchor on indexed stop IDs before touching payloads. Filter metadata and
// route phase policy inside the lateral mapping lookup. Do not select t.payload
// (trip-major) on a stop read, and do not read im.trip_ids.
const STOP_SQL: &str = r#"
WITH selected AS MATERIALIZED (
    SELECT s.* FROM gtfs.maple_v2_stop s
    WHERE s.stop_id = ANY($2)
), mappings AS MATERIALIZED (
    SELECT k.onestop_feed_id, k.attempt_id, k.structure_id, a.mappings
    FROM (SELECT DISTINCT onestop_feed_id, attempt_id, structure_id FROM selected) k
    CROSS JOIN LATERAL (
        SELECT jsonb_agg(jsonb_build_object('timing_index', t.timing_index,
            'metadata', jsonb_build_object(
                'onestop_feed_id', im.onestop_feed_id, 'attempt_id', im.attempt_id,
                'itinerary_pattern_id', im.itinerary_pattern_id, 'chateau', im.chateau,
                'trip_ids', '[]'::jsonb, 'trip_headsign', im.trip_headsign,
                'trip_headsign_translations', im.trip_headsign_translations,
                'shape_id', im.shape_id, 'timezone', im.timezone, 'route_id', im.route_id,
                'direction_pattern_id', im.direction_pattern_id, 'row_count', im.row_count
            ))) AS mappings
        FROM gtfs.maple_v2_trip_major t
        JOIN gtfs.itinerary_pattern_meta im USING (onestop_feed_id, attempt_id, itinerary_pattern_id)
        WHERE t.onestop_feed_id = k.onestop_feed_id AND t.attempt_id = k.attempt_id
          AND t.structure_id = k.structure_id AND im.chateau = $1
          AND ($3::text[] IS NULL OR im.itinerary_pattern_id = ANY($3))
          AND ($4::text[] IS NULL OR im.direction_pattern_id = ANY($4))
          AND ($5::text[] IS NULL OR im.route_id = ANY($5))
          AND (NOT $7 OR EXISTS (
              SELECT 1 FROM gtfs.routes r
              WHERE r.onestop_feed_id = im.onestop_feed_id AND r.attempt_id = im.attempt_id
                AND r.route_id = im.route_id AND r.chateau = $1 AND r.route_type <> 200
                AND (r.route_type <> ALL(ARRAY[0,1,3]::smallint[]) OR r.route_id <> ALL($6))
          ))
    ) a
    WHERE a.mappings IS NOT NULL
)
SELECT c.*, s.stop_id, s.gtfs_stop_sequence, s.metadata, m.mappings
FROM selected s
JOIN mappings m USING (onestop_feed_id, attempt_id, structure_id)
JOIN gtfs.maple_v2_stop_major c USING (onestop_feed_id, attempt_id, structure_id, stop_index)
"#;

pub async fn load_v2_stops(
    conn: &mut AsyncPgConnection, chateau: &str, stops: &[String],
    patterns: Option<&[String]>, directions: Option<&[String]>, routes: Option<&[String]>,
    excluded_local: &[String], nearby: bool,
) -> QueryResult<Vec<(ItineraryPatternRow, ItineraryPatternMeta)>> {
    if stops.is_empty() { return Ok(Vec::new()); }
    let columns = diesel::sql_query(STOP_SQL)
        .bind::<Text,_>(chateau).bind::<Array<Text>,_>(stops)
        .bind::<Nullable<Array<Text>>,_>(patterns)
        .bind::<Nullable<Array<Text>>,_>(directions)
        .bind::<Nullable<Array<Text>>,_>(routes)
        .bind::<Array<Text>,_>(excluded_local).bind::<Bool,_>(nearby)
        .load::<MapleV2StopColumn>(conn).await?;
    let mut out = Vec::new();
    for c in columns {
        let stop = Stop::try_from(c.stop)?;
        let mappings: Vec<MapleV2TimingMapping> = from_json(c.mappings)?;
        let timing = c.timing;
        let exceptions = Exceptions::from(&timing);
        for m in mappings {
            let times = exceptions.times(m.timing_index, || column(&timing.payload, timing.base, timing.unit, timing.bit_width, m.timing_index))?;
            let row = make_row(&timing.onestop_feed_id, &timing.attempt_id, &m.metadata.itinerary_pattern_id, chateau, &stop, times);
            out.push((row, m.metadata));
        }
    }
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
        let data = (0x12345678u64 << 1).to_le_bytes();
        assert_eq!(column(&data,0,1,32,0).unwrap(),0x2468acf0);
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
