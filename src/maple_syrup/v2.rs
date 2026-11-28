//! Additive Maple Syrup v2 pilot. All identities and non-timing semantics
//! originate from the existing reducer; do not independently normalise GTFS.
use super::{ItineraryCover, ResponseFromReduce, StopDifference, pattern_id::encode_pattern_id};
use anyhow::{Context, Result, ensure};
use diesel::sql_types::{Jsonb, Text};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    feeds: Vec<String>,
}

fn matches_feed(rule: &str, feed: &str) -> bool {
    match rule.strip_suffix('*') {
        Some(prefix) => !prefix.is_empty() && feed.starts_with(prefix),
        None => !rule.is_empty() && rule == feed,
    }
}

const DEFAULT_CONFIG_PATH: &str = "maple_syrup_v2.toml";

fn config_path(override_path: Option<std::ffi::OsString>) -> std::path::PathBuf {
    override_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(DEFAULT_CONFIG_PATH))
}

pub fn enabled_for(feed: &str) -> Result<bool> {
    let path = config_path(std::env::var_os("MAPLE_SYRUP_V2_CONFIG"));
    enabled_for_path(feed, &path)
}

fn enabled_for_path(feed: &str, path: &std::path::Path) -> Result<bool> {
    // Read on each feed selection so edits apply to subsequent ingest attempts.
    // Missing or invalid configuration must not silently select v1.
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read Maple Syrup v2 config at {}", path.display()))?;
    let config: Config = toml::from_str(&text)
        .with_context(|| format!("parse Maple Syrup v2 config at {}", path.display()))?;
    ensure!(
        config.feeds.iter().all(|r| {
            let prefix = r.strip_suffix('*').unwrap_or(r);
            !prefix.is_empty() && !prefix.contains('*')
        }),
        "v2 feed rules in {} must be exact IDs or nonempty prefixes followed by *",
        path.display()
    );
    Ok(config.feeds.iter().any(|r| matches_feed(r, feed)))
}

type Times = (Option<i32>, Option<i32>, Option<i32>);

fn times(s: &StopDifference) -> Times {
    (
        s.arrival_time_since_start,
        s.departure_time_since_start,
        s.interpolated_time_since_start,
    )
}

fn structural_key(itinerary: &ItineraryCover) -> ItineraryCover {
    let mut key = itinerary.clone();
    for s in &mut key.stop_sequences {
        s.arrival_time_since_start = None;
        s.departure_time_since_start = None;
        s.interpolated_time_since_start = None;
    }
    key
}

fn primary(t: Times) -> u32 {
    u32::try_from(t.0.or(t.1).or(t.2).unwrap_or(0)).unwrap_or(0)
}

#[derive(Default)]
struct Exceptions {
    indices: Vec<i32>,
    arrivals: Vec<Option<i32>>,
    departures: Vec<Option<i32>>,
    interpolated: Vec<Option<i32>>,
}

impl Exceptions {
    fn push(&mut self, idx: usize, t: Times) -> Result<()> {
        let p = primary(t) as i32;
        if t != (Some(p), Some(p), None) {
            self.indices.push(i32::try_from(idx)?);
            self.arrivals.push(t.0);
            self.departures.push(t.1);
            self.interpolated.push(t.2);
        }
        Ok(())
    }
    fn add_to(&self, row: &mut Value) {
        row["exception_indices"] = json!(self.indices);
        row["exception_arrivals"] = json!(self.arrivals);
        row["exception_departures"] = json!(self.departures);
        row["exception_interpolated"] = json!(self.interpolated);
    }
}

fn fixed(values: &[u32]) -> (i32, Vec<u8>) {
    if values.iter().all(|&v| v <= u16::MAX as u32) {
        (
            2,
            values
                .iter()
                .flat_map(|&v| (v as u16).to_le_bytes())
                .collect(),
        )
    } else {
        (4, values.iter().flat_map(|&v| v.to_le_bytes()).collect())
    }
}

fn gcd(mut a: u32, mut b: u32) -> u32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

fn column(values: &[u32]) -> (u32, u32, u32, Vec<u8>) {
    let base = values.iter().copied().min().unwrap_or(0);
    let unit = values.iter().fold(0, |g, &v| gcd(g, v - base)).max(1);
    let max = values.iter().copied().max().unwrap_or(base);
    let width = 32 - ((max - base) / unit).leading_zeros();
    let mut data = vec![0u8; (values.len() * width as usize).div_ceil(8)];
    for (i, &v) in values.iter().enumerate() {
        let scaled = (v - base) / unit;
        for bit in 0..width as usize {
            let pos = i * width as usize + bit;
            data[pos / 8] |= (((scaled >> bit) & 1) as u8) << (pos % 8);
        }
    }
    (base, unit, width, data)
}

fn bytea(bytes: &[u8]) -> String {
    format!("\\x{}", hex::encode(bytes))
}

// Static SQL only; values are bound as JSONB. jsonb_to_recordset converts
// nullable integer arrays and hex bytea strings to native PostgreSQL types.
const STRUCTURES: &str = "INSERT INTO gtfs.maple_v2_structure
(onestop_feed_id,attempt_id,structure_id,stop_count,timing_count)
SELECT $1,$2,x.* FROM jsonb_to_recordset($3) AS x(
 structure_id text,stop_count integer,timing_count integer)";
const TRIP_MAJOR: &str = "INSERT INTO gtfs.maple_v2_trip_major
(onestop_feed_id,attempt_id,structure_id,timing_index,itinerary_pattern_id,byte_width,payload,
 exception_indices,exception_arrivals,exception_departures,exception_interpolated)
SELECT $1,$2,x.* FROM jsonb_to_recordset($3) AS x(
 structure_id text,timing_index integer,itinerary_pattern_id text,byte_width integer,payload bytea,
 exception_indices integer[],exception_arrivals integer[],exception_departures integer[],
 exception_interpolated integer[])";
const CREATE_STAGE: &str = "CREATE TEMP TABLE maple_v2_unified_stage
    (LIKE gtfs.maple_v2_stop_unified INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
    ON COMMIT DROP";
const STOP_STAGE: &str = "INSERT INTO pg_temp.maple_v2_unified_stage
(onestop_feed_id,attempt_id,structure_id,stop_index,base,unit,bit_width,payload,
 exception_indices,exception_arrivals,exception_departures,exception_interpolated,
 min_departure_offset,max_departure_offset,stop_id,gtfs_stop_sequence,metadata)
SELECT $1,$2,x.* FROM jsonb_to_recordset($3) AS x(
 structure_id text,stop_index integer,base bigint,unit bigint,bit_width integer,payload bytea,
 exception_indices integer[],exception_arrivals integer[],exception_departures integer[],
 exception_interpolated integer[],min_departure_offset integer,max_departure_offset integer,
 stop_id text,gtfs_stop_sequence bigint,metadata jsonb)";

async fn insert(
    conn: &mut AsyncPgConnection,
    feed: &str,
    attempt: &str,
    sql: &str,
    rows: &mut Vec<Value>,
) -> Result<()> {
    for batch in rows.chunks(512) {
        diesel::sql_query(sql)
            .bind::<Text, _>(feed)
            .bind::<Text, _>(attempt)
            .bind::<Jsonb, _>(Value::Array(batch.to_vec()))
            .execute(conn)
            .await?;
    }
    rows.clear();
    Ok(())
}

/// Called after the legacy metadata is inserted, within one transaction.
/// The representative legacy itinerary is the structural ID, avoiding a
/// separate ID allocator and ensuring existing cleanup cascades to v2.
pub async fn persist(
    conn: &mut AsyncPgConnection,
    feed: &str,
    attempt: &str,
    reduction: &ResponseFromReduce,
) -> Result<()> {
    // Persist is called inside the ingest transaction. Stage in bounded
    // batches, then publish in stop-ID order without a feed-sized Rust buffer.
    diesel::sql_query(CREATE_STAGE).execute(conn).await?;
    let mut groups: HashMap<ItineraryCover, Vec<u64>> = HashMap::new();
    for (&id, itinerary) in &reduction.itineraries {
        groups
            .entry(structural_key(itinerary))
            .or_default()
            .push(id);
    }
    let mut structures = Vec::new();
    let mut trips = Vec::new();
    let mut columns = Vec::new();
    for (structure, mut ids) in groups {
        ids.sort_unstable();
        let id = encode_pattern_id(ids[0]);
        let stop_count = i32::try_from(structure.stop_sequences.len())?;
        let timing_count = i32::try_from(ids.len())?;
        structures
            .push(json!({"structure_id":id,"stop_count":stop_count,"timing_count":timing_count}));
        for (idx, &itinerary_id) in ids.iter().enumerate() {
            let itinerary = &reduction.itineraries[&itinerary_id];
            let mut ex = Exceptions::default();
            let mut values = Vec::with_capacity(stop_count as usize);
            for (i, s) in itinerary.stop_sequences.iter().enumerate() {
                let t = times(s);
                ex.push(i, t)?;
                values.push(primary(t));
            }
            let (width, payload) = fixed(&values);
            let mut row = json!({"structure_id":id,"timing_index":idx,
                "itinerary_pattern_id":encode_pattern_id(itinerary_id),
                "byte_width":width,"payload":bytea(&payload)});
            ex.add_to(&mut row);
            trips.push(row);
        }
        for (idx, s) in structure.stop_sequences.iter().enumerate() {
            let mut metadata = serde_json::to_value(s)?;
            metadata["stop_headsign_idx"] = json!(s.stop_headsign.as_ref().and_then(|headsign| {
                structure
                    .stop_headsigns_unique_list
                    .as_ref()
                    .and_then(|list| {
                        list.iter()
                            .position(|value| value == headsign)
                            .map(|i| i as i16)
                    })
            }));
            let mut ex = Exceptions::default();
            let mut values = Vec::with_capacity(ids.len());
            let mut min = None::<i32>;
            let mut max = None::<i32>;
            for (i, itinerary_id) in ids.iter().enumerate() {
                let t = times(&reduction.itineraries[itinerary_id].stop_sequences[idx]);
                ex.push(i, t)?;
                values.push(primary(t));
                if let Some(dep) = t.1.or(t.0).or(t.2) {
                    min = Some(min.map_or(dep, |v| v.min(dep)));
                    max = Some(max.map_or(dep, |v| v.max(dep)));
                }
            }
            let (base, unit, width, payload) = column(&values);
            let mut row = json!({"structure_id":id,"stop_index":idx,"base":base,
                "unit":unit,"bit_width":width,"payload":bytea(&payload),
                "min_departure_offset":min,"max_departure_offset":max,
                "stop_id":s.stop_id,"gtfs_stop_sequence":s.gtfs_stop_sequence,
                "metadata":metadata});
            ex.add_to(&mut row);
            columns.push(row);
        }
        if structures.len() >= 64 || trips.len() >= 2048 || columns.len() >= 2048 {
            insert(conn, feed, attempt, STRUCTURES, &mut structures).await?;
            insert(conn, feed, attempt, TRIP_MAJOR, &mut trips).await?;
            insert(conn, feed, attempt, STOP_STAGE, &mut columns).await?;
        }
    }
    insert(conn, feed, attempt, STRUCTURES, &mut structures).await?;
    insert(conn, feed, attempt, TRIP_MAJOR, &mut trips).await?;
    insert(conn, feed, attempt, STOP_STAGE, &mut columns).await?;
    // The database publication interface mirrors the old layout for rollback
    // during cutover, then writes the unified rows in global stop-ID order.
    diesel::sql_query("SELECT gtfs.maple_v2_publish_unified($1, $2)")
        .bind::<Text, _>(feed)
        .bind::<Text, _>(attempt)
        .execute(conn)
        .await?;
    diesel::sql_query("DROP TABLE pg_temp.maple_v2_unified_stage")
        .execute(conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn allowlist_is_exact_or_prefix() {
        assert!(matches_feed(
            "f-bus~dft~gov~uk*",
            "f-bus~dft~gov~uk~greater~london"
        ));
        assert!(matches_feed("f-networkrail", "f-networkrail"));
        assert!(!matches_feed("f-networkrail", "f-networkrail~rt"));
        assert!(!matches_feed("*", "anything"));
        assert!(!matches_feed("f-bus~dft~gov~uk*", "f-other"));
    }
    #[test]
    fn runtime_config_path_defaults_to_working_directory_and_honors_override() {
        assert_eq!(
            config_path(None),
            std::path::PathBuf::from("maple_syrup_v2.toml")
        );
        assert_eq!(
            config_path(Some("custom/allowlist.toml".into())),
            std::path::PathBuf::from("custom/allowlist.toml")
        );
    }

    #[test]
    fn runtime_config_edits_are_visible_without_recompiling() {
        let path = std::env::temp_dir().join(format!(
            "maple-syrup-v2-{}.toml",
            uuid::Uuid::new_v4()
        ));
        let feed = "f-9q5-metro~losangeles";
        std::fs::write(&path, "feeds = [\"f-9q5-metro~losangeles~rail\"]").unwrap();
        let before = enabled_for_path(feed, &path);
        std::fs::write(&path, "feeds = [\"f-9q5-metro~losangeles\"]").unwrap();
        let after = enabled_for_path(feed, &path);
        std::fs::write(&path, "feeds = [\"*\"]").unwrap();
        let invalid_rule = enabled_for_path(feed, &path);
        std::fs::write(&path, "not valid TOML").unwrap();
        let invalid_toml = enabled_for_path(feed, &path);
        std::fs::remove_file(&path).unwrap();
        let missing = enabled_for_path(feed, &path);

        assert!(!before.unwrap());
        assert!(after.unwrap());
        assert!(invalid_rule.is_err());
        assert!(invalid_toml.is_err());
        let error = missing.unwrap_err().to_string();
        assert!(error.contains("read Maple Syrup v2 config"));
        assert!(error.contains(&path.display().to_string()));
    }

    #[test]
    fn column_roundtrip() {
        for values in [
            vec![0, 0],
            vec![300, 420, 330, 360, 390, 300],
            vec![0, 1, 65536, i32::MAX as u32],
            vec![42],
        ] {
            let (base, unit, width, data) = column(&values);
            for (i, &expected) in values.iter().enumerate() {
                let mut v = 0u32;
                for bit in 0..width as usize {
                    let pos = i * width as usize + bit;
                    v |= (((data[pos / 8] >> (pos % 8)) & 1) as u32) << bit;
                }
                assert_eq!(base + unit * v, expected);
            }
        }
    }
    #[test]
    fn fixed_and_exceptions() {
        assert_eq!(fixed(&[0, 65535]), (2, vec![0, 0, 255, 255]));
        assert_eq!(fixed(&[65536]), (4, vec![0, 0, 1, 0]));
        let mut e = Exceptions::default();
        e.push(0, (Some(10), Some(10), None)).unwrap();
        e.push(1, (Some(20), Some(30), None)).unwrap();
        e.push(2, (None, None, Some(40))).unwrap();
        e.push(3, (Some(-1), None, None)).unwrap();
        assert_eq!(e.indices, vec![1, 2, 3]);
        assert_eq!(e.arrivals, vec![Some(20), None, Some(-1)]);
        assert_eq!(e.departures, vec![Some(30), None, None]);
        assert_eq!(e.interpolated, vec![None, Some(40), None]);
    }
}
