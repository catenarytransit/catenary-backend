//! Convert an internal RAPTOR journey into a client-facing MOTIS plan response.
//! Metadata is looked up by chateau and public GTFS IDs across import attempts.
use anyhow::{Context, Result};
use catenary::routing_common::native_osr::{Endpoint as StreetEndpoint, NativeOsr, path_geometry};
use catenary::routing_common::route_manager::Coordinates;
use catenary::routing_common::timetable::LocationIdx;
use catenary::routing_common::{
    metadata_index::MetadataIndex,
    raptor_query::{Journey, Leg},
    timetable::{Clasz, RouteIdx, TransportIdx, ZeroCopyTimetable},
};
use postgres::{Client, NoTls};
use rayon::prelude::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;
use std::time::Instant;

const METADATA_SQL: &str = r#"
WITH wanted AS MATERIALIZED (
    SELECT * FROM unnest($2::text[], $3::text[], $4::text[], $5::text[])
        AS w(feed, attempt, trip, route_id)
), trip_candidates AS MATERIALIZED (
    -- Parameterized chateau + GTFS ID probes match the existing composite index.
    SELECT tc.onestop_feed_id, tc.attempt_id, tc.trip_id, tc.route_id,
           tc.trip_short_name, tc.itinerary_pattern_id, tc.direction_id,
           ti.production AND ti.ingestion_successfully_finished
               AND NOT ti.ingesting_in_progress AND NOT ti.ingestion_errored
               AND NOT ti.deleted AS usable,
           ti.ingest_end_unix_time_ms
    FROM gtfs.trips_compressed tc
    LEFT JOIN gtfs.ingested_static ti
        ON ti.onestop_feed_id = tc.onestop_feed_id AND ti.attempt_id = tc.attempt_id
    WHERE tc.chateau = $1 AND tc.trip_id = ANY($4::text[])
), selected_trips AS MATERIALIZED (
    SELECT wanted.*, t.onestop_feed_id AS trip_feed, t.attempt_id AS trip_attempt,
           t.route_id AS trip_route, t.trip_short_name, t.itinerary_pattern_id,
           t.direction_id
    FROM wanted
    LEFT JOIN LATERAL (
        SELECT tc.* FROM trip_candidates tc WHERE tc.trip_id = wanted.trip
        -- Feed is a collision tie-breaker, never a required lookup key.
        ORDER BY (tc.onestop_feed_id = wanted.feed) DESC,
                 (tc.route_id = NULLIF(wanted.route_id, '')) DESC NULLS LAST,
                 tc.usable DESC NULLS LAST,
                 tc.ingest_end_unix_time_ms DESC NULLS LAST,
                 tc.attempt_id DESC, tc.onestop_feed_id
        LIMIT 1
    ) t ON TRUE
), route_candidates AS MATERIALIZED (
    SELECT rr.onestop_feed_id, rr.attempt_id, rr.route_id, rr.short_name,
           rr.long_name, rr.agency_id, rr.color, rr.text_color, rr.route_type,
           ri.production AND ri.ingestion_successfully_finished
               AND NOT ri.ingesting_in_progress AND NOT ri.ingestion_errored
               AND NOT ri.deleted AS usable,
           ri.ingest_end_unix_time_ms
    FROM gtfs.routes rr
    LEFT JOIN gtfs.ingested_static ri
        ON ri.onestop_feed_id = rr.onestop_feed_id AND ri.attempt_id = rr.attempt_id
    WHERE rr.chateau = $1
      AND rr.route_id IN (
          SELECT COALESCE(NULLIF(route_id, ''), trip_route) FROM selected_trips
      )
)
SELECT t.feed, t.attempt, t.trip,
       COALESCE(NULLIF(t.route_id, ''), t.trip_route, ''),
       r.short_name, r.long_name, t.trip_short_name, m.trip_headsign,
       r.agency_id, a.agency_name, a.agency_url, r.color, r.text_color,
       r.route_type, t.direction_id,
       CASE WHEN $6::boolean THEN
           COALESCE(ST_AsGeoJSON(s.linestring, 6), ST_AsGeoJSON(ds.linestring, 6))
       END,
       s.color, s.text_color, ds.color, ds.text_color
FROM selected_trips t
-- Preserve route fallback when the timetable trip has been rotated out.
LEFT JOIN LATERAL (
    SELECT rr.* FROM route_candidates rr
    WHERE rr.route_id = COALESCE(NULLIF(t.route_id, ''), t.trip_route)
    ORDER BY (rr.onestop_feed_id = COALESCE(t.trip_feed, t.feed)) DESC,
             rr.usable DESC NULLS LAST,
             rr.ingest_end_unix_time_ms DESC NULLS LAST,
             (rr.attempt_id = t.trip_attempt) DESC NULLS LAST,
             rr.attempt_id DESC, rr.onestop_feed_id
    LIMIT 1
) r ON TRUE
-- Once selected, keep pattern/shape provenance exact. These joins use the
-- existing (feed, attempt, GTFS ID) primary keys; ID-only joins are ambiguous.
LEFT JOIN gtfs.itinerary_pattern_meta m
       ON m.onestop_feed_id = t.trip_feed
      AND m.attempt_id = t.trip_attempt
      AND m.itinerary_pattern_id = t.itinerary_pattern_id
LEFT JOIN gtfs.shapes s
       ON s.onestop_feed_id = m.onestop_feed_id
      AND s.attempt_id = m.attempt_id AND s.shape_id = m.shape_id
LEFT JOIN gtfs.direction_pattern_meta d
       ON d.onestop_feed_id = m.onestop_feed_id
      AND d.attempt_id = m.attempt_id
      AND d.direction_pattern_id = m.direction_pattern_id
LEFT JOIN gtfs.shapes ds
       ON ds.onestop_feed_id = d.onestop_feed_id
      AND ds.attempt_id = d.attempt_id AND ds.shape_id = d.gtfs_shape_id
LEFT JOIN LATERAL (
    SELECT aa.agency_name, aa.agency_url
    FROM gtfs.agencies aa
    LEFT JOIN gtfs.ingested_static ai
        ON ai.onestop_feed_id = aa.static_onestop_id AND ai.attempt_id = aa.attempt_id
    WHERE aa.chateau = $1 AND aa.agency_id = r.agency_id
      -- Agency IDs are local to a feed. Use the selected route's provenance.
      AND aa.static_onestop_id = r.onestop_feed_id
    ORDER BY (aa.attempt_id = r.attempt_id) DESC NULLS LAST,
             (ai.production AND ai.ingestion_successfully_finished
              AND NOT ai.ingesting_in_progress AND NOT ai.ingestion_errored
              AND NOT ai.deleted) DESC NULLS LAST,
             ai.ingest_end_unix_time_ms DESC NULLS LAST, aa.attempt_id DESC
    LIMIT 1
) a ON TRUE
"#;

#[derive(Clone, Default)]
struct TripInfo {
    // Source provenance is always read from the compiled timetable, not the metadata DB.
    chateau: Option<String>,
    id: String,
    route_id: String,
    short_name: Option<String>,
    long_name: Option<String>,
    trip_short_name: Option<String>,
    headsign: Option<String>,
    agency_id: Option<String>,
    agency_name: Option<String>,
    agency_url: Option<String>,
    route_color: Option<String>,
    route_text_color: Option<String>,
    route_type: Option<i16>,
    direction_id: Option<bool>,
    shape: Vec<[f64; 2]>,
}

#[derive(Clone)]
struct TripKey {
    chateau: String,
    feed: String,
    attempt: String,
    trip: String,
    route: String,
}

fn ride_identity(
    tt: &ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    transport: u32,
) -> Option<TripKey> {
    let trip = index.trip_for_transport(transport)?;
    let record = tt.trip_ids().get(index.id_record_for_trip(trip)?)?;
    let (chateau, feed, attempt) = tt.source(record.source)?;
    // Look up colours by the timetable's public route identity, even when the
    // corresponding trips_compressed row has been removed or rotated.
    let route = tt
        .trip_public_route_ids()
        .get(trip)
        .and_then(|id| tt.route_ids().get(id.idx()))
        .and_then(|id| tt.get_string(id.id))
        .unwrap_or_default()
        .to_owned();
    Some(TripKey {
        chateau: chateau.to_owned(),
        feed: feed.to_owned(),
        attempt: attempt.to_owned(),
        trip: tt.get_string(record.id)?.to_owned(),
        route,
    })
}

/// Resolve only returned trips, with one batch per chateau.
/// A dedicated four-thread pool bounds database concurrency across HTTP requests.
/// Timetable attempt identity is retained only to map results to the correct leg.
fn metadata(
    tt: &ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    journeys: &[Journey],
    database_url: Option<&str>,
    detailed: bool,
) -> Result<BTreeMap<(String, String, String), TripInfo>> {
    let mut keys = BTreeMap::new();
    for journey in journeys {
        for leg in &journey.legs {
            if let Leg::Ride { transport, .. } = leg {
                if let Some(k) = ride_identity(tt, index, *transport) {
                    keys.insert((k.feed.clone(), k.attempt.clone(), k.trip.clone()), k);
                }
            }
        }
    }
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some(url) = database_url else {
        return Ok(BTreeMap::new());
    };
    let groups = group_metadata_keys(keys.into_values());
    static WORKERS: OnceLock<std::result::Result<rayon::ThreadPool, rayon::ThreadPoolBuildError>> =
        OnceLock::new();
    let pool = WORKERS
        .get_or_init(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(4)
                .thread_name(|i| format!("edelweiss-metadata-{i}"))
                .build()
        })
        .as_ref()
        .map_err(|e| anyhow::anyhow!("creating metadata workers: {e}"))?;
    let batches: Result<Vec<_>> = pool.install(|| {
        groups
            .par_iter()
            .map(|(chateau, keys)| {
                metadata_for_chateau(url, chateau, keys, detailed)
                    .with_context(|| format!("loading metadata for chateau {chateau}"))
            })
            .collect()
    });
    Ok(batches?.into_iter().flatten().collect())
}

fn group_metadata_keys(keys: impl IntoIterator<Item = TripKey>) -> BTreeMap<String, Vec<TripKey>> {
    let mut groups = BTreeMap::<String, Vec<TripKey>>::new();
    for key in keys {
        groups.entry(key.chateau.clone()).or_default().push(key);
    }
    groups
}

fn metadata_for_chateau(
    url: &str,
    chateau: &str,
    keys: &[TripKey],
    detailed: bool,
) -> Result<BTreeMap<(String, String, String), TripInfo>> {
    let feeds: Vec<_> = keys.iter().map(|k| k.feed.as_str()).collect();
    // Compiled attempts identify response legs; they never filter DB metadata.
    let attempts: Vec<_> = keys.iter().map(|k| k.attempt.as_str()).collect();
    let trips: Vec<_> = keys.iter().map(|k| k.trip.as_str()).collect();
    let routes: Vec<_> = keys.iter().map(|k| k.route.as_str()).collect();
    let mut db = Client::connect(url, NoTls).context("connecting to metadata database")?;
    let rows = db.query(
        METADATA_SQL,
        &[&chateau, &feeds, &attempts, &trips, &routes, &detailed],
    )?;
    let mut result = BTreeMap::new();
    for row in rows {
        let shape_geojson: Option<String> = row.get(15);
        let shape = shape_geojson
            .as_deref()
            .and_then(parse_linestring)
            .unwrap_or_default();
        let id: String = row.get(2);
        let feed: String = row.get(0);
        let attempt: String = row.get(1);
        let route_color: Option<String> = row.get(11);
        let route_text_color: Option<String> = row.get(12);
        let shape_color: Option<String> = row.get(16);
        let shape_text_color: Option<String> = row.get(17);
        let direction_color: Option<String> = row.get(18);
        let direction_text_color: Option<String> = row.get(19);
        result.insert(
            (feed, attempt, id.clone()),
            TripInfo {
                chateau: None, // populated authoritatively from timetable trip source
                id,
                route_id: row.get(3),
                short_name: row.get(4),
                long_name: row.get(5),
                trip_short_name: row.get(6),
                headsign: row.get(7),
                agency_id: row.get(8),
                agency_name: row.get(9),
                agency_url: row.get(10),
                route_color: first_normalized_color(&[
                    route_color.as_deref(),
                    shape_color.as_deref(),
                    direction_color.as_deref(),
                ]),
                route_text_color: first_normalized_color(&[
                    route_text_color.as_deref(),
                    shape_text_color.as_deref(),
                    direction_text_color.as_deref(),
                ]),
                route_type: row.get(13),
                direction_id: row.get(14),
                shape,
            },
        );
    }
    Ok(result)
}

/// Accept only valid GTFS hex colours; return canonical CSS colours for clients.
/// Never turn malformed input into an arbitrary (and misleading) route colour.
fn normalize_color(value: &str) -> Option<String> {
    let value = value.trim();
    let value = value.strip_prefix('#').unwrap_or(value);
    if !matches!(value.len(), 3 | 6) || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let hex = if value.len() == 3 {
        value.chars().flat_map(|ch| [ch, ch]).collect::<String>()
    } else {
        value.to_owned()
    };
    Some(format!("#{}", hex.to_ascii_uppercase()))
}

fn first_normalized_color(candidates: &[Option<&str>]) -> Option<String> {
    candidates
        .iter()
        .filter_map(|value| *value)
        .find_map(normalize_color)
}

fn parse_linestring(s: &str) -> Option<Vec<[f64; 2]>> {
    let obj: Value = serde_json::from_str(s).ok()?;
    if obj["type"] != "LineString" {
        return None;
    }
    let mut out = Vec::new();
    for p in obj["coordinates"].as_array()? {
        let x = p.get(0)?.as_f64()?;
        let y = p.get(1)?.as_f64()?;
        if !x.is_finite() || !y.is_finite() {
            return None;
        }
        out.push([x, y]);
    }
    (out.len() >= 2).then_some(out)
}

fn projection(poly: &[[f64; 2]], point: [f64; 2], start: usize) -> Option<(usize, f64, [f64; 2])> {
    let lat_scale = point[1].to_radians().cos().abs().max(0.01);
    let mut best: Option<(f64, usize, f64, [f64; 2])> = None;
    for i in start..poly.len().saturating_sub(1) {
        let a = poly[i];
        let b = poly[i + 1];
        let dx = (b[0] - a[0]) * lat_scale;
        let dy = b[1] - a[1];
        let px = (point[0] - a[0]) * lat_scale;
        let py = point[1] - a[1];
        let denom = dx * dx + dy * dy;
        let t = if denom > 0.0 {
            ((px * dx + py * dy) / denom).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let projected = [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
        let distance =
            ((projected[0] - point[0]) * lat_scale).powi(2) + (projected[1] - point[1]).powi(2);
        if best.as_ref().is_none_or(|v| distance < v.0) {
            best = Some((distance, i, t, projected));
        }
    }
    best.map(|(_, i, t, p)| (i, t, p))
}

/// Project boarding and alighting points to the actual shape, retaining its
/// intermediate vertices instead of connecting stops by straight lines.
fn clipped_shape_forward(poly: &[[f64; 2]], from: [f64; 2], to: [f64; 2]) -> Vec<[f64; 2]> {
    let Some((a, at, pa)) = projection(poly, from, 0) else {
        return Vec::new();
    };
    let Some((b, bt, pb)) = projection(poly, to, a) else {
        return Vec::new();
    };
    if b == a && bt < at {
        return Vec::new();
    }
    let mut result = vec![pa];
    for coord in poly.iter().take(b + 1).skip(a + 1) {
        if result.last() != Some(coord) {
            result.push(*coord);
        }
    }
    if result.last() != Some(&pb) {
        result.push(pb);
    }
    result
}

/// Shapes are usually encoded in increasing stop order, but a GTFS shape can
/// be stored in the opposite direction. Only use the actual shape vertices;
/// never replace a missing rail/bus shape with a straight-line guess.
fn clipped_shape(poly: &[[f64; 2]], from: [f64; 2], to: [f64; 2]) -> Vec<[f64; 2]> {
    let forward = clipped_shape_forward(poly, from, to);
    if forward.len() >= 2 && forward.first() != forward.last() {
        return forward;
    }
    let reverse: Vec<_> = poly.iter().rev().copied().collect();
    clipped_shape_forward(&reverse, from, to)
}

fn polyline(points: &[[f64; 2]]) -> Value {
    let mut output = String::new();
    let mut last_lat = 0_i64;
    let mut last_lon = 0_i64;
    for &[lon, lat] in points {
        let y = (lat * 1_000_000.0).round() as i64;
        let x = (lon * 1_000_000.0).round() as i64;
        for delta in [y - last_lat, x - last_lon] {
            let mut bits = if delta < 0 {
                (!(delta << 1)) as u64
            } else {
                (delta << 1) as u64
            };
            while bits >= 0x20 {
                output.push(char::from_u32(((0x20 | (bits & 0x1f)) + 63) as u32).unwrap());
                bits >>= 5;
            }
            output.push(char::from_u32((bits + 63) as u32).unwrap());
        }
        last_lat = y;
        last_lon = x;
    }
    json!({"points":output,"precision":6,"length":points.len()})
}

fn timestamp(seconds: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}
fn location(tt: &ZeroCopyTimetable<'_>, stop: u32) -> Option<[f64; 2]> {
    let c = tt.coordinates().get(stop as usize)?;
    Some([f64::from(c.lon), f64::from(c.lat)])
}
/// Resolve civil time at the stop, independently of the trip's service timezone.
/// Existing timetable files contain coordinates but no per-location timezone index.
fn point_timezone(lon: f64, lat: f64) -> Option<&'static str> {
    static FINDER: OnceLock<tzf_rs::DefaultFinder> = OnceLock::new();
    if !lon.is_finite()
        || !lat.is_finite()
        || !(-180.0..=180.0).contains(&lon)
        || !(-90.0..=90.0).contains(&lat)
    {
        return None;
    }
    let name = FINDER
        .get_or_init(tzf_rs::DefaultFinder::new)
        .get_tz_name(lon, lat);
    (!name.is_empty()).then_some(name)
}

fn place(tt: &ZeroCopyTimetable<'_>, stop: u32) -> Value {
    let index = stop as usize;
    let point = location(tt, stop).unwrap_or([0.0, 0.0]);
    let name = tt
        .location_name()
        .get(index)
        .and_then(|s| tt.get_string(*s))
        .unwrap_or("");
    let stop_id = tt
        .location_id()
        .get(index)
        .and_then(|s| tt.get_string(*s))
        .unwrap_or("");
    let timezone = location(tt, stop).and_then(|p| point_timezone(p[0], p[1]));
    json!({"name":name,"stopId":stop_id,"stopIndex":stop,"lat":point[1],"lon":point[0],"timezone":timezone})
}
fn coordinate_place(point: Coordinates, name: &str) -> Value {
    json!({"name": name,"lat":point.lat,"lon":point.lon,"timezone":point_timezone(point.lon, point.lat)})
}

/// A response-local cache avoids rerouting identical selected street legs.
/// IDs, graph region, speeds, direction and budget all affect path selection.
fn street_path_key(
    region: &str,
    from: [f64; 2],
    to: [f64; 2],
    origin: Option<LocationIdx>,
    destination: Option<LocationIdx>,
    budget: u32,
    mode: &str,
    pedestrian_speed: f32,
    cycling_speed: f32,
) -> Result<String> {
    Ok(serde_json::to_string(&(
        region,
        from,
        to,
        origin.map(|s| s.0),
        destination.map(|s| s.0),
        budget.clamp(1, 3600),
        mode,
        pedestrian_speed.to_bits(),
        cycling_speed.to_bits(),
    ))?)
}

fn street_path(
    geometry_cache: &mut HashMap<String, Vec<[f64; 2]>>,
    osr: &NativeOsr,
    tt: &ZeroCopyTimetable<'_>,
    platform_stops: &[catenary::routing_common::route_manager::PlatformStopMeta],
    region: &str,
    from: [f64; 2],
    to: [f64; 2],
    origin_stop: Option<LocationIdx>,
    destination_stop: Option<LocationIdx>,
    budget: u32,
    mode: &str,
    pedestrian_speed: f32,
    cycling_speed: f32,
) -> Result<Vec<[f64; 2]>> {
    let key = street_path_key(
        region,
        from,
        to,
        origin_stop,
        destination_stop,
        budget,
        mode,
        pedestrian_speed,
        cycling_speed,
    )?;
    if let Some(geometry) = geometry_cache.get(&key) {
        return Ok(geometry.clone());
    }
    let origin = StreetEndpoint {
        point: Coordinates {
            lat: from[1],
            lon: from[0],
        },
        stop: origin_stop,
    };
    let destination = StreetEndpoint {
        point: Coordinates {
            lat: to[1],
            lon: to[0],
        },
        stop: destination_stop,
    };
    let profile = match mode {
        "WALK" => osr_ffi::Profile::Foot,
        "BIKE" => osr_ffi::Profile::BikeFast,
        _ => anyhow::bail!("unsupported street geometry mode"),
    };
    let paths = osr.route(
        region,
        tt,
        platform_stops,
        origin,
        &[destination],
        profile,
        0,
        budget.clamp(1, 3600),
        pedestrian_speed,
        cycling_speed,
        true,
    )?;
    let coordinates = paths
        .first()
        .map(|path| path_geometry(origin.point, destination.point, path))
        .unwrap_or_default();
    geometry_cache.insert(key, coordinates.clone());
    Ok(coordinates)
}

fn stop_index(location: &Value) -> Option<LocationIdx> {
    u32::try_from(location.get("stopIndex")?.as_u64()?)
        .ok()
        .map(LocationIdx)
}

fn street_leg(
    geometry_cache: &mut HashMap<String, Vec<[f64; 2]>>,
    osr: &NativeOsr,
    region: &str,
    from: Value,
    to: Value,
    tt: &ZeroCopyTimetable<'_>,
    platform_stops: &[catenary::routing_common::route_manager::PlatformStopMeta],
    start: i64,
    seconds: u32,
    detailed: bool,
    mode: &str,
    pedestrian_speed: f32,
    cycling_speed: f32,
) -> Result<Value> {
    let first = [
        from["lon"].as_f64().unwrap_or_default(),
        from["lat"].as_f64().unwrap_or_default(),
    ];
    let last = [
        to["lon"].as_f64().unwrap_or_default(),
        to["lat"].as_f64().unwrap_or_default(),
    ];
    let coordinates = if detailed {
        street_path(
            geometry_cache,
            osr,
            tt,
            platform_stops,
            region,
            first,
            last,
            stop_index(&from),
            stop_index(&to),
            seconds.saturating_add(900),
            mode,
            pedestrian_speed,
            cycling_speed,
        )?
    } else {
        Vec::new()
    };
    let end = start + i64::from(seconds);
    Ok(json!({
        "mode":mode,"from":from,"to":to,"duration":seconds,
        "startTime":timestamp(start),"endTime":timestamp(end),
        "scheduledStartTime":timestamp(start),"scheduledEndTime":timestamp(end),
        "realTime":false,"scheduled":true,"cancelled":false,
        "legGeometry":polyline(&coordinates)
    }))
}

fn trip_info_for_key(
    info: &BTreeMap<(String, String, String), TripInfo>,
    key: Option<TripKey>,
) -> TripInfo {
    let Some(key) = key else {
        return TripInfo::default();
    };
    let mut result = info
        .get(&(key.feed, key.attempt, key.trip))
        .cloned()
        .unwrap_or_default();
    // The timetable remains authoritative even if PostgreSQL trip metadata is missing.
    result.chateau = Some(key.chateau);
    result
}

fn trip_info(
    tt: &ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    info: &BTreeMap<(String, String, String), TripInfo>,
    transport: u32,
) -> TripInfo {
    trip_info_for_key(info, ride_identity(tt, index, transport))
}

/// The compiled timetable remains usable when PostgreSQL no longer contains
/// the exact GTFS import attempt used for a routed trip.
fn timetable_route_id<'a>(
    tt: &'a ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    transport: u32,
) -> Option<&'a str> {
    let trip = index.trip_for_transport(transport)?;
    let route_idx = tt.trip_public_route_ids().get(trip)?;
    let record = tt.route_ids().get(route_idx.idx())?;
    tt.get_string(record.id)
}

/// Never collapse a known transit class into "TRANSIT" just because a metadata
/// database join did not return the original route type.
fn transit_mode(route_type: Option<i16>, compiled_class: Option<u8>) -> &'static str {
    match route_type {
        Some(0) => return "TRAM",
        Some(1) => return "SUBWAY",
        Some(2) => return "RAIL",
        Some(3) => return "BUS",
        Some(4) => return "FERRY",
        Some(7) => return "FUNICULAR",
        _ => {}
    }
    match compiled_class {
        Some(c) if c == Clasz::Subway as u8 => "SUBWAY",
        Some(c) if c == Clasz::Tram as u8 => "TRAM",
        Some(c) if c == Clasz::Bus as u8 || c == Clasz::Coach as u8 => "BUS",
        Some(c) if c == Clasz::Ship as u8 => "FERRY",
        Some(c) if c == Clasz::Funicular as u8 => "FUNICULAR",
        Some(c)
            if matches!(c, x if x == Clasz::HighSpeed as u8
            || x == Clasz::LongDistance as u8 || x == Clasz::Night as u8
            || x == Clasz::Regional as u8 || x == Clasz::Suburban as u8) =>
        {
            "RAIL"
        }
        _ => "TRANSIT",
    }
}

fn transit_leg(
    tt: &ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    info: &BTreeMap<(String, String, String), TripInfo>,
    ride: &Leg,
    detailed: bool,
) -> Value {
    let Leg::Ride {
        from_stop,
        to_stop,
        route,
        transport,
        service_day,
        board_stop_index,
        alight_stop_index,
        departure_unix_seconds,
        arrival_unix_seconds,
        ..
    } = ride
    else {
        unreachable!()
    };
    let mut meta = trip_info(tt, index, info, *transport);
    if meta.route_id.is_empty() {
        meta.route_id = timetable_route_id(tt, index, *transport)
            .unwrap_or_default()
            .to_owned();
    }
    let all = tt.stops_on(RouteIdx(*route));
    let from = place(tt, *from_stop);
    let to = place(tt, *to_stop);
    let shape = if detailed {
        match (location(tt, *from_stop), location(tt, *to_stop)) {
            (Some(a), Some(b)) => clipped_shape(&meta.shape, a, b),
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };
    let intermediate_stops: Vec<Value> = if board_stop_index < alight_stop_index {
        let first = (i64::from(tt.header().first_utc_day) + i64::from(*service_day)) * 86_400
            + i64::from(tt.transport_first_dep_offset()[*transport as usize].seconds());
        all.iter()
            .enumerate()
            .take(*alight_stop_index)
            .skip(*board_stop_index + 1)
            .map(|(position, stop)| {
                let mut p = place(tt, stop.location().0);
                if let Some(arrival) =
                    tt.event_time(RouteIdx(*route), TransportIdx(*transport), position, true)
                {
                    let t = timestamp(first + i64::from(arrival.seconds()));
                    p["arrival"] = json!(t);
                    p["scheduledArrival"] = json!(t);
                }
                if let Some(departure) =
                    tt.event_time(RouteIdx(*route), TransportIdx(*transport), position, false)
                {
                    let t = timestamp(first + i64::from(departure.seconds()));
                    p["departure"] = json!(t);
                    p["scheduledDeparture"] = json!(t);
                }
                p
            })
            .collect()
    } else {
        Vec::new()
    };
    let mode = transit_mode(
        meta.route_type,
        tt.route_clasz().get(*route as usize).copied(),
    );
    let start = *departure_unix_seconds;
    let end = *arrival_unix_seconds;
    json!({
        "mode":mode, "from":from,"to":to,
        "duration":end.saturating_sub(start),
        "startTime":timestamp(start),"endTime":timestamp(end),
        "scheduledStartTime":timestamp(start),"scheduledEndTime":timestamp(end),
        "realTime":false,"scheduled":true,"cancelled":false,
        "routeId":meta.route_id,"tripId":meta.id,
        "chateau":meta.chateau,
        "routeShortName":meta.short_name,"routeLongName":meta.long_name,
        "tripShortName":meta.trip_short_name,"headsign":meta.headsign,
        "routeColor":meta.route_color,"routeTextColor":meta.route_text_color,
        "routeType":meta.route_type,"agencyId":meta.agency_id,
        "agencyName":meta.agency_name,"agencyUrl":meta.agency_url,
        "directionId":meta.direction_id.map(|d|if d {"1"} else {"0"}),
        "intermediateStops":intermediate_stops,
        "legGeometry":polyline(&shape)
    })
}

pub fn build(
    tt: &ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    platform_stops: &[catenary::routing_common::route_manager::PlatformStopMeta],
    journeys: &[Journey],
    from: Coordinates,
    to: Coordinates,
    origin_osr_region: &str,
    destination_osr_region: &str,
    osr: &NativeOsr,
    database_url: Option<&str>,
    detailed: bool,
    access_mode: &str,
    egress_mode: &str,
    direct_mode: &str,
    pedestrian_speed: f32,
    cycling_speed: f32,
) -> Result<(Value, f64)> {
    let metadata_start = Instant::now();
    let info = metadata(tt, index, journeys, database_url, detailed)?;
    let metadata_ms = metadata_start.elapsed().as_secs_f64() * 1000.0;
    let mut itineraries = Vec::new();
    let mut direct = Vec::new();
    let mut geometry_cache = HashMap::new();
    for journey in journeys {
        let mut cursor = journey.departure_unix_seconds;
        let mut previous = coordinate_place(from, "Origin");
        let mut legs = Vec::new();
        for leg in &journey.legs {
            match leg {
                Leg::Access { to_stop, seconds } => {
                    let end = place(tt, *to_stop);
                    legs.push(street_leg(
                        &mut geometry_cache,
                        osr,
                        origin_osr_region,
                        previous,
                        end.clone(),
                        tt,
                        platform_stops,
                        cursor,
                        *seconds,
                        detailed,
                        access_mode,
                        pedestrian_speed,
                        cycling_speed,
                    )?);
                    cursor += i64::from(*seconds);
                    previous = end;
                }
                Leg::Transfer {
                    from_stop,
                    to_stop,
                    seconds,
                } => {
                    let start = place(tt, *from_stop);
                    let end = place(tt, *to_stop);
                    legs.push(street_leg(
                        &mut geometry_cache,
                        osr,
                        origin_osr_region,
                        start,
                        end.clone(),
                        tt,
                        platform_stops,
                        cursor,
                        *seconds,
                        detailed,
                        "WALK",
                        pedestrian_speed,
                        cycling_speed,
                    )?);
                    cursor += i64::from(*seconds);
                    previous = end;
                }
                Leg::Ride {
                    to_stop,
                    arrival_unix_seconds,
                    ..
                } => {
                    legs.push(transit_leg(tt, index, &info, leg, detailed));
                    cursor = *arrival_unix_seconds;
                    previous = place(tt, *to_stop);
                }
                Leg::Egress { seconds, .. } => {
                    let destination = coordinate_place(to, "Destination");
                    legs.push(street_leg(
                        &mut geometry_cache,
                        osr,
                        destination_osr_region,
                        previous,
                        destination.clone(),
                        tt,
                        platform_stops,
                        cursor,
                        *seconds,
                        detailed,
                        egress_mode,
                        pedestrian_speed,
                        cycling_speed,
                    )?);
                    cursor += i64::from(*seconds);
                    previous = destination;
                }
                Leg::DirectWalk { seconds } => {
                    let destination = coordinate_place(to, "Destination");
                    legs.push(street_leg(
                        &mut geometry_cache,
                        osr,
                        origin_osr_region,
                        previous,
                        destination.clone(),
                        tt,
                        platform_stops,
                        cursor,
                        *seconds,
                        detailed,
                        direct_mode,
                        pedestrian_speed,
                        cycling_speed,
                    )?);
                    cursor += i64::from(*seconds);
                    previous = destination;
                }
            }
        }
        let value = json!({
            "duration":journey.arrival_unix_seconds-journey.departure_unix_seconds,
            "startTime":timestamp(journey.departure_unix_seconds),
            "endTime":timestamp(journey.arrival_unix_seconds),
            "transfers":journey.boardings.saturating_sub(1),
            "id":"",
            "legs":legs
        });
        if journey.boardings == 0 {
            direct.push(value)
        } else {
            itineraries.push(value)
        }
    }
    Ok((
        json!({
            "requestParameters":{}, "debugOutput":{},
            "from":coordinate_place(from,"Origin"),
            "to":coordinate_place(to,"Destination"),
            "direct":direct,"itineraries":itineraries,
            "previousPageCursor":"","nextPageCursor":""
        }),
        metadata_ms,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn places_transmit_local_iana_timezones() {
        assert_eq!(point_timezone(-118.2437, 34.0522), Some("America/Los_Angeles"));
        assert_eq!(point_timezone(-74.0060, 40.7128), Some("America/New_York"));
        let endpoint = coordinate_place(
            Coordinates { lat: 40.7128, lon: -74.0060 },
            "Destination",
        );
        assert_eq!(endpoint["timezone"], "America/New_York");
        assert_eq!(point_timezone(f64::NAN, 0.0), None);
    }

    #[test]
    fn street_geometry_cache_distinguishes_routing_parameters() {
        let key = |origin, budget, mode, speed| {
            street_path_key(
                "graph",
                [1.0, 2.0],
                [3.0, 4.0],
                origin,
                Some(LocationIdx(2)),
                budget,
                mode,
                1.2,
                speed,
            )
            .unwrap()
        };
        assert_eq!(
            key(Some(LocationIdx(1)), 900, "WALK", 4.0),
            key(Some(LocationIdx(1)), 900, "WALK", 4.0)
        );
        let baseline = key(Some(LocationIdx(1)), 900, "BIKE", 4.0);
        assert_ne!(baseline, key(None, 900, "BIKE", 4.0));
        assert_ne!(baseline, key(Some(LocationIdx(1)), 600, "BIKE", 4.0));
        assert_ne!(baseline, key(Some(LocationIdx(1)), 900, "WALK", 4.0));
        assert_ne!(baseline, key(Some(LocationIdx(1)), 900, "BIKE", 6.0));
    }

    #[test]
    fn metadata_lookups_do_not_filter_on_compiled_attempts() {
        assert!(METADATA_SQL.contains("AS w(feed, attempt, trip, route_id)"));
        assert!(!METADATA_SQL.contains("= wanted.attempt"));
        assert!(METADATA_SQL.contains("tc.trip_id = wanted.trip"));
        assert!(METADATA_SQL.contains("rr.route_id = COALESCE"));
        assert!(!METADATA_SQL.contains("tc.onestop_feed_id = wanted.feed\n"));
        // One selected trip, route and agency per ride even with many import attempts.
        assert_eq!(METADATA_SQL.matches("LIMIT 1").count(), 3);
    }

    #[test]
    fn metadata_batches_keep_chateaux_separate_and_preserve_source_keys() {
        let key = TripKey {
            chateau: "metro".into(),
            feed: "feed-one".into(),
            attempt: "compiled-old".into(),
            trip: "T".into(),
            route: "R".into(),
        };
        let mut second = key.clone();
        second.feed = "feed-two".into();
        let mut third = key.clone();
        third.chateau = "other-chateau".into();
        third.feed = "feed-three".into();
        let groups = group_metadata_keys([key, second, third]);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups["metro"].len(), 2);
        assert_eq!(groups["other-chateau"].len(), 1);
        assert_eq!(groups["metro"][0].feed, "feed-one");
        assert_eq!(groups["metro"][1].feed, "feed-two");
        assert_eq!(groups["metro"][0].attempt, "compiled-old");
    }

    #[test]
    fn missing_database_route_type_uses_compiled_timetable_class() {
        assert_eq!(transit_mode(None, Some(Clasz::Bus as u8)), "BUS");
        assert_eq!(transit_mode(None, Some(Clasz::Suburban as u8)), "RAIL");
        assert_eq!(transit_mode(None, Some(Clasz::Tram as u8)), "TRAM");
        assert_eq!(transit_mode(None, Some(Clasz::Subway as u8)), "SUBWAY");
        assert_eq!(transit_mode(None, Some(Clasz::Ship as u8)), "FERRY");
        assert_eq!(transit_mode(None, None), "TRANSIT");
        assert_eq!(transit_mode(Some(3), Some(Clasz::Regional as u8)), "BUS");
    }

    #[test]
    fn detailed_platform_endpoints_retain_timetable_identity() {
        let mut owned = catenary::routing_common::timetable::TimetableOwned::empty(0, 1);
        owned.add_source("test", "metro", "1").unwrap();
        owned
            .coordinates
            .push(catenary::routing_common::timetable::Coordinate {
                lat: 34.0354,
                lon: -118.4341,
            });
        let stop_id = owned.add_string("metro-e-platform").unwrap();
        let stop_name = owned.add_string("Expo / Sepulveda").unwrap();
        owned.location_id.push(stop_id);
        owned.location_name.push(stop_name);
        owned
            .location_source
            .push(catenary::routing_common::timetable::SourceIdx(0));
        owned.location_type.push(1);
        owned
            .location_parent
            .push(catenary::routing_common::timetable::LocationIdx::INVALID);
        owned.location_transfer_seconds.push(120);
        owned.location_routes_offsets.push(0);
        owned.footpaths_out_offsets.push(0);
        owned.footpaths_in_offsets.push(0);
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let location = place(&tt, 0);
        assert_eq!(location["timezone"], "America/Los_Angeles");
        let idx = stop_index(&location).unwrap();
        assert_eq!(idx, LocationIdx(0));
        let endpoint = StreetEndpoint::stop(&tt, idx).unwrap();
        assert_eq!(endpoint.point.lat, f64::from(tt.coordinates()[0].lat));
        assert_eq!(endpoint.point.lon, f64::from(tt.coordinates()[0].lon));
        assert_eq!(
            tt.get_string(tt.location_id()[idx.idx()]),
            Some("metro-e-platform")
        );
        assert!(stop_index(&coordinate_place(endpoint.point, "Origin")).is_none());
    }

    #[test]
    fn polyline_six_digit_encoding() {
        let encoded = polyline(&[[-120.2, 38.5], [-120.95, 40.7], [-126.453, 43.252]]);
        assert_eq!(encoded["precision"], 6);
        assert_eq!(encoded["length"], 3);
        assert!(encoded["points"].as_str().unwrap().len() > 10);
    }
    #[test]
    fn clipping_retains_curvature() {
        let shape = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [2.0, 1.0]];
        let clipped = clipped_shape(&shape, [0.5, 0.0], [1.5, 1.0]);
        assert_eq!(
            clipped,
            vec![[0.5, 0.0], [1.0, 0.0], [1.0, 1.0], [1.5, 1.0]]
        );
    }

    #[test]
    fn reverse_direction_gtfs_geometry_remains_drawable() {
        let shape = [[2.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]];
        let clipped = clipped_shape(&shape, [0.5, 0.0], [1.5, 1.0]);
        assert_eq!(
            clipped,
            vec![[0.5, 0.0], [1.0, 0.0], [1.0, 1.0], [1.5, 1.0]]
        );
    }

    #[test]
    fn transit_chateau_comes_from_timetable_source_even_without_db_metadata() {
        let key = TripKey {
            chateau: "nationalrailuk".into(),
            feed: "f-rail".into(),
            attempt: "attempt-1".into(),
            trip: "train-123".into(),
            route: "R".into(),
        };
        let missing = trip_info_for_key(&BTreeMap::new(), Some(key.clone()));
        assert_eq!(missing.chateau.as_deref(), Some("nationalrailuk"));
        assert_eq!(
            json!({"chateau":missing.chateau})["chateau"],
            "nationalrailuk"
        );
        assert!(missing.short_name.is_none());

        let mut rows = BTreeMap::new();
        rows.insert(
            ("f-rail".into(), "attempt-1".into(), "train-123".into()),
            TripInfo {
                short_name: Some("London".into()),
                ..TripInfo::default()
            },
        );
        let resolved = trip_info_for_key(&rows, Some(key));
        assert_eq!(resolved.chateau.as_deref(), Some("nationalrailuk"));
        assert_eq!(resolved.short_name.as_deref(), Some("London"));
    }

    #[test]
    fn missing_source_identity_does_not_invent_chateau() {
        let result = trip_info_for_key(&BTreeMap::new(), None);
        assert_eq!(result.chateau, None);
        assert!(json!({"chateau":result.chateau})["chateau"].is_null());
    }
    #[test]
    fn postgres_route_colours_are_normalized_and_prefer_explicit_routes() {
        assert_eq!(normalize_color(" ff00aB "), Some("#FF00AB".into()));
        assert_eq!(normalize_color("#abc"), Some("#AABBCC".into()));
        assert_eq!(normalize_color("not a colour"), None);
        assert_eq!(normalize_color("12345"), None);
        assert_eq!(
            first_normalized_color(&[Some(" D9382E "), Some("112233")]),
            Some("#D9382E".into())
        );
        assert_eq!(
            first_normalized_color(&[Some("bad"), Some("a1b2c3"), Some("000000")]),
            Some("#A1B2C3".into())
        );
        assert_eq!(first_normalized_color(&[None, Some(""), None]), None);
    }
}
