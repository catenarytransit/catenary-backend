//! Convert an internal RAPTOR journey into a client-facing MOTIS plan response.
//! All metadata is resolved against the exact feed/attempt/trip, never the route index.
use std::collections::BTreeMap;
use anyhow::{Context, Result};
use catenary::routing_common::{
    metadata_index::MetadataIndex,
    raptor_query::{Journey, Leg},
    timetable::{RouteIdx, TransportIdx, ZeroCopyTimetable},
};
use postgres::{Client, NoTls};
use serde_json::{json, Value};
use catenary::routing_common::route_manager::Coordinates;

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
struct TripKey { chateau: String, feed: String, attempt: String, trip: String }

fn ride_identity(tt: &ZeroCopyTimetable<'_>, index: &MetadataIndex, transport: u32) -> Option<TripKey> {
    let trip = index.trip_for_transport(transport)?;
    let record = tt.trip_ids().get(index.id_record_for_trip(trip)?)?;
    let (chateau, feed, attempt) = tt.source(record.source)?;
    Some(TripKey {
        chateau: chateau.to_owned(),
        feed: feed.to_owned(),
        attempt: attempt.to_owned(),
        trip: tt.get_string(record.id)?.to_owned(),
    })
}

/// Resolve only trips in returned journeys, in one PostgreSQL round trip.
/// Source identity prevents collisions across feeds and import attempts.
fn metadata(
    tt: &ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    journeys: &[Journey],
    database_url: Option<&str>,
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
    if keys.is_empty() { return Ok(BTreeMap::new()); }
    let Some(url) = database_url else { return Ok(BTreeMap::new()); };
    let feeds: Vec<_> = keys.values().map(|k| k.feed.clone()).collect();
    let attempts: Vec<_> = keys.values().map(|k| k.attempt.clone()).collect();
    let trips: Vec<_> = keys.values().map(|k| k.trip.clone()).collect();
    let mut db = Client::connect(url, NoTls).context("connecting to metadata database")?;
    let rows = db.query(
        "SELECT t.onestop_feed_id, t.attempt_id, t.trip_id, t.route_id,
                r.short_name, r.long_name, t.trip_short_name, m.trip_headsign,
                r.agency_id, a.agency_name, a.agency_url, r.color, r.text_color,
                r.route_type, t.direction_id,
                ST_AsGeoJSON(s.linestring, 6)
         FROM unnest($1::text[], $2::text[], $3::text[]) AS wanted(feed, attempt, trip)
         JOIN gtfs.trips_compressed t
              ON t.onestop_feed_id = wanted.feed
             AND t.attempt_id = wanted.attempt AND t.trip_id = wanted.trip
         LEFT JOIN gtfs.routes r
              ON r.onestop_feed_id = t.onestop_feed_id
             AND r.attempt_id = t.attempt_id AND r.route_id = t.route_id
         LEFT JOIN gtfs.itinerary_pattern_meta m
              ON m.onestop_feed_id = t.onestop_feed_id
             AND m.attempt_id = t.attempt_id
             AND m.itinerary_pattern_id = t.itinerary_pattern_id
         LEFT JOIN gtfs.shapes s
              ON s.onestop_feed_id = m.onestop_feed_id
             AND s.attempt_id = m.attempt_id AND s.shape_id = m.shape_id
         LEFT JOIN gtfs.agencies a
              ON a.static_onestop_id = t.onestop_feed_id
             AND a.attempt_id = t.attempt_id AND a.agency_id = r.agency_id",
        &[&feeds, &attempts, &trips],
    )?;
    let mut result = BTreeMap::new();
    for row in rows {
        let shape_geojson: Option<String> = row.get(15);
        let shape = shape_geojson.as_deref().and_then(parse_linestring).unwrap_or_default();
        let id: String = row.get(2);
        let feed: String = row.get(0);
        let attempt: String = row.get(1);
        result.insert((feed, attempt, id.clone()), TripInfo {
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
            route_color: row.get(11),
            route_text_color: row.get(12),
            route_type: row.get(13),
            direction_id: row.get(14),
            shape,
        });
    }
    Ok(result)
}

fn parse_linestring(s: &str) -> Option<Vec<[f64; 2]>> {
    let obj: Value = serde_json::from_str(s).ok()?;
    if obj["type"] != "LineString" { return None; }
    let mut out = Vec::new();
    for p in obj["coordinates"].as_array()? {
        let x = p.get(0)?.as_f64()?;
        let y = p.get(1)?.as_f64()?;
        if !x.is_finite() || !y.is_finite() { return None; }
        out.push([x, y]);
    }
    (out.len() >= 2).then_some(out)
}

fn projection(poly: &[[f64; 2]], point: [f64; 2], start: usize) -> Option<(usize, f64, [f64; 2])> {
    let lat_scale = point[1].to_radians().cos().abs().max(0.01);
    let mut best: Option<(f64, usize, f64, [f64; 2])> = None;
    for i in start..poly.len().saturating_sub(1) {
        let a = poly[i]; let b = poly[i + 1];
        let dx = (b[0] - a[0]) * lat_scale;
        let dy = b[1] - a[1];
        let px = (point[0] - a[0]) * lat_scale;
        let py = point[1] - a[1];
        let denom = dx * dx + dy * dy;
        let t = if denom > 0.0 { ((px*dx + py*dy) / denom).clamp(0.0, 1.0) } else { 0.0 };
        let projected = [a[0] + (b[0]-a[0])*t, a[1] + (b[1]-a[1])*t];
        let distance = ((projected[0]-point[0])*lat_scale).powi(2) + (projected[1]-point[1]).powi(2);
        if best.as_ref().is_none_or(|v| distance < v.0) { best = Some((distance, i, t, projected)); }
    }
    best.map(|(_, i, t, p)| (i,t,p))
}

/// Project boarding and alighting points to the actual shape, retaining its
/// intermediate vertices instead of connecting stops by straight lines.
fn clipped_shape(poly: &[[f64; 2]], from: [f64; 2], to: [f64; 2]) -> Vec<[f64; 2]> {
    let Some((a, at, pa)) = projection(poly, from, 0) else { return Vec::new() };
    let Some((b, bt, pb)) = projection(poly, to, a) else { return Vec::new() };
    if b == a && bt < at { return Vec::new(); }
    let mut result = vec![pa];
    for coord in poly.iter().take(b + 1).skip(a + 1) {
        if result.last() != Some(coord) { result.push(*coord); }
    }
    if result.last() != Some(&pb) { result.push(pb); }
    result
}

fn polyline(points: &[[f64; 2]]) -> Value {
    let mut output = String::new();
    let mut last_lat = 0_i64;
    let mut last_lon = 0_i64;
    for &[lon, lat] in points {
        let y = (lat * 1_000_000.0).round() as i64;
        let x = (lon * 1_000_000.0).round() as i64;
        for delta in [y - last_lat, x - last_lon] {
            let mut bits = if delta < 0 { (!(delta << 1)) as u64 } else { (delta << 1) as u64 };
            while bits >= 0x20 {
                output.push(char::from_u32(((0x20 | (bits & 0x1f)) + 63) as u32).unwrap());
                bits >>= 5;
            }
            output.push(char::from_u32((bits + 63) as u32).unwrap());
        }
        last_lat = y; last_lon = x;
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
fn place(tt: &ZeroCopyTimetable<'_>, stop: u32) -> Value {
    let index = stop as usize;
    let point = location(tt, stop).unwrap_or([0.0, 0.0]);
    let name = tt.location_name().get(index)
        .and_then(|s| tt.get_string(*s)).unwrap_or("");
    let stop_id = tt.location_id().get(index)
        .and_then(|s| tt.get_string(*s)).unwrap_or("");
    json!({"name":name,"stopId":stop_id,"lat":point[1],"lon":point[0]})
}
fn coordinate_place(point: Coordinates, name: &str) -> Value {
    json!({"name": name,"lat":point.lat,"lon":point.lon})
}

#[derive(serde::Serialize)]
struct PathQuery {
    region: String,
    latitude: f64,
    longitude: f64,
    max_seconds: u32,
    include_geometry: bool,
    stops: Vec<PathTarget>,
}
#[derive(serde::Serialize)]
struct PathTarget { stop: u32, latitude: f64, longitude: f64 }
#[derive(serde::Deserialize)]
struct PathReply { reachable: Vec<PathReachable>, error: Option<String> }
#[derive(serde::Deserialize)]
struct PathReachable { geometry: Option<Vec<[f64; 2]>> }

/// Separate reconstruction phase, only for the small number of selected legs.
fn street_path(addr: &str, region: &str, from: [f64;2], to: [f64;2], budget: u32) -> Result<Vec<[f64;2]>> {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpStream;
    let q = PathQuery {
        region: region.to_owned(), latitude: from[1], longitude: from[0],
        max_seconds: budget.clamp(1,3600), include_geometry: true,
        stops: vec![PathTarget { stop:0, latitude:to[1], longitude:to[0] }],
    };
    let mut socket = TcpStream::connect(addr)?;
    socket.set_read_timeout(Some(std::time::Duration::from_secs(45)))?;
    socket.set_write_timeout(Some(std::time::Duration::from_secs(45)))?;
    serde_json::to_writer(&mut socket, &q)?;
    socket.write_all(b"\n")?;
    let mut line = String::new();
    BufReader::new(socket).read_line(&mut line)?;
    let response: PathReply = serde_json::from_str(&line)?;
    if let Some(err) = response.error { anyhow::bail!("OSR geometry: {err}"); }
    Ok(response.reachable.into_iter().next().and_then(|x| x.geometry).unwrap_or_default())
}

fn street_leg(
    addr: &str, region: &str, from: Value, to: Value, start: i64, seconds: u32,
    detailed: bool,
) -> Result<Value> {
    let first = [from["lon"].as_f64().unwrap_or_default(),from["lat"].as_f64().unwrap_or_default()];
    let last = [to["lon"].as_f64().unwrap_or_default(),to["lat"].as_f64().unwrap_or_default()];
    let coordinates = if detailed { street_path(addr,region,first,last,seconds.saturating_add(900))? } else { Vec::new() };
    let end = start + i64::from(seconds);
    Ok(json!({
        "mode":"WALK","from":from,"to":to,"duration":seconds,
        "startTime":timestamp(start),"endTime":timestamp(end),
        "scheduledStartTime":timestamp(start),"scheduledEndTime":timestamp(end),
        "realTime":false,"scheduled":true,"cancelled":false,
        "legGeometry":polyline(&coordinates)
    }))
}

fn trip_info_for_key(info: &BTreeMap<(String,String,String),TripInfo>, key: Option<TripKey>) -> TripInfo {
    let Some(key) = key else { return TripInfo::default() };
    let mut result = info.get(&(key.feed,key.attempt,key.trip)).cloned().unwrap_or_default();
    // The timetable remains authoritative even if PostgreSQL trip metadata is missing.
    result.chateau = Some(key.chateau);
    result
}

fn trip_info(tt: &ZeroCopyTimetable<'_>, index: &MetadataIndex, info: &BTreeMap<(String,String,String),TripInfo>, transport: u32) -> TripInfo {
    trip_info_for_key(info, ride_identity(tt,index,transport))
}

fn transit_leg(
    tt: &ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    info: &BTreeMap<(String,String,String),TripInfo>,
    ride: &Leg,
    detailed: bool,
) -> Value {
    let Leg::Ride {from_stop,to_stop,route,transport,service_day,board_stop_index,alight_stop_index,departure_unix_seconds,arrival_unix_seconds,..}=ride else {unreachable!()};
    let meta = trip_info(tt,index,info,*transport);
    let all = tt.stops_on(RouteIdx(*route));
    let from = place(tt,*from_stop);
    let to = place(tt,*to_stop);
    let shape = if detailed {
        match (location(tt,*from_stop),location(tt,*to_stop)) {
            (Some(a),Some(b)) => clipped_shape(&meta.shape,a,b),
            _ => Vec::new(),
        }
    } else {Vec::new()};
    let intermediate_stops: Vec<Value> = if board_stop_index < alight_stop_index {
        let first = (i64::from(tt.header().first_utc_day) + i64::from(*service_day))
            * 86_400
            + i64::from(tt.transport_first_dep_offset()[*transport as usize].seconds());
        all.iter().enumerate().take(*alight_stop_index)
            .skip(*board_stop_index + 1)
            .map(|(position, stop)| {
                let mut p = place(tt, stop.location().0);
                if let Some(arrival) = tt.event_time(RouteIdx(*route), TransportIdx(*transport), position, true) {
                    let t = timestamp(first + i64::from(arrival.seconds()));
                    p["arrival"] = json!(t);
                    p["scheduledArrival"] = json!(t);
                }
                if let Some(departure) = tt.event_time(RouteIdx(*route), TransportIdx(*transport), position, false) {
                    let t = timestamp(first + i64::from(departure.seconds()));
                    p["departure"] = json!(t);
                    p["scheduledDeparture"] = json!(t);
                }
                p
            }).collect()
    } else { Vec::new() };
    let mode = match meta.route_type {
        Some(0) => "TRAM", Some(1) => "SUBWAY", Some(2) => "RAIL",
        Some(3) => "BUS", Some(4) => "FERRY", Some(7) => "FUNICULAR",
        _ => "TRANSIT"
    };
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
    journeys: &[Journey],
    from: Coordinates,
    to: Coordinates,
    origin_osr_region: &str,
    destination_osr_region: &str,
    osr_worker: &str,
    database_url: Option<&str>,
    detailed: bool,
) -> Result<Value> {
    let info = metadata(tt,index,journeys,database_url)?;
    let mut itineraries = Vec::new();
    let mut direct = Vec::new();
    for journey in journeys {
        let mut cursor = journey.departure_unix_seconds;
        let mut previous = coordinate_place(from,"Origin");
        let mut legs = Vec::new();
        for leg in &journey.legs {
            match leg {
                Leg::Access {to_stop,seconds} => {
                    let end = place(tt,*to_stop);
                    legs.push(street_leg(osr_worker, origin_osr_region,previous,end.clone(),cursor,*seconds,detailed)?);
                    cursor += i64::from(*seconds);
                    previous = end;
                }
                Leg::Transfer {from_stop,to_stop,seconds} => {
                    let start = place(tt,*from_stop);
                    let end = place(tt,*to_stop);
                    legs.push(street_leg(osr_worker, origin_osr_region,start,end.clone(),cursor,*seconds,detailed)?);
                    cursor += i64::from(*seconds);
                    previous = end;
                }
                Leg::Ride {to_stop,arrival_unix_seconds,..} => {
                    legs.push(transit_leg(tt,index,&info,leg,detailed));
                    cursor = *arrival_unix_seconds;
                    previous = place(tt,*to_stop);
                }
                Leg::Egress {seconds,..} => {
                    let destination = coordinate_place(to,"Destination");
                    legs.push(street_leg(osr_worker,destination_osr_region,previous,destination.clone(),cursor,*seconds,detailed)?);
                    cursor += i64::from(*seconds);
                    previous = destination;
                }
                Leg::DirectWalk {seconds} => {
                    let destination = coordinate_place(to,"Destination");
                    legs.push(street_leg(osr_worker,origin_osr_region,previous,destination.clone(),cursor,*seconds,detailed)?);
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
        if journey.boardings == 0 {direct.push(value)} else {itineraries.push(value)}
    }
    Ok(json!({
        "requestParameters":{}, "debugOutput":{},
        "from":coordinate_place(from,"Origin"),
        "to":coordinate_place(to,"Destination"),
        "direct":direct,"itineraries":itineraries,
        "previousPageCursor":"","nextPageCursor":""
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn polyline_six_digit_encoding() {
        let encoded = polyline(&[[-120.2,38.5],[-120.95,40.7],[-126.453,43.252]]);
        assert_eq!(encoded["precision"],6);
        assert_eq!(encoded["length"],3);
        assert!(encoded["points"].as_str().unwrap().len() > 10);
    }
    #[test]
    fn clipping_retains_curvature() {
        let shape = [[0.0,0.0],[1.0,0.0],[1.0,1.0],[2.0,1.0]];
        let clipped = clipped_shape(&shape,[0.5,0.0],[1.5,1.0]);
        assert_eq!(clipped,vec![[0.5,0.0],[1.0,0.0],[1.0,1.0],[1.5,1.0]]);
    }

    #[test]
    fn transit_chateau_comes_from_timetable_source_even_without_db_metadata() {
        let key = TripKey {
            chateau: "nationalrailuk".into(),
            feed: "f-rail".into(),
            attempt: "attempt-1".into(),
            trip: "train-123".into(),
        };
        let missing = trip_info_for_key(&BTreeMap::new(), Some(key.clone()));
        assert_eq!(missing.chateau.as_deref(), Some("nationalrailuk"));
        assert_eq!(json!({"chateau":missing.chateau})["chateau"], "nationalrailuk");
        assert!(missing.short_name.is_none());

        let mut rows = BTreeMap::new();
        rows.insert(
            ("f-rail".into(), "attempt-1".into(), "train-123".into()),
            TripInfo { short_name: Some("London".into()), ..TripInfo::default() },
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
}