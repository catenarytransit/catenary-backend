// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of attribution is not allowed, AGPL license

use crate::DownloadedFeedsInformation;
use crate::gtfs_handlers::colour_correction::{
    fix_background_colour_rgb_feed_route, fix_foreground_colour_rgb_feed,
};
use crate::gtfs_handlers::shape_colour_calculator::{ShapeToColourResponse, shape_to_colour};
use crate::gtfs_handlers::stops_associated_items::make_hashmaps_of_children_stop_info;
use crate::gtfs_ingestion_sequence::calendar_into_postgres::calendar_into_postgres;
use crate::gtfs_ingestion_sequence::extra_stop_to_stop_shapes_into_postgres::insert_stop_to_stop_geometry;
use crate::gtfs_ingestion_sequence::shapes_into_postgres::shapes_into_postgres;
use crate::gtfs_ingestion_sequence::stops_into_postgres::stops_into_postgres_and_elastic;
use crate::gtfs_process::{
    GtfsSummary, continuous_pickup_drop_off_to_i16, faster_stop_time_reader_injection,
};
use crate::shapes_reader::MmapShapeReader;
use anyhow::{Context, Result};
use catenary::enum_to_int::{availability_to_int, route_type_to_int};
use catenary::maple_syrup;
use catenary::models::{
    DirectionPatternMeta, DirectionPatternRow, ItineraryPatternMeta, ItineraryPatternRow,
    Route as RoutePgModel,
};
use catenary::name_shortening_hash_insert_elastic;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::route_id_transform;
use catenary::schedule_filtering::minimum_day_filter;
use chrono::NaiveDate;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use geo::BoundingRect;
use gtfs_structures::{ContinuousPickupDropOff, FeedInfo, Gtfs};
use itertools::Itertools;
use language_tags::LanguageTag;
use prost::Message;
use regex::Regex;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

struct WriterPool {
    base_path: PathBuf,
    filename: String,
    header: String,
    writers: HashMap<String, io::BufWriter<File>>,
}

impl WriterPool {
    fn new(base_path: PathBuf, filename: String, header: String) -> Self {
        Self {
            base_path,
            filename,
            header,
            writers: HashMap::new(),
        }
    }

    fn write_line(&mut self, agency_id: &str, line: &str) -> io::Result<()> {
        if self.writers.len() >= 500 {
            self.writers.clear();
        }

        let entry = self.writers.entry(agency_id.to_string());
        let writer = match entry {
            std::collections::hash_map::Entry::Occupied(occ) => occ.into_mut(),
            std::collections::hash_map::Entry::Vacant(vac) => {
                let agency_dir = self.base_path.join(agency_id);
                fs::create_dir_all(&agency_dir)?;
                let file_path = agency_dir.join(&self.filename);
                let exists = file_path.exists();
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&file_path)?;
                let mut w = io::BufWriter::new(file);
                if !exists {
                    w.write_all(self.header.as_bytes())?;
                    w.write_all(b"\n")?;
                }
                vac.insert(w)
            }
        };
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    fn flush_all(&mut self) -> io::Result<()> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

fn get_field_by_index(line: &str, target_idx: usize) -> Option<&str> {
    let mut current_idx = 0;
    let mut start = 0;
    let mut in_quotes = false;
    for (i, c) in line.char_indices() {
        if c == '"' {
            in_quotes = !in_quotes;
        } else if c == ',' && !in_quotes {
            if current_idx == target_idx {
                return Some(&line[start..i]);
            }
            start = i + 1;
            current_idx += 1;
        }
    }
    if current_idx == target_idx {
        return Some(&line[start..]);
    }
    None
}

fn clean_field(s: &str) -> String {
    let trimmed = s.trim();
    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        if trimmed.len() >= 2 {
            trimmed[1..trimmed.len() - 1].trim().to_string()
        } else {
            "".to_string()
        }
    } else {
        trimmed.to_string()
    }
}

fn sanitize_agency_id(agency_id: &str) -> String {
    let sanitized: String = agency_id
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "unknown_agency".to_string()
    } else {
        sanitized
    }
}

fn split_gtfs_by_agency(
    feed_dir: &str,
    global_gtfs: &Gtfs,
) -> Result<(HashMap<String, Vec<i16>>, HashMap<String, Vec<String>>), Box<dyn Error + Send + Sync>>
{
    let routes_path = format!("{}/routes.txt", feed_dir);
    let mut route_to_agency = HashMap::new();
    let mut route_to_route_type = HashMap::new();

    let routes_file = File::open(&routes_path)?;
    let mut routes_reader = BufReader::new(routes_file);
    let mut header = String::new();
    routes_reader.read_line(&mut header)?;
    let header_clean = header.trim_end();

    let route_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "route_id")
        .context("routes.txt missing route_id")?;
    let agency_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "agency_id")
        .context("routes.txt missing agency_id")?;
    let route_type_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "route_type")
        .context("routes.txt missing route_type")?;

    for line_result in routes_reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }
        if let (Some(route_id_raw), Some(agency_id_raw), Some(route_type_raw)) = (
            get_field_by_index(&line, route_id_idx),
            get_field_by_index(&line, agency_id_idx),
            get_field_by_index(&line, route_type_idx),
        ) {
            let route_id = clean_field(route_id_raw);
            let agency_id = clean_field(agency_id_raw);
            let route_type_num: i16 = clean_field(route_type_raw).parse().unwrap_or(3);
            route_to_agency.insert(route_id.clone(), agency_id);
            route_to_route_type.insert(route_id, route_type_num);
        }
    }

    let trips_path = format!("{}/trips.txt", feed_dir);
    let mut trip_to_agency = HashMap::new();
    let mut trip_to_route_id = HashMap::new();
    let mut shape_to_agencies = HashMap::<String, HashSet<String>>::new();
    let mut service_to_agencies = HashMap::<String, HashSet<String>>::new();

    let trips_file = File::open(&trips_path)?;
    let mut trips_reader = BufReader::new(trips_file);
    let mut header = String::new();
    trips_reader.read_line(&mut header)?;
    let header_clean = header.trim_end();

    let trip_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "trip_id")
        .context("trips.txt missing trip_id")?;
    let route_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "route_id")
        .context("trips.txt missing route_id")?;
    let shape_id_idx = header_clean.split(',').position(|h| h.trim() == "shape_id");
    let service_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "service_id")
        .context("trips.txt missing service_id")?;

    for line_result in trips_reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }
        if let (Some(trip_id_raw), Some(route_id_raw), Some(service_id_raw)) = (
            get_field_by_index(&line, trip_id_idx),
            get_field_by_index(&line, route_id_idx),
            get_field_by_index(&line, service_id_idx),
        ) {
            let trip_id = clean_field(trip_id_raw);
            let route_id = clean_field(route_id_raw);
            let service_id = clean_field(service_id_raw);
            let shape_id = shape_id_idx
                .and_then(|idx| get_field_by_index(&line, idx))
                .map(clean_field)
                .unwrap_or_default();

            if let Some(agency_id) = route_to_agency.get(&route_id).cloned() {
                trip_to_agency.insert(trip_id.clone(), agency_id.clone());
                trip_to_route_id.insert(trip_id, route_id);
                if !shape_id.is_empty() {
                    shape_to_agencies
                        .entry(shape_id)
                        .or_default()
                        .insert(agency_id.clone());
                }
                service_to_agencies
                    .entry(service_id)
                    .or_default()
                    .insert(agency_id);
            }
        }
    }

    let mut agency_to_stops = HashMap::<String, HashSet<String>>::new();
    let mut stop_ids_to_route_types = HashMap::<String, Vec<i16>>::new();
    let mut stop_ids_to_route_ids = HashMap::<String, Vec<String>>::new();

    let stop_times_path = format!("{}/stop_times.txt", feed_dir);
    let stop_times_file = File::open(&stop_times_path)?;
    let mut stop_times_reader = BufReader::new(stop_times_file);
    let mut header = String::new();
    stop_times_reader.read_line(&mut header)?;
    let header_clean = header.trim_end();

    let trip_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "trip_id")
        .context("stop_times.txt missing trip_id")?;
    let stop_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "stop_id")
        .context("stop_times.txt missing stop_id")?;

    let base_path = PathBuf::from(feed_dir).join("split_agencies");
    let mut stop_times_pool = WriterPool::new(
        base_path.clone(),
        "stop_times.txt".to_string(),
        header_clean.to_string(),
    );

    for line_result in stop_times_reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }
        if let (Some(trip_id_raw), Some(stop_id_raw)) = (
            get_field_by_index(&line, trip_id_idx),
            get_field_by_index(&line, stop_id_idx),
        ) {
            let trip_id = clean_field(trip_id_raw);
            let stop_id = clean_field(stop_id_raw);

            if let Some(agency_id) = trip_to_agency.get(&trip_id) {
                let sanitized_agency = sanitize_agency_id(agency_id);
                stop_times_pool.write_line(&sanitized_agency, &line)?;
                agency_to_stops
                    .entry(sanitized_agency)
                    .or_default()
                    .insert(stop_id.clone());

                if let Some(route_id) = trip_to_route_id.get(&trip_id) {
                    if let Some(&route_type_num) = route_to_route_type.get(route_id) {
                        let rt_vec = stop_ids_to_route_types.entry(stop_id.clone()).or_default();
                        if !rt_vec.contains(&route_type_num) {
                            rt_vec.push(route_type_num);
                        }
                    }
                    let r_vec = stop_ids_to_route_ids.entry(stop_id.clone()).or_default();
                    if !r_vec.contains(route_id) {
                        r_vec.push(route_id.clone());
                    }
                }
            }
        }
    }
    stop_times_pool.flush_all()?;

    let mut parent_stations = HashMap::new();
    for (stop_id, stop) in &global_gtfs.stops {
        if let Some(parent) = &stop.parent_station {
            parent_stations.insert(stop_id.clone(), parent.clone());
        }
    }
    for (_, stops_set) in agency_to_stops.iter_mut() {
        let mut parents = Vec::new();
        for stop_id in stops_set.iter() {
            if let Some(parent) = parent_stations.get(stop_id) {
                parents.push(parent.clone());
            }
        }
        for parent in parents {
            stops_set.insert(parent);
        }
    }

    let stops_path = format!("{}/stops.txt", feed_dir);
    let stops_file = File::open(&stops_path)?;
    let mut stops_reader = BufReader::new(stops_file);
    let mut header = String::new();
    stops_reader.read_line(&mut header)?;
    let header_clean = header.trim_end();

    let stop_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "stop_id")
        .context("stops.txt missing stop_id")?;
    let mut stops_pool = WriterPool::new(
        base_path.clone(),
        "stops.txt".to_string(),
        header_clean.to_string(),
    );

    for line_result in stops_reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(stop_id_raw) = get_field_by_index(&line, stop_id_idx) {
            let stop_id = clean_field(stop_id_raw);
            for (sanitized_agency, stops_set) in &agency_to_stops {
                if stops_set.contains(&stop_id) {
                    stops_pool.write_line(sanitized_agency, &line)?;
                }
            }
        }
    }
    stops_pool.flush_all()?;

    let routes_file = File::open(&routes_path)?;
    let mut routes_reader = BufReader::new(routes_file);
    let mut header = String::new();
    routes_reader.read_line(&mut header)?;
    let header_clean = header.trim_end();
    let route_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "route_id")
        .context("routes.txt missing route_id")?;
    let mut routes_pool = WriterPool::new(
        base_path.clone(),
        "routes.txt".to_string(),
        header_clean.to_string(),
    );

    for line_result in routes_reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(route_id_raw) = get_field_by_index(&line, route_id_idx) {
            let route_id = clean_field(route_id_raw);
            if let Some(agency_id) = route_to_agency.get(&route_id) {
                routes_pool.write_line(&sanitize_agency_id(agency_id), &line)?;
            }
        }
    }
    routes_pool.flush_all()?;

    let trips_file = File::open(&trips_path)?;
    let mut trips_reader = BufReader::new(trips_file);
    let mut header = String::new();
    trips_reader.read_line(&mut header)?;
    let header_clean = header.trim_end();
    let trip_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "trip_id")
        .context("trips.txt missing trip_id")?;
    let mut trips_pool = WriterPool::new(
        base_path.clone(),
        "trips.txt".to_string(),
        header_clean.to_string(),
    );

    for line_result in trips_reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(trip_id_raw) = get_field_by_index(&line, trip_id_idx) {
            let trip_id = clean_field(trip_id_raw);
            if let Some(agency_id) = trip_to_agency.get(&trip_id) {
                trips_pool.write_line(&sanitize_agency_id(agency_id), &line)?;
            }
        }
    }
    trips_pool.flush_all()?;

    let shapes_path = format!("{}/shapes.txt", feed_dir);
    if Path::new(&shapes_path).exists() {
        let shapes_file = File::open(&shapes_path)?;
        let mut shapes_reader = BufReader::new(shapes_file);
        let mut header = String::new();
        shapes_reader.read_line(&mut header)?;
        let header_clean = header.trim_end();
        let shape_id_idx = header_clean
            .split(',')
            .position(|h| h.trim() == "shape_id")
            .context("shapes.txt missing shape_id")?;
        let mut shapes_pool = WriterPool::new(
            base_path.clone(),
            "shapes.txt".to_string(),
            header_clean.to_string(),
        );

        for line_result in shapes_reader.lines() {
            let line = line_result?;
            if line.trim().is_empty() {
                continue;
            }
            if let Some(shape_id_raw) = get_field_by_index(&line, shape_id_idx) {
                let shape_id = clean_field(shape_id_raw);
                if let Some(agencies_set) = shape_to_agencies.get(&shape_id) {
                    for agency_id in agencies_set {
                        shapes_pool.write_line(&sanitize_agency_id(agency_id), &line)?;
                    }
                }
            }
        }
        shapes_pool.flush_all()?;
    }

    let calendar_path = format!("{}/calendar.txt", feed_dir);
    if Path::new(&calendar_path).exists() {
        let calendar_file = File::open(&calendar_path)?;
        let mut calendar_reader = BufReader::new(calendar_file);
        let mut header = String::new();
        calendar_reader.read_line(&mut header)?;
        let header_clean = header.trim_end();
        let service_id_idx = header_clean
            .split(',')
            .position(|h| h.trim() == "service_id")
            .context("calendar.txt missing service_id")?;
        let mut calendar_pool = WriterPool::new(
            base_path.clone(),
            "calendar.txt".to_string(),
            header_clean.to_string(),
        );

        for line_result in calendar_reader.lines() {
            let line = line_result?;
            if line.trim().is_empty() {
                continue;
            }
            if let Some(service_id_raw) = get_field_by_index(&line, service_id_idx) {
                let service_id = clean_field(service_id_raw);
                if let Some(agencies_set) = service_to_agencies.get(&service_id) {
                    for agency_id in agencies_set {
                        calendar_pool.write_line(&sanitize_agency_id(agency_id), &line)?;
                    }
                }
            }
        }
        calendar_pool.flush_all()?;
    }

    let calendar_dates_path = format!("{}/calendar_dates.txt", feed_dir);
    if Path::new(&calendar_dates_path).exists() {
        let calendar_dates_file = File::open(&calendar_dates_path)?;
        let mut calendar_dates_reader = BufReader::new(calendar_dates_file);
        let mut header = String::new();
        calendar_dates_reader.read_line(&mut header)?;
        let header_clean = header.trim_end();
        let service_id_idx = header_clean
            .split(',')
            .position(|h| h.trim() == "service_id")
            .context("calendar_dates.txt missing service_id")?;
        let mut calendar_dates_pool = WriterPool::new(
            base_path.clone(),
            "calendar_dates.txt".to_string(),
            header_clean.to_string(),
        );

        for line_result in calendar_dates_reader.lines() {
            let line = line_result?;
            if line.trim().is_empty() {
                continue;
            }
            if let Some(service_id_raw) = get_field_by_index(&line, service_id_idx) {
                let service_id = clean_field(service_id_raw);
                if let Some(agencies_set) = service_to_agencies.get(&service_id) {
                    for agency_id in agencies_set {
                        calendar_dates_pool.write_line(&sanitize_agency_id(agency_id), &line)?;
                    }
                }
            }
        }
        calendar_dates_pool.flush_all()?;
    }

    let frequencies_path = format!("{}/frequencies.txt", feed_dir);
    if Path::new(&frequencies_path).exists() {
        let frequencies_file = File::open(&frequencies_path)?;
        let mut frequencies_reader = BufReader::new(frequencies_file);
        let mut header = String::new();
        frequencies_reader.read_line(&mut header)?;
        let header_clean = header.trim_end();
        let trip_id_idx = header_clean
            .split(',')
            .position(|h| h.trim() == "trip_id")
            .context("frequencies.txt missing trip_id")?;
        let mut frequencies_pool = WriterPool::new(
            base_path.clone(),
            "frequencies.txt".to_string(),
            header_clean.to_string(),
        );

        for line_result in frequencies_reader.lines() {
            let line = line_result?;
            if line.trim().is_empty() {
                continue;
            }
            if let Some(trip_id_raw) = get_field_by_index(&line, trip_id_idx) {
                let trip_id = clean_field(trip_id_raw);
                if let Some(agency_id) = trip_to_agency.get(&trip_id) {
                    frequencies_pool.write_line(&sanitize_agency_id(agency_id), &line)?;
                }
            }
        }
        frequencies_pool.flush_all()?;
    }

    let agency_path = format!("{}/agency.txt", feed_dir);
    let agency_file = File::open(&agency_path)?;
    let mut agency_reader = BufReader::new(agency_file);
    let mut header = String::new();
    agency_reader.read_line(&mut header)?;
    let header_clean = header.trim_end();
    let agency_id_idx = header_clean
        .split(',')
        .position(|h| h.trim() == "agency_id");
    let mut agency_pool = WriterPool::new(
        base_path.clone(),
        "agency.txt".to_string(),
        header_clean.to_string(),
    );

    for line_result in agency_reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }

        let agency_id = if let Some(idx) = agency_id_idx {
            get_field_by_index(&line, idx)
                .map(clean_field)
                .unwrap_or_default()
        } else {
            "".to_string()
        };

        if agency_id.is_empty() {
            for (sanitized_agency, _) in &agency_to_stops {
                agency_pool.write_line(sanitized_agency, &line)?;
            }
        } else {
            agency_pool.write_line(&sanitize_agency_id(&agency_id), &line)?;
        }
    }
    agency_pool.flush_all()?;

    let feed_info_path = format!("{}/feed_info.txt", feed_dir);
    if Path::new(&feed_info_path).exists() {
        for (sanitized_agency, _) in &agency_to_stops {
            let dest = base_path.join(sanitized_agency).join("feed_info.txt");
            let _ = std::fs::copy(&feed_info_path, dest);
        }
    }

    Ok((stop_ids_to_route_types, stop_ids_to_route_ids))
}

fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin() * (d_lat / 2.0).sin()
        + lat1.to_radians().cos()
            * lat2.to_radians().cos()
            * (d_lon / 2.0).sin()
            * (d_lon / 2.0).sin();
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    R * c
}

fn compute_shape_envelope<'a, I>(
    reader: &MmapShapeReader,
    shape_ids: I,
) -> Option<serde_json::Value>
where
    I: IntoIterator<Item = &'a String>,
{
    let mut min_latitude: Option<f64> = None;
    let mut max_latitude: Option<f64> = None;
    let mut min_longitude: Option<f64> = None;
    let mut max_longitude: Option<f64> = None;

    for shape_id in shape_ids {
        let Some(shape_points) = reader.get_shape(shape_id.as_str()) else {
            continue;
        };

        for shape in shape_points {
            let lat = shape.geometry.y();
            let lon = shape.geometry.x();

            min_latitude = Some(min_latitude.map_or(lat, |current| current.min(lat)));
            max_latitude = Some(max_latitude.map_or(lat, |current| current.max(lat)));
            min_longitude = Some(min_longitude.map_or(lon, |current| current.min(lon)));
            max_longitude = Some(max_longitude.map_or(lon, |current| current.max(lon)));
        }
    }

    match (min_longitude, max_latitude, max_longitude, min_latitude) {
        (Some(min_lon), Some(max_lat), Some(max_lon), Some(min_lat)) => Some(json!({
            "type": "envelope",
            "coordinates": [[min_lon, max_lat], [max_lon, min_lat]]
        })),
        _ => None,
    }
}

pub async fn gtfs_process_large_feed(
    gtfs_unzipped_path: &str,
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
    this_download_data: &DownloadedFeedsInformation,
    elasticclient: Option<&elasticsearch::Elasticsearch>,
) -> Result<GtfsSummary, Box<dyn Error + Send + Sync>> {
    let regex_train_starting = regex::RegexBuilder::new(r"^(train)")
        .case_insensitive(true)
        .build()
        .unwrap();

    let start = Instant::now();
    let path = format!("{}/{}", gtfs_unzipped_path, feed_id);

    println!("Reading global GTFS metadata for {}", feed_id);
    let global_gtfs = gtfs_structures::GtfsReader::default()
        .read_shapes(false)
        .read_stop_times(false)
        .read(path.as_str())
        .context("Failed to read global GTFS via gtfs_structures")?;

    let conn_pool = arc_conn_pool.as_ref();

    println!("Inserting agencies globally for {}", feed_id);
    {
        let mut agency_id_already_done: HashSet<Option<&String>> = HashSet::new();
        let mut conn = conn_pool.get().await?;

        for agency in &global_gtfs.agencies {
            use catenary::schema::gtfs::agencies::dsl::agencies;

            if !agency_id_already_done.contains(&agency.id.as_ref()) {
                let agency_row = catenary::models::Agency {
                    static_onestop_id: feed_id.to_string(),
                    agency_id: agency.id.clone().unwrap_or_else(|| "".to_string()),
                    attempt_id: attempt_id.to_string(),
                    agency_name: agency.name.clone(),
                    agency_name_translations: None,
                    agency_url_translations: None,
                    agency_url: agency.url.clone(),
                    agency_fare_url: agency.fare_url.clone(),
                    agency_fare_url_translations: None,
                    chateau: chateau_id.to_string(),
                    agency_lang: agency.lang.clone(),
                    agency_phone: agency.phone.clone(),
                    agency_timezone: agency.timezone.clone(),
                };

                diesel::insert_into(agencies)
                    .values(agency_row)
                    .execute(&mut conn)
                    .await?;

                agency_id_already_done.insert(agency.id.as_ref());
            }
        }
    }

    println!("Splitting GTFS files by agency for {}", feed_id);
    let (stop_ids_to_route_types, stop_ids_to_route_ids) =
        split_gtfs_by_agency(&path, &global_gtfs)?;

    println!("Inserting stops globally for {}", feed_id);
    let stop_id_to_children_ids = make_hashmaps_of_children_stop_info(&global_gtfs);
    let default_lang = global_gtfs
        .feed_info
        .get(0)
        .and_then(|fi| fi.default_lang.clone())
        .or_else(|| Some("de".to_string()));

    stops_into_postgres_and_elastic(
        &global_gtfs,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
        &stop_ids_to_route_types,
        &stop_ids_to_route_ids,
        &stop_id_to_children_ids,
        None,
        &default_lang,
        elasticclient,
    )
    .await?;

    let split_dir = Path::new(&path).join("split_agencies");
    if split_dir.exists() {
        for entry in fs::read_dir(&split_dir)? {
            let entry = entry?;
            let agency_path = entry.path();
            if !agency_path.is_dir() {
                continue;
            }

            let agency_id = agency_path.file_name().unwrap().to_str().unwrap();
            println!("Processing agency partition {} for {}", agency_id, feed_id);

            let mut agency_gtfs = gtfs_structures::GtfsReader::default()
                .read_shapes(false)
                .read_stop_times(false)
                .read(agency_path.to_str().unwrap())?;

            agency_gtfs = faster_stop_time_reader_injection(
                agency_gtfs,
                &agency_path.join("stop_times.txt"),
            )?;

            let mut agency_gtfs =
                crate::gtfs_handlers::gtfs_de_cleanup::gtfs_de_cleanup(agency_gtfs);

            let today = chrono::Utc::now().naive_utc().date();
            let mut agency_gtfs =
                minimum_day_filter(agency_gtfs, today - chrono::Duration::days(7));

            if agency_gtfs.trips.is_empty() {
                println!("Skipping agency split {}, no active trips", agency_id);
                continue;
            }

            maple_syrup::service_optimisation::optimise_services(&mut agency_gtfs);
            let reduction = maple_syrup::reduce(&agency_gtfs);

            let mut route_to_direction_patterns: HashMap<
                String,
                Vec<&maple_syrup::DirectionPattern>,
            > = HashMap::new();
            for (_, direction_pattern) in &reduction.direction_patterns {
                route_to_direction_patterns
                    .entry(direction_pattern.route_id.to_string())
                    .or_default()
                    .push(direction_pattern);
            }

            let ShapeToColourResponse {
                shape_to_color_lookup,
                shape_to_text_color_lookup,
                shape_id_to_route_ids_lookup,
                route_ids_to_shape_ids,
            } = shape_to_colour(feed_id, &agency_gtfs);

            let agency_shapes_txt = agency_path.join("shapes.txt");
            let agency_shapes_minimised = match agency_shapes_txt.exists() {
                true => MmapShapeReader::new(agency_shapes_txt).ok(),
                false => None,
            };

            shapes_into_postgres(
                &agency_gtfs,
                &shape_to_color_lookup,
                &shape_to_text_color_lookup,
                feed_id,
                Arc::clone(&arc_conn_pool),
                chateau_id,
                attempt_id,
                &shape_id_to_route_ids_lookup,
                &agency_shapes_minimised,
            )
            .await?;

            calendar_into_postgres(
                &agency_gtfs,
                feed_id,
                Arc::clone(&arc_conn_pool),
                chateau_id,
                attempt_id,
            )
            .await?;

            for group in &reduction.direction_patterns.iter().chunks(5000) {
                let mut d_final: Vec<DirectionPatternMeta> = vec![];
                let mut d_rows: Vec<Vec<DirectionPatternRow>> = vec![];

                for (direction_pattern_id, direction_pattern) in group {
                    let gtfs_shape_id = match &direction_pattern.gtfs_shape_id {
                        Some(gtfs_shape_id) => gtfs_shape_id.clone(),
                        None => direction_pattern_id.to_string(),
                    };

                    let first_itin_id = reduction
                        .direction_pattern_id_to_itineraries
                        .get(direction_pattern_id)
                        .unwrap()
                        .iter()
                        .next()
                        .expect("Expected Itin for direction id");

                    let itin_pattern = reduction
                        .itineraries
                        .get(first_itin_id)
                        .expect("Did not find itin pattern");

                    if direction_pattern.gtfs_shape_id.is_none() {
                        let stop_points = direction_pattern
                            .stop_sequence
                            .iter()
                            .filter_map(|stop_id| agency_gtfs.stops.get(stop_id.as_str()))
                            .filter(|stop| {
                                stop.latitude.is_some()
                                    && stop.longitude.is_some()
                                    && !(stop.latitude.unwrap() > -1.0
                                        && stop.latitude.unwrap() < 1.0
                                        && stop.longitude.unwrap() > -1.0
                                        && stop.longitude.unwrap() < 1.0)
                            })
                            .filter_map(|stop| match (stop.latitude, stop.longitude) {
                                (Some(latitude), Some(longitude)) => {
                                    Some(postgis_diesel::types::Point {
                                        y: latitude,
                                        x: longitude,
                                        srid: Some(4326),
                                    })
                                }
                                _ => None,
                            })
                            .collect::<Vec<postgis_diesel::types::Point>>();

                        if stop_points.len() > 2 {
                            let linestring = postgis_diesel::types::LineString {
                                points: stop_points,
                                srid: Some(4326),
                            };

                            let route = agency_gtfs.routes.get(direction_pattern.route_id.as_str());

                            if let Some(route) = route {
                                let _ = insert_stop_to_stop_geometry(
                                    feed_id,
                                    attempt_id,
                                    chateau_id,
                                    route,
                                    *direction_pattern_id,
                                    &linestring,
                                    Arc::clone(&arc_conn_pool),
                                )
                                .await;
                            }
                        }
                    }

                    let direction_pattern_meta = DirectionPatternMeta {
                        chateau: chateau_id.to_string(),
                        direction_pattern_id: direction_pattern_id.to_string(),
                        headsign_or_destination: direction_pattern
                            .headsign_or_destination
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        gtfs_shape_id: Some(gtfs_shape_id.clone()),
                        fake_shape: direction_pattern.gtfs_shape_id.is_none(),
                        onestop_feed_id: feed_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                        route_id: Some(itin_pattern.route_id.clone()),
                        route_type: Some(itin_pattern.route_type),
                        direction_id: itin_pattern.direction_id,
                        direction_pattern_id_parents: Some(
                            direction_pattern
                                .direction_pattern_id_with_parents
                                .clone()
                                .to_string(),
                        ),
                        stop_headsigns_unique_list: itin_pattern
                            .stop_headsigns_unique_list
                            .as_ref()
                            .map(|x| {
                                x.into_iter()
                                    .map(|x| Some(x.to_string()))
                                    .collect::<Vec<Option<String>>>()
                            }),
                        row_count: itin_pattern.stop_sequences.len() as i32,
                    };

                    d_final.push(direction_pattern_meta);

                    let direction_pattern_rows: Vec<DirectionPatternRow> = itin_pattern
                        .stop_sequences
                        .iter()
                        .enumerate()
                        .map(|(stop_idx, stop_time)| DirectionPatternRow {
                            attempt_id: attempt_id.to_string(),
                            chateau: chateau_id.to_string(),
                            direction_pattern_id: direction_pattern_id.to_string(),
                            stop_id: stop_time.stop_id.clone(),
                            stop_sequence: stop_idx as u32,
                            onestop_feed_id: feed_id.to_string(),
                            arrival_time_since_start: stop_time.arrival_time_since_start,
                            departure_time_since_start: stop_time.departure_time_since_start,
                            interpolated_time_since_start: stop_time.interpolated_time_since_start,
                            stop_headsign_idx: match &stop_time.stop_headsign {
                                Some(this_stop_headsign) => direction_pattern
                                    .stop_headsigns_unique_list
                                    .as_ref()
                                    .map(|direction_headsigns| {
                                        direction_headsigns
                                            .iter()
                                            .position(|x| x == this_stop_headsign)
                                            .map(|x| x as i16)
                                    })
                                    .flatten(),
                                None => None,
                            },
                        })
                        .collect();

                    for dir_chunk in direction_pattern_rows.chunks(1000) {
                        d_rows.push(dir_chunk.to_vec());
                    }
                }

                let mut conn = conn_pool.get().await?;
                conn.build_transaction()
                    .run::<(), diesel::result::Error, _>(|conn| {
                        Box::pin(async move {
                            for dir_chunk in d_final.chunks(100) {
                                diesel::insert_into(
                                    catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta,
                                )
                                .values(dir_chunk)
                                .execute(conn)
                                .await?;
                            }
                            for dir_chunk in d_rows {
                                diesel::insert_into(
                                    catenary::schema::gtfs::direction_pattern::dsl::direction_pattern,
                                )
                                .values(dir_chunk)
                                .execute(conn)
                                .await?;
                            }
                            Ok(())
                        })
                    })
                    .await?;
            }

            for group in &reduction.itineraries.iter().chunks(5000) {
                let mut t_final: Vec<ItineraryPatternMeta> = vec![];
                let mut t_rows: Vec<Vec<ItineraryPatternRow>> = vec![];

                for (itinerary_id, itinerary) in group {
                    let itinerary_pg_meta = ItineraryPatternMeta {
                        onestop_feed_id: feed_id.to_string(),
                        chateau: chateau_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                        timezone: itinerary.timezone.clone(),
                        trip_headsign: itinerary.trip_headsign.clone(),
                        trip_headsign_translations: None,
                        itinerary_pattern_id: itinerary_id.to_string(),
                        trip_ids: reduction
                            .itineraries_to_trips
                            .get(itinerary_id)
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|trip_under_itin| Some(trip_under_itin.trip_id.to_string()))
                            .collect::<Vec<Option<String>>>(),
                        shape_id: itinerary.shape_id.clone(),
                        route_id: itinerary.route_id.clone(),
                        direction_pattern_id: Some(itinerary.direction_pattern_id.to_string()),
                        row_count: itinerary.stop_sequences.len() as i32,
                    };

                    t_final.push(itinerary_pg_meta);

                    let itinerary_pg = itinerary
                        .stop_sequences
                        .iter()
                        .enumerate()
                        .map(|(stop_index, stop_sequence)| ItineraryPatternRow {
                            onestop_feed_id: feed_id.to_string(),
                            chateau: chateau_id.to_string(),
                            attempt_id: attempt_id.to_string(),
                            itinerary_pattern_id: itinerary_id.to_string(),
                            stop_sequence: stop_index as i32,
                            stop_id: stop_sequence.stop_id.clone(),
                            gtfs_stop_sequence: stop_sequence.gtfs_stop_sequence as u32,
                            arrival_time_since_start: stop_sequence.arrival_time_since_start,
                            departure_time_since_start: stop_sequence.departure_time_since_start,
                            interpolated_time_since_start: stop_sequence
                                .interpolated_time_since_start,
                            timepoint: stop_sequence.timepoint.into(),
                            stop_headsign_idx: match stop_sequence.stop_headsign {
                                Some(ref stop_headsign) => {
                                    itinerary.stop_headsigns_unique_list.as_ref().and_then(|x| {
                                        x.iter().position(|x| x == stop_headsign).map(|x| x as i16)
                                    })
                                }
                                None => None,
                            },
                        })
                        .collect::<Vec<_>>();

                    for itinerary_chunk in itinerary_pg.chunks(1000) {
                        t_rows.push(itinerary_chunk.to_vec());
                    }
                }

                let mut conn = conn_pool.get().await?;
                conn.build_transaction()
                    .run::<(), diesel::result::Error, _>(|conn| {
                        Box::pin(async move {
                            for itinerary_chunk in t_final.chunks(1000) {
                                diesel::insert_into(
                                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta,
                                )
                                .values(itinerary_chunk)
                                .execute(conn)
                                .await?;
                            }

                            for itinerary_chunk in t_rows {
                                diesel::insert_into(
                                    catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern,
                                )
                                .values(&itinerary_chunk)
                                .execute(conn)
                                .await?;
                            }

                            Ok(())
                        })
                    })
                    .await?;
            }

            for group in &reduction.itineraries_to_trips.iter().chunks(5000) {
                let mut t_final: Vec<Vec<catenary::models::CompressedTrip>> = vec![];
                for (itinerary_id, compressed_trip_list) in group {
                    let trip_pg = compressed_trip_list
                        .iter()
                        .map(|compressed_trip_raw| catenary::models::CompressedTrip {
                            onestop_feed_id: feed_id.to_string(),
                            chateau: chateau_id.to_string(),
                            attempt_id: attempt_id.to_string(),
                            itinerary_pattern_id: itinerary_id.to_string(),
                            trip_id: compressed_trip_raw.trip_id.to_string(),
                            service_id: compressed_trip_raw.service_id.clone(),
                            direction_id: reduction
                                .itineraries
                                .get(itinerary_id)
                                .unwrap()
                                .direction_id,
                            start_time: compressed_trip_raw.start_time,
                            trip_short_name: compressed_trip_raw.trip_short_name.clone().map(|x| {
                                CompactString::from(
                                    regex_train_starting.replace(x.as_str(), "").to_string(),
                                )
                            }),
                            block_id: compressed_trip_raw.block_id.clone(),
                            wheelchair_accessible: compressed_trip_raw.wheelchair_accessible,
                            bikes_allowed: compressed_trip_raw.bikes_allowed,
                            has_frequencies: !compressed_trip_raw.frequencies.is_empty(),
                            route_id: route_id_transform(
                                feed_id,
                                compressed_trip_raw.route_id.clone(),
                            ),
                            frequencies: match !compressed_trip_raw.frequencies.is_empty() {
                                true => {
                                    let prost_message =
                                        catenary::gtfs_schedule_protobuf::frequencies_to_protobuf(
                                            &compressed_trip_raw.frequencies,
                                        );
                                    Some(prost_message.encode_to_vec())
                                }
                                false => None,
                            },
                        })
                        .collect::<Vec<_>>();

                    for trip_chunk in trip_pg.chunks(2000) {
                        t_final.push(trip_chunk.to_vec());
                    }
                }

                let mut conn = conn_pool.get().await?;
                conn.build_transaction()
                    .run::<(), diesel::result::Error, _>(|conn| {
                        Box::pin(async move {
                            for trip_chunk in t_final {
                                diesel::insert_into(
                                    catenary::schema::gtfs::trips_compressed::dsl::trips_compressed,
                                )
                                .values(trip_chunk)
                                .execute(conn)
                                .await?;
                            }

                            Ok(())
                        })
                    })
                    .await?;
            }

            let routes_pg: Vec<RoutePgModel> = agency_gtfs
                .routes
                .iter()
                .map(|(route_id, route)| {
                    let colour = fix_background_colour_rgb_feed_route(feed_id, route.color, route);
                    let text_colour =
                        fix_foreground_colour_rgb_feed(feed_id, route.color, route.text_color);

                    let colour_pg = format!("#{:02x}{:02x}{:02x}", colour.r, colour.g, colour.b);
                    let text_colour_pg = format!(
                        "#{:02x}{:02x}{:02x}",
                        text_colour.r, text_colour.g, text_colour.b
                    );

                    let route_pg = RoutePgModel {
                        onestop_feed_id: feed_id.to_string(),
                        route_id: route_id_transform(feed_id, route_id.to_string()),
                        attempt_id: attempt_id.to_string(),
                        agency_id: route.agency_id.clone(),
                        short_name: route.short_name.clone(),
                        long_name: route.long_name.clone(),
                        chateau: chateau_id.to_string(),
                        color: Some(colour_pg),
                        text_color: Some(text_colour_pg),
                        short_name_translations: None,
                        long_name_translations: None,
                        gtfs_desc: route.desc.clone(),
                        gtfs_desc_translations: None,
                        route_type: route_type_to_int(&route.route_type),
                        url: route.url.clone(),
                        url_translations: None,
                        shapes_list: route_ids_to_shape_ids.get(route_id).map(|shapes_list| {
                            shapes_list
                                .iter()
                                .map(|x| Some(x.clone()))
                                .collect::<Vec<Option<String>>>()
                        }),
                        gtfs_order: route.order,
                        continuous_drop_off: continuous_pickup_drop_off_to_i16(
                            &route.continuous_drop_off,
                        ),
                        continuous_pickup: continuous_pickup_drop_off_to_i16(
                            &route.continuous_pickup,
                        ),
                    };
                    route_pg
                })
                .collect();

            let mut finished_route_chunks_elasticsearch: Vec<
                Vec<elasticsearch::http::request::JsonBody<_>>,
            > = Vec::new();

            for route_chunk in &agency_gtfs.routes.iter().chunks(256) {
                let mut insertable_elastic: Vec<elasticsearch::http::request::JsonBody<_>> =
                    Vec::new();
                for (route_id, route) in route_chunk {
                    let envelope = agency_shapes_minimised.as_ref().and_then(|reader| {
                        route_ids_to_shape_ids
                            .get(route_id)
                            .and_then(|shape_ids| compute_shape_envelope(reader, shape_ids.iter()))
                    });

                    let mut short_name_translations_shortened_locales_elastic: HashMap<
                        String,
                        String,
                    > = HashMap::new();
                    let mut long_name_translations_shortened_locales_elastic: HashMap<
                        String,
                        String,
                    > = HashMap::new();
                    let mut important_points: Vec<[f64; 2]> = Vec::new();
                    let mut important_points_set: HashSet<(i32, i32)> = HashSet::new();

                    if let Some(direction_patterns) = route_to_direction_patterns.get(route_id) {
                        for direction_pattern in &*direction_patterns {
                            let stops: Vec<_> = direction_pattern
                                .stop_sequence
                                .iter()
                                .filter_map(|stop_id| agency_gtfs.stops.get(stop_id.as_str()))
                                .collect();

                            if let Some(start_stop) = stops.iter().next() {
                                if let (Some(lat), Some(lon)) =
                                    (start_stop.latitude, start_stop.longitude)
                                {
                                    let lat_i = (lat * 1_000_000.0) as i32;
                                    let lon_i = (lon * 1_000_000.0) as i32;
                                    if important_points_set.insert((lat_i, lon_i)) {
                                        important_points.push([lon, lat]);
                                    }
                                }
                            }

                            if let Some(end_stop) = stops.iter().last() {
                                if let (Some(lat), Some(lon)) =
                                    (end_stop.latitude, end_stop.longitude)
                                {
                                    let lat_i = (lat * 1_000_000.0) as i32;
                                    let lon_i = (lon * 1_000_000.0) as i32;
                                    if important_points_set.insert((lat_i, lon_i)) {
                                        important_points.push([lon, lat]);
                                    }
                                }
                            }
                        }

                        for direction_pattern in &*direction_patterns {
                            let stops: Vec<_> = direction_pattern
                                .stop_sequence
                                .iter()
                                .filter_map(|stop_id| agency_gtfs.stops.get(stop_id.as_str()))
                                .collect();

                            for stop in stops {
                                if let (Some(lat), Some(lon)) = (stop.latitude, stop.longitude) {
                                    let lat_i = (lat * 1_000_000.0) as i32;
                                    let lon_i = (lon * 1_000_000.0) as i32;

                                    if important_points_set.contains(&(lat_i, lon_i)) {
                                        continue;
                                    }

                                    let is_far_enough = important_points
                                        .iter()
                                        .all(|p| haversine_distance(lat, lon, p[1], p[0]) >= 50.0);

                                    if is_far_enough {
                                        important_points.push([lon, lat]);
                                        important_points_set.insert((lat_i, lon_i));
                                    }
                                }
                            }
                        }
                    }

                    let route_id = route_id_transform(feed_id, route_id.to_string());
                    let elastic_id = format!("{}_{}_{}", feed_id, attempt_id, route_id);
                    let route_type = route_type_to_int(&route.route_type);

                    let agency_name = match agency_gtfs.agencies.len() {
                        0 => String::new(),
                        1 => agency_gtfs.agencies[0].name.clone(),
                        _ => match agency_gtfs
                            .agencies
                            .iter()
                            .find(|agency| agency.id == route.agency_id)
                        {
                            Some(agency) => agency.name.clone(),
                            None => String::new(),
                        },
                    };

                    if let Some(default_lang_str) = &default_lang {
                        if let Some(short_name) = &route.short_name {
                            if let Ok(lang_tag) = LanguageTag::parse(default_lang_str.as_str()) {
                                name_shortening_hash_insert_elastic(
                                    &mut short_name_translations_shortened_locales_elastic,
                                    &lang_tag,
                                    short_name.as_str(),
                                );
                            }
                        }

                        if let Some(long_name) = &route.long_name {
                            if let Ok(lang_tag) = LanguageTag::parse(default_lang_str.as_str()) {
                                name_shortening_hash_insert_elastic(
                                    &mut long_name_translations_shortened_locales_elastic,
                                    &lang_tag,
                                    long_name.as_str(),
                                );
                            }
                        }
                    } else {
                        if let Some(short_name) = &route.short_name {
                            short_name_translations_shortened_locales_elastic
                                .insert("en".to_string(), short_name.clone());
                        }

                        if let Some(long_name) = &route.long_name {
                            long_name_translations_shortened_locales_elastic
                                .insert("en".to_string(), long_name.clone());
                        }
                    }

                    if important_points.len() > 1 {
                        insertable_elastic
                            .push(json!({"index": {"_index": "routes", "_id": elastic_id}}).into());
                        insertable_elastic.push(
                            json!({
                                "chateau": chateau_id.to_string(),
                                "attempt_id": attempt_id.to_string(),
                                "route_id": route_id,
                                "route_type": route_type,
                                "agency_name_search": agency_name,
                                "route_short_name": serde_json::to_value(&short_name_translations_shortened_locales_elastic).unwrap(),
                                "route_long_name": serde_json::to_value(&long_name_translations_shortened_locales_elastic).unwrap(),
                                "bbox": envelope,
                                "important_points": important_points
                            })
                            .into(),
                        );
                    }
                }

                finished_route_chunks_elasticsearch.push(insertable_elastic);
            }

            {
                let mut conn = conn_pool.get().await?;
                conn.build_transaction()
                    .run::<(), diesel::result::Error, _>(|conn| {
                        Box::pin(async move {
                            for route_chunk in routes_pg.chunks(50) {
                                diesel::insert_into(catenary::schema::gtfs::routes::dsl::routes)
                                    .values(route_chunk)
                                    .execute(conn)
                                    .await?;
                            }
                            Ok(())
                        })
                    })
                    .await?;
            }

            if let Some(elasticclient) = elasticclient {
                for chunk in finished_route_chunks_elasticsearch {
                    let response = elasticclient
                        .bulk(elasticsearch::BulkParts::Index("routes"))
                        .body(chunk)
                        .send()
                        .await?;

                    let response_body = response.json::<serde_json::Value>().await?;
                    let mut print_err = true;
                    if response_body.get("errors").map(|x| x.as_bool()).flatten() == Some(false) {
                        print_err = false;
                    }
                    if print_err {
                        println!("elastic routes response error: {:#?}", response_body);
                    }
                }
            }
        }
    }

    println!("Calculating feed boundary hull for {}", feed_id);
    let hull = crate::gtfs_handlers::hull_from_gtfs::hull_from_gtfs(&global_gtfs, feed_id);
    let bbox = hull.as_ref().map(|hull| hull.bounding_rect()).flatten();

    let mut gtfs_summary = GtfsSummary {
        feed_start_date: None,
        feed_end_date: None,
        languages_avaliable: HashSet::new(),
        default_lang: default_lang.clone(),
        general_timezone: match global_gtfs.agencies.len() {
            0 => String::from("Etc/UTC"),
            _ => global_gtfs.agencies[0].timezone.clone(),
        },
        bbox,
    };

    let feed_info = global_gtfs.feed_info.get(0).cloned();
    if let Some(feed_info) = &feed_info {
        gtfs_summary.feed_start_date = feed_info.start_date;
        gtfs_summary.feed_end_date = feed_info.end_date;

        if let Some(dl) = &feed_info.default_lang {
            gtfs_summary.default_lang = Some(dl.clone());
            gtfs_summary.languages_avaliable.insert(dl.clone());
        }

        use catenary::schema::gtfs::feed_info::dsl::feed_info as feed_table;
        let feed_info_pg = catenary::models::FeedInfo {
            onestop_feed_id: feed_id.to_string(),
            feed_publisher_name: feed_info.name.clone(),
            feed_publisher_url: feed_info.url.clone(),
            feed_lang: feed_info.lang.clone(),
            feed_start_date: feed_info.start_date,
            feed_end_date: feed_info.end_date,
            feed_version: feed_info.version.clone(),
            feed_contact_email: feed_info.contact_email.clone(),
            feed_contact_url: feed_info.contact_url.clone(),
            attempt_id: attempt_id.to_string(),
            default_lang: feed_info.default_lang.clone(),
            chateau: chateau_id.to_string(),
        };

        let mut conn = conn_pool.get().await?;
        diesel::insert_into(feed_table)
            .values(feed_info_pg)
            .execute(&mut conn)
            .await?;
    }

    let hull_pg = hull.map(|polygon_geo| postgis_diesel::types::Polygon {
        rings: vec![
            polygon_geo
                .into_inner()
                .0
                .into_iter()
                .map(|coord| {
                    postgis_diesel::types::Point::new(coord.x, coord.y, Some(catenary::WGS_84_SRID))
                })
                .collect(),
        ],
        srid: Some(catenary::WGS_84_SRID),
    });

    let languages_avaliable_pg = gtfs_summary
        .languages_avaliable
        .iter()
        .map(|x| Some(x.clone()))
        .collect::<Vec<Option<String>>>();

    let static_feed_pg = catenary::models::StaticFeed {
        onestop_feed_id: feed_id.to_string(),
        chateau: chateau_id.to_string(),
        default_lang: feed_info.map(|fi| fi.lang.clone()),
        previous_chateau_name: chateau_id.to_string(),
        languages_avaliable: languages_avaliable_pg.clone(),
        hull: hull_pg.clone(),
    };

    {
        let mut conn = conn_pool.get().await?;
        let _ = diesel::insert_into(catenary::schema::gtfs::static_feeds::dsl::static_feeds)
            .values(&static_feed_pg)
            .on_conflict(catenary::schema::gtfs::static_feeds::dsl::onestop_feed_id)
            .do_update()
            .set((
                catenary::schema::gtfs::static_feeds::dsl::languages_avaliable
                    .eq(languages_avaliable_pg),
                catenary::schema::gtfs::static_feeds::dsl::hull
                    .eq(hull_pg.map(postgis_diesel::types::GeometryContainer::Polygon)),
            ))
            .execute(&mut conn)
            .await?;
    }

    println!("Matching stops to OSM stations for {}", feed_id);
    {
        let mut conn = conn_pool.get().await?;
        if let Err(e) = crate::osm_station_matching::match_stops_for_feed(
            &mut conn, feed_id, attempt_id, chateau_id,
        )
        .await
        {
            eprintln!(
                "Warning: OSM station matching failed for {}: {:?}",
                feed_id, e
            );
        }
    }

    println!("Cleaning up temporary agency split directories...");
    let _ = fs::remove_dir_all(split_dir);

    println!(
        "Finished processing large feed {}, took {:.3}s",
        feed_id,
        start.elapsed().as_secs_f32()
    );

    Ok(gtfs_summary)
}
